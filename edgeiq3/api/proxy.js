export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { dgId, sport, action } = req.query;
  const scraperKey = process.env.SCRAPER_API_KEY;
  const oddsKey   = process.env.ODDS_API_KEY;

  // ── FETCH HELPERS ────────────────────────────────────────────────
  async function fetchDirect(url, timeout = 8000) {
    return fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/html, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
      },
      signal: AbortSignal.timeout(timeout),
    });
  }

  async function fetchScraper(url, render = false, timeout = 20000) {
    if (!scraperKey) return fetchDirect(url, timeout);
    const credits = render ? '&render=true' : '';
    const proxyUrl = `http://api.scraperapi.com?api_key=${scraperKey}&url=${encodeURIComponent(url)}${credits}`;
    return fetch(proxyUrl, { signal: AbortSignal.timeout(timeout) });
  }

  // ── OWNERSHIP ACTION ─────────────────────────────────────────────
  if (action === 'ownership') {
    const sp = sport || 'nba';
    const ownershipMap = await fetchAllOwnership(sp);
    return res.status(200).json({
      ownership: ownershipMap,
      sourcesHit: Object.keys(ownershipMap).length,
    });
  }

  // ── FETCH OWNERSHIP FROM ALL FREE SOURCES ────────────────────────
  async function fetchAllOwnership(sp) {
    const sources = await Promise.allSettled([
      fetchFantasyProsOwnership(sp),
      fetchRotogrindersOwnership(sp),
      fetchNumberFireOwnership(sp),
      fetchDailyFantasyFuelOwnership(sp),
    ]);

    // Aggregate all sources — average ownership % across sources
    const combined = {};
    const counts   = {};

    for (const result of sources) {
      if (result.status !== 'fulfilled') continue;
      const data = result.value || {};
      for (const [name, pct] of Object.entries(data)) {
        if (!name || typeof pct !== 'number') continue;
        combined[name] = (combined[name] || 0) + pct;
        counts[name]   = (counts[name]   || 0) + 1;
      }
    }

    // Average across sources
    const averaged = {};
    for (const name of Object.keys(combined)) {
      averaged[name] = Math.round(combined[name] / counts[name]);
    }
    return averaged;
  }

  // ── SOURCE 1: FANTASYPROS DFS ────────────────────────────────────
  async function fetchFantasyProsOwnership(sp) {
    const urls = {
      nba: 'https://www.fantasypros.com/nba/dfs/draftkings-player-ownership-projections.php',
      nfl: 'https://www.fantasypros.com/nfl/dfs/draftkings-player-ownership-projections.php',
      mlb: 'https://www.fantasypros.com/mlb/dfs/draftkings-player-ownership-projections.php',
    };
    try {
      const r = await fetchScraper(urls[sp] || urls.nba, false, 15000);
      if (!r.ok) return {};
      const html = await r.text();
      return parseOwnershipHTML(html, 'fantasypros');
    } catch(e) { return {}; }
  }

  // ── SOURCE 2: ROTOGRINDERS ───────────────────────────────────────
  async function fetchRotogrindersOwnership(sp) {
    const urls = {
      nba: 'https://rotogrinders.com/projected-ownership/nba?site=draftkings',
      nfl: 'https://rotogrinders.com/projected-ownership/nfl?site=draftkings',
      mlb: 'https://rotogrinders.com/projected-ownership/mlb?site=draftkings',
    };
    try {
      // Needs JavaScript rendering
      const r = await fetchScraper(urls[sp] || urls.nba, true, 25000);
      if (!r.ok) return {};
      const html = await r.text();
      return parseOwnershipHTML(html, 'rotogrinders');
    } catch(e) { return {}; }
  }

  // ── SOURCE 3: NUMBERFIRE ─────────────────────────────────────────
  async function fetchNumberFireOwnership(sp) {
    const urls = {
      nba: 'https://www.numberfire.com/nba/daily-fantasy/daily-basketball-projections',
      nfl: 'https://www.numberfire.com/nfl/daily-fantasy/daily-football-projections',
      mlb: 'https://www.numberfire.com/mlb/daily-fantasy/daily-baseball-projections',
    };
    try {
      const r = await fetchScraper(urls[sp] || urls.nba, false, 15000);
      if (!r.ok) return {};
      const html = await r.text();
      return parseOwnershipHTML(html, 'numberfire');
    } catch(e) { return {}; }
  }

  // ── SOURCE 4: DAILYFANTASYFUEL ───────────────────────────────────
  async function fetchDailyFantasyFuelOwnership(sp) {
    const urls = {
      nba: 'https://www.dailyfantasyfuel.com/nba',
      nfl: 'https://www.dailyfantasyfuel.com/nfl',
      mlb: 'https://www.dailyfantasyfuel.com/mlb',
    };
    try {
      const r = await fetchScraper(urls[sp] || urls.nba, false, 15000);
      if (!r.ok) return {};
      const html = await r.text();
      return parseOwnershipHTML(html, 'dff');
    } catch(e) { return {}; }
  }

  // ── UNIVERSAL OWNERSHIP HTML PARSER ──────────────────────────────
  function parseOwnershipHTML(html, source) {
    const ownership = {};

    // Method 1: Extract from embedded JSON in script tags
    const scriptPatterns = [
      /window\.__data\s*=\s*({.+?});?\s*<\/script>/s,
      /window\.__NUXT__\s*=\s*({.+?});?\s*<\/script>/s,
      /var\s+(?:ecrData|playerData|projData|dfsData)\s*=\s*({.+?});?\s*(?:var|<\/script>)/s,
      /"players"\s*:\s*(\[.+?\])\s*[,}]/s,
      /ownership.*?(\[[\s\S]+?\])/,
    ];

    for (const pattern of scriptPatterns) {
      try {
        const match = html.match(pattern);
        if (!match) continue;
        const parsed = JSON.parse(match[1]);
        const players = parsed?.players || parsed?.data?.players || parsed;
        if (!Array.isArray(players)) continue;

        for (const p of players) {
          const name = p.player_name || p.name || p.playerName || p.displayName || '';
          const own  = parseFloat(p.ownership || p.projected_ownership || p.own || p.pOwn || 0);
          if (name && own > 0) ownership[name.toLowerCase()] = own;
        }
        if (Object.keys(ownership).length > 5) return ownership;
      } catch(e) { continue; }
    }

    // Method 2: Parse HTML tables — look for name + percentage pairs
    const rowPattern = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    const rows = [...html.matchAll(rowPattern)];

    for (const row of rows) {
      const rowHtml = row[1];
      // Extract all text content from cells
      const cells = [...rowHtml.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)]
        .map(c => c[1].replace(/<[^>]+>/g, '').trim());

      // Look for pattern: name cell followed by percentage cell
      for (let i = 0; i < cells.length - 1; i++) {
        const name = cells[i];
        const pctStr = cells[i + 1] || cells[i + 2] || '';
        const pct = parseFloat(pctStr.replace('%', ''));

        if (name.length > 3 && name.length < 40 && pct > 0 && pct < 100) {
          // Looks like a valid player name + ownership pair
          if (/^[A-Z][a-z]+ [A-Z]/.test(name) || /^[A-Z]\. [A-Z]/.test(name)) {
            ownership[name.toLowerCase()] = pct;
          }
        }
      }
    }

    // Method 3: Look for inline JSON arrays with ownership data
    const inlinePattern = /\{[^{}]*(?:"name"|"player")[^{}]*(?:"own"|"ownership")[^{}]*\}/g;
    const inlineMatches = html.match(inlinePattern) || [];
    for (const match of inlineMatches.slice(0, 200)) {
      try {
        const obj = JSON.parse(match);
        const name = obj.name || obj.player || obj.playerName || '';
        const own  = parseFloat(obj.own || obj.ownership || 0);
        if (name && own > 0) ownership[name.toLowerCase()] = own;
      } catch(e) { continue; }
    }

    return ownership;
  }

  // ── SLEEPER PROJECTIONS (free, no key) ───────────────────────────
  async function fetchSleeperProjections(sp) {
    try {
      const sleeperSport = { nfl:'nfl', nba:'nba', mlb:'mlb' }[sp] || 'nba';
      const season = '2025';
      const now = new Date();
      const weekNum = sp === 'nfl'
        ? Math.ceil((now - new Date('2025-09-04')) / (7*24*60*60*1000))
        : Math.ceil((now - new Date('2025-10-22')) / (7*24*60*60*1000));
      const week = Math.max(1, Math.min(weekNum, 30));

      const positions = sp === 'nba'
        ? 'PG,SG,SF,PF,C'
        : sp === 'nfl'
        ? 'QB,RB,WR,TE,K,DEF'
        : 'SP,RP,C,1B,2B,3B,SS,OF';

      const posParams = positions.split(',').map(p => `position[]=${p}`).join('&');
      const url = `https://api.sleeper.app/v1/projections/${sleeperSport}/${season}/${week}?season_type=regular&${posParams}&order_by=pts_half_ppr`;

      const r = await fetchDirect(url, 8000);
      if (!r.ok) return {};
      const data = await r.json();

      const projMap = {};
      for (const [, proj] of Object.entries(data || {})) {
        if (!proj?.player?.full_name) continue;
        const name = proj.player.full_name.toLowerCase();
        const pts  = proj.stats?.pts_half_ppr || proj.stats?.pts_ppr ||
                     proj.stats?.fpts || proj.stats?.pts || 0;
        if (pts > 0) projMap[name] = { pts, stats: proj.stats };
      }
      return projMap;
    } catch(e) { return {}; }
  }

  // ── VEGAS ODDS ────────────────────────────────────────────────────
  async function fetchVegasOdds(sp) {
    if (!oddsKey) return {};
    try {
      const sportKeys = {
        nfl: 'americanfootball_nfl',
        nba: 'basketball_nba',
        mlb: 'baseball_mlb',
        nhl: 'icehockey_nhl',
      };
      const url = `https://api.the-odds-api.com/v4/sports/${sportKeys[sp]||'basketball_nba'}/odds/?apiKey=${oddsKey}&regions=us&markets=totals,h2h&oddsFormat=american`;
      const r = await fetchDirect(url, 8000);
      if (!r.ok) return {};
      const games = await r.json();
      const teamTotals = {};
      for (const game of games) {
        const total = game.bookmakers?.[0]?.markets?.find(m=>m.key==='totals')?.outcomes?.[0]?.point;
        if (total) {
          teamTotals[game.home_team] = total / 2;
          teamTotals[game.away_team] = total / 2;
          teamTotals[`${game.home_team}_total`] = total;
          teamTotals[`${game.away_team}_total`] = total;
        }
      }
      return teamTotals;
    } catch(e) { return {}; }
  }

  // ── FUZZY NAME MATCH ─────────────────────────────────────────────
  function fuzzyMatch(dkName, dataMap) {
    if (!dkName || !dataMap) return null;
    const clean = n => n.toLowerCase().replace(/[^a-z ]/g,'').trim();
    const dk = clean(dkName);
    if (dataMap[dk] !== undefined) return dataMap[dk];
    // Try last name only
    const last = dk.split(' ').pop();
    if (last.length > 3) {
      const key = Object.keys(dataMap).find(k => k.endsWith(last));
      if (key) return dataMap[key];
    }
    // Try first + last initial
    const parts = dk.split(' ');
    if (parts.length >= 2) {
      const abbr = `${parts[0][0]}. ${parts[parts.length-1]}`;
      if (dataMap[abbr] !== undefined) return dataMap[abbr];
    }
    return null;
  }

  // ── RESOLVE CONTEST ID → DRAFT GROUP ID ──────────────────────────
  if (!dgId) return res.status(400).json({ error: 'No ID provided' });

  let draftGroupId = dgId;
  try {
    const r = await fetchScraper(`https://api.draftkings.com/contests/v1/contests/${dgId}?format=json`);
    if (r.ok) {
      const data = await r.json();
      const resolved = data?.contest?.draftGroupId || data?.data?.contest?.draftGroupId;
      if (resolved) draftGroupId = String(resolved);
    }
  } catch(e) {}

  const sp = sport || 'nba';

  // ── FETCH ALL DATA IN PARALLEL ────────────────────────────────────
  const [oddsRes, sleeperRes, ownershipRes] = await Promise.allSettled([
    fetchVegasOdds(sp),
    fetchSleeperProjections(sp),
    fetchAllOwnership(sp),
  ]);

  const odds       = oddsRes.status       === 'fulfilled' ? oddsRes.value       : {};
  const projections = sleeperRes.status   === 'fulfilled' ? sleeperRes.value    : {};
  const ownership  = ownershipRes.status  === 'fulfilled' ? ownershipRes.value  : {};

  // ── FETCH DRAFTKINGS PLAYERS ──────────────────────────────────────
  const endpoints = [
    `https://www.draftkings.com/lineup/getavailableplayers?draftGroupId=${draftGroupId}`,
    `https://api.draftkings.com/lineups/v1/getavailableplayers?draftGroupId=${draftGroupId}`,
    `https://api.draftkings.com/draftgroups/v1/draftgroups/${draftGroupId}/draftables?format=json`,
  ];

  for (const url of endpoints) {
    try {
      const r = await fetchScraper(url);
      if (!r.ok) continue;
      const data = await r.json();
      const players = data?.playerList || data?.draftables || data?.data?.draftables || data?.players;
      if (!players?.length) continue;

      // ── ATTACH ALL DATA TO EACH PLAYER ───────────────────────────
      const enriched = players.map(p => {
        const name = p.displayName || p.playerName || '';
        const team = p.teamAbbreviation || p.teamAbbrev || p.team || '';

        // Vegas
        const teamOdds = Object.entries(odds).find(([k]) =>
          k.toLowerCase().includes(team.toLowerCase()) ||
          team.toLowerCase().includes(k.toLowerCase().split(' ').pop())
        );
        const vegasImplied = teamOdds ? teamOdds[1] : null;
        const gameTotal    = teamOdds ? odds[`${teamOdds[0]}_total`] : null;

        // Real projections from Sleeper
        const sleeperProj  = fuzzyMatch(name, Object.fromEntries(
          Object.entries(projections).map(([k,v]) => [k, v.pts])
        ));

        // Real ownership from scraped sources
        const fpOwnership  = fuzzyMatch(name, ownership);

        // Injury status
        const status = p.status || p.playerGameAttribute?.injuryStatus || '';
        const isOut  = ['out','ir','o','injured reserve'].includes(status.toLowerCase());

        return {
          ...p,
          vegasImplied,
          gameTotal,
          realProjection: sleeperProj,
          fpOwnership,
          status,
          isOut,
        };
      });

      const ownHits  = enriched.filter(p => p.fpOwnership != null).length;
      const projHits = enriched.filter(p => p.realProjection != null).length;
      const vegasHit = Object.keys(odds).length > 0;

      return res.status(200).json({
        success: true,
        players: enriched,
        contestId: dgId,
        draftGroupId,
        dataSources: {
          vegas:              vegasHit,
          sleeperProjections: projHits > 0,
          ownership:          ownHits > 0,
          gamesWithOdds:      Math.floor(Object.keys(odds).filter(k=>!k.includes('_total')).length/2),
          playersWithProj:    projHits,
          playersWithOwn:     ownHits,
          ownershipSources:   Object.keys(ownership).length > 0 ? 'rotogrinders+fantasypros+numberfire+dff' : 'estimated',
        },
      });

    } catch(e) { continue; }
  }

  return res.status(502).json({
    error: scraperKey
      ? 'Contest not open yet — try again closer to game time.'
      : 'SCRAPER_API_KEY missing from Vercel environment variables.',
    hasKey: !!scraperKey,
    hasOddsKey: !!oddsKey,
  });
}
