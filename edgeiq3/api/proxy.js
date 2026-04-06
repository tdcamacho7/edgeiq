export const config = { maxDuration: 60 }; // Vercel Pro: 60s, Free: 10s

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
    return fetch(proxyUrl, { signal: AbortSignal.timeout(Math.min(timeout, 12000)) });
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
  // Rotowire: free JSON endpoint, no ScraperAPI needed
  async function fetchRotowireOwnership(sp) {
    const path = sp === 'mlb' ? 'mlb' : sp === 'nba' ? 'nba' : 'nfl';
    try {
      const r = await fetch(
        `https://www.rotowire.com/daily/tables/optimizer-${path}.php`,
        { signal: AbortSignal.timeout(6000), headers: { 'User-Agent': 'Mozilla/5.0 Chrome/120' } }
      );
      if (!r.ok) return {};
      const data = await r.json();
      const map = {};
      (Array.isArray(data) ? data : data.players || Object.values(data) || []).forEach(p => {
        const name = (p.player_name || p.name || p.playerName || '').toLowerCase().trim();
        const own  = parseFloat(p.owned || p.ownership || p.own_pct || p.percentDrafted || 0);
        if (name && own > 0) {
          map[name] = own;
          const last = name.split(' ').pop();
          if (last.length > 3) map[last] = own;
        }
      });
      return map;
    } catch(e) { return {}; }
  }

  async function fetchAllOwnership(sp) {
    // Try Rotowire first — free, no ScraperAPI credits
    const rotowire = await fetchRotowireOwnership(sp).catch(() => ({}));
    if (Object.keys(rotowire).length > 15) return rotowire;

    // Fall back to ScraperAPI sources
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
  // ── RECENT FORM — REAL LAST 5 GAME SCORES ────────────────────────
  async function fetchRecentFormData(sp) {
    const sportPath = { nba:'basketball/nba', nfl:'football/nfl', mlb:'baseball/mlb' }[sp] || 'basketball/nba';
    try {
      // ESPN athletes stats endpoint — returns recent game logs
      const r = await fetchDirect(
        `https://site.api.espn.com/apis/site/v2/sports/${sportPath}/scoreboard?limit=10`,
        8000
      );
      if (!r.ok) return {};
      const data = await r.json();
      const formMap = {};

      // Extract player stats from completed games
      const events = data?.events || [];
      for (const event of events) {
        if (event.status?.type?.state !== 'post') continue;
        const competitors = event.competitions?.[0]?.competitors || [];
        for (const team of competitors) {
          const roster = team.roster || team.athletes || [];
          for (const athlete of roster) {
            const name = (athlete.athlete?.displayName || athlete.displayName || '').toLowerCase();
            if (!name) continue;
            const stats = athlete.statistics || athlete.stats || [];

            let pts = 0;
            if (sp === 'nba') {
              pts = parseFloat(stats.find?.(s => s.name === 'points')?.displayValue || 0);
            } else if (sp === 'nfl') {
              const passTD = parseFloat(stats.find?.(s => s.name === 'passingTouchdowns')?.displayValue || 0);
              const passYd = parseFloat(stats.find?.(s => s.name === 'passingYards')?.displayValue || 0);
              const rushTD = parseFloat(stats.find?.(s => s.name === 'rushingTouchdowns')?.displayValue || 0);
              const rushYd = parseFloat(stats.find?.(s => s.name === 'rushingYards')?.displayValue || 0);
              const recTD  = parseFloat(stats.find?.(s => s.name === 'receivingTouchdowns')?.displayValue || 0);
              const recYd  = parseFloat(stats.find?.(s => s.name === 'receivingYards')?.displayValue || 0);
              const rec    = parseFloat(stats.find?.(s => s.name === 'receptions')?.displayValue || 0);
              pts = passTD*4 + passYd*0.04 + rushTD*6 + rushYd*0.1 + recTD*6 + recYd*0.1 + rec*1;
            } else if (sp === 'mlb') {
              const hits = parseFloat(stats.find?.(s => s.name === 'hits')?.displayValue || 0);
              const hr   = parseFloat(stats.find?.(s => s.name === 'homeRuns')?.displayValue || 0);
              const rbi  = parseFloat(stats.find?.(s => s.name === 'rbi')?.displayValue || 0);
              const runs = parseFloat(stats.find?.(s => s.name === 'runs')?.displayValue || 0);
              const sb   = parseFloat(stats.find?.(s => s.name === 'stolenBases')?.displayValue || 0);
              pts = hits*3 + hr*10 + rbi*2 + runs*2 + sb*6;
            }

            if (name && pts > 0) {
              if (!formMap[name]) formMap[name] = [];
              formMap[name].push(pts);
            }
          }
        }
      }

      // Calculate weighted recent form for each player
      const WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08];
      const formScores = {};
      for (const [name, games] of Object.entries(formMap)) {
        const last5 = games.slice(-5).reverse(); // most recent first
        const weighted = last5.reduce((s, pts, i) => s + pts * (WEIGHTS[i] || 0.05), 0);
        const avg = last5.reduce((s,p) => s+p, 0) / last5.length;
        const trend = last5.length >= 2 && last5[0] > avg * 1.15 ? 'hot'
                    : last5.length >= 2 && last5[0] < avg * 0.75 ? 'cold'
                    : 'neutral';
        formScores[name] = { weightedAvg: Math.round(weighted*10)/10, trend, last5 };
      }
      return formScores;
    } catch(e) { return {}; }
  }

  async function fetchPlayerProps(sp) {
    if (!oddsKey) return {};
    try {
      const sportKeys = {
        nfl: 'americanfootball_nfl', nba: 'basketball_nba',
        mlb: 'baseball_mlb', nhl: 'icehockey_nhl',
      };
      const markets = sp === 'nba'
        ? 'player_points,player_rebounds,player_assists'
        : sp === 'nfl'
        ? 'player_pass_tds,player_rush_yds,player_reception_yds'
        : 'pitcher_strikeouts,batter_home_runs,batter_hits';
      const url = `https://api.the-odds-api.com/v4/sports/${sportKeys[sp]||'basketball_nba'}/events?apiKey=${oddsKey}`;
      const eventsRes = await fetchDirect(url, 6000);
      if (!eventsRes.ok) return {};
      const events = await eventsRes.json();
      if (!events?.length) return {};

      // Fetch props for first 4 games (free tier limit)
      const propsMap = {};
      const eventSlice = events.slice(0, 4);
      await Promise.all(eventSlice.map(async event => {
        try {
          const propUrl = `https://api.the-odds-api.com/v4/sports/${sportKeys[sp]}/events/${event.id}/odds?apiKey=${oddsKey}&regions=us&markets=${markets}&oddsFormat=american`;
          const r = await fetchDirect(propUrl, 6000);
          if (!r.ok) return;
          const data = await r.json();
          const bookmakers = data?.bookmakers || [];
          const book = bookmakers[0];
          if (!book) return;
          for (const market of (book.markets || [])) {
            for (const outcome of (market.outcomes || [])) {
              const name = outcome.description?.toLowerCase() || '';
              if (!name) continue;
              if (!propsMap[name]) propsMap[name] = {};
              const statKey = market.key.replace('player_','').replace('pitcher_','').replace('batter_','');
              if (outcome.name === 'Over') {
                propsMap[name][statKey] = { line: outcome.point, type: 'over' };
              }
            }
          }
        } catch(e) {}
      }));
      return propsMap;
    } catch(e) { return {}; }
  }

  async function fetchESPNNews(sp) {
    const sportPath = { nba:'basketball/nba', nfl:'football/nfl', mlb:'baseball/mlb' }[sp] || 'basketball/nba';
    try {
      const r = await fetchDirect(`https://site.api.espn.com/apis/site/v2/sports/${sportPath}/news?limit=20`, 5000);
      if (!r.ok) return [];
      const data = await r.json();
      const articles = data?.articles || [];
      return articles.map(a => ({
        headline: a.headline || '',
        published: a.published || '',
        description: a.description || '',
        players: (a.keywords || []).filter(k => k.type === 'athlete').map(k => k.displayName?.toLowerCase()),
      })).filter(a => a.players.length > 0);
    } catch(e) { return []; }
  }

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
  // ── INJURY STATUS CHECK ACTION ──────────────────────────────────────
  // Fetches from 3 sources and returns definitive OUT/IL player list
  if (action === 'injury_check') {
    const sp = req.query.sp || 'mlb';
    const outPlayers = {};  // name.toLowerCase() -> { status, source }

    // SOURCE 1: ESPN Injuries API
    try {
      const sportPath = sp === 'mlb' ? 'baseball/mlb' : sp === 'nba' ? 'basketball/nba' : 'football/nfl';
      const r = await fetch(
        `https://site.api.espn.com/apis/site/v2/sports/${sportPath}/injuries`,
        { signal: AbortSignal.timeout(5000) }
      );
      if (r.ok) {
        const data = await r.json();
        for (const team of (data?.injuries || [])) {
          for (const inj of (team.injuries || [])) {
            const name = (inj?.athlete?.displayName || '').toLowerCase();
            const status = inj?.status || inj?.type?.description || '';
            const OUT_STATUSES = ['out','ir','il','injured reserve','injured list',
              'day-to-day','doubtful','inactive','suspended'];
            if (name && OUT_STATUSES.some(s => status.toLowerCase().includes(s))) {
              outPlayers[name] = { status, source: 'ESPN' };
            }
          }
        }
      }
    } catch(e) {}

    // SOURCE 2: MLB Transactions API (IL placements in last 7 days)
    if (sp === 'mlb') {
      try {
        const today = new Date();
        const weekAgo = new Date(today - 7*24*60*60*1000);
        const fmt = d => d.toISOString().slice(0,10).replace(/-/g,'');
        const r = await fetch(
          `https://statsapi.mlb.com/api/v1/transactions?sportId=1&startDate=${fmt(weekAgo)}&endDate=${fmt(today)}&limit=200`,
          { signal: AbortSignal.timeout(5000) }
        );
        if (r.ok) {
          const data = await r.json();
          for (const tx of (data?.transactions || [])) {
            const desc = (tx?.description || '').toLowerCase();
            const name = (tx?.player?.fullName || '').toLowerCase();
            // IL placements, activated = no longer out
            if (name && (desc.includes('placed') && (desc.includes('il') || desc.includes('injured list')))) {
              if (!outPlayers[name]) {
                outPlayers[name] = { status: 'IL', source: 'MLB Transactions' };
              }
            }
            // Remove from out if activated
            if (name && desc.includes('activated')) {
              delete outPlayers[name];
            }
          }
        }
      } catch(e) {}
    }

    // SOURCE 3: Sleeper API player stats (status field)
    try {
      const sleeperSport = sp === 'mlb' ? 'baseball' : sp === 'nba' ? 'basketball' : 'football';
      const r = await fetch(
        `https://api.sleeper.app/v1/players/${sleeperSport}`,
        { signal: AbortSignal.timeout(6000) }
      );
      if (r.ok) {
        const data = await r.json();
        for (const [id, player] of Object.entries(data)) {
          const status = (player?.injury_status || player?.status || '').toLowerCase();
          const name = (player?.full_name || player?.name || '').toLowerCase();
          if (name && ['out','ir','pup','nfi','susp'].some(s => status.includes(s))) {
            if (!outPlayers[name]) {
              outPlayers[name] = { status: player.injury_status || status, source: 'Sleeper' };
            }
          }
        }
      }
    } catch(e) {}

    return res.json({
      success: true,
      outPlayers,
      count: Object.keys(outPlayers).length,
      sources: ['ESPN', 'MLB Transactions', 'Sleeper']
    });
  }

  // ── MLB ACTIVE ROSTERS ACTION ──────────────────────────────────────
  if (action === 'mlb_rosters') {
    const teams = (req.query.teams || '').split(',').filter(Boolean);
    const MLB_TEAM_IDS = {
      NYY:147,BOS:111,MIA:146,TEX:140,CIN:113,PHI:143,COL:115,SEA:136,LAA:108,
      MIN:142,ATL:144,ARI:109,CHC:112,CLE:114,NYM:121,SF:137,TB:139,HOU:117,
      STL:138,MIL:158,SD:135,DET:116,BAL:110,PIT:134,OAK:133,WSH:120,KC:118,
      TOR:141,LAD:119,CWS:145
    };
    const season = new Date().getFullYear();
    const activeNames = [];
    await Promise.all(teams.map(async abbr => {
      const id = MLB_TEAM_IDS[abbr]; if (!id) return;
      try {
        const r = await fetch(`https://statsapi.mlb.com/api/v1/teams/${id}/roster/active?season=${season}`,
          { signal: AbortSignal.timeout(4000) });
        if (!r.ok) return;
        const data = await r.json();
        for (const p of (data.roster || [])) {
          const name = (p.person?.fullName || '').toLowerCase();
          if (name) activeNames.push(name);
        }
      } catch(e) {}
    }));
    return res.json({ success: true, activeNames, teamCount: teams.length });
  }

  if (!dgId) return res.status(400).json({ error: 'No ID provided' });

  let draftGroupId = dgId;
  try {
    // Try contest API first (for contest URLs like /draft/contest/12345)
    const r = await fetchScraper(`https://api.draftkings.com/contests/v1/contests/${dgId}?format=json`);
    if (r.ok) {
      const data = await r.json();
      const resolved = data?.contest?.draftGroupId || data?.data?.contest?.draftGroupId;
      if (resolved) { draftGroupId = String(resolved); }
    } else {
      // Fall back to lineups API (for entry URLs like /draft/entry/12345)
      const r2 = await fetchScraper(`https://api.draftkings.com/lineups/v1/lineups/${dgId}?include_draft_group=true`);
      if (r2.ok) {
        const data2 = await r2.json();
        const dg = data2?.draftGroup?.draftGroupId || data2?.lineup?.draftGroupId
          || data2?.payload?.draftGroup?.draftGroupId;
        if (dg) { draftGroupId = String(dg); }
      }
    }
  } catch(e) {}

  const sp = sport || 'nba';

  // ── FETCH ALL DATA IN PARALLEL ────────────────────────────────────
  const [oddsRes, sleeperRes, ownershipRes, recentFormRes, propsRes] = await Promise.allSettled([
    fetchVegasOdds(sp),
    fetchSleeperProjections(sp),
    fetchAllOwnership(sp),
    fetchRecentFormData(sp),
    fetchPlayerProps(sp),
  ]);

  const odds        = oddsRes.status       === 'fulfilled' ? oddsRes.value       : {};
  const projections = sleeperRes.status    === 'fulfilled' ? sleeperRes.value    : {};
  const ownership   = ownershipRes.status  === 'fulfilled' ? ownershipRes.value  : {};
  const recentForm  = recentFormRes.status === 'fulfilled' ? recentFormRes.value : {};
  const playerProps = propsRes.status      === 'fulfilled' ? propsRes.value      : {};

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

        // Player props from Odds API — sharper than game total projections
        const propData = playerProps[name.toLowerCase()] ||
          playerProps[Object.keys(playerProps).find(k =>
            k.includes(name.toLowerCase().split(' ').pop()) ||
            name.toLowerCase().includes(k.split(' ').pop())
          )] || null;

        // Real recent form from ESPN game logs
        const nameLower = name.toLowerCase();
        const formKey = Object.keys(recentForm).find(k =>
          k === nameLower ||
          k.includes(nameLower.split(' ').pop()) ||
          nameLower.includes(k.split(' ').pop())
        );
        const recentFormData = formKey ? recentForm[formKey] : null;

        // Injury status
        const status = p.status || p.playerGameAttribute?.injuryStatus || '';
        const isOut  = ['out','ir','o','injured reserve'].includes(status.toLowerCase());

        return {
          ...p,
          vegasImplied,
          gameTotal,
          realProjection: sleeperProj,
          fpOwnership,
          recentFormData,
          propData,       // player prop lines from Odds API
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
