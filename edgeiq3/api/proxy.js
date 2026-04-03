export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { dgId, sport } = req.query;
  if (!dgId) return res.status(400).json({ error: 'No ID provided' });

  const scraperKey = process.env.SCRAPER_API_KEY;
  const oddsKey = process.env.ODDS_API_KEY;

  async function fetchUrl(targetUrl, useProxy = true) {
    if (useProxy && scraperKey) {
      const proxyUrl = `http://api.scraperapi.com?api_key=${scraperKey}&url=${encodeURIComponent(targetUrl)}`;
      return fetch(proxyUrl, { signal: AbortSignal.timeout(20000) });
    }
    return fetch(targetUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.draftkings.com/',
      },
      signal: AbortSignal.timeout(10000),
    });
  }

  // ── SLEEPER PROJECTIONS (free, no key needed) ──────────────────
  async function fetchSleeperProjections(sp) {
    try {
      const sleeperSport = sp === 'nfl' ? 'nfl' : sp === 'nba' ? 'nba' : 'mlb';
      const season = sp === 'nfl' ? '2025' : '2025';
      // Get current week/period
      const now = new Date();
      const weekNum = sp === 'nfl'
        ? Math.ceil((now - new Date('2025-09-04')) / (7 * 24 * 60 * 60 * 1000))
        : Math.ceil((now - new Date('2025-10-22')) / (7 * 24 * 60 * 60 * 1000));

      const week = Math.max(1, Math.min(weekNum, 18));
      const url = `https://api.sleeper.app/v1/projections/${sleeperSport}/${season}/${week}?season_type=regular&position[]=QB&position[]=RB&position[]=WR&position[]=TE&position[]=K&position[]=DEF&position[]=PG&position[]=SG&position[]=SF&position[]=PF&position[]=C&position[]=SP&position[]=RP&position[]=OF&position[]=1B&position[]=2B&position[]=3B&position[]=SS&position[]=C`;

      // Sleeper is CORS-friendly — call direct without proxy
      const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (!r.ok) return {};
      const data = await r.json();

      // Build name -> projection map
      const projMap = {};
      for (const [playerId, proj] of Object.entries(data)) {
        if (!proj?.player?.full_name) continue;
        const name = proj.player.full_name.toLowerCase();
        const pts = proj.stats?.pts_half_ppr || proj.stats?.pts_ppr ||
                    proj.stats?.fpts || proj.stats?.pts || 0;
        if (pts > 0) projMap[name] = { pts, playerId, raw: proj.stats };
      }
      return projMap;
    } catch (e) {
      return {};
    }
  }

  // ── FANTASYPROS OWNERSHIP (via ScraperAPI) ─────────────────────
  async function fetchFantasyProsOwnership(sp) {
    try {
      const sportPath = { nfl: 'nfl', nba: 'nba', mlb: 'mlb' }[sp] || 'nfl';
      const url = `https://www.fantasypros.com/${sportPath}/rankings/ros-all.php`;
      const r = await fetchUrl(url, true);
      if (!r.ok) return {};
      const html = await r.text();
      // Extract ownership data from embedded JSON in page
      const match = html.match(/ecrData\s*=\s*({.+?});/s) ||
                    html.match(/var\s+data\s*=\s*(\[.+?\]);/s);
      if (!match) return {};
      const parsed = JSON.parse(match[1]);
      const ownershipMap = {};
      const players = parsed?.players || parsed || [];
      for (const p of players) {
        if (p.player_name && p.ownership) {
          ownershipMap[p.player_name.toLowerCase()] = parseFloat(p.ownership) || 0;
        }
      }
      return ownershipMap;
    } catch (e) {
      return {};
    }
  }

  // ── VEGAS ODDS ─────────────────────────────────────────────────
  async function fetchVegasOdds(sp) {
    if (!oddsKey) return {};
    try {
      const sportKeys = {
        nfl: 'americanfootball_nfl',
        nba: 'basketball_nba',
        mlb: 'baseball_mlb',
        nhl: 'icehockey_nhl',
      };
      const url = `https://api.the-odds-api.com/v4/sports/${sportKeys[sp] || 'basketball_nba'}/odds/?apiKey=${oddsKey}&regions=us&markets=totals,h2h&oddsFormat=american`;
      const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (!r.ok) return {};
      const games = await r.json();
      const teamTotals = {};
      for (const game of games) {
        const total = game.bookmakers?.[0]?.markets?.find(m => m.key === 'totals')?.outcomes?.[0]?.point;
        if (total) {
          teamTotals[game.home_team] = total / 2;
          teamTotals[game.away_team] = total / 2;
          teamTotals[`${game.home_team}_total`] = total;
          teamTotals[`${game.away_team}_total`] = total;
        }
      }
      return teamTotals;
    } catch (e) {
      return {};
    }
  }

  // ── NAME MATCHING ──────────────────────────────────────────────
  function fuzzyMatch(dkName, dataMap) {
    if (!dkName) return null;
    const clean = n => n.toLowerCase().replace(/[^a-z ]/g, '').trim();
    const dk = clean(dkName);
    // Exact match first
    if (dataMap[dk] !== undefined) return dataMap[dk];
    // Last name match
    const lastName = dk.split(' ').pop();
    const match = Object.keys(dataMap).find(k => k.endsWith(lastName) || k.includes(lastName));
    return match ? dataMap[match] : null;
  }

  // ── RESOLVE CONTEST ID → DRAFT GROUP ID ───────────────────────
  let draftGroupId = dgId;
  try {
    const r = await fetchUrl(`https://api.draftkings.com/contests/v1/contests/${dgId}?format=json`);
    if (r.ok) {
      const data = await r.json();
      const resolved = data?.contest?.draftGroupId || data?.data?.contest?.draftGroupId;
      if (resolved) draftGroupId = String(resolved);
    }
  } catch (e) {}

  // ── FETCH ALL DATA SOURCES IN PARALLEL ────────────────────────
  const sp = sport || 'nba';
  const [oddsResult, sleeperResult, ownershipResult] = await Promise.allSettled([
    fetchVegasOdds(sp),
    fetchSleeperProjections(sp),
    fetchFantasyProsOwnership(sp),
  ]);

  const odds        = oddsResult.status === 'fulfilled'      ? oddsResult.value      : {};
  const projections = sleeperResult.status === 'fulfilled'   ? sleeperResult.value   : {};
  const ownership   = ownershipResult.status === 'fulfilled' ? ownershipResult.value : {};

  // ── FETCH DRAFTKINGS PLAYERS ───────────────────────────────────
  const endpoints = [
    `https://www.draftkings.com/lineup/getavailableplayers?draftGroupId=${draftGroupId}`,
    `https://api.draftkings.com/lineups/v1/getavailableplayers?draftGroupId=${draftGroupId}`,
    `https://api.draftkings.com/draftgroups/v1/draftgroups/${draftGroupId}/draftables?format=json`,
  ];

  for (const url of endpoints) {
    try {
      const r = await fetchUrl(url);
      if (!r.ok) continue;
      const data = await r.json();
      const players = data?.playerList || data?.draftables || data?.data?.draftables || data?.players;
      if (!players?.length) continue;

      // ── ATTACH ALL DATA SOURCES TO EACH PLAYER ────────────────
      const enriched = players.map(p => {
        const name = p.displayName || p.playerName || '';
        const team = p.teamAbbreviation || p.teamAbbrev || p.team || '';

        // Vegas
        const teamOdds = Object.entries(odds).find(([k]) =>
          k.toLowerCase().includes(team.toLowerCase()) ||
          team.toLowerCase().includes(k.toLowerCase().split(' ').pop())
        );
        const vegasImplied = teamOdds ? teamOdds[1] : null;
        const gameTotal = teamOdds ? odds[`${teamOdds[0]}_total`] : null;

        // Sleeper projections (real projected pts, not historical avg)
        const sleeperProj = fuzzyMatch(name, projections);
        const realProjection = sleeperProj?.pts || null;

        // FantasyPros ownership
        const fpOwnership = fuzzyMatch(name, ownership);

        // Player status
        const status = p.status || p.playerGameAttribute?.injuryStatus || '';
        const isOut = ['out','ir','o'].includes(status.toLowerCase());

        return {
          ...p,
          vegasImplied,
          gameTotal,
          realProjection,   // Sleeper projected pts
          fpOwnership,      // FantasyPros ownership %
          status,
          isOut,
        };
      });

      return res.status(200).json({
        success: true,
        players: enriched,
        contestId: dgId,
        draftGroupId,
        dataSources: {
          vegas: Object.keys(odds).length > 0,
          sleeperProjections: Object.keys(projections).length > 0,
          fantasyProsOwnership: Object.keys(ownership).length > 0,
          gamesWithOdds: Math.floor(Object.keys(odds).filter(k => !k.includes('_total')).length / 2),
          playersWithProjections: enriched.filter(p => p.realProjection).length,
          playersWithOwnership: enriched.filter(p => p.fpOwnership).length,
        },
      });
    } catch (e) { continue; }
  }

  return res.status(502).json({
    error: scraperKey
      ? 'Contest not open yet or invalid ID — try again closer to game time.'
      : 'SCRAPER_API_KEY missing.',
    hasKey: !!scraperKey,
    hasOddsKey: !!oddsKey,
  });
}
