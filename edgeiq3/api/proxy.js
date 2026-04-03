export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { dgId, sport } = req.query;
  if (!dgId) return res.status(400).json({ error: 'No ID provided' });

  const scraperKey = process.env.SCRAPER_API_KEY;
  const oddsKey = process.env.ODDS_API_KEY;

  // Route requests through ScraperAPI to bypass DraftKings IP blocking
  async function fetchUrl(targetUrl) {
    if (scraperKey) {
      const proxyUrl = `http://api.scraperapi.com?api_key=${scraperKey}&url=${encodeURIComponent(targetUrl)}`;
      return fetch(proxyUrl, { signal: AbortSignal.timeout(20000) });
    }
    return fetch(targetUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.draftkings.com/',
      },
      signal: AbortSignal.timeout(10000),
    });
  }

  // Fetch real Vegas odds for every game on the slate
  async function fetchVegasOdds(sportKey) {
    if (!oddsKey) return {};
    try {
      const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/odds/?apiKey=${oddsKey}&regions=us&markets=totals,h2h&oddsFormat=american`;
      const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (!r.ok) return {};
      const games = await r.json();
      // Build a map of team -> implied total
      const teamTotals = {};
      for (const game of games) {
        const total = game.bookmakers?.[0]?.markets?.find(m => m.key === 'totals')?.outcomes?.[0]?.point;
        const homeTeam = game.home_team;
        const awayTeam = game.away_team;
        if (total) {
          // Each team gets half the total as their implied score
          teamTotals[homeTeam] = (total / 2);
          teamTotals[awayTeam] = (total / 2);
          // Store full game total too
          teamTotals[`${homeTeam}_total`] = total;
          teamTotals[`${awayTeam}_total`] = total;
        }
      }
      return teamTotals;
    } catch (e) {
      return {};
    }
  }

  // Map DraftKings sport to Odds API sport key
  const sportKeys = {
    nfl: 'americanfootball_nfl',
    nba: 'basketball_nba',
    mlb: 'baseball_mlb',
    nhl: 'icehockey_nhl',
  };
  const sportKey = sportKeys[sport] || sportKeys.nba;

  // Step 1: Resolve contest ID to draft group ID
  let draftGroupId = dgId;
  try {
    const r = await fetchUrl(`https://api.draftkings.com/contests/v1/contests/${dgId}?format=json`);
    if (r.ok) {
      const data = await r.json();
      const resolved =
        data?.contest?.draftGroupId ||
        data?.data?.contest?.draftGroupId ||
        data?.draftGroupId;
      if (resolved) draftGroupId = String(resolved);
    }
  } catch (e) {}

  // Step 2: Fetch players and Vegas odds in parallel
  const [vegasOdds] = await Promise.allSettled([
    fetchVegasOdds(sportKey),
  ]);
  const odds = vegasOdds.status === 'fulfilled' ? vegasOdds.value : {};

  // Step 3: Fetch DraftKings players
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
      if (players?.length) {
        // Attach Vegas data to each player
        const enrichedPlayers = players.map(p => {
          const team = p.teamAbbreviation || p.teamAbbrev || p.ta || p.team || '';
          // Find matching team in odds (fuzzy match)
          const teamOdds = Object.entries(odds).find(([key]) =>
            key.toLowerCase().includes(team.toLowerCase()) ||
            team.toLowerCase().includes(key.toLowerCase().split(' ').pop())
          );
          return {
            ...p,
            vegasImplied: teamOdds ? teamOdds[1] : null,
            gameTotal: teamOdds ? odds[`${teamOdds[0]}_total`] : null,
          };
        });

        return res.status(200).json({
          success: true,
          players: enrichedPlayers,
          contestId: dgId,
          draftGroupId,
          oddsAvailable: Object.keys(odds).length > 0,
          gamesWithOdds: Object.keys(odds).filter(k => !k.includes('_total')).length / 2,
        });
      }
    } catch (e) { continue; }
  }

  return res.status(502).json({
    error: scraperKey
      ? 'Could not load players. Contest may not be open yet — try again closer to game time.'
      : 'SCRAPER_API_KEY missing from Vercel environment variables.',
    hasKey: !!scraperKey,
    hasOddsKey: !!oddsKey,
  });
}
