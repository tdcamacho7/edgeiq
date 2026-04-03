export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { dgId } = req.query;
  if (!dgId) return res.status(400).json({ error: 'No ID provided' });

  const scraperKey = process.env.SCRAPER_API_KEY;

  async function fetchUrl(targetUrl) {
    if (scraperKey) {
      const proxyUrl = `http://api.scraperapi.com?api_key=${scraperKey}&url=${encodeURIComponent(targetUrl)}`;
      return fetch(proxyUrl, { signal: AbortSignal.timeout(15000) });
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

  // Step 1: Resolve contest ID to draft group ID
  let draftGroupId = dgId;
  try {
    const r = await fetchUrl(`https://api.draftkings.com/contests/v1/contests/${dgId}?format=json`);
    if (r.ok) {
      const data = await r.json();
      const resolved = data?.contest?.draftGroupId || data?.data?.contest?.draftGroupId || data?.draftGroupId;
      if (resolved) draftGroupId = String(resolved);
    }
  } catch (e) {}

  // Step 2: Fetch players
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
        return res.status(200).json({ success: true, players, contestId: dgId, draftGroupId });
      }
    } catch (e) { continue; }
  }

  return res.status(502).json({
    error: scraperKey
      ? 'Could not load players. Contest may not be open yet.'
      : 'SCRAPER_API_KEY missing from Vercel environment variables.',
    hasKey: !!scraperKey,
  });
}
