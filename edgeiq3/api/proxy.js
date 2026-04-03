export default async function handler(req, res) {
  // Allow browser to call this
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { dgId } = req.query;
  if (!dgId) return res.status(400).json({ error: 'No draft group ID provided' });

  // Try multiple DraftKings endpoints — same data, different URL formats
  const endpoints = [
    `https://www.draftkings.com/lineup/getavailableplayers?draftGroupId=${dgId}`,
    `https://api.draftkings.com/lineups/v1/getavailableplayers?draftGroupId=${dgId}`,
    `https://api.draftkings.com/draftgroups/v1/draftgroups/${dgId}/draftables?format=json`,
  ];

  for (const url of endpoints) {
    try {
      const response = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': 'https://www.draftkings.com/',
          'Origin': 'https://www.draftkings.com',
        },
        signal: AbortSignal.timeout(8000),
      });

      if (!response.ok) continue;

      const data = await response.json();
      const players = data?.playerList || data?.draftables || data?.data?.draftables || data?.players;

      if (players?.length) {
        return res.status(200).json({ success: true, players, source: url });
      }
    } catch (e) {
      continue;
    }
  }

  return res.status(502).json({
    error: 'DraftKings did not return player data. The contest may not be open yet, or the draft group ID is invalid.',
    dgId,
  });
}
