export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { dgId } = req.query;
  if (!dgId) return res.status(400).json({ error: 'No ID provided' });

  const headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.draftkings.com/',
    'Origin': 'https://www.draftkings.com',
  };

  // Step 1: Contest ID and draft group ID are different in DraftKings.
  // Resolve the contest ID to a draft group ID first.
  let draftGroupId = dgId;

  try {
    const contestRes = await fetch(
      `https://api.draftkings.com/contests/v1/contests/${dgId}?format=json`,
      { headers, signal: AbortSignal.timeout(5000) }
    );
    if (contestRes.ok) {
      const contestData = await contestRes.json();
      const resolved =
        contestData?.contest?.draftGroupId ||
        contestData?.data?.contest?.draftGroupId ||
        contestData?.draftGroupId;
      if (resolved) draftGroupId = resolved;
    }
  } catch (e) { /* use original ID */ }

  // Step 2: Fetch players using draft group ID
  const endpoints = [
    `https://www.draftkings.com/lineup/getavailableplayers?draftGroupId=${draftGroupId}`,
    `https://api.draftkings.com/lineups/v1/getavailableplayers?draftGroupId=${draftGroupId}`,
    `https://api.draftkings.com/draftgroups/v1/draftgroups/${draftGroupId}/draftables?format=json`,
    `https://www.draftkings.com/lineup/getavailableplayers?draftGroupId=${dgId}`,
  ];

  for (const url of endpoints) {
    try {
      const response = await fetch(url, { headers, signal: AbortSignal.timeout(8000) });
      if (!response.ok) continue;
      const data = await response.json();
      const players = data?.playerList || data?.draftables || data?.data?.draftables || data?.players;
      if (players?.length) {
        return res.status(200).json({ success: true, players, contestId: dgId, draftGroupId });
      }
    } catch (e) { continue; }
  }

  return res.status(502).json({
    error: 'Could not load players. Contest may not be open for lineup building yet — try again closer to game time.',
    contestId: dgId,
    draftGroupId,
  });
}
