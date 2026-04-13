"""
MacroIQ Engine - v2
Scrapes BLS and Fed calendars for accurate economic release data.
No yfinance, no FRED calendar API, no wrong events.
Groq writes the AI context for each event.
"""

import os
import json
import asyncio
import logging
import datetime
import urllib.request
from zoneinfo import ZoneInfo

from groq import AsyncGroq

log = logging.getLogger("MacroEngine")

ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")


# ── Impact scoring ─────────────────────────────────────────────────────────────
EVENT_PROFILES = {
    "FOMC":                      {"hist_vol": 98, "surprise_sens": 95, "contagion": 99, "iv_buildup": 92, "regime_fit": 90},
    "Federal Funds Rate":        {"hist_vol": 98, "surprise_sens": 95, "contagion": 99, "iv_buildup": 92, "regime_fit": 90},
    "FOMC Rate Decision":        {"hist_vol": 98, "surprise_sens": 95, "contagion": 99, "iv_buildup": 92, "regime_fit": 90},
    "Jerome Powell Press Conference": {"hist_vol": 90, "surprise_sens": 92, "contagion": 95, "iv_buildup": 85, "regime_fit": 88},
    "Consumer Price Index":   {"hist_vol": 84, "surprise_sens": 90, "contagion": 86, "iv_buildup": 72, "regime_fit": 88},
    "CPI":                    {"hist_vol": 84, "surprise_sens": 90, "contagion": 86, "iv_buildup": 72, "regime_fit": 88},
    "Core CPI":               {"hist_vol": 84, "surprise_sens": 90, "contagion": 86, "iv_buildup": 72, "regime_fit": 88},
    "PCE":                    {"hist_vol": 78, "surprise_sens": 82, "contagion": 76, "iv_buildup": 65, "regime_fit": 80},
    "Core PCE":               {"hist_vol": 78, "surprise_sens": 82, "contagion": 76, "iv_buildup": 65, "regime_fit": 80},
    "Nonfarm Payrolls":       {"hist_vol": 72, "surprise_sens": 78, "contagion": 75, "iv_buildup": 60, "regime_fit": 70},
    "Employment Situation":   {"hist_vol": 72, "surprise_sens": 78, "contagion": 75, "iv_buildup": 60, "regime_fit": 70},
    "Unemployment Rate":      {"hist_vol": 55, "surprise_sens": 60, "contagion": 65, "iv_buildup": 40, "regime_fit": 62},
    "GDP":                    {"hist_vol": 60, "surprise_sens": 65, "contagion": 68, "iv_buildup": 48, "regime_fit": 60},
    "Gross Domestic Product": {"hist_vol": 60, "surprise_sens": 65, "contagion": 68, "iv_buildup": 48, "regime_fit": 60},
    "Retail Sales":           {"hist_vol": 52, "surprise_sens": 55, "contagion": 50, "iv_buildup": 38, "regime_fit": 55},
    "Producer Price Index":   {"hist_vol": 68, "surprise_sens": 72, "contagion": 65, "iv_buildup": 60, "regime_fit": 70},
    "PPI":                    {"hist_vol": 68, "surprise_sens": 72, "contagion": 65, "iv_buildup": 60, "regime_fit": 70},
    "Consumer Sentiment":     {"hist_vol": 18, "surprise_sens": 22, "contagion": 15, "iv_buildup": 10, "regime_fit": 28},
    "Consumer Confidence":    {"hist_vol": 20, "surprise_sens": 24, "contagion": 18, "iv_buildup": 12, "regime_fit": 30},
    "Durable Goods":          {"hist_vol": 38, "surprise_sens": 40, "contagion": 35, "iv_buildup": 25, "regime_fit": 40},
    "ISM Manufacturing":      {"hist_vol": 45, "surprise_sens": 48, "contagion": 42, "iv_buildup": 32, "regime_fit": 45},
    "ISM Services":           {"hist_vol": 48, "surprise_sens": 50, "contagion": 44, "iv_buildup": 34, "regime_fit": 48},
    "Housing Starts":         {"hist_vol": 30, "surprise_sens": 32, "contagion": 28, "iv_buildup": 18, "regime_fit": 32},
    "Existing Home Sales":    {"hist_vol": 28, "surprise_sens": 30, "contagion": 25, "iv_buildup": 15, "regime_fit": 30},
    "Initial Jobless Claims": {"hist_vol": 45, "surprise_sens": 48, "contagion": 42, "iv_buildup": 35, "regime_fit": 50},
    "Jobless Claims":         {"hist_vol": 45, "surprise_sens": 48, "contagion": 42, "iv_buildup": 35, "regime_fit": 50},
    "Trade Balance":          {"hist_vol": 25, "surprise_sens": 28, "contagion": 22, "iv_buildup": 14, "regime_fit": 28},
    "Factory Orders":         {"hist_vol": 30, "surprise_sens": 32, "contagion": 28, "iv_buildup": 18, "regime_fit": 32},
    "Job Openings":           {"hist_vol": 48, "surprise_sens": 50, "contagion": 45, "iv_buildup": 35, "regime_fit": 52},
    "JOLTS":                  {"hist_vol": 48, "surprise_sens": 50, "contagion": 45, "iv_buildup": 35, "regime_fit": 52},
    "Monthly OPEX":           {"hist_vol": 55, "surprise_sens": 30, "contagion": 60, "iv_buildup": 90, "regime_fit": 70},
    "Quarterly MOPEX":        {"hist_vol": 70, "surprise_sens": 35, "contagion": 75, "iv_buildup": 95, "regime_fit": 80},
}

# FRED release name → trader-friendly display name + default time
# Keys are lowercase substrings of the exact FRED release_name field
FRED_RELEASE_MAP = {
    "consumer price index":                      ("Consumer Price Index (CPI)",          "8:30 AM"),
    "producer price index":                      ("Producer Price Index (PPI)",          "8:30 AM"),
    "employment situation":                      ("Nonfarm Payrolls",                    "8:30 AM"),
    "unemployment insurance weekly claims":      ("Initial Jobless Claims",              "8:30 AM"),
    "job openings and labor turnover":           ("Job Openings (JOLTS)",                "10:00 AM"),
    "advance monthly sales for retail":          ("Retail Sales",                        "8:30 AM"),
    "personal income and outlays":               ("PCE / Personal Income & Spending",    "8:30 AM"),
    "gross domestic product":                    ("GDP",                                 "8:30 AM"),
    "durable goods":                             ("Durable Goods Orders",                "8:30 AM"),
    "manufacturers' shipments":                  ("Durable Goods Orders",                "8:30 AM"),
    "employment cost index":                     ("Employment Cost Index",               "8:30 AM"),
    "u.s. international trade":                  ("Trade Balance",                       "8:30 AM"),
    "new residential construction":              ("Housing Starts",                      "8:30 AM"),
    "existing home sales":                       ("Existing Home Sales",                 "10:00 AM"),
    "university of michigan":                    ("Consumer Sentiment (UMich)",          "10:00 AM"),
    "survey of consumers":                       ("Consumer Sentiment (UMich)",          "10:00 AM"),
    "consumer confidence":                       ("Consumer Confidence",                 "10:00 AM"),
    "productivity and costs":                    ("Productivity & Unit Labor Costs",     "8:30 AM"),
    "g.17 industrial production":                ("Industrial Production",               "9:15 AM"),
    "factory orders":                            ("Factory Orders",                      "10:00 AM"),
    "new residential sales":                     ("New Home Sales",                      "10:00 AM"),
    "construction spending":                     ("Construction Spending",               "10:00 AM"),
    "gdp advance":                               ("GDP (Advance Estimate)",              "8:30 AM"),
    "national income":                           ("GDP",                                 "8:30 AM"),
}


def score_event(event_name: str) -> tuple[int, str]:
    """Returns (composite_score, tier) for an event."""
    name_lower = event_name.lower()
    profile = None
    for key, val in EVENT_PROFILES.items():
        if key.lower() in name_lower:
            profile = val
            break
    if not profile:
        return 20, "LOW"
    composite = round(sum(profile.values()) / len(profile))
    if composite >= 88:
        tier = "EXTREME"
    elif composite >= 65:
        tier = "HIGH"
    elif composite >= 40:
        tier = "MEDIUM"
    else:
        tier = "LOW"
    return composite, tier


# ── FRED release IDs for historical date lookups ───────────────────────────────
# Maps friendly event name keywords → FRED release_id
# Used to fetch the last N release dates for historical ETF performance
FRED_RELEASE_IDS = {
    "consumer price index": 10,
    "producer price index": 46,
    "nonfarm payrolls":     50,
    "employment situation": 50,
    "initial jobless claims": 180,
    "jobless claims":       180,
    "retail sales":         9,
    "pce":                  54,
    "personal income":      54,
    "gdp":                  53,
    "job openings":         251,
    "jolts":                251,
    "ism manufacturing":    None,  # ISM not on FRED
    "ism services":         None,
    "fomc":                 101,
}

def get_fred_release_id(event_name: str) -> int | None:
    """Returns FRED release_id for a given event name, or None if not found."""
    lower = event_name.lower()
    for key, rid in FRED_RELEASE_IDS.items():
        if key in lower:
            return rid
    return None


def scrape_bls_release_dates(report_slug: str, n: int = 3) -> list[str]:
    """
    Scrapes BLS schedule pages to get actual headline announcement dates.
    report_slug examples: 'ppi', 'cpi', 'empsit' (NFP), 'jolts', 'retail'
    These are official BLS announcement dates — not FRED ingestion dates.
    """
    try:
        today_str = datetime.date.today().isoformat()
        url = f"https://www.bls.gov/schedule/news_release/{report_slug}.htm"
        req = urllib.request.Request(url, headers={"User-Agent": "MacroIQ/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8", errors="ignore")

        # BLS schedule pages list dates in format: "Month DD, YYYY"
        # e.g. "January 14, 2026" or "February 27, 2026"
        import re
        pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(20\d{2})'
        matches = re.findall(pattern, content)

        months = {
            "January": 1, "February": 2, "March": 3, "April": 4,
            "May": 5, "June": 6, "July": 7, "August": 8,
            "September": 9, "October": 10, "November": 11, "December": 12
        }

        dates = []
        for month_name, day, year in matches:
            try:
                d = datetime.date(int(year), months[month_name], int(day))
                date_str = d.isoformat()
                if date_str < today_str and date_str not in dates:
                    dates.append(date_str)
            except ValueError:
                continue

        # Sort descending, take n most recent
        dates.sort(reverse=True)
        log.info(f"BLS schedule scrape for {report_slug}: {dates[:n]}")
        return dates[:n]

    except Exception as e:
        log.warning(f"BLS schedule scrape failed for {report_slug}: {e}")
        return []


# Map event names to BLS schedule page slugs
BLS_SCHEDULE_SLUGS = {
    "consumer price index": "cpi",
    "producer price index": "ppi",
    "nonfarm payrolls":     "empsit",
    "employment situation": "empsit",
    "initial jobless claims": "unemploy",
    "jobless claims":       "unemploy",
    "retail sales":         "retail",
    "job openings":         "jolts",
    "jolts":                "jolts",
    "pce":                  None,  # BEA not BLS
    "personal income":      None,
    "gdp":                  None,
}

def get_historical_release_dates(event_name: str, n: int = 3) -> list[str]:
    """
    Returns last N confirmed release dates for a given event.
    Uses FMP economic calendar as primary source — actual BLS announcement dates.
    Falls back to FRED release dates API if FMP fails.
    """
    name_lower = event_name.lower()
    today = datetime.date.today()
    from_date = (today - datetime.timedelta(days=180)).isoformat()
    to_date   = today.isoformat()
    api_key   = os.getenv("FMP_API_KEY", "")

    if api_key:
        try:
            url = (
                f"https://financialmodelingprep.com/stable/economic-calendar"
                f"?from={from_date}&to={to_date}&apikey={api_key}"
            )
            req = urllib.request.Request(url, headers={"User-Agent": "MacroIQ/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            # Match events by name — use keyword matching
            matched = []
            name_keywords = [w for w in name_lower.split() if len(w) > 3]
            for e in data:
                ename = e.get("event", "").lower()
                # Match if any significant keyword from our event name appears in FMP event name
                if any(k in ename for k in name_keywords) or name_lower in ename:
                    d = e.get("date", "")[:10]  # YYYY-MM-DD
                    if d and d < to_date and d not in matched:
                        matched.append(d)

            matched.sort(reverse=True)
            if matched:
                log.info(f"FMP release dates for {event_name}: {matched[:n]}")
                return matched[:n]

        except Exception as e:
            log.warning(f"FMP economic calendar failed for {event_name}: {e}")

    # Fall back to FRED (may return sub-component dates but better than nothing)
    release_id = get_fred_release_id(event_name)
    if not release_id:
        return []
    try:
        today_str = today.isoformat()
        cutoff    = (today - datetime.timedelta(days=185)).isoformat()
        url = (
            f"https://api.stlouisfed.org/fred/release/dates"
            f"?release_id={release_id}"
            f"&api_key={os.getenv('FRED_API_KEY', '')}"
            f"&sort_order=desc"
            f"&realtime_end={today_str}"
            f"&file_type=json"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "MacroIQ/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        all_dates = [r["date"] for r in data.get("release_dates", [])]
        past = sorted([d for d in all_dates if cutoff <= d < today_str], reverse=True)
        seen, filtered = set(), []
        for d in past:
            mk = (datetime.date.fromisoformat(d).year, datetime.date.fromisoformat(d).month)
            if mk not in seen:
                seen.add(mk)
                filtered.append(d)
            if len(filtered) >= n:
                break
        log.info(f"FRED fallback dates for {event_name}: {filtered}")
        return filtered
    except Exception as e:
        log.warning(f"FRED fallback failed for {event_name}: {e}")
        return []



# ── ETF historical performance on release dates ────────────────────────────────
def map_fred_release(release_name: str):
    lower = release_name.lower()
    for key, (friendly, time) in FRED_RELEASE_MAP.items():
        if key in lower:
            return friendly, time
    return None


# ── FRED Calendar ──────────────────────────────────────────────────────────────
def get_fred_calendar(target_date) -> list:
    """
    Pulls scheduled releases from FRED API for a given date.
    Maps to trader-friendly names - filters out anything not in our watchlist.
    """
    api_key  = os.getenv("FRED_API_KEY", "")
    date_str = target_date.isoformat()
    try:
        url = (
            f"https://api.stlouisfed.org/fred/releases/dates"
            f"?api_key={api_key}"
            f"&realtime_start={date_str}&realtime_end={date_str}"
            f"&include_release_dates_with_no_data=true"
            f"&file_type=json"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "MacroIQ/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        releases = data.get("release_dates", [])
        events, seen = [], set()
        for r in releases:
            raw_name = r.get("release_name", "")
            mapped   = map_fred_release(raw_name)
            if mapped is None:
                continue
            friendly, time = mapped

            # Jobless Claims always release on Thursday - skip if not Thursday
            if "jobless claims" in friendly.lower() and target_date.weekday() != 3:
                continue

            if friendly in seen:
                continue
            seen.add(friendly)
            events.append({"name": friendly, "time": time, "prev": "N/A", "est": "N/A"})

        log.info(f"FRED: {len(events)} watchlist events for {target_date} (of {len(releases)} total)")
        return events

    except Exception as e:
        log.warning(f"FRED calendar failed: {e}")
        return []



# ── BEA Calendar Scraper (PCE + GDP) ──────────────────────────────────────────
def scrape_fomc_calendar(target_date: datetime.date) -> list[dict]:
    """
    Checks if target_date is an FOMC meeting day by scraping the Fed calendar.
    The page format is: month header followed by "DD-DD" date range on next line.
    Returns FOMC event if found, empty list otherwise.
    """
    try:
        import re
        url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
        req = urllib.request.Request(url, headers={"User-Agent": "MacroIQ/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8", errors="ignore")

        target_month = target_date.strftime("%B")
        year         = target_date.year

        # The page has month names as headers followed by date ranges like "28-29"
        # Pattern: find the month name then look for date ranges nearby
        # Also handles cross-month ranges like "Apr/May" with "30-1"

        # Find all date range patterns near our target month
        # Look for month followed within ~500 chars by a number range
        month_idx = 0
        search_start = 0
        while True:
            idx = content.find(target_month, search_start)
            if idx == -1:
                break

            # Look for a date range (DD-DD) within the next 200 characters
            nearby = content[idx:idx+200]
            range_match = re.search(r'\b(\d{1,2})-(\d{1,2})\b', nearby)
            if range_match:
                start_day = int(range_match.group(1))
                end_day   = int(range_match.group(2))
                if start_day <= target_date.day <= end_day:
                    is_last_day = target_date.day == end_day
                    if is_last_day:
                        log.info(f"FOMC decision day found for {target_date}")
                        return [
                            {"name": "FOMC Rate Decision",          "time": "2:00 PM", "prev": "N/A", "est": "N/A"},
                            {"name": "Jerome Powell Press Conference", "time": "2:30 PM", "prev": "N/A", "est": "N/A"},
                        ]
                    else:
                        log.info(f"FOMC meeting day 1 found for {target_date}")
                        return [{"name": "FOMC Meeting Day 1", "time": "All Day", "prev": "N/A", "est": "N/A"}]

            search_start = idx + 1

        log.info(f"No FOMC meeting found for {target_date}")
        return []
    except Exception as e:
        log.warning(f"FOMC scraper failed: {e}")
        return []


def scrape_bea_calendar(target_date: datetime.date) -> list[dict]:
    """
    Scrapes BEA release schedule for PCE and GDP on the target date.
    BEA publishes a plain HTML schedule at bea.gov/news/schedule
    """
    try:
        import re, html
        url = "https://www.bea.gov/news/schedule"
        req = urllib.request.Request(url, headers={
            "User-Agent": "MacroIQ/1.0 (economic calendar bot; contact: admin@example.com)"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8", errors="ignore")

        events = []
        # BEA schedule lists dates in format "April 10, 2026" or "Apr. 10"
        target_patterns = [
            target_date.strftime("%B %-d, %Y"),   # April 10, 2026
            target_date.strftime("%B %-d,"),       # April 10,
            target_date.strftime("%b. %-d,"),      # Apr. 10,
        ]

        # Strip to plain text rows
        rows = re.split(r'\n|<tr|<TR', content)
        for row in rows:
            row_text = re.sub(r'<[^>]+>', ' ', row)
            row_text = html.unescape(row_text)
            row_text = ' '.join(row_text.split())

            if not any(p.lower() in row_text.lower() for p in target_patterns):
                continue

            row_lower = row_text.lower()

            # PCE / Personal Income
            if any(k in row_lower for k in ["personal income", "pce", "personal consumption"]):
                events.append({
                    "name": "PCE / Personal Income & Spending",
                    "time": "8:30 AM",
                    "prev": "N/A",
                    "est":  "N/A",
                })

            # GDP
            if "gross domestic product" in row_lower or " gdp" in row_lower:
                # Identify advance/second/third estimate
                label = "GDP (Advance Estimate)"
                if "second" in row_lower:
                    label = "GDP (Second Estimate)"
                elif "third" in row_lower:
                    label = "GDP (Third Estimate)"
                events.append({
                    "name": label,
                    "time": "8:30 AM",
                    "prev": "N/A",
                    "est":  "N/A",
                })

        log.info(f"BEA scraper found {len(events)} events for {target_date}")
        return events

    except Exception as e:
        log.warning(f"BEA scraper failed: {e}")
        return []


# ── OPEX / MOPEX Date Calculator ──────────────────────────────────────────────
def get_opex_event(target_date: datetime.date) -> list[dict]:
    """
    Monthly OPEX  - 3rd Friday of every month
    Quarterly MOPEX - 3rd Friday of Mar, Jun, Sep, Dec (triple witching)
    Pure date math, no API needed.
    """
    year  = target_date.year
    month = target_date.month

    # Find 3rd Friday of the month
    first_day = datetime.date(year, month, 1)
    # weekday() 4 = Friday
    days_to_first_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + datetime.timedelta(days=days_to_first_friday)
    third_friday = first_friday + datetime.timedelta(weeks=2)

    if target_date != third_friday:
        return []

    # MOPEX months: March, June, September, December
    if month in (3, 6, 9, 12):
        return [{
            "name": "Quarterly MOPEX (Triple Witching)",
            "time": "All Day",
            "prev": "N/A",
            "est":  "N/A",
        }]
    else:
        return [{
            "name": "Monthly OPEX",
            "time": "All Day",
            "prev": "N/A",
            "est":  "N/A",
        }]


def get_last_opex_dates(n: int = 3) -> list[str]:
    """Returns the last N monthly OPEX dates (3rd Friday of each month)."""
    today = datetime.date.today()
    dates = []
    year, month = today.year, today.month

    while len(dates) < n:
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        first_day = datetime.date(year, month, 1)
        days_to_first_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + datetime.timedelta(days=days_to_first_friday)
        third_friday = first_friday + datetime.timedelta(weeks=2)
        if third_friday < today:
            dates.append(third_friday.isoformat())

    return dates


# ── ISM Date Calculator ────────────────────────────────────────────────────────
def get_ism_events(target_date: datetime.date) -> list[dict]:
    """
    ISM Manufacturing - first business day of the month at 10:00 AM ET
    ISM Services      - third business day of the month at 10:00 AM ET
    NFP               - first Friday of the month at 8:30 AM ET
    All fully predictable - no scraping needed.
    """
    events = []
    year  = target_date.year
    month = target_date.month

    def nth_business_day(year: int, month: int, n: int) -> datetime.date:
        day   = datetime.date(year, month, 1)
        count = 0
        while True:
            if day.weekday() < 5:
                count += 1
                if count == n:
                    return day
            day += datetime.timedelta(days=1)

    def first_friday(year: int, month: int) -> datetime.date:
        day = datetime.date(year, month, 1)
        while day.weekday() != 4:  # 4 = Friday
            day += datetime.timedelta(days=1)
        return day

    ism_mfg  = nth_business_day(year, month, 1)
    ism_svc  = nth_business_day(year, month, 3)
    nfp_date = first_friday(year, month)

    if target_date == nfp_date:
        events.append({
            "name": "Nonfarm Payrolls",
            "time": "8:30 AM",
            "prev": "N/A",
            "est":  "N/A",
        })
        log.info(f"NFP day: {target_date}")

    if target_date == ism_mfg:
        events.append({
            "name": "ISM Manufacturing PMI",
            "time": "10:00 AM",
            "prev": "N/A",
            "est":  "N/A",
        })
        log.info(f"ISM Manufacturing day: {target_date}")

    if target_date == ism_svc:
        events.append({
            "name": "ISM Services PMI",
            "time": "10:00 AM",
            "prev": "N/A",
            "est":  "N/A",
        })
        log.info(f"ISM Services day: {target_date}")

    return events


# ── Combined Calendar ──────────────────────────────────────────────────────────
def get_economic_calendar_for_date(target: datetime.date) -> list[dict]:
    """Same as get_economic_calendar but accepts a date object directly."""
    if target.weekday() >= 5:
        return []
    fred_events = get_fred_calendar(target)
    fomc_events = scrape_fomc_calendar(target)
    ism_events  = get_ism_events(target)
    opex_events = get_opex_event(target)
    all_events  = fomc_events + opex_events + ism_events + fred_events
    seen, deduped = set(), []
    for e in all_events:
        if e["name"] not in seen:
            seen.add(e["name"])
            deduped.append(e)
    return deduped


def get_economic_calendar(day: str = "today") -> list[dict]:
    """
    Gets economic releases for target day from all four sources:
    - BLS   (CPI, PPI, NFP, Jobless Claims, Retail Sales)
    - Fed   (FOMC)
    - BEA   (PCE, GDP)
    - ISM   (Manufacturing PMI, Services PMI - date math, no scraping)
    """
    today  = datetime.date.today()
    target = today if day == "today" else today + datetime.timedelta(days=1)

    # Skip weekends - no releases
    if target.weekday() >= 5:
        log.info(f"{target} is a weekend - no releases")
        return []

    fred_events = get_fred_calendar(target)
    fomc_events = scrape_fomc_calendar(target)
    ism_events  = get_ism_events(target)
    opex_events = get_opex_event(target)

    all_events = fomc_events + opex_events + ism_events + fred_events

    # Deduplicate by name
    seen    = set()
    deduped = []
    for e in all_events:
        if e["name"] not in seen:
            seen.add(e["name"])
            deduped.append(e)

    log.info(f"Total events for {target}: {len(deduped)} "
             f"(FOMC:{len(fomc_events)} ISM:{len(ism_events)} FRED:{len(fred_events)})")
    return deduped


# ── Groq Prompts ───────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are MacroIQ — a sharp macro analyst texting a group chat of traders, investors, and market followers.

Your voice:
- Talk like a knowledgeable friend who trades, not a financial journalist
- Casual but precise — short sentences, real insight, no filler
- Explain what things MEAN in plain English — your audience includes people who don't know what CPI is
- Never repeat what you said in a previous sentence or section
- Never say "traders await" "markets brace" "all eyes on" or other clichés
- Never give buy/sell advice or price targets
- Never be vague — every sentence should say something specific
- Keep it fresh — each brief should feel written this morning, not recycled

You must respond ONLY with valid JSON. No markdown, no explanation outside the JSON."""


async def fetch_current_macro_context(groq_client, events: list) -> str:
    """
    Uses Groq compound-beta (web search) to fetch current macro context.
    One call per brief — grounds all subsequent prompts in real current data.
    Falls back to empty string silently if unavailable.
    """
    today = datetime.date.today().strftime("%B %d, %Y")
    event_names = ", ".join([e["name"] for e in events])

    search_prompt = (
        f"Today is {today}. Give me current macro context for a trading brief covering: {event_names}. "
        f"In 4-6 bullet points summarize: current Fed funds rate and recent stance, "
        f"recent trend for the most relevant indicator today, "
        f"what markets are pricing for future Fed moves, "
        f"any relevant recent Fed speaker comments. Be specific with numbers."
    )

    try:
        resp = await groq_client.chat.completions.create(
            model="compound-beta",
            messages=[{"role": "user", "content": search_prompt}],
            temperature=0.3,
            max_tokens=400,
        )
        context = resp.choices[0].message.content.strip()
        log.info(f"compound-beta context fetched successfully ({len(context)} chars)")
        return context
    except Exception as e:
        log.warning(f"compound-beta fetch failed (falling back to no context): {e}")
        return ""


def build_event_prompt(event: dict, day: str, macro_context: str = "") -> str:
    today     = datetime.date.today().strftime("%B %d, %Y")
    is_opex   = "opex" in event["name"].lower() or "mopex" in event["name"].lower()
    is_fomc   = "fomc" in event["name"].lower() or "rate decision" in event["name"].lower()
    is_powell = "powell" in event["name"].lower()

    context_block = f"\nCurrent macro context (use this to ground your response):\n{macro_context}\n" if macro_context else ""

    if is_opex:
        return f"""Today is {today}. Write a MacroIQ brief about {event['name']}.
{context_block}
OPEX day — not a data release. Write 3 punchy sentences on:
- What actually happens to price action on OPEX (pinning, gamma hedging, vol crush)
- What specifically to watch heading into close today

Respond with JSON: {{"context": "3 sentences, plain English, specific to today"}}
Keep context under 380 characters."""

    if is_powell:
        return f"""Today is {today}. Write a MacroIQ brief about Jerome Powell's press conference at 2:30 PM ET.
{context_block}
Write 3 punchy sentences explaining:
- What Powell's press conference is and why it moves markets (explain for people who don't know)
- What traders will be listening for specifically today given current Fed stance
- What a hawkish vs dovish tone means in plain English for stocks and bonds today

Rules: No jargon without explanation. Sound like a sharp friend. Never say "all eyes on".

Respond with JSON: {{"context": "3 sentences, plain English, fresh and specific"}}
Keep context under 420 characters."""

    if is_fomc:
        return f"""Today is {today}. Write a MacroIQ brief about the FOMC Rate Decision at 2:00 PM ET.
{context_block}
Write 3 punchy sentences explaining:
- What the FOMC rate decision is and why it's the most important event in markets (plain English)
- What the Fed is expected to do today and what's at stake given current conditions
- What a surprise hike, cut, or hawkish/dovish shift means specifically for markets today

Rules: Explain for someone who doesn't know what the Fed does. Sound like a sharp friend.

Respond with JSON: {{"context": "3 sentences, plain English, grounded in current macro"}}
Keep context under 420 characters."""

    return f"""Today is {today}. Write a MacroIQ morning brief for this economic report.
{context_block}
Report: {event['name']}
Release time: {event['time']} ET
Previous reading: {event.get('prev', 'N/A')}
Consensus estimate: {event.get('est', 'N/A')}

Write 3 punchy sentences:
1. What this report actually measures and why it matters RIGHT NOW — explain it for someone who doesn't know what {event['name']} is
2. What the recent trend has been and how markets have been reacting
3. What a beat vs miss means specifically for stocks, bonds, and the Fed today

Rules:
- Plain English — no unexplained jargon
- Specific to current macro conditions, not generic
- Never say "markets await" or "all eyes on"
- Each sentence must add new information — no repetition

Respond with JSON: {{"context": "3 sentences, plain English, specific and grounded"}}
Keep context under 420 characters."""


def build_market_context_prompt(events: list, macro_context: str = "") -> str:
    today       = datetime.date.today().strftime("%B %d, %Y")
    times       = ", ".join([f"{e['name']} at {e.get('time','TBD')} ET" for e in events])
    context_block = f"\nCurrent macro context (ground your response in this):\n{macro_context}\n" if macro_context else ""

    return f"""Today is {today}. Write a MacroIQ morning market overview.
{context_block}
Events today: {times}

Write exactly 3 sentences:
1. The current macro backdrop — where the Fed stands, what's driving markets right now (be specific)
2. Why today's specific reports matter given that backdrop
3. What a surprise in either direction could mean for markets today — in plain English

Rules:
- Sound like a knowledgeable friend briefing you before the open
- Explain things for someone who follows markets casually — no unexplained jargon
- Be specific — reference actual current conditions, not generic macro language
- Never say "traders await" "all eyes on" "brace for" or similar clichés
- Each sentence must say something different and specific

Respond with JSON: {{"context": "3 sentences, ~300 characters, fresh and grounded"}}"""


# ── Main Engine ────────────────────────────────────────────────────────────────
class MacroEngine:
    def __init__(self):
        self.groq = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

    async def _ask_groq(self, prompt: str) -> dict | None:
        """Single Groq API call with retry."""
        for attempt in range(3):
            try:
                resp = await self.groq.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": prompt},
                    ],
                    temperature=0.7,
                    max_tokens=400,
                    response_format={"type": "json_object"},
                )
                return json.loads(resp.choices[0].message.content)
            except Exception as e:
                log.warning(f"Groq attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2)
        return None

    async def _fetch_etf_performance(self, event_name: str, dates: list[str]) -> dict:
        """
        Fetches real SPY/QQQ/DIA open-to-close % change on specific dates
        using Twelve Data's historical time series API.
        Falls back to empty strings if fetch fails.
        """
        if not dates:
            return {"spy": "", "qqq": "", "dia": "", "reaction_note": ""}

        api_key = os.getenv("TWELVE_DATA_API_KEY", "")
        if not api_key:
            log.warning("TWELVE_DATA_API_KEY not set — skipping ETF fetch")
            return {"spy": "", "qqq": "", "dia": "", "reaction_note": ""}

        tickers = ["SPY", "QQQ", "DIA"]
        # Store results: {ticker: {date: pct_str}}
        results = {t: {} for t in tickers}

        for ticker in tickers:
            for date_str in dates:
                try:
                    # Fetch 5-day window — single-date queries often return 400
                    dt    = datetime.date.fromisoformat(date_str)
                    start = (dt - datetime.timedelta(days=3)).isoformat()
                    end   = (dt + datetime.timedelta(days=1)).isoformat()
                    url = (
                        f"https://api.twelvedata.com/time_series"
                        f"?symbol={ticker}"
                        f"&interval=1day"
                        f"&start_date={start}"
                        f"&end_date={end}"
                        f"&apikey={api_key}"
                    )
                    req = urllib.request.Request(url, headers={"User-Agent": "MacroIQ/1.0"})
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        data = json.loads(resp.read())

                    if data.get("code") == 429:
                        log.warning(f"Twelve Data rate limit — waiting 65s")
                        await asyncio.sleep(65)
                        with urllib.request.urlopen(req, timeout=15) as resp2:
                            data = json.loads(resp2.read())

                    values = data.get("values", [])
                    # Find entry matching target date
                    match = next((v for v in values if v.get("datetime", "").startswith(date_str)), None)
                    if not match and values:
                        match = values[0]  # fallback to closest

                    if match:
                        log.info(f"Twelve Data fields for {ticker} {date_str}: open={match.get('open')} high={match.get('high')} low={match.get('low')} close={match.get('close')}")
                        o = float(match["open"])
                        c = float(match["close"])
                        pct = (c - o) / o * 100
                        sign = "+" if pct >= 0 else ""
                        results[ticker][date_str] = f"{sign}{pct:.1f}%"
                        log.info(f"Twelve Data {ticker} {date_str}: {sign}{pct:.1f}%")
                    else:
                        log.warning(f"Twelve Data no match {ticker} {date_str}: {json.dumps(data)[:150]}")
                        results[ticker][date_str] = "N/A"

                    await asyncio.sleep(8)

                except Exception as e:
                    log.warning(f"Twelve Data fetch failed {ticker} {date_str}: {type(e).__name__}: {e}")
                    results[ticker][date_str] = "N/A"

        # Build display strings in date order
        spy_vals = [results["SPY"].get(d, "N/A") for d in dates]
        qqq_vals = [results["QQQ"].get(d, "N/A") for d in dates]
        dia_vals = [results["DIA"].get(d, "N/A") for d in dates]

        # Only skip if we got no data at all (API key missing etc)
        if not any(results[t] for t in tickers):
            log.warning(f"No Twelve Data results at all for {event_name}")
            return {"spy": "", "qqq": "", "dia": "", "reaction_note": ""}

        # Ask Groq for a reaction note based on the real data
        reaction_note = await self._get_reaction_note(event_name, dates, spy_vals, qqq_vals, dia_vals)

        log.info(f"ETF performance for {event_name}: SPY={spy_vals} QQQ={qqq_vals} DIA={dia_vals}")
        return {
            "spy":           " / ".join(spy_vals),
            "qqq":           " / ".join(qqq_vals),
            "dia":           " / ".join(dia_vals),
            "reaction_note": reaction_note,
        }

    async def _get_reaction_note(self, event_name: str, dates: list[str], spy: list, qqq: list, dia: list) -> str:
        """Asks Groq to write a one-line reaction note based on real ETF data."""
        dates_fmt = [datetime.date.fromisoformat(d).strftime("%b %d") for d in dates]
        prompt = (
            f"Given these real SPY/QQQ/DIA open-to-close moves on the last 3 {event_name} release days:\n"
            f"Dates: {', '.join(dates_fmt)}\n"
            f"SPY: {', '.join(spy)}\n"
            f"QQQ: {', '.join(qqq)}\n"
            f"DIA: {', '.join(dia)}\n"
            f"Write ONE short phrase (under 60 chars) describing the pattern. "
            f"Example: 'Hot prints slam QQQ hardest' or 'Muted moves, market already priced in'. "
            f"Respond with JSON: {{\"note\": \"your phrase here\"}}"
        )
        try:
            resp = await self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=60,
                response_format={"type": "json_object"},
            )
            data = json.loads(resp.choices[0].message.content)
            return data.get("note", "")
        except Exception as e:
            log.warning(f"Reaction note generation failed: {e}")
            return ""

    async def build_weekly_brief(self, next_week: bool = False) -> dict | None:
        """Builds a weekly calendar overview - all significant events for the week."""
        today = datetime.date.today()

        # weekday(): Mon=0, Tue=1, ..., Sat=5, Sun=6
        # Sunday (6) should be treated as "start of next week" not "end of this week"
        weekday = today.weekday()

        if weekday == 6:
            # Sunday - "this week" means the coming Mon-Fri
            days_to_monday = 1
        else:
            days_to_monday = weekday  # 0=Mon already, 1=Tue back 1, etc.

        this_monday = today - datetime.timedelta(days=days_to_monday)
        if weekday == 6:
            this_monday = today + datetime.timedelta(days=1)

        monday = this_monday + datetime.timedelta(days=7) if next_week else this_monday

        week_events = []  # list of (date, event) tuples

        for i in range(5):  # Mon–Fri
            day_date = monday + datetime.timedelta(days=i)

            # Get all events for this day
            releases = get_economic_calendar_for_date(day_date)
            for release in releases:
                score, tier = score_event(release["name"])
                if tier in ("EXTREME", "HIGH", "MEDIUM"):
                    week_events.append({
                        **release,
                        "date":   day_date.strftime("%A, %b %d"),
                        "impact": tier,
                        "score":  score,
                    })

        if not week_events:
            return None

        week_label = "Next Week" if next_week else "This Week"
        monday_str = monday.strftime("%B %d")
        friday_str = (monday + datetime.timedelta(days=4)).strftime("%B %d")

        return {
            "label":      week_label,
            "date_range": f"{monday_str} – {friday_str}",
            "events":     week_events,
        }

    async def build_daily_brief_for_date(self, target: datetime.date) -> dict | None:
        """Builds a full daily brief for a specific date - used for testing."""
        releases = get_economic_calendar_for_date(target)

        significant_events = []
        for release in releases:
            score, tier = score_event(release["name"])
            if tier in ("EXTREME", "HIGH", "MEDIUM"):
                significant_events.append({
                    **release,
                    "impact": tier,
                    "score":  score,
                })

        significant_events.sort(key=lambda x: x["score"], reverse=True)
        significant_events = significant_events[:5]

        if not significant_events:
            return None

        # Fetch current macro context once via compound-beta web search
        macro_context = await fetch_current_macro_context(self.groq, significant_events)

        for event in significant_events:
            is_opex = "opex" in event["name"].lower() or "mopex" in event["name"].lower()
            if is_opex:
                past_dates = get_last_opex_dates(n=3)
            else:
                past_dates = get_historical_release_dates(event["name"], n=3)
            event["verified_dates"] = "  ".join(
                datetime.date.fromisoformat(d).strftime("%b %d") for d in past_dates
            ) if past_dates else ""

            prompt  = build_event_prompt(event, "today", macro_context=macro_context)
            ai_data = await self._ask_groq(prompt)
            if ai_data:
                event["context"] = ai_data.get("context", "Context unavailable.")
            else:
                event["context"] = f"{event['name']} is on the calendar. Watch for market reaction."

            etf = await self._fetch_etf_performance(event["name"], past_dates)
            event["spy"]           = etf.get("spy", "")
            event["qqq"]           = etf.get("qqq", "")
            event["dia"]           = etf.get("dia", "")
            event["reaction_note"] = etf.get("reaction_note", "")

        ctx_data   = await self._ask_groq(build_market_context_prompt(significant_events, macro_context=macro_context))
        market_ctx = ctx_data.get("context", "Key macro events on deck - stay sharp.") if ctx_data else "Key macro events on deck - stay sharp."

        return {
            "date":           target.strftime("%A, %B %d %Y"),
            "market_context": market_ctx,
            "events":         significant_events,
        }

    async def build_daily_brief(self, day: str = "today") -> dict | None:
        """Builds the full daily macro brief."""
        today  = datetime.date.today()
        target = today if day == "today" else today + datetime.timedelta(days=1)

        # Get economic releases from BLS + Fed
        releases = get_economic_calendar(day)

        # Score and filter - only EXTREME, HIGH, MEDIUM
        significant_events = []
        for release in releases:
            score, tier = score_event(release["name"])
            if tier in ("EXTREME", "HIGH", "MEDIUM"):
                significant_events.append({
                    **release,
                    "impact": tier,
                    "score":  score,
                })

        # Sort by impact score, cap at 5
        significant_events.sort(key=lambda x: x["score"], reverse=True)
        significant_events = significant_events[:5]

        if not significant_events:
            log.info(f"No significant events for {day} - quiet day")
            return None

        # Fetch current macro context once via compound-beta web search
        macro_context = await fetch_current_macro_context(self.groq, significant_events)

        # Generate AI context + historical market reaction for each event
        for event in significant_events:
            is_opex = "opex" in event["name"].lower() or "mopex" in event["name"].lower()
            if is_opex:
                past_dates = get_last_opex_dates(n=3)
            else:
                past_dates = get_historical_release_dates(event["name"], n=3)
            event["verified_dates"] = "  ".join(
                datetime.date.fromisoformat(d).strftime("%b %d") for d in past_dates
            ) if past_dates else ""

            prompt  = build_event_prompt(event, day, macro_context=macro_context)
            ai_data = await self._ask_groq(prompt)
            if ai_data:
                event["context"] = ai_data.get("context", "Context unavailable.")
            else:
                event["context"] = f"{event['name']} is on the calendar today. Watch for market reaction."

            etf = await self._fetch_etf_performance(event["name"], past_dates)
            event["spy"]           = etf.get("spy", "")
            event["qqq"]           = etf.get("qqq", "")
            event["dia"]           = etf.get("dia", "")
            event["reaction_note"] = etf.get("reaction_note", "")

        # Generate overall market context
        ctx_data   = await self._ask_groq(build_market_context_prompt(significant_events, macro_context=macro_context))
        market_ctx = ctx_data.get("context", "Key macro events on deck today - stay sharp.") if ctx_data else "Key macro events on deck today - stay sharp."

        return {
            "date":           target.strftime("%A, %B %d %Y"),
            "market_context": market_ctx,
            "events":         significant_events,
        }
