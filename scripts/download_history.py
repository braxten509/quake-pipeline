"""Download earthquake history from USGS once, one month per file, into data/raw/.

Run once. Months already on disk are skipped, so a re-run only fetches what is missing
(the class's "download once, load from your saved copy" rule).

Usage: python scripts/download_history.py [--start 2021-11-01]
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.common import HTTP_HEADERS, USGS_COUNT_URL, USGS_PAGE_LIMIT, USGS_QUERY_URL  # noqa: E402

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PAUSE_SECONDS = 1  # be polite to a free public service


def get(url: str, params: dict) -> str:
    """GET with a few retries; returns the response text."""
    for attempt in range(4):
        try:
            resp = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=120)
            if resp.status_code == 204:  # no events in this window
                return ""
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as err:
            if attempt == 3:
                raise
            wait = 5 * (attempt + 1)
            print(f"  retry in {wait}s after: {err}")
            time.sleep(wait)
    return ""


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S")


def fetch_range(start: datetime, end: datetime) -> list[str]:
    """Return CSV text chunks for [start, end), splitting in half while over the USGS limit."""
    count = int(get(USGS_COUNT_URL, {"starttime": iso(start), "endtime": iso(end)}) or 0)
    time.sleep(PAUSE_SECONDS)
    if count == 0:
        return []
    if count > USGS_PAGE_LIMIT:
        middle = start + (end - start) / 2
        return fetch_range(start, middle) + fetch_range(middle, end)
    text = get(USGS_QUERY_URL, {
        "format": "csv", "starttime": iso(start), "endtime": iso(end), "orderby": "time-asc",
    })
    time.sleep(PAUSE_SECONDS)
    return [text] if text else []


def month_starts(first: datetime, last: datetime):
    t = first.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while t < last:
        yield t
        t = (t + timedelta(days=32)).replace(day=1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2021-11-01", help="first day of history (UTC)")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    first = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    # History stops at the start of the current hour; the hourly job takes over from there.
    seed_end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    for month in month_starts(first, seed_end):
        next_month = (month + timedelta(days=32)).replace(day=1)
        end = min(next_month, seed_end)
        out = RAW_DIR / f"{month:%Y-%m}.csv"
        if out.exists():
            print(f"{out.name}: already downloaded, skipping")
            continue
        chunks = fetch_range(month, end)
        lines = []
        for i, chunk in enumerate(chunks):
            rows = chunk.splitlines()
            lines.extend(rows if i == 0 else rows[1:])  # keep one header line
        tmp = out.with_suffix(".part")
        tmp.write_text("\n".join(lines) + "\n")
        tmp.rename(out)  # only finished months count as downloaded
        print(f"{out.name}: {max(len(lines) - 1, 0)} quakes")

    (RAW_DIR.parent / "SEED_END.txt").write_text(iso(seed_end) + "\n")
    print(f"History ends at {iso(seed_end)} UTC")


if __name__ == "__main__":
    main()
