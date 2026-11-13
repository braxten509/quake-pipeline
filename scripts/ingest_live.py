"""Live job: pull quakes USGS added or changed since our newest data, and merge them in.

Runs on a schedule in GitHub Actions. Each run:
1. Finds the newest USGS update time already in EARTHQUAKES.
2. Asks USGS for everything updated since then (minus a 1-hour overlap for late reports).
3. Merges with the same no-duplicates rule as the history load.
4. Logs the run in INGEST_RUNS ('ok' or 'error').

Because the window starts at our newest data rather than "the last 15 minutes", a missed
or failed run is filled in automatically by the next one (backfill, not skip).
USGS only searches the last 30 days by default, so an outage longer than that would need
a manual history load to close the gap.

Needs SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER and SNOWFLAKE_PRIVATE_KEY (or SNOWFLAKE_PRIVATE_KEY_PATH).
"""

import sys
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.common import (  # noqa: E402
    HTTP_HEADERS, USGS_PAGE_LIMIT, USGS_QUERY_URL, connect, log_run, merge_quakes, usgs_csv_to_frame,
)

OVERLAP = timedelta(hours=1)


def fetch_updated_since(since: str) -> pd.DataFrame:
    """All quakes USGS updated after `since`, paging past the 20,000-per-request limit."""
    frames, offset = [], 1
    while True:
        resp = None
        for attempt in range(3):
            try:
                resp = requests.get(USGS_QUERY_URL, headers=HTTP_HEADERS, timeout=120, params={
                    "format": "csv", "updatedafter": since, "orderby": "time-asc",
                    "limit": USGS_PAGE_LIMIT, "offset": offset,
                })
                if resp.status_code != 204:
                    resp.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(10 * (attempt + 1))
        if resp.status_code == 204 or not resp.text.strip():
            break
        page = usgs_csv_to_frame(resp.text, "live")
        frames.append(page)
        if len(page) < USGS_PAGE_LIMIT:
            break
        offset += USGS_PAGE_LIMIT
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    conn = connect()
    try:
        newest = conn.cursor().execute("SELECT MAX(UPDATED_AT) FROM EARTHQUAKES").fetchone()[0]
        since = (newest - OVERLAP).strftime("%Y-%m-%dT%H:%M:%S")
        try:
            frame = fetch_updated_since(since)
            inserted, updated = merge_quakes(conn, frame)
        except Exception as err:
            log_run(conn, 0, 0, 0, "error", f"since {since}: {type(err).__name__}: {err}")
            raise  # make the GitHub run fail so it shows a red X
        log_run(conn, len(frame), inserted, updated, "ok", f"updated since {since}")
        print(f"Fetched {len(frame)} quakes updated since {since} UTC: inserted {inserted}, updated {updated}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
