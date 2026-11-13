"""Load the saved USGS history files from data/raw/ into Snowflake.

Safe to run more than once: the merge matches on USGS's ID, so a second run adds nothing.

Usage: python scripts/load_history.py
Needs SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER and SNOWFLAKE_PRIVATE_KEY_PATH (or SNOWFLAKE_PRIVATE_KEY).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.common import connect, log_run, merge_quakes, usgs_csv_to_frame  # noqa: E402

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def main() -> None:
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        sys.exit("No files in data/raw/. Run scripts/download_history.py first.")
    frame = pd.concat([usgs_csv_to_frame(str(f), "seed") for f in files], ignore_index=True)
    unique_ids = frame["ID"].nunique()
    print(f"Read {len(frame)} rows ({unique_ids} unique quakes) from {len(files)} files")

    conn = connect()
    try:
        inserted, updated = merge_quakes(conn, frame)
        log_run(conn, len(frame), inserted, updated, "seed", f"history load from {len(files)} files")
        total, distinct = conn.cursor().execute(
            "SELECT COUNT(*), COUNT(DISTINCT ID) FROM EARTHQUAKES"
        ).fetchone()
    finally:
        conn.close()
    print(f"Inserted {inserted}, updated {updated}")
    print(f"Table now has {total} rows, {distinct} distinct IDs")


if __name__ == "__main__":
    main()
