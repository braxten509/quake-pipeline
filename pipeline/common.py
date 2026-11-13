"""Shared pieces for the history load and the hourly job.

- connect(): log in to Snowflake as the key-only robot user.
- usgs_csv_to_frame(): turn a USGS CSV download into rows for our table.
- merge_quakes(): add new quakes and update changed ones, never duplicating.
- log_run(): write one row to INGEST_RUNS so we can prove liveness and spot gaps.
"""

import io
import os

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas

USGS_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_COUNT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/count"
USGS_PAGE_LIMIT = 20000  # most events USGS returns per request
HTTP_HEADERS = {"User-Agent": "cs452-quake-pipeline (student project)"}

# Our column name -> USGS CSV column name
COLUMN_MAP = {
    "ID": "id",
    "EVENT_TIME": "time",
    "UPDATED_AT": "updated",
    "LATITUDE": "latitude",
    "LONGITUDE": "longitude",
    "DEPTH_KM": "depth",
    "MAG": "mag",
    "MAG_TYPE": "magType",
    "PLACE": "place",
    "EVENT_TYPE": "type",
    "STATUS": "status",
    "NET": "net",
}
DATA_COLUMNS = [c for c in COLUMN_MAP if c != "ID"] + ["SOURCE"]


def _private_key_bytes() -> bytes:
    """Read the private key from SNOWFLAKE_PRIVATE_KEY (text) or SNOWFLAKE_PRIVATE_KEY_PATH (file)."""
    pem = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
    if not pem:
        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
            pem = f.read().decode()
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect():
    """Open a Snowflake connection as the pipeline's robot user."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_private_key_bytes(),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH"),
        role="PIPELINE_ROLE",
        database="QUAKES",
        schema="CORE",
    )


def usgs_csv_to_frame(csv_source, source: str) -> pd.DataFrame:
    """Turn USGS CSV text or a CSV file path into rows shaped like our EARTHQUAKES table."""
    if isinstance(csv_source, str) and not os.path.exists(csv_source):
        csv_source = io.StringIO(csv_source)
    raw = pd.read_csv(csv_source, dtype={"id": str, "place": str, "magType": str, "net": str})
    frame = pd.DataFrame({ours: raw[theirs] for ours, theirs in COLUMN_MAP.items()})
    for col in ("EVENT_TIME", "UPDATED_AT"):
        # Store as UTC without a time zone label
        frame[col] = pd.to_datetime(frame[col], utc=True, format="ISO8601").dt.tz_localize(None)
    frame["SOURCE"] = source
    return frame


def merge_quakes(conn, frame: pd.DataFrame) -> tuple[int, int]:
    """Add new quakes and update ones USGS has revised. Returns (inserted, updated).

    Duplicates are blocked by matching on USGS's ID. A row that already exists is only
    changed when USGS's UPDATED_AT is newer, so re-loading the same data changes nothing.
    """
    if frame.empty:
        return 0, 0
    cur = conn.cursor()
    cur.execute("CREATE OR REPLACE TEMPORARY TABLE EQ_STAGE LIKE EARTHQUAKES")
    write_pandas(conn, frame, "EQ_STAGE", use_logical_type=True)
    set_cols = ", ".join(f"{c} = s.{c}" for c in DATA_COLUMNS if c != "SOURCE")
    all_cols = ["ID"] + DATA_COLUMNS
    cur.execute(f"""
        MERGE INTO EARTHQUAKES t
        USING (
            SELECT * FROM EQ_STAGE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATED_AT DESC) = 1
        ) s
        ON t.ID = s.ID
        WHEN MATCHED AND s.UPDATED_AT > t.UPDATED_AT THEN
            UPDATE SET {set_cols}, LOADED_AT = SYSDATE()
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(all_cols)}) VALUES ({", ".join("s." + c for c in all_cols)})
    """)
    inserted, updated = cur.fetchone()
    return int(inserted), int(updated)


def log_run(conn, fetched: int, inserted: int, updated: int, status: str, message: str = "") -> None:
    """Record one pipeline run in INGEST_RUNS."""
    conn.cursor().execute(
        "INSERT INTO INGEST_RUNS (FETCHED, INSERTED, UPDATED, STATUS, MESSAGE) VALUES (%s, %s, %s, %s, %s)",
        (fetched, inserted, updated, status, message[:1000]),
    )
