"""'Is it alive?' dashboard for the quake pipeline. Runs as a Streamlit in Snowflake app.

Shows: headline numbers, new quakes per run, today vs the 5-year average (live data joined
with the historical seed), the latest quakes, and the run log.
"""

import streamlit as st

try:
    st.set_page_config(page_title="Quake Pipeline", layout="wide")
except Exception:
    pass  # some Snowflake runtimes set this themselves

conn = st.connection("snowflake")
DB = "QUAKES.CORE"


def q(sql: str):
    """Run a query and return a pandas DataFrame (cached for 60 seconds)."""
    return conn.query(sql, ttl=60)


st.title("Quake Pipeline: is it alive?")
if st.button("Refresh"):
    st.cache_data.clear()

totals = q(f"SELECT COUNT(*) AS TOTAL, COUNT_IF(SOURCE = 'live') AS LIVE FROM {DB}.EARTHQUAKES")
last = q(f"""
    SELECT STATUS, DATEDIFF('minute', RUN_AT, SYSDATE()) AS MINUTES_AGO
    FROM {DB}.INGEST_RUNS WHERE STATUS <> 'seed' ORDER BY RUN_AT DESC LIMIT 1
""")

c1, c2, c3 = st.columns(3)
c1.metric("Total quakes", f"{int(totals.TOTAL[0]):,}")
c2.metric("Added by the live job", f"+{int(totals.LIVE[0]):,}")
if last.empty:
    c3.metric("Last run", "none yet")
else:
    icon = "✅" if last.STATUS[0] == "ok" else "❌"
    c3.metric("Last run", f"{int(last.MINUTES_AGO[0])} min ago {icon}")

left, right = st.columns(2)

with left:
    st.subheader("New quakes per run")
    runs = q(f"""
        SELECT RUN_AT, INSERTED AS NEW_QUAKES
        FROM {DB}.INGEST_RUNS WHERE STATUS <> 'seed' ORDER BY RUN_AT
    """)
    st.bar_chart(runs, x="RUN_AT", y="NEW_QUAKES")
    st.caption("Each bar is one run of the 15-minute GitHub Actions job (times in UTC).")

with right:
    st.subheader("Today vs 5-year average, by hour (UTC)")
    by_hour = q(f"""
        WITH hist AS (
            SELECT HOUR(EVENT_TIME) AS H, COUNT(*) AS N
            FROM {DB}.EARTHQUAKES
            WHERE SOURCE = 'seed' AND EVENT_TIME < DATE_TRUNC('day', SYSDATE())
            GROUP BY 1
        ),
        days AS (
            SELECT COUNT(DISTINCT DATE(EVENT_TIME)) AS D
            FROM {DB}.EARTHQUAKES
            WHERE SOURCE = 'seed' AND EVENT_TIME < DATE_TRUNC('day', SYSDATE())
        ),
        today AS (
            SELECT HOUR(EVENT_TIME) AS H, COUNT(*) AS N
            FROM {DB}.EARTHQUAKES
            WHERE EVENT_TIME >= DATE_TRUNC('day', SYSDATE())
            GROUP BY 1
        )
        SELECT hist.H AS HOUR_UTC,
               ROUND(hist.N / days.D, 1) AS "5-year average",
               CASE WHEN hist.H <= HOUR(SYSDATE()) THEN COALESCE(today.N, 0) END AS "Today"
        FROM hist CROSS JOIN days LEFT JOIN today ON today.H = hist.H
        ORDER BY 1
    """)
    st.line_chart(by_hour, x="HOUR_UTC", y=["5-year average", "Today"])
    last24 = q(f"""
        SELECT
          (SELECT COUNT(*) FROM {DB}.EARTHQUAKES
            WHERE EVENT_TIME >= DATEADD('hour', -24, SYSDATE())) AS LAST_24H,
          (SELECT COUNT(*) / COUNT(DISTINCT DATE(EVENT_TIME)) FROM {DB}.EARTHQUAKES
            WHERE SOURCE = 'seed' AND EVENT_TIME < DATE_TRUNC('day', SYSDATE())) AS AVG_PER_DAY
    """)
    st.caption(
        f"Last 24 hours: {int(last24.LAST_24H[0]):,} quakes vs a 5-year average of "
        f"{float(last24.AVG_PER_DAY[0]):,.0f} per day. The newest hour looks low because "
        "USGS keeps adding small quakes for a while after they happen."
    )

left, right = st.columns(2)

with left:
    st.subheader("Latest quakes")
    st.dataframe(q(f"""
        SELECT EVENT_TIME, MAG, PLACE, SOURCE, LOADED_AT
        FROM {DB}.EARTHQUAKES ORDER BY EVENT_TIME DESC LIMIT 15
    """), hide_index=True, width="stretch")

with right:
    st.subheader("Run log")
    st.dataframe(q(f"""
        SELECT RUN_AT, FETCHED, INSERTED, UPDATED, STATUS, MESSAGE
        FROM {DB}.INGEST_RUNS ORDER BY RUN_AT DESC LIMIT 20
    """), hide_index=True, width="stretch")
