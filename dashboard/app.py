"""
Streamlit dashboard — live view of the Kafka → Spark → Delta Lake pipeline.
Reads directly from Delta Lake Parquet files (no Spark session needed).
Auto-refreshes every 5 seconds.
"""
import time
import glob
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone

DELTA_PATH = os.getenv("DELTA_OUTPUT_PATH", "C:/tmp/delta/user-events")
REFRESH_INTERVAL = 5  # seconds

st.set_page_config(
    page_title="Kafka · Spark · Delta — Live Pipeline",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ Kafka · Spark · Delta Lake — Live Pipeline")
st.caption(f"Reading from `{DELTA_PATH}` · Auto-refreshes every {REFRESH_INTERVAL}s")


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_delta() -> pd.DataFrame:
    """Read all Parquet files from the Delta table directory."""
    parquet_files = glob.glob(f"{DELTA_PATH}/*.parquet")
    if not parquet_files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    return df.sort_values("event_ts", ascending=False)


df = load_delta()

if df.empty:
    st.warning("No data yet — make sure the producer and Spark streaming job are running.")
    st.stop()

# ── KPI row ────────────────────────────────────────────────────────────────
total        = len(df)
unique_users = df["user_id"].nunique()
total_revenue = df.loc[df["event_type"] == "purchase", "amount"].sum()
last_event   = df["ingested_at"].max()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Events",    f"{total:,}")
k2.metric("Unique Users",    f"{unique_users:,}")
k3.metric("Purchase Revenue", f"${total_revenue:,.2f}")
k4.metric("Last Ingested",   last_event.strftime("%H:%M:%S"))

st.divider()

# ── Charts row ─────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.subheader("Events by Type")
    counts = df["event_type"].value_counts().reset_index()
    counts.columns = ["event_type", "count"]
    fig = px.pie(counts, names="event_type", values="count",
                 color_discrete_sequence=px.colors.qualitative.Bold,
                 hole=0.4)
    fig.update_layout(margin=dict(t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Events Over Time (1-min buckets)")
    df["minute"] = df["event_ts"].dt.floor("1min")
    timeline = df.groupby(["minute", "event_type"]).size().reset_index(name="count")
    fig2 = px.bar(timeline, x="minute", y="count", color="event_type",
                  color_discrete_sequence=px.colors.qualitative.Bold,
                  barmode="stack")
    fig2.update_layout(margin=dict(t=0, b=0), xaxis_title="", yaxis_title="Events")
    st.plotly_chart(fig2, use_container_width=True)

# ── Revenue chart ───────────────────────────────────────────────────────────
st.subheader("Purchase Amount Distribution")
purchases = df[df["event_type"] == "purchase"]
if not purchases.empty:
    fig3 = px.histogram(purchases, x="amount", nbins=30,
                        color_discrete_sequence=["#636EFA"],
                        labels={"amount": "Amount ($)"})
    fig3.update_layout(margin=dict(t=0, b=0))
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("No purchases yet.")

# ── Live events table ───────────────────────────────────────────────────────
st.subheader("Latest Events")
display_cols = ["event_id", "user_id", "event_type", "product_id", "amount", "event_ts", "ingested_at"]
st.dataframe(
    df[display_cols].head(50),
    use_container_width=True,
    column_config={
        "amount":      st.column_config.NumberColumn("Amount", format="$%.2f"),
        "event_ts":    st.column_config.DatetimeColumn("Event Time",    format="HH:mm:ss"),
        "ingested_at": st.column_config.DatetimeColumn("Ingested At",   format="HH:mm:ss"),
    }
)

# ── Auto-refresh ────────────────────────────────────────────────────────────
time.sleep(REFRESH_INTERVAL)
st.rerun()
