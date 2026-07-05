"""Real-time lakehouse dashboard — Streamlit.

Two views on the same data:
  - Hot path:  revenue per minute from the Flink output topic (Kafka)
  - Cold path: Gold Iceberg tables queried through Trino

Run:  streamlit run demo/dashboard.py    (or: make dashboard / scripts/demo.sh)

Environment:
  KAFKA_BOOTSTRAP  default localhost:32100 (k3s nodeport; compose: localhost:29092)
  TRINO_HOST/PORT  default localhost:8080  (kubectl port-forward svc/trino)
"""

import json
import os

import pandas as pd
import streamlit as st

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:32100")
REVENUE_TOPIC = os.environ.get("REVENUE_TOPIC", "gold.order-revenue-1m")
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

st.set_page_config(page_title="Real-Time Lakehouse", page_icon="⚡", layout="wide")
st.title("⚡ Real-Time Lakehouse")
st.caption("Hot path: Flink 1-minute windows via Kafka — Cold path: Iceberg Gold tables via Trino")


def read_revenue_windows(max_windows: int = 60) -> pd.DataFrame:
    """Read the most recent Flink window aggregates from the gold topic."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        REVENUE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=4000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    try:
        rows = [msg.value for msg in consumer]
    finally:
        consumer.close()

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"])
    # Flink emits one row per window; keep the latest emission per window and
    # focus the chart on recent activity — a CDC re-snapshot can emit windows
    # far in the past, which would stretch the time axis.
    df = df.drop_duplicates(subset="window_start", keep="last").sort_values("window_start")
    recent_cutoff = df["window_start"].max() - pd.Timedelta(hours=6)
    return df[df["window_start"] >= recent_cutoff].tail(max_windows)


@st.cache_resource
def trino_connection():
    import trino

    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="dashboard", catalog="iceberg")


def query(sql: str) -> pd.DataFrame:
    cur = trino_connection().cursor()
    cur.execute(sql)
    columns = [c[0] for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=columns)


tab_hot, tab_cold = st.tabs(["🔥 Hot path — Flink", "🗄️ Cold path — Iceberg / Trino"])

with tab_hot:

    @st.fragment(run_every="15s")
    def hot_path() -> None:
        try:
            df = read_revenue_windows()
        except Exception as exc:
            st.warning(f"Kafka not reachable at `{KAFKA_BOOTSTRAP}` — {exc}")
            return
        if df.empty:
            st.info(
                "No windows yet. Start the traffic generator "
                "(`python demo/generate_traffic.py`) and wait ~1 minute."
            )
            return

        latest = df.iloc[-1]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Last window revenue", f"{latest['total_revenue']:,.2f} $")
        c2.metric("Orders in window", int(latest["order_count"]))
        c3.metric("Avg order value", f"{latest['avg_order_value']:,.2f} $")
        c4.metric("Windows observed", len(df))

        st.line_chart(df.set_index("window_start")["total_revenue"], height=280)
        st.bar_chart(df.set_index("window_start")["order_count"], height=180)
        st.caption(f"Auto-refresh every 15 s · topic `{REVENUE_TOPIC}`")

    hot_path()

with tab_cold:

    @st.fragment(run_every="60s")
    def cold_path() -> None:
        try:
            daily = query(
                "SELECT report_date, order_count, net_revenue, avg_order_value, new_customers "
                "FROM gold.daily_revenue ORDER BY report_date"
            )
            customers = query(
                "SELECT customer_id, total_orders, total_revenue, avg_order_value, "
                "days_since_last_order, is_churned "
                "FROM gold.customer_metrics ORDER BY total_revenue DESC LIMIT 10"
            )
            churn = query(
                "SELECT CAST(AVG(CASE WHEN is_churned THEN 1e0 ELSE 0e0 END) AS DOUBLE) AS rate "
                "FROM gold.customer_metrics"
            )
        except Exception as exc:
            st.warning(f"Trino not reachable at `{TRINO_HOST}:{TRINO_PORT}` — {exc}")
            return
        if daily.empty:
            st.info("Gold tables are empty — run the batch: `./scripts/run-batch.sh`")
            return

        total_rev = daily["net_revenue"].astype(float).sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total revenue (all time)", f"{total_rev:,.2f} $")
        c2.metric("Total orders", int(daily["order_count"].sum()))
        churn_rate = float(churn["rate"].iloc[0]) if not churn.empty else 0.0
        c3.metric("Churn rate", f"{churn_rate:.0%}")

        left, right = st.columns(2)
        with left:
            st.subheader("Daily revenue")
            st.bar_chart(daily.set_index("report_date")["net_revenue"], height=300)
        with right:
            st.subheader("Top 10 customers by revenue")
            st.dataframe(customers, use_container_width=True, hide_index=True)

        st.caption("Refreshes every 60 s · data lands here via the Spark batch pipeline")

    cold_path()
