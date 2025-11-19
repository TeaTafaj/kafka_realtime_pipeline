import time

import pandas as pd
import psycopg2
import streamlit as st

# -------- Streamlit page config --------
st.set_page_config(
    page_title="Real-Time Stock Trades",
    layout="wide",
)


# -------- DB helpers --------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="kafka_db",
        user="kafka_user",
        password="kafka_password",
    )


def load_trades(limit: int = 200) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(
        f"""
        SELECT *
        FROM trades
        ORDER BY timestamp DESC
        LIMIT {limit};
        """,
        conn,
    )
    conn.close()

    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


# -------- Live dashboard --------
st.title("📈 Real-Time Stock Trade Dashboard")

st.caption(
    "Streaming trades from Kafka → PostgreSQL → this dashboard. "
    "Auto-refreshing every 2 seconds."
)

placeholder = st.empty()

REFRESH_SECONDS = 2

while True:
    df = load_trades(limit=200)

    with placeholder.container():
        if df.empty:
            st.info(
                "No trades in the database yet. Start the producer to see live data."
            )
        else:
            # KPI row
            total_trades = len(df)
            avg_price = df["price"].mean()
            total_volume = int(df["volume"].sum())
            top_ticker = (
                df.groupby("ticker")["volume"]
                .sum()
                .sort_values(ascending=False)
                .index[0]
            )

            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Trades (last 200)", total_trades)
            kpi2.metric("Avg Price", f"${avg_price:,.2f}")
            kpi3.metric("Total Volume", f"{total_volume:,}")
            kpi4.metric("Most Active Ticker", top_ticker)

            st.markdown("---")

            # Main layout: table + charts
            left, right = st.columns([2, 3])

            with left:
                st.subheader("🧾 Most Recent Trades")
                st.dataframe(df, use_container_width=True)

            with right:
                # Price trend
                st.subheader("📉 Price Trend (Last 200 Trades)")
                latest = df.sort_values("timestamp").tail(200)
                price_chart_df = latest.pivot(
                    index="timestamp", columns="ticker", values="price"
                )
                st.line_chart(price_chart_df)

                # Volume by ticker
                st.subheader("📊 Volume by Ticker (Last 200 Trades)")
                vol_df = (
                    df.groupby("ticker")["volume"]
                    .sum()
                    .sort_values(ascending=False)
                    .reset_index()
                )
                st.bar_chart(
                    vol_df.set_index("ticker"),
                )

    time.sleep(REFRESH_SECONDS)
