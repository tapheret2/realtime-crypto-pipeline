"""Streamlit dashboard for the realtime-crypto-pipeline.

Pulls aggregates straight from PostgreSQL — no caching layer, no API. Refreshes
on a fixed cadence configured by ``DASHBOARD_REFRESH_SECONDS``.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10"))

st.set_page_config(
    page_title="realtime-crypto-pipeline",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)


@st.cache_resource
def _connection_factory():
    def _connect():
        return psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ.get("POSTGRES_DB", "crypto"),
            user=os.environ.get("POSTGRES_USER", "crypto"),
            password=os.environ.get("POSTGRES_PASSWORD", "crypto"),
        )

    return _connect


@st.cache_data(ttl=REFRESH_SECONDS)
def query_df(sql: str, params: tuple | None = None) -> pd.DataFrame:
    connect = _connection_factory()
    with connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def _kpi_row():
    metrics = query_df(
        """
        SELECT
            (SELECT COUNT(*) FROM fact_price_tick) AS total_ticks,
            (SELECT COUNT(DISTINCT asset_id) FROM dim_asset) AS assets,
            (SELECT MAX(event_time) FROM fact_price_tick) AS latest_tick,
            (SELECT COUNT(*) FROM agg_price_minute
                WHERE window_start >= NOW() - INTERVAL '1 hour') AS minute_windows_last_hr,
            (SELECT COUNT(*) FROM data_quality_results
                WHERE checked_at >= NOW() - INTERVAL '1 day' AND status = 'FAIL')
                AS dq_failures_last_24h
        """
    ).iloc[0]

    latest = metrics["latest_tick"]
    if pd.notna(latest):
        age = datetime.now(tz=timezone.utc) - latest.to_pydatetime().astimezone(timezone.utc)
        latest_label = f"{int(age.total_seconds())}s ago"
    else:
        latest_label = "no data yet"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total ticks", f"{int(metrics['total_ticks']):,}")
    c2.metric("Assets tracked", int(metrics["assets"]))
    c3.metric("Latest event", latest_label)
    c4.metric("Minute aggs (last 1h)", int(metrics["minute_windows_last_hr"]))
    c5.metric(
        "DQ failures (24h)",
        int(metrics["dq_failures_last_24h"]),
        delta=None,
        delta_color="inverse",
    )


def _live_chart(symbol: str, hours: int):
    since = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    df = query_df(
        """
        SELECT window_start, close_usd, high_usd, low_usd, open_usd, tick_count
        FROM agg_price_minute
        WHERE symbol = %s AND window_start >= %s
        ORDER BY window_start
        """,
        (symbol, since),
    )
    if df.empty:
        st.info(f"No data yet for {symbol} in the last {hours}h. Give the producer a moment.")
        return
    fig = px.line(
        df,
        x="window_start",
        y="close_usd",
        title=f"{symbol} — close price (1-minute bars, last {hours}h)",
        labels={"window_start": "Time (UTC)", "close_usd": "Close (USD)"},
    )
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)


def _top_movers():
    df = query_df(
        """
        SELECT symbol, trade_date, open_usd, close_usd, price_change_pct, tick_count
        FROM agg_price_daily
        WHERE trade_date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY trade_date DESC, price_change_pct DESC NULLS LAST
        """
    )
    if df.empty:
        st.info("Daily aggregates haven't been generated yet — wait for the daily DAG.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


def _dq_panel():
    df = query_df(
        """
        SELECT checked_at, check_name, check_target, status, metric_value, threshold, details
        FROM data_quality_results
        ORDER BY checked_at DESC
        LIMIT 50
        """
    )
    if df.empty:
        st.info("No data quality results yet.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


def main() -> None:
    st.title(":chart_with_upwards_trend: realtime-crypto-pipeline")
    st.caption(
        "Live view of CoinGecko data flowing through Kafka → Spark → Postgres. "
        f"Auto-refreshes every {REFRESH_SECONDS}s."
    )

    with st.sidebar:
        st.header("Filters")
        symbols_df = query_df("SELECT symbol FROM dim_asset ORDER BY market_cap_rank")
        symbols = symbols_df["symbol"].tolist()
        selected_symbol = st.selectbox(
            "Symbol",
            options=symbols or ["BTC"],
            index=0 if symbols else 0,
        )
        hours = st.slider("Lookback (hours)", min_value=1, max_value=24, value=6)
        if st.button("Refresh now", use_container_width=True):
            query_df.clear()  # type: ignore[attr-defined]

    _kpi_row()
    st.divider()

    tab_live, tab_movers, tab_dq = st.tabs(["Live price", "Daily movers", "Data quality"])
    with tab_live:
        _live_chart(selected_symbol, hours)
    with tab_movers:
        _top_movers()
    with tab_dq:
        _dq_panel()

    # Soft auto-refresh: re-runs the script every REFRESH_SECONDS.
    st.markdown(
        f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
