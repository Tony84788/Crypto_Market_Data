import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
from google.cloud import bigquery


# =========================================================
# Configuration
# =========================================================

PROJECT_ID = "crypto-market-data-platform"
DATASET_ID = "crypto_market"
TABLE_ID = "mart_crypto_latest"


# =========================================================
# Page configuration
# =========================================================

st.set_page_config(
    page_title="Crypto Market Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# Custom styling
# =========================================================

st.markdown(
    """
    <style>

    /* Main application spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        padding-left: 3rem;
        padding-right: 3rem;
    }

    /* Main title */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.25rem;
    }

    /* Subtitle */
    .main-subtitle {
        font-size: 1.05rem;
        opacity: 0.75;
        margin-bottom: 1.5rem;
    }

    /* Section headings */
    .section-title {
        font-size: 1.45rem;
        font-weight: 650;
        margin-top: 0.5rem;
        margin-bottom: 0.8rem;
    }

    /* KPI cards */
    div[data-testid="stMetric"] {
        border: 1px solid rgba(128, 128, 128, 0.20);
        border-radius: 12px;
        padding: 1rem;
        background: rgba(128, 128, 128, 0.05);
        min-height: 125px;
    }

    div[data-testid="stMetricLabel"] {
        font-size: 0.9rem;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.45rem;
        font-weight: 650;
    }

    /* Buttons */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
    }

    /* Dataframe */
    div[data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
    }

    /* Divider */
    hr {
        margin-top: 1.5rem;
        margin-bottom: 1.5rem;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        border-right: 1px solid rgba(128, 128, 128, 0.15);
    }

    /* Small information text */
    .info-text {
        font-size: 0.85rem;
        opacity: 0.7;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# Helper functions
# =========================================================

def format_currency(value):
    """
    Format large USD values using T/B/M/K notation.
    """

    if value is None or pd.isna(value):
        return "N/A"

    value = float(value)

    absolute_value = abs(value)

    if absolute_value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"

    if absolute_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"

    if absolute_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"

    if absolute_value >= 1_000:
        return f"${value / 1_000:.2f}K"

    return f"${value:,.2f}"


def format_percentage(value):
    """
    Format a percentage value.
    """

    if value is None or pd.isna(value):
        return "N/A"

    return f"{float(value):.2f}%"


# =========================================================
# BigQuery data loading
# =========================================================

@st.cache_data(ttl=60)
def load_crypto_data():

    try:
        client = bigquery.Client(
            project=PROJECT_ID
        )

        query = f"""
            SELECT
                crypto_id,
                symbol,
                name,
                current_price,
                market_cap,
                market_cap_rank,
                total_volume,
                high_24h,
                low_24h,
                price_change_24h,
                price_change_percentage_24h,
                circulating_supply,
                total_supply,
                max_supply,
                ath,
                atl,
                last_updated
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            ORDER BY market_cap_rank
        """

        return client.query(query).to_dataframe()

    except Exception as e:

        st.error(
            "Unable to load cryptocurrency data from BigQuery."
        )

        st.exception(e)

        return None


@st.cache_data(ttl=60)
def load_price_history():

    try:
        client = bigquery.Client(
            project=PROJECT_ID
        )

        query = f"""
            SELECT
                crypto_id,
                symbol,
                name,
                current_price,
                last_updated
            FROM `{PROJECT_ID}.{DATASET_ID}.fct_crypto_price_history`
            ORDER BY last_updated
        """

        return client.query(query).to_dataframe()

    except Exception as e:

        st.error(
            "Unable to load historical cryptocurrency data."
        )

        st.exception(e)

        return None


# =========================================================
# Sidebar
# =========================================================

with st.sidebar:

    st.markdown("## ₿ Crypto Market")

    st.markdown(
        """
        **Market Data Platform**

        Data pipeline:

        `CoinGecko → Kafka → Data Lake → BigQuery → dbt`

        Dashboard built with Streamlit.
        """
    )

    st.divider()

    st.markdown("### Dashboard Controls")

    if st.button(
        "🔄 Refresh Data",
        width="stretch",
    ):

        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.markdown("### Data Source")

    st.caption(
        "CoinGecko market data"
    )

    st.caption(
        f"BigQuery dataset: `{DATASET_ID}`"
    )

    st.divider()

    st.markdown("### Pipeline")

    st.caption("🟢 CoinGecko")
    st.caption("🟢 Python ingestion")
    st.caption("🟢 Kafka")
    st.caption("🟢 Data Lake")
    st.caption("🟢 BigQuery")
    st.caption("🟢 dbt")
    st.caption("🟢 Streamlit")


# =========================================================
# Header
# =========================================================

st.markdown(
    '<div class="main-title">'
    '₿ Cryptocurrency Market Dashboard'
    '</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="main-subtitle">'
    'Real-time cryptocurrency market analytics powered by '
    'CoinGecko, Kafka, BigQuery, dbt, and Streamlit.'
    '</div>',
    unsafe_allow_html=True,
)


# =========================================================
# Load latest data
# =========================================================

crypto_df = load_crypto_data()


if crypto_df is None:

    st.warning(
        "The dashboard could not retrieve market data. "
        "Please check your BigQuery connection."
    )

    if st.button("🔁 Retry"):

        st.cache_data.clear()
        st.rerun()

    st.stop()


if crypto_df.empty:

    st.info(
        "No cryptocurrency market data is currently available."
    )

    if st.button("🔁 Retry"):

        st.cache_data.clear()
        st.rerun()

    st.stop()


# =========================================================
# KPI calculations
# =========================================================

total_cryptocurrencies = len(
    crypto_df
)


# ---------------------------------------------------------
# Highest market cap
# ---------------------------------------------------------

if "market_cap" in crypto_df.columns:

    valid_market_caps = (
        crypto_df["market_cap"]
        .dropna()
    )

    highest_market_cap = (
        valid_market_caps.max()
        if not valid_market_caps.empty
        else None
    )

else:

    highest_market_cap = None


# ---------------------------------------------------------
# Total trading volume
# ---------------------------------------------------------

if "total_volume" in crypto_df.columns:

    valid_volumes = (
        crypto_df["total_volume"]
        .dropna()
    )

    total_volume = (
        valid_volumes.sum()
        if not valid_volumes.empty
        else None
    )

else:

    total_volume = None


# ---------------------------------------------------------
# Latest timestamp
# ---------------------------------------------------------

latest_update = None

if "last_updated" in crypto_df.columns:

    valid_updates = (
        crypto_df["last_updated"]
        .dropna()
    )

    if not valid_updates.empty:

        latest_update = valid_updates.max()


# =========================================================
# Data freshness
# =========================================================

if latest_update is None:

    freshness_status = "⚪ Unknown"
    age_minutes = None

else:

    latest_update = pd.to_datetime(
        latest_update,
        utc=True,
    ).to_pydatetime()

    now_utc = datetime.now(
        timezone.utc
    )

    age_minutes = (
        now_utc - latest_update
    ).total_seconds() / 60

    if age_minutes <= 10:

        freshness_status = "🟢 Fresh"

    elif age_minutes <= 30:

        freshness_status = "🟡 Aging"

    else:

        freshness_status = "🔴 Stale"


# =========================================================
# Data freshness section
# =========================================================

st.markdown(
    '<div class="section-title">'
    'Data Freshness'
    '</div>',
    unsafe_allow_html=True,
)

freshness_col1, freshness_col2 = st.columns(2)


with freshness_col1:

    st.metric(
        "Status",
        freshness_status,
    )


with freshness_col2:

    if age_minutes is None:

        age_display = "Unknown"

    elif age_minutes < 60:

        age_display = (
            f"{age_minutes:.1f} minutes"
        )

    else:

        age_display = (
            f"{age_minutes / 60:.1f} hours"
        )

    st.metric(
        "Data Age",
        age_display,
    )


if latest_update is None:

    st.caption(
        "Last update: Unknown"
    )

else:

    st.caption(
        "Last update: "
        + latest_update.strftime(
            "%d %b %Y, %H:%M:%S UTC"
        )
    )


st.caption(
    "Freshness thresholds: "
    "🟢 Fresh ≤ 10 min · "
    "🟡 Aging 10–30 min · "
    "🔴 Stale > 30 min"
)


st.divider()


# =========================================================
# Market overview
# =========================================================

st.markdown(
    '<div class="section-title">'
    'Market Overview'
    '</div>',
    unsafe_allow_html=True,
)

col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Cryptocurrencies",
        total_cryptocurrencies,
    )


with col2:

    st.metric(
        "Highest Market Cap",
        format_currency(
            highest_market_cap
        ),
    )


with col3:

    st.metric(
        "Total 24h Volume",
        format_currency(
            total_volume
        ),
    )


with col4:

    if latest_update is None:

        st.metric(
            "Latest Update",
            "Unknown",
        )

    else:

        st.metric(
            "Latest Update",
            latest_update.strftime(
                "%d %b %Y"
            ),
            delta=latest_update.strftime(
                "%H:%M UTC"
            ),
        )


st.divider()


# =========================================================
# 24-hour price performance
# =========================================================

st.markdown(
    '<div class="section-title">'
    '24-Hour Price Performance'
    '</div>',
    unsafe_allow_html=True,
)


if {
    "name",
    "price_change_percentage_24h",
}.issubset(crypto_df.columns):

    price_change_chart = crypto_df[
        [
            "name",
            "price_change_percentage_24h",
        ]
    ].copy()

    price_change_chart = (
        price_change_chart.rename(
            columns={
                "name": "Cryptocurrency",
                "price_change_percentage_24h":
                    "24h Change (%)",
            }
        )
    )

    price_change_chart = (
        price_change_chart.dropna(
            subset=["24h Change (%)"]
        )
    )

    if price_change_chart.empty:

        st.info(
            "24-hour price-change data "
            "is currently unavailable."
        )

    else:

        price_change_chart["Positive"] = (
            price_change_chart[
                "24h Change (%)"
            ].where(
                price_change_chart[
                    "24h Change (%)"
                ] >= 0
            )
        )

        price_change_chart["Negative"] = (
            price_change_chart[
                "24h Change (%)"
            ].where(
                price_change_chart[
                    "24h Change (%)"
                ] < 0
            )
        )

        st.bar_chart(
            price_change_chart,
            x="Cryptocurrency",
            y=[
                "Positive",
                "Negative",
            ],
            width="stretch",
        )

else:

    st.info(
        "24-hour price-change data "
        "is currently unavailable."
    )


st.divider()


# =========================================================
# Market capitalization
# =========================================================

st.markdown(
    '<div class="section-title">'
    'Market Capitalization'
    '</div>',
    unsafe_allow_html=True,
)


if {
    "name",
    "market_cap",
}.issubset(crypto_df.columns):

    market_cap_chart = crypto_df[
        [
            "name",
            "market_cap",
        ]
    ].copy()

    market_cap_chart = (
        market_cap_chart.rename(
            columns={
                "name": "Cryptocurrency",
                "market_cap":
                    "Market Cap (USD)",
            }
        )
    )

    market_cap_chart = (
        market_cap_chart.dropna(
            subset=["Market Cap (USD)"]
        )
    )

    market_cap_chart = market_cap_chart[
        market_cap_chart[
            "Market Cap (USD)"
        ] > 0
    ]

    market_cap_chart = (
        market_cap_chart.sort_values(
            "Market Cap (USD)",
            ascending=False,
        )
    )

    if market_cap_chart.empty:

        st.info(
            "Market-capitalization data "
            "is currently unavailable."
        )

    else:

        fig = px.bar(
            market_cap_chart,
            x="Cryptocurrency",
            y="Market Cap (USD)",
            title=(
                "Cryptocurrency "
                "Market Capitalization"
            ),
            log_y=True,
            text="Market Cap (USD)",
        )

        fig.update_traces(
            texttemplate="$%{text:.2s}",
            textposition="outside",
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Market Cap: "
                "$%{y:,.0f}"
                "<extra></extra>"
            ),
        )

        fig.update_layout(
            xaxis_title="Cryptocurrency",
            yaxis_title="Market Cap (USD)",
            margin=dict(
                l=20,
                r=20,
                t=60,
                b=20,
            ),
        )

        st.plotly_chart(
            fig,
            width="stretch",
        )

else:

    st.info(
        "Market-capitalization data "
        "is currently unavailable."
    )


st.divider()


# =========================================================
# 24-hour trading volume
# =========================================================

st.markdown(
    '<div class="section-title">'
    '24-Hour Trading Volume'
    '</div>',
    unsafe_allow_html=True,
)


if {
    "name",
    "total_volume",
}.issubset(crypto_df.columns):

    volume_chart = crypto_df[
        [
            "name",
            "total_volume",
        ]
    ].copy()

    volume_chart = (
        volume_chart.rename(
            columns={
                "name": "Cryptocurrency",
                "total_volume":
                    "Trading Volume (USD)",
            }
        )
    )

    volume_chart = (
        volume_chart.dropna(
            subset=["Trading Volume (USD)"]
        )
    )

    volume_chart = volume_chart[
        volume_chart[
            "Trading Volume (USD)"
        ] > 0
    ]

    volume_chart = (
        volume_chart.sort_values(
            "Trading Volume (USD)",
            ascending=False,
        )
    )

    if volume_chart.empty:

        st.info(
            "24-hour trading-volume data "
            "is currently unavailable."
        )

    else:

        volume_chart[
            "Trading Volume (Billion USD)"
        ] = (
            volume_chart[
                "Trading Volume (USD)"
            ]
            / 1_000_000_000
        )

        fig_volume = px.bar(
            volume_chart,
            x="Trading Volume (Billion USD)",
            y="Cryptocurrency",
            orientation="h",
            title=(
                "24-Hour Cryptocurrency "
                "Trading Volume"
            ),
            text="Trading Volume (Billion USD)",
        )

        fig_volume.update_traces(
            texttemplate="%{text:.2f}B",
            textposition="outside",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "24h Volume: "
                "$%{x:.2f}B"
                "<extra></extra>"
            ),
        )

        fig_volume.update_layout(
            xaxis_title=(
                "Trading Volume (Billion USD)"
            ),
            yaxis_title="Cryptocurrency",
            margin=dict(
                l=20,
                r=20,
                t=60,
                b=20,
            ),
        )

        st.plotly_chart(
            fig_volume,
            width="stretch",
        )

else:

    st.info(
        "24-hour trading-volume data "
        "is currently unavailable."
    )


st.divider()


# =========================================================
# Historical cryptocurrency price
# =========================================================

st.markdown(
    '<div class="section-title">'
    'Historical Cryptocurrency Price'
    '</div>',
    unsafe_allow_html=True,
)


price_history_df = load_price_history()


if price_history_df is None:

    st.warning(
        "Historical price data could not be loaded."
    )

    if st.button(
        "🔁 Retry Historical Data",
        key="retry_history",
    ):

        st.cache_data.clear()
        st.rerun()

else:

    if price_history_df.empty:

        st.info(
            "No historical cryptocurrency "
            "price data is currently available."
        )

    elif not {
        "name",
        "current_price",
        "last_updated",
    }.issubset(
        price_history_df.columns
    ):

        st.info(
            "Historical price data is "
            "missing required columns."
        )

    else:

        price_history_df[
            "last_updated"
        ] = pd.to_datetime(
            price_history_df[
                "last_updated"
            ],
            errors="coerce",
            utc=True,
        )

        price_history_df = (
            price_history_df.dropna(
                subset=["last_updated"]
            )
        )

        price_history_df = (
            price_history_df.dropna(
                subset=["current_price"]
            )
        )

        price_history_df = (
            price_history_df[
                price_history_df[
                    "current_price"
                ] > 0
            ]
        )

        if price_history_df.empty:

            st.info(
                "No valid historical price "
                "records are available."
            )

        else:

            available_cryptos = sorted(
                price_history_df[
                    "name"
                ]
                .dropna()
                .unique()
            )

            if not available_cryptos:

                st.info(
                    "No cryptocurrency names "
                    "are available."
                )

            else:

                selected_cryptos = (
                    st.multiselect(
                        "Select cryptocurrency",
                        options=available_cryptos,
                        default=available_cryptos,
                    )
                )

                filtered_price_history = (
                    price_history_df[
                        price_history_df[
                            "name"
                        ].isin(
                            selected_cryptos
                        )
                    ]
                )

                if not selected_cryptos:

                    st.warning(
                        "Select at least one "
                        "cryptocurrency."
                    )

                elif filtered_price_history.empty:

                    st.info(
                        "No historical data is "
                        "available for the selected "
                        "cryptocurrency."
                    )

                else:

                    fig_history = px.line(
                        filtered_price_history,
                        x="last_updated",
                        y="current_price",
                        color="name",
                        markers=True,
                        title=(
                            "Cryptocurrency "
                            "Price History"
                        ),
                    )

                    fig_history.update_traces(
                        hovertemplate=(
                            "<b>%{fullData.name}"
                            "</b><br>"
                            "Time: %{x}<br>"
                            "Price: $%{y:,.2f}"
                            "<extra></extra>"
                        )
                    )

                    fig_history.update_layout(
                        xaxis_title="Time",
                        yaxis_title="Price (USD)",
                        legend_title=(
                            "Cryptocurrency"
                        ),
                        hovermode="x unified",
                        margin=dict(
                            l=20,
                            r=20,
                            t=60,
                            b=20,
                        ),
                    )

                    fig_history.update_xaxes(
                        showgrid=True,
                        gridwidth=1,
                        showline=True,
                        linewidth=1,
                        mirror=True,
                    )

                    fig_history.update_yaxes(
                        showgrid=True,
                        gridwidth=1,
                        showline=True,
                        linewidth=1,
                        mirror=True,
                    )

                    st.plotly_chart(
                        fig_history,
                        width="stretch",
                    )


# =========================================================
# Latest cryptocurrency data
# =========================================================

st.divider()

st.markdown(
    '<div class="section-title">'
    'Latest Cryptocurrency Data'
    '</div>',
    unsafe_allow_html=True,
)


# ---------------------------------------------------------
# Create a cleaner display table
# ---------------------------------------------------------

display_df = crypto_df.copy()


rename_columns = {
    "crypto_id": "Crypto ID",
    "symbol": "Symbol",
    "name": "Name",
    "current_price": "Current Price",
    "market_cap": "Market Cap",
    "market_cap_rank": "Rank",
    "total_volume": "24h Volume",
    "high_24h": "24h High",
    "low_24h": "24h Low",
    "price_change_24h": "24h Change",
    "price_change_percentage_24h": "24h Change %",
    "circulating_supply": "Circulating Supply",
    "total_supply": "Total Supply",
    "max_supply": "Max Supply",
    "ath": "All-Time High",
    "atl": "All-Time Low",
    "last_updated": "Last Updated",
}


display_df = display_df.rename(
    columns=rename_columns
)


# ---------------------------------------------------------
# Format timestamps
# ---------------------------------------------------------

if "Last Updated" in display_df.columns:

    display_df["Last Updated"] = (
        pd.to_datetime(
            display_df["Last Updated"],
            errors="coerce",
            utc=True,
        )
        .dt.strftime(
            "%d %b %Y, %H:%M UTC"
        )
    )


# ---------------------------------------------------------
# Format currency columns
# ---------------------------------------------------------

currency_columns = [
    "Current Price",
    "Market Cap",
    "24h Volume",
    "24h High",
    "24h Low",
    "24h Change",
    "All-Time High",
    "All-Time Low",
]


for column in currency_columns:

    if column in display_df.columns:

        display_df[column] = (
            display_df[column]
            .apply(format_currency)
        )


# ---------------------------------------------------------
# Format percentage
# ---------------------------------------------------------

if "24h Change %" in display_df.columns:

    display_df["24h Change %"] = (
        display_df["24h Change %"]
        .apply(format_percentage)
    )


# ---------------------------------------------------------
# Display table
# ---------------------------------------------------------

st.dataframe(
    display_df,
    width="stretch",
    hide_index=True,
)


# =========================================================
# Footer
# =========================================================

st.divider()

st.caption(
    "Crypto Market Data Platform · "
    "CoinGecko → Kafka → Data Lake → BigQuery → dbt → Streamlit"
)

st.caption(
    "Dashboard data is refreshed from BigQuery. "
    "Pipeline freshness depends on the latest Airflow run."
)