import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

# App layout

st.set_page_config(page_title="NYC Taxi Trip Dashboard", layout="wide")
st.title("NYC Taxi Trip Dashboard")

# Database connection

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "bigdata")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
SCHEMA  = os.getenv("DWH_SCHEMA", "dwh")

@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

# Schema introspection helpers

@st.cache_data(ttl=60)
def get_columns(table):
    q = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"s": SCHEMA, "t": table})

def pick(col_list, candidates):
    for c in candidates:
        if c in col_list:
            return c
    return None

# Tables and column mapping

FACT = "fact_trip"
DIM_DT = "dim_datetime"
DIM_LOC = "dim_location"
DIM_PAY = "dim_payment_type"
DIM_RATE = "dim_rate_code"

fact_cols = get_columns(FACT)["column_name"].tolist()
dt_cols_df = get_columns(DIM_DT)
dt_cols = dt_cols_df["column_name"].tolist()

DT_KEY = pick(dt_cols, ["datetime_key", "date_time_key", "dt_key"])
DT_TIME = pick(
    dt_cols_df[dt_cols_df["data_type"].str.contains("timestamp", case=False, na=False)]["column_name"].tolist(),
    dt_cols
)
DT_DATE = pick(dt_cols, ["date", "full_date"])

PU_DT_KEY = "pickup_datetime_key"
PAX_COL = "passenger_count"
DIST_COL = "trip_distance"

TOTAL_COL = pick(fact_cols, ["total_amount"])
FARE_COL  = pick(fact_cols, ["fare_amount"])
TIP_COL   = pick(fact_cols, ["tip_amount"])

loc_cols = get_columns(DIM_LOC)["column_name"].tolist()
LOC_KEY = pick(loc_cols, ["location_key"])
LOC_BOROUGH = pick(loc_cols, ["borough"])
LOC_ZONE = pick(loc_cols, ["zone", "zone_name"])

def location_label(alias):
    if LOC_BOROUGH and LOC_ZONE:
        return f"{alias}.{LOC_BOROUGH} || ' - ' || {alias}.{LOC_ZONE}"
    if LOC_ZONE:
        return f"{alias}.{LOC_ZONE}"
    if LOC_BOROUGH:
        return f"{alias}.{LOC_BOROUGH}"
    return f"{alias}.{LOC_KEY}::text"

pay_cols = get_columns(DIM_PAY)["column_name"].tolist()
PAY_KEY = pick(pay_cols, ["payment_type_key"])
PAY_LABEL = pick(pay_cols, ["payment_type_name", "payment_type", "label", "name"])

rate_cols = get_columns(DIM_RATE)["column_name"].tolist()
RATE_KEY = pick(rate_cols, ["rate_code_key"])
RATE_LABEL = pick(rate_cols, ["rate_code_name", "rate_code", "label", "name"])

# Time range filter
@st.cache_data(ttl=60)
def get_time_bounds():
    q = text(f"SELECT MIN({DT_TIME}), MAX({DT_TIME}) FROM {SCHEMA}.{DIM_DT}")
    with engine.connect() as conn:
        return conn.execute(q).fetchone()

min_t, max_t = get_time_bounds()
min_d = min_t.date()
max_d = max_t.date()

with st.sidebar:
    st.header("Filters")
    date_range = st.date_input(
        "Date range",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d
    )

ds = pd.to_datetime(date_range[0])
de = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)

where_sql = f"d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de"
params = {"ds": ds, "de": de}

# KPI metrics

@st.cache_data(ttl=60)
def load_kpi():
    money_parts = []
    if TOTAL_COL:
        money_parts.append(f"SUM(f.{TOTAL_COL}) AS total_revenue")
    if FARE_COL:
        money_parts.append(f"AVG(f.{FARE_COL}) AS avg_fare")
    if TIP_COL:
        money_parts.append(f"AVG(f.{TIP_COL}) AS avg_tip")

    q = text(f"""
        SELECT
            COUNT(*) AS trips,
            AVG(f.{PAX_COL}) AS avg_passengers,
            AVG(f.{DIST_COL}) AS avg_distance
            {"," if money_parts else ""} {", ".join(money_parts)}
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
    """)
    with engine.connect() as conn:
        return conn.execute(q, params).fetchone()

kpi = load_kpi()

cols = st.columns(6)
cols[0].metric("Trips", f"{int(kpi[0]):,}")
cols[1].metric("Avg passengers", f"{kpi[1]:.2f}")
cols[2].metric("Avg distance", f"{kpi[2]:.2f}")

idx = 3
if TOTAL_COL:
    cols[idx].metric("Total revenue", f"{kpi[idx]:,.0f}")
    idx += 1
if FARE_COL:
    cols[idx].metric("Avg fare", f"{kpi[idx]:.2f}")
    idx += 1
if TIP_COL:
    cols[idx].metric("Avg tip", f"{kpi[idx]:.2f}")

st.divider()

# Trend + payment mix

@st.cache_data(ttl=60)
def load_timeseries():
    day_expr = f"d.{DT_DATE}" if DT_DATE else f"DATE(d.{DT_TIME})"
    q = text(f"""
        SELECT
            {day_expr} AS day,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

@st.cache_data(ttl=60)
def load_payment_dist():
    label = PAY_LABEL if PAY_LABEL else PAY_KEY
    q = text(f"""
        SELECT
            p.{label} AS payment_type,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        JOIN {SCHEMA}.{DIM_PAY} p
          ON f.payment_type_key = p.{PAY_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY trips DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

ts = load_timeseries()
pay_df = load_payment_dist()

left, right = st.columns([1.4, 1])

with left:
    st.subheader("Trips trend (daily)")
    fig = px.line(ts, x="day", y="trips", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Payment types with the most trips")
    fig = px.pie(pay_df, names="payment_type", values="trips")
    fig.update_traces(textinfo="percent+value", textposition="inside")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Top tips + rate code distribution

@st.cache_data(ttl=60)
def load_rate_stats():
    label = RATE_LABEL if RATE_LABEL else RATE_KEY
    q = text(f"""
        SELECT
            r.{label} AS rate_code,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        JOIN {SCHEMA}.{DIM_RATE} r
          ON f.rate_code_key = r.{RATE_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY trips DESC
        LIMIT 10
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

@st.cache_data(ttl=60)
def load_top_tips_locations():
    if not TIP_COL:
        return pd.DataFrame(columns=["location", "total_tips"])

    q = text(f"""
        SELECT
            {location_label("l")} AS location,
            SUM(f.{TIP_COL}) AS total_tips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        JOIN {SCHEMA}.{DIM_LOC} l
          ON f.pu_location_key = l.{LOC_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY total_tips DESC
        LIMIT 5
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

rate_df = load_rate_stats()
top_tips = load_top_tips_locations()

a, b = st.columns(2)

with a:
    st.subheader("Top 5 pickup locations by total tips")
    if top_tips.empty:
        st.info("No tip data available for the selected period.")
    else:
        fig = px.bar(top_tips, x="location", y="total_tips", text="total_tips")
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(yaxis_title="Total tips", xaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

with b:
    st.subheader("Trips by rate code")
    fig = px.bar(rate_df, x="rate_code", y="trips", text="trips")
    fig.update_traces(textposition="outside")
    fig.update_layout(yaxis_title="Trips", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Sample rows (sanity check)

@st.cache_data(ttl=60)
def load_preview(n):
    q = text(f"""
        SELECT
            f.trip_key,
            d.{DT_TIME} AS pickup_time,
            f.{PAX_COL} AS passenger_count,
            f.{DIST_COL} AS trip_distance
            {"," if TIP_COL else ""} {f"f.{TIP_COL} AS tip_amount" if TIP_COL else ""}
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        ORDER BY d.{DT_TIME} DESC
        LIMIT :n
    """)
    p = dict(params)
    p["n"] = n
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=p)

st.subheader("Data preview")
st.dataframe(load_preview(200), use_container_width=True)

