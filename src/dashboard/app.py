import os
import duckdb
import pandas as pd
import streamlit as st
from datetime import date, timedelta

DB = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")

@st.cache_data(ttl=120)
def fetch_date_bounds():
    con = duckdb.connect(DB, read_only=True)
    df = con.execute("""
        SELECT
          MIN(CAST(order_date AS DATE)) AS min_d,
          MAX(CAST(order_date AS DATE)) AS max_d
        FROM fct_orders
    """).df()
    con.close()
    if df.empty or pd.isna(df.loc[0, "min_d"]):
        today = date.today()
        return today - timedelta(days=30), today
    return df.loc[0, "min_d"], df.loc[0, "max_d"]

@st.cache_data(ttl=120)
def load_kpis(d1, d2):
    con = duckdb.connect(DB, read_only=True)
    q = """
      WITH base AS (
        SELECT *
        FROM fct_orders
        WHERE CAST(order_date AS DATE) BETWEEN ? AND ?
      )
      SELECT
        COUNT(*)                                   AS orders_cnt,
        COALESCE(SUM(net_total), 0)               AS net_before_refunds,
        COALESCE(SUM(refund_total), 0)            AS refunds,
        COALESCE(SUM(COALESCE(net_after_refunds, net_total)), 0) AS net_after_refunds,
        COALESCE(AVG(net_total), 0)               AS aov
      FROM base;
    """
    k = con.execute(q, [d1, d2]).df().iloc[0].to_dict()
    con.close()
    return k

@st.cache_data(ttl=120)
def load_timeseries(d1, d2):
    con = duckdb.connect(DB, read_only=True)
    ts = con.execute("""
      SELECT
        CAST(order_date AS DATE) AS d,
        SUM(COALESCE(net_after_refunds, net_total)) AS net
      FROM fct_orders
      WHERE CAST(order_date AS DATE) BETWEEN ? AND ?
      GROUP BY 1
      ORDER BY 1
    """, [d1, d2]).df()

    con.close()
    return ts

@st.cache_data(ttl=120)
def load_top_products(d1, d2, limit=15):
    con = duckdb.connect(DB, read_only=True)
    df = con.execute("""
      SELECT
        name,
        SUM(total - COALESCE(refunded_total,0)) AS revenue,
        SUM(quantity - COALESCE(refunded_quantity,0)) AS qty_sold
      FROM fct_order_items i
      JOIN fct_orders o USING(order_id)
      WHERE CAST(o.order_date AS DATE) BETWEEN ? AND ?
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT ?
    """, [d1, d2, limit]).df()
    con.close()
    return df

@st.cache_data(ttl=120)
def load_category_mix(d1, d2, limit=15):
    con = duckdb.connect(DB, read_only=True)
    df = con.execute("""
      SELECT
        COALESCE(NULLIF(TRIM(category_snapshot), ''), 'Uncategorized') AS category,
        SUM(total - COALESCE(refunded_total,0)) AS revenue
      FROM fct_order_items i
      JOIN fct_orders o USING(order_id)
      WHERE CAST(o.order_date AS DATE) BETWEEN ? AND ?
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT ?
    """, [d1, d2, limit]).df()
    con.close()
    return df

@st.cache_data(ttl=120)
def load_geo(d1, d2, limit=20):
    con = duckdb.connect(DB, read_only=True)
    df = con.execute("""
      SELECT
        COALESCE(NULLIF(TRIM(billing_country), ''), '—') AS country,
        COALESCE(NULLIF(TRIM(billing_city), ''), '—')     AS city,
        COUNT(*) AS orders,
        SUM(COALESCE(net_after_refunds, net_total)) AS net
      FROM fct_orders
      WHERE CAST(order_date AS DATE) BETWEEN ? AND ?
      GROUP BY 1,2
      HAVING COUNT(*) > 0
      ORDER BY net DESC
      LIMIT ?
    """, [d1, d2, limit]).df()
    con.close()
    return df

# --- UI ---
st.set_page_config(page_title="Ecommerce KPIs", layout="wide")
st.title("🛒 Ecommerce KPIs")

# Sidebar filters
min_d, max_d = fetch_date_bounds()
with st.sidebar:
    st.subheader("Filters")
    d1, d2 = st.date_input(
        "Date range",
        value=(max(min_d, max_d - timedelta(days=30)), max_d),
        min_value=min_d,
        max_value=max_d
    )
    if isinstance(d1, tuple):
        d1, d2 = d1  # streamlit older versions
    st.caption(f"Data window: {d1} → {d2}")

# KPIs
k = load_kpis(d1, d2)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders", f"{int(k['orders_cnt'])}")
c2.metric("Revenue (net)", f"{k['net_after_refunds']:.2f}")
c3.metric("Refunds", f"{k['refunds']:.2f}")
c4.metric("AOV", f"{k['aov']:.2f}")

st.markdown("---")

# Timeseries
st.subheader("Revenue Over Time")
ts = load_timeseries(d1, d2)
if ts.empty:
    st.info("No data for the selected period.")
else:
    st.line_chart(ts.set_index("d")["net"])

# Two-column insights
left, right = st.columns(2)

with left:
    st.subheader("Top Products")
    top_p = load_top_products(d1, d2)
    st.bar_chart(top_p.set_index("name")["revenue"])
    st.dataframe(
        top_p.rename(columns={"revenue": "Revenue", "qty_sold": "Qty"})
             .style.format({"Revenue": "{:.2f}"})
    )

with right:
    st.subheader("Category Mix")
    mix = load_category_mix(d1, d2)
    st.bar_chart(mix.set_index("category")["revenue"])
    st.dataframe(
        mix.rename(columns={"revenue": "Revenue"})
           .style.format({"Revenue": "{:.2f}"})
    )

st.subheader("Top Locations")
geo = load_geo(d1, d2)
st.dataframe(
    geo.rename(columns={"country": "Country", "city": "City", "orders": "Orders", "net": "Net"})
       .style.format({"Net": "{:.2f}"})
)
