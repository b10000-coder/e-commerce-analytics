"""
E-Commerce Analytics Dashboard
-------------------------------
Runs on top of the dbt marts layer in ecom.duckdb.
Every chart is a direct SQL query against main_marts — no pandas wrangling.

Usage (from repo root):
    streamlit run dashboard/app.py
"""

import os
import duckdb
import streamlit as st
import plotly.express as px

# ── connection ───────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ecom.duckdb")

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()

def q(sql):
    """Run a SQL query and return a pandas DataFrame."""
    return con.execute(sql).df()


# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Analytics",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("E-Commerce Analytics")
st.caption("dbt · DuckDB · Star schema — Oct 2024 → Mar 2025")

# ── sidebar filters ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    status_filter = st.multiselect(
        "Order status",
        options=["delivered", "shipped", "processing", "pending", "cancelled", "returned"],
        default=["delivered"],
    )
    platform_filter = st.multiselect(
        "Platform",
        options=["ios", "android", "web"],
        default=["ios", "android", "web"],
    )

status_in  = "(" + ",".join(f"'{s}'" for s in status_filter) + ")" if status_filter else "('delivered')"
platform_in = "(" + ",".join(f"'{p}'" for p in platform_filter) + ")" if platform_filter else "('ios','android','web')"


# ── KPI row ──────────────────────────────────────────────────────────────────
kpis = q(f"""
    SELECT
        ROUND(SUM(line_revenue), 0)                         AS gmv,
        COUNT(DISTINCT order_id)                            AS orders,
        ROUND(SUM(line_revenue) / COUNT(DISTINCT order_id), 2) AS aov,
        COUNT(DISTINCT customer_key)                        AS customers,
        ROUND(AVG(CASE WHEN is_flash_sale THEN 1.0 ELSE 0.0 END) * 100, 1) AS flash_pct
    FROM main_marts.fact_orders
    WHERE order_status IN {status_in}
      AND platform IN {platform_in}
""")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("GMV",             f"₺{int(kpis['gmv'][0]):,}")
c2.metric("Orders",          f"{int(kpis['orders'][0]):,}")
c3.metric("AOV",             f"₺{kpis['aov'][0]:,.0f}")
c4.metric("Customers",       f"{int(kpis['customers'][0]):,}")
c5.metric("Flash sale rate", f"{kpis['flash_pct'][0]}%")

st.divider()

# ── row 1: monthly GMV ────────────────────────────────────────────────────────
st.subheader("Monthly GMV")
monthly = q(f"""
    SELECT
        d.year,
        d.month_number,
        d.month_name,
        ROUND(SUM(f.line_revenue), 0)       AS gmv,
        COUNT(DISTINCT f.order_id)          AS orders
    FROM main_marts.fact_orders f
    JOIN main_marts.dim_date d ON f.date_key = d.date_key
    WHERE f.order_status IN {status_in}
      AND f.platform IN {platform_in}
    GROUP BY d.year, d.month_number, d.month_name
    ORDER BY d.year, d.month_number
""")
monthly["label"] = monthly["month_name"] + " " + monthly["year"].astype(str)

fig = px.bar(
    monthly, x="label", y="gmv",
    labels={"label": "", "gmv": "GMV (₺)"},
    color_discrete_sequence=["#1D9E75"],
    text_auto=".3s",
)
fig.update_traces(textposition="outside")
fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", yaxis_gridcolor="rgba(0,0,0,0.06)")
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── row 2: category + order status ───────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by category")
    cat = q(f"""
        SELECT p.category, ROUND(SUM(f.line_revenue), 0) AS revenue
        FROM main_marts.fact_orders f
        JOIN main_marts.dim_product p ON f.product_key = p.product_key
        WHERE f.order_status IN {status_in}
          AND f.platform IN {platform_in}
        GROUP BY p.category
        ORDER BY revenue DESC
    """)
    fig = px.bar(
        cat, x="revenue", y="category", orientation="h",
        labels={"revenue": "Revenue (₺)", "category": ""},
        color_discrete_sequence=["#534AB7"],
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Orders by status")
    status_df = q("""
        SELECT order_status, COUNT(DISTINCT order_id) AS orders
        FROM main_marts.fact_orders
        GROUP BY order_status ORDER BY orders DESC
    """)
    fig = px.pie(
        status_df, values="orders", names="order_status", hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── row 3: platform + customer tier ──────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by platform")
    plat = q(f"""
        SELECT platform, ROUND(SUM(line_revenue), 0) AS revenue
        FROM main_marts.fact_orders
        WHERE order_status IN {status_in}
        GROUP BY platform ORDER BY revenue DESC
    """)
    fig = px.bar(
        plat, x="platform", y="revenue",
        labels={"platform": "", "revenue": "Revenue (₺)"},
        color_discrete_sequence=["#D85A30"],
        text_auto=".3s",
    )
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Customer tier distribution")
    tier = q(f"""
        SELECT c.customer_tier, COUNT(DISTINCT f.customer_key) AS customers
        FROM main_marts.fact_orders f
        JOIN main_marts.dim_customer c ON f.customer_key = c.customer_key
        WHERE f.order_status IN {status_in}
        GROUP BY c.customer_tier
    """)
    fig = px.pie(
        tier, values="customers", names="customer_tier", hole=0.45,
        color_discrete_sequence=["#EF9F27", "#1D9E75", "#7F77DD"],
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── row 4: flash sale vs regular ─────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Flash sale vs regular revenue")
    flash = q(f"""
        SELECT
            CASE WHEN is_flash_sale THEN 'Flash sale' ELSE 'Regular price' END AS type,
            ROUND(SUM(line_revenue), 0) AS revenue,
            COUNT(*) AS items
        FROM main_marts.fact_orders
        WHERE order_status IN {status_in}
        GROUP BY is_flash_sale
    """)
    fig = px.bar(
        flash, x="type", y="revenue", color="type",
        labels={"type": "", "revenue": "Revenue (₺)"},
        color_discrete_sequence=["#D85A30", "#1D9E75"],
        text_auto=".3s",
    )
    fig.update_layout(showlegend=False, plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Gross margin by category")
    margin = q("""
        SELECT p.category,
               ROUND(AVG(p.gross_margin_pct) * 100, 1) AS avg_margin_pct
        FROM main_marts.dim_product p
        WHERE p.is_active = true
        GROUP BY p.category
        ORDER BY avg_margin_pct DESC
    """)
    fig = px.bar(
        margin, x="avg_margin_pct", y="category", orientation="h",
        labels={"avg_margin_pct": "Avg gross margin %", "category": ""},
        color_discrete_sequence=["#0F6E56"],
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── SCD2 spotlight ────────────────────────────────────────────────────────────
st.subheader("SCD Type 2 — customer history")
st.caption("Customers with more than one version (city or tier change during the period)")

scd = q("""
    SELECT
        customer_id,
        full_name,
        version_number  AS version,
        city,
        customer_tier   AS tier,
        valid_from,
        valid_to,
        is_current
    FROM main_marts.dim_customer_scd2
    WHERE customer_id IN (
        SELECT customer_id FROM main_marts.dim_customer_scd2
        GROUP BY customer_id HAVING COUNT(*) > 1
        LIMIT 8
    )
    ORDER BY customer_id, version_number
""")
scd["valid_to"] = scd["valid_to"].fillna("active")
st.dataframe(
    scd,
    use_container_width=True,
    column_config={
        "is_current": st.column_config.CheckboxColumn("current?"),
        "valid_from": "valid from",
        "valid_to":   "valid to",
    },
    hide_index=True,
)

st.divider()

# ── top 10 sellers ────────────────────────────────────────────────────────────
st.subheader("Top 10 sellers by revenue")
sellers = q(f"""
    SELECT
        s.seller_name,
        s.category,
        ROUND(SUM(f.line_revenue), 0)   AS revenue,
        COUNT(DISTINCT f.order_id)      AS orders,
        ROUND(AVG(s.rating), 1)         AS rating
    FROM main_marts.fact_orders f
    JOIN main_marts.dim_seller s ON f.seller_key = s.seller_key
    WHERE f.order_status IN {status_in}
    GROUP BY s.seller_name, s.category, s.rating
    ORDER BY revenue DESC LIMIT 10
""")
st.dataframe(sellers, use_container_width=True, hide_index=True)
