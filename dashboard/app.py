import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import time

# Page Config
st.set_page_config(
    page_title="E-Commerce ETL Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Refresh button
if st.sidebar.button('🔄 Refresh Data'):
    st.cache_data.clear()

# --- Database Connection ---
# Note: We use 'pg_etl' because we are inside the Docker network
DB_URI = "postgresql+psycopg2://etl_user:etl_pass@pg_etl:5432/analytics_db"

@st.cache_data(ttl=60)  # Cache data for 60 seconds
def load_data():
    engine = create_engine(DB_URI)
    query = "SELECT * FROM orders_clean"
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return pd.DataFrame()

# --- Load Data ---
st.title("📊 E-Commerce Sales Overview")
st.markdown("Real-time view of data processed by Airflow pipeline")

df = load_data()

if not df.empty:
    # --- KPI Row ---
    col1, col2, col3, col4 = st.columns(4)
    
    total_revenue = df['total_price'].sum()
    total_orders = len(df)
    delivered_orders = len(df[df['status'] == 'delivered'])
    avg_order_val = df['total_price'].mean()

    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Orders", total_orders)
    col3.metric("Delivered Orders", delivered_orders)
    col4.metric("Avg Order Value", f"${avg_order_val:,.2f}")

    st.markdown("---")

    # --- Charts Row 1 ---
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Revenue by Status")
        # Aggregation
        status_rev = df.groupby('status')['total_price'].sum().reset_index()
        fig_status = px.bar(
            status_rev, 
            x='status', 
            y='total_price', 
            color='status',
            template="plotly_dark",
            title="Total Revenue per Status"
        )
        st.plotly_chart(fig_status, use_container_width=True)

    with col_right:
        st.subheader("Order Count by Status")
        status_count = df['status'].value_counts().reset_index()
        status_count.columns = ['status', 'count']
        fig_count = px.pie(
            status_count, 
            values='count', 
            names='status', 
            hole=0.4,
            template="plotly_dark",
            title="Distribution of Order Status"
        )
        st.plotly_chart(fig_count, use_container_width=True)

    # --- Charts Row 2 ---
    st.subheader("Daily Revenue Trend")
    # Ensure date is datetime
    df['order_date'] = pd.to_datetime(df['order_date'])
    daily_rev = df.groupby('order_date')['total_price'].sum().reset_index()
    
    fig_line = px.line(
        daily_rev, 
        x='order_date', 
        y='total_price', 
        markers=True,
        template="plotly_dark",
        title="Revenue Over Time"
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # --- Raw Data ---
    with st.expander("View Raw Data"):
        st.dataframe(df.sort_values(by='order_id', ascending=False))

else:
    st.warning("No data found in the database yet. Run the ETL pipeline!")