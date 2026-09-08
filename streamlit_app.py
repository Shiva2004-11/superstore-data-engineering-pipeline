import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import requests
import time
import os
from datetime import datetime
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "superstore_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "Sentinel123")

FLASK_API_URL = "http://localhost:5000/api/sales"
AIRFLOW_API_URL = "http://localhost:8080/api/v1"
AIRFLOW_USER = "airflow"
AIRFLOW_PASSWORD = "airflow"
DAG_ID = "superstore_etl_pipeline"

# Streamlit Page Config
st.set_page_config(
    page_title="Superstore Sales & Profit Analysis",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
<style>
    .kpi-card {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        text-align: center;
        border: 1px solid #333;
    }
    .kpi-title {
        font-size: 1.1rem;
        color: #aaaaaa;
        margin-bottom: 10px;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: #ffffff;
    }
    .status-container {
        display: flex;
        justify-content: space-around;
        background-color: #1e1e1e;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 20px;
        border: 1px solid #333;
    }
</style>
""", unsafe_allow_html=True)

# ----------------------------------------------------
# Helper Functions
# ----------------------------------------------------

@st.cache_resource
def get_db_engine():
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return engine

def check_db_connection():
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            return True
    except Exception:
        return False

def check_flask_api():
    try:
        response = requests.get("http://localhost:5000/api/status", timeout=2)
        return response.status_code == 200
    except:
        return False

def check_airflow():
    try:
        response = requests.get(f"{AIRFLOW_API_URL}/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def check_kafka():
    # It's hard to directly check kafka from python without kafka-python connecting
    # We will assume it's running if flask and airflow are running for this demo
    # Alternatively, you can run a quick command or just return True.
    return True

@st.cache_data(ttl=60)
def load_data():
    try:
        engine = get_db_engine()
        query = "SELECT * FROM superstore_sales_final"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        return pd.DataFrame()

def initialize_database():
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'superstore_sales_final'"))
            table_exists = result.scalar() > 0
            
            if table_exists:
                result = conn.execute(text("SELECT COUNT(*) FROM superstore_sales_final"))
                count = result.scalar()
                if count > 0:
                    return True # Data already exists
                    
        # If we reach here, table does not exist or is empty. Load initial data.
        with st.spinner("Initializing database with existing Superstore dataset... This may take a minute."):
            df = pd.read_csv("Sample - Superstore.csv", encoding="latin1")
            
            # Clean columns similar to staging.py
            df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]
            
            # Parse dates
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            if 'ship_date' in df.columns:
                df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
                
            with engine.begin() as conn:
                df.to_sql("superstore_sales_final", conn, if_exists="replace", index=False)
            
            # Add primary key
            with engine.begin() as conn:
                alter_table_sql = """
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'superstore_sales_final_pkey') THEN
                        ALTER TABLE superstore_sales_final ADD PRIMARY KEY (row_id);
                    END IF;
                END $$;
                """
                conn.execute(text(alter_table_sql))
            st.success("✅ Database initialized successfully with Superstore data!")
            return True
    except Exception as e:
        import traceback
        traceback.print_exc()
        st.error(f"Failed to initialize database: {e}")
        return False

def trigger_airflow_dag():
    try:
        # Airflow API requires json data to be passed, even if empty dict
        data = {"conf": {}}
        response = requests.post(
            f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns",
            auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD),
            json=data
        )
        if response.status_code in (200, 201):
            return response.json()['dag_run_id']
        else:
            st.error(f"Failed to trigger DAG: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error triggering Airflow: {e}")
        return None

def check_dag_run_status(dag_run_id):
    try:
        response = requests.get(
            f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}",
            auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)
        )
        if response.status_code == 200:
            return response.json().get('state')
    except Exception as e:
        pass
    return None

# ----------------------------------------------------
# Main Layout
# ----------------------------------------------------

st.markdown("<h1 style='text-align: center; color: #4A90E2;'>Superstore Sales & Profit Analysis</h1>", unsafe_allow_html=True)
st.markdown("<h4 style='text-align: center; color: #888;'>Real-Time ETL Pipeline powered Sales Analytics</h4>", unsafe_allow_html=True)
st.markdown("---")

# 1. ADD NEW SALES TRANSACTION
st.header("Add New Sales Transaction")

with st.form("add_sales_form"):
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        row_id = st.number_input("Row ID", min_value=1, step=1)
        order_date = st.date_input("Order Date", value=datetime.now())
        sales = st.number_input("Sales ($)", min_value=0.0, step=0.1)
        region = st.selectbox("Region", ["West", "East", "South", "Central"])
        
    with col2:
        order_id = st.text_input("Order ID", "CA-2026-100001")
        ship_date = st.date_input("Ship Date", value=datetime.now())
        quantity = st.number_input("Quantity", min_value=1, step=1)
        segment = st.selectbox("Segment", ["Consumer", "Corporate", "Home Office"])

    with col3:
        customer_id = st.text_input("Customer ID", "CG-12520")
        ship_mode = st.selectbox("Ship Mode", ["Standard Class", "Second Class", "First Class", "Same Day"])
        discount = st.number_input("Discount", min_value=0.0, max_value=1.0, step=0.01)
        category = st.selectbox("Category", ["Technology", "Furniture", "Office Supplies"])
        
    with col4:
        customer_name = st.text_input("Customer Name", "Test Customer")
        product_id = st.text_input("Product ID", "TEC-AC-10003027")
        profit = st.number_input("Profit ($)", value=0.0, step=0.1)
        sub_category = st.text_input("Sub-Category", "Accessories")
        
    # Additional required fields for completeness based on existing schema
    country = st.text_input("Country", "United States")
    city = st.text_input("City", "Seattle")
    state = st.text_input("State", "Washington")
    postal_code = st.text_input("Postal Code", "98105")
    product_name = st.text_input("Product Name", "Test Product Name")
    
    submitted = st.form_submit_button("🚀 Add & Process Data")

if submitted:
    # Validate
    if row_id <= 0 or quantity <= 0 or sales < 0:
        st.error("❌ Invalid inputs. Quantity and Sales must be positive. Row ID must be > 0.")
    else:
        new_record = {
            "Row ID": row_id,
            "Order ID": order_id,
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": ship_date.strftime("%m/%d/%Y"),
            "Ship Mode": ship_mode,
            "Customer ID": customer_id,
            "Customer Name": customer_name,
            "Segment": segment,
            "Country": country,
            "City": city,
            "State": state,
            "Postal Code": postal_code,
            "Region": region,
            "Product ID": product_id,
            "Category": category,
            "Sub-Category": sub_category,
            "Product Name": product_name,
            "Sales": sales,
            "Quantity": quantity,
            "Discount": discount,
            "Profit": profit
        }
        
        st.info("Submitting data to Flask API...")
        try:
            # Send to Flask
            flask_response = requests.post(FLASK_API_URL, json=new_record)
            if flask_response.status_code == 201:
                # Trigger Airflow
                run_id = trigger_airflow_dag()
                if run_id:
                    st.session_state['pipeline_run_id'] = run_id
                    st.session_state['pipeline_state'] = 'RUNNING'
                    st.session_state['pipeline_start_time'] = time.time()
                    st.rerun()
            else:
                st.error(f"❌ Flask API rejected the data: {flask_response.text}")
        except Exception as e:
            st.error(f"❌ Flask API is not running or encountered an error: {e}")

st.markdown("---")

# 2. PIPELINE STATUS
st.header("ETL Pipeline Status")

if 'pipeline_state' in st.session_state and st.session_state['pipeline_state'] is not None:
    run_id = st.session_state.get('pipeline_run_id', 'Unknown')
    state = st.session_state['pipeline_state']
    
    st.info("Submitting data to Flask API...")
    st.success("New transaction accepted. Starting ETL pipeline...")
    st.info(f"Airflow DAG triggered\n\nRun ID: {run_id}\n\nWaiting for completion...")
    
    if state == 'RUNNING':
        st.info("ETL Pipeline Status: RUNNING")
        
        # Non-blocking polling logic
        start_time = st.session_state.get('pipeline_start_time', time.time())
        elapsed = int(time.time() - start_time)
        
        if elapsed > 300: # 5 minutes timeout
            st.warning("⚠️ Polling timed out after 5 minutes. Pipeline may still be running.")
            st.session_state['pipeline_state'] = None
        else:
            current_airflow_state = check_dag_run_status(run_id)
            if current_airflow_state == 'success':
                st.session_state['pipeline_state'] = 'SUCCESS'
                load_data.clear() # Specific cache invalidation
                st.rerun()
            elif current_airflow_state == 'failed':
                st.session_state['pipeline_state'] = 'FAILED'
                st.rerun()
            else:
                time.sleep(3)
                st.rerun()
                
    elif state == 'SUCCESS':
        st.success("ETL Pipeline Status: SUCCESS")
        st.success("Data successfully processed and dashboard updated.")
        st.info("Dashboard refreshed with latest PostgreSQL data.")
        
    elif state == 'FAILED':
        st.error("ETL Pipeline Status: FAILED")
        st.error("Pipeline execution failed. Please check Airflow logs.")

db_status = check_db_connection()
flask_status = check_flask_api()
airflow_status = check_airflow()
kafka_status = check_kafka()

st.markdown(f"""
<div class="status-container">
    <div><b>Flask API</b> {'🟢' if flask_status else '🔴'}</div>
    <div><b>Kafka</b> {'🟢' if kafka_status else '🔴'}</div>
    <div><b>Airflow</b> {'🟢' if airflow_status else '🔴'}</div>
    <div><b>PostgreSQL</b> {'🟢' if db_status else '🔴'}</div>
</div>
""", unsafe_allow_html=True)

if not db_status:
    st.error("❌ PostgreSQL database is unavailable. Cannot load dashboard.")
    st.stop()
else:
    # Initialize DB if empty
    initialize_database()

# Load Data
df = load_data()

if df.empty:
    st.warning("No data found in PostgreSQL database. Please make sure the pipeline has successfully loaded data.")
    st.stop()

# Standardize date columns for filtering if they exist
if 'order_date' in df.columns:
    df['order_date'] = pd.to_datetime(df['order_date'])

st.markdown("---")

# 3. FILTERS
st.header("Filters")
col1, col2, col3, col4 = st.columns(4)

with col1:
    if 'region' in df.columns:
        region_options = ["All"] + list(df['region'].dropna().unique())
        selected_region = st.selectbox("Region", region_options)
    else:
        selected_region = "All"

with col2:
    if 'category' in df.columns:
        category_options = ["All"] + list(df['category'].dropna().unique())
        selected_category = st.selectbox("Category", category_options)
    else:
        selected_category = "All"

with col3:
    if 'segment' in df.columns:
        segment_options = ["All"] + list(df['segment'].dropna().unique())
        selected_segment = st.selectbox("Segment", segment_options)
    else:
        selected_segment = "All"

with col4:
    if 'ship_mode' in df.columns:
        ship_mode_options = ["All"] + list(df['ship_mode'].dropna().unique())
        selected_ship_mode = st.selectbox("Ship Mode", ship_mode_options)
    else:
        selected_ship_mode = "All"

# Date Filter
if 'order_date' in df.columns and not df['order_date'].isnull().all():
    min_date = df['order_date'].min().date()
    max_date = df['order_date'].max().date()
    date_range = st.date_input("Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)
else:
    date_range = []

# Apply Filters
filtered_df = df.copy()

if selected_region != "All" and 'region' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['region'] == selected_region]

if selected_category != "All" and 'category' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['category'] == selected_category]
    
if selected_segment != "All" and 'segment' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['segment'] == selected_segment]
    
if selected_ship_mode != "All" and 'ship_mode' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['ship_mode'] == selected_ship_mode]

if len(date_range) == 2 and 'order_date' in filtered_df.columns:
    start_date, end_date = date_range
    filtered_df = filtered_df[(filtered_df['order_date'].dt.date >= start_date) & (filtered_df['order_date'].dt.date <= end_date)]

st.markdown("---")

# 4. KPI CARDS
st.header("Key Performance Indicators")

total_sales = filtered_df['sales'].sum() if 'sales' in filtered_df.columns else 0
total_profit = filtered_df['profit'].sum() if 'profit' in filtered_df.columns else 0
total_quantity = filtered_df['quantity'].sum() if 'quantity' in filtered_df.columns else 0
total_discount = filtered_df['discount'].sum() if 'discount' in filtered_df.columns else 0

def format_number(num):
    if num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"{num/1_000:.2f}K"
    else:
        return f"{num:.2f}"

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total Sales</div>
        <div class="kpi-value">${format_number(total_sales)}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total Profit</div>
        <div class="kpi-value">${format_number(total_profit)}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total Quantity</div>
        <div class="kpi-value">{format_number(total_quantity)}</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total Discount</div>
        <div class="kpi-value">{format_number(total_discount)}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# 5. CHARTS
st.header("Sales Analysis")

col1, col2 = st.columns(2)

with col1:
    # Chart 1 – Sales by Category
    if 'category' in filtered_df.columns and 'sales' in filtered_df.columns:
        sales_by_cat = filtered_df.groupby('category', as_index=False)['sales'].sum()
        fig1 = px.bar(sales_by_cat, x='category', y='sales', title='Sales by Category', color='category', text_auto='.2s')
        fig1.update_layout(xaxis_title="Category", yaxis_title="Sum of Sales", showlegend=False)
        st.plotly_chart(fig1, use_container_width=True)
    
    # Chart 3 – Sales by Region
    if 'region' in filtered_df.columns and 'sales' in filtered_df.columns:
        sales_by_reg = filtered_df.groupby('region', as_index=False)['sales'].sum()
        fig3 = px.bar(sales_by_reg, x='region', y='sales', title='Sales by Region', color='region', text_auto='.2s')
        fig3.update_layout(xaxis_title="Region", yaxis_title="Sum of Sales", showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

with col2:
    # Chart 2 – Profit by Category
    if 'category' in filtered_df.columns and 'profit' in filtered_df.columns:
        profit_by_cat = filtered_df.groupby('category', as_index=False)['profit'].sum()
        fig2 = px.bar(profit_by_cat, x='category', y='profit', title='Profit by Category', color='category', text_auto='.2s')
        fig2.update_layout(xaxis_title="Category", yaxis_title="Sum of Profit", showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Chart 4 – Monthly Sales Trend
    if 'order_date' in filtered_df.columns and 'sales' in filtered_df.columns:
        temp_df = filtered_df.copy()
        temp_df['month_year'] = temp_df['order_date'].dt.to_period('M')
        monthly_sales = temp_df.groupby('month_year', as_index=False)['sales'].sum()
        monthly_sales['month_year'] = monthly_sales['month_year'].astype(str)
        monthly_sales = monthly_sales.sort_values('month_year')
        
        fig4 = px.line(monthly_sales, x='month_year', y='sales', title='Monthly Sales Trend', markers=True)
        fig4.update_layout(xaxis_title="Month", yaxis_title="Sum of Sales")
        st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")
st.header("Business Analysis")

col3, col4 = st.columns(2)

with col3:
    # Chart 5 – Profit by Region
    if 'region' in filtered_df.columns and 'profit' in filtered_df.columns:
        profit_by_reg = filtered_df.groupby('region', as_index=False)['profit'].sum()
        fig5 = px.bar(profit_by_reg, x='region', y='profit', title='Profit by Region', color='region', text_auto='.2s')
        fig5.update_layout(xaxis_title="Region", yaxis_title="Sum of Profit", showlegend=False)
        st.plotly_chart(fig5, use_container_width=True)
    
    # Chart 7 – Sales by Sub-Category
    if 'sub_category' in filtered_df.columns and 'sales' in filtered_df.columns:
        sales_by_subcat = filtered_df.groupby('sub_category', as_index=False)['sales'].sum().sort_values('sales', ascending=True)
        fig7 = px.bar(sales_by_subcat, x='sales', y='sub_category', orientation='h', title='Sales by Sub-Category', text_auto='.2s')
        fig7.update_layout(xaxis_title="Sum of Sales", yaxis_title="Sub-Category")
        st.plotly_chart(fig7, use_container_width=True)

with col4:
    # Chart 6 – Sales by Segment
    if 'segment' in filtered_df.columns and 'sales' in filtered_df.columns:
        sales_by_seg = filtered_df.groupby('segment', as_index=False)['sales'].sum()
        fig6 = px.pie(sales_by_seg, names='segment', values='sales', title='Sales by Segment', hole=0.4)
        st.plotly_chart(fig6, use_container_width=True)

    # Chart 8 – Profit by Sub-Category
    if 'sub_category' in filtered_df.columns and 'profit' in filtered_df.columns:
        profit_by_subcat = filtered_df.groupby('sub_category', as_index=False)['profit'].sum().sort_values('profit', ascending=True)
        fig8 = px.bar(profit_by_subcat, x='profit', y='sub_category', orientation='h', title='Profit by Sub-Category', text_auto='.2s')
        fig8.update_layout(xaxis_title="Sum of Profit", yaxis_title="Sub-Category")
        st.plotly_chart(fig8, use_container_width=True)

st.markdown("<br><p style='text-align: center; color: #888;'>Data source: PostgreSQL (superstore_sales_final)</p>", unsafe_allow_html=True)
