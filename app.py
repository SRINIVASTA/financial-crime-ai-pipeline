import streamlit as st
import pandas as pd
import numpy as np
import time
import io
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. INITIALIZE SECURE CACHED PYSPARK SESSION
@st.cache_resource
def init_spark():
    return SparkSession.builder \
        .appName("CBA-Desktop-Streaming-Engine") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

spark = init_spark()

# 2. STATE MANAGEMENT FOR THE HISTORICAL DETAILED LEDGER
if 'cumulative_ledger' not in st.session_state:
    st.session_state.cumulative_ledger = []

# Set up desktop dashboard UI layout
st.set_page_config(page_title="CBA Financial Crime Monitor", layout="wide")
st.title("🛡️ Commonwealth Bank - Financial Crime AI Desktop Monitor")
st.subheader("Live Multi-Cloud Transaction Streaming & AML Risk Detection Engine")

# Setup static GCP Watchlist Reference Data in memory
gcp_watchlist_data = """CUSTOMER_ID,RISK_SCORE,AML_FLAG,WATCHLIST_STATUS
CUST_101,88.5,1,HIGH_RISK_ALERT
CUST_102,12.0,0,CLEARED
CUST_103,45.2,0,MONITOR_SUSPICIOUS
CUST_104,5.4,0,CLEARED
CUST_105,97.1,1,IMMEDIATE_BLOCK
CUST_106,75.0,1,HIGH_RISK_ALERT
CUST_107,2.1,0,CLEARED
CUST_108,61.4,0,MONITOR_SUSPICIOUS
CUST_109,94.3,1,IMMEDIATE_BLOCK
CUST_110,18.9,0,CLEARED"""

gcp_pandas_df = pd.read_csv(io.StringIO(gcp_watchlist_data))
gcp_spark_df = spark.createDataFrame(gcp_pandas_df)

# Initialize container anchors to prevent metrics and layout modules from shifting
status_box = st.empty()
metric_anchor = st.empty()

st.write("---")

# UI Layer Split 1: Top Display Grid for Real-Time Alerts
latest_header = st.empty()
latest_grid_placeholder = st.empty()

st.write("---")

# UI Layer Split 2: Lower Split View (Data Logs on the Left, Live Pie Chart on the Right)
historical_header = st.empty()
log_col, chart_col = st.columns([1.2, 0.8]) # Assigning layout width proportions

with log_col:
    historical_grid_placeholder = st.empty()
    download_btn_placeholder = st.empty()

with chart_col:
    chart_header_placeholder = st.empty()
    chart_placeholder = st.empty()
    clear_logs_button = st.button("🚨 Clear History & Chart Buffer")
    if clear_logs_button:
        st.session_state.cumulative_ledger = []
        st.success("Historical logs and visual analytics reset.")

locations = ['Mumbai', 'Sydney', 'Bangalore', 'Melbourne', 'Unknown', 'London', 'New York']
channels = ['ATM', 'POS', 'Online', 'Crypto_Gate', 'Wire_Transfer']
customer_pool = [f"CUST_{i}" for i in range(101, 111)]

# 3. CONTINUOUS STREAMING REFRESH LOOP
while True:
    status_box.markdown("🟢 **Engine Status:** Ingesting live streams from AWS S3 ledger layers...")
    
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    num_tx = int(np.random.randint(4, 8))
    
    # Generate random transactions with explicit Python type casting
    generated_tx = []
    for _ in range(num_tx):
        generated_tx.append({
            "CUSTOMER_ID": str(np.random.choice(customer_pool)),
            "TX_TIMESTAMP": str(current_time),
            "AMOUNT": round(float(np.random.exponential(scale=150000)), 2),
            "LOCATION": str(np.random.choice(locations)),
            "CHANNEL": str(np.random.choice(channels))
        })
    
    aws_pandas_df = pd.DataFrame(generated_tx)
    aws_spark_df = spark.createDataFrame(aws_pandas_df)
    
    # Execute distributed relational join
    flagged_crime_df = aws_spark_df.join(gcp_spark_df, on="CUSTOMER_ID", how="inner")
    financial_alerts_spark = flagged_crime_df.filter(
        (F.col("AMOUNT") >= 150000) | (F.col("AML_FLAG") == 1)
    )
    
    current_batch_pandas = financial_alerts_spark.toPandas()
    
    # 4. APPEND ALL RECENT ALERTS TO HISTORICAL STORAGE
    if not current_batch_pandas.empty:
        clean_batch = current_batch_pandas[['CUSTOMER_ID', 'TX_TIMESTAMP', 'AMOUNT', 'LOCATION', 'CHANNEL', 'RISK_SCORE', 'WATCHLIST_STATUS']].copy()
        for _, row in clean_batch.iterrows():
            st.session_state.cumulative_ledger.append(row.to_dict())

    # Build the full archive log view model
    historical_aggregate_df = pd.DataFrame(st.session_state.cumulative_ledger)
    
    # 5. REFRESH REAL-TIME METRIC CARDS
    total_processed_rows = len(aws_pandas_df)
    latest_alert_count = len(current_batch_pandas)
    total_archived_count = len(historical_aggregate_df)
    
    with metric_anchor.container():
        m_col1, m_col2, m_col3 = st.columns(3)
        m_col1.metric(label="Ingested Inbound Rows", value=f"+{num_tx}", delta=f"Batch Total: {total_processed_rows}")
        m_col2.metric(label="Latest Batch Alerts", value=latest_alert_count, delta="Current Cycle", delta_color="inverse")
        m_col3.metric(label="Total Stored Audit Trails", value=total_archived_count)

    # 6. RENDER THE CURRENT BATCH LOG AT THE TOP (CAPPED TO TOP 3 HIGHEST-VALUE ROWS)
    latest_header.markdown(f"### 🔴 Latest Live Ingestion Batch Alerts ({current_time}) — *Showing Top 3 Priority Alerts*")
    if latest_alert_count > 0:
        display_latest = current_batch_pandas.sort_values(by="AMOUNT", ascending=False).head(3).copy()
        display_latest = display_latest[['CUSTOMER_ID', 'AMOUNT', 'LOCATION', 'CHANNEL', 'RISK_SCORE', 'WATCHLIST_STATUS']]
        display_latest['AMOUNT'] = display_latest['AMOUNT'].apply(lambda x: f"₹{x:,.2f}")
        latest_grid_placeholder.dataframe(display_latest, use_container_width=True)
    else:
        latest_grid_placeholder.success("✅ Current streaming batch clear. No real-time anomalies detected.")

    # 7. RENDER ALL HISTORICAL DETAILS BELOW
    historical_header.markdown("### 📜 Completed Audit Trail Records")
    if total_archived_count > 0:
        display_history = historical_aggregate_df.copy()
        display_history['AMOUNT'] = display_history['AMOUNT'].apply(lambda x: f"₹{x:,.2f}")
        historical_grid_placeholder.dataframe(display_history, use_container_width=True)
        
        # Build live download link
        csv_buffer = io.StringIO()
        historical_aggregate_df.to_csv(csv_buffer, index=False)
        csv_data_string = csv_buffer.getvalue()
        download_btn_placeholder.download_button(
            label="📥 Download Complete Details (.CSV)",
            data=csv_data_string,
            file_name=f"cba_aml_completed_details_{time.strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"csv_down_key_{int(time.time())}"
        )
        
        # ==========================================
        # 8. LIVE MATPLOTLIB PIE CHART GENERATION
        # ==========================================
        chart_header_placeholder.markdown("### 📊 Live Risk Status Distribution")
        
        # Calculate categorical frequencies from the cumulative ledger buffer
        status_counts = historical_aggregate_df['WATCHLIST_STATUS'].value_counts()
        
        # Enforce highly professional corporate color branding maps
        # Red for blocks, Orange for suspicious tracking, Green for cleared validations
        color_map = {
            'IMMEDIATE_BLOCK': '#d9534f',
            'HIGH_RISK_ALERT': '#f0ad4e',
            'MONITOR_SUSPICIOUS': '#f0ad4e',
            'CLEARED': '#5cb85c'
        }
        current_colors = [color_map.get(status, '#bcbd22') for status in status_counts.index]
        
        # Build Matplotlib chart figure space explicitly with a dark matching theme background canvas
        fig, ax = plt.subplots(figsize=(6, 5))
        fig.patch.set_facecolor('#0e1117') # Matches Streamlit's base engine layout tone
        ax.set_facecolor('#0e1117')
        
        # Plot the categorical data slices cleanly
        wedges, texts, autotexts = ax.pie(
            status_counts.values,
            labels=status_counts.index,
            autopct='%1.1f%%',
            startangle=140,
            colors=current_colors,
            textprops=dict(color="w", weight="bold", size=10) # Set readable white text variables
        )
        
        # Equal aspect ratio ensures that pie is drawn as a perfect circle
        ax.axis('equal')  
        plt.tight_layout()
        
        # Stream the freshly compiled image block directly into the dedicated grid space
        chart_placeholder.pyplot(fig)
        plt.close(fig) # Explicitly clear fig out of cache arrays to block leakage
        
    else:
        historical_grid_placeholder.info("Awaiting structural incoming stream data to populate completed logs...")
        chart_header_placeholder.info("📊 Awaiting data matrix to render risk pie chart...")

    time.sleep(3)
