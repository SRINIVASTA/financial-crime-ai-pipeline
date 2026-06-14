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
        .appName("Desktop-Streaming-Engine") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

spark = init_spark()

# 2. STATE MANAGEMENT FOR THE HISTORICAL DETAILED LEDGER
if 'cumulative_ledger' not in st.session_state:
    st.session_state.cumulative_ledger = []

# Set up desktop dashboard UI layout
st.set_page_config(page_title="Financial Crime Monitor", layout="wide")
st.title("🛡️ Financial Crime AI Desktop Monitor")
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

# Initialize container anchors at the top of the interface
status_box = st.empty()

# FIX: Layout configuration placing the chart and metrics side-by-side directly under status_box
top_metrics_col, top_chart_col = st.columns([1.1, 0.9])

with top_metrics_col:
    metric_anchor = st.empty()

with top_chart_col:
    chart_header_placeholder = st.empty()
    chart_placeholder = st.empty()

st.write("---")

# Lower Layout Structure for Data Tables and Export Controls
left_view, right_view = st.columns([1.3, 0.7])

with left_view:
    latest_header = st.empty()
    latest_grid_placeholder = st.empty()
    st.write(" ")
    historical_header = st.empty()
    historical_grid_placeholder = st.empty()

with right_view:
    st.markdown("### 🗃️ Compliance Export Control")
    download_btn_placeholder = st.empty()
    st.write(" ")
    if st.button("🚨 Clear History & Chart Buffer"):
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
        m_col1, m_col2 = st.columns(2)
        m_col1.metric(label="Ingested Inbound Rows", value=f"+{num_tx}", delta=f"Batch Total: {total_processed_rows}")
        m_col2.metric(label="Latest Batch Alerts", value=latest_alert_count, delta="Current Cycle", delta_color="inverse")
        st.write(" ")
        st.metric(label="Total Stored Audit Trails", value=total_archived_count)

    # 6. LIVE MATPLOTLIB PIE CHART GENERATION (DIRECTLY UNDER ENGINE STATUS)
    if total_archived_count > 0:
        chart_header_placeholder.markdown("### 📊 Live Risk Status Distribution")
        status_counts = historical_aggregate_df['WATCHLIST_STATUS'].value_counts()
        
        color_map = {
            'IMMEDIATE_BLOCK': '#d9534f',
            'HIGH_RISK_ALERT': '#f0ad4e',
            'MONITOR_SUSPICIOUS': '#f0ad4e',
            'CLEARED': '#5cb85c'
        }
        current_colors = [color_map.get(status, '#bcbd22') for status in status_counts.index]
        
        fig, ax = plt.subplots(figsize=(5, 4))
        fig.patch.set_facecolor('#0e1117') 
        ax.set_facecolor('#0e1117')
        
        wedges, texts, autotexts = ax.pie(
            status_counts.values,
            labels=status_counts.index,
            autopct='%1.1f%%',
            startangle=140,
            colors=current_colors,
            textprops=dict(color="w", weight="bold", size=9)
        )
        
        ax.axis('equal')  
        plt.tight_layout()
        chart_placeholder.pyplot(fig)
        plt.close(fig)
    else:
        chart_header_placeholder.info("📊 Awaiting data matrix to render risk pie chart...")

    # 7. RENDER THE CURRENT BATCH LOG BELOW (CAPPED TO TOP 3 HIGHEST-VALUE ROWS)
    latest_header.markdown(f"### 🔴 Latest Live Ingestion Batch Alerts ({current_time}) — *Showing Top 3 Priority Alerts*")
    if latest_alert_count > 0:
        display_latest = current_batch_pandas.sort_values(by="AMOUNT", ascending=False).head(3).copy()
        display_latest = display_latest[['CUSTOMER_ID', 'AMOUNT', 'LOCATION', 'CHANNEL', 'RISK_SCORE', 'WATCHLIST_STATUS']]
        display_latest['AMOUNT'] = display_latest['AMOUNT'].apply(lambda x: f"₹{x:,.2f}")
        latest_grid_placeholder.dataframe(display_latest, use_container_width=True)
    else:
        latest_grid_placeholder.success("✅ Current streaming batch clear. No real-time anomalies detected.")

    # 8. RENDER ALL HISTORICAL DETAILS AT THE BOTTOM
    historical_header.markdown("### 📜 All Completed Details (Cumulative Audit Log)")
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
    else:
        historical_grid_placeholder.info("Awaiting structural incoming stream data to populate completed logs...")

    time.sleep(3)
