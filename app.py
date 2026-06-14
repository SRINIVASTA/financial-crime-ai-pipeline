import streamlit as st
import pandas as pd
import numpy as np
import time
import io
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

# 2. STATE MANAGEMENT FOR PERSISTENT AUDIT LOGS
if 'cumulative_alerts' not in st.session_state:
    st.session_state.cumulative_alerts = []

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

# Initialize persistent layout placeholders
status_box = st.empty()
metric_col1, metric_col2, metric_col3 = st.columns(3)

st.write("---")

# Left Column for Live Stream / Right Column for CSV Exporter Controls
left_ui, right_ui = st.columns([3, 1])

with left_ui:
    latest_title = st.empty()
    latest_table_placeholder = st.empty()
    st.write("---")
    historical_title = st.empty()
    historical_table_placeholder = st.empty()

with right_ui:
    st.markdown("### 🗃️ Compliance Export Control")
    download_placeholder = st.empty()
    st.write(" ")
    clear_logs_button = st.button("🚨 Clear Cumulative Audit Logs")
    if clear_logs_button:
        st.session_state.cumulative_alerts = []
        st.success("Audit log buffer cleared.")

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
    
    alerts_pandas = financial_alerts_spark.toPandas()
    
    # 4. STORE AND APPEND DATA IN THE HISTORY LOG
    if not alerts_pandas.empty:
        clean_batch = alerts_pandas[['CUSTOMER_ID', 'TX_TIMESTAMP', 'AMOUNT', 'LOCATION', 'CHANNEL', 'RISK_SCORE', 'WATCHLIST_STATUS']].copy()
        for _, row in clean_batch.iterrows():
            st.session_state.cumulative_alerts.insert(0, row.to_dict()) # Insert at top

    # Convert cumulative tracking data into an analytical DataFrame
    all_historical_df = pd.DataFrame(st.session_state.cumulative_alerts)
    
    # 5. REFRESH REAL-TIME METRIC BARS
    total_processed = len(aws_pandas_df)
    current_flagged_count = len(alerts_pandas)
    cumulative_flagged_count = len(all_historical_df)
    
    metric_col1.metric(label="Ingested Inbound Rows", value=f"+{num_tx}", delta=f"Batch Total: {total_processed}")
    metric_col2.metric(label="Latest Batch Alerts", value=current_flagged_count, delta="Action Needed", delta_color="inverse")
    metric_col3.metric(label="Total Stored Audit Trails", value=cumulative_flagged_count)

    # 6. LAYOUT RENDERING: LATEST AT THE TOP, HISTORY BELOW
    latest_title.markdown(f"### 🔴 Latest Live Ingestion Batch Alerts ({current_time})")
    if current_flagged_count > 0:
        display_latest = alerts_pandas[['CUSTOMER_ID', 'AMOUNT', 'LOCATION', 'CHANNEL', 'RISK_SCORE', 'WATCHLIST_STATUS']].copy()
        display_latest['AMOUNT'] = display_latest['AMOUNT'].apply(lambda x: f"₹{x:,.2f}")
        latest_table_placeholder.dataframe(display_latest, use_container_width=True)
    else:
        latest_table_placeholder.success("✅ Current streaming batch clear. No real-time anomalies detected.")

    historical_title.markdown("### 📜 Historical Compliance Archive Logs (Pushed Down)")
    if cumulative_flagged_count > 0:
        display_historical = all_historical_df.copy()
        display_historical['AMOUNT'] = display_historical['AMOUNT'].apply(lambda x: f"₹{x:,.2f}")
        historical_table_placeholder.dataframe(display_historical, use_container_width=True)
        
        # 7. EXPORT DATA LAYER: BUILD LIVE CSV DOWNLOAD HOOK
        csv_buffer = io.StringIO()
        all_historical_df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()
        
        # FIX: Appending a dynamic epoch timestamp key prevents duplicate identifier exceptions!
        download_placeholder.download_button(
            label="📥 Download Audit Logs (.CSV)",
            data=csv_string,
            file_name=f"cba_aml_audit_log_{time.strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"csv_download_btn_{int(time.time())}"
        )
    else:
        historical_table_placeholder.info("System initializing. Awaiting cumulative streaming anomalies...")

    time.sleep(3)
