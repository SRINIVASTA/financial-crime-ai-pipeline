import streamlit as st
import pandas as pd
import numpy as np
import time
import io
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. INITIALIZE SECURE CACHED PYSPARK SESSION WITH CLOUD MEMORY CONSTRAINTS
@st.cache_resource
def init_spark():
    return SparkSession.builder \
        .appName("CBA-Desktop-Streaming-Engine") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

spark = init_spark()

# Set up desktop dashboard layout
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

# Initialize Streamlit placeholder components
status_box = st.empty()
metric_col1, metric_col2, metric_col3 = st.columns(3)
table_title = st.empty()
alert_table_placeholder = st.empty()

locations = ['Mumbai', 'Sydney', 'Bangalore', 'Melbourne', 'Unknown', 'London', 'New York']
channels = ['ATM', 'POS', 'Online', 'Crypto_Gate', 'Wire_Transfer']
customer_pool = [f"CUST_{i}" for i in range(101, 111)]

st.write("---")
st.info("💡 *This web application simulates live streaming AWS transaction logs being joined with a GCP compliance risk ledger via PySpark every 3 seconds.*")

# 2. CONTINUOUS STREAMING REFRESH LOOP
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
    
    # 3. RUN DISTRIBUTED TRANSFORMS ON FRESH DATA BATCHES
    flagged_crime_df = aws_spark_df.join(gcp_spark_df, on="CUSTOMER_ID", how="inner")
    
    # Apply standard CommBank financial crime filtering rules
    financial_alerts_spark = flagged_crime_df.filter(
        (F.col("AMOUNT") >= 150000) | (F.col("AML_FLAG") == 1)
    )
    
    alerts_pandas = financial_alerts_spark.toPandas()
    
    # 4. REFRESH METRIC CARDS AND AUDIT TABLES
    total_processed = len(aws_pandas_df)
    total_flagged = len(alerts_pandas)
    total_value_flagged = float(alerts_pandas['AMOUNT'].sum()) if total_flagged > 0 else 0.0
    
    metric_col1.metric(label="Ingested Inbound Rows", value=f"+{num_tx}", delta=f"Total Batch: {total_processed}")
    metric_col2.metric(label="Active Fraud/AML Alerts", value=total_flagged, delta="Action Required", delta_color="inverse")
    metric_col3.metric(label="Volume Under Investigation", value=f"₹{total_value_flagged:,.2f}")
        
    table_title.markdown(f"### 🚨 Real-Time Compliance Audit Trail ({current_time})")
    
    if total_flagged > 0:
        display_df = alerts_pandas[['CUSTOMER_ID', 'AMOUNT', 'LOCATION', 'CHANNEL', 'RISK_SCORE', 'WATCHLIST_STATUS']].copy()
        display_df['AMOUNT'] = display_df['AMOUNT'].apply(lambda x: f"₹{x:,.2f}")
        alert_table_placeholder.dataframe(display_df, use_container_width=True)
    else:
        alert_table_placeholder.success("✅ Current streaming batch clear. No high-risk anomalies detected.")
        
    time.sleep(3)
