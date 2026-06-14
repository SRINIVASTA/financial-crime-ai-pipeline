# Multi-Cloud Financial Crime AI & AML Pipeline Simulator 🛡️💼

An enterprise-grade, cloud-deployed **PySpark** and **Streamlit** data application engineering pipeline. This platform emulates real-world, multi-cloud financial operations by streaming synthetic transactional ledgers, cross-referencing global risk watchlists, and generating live categorical compliance analytics.

🚀 **Live Production Application:** (https://financial-crime-ai-pipeline-favmurceuez7ffyjuzsbap.streamlit.app/)

---

## 🏗️ Enterprise Architecture Model

In highly regulated banking ecosystems, data privacy mandates (APRA, RBI) prevent the use of raw production logs in unsecured testing sandboxes. This project establishes a **Zero-Configuration, High-Fidelity Simulation Architecture** that mirrors a true banking technology footprint:


1. **Inbound Ledger Stream (AWS S3 Emulation):** Generates high-velocity operational banking transactions (Amounts, Locations, and Escape Channels like `Crypto_Gate`).
2. **Compliance Ledger Sync (GCP Storage Emulation):** Supplies static reference data containing historical `RISK_SCORE` maps, `AML_FLAG` conditions, and audit states.
3. **Data Fusion & Rule Execution (PySpark Engine):** Executes memory-optimized joins and flags core algorithmic threats (`AMOUNT >= ₹1,50,000` OR `AML_FLAG == 1`).

---

## 📊 Sample Executive Execution View

The engine optimizes user focus by constraining the real-time processing view to the **Top 3 Highest-Value Priority Alerts**, while seamlessly routing the comprehensive underlying transactional stream into a persistent analytical log down the pane.

```text
🛡️ Financial Crime AI Terminal
⏱️ System Timestamp: 2026-06-14 12:40:00 | 🟩 Ingestion Engine: Active

[📊 Live Risk Status Distribution Chart Rendered via Matplotlib]

🚨 Latest Ingestion Batch Alerts (Showing Top 3 Priority Vectors):
+-------------+-------------+-----------+--------------+------------+------------------+

| CUSTOMER_ID |   AMOUNT    | LOCATION  |   CHANNEL    | RISK_SCORE | WATCHLIST_STATUS |
+-------------+-------------+-----------+--------------+------------+------------------+

| CUST_109    | ₹9,94,320.50| Unknown   | Crypto_Gate  |    94.3    | IMMEDIATE_BLOCK  |
| CUST_101    | ₹4,50,000.00| Mumbai    | Wire_Transfer|    88.5    | HIGH_RISK_ALERT  |
| CUST_106    | ₹1,85,200.00| Sydney    | Online       |    75.0    | HIGH_RISK_ALERT  |
+-------------+-------------+-----------+--------------+------------+------------------+

📥 [Download Complete Details (.CSV)] 🚨 [Clear History Logs]
```

---

## ⚙️ Advanced Engineering Solutions Implemented

* **Container Dependency Alignment (`packages.txt`):** Injected the Linux OS layer `default-jdk` directly into the cloud container, resolving standard Python-to-Java virtual machine (JVM) runtime gateway initialization bottlenecks.
* **Memory Optimization Management:** Applied explicit `.copy()` mutations during memory block subsetting to eliminate `SettingWithCopyWarning` alerts, ensuring upstream streaming data frames remain structurally immutable for auditing purposes.
* **Dynamic Widget Key Hashing:** Implemented epoch timestamp key hashing (`key=f"csv_btn_{int(time.time())}"`) within the continuous streaming refresh loop, preventing state collision errors caused by duplicate widget element registrations in stateless frameworks.
* **Priority Load Balancing:** Restructured the frontend canvas to isolate priority alerts up top using vectorized sorting arrays (`.sort_values().head(3)`), protecting compliance analysts from severe alert fatigue while capturing 100% of rows in back-end buffers.

---

## 🛠️ Step-by-Step Setup and Execution

To run this application locally or deploy it to an isolated staging slice:

1. **Clone the repository architecture:**
   ```bash
   git clone https://github.com
   cd financial-crime-ai-pipeline
   ```

2. **Verify local system environment dependencies:**
   Make sure you have Java (JDK 8 or higher) installed on your machine. Install the Python packages using:
   ```bash
   pip install -r requirements.txt
   ```

3. **Launch the desktop streaming engine:**
   ```bash
   streamlit run app.py
   ```

---

## 👔 Project Author
**Appala Srinivas Tanakala** — Data Scientist & FinTech Architect  
* 20+ Years Banking Operations, Strategic Risk, & Capital Markets Domain Mastery  
* 4+ Years Hands-On AI/ML Engineering & Production Data Pipeline Deployment  
* **GitHub:** [://github.com](https://://github.com/srinivasta)
