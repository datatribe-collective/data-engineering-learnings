# app.py

import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="NYC Yellow Taxi Dashboard",
    layout="wide"
)

st.title("🚖 NYC Yellow Taxi Analytics Dashboard")

st.markdown("""
Welcome to the **Data Engineering Capstone Project** by `Tuo Yang`.

This dashboard demonstrates a complete data engineering pipeline for analyzing NYC Yellow Taxi data, including:

---

### 🔁 Pipeline Overview
- **Ingestion**: Parquet data loaded from public TLC source
- **Transformation**: Cleaned with pandas and PySpark
- **Storage**: Processed data uploaded to BigQuery
- **Orchestration**: Airflow DAG for automation (demo)
- **Visualization**: Interactive UI built with Streamlit & Plotly

---

### 📊 Available Visualizations
- **📈 Trend Viewer**: Analyze hourly/daily/weekly changes in fare, tip, trip count and more
- **🗺️ Zone Heatmap**: Explore NYC zones by pickup/dropoff metrics

Navigate via the **sidebar** to start exploring the data!
""")

st.caption(f"🕒 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")