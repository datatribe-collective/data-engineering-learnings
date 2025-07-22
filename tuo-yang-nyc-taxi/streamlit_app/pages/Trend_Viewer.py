# Trend_Viewer.py

import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from utils import config, sql_utils
from utils.get_bigquery_client import get_bigquery_client

# ---------- Contents ----------
metric_units = {
    "trip_count": "Count",
    "avg_fare": "USD",
    "avg_tip": "USD",
    "total_passengers": "Count",
    "avg_distance": "Miles"
}

# ---------- Data Loading ----------
@st.cache_data
def load_summary_table():
    query = sql_utils.get_trip_summary_query()
    client = get_bigquery_client()
    df = client.query(query).to_dataframe()
    df["pickup_hour"] = pd.to_datetime(df["pickup_hour"])
    return df

# ---------- UI Layout ----------
st.set_page_config(page_title="Trend Viewer", layout="wide")
st.title("📈 NYC Yellow Taxi Trend Viewer")

st.markdown("""
This chart shows the evolution of key metrics such as trip count, fare, tip, and more, aggregated by hourly, daily or weekly level.
""")

granularity = st.selectbox("⏱️ Choose time granularity:", config.DEFAULT_GRANULARITIES)
metrics = st.multiselect("📊 Select metrics to display:", [
    "trip_count", "avg_fare", "avg_tip", "total_passengers", "avg_distance"
], default=["trip_count"])

if not metrics:
    st.warning("Please select at least one metric.")
    st.stop()

# ---------- Data Processing ----------
df = load_summary_table()

if granularity == "Hourly":
    df_grouped = df.copy()
    df_grouped["time"] = df_grouped["pickup_hour"]
elif granularity == "Daily":
    df_grouped = df.copy()
    df_grouped["time"] = df_grouped["pickup_hour"].dt.date
    df_grouped = df_grouped.groupby("time")[metrics].mean().reset_index()
elif granularity == "Weekly":
    df_grouped = df.copy()
    df_grouped["time"] = df_grouped["pickup_hour"].dt.to_period("W").apply(lambda r: r.start_time)
    df_grouped = df_grouped.groupby("time")[metrics].mean().reset_index()

# ---------- Add time range slider ----------
min_time = pd.to_datetime(df_grouped["time"].min()).to_pydatetime()
max_time = pd.to_datetime(df_grouped["time"].max()).to_pydatetime()

if granularity == "Hourly":
    slider_value = st.slider(
        "🗓️ Select time range:",
        min_value=min_time,
        max_value=max_time,
        value=(min_time, max_time),
        format="YYYY-MM-DD HH:mm"
    )
else:
    slider_value = st.slider(
        "🗓️ Select time range:",
        min_value=min_time,
        max_value=max_time,
        value=(min_time, max_time),
        format="YYYY-MM-DD"
    )

if granularity in "Daily":
    slider_value = (slider_value[0].date(), slider_value[1].date())

# Apply time filtering
df_grouped = df_grouped[
    (df_grouped["time"] >= slider_value[0]) &
    (df_grouped["time"] <= slider_value[1])
]

# ---------- Chart Drawing ----------
metric_names = [m.replace('_', ' ').title() for m in metrics]

st.markdown(f"### 📈 {' & '.join(metric_names)} over Time ({granularity})")

for metric in metrics:
    fig = px.line(
        df_grouped,
        x="time",
        y=metric,
        markers=True,
        labels={metric: f"{metric.replace('_', ' ').title()} ({metric_units.get(metric, '')})"},
        title=metric.replace('_', ' ').title()
    )
    fig.update_layout(
        height=400,
        margin={"l": 20, "r": 20, "t": 40, "b": 20},
        yaxis_title=f"{metric.replace('_', ' ').title()} ({metric_units.get(metric, '')})"
    )
    st.plotly_chart(fig, use_container_width=True)
