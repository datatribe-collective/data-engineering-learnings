# Zone_Heatmap.py

import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from utils import config, sql_utils
from utils.get_bigquery_client import get_bigquery_client

# ---------- Constants ----------
metric_units = {
    "trip_count": "Count",
    "avg_fare": "USD",
    "avg_tip": "USD",
    "total_passengers": "Count",
    "avg_distance": "Miles"
}

@st.cache_data
def load_geojson_metadata():
    with open(config.GEOJSON_URL, "r") as f:
        geojson = f.read()
    geojson_dict = eval(geojson) if isinstance(geojson, str) else geojson
    id_to_zone = {
        int(feature["properties"]["location_id"]): feature["properties"].get("zone", "")
        for feature in geojson_dict["features"]
    }
    return geojson_dict, id_to_zone

@st.cache_data
def load_zone_summary(location_type):
    table_name = f"zone_summary_{location_type}_day"
    query = f"""
        SELECT *
        FROM `{config.PROJECT_ID}.{config.DATASET_ID}.{table_name}`
    """
    client = get_bigquery_client()
    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df[f"{location_type}_day"])
    return df

# ---------- UI ----------
st.set_page_config(page_title="Zone Heatmap", layout="wide")
st.title("🗺️ NYC Yellow Taxi Zone Heatmap")

st.markdown("""
Explore average fare, tip, trip volume and other metrics for each NYC taxi zone.
You can toggle between pickup vs dropoff, as well as view by single day or average over a date range.
""")

# --- Sidebar selections ---
col1, col2, col3 = st.columns([1,2,2])
with col1:
    location_type = st.radio("Zone Type", ["pickup", "dropoff"], horizontal=True)
with col2:
    selected_metric = st.selectbox("Metric", list(metric_units.keys()), index=0)
with col3:
    view_mode = st.radio("View", ["Raw", "Daily Average"], horizontal=True)

zone_df = load_zone_summary(location_type)
geojson, id_to_zone = load_geojson_metadata()

min_day = zone_df["date"].min().to_pydatetime()
max_day = zone_df["date"].max().to_pydatetime()

if view_mode == "Raw":
    selected_day = st.slider("📅 Select a day",
        min_value=min_day,
        max_value=max_day,
        value=min_day,
        format="YYYY-MM-DD")
    df_day = zone_df[zone_df["date"] == selected_day].copy()
    title_suffix = selected_day.strftime("%Y-%m-%d")
else:
    date_range = st.slider("📆 Select date range",
        min_value=min_day,
        max_value=max_day,
        value=(min_day, max_day),
        format="YYYY-MM-DD")
    start_date, end_date = date_range
    df_filtered = zone_df[(zone_df["date"] >= start_date) & (zone_df["date"] <= end_date)].copy()
    df_day = df_filtered.groupby("zone_id")[selected_metric].mean().reset_index()
    df_day["zone_name"] = df_day["zone_id"].map(id_to_zone)
    title_suffix = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} (Avg)"

if view_mode == "Raw":
    df_day["zone_name"] = df_day["zone_id"].map(id_to_zone)

# ---------- Plot Map ----------
st.markdown(f"### {location_type.title()} Zone Heatmap – {title_suffix}")

fig = px.choropleth_mapbox(
    df_day,
    geojson=geojson,
    locations="zone_id",
    color=selected_metric,
    featureidkey="properties.location_id",
    hover_name="zone_name",
    hover_data={
        selected_metric: True,
        "zone_id": False
    },
    color_continuous_scale="YlOrRd",
    mapbox_style="carto-positron",
    center=config.MAP_CENTER,
    zoom=9,
    opacity=0.65,
    labels={selected_metric: f"{selected_metric.replace('_',' ').title()} ({metric_units[selected_metric]})"},
    height=500
)

st.plotly_chart(fig, use_container_width=True)

st.caption("💾 Data exported as .csv – includes zone name, metric values")
st.download_button(
    label="⬇️ Export CSV",
    data=df_day.to_csv(index=False),
    file_name=f"zone_{location_type}_{selected_metric}_{view_mode.lower()}.csv",
    mime="text/csv"
)