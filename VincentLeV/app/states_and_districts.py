import streamlit as st
import pandas as pd
import os
import plotly.express as px
from utils import rank_dataframe
from utils import get_base_data_url

BASE_DATA_URL = get_base_data_url()
DATA_URL = os.path.join(BASE_DATA_URL, "banned_books.csv")

state_abbreviation_map = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN",
    "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"
}

@st.cache_data
def load_data(path: str):
    data = pd.read_csv(path)
    return data

def by_state_and_district_treemap(data: pd.DataFrame):
    df = data.groupby(["State", "District"])["Title"].nunique().reset_index(name="Titles")
    df["USA"] = "USA" 

    fig = px.treemap(
        df,
        path=["USA", "State", "District"],  
        values="Titles",
        title="Banned Books by State and District",
        width=1200,
        height=700,
    )

    st.plotly_chart(fig)

def by_state_line_chart(data: pd.DataFrame):
    data["State"] = data["State"].fillna("Unknown").astype(str).str.strip()

    state_title_counts = (
        data.groupby("State")["Title"]
        .nunique()
        .reset_index(name="Titles")
    )

    fig = px.line(
        state_title_counts,
        x="State",
        y="Titles",
        labels={"States": "State", "Titles": "Titles"},
        title="Banned Books by State"
    )

    st.plotly_chart(fig)

def by_state(data: pd.DataFrame):
    data["State"] = data["State"].map(state_abbreviation_map).fillna(data["State"])
    grouped = data.groupby(["State", "Ban Status"])["Title"].nunique().reset_index(name="Unique Titles")
    pivot = grouped.pivot(index="State", columns="Ban Status", values="Unique Titles").fillna(0).reset_index()

    pivot["Ban Status Summary"] = pivot.drop(columns=["State"]).apply(
        lambda row: ", ".join([f"{col}: {int(row[col])}" for col in pivot.columns if col != "State" and col != "Ban Status Summary"]),
        axis=1
    )

    pivot["Total Count"] = pivot.drop(columns=["State", "Ban Status Summary"]).sum(axis=1)

    fig = px.choropleth(
        pivot,
        locations="State",
        locationmode="USA-states",
        color="Total Count", 
        scope="usa", 
        title="Banned Books by State",
        color_continuous_scale="Reds",
        hover_name="State",     
        hover_data={"Total Count": True, "Ban Status Summary": False}
    )

    streamlit_bg_color = "#0E1117"
    fig.update_geos(bgcolor=streamlit_bg_color)
    fig.update_layout(
        paper_bgcolor=streamlit_bg_color,
        font_color="white"
    )

    st.plotly_chart(fig)

def by_district_line_chart(data: pd.DataFrame):
    data["District"] = data["District"].fillna("Unknown").astype(str).str.strip()

    state_title_counts = (
        data.groupby("District")["Title"]
        .nunique()
        .reset_index(name="Titles")
    )

    fig = px.line(
        state_title_counts,
        x="District",
        y="Titles",
        labels={"Districts": "District", "Titles": "Titles"},
        title="Banned Books by District"
    )

    st.plotly_chart(fig)

def top_5_banned_states(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned") | (data["Ban Status"] == "banned from libraries and classrooms")]
    state_counts = filtered_data.groupby(["State"]).size().reset_index(name="Count")
    top_states = state_counts.sort_values(by="Count", ascending=False).head(5)
    return top_states[["State", "Count"]]

def state_with_most_banned_books(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned") | (data["Ban Status"] == "banned from libraries and classrooms")]
    state_counts = filtered_data.groupby("State").size().reset_index(name="Count")
    top_state = state_counts.sort_values(by="Count", ascending=False).head(1)
    return top_state.iloc[0]["State"], top_state.iloc[0]["Count"]

def top_5_banned_districts(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned") | (data["Ban Status"] == "banned from libraries and classrooms")]
    district_counts = filtered_data.groupby(["District"]).size().reset_index(name="Count")
    top_districts = district_counts.sort_values(by="Count", ascending=False).head(5)
    return top_districts[["District", "Count"]]

def district_with_most_banned_books(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned") | (data["Ban Status"] == "banned from libraries and classrooms")]
    district_counts = filtered_data.groupby("District").size().reset_index(name="Count")
    top_district = district_counts.sort_values(by="Count", ascending=False).head(1)
    return top_district.iloc[0]["District"], top_district.iloc[0]["Count"]

def display_data(data: pd.DataFrame):
    st.title("States and Districts (2021-2024)")
    by_state_and_district_treemap(data)

    st.subheader("Top States with Banned Books")
    cols1 = st.columns([0.3, 0.7], vertical_alignment="center")
    with cols1[0]:
        top_state = state_with_most_banned_books(data)
        cols1[0].markdown(f"<p style='text-align: center; font-size: 2.5rem; font-weight: bold;'>{top_state[0]}</p>", unsafe_allow_html=True)

    with cols1[1]:
        top_states = top_5_banned_states(data)
        ranked_states = rank_dataframe(top_states, rank_column_name="Rank")
        st.dataframe(ranked_states.set_index("Rank"))

    cols2 = st.columns([0.4, 0.6])
    with cols2[0]:
        by_state(data)
    with cols2[1]:
        by_state_line_chart(data)

    st.subheader("Top Districts with Banned Books")
    cols3 = st.columns([0.3, 0.7], vertical_alignment="center")
    with cols3[0]:
        top_district = district_with_most_banned_books(data)
        cols3[0].markdown(f"<p style='text-align: center; font-size: 2.5rem; font-weight: bold;'>{top_district[0]}</p>", unsafe_allow_html=True)
    with cols3[1]:
        top_districts = top_5_banned_districts(data)
        ranked_districts = rank_dataframe(top_districts, rank_column_name="Rank")
        st.dataframe(ranked_districts.set_index("Rank"))

    by_district_line_chart(data)

display_data(load_data(DATA_URL))