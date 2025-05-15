import streamlit as st
import pandas as pd
import plotly.express as px
import os
from utils import rank_dataframe
from utils import get_base_data_url

BASE_DATA_URL = get_base_data_url()
DATA_URL = os.path.join(BASE_DATA_URL, "banned_books.csv")

ban_colors = {
    "banned": "#bf0603",
    "banned from libraries and classrooms": "#ff6d00",
    "banned by restriction": "#0096c7",
    "banned pending investigation": "#ffea00"
}

@st.cache_data
def load_data(path: str):
    data = pd.read_csv(path)
    return data

def by_year_bar_chart(data: pd.DataFrame):
    year_status_counts = (
        data
          .groupby(["Year", "Ban Status"])
          ["Title"].nunique()
          .reset_index(name="Titles")
    )

    fig = px.bar(
        year_status_counts,
        x="Year",
        y="Titles",
        color="Ban Status",
        color_discrete_map=ban_colors,
        labels={"Titles": "Titles", "Year": "Year", "Ban Status": "Ban Status"},
        title="Banned Books by Year",
        barmode="group"
    )

    st.plotly_chart(fig)

def by_origin_of_challenge_bar_chart(data: pd.DataFrame):
    year_status_counts = (
        data
          .groupby(["Origin of Challenge", "Ban Status"])
          ["Title"].nunique()
          .reset_index(name="Titles")
    )

    fig = px.bar(
        year_status_counts,
        x="Origin of Challenge",
        y="Titles",
        color="Ban Status",
        color_discrete_map=ban_colors,
        labels={"Titles": "Titles", "Origin of Challenge": "Origin of Challenge", "Ban Status": "Ban Status"},
        title="Banned Books by Origin of Challenge",
        barmode="group"
    )

    st.plotly_chart(fig)

def top_5_banned_titles(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned") | (data["Ban Status"] == "banned from libraries and classrooms")]
    title_counts = filtered_data.groupby(["Title", "Author"]).size().reset_index(name="Ban Count")
    top_titles = title_counts.sort_values(by="Ban Count", ascending=False).head(5)
    return top_titles[["Title", "Author", "Ban Count"]]

def top_5_challenged_titles(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned by restriction") | (data["Ban Status"] == "banned pending investigation")]
    title_counts = filtered_data.groupby(["Title", "Author"]).size().reset_index(name="Ban Count")
    top_titles = title_counts.sort_values(by="Ban Count", ascending=False).head(5)
    return top_titles[["Title", "Author", "Ban Count"]]

def top_5_banned_authors(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned") | (data["Ban Status"] == "banned from libraries and classrooms")]
    author_counts = filtered_data.groupby(["Author"]).size().reset_index(name="Count")
    top_authors = author_counts.sort_values(by="Count", ascending=False).head(5)
    return top_authors[["Author", "Count"]]

def top_5_challenged_authors(data: pd.DataFrame):
    filtered_data = data[(data["Ban Status"] == "banned by restriction") | (data["Ban Status"] == "banned pending investigation")]
    author_counts = filtered_data.groupby(["Author"]).size().reset_index(name="Count")
    top_authors = author_counts.sort_values(by="Count", ascending=False).head(5)
    return top_authors[["Author", "Count"]]

def display_data(data: pd.DataFrame):
    st.title("Overview of Banned Books in the US (2021-2024)")

    st.info('Data is pulled from https://pen.org/', icon="ℹ️")

    cols1= st.columns([0.3, 0.7], vertical_alignment="center")

    unique_titles = data["Title"].nunique()
    cols1[0].markdown(f"<p style='text-align: center; font-size: 2.5rem; font-weight: bold;'>{unique_titles}</p>", unsafe_allow_html=True)
    cols1[0].markdown(f"<p style='text-align: center;'>books are banned between 2021 and 2024</p>", unsafe_allow_html=True)

    unique_states = data["State"].nunique()
    cols1[0].markdown(f"<p style='text-align: center; font-size: 2.1rem; font-weight: bold;'>{unique_states}</p>", unsafe_allow_html=True)
    cols1[0].markdown(f"<p style='text-align: center;'>states are involved</p>", unsafe_allow_html=True)

    unique_districts = data["District"].nunique()
    cols1[0].markdown(f"<p style='text-align: center; font-size: 2.1rem; font-weight: bold;'>{unique_districts}</p>", unsafe_allow_html=True)
    cols1[0].markdown(f"<p style='text-align: center;'>districts are involved</p>", unsafe_allow_html=True)
    
    with cols1[1]:
      by_year_bar_chart(data)

    by_origin_of_challenge_bar_chart(data)

    cols2= st.columns([0.5, 0.5], vertical_alignment="center")

    with cols2[0]:
      st.subheader("Top 5 Most Banned Titles")
      top_titles = top_5_banned_titles(data)
      ranked_titles = rank_dataframe(top_titles, rank_column_name="Rank")
      st.dataframe(ranked_titles.set_index("Rank"))

    with cols2[1]:
      st.subheader("Top 5 Most Banned Authors")
      top_authors = top_5_banned_authors(data)
      ranked_authors = rank_dataframe(top_authors, rank_column_name="Rank")
      st.dataframe(ranked_authors.set_index("Rank"))

    cols3= st.columns([0.5, 0.5], vertical_alignment="center")

    with cols3[0]:
      st.subheader("Top 5 Most Challenged Titles")
      top_challenged_titles = top_5_challenged_titles(data)
      ranked_challenged_titles = rank_dataframe(top_challenged_titles, rank_column_name="Rank")
      st.dataframe(ranked_challenged_titles.set_index("Rank"))

    with cols3[1]:
      st.subheader("Top 5 Most Challenged Authors")
      top_challenged_authors = top_5_challenged_authors(data)
      ranked_challenged_authors = rank_dataframe(top_challenged_authors, rank_column_name="Rank")
      st.dataframe(ranked_challenged_authors.set_index("Rank"))

display_data(load_data(DATA_URL))



