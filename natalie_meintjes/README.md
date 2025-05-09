#### Natalie's Capstone

# COVID-19 Global Data Engineering Project

## Overview

This project explores global COVID-19 trends using open data from [Our World in Data](https://docs.owid.io/projects/covid/en/latest/dataset.html). I attempted to implement basic data engineering principles including data ingestion, cleaning, transformation, and visualization. This was my first time using Python and Jupyter notebook for a data analysis project so this was a big challenge for me. In the past, in the epidemiology work I have done, I have used STATA for statistical analysis so I appreciated the opportunity to learn new skills.

## Problem Statement

COVID-19 has affected countries differently over time. My goal was to analyze and visualize how cases, deaths, vaccinations, and testing evolved in selected countries.

## Data Source

- **Dataset**: [OWID COVID-19 Data](https://covid.ourworldindata.org/data/owid-covid-data.csv)
- **Date Range**: Jan 2020 – April 2025
- **Features**: Cases, deaths, hospitalizations, vaccinations, testing, stringency index, etc.
- **Granularity**: Country-level (and regional aggregates (e.g., "Africa", "World"))

## Project Structure

*Before make*

natalie_meintjes/
├── Makefile
├── README.md
├── dags
│   └── covid_analysis_dag.py
├── data
│   └── owid_covid_data.csv
├── notebooks
│   └── covid_analysis.ipynb
├── output
└── scripts
    ├── clean_data.py
    └── run_notebook.py

*After make*
natalie_meintjes/
├── Makefile
├── README.md
├── dags
│   └── covid_analysis_dag.py
├── data
│   ├── cleaned_covid.csv
│   └── owid_covid_data.csv
├── notebooks
│   └── covid_analysis.ipynb
└── scripts
    ├── clean_data.py
    └── run_notebook.py


## Tools

- Python (Pandas, Matplotlib, Seaborn, Sklearn, Numpy)
- Jupyter Notebooks
- Airflow

## Analyses

- Trends in new cases and deaths across regions
- Vaccination progress over time
- Comparison of testing rates by GDP
- Relationship between stringency and reproduction rate

## How to Run

First please download the data from here:
https://github.com/natalie-mein/DataTribe_CapstoneProject 

Download the file: owid-covid-data.csv 

This file can also be downloaded directly from the source:
https://docs.owid.io/projects/covid/en/latest/dataset.html

### option 1

1. Clone the repository
2. Place the raw dataset in `data/owid-covid-data.csv`
3. Run the script:
   ```bash
   python scripts/clean_data.py```
4. Explore the notebook in notebooks/

### option 2

1. Clone the repository
2. Use make:
   ```bash
   make run```
3. Explore the notebook in notebooks/ in the IDE of your choice
