# Zepto Retail Data Pipeline Project

This project is a beginner-level data pipeline that processes and analyses retail data from Zepto, a fast 10-minute grocery delivery service.

##  Objective

To build a simple ETL (Extract, Transform, Load) pipeline using Python, BigQuery and Airflow that:
- Cleans and transforms the dataset
- Performs basic analysis
- Generates outputs
- Automates the workflow using Airflow

## Project Structure

zepto_pipeline_project/
├── data/ # Raw and processed data
├── dags/ # Airflow DAGs
├── notebooks/ # Jupyter notebooks for analysis
├── output/ # Final output reports/graphs
├── scripts/ # Python scripts for data processing
├── README.md # Project documentation
└── requirements.txt # Python dependencies


## Tools Used

- Python (Pandas, Numpy, etc.)
- Jupyter Notebook
- Apache Airflow
- Google BigQuery

##  Workflow Steps

1. Load Zepto dataset (Excel)
2. Clean and transform data
3. Analyse key metrics (e.g. discounts, stock-outs)
4. Schedule and run via Airflow DAG

##  Installation

```bash
pip install -r requirements.txt