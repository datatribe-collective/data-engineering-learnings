####         Jay's Capstone

# Zepto Retail Data Pipeline Project

This project automates the end-to-end data pipeline for Zepto’s inventory data using Python scripts, Jupyter notebooks, and Apache Airflow. The pipeline includes data cleaning, analysis, and visualisation preparation, with orchestration handled by Airflow for automation and scheduling.

##  Objective

To build a simple ETL (Extract, Transform, Load) pipeline using Python, BigQuery and Airflow that:
- Cleans and transforms the dataset
- Performs basic analysis
- Generates outputs
- Automates the workflow using Airflow

## Project Structure
```
zepto_pipeline_project/
├── dags/
│ └── zepto_pipeline_dag.py
├── notebooks/
│ ├── zepto_inventory.ipynb
│ └── zepto_inventory_output.ipynb
├── scripts/
│ ├── clean_data.py
│ └── run_notebook.py
├── data/
│ └── zepto_data_cleaned.csv
├── Makefile
├── README.md 

## Tools Used

- Python (Pandas, Matplotlib, Seaborn, Sklearn, Numpy)
- Jupyter Notebooks
- Airflow

##  Analysis About

- Discount 
- Out of stock Items
- Available Stock Items

##  Installation

```bash


