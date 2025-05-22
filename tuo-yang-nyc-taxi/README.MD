# John's Capstone: Sales Data Ingestion to BigQuery

This capstone demonstrates how to load local CSV sales data into Google BigQuery using `pandas-gbq`, and optionally visualize it in Looker Studio.

## Tools

- Python (3.10+)
- Pandas
- Google BigQuery
- Looker Studio (optional)
- Airflow (optional, DAG included)

---

## Project Flow

1. Clean sales data using Pandas
2. Upload to BigQuery via `pandas-gbq`
3. Visualize data in Looker Studio

---

## Project Structure
john/
├── scripts/
│   └── process_and_load.py  # Cleans and loads data to BigQuery
├── dags/
│   └── bq_pipeline_dag.py     # Optional Airflow DAG
├── data/
│   └── sales_data.csv         # Sample data
└── notebooks/
    └── data_preview.ipynb     # Pandas EDA

## Setup and Run

1. Install dependencies:
   ```bash
   poetry install
   ```
2. Authenticate to Google Cloud:
   ```bash
   gcloud auth application-default login
   ```
3. Run the script:
   ```bash
   make run
   ```