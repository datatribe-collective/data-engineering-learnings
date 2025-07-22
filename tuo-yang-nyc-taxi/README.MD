# NYC Yellow Taxi Data Pipeline 🚕📊

This project demonstrates a complete data pipeline built for NYC Yellow Taxi trip data.  
It covers real-world ELT steps: downloading raw data, cleaning it with PySpark, uploading to BigQuery, and visualizing trends via Streamlit.

Everything runs locally in Docker, but cloud operations (like storage and querying) are delegated to GCP services like BigQuery.  
The pipeline is also fully orchestrated using Airflow, with container builds automated through GitHub Actions.

---

### 🔧 Tech Stack

- **Docker** – Isolates each service (Airflow, Streamlit, etc.) for portability and reproducibility  
- **PySpark** – Cleans and transforms Parquet datasets at scale  
- **Google BigQuery** – Cloud-based warehouse for analytics-ready storage  
- **Airflow** – Automates the pipeline via DAG scheduling  
- **Streamlit** – Provides a simple dashboard to visualize daily and hourly metrics  
- **Pandas & SQLAlchemy** – Used for lightweight querying and quick data previews  
  *(SQLAlchemy acts as a connector layer, making it easier to write to BigQuery from Python code.)*

### 🗂️ System Architecture

The pipeline follows a modular design that separates raw data ingestion, cleaning, cloud storage, orchestration, and frontend rendering.

1. **Download Raw Parquet Data**  
   A Python script pulls NYC Yellow Taxi trip records from the TLC S3 repository.  
   The download is customizable via environment variables for year/month selection.

2. **Clean & Process Locally with PySpark**  
   Data is filtered and transformed locally using PySpark, including dropping invalid records, computing trip duration, and extracting hourly summaries.

3. **Upload to BigQuery**  
   The cleaned dataset is written to a BigQuery table using the `google-cloud-bigquery` Python client.

4. **Orchestration with Airflow**  
   A DAG defines the pipeline steps (from download to upload).  
   Each task runs inside containers defined via Docker Compose.

5. **Streamlit Dashboard**  
   A lightweight dashboard visualizes key metrics:  
   - Average fare and tip rate  
   - Trip volume by hour  
   - Distance patterns  
   - Passenger counts

6. **(Optional) GitHub Actions CI/CD**  
   Docker images for Airflow and Streamlit are automatically built and pushed to Docker Hub upon each commit to `main` using GitHub Actions.  
   This makes redeployment easier across environments.

> ⚙️ A diagram illustrating this system will be added later to this section.

### 🧪 Local Setup

This project was developed and tested on **Windows 11 using WSL2 (Ubuntu)**, which provides a Linux-compatible environment inside Windows.  
If you're using **macOS** or **Linux**, you can skip WSL and run everything directly in your terminal.

All components—Airflow, PySpark, and Streamlit—are containerized using Docker Compose, so you don’t need to install them manually.

---

#### ✅ Prerequisites

Make sure the following are installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)  
- [Python 3.10+](https://www.python.org/) (only if running any standalone scripts)
- [Google Cloud SDK](https://cloud.google.com/sdk) (`gcloud` CLI)
- A Google Cloud project with BigQuery enabled
- A BigQuery dataset (create manually or let the script handle it)

---

#### 🛠️ Setup Steps

1. **Clone the repository**

   ```bash
   git clone https://github.com/your-username/nyc-taxi-pipeline.git
   cd nyc-taxi-pipeline
   ```

2. **Copy and configure environment files**

   Create your own `.env` file:
   ```bash
   cp .env.example .env
   ```

   Then set your local user ID (optional, improves file permissions in WSL/Linux):
   ```bash
   export AIRFLOW_UID=$(id -u)
   ```

   Copy your GCP service account key (downloaded from Google Cloud Console):
   ```bash
   cp creds/gcp_example_account.example.json creds/gcp_service_account.json
   ```

3. **Start the application stack**
   Build and run all services with Docker Compose:
   ```bash
   docker compose up --build
   ```

4. **Access the local services**
   - **Airflow UI**: [http://localhost:18080](http://localhost:18080)
     *(username/password: `airflow` / `airflow`)
   - **Streamlit Dashboard**: [http://localhost:8700](http://localhost:8700)


### ⚙️ Environment Configuration

To keep credentials, configurations, and cloud settings organized, this project uses both a .env file and a YAML-based config file (`settings.yaml`).

This separation allows environment-specific settings to be reused across Airflow, PySpark, and custom scripts.

#### 🔐 `.env` File (For Secrets and Runtime Variables)

This file contains sensitive information and environment runtime variables.
You can start by copying the example file:

```bash
cp .env.example .env
```

Then edit `.env` to fill in your own values:

```bash
GCP_PROJECT=your-project-id
BIGQUERY_DATASET=your_dataset
BIGQUERY_TABLE=your_table
BIGQUERY_SUMMARY_TABLE=your_summary_table
GCP_SERVICE_ACCOUNT_KEY_PATH=./creds/gcp_service_account.json
```

The `.env` file is loaded automatically by Airflow, the BigQuery upload script, and the Streamlit dashboard.

#### `settings.yaml` (Pipeline Parameters and Spark Config)

The `settings.yaml` file stores pipeline-specific parameters in a structured and readable format.

Example:
```bash
data_config:
  year: 2023
  months: [1, 2, 3]
  output_prefix: "yellow_tripdata"

spark_config:
  app_name: "YellowTaxiCleaning"
  master: "local[*]"

bigquery_config:
  project_id: ${BQ_PROJECT_ID}
  dataset: ${BQ_DATASET}
  table: ${BQ_TABLE}
  summary_table: ${BQ_SUMMARY_TABLE}
```

This file allows flexible control over:
   - The year and months of data to download
   - Spark cluster settings (e.g., `local[*]`)
   - Target BigQuery table names

It’s especially useful when triggering Airflow DAGs with dynamic inputs.

#### 🐳 Docker Image

This project includes a prebuilt Docker image hosted on Docker Hub.
To use it directly without building locally, follow the steps below.

📦 **Pull the images manually (optional)**
```bash
docker pull bravo634/nyc-taxi-airflow:latest
docker pull bravo634/nyc-taxi-app:latest
```
This is optional but recommended if you want to avoid building the image locally when running docker-compose up for the first time.

🛠️ Configure your environment
Make sure your .env or .env.example file contains:
```bash
DOCKER_IMAGE_PREFIX=bravo634
```
This will ensure all services (Airflow, Streamlit, etc.) use my public Docker images.

### 📁 Project Structure

The repository is organized to reflect a modular and production-like data engineering pipeline. Here's a quick overview of the main components:

```text
📁 Project Structure

.github/workflows/               # CI setup for GitHub Actions (optional)
├── docker.yml                   # Auto-builds Docker image on push

.streamlit/                      # Streamlit UI config
├── config.toml

airflow_pipeline/                # Main Airflow pipeline (ETL logic)
├── config/                      # DAG-level config files
│   ├── airflow.cfg              # Airflow core settings
│   └── settings.yaml            # Pipeline variables (year, month, etc.)
├── dags/                        # Airflow DAG definitions
│   └── nyc_taxi_etl_dag.py      # DAG for loading & transforming taxi data
├── data/                        # Cleaned data outputs
│   ├── processed/               # Final output CSVs
│   └── tmp/                     # Temp files (if needed)
├── logs/                        # Airflow DAG execution logs
├── scripts/                     # Python modules used in DAG
│   ├── get_bigquery_client.py
│   ├── load_nyc_yellow_taxi_data.py
│   ├── transform_trip_data_spark.py
│   ├── upload_to_big_query.py
│   └── sql_utils.py
├── sql/                         # SQL query templates used in DAG
├── .dockerignore                # Exclude unnecessary files from Docker build
├── Dockerfile                   # Dockerfile for Airflow pipeline container
└── requirements.txt             # Python dependencies for Airflow

creds/                           # GCP credentials (local only, not committed)
├── gcp_service_account.example.json

notebooks/                       # Jupyter notebooks for local exploration
├── 01_explore_raw_data.ipynb
├── 02_transform_trip_summary.ipynb
├── 03_visualize_summary.ipynb
└── 04_analyze_trip_summary.ipynb

streamlit_app/                   # Streamlit frontend app
├── pages/                       # Dashboard pages
│   ├── Trend_Viewer.py
│   └── Zone_Heatmap.py
├── utils/                       # Shared utility functions for dashboard
│   ├── config.py
│   ├── get_bigquery_client.py
│   └── sql_utils.py
├── config.yaml                  # App-level settings (e.g., query range)
├── app.py                       # Streamlit entry point
├── .dockerignore                # Ignore build-time clutter (e.g. .ipynb_checkpoints)
├── Dockerfile                   # Dockerfile for Streamlit deployment
└── requirements.txt             # Python dependencies for dashboard

.env.example                     # Template for setting env vars
.gitignore                       # Ignore config, credentials, etc. in Git
docker-compose.yaml              # Main service configuration
docker-compose.override.yaml     # Optional local override
Makefile                         # CLI shortcut commands (optional)
pyproject.toml                   # Python project metadata
README.md                        # This documentation
```

### 📋 Future Improvements

This project was designed as a lightweight, end-to-end data engineering demo. However, several areas could be further enhanced:

- **Incremental Loads**  
  Currently, the pipeline performs full refreshes. Future versions could use timestamps or partitioning to only load new or updated records.

- **Data Validation**  
  Integrating a library like [Great Expectations](https://greatexpectations.io/) or using SQL tests would help validate raw and cleaned data for quality assurance.

- **Workflow Modularity**  
  The current Airflow DAG uses Python functions for each step. Refactoring with task groups or external task triggers would improve scalability.

- **Enhanced Visualizations**  
  The Streamlit dashboard can be expanded with filters, comparison tools, or animated charts (e.g., via `Plotly` or `Altair`).

- **Cloud Deployment**  
  Although the pipeline was tested locally, deploying Airflow via **Cloud Composer**, and hosting the dashboard on **Streamlit Cloud** or **GCP App Engine** would make it production-ready.

- **PySpark Optimization**  
  PySpark logic could be tuned to support multi-node processing, and caching could be introduced to avoid redundant computations.

---

### 🧠 Lessons Learned

This project started as a simple data upload task, but it gradually grew into a full-stack data pipeline. Over the course of building it, I learned to:

- Handle multiple moving parts in a data engineering stack (PySpark, Airflow, BigQuery, Streamlit)
- Use Docker and `.env` files to isolate environments
- Design modular code for orchestration and transformation
- Translate Jupyter-based logic into robust pipeline steps
- Troubleshoot real-world bugs like permission issues, timezone mismatches, and schema mismatches

The biggest takeaway: **engineering a data pipeline is not just about code—it’s about orchestration, visibility, and maintainability.**

---

### 🙌 Acknowledgements

Special thanks to:

- NYC TLC for making the yellow taxi dataset publicly available  
- Google Cloud’s generous free tier (BigQuery & Storage)  
- Airflow & Streamlit communities for excellent documentation  
- [DataTribe](https://github.com/datatribe-collective-labs/data-engineering-learnings/tree/main) for inspiring the capstone structure and workflow

---

### 📮 Contact

Feel free to connect or share feedback:

- **GitHub**: [tuo-yang](https://github.com/TuoYang263)  
- **LinkedIn**: [tuo-yang-linkedin-profile](https://linkedin.com/in/your-profile)  
- **Email**: yangtuomailbox@gmail.com
