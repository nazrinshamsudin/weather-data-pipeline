# 🌤️ Automated Weather Data Pipeline

An end-to-end data engineering pipeline that automatically collects daily weather data from the OpenWeather API, stores it in Google Cloud Storage, and loads it into BigQuery for analysis and dashboarding.

---

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Dashboard](#dashboard)

---

## Overview

This project automates the daily collection of weather data for New York City using Apache Airflow as the orchestration tool. Raw data is stored as Parquet files in Google Cloud Storage (data lake), then loaded and merged into a partitioned BigQuery table (data warehouse) for querying and visualization.

Key concepts demonstrated:
- **Idempotency** — re-running the pipeline never creates duplicates
- **Backfilling** — historical data can be loaded for any past date
- **Upsert logic** — MERGE statement ensures clean, duplicate-free production data
- **Partitioning** — BigQuery table partitioned by date for query performance

---

## Architecture

OpenWeather API
↓
Apache Airflow (orchestration)
↓
Google Cloud Storage (data lake - Parquet files)
↓
BigQuery Staging Table (stg_daily_data)
↓
BigQuery Production Table (daily_data - partitioned by date)
↓
Looker Studio Dashboard


---

## Tech Stack

| Tool | Purpose |
|---|---|
| Apache Airflow 3.x | Pipeline orchestration and scheduling |
| Docker & Docker Compose | Running Airflow locally |
| OpenWeather API 2.5 | Weather data source |
| Google Cloud Storage | Raw data lake (Parquet files) |
| BigQuery | Data warehouse |
| Looker Studio | Dashboard and visualization |
| Python 3.11 | DAG logic and data transformation |
| Pandas | Data manipulation |
| Coder | Cloud development environment |

---

## Project Structure

weather-data-pipeline/
├── dags/
│   ├── data_ingestion.py      # Main Airflow DAG
│   └── sql/
│       └── upsert_table.sql   # MERGE SQL for staging → production
├── docker-compose.yaml        # Airflow Docker setup
├── .env                       # Environment variables (not committed)
├── .gitignore
└── README.md

---

## Setup & Installation

### Prerequisites
- Docker Desktop installed and running
- Google Cloud Platform account
- OpenWeather API account (free tier works)
- Python 3.11+

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline
```

### 2. Set up Airflow with Docker
```bash
# Create required folders
mkdir -p logs plugins config

# Set Airflow user ID
echo "AIRFLOW_UID=50000" > .env

# Initialize the database
docker compose up airflow-init

# Start Airflow
docker compose up -d
```
<img width="1203" height="668" alt="image" src="https://github.com/user-attachments/assets/5b714986-b130-43ed-ab70-766d796c016e" />


### 3. Access Airflow UI
Go to `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

---

## Configuration

### Airflow Variables
Set these in Airflow UI under **Admin → Variables**:

| Key | Description |
|---|---|
| `weather-api-key` | Your OpenWeather API key |
| `gcp_bucket` | Your GCS bucket name |
| `bq_datawarehouse_project` | Your GCP project ID |

### Google Cloud Connection
Set up in Airflow UI under **Admin → Connections**:
- **Conn ID:** `google_cloud_default`
- **Conn Type:** `Google Cloud`
- **Keyfile JSON:** Your GCP service account key contents

### GCP Permissions Required
Your service account needs these roles:
- `BigQuery Data Editor`
- `BigQuery Job User`
- `Storage Object Admin`

---

## Running the Pipeline

<img width="1618" height="1036" alt="image" src="https://github.com/user-attachments/assets/e107f4cc-928e-42d6-a3ad-15991f837d78" />


### Manual Trigger
1. Go to Airflow UI at `http://localhost:8080`
2. Find `weather_data_ingestion` DAG
3. Click the **Trigger DAG** button

### Scheduled Run
The DAG runs automatically every day at midnight UTC (`@daily`).

### Backfilling Historical Data
To load data for the past 30 days:
1. In `data_ingestion.py`, set:
```python
start_date=datetime(2026, 4, 14),
catchup=True,
```
2. Save and let Airflow automatically backfill

<img width="1918" height="818" alt="image" src="https://github.com/user-attachments/assets/22e31c6e-0c00-450a-a42f-b546938a6f25" />


---

## DAG Tasks

fetch_weather_data        Calls OpenWeather API, flattens response, uploads Parquet to GCS
↓
create_dataset            Creates BigQuery dataset if not exists
↓
create_staging_table      Creates stg_daily_data table if not exists
↓
create_prod_table         Creates daily_data table with DATE partitioning if not exists
↓
gcs_to_bigquery           Loads Parquet file from GCS into staging table
↓
upsert_staging_to_prod    MERGE staging → production (upsert by datetime)

---

## Dashboard

The production table `daily_data` in BigQuery can be connected to Looker Studio for visualization.

**Suggested charts:**
- Temperature trend over time (line chart)
- Daily humidity levels (area chart)
- Wind speed per day (bar chart)
- Weather conditions breakdown (pie chart)

To connect:
1. Go to `lookerstudio.google.com`
2. Create new report → Add data → BigQuery
3. Select `open-weather-api-airflow → weather → daily_data`

---

## Notes

- `dew_point` is always `null` — this field is not available in the OpenWeather 2.5 API
- The pipeline collects data for **New York City** (lat: 40.7128, lon: -74.0060)
- To change location, update `LAT` and `LON` in `data_ingestion.py`

---

## Author

**Nazrin Shamsudin**  
Data Engineering Project — 2026
