from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta, timezone
import requests
import os
import pandas as pd
from google.cloud import storage
from pathlib import Path



API_KEY = Variable.get("weather-api-key")
GCS_BUCKET = Variable.get("gcp_bucket")
PROJECT_ID = Variable.get("bq_datawarehouse_project")

BQ_DATASET = "weather"
BQ_STAGING_DATASET = BQ_DATASET
TABLE_NAME = 'daily_data'
BASE_DIR = Path(__file__).parent
SQL_PATH = BASE_DIR / "sql"
query = open(SQL_PATH / "upsert_table.sql").read()



LAT = 40.7128  # Example: New York City latitude
LON = -74.0060  # Example: New York City longitude


# Fix 1: changed : to =
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def date_to_unix_timestamp(date):
    if date is None:
        # Get the current date
        date = datetime.now().date()

    # Convert to a datetime object with time set to midnight
    date_converted = datetime.combine(date, datetime.min.time())

    # Convert to Unix timestamp (UTC time zone)
    unix_timestamp = int(date_converted.replace(tzinfo=timezone.utc).timestamp())

    return unix_timestamp, date


def fetch_weather_data(**context):
    API_KEY = Variable.get("weather-api-key")
    GCS_BUCKET = Variable.get("gcp_bucket")

    backfill_date = context.get('execution_date', None)
    unix_timestamp, date = date_to_unix_timestamp(backfill_date)

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"

    response = requests.get(url)
    json_data = response.json()
    print(json_data)  # keep for debugging

    # Flatten the nested structure manually
    flat_data = {
        "dt":         json_data.get("dt"),
        "sunrise":    json_data.get("sys", {}).get("sunrise"),
        "sunset":     json_data.get("sys", {}).get("sunset"),
        "temp":       json_data.get("main", {}).get("temp"),
        "feels_like": json_data.get("main", {}).get("feels_like"),
        "pressure":   json_data.get("main", {}).get("pressure"),
        "humidity":   json_data.get("main", {}).get("humidity"),
        "dew_point": float('nan'),  # forces FLOAT type in Parquet instead of INT32
        "clouds":     json_data.get("clouds", {}).get("all"),
        "visibility": json_data.get("visibility"),
        "wind_speed": json_data.get("wind", {}).get("speed"),
        "wind_deg":   json_data.get("wind", {}).get("deg"),
        "weather":    json_data.get("weather", [{}])[0].get("description"),
        "datetime":   date
    }

    df = pd.DataFrame([flat_data])  # wrap in list — single row

    filename = f"weather_data_{date}.parquet"
    context['ti'].xcom_push(key='filename', value=filename)

    from io import BytesIO
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=filename,
        data=buffer.getvalue()
    )

# Fix 3: changed DAG { to DAG(, and schedule_interval to schedule
dag = DAG(
    'weather_data_ingestion',
    default_args=default_args,
    description='Fetch weather data and store in bigquery',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dataset_id=BQ_DATASET,
    project_id=PROJECT_ID,
    location="US",
    exists_ok=True,
    dag=dag,
)

gcs_to_bq_staging_task = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=["{{ task_instance.xcom_pull(task_ids='fetch_weather_data', key='filename') }}"],
    destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.stg_{TABLE_NAME}',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    # time_partitioning removed — staging table doesn't need partitioning
    gcp_conn_id="google_cloud_default",
    source_format='PARQUET',
    schema_fields=[
        {"name": "dt",         "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "sunrise",    "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "sunset",     "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "temp",       "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "feels_like", "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "pressure",   "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "humidity",   "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "dew_point",  "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "clouds",     "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "visibility", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "wind_speed", "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "wind_deg",   "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "weather",    "type": "STRING",  "mode": "NULLABLE"},
        {"name": "datetime",   "type": "DATE",    "mode": "NULLABLE"},
    ],
    dag=dag,
)

create_staging_table = BigQueryInsertJobOperator(
    task_id="create_staging_table",
    location="asia-southeast1",
    configuration={
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BQ_DATASET}.stg_{TABLE_NAME}`
                (
                    dt INTEGER,
                    sunrise INTEGER,
                    sunset INTEGER,
                    temp FLOAT64,
                    feels_like FLOAT64,
                    pressure INTEGER,
                    humidity INTEGER,
                    dew_point FLOAT64,
                    clouds INTEGER,
                    visibility INTEGER,
                    wind_speed FLOAT64,
                    wind_deg INTEGER,
                    weather STRING,
                    datetime DATE
                )
            """,
            "useLegacySql": False
        }
    },
    project_id=PROJECT_ID,
    dag=dag,  # ← this was missing before too
)


# ← ADD THIS NEW TASK RIGHT AFTER
create_prod_table = BigQueryInsertJobOperator(
    task_id="create_prod_table",
    location="asia-southeast1",
    configuration={
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}`
                (
                    dt INTEGER,
                    sunrise INTEGER,
                    sunset INTEGER,
                    temp FLOAT64,
                    feels_like FLOAT64,
                    pressure INTEGER,
                    humidity INTEGER,
                    dew_point FLOAT64,
                    clouds INTEGER,
                    visibility INTEGER,
                    wind_speed FLOAT64,
                    wind_deg INTEGER,
                    weather STRING,
                    datetime DATE
                )
                PARTITION BY datetime
            """,
            "useLegacySql": False
        }
    },
    project_id=PROJECT_ID,
    dag=dag,
)

stg_to_prod_task = BigQueryInsertJobOperator(
    task_id="upsert_staging_to_prod_task",
    project_id=PROJECT_ID,
    location="asia-southeast1",
    configuration={
        "query": {
            "query": open(SQL_PATH / "upsert_table.sql", "r").read()
                .replace('{project_id}', PROJECT_ID)
                .replace('{bq_dataset}', BQ_DATASET)
                .replace('{table_name}', TABLE_NAME),
            "useLegacySql": False
        },
        "createDisposition": "CREATE_IF_NEEDED",
        "destinationTable": {
            "project_id": PROJECT_ID,
            "dataset_id": BQ_DATASET,
            "table_id": TABLE_NAME
        }
    },
    dag=dag
)




# Fix 4: fixed typo feth_ to fetch_ and gcp_ to gcs_
fetch_weather_data_task >> create_dataset >> create_staging_table >> gcs_to_bq_staging_task >> stg_to_prod_task