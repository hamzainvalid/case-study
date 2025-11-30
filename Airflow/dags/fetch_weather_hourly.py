# dags/fetch_weather_data.py
"""
Airflow DAG to fetch weather data from Open-Meteo API
and load it into BigQuery raw.weather table
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import requests
import pandas as pd
from typing import List, Dict
from google.cloud import bigquery

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_outlets_from_bigquery(**context):
    """
    Fetch outlet coordinates from BigQuery
    Returns list of dicts with outlet_id, latitude, longitude
    """
    client = bigquery.Client()

    query = """
          SELECT id as outlet_id, latitude, longitude 
          FROM `case-study-479614.raw.outlet`
          WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
      """

    df = client.query(query).to_dataframe()
    outlets = df.to_dict('records')

    # Push to XCom for next task
    context['ti'].xcom_push(key='outlets', value=outlets)
    print(f"Fetched {len(outlets)} outlets from BigQuery")
    return len(outlets)


def fetch_weather_data_from_api(**context):
    """
    Fetch hourly weather data from Open-Meteo API
    """
    outlets = context['ti'].xcom_pull(key='outlets', task_ids='fetch_outlets')
    execution_date = context['execution_date']

    # Fetch for the previous day
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    all_weather_data = []

    for outlet in outlets:
        latitude = outlet['latitude']
        longitude = outlet['longitude']
        outlet_id = outlet['outlet_id']

        # Open-Meteo API endpoint
        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': date_str,
            'end_date': date_str,
            'hourly': 'temperature_2m,relative_humidity_2m,wind_speed_10m',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Parse hourly data
            if 'hourly' in data:
                hourly = data['hourly']
                timestamps = hourly.get('time', [])
                temperatures = hourly.get('temperature_2m', [])
                humidities = hourly.get('relative_humidity_2m', [])
                wind_speeds = hourly.get('wind_speed_10m', [])

                for i in range(len(timestamps)):
                    all_weather_data.append({
                        'outlet_id': outlet_id,
                        'timestamp': timestamps[i],
                        'temperature_2m': temperatures[i],
                        'relative_humidity_2m': humidities[i],
                        'wind_speed_10m': wind_speeds[i]
                    })

        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather for outlet {outlet_id}: {e}")
            continue

    # Convert to DataFrame with explicit types
    df = pd.DataFrame(all_weather_data)

    # Ensure correct data types to match BigQuery schema
    df['outlet_id'] = df['outlet_id'].astype('Int64')
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['temperature_2m'] = df['temperature_2m'].astype('float64')
    df['relative_humidity_2m'] = df['relative_humidity_2m'].astype('float64')
    df['wind_speed_10m'] = df['wind_speed_10m'].astype('float64')

    # Save to temporary GCS location for BigQuery load
    # Use engine='pyarrow' and specify timestamp resolution
    gcs_path = f'gs://us-central1-casestudy1-68940b42-bucket/weather_data/{date_str}/weather.parquet'
    df.to_parquet(
        gcs_path,
        index=False,
        engine='pyarrow',
        coerce_timestamps='ms',  # Use milliseconds instead of nanoseconds
        allow_truncated_timestamps=True
    )

    context['ti'].xcom_push(key='gcs_path', value=gcs_path)
    context['ti'].xcom_push(key='record_count', value=len(all_weather_data))

    print(f"Fetched {len(all_weather_data)} weather records and saved to {gcs_path}")
    return len(all_weather_data)


def load_weather_to_bigquery(**context):
    """
    Load weather data from GCS to BigQuery
    """
    gcs_path = context['ti'].xcom_pull(key='gcs_path', task_ids='fetch_weather_api')
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='fetch_weather_api')

    if not gcs_path or record_count == 0:
        print("No weather data to load")
        return 0

    client = bigquery.Client()

    table_id = "case-study-479614.raw.weather"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("outlet_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("temperature_2m", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("relative_humidity_2m", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("wind_speed_10m", "FLOAT", mode="NULLABLE"),
        ]
    )

    load_job = client.load_table_from_uri(
        gcs_path,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for job to complete

    print(f"Loaded {record_count} rows into {table_id}")
    return record_count


with DAG(
        'fetch_weather_data',
        default_args=default_args,
        description='Fetch hourly weather data from Open-Meteo API and load to BigQuery',
        schedule_interval='0 2 * * *',  # Run daily at 2 AM UTC
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['weather', 'etl', 'bigquery'],
) as dag:
    fetch_outlets = PythonOperator(
        task_id='fetch_outlets',
        python_callable=fetch_outlets_from_bigquery,
    )

    fetch_weather = PythonOperator(
        task_id='fetch_weather_api',
        python_callable=fetch_weather_data_from_api,
    )

    load_weather = PythonOperator(
        task_id='load_weather_to_bigquery',
        python_callable=load_weather_to_bigquery,
    )

    # Trigger dbt run after loading
    # trigger_dbt = BigQueryInsertJobOperator(
    #     task_id='trigger_dbt_run',
    #     configuration={
    #         "query": {
    #             "query": "SELECT 1",  # Placeholder - replace with actual dbt trigger
    #             "useLegacySql": False,
    #         }
    #     },
    #     gcp_conn_id='google_cloud_default',
    # )

    fetch_outlets >> fetch_weather >> load_weather