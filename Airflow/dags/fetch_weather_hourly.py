from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from google.cloud import bigquery

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def get_date_range_from_data(**context):
    """
    Get the min and max dates from existing business data
    """
    client = bigquery.Client()

    query = """
        SELECT 
            MIN(date_value) as min_date,
            MAX(date_value) as max_date
        FROM (
            SELECT DATE(placed_at) as date_value FROM `case-study-479614.raw.orders`
            UNION ALL
            SELECT PARSE_DATE('%Y-%m-%d', date) as date_value FROM `case-study-479614.raw.orders_daily`
            UNION ALL
            SELECT PARSE_DATE('%Y-%m-%d', date) as date_value FROM `case-study-479614.raw.rank`
            UNION ALL
            SELECT PARSE_DATE('%Y-%m-%d', date) as date_value FROM `case-study-479614.raw.ratings_agg`
        )
        """

    df = client.query(query).to_dataframe()
    min_date = df['min_date'].iloc[0]
    max_date = df['max_date'].iloc[0]

    context['ti'].xcom_push(key='min_date', value=min_date.strftime('%Y-%m-%d'))
    context['ti'].xcom_push(key='max_date', value=max_date.strftime('%Y-%m-%d'))

    print(f"Data range: {min_date} to {max_date}")
    return f"{min_date} to {max_date}"


def fetch_outlets_from_bigquery(**context):
    """
    Fetch outlet coordinates from BigQuery
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

    context['ti'].xcom_push(key='outlets', value=outlets)
    print(f"Fetched {len(outlets)} outlets from BigQuery")
    return len(outlets)


def fetch_historical_weather_data(**context):
    """
    Fetch hourly weather data for the full historical date range
    """
    outlets = context['ti'].xcom_pull(key='outlets', task_ids='fetch_outlets')
    min_date = context['ti'].xcom_pull(key='min_date', task_ids='get_date_range')
    max_date = context['ti'].xcom_pull(key='max_date', task_ids='get_date_range')

    print(f"Fetching weather data from {min_date} to {max_date}")

    all_weather_data = []

    for outlet in outlets:
        latitude = outlet['latitude']
        longitude = outlet['longitude']
        outlet_id = outlet['outlet_id']

        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': min_date,
            'end_date': max_date,
            'hourly': 'temperature_2m,relative_humidity_2m,wind_speed_10m',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

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

    df = pd.DataFrame(all_weather_data)

    df['outlet_id'] = df['outlet_id'].astype('Int64')
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['temperature_2m'] = df['temperature_2m'].astype('float64')
    df['relative_humidity_2m'] = df['relative_humidity_2m'].astype('float64')
    df['wind_speed_10m'] = df['wind_speed_10m'].astype('float64')

    gcs_path = f'gs://us-central1-casestudy1-68940b42-bucket/weather_data/historical/weather_{min_date}_{max_date}.parquet'
    df.to_parquet(
        gcs_path,
        index=False,
        engine='pyarrow',
        coerce_timestamps='ms',
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

    load_job = client.load_table_from_uri(gcs_path, table_id, job_config=job_config)
    load_job.result()

    print(f"Loaded {record_count} rows into {table_id}")
    return record_count


with DAG(
        'fetch_weather_historical_backfill',
        default_args=default_args,
        description='One-time backfill of historical weather data',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['weather', 'backfill', 'bigquery'],
) as dag:
    get_dates = PythonOperator(
        task_id='get_date_range',
        python_callable=get_date_range_from_data,
    )

    fetch_outlets = PythonOperator(
        task_id='fetch_outlets',
        python_callable=fetch_outlets_from_bigquery,
    )

    fetch_weather = PythonOperator(
        task_id='fetch_weather_api',
        python_callable=fetch_historical_weather_data,
        execution_timeout=timedelta(minutes=30),
    )

    load_weather = PythonOperator(
        task_id='load_weather_to_bigquery',
        python_callable=load_weather_to_bigquery,
    )

    get_dates >> fetch_outlets >> fetch_weather >> load_weather