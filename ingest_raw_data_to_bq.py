import os
import pandas as pd
from google.cloud import bigquery

# -------------------------
# CONFIG
# -------------------------
RAW_DATA_PATH = "Data/"
PROJECT_ID = "case-study-479614"
DATASET_ID = "raw"

TABLES = [
    "listing",
    "orders",
    "orders_daily",
    "org",
    "outlet",
    "platform",
    "rank",
    "ratings_agg"
]

# -------------------------
# AUTH
# -------------------------
KEY_PATH = os.getenv("GCP_SA_KEY")  # env var pointing to JSON key
if not KEY_PATH:
    raise EnvironmentError("Environment variable GCP_SA_KEY is not set")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# -------------------------
# BIGQUERY CLIENT
# -------------------------
client = bigquery.Client(project=PROJECT_ID)

# -------------------------
# Ensure dataset exists
# -------------------------
def ensure_dataset(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset {dataset_id} exists")
    except Exception:
        print(f"📦 Dataset {dataset_id} not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # or "EU"
        client.create_dataset(dataset)
        print(f"✅ Dataset {dataset_id} created")

# -------------------------
# Load CSV
# -------------------------
def load_csv_to_bigquery(table_name):
    file_path = os.path.join(RAW_DATA_PATH, f"{table_name}.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    print(f"📥 Loading {file_path} into BigQuery...")

    df = pd.read_csv(file_path)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    print(f"✅ Table {table_id} loaded ({df.shape[0]} rows)")

# -------------------------
# Main
# -------------------------
def main():
    ensure_dataset(DATASET_ID)
    for table in TABLES:
        load_csv_to_bigquery(table)

if __name__ == "__main__":
    main()
