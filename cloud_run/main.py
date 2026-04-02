import functions_framework
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from datetime import datetime, UTC
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

AIRFLOW_BASE_URL = "https://verbose-clearly-anabelle.ngrok-free.dev"
TABLE_ID         = "sales-datawarehouse.raw_data.sales"

@functions_framework.cloud_event
def extract_and_load(cloud_event):
    data     = cloud_event.data
    filename = data["name"]
    bucket   = data["bucket"]

    if not filename.endswith((".xlsx", ".xls")):
        return

    # Extract
    blob = storage.Client().bucket(bucket).blob(filename)
    df   = pd.read_excel(BytesIO(blob.download_as_bytes()))

    # Clean
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[ /]+', '_', regex=True)
        .str.replace(r'[^0-9a-zA-Z_]', '', regex=True)
    )

    BATCH_ID = datetime.now(UTC).strftime("batch_%Y_%m")
    df['source_file_name'] = Path(filename).name
    df['batch_id']         = BATCH_ID
    df['loaded_at']        = datetime.now(UTC)

    # Dedup check
    client = bigquery.Client()
    try:
        result = client.query(f"""
            SELECT COUNT(1) as count FROM {TABLE_ID}
            WHERE batch_id = '{BATCH_ID}'
        """).result()
        if list(result)[0].count > 0:
            print(f"Batch {BATCH_ID} already loaded. Skipping.")
            _trigger_airflow()
            return
    except NotFound:
        print("Table not found — will create on first load.")

    # Load
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("order_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("customer_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("customer_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country_region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("salesperson", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shipped_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("shipper_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ship_country_region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payment_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("unit_price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("quantity", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("shipping_fee", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("revenue_bins", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("source_file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"Loaded {client.get_table(TABLE_ID).num_rows} rows to {TABLE_ID}")

    # Trigger Airflow dbt DAG
    _trigger_airflow()


def _get_airflow_token():
    resp = requests.post(
        f"{AIRFLOW_BASE_URL}/auth/token",
        json={"username": "admin", "password": "admin"},
    )
    return resp.json()["access_token"]


def _trigger_airflow():
    token = _get_airflow_token()
    requests.post(
        f"{AIRFLOW_BASE_URL}/api/v2/dags/transform/dagRuns",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json={
            "logical_date": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "conf": {},
        },
    ).raise_for_status()