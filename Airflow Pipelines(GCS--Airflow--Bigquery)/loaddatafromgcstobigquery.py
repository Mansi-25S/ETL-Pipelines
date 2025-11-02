from google.cloud import bigquery, storage
import pandas as pd
from io import BytesIO

# -----------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------

project_id = "project-10d530ba-598e-48aa-8d2"
bucket_name = "kelvindata"
file_path = "retail_sales_jan.csv"
dataset_id = "reatildata"
table_id = "retail_sales"

# -----------------------------------------------------
# CLIENT SETUP
# -----------------------------------------------------
storage_client = storage.Client(project=project_id)
bq_client = bigquery.Client(project=project_id)

# -----------------------------------------------------
# READ CSV FROM GCS
# -----------------------------------------------------
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_path)
data_bytes = blob.download_as_bytes()
df = pd.read_csv(BytesIO(data_bytes))

# -----------------------------------------------------
# FORMAT DATA
# -----------------------------------------------------
# Convert string date to proper DATE format
df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

# Handle missing or inconsistent data (optional)
df.fillna("", inplace=True)

# -----------------------------------------------------
# DEFINE BIGQUERY SCHEMA


# -----------------------------------------------------
schema = [
    bigquery.SchemaField("TransactionID", "INTEGER"),
    bigquery.SchemaField("Date", "DATE"),
    bigquery.SchemaField("StoreID", "INTEGER"),
    bigquery.SchemaField("ProductID", "INTEGER"),
    bigquery.SchemaField("ProductCategory", "STRING"),
    bigquery.SchemaField("QuantitySold", "INTEGER"),
    bigquery.SchemaField("UnitPrice", "FLOAT"),
    bigquery.SchemaField("PaymentMethod", "STRING"),
    bigquery.SchemaField("CustomerType", "STRING"),
     bigquery.SchemaField("City", "STRING"),
    bigquery.SchemaField("TotalAmount", "FLOAT")
]

# -----------------------------------------------------
# CREATE TABLE IF NOT EXISTS
# -----------------------------------------------------
table_ref = bq_client.dataset(dataset_id).table(table_id)
try:
    bq_client.get_table(table_ref)
    print("✅ Table exists.")
except:
    table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table)
    print("📋 Table created:", table_id)

# -----------------------------------------------------
# LOAD DATA INTO BIGQUERY
# -----------------------------------------------------
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",  # or "WRITE_TRUNCATE" to replace data
)

job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()

print(f"🚀 Successfully loaded {job.output_rows} rows into {dataset_id}.{table_id}")
