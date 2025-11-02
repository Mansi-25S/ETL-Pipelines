# Project 1: Airflow Pipeline (GCS → Airflow → BigQuery)

### Objective  
Built a simple data pipeline using Airflow to load retail sales CSV data from Google Cloud Storage (GCS) into BigQuery automatically.

### Tools Used  
- Google Cloud Storage  
- BigQuery  
- Apache Airflow  
- Python  

### Steps  
1. Uploaded CSV file to GCS.  
2. Wrote `loaddatafromgcstobigquery.py` to load data from GCS into BigQuery.  
3. Created `dag.py` to automate the process through Airflow.  
4. Placed both files in the `/dags` folder so Airflow can detect and run them.  
5. Verified successful data load in BigQuery.

### Outcome  
Automated CSV data loading from GCS to BigQuery using Airflow scheduling.
