# ETL-Pipelines
(GCS-->Airflow-->Bigquery)
Here created a pipeline for practice where use retail sale data 
steps:
1:uploaded csv file in GCS
2:write loaddatafromgcstobigquery.py file which contain code for getting csv file from gcs and load into bigquery.
3:dag.py which automate this task i.e. here written code to run loaddatafromgcstobigquery.py file which is store in webfile under /dag folder
4:check data is loaded successfully in bigquery or not.
