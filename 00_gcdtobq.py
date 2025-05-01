from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# DAG Default arguments
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
}

# DAG definition
with DAG(
    dag_id = 'load_gcs_to_bq',
    default_args = default_args,
    description = 'Load a CSV file from GCS to BigQuery',
    schedule = None,
    start_date = datetime(2024,1,1),
    catchup = False,
    tags=['bigquery','gcs','csv'],
) as dag:
    
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id = 'load_csv_to_bq',
        bucket = 'bkt-health-data', #bucket name
        source_objects = ['global_health_data.csv'], #path to your file in the bucket
        destination_project_dataset_table = 'ace-ensign-453414-f0.staging_dataset.global_data', #project,dataset,and table name
        source_format = 'CSV',
        allow_jagged_rows = True,
        ignore_unknown_values = True,
        write_disposition = 'WRITE_TRUNCATE', #options: WRITE_TRUNCATE,WRITE_APPEND,WRITE_EMPTY
        skip_leading_rows = 1, #skip header row
        field_delimiter = ',', # Delimiter for CSV, default is ','
        autodetect = True ,# Automatically infer schema from the file
        #google_cloud_storage_conn_id='google_cloud_default',  # Replace with your Airflow GCP connection ID if not default
        #bigquery_conn_id='google_cloud_default',  # Replace with your Airflow BigQuery connection ID if not default
    )

    load_csv_to_bigquery