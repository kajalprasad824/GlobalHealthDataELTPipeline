from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

#DAG default arguments
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
}

# Define project, dataset,and table details
project_id = 'ace-ensign-453414-f0'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'

source_table = f'{project_id}.{dataset_id}.global_data' # Main table loaded from CSV
countries = ['USA','India','Germany','Japan','France','Canada','Italy','Turkey','UK','South Africa','Mexico','South Korea','Nigeria','Russia','Saudi Arabia','Australia','Argentina','Indonesia','China']

#DAG definition
with DAG(
    dag_id = 'load_and_transform',
    default_args = default_args,
    description = 'Load a CSV file from GCS to BigQuery and create country-specific tables',
    schedule =None,
    start_date = datetime(2024,1,1),
    catchup = False,
    tags = ['bigquery','gcs','csv'],
) as dag:
    
    check_file_exists = GCSObjectExistenceSensor(
        task_id = 'check_file_exists',
        bucket = 'bkt-health-data',
        object = 'global_health_data.csv',
        timeout = 300, # maximum wait time in seconds
        poke_interval = 30, # Time interval in seconds to check again
        mode='poke', # Use poke mode for synchronous checking
    )

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

    ##Task3 : Create country specific tables
    for country in countries:
        BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower().replace(" ","_")}',
            configuration = {
                "query" : {
                    "query" : f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'                
                 """,
                 "useLegacySql": False
                }
            },
        ).set_upstream(load_csv_to_bigquery)

check_file_exists >> load_csv_to_bigquery 