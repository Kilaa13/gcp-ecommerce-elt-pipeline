from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'shakila',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

project_id = 'your name project'
dataset_id = 'staging_data'
source_table = f'{project_id}.{dataset_id}.online_shoppers'
months = ['Nov', 'Dec', 'Sep', 'May'] 

with DAG(
    dag_id='load_and_transform_view_shoppers',
    default_args=default_args,
    description='Load CSV online_shoppers dari GCS ke BQ, buat tabel per bulan & reporting view',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'ecommerce', 'elt_final'],
) as dag:

    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bkt-ecommerce-raw-data',
        object='online_shoppers.csv',
        timeout=300,
        poke_interval=30,
        mode='poke',
        google_cloud_conn_id='google_cloud_default',
    )

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-ecommerce-raw-data',
        source_objects=['online_shoppers.csv'],
        destination_project_dataset_table=source_table,
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    create_table_tasks = []
    create_view_tasks = []
    for month in months:

        create_table_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{month.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.shoppers_{month.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE Month = '{month}'
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default',
        )

        create_view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{month.lower()}_revenue',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.shoppers_{month.lower()}_revenue_view` AS
                        SELECT 
                            `Administrative`,
                            `ExitRates`,
                            `PageValues`,
                            `VisitorType`,
                            `Revenue`
                        FROM `{project_id}.{dataset_id}.shoppers_{month.lower()}_table`
                        WHERE `Revenue` = TRUE
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default',
        )

        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)
        create_table_tasks.append(create_table_task)
        create_view_tasks.append(create_view_task)

    success_task = EmptyOperator(
        task_id='success_task',
    )

    check_file_exists >> load_csv_to_bigquery
    for create_table_task, create_view_task in zip(create_table_tasks, create_view_tasks):
        create_table_task >> create_view_task >> success_task
