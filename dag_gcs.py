from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.storage import Blob
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Define DAG
dag = DAG("composer_gcs_trigger", schedule_interval=None, start_date=days_ago(1))

# Parameter
gcs_file = "{{ dag_run.conf['name'] }}"

run_this = "echo " + gcs_file

task1 = GCSToBigQueryOperator(
        dag=dag,
        task_id="load_trans_data",
        bucket="<update-bucketname>",
        source_objects=[gcs_file],
        destination_project_dataset_table="retaildb.tbltrans_stg",
        schema_fields=[
            {"name": "txnid", "type": "INT64", "mode": "NULLABLE"},
            {"name": "txndate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custid", "type": "INT64", "mode": "NULLABLE"},
            {"name": "txnamount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
)

task2 = BigQueryInsertJobOperator(
          dag=dag,
          task_id='call_sp_to_processdata',
          configuration={
          "query": {
             "query": "CALL `retaildb.sp_processdata`();",
            "useLegacySql": False,
        }
    }
)

task1 >> task2
