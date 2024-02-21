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

def getfilesize(fname):
    client = storage.Client()
    bucket = client.bucket('inceptezcustomer1')
    desired_file = fname
    for blob in bucket.list_blobs():
        if desired_file== blob.name and blob.size > 0:
            print("Name: "+ blob.name +" Size blob obj: "+str(blob.size) + "bytes")
          
def read_gcs_file(fname):
    hook = GCSHook()
    # perform gcs hook download
    resp_byte = hook.download_as_byte_array(
        bucket_name="inceptezcustomer1",
        object_name=fname,
    )

    resp_string = resp_byte.decode("utf-8")
    print(resp_string)



task1 = PythonOperator(
    task_id="get_file_size",
    python_callable=getfilesize,
    op_kwargs={"fname": gcs_file},
    dag=dag,
)

task2 = PythonOperator(
    task_id="read_file_print",
    python_callable=read_gcs_file,
    op_kwargs={"fname": gcs_file},
    dag=dag,
)



task3 = GCSToBigQueryOperator(
        task_id="load_trans_data",
        bucket="inceptezcustomer1",
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

task4 = BigQueryInsertJobOperator(
          task_id='call_sp_to_processdata',
          configuration={
          "query": {
             "query": "CALL `retaildb.sp_processdata`();",
            #"query": "{% include 'Scripts/Script.sql' %}",
            "useLegacySql": False,
        }
    }
)

task1 >> task2 >> task3 >> task4
