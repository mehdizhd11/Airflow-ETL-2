# Default arguments
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from ETL2.Extractor import extractor
from ETL2.Transporter import transporter
from ETL2.Loader import loader

from airflow import DAG


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

ETL_DAG = DAG(
    'ETL2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

csv_extract_task = PythonOperator(
    task_id='csv_extractor',
    python_callable=extractor.csv_to_redis,
    op_kwargs={'csv_path': '/Users/MeT/Airflow/dags/Data/customers-1000.csv'},
    dag=ETL_DAG,
    retries=1,  # Number of retries for this task
    retry_delay=timedelta(seconds=30),  # Delay between retries for this task
)

elasticsearch_extract_task = PythonOperator(
    task_id='elasticsearch_extractor',
    python_callable=extractor.elasticsearch_to_redis,
    op_kwargs={'elastic_index': 'customers-1000'},
    dag=ETL_DAG,
    retries=1, retry_delay=timedelta(seconds=30),
)

row_slice_transform_task = PythonOperator(
    task_id='row_slice_transform',
    python_callable=transporter.slice_by_row,  
    op_kwargs={'num': 3, 'redis_dest': 'row_transport_data'},
    dag=ETL_DAG,
    retries=1, retry_delay=timedelta(seconds=30),
)

column_slice_transform_task = PythonOperator(
    task_id='column_slice_transform',
    python_callable=transporter.slice_by_column,  
    op_kwargs={'num': 2, 'redis_dest': 'column_transport_data'},
    dag=ETL_DAG,
    retries=1, retry_delay=timedelta(seconds=30),
)

row_loader_task = PythonOperator(
    task_id='row_loader_task',
    python_callable=loader.to_mongo,
    op_kwargs={'redis_key': 'row_transport_data'},
    dag=ETL_DAG,
    retries=1, retry_delay=timedelta(seconds=30),
)

column_loader_task = PythonOperator(
    task_id='column_loader_task',
    python_callable=loader.to_csv,
    op_kwargs={'csv_path': "COLUMN_SLICE", 'redis_key': 'column_transport_data'}, 
    dag=ETL_DAG,
    retries=1, retry_delay=timedelta(seconds=30),
)

[csv_extract_task, elasticsearch_extract_task] >> row_slice_transform_task >> row_loader_task
[csv_extract_task, elasticsearch_extract_task] >> column_slice_transform_task >> column_loader_task
