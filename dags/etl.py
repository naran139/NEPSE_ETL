from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from app.etl.extract import extract
from app.etl.transform import transform
from app.etl.load import load


default_args = { 
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


dag = DAG(
    "NEPSE_DAG",
    start_date=datetime(2024,11,15),
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily'
)

extract_data = PythonOperator(
    task_id = "extract_data",
    python_callable= extract,
    dag = dag, 
)

transform_data = PythonOperator(
    task_id = "transform_data",
    python_callable=transform,
    dag = dag
)

load_data = PythonOperator(
    task_id = "load_data",
    python_callable=load,
    dag = dag
)
extract_data >> transform_data >> load_data
