from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks import load_scopus_data, scrape_data, produce_to_kafka, run_spark_job
import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'scopus_analysis_pipeline',
    default_args=default_args,
    description='Scopus Analysis Pipeline',
    schedule_interval=timedelta(days=1)
)

t1 = PythonOperator(
    task_id='load_scopus_data',
    python_callable=load_scopus_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_to_kafka,
    dag=dag
)

t4 = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag
)

t1 >> t2 >> t3 >> t4