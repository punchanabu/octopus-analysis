from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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

def load_scopus_data(**context):
    from src.data.scopus_loader import ScopusLoader
    loader = ScopusLoader()
    data = loader.load_data()
    return "Scopus data loaded successfully"

def produce_to_kafka(**context):
    from src.kafka.producer import ScopusProducer
    producer = ScopusProducer()
    producer.send_data({"test": "data"})
    return "Data sent to Kafka"

def run_spark_job(**context):
    from src.spark.streaming import SparkStreamingJob
    spark_job = SparkStreamingJob()
    spark_job.start_streaming()
    return "Spark job completed"

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
    task_id='produce_to_kafka',
    python_callable=produce_to_kafka,
    dag=dag
)

t3 = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag
)

t1 >> t2 >> t3