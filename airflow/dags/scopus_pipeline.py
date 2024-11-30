from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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

def stream_scopus_data(**context):
    """
    This task now streams data in chunks instead of loading everything at once.
    Each chunk is processed and passed to the next stage via XCom.
    """
    from src.data.scopus_loader import StreamingScopusLoader
    
    # Initialize the streaming loader with a reasonable chunk size
    loader = StreamingScopusLoader(chunk_size=1000)
    
    # Keep track of processed chunks for monitoring
    chunks_processed = 0
    records_processed = 0
    
    # Process data in chunks
    for chunk in loader.process_data():
        # Push each chunk to XCom with a unique key
        context['task_instance'].xcom_push(
            key=f'scopus_chunk_{chunks_processed}',
            value=chunk
        )
        
        chunks_processed += 1
        records_processed += len(chunk)
        
    # Push metadata about the processing
    context['task_instance'].xcom_push(
        key='processing_metadata',
        value={
            'total_chunks': chunks_processed,
            'total_records': records_processed
        }
    )
    
    return f"Processed {records_processed} records in {chunks_processed} chunks"

def stream_to_kafka(**context):
    """
    Receives chunks of data from the previous task and streams them to Kafka.
    """
    from src.kafka.producer import ScopusProducer
    
    # Get processing metadata
    metadata = context['task_instance'].xcom_pull(
        task_ids='stream_scopus_data',
        key='processing_metadata'
    )
    
    producer = ScopusProducer()
    records_sent = 0
    
    # Process each chunk
    for chunk_id in range(metadata['total_chunks']):
        chunk = context['task_instance'].xcom_pull(
            task_ids='stream_scopus_data',
            key=f'scopus_chunk_{chunk_id}'
        )
        
        # Send each record in the chunk to Kafka
        for record in chunk:
            producer.send_data(record)
            records_sent += 1
        
        # Ensure data is sent before processing next chunk
        producer.flush()
    
    return f"Sent {records_sent} records to Kafka"

def process_with_spark(**context):
    """
    Starts Spark streaming job to process data from Kafka.
    """
    from src.spark.streaming import SparkStreamingJob
    
    # Initialize and start Spark streaming
    spark_job = SparkStreamingJob()
    spark_job.start_streaming()
    
    return "Spark streaming job started successfully"

dag = DAG(
    'scopus_analysis_pipeline',
    default_args=default_args,
    description='Streaming Scopus Analysis Pipeline',
    schedule_interval=timedelta(days=1)
)

stream_data = PythonOperator(
    task_id='stream_scopus_data',
    python_callable=stream_scopus_data,
    dag=dag
)

kafka_stream = PythonOperator(
    task_id='stream_to_kafka',
    python_callable=stream_to_kafka,
    dag=dag
)

spark_stream = PythonOperator(
    task_id='process_with_spark',
    python_callable=process_with_spark,
    dag=dag
)

stream_data >> kafka_stream >> spark_stream