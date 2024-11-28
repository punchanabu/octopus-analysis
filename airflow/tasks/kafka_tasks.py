from src.kafka.producer import KafkaProducer
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def produce_to_kafka(**context):
    try:
        ti = context['task_instance']
        scopus_data = ti.xcom_pull(task_ids='load_scopus_data', key='scopus_data')
        # scraped_data = ti.xcom_pull(task_ids='scrape_additional_data', key='scraped_data')
        
        producer = KafkaProducer()
        producer.send_data(scopus_data)
        # producer.send_data(scraped_data)
    except Exception as e:
        logger.error(f"Error while producing to kafka: {e}")
        raise
    
