from kafka import KafkaProducer
import json
from src.utils.logger import setup_logger
from config import Config

logger = setup_logger(__name__)

class ScopusProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.topic = Config.KAFKA_TOPIC

    def send_data(self, data):
        try:
            future = self.producer.send(self.topic, data)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            logger.info(f"Data sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
        except Exception as e:
            logger.error(f"Error sending data to Kafka: {str(e)}")
            raise

    def close(self):
        self.producer.close()