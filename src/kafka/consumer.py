from kafka import KafkaConsumer
import json
from src.utils.logger import setup_logger
from config import Config

logger = setup_logger(__name__)

class ScopusConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            Config.KAFKA_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=Config.CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def consume(self, process_message):
        try:
            for message in self.consumer:
                try:
                    logger.info(f"Received message from partition {message.partition} at offset {message.offset}")
                    process_message(message.value)
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
            raise
        finally:
            self.close()

    def close(self):
        self.consumer.close()