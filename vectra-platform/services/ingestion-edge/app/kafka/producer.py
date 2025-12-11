from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import structlog
from app.core.config import settings

logger = structlog.get_logger()

# Global Producer
_producer = None

def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=20,     # Batching optimization
                compression_type='gzip', # Network bandwidth optimization
                acks='all',       # Enterprise: Wait for all replicas
                retries=3
            )
            logger.info("Kafka Producer Initialized")
        except Exception as e:
            logger.critical("Failed to initialize Kafka Producer", error=str(e))
            raise e
    return _producer

def on_send_success(record_metadata):
    # Debug level in prod to save logs
    logger.debug("Message delivered", topic=record_metadata.topic, partition=record_metadata.partition)

def on_send_error(ex):
    logger.error("Message delivery failed", error=str(ex))

def send_trace(data: dict):
    producer = get_producer()
    try:
        future = producer.send(settings.KAFKA_TOPIC_TRACES, value=data)
        # Async callbacks
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
    except KafkaError as e:
        logger.error("Kafka Exception during send", error=str(e))

def close_producer():
    global _producer
    if _producer:
        _producer.flush()
        _producer.close()