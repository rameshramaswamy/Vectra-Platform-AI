import json
import io
import boto3
import time
import structlog
from kafka import KafkaConsumer
from app.db.session import SessionLocal
from app.db.models import GpsPoint
from app.core.config import settings

logger = structlog.get_logger()

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        endpoint_url=settings.S3_ENDPOINT
    )

def process_batch(messages, db, s3_client):
    """
    Transactions: If S3 fails, we don't commit to DB. 
    If DB fails, we don't commit Kafka offsets (handled by consumer loop).
    """
    valid_records = []
    
    # 1. Validation Phase
    for msg in messages:
        try:
            data = msg.value
            # Basic schema validation could go here
            valid_records.append(data)
        except Exception as e:
            logger.error("Poison pill detected", error=str(e), offset=msg.offset)
            # Enterprise: Send to DLQ (Dead Letter Queue) topic here
            continue

    if not valid_records:
        return

    # 2. S3 Archival (Parquet) - Using Pandas or PyArrow
    try:
        import pandas as pd
        df = pd.DataFrame(valid_records)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        file_key = f"traces/{int(time.time())}_{messages[0].offset}.parquet"
        s3_client.upload_fileobj(buffer, settings.S3_BUCKET_NAME, file_key)
        logger.info("Archived to S3", count=len(df), key=file_key)
    except Exception as e:
        logger.error("S3 Upload Failed", error=str(e))
        raise e # Retrying at consumer level

    # 3. PostGIS Bulk Insert
    try:
        db_objects = [
            GpsPoint(
                driver_id=r['driver_id'],
                geom=f"POINT({r['longitude']} {r['latitude']})",
                speed=r.get('speed_mps', 0),
                event_type=r.get('event_type', 'UNKNOWN'),
                timestamp=pd.to_datetime(r['timestamp_ms'], unit='ms')
            ) for r in valid_records
        ]
        
        # Bulk save is faster than individual adds
        db.bulk_save_objects(db_objects)
        db.commit()
        logger.info("Committed to PostGIS", count=len(db_objects))
    except Exception as e:
        logger.error("DB Commit Failed", error=str(e))
        db.rollback()
        raise e

def consume_loop():
    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC_TRACES,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='vectra-lake-worker-v1',
        enable_auto_commit=False, # Enterprise: Manual commit after processing
        max_poll_records=500
    )
    
    s3 = get_s3_client()
    db = SessionLocal()
    batch = []
    
    logger.info("Stream Consumer Started")
    
    try:
        for message in consumer:
            batch.append(message)
            
            if len(batch) >= 500:
                process_batch(batch, db, s3)
                consumer.commit() # Commit offsets only after successful processing
                batch = []
    except KeyboardInterrupt:
        logger.info("Stopping Consumer...")
    finally:
        if batch:
            # Process remaining items before exit
            process_batch(batch, db, s3)
            consumer.commit()
        consumer.close()
        db.close()

if __name__ == "__main__":
    from app.core.logging import setup_logging
    setup_logging()
    consume_loop()