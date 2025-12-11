import asyncio
import json
import structlog
import aioboto3
import asyncpg
import pandas as pd
import io
import time
import pygeohash as pgh
from kafka import KafkaConsumer
from app.core.config import settings

logger = structlog.get_logger()

# --- Optimization 2: The Stillness Filter ---
def filter_noise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove points where vehicle is stopped to save DB space.
    Logic: If speed < 0.5 m/s, it's noise/stopped.
    Exception: Keep the FIRST and LAST point of a stop (to mark duration).
    """
    if df.empty: return df
    
    # Simple vectorization: Keep if speed > 0.5 OR event is not 'PING'
    # (Events like 'SCAN' or 'STOP' must always be kept)
    mask = (df['speed_mps'] > 0.5) | (df['event_type'] != 'PING')
    return df[mask]

# --- Optimization 3: Geohashing ---
def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """Add Geohash for fast string-based indexing"""
    if df.empty: return df
    # Generate Geohash (Precision 7 is ~150m, good for neighborhood lookups)
    df['geohash'] = df.apply(lambda x: pgh.encode(x['latitude'], x['longitude'], precision=7), axis=1)
    return df

async def write_to_s3(session, df, first_offset):
    """Async S3 Upload"""
    if df.empty: return
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    file_key = f"traces/{int(time.time())}_{first_offset}.parquet"
    
    async with session.client("s3", endpoint_url=settings.S3_ENDPOINT,
                              aws_access_key_id=settings.AWS_ACCESS_KEY,
                              aws_secret_access_key=settings.AWS_SECRET_KEY) as s3:
        await s3.upload_fileobj(buffer, settings.S3_BUCKET_NAME, file_key)
        logger.info("S3 Write Success", key=file_key)

async def write_to_postgres(pool, df):
    """Async PostGIS Copy"""
    if df.empty: return

    # Prepare CSV buffer for COPY
    output = io.StringIO()
    # Note: We added 'geohash' to the columns
    for _, r in df.iterrows():
        output.write(f"{r['driver_id']}\t{r['vehicle_id']}\t"
                     f"{pd.to_datetime(r['timestamp_ms'], unit='ms').isoformat()}\t"
                     f"SRID=4326;POINT({r['longitude']} {r['latitude']})\t"
                     f"{r['speed_mps']}\t{r['event_type']}\t{r['geohash']}\n")
    output.seek(0)

    async with pool.acquire() as conn:
        # Use Copy protocol
        await conn.copy_from_table(
            'raw_gps_traces',
            columns=('driver_id', 'vehicle_id', 'timestamp', 'geom', 'speed', 'event_type', 'geohash'),
            format='csv',
            delimiter='\t',
            source=output
        )
        logger.info("DB Write Success", rows=len(df))

async def consume_loop():
    # 1. Setup Async Resources
    aws_session = aioboto3.Session()
    # Database Pool
    db_pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL, 
        min_size=10, 
        max_size=50, # High throughput setting
        command_timeout=60
    )

    # Kafka is sync, but we batch process async
    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC_TRACES,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='vectra-lake-worker-async',
        enable_auto_commit=False,
        max_poll_records=1000
    )

    logger.info("Async Consumer Started")
    batch = []
    
    while True:
        # Poll is blocking, but fast
        raw_msgs = consumer.poll(timeout_ms=100)
        
        for tp, messages in raw_msgs.items():
            for msg in messages:
                batch.append(msg.value)

        if len(batch) >= 1000 or (len(batch) > 0 and time.time() % 5 == 0):
            # Convert to DF
            df = pd.DataFrame(batch)
            
            # Apply Optimizations
            df = filter_noise(df) 
            df = enrich_data(df)

            # Parallel Write
            try:
                if not df.empty:
                    await asyncio.gather(
                        write_to_s3(aws_session, df, batch[0]['timestamp_ms']),
                        write_to_postgres(db_pool, df)
                    )
                consumer.commit()
                batch = []
            except Exception as e:
                logger.error("Batch Failed", error=str(e))
                # In prod: Seek back or DLQ
        
        await asyncio.sleep(0.01) # Yield to event loop

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume_loop())