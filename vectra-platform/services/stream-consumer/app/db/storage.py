import io
import csv
from app.db.session import engine

def bulk_insert_traces(records):
    """
    Uses Postgres COPY protocol for maximum throughput.
    records: list of dicts
    """
    if not records:
        return

    # Create an in-memory CSV buffer
    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for r in records:
        # Format geometry as EWKT for PostGIS
        # TIMESTAMP format must be ISO8601
        writer.writerow([
            r['driver_id'],
            r.get('vehicle_id', ''),
            pd.to_datetime(r['timestamp_ms'], unit='ms').isoformat(),
            f"SRID=4326;POINT({r['longitude']} {r['latitude']})",
            r.get('speed_mps', 0),
            r.get('event_type', 'UNKNOWN')
        ])

    output.seek(0)

    # Raw connection for COPY
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.copy_expert(
            """
            COPY raw_gps_traces (driver_id, vehicle_id, timestamp, geom, speed, event_type)
            FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
            """,
            output
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()