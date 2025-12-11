-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- 1. Create the Master Table (Partitioned)
CREATE TABLE raw_gps_traces (
    id              BIGSERIAL,
    driver_id       VARCHAR(50) NOT NULL,
    vehicle_id      VARCHAR(50),
    timestamp       TIMESTAMPTZ NOT NULL,
    geom            GEOMETRY(POINT, 4326),
    speed           FLOAT,
    event_type      VARCHAR(20),
    geohash         VARCHAR(12), -- Optimization 3
    created_at      TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- New Index for optimization
-- This is much faster than GIST for "equals" queries
CREATE INDEX idx_gps_geohash ON raw_gps_traces (geohash);

-- 2. Create Indexes on the Master (Propagates to partitions)
CREATE INDEX idx_gps_driver_time ON raw_gps_traces (driver_id, timestamp DESC);
CREATE INDEX idx_gps_geom ON raw_gps_traces USING GIST (geom);

-- 3. Function to Auto-Create Partitions (Monthly)
CREATE OR REPLACE FUNCTION create_partition_if_not_exists()
RETURNS TRIGGER AS $$
DECLARE
    partition_date TEXT;
    partition_name TEXT;
    start_of_month TIMESTAMP;
    end_of_month TIMESTAMP;
BEGIN
    start_of_month := DATE_TRUNC('month', NEW.timestamp);
    partition_date := TO_CHAR(start_of_month, 'YYYY_MM');
    partition_name := 'raw_gps_traces_' || partition_date;
    end_of_month := start_of_month + INTERVAL '1 month';

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF raw_gps_traces FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_of_month, end_of_month
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- The "Golden Record" table
CREATE TABLE IF NOT EXISTS refined_locations (
    id VARCHAR(20) PRIMARY KEY, -- Geohash or UUID
    nav_point GEOMETRY(POINT, 4326),
    entry_point GEOMETRY(POINT, 4326),
    confidence_score FLOAT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- The Feedback table (Ground Truth)
CREATE TABLE IF NOT EXISTS location_feedback (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(20),
    driver_id VARCHAR(50),
    is_nav_point_accurate BOOLEAN,
    is_entry_point_accurate BOOLEAN,
    corrected_lat FLOAT,
    corrected_lon FLOAT,
    comment TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_refined_id ON refined_locations(id);