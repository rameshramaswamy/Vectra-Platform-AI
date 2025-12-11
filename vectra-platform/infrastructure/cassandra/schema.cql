CREATE KEYSPACE IF NOT EXISTS vectra_wifi 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE vectra_wifi;

-- Partitioned by Geohash (Precision 6 ~1.2km x 0.6km)
-- This allows us to fetch all reference points in a neighborhood quickly.
CREATE TABLE IF NOT EXISTS radio_map (
    geohash_p6 text,
    scan_id uuid,
    latitude double,
    longitude double,
    bssids map<text, int>, -- MAC Address -> RSSI
    created_at timestamp,
    PRIMARY KEY (geohash_p6, scan_id)
);

-- Time-window compaction strategy for efficient cleanup of old signals
ALTER TABLE radio_map WITH compaction = { 
    'class' : 'TimeWindowCompactionStrategy', 
    'compaction_window_unit' : 'DAYS', 
    'compaction_window_size' : 7 
};