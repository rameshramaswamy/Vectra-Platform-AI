FROM osrm/osrm-backend:v5.27.1

# Download a small map (Monaco) for testing, or mount a volume for the planet
# In Enterprise Prod, use a Sidecar or InitContainer to fetch .pbf files from S3
RUN apt-get update && apt-get install -y wget
RUN wget http://download.geofabrik.de/europe/monaco-latest.osm.pbf -O /data/map.osm.pbf

# Pre-process the map
RUN osrm-extract -p /opt/car.lua /data/map.osm.pbf
RUN osrm-partition /data/map.osrm
RUN osrm-customize /data/map.osrm

EXPOSE 5000
CMD ["osrm-routed", "--algorithm", "mld", "/data/map.osrm"]