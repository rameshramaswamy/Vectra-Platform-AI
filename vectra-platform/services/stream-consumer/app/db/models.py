from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Float, BigInteger, DateTime
from geoalchemy2 import Geometry
import datetime

Base = declarative_base()

class GpsPoint(Base):
    __tablename__ = 'raw_gps_traces'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    driver_id = Column(String, index=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    # PostGIS Geometry Column (Point, SRID 4326)
    geom = Column(Geometry('POINT', srid=4326)) 
    speed = Column(Float)
    event_type = Column(String)