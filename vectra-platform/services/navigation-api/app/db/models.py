from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
import datetime

Base = declarative_base()

class RefinedLocation(Base):
    __tablename__ = 'refined_locations'
    
    # In Phase 2, we use geohash as the temporary Address ID
    id = Column(String, primary_key=True) 
    nav_point = Column(Geometry('POINT', srid=4326))
    entry_point = Column(Geometry('POINT', srid=4326))
    confidence_score = Column(Float)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

class LocationFeedback(Base):
    """
    CRITICAL: This table collects Ground Truth 
    """
    __tablename__ = 'location_feedback'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String, index=True) # The Geohash/Address ID
    driver_id = Column(String)
    
    # Did the system work?
    is_nav_point_accurate = Column(Boolean)
    is_entry_point_accurate = Column(Boolean)
    
    # If false, where was the actual point? (Driver taps map)
    corrected_lat = Column(Float, nullable=True)
    corrected_lon = Column(Float, nullable=True)
    
    comment = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)