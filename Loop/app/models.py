from sqlalchemy import Time, Column, Integer, String, DateTime, Text
from database import Base
from datetime import datetime

class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    timestamp_utc = Column(DateTime)
    status = Column(String(length=25))

class BusinessHours(Base):
    __tablename__ = "businesshours"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    day_of_week = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class TimeZone(Base):
    __tablename__ = "timezone"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    timezone_str = Column(String(length=100))

class Report(Base):
    __tablename__ = "reports"
    id = Column(Integer, primary_key=True)
    report_id = Column(String(255), nullable=False, unique=True)
    status = Column(String(length=20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)