import uuid
from datetime import datetime
from sqlalchemy import Column, String, Date, Numeric, Enum, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import enum

Base = declarative_base()

class ChargeStatus(enum.Enum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'

class CSVFile(Base):
    __tablename__ = "csv_files"
    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename = Column(String, nullable=False)
    upload_date = Column(DateTime, default=datetime.utcnow)
    charge_rows = relationship("ChargeRow", back_populates="csv_file", cascade="all, delete-orphan")

class ChargeRow(Base):
    __tablename__ = "charge_rows"
    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    csv_file_id = Column(PG_UUID(as_uuid=True), ForeignKey("csv_files.id"), nullable=False)
    name = Column(String(100), nullable=False)
    government_id = Column(String, nullable=False)
    email = Column(String, nullable=False)
    debt_amount = Column(Numeric, nullable=False)
    debt_due_date = Column(Date, nullable=False)
    debt_id = Column(String, nullable=False)  # storing it as string for simplicity
    status = Column(Enum(ChargeStatus), default=ChargeStatus.PENDING)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    csv_file = relationship("CSVFile", back_populates="charge_rows") 