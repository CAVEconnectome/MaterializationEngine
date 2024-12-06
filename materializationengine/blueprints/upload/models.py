from sqlalchemy import Column, Integer, String, DateTime, JSON, Text, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import enum

Base = declarative_base()


class UploadStatus(enum.Enum):
    UPLOADING = "UPLOADING"
    FORMATTING = "FORMATTING" 
    STAGING = "STAGING"
    PROCESSING = "PROCESSING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"

class UploadMetadata(Base):
    __tablename__ = 'upload_metadata'
    
    id = Column(Integer, primary_key=True)
    table_name = Column(String(100), nullable=False)
    upload_id = Column(String(100), unique=True, nullable=False)

    original_filename = Column(String(500))
    bucket_path = Column(String(500))  # GCS bucket path for formatted CSV
    
    schema_type = Column(String(100), nullable=False)
    column_mapping = Column(JSON, nullable=False)  # Store column mappings
    
    status = Column(
        Enum(UploadStatus, name='upload_status_enum', native_enum=True),
        nullable=False,
        default=UploadStatus.UPLOADING
    )
    error_details = Column(Text)
    
    target_database = Column(String(100))  #TODO refactor to JSON to map mat versions
    row_count = Column(Integer)
      
    created_at = Column(DateTime, nullable=False, default=datetime.now(datetime.timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=datetime.now(datetime.timezone.utc), onupdate=datetime.now(datetime.timezone.utc))
    completed_at = Column(DateTime)