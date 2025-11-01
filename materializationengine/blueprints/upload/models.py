import enum
from datetime import datetime, timezone

from flask import Flask
from sqlalchemy import ARRAY, JSON, Column, DateTime, Enum, Integer, String, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base

from materializationengine.database import dynamic_annotation_cache

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
    bucket_path = Column(String(500))
    
    schema_type = Column(String(100), nullable=False)
    column_mapping = Column(JSON, nullable=False)

    aligned_volume = Column(String(100), nullable=False)
    datastack = Column(String(100), nullable=False)
    materialized_versions = Column(ARRAY(Integer), nullable=True )

    status = Column(
        Enum(UploadStatus, name='upload_status_enum', native_enum=True),
        nullable=False,
        default=UploadStatus.UPLOADING
    )
    error_details = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)
      
    created_at = Column(DateTime, nullable=False, default=datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=True, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    upload_completed_at = Column(DateTime, nullable=True)


def init_staging_database(app: Flask):
    """Initialize the staging database with required tables"""
    try:
        staging_database = app.config.get('STAGING_DATABASE_NAME')
        if not staging_database:
            staging_database = 'staging'

        client = dynamic_annotation_cache.get_db(staging_database)
        UploadMetadata.__table__.create(client.database.engine, checkfirst=True)               
        app.logger.info("Staging database initialized successfully")
        return True

    except SQLAlchemyError as e:
        app.logger.error(f"Database initialization error: {str(e)}")
        return False
    except Exception as e:
        app.logger.error(f"Unexpected error during database initialization: {str(e)}")
        return False
    

def create_reference_table_model(table_name: str):
    """Create a reference table model with dynamic table name for staging database"""
    class ReferenceTable(Base):
        __tablename__ = f'{table_name}'
        
        id = Column(Integer, primary_key=True)
        
        __table_args__ = {'extend_existing': True}

        @classmethod
        def create_table(cls, engine):
            cls.__table__.create(engine, checkfirst=True)
    
    ReferenceTable.__name__ = table_name
    return ReferenceTable