from sqlalchemy import Column, String, Integer, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

MatBase = declarative_base()


class MaterializedMetadata(MatBase):
    __tablename__ = "materializedmetadata"
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    row_count = Column(Integer, nullable=False)
    materialized_timestamp = Column(DateTime, nullable=False)
    segmentation_source = Column(String(255), nullable=True)
    is_merged = Column(Boolean, nullable=True)

