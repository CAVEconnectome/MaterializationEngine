from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from dynamicannotationdb.models import Base

MatBase = declarative_base()


class AnalysisVersion(Base):
    __tablename__ = "analysisversion"
    id = Column(Integer, primary_key=True)
    datastack = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)
    valid = Column(Boolean)
    expires_on = Column(DateTime, nullable=True)

    def __repr__(self):
        return "{}__mat{}".format(self.datastack, self.version)


class AnalysisTable(Base):
    __tablename__ = "analysistables"
    id = Column(Integer, primary_key=True)
    aligned_volume = Column(String(100), nullable=False)
    schema = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    analysisversion_id = Column(Integer, ForeignKey("analysisversion.id"))
    analysisversion = relationship("AnalysisVersion")


class MaterializedMetadata(MatBase):
    __tablename__ = "materializedmetadata"
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    row_count = Column(Integer, nullable=False)
    materialized_timestamp = Column(DateTime, nullable=False)
