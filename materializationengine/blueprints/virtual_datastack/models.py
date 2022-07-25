import datetime
from datetime import datetime

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow_sqlalchemy.fields import Nested
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class VirtualDatastack(Base):
    __tablename__ = "virtualdatastack"

    id = Column(Integer, primary_key=True)
    release_name = Column(String, unique=True)
    datastack = Column(String)
    versions = relationship("Versions", backref="virtualdatastack")
    release_date = Column(DateTime, default=datetime.datetime.utcnow())
    created_at = Column(DateTime, default=datetime.datetime.utcnow())


class Versions(Base):
    __tablename__ = "versions"

    id = Column(Integer, primary_key=True)
    materialization_version = Column(Integer)
    table = relationship("ValidTable", backref="versions")
    virtualdatastack_id = Column(Integer, ForeignKey("virtualdatastack.id"))


class ValidTable(Base):
    __tablename__ = "validtable"
    id = Column(Integer, primary_key=True)
    table_name = Column(String)
    version_id = Column(Integer, ForeignKey("versions.id"))


class ValidTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ValidTable
        include_fk = True
        load_instance = True
        include_relationships = True


class VersionsSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Versions
        include_fk = True
        load_instance = True
        include_relationships = True

    table = Nested(
        ValidTableSchema, many=True, exclude=("id", "version_id", "versions")
    )


class VirtualDatastackSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = VirtualDatastack
        include_relationships = True
        load_instance = True

    versions = Nested(
        VersionsSchema,
        many=True,
        exclude=(
            "id",
            "virtualdatastack",
        ),
    )
