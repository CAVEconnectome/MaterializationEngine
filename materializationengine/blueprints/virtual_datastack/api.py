import logging

import requests
from dynamicannotationdb.models import AnalysisTable, AnalysisVersion, Base
from flask import abort, current_app
from flask_restx import Namespace, Resource, inputs, reqparse
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.database import (
    create_session,
    dynamic_annotation_cache,
    sqlalchemy_cache,
)
from materializationengine.info_client import get_aligned_volumes
from materializationengine.schemas import AnalysisTableSchema, AnalysisVersionSchema
from middle_auth_client import (
    auth_required,
    auth_requires_admin,
    auth_requires_permission,
)
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import NoSuchTableError

from .models import VirtualDatastack
from .schema import VirtualDatastackSchema

__version__ = "4.0.22"

authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}
virtual_datastack_bp = Namespace(
    "Virtual Datastack Client",
    authorizations=authorizations,
    description="Virtual Datastack Client",
)


def get_virtual_datastacks(aligned_volume: str):
    db = dynamic_annotation_cache.get_db(database=aligned_volume)
    results = db.database.cached_session.query(VirtualDatastack).all()
    schema = VirtualDatastackSchema(many=True)
    return schema.dump(results)


def get_virtual_datastack(aligned_volume: str, virtual_datastack_name: str):
    db = dynamic_annotation_cache.get_db(database=aligned_volume)
    results = (
        db.database.cached_session.query(VirtualDatastack)
        .filter(VirtualDatastack.release_name == virtual_datastack_name)
        .one()
    )
    schema = VirtualDatastackSchema()
    return schema.dump(results)


@virtual_datastack_bp.route("/datastack/virtual/versions")
class VirtualDatastackVersions(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="aligned_volume_name")
    @virtual_datastack_bp.doc("datastack_versions", security="apikey")
    def get(self, aligned_volume_name: str):
        """get available versions

        Args:
            datastack_name (str): datastack name

        Returns:
            list(int): list of versions that are available
        """
        datastack_info = get_virtual_datastacks(aligned_volume_name)
        return datastack_info, 200


@virtual_datastack_bp.route("/datastack/virtual/<string:datastack_name>/release")
class VirtualDatastackVersion(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="aligned_volume_name")
    @virtual_datastack_bp.doc("datastack_versions", security="apikey")
    def get(self, aligned_volume_name, datastack_name: str):
        datastack_info = get_virtual_datastack(aligned_volume_name, datastack_name)
        return datastack_info, 200


@virtual_datastack_bp.route(
    "/datastack/virtual/<string:datastack_name>/version/<int:version>"
)
class VirtualDatastackQuery(Resource):
    def get():
        raise NotImplementedError

    def post():
        raise NotImplementedError

    def update():
        raise NotImplementedError

    def delete():
        raise NotImplementedError
