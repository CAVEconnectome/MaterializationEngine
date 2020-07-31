from flask import abort, current_app, request
from flask_restx import Namespace, Resource, reqparse
from flask_accepts import accepts, responds

from materializationengine.models import AnalysisTable, AnalysisVersion
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema
from materializationengine.info_client import get_aligned_volumes
from materializationengine.database import get_db
from materializationengine.blueprints.client.schemas import (
    Metadata,
    SegmentationTableSchema,
    CreateTableSchema,
    PostPutAnnotationSchema,
    GetDeleteAnnotationSchema,
    SegmentationDataSchema
)
from middle_auth_client import auth_required, auth_requires_permission
import logging

__version__ = "0.2.35"

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'query',
        'name': 'middle_auth_token'
    }
}

client_bp = Namespace("Materialization Client",
                      authorizations=authorizations,
                      description="Materialization Client")

annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument('annotation_ids', type=int, action='split', help='list of annotation ids')    
annotation_parser.add_argument('pcg_table_name', type=str, help='name of pcg segmentation table')    


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")

@client_bp.route("/aligned_volume/<string:aligned_volume_name>/table")
class SegmentationTable(Resource):
    @auth_required
    @client_bp.doc("create_segmentation_table", security="apikey")
    @accepts("SegmentationTableSchema", schema=SegmentationTableSchema, api=client_bp)
    def post(self, aligned_volume_name: str):
        """ Create a new segmentation table"""
        check_aligned_volume(aligned_volume_name)

        data = request.parsed_obj
        db = get_db(aligned_volume_name)

        annotation_table_name = data.get("table_name")
        pcg_table_name = data.get("pcg_table_name")

        table_info = db.create_and_attach_seg_table(
            annotation_table_name, pcg_table_name)

        return table_info, 200

    @auth_required
    @client_bp.doc("get_aligned_volume_tables", security="apikey")
    def get(self, aligned_volume_name: str):
        """ Get list of annotation tables for a aligned_volume"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        tables = db._get_existing_table_ids_by_name()
        return tables, 200


@client_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/segmentations"
)
class LinkedSegmentations(Resource):
    @auth_required
    @client_bp.doc("post linked annotations", security="apikey")
    @accepts("SegmentationDataSchema", schema=SegmentationDataSchema, api=client_bp)
    def post(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Insert linked segmentations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        segmentations = data.get("segmentations")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)
        try:
            db.insert_linked_segmentation(table_name,
                                          pcg_table_name,
                                          segmentations)
        except Exception as error:
            logging.error(f"INSERT FAILED {segmentations}")
            abort(404, error)

        return f"Inserted {len(segmentations)} annotations", 200
@client_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/annotations"
)
class LinkedAnnotations(Resource):
    @auth_required
    @client_bp.doc("get linked annotations", security="apikey")
    @client_bp.expect(annotation_parser)
    def get(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Get annotations and segmentation from list of IDs"""
        check_aligned_volume(aligned_volume_name)
        args = annotation_parser.parse_args()

        ids = args["annotation_ids"]
        pcg_table_name = args["pcg_table_name"]

        db = get_db(aligned_volume_name)
        annotations = db.get_linked_annotations(table_name,
                                                pcg_table_name,
                                                ids)

        if annotations is None:
            msg = f"annotation_id {ids} not in {table_name}"
            abort(404, msg)

        return annotations, 200

    @auth_required
    @client_bp.doc("post linked annotations", security="apikey")
    @accepts("PostPutAnnotationSchema", schema=PostPutAnnotationSchema, api=client_bp)
    def post(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Insert linked annotations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        annotations = data.get("annotations")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)
        try:
            db.insert_linked_annotations(table_name,
                                         pcg_table_name,
                                         annotations)
        except Exception as error:
            logging.error(f"INSERT FAILED {annotations}")
            abort(404, error)

        return f"Inserted {len(annotations)} annotations", 200

    @auth_required
    @client_bp.doc("update linked annotations", security="apikey")
    @accepts("PostPutAnnotationSchema", schema=PostPutAnnotationSchema, api=client_bp)
    def put(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Update linked annotations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        annotations = data.get("annotations")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)

        metadata = db.get_table_metadata(aligned_volume_name, table_name)
        schema = metadata.get("schema_type")

        if schema:
            for annotation in annotations:
                db.update_linked_annotations(table_name,
                                             pcg_table_name,
                                             annotations)

        return f"Updated {len(data)} annotations", 200

    @auth_required
    @client_bp.doc("delete linked annotations", security="apikey")
    @accepts("GetDeleteAnnotationSchema", schema=GetDeleteAnnotationSchema, api=client_bp)
    def delete(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Delete linked annotations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj

        ids = data.get("annotation_ids")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)

        for anno_id in ids:
            ann = db.delete_linked_annotation(table_name,
                                              pcg_table_name,
                                              ids)

        if ann is None:
            msg = f"annotation_id {ids} not in {table_name}"
            abort(404, msg)

        return ann, 200
