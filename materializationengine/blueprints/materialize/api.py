import logging

import requests
from flask import abort, current_app
from flask_restx import Namespace, Resource, inputs, reqparse
from materializationengine.database import (create_session,
                                            dynamic_annotation_cache,
                                            sqlalchemy_cache)
from materializationengine.info_client import get_aligned_volumes
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.models import AnalysisTable, AnalysisVersion, Base
from materializationengine.schemas import (AnalysisTableSchema,
                                           AnalysisVersionSchema)
from middle_auth_client import auth_required, auth_requires_admin
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import NoSuchTableError

__version__ = "1.4.2"


bulk_upload_parser = reqparse.RequestParser()
bulk_upload_parser.add_argument('column_mapping', required=True, type=dict, location='json')
bulk_upload_parser.add_argument('project', required=True, type=str)
bulk_upload_parser.add_argument('file_path', required=True, type=str)
bulk_upload_parser.add_argument('schema', required=True, type=str)
bulk_upload_parser.add_argument('materialized_ts', type=float)

missing_chunk_parser = reqparse.RequestParser()
missing_chunk_parser.add_argument('chunks', required=True, type=list, location='json')
missing_chunk_parser.add_argument('column_mapping', required=True, type=dict, location='json')
missing_chunk_parser.add_argument('project', required=True, type=str)
missing_chunk_parser.add_argument('file_path', required=True, type=str)
missing_chunk_parser.add_argument('schema', required=True, type=str)

get_roots_parser = reqparse.RequestParser()
get_roots_parser.add_argument('find_all_expired_roots', default=False, type=inputs.boolean)

materialize_parser = reqparse.RequestParser()
materialize_parser.add_argument('days_to_expire', required=True, default=None, type=int)


authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'query',
        'name': 'middle_auth_token'
    }
}

mat_bp = Namespace("Materialization Engine",
                   authorizations=authorizations,
                   description="Materialization Engine")


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")


def get_datastack_info(datastack_name: str) -> dict:
        INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
        url = INFOSERVICE_ENDPOINT + f"/api/v2/datastack/full/{datastack_name}"
        try:
            auth_header = {"Authorization": f"Bearer {current_app.config['AUTH_TOKEN']}"}
            r = requests.get(url, headers=auth_header)
            r.raise_for_status()
            logging.info(url)
            datastack_info = r.json()
            datastack_info['datastack'] = datastack_name
            return datastack_info
        except requests.exceptions.RequestException as e:
            logging.error(f"ERROR {e}. Cannot connect to {INFOSERVICE_ENDPOINT}")

@mat_bp.route("/celery/test/<int:iterator_length>")
class TestWorkflowResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("Test workflow pattern", security="apikey")
    def post(self, iterator_length: int=50):
        """Test workflow

        Args:
            iterator_length (int): Number of parallel tasks to run. Default = 50
        """
        from materializationengine.workflows.dummy_workflow import \
            start_test_workflow
        status = start_test_workflow.s(iterator_length).apply_async()
        return 200

@mat_bp.route("/celery/status/queue")
class QueueResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("Get task queue size", security="apikey")
    def get(self):
        """Get queued tasks for celery workers
        """
        from materializationengine.celery_worker import get_celery_queue_items
        status = get_celery_queue_items('process')
        return status

@mat_bp.route("/celery/status/info")
class CeleryResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("Get celery worker status", security="apikey")
    def get(self):
        """Get celery worker info
        """
        from materializationengine.celery_worker import \
            get_celery_worker_status
        status = get_celery_worker_status()
        return status

@mat_bp.route("/materialize/run/ingest_annotations/datastack/<string:datastack_name>")
class ProcessNewAnnotationsResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("process new annotations workflow", security="apikey")
    def post(self, datastack_name: str):
        """Process newly added annotations and lookup segmentation data

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.ingest_new_annotations import \
            process_new_annotations_workflow
        datastack_info = get_datastack_info(datastack_name)
        process_new_annotations_workflow.s(datastack_info).apply_async()
        return 200
        

@mat_bp.route("/materialize/run/complete_workflow/datastack/<string:datastack_name>")
class CompleteWorkflowResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.expect(materialize_parser)
    @mat_bp.doc("ingest segmentations > update roots and freeze materialization", security="apikey")
    def post(self, datastack_name: str):
        """Create versioned materialization, finds missing segmentations and updates roots

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.complete_workflow import \
            run_complete_workflow
        datastack_info = get_datastack_info(datastack_name)
        args = materialize_parser.parse_args()
        days_to_expire = args["days_to_expire"]
        
        datastack_info['database_expires'] = days_to_expire  

        run_complete_workflow.s(datastack_info, days_to_expire).apply_async()
        return 200


@mat_bp.route("/materialize/run/create_frozen/datastack/<string:datastack_name>")
class CreateFrozenMaterializationResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.expect(materialize_parser)
    @mat_bp.doc("create frozen materialization", security="apikey")
    def post(self, datastack_name: str):
        """Create a new frozen (versioned) materialization

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.create_frozen_database import \
            create_versioned_materialization_workflow
        args = materialize_parser.parse_args()
        days_to_expire = args["days_to_expire"]       

        datastack_info = get_datastack_info(datastack_name)
        create_versioned_materialization_workflow.s(datastack_info, days_to_expire).apply_async()
        return 200

@mat_bp.route("/materialize/run/update_roots/datastack/<string:datastack_name>")
class UpdateExpiredRootIdsResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.expect(get_roots_parser)
    @mat_bp.doc("update expired root ids", security="apikey")
    def post(self, datastack_name: str):
        """Update expired root ids

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.update_root_ids import \
            expired_root_id_workflow
        datastack_info = get_datastack_info(datastack_name)   

        args = get_roots_parser.parse_args()     
        datastack_info['find_all_expired_roots'] = args["find_all_expired_roots"]

        expired_root_id_workflow.s(datastack_info).apply_async()
        return 200
        


@mat_bp.expect(bulk_upload_parser)
@mat_bp.route("/bulk_upload/upload/<string:datastack_name>/<string:table_name>/<string:segmentation_source>/<string:description>")
class BulkUploadResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("bulk upload", security="apikey")
    def post(self, datastack_name: str, table_name: str, segmentation_source: str, description: str):
        """Run bulk upload from npy files

        Args:
            column_mappings (dict): dict mapping file names to column names in database
            project (str): bucket project path
            file_path (str): bucket project path
            schema (str): type of schema from emannotationschemas
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table in database to create
            segmentation_source (str): source of segmentation data
            description (str): text field added to annotation metadata table for reference
        """
        from materializationengine.workflows.bulk_upload import gcs_bulk_upload_workflow

        args = bulk_upload_parser.parse_args()

        bulk_upload_info = get_datastack_info(datastack_name)

        bulk_upload_info.update({
            'column_mapping': args['column_mapping'],
            'project': args['project'],
            'file_path': args['file_path'],
            'schema': args['schema'],
            'datastack': datastack_name,
            'description': description,
            'annotation_table_name': table_name,
            'segmentation_source': segmentation_source,
            'materialized_ts': args['materialized_ts']
        })
        gcs_bulk_upload_workflow.s(bulk_upload_info).apply_async()
        return f"Datastack upload info : {bulk_upload_info}", 200

@mat_bp.expect(missing_chunk_parser)
@mat_bp.route("/bulk_upload/missing_chunks/<string:datastack_name>/<string:table_name>/<string:segmentation_source>/<string:description>")
class InsertMissingChunks(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("insert missing chunks", security="apikey")
    def post(self, datastack_name: str, table_name: str, segmentation_source: str, description: str):
        """Insert missing chunks of data into database

        Args:
            chunks (list): list mapping file names to column names in database
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table in database to create
            segmentation_source (str): source of segmentation data
            description (str): text field added to annotation metadata table for reference

        """
        from materializationengine.workflows.bulk_upload import \
            gcs_insert_missing_data

        args = missing_chunk_parser.parse_args()

        bulk_upload_info = get_datastack_info(datastack_name)              
        bulk_upload_info.update({
            'chunks': args['chunks'],
            'column_mapping': args['column_mapping'],
            'project': args['project'],
            'file_path': args['file_path'],
            'schema': args['schema'],
            'datastack': datastack_name,
            'description': description,
            'annotation_table_name': table_name,
            'segmentation_source': segmentation_source,
        })
        gcs_insert_missing_data.s(bulk_upload_info).apply_async()
        return f"Uploading : {datastack_name}", 200

@mat_bp.route("/aligned_volume/<aligned_volume_name>")
class DatasetResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("get_aligned_volume_versions", security="apikey")
    def get(self, aligned_volume_name: str):
        db = dynamic_annotation_cache.get(aligned_volume_name)
        response = db.session.query(AnalysisVersion.datastack).distinct()
        aligned_volumes = [r._asdict() for r in response]
        return aligned_volumes

@mat_bp.route("/aligned_volumes/<aligned_volume_name>")
class VersionResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("get_analysis_versions", security="apikey")
    def get(self, aligned_volume_name):
        check_aligned_volume(aligned_volume_name)
        session = sqlalchemy_cache.get(aligned_volume_name)
        
        response = (
            session.query(AnalysisVersion).filter(AnalysisVersion.datastack == aligned_volume_name).all()
        )
        schema = AnalysisVersionSchema(many=True)
        versions, error = schema.dump(response)
        logging.info(versions)
        if versions:
            return versions, 200
        else:
            logging.error(error)
            return abort(404)
    
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("setup new aligned volume database", security="apikey")
    def post(self, aligned_volume_name: str):
        """Create an aligned volume database
        Args:
            aligned_volume_name (str): name of aligned_volume from infoservice
        """
        check_aligned_volume(aligned_volume_name)
        aligned_vol_db = dynamic_annotation_cache.get_db(aligned_volume_name)

        base = Base
        base.metadata.bind = aligned_vol_db.engine
        base.metadata.create_all()
        return 200



@mat_bp.route("/aligned_volumes/<aligned_volume_name>/version/<version>")
class TableResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("get_all_tables", security="apikey")
    def get(self, aligned_volume_name, version):
        check_aligned_volume(aligned_volume_name)
        session = sqlalchemy_cache.get(aligned_volume_name)

        response = (
            session.query(AnalysisTable)
            .filter(AnalysisTable.analysisversion)
            .filter(AnalysisVersion.version == version)
            .filter(AnalysisVersion.datastack == aligned_volume_name)
            .all()
        )
        schema = AnalysisTableSchema(many=True)
        tables, error = schema.dump(response)
        if tables:
            return tables, 200
        else:
            logging.error(error)
            return abort(404)

@mat_bp.route("/aligned_volumes/<aligned_volume_name>/version/<version>/tablename/<tablename>")
class AnnotationResource(Resource):
    @reset_auth
    @auth_requires_admin
    @mat_bp.doc("get_top_materialized_annotations", security="apikey")
    def get(self, aligned_volume_name, version, tablename):
        check_aligned_volume(aligned_volume_name)
        SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]
        sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume_name}")
        session, engine = create_session(sql_uri)
        metadata = MetaData()
        try:
            annotation_table = Table(tablename, metadata, autoload=True, autoload_with=engine)
        except NoSuchTableError as e:
            logging.error(f"No table exists {e}")
            return abort(404)
        response = session.query(annotation_table).limit(10).all()
        annotations = [r._asdict() for r in response]
        if annotations:
            return annotations, 200
        else:
            return abort(404)
