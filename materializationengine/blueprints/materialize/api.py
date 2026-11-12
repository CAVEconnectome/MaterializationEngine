import datetime
import json
import re
import logging
import os
import shlex

import cloudfiles
import redis
from dynamicannotationdb.models import AnalysisTable, AnalysisVersion, Base
from flask import abort, current_app, jsonify, request
from flask_accepts import accepts
from flask_restx import Namespace, Resource, fields, inputs, reqparse
from middle_auth_client import (
    auth_requires_admin,
    auth_requires_dataset_admin,
    auth_requires_permission,
)
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import NoSuchTableError

from materializationengine.blueprints.client.utils import get_latest_version
from materializationengine.blueprints.materialize.schemas import (
    AnnotationIDListSchema,
    BadRootsSchema,
    VirtualVersionSchema,
)
from materializationengine.cloudsql_admin import CloudSQLAdminError, export_csv
from materializationengine.database import (
    db_manager,
    dynamic_annotation_cache,
)
from materializationengine.info_client import (
    get_aligned_volumes,
    get_datastack_info,
    get_relevant_datastack_info,
)
from materializationengine.schemas import AnalysisTableSchema, AnalysisVersionSchema
from materializationengine.utils import check_write_permission, get_config_param

__version__ = "5.37.0"


bulk_upload_parser = reqparse.RequestParser()
bulk_upload_parser.add_argument(
    "column_mapping", required=True, type=dict, location="json"
)
bulk_upload_parser.add_argument("project", required=True, type=str)
bulk_upload_parser.add_argument("file_path", required=True, type=str)
bulk_upload_parser.add_argument("schema", required=True, type=str)
bulk_upload_parser.add_argument("materialized_ts", type=float)


missing_chunk_parser = reqparse.RequestParser()
missing_chunk_parser.add_argument("chunks", required=True, type=list, location="json")
missing_chunk_parser.add_argument(
    "column_mapping", required=True, type=dict, location="json"
)
missing_chunk_parser.add_argument("project", required=True, type=str)
missing_chunk_parser.add_argument("file_path", required=True, type=str)
missing_chunk_parser.add_argument("schema", required=True, type=str)

get_roots_parser = reqparse.RequestParser()
get_roots_parser.add_argument("lookup_all_root_ids", default=False, type=inputs.boolean)

materialize_parser = reqparse.RequestParser()
materialize_parser.add_argument("days_to_expire", required=True, default=None, type=int)
materialize_parser.add_argument("merge_tables", required=True, type=inputs.boolean)

authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

mat_bp = Namespace(
    "Materialization Engine",
    authorizations=authorizations,
    description="Materialization Engine",
)


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")


@mat_bp.route("/celery/test/<int:iterator_length>")
class TestWorkflowResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Test workflow pattern", security="apikey")
    def post(self, iterator_length: int = 50):
        """Test workflow

        Args:
            iterator_length (int): Number of parallel tasks to run. Default = 50
        """
        from materializationengine.workflows.dummy_workflow import start_test_workflow

        status = start_test_workflow.s(iterator_length).apply_async()
        return 200


@mat_bp.route("/workflow/status/active")
class ActiveTasksResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Get current actively running tasks", security="apikey")
    def get(self):
        """Get running tasks from celery"""
        from materializationengine.celery_worker import get_activate_tasks

        return get_activate_tasks()


@mat_bp.route("/workflow/status/locks")
class LockedTasksResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Get locked tasks", security="apikey")
    def get(self):
        """Get locked tasks from redis"""
        from materializationengine.celery_worker import inspect_locked_tasks

        ltdict = inspect_locked_tasks(release_locks=False)
        return {str(k): v for k, v in ltdict.items()}

    @auth_requires_admin
    @mat_bp.doc("Unlock locked tasks", security="apikey")
    def put(self):
        "Unlock locked tasks"
        from materializationengine.celery_worker import inspect_locked_tasks

        ltdict = inspect_locked_tasks(release_locks=True)
        return {str(k): v for k, v in ltdict.items()}


maintenance_parser = reqparse.RequestParser()
maintenance_parser.add_argument("min_rows", type=int, default=100000, location="args")
maintenance_parser.add_argument("max_correlation", type=float, default=None, location="args")

repack_parser = reqparse.RequestParser()
repack_parser.add_argument("dry_run", type=inputs.boolean, default=True, location="args",
                           help="report what pg_repack would do without changing anything (default true)")
repack_parser.add_argument("order_by", type=str, default="id", location="args")
repack_parser.add_argument("jobs", type=int, default=2, location="args",
                           help="parallel index builds (connections)")
repack_parser.add_argument("fillfactor", type=int, default=None, location="args",
                           help="set before repacking, e.g. 90 for tables whose rows are updated")
repack_parser.add_argument("force", type=inputs.boolean, default=False, location="args",
                           help="allow tables larger than max_table_gb")
repack_parser.add_argument("max_table_gb", type=float, default=100, location="args")
repack_parser.add_argument("wait_timeout", type=int, default=60, location="args",
                           help="seconds pg_repack waits for its brief locks before giving up")


@mat_bp.route("/maintenance/table_order/<string:database>")
class TableOrderResource(Resource):
    @auth_requires_admin
    @mat_bp.expect(maintenance_parser)
    @mat_bp.doc("Report how well tables are ordered by id", security="apikey")
    def get(self, database: str):
        """Annotation and segmentation tables with size and id correlation (1.0 = in id order)

        Args:
            database (str): a live (aligned volume) or frozen database on this instance
        """
        from materializationengine.workflows.table_maintenance import RepackRefused, table_order_report

        args = maintenance_parser.parse_args()
        try:
            return {"database": database, "tables": table_order_report(
                database, min_rows=args["min_rows"], max_correlation=args["max_correlation"])}
        except RepackRefused as e:
            abort(400, str(e))


@mat_bp.route("/maintenance/repack/<string:database>/<string:table_name>")
class RepackTableResource(Resource):
    @auth_requires_admin
    @mat_bp.expect(repack_parser)
    @mat_bp.doc("Reorder a table by id with pg_repack", security="apikey")
    def post(self, database: str, table_name: str):
        """Queue a pg_repack of one table (dry run unless dry_run=false). Returns a job id.

        The table stays readable and writable; pg_repack takes an exclusive lock only
        briefly at the start and at the swap, and gives up rather than cancel queries
        holding the table. It needs free disk for a full copy of the table and indexes.
        """
        from materializationengine.workflows.table_maintenance import RepackRefused, start_repack

        args = repack_parser.parse_args()
        try:
            return start_repack(database, table_name, **args)
        except RepackRefused as e:
            abort(400, str(e))


@mat_bp.route("/maintenance/repack/status/<string:job_id>")
class RepackStatusResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Status of a pg_repack job", security="apikey")
    def get(self, job_id: str):
        from materializationengine.workflows.table_maintenance import get_repack_status

        status = get_repack_status(job_id)
        if status is None:
            abort(404, f"no repack job {job_id}")
        return status


@mat_bp.route("/celery/status/queue")
class QueueResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Get task queue size", security="apikey")
    def get(self):
        """Get queued tasks for celery workers"""
        from materializationengine.celery_worker import get_celery_queue_items

        status = get_celery_queue_items("process")
        return status


@mat_bp.route("/celery/status/info")
class CeleryResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Get celery worker status", security="apikey")
    def get(self):
        """Get celery worker info"""
        from materializationengine.celery_worker import get_celery_worker_status

        status = get_celery_worker_status()
        return status


@mat_bp.route("/materialize/run/ingest_annotations/datastack/<string:datastack_name>")
class ProcessNewAnnotationsResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("process new annotations workflow", security="apikey")
    def post(self, datastack_name: str):
        """Process newly added annotations and lookup segmentation data

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.ingest_new_annotations import (
            process_new_annotations_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)
        process_new_annotations_workflow.s(datastack_info).apply_async()
        return 200


@mat_bp.route(
    "/materialize/run/lookup_svid/datastack/<string:datastack_name>/<string:table_name>"
)
class ProcessNewSVIDResource(Resource):
    @auth_requires_permission("edit", table_arg="datastack_name")
    @mat_bp.doc("process new svids workflow", security="apikey")
    @accepts("AnnotationIDList", schema=AnnotationIDListSchema, api=mat_bp)
    def post(self, datastack_name: str, table_name: str):
        """Process newly added annotations and lookup supervoxel data

        Args:
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table
        """
        from materializationengine.workflows.ingest_new_annotations import (
            ingest_table_svids,
        )

        if datastack_name not in current_app.config["DATASTACKS"]:
            abort(404, f"datastack {datastack_name} not configured for materialization")
        annotation_ids = request.parsed_obj.get("annotation_ids", None)
        datastack_info = get_datastack_info(datastack_name)

        info = ingest_table_svids.s(
            datastack_info, table_name, annotation_ids
        ).apply_async()
        return 200


@mat_bp.route(
    "/materialize/run/ingest_annotations/datastack/<string:datastack_name>/<string:table_name>"
)
class ProcessNewAnnotationsTableResource(Resource):
    @auth_requires_permission("edit", table_arg="datastack_name")
    @mat_bp.doc("process new annotations workflow", security="apikey")
    def post(self, datastack_name: str, table_name: str):
        """Process newly added annotations and lookup segmentation data

        Args:
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table
        """
        from materializationengine.workflows.ingest_new_annotations import (
            process_new_annotations_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)
        db = dynamic_annotation_cache.get_db(datastack_info["aligned_volume"]["name"])
        check_write_permission(db, table_name)

        process_new_annotations_workflow.s(
            datastack_info, table_name=table_name
        ).apply_async()
        return 200


@mat_bp.route("/materialize/run/lookup_root_ids/datastack/<string:datastack_name>")
class LookupMissingRootIdsResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("Find all null root ids and lookup new roots", security="apikey")
    def post(self, datastack_name: str):
        """Run workflow to lookup missing root ids and insert into database across
        all tables in the database.

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.ingest_new_annotations import (
            process_dense_missing_roots_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)
        process_dense_missing_roots_workflow.s(datastack_info).apply_async()
        return 200


@mat_bp.route(
    "/materialize/run/sparse_lookup_root_ids/datastack/<string:datastack_name>/table/<string:table_name>"
)
class LookupSparseMissingRootIdsResource(Resource):
    @auth_requires_admin
    @mat_bp.doc(
        "Find null root ids in table and lookup new root ids", security="apikey"
    )
    def post(self, datastack_name: str, table_name: str):
        """Finds null root ids in a given table and lookups new root ids
        using last updated time stamp.

        Args:
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table
        """
        from materializationengine.workflows.ingest_new_annotations import (
            process_sparse_missing_roots_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)
        process_sparse_missing_roots_workflow.s(
            datastack_info, table_name
        ).apply_async()
        return 200


@mat_bp.route(
    "/materialize/run/lookup_all_root_ids/datastack/<string:datastack_name>/table/<string:table_name>"
)
class LookupAllRootIdsResource(Resource):
    @auth_requires_admin
    @mat_bp.doc(
        "Look up every root id of a table from its supervoxel ids", security="apikey"
    )
    def post(self, datastack_name: str, table_name: str):
        """Look up every root id of a table from its supervoxel ids, as of now, and write
        the ones that changed: for a table whose root ids may be stale without knowing
        which. Run it when the datastack's regular updates are not running.

        Args:
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table
        """
        from materializationengine.workflows.update_root_ids import (
            lookup_all_root_ids_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)
        lookup_all_root_ids_workflow.s(datastack_info, table_name).apply_async()
        return 200


@mat_bp.route(
    "/materialize/run/remove_bad_root_ids/datastack/<string:datastack_name>/table/<string:table_name>"
)
class SetBadRootsToNullResource(Resource):
    @auth_requires_admin
    @accepts("BadRootsSchema", schema=BadRootsSchema, api=mat_bp)
    @mat_bp.doc("set bad roots to None", security="apikey")
    def post(self, datastack_name: str, table_name: str):
        """Run workflow to lookup missing root ids and insert into database

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.ingest_new_annotations import (
            fix_root_id_workflow,
        )

        data = request.parsed_obj
        bad_roots_ids = data["bad_roots"]

        datastack_info = get_datastack_info(datastack_name)
        fix_root_id_workflow.s(datastack_info, table_name, bad_roots_ids).apply_async()
        return 200


@mat_bp.route("/materialize/run/complete_workflow/datastack/<string:datastack_name>")
class CompleteWorkflowResource(Resource):
    @auth_requires_admin
    @mat_bp.expect(materialize_parser)
    @mat_bp.doc(
        "ingest segmentations > update roots and freeze materialization",
        security="apikey",
    )
    def post(self, datastack_name: str):
        """Create versioned materialization, finds missing segmentations and updates roots

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.complete_workflow import (
            run_complete_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)

        args = materialize_parser.parse_args()
        days_to_expire = args["days_to_expire"]
        merge_tables = args["merge_tables"]

        datastack_info["database_expires"] = days_to_expire
        datastack_info["merge_tables"] = merge_tables
        run_complete_workflow.s(
            datastack_info, days_to_expire, merge_tables
        ).apply_async()
        return 200


@mat_bp.route("/materialize/run/create_frozen/datastack/<string:datastack_name>")
class CreateFrozenMaterializationResource(Resource):
    @auth_requires_admin
    @mat_bp.expect(materialize_parser)
    @mat_bp.doc("create frozen materialization", security="apikey")
    def post(self, datastack_name: str):
        """Create a new frozen (versioned) materialization

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.create_frozen_database import (
            create_versioned_materialization_workflow,
        )

        args = materialize_parser.parse_args()
        days_to_expire = args["days_to_expire"]
        merge_tables = args["merge_tables"]

        datastack_info = get_datastack_info(datastack_name)
        create_versioned_materialization_workflow.s(
            datastack_info, days_to_expire, merge_tables
        ).apply_async()
        return 200


@mat_bp.route("/materialize/run/update_roots/datastack/<string:datastack_name>")
class UpdateExpiredRootIdsResource(Resource):
    @auth_requires_admin
    @mat_bp.expect(get_roots_parser)
    @mat_bp.doc("Update expired root ids", security="apikey")
    def post(self, datastack_name: str):
        """Lookup root ids

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.update_root_ids import (
            expired_root_id_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)

        args = get_roots_parser.parse_args()
        datastack_info["lookup_all_root_ids"] = args["lookup_all_root_ids"]

        expired_root_id_workflow.s(datastack_info).apply_async()
        return 200


response_model = mat_bp.model(
    "Response",
    {
        "message": fields.String(description="Response message"),
        "csv_path": fields.String(description="Path to csv file", required=False),
        "header_path": fields.String(description="Path to header file", required=False),
    },
)


@mat_bp.route(
    "/materialize/run/dump_csv_table/datastack/<string:datastack_name>/version/<int(signed=True):version>/table_name/<string:table_name>/"
)
class DumpTableToBucketAsCSV(Resource):
    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Take table or view and dump it to a bucket as csv", security="apikey")
    @mat_bp.response(200, "Success", response_model)
    @mat_bp.response(500, "Internal Server Error", response_model)
    def post(self, datastack_name: str, version: int, table_name: str):
        """Dump table to bucket as csv

        Args:
            datastack_name (str): name of datastack from infoservice
            version (int): version of datastack
            table_name (str): name of table or view to dump
        """
        mat_db_name = f"{datastack_name}__mat{version}"
        # get the segmentation table name of the table_name

        # TODO: add validation of parameters
        sql_instance_name = current_app.config.get("SQL_INSTANCE_NAME", None)
        if not sql_instance_name:
            return {"message": "SQL_INSTANCE_NAME not set in app config"}, 500

        bucket = current_app.config.get("MATERIALIZATION_DUMP_BUCKET", None)
        if not bucket:
            return {"message": "MATERIALIZATION_DUMP_BUCKET not set in app config"}, 500

        if version == -1:
            version = get_latest_version(datastack_name)

        cf = cloudfiles.CloudFiles(bucket)
        filename = f"{datastack_name}/v{version}/{table_name}.csv.gz"

        cloudpath = os.path.join(bucket, filename)
        header_file = f"{datastack_name}/v{version}/{table_name}_header.csv"
        header_cloudpath = os.path.join(bucket, header_file)

        # check if the file already exists
        if cf.exists(filename):
            # return a flask respoonse 200 message that says that the file already exitss
            return {
                "message": "file already created",
                "csv_path": cloudpath,
                "header_path": header_cloudpath,
            }, 200

        else:
            header_query = (
                "SELECT column_name, data_type from INFORMATION_SCHEMA.COLUMNS "
                f"where TABLE_NAME = '{table_name}'"
            )
            try:
                # Cloud SQL runs one operation at a time per instance, so wait for the
                # header export before starting the table export.
                export_csv(sql_instance_name, header_cloudpath, mat_db_name, header_query)
            except (CloudSQLAdminError, TimeoutError) as e:
                return {"message": f"header file failed to create: {e}"}, 500

            try:
                export_csv(
                    sql_instance_name,
                    cloudpath,
                    mat_db_name,
                    f"SELECT * from {table_name}",
                    wait=False,
                )
            except CloudSQLAdminError as e:
                return {"message": f"file failed to create: {e}"}, 500

            return {
                "message": "file created sucessefully",
                "csv_path": cloudpath,
                "header_path": header_cloudpath,
            }, 200


@mat_bp.route("/materialize/run/update_database/datastack/<string:datastack_name>")
class UpdateLiveDatabaseResource(Resource):
    @auth_requires_admin
    @mat_bp.expect(get_roots_parser)
    @mat_bp.doc("Ingest new annotations and update expired root ids", security="apikey")
    def post(self, datastack_name: str):
        """Ingest new annotations and update expired root ids

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.update_database_workflow import (
            update_database_workflow,
        )

        datastack_info = get_datastack_info(datastack_name)

        args = get_roots_parser.parse_args()
        datastack_info["lookup_all_root_ids"] = args["lookup_all_root_ids"]

        update_database_workflow.s(datastack_info).apply_async()
        return 200


@mat_bp.expect(bulk_upload_parser)
@mat_bp.route(
    "/bulk_upload/upload/<string:datastack_name>/<string:table_name>/<string:segmentation_source>/<string:description>"
)
class BulkUploadResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("bulk upload", security="apikey")
    def post(
        self,
        datastack_name: str,
        table_name: str,
        segmentation_source: str,
        description: str,
    ):
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

        bulk_upload_info.update(
            {
                "column_mapping": args["column_mapping"],
                "project": args["project"],
                "file_path": args["file_path"],
                "schema": args["schema"],
                "datastack": datastack_name,
                "description": description,
                "annotation_table_name": table_name,
                "segmentation_source": segmentation_source,
                "materialized_ts": args["materialized_ts"],
            }
        )
        gcs_bulk_upload_workflow.s(bulk_upload_info).apply_async()
        return f"Datastack upload info : {bulk_upload_info}", 200


@mat_bp.expect(missing_chunk_parser)
@mat_bp.route(
    "/bulk_upload/missing_chunks/<string:datastack_name>/<string:table_name>/<string:segmentation_source>/<string:description>"
)
class InsertMissingChunks(Resource):
    @auth_requires_admin
    @mat_bp.doc("insert missing chunks", security="apikey")
    def post(
        self,
        datastack_name: str,
        table_name: str,
        segmentation_source: str,
        description: str,
    ):
        """Insert missing chunks of data into database

        Args:
            chunks (list): list mapping file names to column names in database
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table in database to create
            segmentation_source (str): source of segmentation data
            description (str): text field added to annotation metadata table for reference

        """
        from materializationengine.workflows.bulk_upload import gcs_insert_missing_data

        args = missing_chunk_parser.parse_args()

        bulk_upload_info = get_datastack_info(datastack_name)
        bulk_upload_info.update(
            {
                "chunks": args["chunks"],
                "column_mapping": args["column_mapping"],
                "project": args["project"],
                "file_path": args["file_path"],
                "schema": args["schema"],
                "datastack": datastack_name,
                "description": description,
                "annotation_table_name": table_name,
                "segmentation_source": segmentation_source,
            }
        )
        gcs_insert_missing_data.s(bulk_upload_info).apply_async()
        return f"Uploading : {datastack_name}", 200


@mat_bp.route("/aligned_volume/<aligned_volume_name>")
class DatasetResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("get_aligned_volume_versions", security="apikey")
    def get(self, aligned_volume_name: str):
        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        response = db.database.cached_session.query(
            AnalysisVersion.datastack
        ).distinct()
        aligned_volumes = [r._asdict() for r in response]
        return aligned_volumes


@mat_bp.route("/aligned_volumes/<aligned_volume_name>")
class VersionResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("get_analysis_versions", security="apikey")
    def get(self, aligned_volume_name):
        check_aligned_volume(aligned_volume_name)
        with db_manager.session_scope(aligned_volume_name) as session:
            response = (
                session.query(AnalysisVersion)
                .filter(AnalysisVersion.datastack == aligned_volume_name)
                .all()
            )
            schema = AnalysisVersionSchema(many=True)
            versions, error = schema.dump(response)
        logging.info(versions)
        if versions:
            return versions, 200
        else:
            logging.error(error)
            return abort(404)

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
        base.metadata.bind = aligned_vol_db.database.engine
        base.metadata.create_all()
        return 200


@mat_bp.route("/aligned_volumes/<aligned_volume_name>/version/<version>")
class TableResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("get_all_tables", security="apikey")
    def get(self, aligned_volume_name, version):
        check_aligned_volume(aligned_volume_name)

        with db_manager.session_scope(aligned_volume_name) as session:
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


@mat_bp.route(
    "/aligned_volumes/<string:aligned_volume_name>/version/<int:version>/tablename/<string:tablename>"
)
class AnnotationResource(Resource):
    @auth_requires_admin
    @mat_bp.doc("get_top_materialized_annotations", security="apikey")
    def get(self, aligned_volume_name: str, version: int, tablename: str):
        check_aligned_volume(aligned_volume_name)

        try:
            with db_manager.session_scope(aligned_volume_name) as session:
                engine = db_manager.get_engine(aligned_volume_name)

                metadata = MetaData()
                try:
                    annotation_table = Table(
                        tablename, metadata, autoload=True, autoload_with=engine
                    )
                except NoSuchTableError as e:
                    logging.error(f"No table exists {e}")
                    return abort(404)

                response = session.query(annotation_table).limit(10).all()
                annotations = [r._asdict() for r in response]

                return (annotations, 200) if annotations else abort(404)

        except Exception as e:
            logging.error(f"Error querying annotations: {e}")
            return abort(500)


@mat_bp.route("/materialize/run/create_virtual/datastack/<string:datastack_name>")
class CreateVirtualPublicVersionResource(Resource):
    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("create virtual materialization", security="apikey")
    @accepts("VirtualVersionSchema", schema=VirtualVersionSchema, api=mat_bp)
    def post(self, datastack_name: str):
        """Create a virtual version from an existing frozen version.

        Args:
            datastack_name (str): name of datastack

        """

        data = request.parsed_obj

        target_version = data.get("target_version")
        tables_to_include = data.get("tables_to_include")
        virtual_version_name = data.get("virtual_version_name")

        aligned_volume, pcg_table_name = get_relevant_datastack_info(datastack_name)

        if not tables_to_include:
            return abort(400, "No tables included")

        with db_manager.session_scope(aligned_volume) as session:
            analysis_version = (
                session.query(AnalysisVersion)
                .filter(AnalysisVersion.version == target_version)
                .filter(AnalysisVersion.datastack == datastack_name)
                .one()
            )

            if not analysis_version.valid:
                return abort(404, f"Version {target_version} is not a valid version")

            included_tables = (
                session.query(AnalysisTable)
                .filter(AnalysisTable.analysisversion_id == analysis_version.id)
                .filter(AnalysisTable.table_name.in_(tables_to_include))
                .all()
            )
            if not included_tables:
                return abort(
                    404,
                    f"No tables {tables_to_include} found in target version {target_version}",
                )

            virtual_datastack_name = f"{virtual_version_name}"

            time_to_expire = analysis_version.expires_on - datetime.datetime.utcnow()
            if time_to_expire.days < 1000:
                expiration_timestamp = str(
                    analysis_version.expires_on + datetime.timedelta(days=36525)
                )
            else:
                expiration_timestamp = analysis_version.expires_on

            virtual_analysis_version = AnalysisVersion(
                datastack=virtual_datastack_name,
                time_stamp=analysis_version.time_stamp,
                version=analysis_version.version,
                valid=True,
                expires_on=expiration_timestamp,
                parent_version=analysis_version.id,
                status="AVAILABLE",
                is_merged=analysis_version.is_merged,
            )

            session.add(virtual_analysis_version)
            session.flush()

            for table in included_tables:
                table = AnalysisTable(
                    aligned_volume=aligned_volume,
                    schema=table.schema,
                    table_name=table.table_name,
                    valid=True,
                    created=table.created,
                    analysisversion_id=virtual_analysis_version.id,
                )
                session.add(table)
            analysis_version.expires_on = expiration_timestamp

        return f"{virtual_datastack_name} created", 200


@mat_bp.route(
    "/materialize/run/write_deltalake/datastack/<string:datastack_name>/version/<int(signed=True):version>/table_name/<string:table_name>/"
)
class WriteDeltalakeResource(Resource):
    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Export a table to Delta Lake", security="apikey")
    def post(self, datastack_name: str, version: int, table_name: str):
        """Export a frozen materialization table to partitioned Delta Lake.

        Runs as a RayJob (the parallel export, workflows/lake_export): the table is read as
        id ranges at once when it is large, each committed to each lake in one transaction.
        Optional JSON body: ``output_specs`` (a list of DeltaLakeOutputSpec objects).
        ``?workers=`` caps the Ray workers. Poll the GET with the returned ``job_id``.

        Args:
            datastack_name (str): name of datastack from infoservice
            version (int): materialization version (-1 for latest)
            table_name (str): annotation table name to export
        """
        import uuid

        from materializationengine.rayjobs import RayJobSubmissionError, ray_enabled
        from materializationengine.workflows.deltalake_export import DeltaLakeOutputSpec
        from materializationengine.workflows.lake_export import submit as lake_submit
        from materializationengine.workflows.lake_export.settings import ExportSettings

        # ?backend=celery was the escape hatch to the Celery export, which is gone
        if (request.args.get("backend") or "ray").lower() != "ray":
            return abort(400, "Delta Lake exports run on Ray only; the Celery export was removed")
        if not ray_enabled():
            return abort(501, "Ray is not enabled on this deployment")
        if version == -1:
            version = get_latest_version(datastack_name)

        # Accept optional output_specs from JSON body.
        output_specs = None
        if request.is_json and request.json:
            output_specs = request.json.get("output_specs", None)

        if output_specs is not None:
            if not isinstance(output_specs, list):
                return abort(400, "output_specs must be a list")
            for item in output_specs:
                if not isinstance(item, dict):
                    return abort(400, "each entry in output_specs must be an object")
                try:
                    DeltaLakeOutputSpec(**item)
                except (TypeError, ValueError) as exc:
                    return abort(400, f"invalid output_specs entry: {exc}")

        job_id = uuid.uuid4().hex
        settings = ExportSettings.from_config(current_app.config)
        try:
            rayjob_name = lake_submit.submit(
                settings,
                lake_submit.entrypoint(datastack_name, version, job_id, tables=[table_name],
                                       output_specs=output_specs),
                # DNS-label safe: table names allow underscores, RayJob names do not.
                name_prefix=f"mat-deltalake-{table_name.replace('_', '-')}",
                metadata={"datastack": datastack_name, "table": table_name, "version": str(version),
                          "job-id": job_id},
                num_workers=request.args.get("workers", type=int),
            )
        except RayJobSubmissionError as exc:
            return abort(500, f"failed to submit RayJob: {exc}")

        return {
            "message": f"Delta Lake export submitted as RayJob for {table_name} v{version}",
            "job_id": job_id,
            "backend": "ray",
            "rayjob_name": rayjob_name,
        }, 200

    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Get Delta Lake export progress", security="apikey")
    def get(self, datastack_name: str, version: int, table_name: str):
        """Get progress of a Delta Lake export for a table.

        Returns JSON with ``status``, ``rows_processed``, ``total_rows``,
        and ``percent_complete``.  Returns 404 if no export is tracked.

        Accepts optional ``job_id`` query parameter to poll a specific
        export job (required when multiple exports of the same table
        may exist).
        """
        from materializationengine.workflows.deltalake_export import (
            get_deltalake_export_progress,
        )

        if version == -1:
            version = get_latest_version(datastack_name)

        job_id = request.args.get("job_id", None)

        progress = get_deltalake_export_progress(
            datastack_name, version, table_name, job_id=job_id
        )
        if progress is None:
            return {
                "message": f"No export progress found for {table_name} v{version}"
            }, 404

        return progress, 200


@mat_bp.route(
    "/materialize/run/write_deltalake_version/datastack/<string:datastack_name>/version/<int(signed=True):version>/"
)
class WriteDeltalakeVersionResource(Resource):
    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Export every table of a version to Delta Lake (one RayJob)", security="apikey")
    def post(self, datastack_name: str, version: int):
        """Export all of a frozen version's tables to Delta Lake as one RayJob.

        Every table's id ranges share one budget of Postgres streams (the parallel export,
        workflows/lake_export); then, with ``views``, the views stage builds the version's
        views from the table lakes (DELTALAKE_EXPORT_VIEWS_STAGE).

        JSON body (all optional): ``tables`` (list; default all), ``views`` (bool),
        ``max_streams`` (Postgres streams at once; default DELTALAKE_EXPORT_MAX_STREAMS),
        ``config`` (a DELTALAKE_EXPORT_CONFIG object for this run), ``name_suffix``
        (appended to every output spec name, to export a version again beside an earlier
        export). ``?workers=`` caps the Ray workers.
        """
        import uuid

        from materializationengine.rayjobs import RayJobSubmissionError, ray_enabled
        from materializationengine.workflows.deltalake_version_export import (
            load_export_config,
            set_version_status,
        )
        from materializationengine.workflows.lake_export import submit as lake_submit
        from materializationengine.workflows.lake_export.settings import ExportSettings

        if not ray_enabled():
            return abort(501, "Ray is not enabled on this deployment")
        if version == -1:
            version = get_latest_version(datastack_name)
        body = request.json if request.is_json and request.json else {}
        if not isinstance(body, dict):
            return abort(400, "body must be a JSON object")
        if "include_views" in body:
            return abort(400, "include_views was removed: views are no longer exported from SQL; "
                              "pass views: true to run the views stage after the tables")
        tables = body.get("tables")
        if tables is not None and (not isinstance(tables, list) or not all(isinstance(t, str) for t in tables)):
            return abort(400, "tables must be a list of table names")
        views = body.get("views", False)
        if not isinstance(views, bool):
            return abort(400, "views must be true or false")
        max_streams = body.get("max_streams")
        if max_streams is not None and (not isinstance(max_streams, int) or isinstance(max_streams, bool)
                                        or max_streams < 1):
            return abort(400, "max_streams must be a positive integer")
        name_suffix = body.get("name_suffix")
        if name_suffix is not None and not re.fullmatch(r"[A-Za-z0-9_]+", str(name_suffix)):
            return abort(400, "name_suffix may contain only letters, digits and underscores")
        config = body.get("config")
        if config is not None:
            try:
                load_export_config(config)
            except (TypeError, ValueError) as exc:
                return abort(400, f"invalid config: {exc}")

        job_id = uuid.uuid4().hex
        settings = ExportSettings.from_config(current_app.config)
        try:
            rayjob_name = lake_submit.submit(
                settings,
                lake_submit.entrypoint(datastack_name, version, job_id, tables=tables, version_job=True,
                                       views=views, config=config, name_suffix=name_suffix,
                                       max_streams=max_streams),
                name_prefix=f"mat-deltalake-v{int(version)}",
                metadata={"datastack": datastack_name, "version": str(version), "job-id": job_id},
                max_streams=max_streams,
                num_workers=request.args.get("workers", type=int),
            )
        except RayJobSubmissionError as exc:
            return abort(500, f"failed to submit RayJob: {exc}")
        set_version_status(datastack_name, version, job_id, state="submitted", rayjob=rayjob_name)
        return {"message": f"Delta Lake export of {datastack_name} v{version} submitted as RayJob",
                "job_id": job_id, "rayjob_name": rayjob_name}, 200

    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Progress of a version's Delta Lake export", security="apikey")
    def get(self, datastack_name: str, version: int):
        """State of each table of a version export (``?job_id=`` from the POST)."""
        from materializationengine.workflows.deltalake_version_export import get_version_status

        job_id = request.args.get("job_id")
        if not job_id:
            return abort(400, "job_id is required")
        status = get_version_status(datastack_name, version, job_id)
        if status is None:
            return abort(404, f"no version export {job_id} for {datastack_name} v{version}")
        return status, 200


@mat_bp.route(
    "/materialize/run/write_deltalake_version/datastack/<string:datastack_name>/version/<int(signed=True):version>/catalog/"
)
class RegisterDeltalakeVersionResource(Resource):
    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Register a version's complete Delta lakes with the catalog service", security="apikey")
    def post(self, datastack_name: str, version: int):
        """Register every complete lake of a version (``?table_name=`` for one table) with
        the catalog service. Exports do this themselves; this backfills lakes exported
        before CATALOG_SERVICE_URL was set, or whose registration failed."""
        from materializationengine import catalog_service

        if not catalog_service.enabled():
            return abort(501, "CATALOG_SERVICE_URL is not set on this deployment")
        if version == -1:
            version = get_latest_version(datastack_name)
        datastack_info = get_datastack_info(datastack_name)
        result = catalog_service.register_version(
            datastack_info["aligned_volume"]["name"], datastack_name, version,
            table_name=request.args.get("table_name"))
        return {"version": version, **result}, 200


@mat_bp.route(
    "/materialize/run/view_pilot/datastack/<string:datastack_name>/version/<int(signed=True):version>/"
)
class ViewPilotResource(Resource):
    @auth_requires_dataset_admin(table_arg="datastack_name")
    @mat_bp.doc("Build views with Postgres, DuckDB and Daft, and compare (one RayJob)", security="apikey")
    def post(self, datastack_name: str, version: int):
        """Build each view with each engine into lakes under <version>/_pilot/<run_id>/ and
        compare them (rayjobs/entrypoints/view_pilot.py).

        JSON body: ``views`` (required), ``engines`` (default postgres, duckdb, daft; also
        duckdb_arrow, DuckDB reading through pyarrow),
        ``rounds`` (default 1), ``duckdb_memory`` (e.g. "24GB"), ``image_tag`` (default
        VIEW_PILOT_IMAGE_TAG: an image built with the views-pilot dependency group),
        ``task_resources`` (Ray options for the Postgres and DuckDB tasks, e.g.
        {"num_cpus": 3}; default one CPU, which fits any worker), ``worker`` (this job's
        worker size instead of the chart's, e.g. {"cpus": 3, "memory": "11Gi"}: it must
        fit the worker node pool's nodes).
        ``?workers=`` caps the Ray workers.
        """
        import uuid

        from materializationengine.rayjobs import RayJobSubmissionError, ray_enabled, submit_rayjob

        if not ray_enabled():
            return abort(501, "Ray is not enabled on this deployment")
        body = request.json if request.is_json and request.json else {}
        name = re.compile(r"[A-Za-z0-9_]+")
        views, engines = body.get("views"), body.get("engines") or ["postgres", "duckdb", "daft"]
        if not isinstance(views, list) or not views or not all(isinstance(v, str) and name.fullmatch(v) for v in views):
            return abort(400, "views must be a list of view names")
        if not isinstance(engines, list) or not set(engines) <= {"postgres", "duckdb", "duckdb_arrow", "daft"}:
            return abort(400, "engines must be a list of postgres, duckdb, duckdb_arrow, daft")
        rounds = body.get("rounds", 1)
        if not isinstance(rounds, int) or isinstance(rounds, bool) or not 1 <= rounds <= 10:
            return abort(400, "rounds must be an integer from 1 to 10")
        memory = body.get("duckdb_memory")
        if memory is not None and not re.fullmatch(r"[0-9]+(\.[0-9]+)?\s*[KMGT]i?B", str(memory)):
            return abort(400, "duckdb_memory must be like 24GB")
        image_tag = body.get("image_tag") or get_config_param("VIEW_PILOT_IMAGE_TAG", None)
        if image_tag is not None and not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", str(image_tag)):
            return abort(400, "image_tag is not a valid image tag")
        resources = body.get("task_resources")
        if resources is not None and not (
                isinstance(resources, dict) and resources
                and set(resources) <= {"num_cpus", "memory", "resources"}
                and all(isinstance(v, (int, float, dict)) and not isinstance(v, bool) for v in resources.values())):
            return abort(400, "task_resources must be Ray options: num_cpus, memory, resources")
        worker = body.get("worker") or {}
        if not isinstance(worker, dict):
            return abort(400, 'worker must be like {"cpus": 3, "memory": "11Gi"}')
        cpus, worker_memory = worker.get("cpus"), worker.get("memory")
        if (set(worker) - {"cpus", "memory"}
                or (cpus is not None and (not isinstance(cpus, int) or isinstance(cpus, bool) or not 1 <= cpus <= 64))
                or (worker_memory is not None and not re.fullmatch(r"[0-9]+(Mi|Gi)", str(worker_memory)))):
            return abort(400, 'worker must be like {"cpus": 3, "memory": "11Gi"}')
        if version == -1:
            version = get_latest_version(datastack_name)

        run_id = uuid.uuid4().hex[:12]
        entrypoint = (
            "python -m materializationengine.rayjobs.entrypoints.view_pilot"
            f" --datastack {shlex.quote(datastack_name)} --version {int(version)} --run-id {run_id}"
            f" --rounds {rounds} --views {' '.join(views)} --engines {' '.join(engines)}"
        )
        if memory:
            entrypoint += f" --duckdb-memory {shlex.quote(str(memory))}"
        try:
            rayjob_name = submit_rayjob(
                entrypoint=entrypoint,
                name_prefix=f"mat-view-pilot-v{int(version)}",
                num_workers=request.args.get("workers", type=int),
                metadata={"datastack": datastack_name, "version": str(version), "view-pilot": run_id},
                image_tag=image_tag,
                env={"RAY_VIEW_PILOT_RESOURCES": json.dumps(resources)} if resources else None,
                worker_cpus=cpus,
                worker_memory=worker_memory,
            )
        except RayJobSubmissionError as exc:
            return abort(500, f"failed to submit RayJob: {exc}")
        output = f"{get_config_param('DELTALAKE_OUTPUT_BUCKET')}/{datastack_name}/v{version}/_pilot/{run_id}"
        return {"run_id": run_id, "rayjob_name": rayjob_name, "image_tag": image_tag,
                "results": f"{output}/results.json"}, 200


@mat_bp.route("/materialize/ray/job/<string:job_name>/")
class RayJobResource(Resource):
    """Status of a RayJob.

    RayJobs are the durable record for work submitted to the ephemeral Ray
    platform: the Kubernetes CR is the job, so unlike a Celery task there is no
    broker state to consult and no ack that can expire mid-run. This reads the
    CR's status directly.

    Available only where the deployment enables ray (``ray.enabled`` in the
    chart, ``enable_ray`` in Terraform); returns 501 otherwise so callers can
    tell "not configured" apart from "job not found".
    """

    @auth_requires_admin
    @mat_bp.doc("Get RayJob status", security="apikey")
    def get(self, job_name: str):
        from materializationengine.rayjobs import (
            RayJobSubmissionError,
            get_rayjob_status,
            ray_enabled,
        )

        if not ray_enabled():
            return {"message": "Ray is not enabled on this deployment"}, 501

        try:
            return get_rayjob_status(job_name), 200
        except RayJobSubmissionError as exc:
            # Covers both "no such RayJob" and a genuine API failure. The
            # message carries which.
            return {"message": str(exc)}, 404

    @auth_requires_admin
    @mat_bp.doc("Cancel a RayJob", security="apikey")
    def delete(self, job_name: str):
        """Delete a RayJob, tearing down any cluster KubeRay created for it.

        This is also the manual remedy for a wedged job: Ray worker pods are
        owned by the RayCluster CR, which the cluster autoscaler will not evict
        pods on behalf of, so a hung job holds its nodes until either
        ``activeDeadlineSeconds`` fires or the CR is deleted here.
        """
        from materializationengine.rayjobs import (
            RayJobSubmissionError,
            delete_rayjob,
            ray_enabled,
        )

        if not ray_enabled():
            return {"message": "Ray is not enabled on this deployment"}, 501

        try:
            delete_rayjob(job_name)
        except RayJobSubmissionError as exc:
            return {"message": str(exc)}, 404

        return {"message": f"RayJob {job_name} deleted"}, 200
