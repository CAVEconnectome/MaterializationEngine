import inspect
import logging
from functools import wraps

from dynamicannotationdb.models import (
    AnalysisTable,
    AnalysisVersion,
)
from materializationengine.database import sqlalchemy_cache

from materializationengine.info_client import get_relevant_datastack_info


def parse_args(f, *args, **kwargs):
    sig = inspect.signature(f).bind(*args, **kwargs)
    sig.apply_defaults()
    return dict(sig.arguments)


def validate_datastack(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # get args and kwargs as a dict
        arguments = parse_args(f, *args, **kwargs)

        target_table = arguments.get("table_name")
        target_datastack = arguments.get("datastack_name")
        target_version = arguments.get("version")
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            target_datastack
        )
        session = sqlalchemy_cache.get(aligned_volume_name)
        version_query = session.query(AnalysisVersion).filter(
            AnalysisVersion.datastack == target_datastack
        )
        if target_version:
            version_query = version_query.filter(
                AnalysisVersion.version == target_version
            )
        try:
            versions = version_query.all()
        except Exception as e:
            logging.error(e)
            session.rollback()
            versions = None

        # nothing to do here
        if not versions:
            return f(*args, **kwargs)

        parent_version_info = []
        for version in versions:

            if version.parent_version:
                parent_version_info.append(
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.id == version.parent_version)
                    .filter(AnalysisVersion.valid == True)
                    .all()
                )

        if not any(parent_version_info):
            return f(*args, **kwargs)

        parent_database = str(parent_version_info[0][0])
        parent_datastack = parent_database.rsplit("__mat")[0]

        # validate all parent versions
        valid_versions = []
        for version_info in parent_version_info:
            info = version_info[0]
            if info.valid:
                valid_versions.append(info.version)

        if target_table and target_version:
            # confirm target version is valid
            if target_version in valid_versions:
                analysis_version = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.version == target_version)
                    .filter(AnalysisVersion.datastack == target_datastack)
                    .first()
                )
                if analysis_version is None:
                    return None, 404
                response = (
                    session.query(AnalysisTable)
                    .filter(AnalysisTable.analysisversion_id == analysis_version.id)
                    .filter(AnalysisTable.valid == True)
                    .all()
                )
                # check if target table is valid
                valid_tables = [r.table_name for r in response]
                if target_table not in valid_tables:
                    raise ValueError(
                        f"{target_table} not valid for version {target_version}"
                    )
            # remap datastack name to point to parent version
            if kwargs.get("datastack_name"):
                kwargs["datastack_name"] = parent_datastack
            else:
                args_list = list(args)
                args_list[0] = parent_datastack
                new_args = tuple(args_list)
                return f(*new_args, **kwargs)
        return f(*args, **kwargs)

    return wrapper
