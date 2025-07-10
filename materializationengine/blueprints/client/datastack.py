import inspect
import logging
from functools import wraps

from dynamicannotationdb.models import (
    AnalysisTable,
    AnalysisVersion,
)
from materializationengine.database import db_manager

from materializationengine.info_client import get_relevant_datastack_info
from flask import abort


def parse_args(f, *args, **kwargs):
    sig = inspect.signature(f).bind(*args, **kwargs)
    sig.apply_defaults()
    return dict(sig.arguments)


def validate_datastack(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # get args and kwargs as a dict
        arguments = parse_args(f, *args, **kwargs)

        target_datastack = arguments.get("datastack_name")
        target_version = arguments.get("version")
        # save the original target datastack and version in kwargs
        kwargs["target_datastack"] = target_datastack
        kwargs["target_version"] = target_version
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            target_datastack
        )
        with db_manager.session_scope(aligned_volume_name) as session:
            version_query = session.query(AnalysisVersion).filter(
                AnalysisVersion.datastack == target_datastack
            ).order_by(
                AnalysisVersion.time_stamp.desc()
            )
            if target_version:
                if target_version == 0:
                    return f(*args, **kwargs)
                if target_version == -1:
                    return f(*args, **kwargs)
                version_query = version_query.filter(
                    AnalysisVersion.version == target_version
                )
            version = version_query.first()
            if version is None:
                session.rollback()
                abort(404, f"version {target_version} not found in {target_datastack}")

            if version.parent_version is not None:
                # then we want to remap
                parent_version_info = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.id == version.parent_version)
                    .filter(AnalysisVersion.valid == True)
                    .first()
                )

                if parent_version_info is None:
                    session.rollback()
                    abort(
                        404,
                        f"version {target_version} not found in {target_datastack} because there is no valid parent",
                    )
                parent_version = parent_version_info.version
                parent_datastack = parent_version_info.datastack
                # remap datastack name to point to parent version

                if kwargs.get("datastack_name"):
                    kwargs["datastack_name"] = parent_datastack
                    kwargs["version"] = parent_version
                    return f(*args, **kwargs)
                else:
                    args_list = list(args)
                    args_list[0] = parent_datastack
                    args_list[1] = parent_version
                    new_args = tuple(args_list)
                    return f(*new_args, **kwargs)

            else:
                # then we don't
                return f(*args, **kwargs)

    return wrapper
