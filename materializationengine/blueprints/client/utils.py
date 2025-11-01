from io import BytesIO

import pyarrow as pa
from cloudfiles import compression
from dynamicannotationdb.models import AnalysisVersion
from flask import Response, request, send_file

from materializationengine.database import db_manager
from materializationengine.info_client import get_datastack_info


def collect_crud_columns(column_names):
    crud_columns = []
    created_columns = []
    for table in column_names.keys():
        table_crud_columns = [
            column_names[table].get("deleted", None),
            column_names[table].get("superceded_id", None),
        ]
        crud_columns.extend([t for t in table_crud_columns if t is not None])
        if column_names[table].get("created", None):
            created_columns.append(column_names[table]["created"])
    return crud_columns, created_columns


def after_request(response):
    accept_encoding = request.headers.get("Accept-Encoding", "")

    if "gzip" not in accept_encoding.lower():
        return response

    response.direct_passthrough = False

    if (
        response.status_code < 200
        or response.status_code >= 300
        or "Content-Encoding" in response.headers
    ):
        return response

    response.data = compression.gzip_compress(response.data)

    response.headers["Content-Encoding"] = "gzip"
    response.headers["Vary"] = "Accept-Encoding"
    response.headers["Content-Length"] = len(response.data)

    return response


def add_warnings_to_headers(headers, warnings):
    if len(warnings) > 0:
        warnings = [w.replace("\n", " ") for w in warnings]
        headers["Warning"] = warnings
    return headers


def update_notice_text_warnings(ann_md, warnings, table_name):
    notice_text = ann_md.get("notice_text", None)
    if notice_text is not None:
        msg = f"Table Owner Notice on {table_name}: {notice_text}"
        warnings.append(msg)

    return warnings



def get_latest_version(datastack_name):
    aligned_volume_name = get_datastack_info(datastack_name)["aligned_volume"]["name"]
    with db_manager.session_scope(aligned_volume_name) as session:

        # query the database for the latest valid version
        response = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == datastack_name)
            .filter(AnalysisVersion.valid)
            .order_by(AnalysisVersion.time_stamp.desc())
            .first()
        )
        if response is None:
            return None
        else:
            return response.version


def create_query_response(
    df,
    warnings,
    desired_resolution,
    column_names,
    return_pyarrow=True,
    arrow_format=False,
    ipc_compress=True,
):
    accept_encoding = request.headers.get("Accept-Encoding", "")

    headers = add_warnings_to_headers({}, warnings)
    if desired_resolution is not None:
        headers["dataframe_resolution"] = desired_resolution
    headers["column_names"] = column_names
    if return_pyarrow:
        if arrow_format:
            batch = pa.RecordBatch.from_pandas(df)
            sink = pa.BufferOutputStream()
            if ipc_compress:
                if "lz4" in accept_encoding:
                    compression = "LZ4_FRAME"
                elif "zstd" in accept_encoding:
                    compression = "ZSTD"
                else:
                    compression = None
            else:
                compression = None
            opt = pa.ipc.IpcWriteOptions(compression=compression)
            with pa.ipc.new_stream(sink, batch.schema, options=opt) as writer:
                writer.write_batch(batch)
            response = send_file(BytesIO(sink.getvalue().to_pybytes()), "data.arrow")
            response.headers.update(headers)
            return after_request(response)
        headers = add_warnings_to_headers(
            headers,
            [
                "Using deprecated pyarrow serialization method, please upgrade CAVEClient>=5.9.0 with pip install --upgrade caveclient"
            ],
        )
        context = pa.default_serialization_context()
        serialized = context.serialize(df).to_buffer().to_pybytes()
        response = Response(
            serialized, headers=headers, mimetype="x-application/pyarrow"
        )
        return after_request(response)
    else:
        dfjson = df.to_json(orient="records")
        response = Response(dfjson, headers=headers, mimetype="application/json")
        return after_request(response)
