import pyarrow as pa
from flask import Response, abort, current_app, request
from cloudfiles import compression


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
        headers["Warning"] = ". ".join([w.replace("\n", " ") for w in warnings])
    return headers


def update_notice_text_warnings(ann_md, warnings):
    notice_text = ann_md.get("notice_text", None)
    if notice_text is not None:
        msg = f"Table Owner Warning: {notice_text}"
        warnings.append(msg)

    return warnings


def create_query_response(
    df, warnings, desired_resolution, column_names, return_pyarrow=True
):

    headers = add_warnings_to_headers({}, warnings)
    headers["dataframe_resolution"] = desired_resolution
    headers["column_names"] = column_names
    if return_pyarrow:
        context = pa.default_serialization_context()
        serialized = context.serialize(df).to_buffer().to_pybytes()
        return Response(serialized, headers=headers, mimetype="x-application/pyarrow")
    else:
        dfjson = df.to_json(orient="records")
        response = Response(dfjson, headers=headers, mimetype="application/json")
        return after_request(response)