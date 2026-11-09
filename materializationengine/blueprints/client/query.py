import itertools
import logging
from collections import OrderedDict
import os
import tempfile
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import partial

import numpy as np
import pandas as pd
import psycopg2
import shapely
from flask import abort
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from geoalchemy2.types import Geometry
from multiwrapper import multiprocessing_utils as mu
from sqlalchemy import func, not_, text
from sqlalchemy.orm import Query
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.sql.schema import Table
from sqlalchemy.sql.selectable import Alias
from sqlalchemy.sql.sqltypes import Boolean, DateTime, Integer, BigInteger, Float, String
from sqlalchemy.sql import sqltypes as sa_types

from materializationengine.config import as_bool
from materializationengine.utils import get_config_param

try:
    import adbc_driver_postgresql.dbapi as adbc_postgresql
    import pyarrow as pa
    import pyarrow.compute as pc
except ImportError:  # pragma: no cover - both are dependencies; stay usable without them
    adbc_postgresql = None

DEFAULT_SUFFIX_LIST = ["x", "y", "z", "xx", "yy", "zz", "xxx", "yyy", "zzz"]


def statement_timeout_ms() -> int:
    """Postgres statement_timeout for the query and precomputed endpoints, in ms (0 = none).

    An abandoned request (uwsgi harakiri, the ingress, or a client such as Neuroglancer
    dropping a precomputed tile) does not stop the Postgres query behind it. On 2026-10-08
    58 abandoned precomputed-tile queries ran for up to 11 hours, held the Minnie instance at
    85-96% memory, and got a pg_repack cancelled twice by Cloud SQL's memory watchdog. Keep
    this below uwsgi's harakiri so the query ends with its request.
    """
    seconds = get_config_param("QUERY_STATEMENT_TIMEOUT_SECONDS", 480)
    try:
        return max(0, int(float(seconds) * 1000))
    except (TypeError, ValueError):
        return 0


def _set_local_statement_timeout(execute) -> None:
    """SET LOCAL: applies to the current transaction only, so the pooled connection is
    back to the server default once the request's transaction ends."""
    timeout = statement_timeout_ms()
    if timeout:
        execute(f"SET LOCAL statement_timeout = {timeout}")


def _abort_if_timed_out(exc: Exception) -> None:
    cause, seen = exc, set()
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        if isinstance(cause, psycopg2.errors.QueryCanceled) or (
            getattr(cause, "sqlstate", None) == "57014"  # query_canceled, as ADBC reports it
        ):
            abort(
                504,
                f"Query exceeded the {statement_timeout_ms() // 1000}s limit and was "
                "cancelled. Narrow it (filters, a smaller bounding box, fewer columns) "
                "or use a bulk export.",
            )
        cause = getattr(cause, "orig", None) or cause.__cause__ or cause.__context__

dtype_map = {
    Boolean: pd.BooleanDtype(),
    DateTime: np.dtype('datetime64[ns]'),
    Integer: pd.Int32Dtype(),
    BigInteger: pd.Int64Dtype(),
    Float: pd.Float32Dtype(),
}

def concatenate_position_columns(df):
    grps = itertools.groupby(df.columns, key=lambda x: x[:-2])
    for base, g in grps:
        gl = list(g)
        t = "".join([k[-1:] for k in gl])
        if t == "xyz":
            df[base] = [np.array(x) for x in df[gl].values.tolist()]
            df.drop(gl, axis=1, inplace=True)

    return df


def fix_wkb_column(df_col, wkb_data_start_ind=2, n_threads=None):
    """Convert a column with 3-d point data stored as in WKB format
    to list of arrays of integer point locations. The series can not be
    mixed.

    Parameters
    ----------
    df_col : pandas.Series
        N-length Series (representing a column of a dataframe) to convert. All elements
        should be either a hex-string or a geoalchemy2 WKBElement object.
    wkb_data_start_ind : int, optional
        When the WKB data is represented as a hex string, sets the first character
        of the actual data. By default 2, since the current implementation has
        a prefix when the data is imported as text. Set to 0 if the data is just
        an exact hex string already. This value is ignored if the series data is in
        WKBElement object form.
    n_threads : int or None, optional
        Sets number of threads. If None, uses as many threads as CPUs.
        If n_threads is set to 1, multiprocessing is not used.
        Optional, by default None.

    Returns
    -------
    list
        N-length list of arrays of 3d points
    """

    if len(df_col) == 0:
        return df_col.tolist()

    if isinstance(df_col.loc[0], str):
        wkbstr = df_col.loc[0]
        shp = shapely.wkb.loads(wkbstr[wkb_data_start_ind:], hex=True)
        if isinstance(shp, shapely.geometry.point.Point):
            return _fix_wkb_hex_point_column(df_col, n_threads=n_threads)
    elif isinstance(df_col.loc[0], WKBElement):
        return _fix_wkb_object_point_column(df_col, n_threads=n_threads)
    return df_col.tolist()


def fix_columns_with_query(
    df, query, n_threads=None, fix_decimal=True, fix_wkb=True, wkb_data_start_ind=2
):
    """Use a query object to suggest how to convert columns imported from csv to correct types."""


    n_tables = len(query.column_descriptions)
    if n_tables == 1:
        schema_model = query.column_descriptions[0]["type"]
    for colname in df.columns:
        if n_tables == 1:
            coltype = type(getattr(schema_model, colname).type)
        else:
            coltype = type(
                next(
                    col["type"]
                    for col in query.column_descriptions
                    if col["name"] == colname
                )
            )
        if coltype is Boolean:
            pass
        #    df[colname] = _fix_boolean_column(df[colname])
        elif coltype is DateTime:
            # if the first entry for this column has a decimal point,
            # then it is one format and we want to convert it using that format
            df[colname] = pd.to_datetime(
                df[colname], utc=True, format='ISO8601'
            )
        if len(df) > 0:
            if coltype is Geometry and fix_wkb is True:
                
                    df[colname] = fix_wkb_column(
                        df[colname],
                        wkb_data_start_ind=wkb_data_start_ind,
                        n_threads=n_threads,
                    )
            elif isinstance(df[colname].loc[0], Decimal) and fix_decimal is True:
                df[colname] = _fix_decimal_column(df[colname])
    return df


def _wkb_object_point_to_numpy(wkb):
    """Fixes single geometry element"""
    shp = to_shape(wkb)
    return shp.xy[0][0], shp.xy[1][0], shp.z


def _fix_wkb_object_point_column(df_col, n_threads=None):
    if n_threads != 1:
        xyz = mu.multiprocess_func(
            _wkb_object_point_to_numpy, df_col.tolist(), n_threads=n_threads
        )
    else:
        func = np.vectorize(_wkb_object_point_to_numpy)
        xyz = np.vstack(func(df_col.values)).T
    return list(np.array(xyz, dtype=int))


def _wkb_hex_point_to_numpy(wkbstr, wkb_data_start_ind=2):
    shp = shapely.wkb.loads(wkbstr[wkb_data_start_ind:], hex=True)
    return shp.xy[0][0], shp.xy[1][0], shp.z


def _fix_wkb_hex_point_column(df_col, wkb_data_start_ind=2, n_threads=None):
    func = partial(_wkb_hex_point_to_numpy, wkb_data_start_ind=wkb_data_start_ind)
    if n_threads != 1:
        xyz = mu.multiprocess_func(func, df_col.tolist(), n_threads)
    else:
        func = np.vectorize(func)
        xyz = np.vstack(func(df_col.values)).T
    return list(np.array(xyz, dtype=int))


def _fix_boolean_column(df_col):
    return df_col.apply(lambda x: True if x == "t" else False)


def _fix_decimal_column(df_col):
    is_integer_col = np.vectorize(lambda x: float(x).is_integer())
    if np.all(is_integer_col(df_col)):
        return df_col.apply(int)
    else:
        return df_col.apply(float)


def get_column(model, column):
    if isinstance(model, Alias):
        return model.c[column]
    if isinstance(model, AliasedClass):
        return eval(f"model.{column}")
    if isinstance(model, Table):
        return eval(f"model.columns.{column}")
    else:
        return model.__dict__[column]


def make_spatial_filter(model, column_name, bounding_box) -> Query:
    """Generate spatial query that finds annotations within a bounding box.

    Args:

        model (DeclarativeMeta): sqlalchemy model
        column_name (str): name of column to query
        bounding_box (List[List[int]]): Bounding box in the form of [[min_x, min_y, min_z], [max_x, max_y, max_z]]

    Returns:
        Query: [description]
    """

    spatial_column = get_column(model, column_name)

    coord_array = np.array(bounding_box)
    if not (coord_array[0] < coord_array[1]).all():
        raise Exception(
            f"min bounds: {coord_array[0]} must be less than max bounds: {coord_array[1]}"
        )

    start_coord = np.array2string(coord_array[0]).strip("[]")
    end_coord = np.array2string(coord_array[1]).strip("[]")

    return spatial_column.intersects_nd(
        func.ST_3DMakeBox(f"POINTZ({start_coord})", f"POINTZ({end_coord})")
    )


def render_query(statement, dialect=None):
    """
    Based on https://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query#comment39255415_23835766
    Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement.
    """
    if isinstance(statement, Query):
        if dialect is None:
            dialect = statement.session.bind.dialect
        statement = statement.statement
    elif dialect is None:
        dialect = statement.bind.dialect

    class LiteralCompiler(dialect.statement_compiler):
        def visit_bindparam(
            self, bindparam, within_columns_clause=False, literal_binds=False, **kwargs
        ):
            return self.render_literal_value(bindparam.value, bindparam.type)

        def render_array_value(self, val, item_type):
            if isinstance(val, list):
                return "{%s}" % ",".join(
                    [self.render_array_value(x, item_type) for x in val]
                )
            return self.render_literal_value(val, item_type)

        def render_literal_value(self, value, type_):
            if isinstance(value, int):
                return str(value)
            if isinstance(value, bool):
                return bool(value)
            elif isinstance(value, (str, date, datetime, timedelta)):
                return "'%s'" % str(value).replace("'", "''")
            elif isinstance(value, list):
                return "'{%s}'" % (
                    ",".join(
                        [self.render_array_value(x, type_.item_type) for x in value]
                    )
                )
            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return LiteralCompiler(dialect, statement).process(statement)


def read_sql_tmpfile(query, db_engine):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        try:
            cur = conn.cursor()
            _set_local_statement_timeout(cur.execute)
            cur.copy_expert(copy_sql, tmpfile)
        finally:
            # Back to the pool, which rolls back the transaction (and with it SET LOCAL).
            conn.close()
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        return df


def _make_query(
    this_sqlalchemy_session,
    query_args,
    join_args=None,
    filter_args=None,
    select_columns=None,
    offset=None,
    limit=None,
):
    """Constructs a query object with selects, joins, and filters

    Args:
        query_args: Iterable of objects to query
        join_args: Iterable of objects to set as a join (optional)
        filter_args: Iterable of iterables
        select_columns: None or Iterable of str
        offset: Int offset of query

    Returns:
        SQLAchemy query object
    """

    query = this_sqlalchemy_session.query(*query_args)

    if join_args is not None:
        query = query.join(*join_args, full=False)

    if filter_args is not None:
        for f in filter_args:
            query = query.filter(*f)

    if select_columns is not None:
        query = query.with_entities(*select_columns)

    if offset is not None:
        query = query.offset(offset)
    if limit is not None:
        query = query.limit(limit)

    return query


def _execute_query(
    session,
    engine,
    query,
    fix_wkb=True,
    fix_decimal=True,
    n_threads=None,
    index_col=None,
    get_count=False,
    direct_sql_pandas=False
):
    """Query the database and make a dataframe out of the results

    Args:
        query: SQLAlchemy query object
        fix_wkb: Boolean to turn wkb objects into numpy arrays (optional, default is True)
        index_col: None or str
        get_count: bool. If True only the query count is returned

    Returns:
        Dataframe with query results
    """
    # logging.info(query.statement)
    try:
        return _execute_query_unguarded(
            session, engine, query, fix_wkb=fix_wkb, fix_decimal=fix_decimal,
            n_threads=n_threads, index_col=index_col, get_count=get_count,
            direct_sql_pandas=direct_sql_pandas,
        )
    except Exception as e:
        _abort_if_timed_out(e)
        raise


# Cached ADBC connections, per thread, least recently used first. Keyed by pid so a forked worker
# never reuses a socket it inherited from its parent. At most ADBC_CONNECTIONS_KEPT (default:
# WEB_DB_ENGINES_KEPT, 4) are kept; the least recently used is closed when another is opened.
_adbc_local = threading.local()

# Arrow types the ADBC path converts, mapped to what pd.read_sql(dtype_backend="numpy_nullable")
# infers when dtype_map has no override: every integer width to Int64, double to Float64 (float4
# columns use pd.read_sql, see _adbc_handles_query).
# Columns with a dtype_map type (Integer -> Int32, Float -> Float32, ...) are cast afterwards.
# Anything else (geometry, numeric, json, arrays, ...) falls back to pd.read_sql.
_ADBC_PANDAS_TYPES = {
    pa.int64(): pd.Int64Dtype(), pa.int32(): pd.Int64Dtype(), pa.int16(): pd.Int64Dtype(),
    pa.int8(): pd.Int64Dtype(), pa.float64(): pd.Float64Dtype(),
    pa.bool_(): pd.BooleanDtype(), pa.string(): pd.StringDtype(), pa.large_string(): pd.StringDtype(),
} if adbc_postgresql is not None else {}


# SQLAlchemy column types the ADBC path may fetch. Geometry in particular must not go through it:
# ADBC returns PostGIS's binary wire format, where pd.read_sql returns hex WKB.
_ADBC_SQL_TYPES = (sa_types.SmallInteger, sa_types.Integer, sa_types.BigInteger, sa_types.Float,
                   sa_types.Boolean, sa_types.DateTime, sa_types.String)


def _adbc_handles_query(query) -> bool:
    for column in query.statement.columns:
        coltype = column.type
        if isinstance(coltype, sa_types.Numeric) and not isinstance(coltype, sa_types.Float):
            return False  # Decimal: fix_decimal expects Decimal objects
        if isinstance(coltype, sa_types.REAL) or (
            isinstance(coltype, sa_types.Float) and (getattr(coltype, "precision", None) or 53) <= 24
        ):
            # float4: pd.read_sql parses Postgres's shortest text form (0.33333334), while widening
            # ADBC's float32 gives 0.3333333432674408, a different value
            return False
        if not isinstance(coltype, _ADBC_SQL_TYPES) or isinstance(coltype, Geometry):
            return False
    return True


def _adbc_supported(field) -> bool:
    return field.type in _ADBC_PANDAS_TYPES or pa.types.is_timestamp(field.type) or pa.types.is_null(field.type)


def _adbc_uri(engine) -> str:
    url = engine.url
    render = getattr(url, "render_as_string", None)  # SQLAlchemy >= 1.4 masks the password in str()
    uri = render(hide_password=False) if render else str(url)
    return "postgresql://" + uri.split("://", 1)[1]


def _adbc_connections() -> "OrderedDict":
    conns = getattr(_adbc_local, "conns", None)
    if conns is None:
        conns = _adbc_local.conns = OrderedDict()
    return conns


def _close_adbc(key, conn) -> None:
    if key[0] != os.getpid():
        return  # inherited from the parent process: closing it would disconnect the parent's socket
    try:
        conn.close()
    except Exception:
        pass


def _drop_adbc_connection(key) -> None:
    conn = _adbc_connections().pop(key, None)
    if conn is not None:
        _close_adbc(key, conn)


def _adbc_connection(engine):
    timeout = statement_timeout_ms()
    key = (os.getpid(), str(engine.url), timeout)
    conns = _adbc_connections()
    conn = conns.get(key)
    if conn is not None:
        conns.move_to_end(key)
        return key, conn
    keep = int(get_config_param("ADBC_CONNECTIONS_KEPT", get_config_param("WEB_DB_ENGINES_KEPT", 4)))
    while conns and len(conns) >= max(keep, 1):
        old_key, old_conn = conns.popitem(last=False)
        _close_adbc(old_key, old_conn)
    conn = adbc_postgresql.connect(_adbc_uri(engine), autocommit=True)
    if timeout:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {timeout}")
    conns[key] = conn
    return key, conn


def _is_connection_failure(exc) -> bool:
    """A dead or terminated connection (worth one retry on a new one), as opposed to a cancelled
    query or an SQL error. ADBC reports a closed socket as OperationalError without a sqlstate."""
    if not isinstance(exc, adbc_postgresql.OperationalError):
        return False
    sqlstate = getattr(exc, "sqlstate", None)
    return sqlstate is None or sqlstate.startswith("08") or sqlstate in ("57P01", "57P02", "57P03")


def _read_sql_adbc(statement: str, engine):
    """Run *statement* over ADBC (binary COPY straight into Arrow). Returns a pyarrow Table, or
    None when a column has a type the pandas conversion below does not handle.

    A cached connection can be dead (Cloud SQL restart, a dropped version's connections
    terminated): that is retried once on a new connection. Cancellations and SQL errors are not.
    """
    for attempt in (1, 2):
        key, conn = _adbc_connection(engine)
        try:
            with conn.cursor() as cur:
                cur.execute(statement)
                table = cur.fetch_arrow_table()
            break
        except Exception as e:
            _drop_adbc_connection(key)  # a broken or cancelled connection is not reused
            if attempt == 1 and _is_connection_failure(e):
                continue
            raise
    if not all(_adbc_supported(f) for f in table.schema):
        return None
    return table


def _adbc_table_to_dataframe(table, dtypes: dict) -> pd.DataFrame:
    """Arrow from ADBC to the DataFrame pd.read_sql(dtype_backend="numpy_nullable") would build,
    including its inference quirks, so a response does not depend on the fetch path."""
    # Columns that are SQL NULL in every row, decided before NaNs become nulls below: pd.read_sql
    # infers Float64 for a column of NaNs, and string only for one of NULLs.
    all_null = {f.name for f in table.schema if table.num_rows and table.column(f.name).null_count == table.num_rows}
    # pd.read_sql treats a Postgres NaN as missing; ADBC keeps it as a valid NaN.
    for i, field in enumerate(table.schema):
        if pa.types.is_floating(field.type):
            column = table.column(i)
            table = table.set_column(i, field, pc.if_else(pc.is_nan(column), pa.scalar(None, field.type), column))
    df = table.to_pandas(types_mapper=_ADBC_PANDAS_TYPES.get, coerce_temporal_nanoseconds=True)
    for k, dtype in dtypes.items():
        if k in df.columns and str(df[k].dtype) != str(dtype) and not str(dtype).startswith("datetime"):
            df[k] = df[k].astype(dtype)
    # A column without a dtype_map type gets its dtype from pd.read_sql's inference, which has
    # nothing to go on when there are no values: with no rows it stays object (the response then
    # declares Arrow null); with only NULLs it becomes string.
    for k in df.columns:
        if k in dtypes:
            continue
        if table.num_rows == 0:
            df[k] = df[k].astype(object)
        elif k in all_null and str(df[k].dtype) != "string":
            df[k] = df[k].astype(pd.StringDtype())
    return df


def _query_dtypes(query) -> dict:
    dtypes = {}
    for k in query.statement.columns.keys():
        coltype = query.statement.columns[k].type
        if type(coltype) in dtype_map:
            dtypes[k] = dtype_map[type(coltype)]
    return dtypes


def _execute_query_unguarded(
    session, engine, query, fix_wkb, fix_decimal, n_threads, index_col, get_count,
    direct_sql_pandas,
):
    def on_postgres():
        return _execute_query_on_postgres(
            session, engine, query, fix_wkb, fix_decimal, n_threads, index_col, get_count,
            direct_sql_pandas)

    # A frozen version's query can be answered from its Delta Lake exports (lake_query).
    # Only where the response is the ADBC path's: direct_sql_pandas, types it converts.
    if index_col is None and (get_count or (direct_sql_pandas and _adbc_handles_query(query))):
        from materializationengine.blueprints.client import lake_query

        def to_dataframe(table):
            df = _adbc_table_to_dataframe(table, _query_dtypes(query))
            return fix_columns_with_query(df, query, fix_wkb=fix_wkb, fix_decimal=fix_decimal, n_threads=n_threads)

        return lake_query.execute(engine, query, get_count, on_postgres, to_dataframe)
    return on_postgres()


def _execute_query_on_postgres(
    session, engine, query, fix_wkb, fix_decimal, n_threads, index_col, get_count,
    direct_sql_pandas,
):
    if get_count:
        _set_local_statement_timeout(lambda sql: session.execute(text(sql)))
        count = query.count()
        df = pd.DataFrame({"count": [count]})
    else:
        if direct_sql_pandas:
            statement = str(query.statement.compile(engine, compile_kwargs={"literal_binds": True}))
            dtypes = {}
            for k in query.statement.columns.keys():
                coltype = query.statement.columns[k].type
                if type(coltype) in dtype_map:
                    dtypes[k] = dtype_map[type(coltype)]
            # ADBC skips building a Python object per cell: 54,664 synapse rows in 0.33 s
            # instead of 0.67 s for pd.read_sql on ltv7 (2026-11-08), same DataFrame.
            table = None
            if adbc_postgresql is not None and index_col is None and as_bool(
                get_config_param("QUERY_FETCH_WITH_ADBC", True), default=True, name="QUERY_FETCH_WITH_ADBC"
            ) and _adbc_handles_query(query):
                table = _read_sql_adbc(statement, engine)
            if table is not None:
                df = _adbc_table_to_dataframe(table, dtypes)
                return fix_columns_with_query(
                    df, query, fix_wkb=fix_wkb, fix_decimal=fix_decimal, n_threads=n_threads
                )
            # pd.read_sql runs in the session's transaction, so SET LOCAL there covers it.
            _set_local_statement_timeout(lambda sql: session.execute(text(sql)))
            df = pd.read_sql(statement,
                             session.connection().connection,
                             coerce_float=True,
                             index_col=index_col,
                             dtype=dtypes,
                             dtype_backend='numpy_nullable')   
        else:
            df = read_sql_tmpfile(
                query.statement.compile(engine, compile_kwargs={"literal_binds": True}),
                engine,
            )
        df = fix_columns_with_query(
            df, query, fix_wkb=fix_wkb, fix_decimal=fix_decimal, n_threads=n_threads
        )

    return df


def _query(
    this_sqlalchemy_session,
    engine,
    query_args,
    join_args=None,
    filter_args=None,
    select_columns=None,
    fix_wkb=True,
    index_col=None,
    offset=None,
    limit=None,
    get_count=False,
    direct_sql_pandas=False
):
    """Wraps make_query and execute_query in one function

    Parameters
    ----------
    query_args:
    join_args:
    filter_args:
    select_columns:
    fix_wkb: bool
    index_col: str or None
    offset: int or None
    limit: int or None
    get_count: bool


    :param select_columns:
    :param fix_wkb:
    :param index_col:
    :return:
    """
    query = _make_query(
        this_sqlalchemy_session,
        query_args=query_args,
        join_args=join_args,
        filter_args=filter_args,
        select_columns=select_columns,
        offset=offset,
        limit=limit,
    )

    df = _execute_query(
        this_sqlalchemy_session,
        engine,
        query=query,
        fix_wkb=fix_wkb,
        index_col=index_col,
        get_count=get_count,
        direct_sql_pandas=direct_sql_pandas
    )

    return df
