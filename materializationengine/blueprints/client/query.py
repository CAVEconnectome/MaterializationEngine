import itertools
import tempfile
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import partial

import numpy as np
import pandas as pd
import shapely
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from geoalchemy2.types import Geometry
from multiwrapper import multiprocessing_utils as mu
from sqlalchemy import func, not_
from sqlalchemy.orm import Query
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.sql.schema import Table
from sqlalchemy.sql.selectable import Alias
from sqlalchemy.sql.sqltypes import Boolean, DateTime, Integer, BigInteger, Float, String

DEFAULT_SUFFIX_LIST = ["x", "y", "z", "xx", "yy", "zz", "xxx", "yyy", "zzz"]

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
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
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

    if get_count:
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
