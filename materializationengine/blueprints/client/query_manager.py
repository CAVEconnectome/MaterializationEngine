from collections import defaultdict
from materializationengine.database import dynamic_annotation_cache
from flask import abort
from materializationengine.blueprints.client.query import (
    make_spatial_filter,
    _execute_query,
    get_column,
)
import numpy as np
from geoalchemy2.types import Geometry
from sqlalchemy.sql.sqltypes import Integer
from sqlalchemy import or_, func
from sqlalchemy.orm import aliased
from sqlalchemy.sql.selectable import Alias
from sqlalchemy.sql.schema import Table
from sqlalchemy.sql.expression import tablesample
from sqlalchemy.ext.declarative.api import DeclarativeMeta
import datetime

DEFAULT_SUFFIX_LIST = ["x", "y", "z", "xx", "yy", "zz", "xxx", "yyy", "zzz"]
DEFAULT_LIMIT = 250000


class QueryManager:
    def __init__(
        self,
        db_name: str,
        segmentation_source: str,
        split_mode: bool = False,
        meta_db_name: str = None,
        suffixes: defaultdict = None,
        offset: int = 0,
        limit: int = DEFAULT_LIMIT,
        get_count: bool = False,
        split_mode_outer=False,
        random_sample=False,
    ):
        self._db = dynamic_annotation_cache.get_db(db_name)
        if meta_db_name is None:
            self._meta_db = self._db
        else:
            self._meta_db = dynamic_annotation_cache.get_db(meta_db_name)
        self._segmentation_source = segmentation_source
        self._split_mode = split_mode
        self._random_sample = random_sample
        self._split_mode_outer = split_mode_outer
        self._split_models = {}
        self._flat_models = {}
        self._voxel_resolutions = {}
        self._models = {}
        self._tables = set()
        self._joins = []
        self._filters = []
        self._selected_columns = defaultdict(list)
        self.limit = limit
        self.offset = offset
        self.get_count = get_count
        if suffixes is None:
            suffixes = defaultdict(lambda: None)
        else:
            values = list(suffixes.values())
            if len(values) != len(set(values)):
                raise ValueError(f"Duplicate suffixes set in {suffixes}")

        self._suffixes = suffixes

    def set_suffix(self, table_name, suffix):
        self._suffixes[table_name] = suffix
        values = list(self._suffixes.values())
        if len(values) != len(set(values)):
            raise ValueError(f"Duplicate suffix set in {self._suffixes}")

    def _get_split_model(self, table_name):
        if table_name in self._split_models.keys():
            return self._split_models[table_name]
        else:
            md = self._meta_db.database.get_table_metadata(table_name)
            if md is None:
                abort(404, f"Table {table_name} not found in metadata database")
            vox_res = np.array(
                [
                    md["voxel_resolution_x"],
                    md["voxel_resolution_y"],
                    md["voxel_resolution_z"],
                ]
            )

            self._voxel_resolutions[table_name] = vox_res

            reference_table = md.get("reference_table")
            if reference_table:
                table_metadata = {"reference_table": reference_table}
                ref_md = self._meta_db.database.get_table_metadata(reference_table)
                _ = self._db.schema.get_split_models(
                    reference_table,
                    ref_md["schema_type"],
                    self._segmentation_source,
                    table_metadata=None,
                )
            else:
                table_metadata = None

            annmodel, segmodel = self._db.schema.get_split_models(
                table_name,
                md["schema_type"],
                self._segmentation_source,
                table_metadata=table_metadata,
            )
            self._split_models[table_name] = (annmodel, segmodel)
            return annmodel, segmodel

    def _get_flat_model(self, table_name):
        if table_name in self._flat_models.keys():
            return self._flat_models[table_name]
        if table_name in self._models.keys():
            return self._models[table_name]
        else:
            # schema = self._meta_db.database.get_table_schema(table_name)
            md = self._meta_db.database.get_table_metadata(table_name)
            vox_res = np.array(
                [
                    md["voxel_resolution_x"],
                    md["voxel_resolution_y"],
                    md["voxel_resolution_z"],
                ]
            )

            self._voxel_resolutions[table_name] = vox_res

            reference_table = md.get("reference_table")
            if reference_table:
                table_metadata = {"reference_table": reference_table}
            else:
                table_metadata = None

            flatmodel = self._db.schema.create_flat_model(
                table_name=table_name,
                schema_type=md["schema_type"],
                table_metadata=table_metadata,
            )
            self._flat_models[table_name] = flatmodel
            return flatmodel

    def add_view(self, datastack_name, view_name):
        view_table = self._db.database.get_view_table(view_name)
        self._tables.add(view_table)
        self._models[view_name] = view_table
        md = self._meta_db.database.get_view_metadata(datastack_name, view_name)
        vox_res = np.array(
            [
                md["voxel_resolution_x"],
                md["voxel_resolution_y"],
                md["voxel_resolution_z"],
            ]
        )

        self._voxel_resolutions[view_name] = vox_res

    def add_table(self, table_name):
        if table_name not in self._tables:
            self._tables.add(table_name)
            if self._split_mode:
                annmodel, segmodel = self._get_split_model(table_name)

                if segmodel is not None:
                    # create a subquery joining the segmodel and annmodel
                    # on the id column
                    seg_columns = [
                        c for c in segmodel.__table__.columns if c.key != "id"
                    ]
                    if self._random_sample:
                        annmodel_alias1 = aliased(annmodel, tablesample(annmodel, .001))
                    else:
                        annmodel_alias1 = annmodel
                    subquery = (
                        self._db.database.session.query(annmodel_alias1, *seg_columns)
                        .join(segmodel, annmodel_alias1.id == segmodel.id, isouter=True)
                        .subquery()
                    )
                    annmodel_alias = aliased(subquery, name=table_name, flat=True)

                    self._models[table_name] = annmodel_alias
                    # self._models[segmodel.__tablename__] = segmodel_alias

                else:
                    self._models[table_name] = annmodel
            else:
                model = self._get_flat_model(table_name)
                self._models[table_name] = model

    def _find_relevant_model(self, table_name, column_name):
        if self._split_mode:
            model = self._models[table_name]
        else:
            model = self._get_flat_model(table_name)

        return model

    def join_tables(self, table1, column1, table2, column2, isouter=False):

        self.add_table(table1)
        self.add_table(table2)

        model1 = self._models[table1]
        model2 = self._models[table2]

        model2column = get_column(model2, column2)
        model1column = get_column(model1, column1)

        self._joins.append(
            (
                (model2, model1column == model2column),
                {"isouter": isouter},
            )
        )

    def apply_equal_filter(self, table_name, column_name, value):
        model = self._find_relevant_model(
            table_name=table_name, column_name=column_name
        )
        self._filters.append((get_column(model, column_name) == value,))

    def apply_isin_filter(self, table_name, column_name, value):
        model = self._find_relevant_model(
            table_name=table_name, column_name=column_name
        )
        self._filters.append((get_column(model, column_name).in_(value),))

    def apply_notequal_filter(self, table_name, column_name, value):
        model = self._find_relevant_model(
            table_name=table_name, column_name=column_name
        )
        self._filters.append((get_column(model, column_name) != value,))

    def apply_spatial_filter(self, table_name, column_name, bbox):
        model = self._find_relevant_model(
            table_name=table_name, column_name=column_name
        )
        filter = make_spatial_filter(model, column_name, bbox)
        self._filters.append((filter,))

    def apply_table_crud_filter(
        self,
        table_name,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        created_column="created",
        deleted_column="deleted",
    ):
        model = self._find_relevant_model(
            table_name=table_name, column_name=created_column
        )
        f1 = get_column(model, created_column).between(str(start_time), str(end_time))
        f2 = get_column(model, deleted_column).between(str(start_time), str(end_time))
        self._filters.append((or_(f1, f2),))

    def select_column(self, table_name, column_name):
        # if the column_name is not in the table_name list
        # then we should add it
        if column_name not in self._selected_columns[table_name]:

            model = self._find_relevant_model(
                table_name=table_name, column_name=column_name
            )
            if isinstance(model, Alias):
                if column_name not in model.c.keys():
                    raise ValueError(
                        f"{column_name} not in model or models for {table_name}"
                    )
            elif isinstance(model, Table):
                if column_name not in model.c.keys():
                    raise ValueError(
                        f"{column_name} not in model or models for {table_name}"
                    )
            else:
                if column_name not in model.__dict__.keys():
                    raise ValueError(
                        f"{column_name} not in model or models for {table_name}"
                    )

            self._selected_columns[table_name].append(column_name)

    def select_all_columns(self, table_name):
        if self._split_mode:
            annmodel, segmodel = self._get_split_model(table_name=table_name)
            ann_columns = [c.key for c in annmodel.__table__.columns]
            if segmodel is not None:
                seg_columns = [
                    c.key for c in segmodel.__table__.columns if c.key != "id"
                ]
                columns = ann_columns + seg_columns
            else:
                columns = ann_columns
        else:
            model = self._get_flat_model(table_name=table_name)
            if isinstance(model, DeclarativeMeta):
                columns = [c for c in model.__table__.columns.keys()]
            elif isinstance(model, Table):
                columns = [c for c in model.columns.keys()]
            else:
                columns = [c.key for c in model.c.keys()]

        self._selected_columns[table_name] = columns

    def deselect_column(self, table_name, column_name):
        self._selected_columns[table_name].pop(column_name)

    def apply_filter(self, filter, filter_func):
        if filter:
            for table_name in filter:
                for k, v in filter[table_name].items():
                    filter_func(table_name, k, v)

    def configure_query(self, user_data):
        """{
            "table":"table_name",
            "join_tables":[[table_name, table_column, joined_table, joined_column],
                     [joined_table, joincol2, third_table, joincol_third]]
            "timestamp": "XXXXXXX",
            "offset": 0,
            "limit": 200000,
            "suffixes":{
                "table_name":"suffix1",
                "joined_table":"suffix2",
                "third_table":"suffix3"
            },
            "select_columns": {
                "table_name":[ "column","names"]
            },
            "filter_in_dict": {
                "table_name":{
                    "column_name":[included,values]
                }
            },
            "filter_out_dict": {
                "table_name":{
                    "column_name":[excluded,values]
                }
            },
            "filter_equal_dict": {
                "table_name":{
                    "column_name":value
                }
            "filter_spatial_dict": {
                "table_name": {
                "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
            }
        }"""
        self.add_table(user_data["table"])
        if user_data.get("join_tables", None):
            for join in user_data["join_tables"]:
                self.join_tables(join[0], join[1], join[2], join[3])
        # select the columns the user wants
        if user_data.get("select_columns", None):
            for table_name in user_data["select_columns"].keys():
                for c in user_data["select_columns"][table_name]:
                    self.select_column(table_name, c)
        # if none are specified select all the columns in the tables
        # referred to
        else:
            self.select_all_columns(user_data["table"])
            joins = user_data.get("join_tables", [])
            for join in joins:
                self.select_all_columns(join[0])
                self.select_all_columns(join[2])
        if user_data.get("offset", None):
            self.offset = user_data["offset"]
        if user_data.get("limit", None):
            self.limit = user_data["limit"]

        def apply_filter(filter_key, filter_func):
            if user_data.get(filter_key):
                for table_name in user_data[filter_key]:
                    for k, v in user_data[filter_key][table_name].items():
                        filter_func(table_name, k, v)

        self.apply_filter(user_data.get("filter_in_dict", None), self.apply_isin_filter)
        self.apply_filter(
            user_data.get("filter_out_dict", None), self.apply_notequal_filter
        )
        self.apply_filter(
            user_data.get("filter_equal_dict", None), self.apply_equal_filter
        )
        self.apply_filter(
            user_data.get("filter_spatial_dict", None), self.apply_spatial_filter
        )

        if user_data.get("suffixes", None):
            self._suffixes.update(user_data["suffixes"])

    def _make_query(
        self,
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
        if self._random_sample:
            sampled_query = []
            for k, arg in enumerate(query_args):
                if k == 0:
                    sampled_query.append(aliased(arg, tablesample(arg, 0.001)))
                else:
                    sampled_query.append(arg)
            query_args = sampled_query
        query = self._db.database.session.query(*query_args)

        if join_args is not None:
            for join_arg, join_kwargs in join_args:
                query = query.join(*join_arg, **join_kwargs)

        if filter_args is not None:
            for f in filter_args:
                query = query.filter(*f)

        if select_columns is not None:
            query = query.with_entities(*select_columns)
        if offset is not None:
            query = query.offset(offset)
        # if limit is not None:
        #    query = query.limit(limit)

        return query

    def execute_query(self, desired_resolution=None):
        column_lists = self._selected_columns.values()

        col_names, col_counts = np.unique(
            np.concatenate([cl for cl in column_lists]), return_counts=True
        )
        dup_cols = col_names[col_counts > 1]
        query_args = []
        column_names = {}
        for table_num, table_name in enumerate(self._selected_columns.keys()):
            if desired_resolution is not None:
                vox_ratio = self._voxel_resolutions[table_name] / np.array(
                    desired_resolution
                )
                if np.all(vox_ratio == 1):
                    vox_ratio = None
            else:
                vox_ratio = None
            column_names[table_name] = {}
            # lets get the suffix for this table
            suffix = self._suffixes.get(table_name, None)
            if suffix is None:
                suffix = DEFAULT_SUFFIX_LIST[table_num]

            for column_name in self._selected_columns[table_name]:
                model = self._find_relevant_model(table_name, column_name)
                column = get_column(model, column_name)

                if column.key in dup_cols:
                    column_names[table_name][column.key] = column.key + f"{suffix}"
                    if isinstance(column.type, Geometry):
                        column_args = [
                            column.ST_X()
                            .cast(Integer)
                            .label(column.key + "{}_x".format(suffix)),
                            column.ST_Y()
                            .cast(Integer)
                            .label(column.key + "{}_y".format(suffix)),
                            column.ST_Z()
                            .cast(Integer)
                            .label(column.key + "{}_z".format(suffix)),
                        ]
                        if vox_ratio is not None:
                            column_args = [
                                c * r for c, r in zip(column_args, vox_ratio)
                            ]
                        column_args = [
                            c.label(column.key + "{}_{}".format(suffix, xyz))
                            for c, xyz in zip(column_args, ["x", "y", "z"])
                        ]
                        query_args += column_args
                    else:

                        if self._split_mode and (
                            column.key.endswith("_root_id")
                            or column.key.endswith("_supervoxel_id")
                        ):
                            query_args.append(
                                func.coalesce(column, 1).label(column.key + suffix)
                            )
                        else:
                            query_args.append(column.label(column.key + suffix))
                else:
                    column_names[table_name][column.key] = column.key
                    if isinstance(column.type, Geometry):
                        column_args = [
                            column.ST_X().cast(Integer),
                            column.ST_Y().cast(Integer),
                            column.ST_Z().cast(Integer),
                        ]
                        if vox_ratio is not None:
                            column_args = [
                                c * r for c, r in zip(column_args, vox_ratio)
                            ]
                        column_args = [
                            c.label(column.key + s)
                            for c, s in zip(column_args, ["_x", "_y", "_z"])
                        ]
                        query_args += column_args
                    else:
                        if self._split_mode and (
                            column.key.endswith("_root_id")
                            or column.key.endswith("_supervoxel_id")
                        ):
                            query_args.append(
                                func.coalesce(column, 1).label(column.key)
                            )
                        else:
                            query_args.append(column)

        query = self._make_query(
            query_args=self._models.values(),
            join_args=self._joins,
            filter_args=self._filters,
            select_columns=query_args,
            offset=self.offset,
            limit=self.limit,
        )
        df = _execute_query(
            self._db.database.session,
            self._db.database.engine,
            query=query,
            fix_wkb=False,
            index_col=None,
            get_count=self.get_count,
        )
        return df, column_names
