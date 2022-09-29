from requests import request
from sqlalchemy import engine_from_config
from materializationengine.database import (
    sqlalchemy_cache,
    dynamic_annotation_cache
)
from materializationengine.database import (
    create_session,
    dynamic_annotation_cache,
    sqlalchemy_cache,
)


    # query = this_sqlalchemy_session.query(*query_args)

    # if join_args is not None:
    #     query = query.join(*join_args, full=False)

    # if filter_args is not None:
    #     for f in filter_args:
    #         query = query.filter(*f)

    # if select_columns is not None:
    #     query = query.with_entities(*select_columns)

    # if offset is not None:
    #     query = query.offset(offset)
    # if limit is not None:
    #     query = query.limit(limit)

    # return query
class QueryManager():

    def __init__(self, db_name, segmentation_source, split_mode=False):
        self._db = dynamic_annotation_cache.get_db(db_name)

        self._segmentation_source=segmentation_source
        self._split_mode = split_mode
        
        self.entities = []
        self.joins = []
        self.filters= = []

    def add_table(self, table_name):
        schema=self._db.database.get_table_schema(table_name)
        md = self._db.database.get_table_metadata(table_name)

        if self._split_mode:
            annmodel, segmodel = self._db.schema.get_split_models(table_name,
                                                                  schema,
                                                                  self._segmentation_source,
                                                                  md,
                                                                  self._split_mode)
            self.joins.append(annmodel.id == segmodel.id)
            self.entities.append(annmodel)
            self.entities.append(segmodel)

        else:
            flatmodel = self._db.schema.create_flat_model(table_name, schema, self._segmentation_source, md)
            self.entities.append(flatmodel)
            
    def join_tables(self, table1, column1, table2, column2):
        schema1=self._db.database.get_table_schema(table1)
        md1 = self._db.database.get_table_metadata(table1)
        schema2 = self._db.database.get_table_schema(table2)
        md2 = self._db.database.get_table_metadata(table2)

        if self._split_mode:
            annmodel1, segmodel1 = self._db.schema.get_split_models(table1,
                                                                    schema1,
                                                                    self._segmentation_source,
                                                                    md1,
                                                                    self._split_mode)
            

            if column1.endswith('pt_root_id'):

            else:

            if column2.endswith('pt_root_id'):
            
            else:


        else:

            flat1 = self._db.schema.create_flat_model(table1, schema1, self._segmentation_source, md1 )      
            flat2 = self._db.schema.create_flat_model(table2, schema1, self._segmentation_source, md2)

            join_args.append(flat1.__dict__[column1]==flat2.__dict__[column2])

    def apply_filters(data):
        pass
    def _apply_equal_filters(table_name, column_name, value):
    
    def _apply_isin_filters(table_name, column_name, value):

    def _apply_notequal_filter(table_name, column_name, value):

    def _apply_spatial_filter(table_name, column_name, bbox):

    def select_column(table_name, column_name):
    
    def select_all_columns(table_name):

    def deselect_column(table_name, column_name):

    def construct_query():
