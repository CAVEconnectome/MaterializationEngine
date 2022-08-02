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
class QueryManager():

    def __init__(self, db_name, split_mode=False):
        self._db = dynamic_annotation_cache.get_db(db_name)
        self._split_mode = False
        
        self._join_cache = 

        

    def add_table(table_name):

    def join_tables(table1, column1, table2, column2):
    
    def apply_filters(data):

    def _apply_equal_filters(table_name, column_name, value):
    
    def _apply_isin_filters(table_name, column_name, value):

    def _apply_notequal_filter(table_name, column_name, value):

    def _apply_spatial_filter(table_name, column_name, bbox):

    def select_column(table_name, column_name):
    
    def select_all_columns(table_name):

    def deselect_column(table_name, column_name):

    def construct_query():
