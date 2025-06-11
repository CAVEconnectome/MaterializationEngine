from geoalchemy2.types import Geometry
from sqlalchemy import engine, MetaData
from sqlalchemy import inspect

class IndexCache:
    def get_table_indices(self, table_name: str, engine: engine):
        """Reflect current indices, primary key(s) and foreign keys
         on given target table using SQLAlchemy inspector method.

        Args:
            table_name (str): target table to reflect
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine

        Returns:
            dict: Map of reflected indices on given table.
        """
        inspector = inspect(engine)
        try:
            pk_columns = inspector.get_pk_constraint(table_name)
            indexed_columns = inspector.get_indexes(table_name)
            foreign_keys = inspector.get_foreign_keys(table_name)
        except Exception as e:
            print(f"No table named '{table_name}', error: {e}")
            return None
        index_map = {}
        if pk_columns.get("name"):
            pkey_name = pk_columns.get("name").lower()
            pk_name = {"primary_key_name": pkey_name}
            if pk_name["primary_key_name"]:
                pk = {
                    "column_name": pk_columns["constrained_columns"][0],
                    "index_name": pkey_name,
                    "type": "primary_key",
                }
                index_map[pkey_name] = pk

        if indexed_columns:
            for index in indexed_columns:
                dialect_options = index.get("dialect_options", None)
                index_name = index["name"].lower()
                indx_map = {
                    "column_name": index["column_names"][0],
                    "index_name": index_name,
                }
                if dialect_options:
                    if "gist" in dialect_options.values():
                        indx_map.update(
                            {
                                "type": "spatial_index",
                                "dialect_options": index.get("dialect_options"),
                            }
                        )
                else:
                    indx_map.update({"type": "index", "dialect_options": None})

                index_map[index_name] = indx_map
        if foreign_keys:
            for foreign_key in foreign_keys:
                foreign_key_name = foreign_key["name"].lower()
                fkey_column_key = f'{foreign_key["referred_table"]}_{foreign_key["referred_columns"][0]}_fkey' 
                fk_data = {
                    "column_name": foreign_key["referred_columns"][0],
                    "type": "foreign_key",
                    "index_name": fkey_column_key,
                    "foreign_key_name": foreign_key_name,
                    "foreign_key_table": foreign_key["referred_table"],
                    "foreign_key_column": foreign_key["constrained_columns"][0],
                    "target_column": foreign_key["referred_columns"][0],
                }
                index_map[fkey_column_key] = fk_data
        return index_map

    def get_index_from_model(self, table_name, model, engine):
        """Generate index mapping, primary key and foreign keys(s)
        from supplied SQLAlchemy model. Returns a index map.

        Args:
            model (SqlAlchemy Model): database model to reflect indices

        Returns:
            dict: Index map
        """

        model = model.__table__
        index_map = {}
        for column in model.columns:
            if column.primary_key:
                pk_index_name = f"{table_name}_pkey".lower()
                pk = {
                    "column_name": column.name,
                    "index_name": pk_index_name,
                    "type": "primary_key",
                }
                index_map[pk_index_name] = pk
            if column.index:
                index_name = f"ix_{table_name}_{column.name}"
                indx_map = {
                    "column_name": column.name,
                    "index_name": index_name,
                    "type": "index",
                    "dialect_options": None,
                }
                index_map[index_name] = indx_map
            if isinstance(column.type, Geometry):
                sptial_index_name = f"idx_{table_name}_{column.name}".lower()
                spatial_index_map = {
                    "column_name": column.name,
                    "index_name": sptial_index_name,
                    "type": "spatial_index",
                    "dialect_options": {"postgresql_using": "gist"},
                }
                index_map[sptial_index_name] = spatial_index_map
            if column.foreign_keys:
                metadata_obj = MetaData()
                metadata_obj.reflect(bind=engine)
                target_table = metadata_obj.tables.get(table_name)
                foreign_keys = list(target_table.foreign_keys)

                for foreign_key in foreign_keys:
                    (
                        target_table_name,
                        target_column,
                    ) = foreign_key.target_fullname.split(".")
                    foreign_key_name = foreign_key.name.lower()
                    fkey_column_key = f'{target_table_name}_{foreign_key.constraint.column_keys[0]}_fkey' 

                    foreign_key_map = {
                        "type": "foreign_key",
                        "column_name": foreign_key.constraint.column_keys[0],
                        "index_name": fkey_column_key,
                        "foreign_key_name": foreign_key_name,
                        "foreign_key_table": target_table_name,
                        "foreign_key_column": foreign_key.constraint.column_keys[0],
                        "target_column": target_column,
                    }

                    index_map[fkey_column_key] = foreign_key_map
        return index_map

    def drop_table_indices(self, table_name: str, engine, drop_primary_key=True):
        """Generate SQL command to drop all indices and
        constraints on target table.

        Args:
            table_name (str): target table to drop constraints and indices
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine

        Returns:
            bool: True if all constraints and indices are dropped
        """
        indices = self.get_table_indices(table_name, engine)
        if not indices:
            return f"No indices on '{table_name}' found."
        command = f"ALTER TABLE {table_name}"

        connection = engine.connect()
        constraints_list = []
        for column_info in indices.values():
            if "foreign_key" in column_info["type"]:
                constraints_list.append(
                    f"{command} DROP CONSTRAINT IF EXISTS {column_info['foreign_key_name']}"
                )
            if "primary_key" in column_info["type"] and drop_primary_key:
                constraints_list.append(
                    f"{command} DROP CONSTRAINT IF EXISTS {column_info['index_name']}"
                )
        if not constraints_list:
            return f"No constraints on '{table_name}' found"
        drop_constraint = f"{'; '.join(constraints_list)} CASCADE"
        command = f"{drop_constraint};"
        index_list = [
            col["index_name"] for col in indices.values() if "index" in col["type"]
        ]
        if index_list:
            drop_index = f"DROP INDEX {', '.join(index_list)}"
            command = f"{command} {drop_index};"
        try:
            connection.execute(command)
        except Exception as e:
            raise (e)
        return True

    def add_indices_sql_commands(self, table_name: str, model, engine, skip_foreign_keys=False):
        """Add missing indices by comparing reflected table and
        model indices. Will add missing indices from model to table.

        Args:
            table_name (str): target table to drop constraints and indices
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine
            skip_foreign_keys (bool): skip adding foreign keys

        Returns:
            str: list of indices added to table
        """
        current_indices = self.get_table_indices(table_name, engine)
        model_indices = self.get_index_from_model(table_name, model, engine)
        missing_indices = set(model_indices) - set(current_indices)
        commands = []
        for index in missing_indices:
            index_type = model_indices[index]["type"]
            column_name = model_indices[index]["column_name"]
            index_name = model_indices[index]["index_name"]
            if index_type == "primary_key":
                command = f"ALTER TABLE {table_name} add primary key({column_name});"
            if index_type == "index":
                command = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name});"
            if index_type == "spatial_index":
                command = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST ({column_name} gist_geometry_ops_nd);"
            
            if index_type == "foreign_key" and not skip_foreign_keys:
                foreign_key_name = model_indices[index]["foreign_key_name"]
                foreign_key_table = model_indices[index]["foreign_key_table"]
                foreign_key_column = model_indices[index]["foreign_key_column"]
                target_column = model_indices[index]["target_column"]
                command = f"""ALTER TABLE "{table_name}"
                              ADD CONSTRAINT {foreign_key_name}
                              FOREIGN KEY ("{foreign_key_column}") 
                              REFERENCES "{foreign_key_table}" ("{target_column}");"""
            commands.append(command)
        return commands


index_cache = IndexCache()
