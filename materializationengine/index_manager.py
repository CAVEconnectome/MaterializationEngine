from geoalchemy2.types import Geometry
from sqlalchemy import engine, MetaData
from sqlalchemy.engine import reflection


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
        insp = reflection.Inspector.from_engine(engine)
        try:
            pk_columns = insp.get_pk_constraint(table_name)
            indexed_columns = insp.get_indexes(table_name)
            foreign_keys = insp.get_foreign_keys(table_name)
        except Exception as e:
            print(f"No table named '{table_name}', error: {e}")
            return None
        index_map = {}
        if pk_columns:
            pk_name = {"primary_key_name": pk_columns.get("name")}
            if pk_name["primary_key_name"]:
                pk = {"index_name": f"{pk_columns['name']}", "type": "primary_key"}
                index_map.update({pk_columns["constrained_columns"][0]: pk})

        if indexed_columns:
            for index in indexed_columns:
                dialect_options = index.get("dialect_options", None)

                indx_map = {"index_name": index["name"]}
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

                index_map.update({index["column_names"][0]: indx_map})
        if foreign_keys:
            for foreign_key in foreign_keys:
                foreign_key_name = foreign_key["name"]
                fk_data = {
                    "type": "foreign_key",
                    "foreign_key_name": foreign_key_name,
                    "foreign_key_table": foreign_key["referred_table"],
                    "foreign_key_column": foreign_key["constrained_columns"][0],
                    "target_column": foreign_key["referred_columns"][0],
                }
                index_map.update({foreign_key_name: fk_data})
        return index_map

    def get_index_from_model(self, model, engine):
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
                pk = {"index_name": f"{model.name}_pkey", "type": "primary_key"}
                index_map.update({column.name: pk})
            if column.index:
                indx_map = {
                    "index_name": f"ix_{model.name}_{column.name}",
                    "type": "index",
                    "dialect_options": None,
                }
                index_map.update({column.name: indx_map})
            if isinstance(column.type, Geometry):
                spatial_index_map = {
                    "index_name": f"idx_{model.name}_{column.name}",
                    "type": "spatial_index",
                    "dialect_options": {"postgresql_using": "gist"},
                }
                index_map.update({column.name: spatial_index_map})
            if column.foreign_keys:
                insp = reflection.Inspector.from_engine(engine)
                metadata_obj = MetaData()
                metadata_obj.reflect(bind=engine)

                foreign_keys = list(column.foreign_keys)
                for foreign_key in foreign_keys:

                    (
                        target_table_name,
                        target_column,
                    ) = foreign_key.target_fullname.split(".")
                    ref_table = metadata_obj.tables[target_table_name]
                    foreign_key_name = f"{target_table_name}_{target_column}_fkey"
                    foreign_key_map = {
                        "type": "foreign_key",
                        "foreign_key_name": foreign_key_name,
                        "foreign_key_table": target_table_name,
                        "foreign_key_column": foreign_key.constraint.column_keys[0],
                        "target_column": target_column,
                    }
                    index_map.update({foreign_key_name: foreign_key_map})
        return index_map

    def drop_table_indices(self, table_name: str, engine):
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
            if "primary_key" in column_info["type"]:
                constraints_list.append(
                    f"{command} DROP CONSTRAINT IF EXISTS {column_info['index_name']}"
                )

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

    def add_indices_sql_commands(self, table_name: str, model, engine):
        """Add missing indices by comparing reflected table and
        model indices. Will add missing indices from model to table.

        Args:
            table_name (str): target table to drop constraints and indices
            engine (SQLAlchemy Engine instance): supplied SQLAlchemy engine

        Returns:
            str: list of indices added to table
        """
        current_indices = self.get_table_indices(table_name, engine)
        model_indices = self.get_index_from_model(model, engine)
        missing_indices = set(model_indices) - set(current_indices)
        commands = []
        for column_name in missing_indices:
            index_type = model_indices[column_name]["type"]
            if index_type == "primary_key":
                command = f"ALTER TABLE {table_name} add primary key({column_name});"
            if index_type == "index":
                command = f"CREATE INDEX IF NOT EXISTS ix_{table_name}_{column_name} ON {table_name} ({column_name});"
            if index_type == "spatial_index":
                command = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{column_name} ON {table_name} USING GIST ({column_name} gist_geometry_ops_nd);"
            if index_type == "foreign_key":
                foreign_key_name = model_indices[column_name]["foreign_key_name"]
                foreign_key_table = model_indices[column_name]["foreign_key_table"]
                foreign_key_column = model_indices[column_name]["foreign_key_column"]
                target_column = model_indices[column_name]["target_column"]
                command = f"""ALTER TABLE "{table_name}"
                              ADD CONSTRAINT {foreign_key_name}
                              FOREIGN KEY ("{foreign_key_column}") 
                              REFERENCES "{foreign_key_table}" ("{target_column}");"""
                missing_indices.add(foreign_key_name)
            commands.append(command)
        return commands


index_cache = IndexCache()
