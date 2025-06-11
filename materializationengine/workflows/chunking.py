import numpy as np
from celery.utils.log import get_task_logger
from geoalchemy2 import Geometry

from materializationengine.database import dynamic_annotation_cache
from materializationengine.utils import (
    create_annotation_model,
)


celery_logger = get_task_logger(__name__)


class ChunkingStrategy:
    """
    Represents a strategy for dividing spatial data into processable chunks.

    This class encapsulates the logic for determining spatial bounds, selecting
    an optimal chunking strategy, and generating chunks for processing.
    """

    def __init__(self, engine, table_name, database, base_chunk_size=1024):
        """
        Initialize a chunking strategy.

        Args:
            engine: SQLAlchemy engine for database operations
            table_name: Name of the annotation table to process
            database: Database name
            base_chunk_size: Base size of each chunk in spatial units
        """
        self.engine = engine
        self.table_name = table_name
        self.database = database
        self.base_chunk_size = base_chunk_size
        self.min_coords = None
        self.max_coords = None
        self.estimated_rows = None
        self.strategy_name = "grid"  # Default strategy
        self.total_chunks = 0
        self.actual_chunk_size = base_chunk_size
        self.data_chunk_bounds_list = None
        self._chunk_info = None
        self._chunk_generator = None

    def determine_bounds(self):
        """
        Determine spatial bounds of the table data.

        Returns:
            bool: True if bounds were successfully determined, False otherwise
        """
        if self.min_coords is not None and self.max_coords is not None:
            celery_logger.debug(f"Bounds already determined for {self.table_name}")
            return True

        self.min_coords, self.max_coords = self.get_table_geometry_bounds(
            self.engine, self.table_name, self.database
        )
        return self.min_coords is not None and self.max_coords is not None

    def get_table_geometry_bounds(self, engine, table_name: str, aligned_volume: str):
        """
        Get bounding box for a table's geometry columns.

        Args:
            engine: SQLAlchemy engine
            table_name: Name of the annotation table
            aligned_volume: Name of the aligned volume

        Returns:
            tuple: (min_coords, max_coords, bounding_box)
        """
        celery_logger.info(f"Getting bounding box for table {table_name}")

        try:
            db = dynamic_annotation_cache.get_db(aligned_volume)
            schema = db.database.get_table_schema(table_name)
            mat_metadata = {
                "annotation_table_name": table_name,
                "schema": schema,
                "pcg_table_name": "",  # Not needed for just getting geometry bounds
            }
            AnnotationModel = create_annotation_model(mat_metadata)

            geom_cols = []
            for column in AnnotationModel.__table__.columns:
                if (
                    isinstance(column.type, Geometry)
                    and "Z" in column.type.geometry_type.upper()
                ):
                    geom_cols.append(column.name)

            if not geom_cols:
                celery_logger.error(f"No PointZ columns found in {table_name}")
                return None, None, None

            celery_logger.info(f"Found geometry columns: {geom_cols}")

            # Get bounding box for each geometry column
            min_x, min_y, min_z = float("inf"), float("inf"), float("inf")
            max_x, max_y, max_z = float("-inf"), float("-inf"), float("-inf")

            with engine.connect() as connection:
                for col in geom_cols:
                    bbox_query = f"""
                    SELECT 
                        ST_XMin(ST_3DExtent({col})) as xmin,
                        ST_YMin(ST_3DExtent({col})) as ymin,
                        ST_ZMin(ST_3DExtent({col})) as zmin,
                        ST_XMax(ST_3DExtent({col})) as xmax,
                        ST_YMax(ST_3DExtent({col})) as ymax,
                        ST_ZMax(ST_3DExtent({col})) as zmax
                    FROM {table_name}
                    WHERE {col} IS NOT NULL
                    """

                    try:
                        bbox_result = connection.execute(bbox_query).fetchone()

                        if bbox_result and None not in bbox_result:
                            xmin, ymin, zmin, xmax, ymax, zmax = bbox_result
                            min_x = min(min_x, float(xmin))
                            min_y = min(min_y, float(ymin))
                            min_z = min(min_z, float(zmin))
                            max_x = max(max_x, float(xmax))
                            max_y = max(max_y, float(ymax))
                            max_z = max(max_z, float(zmax))
                            celery_logger.info(
                                f"Column {col} bounds: ({xmin},{ymin},{zmin}) to ({xmax},{ymax},{zmax})"
                            )
                    except Exception as e:
                        celery_logger.warning(
                            f"Error getting bounding box for column {col}: {str(e)}"
                        )
                        continue

            if min_x == float("inf") or max_x == float("-inf"):
                celery_logger.warning(
                    f"Could not determine bounds for table {table_name}"
                )
                return None, None, None

            min_coords = np.array([min_x, min_y, min_z])
            max_coords = np.array([max_x, max_y, max_z])

            chunk_size = self.actual_chunk_size
            aligned_min = np.floor(min_coords / chunk_size) * chunk_size
            aligned_max = np.ceil(max_coords / chunk_size) * chunk_size

            celery_logger.info(
                f"Bounding box calculation for table {table_name}:\n"
                f"  Raw bounds: {min_coords} to {max_coords}\n"
                f"  Aligned bounds: {aligned_min} to {aligned_max}\n"
                f"  Chunk size: {chunk_size}"
            )

            return aligned_min, aligned_max

        except Exception as e:
            celery_logger.error(
                f"Error querying database for bounds in table {table_name}: {str(e)}"
            )
            self.min_coords = None
            self.max_coords = None
            return None, None

    def estimate_row_count(self):
        """
        Estimate the number of rows in the table.

        Returns:
            int: Estimated number of rows
        """
        if self.estimated_rows is not None:
            return self.estimated_rows

        try:
            with self.engine.connect() as connection:
                size_query = """
                SELECT 
                    reltuples::bigint as est_rows,
                    pg_total_relation_size(%s) as table_size_bytes
                FROM pg_class c
                WHERE c.relname = %s
                """
                size_info = connection.execute(
                    size_query, (self.table_name, self.table_name)
                ).fetchone()
                self.estimated_rows = int(size_info.est_rows) if size_info else 0
                table_size_bytes = int(size_info.table_size_bytes) if size_info else 0

                celery_logger.info(
                    f"Table {self.table_name} has approximately {self.estimated_rows:,} rows "
                    f"({table_size_bytes/(1024*1024):.1f} MB)"
                )

                return self.estimated_rows
        except Exception as e:
            celery_logger.error(f"Error estimating row count: {str(e)}")
            self.estimated_rows = 0
            return 0

    def select_strategy(self):
        """
        Select the optimal chunking strategy based on table characteristics.

        Returns:
            str: Selected strategy name ("grid" or "data_chunks")
        """
        if not self.determine_bounds():
            celery_logger.warning(
                "Could not determine bounds, falling back to grid strategy"
            )
            self.strategy_name = "grid"
            if self.min_coords is None or self.max_coords is None:
                self.min_coords = np.array([0, 0, 0])
                self.max_coords = np.array([0, 0, 0])
                celery_logger.warning(
                    f"Using default zero-volume bounds for {self.table_name} due to earlier error."
                )
            self._create_grid_chunking()
            return self.strategy_name

        if self.estimated_rows is None:
            self.estimate_row_count()

        # For small/medium tables (under 1 million rows), use data-specific approach
        SMALL_TABLE_THRESHOLD = 1000000
        if self.estimated_rows < SMALL_TABLE_THRESHOLD:
            celery_logger.info(
                f"Using data-specific chunking for table with {self.estimated_rows} rows"
            )

            # Calculate appropriate chunk size based on data distribution
            target_chunks = min(5000, max(1000, self.estimated_rows // 50))

            # Calculate volume of the bounding box and volume per chunk
            volume = np.prod(np.array(self.max_coords) - np.array(self.min_coords))
            vol_per_chunk = volume / target_chunks

            # Calculate chunk size as cube root of vol_per_chunk
            data_chunk_size = int(np.cbrt(vol_per_chunk))

            # Ensure chunk size is at least base_chunk_size
            self.actual_chunk_size = max(self.base_chunk_size, data_chunk_size)

            # Check if data-specific chunking is feasible
            data_chunking_info = self._create_data_specific_chunks()

            if data_chunking_info:
                data_specific_bounds, chunk_count = data_chunking_info
                self.data_chunk_bounds_list = data_specific_bounds
                self.total_chunks = chunk_count
                self.strategy_name = "data_chunks"
                celery_logger.info(
                    f"Using data-specific chunking with {self.total_chunks} chunks of size {self.actual_chunk_size}"
                )
                return self.strategy_name

        # Fall back to grid approach
        self._create_grid_chunking()
        self.strategy_name = "grid"
        celery_logger.info(
            f"Using grid approach with {self.total_chunks} chunks of size {self.actual_chunk_size}"
        )
        return self.strategy_name

    def _create_grid_chunking(self):
        """
        Calculate grid-based chunking parameters.
        """
        if not self.determine_bounds():
            celery_logger.error("Cannot create grid chunking without spatial bounds")
            self.min_coords = (
                self.min_coords if self.min_coords is not None else np.array([0, 0, 0])
            )
            self.max_coords = (
                self.max_coords if self.max_coords is not None else np.array([0, 0, 0])
            )
            self.total_chunks = 1
            self._chunk_info = {
                "min_coords": self.min_coords,
                "max_coords": self.max_coords,
                "chunk_size": self.actual_chunk_size,
                "x_chunks": 1,
                "y_chunks": 1,
                "z_chunks": 1,
                "total_chunks": 1,
            }
            return

        # Calculate spans
        x_span = self.max_coords[0] - self.min_coords[0]
        y_span = self.max_coords[1] - self.min_coords[1]
        z_span = self.max_coords[2] - self.min_coords[2]

        # Calculate chunks in each dimension
        x_chunks = max(1, int(np.ceil(x_span / self.actual_chunk_size)))
        y_chunks = max(1, int(np.ceil(y_span / self.actual_chunk_size)))
        z_chunks = max(1, int(np.ceil(z_span / self.actual_chunk_size)))

        self.total_chunks = x_chunks * y_chunks * z_chunks

        celery_logger.info(
            f"Grid chunking: {x_chunks}×{y_chunks}×{z_chunks} = {self.total_chunks} chunks "
            f"with chunk size {self.actual_chunk_size}. "
            f"Spans: x={x_span:.1f}, y={y_span:.1f}, z={z_span:.1f}."
        )

        self._chunk_info = {
            "min_coords": self.min_coords,
            "max_coords": self.max_coords,
            "chunk_size": self.actual_chunk_size,
            "x_chunks": x_chunks,
            "y_chunks": y_chunks,
            "z_chunks": z_chunks,
            "total_chunks": self.total_chunks,
            "table_name": self.table_name,
            "database": self.database,
        }

    def _create_data_specific_chunks(self):
        """
        Create data-specific chunks based on where data actually exists.

        Returns:
            tuple: (chunk_generator_function, chunk_count) or None if failed
        """
        try:
            db = dynamic_annotation_cache.get_db(self.database)
            schema = db.database.get_table_schema(self.table_name)
            mat_metadata = {
                "annotation_table_name": self.table_name,
                "schema": schema,
                "pcg_table_name": "",  # Not needed for just getting geometry bounds
            }
            AnnotationModel = create_annotation_model(mat_metadata)

            geom_cols = []
            for column in AnnotationModel.__table__.columns:
                if (
                    isinstance(column.type, Geometry)
                    and "Z" in column.type.geometry_type.upper()
                ):
                    geom_cols.append(column.name)

            if not geom_cols:
                celery_logger.error(f"No PointZ columns found in {self.table_name}")
                return None

            # Create a UNION query to find populated regions across all geometry columns
            union_queries = []
            for col in geom_cols:
                union_queries.append(
                    f"""
                SELECT 
                    FLOOR(ST_X({col})/{self.actual_chunk_size}) * {self.actual_chunk_size} as x_min,
                    FLOOR(ST_Y({col})/{self.actual_chunk_size}) * {self.actual_chunk_size} as y_min,
                    FLOOR(ST_Z({col})/{self.actual_chunk_size}) * {self.actual_chunk_size} as z_min
                FROM {self.table_name}
                WHERE {col} IS NOT NULL
                """
                )

            # Combine all geometry column queries and group by the grid cells
            data_chunk_query = f"""
            WITH all_points AS (
                {" UNION ALL ".join(union_queries)}
            )
            SELECT x_min, y_min, z_min, COUNT(*) as point_count
            FROM all_points
            GROUP BY x_min, y_min, z_min
            """

            # Get the count of chunks without loading all data
            with self.engine.connect() as connection:
                count_query = f"SELECT COUNT(*) FROM ({data_chunk_query}) as chunks"
                chunk_count = connection.execute(count_query).scalar()

                if chunk_count == 0:
                    celery_logger.info(
                        f"No populated data-specific chunks found for {self.table_name}."
                    )
                    return [], 0

                celery_logger.info(
                    f"Found {chunk_count} populated data-specific chunks for {self.table_name}."
                )

            data_specific_bounds = []
            with self.engine.connect() as conn:
                result = conn.execute(
                    data_chunk_query
                )  # No server-side cursor needed if chunk_count is reasonable
                for row in result:
                    min_corner = [
                        float(row.x_min),
                        float(row.y_min),
                        float(row.z_min),
                    ]
                    max_corner = [
                        float(row.x_min) + self.actual_chunk_size,
                        float(row.y_min) + self.actual_chunk_size,
                        float(row.z_min) + self.actual_chunk_size,
                    ]
                    data_specific_bounds.append((min_corner, max_corner))

            if len(data_specific_bounds) != chunk_count:
                celery_logger.warning(
                    f"Data chunk count mismatch for {self.table_name}: query said {chunk_count}, collected {len(data_specific_bounds)}. Using collected."
                )
                chunk_count = len(data_specific_bounds)

            return data_specific_bounds, chunk_count

        except Exception as e:
            celery_logger.error(f"Error creating data-specific chunks: {str(e)}")
            import traceback

            celery_logger.error(traceback.format_exc())
            return None

    def create_chunk_generator(self):
        """
        Create a generator function that yields chunks.

        Returns:
            function: Generator that yields (min_corner, max_corner) tuples
        """
        if self._chunk_generator is not None:
            return self._chunk_generator

        if self.strategy_name == "data_chunks":
            if hasattr(self, "data_chunk_bounds_list") and self.data_chunk_bounds_list:

                def data_chunk_list_generator():
                    yield from self.data_chunk_bounds_list

                self._chunk_generator = data_chunk_list_generator
                return self._chunk_generator
            else:
                celery_logger.warning(
                    f"Data_chunks strategy selected for {self.table_name} but data_chunk_bounds_list is missing or empty. Falling back to empty generator."
                )

                def empty_gen():
                    yield from []

                self._chunk_generator = empty_gen
                return self._chunk_generator

        if self._chunk_info is None:
            self._create_grid_chunking()

        def grid_generator():
            min_coords = self._chunk_info["min_coords"]
            max_coords = self._chunk_info["max_coords"]
            x_chunks = self._chunk_info["x_chunks"]
            y_chunks = self._chunk_info["y_chunks"]
            z_chunks = self._chunk_info["z_chunks"]
            chunk_size = self._chunk_info["chunk_size"]

            for x_idx in range(x_chunks):
                for y_idx in range(y_chunks):
                    for z_idx in range(z_chunks):
                        x_start = min_coords[0] + x_idx * chunk_size
                        y_start = min_coords[1] + y_idx * chunk_size
                        z_start = min_coords[2] + z_idx * chunk_size
                        x_end = min(x_start + chunk_size, max_coords[0])
                        y_end = min(y_start + chunk_size, max_coords[1])
                        z_end = min(z_start + chunk_size, max_coords[2])

                        min_corner = np.array([x_start, y_start, z_start])
                        max_corner = np.array([x_end, y_end, z_end])

                        yield (min_corner, max_corner)

        self._chunk_generator = grid_generator
        return self._chunk_generator

    def skip_to_index(self, start_index):
        """
        Create a generator that skips to a specific starting index.

        Args:
            start_index: Index to start from

        Returns:
            function: Generator that yields (min_corner, max_corner) tuples
        """
        if start_index <= 0:
            return self.create_chunk_generator()

        if self.strategy_name == "grid" and self._chunk_info:

            def grid_skip_generator():
                min_coords = self._chunk_info["min_coords"]
                max_coords = self._chunk_info["max_coords"]
                x_chunks = self._chunk_info["x_chunks"]
                y_chunks = self._chunk_info["y_chunks"]
                z_chunks = self._chunk_info["z_chunks"]
                chunk_size = self._chunk_info["chunk_size"]

                remaining = start_index
                x_idx = remaining // (y_chunks * z_chunks)
                remaining %= y_chunks * z_chunks
                y_idx = remaining // z_chunks
                z_idx = remaining % z_chunks

                for x_i in range(x_idx, x_chunks):
                    for y_i in range(y_idx if x_i == x_idx else 0, y_chunks):
                        for z_i in range(
                            z_idx if (x_i == x_idx and y_i == y_idx) else 0, z_chunks
                        ):
                            x_start = min_coords[0] + x_i * chunk_size
                            y_start = min_coords[1] + y_i * chunk_size
                            z_start = min_coords[2] + z_i * chunk_size
                            x_end = min(x_start + chunk_size, max_coords[0])
                            y_end = min(y_start + chunk_size, max_coords[1])
                            z_end = min(z_start + chunk_size, max_coords[2])

                            min_corner = np.array([x_start, y_start, z_start])
                            max_corner = np.array([x_end, y_end, z_end])

                            yield (min_corner, max_corner)

            return grid_skip_generator

        if self.strategy_name == "data_chunks":
            if hasattr(self, "data_chunk_bounds_list") and self.data_chunk_bounds_list:

                def data_chunk_skip_generator():
                    if 0 <= start_index < len(self.data_chunk_bounds_list):
                        yield from self.data_chunk_bounds_list[start_index:]
                    elif start_index >= len(self.data_chunk_bounds_list):
                        celery_logger.warning(
                            f"skip_to_index for data_chunks: start_index {start_index} is out of bounds "
                            f"(total: {len(self.data_chunk_bounds_list)}). Yielding nothing."
                        )
                        yield from [] 
                    else: 
                        yield from self.data_chunk_bounds_list

                return data_chunk_skip_generator
            else:
                celery_logger.warning(
                    f"skip_to_index for data_chunks: data_chunk_bounds_list missing for {self.table_name}. Yielding nothing."
                )

                def empty_gen():
                    yield from []

                return empty_gen

        generator_func = self.create_chunk_generator()

        def skip_and_yield():
            gen = generator_func()
            skipped = 0
            try:
                while skipped < start_index:
                    next(gen)
                    skipped += 1
            except StopIteration:
                celery_logger.warning(
                    f"skip_to_index: Attempted to skip {start_index} chunks, "
                    f"but only found {skipped}. No further chunks to yield."
                )
                return

            for chunk in gen:
                yield chunk

        return skip_and_yield

    def to_dict(self):
        """
        Convert strategy to a dictionary for serialization.

        Returns:
            dict: Strategy data in serializable form
        """
        if self._chunk_info is None and self.strategy_name == "grid":
            self._create_grid_chunking()

        result = {
            "strategy_name": self.strategy_name,
            "base_chunk_size": self.base_chunk_size,
            "actual_chunk_size": self.actual_chunk_size,
            "min_coords": (
                self.min_coords.tolist()
                if isinstance(self.min_coords, np.ndarray)
                else self.min_coords
            ),
            "max_coords": (
                self.max_coords.tolist()
                if isinstance(self.max_coords, np.ndarray)
                else self.max_coords
            ),
            "total_chunks": self.total_chunks,
            "estimated_rows": self.estimated_rows,
            "table_name": self.table_name,
            "database": self.database,
        }
        if self.strategy_name == "grid" and self._chunk_info:
            result["grid_info"] = {
                "x_chunks": self._chunk_info.get("x_chunks"),
                "y_chunks": self._chunk_info.get("y_chunks"),
                "z_chunks": self._chunk_info.get("z_chunks"),
            }
        elif self.strategy_name == "data_chunks" and hasattr(
            self, "data_chunk_bounds_list"
        ):
            result["data_chunk_bounds_list"] = self.data_chunk_bounds_list
        return result

    @classmethod
    def from_checkpoint(cls, checkpoint_data, engine, table_name, database):
        """
        Create a strategy instance from checkpoint data.

        Args:
            checkpoint_data: WorkflowData object from checkpoint
            engine: SQLAlchemy engine
            table_name: Table name
            database: Database name

        Returns:
            ChunkingStrategy: Reconstructed strategy
        """
        strategy = cls(engine, table_name, database)

        if (
            hasattr(checkpoint_data, "min_enclosing_bbox")
            and checkpoint_data.min_enclosing_bbox
        ):
            strategy.min_coords = np.array(checkpoint_data.min_enclosing_bbox[0])
            strategy.max_coords = np.array(checkpoint_data.min_enclosing_bbox[1])

        strategy.strategy_name = getattr(checkpoint_data, "chunking_strategy", "grid")
        strategy.total_chunks = getattr(checkpoint_data, "total_chunks", 0)
        strategy.actual_chunk_size = getattr(
            checkpoint_data, "used_chunk_size", strategy.base_chunk_size
        )
        strategy.estimated_rows = getattr(checkpoint_data, "total_row_estimate", None)

        if strategy.strategy_name == "data_chunks":
            strategy.data_chunk_bounds_list = checkpoint_data.chunking_parameters.get(
                "data_chunk_bounds_list", []
            )
            if not strategy.data_chunk_bounds_list and strategy.total_chunks > 0:
                celery_logger.warning(
                    f"Data_chunks strategy for {table_name} loaded from checkpoint, but data_chunk_bounds_list is empty. Total_chunks: {strategy.total_chunks}"
                )
            elif strategy.total_chunks != len(strategy.data_chunk_bounds_list):
                celery_logger.warning(
                    f"Data_chunks for {table_name}: total_chunks ({strategy.total_chunks}) mismatch with len(data_chunk_bounds_list) ({len(strategy.data_chunk_bounds_list)}). Check parameters."
                )
               
        if (
            strategy.strategy_name == "grid"
            and strategy.min_coords is not None
            and strategy.max_coords is not None
        ):
            x_span = strategy.max_coords[0] - strategy.min_coords[0]
            y_span = strategy.max_coords[1] - strategy.min_coords[1]
            z_span = strategy.max_coords[2] - strategy.min_coords[2]
            x_chunks = max(1, int(np.ceil(x_span / strategy.actual_chunk_size)))
            y_chunks = max(1, int(np.ceil(y_span / strategy.actual_chunk_size)))
            z_chunks = max(1, int(np.ceil(z_span / strategy.actual_chunk_size)))
            strategy._chunk_info = {
                "min_coords": strategy.min_coords,
                "max_coords": strategy.max_coords,
                "chunk_size": strategy.actual_chunk_size,
                "x_chunks": x_chunks,
                "y_chunks": y_chunks,
                "z_chunks": z_chunks,
                "total_chunks": strategy.total_chunks,
                "table_name": strategy.table_name,
                "database": strategy.database,
            }
            calculated_total = x_chunks * y_chunks * z_chunks
            if (
                strategy.total_chunks != calculated_total and calculated_total > 0
            ): 
                celery_logger.warning(
                    f"Reconstructed total_chunks ({calculated_total}) for {table_name} "
                    f"differs from checkpoint_data.total_chunks ({strategy.total_chunks}). Using calculated."
                )
              
                if strategy.total_chunks == 0 and calculated_total > 0:
                    celery_logger.warning(
                        f"Total chunks from checkpoint is 0 but calculated is {calculated_total}. Check parameters."
                    )
                 

        return strategy


def reconstruct_chunk_bounds(
    chunk_index: int, chunking_parameters: dict
) -> tuple[list[float], list[float]]:
    """
    Reconstructs the bounding box of a specific chunk given its index and the chunking strategy parameters.

    Args:
        chunk_index: The 0-based index of the chunk.
        chunking_parameters: A dictionary containing the parameters of the chunking strategy,
                             as produced by ChunkingStrategy.to_dict().
                             Expected keys for 'grid' strategy:
                                'strategy_name': 'grid'
                                'min_coords': [x,y,z]
                                'max_coords': [x,y,z]
                                'actual_chunk_size': size
                                'grid_info': {'x_chunks': xc, 'y_chunks': yc, 'z_chunks': zc}
                             For 'data_chunks', reconstruction is not supported by index alone without
                             re-querying or having the full list of chunk bounds. This function
                             currently only supports 'grid'.

    Returns:
        A tuple (min_corner, max_corner) for the chunk.

    Raises:
        ValueError: If chunk_index is out of bounds or parameters are missing/invalid.
        NotImplementedError: If the strategy is not 'grid'.
    """
    strategy_name = chunking_parameters.get("strategy_name")
    if strategy_name != "grid":
        if strategy_name == "data_chunks":
            data_chunk_bounds_list = chunking_parameters.get("data_chunk_bounds_list")
            if not isinstance(data_chunk_bounds_list, list):
                raise ValueError(
                    "Missing or invalid 'data_chunk_bounds_list' for 'data_chunks' strategy."
                )

            total_data_chunks = len(data_chunk_bounds_list)
            if not (0 <= chunk_index < total_data_chunks):
                raise ValueError(
                    f"Chunk index {chunk_index} is out of bounds for 'data_chunks' strategy. "
                    f"Total data chunks: {total_data_chunks}."
                )
            return data_chunk_bounds_list[
                chunk_index
            ]  
        raise NotImplementedError(
            f"Chunk reconstruction for strategy '{strategy_name}' is not yet implemented."
        )

    min_coords_list = chunking_parameters.get("min_coords")
    max_coords_list = chunking_parameters.get("max_coords")
    actual_chunk_size = chunking_parameters.get("actual_chunk_size")
    grid_info = chunking_parameters.get("grid_info")

    if not all([min_coords_list, max_coords_list, actual_chunk_size, grid_info]):
        raise ValueError(
            "Missing required chunking parameters for 'grid' strategy in reconstruct_chunk_bounds."
        )

    min_coords = np.array(min_coords_list)
    max_coords = np.array(max_coords_list)

    x_chunks = grid_info.get("x_chunks")
    y_chunks = grid_info.get("y_chunks")
    z_chunks = grid_info.get("z_chunks")
    total_stored_chunks = chunking_parameters.get(
        "total_chunks", x_chunks * y_chunks * z_chunks
    )

    if not all(isinstance(c, int) and c > 0 for c in [x_chunks, y_chunks, z_chunks]):
        raise ValueError(
            f"Invalid x_chunks, y_chunks, or z_chunks in grid_info: {grid_info}"
        )

    calculated_total_chunks = x_chunks * y_chunks * z_chunks
    if chunk_index < 0 or chunk_index >= calculated_total_chunks:
      
        raise ValueError(
            f"Chunk index {chunk_index} is out of bounds. "
            f"Total chunks based on grid dimensions: {calculated_total_chunks}. "
            f"(Stored total_chunks: {total_stored_chunks})"
        )

    x_idx = chunk_index // (y_chunks * z_chunks)
    remainder = chunk_index % (y_chunks * z_chunks)
    y_idx = remainder // z_chunks
    z_idx = remainder % z_chunks

    if not (0 <= x_idx < x_chunks and 0 <= y_idx < y_chunks and 0 <= z_idx < z_chunks):
        raise ValueError(
            f"Calculated indices ({x_idx}, {y_idx}, {z_idx}) are out of bounds "
            f"for grid dimensions ({x_chunks}, {y_chunks}, {z_chunks}) from chunk_index {chunk_index}."
            f"This indicates an inconsistency in total_chunks or chunk_index."
        )

    x_start = min_coords[0] + x_idx * actual_chunk_size
    y_start = min_coords[1] + y_idx * actual_chunk_size
    z_start = min_coords[2] + z_idx * actual_chunk_size

    # Ensure reconstructed bounds do not exceed the original max_coords
    x_end = min(x_start + actual_chunk_size, max_coords[0])
    y_end = min(y_start + actual_chunk_size, max_coords[1])
    z_end = min(z_start + actual_chunk_size, max_coords[2])

    chunk_min_corner = [x_start, y_start, z_start]
    chunk_max_corner = [x_end, y_end, z_end]

    return chunk_min_corner, chunk_max_corner
