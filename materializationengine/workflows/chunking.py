
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
        self._chunk_info = None
        self._chunk_generator = None
        
    def determine_bounds(self):
        """
        Determine spatial bounds of the table data.
        
        Returns:
            bool: True if bounds were successfully determined, False otherwise
        """
        if self.min_coords is not None and self.max_coords is not None:
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
                celery_logger.warning(f"Could not determine bounds for table {table_name}")
                return None, None, None

            min_coords = np.array([min_x, min_y, min_z])
            max_coords = np.array([max_x, max_y, max_z])

            return min_coords, max_coords

        except Exception as e:
            celery_logger.error(f"Error querying database: {str(e)}")
            raise


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
            celery_logger.warning("Could not determine bounds, falling back to grid strategy")
            self.strategy_name = "grid"
            return self.strategy_name
            
        if self.estimated_rows is None:
            self.estimate_row_count()
            
        # For small/medium tables (under 1 million rows), use data-specific approach
        SMALL_TABLE_THRESHOLD = 1000000
        if self.estimated_rows < SMALL_TABLE_THRESHOLD:
            celery_logger.info(f"Using data-specific chunking for table with {self.estimated_rows} rows")
            
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
                self._chunk_generator, self.total_chunks = data_chunking_info
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
                union_queries.append(f"""
                SELECT 
                    FLOOR(ST_X({col})/{self.actual_chunk_size}) * {self.actual_chunk_size} as x_min,
                    FLOOR(ST_Y({col})/{self.actual_chunk_size}) * {self.actual_chunk_size} as y_min,
                    FLOOR(ST_Z({col})/{self.actual_chunk_size}) * {self.actual_chunk_size} as z_min
                FROM {self.table_name}
                WHERE {col} IS NOT NULL
                """)
            
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
                    return None
                    
                celery_logger.info(f"Found {chunk_count} populated chunks across all geometry columns")
            
            # Create a generator function that will execute the query and yield chunks when called
            def chunks_generator():
                with self.engine.connect() as conn:
                    # Use server-side cursor for memory efficiency
                    result = conn.execution_options(stream_results=True).execute(data_chunk_query)
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
                        yield (min_corner, max_corner)
            
            return chunks_generator, chunk_count
        
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
            data_chunking_info = self._create_data_specific_chunks()
            if data_chunking_info:
                self._chunk_generator, _ = data_chunking_info
                return self._chunk_generator
        
        # Grid generator
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
                remaining %= (y_chunks * z_chunks)
                y_idx = remaining // z_chunks
                z_idx = remaining % z_chunks
                
                for x_i in range(x_idx, x_chunks):
                    for y_i in range(y_idx if x_i == x_idx else 0, y_chunks):
                        for z_i in range(z_idx if (x_i == x_idx and y_i == y_idx) else 0, z_chunks):
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
        

        generator = self.create_chunk_generator()
        
        def skip_generator():
            gen = generator()
            skipped = 0
            try:
                while skipped < start_index:
                    next(gen)
                    skipped += 1
            except StopIteration:
                # Ran out of chunks to skip
                return
            
            # Yield all remaining chunks
            try:
                while True:
                    yield next(gen)
            except StopIteration:
                return
        
        return skip_generator
            
    def to_dict(self):
        """
        Convert strategy to a dictionary for serialization.
        
        Returns:
            dict: Strategy data in serializable form
        """
        if self._chunk_info is None and self.strategy_name == "grid":
            self._create_grid_chunking()
            
        return {
            "strategy_name": self.strategy_name,
            "base_chunk_size": self.base_chunk_size,
            "actual_chunk_size": self.actual_chunk_size,
            "min_coords": self.min_coords.tolist() if isinstance(self.min_coords, np.ndarray) else self.min_coords,
            "max_coords": self.max_coords.tolist() if isinstance(self.max_coords, np.ndarray) else self.max_coords,
            "total_chunks": self.total_chunks,
            "estimated_rows": self.estimated_rows,
        }
        
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
        
        if hasattr(checkpoint_data, 'min_enclosing_bbox') and checkpoint_data.min_enclosing_bbox:
            strategy.min_coords = np.array(checkpoint_data.min_enclosing_bbox[0])
            strategy.max_coords = np.array(checkpoint_data.min_enclosing_bbox[1])
            
        strategy.strategy_name = getattr(checkpoint_data, 'chunking_strategy', 'grid')
        strategy.total_chunks = getattr(checkpoint_data, 'total_chunks', 0)
        strategy.actual_chunk_size = getattr(checkpoint_data, 'used_chunk_size', strategy.base_chunk_size)
        strategy.estimated_rows = getattr(checkpoint_data, 'total_row_estimate', None)
        
        return strategy