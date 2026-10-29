import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import marshmallow as mm
import pandas as pd
import numpy as np
from emannotationschemas import get_schema
from emannotationschemas.models import make_model_from_schema, split_annotation_schema
from emannotationschemas.schemas.base import ReferenceAnnotation, SpatialPoint
from geoalchemy2 import WKBElement
from shapely.geometry import Point
from sqlalchemy import inspect

logger = logging.getLogger(__name__)


def create_wkt_element(geom):
    return WKBElement(geom.wkb)


# EWKB for a POINT Z: little-endian byte order (01) and type 0x80000001 (Point with the Z flag).
_POINTZ_WKB_HEADER = np.frombuffer(bytes.fromhex("0101000080"), dtype=np.uint8)
# Two hex characters per byte value, as one uint16 each so a lookup builds the hex string in place.
_HEX_PAIRS = np.array([f"{i:02x}" for i in range(256)], dtype="S2").view(np.uint16)


def encode_points_wkb_hex(xyz: np.ndarray) -> np.ndarray:
    """Encode an (n, 3) array of coordinates as hex EWKB POINT Z strings.

    Gives the same text as str(create_wkt_element(Point(x, y, z))) for each row,
    without a Python call per point. Rows with any NaN coordinate become None.
    """
    xyz = np.ascontiguousarray(xyz, dtype="<f8").reshape(-1, 3)
    n = len(xyz)
    raw = np.empty((n, 29), dtype=np.uint8)
    raw[:, :5] = _POINTZ_WKB_HEADER
    raw[:, 5:] = xyz.view(np.uint8).reshape(n, 24)
    encoded = _HEX_PAIRS[raw].view("S58").ravel().astype(str).astype(object)
    encoded[np.isnan(xyz).any(axis=1)] = None
    return encoded


def _parse_combined_xyz(value) -> Tuple[float, float, float]:
    """Parse one combined "[x,y,z]" value; anything unusable gives NaNs."""
    try:
        if isinstance(value, str):
            coords = [float(x.strip()) for x in value.strip("[]()").split(",")]
        elif isinstance(value, (list, tuple, np.ndarray)):
            coords = [float(x) for x in value]
        else:
            return (np.nan, np.nan, np.nan)
    except (ValueError, TypeError):
        return (np.nan, np.nan, np.nan)
    if len(coords) != 3:
        return (np.nan, np.nan, np.nan)
    return tuple(coords)


SUPERVOXEL_SUFFIX = "_supervoxel_id"
ROOT_SUFFIX = "_root_id"
# Above this a float has lost integer precision: supervoxel and root ids (~8.6e16 to
# ~8.6e17 on minnie) do not survive a trip through float64.
_EXACT_FLOAT_LIMIT = 2**53


def segmentation_columns(schema_type: str) -> Dict[str, Tuple[str, str]]:
    """{point field: (supervoxel id column, root id column)} of the segmentation table,
    for each bound spatial point of the schema (synapse: pre_pt and post_pt)."""
    _, flat_segmentation = split_annotation_schema(get_schema(schema_type))
    names = list(flat_segmentation._declared_fields)
    return {
        name[: -len(SUPERVOXEL_SUFFIX)]: (name, f"{name[: -len(SUPERVOXEL_SUFFIX)]}{ROOT_SUFFIX}")
        for name in names
        if name.endswith(SUPERVOXEL_SUFFIX)
        and f"{name[: -len(SUPERVOXEL_SUFFIX)]}{ROOT_SUFFIX}" in names
    }


def exact_ids(series: pd.Series, column: str) -> np.ndarray:
    """A column of supplied supervoxel or root ids as int64, with blanks as 0.

    Strings are parsed exactly (the CSV reader keeps these columns as text, see
    GCSCsvProcessor.text_columns); floats are refused once they are too large to hold
    an id exactly, which is where a nullable integer column read as float64 ends up.
    """
    if pd.api.types.is_integer_dtype(series.dtype):
        values = series.fillna(0).to_numpy(dtype=np.int64)
    elif pd.api.types.is_float_dtype(series.dtype):
        floats = series.fillna(0).to_numpy(dtype=float)
        if np.any(np.abs(floats) >= _EXACT_FLOAT_LIMIT) or np.any(floats != np.floor(floats)):
            raise ValueError(
                f"Column {column} holds ids as floating point numbers, which cannot store "
                f"them exactly; write it as integers"
            )
        values = floats.astype(np.int64)
    else:
        text = series.astype("string").str.strip().fillna("")
        text = text.str.replace(r"\.0+$", "", regex=True).replace("", "0")
        try:
            values = np.array([int(v) for v in text], dtype=np.int64)
        except (ValueError, OverflowError) as e:
            raise ValueError(f"Column {column} has a value that is not an id: {e}") from None
    if np.any(values < 0):
        raise ValueError(f"Column {column} has negative ids")
    return values


class SchemaProcessor:
    """Processes CSV data according to schema types, handling spatial points and references"""

    def __init__(
        self,
        schema_type: str,
        reference_table: str = None,
        column_mapping: Dict[str, str] = None,
        ignored_columns: List[str] = None,
        id_counter_start: int = 0,
    ):
        """
        Initialize processor with schema name and optional column mapping

        Args:
            schema_type: Name of the schema to use
            reference_table: Reference table name for reference schemas
            column_mapping: Dictionary mapping schema column names to CSV column names
            ignored_columns: List of CSV column names to ignore during processing
        """
        self.schema = get_schema(schema_type)
        self.schema_name = schema_type
        self.is_reference = issubclass(self.schema, ReferenceAnnotation)
        raw_column_mapping = column_mapping or {}
        self.column_mapping = {
            schema_col: csv_col
            for schema_col, csv_col in raw_column_mapping.items()
            if csv_col and csv_col.strip()
        }
        self.reverse_mapping = {v: k for k, v in self.column_mapping.items()}
        self.ignored_columns = set(ignored_columns or [])
        self.generate_ids = "id" not in self.column_mapping
        self._id_counter = id_counter_start

        if self.is_reference and reference_table is None:
            raise ValueError(
                f"Schema {schema_type} is a reference type and requires a reference_table"
            )

        self.spatial_points = self._get_spatial_point_fields()
        # supervoxel and root ids the source already has, for the segmentation table
        self.segmentation_columns = segmentation_columns(schema_type)
        self.supplied_segmentation_columns = [
            column
            for sv_col, root_col in self.segmentation_columns.values()
            for column in (sv_col, root_col)
            if column in self.column_mapping
        ]
        # the point fields whose supervoxels the spatial lookup still has to find
        self.lookup_fields = [
            field
            for field, (sv_col, _) in self.segmentation_columns.items()
            if sv_col not in self.column_mapping
        ]
        self.segmentation_column_order = ["id"] + [
            column
            for sv_col, root_col in self.segmentation_columns.values()
            for column in (sv_col, root_col)
        ]
        self.required_spatial_points = self._get_required_fields()
        self.dropped_rows = 0  # cumulative count of rows dropped due to invalid/missing required fields

        table_metadata = (
            {"reference_table": reference_table} if self.is_reference else None
        )
        self.model = make_model_from_schema(
            table_name=f"temp_{schema_type}",
            schema_type=schema_type,
            table_metadata=table_metadata,
        )
        self.column_order = [c.name for c in inspect(self.model).columns]
        logger.info(f"Column order from model: {self.column_order}")

    def _get_mapped_column(self, column_name: str) -> str:
        """Get the CSV column name for a schema column name"""
        return self.column_mapping.get(column_name, column_name)

    def _get_schema_column(self, csv_column: str) -> str:
        """Get the schema column name for a CSV column name"""
        return self.reverse_mapping.get(csv_column, csv_column)

    def _is_required_field(self, field: mm.fields.Field) -> bool:
        """Check if a field is required"""
        return field.required or not field.allow_none

    def _get_required_fields(self) -> Set[str]:
        """Get names of required spatial point fields"""
        required = set()
        for field_name, field in self.schema._declared_fields.items():
            if isinstance(field, mm.fields.Nested):
                if isinstance(field.schema, SpatialPoint) and self._is_required_field(
                    field
                ):
                    required.add(field_name)
        return required
    
    def _get_spatial_point_fields(self) -> List[Tuple[str, List[str]]]:
        """Get all BoundSpatialPoint fields and their required coordinates.
        
        Supports two formats:
        1. Separate columns: field_position_x, field_position_y, field_position_z
        2. Combined column: field_position containing [x,y,z] values
        
        Returns:
            List of tuples containing (field_name, coordinate_columns)
            where coordinate_columns is either:
            - [x_col, y_col, z_col] for separate columns
            - [xyz_col] for combined column
        """
        spatial_points = []
        
        for field_name, field in self.schema._declared_fields.items():
            if not (isinstance(field, mm.fields.Nested) and isinstance(field.schema, SpatialPoint)):
                continue
                
            # Try combined column first
            combined_col = self._get_mapped_column(f"{field_name}_position")
            if combined_col in self.column_mapping.values():
                spatial_points.append((field_name, [combined_col]))
                continue
                
            # Fall back to separate coordinates
            coordinate_cols = [
                self._get_mapped_column(f"{field_name}_position_{suffix}")
                for suffix in ['x', 'y', 'z']
            ]
            spatial_points.append((field_name, coordinate_cols))
        
        return spatial_points

    def validate_columns(self, df: pd.DataFrame) -> None:
        """Validate that all required columns are present"""
        required_columns = []

        if not self.generate_ids:
            required_columns.append(self._get_mapped_column("id"))

        for field_name, coordinate_cols in self.spatial_points:
            if field_name in self.required_spatial_points:
                required_columns.extend(coordinate_cols)

        if self.is_reference:
            required_columns.append(self._get_mapped_column("target_id"))
        
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    @staticmethod
    def point_xyz(chunk: pd.DataFrame, coordinate_cols: List[str]) -> np.ndarray:
        """The (n, 3) coordinates of one spatial point field, NaN where a row's point
        is missing or unparseable. coordinate_cols is [x, y, z] or [combined]."""
        if len(coordinate_cols) == 1:
            return np.array(
                [_parse_combined_xyz(v) for v in chunk[coordinate_cols[0]]],
                dtype=float,
            ).reshape(-1, 3)
        return np.column_stack(
            [
                pd.to_numeric(chunk[col], errors="coerce").to_numpy(dtype=float)
                for col in coordinate_cols
            ]
        )

    def _spatial_column(
        self, chunk: pd.DataFrame, coordinate_cols: List[str]
    ) -> np.ndarray:
        """Hex EWKB POINT Z for every row of a chunk, None where a point is invalid.

        Same result as process_spatial_point on each row, vectorized: a per-row
        loop cost about 1 billion Python calls on a 337M-row synapse upload.
        """
        return encode_points_wkb_hex(self.point_xyz(chunk, coordinate_cols))

    def process_spatial_point(
        self,
        row: pd.Series, 
        coordinate_cols: List[str]
    ) -> Optional[str]:
        """Convert coordinate data to PostGIS POINTZ format.
        
        Handles both separate coordinates and combined [x,y,z] format.
        
        Args:
            row: DataFrame row
            coordinate_cols: Either [x_col, y_col, z_col] or [xyz_col]
        
        Returns:
            WKBElement representing the point or None if invalid
        """
        try:
            if len(coordinate_cols) == 1:
                # Handle combined [x,y,z] format
                xyz_col = coordinate_cols[0]
                if pd.isna(row[xyz_col]):
                    return None
                    
                # Parse string "[x,y,z]" or handle list/array
                coords = row[xyz_col]
                if isinstance(coords, str):
                    # Remove brackets and split
                    coords = [float(x.strip()) for x in coords.strip('[]()').split(',')]
                elif isinstance(coords, (list, np.ndarray)):
                    coords = [float(x) for x in coords]
                else:
                    raise ValueError(f"Invalid coordinate format: {coords}")
                    
            else:
                # Handle separate x,y,z columns
                coords = [float(row[col]) for col in coordinate_cols]

            if len(coords) != 3:
                raise ValueError(f"Expected 3 coordinates, got {len(coords)}")

            # NaN coordinates cannot be stored as a valid PointZ — Shapely/GEOS drops
            # the Z dimension for all-NaN points, producing a 2D WKB that PostgreSQL
            # rejects on a PointZ column.  Treat missing coordinates as NULL instead.
            if any(np.isnan(c) for c in coords):
                return None

            point = Point(coords)
            return create_wkt_element(point)
            
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Error processing spatial point: {str(e)}")
            return None

    @property
    def writes_segmentation(self) -> bool:
        """Whether the source supplies segmentation columns, written to their own CSV."""
        return bool(self.supplied_segmentation_columns)

    def source_text_columns(self) -> List[str]:
        """The source columns to read as text, so that ids are not parsed as floats."""
        return [self.column_mapping[c] for c in self.supplied_segmentation_columns]

    def transform(self, timestamp: datetime):
        """The chunk transform the CSV writers take: the processed annotation rows, and
        the segmentation rows too when the source supplies segmentation columns."""
        if self.writes_segmentation:
            return lambda chunk: self.process_chunk_with_segmentation(chunk, timestamp)
        return lambda chunk: self.process_chunk(chunk, timestamp)

    def process_chunk(self, chunk: pd.DataFrame, timestamp: datetime) -> pd.DataFrame:
        """Process a chunk of data using vectorized operations"""
        return self.process_chunk_with_segmentation(chunk, timestamp)[0]

    def process_chunk_with_segmentation(
        self, chunk: pd.DataFrame, timestamp: datetime
    ) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """The annotation rows of a chunk, and its segmentation rows (id and every
        segmentation column, 0 where the source supplies none) when the source supplies
        any segmentation column, else None. Both keep the same rows, in order."""
        self.validate_columns(chunk)
        chunk_size = len(chunk)

        processed_data = {
            "created": [timestamp] * chunk_size,
            "deleted": [""] * chunk_size,
            "superceded_id": [""] * chunk_size,
            "valid": [True] * chunk_size,
        }

        if self.generate_ids:
            start_id = self._id_counter + 1
            end_id = start_id + chunk_size
            processed_data["id"] = range(start_id, end_id)
            self._id_counter = end_id - 1
        else:
            id_col = self._get_mapped_column("id")
            processed_data["id"] = chunk[id_col].astype(int)

        if self.is_reference:
            target_id_col = self._get_mapped_column("target_id")
            processed_data["target_id"] = chunk[target_id_col].astype(int)

        for field_name, coordinate_cols in self.spatial_points:
            if all(col in chunk.columns for col in coordinate_cols):
                processed_data[f"{field_name}_position"] = self._spatial_column(
                    chunk, coordinate_cols
                )
            else:
                if field_name in self.required_spatial_points:
                    raise ValueError(
                        f"Missing coordinates for required spatial point: {field_name}"
                    )
                processed_data[f"{field_name}_position"] = [None] * chunk_size

        for field_name, field in self.schema._declared_fields.items():
            if not isinstance(field, mm.fields.Nested):
                csv_col = self._get_mapped_column(field_name)
                if csv_col in chunk.columns and csv_col not in self.ignored_columns:
                    if isinstance(field, mm.fields.Int):
                        processed_data[field_name] = chunk[csv_col].astype("Int64")
                    elif isinstance(field, mm.fields.Float):
                        processed_data[field_name] = chunk[csv_col].astype(float)
                    elif isinstance(field, mm.fields.Bool):
                        processed_data[field_name] = chunk[csv_col].astype("boolean")
                    else:
                        processed_data[field_name] = chunk[csv_col]

        df = pd.DataFrame(processed_data)
        segmentation = None
        if self.writes_segmentation:
            segmentation = pd.DataFrame(
                {"id": df["id"].to_numpy(dtype=np.int64)}, index=df.index
            )
            for column in self.segmentation_column_order[1:]:
                if column in self.supplied_segmentation_columns:
                    csv_col = self.column_mapping[column]
                    if csv_col not in chunk.columns:
                        raise ValueError(f"Missing column {csv_col} for {column}")
                    segmentation[column] = exact_ids(chunk[csv_col], csv_col)
                else:
                    segmentation[column] = np.zeros(chunk_size, dtype=np.int64)

        for col in self.column_order:
            if col not in df.columns:
                df[col] = [None] * chunk_size

        # Drop rows where any required spatial point is NULL (missing or NaN coordinates).
        # These rows cannot be meaningfully stored and would cause DB errors.
        required_position_cols = [
            f"{field_name}_position"
            for field_name in self.required_spatial_points
            if f"{field_name}_position" in df.columns
        ]
        if required_position_cols:
            invalid_mask = df[required_position_cols].isnull().any(axis=1)
            n_dropped = int(invalid_mask.sum())
            if n_dropped:
                logger.warning(
                    f"Dropping {n_dropped} row(s) with missing required spatial coordinates "
                    f"(columns: {required_position_cols})"
                )
                self.dropped_rows += n_dropped
                df = df[~invalid_mask]
                if segmentation is not None:
                    segmentation = segmentation[~invalid_mask.to_numpy()]

        return df[self.column_order], segmentation
