import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import marshmallow as mm
import pandas as pd
from emannotationschemas import get_schema
from emannotationschemas.models import make_model_from_schema
from emannotationschemas.schemas.base import ReferenceAnnotation, SpatialPoint
from geoalchemy2 import WKBElement
from shapely.geometry import Point
from sqlalchemy import inspect

logger = logging.getLogger(__name__)


def create_wkt_element(geom):
    return WKBElement(geom.wkb)


class SchemaProcessor:
    """Processes CSV data according to schema types, handling spatial points and references"""

    def __init__(
        self,
        schema_type: str,
        reference_table: str = None,
        column_mapping: Dict[str, str] = None,
        ignored_columns: List[str] = None,
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
        self.column_mapping = column_mapping or {}
        self.reverse_mapping = {v: k for k, v in self.column_mapping.items()}
        self.ignored_columns = set(ignored_columns or [])
        self.generate_ids = "id" in self.column_mapping or any(
            v == "id" for v in self.column_mapping.values()
        )
        self._id_counter = 0

        if self.is_reference and reference_table is None:
            raise ValueError(
                f"Schema {schema_type} is a reference type and requires a reference_table"
            )

        self.spatial_points = self._get_spatial_point_fields()
        self.required_spatial_points = self._get_required_fields()

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
        """Get all BoundSpatialPoint fields and their required coordinates"""
        spatial_points = []
        for field_name, field in self.schema._declared_fields.items():
            if isinstance(field, mm.fields.Nested):
                if isinstance(field.schema, SpatialPoint):
                    coordinate_cols = [
                        self._get_mapped_column(f"{field_name}_position_x"),
                        self._get_mapped_column(f"{field_name}_position_y"),
                        self._get_mapped_column(f"{field_name}_position_z"),
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

    def process_spatial_point(
        self, row: pd.Series, coordinate_cols: List[str]
    ) -> Optional[str]:
        """Convert coordinate columns to PostGIS POINTZ format if coordinates exist and convert to WKBElement"""
        try:
            coords = [float(row[col]) for col in coordinate_cols]
            point = Point(coords)
            return create_wkt_element(point)
        except (KeyError, ValueError):
            return None

    def process_chunk(self, chunk: pd.DataFrame, timestamp: datetime) -> str:
        """Process a chunk of data using vectorized operations"""
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
                coordinates = chunk[coordinate_cols].astype(float).values
                points = [Point(coords) for coords in coordinates]
                processed_data[f"{field_name}_position"] = [
                    create_wkt_element(point) if not any(pd.isna(coords)) else ""
                    for point, coords in zip(points, coordinates)
                ]
            else:
                if field_name in self.required_spatial_points:
                    raise ValueError(
                        f"Missing coordinates for required spatial point: {field_name}"
                    )
                processed_data[f"{field_name}_position"] = [""] * chunk_size

        for field_name, field in self.schema._declared_fields.items():
            if not isinstance(field, mm.fields.Nested):
                csv_col = self._get_mapped_column(field_name)
                if csv_col in chunk.columns and csv_col not in self.ignored_columns:
                    if isinstance(field, mm.fields.Int):
                        processed_data[field_name] = (
                            chunk[csv_col].fillna(0).astype(int)
                        )
                    elif isinstance(field, mm.fields.Float):
                        processed_data[field_name] = (
                            chunk[csv_col].fillna(0.0).astype(float)
                        )
                    elif isinstance(field, mm.fields.Bool):
                        processed_data[field_name] = (
                            chunk[csv_col].fillna(False).astype(bool)
                        )
                    else:
                        processed_data[field_name] = chunk[csv_col].fillna("")

        df = pd.DataFrame(processed_data)

        for col in self.column_order:
            if col not in df.columns:
                df[col] = [""] * chunk_size

        data = df[self.column_order]

        return data.to_csv(index=False, lineterminator="\n").encode("utf-8")


def process_file(
    file_path: str,
    schema_name: str,
    reference_table: str = None,
    column_mapping: Dict[str, str] = None,
    ignored_columns: List[str] = None,
    chunk_size: int = 10000,
) -> str:
    """
    Process a CSV file and save the processed data with optimized chunk handling
    """
    processor = SchemaProcessor(
        schema_name,
        reference_table,
        column_mapping=column_mapping,
        ignored_columns=ignored_columns,
    )
    timestamp = datetime.now(timezone.utc)
    output_path = f"processed_{schema_name}_{timestamp}.csv"

    try:
        first_chunk = True
        with pd.read_csv(file_path, chunksize=chunk_size) as chunks:
            for chunk_num, chunk in enumerate(chunks):
                logging.debug(f"Processing chunk {chunk_num + 1} with {len(chunk)} rows")
                processed_chunk = processor.process_chunk(chunk, timestamp)

                processed_chunk.to_csv(
                    output_path,
                    mode="w" if first_chunk else "a",
                    header=False,
                    index=False,
                )
                first_chunk = False

        logger.info(f"Processed file saved to: {output_path}")
        logger.info(f"Columns in processed file: {processor.column_order}")
        return output_path

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}", exc_info=True)
        raise
