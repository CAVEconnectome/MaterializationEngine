import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import marshmallow as mm
import numpy as np
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
        raw_column_mapping = column_mapping or {}
        self.column_mapping = {
            schema_col: csv_col
            for schema_col, csv_col in raw_column_mapping.items()
            if csv_col and csv_col.strip()
        }
        self.reverse_mapping = {v: k for k, v in self.column_mapping.items()}
        self.ignored_columns = set(ignored_columns or [])
        self.generate_ids = "id" not in self.column_mapping
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
                
            point = Point(coords)
            return create_wkt_element(point)
            
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Error processing spatial point: {str(e)}")
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

        return df[self.column_order]
