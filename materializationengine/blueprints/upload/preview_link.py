"""A Neuroglancer link showing the first rows of a proposed upload, before it starts.

Each spatial point field of the schema becomes a point annotation layer over the
datastack's imagery and segmentation. The points keep the coordinates as uploaded, in
the table's declared voxel resolution: the layer's source declares that resolution as
its output dimensions, so Neuroglancer places them in the datastack's own (viewer)
resolution. Points that land outside the data mean the coordinates or the declared
resolution are wrong, which is cheaper to find out here than after the upload.
"""

import csv
import io
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from materializationengine.blueprints.upload.processor import SchemaProcessor

PREVIEW_ROWS = 10
# Enough of a CSV for its header and PREVIEW_ROWS rows of a wide table.
PREVIEW_BYTES = 256 * 1024

POINT_COLORS = ("#ff4040", "#40a0ff", "#ffd700", "#40ff80", "#ff40ff", "#40ffff")


def read_first_rows(uri: str, source=None, rows: int = PREVIEW_ROWS) -> pd.DataFrame:
    """The first rows of a CSV or parquet source, every value as a string (as the
    upload reads them, before the schema's coercion)."""
    from materializationengine.blueprints.upload.parquet_source import (
        is_parquet_source,
        preview_parquet,
    )

    if is_parquet_source(uri):
        preview = preview_parquet(uri, rows=rows)["preview_rows"]
        return pd.DataFrame(preview[1:], columns=preview[0])
    if source is None:
        raise ValueError(f"No reader for {uri}")
    head = source.read(0, min(source.size, PREVIEW_BYTES))
    text = head.decode("utf-8-sig", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if len(head) < source.size and len(lines) > 1:
        lines = lines[:-1]  # the last line may be cut off
    parsed = list(csv.reader(lines[: rows + 1]))
    if not parsed:
        raise ValueError(f"{uri} is empty")
    header = [c.strip() for c in parsed[0]]
    return pd.DataFrame([[c.strip() for c in r] for r in parsed[1:]], columns=header)


def spatial_points(
    schema_type: str,
    column_mapping: Dict[str, str],
    ignored_columns: Sequence[str] = (),
    reference_table: Optional[str] = None,
) -> List[Tuple[str, List[str]]]:
    """(schema field, source columns) for each spatial point field, as the upload
    resolves them: [x, y, z] columns, or one combined [x,y,z] column."""
    processor = SchemaProcessor(
        schema_type,
        reference_table=reference_table or None,
        column_mapping=column_mapping,
        ignored_columns=list(ignored_columns),
    )
    return processor.spatial_points


def point_layers(
    rows: pd.DataFrame, points: List[Tuple[str, List[str]]], id_column: Optional[str]
) -> Dict[str, pd.DataFrame]:
    """Per point field present in rows: a frame of x, y, z and a description, without
    the rows whose point is missing."""
    layers = {}
    for field, cols in points:
        if not set(cols) <= set(rows.columns):
            continue
        xyz = SchemaProcessor.point_xyz(rows, cols)
        keep = ~np.isnan(xyz).any(axis=1)
        if not keep.any():
            continue
        if id_column and id_column in rows.columns:
            description = rows[id_column].astype(str).to_numpy()
        else:
            description = np.array([f"row {i + 1}" for i in range(len(rows))])
        layers[field] = pd.DataFrame(
            {
                "x": xyz[keep, 0],
                "y": xyz[keep, 1],
                "z": xyz[keep, 2],
                "description": description[keep],
            }
        )
    return layers


def build_preview_state(client, layers: Dict[str, pd.DataFrame], data_resolution: Sequence[float]):
    """The datastack's imagery and segmentation in its own resolution, and a point layer
    per field whose source declares data_resolution."""
    from nglui.statebuilder import ViewerState

    # the client gives the dimensions and the points the position: no need for nglui to
    # open the imagery to infer them
    state = ViewerState(client=client, infer_coordinates=False).add_layers_from_client(
        alpha_3d=0.3, target_url=True
    )
    for i, (field, df) in enumerate(layers.items()):
        state.add_points(
            df,
            name=field,
            point_column=["x", "y", "z"],
            description_column="description",
            data_resolution=[float(r) for r in data_resolution],
            color=POINT_COLORS[i % len(POINT_COLORS)],
            linked_segmentation=True,
        )
    return state


def preview_summary(state, layers: Dict[str, pd.DataFrame], data_resolution) -> dict:
    """What the link shows, for the page: the viewer and data resolutions, the layers
    and where the view opens."""
    st = state.to_dict()
    return {
        "viewer_resolution": [round(st["dimensions"][a][0] * 1e9, 6) for a in "xyz"],
        "data_resolution": [float(r) for r in data_resolution],
        "position": st.get("position"),
        "layers": [{"name": field, "points": len(df)} for field, df in layers.items()],
    }
