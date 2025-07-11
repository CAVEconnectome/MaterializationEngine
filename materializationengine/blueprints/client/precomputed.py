import numbers
from typing import Literal, NamedTuple, Optional, Union, cast
from collections.abc import Sequence
import struct
from neuroglancer import coordinate_space, viewer_state
import numpy as np


class Annotation(NamedTuple):
    id: int
    encoded: bytes
    relationships: Sequence[Sequence[int]]


_PROPERTY_DTYPES: dict[
    str, tuple[Union[tuple[str], tuple[str, tuple[int, ...]]], int]
] = {
    "uint8": (("|u1",), 1),
    "uint16": (("<u2",), 2),
    "uint32": (("<u4",), 3),
    "int8": (("|i1",), 1),
    "int16": (("<i2",), 2),
    "int32": (("<i4",), 4),
    "float32": (("<f4",), 4),
    "rgb": (("|u1", (3,)), 1),
    "rgba": (("|u1", (4,)), 1),
}

AnnotationType = Literal["point", "line", "axis_aligned_bounding_box", "ellipsoid"]


def _get_dtype_for_geometry(annotation_type: AnnotationType, rank: int):
    geometry_size = rank if annotation_type == "point" else 2 * rank
    return [("geometry", "<f4", geometry_size)]


def _get_dtype_for_properties(
    properties: Sequence[viewer_state.AnnotationPropertySpec],
):
    dtype = []
    offset = 0
    for i, p in enumerate(properties):
        dtype_entry, alignment = _PROPERTY_DTYPES[p.type]
        # if offset % alignment:
        #     padded_offset = (offset + alignment - 1) // alignment * alignment
        #     padding = padded_offset - offset
        #     dtype.append((f"padding{offset}", "|u1", (padding,)))
        #     offset += padding
        dtype.append((f"{p.id}", *dtype_entry))  # type: ignore[arg-type]
        size = np.dtype(dtype[-1:]).itemsize
        offset += size
    alignment = 4
    if offset % alignment:
        padded_offset = (offset + alignment - 1) // alignment * alignment
        padding = padded_offset - offset
        dtype.append((f"padding{offset}", "|u1", (padding,)))
        offset += padding
    return dtype


class AnnotationWriter:
    annotations: list[Annotation]
    related_annotations: list[dict[int, list[Annotation]]]

    def __init__(
        self,
        annotation_type: AnnotationType,
        names: Sequence[str] = ["x", "y", "z"],
        scales: Sequence[float] = (1.0, 1.0, 1.0),
        units: [str, Sequence[str]] = "nm",
        relationships: Sequence[str] = (),
        properties: Sequence[viewer_state.AnnotationPropertySpec] = (),
    ):
        """Initializes an `AnnotationWriter`.

        Args:

            annotation_type: The type of annotation.  Must be one of "point",
                "line", "axis_aligned_bounding_box", or "ellipsoid".
            names : The names of the dimensions of the coordinate space.
                    Default is ["x", "y", "z"], should match whatever
                    axes names you are trying to visualize in neuroglancer.
            scales: The scales of the dimensions of the coordinate space.
                Default is (1.0, 1.0, 1.0). Needs to match length of names
            units: The units of the dimensions of the coordinate space.
                Can be a list, or if all dimensions are a single unit, a single string.
                Default is "nm". Must be a combo of a valid SI unit ["m", "s", "rad/s", "Hz"]
                with a valid prefix ["Y", "Z", "E", "P", "T", "G", "M", "k", "h", "", "c", "m", "u", "µ", "n", "p", "f", "a", "z", "y"]
                If a list it needs to match length of names.
            lower_bound: The lower bound of the bounding box of the annotations.
            relationships: The names of relationships between annotations.  Each
                relationship is a string that is used as a key in the `relationships`
                field of each annotation.  For example, if `relationships` is
                `["parent", "child"]`, then each annotation may have a `parent` and
                `child` relationship, and the `relationships` field of each annotation
                is a dictionary with keys `"parent"` and `"child"`.
            properties: The properties of each annotation.  Each property is a
                `AnnotationPropertySpec` object.
        """
        if isinstance(units, str):
            units = [units] * len(names)

        if len(names) != len(scales):
            raise ValueError(
                f"Expected names and scales to have the same length, but received: {len(names)} and {len(scales)}"
            )
        if len(names) != len(units):
            raise ValueError(
                f"Expected names and units to have the same length, but received: {len(names)} and {len(units)}"
            )

        self.coordinate_space = coordinate_space.CoordinateSpace(
            names=names,
            scales=scales,
            units=units,
        )
        self.relationships = list(relationships)
        self.annotation_type = annotation_type
        self.properties = list(properties)
        self.annotations = []
        self.rank = self.coordinate_space.rank
        self.dtype = _get_dtype_for_geometry(
            annotation_type, self.coordinate_space.rank
        ) + _get_dtype_for_properties(self.properties)

    def add_point(self, point: Sequence[float], id: Optional[int] = None, **kwargs):
        if self.annotation_type != "point":
            raise ValueError(
                f"Expected annotation type point, but received: {self.annotation_type}"
            )
        if len(point) != self.coordinate_space.rank:
            raise ValueError(
                f"Expected point to have length {self.coordinate_space.rank}, but received: {len(point)}"
            )

        self._add_obj(point, id, **kwargs)

    def add_axis_aligned_bounding_box(
        self,
        point_a: Sequence[float],
        point_b: Sequence[float],
        id: Optional[int] = None,
        **kwargs,
    ):
        if self.annotation_type != "axis_aligned_bounding_box":
            raise ValueError(
                f"Expected annotation type axis_aligned_bounding_box, but received: {self.annotation_type}"
            )
        self._add_two_point_obj(point_a, point_b, id, **kwargs)

    def add_ellipsoid(
        self,
        center: Sequence[float],
        radii: Sequence[float],
        id: Optional[int] = None,
        **kwargs,
    ):
        if self.annotation_type != "ellipsoid":
            raise ValueError(
                f"Expected annotation type ellipsoid, but received: {self.annotation_type}"
            )
        if len(center) != self.coordinate_space.rank:
            raise ValueError(
                f"Expected center to have length {self.coordinate_space.rank}, but received: {len(center)}"
            )

        if len(radii) != self.coordinate_space.rank:
            raise ValueError(
                f"Expected radii to have length {self.coordinate_space.rank}, but received: {len(radii)}"
            )

        self._add_two_point_obj(center, radii, id, **kwargs)

    def add_line(
        self,
        point_a: Sequence[float],
        point_b: Sequence[float],
        id: Optional[int] = None,
        **kwargs,
    ):
        if self.annotation_type != "line":
            raise ValueError(
                f"Expected annotation type line, but received: {self.annotation_type}"
            )

        self._add_two_point_obj(point_a, point_b, id, **kwargs)

    def _add_two_point_obj(
        self,
        point_a: Sequence[float],
        point_b: Sequence[float],
        id: Optional[int] = None,
        **kwargs,
    ):
        if len(point_a) != self.coordinate_space.rank:
            raise ValueError(
                f"Expected coordinates to have length {self.coordinate_space.rank}, but received: {len(point_a)}"
            )

        if len(point_b) != self.coordinate_space.rank:
            raise ValueError(
                f"Expected coordinates to have length {self.coordinate_space.rank}, but received: {len(point_b)}"
            )

        coords = np.concatenate((point_a, point_b))
        self._add_obj(
            cast(Sequence[float], coords),
            id,
            **kwargs,
        )

    def _add_obj(
        self,
        coords: Sequence[float],
        id: Optional[int],
        **kwargs,
    ):
        encoded = np.zeros(shape=(), dtype=self.dtype)
        encoded[()]["geometry"] = coords

        for i, p in enumerate(self.properties):
            if p.id in kwargs:
                encoded[()][f"{p.id}"] = kwargs.pop(p.id)

        related_ids = []
        for relationship in self.relationships:
            ids = kwargs.pop(relationship, None)
            if ids is None:
                ids = []
            if isinstance(ids, numbers.Integral):
                ids = [ids]
            related_ids.append(ids)

        if kwargs:
            raise ValueError(f"Unexpected keyword arguments {kwargs}")

        if id is None:
            id = len(self.annotations)

        annotation = Annotation(
            id=id, encoded=encoded.tobytes(), relationships=related_ids
        )

        self.annotations.append(annotation)

    def _serialize_annotations(self, f, annotations: list[Annotation]):
        f.write(self._encode_multiple_annotations(annotations))

    def _serialize_annotation(self, f, annotation: Annotation):
        f.write(self._encode_single_annotation(annotation))
      
    def _encode_single_annotation(self, annotation: Annotation):
        """
        This function creates a binary string from a single annotation.

        Parameters:
            annotation (Annotation): The annotation object to encode.

        Returns:
            bytes: Binary string representation of the annotation.
        """
        binary_components = []
        binary_components.append(annotation.encoded)
        for related_ids in annotation.relationships:
            binary_components.append(struct.pack("<I", len(related_ids)))
            for related_id in related_ids:
                binary_components.append(struct.pack("<Q", related_id))

        return b"".join(binary_components)

    def _encode_multiple_annotations(self, annotations: list[Annotation]):
        """
        This function creates a binary string from a list of annotations.

        Parameters:
            annotations (list): List of annotation objects. Each object should have 'encoded' and 'id' attributes.

        Returns:
            bytes: Binary string of all components together.
        """
        binary_components = []
        binary_components.append(struct.pack("<Q", len(annotations)))
        for annotation in annotations:
            binary_components.append(annotation.encoded)
        for annotation in annotations:
            binary_components.append(struct.pack("<Q", annotation.id))
        return b"".join(binary_components)
