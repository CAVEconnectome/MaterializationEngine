"""Supervoxel lookup by storage chunk gives the same labels as scattered_points."""

import os

import numpy as np
import pytest
from cloudvolume import CloudVolume
from flask import Flask, g

from materializationengine import database
from materializationengine.workflows import spatial_lookup
from materializationengine.workflows.spatial_lookup import (
    lookup_supervoxel_ids,
    points_to_voxels,
    split_points_by_storage_chunk,
    storage_chunk_keys,
)

COORD_RESOLUTION = [4, 4, 40]  # points arrive at 4x4x40 nm, the volume is 8x8x40


def make_volume(tmp_path, encoding):
    rng = np.random.default_rng(0)
    labels = rng.integers(1, 2**40, size=(64, 48, 24), dtype=np.uint64)
    path = f"file://{tmp_path}/{encoding}"
    cv = CloudVolume.from_numpy(
        labels,
        vol_path=path,
        resolution=(8, 8, 40),
        voxel_offset=(100, 200, 10),
        chunk_size=(16, 16, 8),
        layer_type="segmentation",
        encoding=encoding,
        compress=True,
    )
    cv = CloudVolume(path, mip=0, bounded=False, fill_missing=True, progress=False)
    return cv, labels


def random_points(cv, n, seed=1):
    """Points at 4x4x40, mostly inside the volume, some outside it."""
    rng = np.random.default_rng(seed)
    bounds = cv.meta.bounds(0)
    lo = np.asarray(bounds.minpt) - 4
    hi = np.asarray(bounds.maxpt) + 4
    voxels = rng.integers(lo, hi, size=(n, 3))
    # back to 4x4x40 coordinates, anywhere inside the voxel
    return voxels * np.array([2, 2, 1]) + rng.integers(0, 2, size=(n, 3)) * np.array([1, 1, 0])


def expected_labels(cv, points):
    found = cv.scattered_points([list(p) for p in points], coord_resolution=COORD_RESOLUTION)
    voxels = points_to_voxels(cv, points, COORD_RESOLUTION)
    return np.array([found[tuple(int(c) for c in v)] for v in voxels], dtype=np.uint64)


class TestLookupSupervoxelIds:
    @pytest.mark.parametrize("encoding", ["compressed_segmentation", "raw"])
    def test_matches_scattered_points(self, tmp_path, encoding):
        cv, _ = make_volume(tmp_path, encoding)
        points = random_points(cv, 400)
        labels = lookup_supervoxel_ids(cv, points, COORD_RESOLUTION)
        assert labels.dtype == np.uint64 and len(labels) == len(points)
        assert (labels == expected_labels(cv, points)).all()
        assert (labels == 0).any() and (labels != 0).any()  # some points fall outside

    def test_reads_the_right_voxel(self, tmp_path):
        cv, labels = make_volume(tmp_path, "compressed_segmentation")
        offset = np.array(cv.meta.voxel_offset(0))
        voxel = np.array([37, 5, 19])
        point = (offset + voxel) * np.array([2, 2, 1])
        assert lookup_supervoxel_ids(cv, [point], COORD_RESOLUTION)[0] == labels[tuple(voxel)]

    def test_missing_chunk_reads_as_zero(self, tmp_path):
        cv, _ = make_volume(tmp_path, "compressed_segmentation")
        points = random_points(cv, 300)
        voxels = points_to_voxels(cv, points, COORD_RESOLUTION)
        bounds = cv.meta.bounds(0)
        inside = np.all((voxels >= bounds.minpt) & (voxels < bounds.maxpt), axis=1)
        # delete the chunk holding the first point inside the volume
        first = voxels[np.nonzero(inside)[0][0]]
        key = storage_chunk_keys(cv, first[None, :])[0]
        lo = np.asarray(cv.meta.voxel_offset(0)) + key * np.asarray(cv.meta.chunk_size(0))
        chunk_dir = os.path.join(cv.meta.cloudpath.replace("file://", ""), cv.meta.key(0))
        name = next(f for f in os.listdir(chunk_dir) if f.startswith(f"{lo[0]}-"))
        matching = [
            f for f in os.listdir(chunk_dir)
            if f.split(".")[0].startswith(f"{lo[0]}-{lo[0] + 16}_{lo[1]}-{lo[1] + 16}_{lo[2]}-")
        ]
        assert matching, name
        os.remove(os.path.join(chunk_dir, matching[0]))
        labels = lookup_supervoxel_ids(cv, points, COORD_RESOLUTION)
        in_missing = np.all(storage_chunk_keys(cv, voxels) == key, axis=1) & inside
        assert in_missing.any() and (labels[in_missing] == 0).all()
        assert (labels == expected_labels(cv, points)).all()

    def test_downloads_each_chunk_once(self, tmp_path, monkeypatch):
        cv, _ = make_volume(tmp_path, "compressed_segmentation")
        points = np.repeat(random_points(cv, 50), 4, axis=0)  # every voxel four times
        requested = []
        real_get = spatial_lookup.CloudFiles.get

        def counting_get(self, paths, *args, **kwargs):
            requested.extend(paths)
            return real_get(self, paths, *args, **kwargs)

        monkeypatch.setattr(spatial_lookup.CloudFiles, "get", counting_get)
        lookup_supervoxel_ids(cv, points, COORD_RESOLUTION)
        assert len(requested) == len(set(requested))

    def test_empty(self, tmp_path):
        cv, _ = make_volume(tmp_path, "compressed_segmentation")
        assert len(lookup_supervoxel_ids(cv, np.empty((0, 3)), COORD_RESOLUTION)) == 0


class TestSplitPointsByStorageChunk:
    def test_chunks_are_never_split_and_every_point_is_kept(self, tmp_path):
        cv, _ = make_volume(tmp_path, "compressed_segmentation")
        points = random_points(cv, 1000)
        data = [
            {"id": i // 2, "type": "pre_pt" if i % 2 == 0 else "post_pt", "pt_position": list(p)}
            for i, p in enumerate(points)
        ]
        batches = split_points_by_storage_chunk(data, cv, COORD_RESOLUTION, 60)
        flat = [p for b in batches for p in b]
        assert sorted((p["id"], p["type"]) for p in flat) == sorted((p["id"], p["type"]) for p in data)
        owner = {}
        for i, batch in enumerate(batches):
            keys = storage_chunk_keys(
                cv, points_to_voxels(cv, [p["pt_position"] for p in batch], COORD_RESOLUTION)
            )
            for key in map(tuple, keys):
                assert owner.setdefault(key, i) == i
        # batches close once they reach the target, at a chunk boundary
        assert all(len(b) >= 60 for b in batches[:-1])
        assert len(batches) > 1

    def test_empty(self, tmp_path):
        cv, _ = make_volume(tmp_path, "compressed_segmentation")
        assert split_points_by_storage_chunk([], cv, COORD_RESOLUTION, 50) == []


class TestKeepDbConnections:
    def test_cleanup_is_skipped_only_when_asked(self, monkeypatch):
        calls = []
        monkeypatch.setattr(database.db_manager, "cleanup", lambda: calls.append(1))
        app = Flask(__name__)
        with app.app_context():
            database.cleanup_after_app_context()
            g.keep_db_connections = True
            database.cleanup_after_app_context()
        assert calls == [1]

    def test_spatial_lookup_tasks_keep_their_connections(self):
        assert spatial_lookup.process_and_insert_sub_batch.keep_db_connections is True
        assert spatial_lookup.process_chunk.keep_db_connections is True
