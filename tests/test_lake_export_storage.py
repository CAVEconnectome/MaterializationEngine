"""lake_export.storage: idempotent deletes under a (local) lake."""

import os

from materializationengine.workflows.lake_export import storage


def touch(root, *paths):
    for p in paths:
        os.makedirs(os.path.dirname(os.path.join(root, p)), exist_ok=True)
        with open(os.path.join(root, p), "w") as fh:
            fh.write("x")


class TestStorage:
    def test_list_paths(self, tmp_path):
        root = str(tmp_path / "lake")
        touch(root, "a/1.parquet", "a/2.parquet", "b/3.parquet")
        assert sorted(storage.list_paths(root)) == ["a/1.parquet", "a/2.parquet", "b/3.parquet"]
        assert sorted(storage.list_paths(root, "a")) == ["a/1.parquet", "a/2.parquet"]
        assert storage.list_paths(root, "missing") == []

    def test_delete_paths_is_idempotent(self, tmp_path):
        root = str(tmp_path / "lake")
        touch(root, "p=1/f1.parquet", "p=1/f2.parquet", "p=2/f3.parquet")
        assert storage.delete_paths(root, ["p=1/f1.parquet", "p=1/f2.parquet"]) == 2
        # already gone, and URL-encoded as a Delta log records it
        assert storage.delete_paths(root, ["p%3D1/f1.parquet", "p=1/f2.parquet"]) == 2
        assert storage.list_paths(root) == ["p=2/f3.parquet"]
        assert storage.delete_paths(root, []) == 0

    def test_delete_many_in_chunks(self, tmp_path):
        root = str(tmp_path / "lake")
        paths = [f"d/{i}.parquet" for i in range(250)]
        touch(root, *paths)
        assert storage.delete_paths(root, paths, threads=4) == 250
        assert storage.list_paths(root) == []

    def test_delete_prefix_only_under_prefix(self, tmp_path):
        root = str(tmp_path / "lake")
        touch(root, "staging/r00001-a/_delta_log/0.json", "staging/r00001-a/x.parquet", "keep.parquet")
        assert storage.delete_prefix(root, "staging/r00001-a/_delta_log") == 1
        assert sorted(storage.list_paths(root)) == ["keep.parquet", "staging/r00001-a/x.parquet"]
        assert storage.delete_prefix(root, "staging") == 1
        assert not os.path.exists(os.path.join(root, "staging"))
        assert storage.list_paths(root) == ["keep.parquet"]
