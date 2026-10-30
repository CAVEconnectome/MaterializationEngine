"""Uploads whose source supplies supervoxel and/or root ids (supplied_ids.py): the ids
go into a second processed CSV, the stages skip the lookups they make unnecessary, and
resolve_roots fills blanks and replaces stale roots."""

import datetime
import io
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import sqlalchemy as sa

from materializationengine.blueprints.upload import production_build as pb
from materializationengine.blueprints.upload import supplied_ids, tasks
from materializationengine.blueprints.upload.gcs_processor import segmentation_blob_name
from materializationengine.blueprints.upload.processor import (
    SchemaProcessor,
    exact_ids,
    segmentation_columns,
)
from materializationengine.blueprints.upload.schemas import MetadataSchema
from materializationengine.rayjobs import runner as runner_module
from materializationengine.rayjobs.entrypoints import bulk_upload
from materializationengine.upload_state import store
from materializationengine.workflows import resolve_roots
from tests.test_production_build import META, PCG, PRODUCTION_PARAMS, app, production  # noqa: F401
from tests.test_upload_csv_parts import (  # noqa: F401
    BUCKET,
    FakeBucket,
    make_processor,
    redis_clients,
    run_parts_inline,
)
from tests.test_upload_state_store import JOB, admin, engine, new_job  # noqa: F401

POSITIONS = {
    f"{point}_position_{axis}": f"{point.split('_')[0]}_{axis}"
    for point in ["pre_pt", "post_pt", "ctr_pt"]
    for axis in "xyz"
}
# root ids of minnie's size: float64 cannot hold them exactly
ROOT = 864691135000000000
SV = 88000000000000000


def mapping(*supplied):
    m = {**POSITIONS, "size": "size"}
    for column in supplied:
        m[column] = column
    return m


def csv_with_ids(n=20):
    lines = ["pre_x,pre_y,pre_z,post_x,post_y,post_z,ctr_x,ctr_y,ctr_z,size,pre_pt_supervoxel_id,pre_pt_root_id"]
    for i in range(n):
        sv = "" if i == 3 else str(SV + i)  # a blank supervoxel
        pre_x = "" if i == 7 else str(i)  # a missing required point: the row is dropped
        lines.append(f"{pre_x},{i},{i},{i},{i},{i},{i},{i},{i},{i},{sv},{ROOT + i}")
    return ("\n".join(lines) + "\n").encode()


class TestProcessor:
    def test_segmentation_columns_come_from_the_schema(self):
        assert segmentation_columns("synapse") == {
            "pre_pt": ("pre_pt_supervoxel_id", "pre_pt_root_id"),
            "post_pt": ("post_pt_supervoxel_id", "post_pt_root_id"),
        }
        assert segmentation_columns("reference_tag") == {}

    def test_nothing_supplied_writes_no_segmentation(self):
        processor = SchemaProcessor("synapse", column_mapping=mapping())
        assert not processor.writes_segmentation
        assert processor.lookup_fields == ["pre_pt", "post_pt"]

    def test_supplied_ids_give_segmentation_rows_aligned_with_the_annotation_rows(self):
        processor = SchemaProcessor(
            "synapse", column_mapping=mapping("pre_pt_supervoxel_id", "pre_pt_root_id")
        )
        assert processor.writes_segmentation
        assert processor.lookup_fields == ["post_pt"]
        assert processor.source_text_columns() == ["pre_pt_supervoxel_id", "pre_pt_root_id"]
        chunk = pd.read_csv(io.BytesIO(csv_with_ids()), dtype={c: str for c in processor.source_text_columns()})
        rows, seg = processor.transform(datetime.datetime(2026, 10, 29))(chunk)
        assert list(seg.columns) == processor.segmentation_column_order == [
            "id", "pre_pt_supervoxel_id", "pre_pt_root_id", "post_pt_supervoxel_id", "post_pt_root_id",
        ]
        assert len(rows) == len(seg) == 19  # row 7 dropped from both
        assert seg["id"].tolist() == rows["id"].tolist()
        by_id = seg.set_index("id")
        assert by_id.loc[1, "pre_pt_root_id"] == ROOT + 0  # exact, not rounded
        assert by_id.loc[4, "pre_pt_supervoxel_id"] == 0  # the blank one
        assert by_id.loc[20, "pre_pt_root_id"] == ROOT + 19
        assert (seg["post_pt_supervoxel_id"] == 0).all()

    def test_ids_as_large_floats_are_refused(self):
        with pytest.raises(ValueError, match="floating point"):
            exact_ids(pd.Series([float(ROOT), np.nan]), "pre_pt_root_id")
        assert exact_ids(pd.Series([5.0, np.nan]), "c").tolist() == [5, 0]
        assert exact_ids(pd.Series(["12", " 13.0 ", None, ""]), "c").tolist() == [12, 13, 0, 0]
        with pytest.raises(ValueError, match="not an id"):
            exact_ids(pd.Series(["abc"]), "c")
        with pytest.raises(ValueError, match="negative"):
            exact_ids(pd.Series(["-1"]), "c")


class TestProcessCsv:
    def test_one_stream_and_parts_write_the_same_segmentation_csv(self, redis_clients):
        data = csv_with_ids(300)
        m = mapping("pre_pt_supervoxel_id", "pre_pt_root_id")
        timestamp = datetime.datetime(2026, 10, 29, tzinfo=datetime.timezone.utc)

        one = FakeBucket({"in.csv": data})
        processor = SchemaProcessor("synapse", column_mapping=m)
        make_processor(one, 50).process_csv_in_chunks(
            "in.csv", "out.csv", processor.transform(timestamp),
            segmentation=True, text_columns=processor.source_text_columns(),
        )
        assert segmentation_blob_name("out.csv") == "out.segmentation.csv"
        seg = pd.read_csv(io.BytesIO(one.objects["out.segmentation.csv"]), header=None)
        assert len(seg) == 299 and seg[2].max() == ROOT + 299

        parts = FakeBucket({"in.csv": data})
        gcs = make_processor(parts, 50)
        header, ranges = gcs.plan_csv_parts("in.csv", 2000)
        assert len(ranges) > 3
        with mock.patch.object(tasks, "_run_csv_part_tasks", run_parts_inline), \
                mock.patch.object(tasks, "GCSCsvProcessor", lambda *a, **k: make_processor(parts, 50)):
            tasks._process_csv_in_parts(
                gcs, BUCKET, "in.csv", header, ranges, "out.csv", JOB,
                {"schema_type": "synapse", "column_mapping": m, "chunk_size": 50,
                 "timestamp": timestamp.isoformat()},
                generate_ids=True, id_counter_start=0, segmentation=True,
            )
        assert parts.objects["out.csv"] == one.objects["out.csv"]
        assert parts.objects["out.segmentation.csv"] == one.objects["out.segmentation.csv"]
        assert not any(n.startswith("processed_parts/") for n in parts.objects)


def file_metadata(*supplied, root_ids_lookup=None):
    return {
        "column_mapping": mapping(*supplied),
        "ignored_columns": [],
        "metadata": {"schema_type": "synapse", "root_ids_lookup": root_ids_lookup},
    }


class TestStageOrder:
    @pytest.mark.parametrize(
        "supplied, lookup, resolve",
        [
            ((), None, None),
            (("pre_pt_supervoxel_id", "post_pt_supervoxel_id"), False, True),
            (("pre_pt_supervoxel_id", "pre_pt_root_id", "post_pt_supervoxel_id", "post_pt_root_id"), False, True),
            (("pre_pt_root_id",), True, True),
            (("pre_pt_supervoxel_id",), True, True),
        ],
    )
    def test_stages_follow_what_is_supplied(self, supplied, lookup, resolve):
        params = {"build_in_production": True}
        info = supplied_ids.supplied_segmentation(file_metadata(*supplied))
        if info:
            params["supplied_segmentation"] = info
        order = bulk_upload.stage_order(params)
        if lookup is None:
            assert order == bulk_upload.PRODUCTION_STAGE_ORDER
            return
        assert "load_segmentation" in order
        assert ("spatial_chunks" in order) is lookup
        assert ("resolve_roots" in order) is resolve
        assert order.index("load_segmentation") < order.index("resolve_roots") < order.index("finish_tables")
        if lookup:
            assert order.index("spatial_chunks") < order.index("resolve_roots")

    def test_the_spatial_lookup_skips_fields_with_supplied_supervoxels(self):
        from materializationengine.workflows.spatial_lookup import spatial_lookup_point_columns

        info = {"annotation_table_name": "t", "schema": "synapse", "pcg_table_name": PCG}
        assert spatial_lookup_point_columns(dict(info))[1] == ["pre_pt_position", "post_pt_position"]
        info["lookup_fields"] = ["post_pt"]
        assert spatial_lookup_point_columns(dict(info))[1] == ["post_pt_position"]


class TestRootIdsLookup:
    BASE = dict(
        schema_type="synapse", datastack_name="d", table_name="t", description="x",
        write_permission="PRIVATE", read_permission="PUBLIC",
        voxel_resolution_nm_x=4, voxel_resolution_nm_y=4, voxel_resolution_nm_z=40,
    )

    def test_optional(self):
        assert MetadataSchema().load(self.BASE)["root_ids_lookup"] is None

    @pytest.mark.parametrize(
        "value, error",
        [
            ({"mode": "at"}, "Give the time"),
            ({"mode": "between", "start": "2026-02-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"}, "start before"),
            ({"mode": "at", "at": "2999-01-01T00:00:00Z"}, "future"),
            ({"mode": "sometime"}, "Must be one of"),
        ],
    )
    def test_refused(self, value, error):
        with pytest.raises(Exception, match=error):
            MetadataSchema().load({**self.BASE, "root_ids_lookup": value})

    def test_lookup_time(self):
        assert supplied_ids.lookup_time(None) is None
        assert supplied_ids.lookup_time({"mode": "unknown"}) is None
        end = supplied_ids.lookup_time({"mode": "between", "start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"})
        assert end == datetime.datetime(2026, 2, 1, tzinfo=datetime.timezone.utc)


class FakeChunkedGraph:
    """caveclient's chunkedgraph: roots by supervoxel, and which roots are current."""

    def __init__(self, roots, current):
        self.roots, self.current = roots, set(current)

    def get_roots(self, svs, timestamp=None):
        return np.array([self.roots.get(int(s), 0) for s in svs], dtype=np.uint64)

    def is_latest_roots(self, roots, timestamp=None):
        return np.array([int(r) in self.current for r in roots])


class TestSampleCheck:
    def test_counts_matches_and_current_roots(self):
        rows = pd.DataFrame({
            "pre_pt_supervoxel_id": ["1", "2", "3"],
            "pre_pt_root_id": ["11", "99", "33"],
        })
        meta = file_metadata("pre_pt_supervoxel_id", "pre_pt_root_id")
        check = supplied_ids.sample_check(rows, meta, FakeChunkedGraph({1: 11, 2: 22, 3: 33}, {11}))
        (pre,) = check["fields"]
        assert pre["field"] == "pre_pt"
        assert (pre["supervoxels"], pre["supervoxels_with_root"]) == (3, 3)
        assert (pre["roots_compared"], pre["roots_matching"]) == (3, 2)
        assert (pre["roots"], pre["roots_current_now"]) == (3, 1)

    def test_nothing_supplied(self):
        assert supplied_ids.sample_check(pd.DataFrame(), file_metadata(), None) is None


class FakeRootExt:
    """kvdbclient's root extension. Roots are ROOT + n (top layer); stale is a set."""

    _layer_count = 10

    def __init__(self, roots, stale=()):
        self.roots, self.stale = roots, set(stale)
        self.checked = []

    def get_chunk_layers(self, ids):
        return np.array([10 if int(i) >= ROOT else 2 for i in ids])

    def is_latest_roots(self, roots, time_stamp=None):
        self.checked.extend(int(r) for r in roots)
        return np.array([int(r) not in self.stale for r in roots])

    def get_roots(self, svs, time_stamp=None, fail_to_zero=False):
        return np.array([self.roots.get(int(s), 0) for s in svs], dtype=np.uint64)


def seg_table(prod, names, rows):
    # the segmentation table's foreign key needs the annotation table to create it
    pb.create_table(prod, pb.annotation_model(names["annotation"], META), JOB, keep_primary_key=True)
    seg = pb.segmentation_model(names["annotation"], META, PCG)
    pb.create_table(prod, seg, JOB, keep_primary_key=True)
    with prod.begin() as conn:
        for r in rows:
            conn.execute(sa.text(
                f'INSERT INTO "{names["segmentation"]}" (id, pre_pt_supervoxel_id, pre_pt_root_id, '
                "post_pt_supervoxel_id, post_pt_root_id) VALUES (:id, :a, :b, :c, :d)"
            ), dict(zip("id a b c d".split(), r)))


def seg_rows(prod, names):
    with prod.connect() as conn:
        return [tuple(r) for r in conn.execute(
            f'SELECT id, pre_pt_supervoxel_id, pre_pt_root_id, post_pt_supervoxel_id, post_pt_root_id '
            f'FROM "{names["segmentation"]}" ORDER BY id'
        )]


class TestResolveIdRange:
    def info(self, names, pre=(True, True), post=None):
        fields = {"pre_pt": {
            "supervoxel_id": "pre_pt_supervoxel_id", "root_id": "pre_pt_root_id", "position": "pre_pt_position",
            "supervoxels_supplied": pre[0], "roots_supplied": pre[1],
        }}
        if post:
            fields["post_pt"] = {
                "supervoxel_id": "post_pt_supervoxel_id", "root_id": "post_pt_root_id", "position": "post_pt_position",
                "supervoxels_supplied": post[0], "roots_supplied": post[1],
            }
        return {
            "segmentation_table": names["segmentation"], "annotation_table": names["annotation"],
            "pcg_table_name": PCG, "segmentation_source": "graphene://fake", "coord_resolution": [4, 4, 40],
            "materialization_time_stamp": "2026-10-29T12:00:00", "fields": fields,
        }

    def run(self, prod, info, root_ext, first=1, last=10**9, supervoxels=None):
        client = mock.Mock(root_ext=root_ext)
        with mock.patch("materializationengine.database.db_manager.get_engine", return_value=prod), \
                mock.patch("materializationengine.kvdb_gateway.kvdb_cache.get_client", return_value=client), \
                mock.patch.object(
                    resolve_roots, "lookup_supervoxels",
                    side_effect=supervoxels or (lambda engine, info, field, ids: np.zeros(len(ids), dtype=np.int64)),
                ):
            return resolve_roots.resolve_id_range("prod", info, first, last, batch_size=2)

    def test_current_roots_stay_stale_and_missing_ones_are_looked_up(self, production):
        prod, _, table = production
        names = pb.build_names(JOB, table, PCG)
        seg_table(prod, names, [
            (1, SV + 1, ROOT + 1, 0, 0),  # current
            (2, SV + 2, ROOT + 2, 0, 0),  # stale
            (3, SV + 3, 0, 0, 0),         # no root given
            (4, SV + 4, SV + 4, 0, 0),    # a supervoxel in the root column
            (5, 0, 0, 0, 0),              # nothing: point without a label
        ])
        root_ext = FakeRootExt({SV + 2: ROOT + 20, SV + 3: ROOT + 30, SV + 4: ROOT + 40}, stale={ROOT + 2})
        counts = self.run(prod, self.info(names), root_ext)
        assert seg_rows(prod, names) == [
            (1, SV + 1, ROOT + 1, 0, 0),
            (2, SV + 2, ROOT + 20, 0, 0),
            (3, SV + 3, ROOT + 30, 0, 0),
            (4, SV + 4, ROOT + 40, 0, 0),
            (5, 0, 0, 0, 0),
        ]
        assert counts == {
            "rows": 5, "rows_updated": 3, "supervoxels_looked_up": 0,
            "roots_checked": 3, "roots_stale": 2, "roots_looked_up": 3,
        }
        # each distinct root read once; the non-root is never asked about
        assert sorted(root_ext.checked) == [ROOT + 1, ROOT + 2]

    def test_blank_supervoxels_are_looked_up_and_other_fields_untouched(self, production):
        prod, _, table = production
        names = pb.build_names(JOB, table, PCG)
        seg_table(prod, names, [(1, 0, 0, SV + 9, ROOT + 9), (2, SV + 2, 0, SV + 8, ROOT + 8)])
        root_ext = FakeRootExt({SV + 1: ROOT + 1, SV + 2: ROOT + 2})
        found = []

        def supervoxels(engine, info, field, ids):
            found.append(ids.tolist())
            return np.array([SV + 1], dtype=np.int64)

        counts = self.run(prod, self.info(names, pre=(True, False)), root_ext, supervoxels=supervoxels)
        assert found == [[1]]
        assert seg_rows(prod, names) == [(1, SV + 1, ROOT + 1, SV + 9, ROOT + 9), (2, SV + 2, ROOT + 2, SV + 8, ROOT + 8)]
        assert counts["supervoxels_looked_up"] == 1 and counts["roots_looked_up"] == 2
        assert root_ext.checked == []  # roots were not supplied for pre_pt

    def test_only_the_range_is_read(self, production):
        prod, _, table = production
        names = pb.build_names(JOB, table, PCG)
        seg_table(prod, names, [(i, SV + i, 0, 0, 0) for i in range(1, 7)])
        root_ext = FakeRootExt({SV + i: ROOT + i for i in range(1, 7)})
        assert self.run(prod, self.info(names), root_ext, first=3, last=4)["rows"] == 2
        assert [r[2] for r in seg_rows(prod, names)] == [0, 0, ROOT + 3, ROOT + 4, 0, 0]


class TestSuppliedStages:
    """load_segmentation and resolve_roots against a scratch aligned volume database,
    the Cloud SQL import stood in for by inserts, the chunks run inline."""

    def test_load_resolve_finish_and_publish(self, production, engine, app):
        from materializationengine.upload_state.status_sink import StoreSink, use_sink

        prod, interface, table = production
        names = pb.build_names(JOB, table, PCG)
        meta = file_metadata("pre_pt_supervoxel_id", "pre_pt_root_id", "post_pt_supervoxel_id")
        supplied = supplied_ids.supplied_segmentation(meta)
        params = {
            **PRODUCTION_PARAMS,
            "file_metadata": {**meta, "metadata": {**meta["metadata"], "table_name": table}},
            "build_tables": names,
            "supplied_segmentation": supplied,
        }
        assert bulk_upload.stage_order(params) == [
            "process_source", "load_annotations", "load_segmentation", "resolve_roots",
            "finish_tables", "publish", "cleanup",
        ]
        new_job(engine)
        store.set_job_fields(engine, JOB, params=params)
        ctx = bulk_upload.Context(
            engine=engine, job_id=JOB, epoch=1, params=params,
            runner=runner_module.InlineRunner(), max_in_flight=4,
        )
        ctx.results["process_source"] = {
            "output_path": "bucket/processed.csv",
            "segmentation_output_path": "bucket/processed.segmentation.csv",
        }
        imports = []

        def fake_import(instance, uri, database, table, columns=None, on_started=None, **kwargs):
            on_started(f"op-{len(imports)}")
            imports.append((uri, columns))
            with prod.begin() as conn:
                if uri.endswith(".segmentation.csv"):
                    assert columns == supplied["columns"]
                    conn.execute(sa.text(
                        f'INSERT INTO "{table}" (id, pre_pt_supervoxel_id, pre_pt_root_id, '
                        "post_pt_supervoxel_id, post_pt_root_id) "
                        f"SELECT x, {SV} + x, CASE WHEN x % 10 = 0 THEN {ROOT} ELSE {ROOT} + x END, {SV} + 1000 + x, 0 "
                        "FROM generate_series(1, 250) x"
                    ))
                else:
                    conn.execute(sa.text(
                        f'INSERT INTO "{table}" (id, created, valid, size, pre_pt_position, post_pt_position, ctr_pt_position)'
                        " SELECT x, now(), true, x, ST_MakePoint(x, x, x), ST_MakePoint(x, x, x + 1),"
                        " ST_MakePoint(x, x, x + 2) FROM generate_series(1, 250) x"
                    ))

        # roots: pre_pt's ROOT + x are current except ROOT itself (given for x % 10 == 0)
        roots = {SV + x: ROOT + x for x in range(1, 251)}
        roots.update({SV + 1000 + x: ROOT + 5000 + x for x in range(1, 251)})
        root_ext = FakeRootExt(roots, stale={ROOT})
        config = {"UPLOAD_ROOT_CHUNK_ROWS": 60}
        with use_sink(StoreSink(engine, JOB, epoch=1)), \
                mock.patch("materializationengine.database.db_manager.get_engine", return_value=prod), \
                mock.patch("materializationengine.database.dynamic_annotation_cache.get_db", return_value=interface), \
                mock.patch("materializationengine.kvdb_gateway.kvdb_cache.get_client", return_value=mock.Mock(root_ext=root_ext)), \
                mock.patch.object(tasks, "import_csv", side_effect=fake_import), \
                mock.patch.object(tasks, "get_config_param", return_value=None), \
                mock.patch("materializationengine.utils.get_config_param", side_effect=lambda k, d=None: config.get(k, d)):
            ctx.results["load_annotations"] = bulk_upload.stage_load_annotations(ctx)
            ctx.results["load_segmentation"] = bulk_upload.stage_load_segmentation(ctx)
            resolved = bulk_upload.stage_resolve_roots(ctx)
            chunks = store.root_chunks(engine, JOB)
            # resumed: every chunk is done, so nothing runs again
            again = bulk_upload.stage_resolve_roots(ctx)
            bulk_upload.stage_finish_tables(ctx)
            published = bulk_upload.stage_publish(ctx)

        assert [uri for uri, _ in imports] == ["gs://bucket/processed.csv", "gs://bucket/processed.segmentation.csv"]
        assert len(chunks) == 5 and all(c["status"] == "COMPLETED" for c in chunks)
        assert (chunks[0]["first_id"], chunks[-1]["last_id"]) == (1, 250)
        assert resolved["roots_checked"] == 250 and resolved["roots_stale"] == 25
        assert resolved["roots_looked_up"] == 25 + 250  # the stale pre roots, every post root
        assert again["chunks"] == 5 and again["roots_stale"] == 25
        assert published["publish"] == "published"
        with prod.connect() as conn:
            rows = conn.execute(
                f'SELECT id, pre_pt_root_id, post_pt_root_id FROM "{names["final_segmentation"]}" ORDER BY id'
            ).fetchall()
        assert [tuple(r) for r in rows] == [(x, ROOT + x, ROOT + 5000 + x) for x in range(1, 251)]
        assert pb.index_commands(
            prod, names["final_segmentation"], pb.segmentation_model(table, META, PCG)
        ) == []


class TestThreadedBatches:
    def test_pieces_keep_their_order_with_threads(self):
        ids = np.arange(1, 2501, dtype=np.uint64)
        seen = []

        def fn(piece):
            seen.append(len(piece))
            return piece * 10

        out = resolve_roots._map_batches(fn, ids, 1000, threads=4)
        assert out.tolist() == (ids * 10).tolist()
        assert sorted(seen) == [500, 1000, 1000]

    def test_lookups_split_into_pieces(self):
        root_ext = FakeRootExt({SV + i: ROOT + i for i in range(10)})
        svs = np.array([SV + i for i in range(10)] * 2, dtype=np.uint64)
        roots = resolve_roots.lookup_roots(mock.Mock(root_ext=root_ext), svs, None, "c", batch_size=3, threads=4)
        assert roots.tolist() == [ROOT + i for i in range(10)] * 2
        current = resolve_roots.current_roots(
            mock.Mock(root_ext=FakeRootExt({}, stale={ROOT + 4})), roots, None, batch_size=3, threads=4
        )
        assert current.tolist() == [i != 4 for i in range(10)] * 2
