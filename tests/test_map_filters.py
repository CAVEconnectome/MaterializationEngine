"""Tests for map_filters, the root-id remap that live queries depend on.

The bug these guard against: the "query timestamp precedes the materialized version" branch was
unreachable (its condition duplicated the branch above), so such queries were never remapped. The
caller's root id went straight into SQL against a table that does not contain it and the query
returned zero rows with no error -- indistinguishable from "this object has no annotations".
"""

import datetime

import numpy as np
import pytest

from materializationengine.blueprints.client.new_query import map_filters

MAT = datetime.datetime(2021, 6, 11, 8, 10, tzinfo=datetime.timezone.utc)
BEFORE = datetime.datetime(2020, 9, 1, tzinfo=datetime.timezone.utc)
AFTER = datetime.datetime(2026, 8, 18, tzinfo=datetime.timezone.utc)

QUERY_ID = 864691135683974071
MAT_IDS = [864691135538204786, 864691135683480306]


class FakeCG:
    """Mimics the chunkedgraph surface map_filters uses, including its quirks."""

    def __init__(self, latest=None, past=None, all_valid=True):
        self._latest = latest or {}
        self._past = past or {}
        self._all_valid = all_valid
        self.latest_roots_calls = []
        self.past_ids_calls = []

    def is_latest_roots(self, root_ids, timestamp=None):
        return np.ones(len(root_ids), dtype=bool) * self._all_valid

    def get_latest_roots(self, root_id, timestamp=None):
        # Deliberately rejects iterables: the real client silently returns only the first
        # root's expansion when handed a list, which would corrupt multi-root filters.
        if isinstance(root_id, (list, tuple, np.ndarray)):
            raise AssertionError("get_latest_roots must be called with a scalar root_id")
        self.latest_roots_calls.append((int(root_id), timestamp))
        return np.array(self._latest.get(int(root_id), []), dtype=np.int64)

    def get_past_ids(self, root_ids, timestamp_past=None, timestamp_future=None):
        self.past_ids_calls.append((list(map(int, root_ids)), timestamp_past, timestamp_future))
        return {
            "past_id_map": {int(r): np.array(self._past.get(int(r), []), dtype=np.int64)
                            for r in root_ids},
            # The server never populates this; keep the fake honest about that.
            "future_id_map": {},
        }


def filters_for(rid):
    return [{"synapses_pni_2": {"post_pt_root_id": rid}}]


class TestQueryBeforeMaterialized:
    """The previously-unreachable direction."""

    def test_filter_is_expanded_to_materialized_ids(self):
        cg = FakeCG(latest={QUERY_ID: MAT_IDS})
        new_filters, query_map, warnings = map_filters(
            filters_for(QUERY_ID), BEFORE, MAT, cg
        )
        got = new_filters[0]["synapses_pni_2"]["post_pt_root_id"]
        assert sorted(int(x) for x in got) == sorted(MAT_IDS), (
            "the query-time id must be expanded to the ids the materialized table stores; "
            "returning it unchanged is what produced silent zero-row results"
        )

    def test_expansion_uses_the_materialized_timestamp(self):
        cg = FakeCG(latest={QUERY_ID: MAT_IDS})
        map_filters(filters_for(QUERY_ID), BEFORE, MAT, cg)
        assert cg.latest_roots_calls == [(QUERY_ID, MAT)]

    def test_query_map_is_empty_so_supervoxel_lookup_corrects_results(self):
        """update_rootids applies query_map with pandas .replace(), which needs scalar->scalar.

        A materialized-era id can map back to several query-era ids, so the mapping is left empty
        and result correction is done by re-looking up supervoxels at the query timestamp.
        """
        cg = FakeCG(latest={QUERY_ID: MAT_IDS})
        _, query_map, _ = map_filters(filters_for(QUERY_ID), BEFORE, MAT, cg)
        assert query_map == {}

    def test_multiple_root_ids_each_get_their_own_expansion(self):
        other = 864691135406097394
        cg = FakeCG(latest={QUERY_ID: MAT_IDS, other: [111, 222]})
        new_filters, _, _ = map_filters(
            [{"synapses_pni_2": {"post_pt_root_id": [QUERY_ID, other]}}], BEFORE, MAT, cg
        )
        got = sorted(int(x) for x in new_filters[0]["synapses_pni_2"]["post_pt_root_id"])
        assert got == sorted(MAT_IDS + [111, 222])
        assert sorted(c[0] for c in cg.latest_roots_calls) == sorted([QUERY_ID, other])

    def test_root_with_no_materialized_counterpart_warns(self):
        cg = FakeCG(latest={QUERY_ID: []})
        _, _, warnings = map_filters(filters_for(QUERY_ID), BEFORE, MAT, cg)
        assert any("no corresponding ids" in w for w in warnings)

    def test_does_not_use_future_id_map_from_get_past_ids(self):
        """That map is always empty server-side; relying on it is what made the branch inert."""
        cg = FakeCG(latest={QUERY_ID: MAT_IDS})
        map_filters(filters_for(QUERY_ID), BEFORE, MAT, cg)
        assert cg.past_ids_calls == []


class TestQueryAfterMaterialized:
    """The already-working direction must be untouched."""

    def test_uses_past_id_map(self):
        cg = FakeCG(past={QUERY_ID: MAT_IDS})
        new_filters, _, _ = map_filters(filters_for(QUERY_ID), AFTER, MAT, cg)
        got = sorted(int(x) for x in new_filters[0]["synapses_pni_2"]["post_pt_root_id"])
        assert got == sorted(MAT_IDS)
        assert cg.past_ids_calls and cg.latest_roots_calls == []


class TestEqualTimestamps:
    def test_no_remap(self):
        cg = FakeCG()
        original = filters_for(QUERY_ID)
        new_filters, query_map, _ = map_filters(original, MAT, MAT, cg)
        assert new_filters is original
        assert query_map == {}
        assert cg.latest_roots_calls == [] and cg.past_ids_calls == []


class TestNoRootIdFilters:
    def test_returns_untouched(self):
        cg = FakeCG()
        filters = [{"synapses_pni_2": {"size": 5}}]
        new_filters, query_map, _ = map_filters(filters, BEFORE, MAT, cg)
        assert new_filters is filters
        assert query_map == {}
