"""Tests for VersionedDataCache.

Covers versioning, finality tagging, checksum validation, version pinning,
provisional data handling, and eviction.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from almanak.framework.data.cache.versioned_cache import (
    CacheEntry,
    CacheStats,
    VersionedDataCache,
)


@pytest.fixture()
def cache(tmp_path: Path) -> VersionedDataCache:
    """Return a VersionedDataCache writing to a temp directory."""
    return VersionedDataCache(cache_dir=tmp_path, data_type="test_data")


# ---------------------------------------------------------------------------
# CacheEntry model
# ---------------------------------------------------------------------------


class TestCacheEntry:
    def test_valid_finalized(self):
        entry = CacheEntry(
            data={"price": "1800"},
            dataset_version=1,
            fetched_at="2026-01-01T00:00:00+00:00",
            finality_status="finalized",
            checksum="abc123",
        )
        assert entry.finality_status == "finalized"
        assert entry.dataset_version == 1

    def test_valid_provisional(self):
        entry = CacheEntry(
            data=[1, 2, 3],
            dataset_version=1,
            fetched_at="2026-01-01T00:00:00+00:00",
            finality_status="provisional",
            checksum="def456",
        )
        assert entry.finality_status == "provisional"

    def test_invalid_finality_status(self):
        with pytest.raises(ValueError, match="finality_status must be"):
            CacheEntry(
                data={},
                dataset_version=1,
                fetched_at="2026-01-01T00:00:00+00:00",
                finality_status="unknown",
                checksum="x",
            )

    def test_invalid_version_zero(self):
        with pytest.raises(ValueError, match="dataset_version must be >= 1"):
            CacheEntry(
                data={},
                dataset_version=0,
                fetched_at="2026-01-01T00:00:00+00:00",
                finality_status="finalized",
                checksum="x",
            )

    def test_frozen(self):
        entry = CacheEntry(
            data={},
            dataset_version=1,
            fetched_at="2026-01-01T00:00:00+00:00",
            finality_status="finalized",
            checksum="x",
        )
        with pytest.raises(AttributeError):
            entry.dataset_version = 2  # type: ignore[misc]


# ---------------------------------------------------------------------------
# CacheStats
# ---------------------------------------------------------------------------


class TestCacheStats:
    def test_initial_state(self):
        stats = CacheStats()
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0
        assert stats.integrity_failures == 0
        assert stats.hit_rate == 0.0

    def test_hit_rate(self):
        stats = CacheStats(hits=3, misses=1)
        assert stats.hit_rate == 0.75


# ---------------------------------------------------------------------------
# Basic put / get
# ---------------------------------------------------------------------------


class TestVersionedCachePutGet:
    def test_put_and_get_finalized(self, cache: VersionedDataCache):
        data = {"prices": [100, 200, 300]}
        entry = cache.put("key1", data, finality_status="finalized")
        assert entry.dataset_version == 1
        assert entry.finality_status == "finalized"
        assert entry.data == data

        retrieved = cache.get("key1")
        assert retrieved is not None
        assert retrieved.data == data
        assert retrieved.dataset_version == 1
        assert retrieved.finality_status == "finalized"

    def test_get_missing_key_returns_none(self, cache: VersionedDataCache):
        assert cache.get("nonexistent") is None
        assert cache.stats.misses == 1

    def test_put_and_get_provisional(self, cache: VersionedDataCache):
        data = {"temp": True}
        entry = cache.put("key1", data, finality_status="provisional")
        assert entry.finality_status == "provisional"

        retrieved = cache.get("key1")
        assert retrieved is not None
        assert retrieved.data == data
        assert retrieved.finality_status == "provisional"

    def test_stats_tracking(self, cache: VersionedDataCache):
        cache.put("key1", {"a": 1}, finality_status="finalized")
        cache.get("key1")  # hit
        cache.get("key1")  # hit
        cache.get("missing")  # miss
        assert cache.stats.hits == 2
        assert cache.stats.misses == 1


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


class TestVersioning:
    def test_auto_increment_on_data_change(self, cache: VersionedDataCache):
        cache.put("key1", {"v": 1}, finality_status="finalized")
        cache.put("key1", {"v": 2}, finality_status="finalized")
        cache.put("key1", {"v": 3}, finality_status="finalized")

        latest = cache.get("key1")
        assert latest is not None
        assert latest.dataset_version == 3
        assert latest.data == {"v": 3}

    def test_no_duplicate_version_if_data_unchanged(self, cache: VersionedDataCache):
        data = {"stable": True}
        e1 = cache.put("key1", data, finality_status="finalized")
        e2 = cache.put("key1", data, finality_status="finalized")
        assert e1.dataset_version == e2.dataset_version == 1
        assert cache.get_versions("key1") == [1]

    def test_version_pinning(self, cache: VersionedDataCache):
        cache.put("key1", {"v": 1}, finality_status="finalized")
        cache.put("key1", {"v": 2}, finality_status="finalized")
        cache.put("key1", {"v": 3}, finality_status="finalized")

        v1 = cache.get("key1", dataset_version=1)
        assert v1 is not None
        assert v1.data == {"v": 1}
        assert v1.dataset_version == 1

        v2 = cache.get("key1", dataset_version=2)
        assert v2 is not None
        assert v2.data == {"v": 2}

        v_latest = cache.get("key1")
        assert v_latest is not None
        assert v_latest.dataset_version == 3

    def test_get_versions_sorted(self, cache: VersionedDataCache):
        cache.put("key1", {"a": 1}, finality_status="finalized")
        cache.put("key1", {"a": 2}, finality_status="finalized")
        cache.put("key1", {"a": 3}, finality_status="finalized")
        assert cache.get_versions("key1") == [1, 2, 3]

    def test_get_versions_empty(self, cache: VersionedDataCache):
        assert cache.get_versions("nonexistent") == []

    def test_pinned_version_not_found(self, cache: VersionedDataCache):
        cache.put("key1", {"a": 1}, finality_status="finalized")
        assert cache.get("key1", dataset_version=99) is None


# ---------------------------------------------------------------------------
# Finality tagging
# ---------------------------------------------------------------------------


class TestFinalityTagging:
    def test_provisional_overwritten_by_provisional(self, cache: VersionedDataCache):
        cache.put("key1", {"old": True}, finality_status="provisional")
        cache.put("key1", {"new": True}, finality_status="provisional")

        result = cache.get("key1")
        assert result is not None
        assert result.data == {"new": True}
        assert result.finality_status == "provisional"

    def test_provisional_superseded_by_finalized(self, cache: VersionedDataCache):
        cache.put("key1", {"temp": True}, finality_status="provisional")
        cache.put("key1", {"final": True}, finality_status="finalized")

        result = cache.get("key1")
        assert result is not None
        assert result.data == {"final": True}
        assert result.finality_status == "finalized"

        # Provisional sidecar should be removed
        prov_path = cache._provisional_path("key1")
        assert not prov_path.exists()

    def test_finalized_takes_precedence_over_provisional(self, cache: VersionedDataCache):
        """When both finalized and provisional exist, get() returns finalized."""
        cache.put("key1", {"v": 1}, finality_status="finalized")
        cache.put("key1", {"v": 2, "prov": True}, finality_status="provisional")

        result = cache.get("key1")
        assert result is not None
        assert result.data == {"v": 1}
        assert result.finality_status == "finalized"

    def test_provisional_returned_when_no_finalized(self, cache: VersionedDataCache):
        cache.put("key1", {"prov": True}, finality_status="provisional")
        result = cache.get("key1")
        assert result is not None
        assert result.finality_status == "provisional"


# ---------------------------------------------------------------------------
# Checksum integrity
# ---------------------------------------------------------------------------


class TestChecksumIntegrity:
    def test_valid_checksum_passes(self, cache: VersionedDataCache):
        cache.put("key1", {"valid": True}, finality_status="finalized")
        result = cache.get("key1")
        assert result is not None
        assert result.data == {"valid": True}

    def test_corrupted_data_evicted(self, cache: VersionedDataCache):
        cache.put("key1", {"original": True}, finality_status="finalized")

        # Corrupt the cached file
        path = cache._version_path("key1", 1)
        raw = json.loads(path.read_text())
        raw["data"] = {"tampered": True}
        path.write_text(json.dumps(raw))

        result = cache.get("key1")
        assert result is None
        assert cache.stats.integrity_failures == 1
        assert cache.stats.evictions == 1

        # File should be removed
        assert not path.exists()

    def test_corrupted_checksum_evicted(self, cache: VersionedDataCache):
        cache.put("key1", {"original": True}, finality_status="finalized")

        path = cache._version_path("key1", 1)
        raw = json.loads(path.read_text())
        raw["checksum"] = "deadbeef" * 8
        path.write_text(json.dumps(raw))

        result = cache.get("key1")
        assert result is None
        assert cache.stats.integrity_failures == 1

    def test_checksum_computation_deterministic(self, cache: VersionedDataCache):
        data = {"b": 2, "a": 1}  # Unordered keys
        c1 = cache._compute_checksum(data)
        c2 = cache._compute_checksum(data)
        assert c1 == c2

        # Same data, different dict ordering -> same checksum (sort_keys=True)
        data2 = {"a": 1, "b": 2}
        c3 = cache._compute_checksum(data2)
        assert c1 == c3


# ---------------------------------------------------------------------------
# Eviction
# ---------------------------------------------------------------------------


class TestEviction:
    def test_evict_removes_all_versions(self, cache: VersionedDataCache):
        cache.put("key1", {"v": 1}, finality_status="finalized")
        cache.put("key1", {"v": 2}, finality_status="finalized")
        cache.put("key1", {"v": 3, "prov": True}, finality_status="provisional")

        removed = cache.evict("key1")
        assert removed >= 3  # v1, v2, provisional, meta

        assert cache.get("key1") is None
        assert cache.get_versions("key1") == []

    def test_evict_nonexistent_key(self, cache: VersionedDataCache):
        assert cache.evict("nonexistent") == 0

    def test_evict_updates_meta_after_corruption(self, cache: VersionedDataCache):
        cache.put("key1", {"v": 1}, finality_status="finalized")
        cache.put("key1", {"v": 2}, finality_status="finalized")

        # Corrupt v2
        path = cache._version_path("key1", 2)
        raw = json.loads(path.read_text())
        raw["checksum"] = "bad"
        path.write_text(json.dumps(raw))

        # get should evict v2 and update meta to v1
        result = cache.get("key1")
        assert result is None  # v2 was latest, got evicted

        # But v1 should still be accessible via pinning
        v1 = cache.get("key1", dataset_version=1)
        assert v1 is not None
        assert v1.data == {"v": 1}


# ---------------------------------------------------------------------------
# Multiple data types
# ---------------------------------------------------------------------------


class TestDataTypes:
    def test_different_data_types_isolated(self, tmp_path: Path):
        cache_a = VersionedDataCache(cache_dir=tmp_path, data_type="pool_history")
        cache_b = VersionedDataCache(cache_dir=tmp_path, data_type="lending_rates")

        cache_a.put("key1", {"pool": True}, finality_status="finalized")
        cache_b.put("key1", {"rate": True}, finality_status="finalized")

        a = cache_a.get("key1")
        b = cache_b.get("key1")
        assert a is not None and a.data == {"pool": True}
        assert b is not None and b.data == {"rate": True}

    def test_data_type_creates_subdirectory(self, tmp_path: Path):
        cache = VersionedDataCache(cache_dir=tmp_path, data_type="my_type")
        cache.put("key1", {"x": 1}, finality_status="finalized")
        assert (tmp_path / "my_type").is_dir()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_nested_data_structures(self, cache: VersionedDataCache):
        data = {
            "snapshots": [
                {"tvl": "1000000", "timestamp": "2026-01-01T00:00:00"},
                {"tvl": "1100000", "timestamp": "2026-01-02T00:00:00"},
            ],
            "metadata": {"chain": "arbitrum", "protocol": "uniswap_v3"},
        }
        cache.put("complex_key", data, finality_status="finalized")
        result = cache.get("complex_key")
        assert result is not None
        assert result.data == data

    def test_empty_data(self, cache: VersionedDataCache):
        cache.put("empty", {}, finality_status="finalized")
        result = cache.get("empty")
        assert result is not None
        assert result.data == {}

    def test_list_data(self, cache: VersionedDataCache):
        data = [{"a": 1}, {"b": 2}]
        cache.put("list_key", data, finality_status="finalized")
        result = cache.get("list_key")
        assert result is not None
        assert result.data == data

    def test_malformed_file_returns_none(self, cache: VersionedDataCache):
        cache.put("key1", {"v": 1}, finality_status="finalized")
        path = cache._version_path("key1", 1)
        path.write_text("this is not json")

        result = cache.get("key1", dataset_version=1)
        assert result is None

    def test_concurrent_data_type_dirs(self, tmp_path: Path):
        """Multiple data types can coexist under same cache_dir."""
        types = ["pool_history", "lending_rates", "funding_rates"]
        caches = [VersionedDataCache(cache_dir=tmp_path, data_type=t) for t in types]

        for i, c in enumerate(caches):
            c.put("shared_key", {"type_index": i}, finality_status="finalized")

        for i, c in enumerate(caches):
            result = c.get("shared_key")
            assert result is not None
            assert result.data == {"type_index": i}

    def test_version_after_eviction_and_re_put(self, cache: VersionedDataCache):
        """After evicting, re-putting starts from version 1."""
        cache.put("key1", {"v": 1}, finality_status="finalized")
        cache.put("key1", {"v": 2}, finality_status="finalized")
        cache.evict("key1")

        entry = cache.put("key1", {"v": "new"}, finality_status="finalized")
        assert entry.dataset_version == 1

    def test_special_characters_in_key(self, cache: VersionedDataCache):
        key = "pool:0xAbC123/arbitrum/1h"
        cache.put(key, {"special": True}, finality_status="finalized")
        result = cache.get(key)
        assert result is not None
        assert result.data == {"special": True}
