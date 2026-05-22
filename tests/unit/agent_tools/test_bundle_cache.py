"""Tests for the persistent compiled-bundle cache (VIB-2996)."""

from __future__ import annotations

import base64
import json
import os
import stat
import time

import pytest

from almanak.framework.agent_tools.bundle_cache import (
    DEFAULT_TTL_SECONDS,
    BundleCache,
    BundleCacheEntry,
    BundleExpiredError,
    default_cache_dir,
)


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path / "bundles"


@pytest.fixture
def cache(cache_dir):
    return BundleCache(cache_dir=cache_dir)


def _put(cache: BundleCache, bundle_id: str = "bundle-1", *, chain: str = "arbitrum") -> None:
    cache.put(bundle_id, chain, b'{"actions":[]}', {"intent_type": "swap"})


class TestBundleCacheBasics:
    def test_put_then_get_same_process(self, cache):
        _put(cache)
        entry = cache.get("bundle-1")
        assert entry is not None
        assert entry.chain == "arbitrum"
        assert entry.bundle_bytes == b'{"actions":[]}'
        assert entry.args == {"intent_type": "swap"}
        assert entry.ttl_seconds == DEFAULT_TTL_SECONDS

    def test_get_missing_returns_none(self, cache):
        assert cache.get("does-not-exist") is None

    def test_cross_process_disk_persistence(self, cache_dir):
        """A new BundleCache instance must recover bundles from disk."""
        first = BundleCache(cache_dir=cache_dir)
        first.put("bundle-x", "base", b"payload-bytes", {"intent_type": "lp_open"})

        # Simulate a fresh CLI invocation with no in-memory state.
        second = BundleCache(cache_dir=cache_dir)
        entry = second.get("bundle-x")
        assert entry is not None
        assert entry.chain == "base"
        assert entry.bundle_bytes == b"payload-bytes"
        assert entry.args == {"intent_type": "lp_open"}

    def test_pop_removes_from_memory_and_disk(self, cache, cache_dir):
        _put(cache)
        cache.pop("bundle-1")
        assert cache.get("bundle-1") is None
        # Disk file removed too.
        assert not (cache_dir / "bundle-1.json").exists()

    def test_pop_missing_is_noop(self, cache):
        cache.pop("nonexistent")  # must not raise

    def test_clear_removes_all_entries(self, cache):
        for i in range(3):
            _put(cache, f"bundle-{i}")
        removed = cache.clear()
        assert removed >= 3
        for i in range(3):
            assert cache.get(f"bundle-{i}") is None


class TestExpiry:
    """Deterministic expiry tests — time.time is monkeypatched so these are
    not sensitive to CI jitter or filesystem timestamp precision.
    (CodeRabbit round 2 flaky-sleep guidance.)
    """

    _BUNDLE_CACHE = "almanak.framework.agent_tools.bundle_cache.time.time"

    def test_get_raises_bundle_expired(self, cache_dir, monkeypatch):
        base = 1_700_000_000.0
        monkeypatch.setattr(self._BUNDLE_CACHE, lambda: base)

        cache = BundleCache(cache_dir=cache_dir, default_ttl_seconds=1)
        cache.put("bundle-exp", "arbitrum", b"x", {})

        # Advance past the 1-second TTL.
        monkeypatch.setattr(self._BUNDLE_CACHE, lambda: base + 2.0)

        fresh = BundleCache(cache_dir=cache_dir, default_ttl_seconds=1)
        with pytest.raises(BundleExpiredError) as exc_info:
            fresh.get("bundle-exp")
        err = exc_info.value
        assert err.bundle_id == "bundle-exp"
        assert err.ttl_seconds == 1
        assert "expired" in str(err).lower()
        # Subsequent get must return None (file already removed).
        assert fresh.get("bundle-exp") is None

    def test_memory_hit_expiry_also_raises(self, cache_dir, monkeypatch):
        base = 1_700_000_000.0
        monkeypatch.setattr(self._BUNDLE_CACHE, lambda: base)

        cache = BundleCache(cache_dir=cache_dir, default_ttl_seconds=1)
        cache.put("bundle-exp", "arbitrum", b"x", {})

        monkeypatch.setattr(self._BUNDLE_CACHE, lambda: base + 2.0)
        with pytest.raises(BundleExpiredError):
            cache.get("bundle-exp")
        assert cache.get("bundle-exp") is None

    def test_prune_expired_removes_only_expired(self, cache_dir, monkeypatch):
        base = 1_700_000_000.0
        monkeypatch.setattr(self._BUNDLE_CACHE, lambda: base)

        cache = BundleCache(cache_dir=cache_dir, default_ttl_seconds=1)
        cache.put("bundle-exp", "arbitrum", b"x", {})

        # Advance past the first entry's TTL, then insert a live one.
        monkeypatch.setattr(self._BUNDLE_CACHE, lambda: base + 2.0)
        cache.put("bundle-live", "arbitrum", b"y", {})
        removed = cache.prune_expired()
        assert removed == 1
        # live survives
        assert cache.get("bundle-live") is not None
        # expired gone, no raise
        assert cache.get("bundle-exp") is None


class TestDiskFormat:
    def test_file_permissions_are_0600(self, cache, cache_dir):
        _put(cache)
        path = cache_dir / "bundle-1.json"
        assert path.is_file()
        mode = path.stat().st_mode & 0o777
        assert mode == 0o600, f"expected 0600, got {oct(mode)}"

    def test_dir_permissions_are_0700(self, cache, cache_dir):
        _put(cache)
        mode = cache_dir.stat().st_mode & 0o777
        assert mode == 0o700, f"expected 0700, got {oct(mode)}"

    def test_disk_payload_is_valid_json(self, cache, cache_dir):
        _put(cache)
        payload = json.loads((cache_dir / "bundle-1.json").read_text())
        assert payload["schema_version"] == 2
        assert payload["chain"] == "arbitrum"
        assert "wallet_address" in payload
        assert "deployment_id" in payload
        # Bundle bytes are base64-encoded so binary protobuf can round-trip.
        assert base64.b64decode(payload["bundle_b64"]) == b'{"actions":[]}'
        assert payload["args"] == {"intent_type": "swap"}
        assert "created_at" in payload
        assert payload["ttl_seconds"] == DEFAULT_TTL_SECONDS

    def test_corrupt_file_is_removed_and_miss_returned(self, cache, cache_dir):
        _put(cache)
        path = cache_dir / "bundle-1.json"
        path.write_text("not valid json {")
        # Drop the memory cache so we fall through to disk.
        cache._memory.clear()
        assert cache.get("bundle-1") is None
        assert not path.exists()

    def test_unknown_schema_version_treated_as_corrupt(self, cache, cache_dir):
        _put(cache)
        path = cache_dir / "bundle-1.json"
        data = json.loads(path.read_text())
        data["schema_version"] = 999
        path.write_text(json.dumps(data))
        cache._memory.clear()
        assert cache.get("bundle-1") is None

    @pytest.mark.parametrize(
        "mutation",
        [
            {"chain": []},  # wrong type
            {"chain": ""},  # empty
            {"bundle_b64": 42},  # not a string
            {"bundle_b64": "not-valid-base64!!!"},  # malformed
            {"args": "oops"},  # non-dict
            {"args": None},  # None
            {"created_at": "soon"},  # non-numeric
            {"ttl_seconds": 0},  # non-positive
            {"ttl_seconds": -1},
            {"ttl_seconds": "15m"},
            {"wallet_address": 123},  # non-string
            {"deployment_id": []},
        ],
    )
    def test_malformed_field_types_are_treated_as_corrupt(self, cache, cache_dir, mutation):
        """CodeRabbit round 2: a payload with wrong field types must be
        rejected and the file unlinked so it doesn't blow up later.
        """
        _put(cache)
        path = cache_dir / "bundle-1.json"
        data = json.loads(path.read_text())
        data.update(mutation)
        path.write_text(json.dumps(data))
        cache._memory.clear()
        assert cache.get("bundle-1") is None
        assert not path.exists()


class TestListEntries:
    def test_list_entries_empty(self, cache):
        assert cache.list_entries() == []

    def test_list_entries_ordered_newest_first(self, cache, monkeypatch):
        # Control the clock explicitly so the sort order isn't sensitive to
        # FS timestamp precision or scheduling jitter.
        times = iter([1_700_000_000.0, 1_700_000_060.0])
        monkeypatch.setattr(
            "almanak.framework.agent_tools.bundle_cache.time.time", lambda: next(times)
        )
        cache.put("old", "arbitrum", b"x", {"intent_type": "swap"})
        cache.put("new", "arbitrum", b"y", {"intent_type": "swap"})
        entries = cache.list_entries()
        ids = [bid for bid, _ in entries]
        assert ids[0] == "new"
        assert ids[1] == "old"


class TestLRU:
    def test_memory_layer_evicts_beyond_max(self, cache_dir):
        cache = BundleCache(cache_dir=cache_dir, max_memory_entries=2)
        cache.put("a", "arbitrum", b"1", {})
        cache.put("b", "arbitrum", b"2", {})
        cache.put("c", "arbitrum", b"3", {})
        # 'a' is evicted from memory but still retrievable from disk.
        assert len(cache._memory) == 2
        assert "a" not in cache._memory
        assert cache.get("a") is not None  # loaded from disk

    def test_max_memory_zero_is_disk_only(self, cache_dir):
        # Regression: ``max_memory_entries=0`` used to crash on the first
        # ``put`` because ``popitem()`` ran against an empty OrderedDict.
        # (CodeRabbit round 2 Major.)
        cache = BundleCache(cache_dir=cache_dir, max_memory_entries=0)
        cache.put("only-disk", "arbitrum", b"x", {"intent_type": "swap"})
        # Memory layer is empty in this mode.
        assert len(cache._memory) == 0
        # Disk round-trip still works.
        assert cache.get("only-disk") is not None


class TestPathSafety:
    def test_path_stays_inside_cache_dir(self, cache):
        # The executor only writes UUID4 ids, but defensively check that a
        # traversal-style id still resolves inside cache_dir after sanitisation.
        path = cache._path_for("../../etc/passwd")
        assert path.parent == cache._cache_dir
        assert path.name == "etcpasswd.json"

    def test_fully_invalid_id_raises(self, cache):
        # A fully-invalid id (no safe chars at all) must raise rather than
        # silently degrade to writing outside the cache dir.
        with pytest.raises(ValueError):
            cache._path_for("..")
        with pytest.raises(ValueError):
            cache._path_for("///")

    def test_get_with_fully_invalid_id_returns_miss(self, cache):
        # VIB-2996 / Codex P3 + CodeRabbit regression: BundleCache.get() must
        # turn a sanitisation ValueError into a clean cache miss so the
        # executor surfaces a validation error, not an internal_error.
        assert cache.get("..") is None
        assert cache.get("///") is None


class TestIdentityBinding:
    """VIB-2996 P1: bundle must not execute under a different wallet/strategy.

    Confirmed by Codex + CodeRabbit (2-auditor high-confidence finding).
    """

    def test_put_then_get_roundtrips_identity(self, cache):
        cache.put(
            "bundle-id",
            "arbitrum",
            b"payload",
            {"intent_type": "swap"},
            wallet_address="0xAbCdEf0123456789aBcDeF0123456789AbCdef01",
            deployment_id="demo_strategy",
        )
        entry = cache.get("bundle-id")
        assert entry is not None
        assert entry.wallet_address == "0xAbCdEf0123456789aBcDeF0123456789AbCdef01"
        assert entry.deployment_id == "demo_strategy"

    def test_identity_survives_disk_roundtrip(self, cache_dir):
        first = BundleCache(cache_dir=cache_dir)
        first.put(
            "bundle-id",
            "arbitrum",
            b"payload",
            {},
            wallet_address="0xABC",
            deployment_id="my_strat",
        )
        # Simulate a fresh process.
        second = BundleCache(cache_dir=cache_dir)
        entry = second.get("bundle-id")
        assert entry is not None
        assert entry.wallet_address == "0xABC"
        assert entry.deployment_id == "my_strat"

    def test_legacy_v1_file_is_treated_as_corrupt(self, cache, cache_dir):
        # If an attacker (or an older almanak-sdk build) drops a v1 file, the
        # loader must refuse it — v1 doesn't record wallet_address, so the
        # identity check could be bypassed.
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "legacy.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "chain": "arbitrum",
                    "bundle_b64": base64.b64encode(b"x").decode(),
                    "args": {},
                    "created_at": time.time(),
                    "ttl_seconds": 900,
                }
            )
        )
        assert cache.get("legacy") is None
        # The bogus file was unlinked on read.
        assert not (cache_dir / "legacy.json").exists()


class TestDefaultCacheDir:
    def test_default_cache_dir_honours_xdg(self, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", "/tmp/vib-2996-xdg")
        assert default_cache_dir() == type(default_cache_dir())("/tmp/vib-2996-xdg/almanak/bundles")

    def test_default_cache_dir_falls_back_to_home(self, monkeypatch):
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        path = default_cache_dir()
        assert path.parts[-3:] == (".cache", "almanak", "bundles")
