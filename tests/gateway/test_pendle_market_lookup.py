"""Tests for the Pendle market lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.pendle.gateway.market_lookup import (
    CACHE_TTL_SECONDS,
    PendleMarketLookup,
    PendleTokenMetadata,
)

SAMPLE_ASSETS = [
    # Ethereum PT / YT / SY / LP (mapped chain)
    {
        "chainId": 1,
        "address": "0xC689F76F90Fe1762FAC55983Ff25aE71033a84F7",
        "symbol": "PT-sUSDat-27AUG2026",
        "decimals": 6,
        "name": "PT-sUSDat-27AUG2026",
        "tags": ["PT"],
    },
    {
        "chainId": 1,
        "address": "0x7956bb9504b8a1f515f2890e309cee398198d3bd",
        "symbol": "YT-sUSDat-27AUG2026",
        "decimals": 6,
        "name": "YT-sUSDat-27AUG2026",
        "tags": ["YT"],
    },
    {
        "chainId": 1,
        "address": "0x8917f8c7feb840b5837edc7e128123baa2f289f9",
        "symbol": "SY-sUSDat",
        "decimals": 18,
        "name": "SY-sUSDat",
        "tags": ["SY"],
    },
    {
        "chainId": 1,
        "address": "0x91bc86899c8391b6caaf26535b9cd82efe49a189",
        "symbol": "PENDLE-LPT",
        "decimals": 18,
        "name": "PENDLE-LPT",
        "tags": ["PENDLE_LP"],
    },
    # Arbitrum PT
    {
        "chainId": 42161,
        "address": "0x71fbf40651e9d4278a74586afc99f307f369ce9a",
        "symbol": "PT-wstETH-25JUN2026",
        "decimals": 18,
        "name": "PT-wstETH-25JUN2026",
        "tags": ["PT"],
    },
    # Unmapped chain (Sonic) — must be silently dropped
    {
        "chainId": 146,
        "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "symbol": "PT-SONIC-01JAN2099",
        "decimals": 18,
        "name": "PT-SONIC-01JAN2099",
        "tags": ["PT"],
    },
    # Non-Pendle-tagged entry (GENERIC) — must be dropped
    {
        "chainId": 1,
        "address": "0x808507121b80c02388fad14726482e061b8da827",
        "symbol": "PENDLE",
        "decimals": 18,
        "name": "PENDLE",
        "tags": ["GENERIC"],
    },
    # Malformed: missing address — must be skipped without raising
    {
        "chainId": 1,
        "address": "",
        "symbol": "PT-BROKEN",
        "decimals": 18,
        "tags": ["PT"],
    },
]


class TestBuildIndices:
    """Tests for _build_indices."""

    def test_only_mapped_chains_indexed(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)

        assert set(lookup._symbol_indices.keys()) == {"ethereum", "arbitrum"}
        assert "sonic" not in lookup._symbol_indices

    def test_all_four_token_types_indexed_on_ethereum(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)

        eth = lookup._symbol_indices["ethereum"]
        assert "PT-SUSDAT-27AUG2026" in eth
        assert "YT-SUSDAT-27AUG2026" in eth
        assert "SY-SUSDAT" in eth
        assert "PENDLE-LPT" in eth

    def test_pendle_lp_tag_normalised_to_LP_token_type(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)
        meta = lookup._symbol_indices["ethereum"]["PENDLE-LPT"]
        assert meta.token_type == "LP"

    def test_generic_and_unmapped_tags_dropped(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)
        # PENDLE (baseType=GENERIC) should not be indexed
        assert "PENDLE" not in lookup._symbol_indices["ethereum"]

    def test_malformed_entry_skipped_without_raising(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)
        # PT-BROKEN had an empty address and must not appear
        assert "PT-BROKEN" not in lookup._symbol_indices["ethereum"]

    def test_address_lowercased_in_index(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)
        # Input was mixed-case "0xC689..." — index must store it lowercased
        assert "0xc689f76f90fe1762fac55983ff25ae71033a84f7" in lookup._address_indices["ethereum"]
        assert "0xC689F76F90Fe1762FAC55983Ff25aE71033a84F7" not in lookup._address_indices["ethereum"]


class TestLookupAPI:
    """Tests for public lookup_by_symbol / lookup_by_address methods."""

    @pytest.fixture
    def loaded_lookup(self):
        lookup = PendleMarketLookup()
        lookup._build_indices(SAMPLE_ASSETS)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("pt-susdat-27aug2026", "ethereum")
        assert meta is not None
        assert meta.symbol == "PT-sUSDat-27AUG2026"
        assert meta.decimals == 6
        assert meta.token_type == "PT"
        assert meta.chain == "ethereum"

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("PT-WSTETH-25JUN2026", "ARBITRUM")
        assert meta is not None
        assert meta.chain == "arbitrum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        # PT-sUSDat only exists on ethereum; querying on arbitrum must miss
        meta = loaded_lookup.lookup_by_symbol("PT-sUSDat-27AUG2026", "arbitrum")
        assert meta is None

    def test_lookup_by_address_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0xC689F76F90Fe1762FAC55983Ff25aE71033a84F7", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "PT-sUSDat-27AUG2026"

    def test_lookup_unknown_chain_returns_none(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("PT-sUSDat-27AUG2026", "polygon") is None
        assert loaded_lookup.lookup_by_address("0x0", "polygon") is None


class TestDiskCache:
    """Tests for disk cache read/write."""

    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = PendleMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "pendle_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_ASSETS))
        # Age the mtime beyond TTL
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))

        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = PendleMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "pendle_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_ASSETS))

        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = PendleMarketLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert isinstance(data, list)
        assert len(data) == len(SAMPLE_ASSETS)

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "pendle_market_cache.json"
        cache_path.write_text('{"not":"a list"}')
        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = PendleMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "pendle_market_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH", cache_path
        )

        lookup = PendleMarketLookup()
        lookup._write_disk_cache(SAMPLE_ASSETS)

        assert cache_path.exists()
        # .tmp must be cleaned up after rename
        assert not cache_path.with_suffix(".tmp").exists()
        round_trip = json.loads(cache_path.read_text())
        assert round_trip == SAMPLE_ASSETS


class TestLoadFlow:
    """Tests for the _load orchestration (cache-first, network fallback, backoff)."""

    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "pendle_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_ASSETS))
        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH", cache_path
        )

        lookup = PendleMarketLookup()

        async def fail_fetch() -> None:  # pragma: no cover — asserted not-called
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices

    def test_load_sets_backoff_when_everything_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH",
            tmp_path / "missing.json",
        )

        lookup = PendleMarketLookup()

        async def empty_fetch() -> None:
            return None

        with patch.object(lookup, "_fetch_from_network", side_effect=empty_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded is False
        assert lookup._load_failed is True
        assert lookup._retry_after > time.monotonic()

    def test_retry_skipped_inside_backoff_window(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.pendle.gateway.market_lookup.CACHE_PATH",
            tmp_path / "missing.json",
        )

        lookup = PendleMarketLookup()
        # Simulate a prior failure that set a future retry_after
        lookup._load_failed = True
        lookup._retry_after = time.monotonic() + 3600  # 1h in future

        call_count = 0

        async def tracked_fetch() -> None:
            nonlocal call_count
            call_count += 1
            return None

        with patch.object(lookup, "_fetch_from_network", side_effect=tracked_fetch):
            asyncio.run(lookup._load())

        assert call_count == 0, "Must not fire network fetch while inside backoff window"


class TestPendleTokenMetadata:
    """Sanity checks on the dataclass."""

    def test_fields(self):
        meta = PendleTokenMetadata(
            address="0xabc",
            symbol="PT-FOO",
            name="PT-FOO",
            decimals=18,
            token_type="PT",
            chain="ethereum",
        )
        assert meta.address == "0xabc"
        assert meta.token_type == "PT"
        assert meta.chain == "ethereum"


class TestLooksLikePendleSymbol:
    """Tests for the symbol prefix predicate used by TokenService."""

    @pytest.mark.parametrize(
        "symbol",
        [
            "PT-sUSDe-7MAY2026",
            "pt-susde-7may2026",  # lowercase
            "YT-wstETH",
            "SY-USDG",
            "LP-sUSDe",
            "PENDLE-LPT",  # legacy pool token naming
        ],
    )
    def test_recognises_pendle_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_pendle_symbol
        assert _looks_like_pendle_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "USDC",
            "WETH",
            "AUSDC",  # Aave token — not Pendle
            "stETH",
            "",
        ],
    )
    def test_rejects_non_pendle_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_pendle_symbol
        assert _looks_like_pendle_symbol(symbol) is False
