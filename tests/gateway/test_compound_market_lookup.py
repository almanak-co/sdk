"""Tests for the Compound v3 market lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.compound_v3.gateway.market_lookup import (
    CACHE_TTL_SECONDS,
    CompoundMarketLookup,
    CompoundMarketToken,
)


SAMPLE_AGGREGATOR = {
    "markets": {
        # mainnet maps to 'ethereum' in the gateway
        "mainnet": {
            "cUSDCv3": {
                "baseToken": {
                    "symbol": "USDC",
                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "decimals": 6,
                },
                "contracts": {"comet": "0xc3d688B66703497DAA19211EEdff47f25384cdc3"},
            },
            "cWETHv3": {
                "baseToken": {
                    "symbol": "WETH",
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "decimals": 18,
                },
                "contracts": {"comet": "0xA17581A9E335a9F94D2Ad2EDCe70928ed4Dc3F9c"},
            },
            "cWstETHv3": {
                "baseToken": {
                    "symbol": "wstETH",
                    "address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
                    "decimals": 18,
                },
                "contracts": {"comet": "0x3D0bb1ccaB520A66e607822fC55BC921738fAFE3"},
            },
        },
        "arbitrum": {
            "cUSDCv3": {
                "baseToken": {
                    "symbol": "USDC",
                    "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "decimals": 6,
                },
                "contracts": {"comet": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf"},
            },
            "cUSDCev3": {
                "baseToken": {
                    "symbol": "USDC.e",
                    "address": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
                    "decimals": 6,
                },
                "contracts": {"comet": "0xa5edbdd9646f8dff606d7448e414884c7d905dca"},
            },
        },
        # Unmapped network (ronin) — must be silently dropped
        "ronin": {
            "cWETHv3": {
                "baseToken": {
                    "symbol": "WETH",
                    "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                    "decimals": 18,
                },
                "contracts": {"comet": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"},
            },
        },
        # Malformed — missing comet address
        "base": {
            "cBROKEN": {
                "baseToken": {"symbol": "BROKEN", "address": "0x0", "decimals": 18},
                "contracts": {},
            },
        },
    },
    "rewards": {},
    "networkCompBalance": {},
}


class TestBuildIndices:
    """Tests for _build_indices."""

    def test_only_mapped_networks_indexed(self):
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)

        assert "ethereum" in lookup._symbol_indices
        assert "arbitrum" in lookup._symbol_indices
        # ronin is unmapped → dropped
        assert "ronin" not in lookup._symbol_indices

    def test_mainnet_normalised_to_ethereum(self):
        """The aggregator uses ``mainnet``; the gateway uses ``ethereum``."""
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        assert "mainnet" not in lookup._symbol_indices
        assert "CUSDCV3" in lookup._symbol_indices["ethereum"]

    def test_ctoken_indexed_with_correct_metadata(self):
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        meta = lookup._symbol_indices["ethereum"]["CUSDCV3"]
        assert meta.symbol == "cUSDCv3"
        assert meta.address == "0xc3d688b66703497daa19211eedff47f25384cdc3"
        assert meta.decimals == 6
        assert meta.underlying_symbol == "USDC"
        assert meta.chain == "ethereum"

    def test_decimals_inherit_from_baseToken(self):
        """Comet markets share the base token's decimals."""
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        assert lookup._symbol_indices["ethereum"]["CWETHV3"].decimals == 18
        assert lookup._symbol_indices["ethereum"]["CUSDCV3"].decimals == 6

    def test_name_follows_compound_base_convention(self):
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        assert lookup._symbol_indices["ethereum"]["CUSDCV3"].name == "Compound USDC"

    def test_malformed_entry_skipped_without_raising(self):
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        # cBROKEN on base had empty contracts; must not be indexed
        assert "CBROKEN" not in lookup._symbol_indices.get("base", {})

    def test_address_lowercased_in_index(self):
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        # Mixed-case aggregator address must be lowercased for indexing
        assert "0xc3d688b66703497daa19211eedff47f25384cdc3" in lookup._address_indices["ethereum"]
        assert "0xc3d688B66703497DAA19211EEdff47f25384cdc3" not in lookup._address_indices["ethereum"]

    def test_same_symbol_different_chains_coexist(self):
        """cUSDCv3 exists on both ethereum and arbitrum with distinct addresses."""
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        eth = lookup._symbol_indices["ethereum"]["CUSDCV3"]
        arb = lookup._symbol_indices["arbitrum"]["CUSDCV3"]
        assert eth.address != arb.address


class TestLookupAPI:
    """Tests for public lookup_by_symbol / lookup_by_address methods."""

    @pytest.fixture
    def loaded_lookup(self):
        lookup = CompoundMarketLookup()
        lookup._build_indices(SAMPLE_AGGREGATOR)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("cusdcv3", "ethereum")
        assert meta is not None
        assert meta.symbol == "cUSDCv3"

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("CUSDCv3", "ARBITRUM")
        assert meta is not None
        assert meta.chain == "arbitrum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        # cWstETHv3 only on ethereum; querying arbitrum must miss
        assert loaded_lookup.lookup_by_symbol("cWstETHv3", "arbitrum") is None

    def test_lookup_by_address(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0xc3d688B66703497DAA19211EEdff47f25384cdc3", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "cUSDCv3"


class TestValidatePayload:
    """Shape validation — regressions against aggregator schema drift."""

    def test_accepts_well_formed_payload(self):
        lookup = CompoundMarketLookup()
        assert lookup._validate_payload(SAMPLE_AGGREGATOR) is True

    def test_rejects_list_top_level(self):
        lookup = CompoundMarketLookup()
        assert lookup._validate_payload([]) is False

    def test_rejects_missing_markets_key(self):
        lookup = CompoundMarketLookup()
        assert lookup._validate_payload({"rewards": {}}) is False

    def test_rejects_empty_markets(self):
        lookup = CompoundMarketLookup()
        assert lookup._validate_payload({"markets": {}}) is False


class TestDiskCache:
    """Tests for disk cache read/write."""

    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.compound_v3.gateway.market_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = CompoundMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "compound_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_AGGREGATOR))
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))

        monkeypatch.setattr(
            "almanak.connectors.compound_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = CompoundMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "compound_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_AGGREGATOR))

        monkeypatch.setattr(
            "almanak.connectors.compound_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = CompoundMarketLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert "markets" in data

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "compound_market_cache.json"
        cache_path.write_text(json.dumps([]))  # list top-level — wrong shape
        monkeypatch.setattr(
            "almanak.connectors.compound_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = CompoundMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "compound_market_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.compound_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )

        lookup = CompoundMarketLookup()
        lookup._write_disk_cache(SAMPLE_AGGREGATOR)

        assert cache_path.exists()
        assert not cache_path.with_suffix(".tmp").exists()
        round_trip = json.loads(cache_path.read_text())
        assert round_trip == SAMPLE_AGGREGATOR


class TestLoadFlow:
    """Tests for the _load orchestration."""

    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "compound_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_AGGREGATOR))
        monkeypatch.setattr(
            "almanak.connectors.compound_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )

        lookup = CompoundMarketLookup()

        async def fail_fetch() -> None:  # pragma: no cover — asserted not-called
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices


class TestLooksLikeCompoundSymbol:
    """Tests for the Compound prefix predicate used by TokenService."""

    @pytest.mark.parametrize(
        "symbol",
        [
            "cUSDCv3",
            "cWETHv3",
            "cWstETHv3",
            "cAEROv3",
            "cUSDCev3",
            "cWBTCv3",
            "cUSDCV3",  # mixed case in the suffix
            "CUSDCV3",  # fully uppercase — users sometimes shout-case
            "CUSDCv3",  # uppercase prefix, lowercase suffix
            "cusdcv3",  # fully lowercase
        ],
    )
    def test_recognises_compound_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_compound_symbol

        assert _looks_like_compound_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "USDC",  # not c-prefixed
            "COMP",  # uppercase C + no v3
            "cbBTC",  # Coinbase wrapped BTC — c-prefixed but no v3 suffix
            "cbETH",
            "crvUSD",  # Curve — c-prefixed, no v3
            "cUSDC",  # hypothetical Compound v2 cToken — no v3
            "PT-sUSDe",
            "",
            "c",
            "cv3",  # too short even though pattern matches
        ],
    )
    def test_rejects_non_compound_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_compound_symbol

        assert _looks_like_compound_symbol(symbol) is False


class TestMarketTokenDataclass:
    def test_fields(self):
        meta = CompoundMarketToken(
            address="0xabc",
            symbol="cUSDCv3",
            name="Compound USDC",
            decimals=6,
            chain="ethereum",
            underlying_symbol="USDC",
            underlying_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        )
        assert meta.address == "0xabc"
        assert meta.underlying_symbol == "USDC"
