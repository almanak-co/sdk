"""Tests for the Fluid (Instadapp) market lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.fluid.gateway.market_lookup import (
    CACHE_TTL_SECONDS,
    FluidMarketLookup,
    FluidMarketToken,
)

SAMPLE_DATA = {
    "ethereum": [
        {
            "address": "0x9Fb7b4477576Fe5B32be4C1843aFB1e55F251B33",
            "symbol": "fUSDC",
            "name": "Fluid USD Coin",
            "decimals": 6,
            "assetAddress": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "asset": {"symbol": "USDC", "decimals": 6},
        },
        {
            "address": "0x6A29A46E21C730DcA1d8b23d637c101cec605C5B",
            "symbol": "fGHO",
            "name": "Fluid GHO",
            "decimals": 18,
            "assetAddress": "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f",
            "asset": {"symbol": "GHO", "decimals": 18},
        },
    ],
    "arbitrum": [
        {
            "address": "0x1111111111111111111111111111111111111111",
            "symbol": "fUSDC",
            "name": "Fluid USDC (Arbitrum)",
            "decimals": 6,
            "assetAddress": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "asset": {"symbol": "USDC", "decimals": 6},
        },
    ],
    "base": [
        {
            "address": "0x2222222222222222222222222222222222222222",
            "symbol": "fWETH",
            "name": "Fluid WETH (Base)",
            "decimals": 18,
            "assetAddress": "0x4200000000000000000000000000000000000006",
            "asset": {"symbol": "WETH", "decimals": 18},
        },
    ],
    # Unmapped chain — must be dropped
    "katana": [
        {
            "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "symbol": "fETH",
            "name": "Fluid ETH Katana",
            "decimals": 18,
            "assetAddress": "0x0",
            "asset": {"symbol": "ETH"},
        },
    ],
}


class TestBuildIndices:
    def test_mapped_chains_indexed(self):
        lookup = FluidMarketLookup()
        lookup._build_indices(SAMPLE_DATA)
        assert "ethereum" in lookup._symbol_indices
        assert "arbitrum" in lookup._symbol_indices
        assert "base" in lookup._symbol_indices

    def test_unmapped_chain_dropped(self):
        lookup = FluidMarketLookup()
        lookup._build_indices(SAMPLE_DATA)
        assert "katana" not in lookup._symbol_indices

    def test_ftoken_indexed_with_correct_metadata(self):
        lookup = FluidMarketLookup()
        lookup._build_indices(SAMPLE_DATA)
        meta = lookup._symbol_indices["ethereum"]["FUSDC"]
        assert meta.symbol == "fUSDC"
        assert meta.address == "0x9fb7b4477576fe5b32be4c1843afb1e55f251b33"
        assert meta.decimals == 6
        assert meta.underlying_symbol == "USDC"
        assert meta.chain == "ethereum"

    def test_same_symbol_different_chains_coexist(self):
        """fUSDC on ethereum + fUSDC on arbitrum are distinct entries."""
        lookup = FluidMarketLookup()
        lookup._build_indices(SAMPLE_DATA)
        eth = lookup._symbol_indices["ethereum"]["FUSDC"]
        arb = lookup._symbol_indices["arbitrum"]["FUSDC"]
        assert eth.address != arb.address

    def test_address_lowercased(self):
        lookup = FluidMarketLookup()
        lookup._build_indices(SAMPLE_DATA)
        assert "0x9fb7b4477576fe5b32be4c1843afb1e55f251b33" in lookup._address_indices["ethereum"]
        assert "0x9Fb7b4477576Fe5B32be4C1843aFB1e55F251B33" not in lookup._address_indices["ethereum"]


class TestLookupAPI:
    @pytest.fixture
    def loaded_lookup(self):
        lookup = FluidMarketLookup()
        lookup._build_indices(SAMPLE_DATA)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("fusdc", "ethereum")
        assert meta is not None
        assert meta.symbol == "fUSDC"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("fGHO", "arbitrum")
        assert meta is None

    def test_lookup_by_address(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0x9Fb7b4477576Fe5B32be4C1843aFB1e55F251B33", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "fUSDC"

    def test_lookup_unknown_chain_returns_none(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("fUSDC", "solana") is None


class TestValidatePayload:
    def test_accepts_well_formed_payload(self):
        lookup = FluidMarketLookup()
        assert lookup._validate_payload(SAMPLE_DATA) is True

    def test_rejects_list_top_level(self):
        lookup = FluidMarketLookup()
        assert lookup._validate_payload([]) is False

    def test_rejects_empty_dict(self):
        lookup = FluidMarketLookup()
        assert lookup._validate_payload({}) is False

    def test_rejects_dict_of_empty_lists(self):
        lookup = FluidMarketLookup()
        assert lookup._validate_payload({"ethereum": [], "arbitrum": []}) is False


class TestDiskCache:
    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.fluid.gateway.market_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = FluidMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "fluid_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_DATA))
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))
        monkeypatch.setattr(
            "almanak.connectors.fluid.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = FluidMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "fluid_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_DATA))
        monkeypatch.setattr(
            "almanak.connectors.fluid.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = FluidMarketLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert "ethereum" in data

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "fluid_market_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.fluid.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = FluidMarketLookup()
        lookup._write_disk_cache(SAMPLE_DATA)
        assert cache_path.exists()
        assert not cache_path.with_suffix(".tmp").exists()
        assert json.loads(cache_path.read_text()) == SAMPLE_DATA


class TestLoadFlow:
    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "fluid_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_DATA))
        monkeypatch.setattr(
            "almanak.connectors.fluid.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = FluidMarketLookup()

        async def fail_fetch() -> None:  # pragma: no cover
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())
        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices


class TestMarketTokenDataclass:
    def test_fields(self):
        meta = FluidMarketToken(
            address="0xabc",
            symbol="fUSDC",
            name="Fluid USDC",
            decimals=6,
            chain="ethereum",
            underlying_symbol="USDC",
            underlying_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        )
        assert meta.symbol == "fUSDC"


class TestLooksLikeFluidSymbol:
    """Tests for the Fluid prefix predicate used by TokenService.

    The predicate is intentionally loose: it admits anything starting
    with ``f`` / ``F`` (case-insensitive) of length >= 2.  The lookup
    itself is case-insensitive, so a stricter gate would skip Fluid
    when the user types ``FUSDC`` / ``fusdc``.  False positives like
    ``FRAX`` cost one dict miss before falling through to the next
    tier — acceptable.
    """

    @pytest.mark.parametrize(
        "symbol",
        [
            "fUSDC",
            "fUSDT",
            "fGHO",
            "fWETH",
            "fwstETH",
            "fwPOL",
            "fEURC",
            "fARB",
            "fAUSD",
            # Case-insensitive prefix: the lookup itself normalizes,
            # so the gate must too.
            "FUSDC",
            "fusdc",
            "FWETH",
            "fweth",
        ],
    )
    def test_recognises_fluid_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_fluid_symbol

        assert _looks_like_fluid_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "f",  # too short
            "",
            "USDC",  # no f prefix
            "aEthUSDC",
        ],
    )
    def test_rejects_non_fluid_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_fluid_symbol

        assert _looks_like_fluid_symbol(symbol) is False
