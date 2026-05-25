"""Tests for the Aave v3 market lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.aave_v3.gateway.market_lookup import (
    CACHE_TTL_SECONDS,
    AaveMarketLookup,
    AaveReserveToken,
)


SAMPLE_MARKETS = [
    # Ethereum Core market
    {
        "chain": {"chainId": 1},
        "reserves": [
            {
                "underlyingToken": {
                    "symbol": "USDC",
                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "decimals": 6,
                },
                "aToken": {
                    "symbol": "aEthUSDC",
                    "name": "Aave Ethereum USDC",
                    "address": "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c",
                    "decimals": 6,
                },
                "vToken": {
                    "symbol": "variableDebtEthUSDC",
                    "name": "Aave Ethereum Variable Debt USDC",
                    "address": "0x72E95b8931767C79bA4EeE721354d6E99a61D004",
                    "decimals": 6,
                },
            },
            {
                "underlyingToken": {
                    "symbol": "WETH",
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "decimals": 18,
                },
                "aToken": {
                    "symbol": "aEthWETH",
                    "name": "Aave Ethereum WETH",
                    "address": "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8",
                    "decimals": 18,
                },
                "vToken": {
                    "symbol": "variableDebtEthWETH",
                    "name": "Aave Ethereum Variable Debt WETH",
                    "address": "0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE",
                    "decimals": 18,
                },
            },
        ],
    },
    # Ethereum Lido sub-market (same chainId, distinct symbols)
    {
        "chain": {"chainId": 1},
        "reserves": [
            {
                "underlyingToken": {
                    "symbol": "WETH",
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "decimals": 18,
                },
                "aToken": {
                    "symbol": "aEthLidoWETH",
                    "name": "Aave Ethereum Lido WETH",
                    "address": "0xfA1fDbBD71B0aA05f5bbCb0fC4AAF4fE7C1Fe2A7",
                    "decimals": 18,
                },
                "vToken": {
                    "symbol": "variableDebtEthLidoWETH",
                    "name": "Aave Ethereum Lido Variable Debt WETH",
                    "address": "0x0000000000000000000000000000000000000123",
                    "decimals": 18,
                },
            },
        ],
    },
    # Arbitrum
    {
        "chain": {"chainId": 42161},
        "reserves": [
            {
                "underlyingToken": {
                    "symbol": "USDT",
                    "address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                    "decimals": 6,
                },
                "aToken": {
                    "symbol": "aArbUSDT",
                    "name": "Aave Arbitrum USDT",
                    "address": "0x6ab707AcA953eDAeFBc4fD23bA73294241490620",
                    "decimals": 6,
                },
                "vToken": {
                    "symbol": "variableDebtArbUSDT",
                    "name": "Aave Arbitrum Variable Debt USDT",
                    "address": "0x0000000000000000000000000000000000000456",
                    "decimals": 6,
                },
            },
        ],
    },
    # Unmapped chain (Celo) — must be silently dropped
    {
        "chain": {"chainId": 42220},
        "reserves": [
            {
                "underlyingToken": {
                    "symbol": "CELO",
                    "address": "0xdeadbeef00000000000000000000000000000000",
                    "decimals": 18,
                },
                "aToken": {
                    "symbol": "aCelCELO",
                    "name": "Aave Celo CELO",
                    "address": "0xdeadbeef00000000000000000000000000000001",
                    "decimals": 18,
                },
                "vToken": {
                    "symbol": "variableDebtCelCELO",
                    "name": "Aave Celo Variable Debt CELO",
                    "address": "0xdeadbeef00000000000000000000000000000002",
                    "decimals": 18,
                },
            },
        ],
    },
    # Malformed entries in ethereum — must be skipped without raising
    {
        "chain": {"chainId": 1},
        "reserves": [
            {
                "underlyingToken": {"symbol": "BROKEN", "address": "", "decimals": 18},
                "aToken": {"symbol": "", "address": "0xabc", "decimals": 18},
                "vToken": {"symbol": "variableDebtBROKEN", "address": "", "decimals": 18},
            },
        ],
    },
]


class TestBuildIndices:
    """Tests for _build_indices."""

    def test_only_mapped_chains_indexed(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)

        assert "ethereum" in lookup._symbol_indices
        assert "arbitrum" in lookup._symbol_indices
        assert "celo" not in lookup._symbol_indices

    def test_atoken_and_vtoken_both_indexed(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)

        eth = lookup._symbol_indices["ethereum"]
        assert "AETHUSDC" in eth
        assert "VARIABLEDEBTETHUSDC" in eth
        assert "AETHWETH" in eth
        assert "VARIABLEDEBTETHWETH" in eth

    def test_atoken_token_type_flag(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)
        assert lookup._symbol_indices["ethereum"]["AETHUSDC"].token_type == "A"
        assert lookup._symbol_indices["ethereum"]["VARIABLEDEBTETHUSDC"].token_type == "V"

    def test_sub_market_symbols_coexist_with_core_on_same_chain(self):
        """Ethereum has Core + Lido markets; both sets of symbols must be present."""
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)
        eth = lookup._symbol_indices["ethereum"]
        # Core
        assert "AETHWETH" in eth
        # Lido sub-market — distinct symbol, same chain
        assert "AETHLIDOWETH" in eth
        # The two map to different addresses
        assert eth["AETHWETH"].address != eth["AETHLIDOWETH"].address

    def test_underlying_metadata_attached(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)
        meta = lookup._symbol_indices["ethereum"]["AETHUSDC"]
        assert meta.underlying_symbol == "USDC"
        assert meta.underlying_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_malformed_entry_skipped_without_raising(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)
        # The malformed reserve had empty-symbol aToken and empty-address vToken.
        # Neither should end up in the index.
        eth = lookup._symbol_indices["ethereum"]
        assert "" not in eth  # empty-symbol aToken rejected
        assert "VARIABLEDEBTBROKEN" not in eth  # empty-address vToken rejected

    def test_address_lowercased_in_index(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)
        # Input mixed-case address must be normalised to lowercase
        assert "0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c" in lookup._address_indices["ethereum"]
        assert "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c" not in lookup._address_indices["ethereum"]


class TestLookupAPI:
    """Tests for public lookup_by_symbol / lookup_by_address methods."""

    @pytest.fixture
    def loaded_lookup(self):
        lookup = AaveMarketLookup()
        lookup._build_indices(SAMPLE_MARKETS)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("aethusdc", "ethereum")
        assert meta is not None
        assert meta.symbol == "aEthUSDC"
        assert meta.decimals == 6
        assert meta.token_type == "A"

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("AARBUSDT", "ARBITRUM")
        assert meta is not None
        assert meta.chain == "arbitrum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        # aEthUSDC is on ethereum only; querying on arbitrum must miss
        meta = loaded_lookup.lookup_by_symbol("aEthUSDC", "arbitrum")
        assert meta is None

    def test_lookup_by_address_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "aEthUSDC"

    def test_lookup_unknown_chain_returns_none(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("aEthUSDC", "solana") is None
        assert loaded_lookup.lookup_by_address("0xabc", "polygon") is None


class TestDiskCache:
    """Tests for disk cache read/write."""

    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = AaveMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "aave_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_MARKETS))
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))

        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = AaveMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "aave_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_MARKETS))

        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = AaveMarketLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert isinstance(data, list)
        assert len(data) == len(SAMPLE_MARKETS)

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "aave_market_cache.json"
        cache_path.write_text('{"not":"a list"}')
        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )
        lookup = AaveMarketLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "aave_market_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )

        lookup = AaveMarketLookup()
        lookup._write_disk_cache(SAMPLE_MARKETS)

        assert cache_path.exists()
        # .tmp must be cleaned up after rename
        assert not cache_path.with_suffix(".tmp").exists()
        round_trip = json.loads(cache_path.read_text())
        assert round_trip == SAMPLE_MARKETS


class TestLoadFlow:
    """Tests for the _load orchestration."""

    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "aave_market_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_MARKETS))
        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH", cache_path
        )

        lookup = AaveMarketLookup()

        async def fail_fetch() -> None:  # pragma: no cover — asserted not-called
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices

    def test_load_sets_backoff_when_everything_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH",
            tmp_path / "missing.json",
        )
        lookup = AaveMarketLookup()

        async def empty_fetch() -> None:
            return None

        with patch.object(lookup, "_fetch_from_network", side_effect=empty_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded is False
        assert lookup._load_failed is True
        assert lookup._retry_after > time.monotonic()

    def test_retry_skipped_inside_backoff_window(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.aave_v3.gateway.market_lookup.CACHE_PATH",
            tmp_path / "missing.json",
        )
        lookup = AaveMarketLookup()
        # Simulate a prior failure with future retry_after
        lookup._load_failed = True
        lookup._retry_after = time.monotonic() + 3600

        call_count = 0

        async def tracked_fetch() -> None:
            nonlocal call_count
            call_count += 1
            return None

        with patch.object(lookup, "_fetch_from_network", side_effect=tracked_fetch):
            asyncio.run(lookup._load())

        assert call_count == 0, "Must not fire network fetch while inside backoff window"


class TestReserveTokenDataclass:
    def test_fields(self):
        meta = AaveReserveToken(
            address="0xabc",
            symbol="aEthUSDC",
            name="Aave Ethereum USDC",
            decimals=6,
            token_type="A",
            chain="ethereum",
            underlying_symbol="USDC",
            underlying_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        )
        assert meta.address == "0xabc"
        assert meta.token_type == "A"
        assert meta.underlying_symbol == "USDC"


class TestLooksLikeAaveSymbol:
    """Tests for the Aave symbol prefix predicate used by TokenService."""

    @pytest.mark.parametrize(
        "symbol",
        [
            "aEthUSDC",
            "aethusdc",  # lowercase
            "aEthLidoWETH",
            "aEthEtherFiweETH",
            "aArbUSDT",
            "aBasUSDC",
            "aOptAAVE",
            "aBnbETH",
            "aPolAAVE",
            "aAvaAAVE",
            "aGnoEURe",
            "aLinUSDC",
            "aHorRwaACRED",
            "variableDebtEthUSDC",
            "variableDebtArbUSDT",
            "variabledebtethusdc",  # lowercase
        ],
    )
    def test_recognises_aave_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_aave_symbol

        assert _looks_like_aave_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "USDC",
            "WETH",
            "AAVE",  # Aave governance token — NOT an aToken
            "stETH",
            "PT-sUSDe",
            "",
        ],
    )
    def test_rejects_non_aave_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_aave_symbol

        assert _looks_like_aave_symbol(symbol) is False
