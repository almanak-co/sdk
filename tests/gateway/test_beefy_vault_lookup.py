"""Tests for the Beefy vault lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.beefy.gateway.vault_lookup import (
    CACHE_TTL_SECONDS,
    BeefyVaultLookup,
    BeefyVaultToken,
)


SAMPLE_VAULTS = [
    # Ethereum active vault
    {
        "id": "curve-usdc-usdf",
        "name": "USDf/USDC",
        "token": "USDC/USDf",
        "tokenAddress": "0x72310DAAed61321b02B08A547150c07522c6a976",
        "tokenDecimals": 18,
        "earnedToken": "mooCurveUSDC-USDf",
        "earnedTokenAddress": "0x0014E0be19De3118b5b29842dd1696a2A98EB9Db",
        "chain": "ethereum",
        "status": "active",
        "platformId": "convex",
    },
    # Arbitrum active
    {
        "id": "aavev3-arb-wbtc",
        "name": "WBTC",
        "token": "WBTC",
        "tokenAddress": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "tokenDecimals": 8,
        "earnedToken": "mooAaveV3WBTCArb",
        "earnedTokenAddress": "0x1111111111111111111111111111111111111111",
        "chain": "arbitrum",
        "status": "active",
        "platformId": "aave",
    },
    # Avalanche (Beefy calls it 'avax'; we map to 'avalanche')
    {
        "id": "aave-avax-usdt",
        "name": "USDT",
        "token": "USDT",
        "tokenAddress": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "tokenDecimals": 6,
        "earnedToken": "mooAaveUSDTav",
        "earnedTokenAddress": "0x2222222222222222222222222222222222222222",
        "chain": "avax",
        "status": "active",
        "platformId": "aave",
    },
    # EOL vault (must be skipped)
    {
        "id": "dead-curve-tricrv",
        "name": "Tri-CRV",
        "token": "3CRV",
        "tokenAddress": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
        "tokenDecimals": 18,
        "earnedToken": "mooCurveTricrypto",
        "earnedTokenAddress": "0x3333333333333333333333333333333333333333",
        "chain": "ethereum",
        "status": "eol",
        "platformId": "convex",
    },
    # Paused vault (must be skipped)
    {
        "id": "paused-vault",
        "name": "Paused",
        "token": "ETH",
        "tokenAddress": "0x0",
        "tokenDecimals": 18,
        "earnedToken": "mooPausedVault",
        "earnedTokenAddress": "0x4444444444444444444444444444444444444444",
        "chain": "ethereum",
        "status": "paused",
        "platformId": "test",
    },
    # Unmapped chain (Monad) — must be silently dropped
    {
        "id": "monad-vault",
        "name": "Monad",
        "token": "USDC",
        "tokenAddress": "0x0",
        "tokenDecimals": 6,
        "earnedToken": "mooMonadUSDC",
        "earnedTokenAddress": "0x5555555555555555555555555555555555555555",
        "chain": "monad",
        "status": "active",
        "platformId": "test",
    },
    # Malformed — missing earnedToken symbol
    {
        "id": "broken",
        "chain": "ethereum",
        "status": "active",
        "earnedToken": "",
        "earnedTokenAddress": "0x6666666666666666666666666666666666666666",
    },
]


class TestBuildIndices:
    """Tests for _build_indices."""

    def test_active_vaults_indexed(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        eth = lookup._symbol_indices["ethereum"]
        assert "MOOCURVEUSDC-USDF" in eth

    def test_eol_vaults_skipped(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        eth = lookup._symbol_indices.get("ethereum", {})
        assert "MOOCURVETRICRYPTO" not in eth

    def test_paused_vaults_skipped(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        eth = lookup._symbol_indices.get("ethereum", {})
        assert "MOOPAUSEDVAULT" not in eth

    def test_avax_chain_normalised_to_avalanche(self):
        """Beefy uses ``avax``; gateway uses ``avalanche``."""
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert "avax" not in lookup._symbol_indices
        assert "avalanche" in lookup._symbol_indices
        assert "MOOAAVEUSDTAV" in lookup._symbol_indices["avalanche"]

    def test_unmapped_chain_skipped(self):
        """Beefy supports monad, sonic, etc. — gateway doesn't speak them yet."""
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert "monad" not in lookup._symbol_indices

    def test_decimals_inherit_from_underlying(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert lookup._symbol_indices["ethereum"]["MOOCURVEUSDC-USDF"].decimals == 18
        assert lookup._symbol_indices["arbitrum"]["MOOAAVEV3WBTCARB"].decimals == 8
        assert lookup._symbol_indices["avalanche"]["MOOAAVEUSDTAV"].decimals == 6

    def test_platform_and_underlying_metadata_attached(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        meta = lookup._symbol_indices["ethereum"]["MOOCURVEUSDC-USDF"]
        assert meta.platform == "convex"
        assert meta.underlying_symbol == "USDC/USDf"
        assert meta.underlying_address == "0x72310daaed61321b02b08a547150c07522c6a976"

    def test_malformed_entry_skipped_without_raising(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        # The empty-symbol broken entry must not appear
        for chain_idx in lookup._symbol_indices.values():
            assert "" not in chain_idx

    def test_address_lowercased_in_index(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        # Mixed-case input → lowercased
        assert "0x0014e0be19de3118b5b29842dd1696a2a98eb9db" in lookup._address_indices["ethereum"]
        assert "0x0014E0be19De3118b5b29842dd1696a2A98EB9Db" not in lookup._address_indices["ethereum"]


class TestLookupAPI:
    """Tests for public lookup_by_symbol / lookup_by_address methods."""

    @pytest.fixture
    def loaded_lookup(self):
        lookup = BeefyVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("moocurveusdc-usdf", "ethereum")
        assert meta is not None
        assert meta.symbol == "mooCurveUSDC-USDf"

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("mooAaveV3WBTCArb", "ARBITRUM")
        assert meta is not None
        assert meta.chain == "arbitrum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        # The Curve USDC-USDf vault is on ethereum, not arbitrum
        assert loaded_lookup.lookup_by_symbol("mooCurveUSDC-USDf", "arbitrum") is None

    def test_lookup_by_address(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0x0014E0be19De3118b5b29842dd1696a2A98EB9Db", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "mooCurveUSDC-USDf"

    def test_lookup_unknown_chain_returns_none(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("mooCurveUSDC-USDf", "solana") is None


class TestDiskCache:
    """Tests for disk cache read/write."""

    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.beefy.gateway.vault_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = BeefyVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "beefy_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))

        monkeypatch.setattr(
            "almanak.connectors.beefy.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = BeefyVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "beefy_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))

        monkeypatch.setattr(
            "almanak.connectors.beefy.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = BeefyVaultLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert isinstance(data, list)

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "beefy_vault_cache.json"
        cache_path.write_text('{"not":"a list"}')
        monkeypatch.setattr(
            "almanak.connectors.beefy.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = BeefyVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "beefy_vault_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.beefy.gateway.vault_lookup.CACHE_PATH", cache_path
        )

        lookup = BeefyVaultLookup()
        lookup._write_disk_cache(SAMPLE_VAULTS)

        assert cache_path.exists()
        assert not cache_path.with_suffix(".tmp").exists()
        round_trip = json.loads(cache_path.read_text())
        assert round_trip == SAMPLE_VAULTS


class TestLoadFlow:
    """Tests for the _load orchestration."""

    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "beefy_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        monkeypatch.setattr(
            "almanak.connectors.beefy.gateway.vault_lookup.CACHE_PATH", cache_path
        )

        lookup = BeefyVaultLookup()

        async def fail_fetch() -> None:  # pragma: no cover — asserted not-called
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices


class TestVaultTokenDataclass:
    def test_fields(self):
        meta = BeefyVaultToken(
            address="0xabc",
            symbol="mooAaveWBTC",
            name="WBTC",
            decimals=8,
            chain="arbitrum",
            underlying_symbol="WBTC",
            underlying_address="0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
            platform="aave",
        )
        assert meta.symbol == "mooAaveWBTC"
        assert meta.platform == "aave"


class TestLooksLikeBeefySymbol:
    """Tests for the Beefy prefix predicate used by TokenService."""

    @pytest.mark.parametrize(
        "symbol",
        [
            "mooCurveUSDC-USDf",
            "mooAaveV3WBTC",
            "moocurveusdc",  # lowercase
            "MooSkyUSDS_SPK",
            "MOOSOMETHING",
        ],
    )
    def test_recognises_beefy_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_beefy_symbol

        assert _looks_like_beefy_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "USDC",
            "WETH",
            "aEthUSDC",
            "PT-sUSDe",
            "cUSDCv3",
            "gtUSDC",
            "mo",  # too short
            "",
        ],
    )
    def test_rejects_non_beefy_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_beefy_symbol

        assert _looks_like_beefy_symbol(symbol) is False
