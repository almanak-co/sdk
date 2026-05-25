"""Tests for the Yearn vault lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.yearn.gateway.vault_lookup import (
    CACHE_TTL_SECONDS,
    YearnVaultLookup,
    YearnVaultToken,
)

SAMPLE_VAULTS = [
    # Ethereum v3
    {
        "address": "0xBe53A109B494E5c9f97b9Cd39Fe969EE68BF4C31",
        "symbol": "yvUSDC-1",
        "name": "USDC yVault",
        "decimals": 6,
        "chainID": 1,
        "kind": "Multi Strategy",
        "version": "3.0.2",
        "category": "Stablecoin",
        "token": {
            "symbol": "USDC",
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
        },
    },
    # Ethereum v2 legacy (Curve factory vault)
    {
        "address": "0x03eaE26089A8c28DDC4c81d65bEfE014C793A60d",
        "symbol": "yvCurve-stETH-frxETH-f",
        "name": "Curve stETH-frxETH Factory yVault",
        "decimals": 18,
        "chainID": 1,
        "kind": "Legacy",
        "version": "0.4.6",
        "category": "Curve",
        "token": {
            "symbol": "st-frxETH-f",
            "address": "0x4d9f9D15101EEC665F77210cB999639f760F831E",
            "decimals": 18,
        },
    },
    # Arbitrum
    {
        "address": "0x1111111111111111111111111111111111111111",
        "symbol": "yvUSDCe",
        "name": "USDC.e yVault",
        "decimals": 6,
        "chainID": 42161,
        "kind": "Single Strategy",
        "version": "3.0.4",
        "category": "Stablecoin",
        "token": {
            "symbol": "USDC.e",
            "address": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            "decimals": 6,
        },
    },
    # Gnosis (small chain, 1 vault)
    {
        "address": "0x2222222222222222222222222222222222222222",
        "symbol": "yvDAI-gno",
        "name": "DAI yVault (Gnosis)",
        "decimals": 18,
        "chainID": 100,
        "kind": "Single Strategy",
        "version": "3.0.2",
        "category": "Stablecoin",
        "token": {
            "symbol": "DAI",
            "address": "0x0",
            "decimals": 18,
        },
    },
    # Unmapped chain (Katana 747474) — must be silently dropped
    {
        "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "symbol": "yvKatanaETH",
        "name": "Katana yVault",
        "decimals": 18,
        "chainID": 747474,
        "kind": "Single Strategy",
        "version": "3.0.2",
        "token": {"symbol": "ETH", "address": "0x0", "decimals": 18},
    },
    # Malformed — empty symbol
    {
        "address": "0x3333333333333333333333333333333333333333",
        "symbol": "",
        "chainID": 1,
        "kind": "Legacy",
    },
]


class TestBuildIndices:
    """Tests for _build_indices."""

    def test_mapped_chains_indexed(self):
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert "ethereum" in lookup._symbol_indices
        assert "arbitrum" in lookup._symbol_indices
        # Gnosis (chain id 100) used to be indexed but is now dropped —
        # the gateway's ``ALLOWED_CHAINS`` and ``Chain`` enum don't
        # include Gnosis, so any Gnosis request is rejected upstream
        # before it reaches this lookup. The sample still carries a
        # Gnosis vault to prove it's filtered out here.
        assert "gnosis" not in lookup._symbol_indices

    def test_unmapped_chain_skipped(self):
        """Katana (747474) is not in our chain map; must be dropped."""
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        # No chain_idx should contain the Katana entry
        for chain_idx in lookup._symbol_indices.values():
            assert "YVKATANAETH" not in chain_idx

    def test_v2_and_v3_both_indexed(self):
        """Legacy (v2) and v3 vaults both register."""
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        eth = lookup._symbol_indices["ethereum"]
        assert "YVUSDC-1" in eth  # v3 (3.0.2)
        assert "YVCURVE-STETH-FRXETH-F" in eth  # Legacy (0.4.6)

    def test_decimals_come_from_vault_not_underlying(self):
        """Unlike Beefy/Morpho, Yearn provides vault decimals directly."""
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        # yvUSDC-1 vault has decimals=6 (same as USDC)
        # yvCurve-stETH-frxETH-f vault has decimals=18
        assert lookup._symbol_indices["ethereum"]["YVUSDC-1"].decimals == 6
        assert lookup._symbol_indices["ethereum"]["YVCURVE-STETH-FRXETH-F"].decimals == 18

    def test_category_and_version_attached(self):
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        meta = lookup._symbol_indices["ethereum"]["YVUSDC-1"]
        assert meta.category == "Stablecoin"
        assert meta.version == "3.0.2"
        assert meta.kind == "Multi Strategy"

    def test_underlying_metadata_attached(self):
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        meta = lookup._symbol_indices["ethereum"]["YVUSDC-1"]
        assert meta.underlying_symbol == "USDC"
        assert meta.underlying_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_malformed_entry_skipped_without_raising(self):
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        for chain_idx in lookup._symbol_indices.values():
            assert "" not in chain_idx

    def test_address_lowercased_in_index(self):
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert "0xbe53a109b494e5c9f97b9cd39fe969ee68bf4c31" in lookup._address_indices["ethereum"]
        assert "0xBe53A109B494E5c9f97b9Cd39Fe969EE68BF4C31" not in lookup._address_indices["ethereum"]


class TestLookupAPI:
    """Tests for public lookup_by_symbol / lookup_by_address methods."""

    @pytest.fixture
    def loaded_lookup(self):
        lookup = YearnVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("yvusdc-1", "ethereum")
        assert meta is not None
        assert meta.symbol == "yvUSDC-1"

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("yvUSDCe", "ARBITRUM")
        assert meta is not None
        assert meta.chain == "arbitrum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("yvUSDC-1", "arbitrum") is None

    def test_lookup_by_address(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0xBe53A109B494E5c9f97b9Cd39Fe969EE68BF4C31", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "yvUSDC-1"

    def test_lookup_unknown_chain_returns_none(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("yvUSDC-1", "solana") is None


class TestDiskCache:
    """Tests for disk cache read/write."""

    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.yearn.gateway.vault_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = YearnVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "yearn_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))

        monkeypatch.setattr(
            "almanak.connectors.yearn.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = YearnVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "yearn_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        monkeypatch.setattr(
            "almanak.connectors.yearn.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = YearnVaultLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert isinstance(data, list)

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "yearn_vault_cache.json"
        cache_path.write_text('{"not":"a list"}')
        monkeypatch.setattr(
            "almanak.connectors.yearn.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = YearnVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "yearn_vault_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.yearn.gateway.vault_lookup.CACHE_PATH", cache_path
        )

        lookup = YearnVaultLookup()
        lookup._write_disk_cache(SAMPLE_VAULTS)

        assert cache_path.exists()
        assert not cache_path.with_suffix(".tmp").exists()
        round_trip = json.loads(cache_path.read_text())
        assert round_trip == SAMPLE_VAULTS


class TestLoadFlow:
    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "yearn_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        monkeypatch.setattr(
            "almanak.connectors.yearn.gateway.vault_lookup.CACHE_PATH", cache_path
        )

        lookup = YearnVaultLookup()

        async def fail_fetch() -> None:  # pragma: no cover — asserted not-called
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices


class TestVaultTokenDataclass:
    def test_fields(self):
        meta = YearnVaultToken(
            address="0xabc",
            symbol="yvUSDC-1",
            name="USDC yVault",
            decimals=6,
            chain="ethereum",
            underlying_symbol="USDC",
            underlying_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            category="Stablecoin",
            version="3.0.2",
            kind="Multi Strategy",
        )
        assert meta.symbol == "yvUSDC-1"
        assert meta.category == "Stablecoin"


class TestLooksLikeYearnSymbol:
    """Tests for the Yearn prefix predicate used by TokenService."""

    @pytest.mark.parametrize(
        "symbol",
        [
            "yvUSDC",
            "yvDAI-1",
            "yvCurve-stETH-frxETH-f",
            "YVUSDC",  # uppercase
            "yvusdc",  # lowercase
        ],
    )
    def test_recognises_yearn_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_yearn_symbol

        assert _looks_like_yearn_symbol(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "USDC",
            "WETH",
            "aEthUSDC",
            "mooAaveWBTC",
            "PT-sUSDe",
            "cUSDCv3",
            "gtUSDC",
            "YFII",  # not yvToken (YFI copycat)
            "",
        ],
    )
    def test_rejects_non_yearn_symbols(self, symbol):
        from almanak.gateway.services.token_service import _looks_like_yearn_symbol

        assert _looks_like_yearn_symbol(symbol) is False
