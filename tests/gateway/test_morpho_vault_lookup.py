"""Tests for the Morpho vault lookup service."""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from almanak.connectors.morpho_vault.gateway.vault_lookup import (
    CACHE_TTL_SECONDS,
    MorphoVaultLookup,
    MorphoVaultToken,
)

SAMPLE_VAULTS = [
    # Ethereum — Gauntlet USDC Prime
    {
        "address": "0xdd0f28e19C1780eb6396170735D45153D261490d",
        "name": "Gauntlet USDC Prime",
        "symbol": "gtUSDC",
        "chain": {"id": 1},
        "asset": {
            "symbol": "USDC",
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
        },
    },
    # Ethereum — Spark Blue Chip USDC
    {
        "address": "0xfeaC08ffA38d95ec5Ed7C46c933C8891a44C5F26",
        "name": "Spark Blue Chip USDC Vault",
        "symbol": "sparkUSDCbc",
        "chain": {"id": 1},
        "asset": {
            "symbol": "USDC",
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "decimals": 6,
        },
    },
    # Base — Gauntlet-curated vault on Base
    {
        "address": "0x1111111111111111111111111111111111111111",
        "name": "Gauntlet USDC Base",
        "symbol": "gtUSDCbase",
        "chain": {"id": 8453},
        "asset": {
            "symbol": "USDC",
            "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "decimals": 6,
        },
    },
    # Arbitrum
    {
        "address": "0x2222222222222222222222222222222222222222",
        "name": "kpk USDC Yield",
        "symbol": "kpk_USDC_Yield",
        "chain": {"id": 42161},
        "asset": {
            "symbol": "USDC",
            "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "decimals": 6,
        },
    },
    # Unmapped chain (HyperEVM 999) — must be silently dropped
    {
        "address": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "name": "HyperEVM vault",
        "symbol": "hyperVault",
        "chain": {"id": 999},
        "asset": {
            "symbol": "USDC",
            "address": "0xb88339CB7199b77E23DB6E890353E22632Ba630f",
            "decimals": 6,
        },
    },
    # Malformed entry — empty symbol, must be skipped without raising
    {
        "address": "0x3333333333333333333333333333333333333333",
        "name": "broken",
        "symbol": "",
        "chain": {"id": 1},
        "asset": {"symbol": "USDC", "address": "0x0", "decimals": 6},
    },
]


class TestBuildIndices:
    """Tests for _build_indices."""

    def test_only_mapped_chains_indexed(self):
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)

        # Ethereum (1), Base (8453), Arbitrum (42161) → mapped
        assert "ethereum" in lookup._symbol_indices
        assert "base" in lookup._symbol_indices
        assert "arbitrum" in lookup._symbol_indices
        # HyperEVM 999 → unmapped
        assert "hyperEVM" not in lookup._symbol_indices
        # 999 must not leak through as a stringified chain id either
        assert "999" not in lookup._symbol_indices

    def test_vault_indexed_by_symbol_on_correct_chain(self):
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)

        eth = lookup._symbol_indices["ethereum"]
        assert "GTUSDC" in eth
        assert "SPARKUSDCBC" in eth
        assert eth["GTUSDC"].chain == "ethereum"
        assert eth["GTUSDC"].name == "Gauntlet USDC Prime"

    def test_decimals_inherited_from_underlying(self):
        """MetaMorpho vaults are ERC4626 and inherit underlying decimals."""
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert lookup._symbol_indices["ethereum"]["GTUSDC"].decimals == 6

    def test_underlying_metadata_attached(self):
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        meta = lookup._symbol_indices["ethereum"]["GTUSDC"]
        assert meta.underlying_symbol == "USDC"
        assert meta.underlying_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_malformed_entry_skipped_without_raising(self):
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        # The empty-symbol broken entry must not be indexed
        eth = lookup._symbol_indices["ethereum"]
        assert "" not in eth
        assert "0x3333333333333333333333333333333333333333" not in lookup._address_indices["ethereum"]

    def test_address_lowercased_in_index(self):
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert "0xdd0f28e19c1780eb6396170735d45153d261490d" in lookup._address_indices["ethereum"]
        assert "0xdd0f28e19C1780eb6396170735D45153D261490d" not in lookup._address_indices["ethereum"]

    def test_symbol_collision_across_chains_resolved_per_chain(self):
        """``gtUSDC`` on ethereum vs ``gtUSDCbase`` on base are distinct; but
        per-chain scoping also means if two vaults happened to share a symbol
        on different chains, both would be addressable."""
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        assert lookup.lookup_by_symbol("gtUSDC", "ethereum") is not None
        assert lookup.lookup_by_symbol("gtUSDC", "base") is None
        assert lookup.lookup_by_symbol("gtUSDCbase", "base") is not None


class TestLookupAPI:
    """Tests for public lookup_by_symbol / lookup_by_address methods."""

    @pytest.fixture
    def loaded_lookup(self):
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        return lookup

    def test_lookup_by_symbol_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("gtusdc", "ethereum")
        assert meta is not None
        assert meta.symbol == "gtUSDC"
        assert meta.decimals == 6

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("GTUSDC", "ETHEREUM")
        assert meta is not None
        assert meta.chain == "ethereum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        # gtUSDC is on ethereum only; querying on arbitrum must miss
        assert loaded_lookup.lookup_by_symbol("gtUSDC", "arbitrum") is None

    def test_lookup_by_address_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address(
            "0xdd0f28e19C1780eb6396170735D45153D261490d", "ethereum"
        )
        assert meta is not None
        assert meta.symbol == "gtUSDC"

    def test_lookup_unknown_chain_returns_none(self, loaded_lookup):
        assert loaded_lookup.lookup_by_symbol("gtUSDC", "solana") is None
        assert loaded_lookup.lookup_by_address("0x0", "polygon") is None


class TestDiskCache:
    """Tests for disk cache read/write."""

    def test_read_disk_cache_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH",
            tmp_path / "nope.json",
        )
        lookup = MorphoVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_none_when_expired(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        stale_mtime = time.time() - CACHE_TTL_SECONDS - 60
        import os

        os.utime(cache_path, (stale_mtime, stale_mtime))

        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = MorphoVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))

        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = MorphoVaultLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert isinstance(data, list)
        assert len(data) == len(SAMPLE_VAULTS)

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        cache_path.write_text('{"not":"a list"}')
        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path
        )
        lookup = MorphoVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path
        )

        lookup = MorphoVaultLookup()
        lookup._write_disk_cache(SAMPLE_VAULTS)

        assert cache_path.exists()
        # .tmp must be cleaned up after rename
        assert not cache_path.with_suffix(".tmp").exists()
        round_trip = json.loads(cache_path.read_text())
        assert round_trip == SAMPLE_VAULTS


class TestLoadFlow:
    """Tests for the _load orchestration."""

    def test_load_uses_disk_cache_when_fresh(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))
        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path
        )

        lookup = MorphoVaultLookup()

        async def fail_fetch() -> None:  # pragma: no cover — asserted not-called
            raise AssertionError("Network fetch must not fire when disk cache is fresh")

        with patch.object(lookup, "_fetch_from_network", side_effect=fail_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded
        assert "ethereum" in lookup._symbol_indices

    def test_load_sets_backoff_when_everything_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH",
            tmp_path / "missing.json",
        )
        lookup = MorphoVaultLookup()

        async def empty_fetch() -> None:
            return None

        with patch.object(lookup, "_fetch_from_network", side_effect=empty_fetch):
            asyncio.run(lookup._load())

        assert lookup.is_loaded is False
        assert lookup._load_failed is True
        assert lookup._retry_after > time.monotonic()

    def test_retry_skipped_inside_backoff_window(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH",
            tmp_path / "missing.json",
        )
        lookup = MorphoVaultLookup()
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


class TestVaultTokenDataclass:
    def test_fields(self):
        meta = MorphoVaultToken(
            address="0xabc",
            symbol="gtUSDC",
            name="Gauntlet USDC Prime",
            decimals=6,
            chain="ethereum",
            underlying_symbol="USDC",
            underlying_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        )
        assert meta.address == "0xabc"
        assert meta.underlying_symbol == "USDC"
