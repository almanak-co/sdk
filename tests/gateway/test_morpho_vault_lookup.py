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

        # Only the connector's advertised Ethereum and Base chains are mapped.
        assert "ethereum" in lookup._symbol_indices
        assert "base" in lookup._symbol_indices
        assert "arbitrum" not in lookup._symbol_indices
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

    def test_share_decimals_are_at_least_18_never_the_underlying(self):
        """A USDC (6-dec) vault mints 18-decimal shares — both Morpho generations add an offset to reach 18.

        Copying ``asset.decimals`` onto the share token (the previous behaviour)
        published 6 for an 18-decimal share, so a raw balance of 10**18 read as a
        trillion shares to every resolver-scaled caller.
        """
        lookup = MorphoVaultLookup()
        lookup._build_indices(SAMPLE_VAULTS)
        meta = lookup.lookup_by_symbol("gtUSDC", "ethereum")
        assert meta is not None
        assert meta.decimals == 18
        assert meta.underlying_decimals == 6

    def test_share_decimals_follow_an_18_plus_underlying(self):
        from almanak.connectors.morpho_vault.gateway.vault_lookup import share_decimals_for

        assert share_decimals_for(6) == 18
        assert share_decimals_for(18) == 18
        assert share_decimals_for(24) == 24

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
        assert meta.decimals == 18  # share decimals: max(18, underlying); the underlying is 6

    def test_lookup_by_symbol_chain_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_symbol("GTUSDC", "ETHEREUM")
        assert meta is not None
        assert meta.chain == "ethereum"

    def test_lookup_by_symbol_wrong_chain_misses(self, loaded_lookup):
        # gtUSDC is on ethereum only; querying on arbitrum must miss
        assert loaded_lookup.lookup_by_symbol("gtUSDC", "arbitrum") is None

    def test_lookup_by_address_case_insensitive(self, loaded_lookup):
        meta = loaded_lookup.lookup_by_address("0xdd0f28e19C1780eb6396170735D45153D261490d", "ethereum")
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

        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path)
        lookup = MorphoVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_read_disk_cache_returns_fresh_data(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        cache_path.write_text(json.dumps(SAMPLE_VAULTS))

        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path)
        lookup = MorphoVaultLookup()
        data = lookup._read_disk_cache()
        assert data is not None
        assert isinstance(data, list)
        assert len(data) == len(SAMPLE_VAULTS)

    def test_read_disk_cache_rejects_malformed_json(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        cache_path.write_text('{"not":"a list"}')
        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path)
        lookup = MorphoVaultLookup()
        assert lookup._read_disk_cache() is None

    def test_write_disk_cache_atomic(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "morpho_vault_cache.json"
        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path)

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
        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", cache_path)

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


# Morpho Vault V2 + schema-drift regression (session be5a3567).
#
# The Morpho API renamed the vault filter ``whitelisted`` -> ``listed``; the
# old field is a hard GraphQL validation error, so a query still using it
# indexed ZERO vaults (every vault symbol resolved ``not_found`` on staging
# and prod). V2 vaults additionally live under ``vaultV2s``.

from almanak.connectors.morpho_vault.gateway.vault_lookup import (  # noqa: E402
    _MORPHO_VAULT_QUERIES,
    _MORPHO_VAULTS_QUERY,
    _MORPHO_VAULTS_V2_QUERY,
    VAULT_VERSION_V1,
    VAULT_VERSION_V2,
    tag_vault_payload,
)

STEAK_V2_BASE = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"  # Steakhouse Prime USDC (Vault V2)
STEAK_V1_BASE = "0xbeef010f9cb27031ad51e3333f9af9c6b1228183"  # Steakhouse USDC (MetaMorpho v1)


def _base_usdc_asset() -> dict:
    return {"symbol": "USDC", "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "decimals": 6}


class TestQuerySchema:
    def test_queries_use_listed_filter_not_whitelisted(self):
        for query in (_MORPHO_VAULTS_QUERY, _MORPHO_VAULTS_V2_QUERY):
            assert "listed: true" in query
            assert "whitelisted" not in query

    def test_v2_query_targets_vaultv2s_root_field(self):
        assert "vaultV2s(" in _MORPHO_VAULTS_V2_QUERY
        assert "vaults(" in _MORPHO_VAULTS_QUERY
        assert [(root, version) for root, _q, version in _MORPHO_VAULT_QUERIES] == [
            ("vaults", VAULT_VERSION_V1),
            ("vaultV2s", VAULT_VERSION_V2),
        ]


class TestTagVaultPayload:
    def test_tags_items_with_version(self):
        body = {"data": {"vaultV2s": {"items": [{"address": STEAK_V2_BASE, "symbol": "steakUSDC"}]}}}
        tagged = tag_vault_payload(body, "vaultV2s", VAULT_VERSION_V2)
        assert tagged == [{"address": STEAK_V2_BASE, "symbol": "steakUSDC", "vault_version": "v2"}]

    def test_graphql_errors_yield_empty_list(self, caplog):
        body = {
            "errors": [{"message": 'Field "whitelisted" is not defined by type "VaultFilters". Did you mean "listed"?'}]
        }
        with caplog.at_level("WARNING"):
            assert tag_vault_payload(body, "vaults", VAULT_VERSION_V1) == []
        assert "whitelisted" in caplog.text  # the API's own message reaches the gateway log

    def test_non_dict_body_and_missing_root_yield_empty(self):
        assert tag_vault_payload(["not", "a", "dict"], "vaults", VAULT_VERSION_V1) == []
        assert tag_vault_payload({"data": {}}, "vaults", VAULT_VERSION_V1) == []
        assert tag_vault_payload({"data": {"vaults": {"items": "nope"}}}, "vaults", VAULT_VERSION_V1) == []


class TestV2Indexing:
    def _lookup(self, payload):
        lookup = MorphoVaultLookup()
        lookup._build_indices(payload)
        return lookup

    def test_v2_vault_indexed_with_version_by_address(self):
        lookup = self._lookup(
            [
                {
                    "address": STEAK_V2_BASE,
                    "name": "Steakhouse Prime USDC",
                    "symbol": "steakUSDC",
                    "chain": {"id": 8453},
                    "asset": _base_usdc_asset(),
                    "vault_version": "v2",
                }
            ]
        )
        meta = lookup.lookup_by_address(STEAK_V2_BASE, "base")
        assert meta is not None
        assert meta.vault_version == VAULT_VERSION_V2
        assert meta.underlying_symbol == "USDC"

    def test_untagged_entry_defaults_to_v1(self):
        # A disk cache written before the V2 query existed has no tag.
        lookup = self._lookup(
            [
                {
                    "address": STEAK_V1_BASE,
                    "name": "Steakhouse USDC",
                    "symbol": "steakUSDC",
                    "chain": {"id": 8453},
                    "asset": _base_usdc_asset(),
                }
            ]
        )
        meta = lookup.lookup_by_address(STEAK_V1_BASE, "base")
        assert meta is not None
        assert meta.vault_version == VAULT_VERSION_V1

    def test_symbol_collision_across_generations_keeps_v1_but_addresses_stay_exact(self):
        payload = [
            {
                "address": STEAK_V1_BASE,
                "name": "Steakhouse USDC",
                "symbol": "steakUSDC",
                "chain": {"id": 8453},
                "asset": _base_usdc_asset(),
                "vault_version": "v1",
            },
            {
                "address": STEAK_V2_BASE,
                "name": "Steakhouse Prime USDC",
                "symbol": "steakUSDC",
                "chain": {"id": 8453},
                "asset": _base_usdc_asset(),
                "vault_version": "v2",
            },
        ]
        lookup = self._lookup(payload)
        by_symbol = lookup.lookup_by_symbol("steakUSDC", "base")
        assert by_symbol is not None and by_symbol.address == STEAK_V1_BASE  # v1 fetched first wins
        assert lookup.lookup_by_address(STEAK_V2_BASE, "base").vault_version == VAULT_VERSION_V2
        assert lookup.lookup_by_address(STEAK_V1_BASE, "base").vault_version == VAULT_VERSION_V1


class TestFetchMergesGenerations:
    def test_fetch_merges_v1_and_v2_and_tolerates_one_failing_root(self, tmp_path, monkeypatch):
        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", tmp_path / "cache.json")
        lookup = MorphoVaultLookup()
        lookup._cache_path = tmp_path / "cache.json"

        async def fake_generation(session, root_field, query, version):
            if root_field == "vaults":
                return None  # the v1 query failed (HTTP error / GraphQL errors / transport)
            return tag_vault_payload(
                {"data": {"vaultV2s": {"items": [{"address": STEAK_V2_BASE, "symbol": "steakUSDC"}]}}},
                root_field,
                version,
            )

        with patch.object(lookup, "_fetch_generation", side_effect=fake_generation):
            merged = asyncio.run(lookup._fetch_from_network())

        # The surviving generation is served in memory ...
        assert merged == [{"address": STEAK_V2_BASE, "symbol": "steakUSDC", "vault_version": "v2"}]
        # ... but a PARTIAL index is never persisted: a cached v2-only list would
        # hide every v1 symbol/address for the 24h TTL, long after the API recovered.
        assert not (tmp_path / "cache.json").exists()

    def test_malformed_successful_payload_is_a_failed_generation_and_withholds_the_cache(self, tmp_path, monkeypatch):
        """A 200 with ``{"data": {}}`` (or a non-list ``items``) must not count as an empty answer."""
        import asyncio as _asyncio
        from contextlib import asynccontextmanager

        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", tmp_path / "cache.json")
        lookup = MorphoVaultLookup()
        lookup._cache_path = tmp_path / "cache.json"

        class _Resp:
            status = 200

            def __init__(self, payload):
                self._payload = payload

            async def json(self, content_type=None):
                return self._payload

        class _Session:
            def __init__(self, payloads):
                self._payloads = payloads

            @asynccontextmanager
            async def post(self, url, json, timeout):
                yield _Resp(self._payloads[json["query"]])

        from almanak.connectors.morpho_vault.gateway.vault_lookup import _MORPHO_VAULT_QUERIES

        (v1_root, v1_query, _), (v2_root, v2_query, _) = _MORPHO_VAULT_QUERIES
        session = _Session(
            {
                v1_query: {"data": {}},  # structurally malformed "success"
                v2_query: {"data": {v2_root: {"items": [{"address": STEAK_V2_BASE, "symbol": "steakUSDC"}]}}},
            }
        )
        assert _asyncio.run(lookup._fetch_generation(session, v1_root, v1_query, "v1")) is None
        assert _asyncio.run(lookup._fetch_generation(session, v2_root, v2_query, "v2")) == [
            {"address": STEAK_V2_BASE, "symbol": "steakUSDC", "vault_version": "v2"}
        ]
        non_list = _Session({v1_query: {"data": {v1_root: {"items": {"oops": 1}}}}})
        assert _asyncio.run(lookup._fetch_generation(non_list, v1_root, v1_query, "v1")) is None

        real_fetch_generation = lookup._fetch_generation  # bound before patching, else the fake recurses

        async def fake_generation(session_, root_field, query, version):
            return await real_fetch_generation(session, root_field, query, version)

        with patch.object(lookup, "_fetch_generation", side_effect=fake_generation):
            merged = _asyncio.run(lookup._fetch_from_network())
        assert merged == [{"address": STEAK_V2_BASE, "symbol": "steakUSDC", "vault_version": "v2"}]
        assert not (tmp_path / "cache.json").exists()

    def test_fetch_writes_cache_only_when_both_generations_answer(self, tmp_path, monkeypatch):
        monkeypatch.setattr("almanak.connectors.morpho_vault.gateway.vault_lookup.CACHE_PATH", tmp_path / "cache.json")
        lookup = MorphoVaultLookup()
        lookup._cache_path = tmp_path / "cache.json"

        async def fake_generation(session, root_field, query, version):
            if root_field == "vaults":
                return []  # answered, genuinely empty
            return tag_vault_payload(
                {"data": {"vaultV2s": {"items": [{"address": STEAK_V2_BASE, "symbol": "steakUSDC"}]}}},
                root_field,
                version,
            )

        with patch.object(lookup, "_fetch_generation", side_effect=fake_generation):
            merged = asyncio.run(lookup._fetch_from_network())

        assert merged == [{"address": STEAK_V2_BASE, "symbol": "steakUSDC", "vault_version": "v2"}]
        assert json.loads((tmp_path / "cache.json").read_text()) == merged

    def test_fetch_returns_none_when_both_roots_fail(self, tmp_path, monkeypatch):
        lookup = MorphoVaultLookup()
        lookup._cache_path = tmp_path / "cache.json"

        async def nothing(session, root_field, query, version):
            return None

        with patch.object(lookup, "_fetch_generation", side_effect=nothing):
            assert asyncio.run(lookup._fetch_from_network()) is None
        assert not (tmp_path / "cache.json").exists()


class TestResolverPublishesShareDecimals:
    """The gateway resolver must publish the SHARE token's decimals, not the underlying's."""

    def test_resolved_token_carries_share_decimals_with_a_6_decimal_underlying(self):
        from almanak.connectors.morpho_vault.gateway.vault_lookup import MorphoVaultToken, share_decimals_for
        from almanak.gateway.services.token_service import TokenServiceServicer

        meta = MorphoVaultToken(
            address="0xbeef0e0834849acc03f0089f01f4f1eeb06873c9",
            symbol="steakUSDC",
            name="Steakhouse Prime USDC",
            decimals=share_decimals_for(6),
            chain="base",
            underlying_symbol="USDC",
            underlying_address="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            underlying_decimals=6,
            vault_version="v2",
        )
        resolved = TokenServiceServicer._build_resolved_from_morpho(None, meta)  # the builder does not touch self
        assert resolved.decimals == 18
        assert resolved.symbol == "steakUSDC"
        assert resolved.is_verified is True
        # 10**18 raw shares is ONE share, not a trillion.
        assert 10**18 / 10**resolved.decimals == 1


class TestMissingUnderlyingDecimalsIsNotGuessed:
    """Empty != Zero: an entry whose payload lacks the underlying's decimals is skipped, never defaulted."""

    def test_entry_without_asset_decimals_is_skipped_loudly(self, caplog):
        import copy
        import logging

        entry = copy.deepcopy(SAMPLE_VAULTS[0])
        entry["asset"].pop("decimals")
        lookup = MorphoVaultLookup()
        with caplog.at_level(logging.WARNING):
            lookup._build_indices([entry])
        assert lookup.lookup_by_symbol(entry["symbol"], "ethereum") is None
        assert any("underlying decimals missing" in rec.getMessage() for rec in caplog.records)

    def test_boolean_decimals_is_treated_as_missing(self):
        import copy

        entry = copy.deepcopy(SAMPLE_VAULTS[0])
        entry["asset"]["decimals"] = True
        lookup = MorphoVaultLookup()
        lookup._build_indices([entry])
        assert lookup.lookup_by_symbol(entry["symbol"], "ethereum") is None
