"""POOL-2 (VIB-4750) skeleton tests for PoolHistoryService.

Maps to the umbrella UAT card at ``docs/internal/uat-cards/VIB-4728.md``:
- D2.M5.a (auth + kill-switch skeleton — runs against the pure skeleton)
- D3.F11 (counter export shape — POOL-8 fills values; POOL-2 locks NAMES)

The POOL-2 acceptance scope is intentionally narrow: prove the registered-
but-default-disabled handler behaves correctly, prove the locked health()
schema is exposed from day 1, and prove the env-var kill-switch flips
behavior between UNAVAILABLE and UNIMPLEMENTED. No providers, no caching,
no real history data — those land in POOL-4 / POOL-5 / POOL-6.
"""

from __future__ import annotations

import asyncio

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import (
    PoolHistoryServiceServicer,
    _zero_health_snapshot,
)


class _MockContext:
    """Captures the (code, details) the servicer sets on the gRPC context."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _request() -> gateway_pb2.PoolHistoryRequest:
    """A syntactically valid request — never reaches a validator in POOL-2."""
    return gateway_pb2.PoolHistoryRequest(
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,  # +7d
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )


# ============================================================================
# Kill-switch: default-disabled -> UNAVAILABLE
# ============================================================================


def test_killswitch_off_returns_unavailable_with_vib_pointer():
    """ALMANAK_GATEWAY_POOL_HISTORY_ENABLED default false: every call
    returns UNAVAILABLE with a message pointing at the umbrella epic."""
    settings = GatewaySettings(pool_history_enabled=False)
    servicer = PoolHistoryServiceServicer(settings)
    ctx = _MockContext()

    response = asyncio.run(servicer.GetPoolHistory(_request(), ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "VIB-4728" in ctx.details
    assert "not yet enabled" in ctx.details
    assert response.success is False
    assert "VIB-4728" in response.error
    # Failure envelope shape (UAT card D3.F6 lock): all metadata fields are
    # in their non-stale form on success=False.
    assert response.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
    assert response.next_start_ts == 0
    assert response.finalized_only is False
    assert response.source == ""
    assert len(response.snapshots) == 0


def test_killswitch_default_is_false():
    """GatewaySettings.pool_history_enabled MUST default to false. POOL-9
    flips this; if it flips earlier by accident, this test catches it."""
    settings = GatewaySettings()
    assert settings.pool_history_enabled is False


# ============================================================================
# Kill-switch on, providers not yet wired -> UNIMPLEMENTED
# ============================================================================


def test_killswitch_on_returns_unimplemented_until_pool5():
    """ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true AND providers absent
    (POOL-2 -> POOL-5 window): handler returns UNIMPLEMENTED per gRPC."""
    settings = GatewaySettings(pool_history_enabled=True)
    servicer = PoolHistoryServiceServicer(settings)
    ctx = _MockContext()

    response = asyncio.run(servicer.GetPoolHistory(_request(), ctx))  # type: ignore[arg-type]

    assert ctx.code == grpc.StatusCode.UNIMPLEMENTED
    assert "POOL-5" in ctx.details or "VIB-4753" in ctx.details
    assert response.success is False
    # Same failure-envelope shape as the disabled case.
    assert response.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
    assert response.next_start_ts == 0
    assert response.source == ""
    assert len(response.snapshots) == 0


# ============================================================================
# health() schema lock (POOL-2 locks NAMES; POOL-8 fills VALUES)
# ============================================================================


def test_health_top_level_keys_locked():
    servicer = PoolHistoryServiceServicer(GatewaySettings())
    h = servicer.health()
    assert set(h.keys()) == {"per_rpc", "per_provider", "budget"}


def test_health_per_rpc_counter_names_locked():
    """The per-RPC counter NAMES are a stable observability contract.
    Adding / removing a key requires bumping POOL-8 acceptance AND
    the umbrella UAT card. This test is the lockdown."""
    servicer = PoolHistoryServiceServicer(GatewaySettings())
    rpc = servicer.health()["per_rpc"]
    expected_scalars = {
        "requests_total",
        "cache_hits",
        "cache_misses",
        "provider_fallback",
        "inflight_dedup_hits",
        "cache_evictions_by_entries",
        "cache_evictions_by_bytes",
        "cache_bytes_resident",
    }
    expected_dicts = {
        "truncated_by_reason",
        "errors_by_grpc_code",
        "raw_cache_entries_by_provider",
    }
    assert set(rpc.keys()) == expected_scalars | expected_dicts
    for name in expected_scalars:
        assert isinstance(rpc[name], int), f"{name} must be int"
        assert rpc[name] == 0, f"POOL-2 skeleton must initialize {name} to zero"
    for name in expected_dicts:
        assert isinstance(rpc[name], dict), f"{name} must be a dict"


def test_health_truncation_reason_keys_match_proto_enum():
    """truncated_by_reason MUST be keyed by the TruncationReason enum
    names. A regression that introduces a different key set (e.g. lowercased
    or short forms) would silently break monitoring dashboards."""
    servicer = PoolHistoryServiceServicer(GatewaySettings())
    rpc = servicer.health()["per_rpc"]
    proto_enum_names = {
        d.name for d in gateway_pb2.TruncationReason.DESCRIPTOR.values
    }
    assert set(rpc["truncated_by_reason"].keys()) == proto_enum_names


def test_health_budget_counter_names_locked():
    """Budget counter NAMES locked here. Source-of-truth for values lives
    in the POOL-5 prerequisite-spike store; health() is the READ surface."""
    servicer = PoolHistoryServiceServicer(GatewaySettings())
    budget = servicer.health()["budget"]
    assert set(budget.keys()) == {
        "the_graph_monthly_queries",
        "the_graph_monthly_budget_max",
    }


def test_health_returns_defensive_copy():
    """A caller mutating health()'s return must not affect the live counter
    store. The analytics service has historically been flaky here."""
    servicer = PoolHistoryServiceServicer(GatewaySettings())
    h1 = servicer.health()
    h1["per_rpc"]["requests_total"] = 999  # type: ignore[index]
    h2 = servicer.health()
    assert h2["per_rpc"]["requests_total"] == 0, "live store must not be mutated"


# ============================================================================
# Zero-snapshot helper is callable and matches health() at construction
# ============================================================================


def test_zero_health_snapshot_matches_fresh_servicer():
    """``_zero_health_snapshot`` is the data-only schema source-of-truth.
    A freshly-constructed servicer's health() output MUST equal it (modulo
    defensive-copy)."""
    fresh = PoolHistoryServiceServicer(GatewaySettings()).health()
    assert fresh == _zero_health_snapshot()


# ============================================================================
# No-egress smoke: pure skeleton must not depend on aiohttp / grpc.aio chans
# ============================================================================


def test_skeleton_does_not_import_aiohttp():
    """POOL-2 deliverable: 'No egress yet — pure skeleton.' The skeleton
    module itself must not pull in aiohttp / httpx / requests (the POOL-5
    providers will). AST-based scan catches both ``import aiohttp`` and
    ``from aiohttp import ClientSession`` style imports; substring fallback
    catches the Web3 provider instantiation pattern.

    Note: ``web3`` is intentionally NOT in the forbidden module set —
    AGENTS.md "Gateway boundary" explicitly allows the web3.py library for
    ABI / checksum / encoding utilities. The forbidden part is calling
    ``Web3(HTTPProvider(...))`` to instantiate a provider, which is what the
    substring assert below catches.
    """
    import ast
    import inspect

    import almanak.gateway.services.pool_history_service as mod

    src = inspect.getsource(mod)
    forbidden_modules = {"aiohttp", "httpx", "requests"}
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                assert root not in forbidden_modules, (
                    f"POOL-2 skeleton must not import {root!r}"
                )
        elif isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".")[0]
            assert root not in forbidden_modules, (
                f"POOL-2 skeleton must not import from {root!r}"
            )
    assert "Web3(HTTPProvider(" not in src, (
        "POOL-2 skeleton must not instantiate Web3 HTTP providers — "
        "egress is the gateway's job"
    )


# ============================================================================
# POOL-4 cache wiring: health() reads from live cache instances
# ============================================================================


def test_servicer_has_public_and_raw_cache_instances():
    """POOL-4 (VIB-4752): servicer owns the two cache tiers. POOL-5 will
    populate them; here we just verify they exist with the right partition
    extractor on the raw cache."""
    from almanak.gateway.services._history_cache import HistoryCache

    servicer = PoolHistoryServiceServicer(GatewaySettings())
    assert isinstance(servicer._public_cache, HistoryCache)
    assert isinstance(servicer._raw_cache, HistoryCache)
    # Public cache has no partition extractor (the 7-tuple key omits provider).
    assert servicer._public_cache._partition_extractor is None
    # Raw cache has the provider extractor — required for
    # raw_cache_entries_by_provider in health().
    assert servicer._raw_cache._partition_extractor is not None


def test_health_reads_live_cache_hits_after_put():
    """A put + get on the public cache MUST surface as cache_hits=1,
    cache_misses=0 in health(). This is the live wiring proof; POOL-5
    will exercise the same path via the dispatcher."""
    from almanak.gateway.proto import gateway_pb2 as pb
    from almanak.gateway.services._history_cache import FINALITY_FINALIZED, make_public_key

    servicer = PoolHistoryServiceServicer(GatewaySettings())
    key = make_public_key(
        chain="arbitrum",
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        protocol="uniswap_v3",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
        resolution=pb.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    servicer._public_cache.put(key, pb.PoolHistoryResponse(success=True), FINALITY_FINALIZED)
    # Cache hit: get returns the value.
    assert servicer._public_cache.get(key) is not None
    rpc = servicer.health()["per_rpc"]
    assert rpc["cache_hits"] == 1
    assert rpc["cache_misses"] == 0
    assert rpc["cache_bytes_resident"] > 0


def test_health_raw_cache_entries_by_provider_reflects_live_puts():
    """A put on the raw cache with provider='the_graph' MUST surface in
    health()['per_rpc']['raw_cache_entries_by_provider']. The partition
    extractor pulls the provider name from the 8-tuple key."""
    from almanak.gateway.proto import gateway_pb2 as pb
    from almanak.gateway.services._history_cache import FINALITY_FINALIZED, make_raw_key

    servicer = PoolHistoryServiceServicer(GatewaySettings())
    common_kwargs: dict = dict(
        chain="arbitrum",
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        protocol="uniswap_v3",
        end_ts=1_700_604_800,
        resolution=pb.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    k_graph = make_raw_key(start_ts=1_700_000_000, provider="the_graph", **common_kwargs)
    k_llama = make_raw_key(start_ts=1_700_000_000, provider="defillama", **common_kwargs)
    k_llama2 = make_raw_key(start_ts=1_700_001_000, provider="defillama", **common_kwargs)
    for k in (k_graph, k_llama, k_llama2):
        servicer._raw_cache.put(k, pb.PoolHistoryResponse(success=True), FINALITY_FINALIZED)
    rpc = servicer.health()["per_rpc"]
    assert rpc["raw_cache_entries_by_provider"] == {"the_graph": 1, "defillama": 2}


def test_health_cache_counters_are_sums_across_both_tiers():
    """``cache_hits`` / ``cache_misses`` / ``cache_evictions_*`` /
    ``cache_bytes_resident`` are summed across the public + raw caches
    (both tiers consume gateway memory and burn the upstream-fetch
    avoidance budget). The single-tier counter is preserved in
    individual cache.stats() for ops drill-down."""
    from almanak.gateway.proto import gateway_pb2 as pb
    from almanak.gateway.services._history_cache import (
        FINALITY_FINALIZED,
        make_public_key,
        make_raw_key,
    )

    servicer = PoolHistoryServiceServicer(GatewaySettings())
    pub_key = make_public_key(
        chain="arbitrum",
        pool_address="0xabc",
        protocol="uniswap_v3",
        start_ts=10,
        end_ts=20,
        resolution=pb.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    raw_key = make_raw_key(
        chain="arbitrum",
        pool_address="0xabc",
        protocol="uniswap_v3",
        start_ts=10,
        end_ts=20,
        resolution=pb.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
        provider="the_graph",
    )
    # 1 miss on public, 1 hit on public, 1 miss on raw, 1 hit on raw.
    servicer._public_cache.get(pub_key)  # miss
    servicer._public_cache.put(pub_key, pb.PoolHistoryResponse(success=True), FINALITY_FINALIZED)
    servicer._public_cache.get(pub_key)  # hit
    servicer._raw_cache.get(raw_key)  # miss
    servicer._raw_cache.put(raw_key, pb.PoolHistoryResponse(success=True), FINALITY_FINALIZED)
    servicer._raw_cache.get(raw_key)  # hit

    rpc = servicer.health()["per_rpc"]
    assert rpc["cache_hits"] == 2  # 1 public + 1 raw
    assert rpc["cache_misses"] == 2  # 1 public + 1 raw
    assert rpc["cache_bytes_resident"] > 0




# ============================================================================
# Settings -> servicer wiring: env var flips the kill-switch
# ============================================================================


@pytest.mark.parametrize(
    "env_value, expected_enabled",
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("false", False),
        ("False", False),
        ("0", False),
    ],
)
def test_env_var_flips_kill_switch(env_value: str, expected_enabled: bool, monkeypatch):
    """Pydantic parses ``ALMANAK_GATEWAY_POOL_HISTORY_ENABLED`` into the
    settings field. This test pins the env-var name + parsing behavior."""
    monkeypatch.setenv("ALMANAK_GATEWAY_POOL_HISTORY_ENABLED", env_value)
    settings = GatewaySettings()
    assert settings.pool_history_enabled is expected_enabled
    servicer = PoolHistoryServiceServicer(settings)
    assert servicer._enabled is expected_enabled
