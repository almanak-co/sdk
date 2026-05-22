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
