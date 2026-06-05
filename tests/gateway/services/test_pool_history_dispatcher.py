"""PoolHistoryDispatcher eligibility-table tests (POOL-5 / VIB-4753).

Covers UAT card ``docs/internal/uat-cards/VIB-4728.md`` D2.M3.b — the
dispatcher's routing table proven correct AS DATA (decoupled from any one
fixture). Combined with the runtime-behaviour cells (D1.S3 + D2.M1 + D2.M2 +
D2.M3) this is the algebraic claim that lets the orthogonal dimensions stand
in for the full Cartesian product.
"""

from __future__ import annotations

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history.dispatcher import PoolHistoryDispatcher
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import (
    PoolHistoryServiceServicer,
    is_supported_pool_pair,
)


def _dispatcher():
    return PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))._dispatcher


# =============================================================================
# D2.M3.b — eligibility table as data
# =============================================================================


def test_dispatcher_eligibility_table():
    """The eligible-providers table is correct per resolution.

    DefiLlama is daily-only -> excluded from 1h / 4h. UNSPECIFIED raises (the
    validator should have rejected it first).
    """
    d = _dispatcher()
    assert d.eligible_providers(gateway_pb2.Resolution.RESOLUTION_1H) == ("the_graph", "geckoterminal")
    assert d.eligible_providers(gateway_pb2.Resolution.RESOLUTION_4H) == ("the_graph", "geckoterminal")
    assert d.eligible_providers(gateway_pb2.Resolution.RESOLUTION_1D) == (
        "the_graph",
        "defillama",
        "geckoterminal",
    )
    with pytest.raises(ValueError):
        d.eligible_providers(gateway_pb2.Resolution.RESOLUTION_UNSPECIFIED)


def test_dispatcher_is_supported_table():
    """The (chain, protocol) support table — registry-derived (VIB-4811)."""
    d = _dispatcher()
    # Supported.
    assert d.is_supported(chain="arbitrum", protocol="uniswap_v3") is True
    assert d.is_supported(chain="ethereum", protocol="uniswap_v3") is True
    assert d.is_supported(chain="base", protocol="uniswap_v3") is True
    assert d.is_supported(chain="base", protocol="aerodrome") is True
    assert d.is_supported(chain="optimism", protocol="uniswap_v3") is True
    # Unsupported.
    assert d.is_supported(chain="ethereum", protocol="aerodrome") is False
    assert d.is_supported(chain="arbitrum", protocol="aerodrome") is False
    assert d.is_supported(chain="solana", protocol="uniswap_v3") is False
    assert d.is_supported(chain="unknown_chain", protocol="uniswap_v3") is False


def test_dispatcher_is_supported_delegates_to_module_table():
    """``dispatcher.is_supported`` is the same predicate the validator uses
    (``is_supported_pool_pair``) — a single source of truth (Codex Round-8 #2)."""
    d = _dispatcher()
    for chain, protocol in [
        ("arbitrum", "uniswap_v3"),
        ("base", "aerodrome"),
        ("ethereum", "aerodrome"),
        ("solana", "uniswap_v3"),
    ]:
        assert d.is_supported(chain, protocol) == is_supported_pool_pair(chain, protocol)


def test_dispatcher_propagates_coingecko_api_key_to_pool_history_provider():
    """PoolHistoryDispatcher wires the CoinGecko key into the fallback provider."""
    dispatcher = PoolHistoryDispatcher(
        thegraph_api_key=None,
        thegraph_monthly_budget_max=100000,
        is_supported_fn=lambda _chain, _protocol: True,
        coingecko_api_key="test-key",
    )

    provider = dispatcher._geckoterminal
    assert "pro-api.coingecko.com" in provider._api_base
    assert provider._headers["x-cg-pro-api-key"] == "test-key"


def test_dispatcher_without_coingecko_api_key_has_no_auth_header():
    """An omitted CoinGecko key never sends a bogus auth header."""
    dispatcher = PoolHistoryDispatcher(
        thegraph_api_key=None,
        thegraph_monthly_budget_max=100000,
        is_supported_fn=lambda _chain, _protocol: True,
        coingecko_api_key=None,
    )

    provider = dispatcher._geckoterminal
    assert "api.coingecko.com" in provider._api_base
    assert "x-cg-pro-api-key" not in provider._headers


def test_servicer_propagates_coingecko_api_key_to_dispatcher():
    """GatewaySettings.coingecko_api_key reaches the pool-history provider."""
    servicer = PoolHistoryServiceServicer(
        GatewaySettings(pool_history_enabled=True, coingecko_api_key="settings-key")
    )

    provider = servicer._dispatcher._geckoterminal
    assert "pro-api.coingecko.com" in provider._api_base
    assert provider._headers["x-cg-pro-api-key"] == "settings-key"
