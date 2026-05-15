"""VIB-4347: tests for the OHLCV stack factory."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.data.ohlcv import (
    OHLCVStack,
    create_ohlcv_stack,
    create_routing_ohlcv_provider,
)
from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter
from almanak.framework.data.ohlcv.routing_provider import RoutingOHLCVProvider


@pytest.fixture
def fake_gateway_client() -> MagicMock:
    """Minimal stub — the factory only stores the reference."""
    return MagicMock(name="GatewayClient")


# =============================================================================
# D1.1 — Factory returns OHLCVStack with both router and provider
# =============================================================================


def test_factory_returns_ohlcv_stack(fake_gateway_client: MagicMock) -> None:
    stack = create_ohlcv_stack(gateway_client=fake_gateway_client, chain="arbitrum")
    assert isinstance(stack, OHLCVStack)
    assert isinstance(stack.router, OHLCVRouter)
    assert isinstance(stack.provider, RoutingOHLCVProvider)
    # The provider must wrap the same router instance — shared cache + TTL.
    assert stack.provider._router is stack.router


# =============================================================================
# D1.2 — Factory accepts pool_address directly
# =============================================================================


def test_factory_accepts_pool_address_direct(fake_gateway_client: MagicMock) -> None:
    stack = create_ohlcv_stack(
        gateway_client=fake_gateway_client,
        chain="arbitrum",
        pool_address="0xabc",
    )
    assert stack.provider._pool_address == "0xabc"


def test_factory_pool_address_none_default(fake_gateway_client: MagicMock) -> None:
    stack = create_ohlcv_stack(gateway_client=fake_gateway_client, chain="arbitrum")
    assert stack.provider._pool_address is None


# =============================================================================
# D2.1 / F3 — Provider chain order is deterministic; both providers registered
# =============================================================================


def test_factory_registers_two_providers(fake_gateway_client: MagicMock) -> None:
    stack = create_ohlcv_stack(gateway_client=fake_gateway_client, chain="arbitrum")
    assert set(stack.router._providers.keys()) == {"geckoterminal", "binance"}


def test_provider_chain_geckoterminal_first(fake_gateway_client: MagicMock) -> None:
    """Order matters: ``_PROVIDER_CHAINS['defi_primary']`` expects gecko before binance."""
    stack = create_ohlcv_stack(gateway_client=fake_gateway_client, chain="arbitrum")
    keys = list(stack.router._providers.keys())
    assert keys.index("geckoterminal") < keys.index("binance")


# =============================================================================
# D2.2 — Back-compat shim still works
# =============================================================================


def test_create_routing_ohlcv_provider_is_factory_shorthand(
    fake_gateway_client: MagicMock,
) -> None:
    """``create_routing_ohlcv_provider(...)`` MUST return the provider from
    a fresh stack — pre-existing call sites continue to receive a
    :class:`RoutingOHLCVProvider`, no signature surprise."""
    provider = create_routing_ohlcv_provider(
        gateway_client=fake_gateway_client,
        chain="base",
        pool_address="0xdeadbeef",
    )
    assert isinstance(provider, RoutingOHLCVProvider)
    assert provider._pool_address == "0xdeadbeef"
    assert provider._chain == "base"


def test_back_compat_shim_from_cli_run_resolves_to_factory() -> None:
    """``from almanak.framework.cli.run import create_routing_ohlcv_provider``
    must resolve to the relocated factory function (not the old inline body)."""
    from almanak.framework.cli.run import create_routing_ohlcv_provider as via_shim
    from almanak.framework.data.ohlcv.factory import (
        create_routing_ohlcv_provider as via_factory,
    )

    assert via_shim is via_factory
    assert via_shim.__module__ == "almanak.framework.data.ohlcv.factory"


def test_factory_pool_address_falsy_normalizes_to_none(
    fake_gateway_client: MagicMock,
) -> None:
    """Empty-string pool_address must coerce to None (the routing provider's
    convention) — mirrors the old inline factory's ``str(pool_address) if
    pool_address else None`` guard."""
    stack = create_ohlcv_stack(
        gateway_client=fake_gateway_client,
        chain="arbitrum",
        pool_address="",
    )
    assert stack.provider._pool_address is None
