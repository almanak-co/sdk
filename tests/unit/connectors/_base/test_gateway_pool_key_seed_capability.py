"""``GatewayPoolKeySeedCapability`` contract tests (VIB-4810 / VIB-4817).

VIB-4810 introduced the capability; VIB-4817 routed
``MarketService._get_v4_pool_key_cache`` through
``GATEWAY_REGISTRY.capability_providers(GatewayPoolKeySeedCapability)``
so the gateway no longer hardcodes a uniswap_v4 import to invoke the
canonical PoolKey seed table.

Tests pin:

* ``isinstance(connector, GatewayPoolKeySeedCapability)`` is True iff
  the connector defines ``seed_pool_keys``.
* The registered ``uniswap_v4`` connector advertises the capability.
* ``MarketService._get_v4_pool_key_cache`` iterates every registered
  ``GatewayPoolKeySeedCapability`` provider, calling
  ``seed_pool_keys(cache)`` on each.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayPoolKeySeedCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _SeedImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("pool_key_seed_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def __init__(self) -> None:
        super().__init__()
        self.seeded_with: list[Any] = []

    def seed_pool_keys(self, cache: Any) -> None:
        self.seeded_with.append(cache)


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_pool_key_seed_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_pool_key_seed_capability_runtime_isinstance() -> None:
    assert isinstance(_SeedImpl(), GatewayPoolKeySeedCapability)
    assert not isinstance(_BareConnector(), GatewayPoolKeySeedCapability)


def test_uniswap_v4_advertises_pool_key_seed_capability() -> None:
    """The registered ``uniswap_v4`` connector exposes the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayPoolKeySeedCapability)
    protocols = {str(p.protocol) for p in providers}
    assert "uniswap_v4" in protocols


@pytest.mark.asyncio
async def test_market_service_seeds_via_registry() -> None:
    """``_get_v4_pool_key_cache`` calls every registered seed provider.

    VIB-4817 collapse: ``MarketService`` no longer hardcodes
    ``seed_canonical_pool_keys`` — it iterates the registry. Adding a
    new ``GatewayPoolKeySeedCapability`` provider therefore happens
    with zero gateway edits.
    """
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.market_service import MarketServiceServicer

    extra = _SeedImpl()
    GATEWAY_REGISTRY.register(extra)
    try:
        service = MarketServiceServicer(GatewaySettings())
        cache = await service._get_v4_pool_key_cache()
        # The extra provider must have been invoked exactly once.
        assert extra.seeded_with == [cache]
        # Second call returns the cached instance and does NOT re-seed.
        again = await service._get_v4_pool_key_cache()
        assert again is cache
        assert extra.seeded_with == [cache]
    finally:
        GATEWAY_REGISTRY._connectors.pop(extra.protocol, None)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_market_service_invokes_uniswap_v4_seed() -> None:
    """The shipped ``UniswapV4GatewayConnector.seed_pool_keys`` is called.

    Pins the behaviour the previous hardcoded
    ``seed_canonical_pool_keys(cache)`` call provided — pre-Phase-6,
    seeding ran unconditionally on first ``LookupV4PoolKey``; after
    the collapse, the registry-driven path must still invoke it.
    """
    from unittest.mock import patch

    from almanak.connectors.uniswap_v4.gateway.provider import (
        UniswapV4GatewayConnector,
    )
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.market_service import MarketServiceServicer

    service = MarketServiceServicer(GatewaySettings())
    with patch.object(
        UniswapV4GatewayConnector,
        "seed_pool_keys",
        autospec=True,
    ) as mock_seed:
        cache = await service._get_v4_pool_key_cache()
        assert mock_seed.called
        # First positional arg after ``self`` is the cache instance.
        called_cache = mock_seed.call_args.args[1]
        assert called_cache is cache
