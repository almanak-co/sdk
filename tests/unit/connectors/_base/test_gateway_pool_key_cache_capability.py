"""``GatewayPoolKeyCacheCapability`` contract tests (VIB-4818).

Supersedes ``test_gateway_pool_key_seed_capability.py``. VIB-4810 split
"construct cache" and "seed cache" — the gateway instantiated the cache
class itself and iterated ``GatewayPoolKeySeedCapability`` providers to
seed it. VIB-4818 folds both into ``build_cache(*, network=...)`` so the
gateway no longer needs to import the connector-specific cache class or
the connector-specific lookup-error class.

Tests pin:

* ``isinstance(connector, GatewayPoolKeyCacheCapability)`` is True iff
  the connector defines ``build_cache``.
* The registered ``uniswap_v4`` connector advertises the capability.
* ``MarketService._get_pool_key_cache`` invokes the provider's
  ``build_cache`` exactly once and caches the result.
* Two providers raises loudly (winner-takes-all would mask the bug).
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayPoolKeyCacheCapability,
    PoolKeyCacheProtocol,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _FakePoolKeyCache:
    """Minimal ``PoolKeyCacheProtocol`` impl for tests."""

    async def lookup(self, chain: str, pool_id: bytes) -> Any | None:
        return None


class _CacheImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("pool_key_cache_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def __init__(self) -> None:
        super().__init__()
        self.built_with: list[str] = []

    def build_cache(self, *, network: str) -> PoolKeyCacheProtocol:
        self.built_with.append(network)
        return _FakePoolKeyCache()


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_pool_key_cache_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_pool_key_cache_capability_runtime_isinstance() -> None:
    assert isinstance(_CacheImpl(), GatewayPoolKeyCacheCapability)
    assert not isinstance(_BareConnector(), GatewayPoolKeyCacheCapability)


def test_uniswap_v4_advertises_pool_key_cache_capability() -> None:
    """The registered ``uniswap_v4`` connector exposes the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayPoolKeyCacheCapability)
    protocols = {str(p.protocol) for p in providers}
    assert "uniswap_v4" in protocols


@pytest.mark.asyncio
async def test_market_service_invokes_build_cache_once() -> None:
    """The shipped ``UniswapV4GatewayConnector.build_cache`` is called once.

    Pins the lazy-construct + memoise contract: first call constructs;
    subsequent calls return the same instance without re-invoking
    ``build_cache`` (which would discard backfill state).
    """
    from almanak.connectors.uniswap_v4.gateway.provider import (
        UniswapV4GatewayConnector,
    )
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.market_service import MarketServiceServicer

    sentinel_cache = _FakePoolKeyCache()
    service = MarketServiceServicer(GatewaySettings())
    with patch.object(
        UniswapV4GatewayConnector,
        "build_cache",
        autospec=True,
        return_value=sentinel_cache,
    ) as mock_build:
        cache = await service._get_pool_key_cache()
        assert cache is sentinel_cache
        assert mock_build.call_count == 1

        again = await service._get_pool_key_cache()
        assert again is sentinel_cache
        # Memoised — no second construction.
        assert mock_build.call_count == 1


@pytest.mark.asyncio
async def test_market_service_raises_on_multiple_providers() -> None:
    """Two registered providers is a configuration ambiguity, not a winner.

    Today exactly one connector (``uniswap_v4``) implements the
    capability. If a second pool-keyed protocol ever lands, the gateway
    refuses to silently winner-takes-all so the dispatcher can be
    designed explicitly (e.g. chain-keyed) rather than implicit.

    Mirrors the zero-provider test: patches ``capability_providers``
    rather than mutating ``GATEWAY_REGISTRY``. Mutation races under
    pytest-xdist where multiple workers share the global registry.
    """
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.market_service import MarketServiceServicer

    service = MarketServiceServicer(GatewaySettings())
    with patch(
        "almanak.gateway.services.market_service.GATEWAY_REGISTRY"
    ) as mock_registry:
        mock_registry.capability_providers = MagicMock(
            return_value=iter([_CacheImpl(), _CacheImpl()])
        )
        with pytest.raises(RuntimeError, match="Ambiguous GatewayPoolKeyCacheCapability"):
            await service._get_pool_key_cache()


@pytest.mark.asyncio
async def test_market_service_raises_on_zero_providers() -> None:
    """No capability provider registered is a misconfigured deployment.

    Achieved by patching ``capability_providers`` to return an empty list
    rather than mutating ``GATEWAY_REGISTRY`` (mutation would race with
    parallel tests via the global registry).
    """
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.market_service import MarketServiceServicer

    service = MarketServiceServicer(GatewaySettings())
    with patch(
        "almanak.gateway.services.market_service.GATEWAY_REGISTRY"
    ) as mock_registry:
        mock_registry.capability_providers = MagicMock(return_value=iter([]))
        with pytest.raises(RuntimeError, match="No GatewayPoolKeyCacheCapability provider"):
            await service._get_pool_key_cache()
