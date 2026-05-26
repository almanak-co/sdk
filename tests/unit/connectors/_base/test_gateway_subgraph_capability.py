"""``GatewaySubgraphCapability`` contract tests (VIB-4811 / VIB-4817).

``TheGraphIntegration``'s default allowlist is assembled from
``GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability)``.
VIB-4817 retired the ``_PENDING_SUBGRAPHS`` fallback dict; Curve is now
a fully-fledged ``GatewaySubgraphCapability`` provider. Tests pin:

* ``isinstance(connector, GatewaySubgraphCapability)`` is True iff the
  connector defines ``subgraph_endpoints``.
* The registered Uniswap V3, Aave v3, Balancer v2, and Curve connectors
  return the historical alias → URL pairs.
* The assembled default allowlist is byte-identical to the pre-refactor
  ``DEFAULT_ALLOWED_SUBGRAPHS`` dict.
* Collision on alias raises a loud ``RuntimeError`` at assembly time.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _SubgraphImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("subgraph_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def subgraph_endpoints(self) -> dict[str, str]:
        return {"demo-protocol-ethereum": "https://example.com/subgraph"}


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_subgraph_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_subgraph_capability_runtime_isinstance() -> None:
    assert isinstance(_SubgraphImpl(), GatewaySubgraphCapability)
    assert not isinstance(_BareConnector(), GatewaySubgraphCapability)


def test_registered_connectors_advertise_capability() -> None:
    """Uniswap V3, Aave v3, Balancer v2, and Curve each expose the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability)
    protocols = {p.protocol for p in providers}
    # VIB-4811 (Phase 3) registered the first three.
    # VIB-4817 added curve as a full ``GatewaySubgraphCapability``
    # provider, retiring the ``_PENDING_SUBGRAPHS`` fallback.
    assert {
        ProtocolName("uniswap_v3"),
        ProtocolName("aave_v3"),
        ProtocolName("balancer_v2"),
        ProtocolName("curve"),
    }.issubset(protocols)


def test_curve_subgraph_endpoints_match_legacy_dict() -> None:
    """VIB-4817: curve connector returns the legacy ``_PENDING_SUBGRAPHS`` rows."""
    from almanak.connectors.curve.gateway.provider import CurveGatewayConnector

    connector = CurveGatewayConnector()
    assert isinstance(connector, GatewaySubgraphCapability)
    expected = {
        "curve-ethereum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet",
        "curve-arbitrum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-arbitrum",
    }
    assert connector.subgraph_endpoints() == expected


def test_uniswap_v3_subgraph_endpoints_match_legacy_dict() -> None:
    """Uniswap V3 connector returns the legacy ``DEFAULT_ALLOWED_SUBGRAPHS`` rows."""
    from almanak.connectors.uniswap_v3.gateway.provider import (
        UniswapV3GatewayConnector,
    )

    connector = UniswapV3GatewayConnector()
    expected = {
        "uniswap-v3-ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
        "uniswap-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
        "uniswap-v3-optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
        "uniswap-v3-polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
        "uniswap-v3-base": "https://api.studio.thegraph.com/query/48211/uniswap-v3-base/version/latest",
    }
    assert connector.subgraph_endpoints() == expected


def test_aave_v3_subgraph_endpoints_match_legacy_dict() -> None:
    """Aave v3 connector returns the legacy ``DEFAULT_ALLOWED_SUBGRAPHS`` rows."""
    from almanak.connectors.aave_v3.gateway.provider import (
        AaveV3GatewayConnector,
    )

    connector = AaveV3GatewayConnector()
    expected = {
        "aave-v3-ethereum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3",
        "aave-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum",
        "aave-v3-optimism": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism",
        "aave-v3-polygon": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon",
    }
    assert connector.subgraph_endpoints() == expected


def test_balancer_v2_subgraph_endpoints_match_legacy_dict() -> None:
    """Balancer v2 connector returns the legacy ``DEFAULT_ALLOWED_SUBGRAPHS`` rows."""
    from almanak.connectors.balancer_v2.gateway.provider import (
        BalancerV2GatewayConnector,
    )

    connector = BalancerV2GatewayConnector()
    expected = {
        "balancer-v2-ethereum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2",
        "balancer-v2-arbitrum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-arbitrum-v2",
    }
    assert connector.subgraph_endpoints() == expected


def test_default_allowed_subgraphs_is_byte_identical_to_legacy() -> None:
    """The assembled allowlist matches the pre-refactor hardcoded dict."""
    from almanak.gateway.integrations.thegraph import DEFAULT_ALLOWED_SUBGRAPHS

    legacy = {
        # Uniswap V3 subgraphs
        "uniswap-v3-ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
        "uniswap-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
        "uniswap-v3-optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
        "uniswap-v3-polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
        "uniswap-v3-base": "https://api.studio.thegraph.com/query/48211/uniswap-v3-base/version/latest",
        # Aave V3 subgraphs
        "aave-v3-ethereum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3",
        "aave-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum",
        "aave-v3-optimism": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism",
        "aave-v3-polygon": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon",
        # Curve subgraphs (VIB-4817: migrated onto CurveGatewayConnector)
        "curve-ethereum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet",
        "curve-arbitrum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-arbitrum",
        # Balancer subgraphs
        "balancer-v2-ethereum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2",
        "balancer-v2-arbitrum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-arbitrum-v2",
    }
    assert DEFAULT_ALLOWED_SUBGRAPHS == legacy


def test_subgraph_alias_collision_raises() -> None:
    """Two providers publishing the same alias with different URLs raises."""
    import almanak.connectors._gateway_registry as registry_mod
    from almanak.gateway.integrations.thegraph import (
        _build_default_allowed_subgraphs,
    )

    class _ProviderA:
        def subgraph_endpoints(self) -> dict[str, str]:
            return {"shared-alias": "https://a.example.com"}

    class _ProviderB:
        def subgraph_endpoints(self) -> dict[str, str]:
            return {"shared-alias": "https://b.example.com"}

    class _FakeRegistry:
        def capability_providers(self, _cap: object) -> tuple[object, ...]:
            return (_ProviderA(), _ProviderB())

    original = registry_mod.GATEWAY_REGISTRY
    registry_mod.GATEWAY_REGISTRY = _FakeRegistry()  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="alias collision"):
            _build_default_allowed_subgraphs()
    finally:
        registry_mod.GATEWAY_REGISTRY = original
