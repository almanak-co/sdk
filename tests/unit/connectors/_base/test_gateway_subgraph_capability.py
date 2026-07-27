"""GatewaySubgraphCapability deployment-metadata contract tests."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewaySubgraphCapability,
    GatewaySubgraphDeployment,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

_ID_A = "11111111111111111111111111111111111111111111"
_ID_B = "22222222222222222222222222222222222222222222"


class _SubgraphImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("subgraph_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def subgraph_deployments(self) -> dict[str, GatewaySubgraphDeployment]:
        return {
            "demo-protocol-ethereum": GatewaySubgraphDeployment(
                deployment_id=_ID_A,
                schema_family="demo",
            )
        }


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_subgraph_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_subgraph_capability_runtime_isinstance() -> None:
    assert isinstance(_SubgraphImpl(), GatewaySubgraphCapability)
    assert not isinstance(_BareConnector(), GatewaySubgraphCapability)


def test_registered_connectors_advertise_only_verified_alias_capability() -> None:
    """Only connectors with verified replacement deployments expose aliases."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability)
    protocols = {p.protocol for p in providers}
    assert ProtocolName("uniswap_v3") in protocols
    assert ProtocolName("aave_v3") in protocols
    assert ProtocolName("curve") not in protocols
    assert ProtocolName("balancer_v2") not in protocols


def test_uniswap_v3_deployments_are_v3_native_and_pool_history_compatible() -> None:
    from almanak.connectors.uniswap_v3.gateway.provider import (
        _UNISWAP_V3_VOLUME_SUBGRAPH_IDS,
        UniswapV3GatewayConnector,
    )

    deployments = UniswapV3GatewayConnector().subgraph_deployments()
    assert set(deployments) == {
        "uniswap-v3-ethereum",
        "uniswap-v3-arbitrum",
        "uniswap-v3-base",
        "uniswap-v3-polygon",
    }
    assert "uniswap-v3-optimism" not in deployments
    for alias, deployment in deployments.items():
        chain = alias.removeprefix("uniswap-v3-")
        assert deployment.deployment_id == _UNISWAP_V3_VOLUME_SUBGRAPH_IDS[chain]
        assert deployment.schema_family == "uniswap_v3"
        assert deployment.supports_pool_history is True


def test_aave_v3_deployments_reuse_canonical_historical_provider_ids() -> None:
    from almanak.connectors.aave_v3.gateway.provider import AaveV3GatewayConnector
    from almanak.connectors.aave_v3.subgraph_ids import AAVE_V3_SUBGRAPH_IDS

    deployments = AaveV3GatewayConnector().subgraph_deployments()
    assert set(deployments) == {
        "aave-v3-ethereum",
        "aave-v3-arbitrum",
        "aave-v3-optimism",
        "aave-v3-polygon",
    }
    for alias, deployment in deployments.items():
        chain = alias.removeprefix("aave-v3-")
        assert deployment.deployment_id == AAVE_V3_SUBGRAPH_IDS[chain]
        assert deployment.schema_family == "aave_v3"
        assert deployment.supports_pool_history is False


def test_default_allowlist_uses_only_credential_free_network_urls() -> None:
    from almanak.gateway.data._thegraph_network import THEGRAPH_GATEWAY_BASE_URL
    from almanak.gateway.integrations.thegraph import DEFAULT_ALLOWED_SUBGRAPHS

    expected_aliases = {
        "uniswap-v3-ethereum",
        "uniswap-v3-arbitrum",
        "uniswap-v3-base",
        "uniswap-v3-polygon",
        "aave-v3-ethereum",
        "aave-v3-arbitrum",
        "aave-v3-optimism",
        "aave-v3-polygon",
    }
    assert set(DEFAULT_ALLOWED_SUBGRAPHS) == expected_aliases
    assert all(url.startswith(f"{THEGRAPH_GATEWAY_BASE_URL}/") for url in DEFAULT_ALLOWED_SUBGRAPHS.values())
    assert all("/api/subgraphs/id/" in url for url in DEFAULT_ALLOWED_SUBGRAPHS.values())


def test_framework_alias_list_matches_gateway_allowlist() -> None:
    from almanak.framework.integrations.thegraph import SUBGRAPH_ALIASES
    from almanak.gateway.integrations.thegraph import DEFAULT_ALLOWED_SUBGRAPHS

    assert set(SUBGRAPH_ALIASES) == set(DEFAULT_ALLOWED_SUBGRAPHS)


def test_production_python_contains_no_decommissioned_thegraph_hosts() -> None:
    """Regression guard: dead hosted-service domains cannot return to runtime code."""
    repository_root = Path(__file__).resolve().parents[4]
    legacy_hosts = ("api." + "thegraph.com", "api.studio." + "thegraph.com")
    offenders: list[str] = []
    for path in (repository_root / "almanak").rglob("*.py"):
        text = path.read_text()
        if any(host in text for host in legacy_hosts):
            offenders.append(str(path.relative_to(repository_root)))
    assert offenders == []


def test_deployment_metadata_rejects_urls() -> None:
    with pytest.raises(ValueError, match="bare"):
        GatewaySubgraphDeployment(
            deployment_id="https://gateway.thegraph.com/api/subgraphs/id/example",
            schema_family="demo",
        )


def test_subgraph_alias_collision_raises() -> None:
    import almanak.connectors._gateway_registry as registry_mod
    from almanak.gateway.data._thegraph_network import build_registered_subgraph_deployments

    class _ProviderA:
        def subgraph_deployments(self) -> dict[str, GatewaySubgraphDeployment]:
            return {
                "shared-alias": GatewaySubgraphDeployment(
                    deployment_id=_ID_A,
                    schema_family="demo",
                )
            }

    class _ProviderB:
        def subgraph_deployments(self) -> dict[str, GatewaySubgraphDeployment]:
            return {
                "shared-alias": GatewaySubgraphDeployment(
                    deployment_id=_ID_B,
                    schema_family="demo",
                )
            }

    class _FakeRegistry:
        def capability_providers(self, _cap: object) -> tuple[object, ...]:
            return (_ProviderA(), _ProviderB())

    original = registry_mod.GATEWAY_REGISTRY
    registry_mod.GATEWAY_REGISTRY = _FakeRegistry()  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="alias collision"):
            build_registered_subgraph_deployments()
    finally:
        registry_mod.GATEWAY_REGISTRY = original
