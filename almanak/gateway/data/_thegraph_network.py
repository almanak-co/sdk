"""Shared The Graph Network endpoint and connector-registry policy.

Connector capabilities publish deployment IDs and schema metadata. This
gateway-owned module is the only place that turns those IDs into network
URLs, preventing connector manifests from carrying legacy hosts or secrets.
"""

from __future__ import annotations

import re

from almanak.connectors._base.gateway_capabilities import GatewaySubgraphDeployment

THEGRAPH_GATEWAY_BASE_URL = "https://gateway.thegraph.com/api/subgraphs/id"
MISSING_THEGRAPH_API_KEY_MESSAGE = "ALMANAK_GATEWAY_THEGRAPH_API_KEY is not configured"

# Decentralized-network deployment IDs are normally base58 strings. The
# integration also retains support for exact bytes32 IDs accepted by the
# Network gateway. Full-string validation prevents a direct ID from injecting
# another URL path segment.
_BASE58_DEPLOYMENT_ID_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{40,50}$")
_BYTES32_DEPLOYMENT_ID_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def is_thegraph_deployment_id(value: str) -> bool:
    """Return whether ``value`` is a supported direct deployment identifier."""
    return bool(
        isinstance(value, str)
        and (_BASE58_DEPLOYMENT_ID_RE.fullmatch(value) or _BYTES32_DEPLOYMENT_ID_RE.fullmatch(value))
    )


def thegraph_deployment_url(deployment_id: str) -> str:
    """Build the API-key-free The Graph Network URL for ``deployment_id``."""
    if not is_thegraph_deployment_id(deployment_id):
        raise ValueError(f"Invalid The Graph deployment ID: {deployment_id!r}")
    return f"{THEGRAPH_GATEWAY_BASE_URL}/{deployment_id}"


def normalize_thegraph_api_key(api_key: str | None) -> str | None:
    """Normalize an API key without ever logging or otherwise exposing it."""
    if not isinstance(api_key, str):
        return None
    normalized = api_key.strip()
    return normalized or None


def build_registered_subgraph_deployments() -> dict[str, GatewaySubgraphDeployment]:
    """Merge connector-published deployments with loud collision detection.

    Imports are local to avoid the connector-registry circular import during
    gateway module initialization.
    """
    from almanak.connectors._base.gateway_capabilities import GatewaySubgraphCapability
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    merged: dict[str, GatewaySubgraphDeployment] = {}
    for connector in GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability):  # type: ignore[type-abstract]
        for alias, deployment in connector.subgraph_deployments().items():
            if not isinstance(alias, str) or not alias or alias != alias.lower():
                raise RuntimeError(
                    f"Subgraph alias from {type(connector).__qualname__} must be non-empty lowercase: {alias!r}"
                )
            if not isinstance(deployment, GatewaySubgraphDeployment):
                raise RuntimeError(
                    f"Subgraph alias {alias!r} from {type(connector).__qualname__} "
                    "must map to GatewaySubgraphDeployment"
                )
            # Validate the ID at registry assembly as well as at URL
            # construction. The dataclass rejects URLs; this check rejects a
            # typo that only resembles a bare ID.
            if not is_thegraph_deployment_id(deployment.deployment_id):
                raise RuntimeError(
                    f"Subgraph alias {alias!r} from {type(connector).__qualname__} "
                    f"has invalid deployment ID {deployment.deployment_id!r}"
                )
            existing = merged.get(alias)
            if existing is not None and existing != deployment:
                raise RuntimeError(
                    f"Subgraph alias collision for {alias!r}: already registered as "
                    f"{existing!r}, refusing to overwrite with {deployment!r} from "
                    f"{type(connector).__qualname__}"
                )
            merged[alias] = deployment
    return merged


__all__ = [
    "MISSING_THEGRAPH_API_KEY_MESSAGE",
    "THEGRAPH_GATEWAY_BASE_URL",
    "build_registered_subgraph_deployments",
    "is_thegraph_deployment_id",
    "normalize_thegraph_api_key",
    "thegraph_deployment_url",
]
