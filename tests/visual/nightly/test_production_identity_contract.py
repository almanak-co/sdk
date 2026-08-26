"""Managed-gateway canary for exact production Data identities."""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.framework.data.qa.identity_bundle import validate_identity_bundle
from almanak.framework.data.qa.identity_collector import collect_identity_bundle
from almanak.framework.data.qa.production_identity import ResourceKind, derive_production_requirements
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

pytestmark = pytest.mark.integration


def _canary_ids(chain: str) -> list[str]:
    requirements = [item for item in derive_production_requirements() if item.chain == chain]
    selected: list[str] = []
    for kind in (ResourceKind.TOKEN, ResourceKind.DIRECT_CHAINLINK_FEED, ResourceKind.V3_POOL):
        candidates = [item for item in requirements if item.kind is kind]
        # The canary proves the collector/sealer plumbing with the canonical
        # Uniswap family. A full-chain run remains responsible for exposing
        # every connector-owned pool row independently.
        match = next((item for item in candidates if getattr(item, "protocol", None) == "uniswap_v3"), None)
        match = match or next(iter(candidates), None)
        if match is not None:
            selected.append(match.requirement_id)
    return selected


def test_registry_identities_are_observed_at_one_pinned_block(
    nightly_gateway_runtime,
    tmp_path: Path,
) -> None:
    """One resource per supported family must survive capture and replay."""
    requirement_ids = _canary_ids(nightly_gateway_runtime.chain)
    assert requirement_ids, f"No EVM identity requirements declared for {nightly_gateway_runtime.chain}"
    config = GatewayClientConfig(host=nightly_gateway_runtime.host, port=nightly_gateway_runtime.port)
    with GatewayClient(config) as client:
        bundle = collect_identity_bundle(
            client,
            chain=nightly_gateway_runtime.chain,
            output=tmp_path / "identity",
            requirement_ids=requirement_ids,
        )

    validated = validate_identity_bundle(bundle)

    assert validated.evaluation.passed
    assert {observation.kind for observation in validated.observations} == {
        requirement.kind
        for requirement in derive_production_requirements()
        if requirement.requirement_id in requirement_ids
    }
