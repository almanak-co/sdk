"""Frozen snapshots for connector chain-ID compatibility views."""

from collections.abc import Mapping

import pytest

from almanak.connectors._fluid_core.gateway.market_lookup import FLUID_CHAIN_IDS
from almanak.connectors.aave_v3.gateway.market_lookup import AAVE_CHAIN_IDS
from almanak.connectors.across.adapter import ACROSS_CHAIN_IDS
from almanak.connectors.enso.client import CHAIN_MAPPING as ENSO_CLIENT_CHAIN_IDS
from almanak.connectors.enso.gateway.service import CHAIN_MAPPING as ENSO_GATEWAY_CHAIN_IDS
from almanak.connectors.lifi.client import CHAIN_MAPPING as LIFI_CHAIN_IDS
from almanak.connectors.morpho_vault.gateway.vault_lookup import MORPHO_CHAIN_IDS
from almanak.connectors.pendle.api_client import CHAIN_ID_MAP as PENDLE_CLIENT_CHAIN_IDS
from almanak.connectors.pendle.gateway.market_lookup import PENDLE_CHAIN_IDS
from almanak.connectors.stargate.adapter import EVM_CHAIN_IDS, STARGATE_ENDPOINT_IDS

EVM_SEVEN = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
    "base": 8453,
    "avalanche": 43114,
    "bsc": 56,
}
ENSO_REGISTERED_DEPLOYMENTS = EVM_SEVEN | {"sonic": 146, "linea": 59144, "berachain": 80094}


@pytest.mark.parametrize(
    ("chain_ids", "expected"),
    [
        (
            ACROSS_CHAIN_IDS,
            {
                "ethereum": 1,
                "arbitrum": 42161,
                "base": 8453,
                "optimism": 10,
                "polygon": 137,
                "linea": 59144,
            },
        ),
        (EVM_CHAIN_IDS, EVM_SEVEN),
        (ENSO_CLIENT_CHAIN_IDS, ENSO_REGISTERED_DEPLOYMENTS),
        (ENSO_GATEWAY_CHAIN_IDS, ENSO_REGISTERED_DEPLOYMENTS),
        (LIFI_CHAIN_IDS, EVM_SEVEN),
        (PENDLE_CLIENT_CHAIN_IDS, {"arbitrum": 42161, "ethereum": 1}),
        (PENDLE_CHAIN_IDS, {"arbitrum": 42161, "ethereum": 1}),
        (
            AAVE_CHAIN_IDS,
            {
                "ethereum": 1,
                "arbitrum": 42161,
                "optimism": 10,
                "polygon": 137,
                "base": 8453,
                "avalanche": 43114,
                "bsc": 56,
                "mantle": 5000,
                "xlayer": 196,
                "linea": 59144,
            },
        ),
        (FLUID_CHAIN_IDS, {"arbitrum": 42161, "base": 8453, "ethereum": 1, "polygon": 137}),
        (MORPHO_CHAIN_IDS, {"ethereum": 1, "base": 8453}),
    ],
)
def test_chain_id_views_are_frozen_manifest_snapshots(
    chain_ids: Mapping[str, int], expected: dict[str, int]
) -> None:
    assert dict(chain_ids) == expected
    with pytest.raises(TypeError):
        chain_ids["unregistered"] = 999  # type: ignore[index]


def test_stargate_endpoint_ids_are_named_and_frozen_separately() -> None:
    assert dict(STARGATE_ENDPOINT_IDS) == {
        "ethereum": 30101,
        "arbitrum": 30110,
        "optimism": 30111,
        "polygon": 30109,
        "base": 30184,
        "avalanche": 30106,
        "bsc": 30102,
    }
    assert set(STARGATE_ENDPOINT_IDS) == set(EVM_CHAIN_IDS)
    with pytest.raises(TypeError):
        STARGATE_ENDPOINT_IDS["ethereum"] = 1  # type: ignore[index]
