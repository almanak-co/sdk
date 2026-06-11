"""Balancer V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    DexVolumeDecl,
    ImportRef,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="balancer_v2",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "arbitrum", "polygon"),
        amm_family="weighted",
        aliases=("bal",),
        name="balancer",
        dex="balancer_v2",
        volume_data_source="balancer_v2_subgraph",
    ),
    address_tables=(
        AddressTableSpec(
            protocol="balancer_v2",
            module="almanak.connectors.balancer_v2.addresses",
            attribute="BALANCER_V2",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.balancer_v2.gateway.provider",
        attribute="BalancerV2GatewayConnector",
        order=16,
    ),
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.balancer_v2.gas_estimate_provider",
        attribute="BalancerV2GasEstimateConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.balancer_v2.contract_roles",
        attribute="CONTRACT_ROLES",
        order=11,
    ),
    flash_loan_provider_name="balancer",
    flash_loan_provider=ImportRef(
        module="almanak.connectors.balancer_v2.flash_loan_provider",
        attribute="BalancerFlashLoanProvider",
        order=2,
    ),
    flash_loan_builder=ImportRef(
        module="almanak.connectors.balancer_v2.flash_loan",
        attribute="build_balancer_flash_loan",
    ),
    flash_loan_synthetic_discovery=True,
    strategy_intents=("FLASH_LOAN",),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche"),
    # Matrix output keeps the historical "balancer" row name for flash loans.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="balancer",
            category="flash_loan",
            chains=frozenset(("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche")),
        ),
    ),
)

__all__ = ["CONNECTOR"]
