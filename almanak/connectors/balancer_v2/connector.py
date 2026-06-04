"""Balancer V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="balancer_v2",
    kind=ProtocolKind.LP,
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
)

__all__ = ["CONNECTOR"]
