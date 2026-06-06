"""GMX V2 contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

GMX_V2_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="gmx_v2",
        contract_key="exchange_router",
        parser_module="almanak.connectors.gmx_v2.receipt_parser",
        parser_class_name="GMXv2ReceiptParser",
        supported_actions=("PERP_OPEN", "PERP_CLOSE"),
    ),
)

__all__ = ["GMX_V2_CONTRACT_MONITORING_SPECS"]
