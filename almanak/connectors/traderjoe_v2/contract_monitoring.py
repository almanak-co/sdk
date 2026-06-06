"""Trader Joe V2 contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

TRADERJOE_V2_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="traderjoe_v2",
        contract_key="router",
        parser_module="almanak.connectors.traderjoe_v2.receipt_parser",
        parser_class_name="TraderJoeV2ReceiptParser",
        supported_actions=("SWAP",),
    ),
)

__all__ = ["TRADERJOE_V2_CONTRACT_MONITORING_SPECS"]
