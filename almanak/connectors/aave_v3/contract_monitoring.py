"""Aave V3 contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

AAVE_V3_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="aave_v3",
        contract_key="pool",
        parser_module="almanak.connectors.aave_v3.receipt_parser",
        parser_class_name="AaveV3ReceiptParser",
        supported_actions=("SUPPLY", "WITHDRAW", "BORROW", "REPAY"),
    ),
)

__all__ = ["AAVE_V3_CONTRACT_MONITORING_SPECS"]
