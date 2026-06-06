"""Morpho Blue contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

MORPHO_BLUE_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="morpho_blue",
        contract_key="morpho",
        parser_module="almanak.connectors.morpho_blue.receipt_parser",
        parser_class_name="MorphoBlueReceiptParser",
        supported_actions=("SUPPLY", "WITHDRAW", "BORROW", "REPAY"),
    ),
)

__all__ = ["MORPHO_BLUE_CONTRACT_MONITORING_SPECS"]
