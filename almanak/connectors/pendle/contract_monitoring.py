"""Contract monitoring declarations owned by the Pendle connector."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

_PENDLE_ACTIONS = ("SWAP", "LP_OPEN", "LP_CLOSE")
_PENDLE_RECEIPT_PARSER_MODULE = "almanak.connectors.pendle.receipt_parser"
_PENDLE_RECEIPT_PARSER_CLASS = "PendleReceiptParser"

PENDLE_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="pendle",
        contract_key="router",
        parser_module=_PENDLE_RECEIPT_PARSER_MODULE,
        parser_class_name=_PENDLE_RECEIPT_PARSER_CLASS,
        supported_actions=_PENDLE_ACTIONS,
    ),
    ContractMonitoringSpec(
        protocol="pendle",
        contract_key_prefix="market_",
        parser_module=_PENDLE_RECEIPT_PARSER_MODULE,
        parser_class_name=_PENDLE_RECEIPT_PARSER_CLASS,
        supported_actions=_PENDLE_ACTIONS,
    ),
)

__all__ = ["PENDLE_CONTRACT_MONITORING_SPECS"]
