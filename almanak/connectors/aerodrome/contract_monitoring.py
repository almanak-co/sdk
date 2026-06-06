"""Aerodrome contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

AERODROME_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="aerodrome",
        contract_key="router",
        parser_module="almanak.connectors.aerodrome.receipt_parser",
        parser_class_name="AerodromeReceiptParser",
        supported_actions=("SWAP",),
    ),
)

__all__ = ["AERODROME_CONTRACT_MONITORING_SPECS"]
