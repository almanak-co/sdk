"""Uniswap V4 contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

UNISWAP_V4_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="uniswap_v4",
        contract_key="pool_manager",
        parser_module="almanak.connectors.uniswap_v4.receipt_parser",
        parser_class_name="UniswapV4ReceiptParser",
        supported_actions=("SWAP",),
    ),
    ContractMonitoringSpec(
        protocol="uniswap_v4",
        contract_key="position_manager",
        parser_module="almanak.connectors.uniswap_v4.receipt_parser",
        parser_class_name="UniswapV4ReceiptParser",
        supported_actions=("LP_OPEN", "LP_CLOSE"),
    ),
)

__all__ = ["UNISWAP_V4_CONTRACT_MONITORING_SPECS"]
