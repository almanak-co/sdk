"""Uniswap V3 family contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

UNISWAP_V3_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="uniswap_v3",
        contract_key="swap_router",
        parser_module="almanak.connectors.uniswap_v3.receipt_parser",
        parser_class_name="UniswapV3ReceiptParser",
        supported_actions=("SWAP",),
    ),
    ContractMonitoringSpec(
        protocol="agni_finance",
        contract_key="swap_router",
        parser_module="almanak.connectors.uniswap_v3.receipt_parser",
        parser_class_name="UniswapV3ReceiptParser",
        supported_actions=("SWAP",),
    ),
    ContractMonitoringSpec(
        protocol="uniswap_v3",
        contract_key="position_manager",
        parser_module="almanak.connectors.uniswap_v3.receipt_parser",
        parser_class_name="UniswapV3ReceiptParser",
        supported_actions=("LP_OPEN", "LP_CLOSE"),
    ),
    ContractMonitoringSpec(
        protocol="agni_finance",
        contract_key="position_manager",
        parser_module="almanak.connectors.uniswap_v3.receipt_parser",
        parser_class_name="UniswapV3ReceiptParser",
        supported_actions=("LP_OPEN", "LP_CLOSE"),
    ),
)

__all__ = ["UNISWAP_V3_CONTRACT_MONITORING_SPECS"]
