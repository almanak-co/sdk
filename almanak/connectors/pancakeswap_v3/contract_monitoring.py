"""PancakeSwap V3 contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

PANCAKESWAP_V3_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="pancakeswap_v3",
        contract_key="swap_router",
        parser_module="almanak.connectors.pancakeswap_v3.receipt_parser",
        parser_class_name="PancakeSwapV3ReceiptParser",
        supported_actions=("SWAP",),
    ),
    ContractMonitoringSpec(
        protocol="pancakeswap_v3",
        contract_key="nft",
        parser_module="almanak.connectors.pancakeswap_v3.receipt_parser",
        parser_class_name="PancakeSwapV3ReceiptParser",
        supported_actions=("LP_OPEN", "LP_CLOSE"),
    ),
)

__all__ = ["PANCAKESWAP_V3_CONTRACT_MONITORING_SPECS"]
