"""SushiSwap V3 contract monitoring declarations."""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_monitoring import ContractMonitoringSpec

SUSHISWAP_V3_CONTRACT_MONITORING_SPECS = (
    ContractMonitoringSpec(
        protocol="sushiswap_v3",
        contract_key="swap_router",
        parser_module="almanak.connectors.sushiswap_v3.receipt_parser",
        parser_class_name="SushiSwapV3ReceiptParser",
        supported_actions=("SWAP",),
    ),
    ContractMonitoringSpec(
        protocol="sushiswap_v3",
        contract_key="position_manager",
        parser_module="almanak.connectors.sushiswap_v3.receipt_parser",
        parser_class_name="SushiSwapV3ReceiptParser",
        supported_actions=("LP_OPEN", "LP_CLOSE"),
    ),
)

__all__ = ["SUSHISWAP_V3_CONTRACT_MONITORING_SPECS"]
