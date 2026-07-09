"""Tests for connector manifest discovery."""

from __future__ import annotations

import ast
import importlib
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

import almanak.connectors._connector_descriptor as connector_descriptor_module
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._connector import (
    CONNECTOR_REGISTRY,
    BacktestStrategyTypeDecl,
    Connector,
    ConnectorDiscoveryError,
    ConnectorRegistry,
    DexVolumeDecl,
    ImportRef,
    LendingReadDecl,
    PerpsReadDecl,
    StrategyMatrixEntry,
)
from almanak.connectors._connector_descriptor import (
    CONNECTOR_DESCRIPTOR_REGISTRY,
    ConnectorDescriptor,
    ConnectorDescriptorRegistry,
)
from almanak.connectors._strategy_base.accounting_report_registry import (
    AccountingReportConnector,
    AccountingReportSectionCapability,
)
from almanak.connectors._strategy_base.accounting_treatment_base import (
    AccountingTreatmentSpec,
)
from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadConnector,
)
from almanak.connectors._strategy_base.base.compiler import BaseProtocolCompiler
from almanak.connectors._strategy_base.bridge_base import BridgeAdapter
from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry
from almanak.connectors._strategy_base.contract_monitoring import (
    ContractMonitoringSpec,
)
from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRoleSpec,
)
from almanak.connectors._strategy_base.deferred_refresh_registry import (
    DeferredRefreshConnector,
)
from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateConnector,
)
from almanak.connectors._strategy_base.principal_token_market_reader_registry import (
    PrincipalTokenMarketReadConnector,
)
from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamilySpec,
)
from almanak.connectors._strategy_base.protocol_metadata_registry import (
    ProtocolMetadataConnector,
)
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserConnector,
)
from almanak.connectors._strategy_base.registry import ConnectorRegistry as StrategyConnectorRegistry
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
)
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec
from almanak.connectors._strategy_base.swap_classification_registry import (
    SwapClassificationSpec,
)
from almanak.connectors._strategy_base.swap_quote_registry import (
    SWAP_QUOTE_REGISTRY,
    SwapQuoteConnector,
)
from almanak.connectors._strategy_base.swap_route_inference_registry import (
    SwapRouteInferenceConnector,
)
from almanak.connectors._strategy_base.vault_representatives import VaultRepresentativeSpec
from almanak.connectors._strategy_base.vault_tool_registry import (
    VaultToolConnector,
)
from almanak.framework.permissions.models import ContractPermission

EXPECTED_CONNECTOR_KINDS = {
    "aave_v3": ProtocolKind.LENDING,
    "across": ProtocolKind.BRIDGE,
    "aerodrome": ProtocolKind.LP,
    "aster_perps": ProtocolKind.PERP,
    "balancer_v2": ProtocolKind.LP,
    "beefy": ProtocolKind.VAULT,
    "benqi": ProtocolKind.LENDING,
    "camelot": ProtocolKind.SWAP,
    "compound_v3": ProtocolKind.LENDING,
    "curvance": ProtocolKind.LENDING,
    "curve": ProtocolKind.LP,
    "drift": ProtocolKind.PERP,
    "enso": ProtocolKind.SWAP,
    "ethena": ProtocolKind.LENDING,
    "euler_v2": ProtocolKind.LENDING,
    "fluid": ProtocolKind.SWAP,
    "gimo": ProtocolKind.LENDING,
    "gmx_v2": ProtocolKind.PERP,
    "hyperliquid": ProtocolKind.PERP,
    "joelend": ProtocolKind.LENDING,
    "jupiter": ProtocolKind.SWAP,
    "jupiter_lend": ProtocolKind.LENDING,
    "kamino": ProtocolKind.LENDING,
    "kraken": ProtocolKind.SWAP,
    "lagoon": ProtocolKind.VAULT,
    "lido": ProtocolKind.LENDING,
    "lifi": ProtocolKind.BRIDGE,
    "meteora": ProtocolKind.LP,
    "morpho_blue": ProtocolKind.LENDING,
    "morpho_vault": ProtocolKind.VAULT,
    "orca": ProtocolKind.LP,
    "pancakeswap_perps": ProtocolKind.PERP,
    "pancakeswap_v3": ProtocolKind.LP,
    "pendle": ProtocolKind.YIELD_TRADING,
    "polymarket": ProtocolKind.PREDICTION_MARKET,
    "raydium": ProtocolKind.LP,
    "silo_v2": ProtocolKind.LENDING,
    "spark": ProtocolKind.LENDING,
    "stargate": ProtocolKind.BRIDGE,
    "sushiswap_v3": ProtocolKind.LP,
    "traderjoe_v2": ProtocolKind.LP,
    "uniswap_v3": ProtocolKind.LP,
    "uniswap_v4": ProtocolKind.LP,
    "yearn": ProtocolKind.VAULT,
}

EXPECTED_ALIASES = {
    "aerodrome": ("aerodrome_slipstream",),
    "fluid": ("fluid_lending",),
    "kamino": ("kamino_klend",),
    "meteora": ("meteora_dlmm",),
    "morpho_blue": ("morpho",),
    "orca": ("orca_whirlpools",),
    "raydium": ("raydium_clmm",),
    "uniswap_v3": ("agni_finance",),
}

MIGRATED_STRATEGY_REGISTRATION = {
    "aave_v3": (
        ("SUPPLY", "BORROW", "REPAY", "WITHDRAW", "FLASH_LOAN"),
        ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "mantle", "xlayer"),
    ),
    "across": (("BRIDGE",), ("ethereum", "arbitrum", "base", "optimism", "polygon", "linea")),
    "aerodrome": (("SWAP", "LP_OPEN", "LP_CLOSE"), ("base", "optimism")),
    "aster_perps": (("PERP_OPEN", "PERP_CLOSE"), ("bnb",)),
    "balancer_v2": (("FLASH_LOAN",), ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche")),
    "benqi": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("avalanche",)),
    "camelot": (("SWAP",), ("arbitrum",)),
    "compound_v3": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("ethereum", "arbitrum", "base", "optimism", "polygon")),
    "curvance": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("monad",)),
    "curve": (("SWAP", "LP_OPEN", "LP_CLOSE"), ("ethereum", "arbitrum", "optimism", "polygon", "base")),
    "drift": (("PERP_OPEN", "PERP_CLOSE"), ("solana",)),
    "enso": (("SWAP",), ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb")),
    "ethena": (("STAKE", "UNSTAKE"), ("ethereum",)),
    "euler_v2": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("ethereum", "avalanche", "base", "arbitrum")),
    "fluid": (("SWAP", "SUPPLY", "WITHDRAW"), ("arbitrum", "base", "ethereum", "polygon")),
    "fluid_dex_lp": (("LP_OPEN", "LP_CLOSE"), ("arbitrum",)),
    "gimo": (("STAKE", "UNSTAKE"), ("zerog",)),
    "gmx_v2": (("PERP_OPEN", "PERP_CLOSE"), ("arbitrum", "avalanche")),
    "jupiter": (("SWAP",), ("solana",)),
    "kamino": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("solana",)),
    "kraken": (("SWAP",), None),
    "lagoon": (("VAULT_DEPOSIT", "VAULT_REDEEM"), ("ethereum", "base")),
    "lido": (("STAKE", "UNSTAKE"), ("ethereum",)),
    "lifi": (("SWAP", "BRIDGE"), ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb")),
    "meteora": (("LP_OPEN", "LP_CLOSE"), ("solana",)),
    "morpho_blue": (
        ("SUPPLY", "BORROW", "REPAY", "WITHDRAW", "FLASH_LOAN"),
        ("ethereum", "base", "arbitrum", "polygon", "monad"),
    ),
    "morpho_vault": (("VAULT_DEPOSIT", "VAULT_REDEEM"), ("ethereum", "base")),
    "orca": (("LP_OPEN", "LP_CLOSE"), ("solana",)),
    "pancakeswap_perps": (("PERP_OPEN", "PERP_CLOSE"), ("bnb",)),
    "pancakeswap_v3": (("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"), ("bnb", "ethereum", "arbitrum", "base")),
    "pendle": (("SWAP", "LP_OPEN", "LP_CLOSE", "WITHDRAW"), ("arbitrum", "ethereum")),
    "polymarket": (("PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"), ("polygon",)),
    "raydium": (("LP_OPEN", "LP_CLOSE"), ("solana",)),
    "silo_v2": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("avalanche",)),
    "spark": (("SUPPLY", "BORROW", "REPAY", "WITHDRAW"), ("ethereum",)),
    "stargate": (("BRIDGE",), ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb")),
    "sushiswap_v3": (
        ("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
        ("ethereum", "arbitrum", "base", "optimism", "polygon", "bnb"),
    ),
    "traderjoe_v2": (
        ("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
        ("avalanche", "arbitrum", "bnb", "ethereum"),
    ),
    "uniswap_v3": (
        ("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
        ("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "monad"),
    ),
    "uniswap_v4": (
        ("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
        # VIB-4421: extended to the full deployed set (matches UNISWAP_V4 +
        # the 28 on-chain intent tests). "bnb" is the venue-name alias.
        ("ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bnb"),
    ),
}

EXPECTED_STRATEGY_MATRIX_ENTRIES = {
    "aave_v3": (
        StrategyMatrixEntry(
            matrix_name="aave_v3",
            category="lending",
            chains=frozenset(
                (
                    "ethereum",
                    "arbitrum",
                    "optimism",
                    "polygon",
                    "base",
                    "avalanche",
                    "bsc",
                    "linea",
                    "plasma",
                    "sonic",
                    "mantle",
                    "xlayer",
                )
            ),
        ),
    ),
    "balancer_v2": (
        StrategyMatrixEntry(
            matrix_name="balancer",
            category="flash_loan",
            chains=frozenset(("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche")),
        ),
    ),
    "enso": (
        StrategyMatrixEntry(
            matrix_name="enso",
            category="aggregator",
            chains=frozenset(
                (
                    "ethereum",
                    "optimism",
                    "bsc",
                    "gnosis",
                    "polygon",
                    "zksync",
                    "base",
                    "arbitrum",
                    "avalanche",
                    "sonic",
                    "linea",
                    "berachain",
                    "sepolia",
                )
            ),
        ),
    ),
    "lagoon": (),
    "lifi": (
        StrategyMatrixEntry(
            matrix_name="lifi",
            category="aggregator",
            chains=frozenset(
                ("ethereum", "optimism", "bsc", "gnosis", "polygon", "base", "arbitrum", "avalanche", "sonic", "linea")
            ),
        ),
    ),
    "morpho_blue": (
        StrategyMatrixEntry(
            matrix_name="morpho_blue",
            category="lending",
            chains=frozenset(("ethereum", "base", "arbitrum", "polygon", "monad")),
        ),
    ),
    "pendle": (
        StrategyMatrixEntry(
            matrix_name="pendle",
            category="yield",
            # VIB-5300 trimmed this to the chains Pendle can actually compile on
            # ({arbitrum, ethereum}); the prior 7-chain set over-advertised chains
            # that failed at compile time.
            chains=frozenset(("arbitrum", "ethereum")),
        ),
    ),
    "fluid": (
        StrategyMatrixEntry(
            matrix_name="fluid",
            category="swap",
            chains=frozenset(("arbitrum", "base", "ethereum", "polygon")),
        ),
        StrategyMatrixEntry(
            matrix_name="fluid",
            category="lending",
            chains=frozenset(("arbitrum", "base")),
        ),
    ),
    "uniswap_v4": (
        StrategyMatrixEntry(
            matrix_name="uniswap_v4",
            category="swap",
            chains=frozenset(("ethereum", "base", "arbitrum", "optimism", "polygon", "avalanche", "bsc")),
        ),
        StrategyMatrixEntry(
            matrix_name="uniswap_v4",
            category="lp",
            chains=frozenset(("ethereum", "base", "arbitrum", "optimism", "polygon", "avalanche", "bsc")),
        ),
    ),
}

EXPECTED_RECEIPT_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.receipt_parser_provider",
    "across": "almanak.connectors.across.receipt_parser_provider",
    "aerodrome": "almanak.connectors.aerodrome.receipt_parser_provider",
    "aster_perps": "almanak.connectors.aster_perps.receipt_parser_provider",
    "benqi": "almanak.connectors.benqi.receipt_parser_provider",
    "compound_v3": "almanak.connectors.compound_v3.receipt_parser_provider",
    "curvance": "almanak.connectors.curvance.receipt_parser_provider",
    "curve": "almanak.connectors.curve.receipt_parser_provider",
    "drift": "almanak.connectors.drift.receipt_parser_provider",
    "enso": "almanak.connectors.enso.receipt_parser_provider",
    "ethena": "almanak.connectors.ethena.receipt_parser_provider",
    "euler_v2": "almanak.connectors.euler_v2.receipt_parser_provider",
    "fluid": "almanak.connectors.fluid.receipt_parser_provider",
    "gimo": "almanak.connectors.gimo.receipt_parser_provider",
    "gmx_v2": "almanak.connectors.gmx_v2.receipt_parser_provider",
    "joelend": "almanak.connectors.joelend.receipt_parser_provider",
    "jupiter": "almanak.connectors.jupiter.receipt_parser_provider",
    "jupiter_lend": "almanak.connectors.jupiter_lend.receipt_parser_provider",
    "kamino": "almanak.connectors.kamino.receipt_parser_provider",
    "lagoon": "almanak.connectors.lagoon.receipt_parser_provider",
    "lido": "almanak.connectors.lido.receipt_parser_provider",
    "lifi": "almanak.connectors.lifi.receipt_parser_provider",
    "meteora": "almanak.connectors.meteora.receipt_parser_provider",
    "morpho_blue": "almanak.connectors.morpho_blue.receipt_parser_provider",
    "morpho_vault": "almanak.connectors.morpho_vault.receipt_parser_provider",
    "orca": "almanak.connectors.orca.receipt_parser_provider",
    "pancakeswap_perps": "almanak.connectors.pancakeswap_perps.receipt_parser_provider",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.receipt_parser_provider",
    "pendle": "almanak.connectors.pendle.receipt_parser_provider",
    "polymarket": "almanak.connectors.polymarket.receipt_parser_provider",
    "raydium": "almanak.connectors.raydium.receipt_parser_provider",
    "silo_v2": "almanak.connectors.silo_v2.receipt_parser_provider",
    "spark": "almanak.connectors.spark.receipt_parser_provider",
    "stargate": "almanak.connectors.stargate.receipt_parser_provider",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.receipt_parser_provider",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.receipt_parser_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.receipt_parser_provider",
    "uniswap_v4": "almanak.connectors.uniswap_v4.receipt_parser_provider",
}

EXPECTED_GATEWAY_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.gateway.provider",
    "aerodrome": "almanak.connectors.aerodrome.gateway.provider",
    "agni_finance": "almanak.connectors.uniswap_v3.gateway.agni_provider",
    "aster_perps": "almanak.connectors._aster_perps_core.gateway.provider",
    "balancer_v2": "almanak.connectors.balancer_v2.gateway.provider",
    "beefy": "almanak.connectors.beefy.gateway.provider",
    "benqi": "almanak.connectors.benqi.gateway.provider",
    "compound_v3": "almanak.connectors.compound_v3.gateway.provider",
    "curve": "almanak.connectors.curve.gateway.provider",
    "enso": "almanak.connectors.enso.gateway.provider",
    "ethena": "almanak.connectors.ethena.gateway.provider",
    "fluid": "almanak.connectors._fluid_core.gateway.provider",
    "gmx_v2": "almanak.connectors.gmx_v2.gateway.provider",
    "hyperliquid": "almanak.connectors.hyperliquid.gateway.provider",
    "jupiter": "almanak.connectors.jupiter.gateway.provider",
    "lido": "almanak.connectors.lido.gateway.provider",
    "morpho_blue": "almanak.connectors.morpho_blue.gateway.provider",
    "morpho_vault": "almanak.connectors.morpho_vault.gateway.provider",
    "orca": "almanak.connectors.orca.gateway.provider",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.gateway.provider",
    "pendle": "almanak.connectors.pendle.gateway.provider",
    "polymarket": "almanak.connectors.polymarket.gateway.provider",
    "raydium": "almanak.connectors.raydium.gateway.provider",
    "spark": "almanak.connectors.spark.gateway.provider",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.gateway.provider",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.gateway.provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.gateway.provider",
    "uniswap_v4": "almanak.connectors.uniswap_v4.gateway.provider",
    "yearn": "almanak.connectors.yearn.gateway.provider",
}

EXPECTED_GATEWAY_PROVIDER_ORDER = (
    "uniswap_v4",
    "aave_v3",
    "compound_v3",
    "fluid",
    "morpho_vault",
    "pendle",
    "jupiter",
    "beefy",
    "yearn",
    "enso",
    "polymarket",
    "uniswap_v3",
    "aerodrome",
    "gmx_v2",
    "hyperliquid",
    "balancer_v2",
    "lido",
    "ethena",
    "traderjoe_v2",
    "pancakeswap_v3",
    "raydium",
    "orca",
    "benqi",
    "curve",
    "sushiswap_v3",
    "agni_finance",
    "morpho_blue",
    "aster_perps",
    "spark",
)

EXPECTED_GATEWAY_SETTINGS_MODULES = {
    "polymarket": ("almanak.connectors.polymarket.gateway.settings", "PolymarketGatewaySettings"),
    "enso": ("almanak.connectors.enso.gateway.settings", "EnsoGatewaySettings"),
    "pendle": ("almanak.connectors.pendle.gateway.settings", "PendleGatewaySettings"),
}

EXPECTED_GATEWAY_SETTINGS_ORDER = ("polymarket", "enso", "pendle")

EXPECTED_GAS_ESTIMATE_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.gas_estimate_provider",
    "across": "almanak.connectors.across.gas_estimate_provider",
    "balancer_v2": "almanak.connectors.balancer_v2.gas_estimate_provider",
    "morpho_vault": "almanak.connectors.morpho_vault.gas_estimate_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.gas_estimate_provider",
}

EXPECTED_CONTRACT_ROLE_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.contract_roles",
    "aerodrome": "almanak.connectors.aerodrome.contract_roles",
    "balancer_v2": "almanak.connectors.balancer_v2.contract_roles",
    "camelot": "almanak.connectors.camelot.contract_roles",
    "fluid": "almanak.connectors.fluid.contract_roles",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.contract_roles",
    "spark": "almanak.connectors.spark.contract_roles",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.contract_roles",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.contract_roles",
    "uniswap_v3": "almanak.connectors.uniswap_v3.contract_roles",
    "uniswap_v4": "almanak.connectors.uniswap_v4.contract_roles",
}

EXPECTED_CONTRACT_ROLE_ORDER = (
    "uniswap_v3",
    "uniswap_v4",
    "sushiswap_v3",
    "pancakeswap_v3",
    "aerodrome",
    "traderjoe_v2",
    "camelot",
    "fluid",
    "aave_v3",
    "spark",
    "balancer_v2",
)

EXPECTED_PERMISSION_INFRASTRUCTURE_MODULES = {
    "enso": "almanak.connectors.enso.permission_hints",
}

EXPECTED_BRIDGE_ADAPTER_MODULES = {
    "across": "almanak.connectors.across.adapter",
    "stargate": "almanak.connectors.stargate.adapter",
}

EXPECTED_BRIDGE_ADAPTER_ORDER = ("across", "stargate")

EXPECTED_COMPILER_MODULES = {
    "aave_v3": ("almanak.connectors.aave_v3.compiler", "AaveV3Compiler"),
    "across": ("almanak.connectors._strategy_base.bridge_compiler", "BridgeCompiler"),
    "aerodrome": ("almanak.connectors.aerodrome.compiler", "AerodromeCompiler"),
    "aster_perps": ("almanak.connectors.aster_perps.compiler", "AsterPerpsCompiler"),
    "benqi": ("almanak.connectors.benqi.compiler", "BenqiCompiler"),
    "camelot": ("almanak.connectors.camelot.compiler", "CamelotCompiler"),
    "compound_v3": ("almanak.connectors.compound_v3.compiler", "CompoundV3Compiler"),
    "curvance": ("almanak.connectors.curvance.compiler", "CurvanceCompiler"),
    "curve": ("almanak.connectors.curve.compiler", "CurveCompiler"),
    "drift": ("almanak.connectors.drift.compiler", "DriftCompiler"),
    "enso": ("almanak.connectors.enso.compiler", "EnsoCompiler"),
    "ethena": ("almanak.connectors.ethena.compiler", "EthenaCompiler"),
    "euler_v2": ("almanak.connectors.euler_v2.compiler", "EulerV2Compiler"),
    "fluid": ("almanak.connectors.fluid.compiler", "FluidCompiler"),
    # VIB-5031: the vault NFT-CDP surface is a SECOND thin manifest over the
    # fluid package (one codebase, two manifests — ADR r2 Q0).
    "fluid_vault": ("almanak.connectors._fluid_core.vault_compiler", "FluidVaultCompiler"),
    # VIB-5032: the DEX LP (SmartLending) surface is a THIRD thin manifest over
    # the fluid package (one codebase, three manifests).
    "fluid_dex_lp": ("almanak.connectors._fluid_core.dex_lp_compiler", "FluidDexLpCompiler"),
    "gimo": ("almanak.connectors.gimo.compiler", "GimoCompiler"),
    "gmx_v2": ("almanak.connectors.gmx_v2.compiler", "GMXV2Compiler"),
    "hyperliquid": ("almanak.connectors.hyperliquid.compiler", "HyperliquidCompiler"),
    "jupiter": ("almanak.connectors.jupiter.compiler", "JupiterCompiler"),
    "jupiter_lend": ("almanak.connectors.jupiter_lend.compiler", "JupiterLendCompiler"),
    "kamino": ("almanak.connectors.kamino.compiler", "KaminoCompiler"),
    "lido": ("almanak.connectors.lido.compiler", "LidoCompiler"),
    "lifi": ("almanak.connectors.lifi.compiler", "LiFiCompiler"),
    "meteora": ("almanak.connectors.meteora.compiler", "MeteoraCompiler"),
    "morpho_blue": ("almanak.connectors.morpho_blue.compiler", "MorphoBlueCompiler"),
    "morpho_vault": ("almanak.connectors.morpho_vault.compiler", "MorphoVaultCompiler"),
    "orca": ("almanak.connectors.orca.compiler", "OrcaCompiler"),
    "pancakeswap_perps": ("almanak.connectors._aster_perps_core.compiler", "AsterPerpsCompiler"),
    "pancakeswap_v3": ("almanak.connectors.uniswap_v3.compiler", "UniswapV3Compiler"),
    "pendle": ("almanak.connectors.pendle.compiler", "PendleCompiler"),
    "polymarket": ("almanak.connectors.polymarket.compiler", "PolymarketCompiler"),
    "raydium": ("almanak.connectors.raydium.compiler", "RaydiumCompiler"),
    "silo_v2": ("almanak.connectors.silo_v2.compiler", "SiloV2Compiler"),
    "spark": ("almanak.connectors.spark.compiler", "SparkCompiler"),
    "stargate": ("almanak.connectors._strategy_base.bridge_compiler", "BridgeCompiler"),
    "sushiswap_v3": ("almanak.connectors.uniswap_v3.compiler", "UniswapV3Compiler"),
    "traderjoe_v2": ("almanak.connectors.traderjoe_v2.compiler", "TraderJoeV2Compiler"),
    "uniswap_v3": ("almanak.connectors.uniswap_v3.compiler", "UniswapV3Compiler"),
    "uniswap_v4": ("almanak.connectors.uniswap_v4.compiler", "UniswapV4Compiler"),
}

EXPECTED_COMPILER_PROTOCOLS = {
    "aave_v3": ("aave_v3",),
    "across": ("across",),
    "aerodrome": ("aerodrome", "aerodrome_slipstream"),
    "aster_perps": ("aster_perps",),
    "benqi": ("benqi",),
    "camelot": ("camelot",),
    "compound_v3": ("compound_v3",),
    "curvance": ("curvance",),
    "curve": ("curve",),
    "drift": ("drift",),
    "enso": ("enso",),
    "ethena": ("ethena",),
    "euler_v2": ("euler_v2",),
    "fluid": ("fluid", "fluid_lending"),
    "fluid_vault": ("fluid_vault",),
    "fluid_dex_lp": ("fluid_dex_lp",),
    "gimo": ("gimo",),
    "gmx_v2": ("gmx_v2",),
    "hyperliquid": ("hyperliquid",),
    "jupiter": ("jupiter",),
    "jupiter_lend": ("jupiter_lend",),
    "kamino": ("kamino",),
    "lido": ("lido",),
    "lifi": ("lifi",),
    "meteora": ("meteora_dlmm",),
    "morpho_blue": ("morpho", "morpho_blue"),
    "morpho_vault": ("metamorpho", "morpho_vault"),
    "orca": ("orca_whirlpools",),
    "pancakeswap_perps": ("pancakeswap_perps",),
    "pancakeswap_v3": ("pancakeswap_v3",),
    "pendle": ("pendle",),
    "polymarket": ("polymarket",),
    "raydium": ("raydium_clmm",),
    "silo_v2": ("silo_v2",),
    "spark": ("spark",),
    "stargate": ("stargate",),
    "sushiswap_v3": ("sushiswap_v3",),
    "traderjoe_v2": ("traderjoe_v2",),
    "uniswap_v3": ("agni_finance", "uniswap_v3"),
    "uniswap_v4": ("uniswap_v4",),
}

EXPECTED_COMPILER_DEFAULTS = {
    "across": ("BRIDGE",),
    "enso": ("SWAP_CROSS_CHAIN",),
    "polymarket": ("PREDICTION",),
}

EXPECTED_FLASH_LOAN_MODULES = {
    "aave_v3": (
        "aave",
        "almanak.connectors.aave_v3.flash_loan_provider",
        "almanak.connectors.aave_v3.flash_loan",
        True,
    ),
    "balancer_v2": (
        "balancer",
        "almanak.connectors.balancer_v2.flash_loan_provider",
        "almanak.connectors.balancer_v2.flash_loan",
        True,
    ),
    "morpho_blue": (
        "morpho",
        "almanak.connectors.morpho_blue.flash_loan_provider",
        "almanak.connectors.morpho_blue.flash_loan",
        False,
    ),
}

EXPECTED_FLASH_LOAN_ORDER = ("aave_v3", "balancer_v2", "morpho_blue")

EXPECTED_PROTOCOL_FAMILY_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.protocol_family",
    "aerodrome": "almanak.connectors.aerodrome.protocol_family",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.protocol_family",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.protocol_family",
    "uniswap_v3": "almanak.connectors.uniswap_v3.protocol_family",
}

EXPECTED_SWAP_CLASSIFICATION_MODULES = {
    "camelot": "almanak.connectors.camelot.swap_classification",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.swap_classification",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.swap_classification",
    "uniswap_v3": "almanak.connectors.uniswap_v3.swap_classification",
}

EXPECTED_SWAP_CLASSIFICATION_ORDER = (
    "uniswap_v3",
    "sushiswap_v3",
    "pancakeswap_v3",
    "camelot",
)

EXPECTED_AGENT_READ_PROVIDER_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.agent_read_provider",
    "compound_v3": "almanak.connectors.compound_v3.agent_read_provider",
    "morpho_blue": "almanak.connectors.morpho_blue.agent_read_provider",
    "spark": "almanak.connectors.spark.agent_read_provider",
    "aerodrome_slipstream": "almanak.connectors.aerodrome.agent_read_provider",
    "agni_finance": "almanak.connectors.uniswap_v3.agent_read_provider",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.agent_read_provider",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.agent_read_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.agent_read_provider",
}

EXPECTED_AGENT_READ_PROVIDER_ORDER = (
    "uniswap_v3",
    "agni_finance",
    "aerodrome_slipstream",
    "pancakeswap_v3",
    "sushiswap_v3",
    "aave_v3",
    "compound_v3",
    "morpho_blue",
    "spark",
)

EXPECTED_VAULT_TOOL_PROVIDER_MODULES = {
    "lagoon": "almanak.connectors.lagoon.vault_tool_provider",
}

EXPECTED_RUNNER_HOOK_PROVIDER_MODULES = {
    # VIB-5628: builds the sync pool_meta_lookup the receipt parser uses to
    # resolve uncurated Curve pools (MetaRegistry) for leg-labeling.
    "curve": "almanak.connectors.curve.runner_hooks",
    # VIB-5031: stamps FluidVaultOperateData (the nftId home) into
    # extracted_data before the ledger write.
    "fluid_vault": "almanak.connectors._fluid_core.runner_hooks",
    # VIB-5595: reconstructs off-EVM perp fill economics (fee / realized-PnL /
    # funding) from HyperCore userFills/userFunding into extracted_data.
    "hyperliquid": "almanak.connectors.hyperliquid.runner_hooks",
    "uniswap_v3": "almanak.connectors.uniswap_v3.runner_hooks",
    "uniswap_v4": "almanak.connectors.uniswap_v4.runner_hooks",
}

EXPECTED_PROTOCOL_METADATA_MODULES = {
    "pendle": "almanak.connectors.pendle.metadata_provider",
}

EXPECTED_PRINCIPAL_TOKEN_MARKET_READER_MODULES = {
    "pendle": "almanak.connectors.pendle.on_chain_reader_provider",
}

EXPECTED_SWAP_ROUTE_INFERENCE_MODULES = {
    "pendle": "almanak.connectors.pendle.swap_route_inference",
}

EXPECTED_TEARDOWN_POST_CONDITION_MODULES = {
    "gmx_v2": "almanak.connectors.gmx_v2.teardown_post_condition",
    "pendle": "almanak.connectors.pendle.teardown_post_condition",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.teardown_post_condition",
    "uniswap_v4": "almanak.connectors.uniswap_v4.teardown_post_condition",
}

EXPECTED_TEARDOWN_RESIDUAL_DISCOVERY_MODULES = {
    "gmx_v2": "almanak.connectors.gmx_v2.teardown_residual_discovery",
}

EXPECTED_DEFERRED_REFRESH_MODULES = {
    "enso": "almanak.connectors.enso.deferred_refresh_provider",
    "lifi": "almanak.connectors.lifi.deferred_refresh_provider",
}

EXPECTED_SWAP_QUOTE_MODULES = {
    "aerodrome": "almanak.connectors.aerodrome.swap_quote_provider",
    "curve": "almanak.connectors.curve.swap_quote_provider",
    "fluid": "almanak.connectors._fluid_core.swap_quote_provider",
    "uniswap_v3": "almanak.connectors.uniswap_v3.swap_quote_provider",
    "uniswap_v4": "almanak.connectors.uniswap_v4.swap_quote_provider",
}

EXPECTED_ACCOUNTING_TREATMENT_MODULES = {
    "pendle": "almanak.connectors.pendle.accounting_spec",
}

EXPECTED_ACCOUNTING_REPORT_MODULES = {
    "pendle": "almanak.connectors.pendle.reporting",
}

EXPECTED_CONTRACT_MONITORING_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.contract_monitoring",
    "aerodrome": "almanak.connectors.aerodrome.contract_monitoring",
    "gmx_v2": "almanak.connectors.gmx_v2.contract_monitoring",
    "morpho_blue": "almanak.connectors.morpho_blue.contract_monitoring",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.contract_monitoring",
    "pendle": "almanak.connectors.pendle.contract_monitoring",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.contract_monitoring",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.contract_monitoring",
    "uniswap_v3": "almanak.connectors.uniswap_v3.contract_monitoring",
    "uniswap_v4": "almanak.connectors.uniswap_v4.contract_monitoring",
}

EXPECTED_CONTRACT_MONITORING_PROTOCOLS = {
    "aave_v3": ("aave_v3",),
    "aerodrome": ("aerodrome",),
    "gmx_v2": ("gmx_v2",),
    "morpho_blue": ("morpho_blue",),
    "pancakeswap_v3": ("pancakeswap_v3",),
    "pendle": ("pendle",),
    "sushiswap_v3": ("sushiswap_v3",),
    "traderjoe_v2": ("traderjoe_v2",),
    "uniswap_v3": ("uniswap_v3", "agni_finance"),
    "uniswap_v4": ("uniswap_v4",),
}

EXPECTED_ADDRESS_TABLE_MODULES = {
    "aave_v3": "almanak.connectors.aave_v3.addresses",
    "aerodrome": "almanak.connectors.aerodrome.addresses",
    "aster_perps": "almanak.connectors.aster_perps.addresses",
    "balancer_v2": "almanak.connectors.balancer_v2.addresses",
    "camelot": "almanak.connectors.camelot.addresses",
    "compound_v3": "almanak.connectors.compound_v3.addresses",
    "fluid": "almanak.connectors.fluid.addresses",
    "fluid_vault": "almanak.connectors._fluid_core.addresses",
    "fluid_dex_lp": "almanak.connectors._fluid_core.addresses",
    "gmx_v2": "almanak.connectors.gmx_v2.addresses",
    "morpho_blue": "almanak.connectors.morpho_blue.addresses",
    "pancakeswap_perps": "almanak.connectors.pancakeswap_perps.addresses",
    "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.addresses",
    "pendle": "almanak.connectors.pendle.addresses",
    "spark": "almanak.connectors.spark.addresses",
    "sushiswap_v3": "almanak.connectors.sushiswap_v3.addresses",
    "traderjoe_v2": "almanak.connectors.traderjoe_v2.addresses",
    "uniswap_v3": "almanak.connectors.uniswap_v3.addresses",
    "uniswap_v4": "almanak.connectors.uniswap_v4.addresses",
}

EXPECTED_ADDRESS_TABLE_PROTOCOLS = {
    "aave_v3": ("aave_v3", "aave_v3_tokens"),
    "aerodrome": ("aerodrome",),
    "aster_perps": ("aster_perps",),
    "balancer_v2": ("balancer_v2",),
    "camelot": ("camelot",),
    "compound_v3": ("compound_v3",),
    "fluid": ("fluid",),
    "fluid_vault": ("fluid_vault",),
    "fluid_dex_lp": ("fluid_dex_lp",),
    "gmx_v2": ("gmx_v2", "gmx_v2_markets", "gmx_v2_tokens"),
    "morpho_blue": ("morpho_blue",),
    "pancakeswap_perps": ("pancakeswap_perps",),
    "pancakeswap_v3": ("pancakeswap_v3",),
    "pendle": ("pendle",),
    "spark": ("spark",),
    "sushiswap_v3": ("sushiswap_v3",),
    "traderjoe_v2": ("traderjoe_v2",),
    "uniswap_v3": ("uniswap_v3", "agni_finance"),
    "uniswap_v4": ("uniswap_v4",),
}

EXPECTED_SOLANA_PROGRAM_PROTOCOLS = {
    "drift": ("drift",),
    "jupiter": ("jupiter",),
    "kamino": ("kamino",),
    "meteora": ("meteora",),
    "orca": ("metaplex_token_metadata", "orca"),
    "raydium": ("raydium",),
}


@pytest.fixture(autouse=True)
def _isolate_strategy_connector_registry() -> Iterator[None]:
    """Keep descriptor provider imports from leaking strategy registry state."""
    AddressRegistry.reset_cache()
    CompilerRegistry.reset_cache()
    StrategyConnectorRegistry._clear()
    yield
    AddressRegistry.reset_cache()
    CompilerRegistry.reset_cache()
    StrategyConnectorRegistry._clear()


def test_descriptor_names_alias_connector_interface() -> None:
    """Legacy descriptor names remain aliases for the canonical Connector API."""
    assert ConnectorDescriptor is Connector
    assert ConnectorDescriptorRegistry is ConnectorRegistry
    assert CONNECTOR_DESCRIPTOR_REGISTRY is CONNECTOR_REGISTRY


def test_connector_accepts_strategy_support_metadata() -> None:
    """Connector manifests can own strategy registry metadata without framework imports."""
    connector = Connector(
        name="strategy_supported",
        kind=ProtocolKind.SWAP,
        strategy_intents=("SWAP",),
        strategy_chains=("ethereum",),
        strategy_matrix_entries=(
            StrategyMatrixEntry(
                matrix_name="strategy_supported",
                category="swap",
                chains=frozenset({"ethereum"}),
            ),
        ),
    )

    assert connector.has_strategy_support is True
    assert connector.strategy_intents == ("SWAP",)
    assert connector.strategy_chains == ("ethereum",)


def test_connector_rejects_strategy_chains_without_strategy_intents() -> None:
    """Strategy chains are meaningful only when the connector declares intents."""
    with pytest.raises(ValueError, match="strategy_chains"):
        Connector(
            name="bad_strategy_chains",
            kind=ProtocolKind.SWAP,
            strategy_chains=("ethereum",),
        )


def test_connector_rejects_duplicate_strategy_intents() -> None:
    """Descriptor-owned strategy intents must stay unambiguous."""
    with pytest.raises(ValueError, match="strategy_intents contains duplicates"):
        Connector(
            name="bad_strategy_intents",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP", "SWAP"),
            strategy_chains=("ethereum",),
        )


def test_connector_registry_filters_strategy_support() -> None:
    """Descriptor registry exposes connectors that own strategy support."""
    registry = ConnectorRegistry()
    connectors = (
        Connector(name="without_strategy", kind=ProtocolKind.SWAP),
        Connector(
            name="with_strategy",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP",),
            strategy_chains=("ethereum",),
        ),
    )

    registry._connectors = connectors

    assert registry.with_strategy_support() == (connectors[1],)


def test_migrated_strategy_registration_is_descriptor_owned() -> None:
    """Migrated canary connectors publish strategy support without legacy init calls."""
    repo_root = Path(__file__).resolve().parents[3]
    CONNECTOR_REGISTRY.clear()
    connectors = {connector.name: connector for connector in CONNECTOR_REGISTRY.with_strategy_support()}

    for name, (intents, chains) in MIGRATED_STRATEGY_REGISTRATION.items():
        connector = connectors[name]
        assert connector.strategy_intents == intents
        assert connector.strategy_chains == chains
        if name in EXPECTED_STRATEGY_MATRIX_ENTRIES:
            assert connector.strategy_matrix_entries == EXPECTED_STRATEGY_MATRIX_ENTRIES[name]

        init_py = repo_root / "almanak" / "connectors" / name / "__init__.py"
        assert "register_connector(" not in init_py.read_text(encoding="utf-8")


def test_connector_inits_do_not_call_legacy_strategy_registration() -> None:
    """Connector package init files are lazy export surfaces, not registration owners."""
    repo_root = Path(__file__).resolve().parents[3]
    connector_root = repo_root / "almanak" / "connectors"
    offenders: list[str] = []

    for init_py in sorted(connector_root.glob("*/__init__.py")):
        if init_py.parent.name.startswith("_"):
            continue
        tree = ast.parse(init_py.read_text(encoding="utf-8"), filename=str(init_py))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            is_legacy_call = (isinstance(func, ast.Name) and func.id == "register_connector") or (
                isinstance(func, ast.Attribute) and func.attr == "register_connector"
            )
            if is_legacy_call:
                offenders.append(str(init_py.relative_to(repo_root)))

    assert offenders == []


def test_discovers_migrated_connectors() -> None:
    """Discovery finds connector-owned manifests without central imports."""
    CONNECTOR_REGISTRY.clear()

    connectors = {connector.name: connector for connector in CONNECTOR_REGISTRY.all()}

    assert set(EXPECTED_CONNECTOR_KINDS) <= connectors.keys()
    for name, kind in EXPECTED_CONNECTOR_KINDS.items():
        assert isinstance(connectors[name], Connector)
        assert connectors[name].kind is kind
    for name, aliases in EXPECTED_ALIASES.items():
        assert connectors[name].aliases == aliases
    assert connectors["morpho_vault"].receipt_parser_protocols == ("metamorpho",)
    assert connectors["morpho_vault"].receipt_parser_keys == frozenset({"metamorpho"})


def test_manifest_discovery_does_not_import_connector_package(monkeypatch: pytest.MonkeyPatch) -> None:
    """Manifest discovery avoids connector package side effects."""
    import sys

    import almanak.connectors as connectors_package

    CONNECTOR_REGISTRY.clear()
    sys.modules.pop("almanak.connectors.pancakeswap_perps", None)
    sys.modules.pop("almanak.connectors.pancakeswap_perps.connector", None)
    monkeypatch.delattr(connectors_package, "pancakeswap_perps", raising=False)

    connector_manifest = CONNECTOR_REGISTRY.get("pancakeswap_perps")

    assert connector_manifest is not None
    assert connector_manifest.name == "pancakeswap_perps"
    assert "almanak.connectors.pancakeswap_perps" not in sys.modules


def test_connector_registry_rejects_reentrant_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recursive manifest discovery fails with an actionable registry error."""
    registry = ConnectorRegistry()

    def reenter_discovery() -> tuple[Connector, ...]:
        return registry.all()

    monkeypatch.setattr(registry, "_discover", reenter_discovery)

    with pytest.raises(ConnectorDiscoveryError, match="recursive connector discovery"):
        registry.all()

    monkeypatch.setattr(registry, "_discover", lambda: ())
    assert registry.all() == ()


@pytest.mark.parametrize(
    ("capability", "connector_kwargs"),
    (
        (
            "Gateway connector",
            lambda name: {
                "kind": ProtocolKind.LP,
                "gateway_connector": ImportRef(module=f"tests.fake_{name}", attribute="Gateway", order=7),
            },
        ),
        (
            "Gateway settings",
            lambda name: {
                "kind": ProtocolKind.SWAP,
                "gateway_settings": ImportRef(module=f"tests.fake_{name}", attribute="Settings", order=7),
            },
        ),
        (
            "Contract-role",
            lambda name: {
                "kind": ProtocolKind.LP,
                "contract_roles": ImportRef(module=f"tests.fake_{name}", attribute="CONTRACT_ROLES", order=7),
            },
        ),
        (
            "Swap-classification",
            lambda name: {
                "kind": ProtocolKind.SWAP,
                "swap_classification": ImportRef(
                    module=f"tests.fake_{name}",
                    attribute="SWAP_CLASSIFICATION",
                    order=7,
                ),
            },
        ),
        (
            "Bridge adapter",
            lambda name: {
                "kind": ProtocolKind.BRIDGE,
                "bridge_adapter": ImportRef(module=f"tests.fake_{name}", attribute="BridgeAdapter", order=7),
            },
        ),
        (
            "Flash-loan provider",
            lambda name: {
                "kind": ProtocolKind.LENDING,
                "flash_loan_provider_name": name,
                "flash_loan_provider": ImportRef(module=f"tests.fake_{name}", attribute="FlashLoanProvider", order=7),
                "flash_loan_builder": ImportRef(module=f"tests.fake_{name}", attribute="build_flash_loan"),
            },
        ),
    ),
)
def test_connector_registry_rejects_duplicate_order_bearing_refs(
    monkeypatch: pytest.MonkeyPatch,
    capability: str,
    connector_kwargs: object,
) -> None:
    """Discovery rejects duplicate explicit order keys before stable-sort tie breaking."""
    registry = ConnectorRegistry()
    kwargs_for_name = connector_kwargs
    assert callable(kwargs_for_name)
    connectors = {name: Connector(name=name, **kwargs_for_name(name)) for name in ("alpha", "beta")}

    monkeypatch.setattr(
        connector_descriptor_module.pkgutil,
        "iter_modules",
        lambda _paths: iter(SimpleNamespace(ispkg=True, name=name) for name in connectors),
    )
    monkeypatch.setattr(registry, "_load_connector", lambda name: connectors[name])

    with pytest.raises(
        ConnectorDiscoveryError,
        match=rf"{capability} order 7 is claimed by both 'alpha' and 'beta'",
    ):
        registry.all()


def test_connector_registry_rejects_duplicate_compiler_protocol_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two connectors cannot claim the same compiler protocol key."""
    registry = ConnectorRegistry()
    protocols = {"alpha": ("shared-protocol ",), "beta": (" shared_protocol",)}
    connectors = {
        name: Connector(
            name=name,
            kind=ProtocolKind.SWAP,
            compiler=ImportRef(module=f"tests.fake_{name}", attribute="Compiler"),
            compiler_protocols=protocols[name],
        )
        for name in ("alpha", "beta")
    }

    monkeypatch.setattr(
        connector_descriptor_module.pkgutil,
        "iter_modules",
        lambda _paths: iter(SimpleNamespace(ispkg=True, name=name) for name in connectors),
    )
    monkeypatch.setattr(registry, "_load_connector", lambda name: connectors[name])

    with pytest.raises(
        ConnectorDiscoveryError,
        match=r"Compiler protocol 'shared_protocol' is claimed by both 'alpha' and 'beta'",
    ):
        registry.all()


def test_connector_registry_rejects_duplicate_compiler_default_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two connectors cannot claim the same compiler dispatch default key."""
    registry = ConnectorRegistry()
    defaults = {"alpha": (" prediction ",), "beta": ("PREDICTION",)}
    connectors = {
        name: Connector(
            name=name,
            kind=ProtocolKind.SWAP,
            compiler=ImportRef(module=f"tests.fake_{name}", attribute="Compiler"),
            compiler_default_keys=defaults[name],
        )
        for name in ("alpha", "beta")
    }

    monkeypatch.setattr(
        connector_descriptor_module.pkgutil,
        "iter_modules",
        lambda _paths: iter(SimpleNamespace(ispkg=True, name=name) for name in connectors),
    )
    monkeypatch.setattr(registry, "_load_connector", lambda name: connectors[name])

    with pytest.raises(
        ConnectorDiscoveryError,
        match=r"Compiler default key 'PREDICTION' is claimed by both 'alpha' and 'beta'",
    ):
        registry.all()


def test_connector_rejects_invalid_gateway_connector_ref() -> None:
    """Invalid singular gateway refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.gateway_connector"):
        Connector(
            name="bad_gateway_ref",
            kind=ProtocolKind.LP,
            gateway_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_accepts_gateway_settings_ref() -> None:
    """Connector manifests can own gateway settings fragments."""
    connector = Connector(
        name="settings_supported",
        kind=ProtocolKind.SWAP,
        gateway_settings=ImportRef(
            module="tests.fake_gateway_settings",
            attribute="FakeGatewaySettings",
            order=3,
        ),
    )

    assert connector.gateway_settings is not None
    assert connector.gateway_settings.module == "tests.fake_gateway_settings"
    assert connector.gateway_settings.attribute == "FakeGatewaySettings"
    assert connector.gateway_settings.order == 3


def test_connector_rejects_invalid_gateway_settings_ref() -> None:
    """Invalid gateway-settings refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.gateway_settings"):
        Connector(
            name="bad_gateway_settings_ref",
            kind=ProtocolKind.SWAP,
            gateway_settings=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_address_tables_ref() -> None:
    """Invalid address-table refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.address_tables"):
        Connector(
            name="bad_address_tables_ref",
            kind=ProtocolKind.LP,
            address_tables=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_solana_programs_ref() -> None:
    """Invalid Solana program specs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.solana_programs"):
        Connector(
            name="bad_solana_programs_ref",
            kind=ProtocolKind.LP,
            solana_programs=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_duplicate_solana_program_ids() -> None:
    """A single connector may not declare the same Solana program ID twice."""
    shared_program_id = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"
    with pytest.raises(ValueError, match="Connector.solana_programs contains duplicate program IDs"):
        Connector(
            name="dup_solana_program_ids",
            kind=ProtocolKind.LP,
            solana_programs=(
                SolanaProgramSpec(protocol="alpha", program_id=shared_program_id),
                SolanaProgramSpec(protocol="beta", program_id=shared_program_id),
            ),
        )


def test_solana_program_spec_rejects_whitespace_program_id() -> None:
    """Base58 program IDs never contain whitespace; a stray newline is a copy-paste error."""
    with pytest.raises(ValueError, match="must not contain whitespace"):
        SolanaProgramSpec(protocol="drift", program_id="dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH\n")


def test_connector_rejects_invalid_teardown_post_condition_ref() -> None:
    """Invalid teardown post-condition refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.teardown_post_condition"):
        Connector(
            name="bad_teardown_post_condition_ref",
            kind=ProtocolKind.LP,
            teardown_post_condition=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_teardown_residual_discovery_ref() -> None:
    """Invalid teardown residual-discovery refs fail during manifest validation (VIB-5116 S1)."""
    with pytest.raises(ValueError, match="Connector.teardown_residual_discovery"):
        Connector(
            name="bad_teardown_residual_discovery_ref",
            kind=ProtocolKind.PERP,
            teardown_residual_discovery=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_receipt_parser_connector_ref() -> None:
    """Invalid singular receipt-parser refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.receipt_parser_connector"):
        Connector(
            name="bad_receipt_ref",
            kind=ProtocolKind.LP,
            receipt_parser_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_gas_estimate_connector_ref() -> None:
    """Invalid gas-estimate refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.gas_estimate_connector"):
        Connector(
            name="bad_gas_ref",
            kind=ProtocolKind.LP,
            gas_estimate_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_agent_read_connector_ref() -> None:
    """Invalid agent-read refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.agent_read_connector"):
        Connector(
            name="bad_agent_read_ref",
            kind=ProtocolKind.LP,
            agent_read_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_vault_tool_connector_ref() -> None:
    """Invalid vault-tool refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.vault_tool_connector"):
        Connector(
            name="bad_vault_tool_ref",
            kind=ProtocolKind.VAULT,
            vault_tool_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_runner_hook_connector_ref() -> None:
    """Invalid runner-hook refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.runner_hook_connector"):
        Connector(
            name="bad_runner_hook_ref",
            kind=ProtocolKind.LP,
            runner_hook_connector=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_protocol_metadata_ref() -> None:
    """Invalid protocol-metadata refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.protocol_metadata"):
        Connector(
            name="bad_protocol_metadata_ref",
            kind=ProtocolKind.YIELD_TRADING,
            protocol_metadata=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_principal_token_market_reader_ref() -> None:
    """Invalid principal-token reader refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.principal_token_market_reader"):
        Connector(
            name="bad_principal_token_market_reader_ref",
            kind=ProtocolKind.YIELD_TRADING,
            principal_token_market_reader=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_swap_route_inference_ref() -> None:
    """Invalid swap-route inference refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.swap_route_inference"):
        Connector(
            name="bad_swap_route_inference_ref",
            kind=ProtocolKind.YIELD_TRADING,
            swap_route_inference=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_deferred_refresh_ref() -> None:
    """Invalid deferred-refresh refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.deferred_refresh"):
        Connector(
            name="bad_deferred_refresh_ref",
            kind=ProtocolKind.SWAP,
            deferred_refresh=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_accounting_treatment_ref() -> None:
    """Invalid accounting-treatment refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.accounting_treatment"):
        Connector(
            name="bad_accounting_treatment_ref",
            kind=ProtocolKind.YIELD_TRADING,
            accounting_treatment=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_accounting_report_ref() -> None:
    """Invalid accounting-report refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.accounting_report"):
        Connector(
            name="bad_accounting_report_ref",
            kind=ProtocolKind.YIELD_TRADING,
            accounting_report=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_protocol_family_ref() -> None:
    """Invalid protocol-family refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.protocol_family"):
        Connector(
            name="bad_protocol_family_ref",
            kind=ProtocolKind.LP,
            protocol_family=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_swap_classification_ref() -> None:
    """Invalid swap-classification refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.swap_classification"):
        Connector(
            name="bad_swap_classification_ref",
            kind=ProtocolKind.SWAP,
            swap_classification=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_contract_monitoring_ref() -> None:
    """Invalid contract-monitoring refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.contract_monitoring"):
        Connector(
            name="bad_contract_monitoring_ref",
            kind=ProtocolKind.YIELD_TRADING,
            contract_monitoring=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_contract_roles_ref() -> None:
    """Invalid contract-role refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.contract_roles"):
        Connector(
            name="bad_contract_roles_ref",
            kind=ProtocolKind.LP,
            contract_roles=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_permission_infrastructure_ref() -> None:
    """Invalid infrastructure-permission refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.permission_infrastructure"):
        Connector(
            name="bad_permission_infrastructure",
            kind=ProtocolKind.SWAP,
            permission_infrastructure=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_invalid_bridge_adapter_ref() -> None:
    """Invalid bridge-adapter refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.bridge_adapter"):
        Connector(
            name="bad_bridge_adapter_ref",
            kind=ProtocolKind.BRIDGE,
            bridge_adapter=object(),  # type: ignore[arg-type]
        )


def test_connector_accepts_compiler_metadata() -> None:
    """Connector manifests can own compiler refs and dispatch defaults."""
    connector = Connector(
        name="compiler_supported",
        kind=ProtocolKind.SWAP,
        compiler=ImportRef(module="tests.fake_compiler", attribute="FakeCompiler"),
        compiler_protocols=("compiler_supported", "compiler_alias"),
        compiler_default_keys=("SWAP_CROSS_CHAIN",),
    )

    assert connector.compiler is not None
    assert connector.compiler_keys == frozenset({"compiler_supported", "compiler_alias"})
    assert connector.compiler_default_keys == ("SWAP_CROSS_CHAIN",)


def test_connector_rejects_invalid_compiler_ref() -> None:
    """Invalid compiler refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.compiler"):
        Connector(
            name="bad_compiler_ref",
            kind=ProtocolKind.SWAP,
            compiler=object(),  # type: ignore[arg-type]
        )


def test_connector_rejects_compiler_protocols_without_compiler() -> None:
    """Compiler protocol keys only make sense with a compiler ref."""
    with pytest.raises(ValueError, match="compiler_protocols"):
        Connector(
            name="bad_compiler_protocols",
            kind=ProtocolKind.SWAP,
            compiler_protocols=("bad_compiler_protocols",),
        )


def test_connector_rejects_duplicate_compiler_protocols() -> None:
    """Descriptor-owned compiler protocol keys must stay unambiguous."""
    with pytest.raises(ValueError, match="compiler_protocols contains duplicates"):
        Connector(
            name="bad_compiler_protocols",
            kind=ProtocolKind.SWAP,
            compiler=ImportRef(module="tests.fake_compiler", attribute="FakeCompiler"),
            compiler_protocols=("bad", "bad"),
        )


def test_connector_rejects_compiler_default_keys_without_compiler() -> None:
    """Default dispatch keys only make sense with a compiler ref."""
    with pytest.raises(ValueError, match="compiler_default_keys"):
        Connector(
            name="bad_compiler_default",
            kind=ProtocolKind.SWAP,
            compiler_default_keys=("PREDICTION",),
        )


def test_connector_rejects_invalid_flash_loan_provider_ref() -> None:
    """Invalid flash-loan provider refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.flash_loan_provider"):
        Connector(
            name="bad_flash_loan_provider_ref",
            kind=ProtocolKind.LENDING,
            flash_loan_provider_name="bad",
            flash_loan_provider=object(),  # type: ignore[arg-type]
            flash_loan_builder=ImportRef(module="tests.fake_flash_loan", attribute="build_fake_flash_loan"),
        )


def test_connector_rejects_invalid_flash_loan_builder_ref() -> None:
    """Invalid flash-loan builder refs fail during manifest validation."""
    with pytest.raises(ValueError, match="Connector.flash_loan_builder"):
        Connector(
            name="bad_flash_loan_builder_ref",
            kind=ProtocolKind.LENDING,
            flash_loan_provider_name="bad",
            flash_loan_provider=ImportRef(module="tests.fake_flash_loan_provider", attribute="FakeProvider"),
            flash_loan_builder=object(),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    (
        (
            {
                "flash_loan_provider": ImportRef(
                    module="tests.fake_flash_loan_provider",
                    attribute="FakeProvider",
                ),
                "flash_loan_builder": ImportRef(
                    module="tests.fake_flash_loan",
                    attribute="build_fake_flash_loan",
                ),
            },
            "Connector.flash_loan_provider_name",
        ),
        (
            {
                "flash_loan_synthetic_discovery": True,
            },
            "Connector.flash_loan_provider_name",
        ),
        (
            {
                "flash_loan_provider_name": "fake",
                "flash_loan_builder": ImportRef(
                    module="tests.fake_flash_loan",
                    attribute="build_fake_flash_loan",
                ),
            },
            "Connector.flash_loan_provider is required",
        ),
        (
            {
                "flash_loan_provider_name": "fake",
                "flash_loan_provider": ImportRef(
                    module="tests.fake_flash_loan_provider",
                    attribute="FakeProvider",
                ),
            },
            "Connector.flash_loan_builder is required",
        ),
    ),
)
def test_connector_rejects_partial_flash_loan_metadata(kwargs: dict[str, object], match: str) -> None:
    """Flash-loan manifest fields must be published as a complete set."""
    with pytest.raises(ValueError, match=match):
        Connector(
            name="partial_flash_loan",
            kind=ProtocolKind.LENDING,
            **kwargs,
        )


def test_connector_rejects_invalid_flash_loan_synthetic_discovery_flag() -> None:
    """Flash-loan synthetic-discovery opt-in must be boolean."""
    with pytest.raises(ValueError, match="Connector.flash_loan_synthetic_discovery"):
        Connector(
            name="bad_flash_loan_synthetic_discovery",
            kind=ProtocolKind.LENDING,
            flash_loan_synthetic_discovery="yes",  # type: ignore[arg-type]
        )


def test_receipt_parser_connectors_instantiate_from_descriptors() -> None:
    """Migrated receipt-parser providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    for name, module in EXPECTED_RECEIPT_PROVIDER_MODULES.items():
        connector_manifest = CONNECTOR_REGISTRY.get(name)

        assert connector_manifest is not None
        assert connector_manifest.receipt_parser_connector is not None
        connector = connector_manifest.receipt_parser_connector.instantiate()

        assert isinstance(connector, ReceiptParserConnector)
        assert str(connector.protocol) in connector_manifest.receipt_parser_keys
        assert connector.receipt_parser_keys() == connector_manifest.receipt_parser_keys
        assert type(connector).__module__ == module


def test_gas_estimate_connectors_instantiate_from_descriptors() -> None:
    """Migrated gas-estimate providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    for name, module in EXPECTED_GAS_ESTIMATE_PROVIDER_MODULES.items():
        connector_manifest = CONNECTOR_REGISTRY.get(name)

        assert connector_manifest is not None
        assert connector_manifest.gas_estimate_connector is not None
        connector = connector_manifest.gas_estimate_connector.instantiate()

        assert isinstance(connector, GasEstimateConnector)
        assert type(connector).__module__ == module


def test_contract_role_specs_load_from_descriptors() -> None:
    """Migrated contract-role specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[tuple[ContractRoleSpec, ...], str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_contract_roles():
        assert connector_manifest.contract_roles is not None
        specs = connector_manifest.contract_roles.load()
        assert isinstance(specs, tuple)
        assert specs
        assert all(isinstance(spec, ContractRoleSpec) for spec in specs)
        connectors[connector_manifest.name] = (specs, connector_manifest.contract_roles.module)
        orders[connector_manifest.name] = connector_manifest.contract_roles.order

    assert set(EXPECTED_CONTRACT_ROLE_MODULES) == connectors.keys()
    for name, module in EXPECTED_CONTRACT_ROLE_MODULES.items():
        _specs, actual_module = connectors[name]

        assert actual_module == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_CONTRACT_ROLE_ORDER
    )


def test_permission_infrastructure_builders_load_from_descriptors() -> None:
    """Migrated infrastructure-permission hooks are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, str] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_permission_infrastructure():
        assert connector_manifest.permission_infrastructure is not None
        builder = connector_manifest.permission_infrastructure.load()
        assert callable(builder)
        permissions = builder("arbitrum")
        assert isinstance(permissions, list)
        assert permissions
        assert all(isinstance(permission, ContractPermission) for permission in permissions)
        connectors[connector_manifest.name] = connector_manifest.permission_infrastructure.module

    assert EXPECTED_PERMISSION_INFRASTRUCTURE_MODULES == connectors


def test_bridge_adapters_load_from_descriptors() -> None:
    """Migrated bridge adapters are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    adapters: dict[str, tuple[type[BridgeAdapter], str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_bridge_adapter():
        assert connector_manifest.bridge_adapter is not None
        adapter_cls = connector_manifest.bridge_adapter.load()
        assert isinstance(adapter_cls, type)
        assert issubclass(adapter_cls, BridgeAdapter)
        adapters[connector_manifest.name] = (adapter_cls, connector_manifest.bridge_adapter.module)
        orders[connector_manifest.name] = connector_manifest.bridge_adapter.order

    assert set(EXPECTED_BRIDGE_ADAPTER_MODULES) == adapters.keys()
    for name, module in EXPECTED_BRIDGE_ADAPTER_MODULES.items():
        adapter_cls, actual_module = adapters[name]

        assert actual_module == module
        assert adapter_cls.__module__ == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_BRIDGE_ADAPTER_ORDER
    )


def test_compilers_load_from_descriptors() -> None:
    """Connector compiler loaders are published through manifests."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[str, str, frozenset[str], tuple[str, ...]]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_compiler():
        assert connector_manifest.compiler is not None
        compiler_cls = connector_manifest.compiler.load()

        assert isinstance(compiler_cls, type)
        assert issubclass(compiler_cls, BaseProtocolCompiler)
        connectors[connector_manifest.name] = (
            connector_manifest.compiler.module,
            connector_manifest.compiler.attribute,
            connector_manifest.compiler_keys,
            connector_manifest.compiler_default_keys,
        )

    assert set(EXPECTED_COMPILER_MODULES) == connectors.keys()
    for name, (expected_module, expected_attribute) in EXPECTED_COMPILER_MODULES.items():
        actual_module, actual_attribute, actual_protocols, actual_defaults = connectors[name]

        assert actual_module == expected_module
        assert actual_attribute == expected_attribute
        assert actual_protocols == frozenset(EXPECTED_COMPILER_PROTOCOLS[name])
        assert actual_defaults == EXPECTED_COMPILER_DEFAULTS.get(name, ())


def test_compiler_registry_is_manifest_owned() -> None:
    """The compiler registry composes descriptors instead of naming connectors."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_base/compiler_registry.py").read_text()

    assert "_BUILTIN_LOADERS" not in source
    assert "_DEFAULT_BY_KEY" not in source
    for module, _attribute in EXPECTED_COMPILER_MODULES.values():
        if "._strategy_base." in module:
            continue
        assert module not in source

    assert CompilerRegistry.supported_protocols() == tuple(
        sorted(protocol for protocols in EXPECTED_COMPILER_PROTOCOLS.values() for protocol in protocols)
    )
    assert CompilerRegistry.default_protocol("BRIDGE") == "across"
    assert CompilerRegistry.default_protocol("SWAP_CROSS_CHAIN") == "enso"
    assert CompilerRegistry.default_protocol("PREDICTION") == "polymarket"


def test_flash_loan_providers_load_from_descriptors() -> None:
    """Migrated flash-loan providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    providers: dict[str, tuple[str, type[FlashLoanProvider], str, str, bool]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_flash_loan():
        assert connector_manifest.flash_loan_provider_name is not None
        assert connector_manifest.flash_loan_provider is not None
        assert connector_manifest.flash_loan_builder is not None

        provider_cls = connector_manifest.flash_loan_provider.load()
        builder = connector_manifest.flash_loan_builder.load()

        assert isinstance(provider_cls, type)
        assert issubclass(provider_cls, FlashLoanProvider)
        assert callable(builder)

        provider = provider_cls()
        assert provider.name == connector_manifest.flash_loan_provider_name
        providers[connector_manifest.name] = (
            connector_manifest.flash_loan_provider_name,
            provider_cls,
            connector_manifest.flash_loan_provider.module,
            connector_manifest.flash_loan_builder.module,
            connector_manifest.flash_loan_synthetic_discovery,
        )
        orders[connector_manifest.name] = connector_manifest.flash_loan_provider.order

    assert set(EXPECTED_FLASH_LOAN_MODULES) == providers.keys()
    for name, (
        expected_provider_name,
        expected_provider_module,
        expected_builder_module,
        expected_synthetic,
    ) in EXPECTED_FLASH_LOAN_MODULES.items():
        provider_name, provider_cls, actual_provider_module, actual_builder_module, actual_synthetic = providers[name]

        assert provider_name == expected_provider_name
        assert actual_provider_module == expected_provider_module
        assert actual_builder_module == expected_builder_module
        assert actual_synthetic is expected_synthetic
        assert provider_cls.__module__ == expected_provider_module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_FLASH_LOAN_ORDER
    )


def test_protocol_family_specs_load_from_descriptors() -> None:
    """Migrated protocol-family specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    for name, module in EXPECTED_PROTOCOL_FAMILY_MODULES.items():
        connector_manifest = CONNECTOR_REGISTRY.get(name)

        assert connector_manifest is not None
        assert connector_manifest.protocol_family is not None
        spec = connector_manifest.protocol_family.load()

        assert isinstance(spec, ProtocolFamilySpec)
        assert spec.families
        assert connector_manifest.protocol_family.module == module


def test_swap_classification_specs_load_from_descriptors() -> None:
    """Migrated swap-classification specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[tuple[SwapClassificationSpec, ...], str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_classification():
        assert connector_manifest.swap_classification is not None
        specs = connector_manifest.swap_classification.load()
        assert isinstance(specs, tuple)
        assert specs
        assert all(isinstance(spec, SwapClassificationSpec) for spec in specs)
        connectors[connector_manifest.name] = (specs, connector_manifest.swap_classification.module)
        orders[connector_manifest.name] = connector_manifest.swap_classification.order

    assert set(EXPECTED_SWAP_CLASSIFICATION_MODULES) == connectors.keys()
    for name, module in EXPECTED_SWAP_CLASSIFICATION_MODULES.items():
        _specs, actual_module = connectors[name]

        assert actual_module == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_SWAP_CLASSIFICATION_ORDER
    )


def test_address_table_specs_load_from_descriptors() -> None:
    """Migrated address-table specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[tuple[AddressTableSpec, ...], str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_address_tables():
        assert connector_manifest.address_tables is not None
        specs = connector_manifest.address_tables
        modules = {spec.module for spec in specs}

        assert specs
        assert all(isinstance(spec, AddressTableSpec) for spec in specs)
        assert len(modules) == 1
        connectors[connector_manifest.name] = (specs, next(iter(modules)))

    assert set(EXPECTED_ADDRESS_TABLE_MODULES) == connectors.keys()
    for name, module in EXPECTED_ADDRESS_TABLE_MODULES.items():
        specs, actual_module = connectors[name]

        assert actual_module == module
        assert {spec.protocol for spec in specs} == set(EXPECTED_ADDRESS_TABLE_PROTOCOLS[name])

        for spec in specs:
            table = spec.load_table()
            assert table
            assert all(
                isinstance(chain, str) and isinstance(contracts, dict) and contracts
                for chain, contracts in table.items()
            )


def test_connector_address_tables_are_not_hardcoded_in_address_registry() -> None:
    """The address registry discovers manifests instead of naming connectors."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_base/address_registry.py").read_text()

    assert "_BUILTIN_LOADERS" not in source
    assert "_ABI_FAMILIES" not in source
    for module in EXPECTED_ADDRESS_TABLE_MODULES.values():
        assert module not in source


def test_vault_representative_specs_load_from_descriptors() -> None:
    """Representative vault metadata is published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[str, ...]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_vault_representatives():
        assert connector_manifest.vault_representatives is not None
        specs = connector_manifest.vault_representatives

        assert specs
        assert all(isinstance(spec, VaultRepresentativeSpec) for spec in specs)
        connectors[connector_manifest.name] = tuple(spec.protocol for spec in specs)
        for spec in specs:
            table = spec.load_table()
            assert table
            assert all({"vault", "underlying"} <= set(row) for row in table.values())

    assert connectors == {"morpho_vault": ("metamorpho",)}


def test_vault_representative_spec_rejects_blank_row_values(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.fake_blank_vault_representatives"
    monkeypatch.setitem(
        sys.modules,
        module_name,
        SimpleNamespace(TABLE={"base": {"vault": " ", "underlying": "0x" + "11" * 20}}),
    )
    spec = VaultRepresentativeSpec(protocol="fake", module=module_name, attribute="TABLE")

    with pytest.raises(TypeError, match="invalid row"):
        spec.load_table()


def test_vault_representative_spec_rejects_duplicate_normalized_chains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "tests.fake_duplicate_vault_representatives"
    monkeypatch.setitem(
        sys.modules,
        module_name,
        SimpleNamespace(
            TABLE={
                "Base": {"vault": "0x" + "11" * 20, "underlying": "0x" + "22" * 20},
                " base ": {"vault": "0x" + "33" * 20, "underlying": "0x" + "44" * 20},
            }
        ),
    )
    spec = VaultRepresentativeSpec(protocol="fake", module=module_name, attribute="TABLE")

    with pytest.raises(ValueError, match="duplicate chain"):
        spec.load_table()


def test_vault_representatives_are_not_hardcoded_in_framework_permissions() -> None:
    """The permission layer discovers representative vaults instead of naming connectors."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/permissions/constants.py").read_text()

    assert "almanak.connectors.morpho_vault" not in source


def test_solana_program_specs_load_from_descriptors() -> None:
    """Solana validator clone specs are published through connector manifests."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[str, ...]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_solana_programs():
        assert connector_manifest.solana_programs is not None
        specs = connector_manifest.solana_programs

        assert specs
        assert all(isinstance(spec, SolanaProgramSpec) for spec in specs)
        # program_id is required; notes is optional (defaults to "") per the spec contract.
        assert all(spec.program_id for spec in specs)
        assert all(isinstance(spec.notes, str) for spec in specs)
        connectors[connector_manifest.name] = tuple(spec.protocol for spec in specs)

    assert connectors == EXPECTED_SOLANA_PROGRAM_PROTOCOLS


def test_solana_program_specs_are_not_hardcoded_in_framework_registry() -> None:
    """The framework Solana program registry discovers manifests instead of naming connectors."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/anvil/solana_program_registry.py").read_text()

    assert "with_solana_programs()" in source
    assert "almanak.connectors.drift" not in source
    assert "almanak.connectors.jupiter" not in source
    assert "almanak.connectors.kamino" not in source
    assert "almanak.connectors.meteora" not in source
    assert "almanak.connectors.orca" not in source
    assert "almanak.connectors.raydium" not in source


def test_contract_monitoring_specs_load_from_descriptors() -> None:
    """Migrated contract-monitoring specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[tuple[ContractMonitoringSpec, ...], str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_contract_monitoring():
        assert connector_manifest.contract_monitoring is not None
        loaded = connector_manifest.contract_monitoring.load()
        specs = (loaded,) if isinstance(loaded, ContractMonitoringSpec) else loaded

        assert isinstance(specs, tuple)
        assert specs
        assert all(isinstance(spec, ContractMonitoringSpec) for spec in specs)
        connectors[connector_manifest.name] = (specs, connector_manifest.contract_monitoring.module)

    assert set(EXPECTED_CONTRACT_MONITORING_MODULES) == connectors.keys()
    for name, module in EXPECTED_CONTRACT_MONITORING_MODULES.items():
        specs, actual_module = connectors[name]

        assert actual_module == module
        assert {spec.protocol for spec in specs} == set(EXPECTED_CONTRACT_MONITORING_PROTOCOLS[name])

        for spec in specs:
            matches = []
            for chain in AddressRegistry.address_supported_chains(spec.protocol):
                contracts = AddressRegistry.addresses_for(spec.protocol, chain)
                matches.extend(spec.matching_contracts(contracts))
            assert matches, f"{name} contract-monitoring spec matched no addresses: {spec!r}"


def test_agent_read_connectors_instantiate_from_descriptors() -> None:
    """Migrated agent-read providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[AgentReadConnector, str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.agent_read_connector_refs:
            connector = import_ref.instantiate()
            connectors[str(connector.protocol)] = (connector, import_ref.module)
            orders[str(connector.protocol)] = import_ref.order

    assert set(EXPECTED_AGENT_READ_PROVIDER_MODULES) == connectors.keys()
    for name, module in EXPECTED_AGENT_READ_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, AgentReadConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module
    assert (
        tuple(
            name
            for name, _order in sorted(
                orders.items(),
                key=lambda item: (item[1] is None, item[1] if item[1] is not None else 0),
            )
        )
        == EXPECTED_AGENT_READ_PROVIDER_ORDER
    )


def test_vault_tool_connectors_instantiate_from_descriptors() -> None:
    """Migrated vault-tool providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[VaultToolConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.vault_tool_connector_refs:
            connector = import_ref.instantiate()
            connectors[str(connector.protocol)] = (connector, import_ref.module)

    assert set(EXPECTED_VAULT_TOOL_PROVIDER_MODULES) == connectors.keys()
    for name, module in EXPECTED_VAULT_TOOL_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, VaultToolConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_runner_hook_connectors_instantiate_from_descriptors() -> None:
    """Migrated runner-hook providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[RunnerHookConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_runner_hooks():
        assert connector_manifest.runner_hook_connector is not None
        connector = connector_manifest.runner_hook_connector.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.runner_hook_connector.module)

    assert set(EXPECTED_RUNNER_HOOK_PROVIDER_MODULES) == connectors.keys()
    for name, module in EXPECTED_RUNNER_HOOK_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, RunnerHookConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_fluid_vault_runner_hook_resolves_to_operate_enrichment_capability() -> None:
    """The fluid_vault hook must BE the FluidVaultOperate-enrichment class.

    Registry membership alone (the generic test above) would pass for any
    stub published under the fluid_vault key; the nftId persistence path
    (VIB-5031) depends on the actual result-enrichment capability class.
    """
    from almanak.connectors._fluid_core.runner_hooks import FluidVaultRunnerHookConnector
    from almanak.connectors._strategy_base.runner_hook_registry import (
        RunnerResultEnrichmentCapability,
    )

    CONNECTOR_REGISTRY.clear()

    manifest = next(
        m
        for m in CONNECTOR_REGISTRY.with_runner_hooks()
        if m.runner_hook_connector is not None and str(m.runner_hook_connector.instantiate().protocol) == "fluid_vault"
    )
    connector = manifest.runner_hook_connector.instantiate()
    assert type(connector) is FluidVaultRunnerHookConnector
    assert isinstance(connector, RunnerResultEnrichmentCapability)
    assert callable(connector.enrich_result)


def test_protocol_metadata_connectors_instantiate_from_descriptors() -> None:
    """Migrated protocol-metadata providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[ProtocolMetadataConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_protocol_metadata():
        assert connector_manifest.protocol_metadata is not None
        connector = connector_manifest.protocol_metadata.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.protocol_metadata.module)

    assert set(EXPECTED_PROTOCOL_METADATA_MODULES) == connectors.keys()
    for name, module in EXPECTED_PROTOCOL_METADATA_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, ProtocolMetadataConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_principal_token_market_readers_instantiate_from_descriptors() -> None:
    """Migrated principal-token market readers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[PrincipalTokenMarketReadConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_principal_token_market_reader():
        assert connector_manifest.principal_token_market_reader is not None
        connector = connector_manifest.principal_token_market_reader.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.principal_token_market_reader.module)

    assert set(EXPECTED_PRINCIPAL_TOKEN_MARKET_READER_MODULES) == connectors.keys()
    for name, module in EXPECTED_PRINCIPAL_TOKEN_MARKET_READER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, PrincipalTokenMarketReadConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_swap_route_inference_connectors_instantiate_from_descriptors() -> None:
    """Migrated swap-route inference providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[SwapRouteInferenceConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_route_inference():
        assert connector_manifest.swap_route_inference is not None
        connector = connector_manifest.swap_route_inference.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.swap_route_inference.module)

    assert set(EXPECTED_SWAP_ROUTE_INFERENCE_MODULES) == connectors.keys()
    for name, module in EXPECTED_SWAP_ROUTE_INFERENCE_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, SwapRouteInferenceConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_teardown_post_conditions_load_from_descriptors() -> None:
    """Migrated teardown post-condition hooks are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, str] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_teardown_post_condition():
        assert connector_manifest.teardown_post_condition is not None
        hook = connector_manifest.teardown_post_condition.load()

        assert callable(hook)
        connectors[connector_manifest.name] = connector_manifest.teardown_post_condition.module

    assert connectors == EXPECTED_TEARDOWN_POST_CONDITION_MODULES


def test_connector_teardown_post_conditions_are_not_hardcoded_in_framework() -> None:
    """The framework teardown registry discovers connector-owned post-condition hooks."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/teardown/post_conditions.py").read_text()

    assert "with_teardown_post_condition()" in source
    assert "almanak.connectors.traderjoe_v2" not in source


def test_teardown_residual_discoveries_load_from_descriptors() -> None:
    """Teardown residual-discovery hooks are published through connectors (VIB-5116)."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, str] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_teardown_residual_discovery():
        assert connector_manifest.teardown_residual_discovery is not None
        hook = connector_manifest.teardown_residual_discovery.load()

        assert callable(hook)
        connectors[connector_manifest.name] = connector_manifest.teardown_residual_discovery.module

    assert connectors == EXPECTED_TEARDOWN_RESIDUAL_DISCOVERY_MODULES


def test_connector_teardown_residual_discoveries_are_not_hardcoded_in_framework() -> None:
    """The framework residual-discovery registry discovers connector-owned hooks (no protocol literal)."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/teardown/residual_discovery.py").read_text()

    assert "with_teardown_residual_discovery()" in source
    assert "gmx_v2" not in source  # the framework names no protocol (coupling ratchet)


def test_deferred_refresh_connectors_instantiate_from_descriptors() -> None:
    """Migrated deferred-refresh providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[DeferredRefreshConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_deferred_refresh():
        assert connector_manifest.deferred_refresh is not None
        connector = connector_manifest.deferred_refresh.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.deferred_refresh.module)

    assert set(EXPECTED_DEFERRED_REFRESH_MODULES) == connectors.keys()
    for name, module in EXPECTED_DEFERRED_REFRESH_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, DeferredRefreshConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_swap_quote_connectors_instantiate_from_descriptors() -> None:
    """Swap quote providers are published through connector descriptors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[SwapQuoteConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_quote():
        assert connector_manifest.swap_quote_connector is not None
        connector = connector_manifest.swap_quote_connector.instantiate()
        connectors[str(connector.protocol)] = (connector, connector_manifest.swap_quote_connector.module)

    assert set(EXPECTED_SWAP_QUOTE_MODULES) == connectors.keys()
    for name, module in EXPECTED_SWAP_QUOTE_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, SwapQuoteConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module


def test_swap_quote_bootstrap_registers_manifest_providers() -> None:
    """The swap-quote boot module hydrates runtime registry entries from manifests."""
    CONNECTOR_REGISTRY.clear()
    SWAP_QUOTE_REGISTRY.clear()

    boot_module = importlib.import_module("almanak.connectors._strategy_swap_quote_registry")
    boot_module = importlib.reload(boot_module)
    boot_module.ensure_swap_quote_registry_loaded()

    connectors = {str(connector.protocol): connector for connector in SWAP_QUOTE_REGISTRY.all()}
    assert set(EXPECTED_SWAP_QUOTE_MODULES) == connectors.keys()
    for name, connector in connectors.items():
        assert isinstance(connector, SwapQuoteConnector)
        assert connector.protocol == ProtocolName(name)
        assert type(connector).__module__ == EXPECTED_SWAP_QUOTE_MODULES[name]


def test_accounting_treatment_specs_load_from_descriptors() -> None:
    """Migrated accounting-treatment specs are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    specs: dict[str, tuple[AccountingTreatmentSpec, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_treatment():
        assert connector_manifest.accounting_treatment is not None
        spec = connector_manifest.accounting_treatment.load()
        specs[connector_manifest.name] = (spec, connector_manifest.accounting_treatment.module)

    assert set(EXPECTED_ACCOUNTING_TREATMENT_MODULES) == specs.keys()
    for name, module in EXPECTED_ACCOUNTING_TREATMENT_MODULES.items():
        spec, actual_module = specs[name]

        assert isinstance(spec, AccountingTreatmentSpec)
        assert actual_module == module


def test_accounting_report_connectors_instantiate_from_descriptors() -> None:
    """Migrated accounting-report providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[AccountingReportConnector, str]] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        assert connector_manifest.accounting_report is not None
        connector = connector_manifest.accounting_report.instantiate()
        connectors[connector.key] = (connector, connector_manifest.accounting_report.module)

    assert set(EXPECTED_ACCOUNTING_REPORT_MODULES) == connectors.keys()
    for name, module in EXPECTED_ACCOUNTING_REPORT_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, AccountingReportConnector)
        assert connector.key == name
        assert connector.strategy_class == name
        assert isinstance(connector, AccountingReportSectionCapability)
        assert connector.section_key == name
        assert actual_module == module
        assert type(connector).__module__ == module


def test_gateway_connectors_instantiate_from_descriptors() -> None:
    """Migrated gateway providers are published through connectors."""
    CONNECTOR_REGISTRY.clear()

    connectors: dict[str, tuple[GatewayConnector, str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.gateway_connector_refs:
            connector = import_ref.instantiate()
            connectors[str(connector.protocol)] = (connector, import_ref.module)
            orders[str(connector.protocol)] = import_ref.order

    assert set(EXPECTED_GATEWAY_PROVIDER_MODULES) <= connectors.keys()
    for name, module in EXPECTED_GATEWAY_PROVIDER_MODULES.items():
        connector, actual_module = connectors[name]

        assert isinstance(connector, GatewayConnector)
        assert connector.protocol == ProtocolName(name)
        assert actual_module == module
        assert type(connector).__module__ == module
    assert tuple(name for name, _order in sorted(orders.items(), key=lambda item: item[1] or 0)) == (
        EXPECTED_GATEWAY_PROVIDER_ORDER
    )
    assert CONNECTOR_REGISTRY.get("agni_finance").name == "uniswap_v3"


def test_gateway_settings_load_from_descriptors() -> None:
    """Gateway settings fragments are published through connector manifests."""
    CONNECTOR_REGISTRY.clear()

    settings: dict[str, tuple[str, str]] = {}
    orders: dict[str, int | None] = {}
    for connector_manifest in CONNECTOR_REGISTRY.with_gateway_settings():
        assert connector_manifest.gateway_settings is not None
        settings_cls = connector_manifest.gateway_settings.load()

        assert isinstance(settings_cls, type)
        assert issubclass(settings_cls, BaseModel)
        settings[connector_manifest.name] = (
            connector_manifest.gateway_settings.module,
            connector_manifest.gateway_settings.attribute,
        )
        orders[connector_manifest.name] = connector_manifest.gateway_settings.order

    assert settings == EXPECTED_GATEWAY_SETTINGS_MODULES
    assert tuple(name for name, _order in sorted(orders.items(), key=lambda item: item[1] or 0)) == (
        EXPECTED_GATEWAY_SETTINGS_ORDER
    )


def test_gateway_settings_fragments_are_not_hardcoded_in_gateway_settings() -> None:
    """Connector-backed settings fragments must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/gateway/core/settings.py").read_text()

    assert "with_gateway_settings()" in source
    for connector_manifest in CONNECTOR_REGISTRY.with_gateway_settings():
        assert connector_manifest.gateway_settings is not None
        import_ref = connector_manifest.gateway_settings
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_receipt_parsers_are_not_in_legacy_boot_file() -> None:
    """Connector-backed receipt parsers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_receipt_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_receipt_parser():
        assert connector_manifest.receipt_parser_connector is not None
        import_ref = connector_manifest.receipt_parser_connector
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_gateway_connectors_are_not_in_legacy_boot_file() -> None:
    """Connector-backed gateway providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_gateway_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.all():
        for import_ref in connector_manifest.gateway_connector_refs:
            assert import_ref.module not in source
            assert import_ref.attribute not in source


def test_connector_gas_estimates_are_not_in_legacy_boot_file() -> None:
    """Connector-backed gas-estimate providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_gas_estimate_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_gas_estimate():
        assert connector_manifest.gas_estimate_connector is not None
        import_ref = connector_manifest.gas_estimate_connector
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_contract_roles_are_not_in_legacy_boot_file() -> None:
    """Connector-backed contract-role specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_contract_role_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_contract_roles():
        assert connector_manifest.contract_roles is not None
        import_ref = connector_manifest.contract_roles
        assert import_ref.module not in source


def test_connector_permission_infrastructure_is_not_hardcoded_in_framework() -> None:
    """Connector-backed infrastructure-permission hooks must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/permissions/generator.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_permission_infrastructure():
        assert connector_manifest.permission_infrastructure is not None
        import_ref = connector_manifest.permission_infrastructure
        assert import_ref.module not in source
        assert import_ref.attribute not in source
        assert f"almanak.connectors.{connector_manifest.name}." not in source


def test_connector_bridge_adapters_are_not_in_legacy_boot_file() -> None:
    """Connector-backed bridge adapters must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_bridge_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_bridge_adapter():
        assert connector_manifest.bridge_adapter is not None
        import_ref = connector_manifest.bridge_adapter
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_flash_loans_are_not_in_legacy_boot_file() -> None:
    """Connector-backed flash-loan providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_flash_loan_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_flash_loan():
        assert connector_manifest.flash_loan_provider is not None
        assert connector_manifest.flash_loan_builder is not None
        assert connector_manifest.flash_loan_provider.module not in source
        assert connector_manifest.flash_loan_provider.attribute not in source
        assert connector_manifest.flash_loan_builder.module not in source
        assert connector_manifest.flash_loan_builder.attribute not in source


def test_connector_protocol_families_are_not_in_legacy_boot_file() -> None:
    """Connector-backed protocol-family specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_protocol_family_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_protocol_family():
        assert connector_manifest.protocol_family is not None
        import_ref = connector_manifest.protocol_family
        assert import_ref.module not in source


def test_connector_swap_classifications_are_not_in_legacy_boot_file() -> None:
    """Connector-backed swap-classification specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_swap_classification_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_swap_classification():
        assert connector_manifest.swap_classification is not None
        import_ref = connector_manifest.swap_classification
        assert import_ref.module not in source


def test_connector_contract_monitoring_is_not_hardcoded_in_contract_registry() -> None:
    """Connector-backed contract-monitoring specs must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_base/contract_registry.py").read_text()

    assert "_PROTOCOL_DEFS" not in source
    assert "ContractMonitoringSpec(" not in source
    for connector_manifest in CONNECTOR_REGISTRY.with_contract_monitoring():
        assert connector_manifest.contract_monitoring is not None
        import_ref = connector_manifest.contract_monitoring
        assert import_ref.module not in source
        assert import_ref.attribute not in source
        loaded = import_ref.load()
        specs = (loaded,) if isinstance(loaded, ContractMonitoringSpec) else loaded
        for spec in specs:
            assert spec.parser_module not in source
            assert spec.parser_class_name not in source
            assert not re.search(rf'protocol\s*=\s*["\']{re.escape(spec.protocol)}["\']', source)


def test_connector_agent_tools_are_not_in_legacy_boot_file() -> None:
    """Connector-backed agent-tool providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_agent_tool_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_agent_read():
        for import_ref in connector_manifest.agent_read_connector_refs:
            assert import_ref.module not in source
            assert import_ref.attribute not in source
    for connector_manifest in CONNECTOR_REGISTRY.with_vault_tool():
        for import_ref in connector_manifest.vault_tool_connector_refs:
            assert import_ref.module not in source
            assert import_ref.attribute not in source


def test_connector_vault_lifecycle_is_not_hardcoded_in_framework() -> None:
    """Framework vault lifecycle paths must not import concrete vault connector modules."""
    repo_root = Path(__file__).resolve().parents[3]
    # Plan 019: also scan all _run_*.py split modules so vault-code motion is covered.
    _run_star_texts = [p.read_text() for p in sorted((repo_root / "almanak/framework/cli").glob("_run_*.py"))]
    source = "\n".join(
        [
            (repo_root / path).read_text()
            for path in (
                "almanak/framework/vault/lifecycle.py",
                "almanak/framework/cli/run_helpers.py",
                "almanak/framework/cli/run.py",
            )
        ]
        + _run_star_texts
    )

    assert re.search(r"\bget_vault_tool_capability\s*\(", source) is not None
    assert re.search(r"\balmanak\.connectors\.(?!_)[a-zA-Z0-9_]+", source) is None
    assert (
        re.search(
            r"^\s*from\s+almanak\.connectors(?:\.[A-Za-z0-9_]+)?\s+import\s+(?!_)[A-Za-z0-9_]+",
            source,
            re.MULTILINE,
        )
        is None
    )
    assert "almanak.connectors.lagoon" not in source
    assert "LagoonVaultSDK" not in source
    assert "LagoonVaultAdapter" not in source
    assert "LagoonVaultDeployer" not in source
    assert "VaultDeployParams" not in source
    assert "LagoonReceiptParser" not in source


def test_connector_runner_hooks_are_not_hardcoded_in_framework_runner() -> None:
    """Framework runner paths must not import concrete connector hook modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text()
        for path in (
            "almanak/framework/runner/strategy_runner.py",
            "almanak/framework/runner/inner_runner.py",
            "almanak/framework/runner/teardown_commit.py",
        )
    )

    assert "almanak.connectors.uniswap_v3.slot0_fallback" not in source
    assert "almanak.connectors.uniswap_v3.receipt_parser" not in source
    assert "UniswapV3ReceiptParser" not in source
    assert "almanak.connectors.uniswap_v4.gateway_pool_key_client" not in source
    assert "make_sync_pool_key_lookup" not in source
    assert "_build_v4_pool_key_lookup" not in source


def test_connector_protocol_metadata_is_not_hardcoded_in_framework_data() -> None:
    """Framework data paths must not import concrete connector metadata modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/data/tokens/resolver.py").read_text()

    assert "almanak.connectors.pendle.sdk" not in source
    assert "_register_pendle_tokens" not in source
    assert "pendle_registry_" not in source
    assert "dropped_pendle" not in source


def test_framework_pendle_data_package_is_removed() -> None:
    """Pendle data implementation must stay connector-owned."""
    repo_root = Path(__file__).resolve().parents[3]

    assert not (repo_root / "almanak/framework/data/pendle").exists()


def test_pendle_data_tests_are_connector_owned() -> None:
    """Pendle data tests must live with the connector-owned implementation."""
    repo_root = Path(__file__).resolve().parents[3]
    old_test_dir = repo_root / "tests/unit/data/pendle"
    connector_test_dir = repo_root / "tests/unit/connectors/pendle"

    assert not list(old_test_dir.glob("*.py"))
    for test_name in (
        "test_api_client.py",
        "test_models.py",
        "test_on_chain_reader.py",
        "test_resolver.py",
    ):
        assert (connector_test_dir / test_name).exists()


def test_connector_principal_token_reader_is_not_hardcoded_in_framework_data() -> None:
    """Framework PT health paths must not import concrete Pendle reader modules."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/data/position_health.py").read_text()

    assert "almanak.framework.data.pendle.on_chain_reader" not in source
    assert "almanak.connectors.pendle.on_chain_reader" not in source
    assert "PendleOnChainReader" not in source


def test_framework_pendle_valuer_is_removed() -> None:
    """Pendle valuation implementation must stay connector-owned."""
    repo_root = Path(__file__).resolve().parents[3]

    assert not (repo_root / "almanak/framework/valuation/pendle_valuer.py").exists()
    assert (repo_root / "almanak/connectors/pendle/valuation.py").exists()


def test_connector_swap_route_inference_is_not_hardcoded_in_framework_compiler() -> None:
    """Framework compiler asks route inference providers instead of naming concrete heuristics."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/intents/compiler.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_swap_route_inference():
        assert connector_manifest.swap_route_inference is not None
        assert connector_manifest.swap_route_inference.module not in source
        assert connector_manifest.swap_route_inference.attribute not in source
    assert "_has_pendle_token_prefix" not in source
    assert 'get_connector_compiler("pendle")' not in source


def test_connector_cl_lp_adapter_factory_is_not_hardcoded_in_framework_compiler() -> None:
    """Framework compiler asks CL compilers for LP adapter factories instead of naming Uniswap."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/intents/compiler.py").read_text()

    assert "connector_compiler.build_lp_adapter_factory(factory_context)" in source
    assert "UniswapV3LPAdapter" not in source
    assert "almanak.connectors.uniswap_v3.adapter" not in source


def test_connector_deferred_refresh_is_not_hardcoded_in_framework_execution() -> None:
    """Framework execution asks deferred-refresh providers instead of naming connectors."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/execution/deferred_refresh.py").read_text()

    assert "DEFERRED_REFRESH_REGISTRY.lookup" in source
    for connector_manifest in CONNECTOR_REGISTRY.with_deferred_refresh():
        assert connector_manifest.deferred_refresh is not None
        assert connector_manifest.deferred_refresh.module not in source
        assert connector_manifest.deferred_refresh.attribute not in source
    assert "_DEFERRED_REFRESHERS" not in source
    assert "LiFiAdapter" not in source
    assert "EnsoAdapter" not in source


def test_connector_swap_discovery_is_not_hardcoded_in_framework_router_gate() -> None:
    """Framework permission discovery must not exempt Pendle from the router gate."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/permissions/synthetic_intents.py").read_text()

    assert '("enso", "pendle")' not in source
    assert 'protocol not in ("enso", "pendle")' not in source
    assert "Enso/Pendle" not in source


def test_connector_enrichment_kwargs_are_not_hardcoded_in_framework_enricher() -> None:
    """Framework result enrichment lets parsers own connector-specific kwargs."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/execution/result_enricher.py").read_text()

    assert "pendle" not in source.lower()
    assert "intent_swap_type" not in source


def test_connector_swap_quotes_are_not_in_legacy_boot_file() -> None:
    """Connector-backed swap quote providers must not also be hardcoded."""
    CONNECTOR_REGISTRY.clear()
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/connectors/_strategy_swap_quote_registry.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_swap_quote():
        assert connector_manifest.swap_quote_connector is not None
        import_ref = connector_manifest.swap_quote_connector
        assert import_ref.module not in source
        assert import_ref.attribute not in source


def test_connector_accounting_treatment_is_not_hardcoded_in_framework_registry() -> None:
    """Framework accounting paths discover connector treatments from manifests."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text()
        for path in (
            "almanak/connectors/_strategy_base/accounting_treatment_registry.py",
            "almanak/connectors/_strategy_accounting_treatment_registry.py",
            "almanak/framework/accounting/processor.py",
            "almanak/framework/runner/strategy_runner.py",
        )
    )

    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_treatment():
        assert connector_manifest.accounting_treatment is not None
        assert connector_manifest.accounting_treatment.module not in source
    assert '"pendle": ("almanak.connectors.pendle.accounting_spec"' not in source


def test_connector_accounting_position_keys_are_not_hardcoded_in_framework_runner() -> None:
    """Framework runner position-key routing must not carry protocol-name exclusions."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/runner/strategy_runner.py").read_text()

    assert re.search(r"['\"]pendle['\"]\s+not\s+in\s+protocol", source) is None
    assert "Non-Pendle LP" not in source


def test_connector_accounting_treatment_is_not_hardcoded_in_generic_handlers() -> None:
    """Generic accounting handlers do not carry protocol-name treatment guards."""
    repo_root = Path(__file__).resolve().parents[3]
    source = "\n".join(
        (repo_root / path).read_text().lower()
        for path in (
            "almanak/framework/accounting/category_handlers/lp_handler.py",
            "almanak/framework/accounting/category_handlers/swap_handler.py",
            "almanak/framework/accounting/lp_accounting.py",
        )
    )

    assert "pendle" not in source


def test_connector_accounting_report_is_not_hardcoded_in_reporting_loader() -> None:
    """Framework reporting loader asks connector report providers for connector events."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/accounting/reporting/loader.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        assert connector_manifest.accounting_report is not None
        assert connector_manifest.accounting_report.module not in source
        assert connector_manifest.accounting_report.attribute not in source
    assert "PendleAccountingEvent" not in source
    assert "PendleEventType" not in source
    assert "StrategyClass.PENDLE" not in source
    assert 'connector_events.get("pendle"' not in source
    assert "pendle_events" not in source
    assert "has_pendle" not in source


def test_connector_accounting_sections_are_not_hardcoded_in_strat_pnl() -> None:
    """The strat-pnl CLI discovers connector report sections through providers."""
    repo_root = Path(__file__).resolve().parents[3]
    source = (repo_root / "almanak/framework/cli/strat_pnl.py").read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        assert connector_manifest.accounting_report is not None
        assert connector_manifest.accounting_report.module not in source
        assert connector_manifest.accounting_report.attribute not in source

    assert "build_pendle_report" not in source
    assert "render_pendle_section" not in source
    assert "pendle_section_to_dict" not in source
    assert "acct_data.pendle_events" not in source


def test_connector_accounting_sections_are_owned_by_connector_modules() -> None:
    """Framework reporting does not own or re-export connector section logic."""
    repo_root = Path(__file__).resolve().parents[3]

    reporting_init_source = (repo_root / "almanak/framework/accounting/reporting/__init__.py").read_text()
    pendle_report_path = repo_root / "almanak/framework/accounting/reporting/pendle_report.py"
    render_text_source = (repo_root / "almanak/framework/accounting/reporting/render_text.py").read_text()
    render_json_source = (repo_root / "almanak/framework/accounting/reporting/render_json.py").read_text()
    connector_source = (repo_root / "almanak/connectors/pendle/reporting.py").read_text()

    assert not pendle_report_path.exists()
    assert "from .pendle_report import" not in reporting_init_source
    assert "from .pendle_report import" not in render_text_source
    assert "from .pendle_report import" not in render_json_source
    assert "almanak.connectors.pendle.reporting" not in render_text_source
    assert "almanak.connectors.pendle.reporting" not in render_json_source
    assert "PendleSection" not in reporting_init_source
    assert "build_pendle_report" not in reporting_init_source
    assert "render_pendle_section" not in render_text_source
    assert "pendle_section_to_dict" not in render_json_source

    assert "PendleAccountingEvent" in connector_source
    assert "PendleEventType" in connector_source
    assert "class PendlePositionSummary" in connector_source
    assert "class PendleSection" in connector_source


def test_connector_modules_use_canonical_connector_name() -> None:
    """Connector-local manifests use ``Connector``, not the descriptor alias."""
    repo_root = Path(__file__).resolve().parents[3]

    offenders = []
    for connector_file in sorted((repo_root / "almanak/connectors").glob("*/connector.py")):
        source = connector_file.read_text()
        if "ConnectorDescriptor" in source or "_connector_descriptor" in source:
            offenders.append(connector_file.relative_to(repo_root).as_posix())

    assert not offenders


class TestDeclarationReachabilityGuards:
    """Names and decl aliases must already be in lookup-folded form.

    Registry lookups fold case + hyphens on the REQUEST side only; the
    canonical name and domain-scoped decl aliases are consumed raw as dispatch
    keys. Anything not already folded would be silently unreachable, so the
    descriptor rejects it at construction (PR #2664 review).
    """

    def test_connector_name_rejects_uppercase(self):
        with pytest.raises(ValueError, match="lowercase, hyphen-free"):
            Connector(name="Aave_V3", kind=ProtocolKind.LENDING)

    def test_connector_name_rejects_hyphens(self):
        with pytest.raises(ValueError, match="lowercase, hyphen-free"):
            Connector(name="aave-v3", kind=ProtocolKind.LENDING)

    def test_lending_decl_rejects_hyphenated_alias(self):
        with pytest.raises(ValueError, match="must not contain hyphens"):
            LendingReadDecl(
                spec=ImportRef(module="almanak.connectors.aave_v3.lending_read", attribute="LENDING_READ_SPEC"),
                aliases=("aave-v3",),
            )

    def test_perps_decl_rejects_hyphenated_alias(self):
        with pytest.raises(ValueError, match="must not contain hyphens"):
            PerpsReadDecl(
                spec=ImportRef(module="almanak.connectors.gmx_v2.perps_read", attribute="PERPS_READ_SPEC"),
                aliases=("gmx-v2",),
            )


class TestDexVolumeDeclValidation:
    """Each DexVolumeDecl.__post_init__ guard fails closed (VIB-4851 Phase D)."""

    @staticmethod
    def _decl(**overrides):
        kwargs = {"chains": ("ethereum",), "amm_family": "v3_concentrated"}
        kwargs.update(overrides)
        return DexVolumeDecl(**kwargs)

    def test_valid_declaration_accepts_all_fields(self):
        decl = self._decl(
            name="balancer",
            dex="balancer_v2",
            aliases=("bal",),
            volume_data_source="balancer_v2_subgraph",
            chain_default=("ethereum",),
            generic_default=True,
        )
        assert decl.name == "balancer"
        assert decl.chain_default == ("ethereum",)

    @pytest.mark.parametrize("field", ["name", "dex"])
    @pytest.mark.parametrize("bad", ["", "   ", 7])
    def test_rejects_non_string_or_blank_keys(self, field, bad):
        with pytest.raises(ValueError, match=f"{field} must be None or a non-empty string"):
            self._decl(**{field: bad})

    @pytest.mark.parametrize("field", ["name", "dex"])
    @pytest.mark.parametrize("bad", ["Uniswap_V3", "uni-v3"])
    def test_rejects_uppercase_or_hyphenated_keys(self, field, bad):
        with pytest.raises(ValueError, match=f"{field} must be lowercase and hyphen-free"):
            self._decl(**{field: bad})

    @pytest.mark.parametrize("bad", [(), ["ethereum"], "ethereum"])
    def test_rejects_non_tuple_or_empty_chains(self, bad):
        with pytest.raises(ValueError, match="chains must be a non-empty tuple"):
            self._decl(chains=bad)

    @pytest.mark.parametrize("bad_chain", ["", "  ", "Ethereum", 7])
    def test_rejects_malformed_chain_entries(self, bad_chain):
        with pytest.raises(ValueError, match="chains must contain lowercase non-empty strings"):
            self._decl(chains=("ethereum", bad_chain))

    def test_rejects_duplicate_chains(self):
        with pytest.raises(ValueError, match="chains contains duplicates"):
            self._decl(chains=("ethereum", "ethereum"))

    def test_rejects_unknown_amm_family(self):
        with pytest.raises(ValueError, match="amm_family must be one of"):
            self._decl(amm_family="constant_product")

    @pytest.mark.parametrize("bad", ["", "   ", 7])
    def test_rejects_blank_volume_data_source(self, bad):
        with pytest.raises(ValueError, match="volume_data_source must be None or a non-empty string"):
            self._decl(volume_data_source=bad)

    def test_rejects_non_tuple_chain_default(self):
        with pytest.raises(ValueError, match="chain_default must be a tuple"):
            self._decl(chain_default=["ethereum"])

    def test_rejects_chain_default_outside_declared_chains(self):
        with pytest.raises(ValueError, match="chain_default chains must be declared in chains"):
            self._decl(chain_default=("base",))

    def test_rejects_non_bool_generic_default(self):
        with pytest.raises(ValueError, match="generic_default must be a bool"):
            self._decl(generic_default=1)

    def test_rejects_hyphenated_alias(self):
        with pytest.raises(ValueError, match="must not contain hyphens"):
            self._decl(aliases=("uni-v3",))

    def test_rejects_alias_shadowing_primary_name(self):
        with pytest.raises(ValueError, match="aliases must not include the primary name"):
            self._decl(name="uniswap_v3", aliases=("uniswap_v3",))

    def test_accepts_twap_reference_pools_import_ref(self):
        decl = self._decl(
            twap_reference_pools=ImportRef(
                module="almanak.connectors.uniswap_v3.backtest_pools",
                attribute="TWAP_REFERENCE_POOLS",
            )
        )
        assert decl.twap_reference_pools is not None

    @pytest.mark.parametrize("bad", ["not-a-ref", 123, {}])
    def test_rejects_non_import_ref_twap_reference_pools(self, bad):
        with pytest.raises(ValueError, match="twap_reference_pools must be None or an ImportRef"):
            self._decl(twap_reference_pools=bad)

    @pytest.mark.parametrize("field", ["volume_subgraph_urls", "hosted_volume_subgraph_urls"])
    def test_accepts_lowercase_https_subgraph_urls(self, field):
        decl = self._decl(**{field: {"ethereum": "https://gateway.example/subgraph"}})
        assert getattr(decl, field) == {"ethereum": "https://gateway.example/subgraph"}

    @pytest.mark.parametrize("field", ["volume_subgraph_urls", "hosted_volume_subgraph_urls"])
    @pytest.mark.parametrize(
        "bad_urls",
        [
            {"Ethereum": "https://gateway.example/subgraph"},  # uppercase key
            {"  ": "https://gateway.example/subgraph"},  # blank key
            {"ethereum": "http://gateway.example/subgraph"},  # non-https value
            {"ethereum": ""},  # blank value
        ],
    )
    def test_rejects_malformed_subgraph_urls(self, field, bad_urls):
        with pytest.raises(ValueError, match=f"{field} values must be https:// URLs keyed by lowercase chain name"):
            self._decl(**{field: bad_urls})

    @pytest.mark.parametrize("field", ["volume_subgraph_urls", "hosted_volume_subgraph_urls"])
    def test_rejects_non_mapping_subgraph_urls(self, field):
        with pytest.raises(ValueError, match=f"{field} must be None or a Mapping"):
            self._decl(**{field: [("ethereum", "https://gateway.example/subgraph")]})

    def test_accepts_lowercase_liquidity_subgraph_ids(self):
        decl = self._decl(liquidity_subgraph_ids={"ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"})
        assert decl.liquidity_subgraph_ids == {"ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"}

    @pytest.mark.parametrize(
        "bad_ids",
        [
            {"Ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"},  # uppercase key
            {"  ": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"},  # blank key
            {"ethereum": "https://gateway.example/subgraph"},  # URL value
            {"ethereum": ""},  # blank value
        ],
    )
    def test_rejects_malformed_liquidity_subgraph_ids(self, bad_ids):
        with pytest.raises(
            ValueError,
            match="liquidity_subgraph_ids values must be deployment IDs keyed by lowercase chain name",
        ):
            self._decl(liquidity_subgraph_ids=bad_ids)

    def test_rejects_non_mapping_liquidity_subgraph_ids(self):
        with pytest.raises(ValueError, match="liquidity_subgraph_ids must be None or a Mapping"):
            self._decl(liquidity_subgraph_ids=[("ethereum", "deployment")])


class TestBacktestStrategyTypeDeclValidation:
    """Each BacktestStrategyTypeDecl.__post_init__ guard fails closed (VIB-4851)."""

    @staticmethod
    def _decl(**overrides):
        kwargs = {"strategy_type": "lp"}
        kwargs.update(overrides)
        return BacktestStrategyTypeDecl(**kwargs)

    def test_valid_declaration_accepts_all_fields(self):
        decl = self._decl(strategy_type="lending", name="balancer", aliases=("bal",))
        assert decl.strategy_type == "lending"
        assert decl.name == "balancer"
        assert decl.aliases == ("bal",)

    @pytest.mark.parametrize("bad", ["", "vault", "LP", 7, None])
    def test_rejects_unknown_strategy_type(self, bad):
        with pytest.raises(ValueError, match="strategy_type must be one of"):
            self._decl(strategy_type=bad)

    @pytest.mark.parametrize("bad", ["", "   ", 7])
    def test_rejects_non_string_or_blank_name(self, bad):
        with pytest.raises(ValueError, match="name must be None or a non-empty string"):
            self._decl(name=bad)

    @pytest.mark.parametrize("bad", ["Balancer", "balancer-v2"])
    def test_rejects_uppercase_or_hyphenated_name(self, bad):
        with pytest.raises(ValueError, match="name must be lowercase and hyphen-free"):
            self._decl(name=bad)

    def test_rejects_hyphenated_alias(self):
        with pytest.raises(ValueError, match="must not contain hyphens"):
            self._decl(aliases=("uniswap-v2",))

    def test_rejects_duplicate_aliases(self):
        with pytest.raises(ValueError, match="aliases contains duplicates"):
            self._decl(aliases=("uniswap", "uniswap"))

    def test_rejects_alias_shadowing_primary_name(self):
        with pytest.raises(ValueError, match="aliases must not include the primary name"):
            self._decl(name="balancer", aliases=("balancer",))

    def test_connector_rejects_non_decl_value(self):
        with pytest.raises(ValueError, match="backtest_strategy_type must be None or a BacktestStrategyTypeDecl"):
            Connector(name="aave_v3", kind=ProtocolKind.LENDING, backtest_strategy_type="lending")

    def test_connector_rejects_alias_shadowing_effective_primary_name(self):
        # name=None defaults the primary key to the connector name; an alias
        # repeating it must fail at manifest construction, not surface later
        # as a confusing duplicate-ownership error at discovery.
        with pytest.raises(ValueError, match="aliases must not include the effective primary name"):
            Connector(
                name="aave_v3",
                kind=ProtocolKind.LENDING,
                backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lending", aliases=("aave_v3",)),
            )


class TestLendingReadDeclRateLaneValidation:
    """Phase D LendingReadDecl rate-lane guards fail closed (VIB-4851 D4+D5)."""

    @staticmethod
    def _decl(**overrides):
        kwargs = {
            "spec": ImportRef(module="almanak.connectors.aave_v3.lending_read", attribute="LENDING_READ_SPEC"),
        }
        kwargs.update(overrides)
        return LendingReadDecl(**kwargs)

    def test_valid_rate_lane_declaration(self):
        decl = self._decl(
            rate_history_chains=("ethereum", "base"),
            backtest_provider=ImportRef(
                module="almanak.connectors.aave_v3.backtest_apy",
                attribute="AaveV3APYProvider",
            ),
            backtest_default_supply_apy="0.03",
            backtest_default_borrow_apy="0.05",
        )
        assert decl.rate_history_chains == ("ethereum", "base")
        assert decl.backtest_provider is not None

    @pytest.mark.parametrize("bad", ["not-a-ref", 123, {}])
    def test_rejects_non_import_ref_backtest_provider(self, bad):
        with pytest.raises(ValueError, match="backtest_provider must be None or an ImportRef"):
            self._decl(backtest_provider=bad)

    @pytest.mark.parametrize("bad", [["ethereum"], "ethereum"])
    def test_rejects_non_tuple_rate_history_chains(self, bad):
        with pytest.raises(ValueError, match="rate_history_chains must be a tuple"):
            self._decl(rate_history_chains=bad)

    @pytest.mark.parametrize("bad_chain", ["", "  ", "Ethereum", 7])
    def test_rejects_malformed_rate_history_chain_entries(self, bad_chain):
        with pytest.raises(ValueError, match="rate_history_chains must contain lowercase non-empty strings"):
            self._decl(rate_history_chains=("ethereum", bad_chain))

    def test_rejects_duplicate_rate_history_chains(self):
        with pytest.raises(ValueError, match="rate_history_chains contains duplicates"):
            self._decl(rate_history_chains=("ethereum", "ethereum"))

    @pytest.mark.parametrize("field", ["backtest_default_supply_apy", "backtest_default_borrow_apy"])
    def test_rejects_non_string_default_apy(self, field):
        with pytest.raises(ValueError, match=f"{field} must be None or a decimal string"):
            self._decl(**{field: 0.03})

    @pytest.mark.parametrize("field", ["backtest_default_supply_apy", "backtest_default_borrow_apy"])
    def test_rejects_non_decimal_default_apy(self, field):
        with pytest.raises(ValueError, match=f"{field} must parse as a Decimal"):
            self._decl(**{field: "three percent"})

    def test_token_keyed_defaults_to_false(self):
        # VIB-5493: account/vault-keyed is the safe default; a connector must
        # opt in explicitly to per-token position splitting.
        assert self._decl().token_keyed is False

    def test_accepts_token_keyed_bool(self):
        assert self._decl(token_keyed=True).token_keyed is True

    @pytest.mark.parametrize("bad", [1, "true", None, 0])
    def test_rejects_non_bool_token_keyed(self, bad):
        # VIB-5493: token_keyed must be a real bool — a truthy/falsy non-bool
        # (e.g. 1 / "true") must fail closed at declaration time, not silently
        # flip the teardown guard's per-token split.
        with pytest.raises(ValueError, match="token_keyed must be a bool"):
            self._decl(token_keyed=bad)
