"""Curvance protocol connector.

Curvance is a permissionless isolated-market lending protocol deployed on Monad.
Unlike Compound/Aave-style protocols with a single pool, Curvance deploys a
dedicated cToken (collateral, ERC-4626-style) and BorrowableCToken (debt side)
pair per market. The ``market_id`` used throughout the adapter is the
MarketManager address.

Key entry points:
    CurvanceAdapter      — high-level interface (supply_collateral, borrow, repay, withdraw)
    CurvanceConfig       — adapter configuration (chain, wallet, optional gateway client)
    CurvanceSDK          — low-level calldata/encoding helpers
    CurvanceReceiptParser — event parsing for ResultEnricher
    CURVANCE_MARKETS     — per-chain market registry (MarketManager -> cToken / BorrowableCToken)

Supported chains: Monad.
"""

from .adapter import CurvanceAdapter, CurvanceConfig, CurvanceMarketInfo
from .constants import CURVANCE_MARKETS, CURVANCE_PROTOCOL_CONTRACTS
from .receipt_parser import CurvanceReceiptParser
from .sdk import CurvanceSDK

__all__ = [
    "CURVANCE_MARKETS",
    "CURVANCE_PROTOCOL_CONTRACTS",
    "CurvanceAdapter",
    "CurvanceConfig",
    "CurvanceMarketInfo",
    "CurvanceReceiptParser",
    "CurvanceSDK",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="curvance",
    intents=(
        IntentType.SUPPLY,
        IntentType.BORROW,
        IntentType.REPAY,
        IntentType.WITHDRAW,
    ),
    chains=("monad",),
)
