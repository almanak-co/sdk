"""Raydium CLMM concentrated liquidity connector.

Provides LP operations on Raydium CLMM pools on Solana:
- Open concentrated liquidity positions
- Close positions (decrease liquidity + burn NFT)

Unlike Jupiter/Kamino (REST API), Raydium CLMM builds instructions
locally using `solders` and submits via SolanaExecutionPlanner.
"""

from .adapter import RaydiumAdapter, RaydiumConfig
from .constants import CLMM_PROGRAM_ID
from .exceptions import RaydiumAPIError, RaydiumConfigError, RaydiumError, RaydiumPoolError, RaydiumTickError
from .models import RaydiumPool, RaydiumPosition, RaydiumTransactionBundle
from .receipt_parser import RaydiumReceiptParser
from .sdk import RaydiumCLMMSDK

__all__ = [
    "CLMM_PROGRAM_ID",
    "RaydiumAPIError",
    "RaydiumAdapter",
    "RaydiumCLMMSDK",
    "RaydiumConfig",
    "RaydiumConfigError",
    "RaydiumError",
    "RaydiumPool",
    "RaydiumPoolError",
    "RaydiumPosition",
    "RaydiumReceiptParser",
    "RaydiumTickError",
    "RaydiumTransactionBundle",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="raydium",
    intents=(
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
    ),
    chains=("solana",),
)
