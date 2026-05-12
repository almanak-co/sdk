"""Meteora DLMM concentrated liquidity connector.

Provides LP operations on Meteora DLMM pools on Solana:
- Open concentrated liquidity positions (discrete bins)
- Close positions (remove liquidity + close account)

Unlike Raydium CLMM (NFT positions, continuous ticks), Meteora DLMM uses:
- Discrete price bins instead of continuous ticks
- Non-transferable Keypair-based position accounts (not NFTs)
- SpotBalanced strategy for even liquidity distribution
"""

from .adapter import MeteoraAdapter, MeteoraConfig
from .constants import DLMM_PROGRAM_ID
from .exceptions import MeteoraAPIError, MeteoraError, MeteoraPoolError, MeteoraPositionError
from .models import MeteoraBin, MeteoraPool, MeteoraPosition
from .receipt_parser import MeteoraReceiptParser
from .sdk import MeteoraSDK

__all__ = [
    "DLMM_PROGRAM_ID",
    "MeteoraAPIError",
    "MeteoraAdapter",
    "MeteoraBin",
    "MeteoraConfig",
    "MeteoraError",
    "MeteoraPool",
    "MeteoraPoolError",
    "MeteoraPosition",
    "MeteoraPositionError",
    "MeteoraReceiptParser",
    "MeteoraSDK",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="meteora",
    intents=(
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
    ),
    chains=("solana",),
)
