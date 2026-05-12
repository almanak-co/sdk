"""Orca Whirlpools concentrated liquidity connector.

Provides LP operations on Orca Whirlpool pools on Solana:
- Open concentrated liquidity positions
- Close positions (decrease liquidity + burn NFT)

Uses the same Q64.64 tick math as Raydium CLMM (reused from
connectors/raydium/math.py) and Anchor-style instruction encoding.

Reference: https://github.com/orca-so/whirlpools
"""

from .adapter import OrcaAdapter, OrcaConfig
from .constants import WHIRLPOOL_PROGRAM_ID
from .exceptions import OrcaAPIError, OrcaConfigError, OrcaError, OrcaPoolError
from .models import OrcaPool, OrcaPosition, OrcaTransactionBundle
from .receipt_parser import OrcaReceiptParser
from .sdk import OrcaWhirlpoolSDK

__all__ = [
    "WHIRLPOOL_PROGRAM_ID",
    "OrcaAPIError",
    "OrcaAdapter",
    "OrcaConfig",
    "OrcaConfigError",
    "OrcaError",
    "OrcaPool",
    "OrcaPoolError",
    "OrcaPosition",
    "OrcaReceiptParser",
    "OrcaTransactionBundle",
    "OrcaWhirlpoolSDK",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="orca",
    intents=(
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
    ),
    chains=("solana",),
)
