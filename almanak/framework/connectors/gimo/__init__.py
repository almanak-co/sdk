"""Gimo Finance Connector — liquid staking on 0G Chain.

Gimo Finance is a liquid staking protocol on 0G Chain (AI L1) built on StaFi's
EVM LSD Stack. Users stake A0GI (0G's native token) and receive st0G, a
yield-bearing liquid staking derivative.

Architecture (StaFi EVM LSD Stack):
    - LsdToken (st0G): ERC-20 liquid staking derivative
    - StakeManager: Manages staking lifecycle and exchange rate
    - StakePool: Holds staked A0GI and distributes to validators

Available Operations:
    - stake(): Deposit A0GI -> receive st0G
    - unstake(): Request st0G -> A0GI withdrawal (22-day unbonding)
    - withdraw(): Claim A0GI after unbonding period

Reference:
    - StaFi EVM LSD Architecture: https://docs.stafi.io/lsaas/architecture_evm_lsd/
    - Gimo Finance Docs: https://docs.gimofinance.xyz/docs/
"""

from .adapter import (
    DEFAULT_GAS_ESTIMATES,
    GIMO_ADDRESSES,
    GIMO_STAKE_SELECTOR,
    GIMO_UNSTAKE_SELECTOR,
    GimoAdapter,
    GimoConfig,
    TransactionResult,
)
from .receipt_parser import (
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    GimoEventType,
    GimoReceiptParser,
    ParseResult,
    StakeEventData,
    UnstakeEventData,
)

__all__ = [
    # Adapter
    "GimoAdapter",
    "GimoConfig",
    "TransactionResult",
    # Receipt Parser
    "GimoReceiptParser",
    "GimoEventType",
    "StakeEventData",
    "UnstakeEventData",
    "ParseResult",
    # Constants
    "GIMO_ADDRESSES",
    "GIMO_STAKE_SELECTOR",
    "GIMO_UNSTAKE_SELECTOR",
    "DEFAULT_GAS_ESTIMATES",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
]
