"""Gimo Finance Adapter — liquid staking on 0G Chain.

This module provides an adapter for interacting with Gimo Finance liquid staking
protocol on 0G Chain. Built on StaFi's EVM LSD Stack.

Supported operations:
- Stake A0GI to receive st0G (liquid staking derivative)
- Unstake st0G to initiate A0GI withdrawal (22-day unbonding)

Supported chains:
- 0G Chain (zerog) — mainnet only

Reference:
    StaFi EVM LSD Architecture: https://docs.stafi.io/lsaas/architecture_evm_lsd/

Example:
    from almanak.framework.connectors.gimo import GimoAdapter, GimoConfig

    config = GimoConfig(chain="zerog", wallet_address="0x...")
    adapter = GimoAdapter(config)

    # Stake A0GI to receive st0G
    result = adapter.stake(amount=Decimal("100.0"))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.intents.vocabulary import StakeIntent, UnstakeIntent
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Gimo contract addresses on 0G Chain
GIMO_ADDRESSES: dict[str, dict[str, str]] = {
    "zerog": {
        "st0g": "0x7bBC63D01CA42491c3E084C941c3E86e55951404",
        # StakeManager and StakePool addresses TBD — contracts unverified
        # Using st0G contract as the primary stake entry point
        # StaFi LSD pattern: stake() is called on the StakePool contract
        # For now, we encode a direct stake via the StakePool proxy
        "stake_pool": "0x7bBC63D01CA42491c3E084C941c3E86e55951404",
    },
}

# Function selectors (StaFi EVM LSD Stack pattern)
# stake() payable — deposit native token, receive LSD token
GIMO_STAKE_SELECTOR = "0x3a4b66f1"  # stake()
# unstake(uint256 _lsdTokenAmount) — burn LSD, initiate unbonding
GIMO_UNSTAKE_SELECTOR = "0x2e17de78"  # unstake(uint256)

# Gas estimates for Gimo operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "stake": 150000,  # Stake A0GI -> st0G
    "unstake": 200000,  # Unstake st0G -> A0GI (initiate unbonding)
    "approve": 60000,  # ERC-20 approve for st0G
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class GimoConfig:
    """Configuration for Gimo adapter.

    Attributes:
        chain: Must be "zerog"
        wallet_address: User wallet address
    """

    chain: str
    wallet_address: str

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain not in GIMO_ADDRESSES:
            raise ValueError(f"Invalid chain: {self.chain}. Gimo only supports: {set(GIMO_ADDRESSES.keys())}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")


@dataclass
class TransactionResult:
    """Result of a transaction build operation."""

    success: bool
    tx_data: dict[str, Any] | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None


# =============================================================================
# Adapter
# =============================================================================


class GimoAdapter:
    """Adapter for Gimo Finance liquid staking protocol on 0G Chain.

    Provides methods for:
    - Stake A0GI to receive st0G
    - Unstake st0G to initiate A0GI withdrawal

    Example:
        config = GimoConfig(chain="zerog", wallet_address="0x...")
        adapter = GimoAdapter(config)
        result = adapter.stake(Decimal("100.0"))
    """

    def __init__(self, config: GimoConfig, token_resolver: TokenResolverType | None = None) -> None:
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        chain_addresses = GIMO_ADDRESSES[config.chain]
        self.st0g_address = chain_addresses["st0g"]
        self.stake_pool_address = chain_addresses["stake_pool"]

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"GimoAdapter initialized for chain={config.chain}, wallet={config.wallet_address[:10]}...")

    def stake(self, amount: Decimal) -> TransactionResult:
        """Build a stake transaction to receive st0G.

        Stakes A0GI (native token) to the Gimo StakePool contract and receives
        st0G in return. A0GI is sent as msg.value.

        Args:
            amount: Amount of A0GI to stake

        Returns:
            TransactionResult with transaction data
        """
        if amount <= 0:
            return TransactionResult(success=False, error="Stake amount must be positive")

        try:
            amount_wei = int(amount * Decimal(10**18))

            # StaFi LSD pattern: stake() is a payable function with no args
            calldata = GIMO_STAKE_SELECTOR

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.stake_pool_address,
                    "value": amount_wei,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["stake"],
                description=f"Stake {amount} A0GI to Gimo for st0G",
            )

        except Exception as e:
            logger.exception(f"Failed to build stake transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def unstake(self, amount: Decimal) -> TransactionResult:
        """Build an unstake transaction to initiate A0GI withdrawal.

        Burns st0G and initiates the 22-day unbonding period.

        Args:
            amount: Amount of st0G to unstake

        Returns:
            TransactionResult with transaction data
        """
        if amount <= 0:
            return TransactionResult(success=False, error="Unstake amount must be positive")

        try:
            amount_wei = int(amount * Decimal(10**18))

            # unstake(uint256 _lsdTokenAmount)
            calldata = GIMO_UNSTAKE_SELECTOR + self._pad_uint256(amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.stake_pool_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["unstake"],
                description=f"Unstake {amount} st0G from Gimo (22-day unbonding)",
            )

        except Exception as e:
            logger.exception(f"Failed to build unstake transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Utility Methods
    # =========================================================================

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # Intent Compilation Methods
    # =========================================================================

    def compile_stake_intent(
        self,
        intent: StakeIntent,
        market_snapshot: Any | None = None,
    ) -> ActionBundle:
        """Compile a StakeIntent to an ActionBundle.

        Converts a high-level StakeIntent into executable transaction data.
        Stakes A0GI (native token) to receive st0G.

        Args:
            intent: The StakeIntent to compile
            market_snapshot: Optional market data (not used)

        Returns:
            ActionBundle containing transaction(s) for execution
        """
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if intent.amount == "all":
            raise ValueError(
                "amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts."
            )

        amount: Decimal = intent.amount  # type: ignore[assignment]

        transactions: list[dict[str, Any]] = []
        total_gas = 0

        # Stake A0GI to get st0G
        stake_result = self.stake(amount)
        if not stake_result.success or stake_result.tx_data is None:
            return ActionBundle(
                intent_type="STAKE",
                transactions=[],
                metadata={
                    "error": stake_result.error or "Unknown error",
                    "intent_id": intent.intent_id,
                    "protocol": "gimo",
                },
            )

        stake_tx_data = stake_result.tx_data
        transactions.append(
            {
                "to": stake_tx_data["to"],
                "value": stake_tx_data["value"],
                "data": stake_tx_data["data"],
                "gas_estimate": stake_result.gas_estimate,
                "description": stake_result.description,
                "action_type": "stake",
            }
        )
        total_gas += stake_result.gas_estimate

        metadata = {
            "intent_id": intent.intent_id,
            "protocol": "gimo",
            "token_in": intent.token_in,
            "amount": str(amount),
            "output_token": "st0G",
            "chain": self.chain,
            "total_gas_estimate": total_gas,
            "num_transactions": len(transactions),
        }

        return ActionBundle(
            intent_type="STAKE",
            transactions=transactions,
            metadata=metadata,
        )

    def compile_unstake_intent(
        self,
        intent: UnstakeIntent,
        market_snapshot: Any | None = None,
    ) -> ActionBundle:
        """Compile an UnstakeIntent to an ActionBundle.

        Converts a high-level UnstakeIntent into executable transaction data.
        Burns st0G and initiates 22-day unbonding for A0GI.

        Args:
            intent: The UnstakeIntent to compile
            market_snapshot: Optional market data (not used)

        Returns:
            ActionBundle containing transaction(s) for execution
        """
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if intent.amount == "all":
            raise ValueError(
                "amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts."
            )

        amount: Decimal = intent.amount  # type: ignore[assignment]

        transactions: list[dict[str, Any]] = []
        total_gas = 0

        # Approve exact st0G amount for StakePool (exact approval — Gimo contracts are unverified)
        amount_wei = int(amount * Decimal(10**18))
        approve_data = "0x095ea7b3" + self._pad_address(self.stake_pool_address) + self._pad_uint256(amount_wei)
        transactions.append(
            {
                "to": self.st0g_address,
                "value": 0,
                "data": approve_data,
                "gas_estimate": DEFAULT_GAS_ESTIMATES["approve"],
                "description": f"Approve Gimo StakePool to spend {amount} st0G",
                "action_type": "approve",
            }
        )
        total_gas += DEFAULT_GAS_ESTIMATES["approve"]

        # Unstake st0G
        unstake_result = self.unstake(amount)
        if not unstake_result.success or unstake_result.tx_data is None:
            return ActionBundle(
                intent_type="UNSTAKE",
                transactions=[],
                metadata={
                    "error": unstake_result.error or "Unknown error",
                    "intent_id": intent.intent_id,
                    "protocol": "gimo",
                },
            )

        unstake_tx_data = unstake_result.tx_data
        transactions.append(
            {
                "to": unstake_tx_data["to"],
                "value": unstake_tx_data["value"],
                "data": unstake_tx_data["data"],
                "gas_estimate": unstake_result.gas_estimate,
                "description": unstake_result.description,
                "action_type": "unstake",
            }
        )
        total_gas += unstake_result.gas_estimate

        metadata = {
            "intent_id": intent.intent_id,
            "protocol": "gimo",
            "token_in": intent.token_in,
            "amount": str(amount),
            "output_token": "A0GI",
            "chain": self.chain,
            "total_gas_estimate": total_gas,
            "num_transactions": len(transactions),
            "note": "Unstake initiated. A0GI claimable after 22-day unbonding period.",
        }

        return ActionBundle(
            intent_type="UNSTAKE",
            transactions=transactions,
            metadata=metadata,
        )
