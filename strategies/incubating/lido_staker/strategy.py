"""
===============================================================================
TUTORIAL: Lido Staker Strategy - Stake ETH for Liquid Staking Derivatives
===============================================================================

This tutorial strategy demonstrates how to stake ETH with Lido to receive
liquid staking derivatives (stETH or wstETH).

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors ETH balance in the wallet
2. Reserves a configurable amount of ETH for gas fees (default: 0.01 ETH)
3. When (ETH balance - gas_reserve) >= min_stake_amount: Stakes ETH with Lido
4. Receives stETH (rebasing) or wstETH (non-rebasing, wrapped)
5. Holds when already staked or insufficient balance

WHAT IS LIDO?
-------------
Lido is the largest liquid staking protocol for Ethereum:
- Stake ETH without running a validator (32 ETH minimum for solo staking)
- Receive stETH (liquid staking token) representing your staked ETH
- stETH earns ~4% APY from Ethereum staking rewards
- stETH is liquid - trade, use as collateral, LP in DeFi

stETH vs wstETH:
- stETH: Rebasing token - balance increases daily with staking rewards
- wstETH: Non-rebasing wrapper - share value increases, balance constant
- wstETH is better for DeFi (most protocols prefer non-rebasing tokens)

RISKS:
------
- Smart Contract Risk: Lido protocol bugs, exploits
- Slashing Risk: If Lido validators are slashed, stakers may lose funds
- Liquidity Risk: stETH may trade at discount to ETH
- Withdrawal Queue: Unstaking takes 3-5 days through Lido withdrawal queue

USAGE:
------
    # Run once
    python -m src.cli.run --strategy demo_lido_staker --once

    # Run continuously
    python -m src.cli.run --strategy demo_lido_staker

    # Test on Anvil
    python strategies/demo/lido_staker/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from decimal import Decimal
from typing import Any

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human

# Logger for debugging
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY METADATA
# =============================================================================


@almanak_strategy(
    # Unique identifier for CLI
    name="demo_lido_staker",
    # Description
    description="Tutorial strategy - stake ETH with Lido for liquid staking yield",
    # Version
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "staking", "lido", "liquid-staking", "eth"],
    # Supported chains (Lido is on Ethereum mainnet)
    supported_chains=["ethereum"],
    # Protocols used
    supported_protocols=["lido"],
    # Intent types this strategy may emit
    # STAKE: Stake ETH to receive stETH/wstETH
    # HOLD: No action
    intent_types=["STAKE", "HOLD"],
)
class LidoStakerStrategy(IntentStrategy):
    """
    Lido staking strategy for educational purposes.

    This strategy demonstrates:
    - How to stake ETH with Lido
    - How to choose between stETH and wstETH
    - How to track staking state

    Configuration Parameters (from config.yaml or config.json):
    -----------------------------------------------------------
    - min_stake_amount: Minimum ETH balance to trigger staking (default: "0.1")
    - gas_reserve: ETH reserved for gas fees, never staked (default: "0.01")
    - receive_wrapped: Whether to receive wstETH instead of stETH (default: True)
    - force_action: Force "stake" for testing

    Example Config:
    ---------------
    {
        "min_stake_amount": "0.1",
        "gas_reserve": "0.01",
        "receive_wrapped": true,
        "force_action": ""
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the staking strategy.

        Extracts configuration and sets up internal state for tracking
        the staking process.
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration
        # =====================================================================

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Staking configuration
        self.min_stake_amount = Decimal(str(get_config("min_stake_amount", "0.1")))

        # ETH reserved for gas fees (never staked)
        self.gas_reserve = Decimal(str(get_config("gas_reserve", "0.01")))
        if self.gas_reserve < 0:
            raise ValueError("gas_reserve must be >= 0")

        # Whether to receive wstETH (wrapped, non-rebasing) instead of stETH
        self.receive_wrapped = get_config("receive_wrapped", True)

        # Force action for testing
        self.force_action = str(get_config("force_action", "")).lower()

        # Internal state tracking
        self._staked = False
        self._staked_amount = Decimal("0")

        logger.info(
            f"LidoStakerStrategy initialized: min_stake={self.min_stake_amount} ETH, "
            f"gas_reserve={self.gas_reserve} ETH, receive_wrapped={self.receive_wrapped}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a staking decision based on wallet balance and configuration.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If already staked, hold
        3. If (ETH balance - gas_reserve) >= min_stake_amount, stake
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: STAKE or HOLD
        """
        try:
            # =================================================================
            # STEP 1: Handle forced actions (for testing)
            # =================================================================

            if self.force_action == "stake":
                logger.info("Forced action: STAKE ETH")
                return self._create_stake_intent(self.min_stake_amount)

            # =================================================================
            # STEP 2: Check if already staked
            # =================================================================

            if self._staked:
                output_token = "wstETH" if self.receive_wrapped else "stETH"
                return Intent.hold(reason=f"Already staked {self._staked_amount} ETH -> {output_token}")

            # =================================================================
            # STEP 3: Check ETH balance
            # =================================================================

            try:
                eth_balance = market.balance("ETH")
                # Extract balance value from TokenBalance object if needed
                balance_value = eth_balance.balance if hasattr(eth_balance, "balance") else eth_balance
                logger.debug(f"ETH balance: {balance_value}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get ETH balance: {e}")
                return Intent.hold(reason=f"Could not fetch ETH balance: {e}")

            # =================================================================
            # STEP 4: Stake if sufficient balance
            # =================================================================

            # Reserve ETH for gas fees - never stake the entire balance
            stake_amount = max(Decimal("0"), balance_value - self.gas_reserve)
            if stake_amount >= self.min_stake_amount:
                logger.info(
                    f"ETH balance ({balance_value}) - gas_reserve ({self.gas_reserve}) = "
                    f"{stake_amount} >= min_stake ({self.min_stake_amount}), staking"
                )
                return self._create_stake_intent(stake_amount)

            # =================================================================
            # STEP 5: Insufficient balance - hold
            # =================================================================

            return Intent.hold(
                reason=f"Insufficient stakeable ETH: {balance_value} - {self.gas_reserve} gas reserve = {stake_amount} < {self.min_stake_amount}"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_stake_intent(self, amount: Decimal) -> Intent:
        """
        Create a STAKE intent to deposit ETH with Lido.

        Staking flow:
        1. ETH is sent to Lido's staking contract
        2. You receive stETH (1:1 ratio initially)
        3. If receive_wrapped=True, stETH is wrapped to wstETH

        Parameters:
            amount: Amount of ETH to stake

        Returns:
            StakeIntent ready for compilation
        """
        output_token = "wstETH" if self.receive_wrapped else "stETH"
        logger.info(f"STAKE intent: {format_token_amount_human(amount, 'ETH')} -> {output_token}")

        return Intent.stake(
            protocol="lido",
            token_in="ETH",
            amount=amount,
            receive_wrapped=self.receive_wrapped,
            chain="ethereum",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Updates internal state to track staking status.
        """
        intent_type = intent.intent_type.value

        if success and intent_type == "STAKE":
            self._staked = True
            # Extract amount from intent
            if hasattr(intent, "amount"):
                self._staked_amount = intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
            output_token = "wstETH" if self.receive_wrapped else "stETH"
            logger.info(f"Staking successful: {self._staked_amount} ETH -> {output_token}")
        elif not success:
            logger.warning(f"{intent_type} failed")

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_lido_staker",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "min_stake_amount": str(self.min_stake_amount),
                "gas_reserve": str(self.gas_reserve),
                "receive_wrapped": self.receive_wrapped,
            },
            "state": {
                "staked": self._staked,
                "staked_amount": str(self._staked_amount),
            },
        }

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery.

        This allows the strategy to resume from where it left off.
        """
        return {
            "staked": self._staked,
            "staked_amount": str(self._staked_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup.

        Called when resuming a strategy after crash/restart.
        """
        if "staked" in state:
            self._staked = state["staked"]
            logger.info(f"Restored staked state: {self._staked}")

        if "staked_amount" in state:
            self._staked_amount = Decimal(str(state["staked_amount"]))
            logger.info(f"Restored staked_amount: {self._staked_amount}")


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("LidoStakerStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {LidoStakerStrategy.STRATEGY_NAME}")
    print(f"Version: {LidoStakerStrategy.STRATEGY_METADATA.get('version', 'N/A')}")
    print(f"Supported Chains: {LidoStakerStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {LidoStakerStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {LidoStakerStrategy.INTENT_TYPES}")
    print(f"\nDescription: {LidoStakerStrategy.STRATEGY_METADATA.get('description', 'N/A')}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_lido_staker --once")
