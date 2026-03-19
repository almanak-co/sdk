"""
===============================================================================
TUTORIAL: Ethena Yield Strategy - Stake USDe for Yield-Bearing sUSDe
===============================================================================

This tutorial strategy demonstrates how to stake USDe with Ethena to receive
sUSDe, a yield-bearing synthetic dollar token.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors USDe balance in the wallet
2. When USDe balance > min_stake_amount: Stakes USDe with Ethena
3. Receives sUSDe (yield-bearing vault token)
4. Holds when already staked or insufficient balance

WHAT IS ETHENA?
---------------
Ethena is a synthetic dollar protocol that provides:
- USDe: Synthetic dollar backed by delta-neutral ETH positions
- sUSDe: Staked USDe that earns yield from funding rates
- Yield comes from perpetual futures funding rate arbitrage
- Currently ~15-25% APY (varies with market conditions)

sUSDe is an ERC4626 vault token:
- Deposit USDe to mint sUSDe
- sUSDe appreciates in value over time as yield accrues
- To withdraw: initiate cooldown (7 days), then claim USDe

RISKS:
------
- Smart Contract Risk: Ethena protocol bugs, exploits
- Depeg Risk: USDe may trade below $1 in extreme conditions
- Funding Rate Risk: Negative funding reduces yield
- Cooldown Period: 7-day wait to unstake sUSDe
- Custodial Risk: Ethena relies on centralized exchanges for hedging

OPTIONAL USDC -> USDe SWAP:
---------------------------
If you have USDC instead of USDe, enable `swap_usdc_to_usde: true` in config.
The strategy will first swap USDC to USDe via Enso aggregator, then stake.

USAGE:
------
    # Run once
    almanak strat run -d ethena_yield --once

    # Run continuously
    almanak strat run -d ethena_yield --interval 60

    # Test on Anvil
    almanak strat run -d ethena_yield --network anvil --once

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
    name="demo_ethena_yield",
    # Description
    description="Tutorial strategy - stake USDe with Ethena for yield-bearing sUSDe",
    # Version
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "staking", "ethena", "yield", "usde", "susde"],
    # Supported chains (Ethena is on Ethereum mainnet)
    supported_chains=["ethereum"],
    # Protocols used
    supported_protocols=["ethena", "enso"],
    # Intent types this strategy may emit
    # SWAP: Convert USDC to USDe via Enso (optional)
    # STAKE: Stake USDe to receive sUSDe
    # HOLD: No action
    intent_types=["SWAP", "STAKE", "HOLD"],
    default_chain="ethereum",
)
class EthenaYieldStrategy(IntentStrategy):
    """
    Ethena yield strategy for educational purposes.

    This strategy demonstrates:
    - How to swap USDC to USDe via Enso aggregator (optional)
    - How to stake USDe with Ethena
    - How to receive yield-bearing sUSDe
    - How to track staking state

    Configuration Parameters (from config.yaml or config.json):
    -----------------------------------------------------------
    - min_stake_amount: Minimum USDe balance to trigger staking (default: "100")
    - swap_usdc_to_usde: Enable USDC -> USDe swap via Enso (default: false)
    - min_usdc_amount: Minimum USDC balance to trigger swap (default: "100")
    - max_slippage_pct: Max slippage for USDC -> USDe swap (default: 0.5)
    - force_action: Force "stake" or "swap" for testing

    Example Config:
    ---------------
    {
        "min_stake_amount": "100",
        "swap_usdc_to_usde": true,
        "min_usdc_amount": "100",
        "max_slippage_pct": 0.5,
        "force_action": ""
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the Ethena yield strategy.

        Extracts configuration and sets up internal state for tracking
        the staking process.
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration
        # =====================================================================

        # Staking configuration
        self.min_stake_amount = Decimal(str(self.get_config("min_stake_amount", "100")))

        # USDC -> USDe swap configuration (optional)
        raw_swap = self.get_config("swap_usdc_to_usde", False)
        if isinstance(raw_swap, bool):
            self.swap_usdc_to_usde = raw_swap
        elif isinstance(raw_swap, str):
            self.swap_usdc_to_usde = raw_swap.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self.swap_usdc_to_usde = bool(raw_swap)
        self.min_usdc_amount = Decimal(str(self.get_config("min_usdc_amount", "100")))
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 0.5))

        # Force action for testing
        self.force_action = str(self.get_config("force_action", "")).lower()

        # Internal state tracking
        self._swapped = False
        self._swapped_amount = Decimal("0")
        self._staked = False
        self._staked_amount = Decimal("0")

        logger.info(
            f"EthenaYieldStrategy initialized: min_stake={self.min_stake_amount} USDe, swap_usdc_to_usde={self.swap_usdc_to_usde}, min_usdc={self.min_usdc_amount}"
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
        3. If swap_usdc_to_usde enabled and USDC balance sufficient, swap USDC -> USDe
        4. If USDe balance > min_stake_amount, stake
        5. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: SWAP, STAKE, or HOLD
        """
        # =================================================================
        # STEP 1: Handle forced actions (for testing)
        # =================================================================

        if self.force_action == "stake":
            logger.info("Forced action: STAKE USDe")
            return self._create_stake_intent(self.min_stake_amount)

        if self.force_action == "swap":
            logger.info("Forced action: SWAP USDC -> USDe")
            return self._create_swap_intent(self.min_usdc_amount)

        # =================================================================
        # STEP 2: Check if already staked
        # =================================================================

        if self._staked:
            return Intent.hold(reason=f"Already staked {self._staked_amount} USDe -> sUSDe")

        # =================================================================
        # STEP 3: Check USDe balance first
        # =================================================================

        usde_balance_value = Decimal("0")
        try:
            usde_balance = market.balance("USDe")
            usde_balance_value = usde_balance.balance if hasattr(usde_balance, "balance") else usde_balance
            logger.debug(f"USDe balance: {usde_balance_value}")
        except (ValueError, KeyError) as e:
            logger.debug(f"Could not get USDe balance: {e}")

        # =================================================================
        # STEP 4: If USDe sufficient, stake directly
        # =================================================================

        if usde_balance_value >= self.min_stake_amount:
            logger.info(f"USDe balance ({usde_balance_value}) >= min_stake ({self.min_stake_amount}), staking")
            return self._create_stake_intent(usde_balance_value)

        # =================================================================
        # STEP 5: If swap enabled and USDC sufficient, swap USDC -> USDe
        # =================================================================

        if self.swap_usdc_to_usde and not self._swapped:
            try:
                usdc_balance = market.balance("USDC")
                usdc_balance_value = usdc_balance.balance if hasattr(usdc_balance, "balance") else usdc_balance
                logger.debug(f"USDC balance: {usdc_balance_value}")

                if usdc_balance_value >= self.min_usdc_amount:
                    logger.info(
                        f"USDC balance ({usdc_balance_value}) >= min_usdc ({self.min_usdc_amount}), swapping to USDe via Enso"
                    )
                    return self._create_swap_intent(usdc_balance_value)

            except (ValueError, KeyError) as e:
                logger.debug(f"Could not get USDC balance: {e}")

        # =================================================================
        # STEP 6: Insufficient balance - hold
        # =================================================================

        if self.swap_usdc_to_usde:
            return Intent.hold(
                reason=f"Insufficient balance: USDe={usde_balance_value} < {self.min_stake_amount}, swap_usdc_to_usde enabled but no USDC"
            )

        return Intent.hold(reason=f"Insufficient USDe balance: {usde_balance_value} < {self.min_stake_amount}")

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_stake_intent(self, amount: Decimal) -> Intent:
        """
        Create a STAKE intent to deposit USDe with Ethena.

        Staking flow:
        1. USDe is deposited into sUSDe vault contract
        2. You receive sUSDe (ERC4626 vault shares)
        3. sUSDe accrues value over time from yield

        Parameters:
            amount: Amount of USDe to stake

        Returns:
            StakeIntent ready for compilation
        """
        logger.info(f"STAKE intent: {format_token_amount_human(amount, 'USDe')} -> sUSDe")

        return Intent.stake(
            protocol="ethena",
            token_in="USDe",
            amount=amount,
            receive_wrapped=False,  # Ethena only outputs sUSDe
            chain="ethereum",
        )

    def _create_swap_intent(self, amount: Decimal) -> Intent:
        """
        Create a SWAP intent to convert USDC to USDe via Enso aggregator.

        Enso will find the best route across multiple DEXs to swap
        USDC to USDe (e.g., via Curve, Uniswap, or direct mint).

        Parameters:
            amount: Amount of USDC to swap

        Returns:
            SwapIntent ready for compilation
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"SWAP intent: {format_token_amount_human(amount, 'USDC')} -> USDe via Enso (slippage={self.max_slippage_pct}%)"
        )

        return Intent.swap(
            from_token="USDC",
            to_token="USDe",
            amount=amount,  # Swap the full USDC amount
            max_slippage=max_slippage,
            protocol="enso",
            chain="ethereum",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Updates internal state to track swap and staking status.
        """
        intent_type = intent.intent_type.value

        if success and intent_type == "SWAP":
            self._swapped = True
            # Extract amount from intent
            if hasattr(intent, "amount"):
                self._swapped_amount = intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
            logger.info(f"Swap successful: {self._swapped_amount} USDC -> USDe")

        elif success and intent_type == "STAKE":
            self._staked = True
            # Extract amount from intent
            if hasattr(intent, "amount"):
                self._staked_amount = intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
            logger.info(f"Staking successful: {self._staked_amount} USDe -> sUSDe")

        elif not success:
            logger.warning(f"{intent_type} failed")

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_ethena_yield",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "min_stake_amount": str(self.min_stake_amount),
                "swap_usdc_to_usde": self.swap_usdc_to_usde,
                "min_usdc_amount": str(self.min_usdc_amount),
                "max_slippage_pct": self.max_slippage_pct,
            },
            "state": {
                "swapped": self._swapped,
                "swapped_amount": str(self._swapped_amount),
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
            "swapped": self._swapped,
            "swapped_amount": str(self._swapped_amount),
            "staked": self._staked,
            "staked_amount": str(self._staked_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup.

        Called when resuming a strategy after crash/restart.
        """
        if "swapped" in state:
            self._swapped = state["swapped"]
            logger.info(f"Restored swapped state: {self._swapped}")

        if "swapped_amount" in state:
            self._swapped_amount = Decimal(str(state["swapped_amount"]))
            logger.info(f"Restored swapped_amount: {self._swapped_amount}")

        if "staked" in state:
            self._staked = state["staked"]
            logger.info(f"Restored staked state: {self._staked}")

        if "staked_amount" in state:
            self._staked_amount = Decimal(str(state["staked_amount"]))
            logger.info(f"Restored staked_amount: {self._staked_amount}")

    def get_open_positions(self):
        """Return open positions for teardown."""
        from almanak.framework.teardown import TeardownPositionSummary

        logger.warning(
            "%s: teardown not yet implemented — positions may remain open. "
            "Implement get_open_positions() and generate_teardown_intents() for real teardown.",
            self.__class__.__name__,
        )
        return TeardownPositionSummary.empty(self.strategy_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        """Return intents to close all positions."""
        return []


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("EthenaYieldStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {EthenaYieldStrategy.STRATEGY_NAME}")
    print(f"Version: {EthenaYieldStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {EthenaYieldStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {EthenaYieldStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {EthenaYieldStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {EthenaYieldStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  almanak strat run -d ethena_yield --once")

