"""
===============================================================================
TUTORIAL: Spark Lender Strategy - Supply DAI to Earn Yield
===============================================================================

This tutorial strategy demonstrates how to supply DAI to Spark lending
protocol to earn yield through interest on supplied assets.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors DAI balance in the wallet
2. When DAI balance > min_supply_amount: Supplies DAI to Spark
3. Receives spDAI (Spark's interest-bearing DAI token)
4. Holds when already supplied or insufficient balance

WHAT IS SPARK?
--------------
Spark is a decentralized lending protocol (Aave V3 fork) in the Maker/Sky ecosystem:
- Supply assets to earn yield from borrowers
- Supports DAI, USDC, WETH, wstETH, and other assets
- Interest rates are determined by supply/demand
- Currently ~5-8% APY on DAI (varies with market conditions)

Key differences from Aave V3:
- Focused on Maker ecosystem (DAI-centric)
- Different governance (MakerDAO)
- Slightly different asset selection

RISKS:
------
- Smart Contract Risk: Spark protocol bugs, exploits
- Interest Rate Risk: Rates fluctuate with market conditions
- Utilization Risk: High utilization can prevent withdrawals temporarily
- Oracle Risk: Price oracle manipulation could affect the protocol

USAGE:
------
    # Run once
    python -m src.cli.run --strategy demo_spark_lender --once

    # Run continuously
    python -m src.cli.run --strategy demo_spark_lender

    # Test on Anvil
    python strategies/demo/spark_lender/run_anvil.py

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
    name="demo_spark_lender",
    # Description
    description="Tutorial strategy - supply DAI to Spark for lending yield",
    # Version
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "lending", "spark", "dai", "yield"],
    # Supported chains (Spark is on Ethereum mainnet)
    supported_chains=["ethereum"],
    # Protocols used
    supported_protocols=["spark"],
    # Intent types this strategy may emit
    # SUPPLY: Supply DAI to earn yield
    # HOLD: No action
    intent_types=["SUPPLY", "HOLD"],
)
class SparkLenderStrategy(IntentStrategy):
    """
    Spark lending strategy for educational purposes.

    This strategy demonstrates:
    - How to supply DAI to Spark
    - How to receive interest-bearing spDAI
    - How to track supply state

    Configuration Parameters (from config.yaml or config.json):
    -----------------------------------------------------------
    - min_supply_amount: Minimum token balance to trigger supply (default: "100")
    - supply_token: Token symbol to supply (default: "DAI")
    - force_action: Force "supply" for testing

    Note: Spark automatically uses all supplied assets as collateral.
    Unlike Aave V3, this cannot be disabled per-asset.

    Example Config:
    ---------------
    {
        "min_supply_amount": "100",
        "supply_token": "DAI",
        "force_action": ""
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the Spark lending strategy.

        Extracts configuration and sets up internal state for tracking
        the supply process.
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration
        # =====================================================================

        # Supply configuration
        self.min_supply_amount = Decimal(str(self.get_config("min_supply_amount", "100")))
        self.supply_token = str(self.get_config("supply_token", "DAI"))

        # Force action for testing
        self.force_action = str(self.get_config("force_action", "")).lower()

        # Internal state tracking
        self._supplied = False
        self._supplied_amount = Decimal("0")

        logger.info(f"SparkLenderStrategy initialized: min_supply={self.min_supply_amount} {self.supply_token}")

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a supply decision based on wallet balance and configuration.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If already supplied, hold
        3. If DAI balance > min_supply_amount, supply
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: SUPPLY or HOLD
        """
        try:
            # =================================================================
            # STEP 1: Handle forced actions (for testing)
            # =================================================================

            if self.force_action == "supply":
                logger.info(f"Forced action: SUPPLY {self.supply_token}")
                return self._create_supply_intent(self.min_supply_amount)

            # =================================================================
            # STEP 2: Check if already supplied
            # =================================================================

            if self._supplied:
                return Intent.hold(reason=f"Already supplied {self._supplied_amount} {self.supply_token} -> sp{self.supply_token}")

            # =================================================================
            # STEP 3: Check DAI balance
            # =================================================================

            try:
                token_balance = market.balance(self.supply_token)
                # Extract balance value from TokenBalance object if needed
                balance_value = token_balance.balance if hasattr(token_balance, "balance") else token_balance
                logger.debug(f"{self.supply_token} balance: {balance_value}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get {self.supply_token} balance: {e}")
                return Intent.hold(reason=f"Could not fetch {self.supply_token} balance: {e}")

            # =================================================================
            # STEP 4: Supply if sufficient balance
            # =================================================================

            if balance_value >= self.min_supply_amount:
                logger.info(
                    f"{self.supply_token} balance ({balance_value}) >= min_supply ({self.min_supply_amount}), supplying"
                )
                return self._create_supply_intent(balance_value)

            # =================================================================
            # STEP 5: Insufficient balance - hold
            # =================================================================

            return Intent.hold(
                reason=f"Insufficient {self.supply_token} balance: {balance_value} < {self.min_supply_amount}"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_supply_intent(self, amount: Decimal) -> Intent:
        """
        Create a SUPPLY intent to deposit DAI into Spark.

        Supply flow:
        1. DAI is deposited into Spark pool
        2. You receive spDAI (interest-bearing token)
        3. spDAI accrues interest over time

        Parameters:
            amount: Amount of DAI to supply

        Returns:
            SupplyIntent ready for compilation
        """
        logger.info(f"SUPPLY intent: {format_token_amount_human(amount, self.supply_token)} -> Spark")

        # Note: Spark automatically uses all supplied assets as collateral.
        # Unlike Aave V3, this cannot be disabled per-asset.
        return Intent.supply(
            protocol="spark",
            token=self.supply_token,
            amount=amount,
            use_as_collateral=True,  # Always True for Spark
            chain="ethereum",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Updates internal state to track supply status.
        """
        intent_type = intent.intent_type.value

        if success and intent_type == "SUPPLY":
            self._supplied = True
            # Extract amount from intent
            if hasattr(intent, "amount"):
                self._supplied_amount = intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
            logger.info(f"Supply successful: {self._supplied_amount} {self.supply_token} -> Spark")
        elif not success:
            logger.warning(f"{intent_type} failed")

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_spark_lender",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "min_supply_amount": str(self.min_supply_amount),
                "supply_token": self.supply_token,
            },
            "state": {
                "supplied": self._supplied,
                "supplied_amount": str(self._supplied_amount),
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
            "supplied": self._supplied,
            "supplied_amount": str(self._supplied_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup.

        Called when resuming a strategy after crash/restart.
        """
        if "supplied" in state:
            self._supplied = state["supplied"]
            logger.info(f"Restored supplied state: {self._supplied}")

        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
            logger.info(f"Restored supplied_amount: {self._supplied_amount}")


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("SparkLenderStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {SparkLenderStrategy.STRATEGY_NAME}")
    print(f"Version: {SparkLenderStrategy.STRATEGY_METADATA.get('version', 'N/A')}")
    print(f"Supported Chains: {SparkLenderStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {SparkLenderStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {SparkLenderStrategy.INTENT_TYPES}")
    print(f"\nDescription: {SparkLenderStrategy.STRATEGY_METADATA.get('description', 'N/A')}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_spark_lender --once")
