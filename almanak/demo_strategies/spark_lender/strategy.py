"""
===============================================================================
TUTORIAL: Spark Lender Strategy - Supply an Asset to Earn Yield
===============================================================================

This tutorial strategy demonstrates how to supply an asset (default WETH) to the
Spark lending protocol to earn yield through interest on supplied assets. The
asset is configurable via `supply_token`.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors the supply-token balance in the wallet
2. When balance > min_supply_amount: Supplies the asset to Spark
3. Receives the Spark interest-bearing token (e.g. spWETH)
4. Holds when already supplied or insufficient balance
5. Teardown reclaims the full LIVE supply (principal + accrued interest) via
   withdraw_all (VIB-5465 live per-position close)

NOTE ON ASSET CHOICE:
---------------------
Default is WETH, a Spark *collateral* asset (LTV > 0). DAI is now LTV=0 on Spark
(post-USDS migration): it can still be supplied, but reports zero USD collateral
value, which the lending teardown guard currently treats as "nothing to
withdraw" — so a DAI deposit would be stranded at teardown.

WHAT IS SPARK?
--------------
Spark is a decentralized lending protocol (Aave V3 fork) in the Maker/Sky ecosystem:
- Supply assets to earn yield from borrowers
- Supports DAI, USDC, WETH, wstETH, and other assets
- Interest rates are determined by supply/demand
- Yield varies per asset and with market conditions

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
    python almanak/demo_strategies/spark_lender/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

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
    description="Tutorial strategy - supply an asset (default WETH) to Spark for lending yield",
    # Version
    version="1.1.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "lending", "spark", "weth", "yield"],
    # Supported chains (Spark is on Ethereum mainnet)
    supported_chains=["ethereum"],
    # Protocols used
    supported_protocols=["spark"],
    # Intent types this strategy may emit
    # SUPPLY: Supply the asset (default WETH) to earn yield
    # WITHDRAW: Reclaim the supplied asset at teardown (withdraw_all -> live balance)
    # HOLD: No action
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
    quote_asset="USD",
)
class SparkLenderStrategy(IntentStrategy):
    """
    Spark lending strategy for educational purposes.

    This strategy demonstrates:
    - How to supply an asset (default WETH) to Spark
    - How to receive the interest-bearing Spark token (e.g. spWETH)
    - How to track supply state

    Configuration Parameters (from config.yaml or config.json):
    -----------------------------------------------------------
    - min_supply_amount: Minimum token balance to trigger supply (default: "0.1")
    - supply_token: Token symbol to supply (default: "WETH")
    - force_action: Force "supply" for testing

    Note: Spark automatically uses all supplied assets as collateral.
    Unlike Aave V3, this cannot be disabled per-asset.

    Example Config:
    ---------------
    {
        "min_supply_amount": "0.1",
        "supply_token": "WETH",
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
        # Default to WETH: a Spark collateral asset with LTV > 0. A non-collateral
        # reserve (e.g. DAI is LTV=0 on Spark post-USDS migration) supplies fine
        # but reports zero USD collateral value, which the lending teardown guard
        # currently treats as "nothing to withdraw" (tracked separately).
        self.min_supply_amount = Decimal(str(self.get_config("min_supply_amount", "0.1")))
        self.supply_token = str(self.get_config("supply_token", "WETH"))

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
        3. If supply-token balance > min_supply_amount, supply
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: SUPPLY or HOLD
        """
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
        # STEP 3: Check supply-token balance
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

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_supply_intent(self, amount: Decimal) -> Intent:
        """
        Create a SUPPLY intent to deposit the asset into Spark.

        Supply flow:
        1. The asset is deposited into the Spark pool
        2. You receive the interest-bearing Spark token (e.g. spWETH)
        3. The Spark token accrues interest over time

        Parameters:
            amount: Amount to supply

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
        elif success and intent_type == "WITHDRAW":
            # Teardown withdrew the supply — clear cached state so status /
            # get_open_positions() no longer report an open position and a later
            # full-close is never re-emitted for an already-withdrawn deposit.
            self._supplied = False
            self._supplied_amount = Decimal("0")
            logger.info(f"Withdraw successful: cleared Spark supply state for {self.supply_token}")
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

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        """Return the open Spark supply position (interest-bearing Spark token).

        The supplied collateral is a single SUPPLY position. Its size is the
        LIVE Spark-token balance (principal + accrued interest) — we do NOT
        freeze a plan-build amount here; the teardown WITHDRAW resolves the live
        figure at execution (see generate_teardown_intents / VIB-5465).
        """
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._supplied and self._supplied_amount > 0:
            # Best-effort live valuation for the preview / loss-cap only — never
            # used to size the exit (which resolves live via withdraw_all).
            try:
                market = self.create_market_snapshot()
                supply_price = Decimal(str(market.price(self.supply_token)))
            except Exception:
                logger.warning("Unable to fetch live price in Spark teardown valuation")
                supply_price = Decimal("0")

            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"spark-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="spark",
                    value_usd=self._supplied_amount * supply_price,
                    details={"asset": self.supply_token, "type": "collateral"},
                )
            )

        return TeardownPositionSummary(
            deployment_id=self.deployment_id or self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        """Close the Spark supply position fully at the LIVE on-chain size.

        VIB-5465 / VIB-5417: delegates to the framework's per-KNOWN-position
        live full-close helper. The SUPPLY position compiles to
        ``Intent.withdraw(..., withdraw_all=True)`` — MAX_UINT256, so Spark
        settles the full Spark-token balance (e.g. spWETH) INCLUDING interest
        accrued while held, resolved at execution rather than a plan-build snapshot.
        """
        return self.teardown_full_close_intents()


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("SparkLenderStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {SparkLenderStrategy.STRATEGY_NAME}")
    print(f"Version: {SparkLenderStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {SparkLenderStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {SparkLenderStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {SparkLenderStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {SparkLenderStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_spark_lender --once")

