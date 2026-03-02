"""
Morpho Blue Simple Supply -- Supply wstETH collateral to Morpho Blue on Ethereum.

First yailoop test of the Morpho Blue connector. Exercises the supply_collateral
path through the intent system: token resolution -> compile -> approve -> supply -> receipt parse.

NOTE: Intent.supply() with protocol="morpho_blue" always compiles to supply_collateral()
(deposit collateral for borrowing), NOT lending supply (earn interest). This is a design
gap documented in the experience report.

Market: wstETH/WETH (94.5% LLTV) on Ethereum
  - Loan token: WETH
  - Collateral token: wstETH
  - Market ID: 0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="morpho_blue_supply",
    description="Supply wstETH collateral to Morpho Blue on Ethereum",
    version="1.0.0",
    author="YAInnick Loop",
    tags=["incubating", "lending", "morpho-blue", "wsteth", "ethereum"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue"],
    intent_types=["SUPPLY", "HOLD"],
)
class MorphoBlueSupplyStrategy(IntentStrategy):
    """Supply wstETH as collateral to a Morpho Blue market on Ethereum.

    Configuration (config.json):
        market_id: Morpho Blue market identifier (required)
        supply_token: Token to supply as collateral (default: wstETH)
        supply_amount: Amount to supply (default: 0.01)
        force_action: "supply" to bypass balance checks (for testing)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.market_id = str(get_config("market_id", ""))
        self.supply_token = str(get_config("supply_token", "wstETH"))
        self.supply_amount = Decimal(str(get_config("supply_amount", "0.01")))
        self.force_action = str(get_config("force_action", "")).lower()

        # Internal state
        self._supplied = False
        self._supplied_amount = Decimal("0")

        if not self.market_id:
            raise ValueError("market_id is required in config.json for Morpho Blue strategies")

        logger.info(
            f"MorphoBlueSupplyStrategy initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"market={self.market_id[:16]}..."
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to supply collateral to Morpho Blue.

        Decision flow:
        1. If force_action="supply", supply immediately
        2. If already supplied, hold
        3. If wstETH balance >= supply_amount, supply
        4. Otherwise, hold
        """
        try:
            # Force action for testing
            if self.force_action == "supply":
                logger.info(f"Forced action: SUPPLY {self.supply_amount} {self.supply_token}")
                return self._create_supply_intent(self.supply_amount)

            # Already supplied
            if self._supplied:
                return Intent.hold(
                    reason=f"Already supplied {self._supplied_amount} {self.supply_token} to Morpho Blue"
                )

            # Check balance
            try:
                token_balance = market.balance(self.supply_token)
                balance_value = token_balance.balance if hasattr(token_balance, "balance") else token_balance
                logger.info(f"{self.supply_token} balance: {balance_value}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get {self.supply_token} balance: {e}")
                return Intent.hold(reason=f"Could not fetch {self.supply_token} balance: {e}")

            # Supply if sufficient balance
            if balance_value >= self.supply_amount:
                logger.info(
                    f"{self.supply_token} balance ({balance_value}) >= "
                    f"supply_amount ({self.supply_amount}), supplying"
                )
                return self._create_supply_intent(self.supply_amount)

            return Intent.hold(
                reason=f"Insufficient {self.supply_token}: {balance_value} < {self.supply_amount}"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _create_supply_intent(self, amount: Decimal) -> Intent:
        """Create a supply collateral intent for Morpho Blue."""
        logger.info(
            f"SUPPLY intent: {format_token_amount_human(amount, self.supply_token)} "
            f"-> Morpho Blue market {self.market_id[:16]}..."
        )
        return Intent.supply(
            protocol="morpho_blue",
            token=self.supply_token,
            amount=amount,
            use_as_collateral=True,
            market_id=self.market_id,
            chain="ethereum",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track supply state after execution."""
        intent_type = intent.intent_type.value

        if success and intent_type == "SUPPLY":
            self._supplied = True
            if hasattr(intent, "amount"):
                self._supplied_amount = (
                    intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
                )
            logger.info(
                f"Supply successful: {self._supplied_amount} {self.supply_token} "
                f"-> Morpho Blue market {self.market_id[:16]}..."
            )
        elif not success:
            logger.warning(f"{intent_type} failed -- will retry on next iteration")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "morpho_blue_supply",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "market_id": self.market_id[:16] + "...",
                "supply_token": self.supply_token,
                "supply_amount": str(self.supply_amount),
            },
            "state": {
                "supplied": self._supplied,
                "supplied_amount": str(self._supplied_amount),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "supplied": self._supplied,
            "supplied_amount": str(self._supplied_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "supplied" in state:
            self._supplied = state["supplied"]
            logger.info(f"Restored supplied state: {self._supplied}")
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
            logger.info(f"Restored supplied_amount: {self._supplied_amount}")
