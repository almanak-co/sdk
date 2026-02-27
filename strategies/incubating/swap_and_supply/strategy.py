"""Swap-and-Supply IntentSequence Strategy.

YAInnick Loop Iteration 15: First-ever IntentSequence test.

Swaps USDC -> WETH on Uniswap V3, then supplies ALL received WETH to Aave V3
as collateral. Uses amount="all" chaining to pass the swap output directly
to the supply step -- validating the amount chaining pipeline that has been
identified as the most pervasive latent bug (11/13 strategies affected).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="swap_and_supply",
    description="IntentSequence: swap USDC->WETH then supply to Aave V3",
    version="1.0.0",
    author="YAInnick Loop (Iteration 15)",
    tags=["demo", "intent-sequence", "multi-protocol", "lending", "swap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3", "aave_v3"],
    intent_types=["SWAP", "SUPPLY", "HOLD"],
)
class SwapAndSupplyStrategy(IntentStrategy):
    """Swap USDC -> WETH on Uniswap V3 and supply all WETH to Aave V3.

    This strategy exercises IntentSequence with amount="all" chaining:
    1. SwapIntent: Buy WETH with a fixed USD amount of USDC
    2. SupplyIntent: Supply ALL received WETH to Aave V3 (amount="all")

    The framework must extract the swap output amount from the receipt
    and resolve the chained "all" to a concrete Decimal before compiling
    the supply step.

    Configuration:
        swap_amount_usd: USD value to swap (default: 5)
        max_slippage_bps: Max slippage in basis points (default: 100 = 1%)
        base_token: Token to buy (default: WETH)
        quote_token: Token to sell (default: USDC)
        lending_protocol: Where to supply (default: aave_v3)
        force_action: Set to "execute" to bypass signal check
    """

    STRATEGY_NAME = "swap_and_supply"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.swap_amount_usd = Decimal(str(get_config("swap_amount_usd", "5")))
        self.max_slippage = Decimal(str(get_config("max_slippage_bps", "100"))) / Decimal("10000")
        self.base_token = str(get_config("base_token", "WETH"))
        self.quote_token = str(get_config("quote_token", "USDC"))
        self.lending_protocol = str(get_config("lending_protocol", "aave_v3"))
        self.force_action = str(get_config("force_action", "")).lower()

        self._executed = False

        logger.info(
            f"SwapAndSupply initialized: {self.quote_token} -> {self.base_token} "
            f"({format_usd(self.swap_amount_usd)}) -> {self.lending_protocol} supply"
        )

    def decide(self, market: MarketSnapshot) -> Any:
        """Decide whether to execute the swap-and-supply sequence.

        Returns an IntentSequence with two steps:
        1. SwapIntent: USDC -> WETH (fixed amount)
        2. SupplyIntent: WETH -> Aave V3 (amount="all" -- chained from swap)
        """
        try:
            # Already executed -- hold
            if self._executed:
                return Intent.hold(reason="Sequence already executed")

            # Get market data for logging
            try:
                base_price = market.price(self.base_token)
                logger.info(f"{self.base_token} price: ${float(base_price):,.2f}")
            except Exception as e:
                logger.warning(f"Could not get {self.base_token} price: {e}")
                base_price = None

            # Check balance
            try:
                quote_balance = market.balance(self.quote_token)
                logger.info(f"{self.quote_token} balance: {float(quote_balance):.2f}")
                if quote_balance < self.swap_amount_usd:
                    return Intent.hold(
                        reason=f"Insufficient {self.quote_token} balance "
                        f"({float(quote_balance):.2f} < {self.swap_amount_usd})"
                    )
            except Exception as e:
                logger.warning(f"Could not check {self.quote_token} balance: {e}")

            # Force action bypass for testing
            if self.force_action == "execute":
                logger.info("Forced action: executing swap-and-supply sequence")
                return self._build_sequence()

            # Signal-based entry: execute if price data is available
            if base_price is not None:
                logger.info(
                    f"Executing swap-and-supply: {format_usd(self.swap_amount_usd)} "
                    f"{self.quote_token} -> {self.base_token} -> {self.lending_protocol}"
                )
                return self._build_sequence()

            return Intent.hold(reason="No price data available")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _build_sequence(self) -> Any:
        """Build the swap-and-supply IntentSequence."""
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=(
                    f"Building sequence: {self.quote_token} -> {self.base_token} -> "
                    f"{self.lending_protocol} supply"
                ),
                strategy_id=self.strategy_id,
                details={
                    "step_1": "swap",
                    "step_2": "supply",
                    "amount_usd": str(self.swap_amount_usd),
                    "chaining": "amount='all'",
                },
            )
        )

        return Intent.sequence(
            [
                # Step 1: Swap USDC -> WETH (fixed USD amount)
                Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.swap_amount_usd,
                    max_slippage=self.max_slippage,
                    protocol="uniswap_v3",
                    chain=self.chain,
                ),
                # Step 2: Supply ALL received WETH to Aave V3 (amount chaining)
                Intent.supply(
                    protocol=self.lending_protocol,
                    token=self.base_token,
                    amount="all",
                    use_as_collateral=True,
                    chain=self.chain,
                ),
            ],
            description=(
                f"Swap {format_usd(self.swap_amount_usd)} {self.quote_token} -> "
                f"{self.base_token}, supply all to {self.lending_protocol}"
            ),
        )

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Handle execution results for each intent in the sequence."""
        intent_type = getattr(intent, "intent_type", None)
        type_name = intent_type.value if intent_type else "UNKNOWN"

        if success:
            logger.info(f"[SUCCESS] {type_name} executed successfully")

            if type_name == "SWAP":
                # Check if swap_amounts were extracted (critical for amount chaining)
                swap_amounts = getattr(result, "swap_amounts", None)
                if swap_amounts:
                    logger.info(
                        f"  Swap output: {swap_amounts.amount_out_decimal} {self.base_token} "
                        f"(amount chaining should pass this to supply step)"
                    )
                else:
                    logger.warning(
                        "  swap_amounts NOT extracted from receipt -- "
                        "amount='all' in next step will FAIL"
                    )

            elif type_name == "SUPPLY":
                self._executed = True
                logger.info(
                    f"  Supply to {self.lending_protocol} complete -- "
                    f"IntentSequence with amount chaining SUCCEEDED"
                )

                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=(
                            f"Swap-and-supply sequence completed: "
                            f"{self.quote_token} -> {self.base_token} -> {self.lending_protocol}"
                        ),
                        strategy_id=self.strategy_id,
                        details={"action": "sequence_complete"},
                    )
                )
        else:
            error_msg = getattr(result, "error", "unknown error")
            logger.warning(f"[FAILED] {type_name} failed: {error_msg}")

            if type_name == "SUPPLY":
                logger.warning(
                    "  Supply failed -- check if amount='all' was resolved correctly. "
                    "This may indicate the amount chaining bug (11/13 strategies affected)."
                )
