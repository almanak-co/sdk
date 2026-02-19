"""
Buy The Dip Strategy (v2)

RSI-based accumulation strategy that buys a target token on oversold signals
and takes profit on overbought signals, with net accumulation guaranteed by
enforcing sell_percentage < buy_percentage.

Key behaviors:
- Signal change detection: Only trades when RSI crosses into a new zone,
  preventing repeated trades while RSI stays in the same zone.
- Cooldown enforcement: Minimum time between consecutive trades.
- Percentage-based sizing: Buys X% of quote balance, sells Y% of base balance.
- Dust threshold: Stops trading when quote balance falls below threshold.
- Teardown: Converts all base token holdings back to quote token.
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

SIGNAL_NEUTRAL = "NEUTRAL"
SIGNAL_OVERSOLD = "OVERSOLD"
SIGNAL_OVERBOUGHT = "OVERBOUGHT"


@almanak_strategy(
    name="buy_the_dip",
    description="RSI-based dip buying with net accumulation - buys oversold, sells overbought",
    version="1.0.0",
    author="Almanak",
    tags=["trading", "rsi", "accumulation", "mean-reversion", "dip-buying"],
    supported_chains=["arbitrum", "ethereum", "base", "optimism", "avalanche"],
    supported_protocols=["uniswap_v3", "pancakeswap_v3", "aerodrome", "trader_joe_v2"],
    intent_types=["SWAP", "HOLD"],
)
class BuyTheDipStrategy(IntentStrategy):
    """RSI-based accumulation strategy.

    Buys a target token (base_token) when RSI crosses into oversold territory
    and sells a smaller portion when RSI crosses into overbought territory,
    resulting in net accumulation of the target token over time.

    The strategy only acts on signal CHANGES (RSI crossing into a new zone),
    not on RSI staying within a zone. This prevents spam trading during
    extended oversold/overbought periods.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token pair and protocol
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.protocol = get_config("protocol", "uniswap_v3")

        # RSI parameters
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", "70")))

        # Trade sizing (percentage of balance)
        self.buy_percentage = Decimal(str(get_config("buy_percentage", "0.20")))
        self.sell_percentage = Decimal(str(get_config("sell_percentage", "0.15")))

        # Validate net accumulation: sell% must be less than buy%
        if self.sell_percentage >= self.buy_percentage:
            raise ValueError(
                f"sell_percentage ({self.sell_percentage}) must be less than "
                f"buy_percentage ({self.buy_percentage}) for net accumulation"
            )

        # Cooldown and thresholds
        self.cooldown_minutes = int(get_config("cooldown_minutes", 60))
        self.dust_threshold_usd = Decimal(str(get_config("dust_threshold_usd", "0.50")))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self._max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        # Internal state
        self._last_rsi_signal = SIGNAL_NEUTRAL
        self._last_buy_time: datetime | None = None
        self._last_sell_time: datetime | None = None
        self._buy_count = 0
        self._sell_count = 0
        self._terminated = False
        self._pending_trade: dict[str, Any] | None = None

        logger.info(
            f"BuyTheDipStrategy initialized: "
            f"pair={self.base_token}/{self.quote_token} via {self.protocol}, "
            f"RSI({self.rsi_period}) oversold={self.rsi_oversold}/overbought={self.rsi_overbought}, "
            f"buy={self.buy_percentage*100}%/sell={self.sell_percentage*100}%, "
            f"cooldown={self.cooldown_minutes}min"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Evaluate RSI signals and return a swap or hold intent."""
        try:
            if self._terminated:
                return Intent.hold(reason="Strategy terminated (quote balance below dust threshold)")

            # Get RSI
            try:
                rsi = market.rsi(self.base_token, period=self.rsi_period)
            except ValueError as e:
                logger.warning(f"RSI unavailable: {e}")
                return Intent.hold(reason="RSI data unavailable")

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Balance check failed: {e}")
                return Intent.hold(reason="Balance data unavailable")

            rsi_value = Decimal(str(rsi.value))
            now = datetime.now(UTC)

            # Determine current RSI zone
            if rsi_value <= self.rsi_oversold:
                current_signal = SIGNAL_OVERSOLD
            elif rsi_value >= self.rsi_overbought:
                current_signal = SIGNAL_OVERBOUGHT
            else:
                current_signal = SIGNAL_NEUTRAL

            # Detect signal change
            signal_changed = current_signal != self._last_rsi_signal
            previous_signal = self._last_rsi_signal

            # Always update the signal tracker
            if signal_changed:
                self._last_rsi_signal = current_signal
                logger.info(f"RSI signal change: {previous_signal} -> {current_signal} (RSI={rsi_value:.2f})")
                self._emit_signal_change(previous_signal, current_signal, rsi_value)

            # Check for termination: quote balance below dust threshold
            if quote_balance.balance_usd < self.dust_threshold_usd:
                if current_signal == SIGNAL_OVERSOLD and signal_changed:
                    # Would buy but can't - terminate
                    logger.info(
                        f"Quote balance {format_usd(quote_balance.balance_usd)} below dust threshold "
                        f"{format_usd(self.dust_threshold_usd)} - terminating"
                    )
                    self._terminated = True
                    self._emit_event("TERMINATED", f"Quote balance below dust threshold ({format_usd(self.dust_threshold_usd)})")
                    return Intent.hold(reason="Terminated: quote balance below dust threshold")

            # BUY: RSI crossed into oversold zone
            if current_signal == SIGNAL_OVERSOLD and signal_changed:
                return self._handle_buy(quote_balance, rsi_value, now)

            # SELL: RSI crossed into overbought zone
            if current_signal == SIGNAL_OVERBOUGHT and signal_changed:
                return self._handle_sell(base_balance, market, rsi_value, now)

            # HOLD: neutral zone or no signal change
            return Intent.hold(
                reason=f"RSI={rsi_value:.2f} signal={current_signal} "
                f"(buys={self._buy_count}, sells={self._sell_count})"
            )

        except Exception as e:
            logger.exception("Error in decide()")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # TRADE HANDLERS
    # =========================================================================

    def _round_down(self, amount: Decimal, token: str) -> Decimal:
        """Round down to token's decimal precision to avoid overspending."""
        from almanak.framework.data.tokens import get_token_resolver

        token_info = get_token_resolver().resolve_for_swap(token, self.chain)
        return amount.quantize(Decimal(10) ** -token_info.decimals, rounding=ROUND_DOWN)

    def _handle_buy(self, quote_balance, rsi_value: Decimal, now: datetime) -> Intent:
        """Handle a buy signal (RSI crossed into oversold zone)."""
        # Check cooldown
        if self._last_buy_time and (now - self._last_buy_time).total_seconds() < self.cooldown_minutes * 60:
            remaining = self.cooldown_minutes - (now - self._last_buy_time).total_seconds() / 60
            return Intent.hold(reason=f"Buy cooldown active ({remaining:.0f}min remaining)")

        # Check balance (recalculate USD from rounded amount)
        buy_amount = self._round_down(self.buy_percentage * quote_balance.balance, self.quote_token)
        price_per_unit = quote_balance.balance_usd / quote_balance.balance if quote_balance.balance > 0 else Decimal("0")
        buy_amount_usd = buy_amount * price_per_unit

        if buy_amount_usd < self.dust_threshold_usd:
            return Intent.hold(
                reason=f"Buy amount {format_usd(buy_amount_usd)} below dust threshold"
            )

        logger.info(
            f"BUY: RSI={rsi_value:.2f} | "
            f"Spending {self.buy_percentage*100:.0f}% of {self.quote_token} "
            f"({format_token_amount_human(buy_amount, self.quote_token)}) "
            f"on {self.base_token}"
        )

        self._pending_trade = {"side": "BUY", "amount": buy_amount, "rsi": rsi_value}

        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount=buy_amount,
            max_slippage=self._max_slippage,
            protocol=self.protocol,
        )

    def _handle_sell(self, base_balance, market: MarketSnapshot, rsi_value: Decimal, now: datetime) -> Intent:
        """Handle a sell signal (RSI crossed into overbought zone)."""
        # Check cooldown
        if self._last_sell_time and (now - self._last_sell_time).total_seconds() < self.cooldown_minutes * 60:
            remaining = self.cooldown_minutes - (now - self._last_sell_time).total_seconds() / 60
            return Intent.hold(reason=f"Sell cooldown active ({remaining:.0f}min remaining)")

        # Check balance (recalculate USD from rounded amount)
        sell_amount = self._round_down(self.sell_percentage * base_balance.balance, self.base_token)
        price_per_unit = base_balance.balance_usd / base_balance.balance if base_balance.balance > 0 else Decimal("0")
        sell_amount_usd = sell_amount * price_per_unit

        if sell_amount_usd < self.dust_threshold_usd:
            return Intent.hold(
                reason="Sell value below dust threshold"
            )

        logger.info(
            f"SELL: RSI={rsi_value:.2f} | "
            f"Selling {self.sell_percentage*100:.0f}% of {self.base_token} "
            f"({format_token_amount_human(sell_amount, self.base_token)}) "
            f"for {self.quote_token}"
        )

        self._pending_trade = {"side": "SELL", "amount": sell_amount, "rsi": rsi_value}

        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount=sell_amount,
            max_slippage=self._max_slippage,
            protocol=self.protocol,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, _intent: Any, success: bool, result: Any) -> None:
        """Update cooldowns and counters only after successful execution."""
        if not self._pending_trade:
            return

        trade = self._pending_trade
        self._pending_trade = None

        if not success:
            logger.warning(f"Trade intent failed ({trade['side']}) - state not updated")
            return

        now = datetime.now(UTC)
        if trade["side"] == "BUY":
            self._last_buy_time = now
            self._buy_count += 1
        else:
            self._last_sell_time = now
            self._sell_count += 1

        self._emit_trade(trade["side"], trade["amount"], trade["rsi"])

    # =========================================================================
    # TIMELINE EVENTS
    # =========================================================================

    def _emit_signal_change(self, old_signal: str, new_signal: str, rsi_value: Decimal) -> None:
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"RSI signal: {old_signal} -> {new_signal} (RSI={rsi_value:.2f})",
                strategy_id=self.strategy_id,
                details={"old_signal": old_signal, "new_signal": new_signal, "rsi": str(rsi_value)},
            )
        )

    def _emit_trade(self, side: str, amount: Decimal, rsi_value: Decimal) -> None:
        token = self.quote_token if side == "BUY" else self.base_token
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.POSITION_MODIFIED,
                description=f"{side} {format_token_amount_human(amount, token)} (RSI={rsi_value:.2f})",
                strategy_id=self.strategy_id,
                details={
                    "side": side,
                    "token": token,
                    "amount": str(amount),
                    "rsi": str(rsi_value),
                    "buy_count": self._buy_count,
                    "sell_count": self._sell_count,
                },
            )
        )

    def _emit_event(self, event_name: str, description: str) -> None:
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=description,
                strategy_id=self.strategy_id,
                details={"event": event_name},
            )
        )

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Serialize state for crash recovery."""
        return {
            "last_rsi_signal": self._last_rsi_signal,
            "last_buy_time": self._last_buy_time.isoformat() if self._last_buy_time else None,
            "last_sell_time": self._last_sell_time.isoformat() if self._last_sell_time else None,
            "buy_count": self._buy_count,
            "sell_count": self._sell_count,
            "terminated": self._terminated,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state on startup."""
        if "last_rsi_signal" in state:
            self._last_rsi_signal = state["last_rsi_signal"]
        if "last_buy_time" in state and state["last_buy_time"]:
            self._last_buy_time = datetime.fromisoformat(state["last_buy_time"])
        if "last_sell_time" in state and state["last_sell_time"]:
            self._last_sell_time = datetime.fromisoformat(state["last_sell_time"])
        if "buy_count" in state:
            self._buy_count = int(state["buy_count"])
        if "sell_count" in state:
            self._sell_count = int(state["sell_count"])
        if "terminated" in state:
            self._terminated = bool(state["terminated"])

        logger.info(
            f"Restored state: signal={self._last_rsi_signal}, "
            f"buys={self._buy_count}, sells={self._sell_count}, "
            f"terminated={self._terminated}"
        )

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "buy_the_dip",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pair": f"{self.base_token}/{self.quote_token}",
                "protocol": self.protocol,
                "rsi_period": self.rsi_period,
                "rsi_oversold": str(self.rsi_oversold),
                "rsi_overbought": str(self.rsi_overbought),
                "buy_percentage": str(self.buy_percentage),
                "sell_percentage": str(self.sell_percentage),
                "cooldown_minutes": self.cooldown_minutes,
                "dust_threshold_usd": str(self.dust_threshold_usd),
            },
            "state": {
                "last_rsi_signal": self._last_rsi_signal,
                "buy_count": self._buy_count,
                "sell_count": self._sell_count,
                "terminated": self._terminated,
            },
        }

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = [
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="buy_the_dip_base_holdings",
                chain=self.chain,
                protocol=self.protocol,
                value_usd=Decimal("0"),  # Actual value from on-chain balance
                details={
                    "asset": self.base_token,
                    "quote_token": self.quote_token,
                    "buy_count": self._buy_count,
                    "sell_count": self._sell_count,
                },
            )
        ]

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "buy_the_dip"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else self._max_slippage

        logger.info(f"Teardown: swapping all {self.base_token} -> {self.quote_token} (mode={mode.value})")

        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol=self.protocol,
            )
        ]

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown completed. Recovered {format_usd(recovered_usd)}")
            # Reset _terminated: teardown converts base -> quote, restoring the balance
            # that triggered termination. Strategy can resume with fresh capital.
            self._terminated = False
            self._last_rsi_signal = SIGNAL_NEUTRAL
            self._buy_count = 0
            self._sell_count = 0
            self._last_buy_time = None
            self._last_sell_time = None
        else:
            logger.error("Teardown failed - manual intervention may be required")
