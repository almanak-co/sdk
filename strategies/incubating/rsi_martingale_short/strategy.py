"""RSI Martingale Short - Overbought Mean-Reversion Short with Martingale Sizing.

THESIS:
------
Short tokens showing RSI overbought conditions after a significant rally.
Use martingale-style position sizing: start small so you can double down ~5 times
if the pump continues. A retracement is near-certain after a parabolic move --
the question is *when* and *how deep*.

TARGET: 30% retracement from the local top (conservative -- most go to 50-61.8% Fib).

MARTINGALE SIZING:
-----------------
Given a risk budget R and max_doublings N, the initial collateral is:
    initial_collateral = R / (2^(N+1) - 1)

This ensures all doublings fit exactly within the risk budget:
    Level 0: C,  Level 1: 2C,  Level 2: 4C, ... Level N: 2^N * C
    Total = C * (2^(N+1) - 1) = R

Each doubling triggers when price moves another X% higher from the last entry.
All positions close together when the retracement target is hit.

PROTOCOL: GMX V2 perpetual futures (Arbitrum).
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="rsi_martingale_short",
    description="Overbought RSI mean-reversion short with martingale position sizing on GMX V2",
    version="1.0.0",
    author="YAInnick Loop",
    tags=["perps", "gmx", "mean-reversion", "martingale", "short", "rsi"],
    supported_chains=["arbitrum", "avalanche"],
    supported_protocols=["gmx_v2"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class RSIMartingaleShortStrategy(IntentStrategy):
    """Overbought RSI mean-reversion short with martingale position sizing.

    State machine:
        SCANNING  -> no position, watching RSI + price change
        POSITIONED -> active short at martingale level N, monitoring for exit/doubling
        COOLDOWN  -> recently closed, waiting before re-entry
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse config (handles dict, DictConfigWrapper, or dataclass)
        if hasattr(self.config, "get"):
            cfg = self.config
        elif isinstance(self.config, dict):
            cfg = self.config
        else:
            cfg = {}

        # Market params
        self.market = cfg.get("market", "ETH/USD")
        self.collateral_token = cfg.get("collateral_token", "USDC")

        # Risk budget: total collateral across all martingale levels
        self.risk_budget_usd = Decimal(str(cfg.get("risk_budget_usd", "100")))
        self.leverage = Decimal(str(cfg.get("leverage", "2.0")))

        # RSI entry filter
        self.rsi_threshold = Decimal(str(cfg.get("rsi_threshold", "75")))
        self.rsi_period = int(cfg.get("rsi_period", 14))

        # Rally filter: minimum 24h price change to qualify
        self.rally_threshold_24h_pct = Decimal(str(cfg.get("rally_threshold_24h_pct", "5")))

        # Martingale params
        self.max_doublings = int(cfg.get("max_doublings", 5))
        self.doubling_trigger_pct = Decimal(str(cfg.get("doubling_trigger_pct", "10")))

        # Exit params
        self.retracement_target_pct = Decimal(str(cfg.get("retracement_target_pct", "30")))
        self.hard_stop_above_last_entry_pct = Decimal(
            str(cfg.get("hard_stop_above_last_entry_pct", "15"))
        )
        self.time_stop_hours = int(cfg.get("time_stop_hours", 168))

        # Slippage
        self.max_slippage_pct = Decimal(str(cfg.get("max_slippage_pct", "1.0")))

        # Cooldown
        self.cooldown_minutes = int(cfg.get("cooldown_minutes", 60))

        # Force action (testing)
        self.force_action = cfg.get("force_action", None)

        # Compute initial collateral per the martingale formula
        # Total = C * (2^(N+1) - 1) = R  =>  C = R / (2^(N+1) - 1)
        total_levels = self.max_doublings + 1
        divisor = Decimal(str(2**total_levels - 1))
        self._initial_collateral = (self.risk_budget_usd / divisor).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

        # --- Internal state ---
        self._martingale_level = 0  # Current doubling level (0 = initial)
        self._entry_prices: list[Decimal] = []  # Price at each entry
        self._entry_collaterals: list[Decimal] = []  # Collateral at each entry
        self._local_top = Decimal("0")  # Highest price seen since first entry
        self._total_collateral_deployed = Decimal("0")
        self._total_position_size_usd = Decimal("0")
        self._first_entry_at: datetime | None = None
        self._last_close_at: datetime | None = None
        self._trades_opened = 0
        self._trades_closed = 0
        self._wins = 0
        self._losses = 0

        logger.info(
            f"RSIMartingaleShort initialized: market={self.market}, "
            f"budget={format_usd(self.risk_budget_usd)}, "
            f"initial_collateral={format_usd(self._initial_collateral)}, "
            f"leverage={self.leverage}x, "
            f"max_doublings={self.max_doublings}, "
            f"RSI>{self.rsi_threshold}, "
            f"retracement_target={self.retracement_target_pct}%"
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def _has_position(self) -> bool:
        return len(self._entry_prices) > 0

    @property
    def _collateral_for_level(self) -> Decimal:
        """Collateral for the current martingale level."""
        return (self._initial_collateral * Decimal(str(2**self._martingale_level))).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

    # =========================================================================
    # Main decision logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide action based on RSI, price, and martingale state."""
        try:
            index_token = self.market.split("/")[0]

            # Get current price
            try:
                current_price = market.price(index_token)
            except ValueError:
                default_prices = {"ETH": Decimal("3500"), "BTC": Decimal("95000")}
                current_price = default_prices.get(index_token, Decimal("100"))
                logger.warning(f"Price for {index_token} unavailable, using ${current_price}")

            # Handle forced actions (testing)
            if self.force_action:
                return self._handle_force_action(self.force_action, current_price)

            # Update local top if we have a position
            if self._has_position and current_price > self._local_top:
                self._local_top = current_price

            # Branch on state
            if self._has_position:
                return self._decide_positioned(market, current_price, index_token)
            else:
                return self._decide_scanning(market, current_price, index_token)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _decide_scanning(
        self, market: MarketSnapshot, current_price: Decimal, index_token: str
    ) -> Intent | None:
        """No position open -- scan for entry conditions."""
        now = datetime.now(UTC)

        # Check cooldown
        if self._last_close_at:
            elapsed = now - self._last_close_at
            if elapsed < timedelta(minutes=self.cooldown_minutes):
                remaining = timedelta(minutes=self.cooldown_minutes) - elapsed
                return Intent.hold(
                    reason=f"Cooldown: {int(remaining.total_seconds())}s remaining"
                )

        # Check RSI
        try:
            rsi = market.rsi(index_token, period=self.rsi_period)
        except ValueError as e:
            return Intent.hold(reason=f"RSI unavailable: {e}")

        if rsi.value < self.rsi_threshold:
            return Intent.hold(
                reason=f"RSI={rsi.value:.1f} < {self.rsi_threshold} (not overbought)"
            )

        # Check rally magnitude (24h price change)
        try:
            price_data = market.price_data(index_token)
            change_24h = price_data.change_24h_pct
        except (ValueError, AttributeError):
            change_24h = Decimal("0")
            logger.debug("24h price change unavailable, skipping rally filter")

        if change_24h < self.rally_threshold_24h_pct and change_24h != Decimal("0"):
            return Intent.hold(
                reason=f"RSI={rsi.value:.1f} overbought but 24h change={change_24h:.1f}% "
                f"< {self.rally_threshold_24h_pct}% threshold"
            )

        # Entry conditions met -- open initial short
        logger.info(
            f"ENTRY SIGNAL: RSI={rsi.value:.1f} > {self.rsi_threshold} "
            f"(overbought), 24h change={change_24h:.1f}% | Opening initial SHORT"
        )
        return self._create_open_intent(current_price, level=0)

    def _decide_positioned(
        self, market: MarketSnapshot, current_price: Decimal, index_token: str
    ) -> Intent | None:
        """Position open -- check for exit or doubling."""
        now = datetime.now(UTC)
        last_entry_price = self._entry_prices[-1]

        # --- EXIT CHECK 1: Retracement target hit ---
        if self._local_top > Decimal("0"):
            retracement_price = self._local_top * (
                Decimal("1") - self.retracement_target_pct / Decimal("100")
            )
            if current_price <= retracement_price:
                retracement_pct = (
                    (self._local_top - current_price) / self._local_top * Decimal("100")
                )
                logger.info(
                    f"TAKE PROFIT: Price ${current_price:,.2f} dropped "
                    f"{retracement_pct:.1f}% from local top ${self._local_top:,.2f} "
                    f"(target: {self.retracement_target_pct}%) | Closing all"
                )
                return self._create_close_intent(reason="take_profit")

        # --- EXIT CHECK 2: Time stop ---
        if self._first_entry_at:
            time_held = now - self._first_entry_at
            if time_held >= timedelta(hours=self.time_stop_hours):
                logger.info(
                    f"TIME STOP: Position held for {time_held} "
                    f"(limit: {self.time_stop_hours}h) | Closing all"
                )
                return self._create_close_intent(reason="time_stop")

        # --- EXIT CHECK 3: Hard stop (all doublings exhausted + price still rising) ---
        if self._martingale_level >= self.max_doublings:
            price_above_last_entry_pct = (
                (current_price - last_entry_price) / last_entry_price * Decimal("100")
            )
            if price_above_last_entry_pct >= self.hard_stop_above_last_entry_pct:
                logger.info(
                    f"HARD STOP: All {self.max_doublings} doublings exhausted and "
                    f"price still {price_above_last_entry_pct:.1f}% above last entry | Closing all"
                )
                return self._create_close_intent(reason="hard_stop")

        # --- DOUBLING CHECK: Price moved enough above last entry ---
        if self._martingale_level < self.max_doublings:
            price_move_pct = (
                (current_price - last_entry_price) / last_entry_price * Decimal("100")
            )
            if price_move_pct >= self.doubling_trigger_pct:
                next_level = self._martingale_level + 1
                logger.info(
                    f"DOUBLING: Price {price_move_pct:.1f}% above last entry "
                    f"(trigger: {self.doubling_trigger_pct}%) | "
                    f"Level {self._martingale_level} -> {next_level}"
                )
                return self._create_open_intent(current_price, level=next_level)

        # Hold
        level_str = f"L{self._martingale_level}/{self.max_doublings}"
        price_from_last = (
            (current_price - last_entry_price) / last_entry_price * Decimal("100")
        )
        retracement_from_top = Decimal("0")
        if self._local_top > Decimal("0"):
            retracement_from_top = (
                (self._local_top - current_price) / self._local_top * Decimal("100")
            )

        return Intent.hold(
            reason=f"SHORT {level_str} | "
            f"price=${current_price:,.0f} "
            f"({price_from_last:+.1f}% from last entry, "
            f"{retracement_from_top:.1f}% from top) | "
            f"collateral={format_usd(self._total_collateral_deployed)}"
        )

    # =========================================================================
    # Intent creation
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal, level: int) -> Intent:
        """Create a PERP_OPEN intent for a SHORT at the given martingale level."""
        # Compute collateral for this level
        collateral = (self._initial_collateral * Decimal(str(2**level))).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )

        # For stablecoin collateral (USDC), collateral_value_usd = collateral amount
        # For non-stable collateral, we'd need to convert
        if self.collateral_token in ("USDC", "USDT", "DAI", "USDC.e"):
            collateral_value_usd = collateral
        else:
            collateral_value_usd = collateral * current_price

        position_size_usd = collateral_value_usd * self.leverage
        max_slippage = self.max_slippage_pct / Decimal("100")

        logger.info(
            f"SHORT L{level}: {format_usd(collateral)} {self.collateral_token} collateral "
            f"-> {format_usd(position_size_usd)} position @ {self.leverage}x | "
            f"entry price=${current_price:,.2f}"
        )

        # Update internal state
        self._martingale_level = level
        self._entry_prices.append(current_price)
        self._entry_collaterals.append(collateral)
        self._total_collateral_deployed += collateral
        self._total_position_size_usd += position_size_usd
        self._trades_opened += 1

        if level == 0:
            self._first_entry_at = datetime.now(UTC)
            self._local_top = current_price

        # Update local top
        if current_price > self._local_top:
            self._local_top = current_price

        return Intent.perp_open(
            market=self.market,
            collateral_token=self.collateral_token,
            collateral_amount=collateral,
            size_usd=position_size_usd,
            is_long=False,  # SHORT
            leverage=self.leverage,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def _create_close_intent(self, reason: str = "unknown") -> Intent:
        """Create a PERP_CLOSE intent to close the entire position."""
        max_slippage = self.max_slippage_pct / Decimal("100")

        levels = len(self._entry_prices)
        logger.info(
            f"CLOSING ALL: {levels} level(s), "
            f"total collateral={format_usd(self._total_collateral_deployed)}, "
            f"total size={format_usd(self._total_position_size_usd)}, "
            f"reason={reason}"
        )

        # Track win/loss
        if reason == "take_profit":
            self._wins += 1
        else:
            self._losses += 1

        self._trades_closed += 1

        # Capture size before reset
        total_size = self._total_position_size_usd

        # Reset position state
        self._martingale_level = 0
        self._entry_prices.clear()
        self._entry_collaterals.clear()
        self._local_top = Decimal("0")
        self._total_collateral_deployed = Decimal("0")
        self._total_position_size_usd = Decimal("0")
        self._first_entry_at = None
        self._last_close_at = datetime.now(UTC)

        return Intent.perp_close(
            market=self.market,
            collateral_token=self.collateral_token,
            is_long=False,  # SHORT
            size_usd=total_size,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    # =========================================================================
    # Force action (testing)
    # =========================================================================

    def _handle_force_action(self, action: str, current_price: Decimal) -> Intent | None:
        """Handle forced actions for testing."""
        logger.info(f"Force action: {action}")
        if action == "open":
            return self._create_open_intent(current_price, level=0)
        elif action == "double":
            if self._has_position:
                next_level = min(self._martingale_level + 1, self.max_doublings)
                return self._create_open_intent(current_price, level=next_level)
            return self._create_open_intent(current_price, level=0)
        elif action == "close":
            if self._has_position:
                return self._create_close_intent(reason="forced")
            return Intent.hold(reason="No position to close")
        else:
            logger.warning(f"Unknown force_action: {action}")
            return Intent.hold(reason=f"Unknown force_action: {action}")

    # =========================================================================
    # Status and monitoring
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring."""
        return {
            "strategy": "rsi_martingale_short",
            "chain": self.chain,
            "config": {
                "market": self.market,
                "risk_budget_usd": str(self.risk_budget_usd),
                "initial_collateral": str(self._initial_collateral),
                "leverage": str(self.leverage),
                "rsi_threshold": str(self.rsi_threshold),
                "max_doublings": self.max_doublings,
                "doubling_trigger_pct": str(self.doubling_trigger_pct),
                "retracement_target_pct": str(self.retracement_target_pct),
            },
            "state": {
                "has_position": self._has_position,
                "martingale_level": self._martingale_level,
                "entry_prices": [str(p) for p in self._entry_prices],
                "local_top": str(self._local_top),
                "total_collateral_deployed": str(self._total_collateral_deployed),
                "total_position_size_usd": str(self._total_position_size_usd),
                "first_entry_at": (
                    self._first_entry_at.isoformat() if self._first_entry_at else None
                ),
            },
            "stats": {
                "trades_opened": self._trades_opened,
                "trades_closed": self._trades_closed,
                "wins": self._wins,
                "losses": self._losses,
            },
        }

    # =========================================================================
    # Teardown support
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_position and self._total_position_size_usd > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"martingale-short-{self.market}-{self.chain}",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=self._total_position_size_usd,
                    details={
                        "market": self.market,
                        "is_long": False,
                        "leverage": str(self.leverage),
                        "martingale_level": self._martingale_level,
                        "entry_prices": [str(p) for p in self._entry_prices],
                        "local_top": str(self._local_top),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "rsi_martingale_short"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._has_position:
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            intents.append(
                Intent.perp_close(
                    market=self.market,
                    collateral_token=self.collateral_token,
                    is_long=False,
                    size_usd=self._total_position_size_usd,
                    max_slippage=slippage,
                    protocol="gmx_v2",
                )
            )
        return intents

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()
        config_dict = self.config if isinstance(self.config, dict) else {}
        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }


if __name__ == "__main__":
    print("RSIMartingaleShortStrategy loaded successfully!")
    print(f"Metadata: {RSIMartingaleShortStrategy.STRATEGY_METADATA}")
