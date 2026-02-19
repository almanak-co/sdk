"""Momentum Accumulation Strategy (v2).

Combines RSI-based dip buying with wide-range concentrated LP to accumulate
a target token. The strategy never sells the target token.

Capital allocation:
    - 60% in LP position (30% stable + 30% target by value)
    - 40% reserved for dip buying on RSI oversold signals

State machine:
    init_swap -> init_lp -> monitoring -> (buy_dip | rebalance | teardown) -> terminated

Key behaviors:
    - Triggers on RSI signal *changes* (not raw values) to prevent spam buying
    - Enforces cooldown between dip buys
    - On-chain accounting: reads balances each cycle, no internal tracking
    - When LP goes out of range below: holds (never sells target)
    - When LP goes out of range above: rebalances to new range
    - When stables hit dust threshold: tears down LP and does final swap
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.teardown.models import TeardownMode, TeardownPositionSummary

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(config, key: str, default: Any = None) -> Any:
    """Extract a value from config (dict or object)."""
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

@almanak_strategy(
    name="momentum_accumulation",
    description="RSI-based dip buying with wide-range LP for token accumulation",
    version="1.0.0",
    author="Almanak",
    tags=["accumulation", "rsi", "lp", "dip-buying", "momentum"],
    supported_chains=["ethereum", "arbitrum", "base"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "LP_OPEN", "LP_CLOSE", "HOLD"],
)
class MomentumAccumulation(IntentStrategy):
    """Momentum accumulation: RSI dip-buying + wide-range concentrated LP.

    Never sells the target token. Pure accumulation strategy.
    """

    # --------------------------------------------------------------------- #
    # Init
    # --------------------------------------------------------------------- #

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        c = self.config

        # Tokens & pool
        self.target_token: str = _cfg(c, "target_token", "WETH")
        self.stable_token: str = _cfg(c, "stable_token", "USDC")
        self.pool_address: str = _cfg(c, "pool_address", "")
        self.protocol: str = _cfg(c, "protocol", "uniswap_v3")
        self.stable_is_token0: bool = bool(_cfg(c, "stable_is_token0", True))

        # RSI
        self.rsi_period: int = int(_cfg(c, "rsi_period", 14))
        self.rsi_oversold: int = int(_cfg(c, "rsi_oversold_threshold", 30))

        # LP
        self.lp_range_pct = Decimal(str(_cfg(c, "lp_range_percent", 20))) / Decimal("100")
        self.lp_capital_pct = Decimal(str(_cfg(c, "lp_capital_percent", 60))) / Decimal("100")

        # Dip buying
        self.dip_buy_pct = Decimal(str(_cfg(c, "dip_buy_percent", 25))) / Decimal("100")
        self.cooldown = timedelta(minutes=int(_cfg(c, "cooldown_minutes", 60)))

        # Thresholds
        self.dust_threshold_usd = Decimal(str(_cfg(c, "dust_threshold_usd", 5)))
        self.max_slippage = Decimal(str(_cfg(c, "max_slippage_bps", 100))) / Decimal("10000")

        # Internal state (persisted via get/load_persistent_state)
        self._phase: str = "init_swap"
        self._position_id: str | None = None
        self._range_lower: Decimal | None = None
        self._range_upper: Decimal | None = None
        self._last_rsi_signal: str = "NEUTRAL"
        self._last_dip_buy_time: datetime | None = None
        self._total_dip_buys: int = 0

        logger.info(
            f"MomentumAccumulation initialized: "
            f"{self.target_token}/{self.stable_token} on {self.protocol}, "
            f"RSI({self.rsi_period}) oversold<{self.rsi_oversold}, "
            f"LP range +/-{self.lp_range_pct * 100:.0f}%, "
            f"dip buy {self.dip_buy_pct * 100:.0f}% of stables"
        )

    # --------------------------------------------------------------------- #
    # decide() - main entry point
    # --------------------------------------------------------------------- #

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            return self._dispatch(market)
        except Exception as e:
            logger.exception("Error in decide()")
            return Intent.hold(reason=f"Error: {e}")

    def _dispatch(self, market: MarketSnapshot) -> Intent | None:
        phase = self._phase

        if phase == "init_swap":
            return self._do_init_swap(market)
        if phase == "init_lp":
            return self._do_init_lp(market)
        if phase == "monitoring":
            return self._do_monitoring(market)
        if phase == "buy_dip":
            return self._do_buy_dip(market)
        if phase == "rebalance_close":
            return self._do_rebalance_close()
        if phase == "rebalance_open":
            return self._do_rebalance_open(market)
        if phase == "teardown_close":
            return self._do_teardown_close()
        if phase == "teardown_swap":
            return self._do_teardown_swap(market)
        if phase == "terminated":
            return Intent.hold(reason="Strategy terminated - all capital accumulated")

        return Intent.hold(reason=f"Unknown phase: {phase}")

    # --------------------------------------------------------------------- #
    # Phase: init_swap
    # --------------------------------------------------------------------- #

    def _do_init_swap(self, market: MarketSnapshot) -> Intent:
        """Swap half of the LP allocation (30% of total) from stable to target."""
        stable_bal = market.balance(self.stable_token)
        swap_amount = self._round_down(
            stable_bal.balance * self.lp_capital_pct / Decimal("2"), self.stable_token
        )

        if swap_amount <= 0:
            logger.warning("No stable balance for init swap")
            return Intent.hold(reason="No stable balance")

        logger.info(
            f"Init: swapping {swap_amount:.6f} {self.stable_token} -> {self.target_token}"
        )

        return Intent.swap(
            from_token=self.stable_token,
            to_token=self.target_token,
            amount=swap_amount,
            max_slippage=self.max_slippage,
            protocol=self.protocol,
        )

    # --------------------------------------------------------------------- #
    # Phase: init_lp
    # --------------------------------------------------------------------- #

    def _do_init_lp(self, market: MarketSnapshot) -> Intent:
        """Open LP with all target token + matching stable value (~60% of capital)."""
        return self._open_lp(market)

    # --------------------------------------------------------------------- #
    # Phase: monitoring
    # --------------------------------------------------------------------- #

    def _do_monitoring(self, market: MarketSnapshot) -> Intent | None:
        """Main loop: check dust threshold, LP range, RSI signal."""
        stable_bal = market.balance(self.stable_token)
        target_price = market.price(self.target_token)

        # 1. Dust threshold -> teardown
        if stable_bal.balance_usd < self.dust_threshold_usd and self._position_id:
            logger.info(
                f"Stable balance ${stable_bal.balance_usd:.2f} below dust "
                f"threshold ${self.dust_threshold_usd} - starting teardown"
            )
            self._phase = "teardown_close"
            return self._do_teardown_close()

        # 2. LP out-of-range check
        if self._position_id and self._range_upper and self._range_lower:
            if target_price > self._range_upper:
                logger.info(
                    f"Price {target_price:.2f} above LP upper bound "
                    f"{self._range_upper:.2f} - rebalancing"
                )
                self._phase = "rebalance_close"
                return self._do_rebalance_close()
            # Price below range -> hold; never sell the target token
            if target_price < self._range_lower:
                return Intent.hold(
                    reason=f"Price {target_price:.2f} below LP range "
                    f"{self._range_lower:.2f} - holding (never sell target)"
                )

        # 3. RSI signal-change detection
        try:
            rsi = market.rsi(self.target_token, period=self.rsi_period)
        except Exception as e:
            logger.warning(f"RSI unavailable: {e}")
            return Intent.hold(reason="RSI data unavailable")

        current_signal = "OVERSOLD" if rsi.value < self.rsi_oversold else "NEUTRAL"

        if current_signal == "OVERSOLD" and self._last_rsi_signal != "OVERSOLD":
            logger.info(
                f"RSI signal change: {self._last_rsi_signal} -> OVERSOLD "
                f"(RSI={rsi.value:.1f})"
            )
            self._last_rsi_signal = current_signal

            if self._cooldown_passed() and stable_bal.balance_usd >= self.dust_threshold_usd:
                self._phase = "buy_dip"
                return self._do_buy_dip(market)

            if not self._cooldown_passed():
                logger.info("Dip signal detected but cooldown still active")
            elif stable_bal.balance_usd < self.dust_threshold_usd:
                logger.info("Dip signal detected but stable balance too low")

        self._last_rsi_signal = current_signal

        return Intent.hold(
            reason=f"Monitoring: RSI={rsi.value:.1f}, "
            f"price={target_price:.2f}, "
            f"stables=${stable_bal.balance_usd:.2f}"
        )

    # --------------------------------------------------------------------- #
    # Phase: buy_dip
    # --------------------------------------------------------------------- #

    def _do_buy_dip(self, market: MarketSnapshot) -> Intent:
        """Buy dip: swap configured % of remaining stables to target."""
        stable_bal = market.balance(self.stable_token)
        buy_amount = self._round_down(stable_bal.balance * self.dip_buy_pct, self.stable_token)

        if buy_amount <= 0:
            logger.warning("No stable balance for dip buy")
            self._phase = "monitoring"
            return Intent.hold(reason="No stables for dip buy")

        logger.info(
            f"Dip buy #{self._total_dip_buys + 1}: "
            f"{buy_amount:.6f} {self.stable_token} -> {self.target_token}"
        )

        return Intent.swap(
            from_token=self.stable_token,
            to_token=self.target_token,
            amount=buy_amount,
            max_slippage=self.max_slippage,
            protocol=self.protocol,
        )

    # --------------------------------------------------------------------- #
    # Phase: rebalance_close / rebalance_open
    # --------------------------------------------------------------------- #

    def _do_rebalance_close(self) -> Intent:
        """Close the out-of-range LP position."""
        logger.info(f"Rebalance: closing LP position {self._position_id}")
        return Intent.lp_close(
            position_id=str(self._position_id),
            pool=self.pool_address,
            collect_fees=True,
            protocol=self.protocol,
        )

    def _do_rebalance_open(self, market: MarketSnapshot) -> Intent:
        """Open a fresh LP position centered on current price."""
        return self._open_lp(market)

    # --------------------------------------------------------------------- #
    # Phase: teardown_close / teardown_swap
    # --------------------------------------------------------------------- #

    def _do_teardown_close(self) -> Intent:
        """Close LP position for final teardown."""
        if not self._position_id:
            self._phase = "teardown_swap"
            return Intent.hold(reason="No LP position to close, proceeding to final swap")

        logger.info(f"Teardown: closing LP position {self._position_id}")
        return Intent.lp_close(
            position_id=str(self._position_id),
            pool=self.pool_address,
            collect_fees=True,
            protocol=self.protocol,
        )

    def _do_teardown_swap(self, market: MarketSnapshot) -> Intent:
        """Final swap: convert all remaining stables to target token."""
        stable_bal = market.balance(self.stable_token)

        if stable_bal.balance <= 0:
            logger.info("Teardown complete: no stables remaining")
            self._phase = "terminated"
            return Intent.hold(reason="Teardown complete")

        logger.info(
            f"Teardown: final swap {stable_bal.balance:.6f} "
            f"{self.stable_token} -> {self.target_token}"
        )

        return Intent.swap(
            from_token=self.stable_token,
            to_token=self.target_token,
            amount=self._round_down(stable_bal.balance, self.stable_token),
            max_slippage=self.max_slippage,
            protocol=self.protocol,
        )

    # --------------------------------------------------------------------- #
    # Shared: open LP position
    # --------------------------------------------------------------------- #

    def _open_lp(self, market: MarketSnapshot) -> Intent:
        """Open LP position using all target token + matching stable value."""
        target_price = market.price(self.target_token)
        target_bal = market.balance(self.target_token)
        stable_bal = market.balance(self.stable_token)

        if target_bal.balance <= 0:
            logger.warning("No target token balance for LP")
            self._phase = "monitoring"
            return Intent.hold(reason="No target token for LP")

        range_lower = target_price * (Decimal("1") - self.lp_range_pct)
        range_upper = target_price * (Decimal("1") + self.lp_range_pct)

        # Use all target token; match with equal USD value of stable
        target_amount = self._round_down(target_bal.balance, self.target_token)
        stable_price = market.price(self.stable_token)
        stable_for_lp = self._round_down(
            min(target_bal.balance_usd / stable_price, stable_bal.balance),
            self.stable_token,
        )

        # Token ordering: Uniswap V3 sorts by address (lower = token0)
        if self.stable_is_token0:
            amount0 = stable_for_lp
            amount1 = target_amount
        else:
            amount0 = target_amount
            amount1 = stable_for_lp

        # Store range for in-range monitoring
        self._range_lower = range_lower
        self._range_upper = range_upper

        logger.info(
            f"Opening LP: price={target_price:.2f}, "
            f"range=[{range_lower:.2f}, {range_upper:.2f}], "
            f"target={target_amount:.6f}, stable={stable_for_lp:.2f}"
        )

        return Intent.lp_open(
            pool=self.pool_address,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol=self.protocol,
        )

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #

    def _round_down(self, amount: Decimal, token: str) -> Decimal:
        """Round down to token's decimal precision to avoid overspending."""
        from almanak.framework.data.tokens import get_token_resolver

        token_info = get_token_resolver().resolve_for_swap(token, self.chain)
        return amount.quantize(Decimal(10) ** -token_info.decimals, rounding=ROUND_DOWN)

    def _cooldown_passed(self) -> bool:
        if not self._last_dip_buy_time:
            return True
        return datetime.now(UTC) - self._last_dip_buy_time >= self.cooldown

    # --------------------------------------------------------------------- #
    # Lifecycle hooks
    # --------------------------------------------------------------------- #

    def on_intent_executed(self, _intent: Any, success: bool, result: Any) -> None:
        if not success:
            logger.warning(f"Intent failed in phase '{self._phase}'")
            return

        if self._phase == "init_swap":
            logger.info("Init swap complete -> opening LP")
            self._phase = "init_lp"

        elif self._phase == "init_lp":
            self._position_id = getattr(result, "position_id", None)
            if self._position_id:
                self._position_id = str(self._position_id)
            logger.info(f"LP opened, position_id={self._position_id} -> monitoring")
            self._phase = "monitoring"

        elif self._phase == "buy_dip":
            self._last_dip_buy_time = datetime.now(UTC)
            self._total_dip_buys += 1
            logger.info(f"Dip buy #{self._total_dip_buys} complete -> monitoring")
            self._phase = "monitoring"

        elif self._phase == "rebalance_close":
            self._position_id = None
            logger.info("Rebalance: LP closed -> opening new position")
            self._phase = "rebalance_open"

        elif self._phase == "rebalance_open":
            self._position_id = getattr(result, "position_id", None)
            if self._position_id:
                self._position_id = str(self._position_id)
            logger.info(f"Rebalance complete, position_id={self._position_id} -> monitoring")
            self._phase = "monitoring"

        elif self._phase == "teardown_close":
            self._position_id = None
            logger.info("Teardown: LP closed -> final swap")
            self._phase = "teardown_swap"

        elif self._phase == "teardown_swap":
            logger.info("Teardown complete -> terminated")
            self._phase = "terminated"

    # --------------------------------------------------------------------- #
    # State persistence
    # --------------------------------------------------------------------- #

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "phase": self._phase,
            "position_id": self._position_id,
            "range_lower": str(self._range_lower) if self._range_lower else None,
            "range_upper": str(self._range_upper) if self._range_upper else None,
            "last_rsi_signal": self._last_rsi_signal,
            "last_dip_buy_time": (
                self._last_dip_buy_time.isoformat() if self._last_dip_buy_time else None
            ),
            "total_dip_buys": self._total_dip_buys,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._phase = state.get("phase", "init_swap")
        self._position_id = state.get("position_id")

        rl = state.get("range_lower")
        self._range_lower = Decimal(rl) if rl else None
        ru = state.get("range_upper")
        self._range_upper = Decimal(ru) if ru else None

        self._last_rsi_signal = state.get("last_rsi_signal", "NEUTRAL")

        dbt = state.get("last_dip_buy_time")
        self._last_dip_buy_time = datetime.fromisoformat(dbt) if dbt else None

        self._total_dip_buys = state.get("total_dip_buys", 0)

        logger.info(
            f"Loaded state: phase={self._phase}, position={self._position_id}, "
            f"dip_buys={self._total_dip_buys}"
        )

    # --------------------------------------------------------------------- #
    # Status & teardown
    # --------------------------------------------------------------------- #

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "momentum_accumulation",
            "phase": self._phase,
            "target_token": self.target_token,
            "stable_token": self.stable_token,
            "position_id": self._position_id,
            "lp_range": (
                f"[{self._range_lower:.2f}, {self._range_upper:.2f}]"
                if self._range_lower and self._range_upper
                else "none"
            ),
            "last_rsi_signal": self._last_rsi_signal,
            "total_dip_buys": self._total_dip_buys,
        }

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._position_id:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._position_id,
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=Decimal("0"),
                    details={
                        "pool": self.pool_address,
                        "range_lower": str(self._range_lower),
                        "range_upper": str(self._range_upper),
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "momentum_accumulation"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = (
            Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage
        )

        intents: list[Intent] = []

        if self._position_id:
            intents.append(
                Intent.lp_close(
                    position_id=str(self._position_id),
                    pool=self.pool_address,
                    collect_fees=True,
                    protocol=self.protocol,
                )
            )

        intents.append(
            Intent.swap(
                from_token=self.stable_token,
                to_token=self.target_token,
                amount="all",
                max_slippage=max_slippage,
                protocol=self.protocol,
            )
        )

        return intents
