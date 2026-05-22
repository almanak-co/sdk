"""Uniswap V3 LP Rebalance on X-Layer.

Concentrated LP lifecycle manager on a WOKB/USDT Uniswap V3 pool with
automated range monitoring and rebalance.

Intent flow:
1. Open concentrated LP position in a defined tick range (LPOpenIntent)
2. Monitor price vs position range each cycle
3. If price exits range by >threshold: close position (LPCloseIntent),
   rebalance amounts (SwapIntent), re-open at new range (LPOpenIntent)
4. Teardown: close LP, swap all to USDT

This exercises 4 intent types (swap, LP open, LP close, hold) and
demonstrates real DeFi composability with state management.

Usage:
    almanak strat run -d almanak/demo_strategies/xlayer_lp_rebalance --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_xlayer_lp_rebalance",
    description="Uniswap V3 concentrated LP lifecycle on X-Layer — open, monitor, rebalance",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "xlayer", "lp", "rebalance", "uniswap-v3", "lifecycle"],
    supported_chains=["xlayer"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
    default_chain="xlayer",
)
class XLayerLPRebalanceStrategy(IntentStrategy):
    """Uniswap V3 concentrated LP lifecycle on X-Layer.

    State machine:
        idle -> opening -> open -> (monitoring) -> closing -> closed -> swapping -> swapped -> opening -> ...

    Configuration (config.json):
        pool: Pool identifier (e.g., "WOKB/USDT/3000")
        range_width_pct: Price range width as decimal (0.10 = ±5%)
        rebalance_threshold_pct: Trigger rebalance if price exits range by this %
        amount_token0: Amount of token0 to provide
        amount_token1: Amount of token1 to provide
        force_action: Override state machine ("open", "close", or "" for auto)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        pool_str = self.get_config("pool", "WOKB/USDT/3000")
        parts = pool_str.split("/")
        self.token0 = parts[0] if len(parts) > 0 else "WOKB"
        self.token1 = parts[1] if len(parts) > 1 else "USDT"
        self.fee_tier = int(parts[2]) if len(parts) > 2 else 3000
        self.pool = pool_str

        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.10")))
        self.rebalance_threshold_pct = Decimal(str(self.get_config("rebalance_threshold_pct", "0.05")))
        self.amount_token0 = Decimal(str(self.get_config("amount_token0", "1.0")))
        self.amount_token1 = Decimal(str(self.get_config("amount_token1", "50")))
        self.force_action = str(self.get_config("force_action", "")).lower()

        # State tracking
        self._state = "idle"  # idle, opening, open, closing, closed, swapping, swapped
        self._previous_stable_state = "idle"
        self._position_id: str | None = None
        self._range_lower: Decimal | None = None
        self._range_upper: Decimal | None = None
        self._last_token0_price: Decimal = Decimal("1")
        self._last_token1_price: Decimal = Decimal("1")
        self._rebalance_count = 0

        logger.info(
            f"XLayerLPRebalance: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100:.0f}%, "
            f"rebalance_threshold={self.rebalance_threshold_pct * 100:.0f}%, "
            f"amounts={self.amount_token0} {self.token0} + {self.amount_token1} {self.token1}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute LP lifecycle: open, monitor, rebalance."""
        # Handle force_action overrides
        if self.force_action == "open" and self._state == "idle":
            return self._open_position(market)
        if self.force_action == "close" and self._state == "open":
            return self._close_position()

        # Get current prices
        try:
            token0_price = Decimal(str(market.price(self.token0)))
            token1_price = Decimal(str(market.price(self.token1)))
            if token0_price <= 0 or token1_price <= 0:
                return Intent.hold(reason=f"Invalid prices: {self.token0}=${token0_price}, {self.token1}=${token1_price}")
            self._last_token0_price = token0_price
            self._last_token1_price = token1_price
            pair_price = token0_price / token1_price
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # State machine
        if self._state == "idle":
            return self._open_position(market)

        if self._state == "open":
            return self._monitor_and_maybe_rebalance(pair_price)

        if self._state == "closed":
            # Close confirmed by on_intent_executed — now swap to rebalance
            return self._swap_for_rebalance(market)

        if self._state == "swapped":
            # Swap confirmed by on_intent_executed — now re-open position
            return self._open_position(market)

        # Safety: hold in transitional states until on_intent_executed callback
        if self._state in ("opening", "closing", "swapping"):
            return Intent.hold(reason=f"Waiting for {self._state} completion")

        return Intent.hold(reason=f"state={self._state}, rebalances={self._rebalance_count}")

    def _open_position(self, market: MarketSnapshot) -> Intent:
        """Open a new concentrated LP position."""
        try:
            token0_price = Decimal(str(market.price(self.token0)))
            token1_price = Decimal(str(market.price(self.token1)))
            pair_price = token0_price / token1_price
        except (ValueError, KeyError):
            return Intent.hold(reason="Cannot open LP without price data")

        half_width = self.range_width_pct / Decimal("2")
        self._range_lower = pair_price * (Decimal("1") - half_width)
        self._range_upper = pair_price * (Decimal("1") + half_width)

        self._previous_stable_state = self._state
        self._state = "opening"

        logger.info(
            f"LP_OPEN: {self.amount_token0} {self.token0} + {self.amount_token1} {self.token1}, "
            f"range [{self._range_lower:.4f} - {self._range_upper:.4f}]"
        )
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount_token0,
            amount1=self.amount_token1,
            range_lower=self._range_lower,
            range_upper=self._range_upper,
            protocol="uniswap_v3",
        )

    def _monitor_and_maybe_rebalance(self, pair_price: Decimal) -> Intent:
        """Check if price has exited the range and trigger rebalance if needed."""
        if self._range_lower is None or self._range_upper is None:
            return Intent.hold(reason="No range set — waiting")

        range_mid = (self._range_lower + self._range_upper) / Decimal("2")
        deviation = abs(pair_price - range_mid) / range_mid

        # Check if price has moved beyond the range + threshold
        price_below = pair_price < self._range_lower * (Decimal("1") - self.rebalance_threshold_pct)
        price_above = pair_price > self._range_upper * (Decimal("1") + self.rebalance_threshold_pct)

        if price_below or price_above:
            direction = "below" if price_below else "above"
            logger.info(
                f"Rebalance triggered: price={pair_price:.4f} {direction} range "
                f"[{self._range_lower:.4f} - {self._range_upper:.4f}], deviation={deviation:.2%}"
            )
            return self._close_position()

        return Intent.hold(
            reason=f"Position {self._position_id} in range: "
            f"price={pair_price:.4f} in [{self._range_lower:.4f} - {self._range_upper:.4f}]"
        )

    def _close_position(self) -> Intent:
        """Close the LP position."""
        if not self._position_id:
            self._state = "idle"
            return Intent.hold(reason="No position_id tracked — cannot close")

        self._previous_stable_state = self._state
        self._state = "closing"

        logger.info(f"LP_CLOSE: position_id={self._position_id}, pool={self.pool}")
        return Intent.lp_close(
            position_id=self._position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v3",
        )

    def _swap_for_rebalance(self, market: MarketSnapshot) -> Intent:
        """Swap to rebalance token ratios before re-opening."""
        self._previous_stable_state = self._state
        self._state = "swapping"
        self._rebalance_count += 1

        # Swap half of token1 to token0 to rebalance
        try:
            token1_bal = market.balance(self.token1)
            swap_amount = token1_bal.balance / Decimal("2")
            if swap_amount <= 0:
                self._state = "idle"
                return Intent.hold(reason="No token1 balance for rebalance swap")
        except (ValueError, KeyError):
            swap_amount = self.amount_token1 / Decimal("2")

        logger.info(f"Rebalance swap: {swap_amount} {self.token1} -> {self.token0}")
        return Intent.swap(
            from_token=self.token1,
            to_token=self.token0,
            amount=swap_amount,
            max_slippage=Decimal("0.01"),
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "LP_OPEN":
                self._state = "open"
                position_id = getattr(result, "position_id", None)
                if position_id:
                    self._position_id = str(position_id)
                logger.info(f"LP_OPEN SUCCESS: position_id={self._position_id}")

            elif intent_type == "LP_CLOSE":
                self._state = "closed"  # Next decide() will trigger swap to rebalance
                self._position_id = None
                logger.info("LP_CLOSE SUCCESS: position closed")

            elif intent_type == "SWAP":
                self._state = "swapped"  # Next decide() will re-open position
                logger.info(f"SWAP SUCCESS: rebalance #{self._rebalance_count} complete")
        else:
            if intent_type == "LP_CLOSE":
                # Check if the failure is due to a non-existent position (stale state)
                # vs a transient execution error (slippage, gas, RPC timeout, etc.)
                error_text = str(getattr(result, "error", "")).lower()
                stale_position = any(
                    marker in error_text
                    for marker in ("invalid token id", "nonexistent token", "position does not exist", "position not found")
                )
                if stale_position:
                    logger.warning("LP_CLOSE failed due to stale position_id — resetting to idle")
                    self._state = "idle"
                    self._position_id = None
                else:
                    revert_to = self._previous_stable_state
                    logger.warning(f"LP_CLOSE failed ({error_text or 'unknown error'}), reverting to '{revert_to}'")
                    self._state = revert_to
            else:
                revert_to = self._previous_stable_state
                logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
                self._state = revert_to

    # -- Status & Persistence --

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_xlayer_lp_rebalance",
            "chain": self.chain,
            "pool": self.pool,
            "state": self._state,
            "position_id": self._position_id,
            "range": f"[{self._range_lower} - {self._range_upper}]" if self._range_lower else "none",
            "rebalance_count": self._rebalance_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "position_id": self._position_id,
            "range_lower": str(self._range_lower) if self._range_lower else None,
            "range_upper": str(self._range_upper) if self._range_upper else None,
            "rebalance_count": self._rebalance_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "position_id" in state:
            self._position_id = state.get("position_id")
        if state.get("range_lower"):
            self._range_lower = Decimal(str(state["range_lower"]))
        if state.get("range_upper"):
            self._range_upper = Decimal(str(state["range_upper"]))
        if "rebalance_count" in state:
            self._rebalance_count = int(state["rebalance_count"])

    # -- Teardown --

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._position_id and self._state == "open":
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"xlayer-lp-{self._position_id}",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=self.amount_token0 * self._last_token0_price + self.amount_token1 * self._last_token1_price,
                    details={"pool": self.pool, "range_lower": str(self._range_lower), "range_upper": str(self._range_upper)},
                )
            )
        return TeardownPositionSummary(
            deployment_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")

        # Close LP position if open
        if self._position_id:
            intents.append(
                Intent.lp_close(
                    position_id=self._position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="uniswap_v3",
                )
            )

        # Swap all token0 to token1 (USDT)
        intents.append(
            Intent.swap(
                from_token=self.token0,
                to_token=self.token1,
                amount="all",
                max_slippage=max_slippage,
            )
        )

        return intents
