"""
===============================================================================
DEMO: SushiSwap V3 Paper Trade LP on BSC
===============================================================================

Paper trading demo for SushiSwap V3 LP on BSC (BNB Chain). Opens a WBNB/USDT
concentrated liquidity position, holds for a configurable number of ticks,
then closes — exercising the full LP lifecycle through the paper trading engine.

PURPOSE:
--------
1. Validate paper trading on BSC with SushiSwap V3 LP
2. Exercise LP_OPEN / LP_CLOSE intents with tick-range mechanics
3. Generate multi-tick PnL journal entries for equity curve tracking
4. Test BSC-specific Anvil fork behavior (WBNB 18 decimals, USDT 18 decimals)
5. First paper trading demo on BSC — fills BSC backtesting coverage gap

USAGE:
------
    # Paper trade for 8 ticks at 30-second intervals
    almanak strat backtest paper start \\
        -s demo_sushiswap_v3_paper_trade_lp_bsc \\
        --chain bsc \\
        --max-ticks 8 \\
        --tick-interval 30 \\
        --foreground

    # Or run directly on Anvil (single iteration — opens LP)
    almanak strat run -d strategies/demo/sushiswap_v3_paper_trade_lp_bsc \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. If no position and tick < close_after_ticks -> open LP
  2. If has position and ticks_held >= hold_ticks -> close LP
  3. Otherwise -> hold (monitoring position)

Kitchen Loop iteration 114, VIB-1625.
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.sushiswap_v3 import (
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_tick,
)
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_sushiswap_v3_paper_trade_lp_bsc",
    description="Paper trading demo — SushiSwap V3 LP lifecycle on BSC",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lp", "sushiswap_v3", "bsc", "backtesting"],
    supported_chains=["bsc"],
    supported_protocols=["sushiswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="bsc",
)
class SushiSwapV3PaperTradeLPBSCStrategy(IntentStrategy):
    """SushiSwap V3 LP lifecycle strategy for paper trading on BSC.

    Opens a concentrated liquidity position, holds it for a configurable number
    of ticks, then closes it. Designed to exercise the full LP lifecycle through
    the paper trading engine.

    Configuration (config.json):
        pool: Pool identifier (e.g. "WBNB/USDT/3000")
        amount0: Token0 amount to LP (e.g. "0.1" WBNB)
        amount1: Token1 amount to LP (e.g. "50" USDT)
        range_width_pct: Total price range width (0.20 = 20%)
        hold_ticks: Number of ticks to hold position before closing (default: 3)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        pool = str(self.get_config("pool", "WBNB/USDT/3000"))
        pool_parts = pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WBNB"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDT"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else int(self.get_config("fee_tier", 3000))
        self.pool = pool

        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.20")))
        self.amount0 = Decimal(str(self.get_config("amount0", "0.1")))
        self.amount1 = Decimal(str(self.get_config("amount1", "50")))
        self.hold_ticks = int(self.get_config("hold_ticks", 3))

        self._has_position = False
        self._position_id: int | None = None
        self._ticks_held = 0
        self._tick_count = 0

        logger.info(
            f"SushiSwapV3PaperTradeLPBSC initialized: pool={self.pool}, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"hold_ticks={self.hold_ticks}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Tick-count lifecycle: open -> hold N ticks -> close."""
        self._tick_count += 1

        try:
            # Get current price
            try:
                token0_price_usd = market.price(self.token0_symbol)
                token1_price_usd = market.price(self.token1_symbol)
                if token1_price_usd == Decimal("0"):
                    return Intent.hold(reason=f"{self.token1_symbol} price is zero")
                current_price = token0_price_usd / token1_price_usd
                logger.info(
                    f"[tick {self._tick_count}] Price: {self.token0_symbol}=${token0_price_usd:.2f}, "
                    f"pool_price={current_price:.4f} {self.token1_symbol}/{self.token0_symbol}"
                )
            except Exception as price_err:
                logger.warning(f"Price fetch failed: {price_err}")
                return Intent.hold(reason=f"Price unavailable: {price_err}")

            if self._has_position:
                self._ticks_held += 1
                if self._ticks_held >= self.hold_ticks:
                    logger.info(
                        f"[tick {self._tick_count}] Held for {self._ticks_held} ticks, closing LP"
                    )
                    return self._create_close_intent()
                return Intent.hold(
                    reason=f"LP active, held {self._ticks_held}/{self.hold_ticks} ticks"
                )
            else:
                logger.info(f"[tick {self._tick_count}] No position, opening LP")
                return self._create_open_intent(current_price)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent centered on current price."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        # Both WBNB and USDT are 18 decimals on BSC
        decimals0 = 18
        decimals1 = 18

        tick_lower = get_nearest_tick(price_to_tick(range_lower, decimals0, decimals1), self.fee_tier)
        tick_upper = get_nearest_tick(price_to_tick(range_upper, decimals0, decimals1), self.fee_tier)

        min_tick = get_min_tick(self.fee_tier)
        max_tick = get_max_tick(self.fee_tier)
        tick_lower = max(tick_lower, min_tick)
        tick_upper = min(tick_upper, max_tick)

        if tick_lower >= tick_upper:
            raise ValueError(
                f"Invalid tick range: tick_lower={tick_lower} >= tick_upper={tick_upper}. "
                f"Try widening range_width_pct (currently {self.range_width_pct})."
            )

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}], "
            f"ticks [{tick_lower} - {tick_upper}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="sushiswap_v3",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent."""
        position_id_str = str(self._position_id) if self._position_id else self._lp_position_id()
        logger.info(f"LP_CLOSE: position_id={position_id_str}")
        return Intent.lp_close(
            position_id=position_id_str,
            pool=self.pool,
            collect_fees=True,
            protocol="sushiswap_v3",
        )

    def _lp_position_id(self) -> str:
        """Canonical LP position ID."""
        return f"sushiswap-v3-lp-{self.pool.replace('/', '-')}-bsc"

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track LP position state from execution results."""
        if not success:
            logger.warning(f"Intent failed: {getattr(intent, 'intent_type', 'unknown')}")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_val == "LP_OPEN":
            self._has_position = True
            self._ticks_held = 0
            position_id = getattr(result, "position_id", None) if result else None
            if position_id:
                self._position_id = int(position_id)
            logger.info(f"LP position opened in {self.pool}, position_id={self._position_id}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Opened SushiSwap V3 LP in {self.pool} (BSC)",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_open", "pool": self.pool, "position_id": self._position_id},
                )
            )

        elif intent_type_val == "LP_CLOSE":
            logger.info(f"LP position closed in {self.pool} after {self._ticks_held} ticks")
            self._has_position = False
            self._ticks_held = 0
            self._position_id = None
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Closed SushiSwap V3 LP in {self.pool} (BSC)",
                    strategy_id=self.strategy_id,
                    details={"action": "lp_close", "pool": self.pool},
                )
            )

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "has_position": self._has_position,
            "position_id": self._position_id,
            "ticks_held": self._ticks_held,
            "tick_count": self._tick_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_position" in state:
            self._has_position = bool(state["has_position"])
        if "position_id" in state:
            val = state["position_id"]
            self._position_id = int(val) if val is not None else None
        if "ticks_held" in state:
            self._ticks_held = int(state["ticks_held"])
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])
        logger.info(
            f"Restored state: has_position={self._has_position}, "
            f"ticks_held={self._ticks_held}, tick_count={self._tick_count}"
        )

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_position:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._lp_position_id(),
                    chain=self.chain,
                    protocol="sushiswap_v3",
                    value_usd=Decimal("0"),
                    details={
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                        "fee_tier": self.fee_tier,
                        "nft_position_id": self._position_id,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []
        if self._has_position:
            intents.append(self._create_close_intent())
        return intents

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_sushiswap_v3_paper_trade_lp_bsc",
            "chain": self.chain,
            "state": {
                "has_position": self._has_position,
                "position_id": self._position_id,
                "ticks_held": self._ticks_held,
                "tick_count": self._tick_count,
            },
            "config": {
                "pool": self.pool,
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
                "hold_ticks": self.hold_ticks,
            },
        }
