"""
PancakeSwap V3 LP Full Lifecycle Strategy on BSC.

Tests LP_OPEN -> HOLD -> LP_CLOSE full lifecycle using PancakeSwap V3
on BSC with the WBNB/USDT pool. Exercises concentrated liquidity
management on BSC which has been an underexercised chain for LP operations.

Kitchen Loop iteration 155 (VIB-2308).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data import PriceUnavailableError
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="pancakeswap_lp_lifecycle_bsc",
    description="PancakeSwap V3 LP full lifecycle (LP_OPEN + LP_CLOSE) on BSC",
    version="1.0.0",
    author="Kitchen Loop iter 155",
    tags=["demo", "lp", "pancakeswap-v3", "bsc", "lifecycle", "lp-close"],
    supported_chains=["bsc"],
    default_chain="bsc",
    supported_protocols=["pancakeswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class PancakeSwapV3LPLifecycleBSCStrategy(IntentStrategy):
    """PancakeSwap V3 LP lifecycle on BSC: open, hold, close.

    Tick 1: LP_OPEN (mint concentrated liquidity position in WBNB/USDT)
    Tick 2: HOLD (verify position was created)
    Tick 3: LP_CLOSE (decreaseLiquidity + collect + burn)
    Tick 4+: HOLD (lifecycle complete)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.get_config("pool", "WBNB/USDT/2500")
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WBNB"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDT"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 2500

        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.20")))
        self.amount0 = Decimal(str(self.get_config("amount0", "0.01")))
        self.amount1 = Decimal(str(self.get_config("amount1", "5")))

        self._current_position_id: str | None = None
        self._tick = 0
        self._lifecycle_complete = False
        self._lp_close_attempted = False

        logger.info(
            f"PancakeSwapV3LPLifecycleBSCStrategy initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """LP lifecycle: open -> hold -> close."""
        self._tick += 1

        if self._lifecycle_complete:
            return Intent.hold(reason="LP lifecycle complete")

        # Tick 1: Open LP position (requires price for range calculation)
        if self._tick == 1:
            try:
                token0_price = market.price(self.token0_symbol)
                token1_price = market.price(self.token1_symbol)
                if token1_price == 0:
                    logger.warning("Token1 price is zero, cannot compute price ratio")
                    return Intent.hold(reason="Token1 price is zero")
                current_price = token0_price / token1_price
            except (ValueError, KeyError, PriceUnavailableError, ZeroDivisionError) as e:
                logger.warning(f"Could not get price: {e}")
                return Intent.hold(reason="Price data unavailable")
            return self._open_lp(current_price)

        # Tick 2: Hold (verify position was created)
        if self._tick == 2:
            if self._current_position_id:
                return Intent.hold(
                    reason=f"LP position {self._current_position_id} active, will close next tick"
                )
            else:
                logger.warning("LP_OPEN completed without position_id")
                self._lifecycle_complete = True
                return Intent.hold(reason="LP_OPEN position_id missing; holding for inspection")

        # Tick 3: Close LP position
        if self._tick == 3 and self._current_position_id and not self._lp_close_attempted:
            self._lp_close_attempted = True
            logger.info(f"Closing PancakeSwap V3 LP position {self._current_position_id}")
            return Intent.lp_close(
                position_id=self._current_position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="pancakeswap_v3",
            )

        # Tick 4+: Done
        self._lifecycle_complete = True
        return Intent.hold(reason="LP lifecycle complete")

    def _open_lp(self, current_price: Decimal) -> Intent:
        """Build LP_OPEN intent with range centered on current price."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"Opening PancakeSwap V3 LP: {self.amount0} {self.token0_symbol} + "
            f"{self.amount1} {self.token1_symbol}, "
            f"range [{range_lower:.2f} - {range_upper:.2f}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="pancakeswap_v3",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position ID after LP_OPEN, clear after LP_CLOSE."""
        intent_type = intent.intent_type.value

        if intent_type == "LP_OPEN":
            if success:
                position_id = getattr(result, "position_id", None) if result else None
                if position_id:
                    self._current_position_id = str(position_id)
                    logger.info(f"LP_OPEN SUCCESS: position_id={position_id}")
                else:
                    logger.warning("LP_OPEN succeeded but position_id not extracted")
            else:
                logger.error(f"LP_OPEN FAILED: {result}")

        elif intent_type == "LP_CLOSE":
            if success:
                logger.info(f"LP_CLOSE SUCCESS: position {self._current_position_id} closed")
                lp_close_data = getattr(result, "lp_close_data", None) if result else None
                if lp_close_data:
                    logger.info(f"LP_CLOSE enrichment data: {lp_close_data}")
                self._current_position_id = None
                self._lifecycle_complete = True
            else:
                logger.error(f"LP_CLOSE FAILED for position {self._current_position_id}: {result}")
                self._lifecycle_complete = True

    def _estimate_position_value(self) -> Decimal:
        """Estimate position value using live prices."""
        try:
            market = self.create_market_snapshot()
            token0_price = market.price(self.token0_symbol)
            token1_price = market.price(self.token1_symbol)
            return self.amount0 * token0_price + self.amount1 * token1_price
        except Exception:
            return Decimal("0")

    # Teardown support

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._current_position_id:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._current_position_id),
                    chain=self.chain,
                    protocol="pancakeswap_v3",
                    value_usd=self._estimate_position_value(),
                    details={
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self._strategy_id or "pancakeswap_lp_lifecycle_bsc",
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        if not self._current_position_id:
            return []
        return [
            Intent.lp_close(
                position_id=self._current_position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="pancakeswap_v3",
            )
        ]
