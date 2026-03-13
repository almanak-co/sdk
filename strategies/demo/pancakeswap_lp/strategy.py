"""
PancakeSwap V3 LP Lifecycle Strategy on Arbitrum.

Tests LP_OPEN and LP_CLOSE with PancakeSwap V3 on Arbitrum.
Validates the VIB-594 fix (LP_POSITION_MANAGERS entries) end-to-end.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_pancakeswap_lp",
    description="PancakeSwap V3 LP lifecycle on Arbitrum (LP_OPEN + LP_CLOSE)",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lp", "pancakeswap-v3", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["pancakeswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class PancakeSwapLPStrategy(IntentStrategy):
    """PancakeSwap V3 LP strategy for testing LP lifecycle on Arbitrum."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.get_config("pool", "WETH/USDC/500")
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 500

        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.20")))
        self.amount0 = Decimal(str(self.get_config("amount0", "0.001")))
        self.amount1 = Decimal(str(self.get_config("amount1", "3")))

        self._current_position_id: str | None = None
        self._load_position_from_state()

        logger.info(
            f"PancakeSwapLPStrategy initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """LP decision: open if no position, hold if position exists."""
        # Get current price
        try:
            token0_price = market.price(self.token0_symbol)
            token1_price = market.price(self.token1_symbol)
            current_price = token0_price / token1_price
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}")
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # If we have a position, hold
        if self._current_position_id:
            return Intent.hold(
                reason=f"PancakeSwap V3 LP position {self._current_position_id} active"
            )

        # Check balances
        try:
            token0_bal = market.balance(self.token0_symbol)
            token1_bal = market.balance(self.token1_symbol)
            bal0 = token0_bal.balance if hasattr(token0_bal, "balance") else token0_bal
            bal1 = token1_bal.balance if hasattr(token1_bal, "balance") else token1_bal

            if bal0 < self.amount0:
                return Intent.hold(
                    reason=f"Insufficient {self.token0_symbol}: {bal0} < {self.amount0}"
                )
            if bal1 < self.amount1:
                return Intent.hold(
                    reason=f"Insufficient {self.token1_symbol}: {bal1} < {self.amount1}"
                )
        except (ValueError, KeyError):
            logger.warning("Could not verify balances, proceeding anyway")

        # Open LP position
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
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = getattr(result, "position_id", None) if result else None
            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"PancakeSwap V3 LP opened: position_id={position_id}")
            else:
                logger.warning("LP opened but could not extract position ID")
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"PancakeSwap V3 LP closed: position_id={self._current_position_id}")
            self._current_position_id = None

    def _load_position_from_state(self) -> None:
        """Load position ID from persistent state."""
        state = self.get_persistent_state()
        if state and "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])

    def get_persistent_state(self) -> dict[str, Any]:
        """Get persistent state including position ID."""
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persistent state including position ID."""
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])

    # Teardown support

    def supports_teardown(self) -> bool:
        return True

    def _estimate_lp_value_usd(self) -> Decimal:
        """Estimate LP position value using live prices."""
        try:
            snapshot = self.create_market_snapshot()
            token0_price = snapshot.price(self.token0_symbol)
            token1_price = snapshot.price(self.token1_symbol)
            return self.amount0 * token0_price + self.amount1 * token1_price
        except Exception:  # noqa: BLE001
            logger.debug("Could not get live prices for LP value estimate, using fallback $0")
            return Decimal("0")

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
                    value_usd=self._estimate_lp_value_usd(),
                    details={
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_pancakeswap_lp"),
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
