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
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_pancakeswap_lp",
    description="PancakeSwap V3 LP lifecycle on Arbitrum (LP_OPEN + LP_CLOSE)",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lp", "pancakeswap-v3", "arbitrum"],
    supported_chains=["arbitrum"],
    default_chain="arbitrum",
    supported_protocols=["pancakeswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
    quote_asset="USD",
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

        # Minimum total inventory (USD) required to (re)open a position.
        self.min_position_usd = Decimal(str(self.get_config("min_position_usd", "100")))

        self._current_position_id: str | None = None
        # Range the live position was opened with -- used to detect drift and
        # trigger a rebalance (close -> swap-to-ratio -> reopen).
        self._range_lower: Decimal | None = None
        self._range_upper: Decimal | None = None
        self._load_position_from_state()

        logger.info(
            f"PancakeSwapLPStrategy initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """LP decision: rebalance on drift, balance inventory then (re)open."""
        # Get current price
        try:
            token0_price = market.price(self.token0_symbol)
            token1_price = market.price(self.token1_symbol)
            current_price = token0_price / token1_price
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}")
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # Position open -> rebalance if price has drifted out of range
        if self._current_position_id:
            if self._range_lower is not None and self._range_upper is not None:
                if current_price < self._range_lower or current_price > self._range_upper:
                    logger.info(
                        f"Price {current_price:.2f} exited range "
                        f"[{self._range_lower:.2f}, {self._range_upper:.2f}] - closing to rebalance"
                    )
                    return self._create_close_intent(self._current_position_id)
                return Intent.hold(
                    reason=f"Position {self._current_position_id} in range "
                    f"[{self._range_lower:.2f}, {self._range_upper:.2f}]"
                )
            # Range unknown (e.g. opened by an older version) -- hold rather than
            # rebalance blindly.
            return Intent.hold(
                reason=f"PancakeSwap V3 LP position {self._current_position_id} active - range unknown"
            )

        # No position -> balance inventory to ~50/50, then (re)open.
        # After a drift-close the wallet holds a skewed inventory (mostly one
        # token), so swap the heavy side back toward 50/50 BEFORE reopening --
        # otherwise the new range opens lopsided.
        try:
            t0 = market.balance(self.token0_symbol, price=token0_price)
            t1 = market.balance(self.token1_symbol, price=token1_price)
            token0_balance = Decimal(str(t0.balance))
            token1_balance = Decimal(str(t1.balance))
            token0_usd = Decimal(str(t0.balance_usd))
            token1_usd = Decimal(str(t1.balance_usd))
        except (ValueError, KeyError):
            return Intent.hold(reason="Cannot check balances")

        total_usd = token0_usd + token1_usd
        if total_usd < self.min_position_usd:
            return Intent.hold(
                reason=f"Total ${total_usd:.2f} below min_position_usd ${self.min_position_usd:.2f}"
            )

        swap_intent = self._rebalance_swap_intent(token0_usd, token1_usd, total_usd)
        if swap_intent is not None:
            return swap_intent

        logger.info("No position - opening PancakeSwap V3 LP with balanced inventory")
        return self._create_open_intent(
            current_price,
            amount0=token0_balance * Decimal("0.95"),
            amount1=token1_balance * Decimal("0.95"),
        )

    def _create_open_intent(
        self,
        current_price: Decimal,
        amount0: Decimal | None = None,
        amount1: Decimal | None = None,
    ) -> Intent:
        """Create an LP_OPEN intent centered on current price."""
        amount0 = self.amount0 if amount0 is None else amount0
        amount1 = self.amount1 if amount1 is None else amount1

        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"Opening PancakeSwap V3 LP: {amount0} {self.token0_symbol} + "
            f"{amount1} {self.token1_symbol}, "
            f"range [{range_lower:.2f} - {range_upper:.2f}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="pancakeswap_v3",
        )

    def _rebalance_swap_intent(
        self, token0_usd: Decimal, token1_usd: Decimal, total_usd: Decimal
    ) -> Intent | None:
        """Swap the heavy side toward a ~50/50 USD split before (re)opening.

        Returns a SWAP intent when inventory is skewed beyond a 10% tolerance
        band, else None (balanced enough to open as-is).
        """
        half_usd = total_usd / Decimal("2")
        tolerance_usd = total_usd * Decimal("0.10")
        if token0_usd - half_usd > tolerance_usd:
            logger.info(
                f"Rebalance swap: {self.token0_symbol} -> {self.token1_symbol} "
                f"(${token0_usd - half_usd:.2f} to reach ~50/50)"
            )
            return Intent.swap(
                from_token=self.token0_symbol,
                to_token=self.token1_symbol,
                amount_usd=token0_usd - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="pancakeswap_v3",
            )
        if token1_usd - half_usd > tolerance_usd:
            logger.info(
                f"Rebalance swap: {self.token1_symbol} -> {self.token0_symbol} "
                f"(${token1_usd - half_usd:.2f} to reach ~50/50)"
            )
            return Intent.swap(
                from_token=self.token1_symbol,
                to_token=self.token0_symbol,
                amount_usd=token1_usd - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="pancakeswap_v3",
            )
        return None

    def _create_close_intent(self, position_id: str) -> Intent:
        """Create an LP_CLOSE intent to close an existing position."""
        logger.info(f"LP_CLOSE: position={position_id}")
        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="pancakeswap_v3",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position ID after LP_OPEN, clear after LP_CLOSE."""
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = getattr(result, "position_id", None) if result else None

            # Record the range we opened with so decide() can detect drift.
            rl = getattr(intent, "range_lower", None)
            ru = getattr(intent, "range_upper", None)
            self._range_lower = Decimal(str(rl)) if rl is not None else None
            self._range_upper = Decimal(str(ru)) if ru is not None else None

            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"PancakeSwap V3 LP opened: position_id={position_id}")
            else:
                logger.warning("LP opened but could not extract position ID")
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"PancakeSwap V3 LP closed: position_id={self._current_position_id}")
            self._current_position_id = None
            self._range_lower = None
            self._range_upper = None

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
        if self._range_lower is not None:
            state["range_lower"] = str(self._range_lower)
        if self._range_upper is not None:
            state["range_upper"] = str(self._range_upper)
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persistent state including position ID."""
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
        if state.get("range_lower") is not None:
            self._range_lower = Decimal(str(state["range_lower"]))
        if state.get("range_upper") is not None:
            self._range_upper = Decimal(str(state["range_upper"]))

    # Teardown support

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
            deployment_id=getattr(self, "deployment_id", "demo_pancakeswap_lp"),
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
