"""
===============================================================================
Uniswap V4 LP Strategy — Concentrated Liquidity via PositionManager
===============================================================================

Demonstrates Uniswap V4 concentrated liquidity management using the V4
PositionManager's flash accounting model (modifyLiquidities + BalanceDelta).

WHAT THIS STRATEGY DOES:
1. Opens a WETH/USDC concentrated LP position on Uniswap V4 (Arbitrum)
2. Monitors if the position is still in range
3. When out of range: closes the position and re-opens centered on current price
4. Collects fees via LP_COLLECT_FEES intent

KEY V4 DIFFERENCES FROM V3:
- Singleton PoolManager (all pools in one contract)
- Flash accounting: modifyLiquidities batches multiple operations atomically
- Pool keys include a hooks address field
- Native ETH support (no mandatory WETH wrapping for pools)
- Uses protocol="uniswap_v4" in all intents

V4 compilation and execution are functional on all supported chains.
LP positions use the PositionManager's flash accounting model.

===============================================================================
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)


@dataclass
class UniswapV4LPConfig:
    """Configuration for Uniswap V4 LP strategy.

    Attributes:
        pool: Pool identifier in format "TOKEN0/TOKEN1/FEE" (e.g., "WETH/USDC/3000")
        range_width_pct: Total width of price range as decimal (0.20 = 20%)
        amount0: Amount of token0 to provide (e.g., "0.01" WETH)
        amount1: Amount of token1 to provide (e.g., "30" USDC)
        min_position_usd: Minimum total inventory (USD) required to (re)open a position
    """

    pool: str = "WETH/USDC/3000"
    range_width_pct: Decimal = Decimal("0.20")
    amount0: Decimal = Decimal("0.01")
    amount1: Decimal = Decimal("30")
    min_position_usd: Decimal = Decimal("100")
    force_action: str = ""
    position_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "min_position_usd": str(self.min_position_usd),
            "force_action": self.force_action,
            "position_id": self.position_id,
        }

    def update(self, **kwargs: Any) -> Any:
        @dataclass
        class UpdateResult:
            success: bool = True
            updated_fields: list = field(default_factory=list)

        updated = []
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                updated.append(k)
        return UpdateResult(success=True, updated_fields=updated)


@almanak_strategy(
    name="demo_uniswap_v4_lp",
    description="Uniswap V4 concentrated LP — PositionManager flash accounting on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lp", "liquidity", "uniswap-v4", "arbitrum", "v4"],
    supported_chains=["arbitrum", "ethereum", "base"],
    supported_protocols=["uniswap_v4"],
    intent_types=["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "SWAP", "HOLD"],
    default_chain="arbitrum",
    quote_asset="USD",
)
class UniswapV4LPStrategy(IntentStrategy[UniswapV4LPConfig]):
    """Uniswap V4 concentrated liquidity strategy.

    Manages LP positions using V4's PositionManager. Key differences from V3:
    - Uses protocol="uniswap_v4" for all intents
    - PositionManager uses flash accounting (modifyLiquidities)
    - Pool keys include hooks address (zero address for hookless pools)
    - LP_COLLECT_FEES intent supported natively
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 3000

        self.range_width_pct = Decimal(str(self.config.range_width_pct))
        self.amount0 = Decimal(str(self.config.amount0))
        self.amount1 = Decimal(str(self.config.amount1))

        self.force_action = str(self.config.force_action).lower() if self.config.force_action else ""
        self._config_position_id = self.config.position_id

        # Minimum total inventory (USD) required to (re)open a position.
        self.min_position_usd = Decimal(str(self.get_config("min_position_usd", "100")))

        self._current_position_id: str | None = None
        self._liquidity: int | None = None
        # Range the live position was opened with -- used to detect drift and
        # trigger a rebalance (close -> swap-to-ratio -> reopen).
        self._range_lower: Decimal | None = None
        self._range_upper: Decimal | None = None
        self._load_position_from_state()

        # Config position_id overrides state (for testing)
        if self._config_position_id and not self._current_position_id:
            self._current_position_id = self._config_position_id

        logger.info(
            f"UniswapV4LPStrategy initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """LP decision: open, rebalance, collect fees, or hold."""
        # Forced close/collect don't need price data — handle before price fetch
        if self.force_action == "close":
            pos_id = self._current_position_id or self._config_position_id
            if pos_id:
                logger.info(f"Forced LP_CLOSE for position {pos_id}")
                return self._create_close_intent(pos_id)
            return Intent.hold(reason="Close requested but no position_id")

        if self.force_action == "collect":
            if self._current_position_id:
                logger.info(f"Forced LP_COLLECT_FEES for position {self._current_position_id}")
                return self._create_collect_fees_intent()
            return Intent.hold(reason="Collect requested but no position")

        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            if not token1_price_usd:
                return Intent.hold(reason=f"Invalid price for {self.token1_symbol}")
            current_price = token0_price_usd / token1_price_usd
        except (ValueError, KeyError, ZeroDivisionError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        if self.force_action == "open":
            logger.info("Forced LP_OPEN")
            return self._create_open_intent(current_price)

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
                    reason=f"V4 position {self._current_position_id} in range "
                    f"[{self._range_lower:.2f}, {self._range_upper:.2f}]"
                )
            # Range unknown (e.g. opened by an older version) -- hold rather than
            # rebalance blindly.
            return Intent.hold(
                reason=f"V4 position {self._current_position_id} active — range unknown"
            )

        # No position -> balance inventory to ~50/50, then (re)open.
        # After a drift-close the wallet holds a skewed inventory (mostly one
        # token), so swap the heavy side back toward 50/50 BEFORE reopening --
        # otherwise the new range opens lopsided.
        try:
            t0 = market.balance(self.token0_symbol, price=token0_price_usd)
            t1 = market.balance(self.token1_symbol, price=token1_price_usd)
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

        logger.info("No V4 position found — opening new LP position with balanced inventory")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="Opening new V4 LP position with balanced inventory",
                deployment_id=self.deployment_id,
                details={"action": "opening_v4_position"},
            )
        )
        # Deploy ~95% of each balanced side (small buffer for gas/rounding).
        return self._create_open_intent(
            current_price,
            amount0=token0_balance * Decimal("0.95"),
            amount1=token1_balance * Decimal("0.95"),
        )

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_open_intent(
        self,
        current_price: Decimal,
        amount0: Decimal | None = None,
        amount1: Decimal | None = None,
    ) -> Intent:
        """Create LP_OPEN intent for V4 PositionManager.

        Amounts default to the configured amount0/amount1 (initial open /
        force_action); the rebalance path passes the balanced wallet amounts
        to redeploy.
        """
        amount0 = self.amount0 if amount0 is None else amount0
        amount1 = self.amount1 if amount1 is None else amount1

        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN (V4): {format_token_amount_human(amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(amount1, self.token1_symbol)}, "
            f"range [{format_usd(range_lower)} - {format_usd(range_upper)}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v4",
            # VIB-2180/VIB-2701: V4 StateView.getSlot0 reverts on the Anvil fork ->
            # estimated price; opt in so the money-safety guard doesn't block the open.
            protocol_params={"allow_estimated_price": True},
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
                protocol="uniswap_v4",
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
                protocol="uniswap_v4",
            )
        return None

    def _create_close_intent(self, position_id: str) -> Intent:
        """Create LP_CLOSE intent for V4 PositionManager.

        The compiler will query on-chain liquidity via PositionManager.getPositionLiquidity()
        if not provided in protocol_params. We pass cached liquidity when available to
        avoid an extra RPC call, but the compiler handles the fallback.
        """
        protocol_params: dict[str, Any] = {}

        # Pass cached liquidity if available (saves an RPC call at compilation)
        if self._liquidity is not None:
            protocol_params["liquidity"] = self._liquidity

        logger.info(f"LP_CLOSE (V4): position={position_id}, cached_liquidity={self._liquidity}")

        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v4",
            protocol_params=protocol_params,
        )

    def _create_collect_fees_intent(self) -> Intent:
        """Create LP_COLLECT_FEES intent for V4 PositionManager."""
        logger.info(f"LP_COLLECT_FEES (V4): pool={self.pool} position={self._current_position_id}")
        return Intent.collect_fees(
            pool=self.pool,
            protocol="uniswap_v4",
            protocol_params={"position_id": self._current_position_id},
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = result.position_id if result else None

            # Record the range we opened with so decide() can detect drift.
            rl = getattr(intent, "range_lower", None)
            ru = getattr(intent, "range_upper", None)
            self._range_lower = Decimal(str(rl)) if rl is not None else None
            self._range_upper = Decimal(str(ru)) if ru is not None else None

            if position_id:
                self._current_position_id = str(position_id)
                # Reset liquidity before extraction to avoid stale values from previous position
                self._liquidity = None
                if result and hasattr(result, "extracted_data"):
                    liq = result.extracted_data.get("liquidity")
                    if liq is not None:
                        self._liquidity = int(liq)
                logger.info(f"V4 LP position opened: position_id={position_id}, liquidity={self._liquidity}")
                self._save_position_to_state(position_id)
            else:
                logger.warning("V4 LP position opened but could not extract position ID")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"V4 LP position opened on {self.pool}"
                    + (f" (ID: {position_id})" if position_id else ""),
                    deployment_id=self.deployment_id,
                    details={"pool": self.pool, "position_id": str(position_id) if position_id else None},
                )
            )
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"V4 LP position closed: position_id={self._current_position_id}")
            self._current_position_id = None
            self._liquidity = None
            self._range_lower = None
            self._range_upper = None

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def _load_position_from_state(self) -> None:
        state = self.get_persistent_state()
        if state and "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
            logger.info(f"Loaded V4 position ID from state: {self._current_position_id}")

    def _save_position_to_state(self, position_id: int) -> None:
        self._current_position_id = str(position_id)

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
            if "position_opened_at" not in state:
                state["position_opened_at"] = datetime.now(UTC).isoformat()
        else:
            # Clear stale position after LP_CLOSE so restarts don't see a phantom position
            state.pop("current_position_id", None)
            state.pop("position_opened_at", None)
        if self._range_lower is not None:
            state["range_lower"] = str(self._range_lower)
        else:
            state.pop("range_lower", None)
        if self._range_upper is not None:
            state["range_upper"] = str(self._range_upper)
        else:
            state.pop("range_upper", None)
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
        if state.get("range_lower") is not None:
            self._range_lower = Decimal(str(state["range_lower"]))
        if state.get("range_upper") is not None:
            self._range_upper = Decimal(str(state["range_upper"]))

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        if self._current_position_id:
            try:
                snapshot = self.create_market_snapshot()
                t0_price = snapshot.price(self.token0_symbol)
                t1_price = snapshot.price(self.token1_symbol)
            except Exception:  # noqa: BLE001
                t0_price = Decimal("0")
                t1_price = Decimal("0")

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._current_position_id,
                    chain=self.chain,
                    protocol="uniswap_v4",
                    value_usd=self.amount0 * t0_price + self.amount1 * t1_price,
                    details={
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "demo_uniswap_v4_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if not self._current_position_id:
            return []

        logger.info(f"V4 teardown: closing position {self._current_position_id} (mode={mode.value})")
        return [self._create_close_intent(self._current_position_id)]

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"V4 LP teardown completed. Recovered: ${recovered_usd:,.2f}")
            self._current_position_id = None
            self._liquidity = None
        else:
            logger.warning(f"V4 LP teardown failed. Partial recovery: ${recovered_usd:,.2f}")
