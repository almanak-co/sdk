"""Uniswap V3 LP on Optimism -- YAInnick Loop Iteration 18.

First yailoop strategy on Optimism chain. Opens a concentrated Uniswap V3 LP
position (WETH/USDC, 0.05% fee tier) around the current price. Exercises chain
support, token resolution, Anvil fork, gateway auto-start, and wallet funding
on Optimism.

Run:
    almanak strat run -d strategies/incubating/uniswap_lp_optimism --network anvil --once
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)


@dataclass
class OptimismLPConfig:
    """Configuration for Uniswap V3 LP strategy on Optimism."""

    pool: str = "WETH/USDC/500"
    range_width_pct: Decimal = Decimal("0.20")
    amount0: Decimal = Decimal("0.001")
    amount1: Decimal = Decimal("3")
    force_action: str = ""
    position_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
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
    name="uniswap_lp_optimism",
    description="Uniswap V3 LP on Optimism - first yailoop test on OP chain",
    version="1.0.0",
    author="YAInnick Loop",
    tags=["incubating", "lp", "uniswap-v3", "optimism"],
    supported_chains=["optimism"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
)
class UniswapLPOptimismStrategy(IntentStrategy[OptimismLPConfig]):
    """Uniswap V3 concentrated LP on Optimism.

    Opens a WETH/USDC LP position with a configurable price range width.
    Designed to stress-test the Optimism chain path end-to-end.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 500

        self.range_width_pct = Decimal(str(self.config.range_width_pct))
        self.amount0 = Decimal(str(self.config.amount0))
        self.amount1 = Decimal(str(self.config.amount1))
        self.force_action = str(self.config.force_action).lower()
        self.position_id = self.config.position_id

        self._current_position_id: str | None = None
        self._load_position_from_state()

        logger.info(
            f"UniswapLPOptimismStrategy initialized: "
            f"pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
            + (f", position_id={self._current_position_id}" if self._current_position_id else "")
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make LP decision based on market conditions."""
        try:
            # Get current price (token1 per token0)
            try:
                token0_price_usd = market.price(self.token0_symbol)
                token1_price_usd = market.price(self.token1_symbol)
                current_price = token0_price_usd / token1_price_usd
                logger.info(f"Current price: {current_price:.2f} {self.token1_symbol}/{self.token0_symbol}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get price: {e}")
                current_price = Decimal("2500")

            # Handle forced actions (for testing)
            if self.force_action == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(current_price)

            if self.force_action == "close":
                if not self.position_id:
                    logger.warning("force_action=close but no position_id provided")
                    return Intent.hold(reason="Close requested but no position_id")
                logger.info(f"Forced action: CLOSE LP position {self.position_id}")
                return self._create_close_intent(self.position_id)

            # If we have a position, monitor it
            if self._current_position_id:
                return Intent.hold(reason=f"Position {self._current_position_id} exists - monitoring")

            # No position -- check balances and open
            try:
                token0_balance_result = market.balance(self.token0_symbol)
                token1_balance_result = market.balance(self.token1_symbol)

                token0_balance = (
                    token0_balance_result.balance
                    if hasattr(token0_balance_result, "balance")
                    else token0_balance_result
                )
                token1_balance = (
                    token1_balance_result.balance
                    if hasattr(token1_balance_result, "balance")
                    else token1_balance_result
                )

                if token0_balance < self.amount0:
                    return Intent.hold(
                        reason=f"Insufficient {self.token0_symbol}: {token0_balance} < {self.amount0}"
                    )
                if token1_balance < self.amount1:
                    return Intent.hold(
                        reason=f"Insufficient {self.token1_symbol}: {token1_balance} < {self.amount1}"
                    )
            except (ValueError, KeyError):
                logger.warning("Could not verify balances, proceeding anyway")

            logger.info("No position found - opening new LP position")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description="No position found - opening new LP position",
                    strategy_id=self.strategy_id,
                    details={"action": "opening_new_position"},
                )
            )
            return self._create_open_intent(current_price)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent centered on current price."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"range [{format_usd(range_lower)} - {format_usd(range_upper)}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
        )

    def _create_close_intent(self, position_id: str) -> Intent:
        """Create LP_CLOSE intent for an existing position."""
        logger.info(f"LP_CLOSE: position={position_id}")

        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v3",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position ID after LP_OPEN."""
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = result.position_id if result else None

            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"LP position opened successfully: position_id={position_id}")
                self._save_position_to_state(position_id)
            else:
                logger.warning("LP position opened but could not extract position ID from receipt")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"LP position opened on {self.pool}"
                    + (f" (ID: {position_id})" if position_id else ""),
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "position_id": str(position_id) if position_id else None},
                )
            )

    def _load_position_from_state(self) -> None:
        """Load position ID from persistent state."""
        try:
            state = self.get_persistent_state()
            if state and "current_position_id" in state:
                self._current_position_id = str(state["current_position_id"])
                logger.info(f"Loaded position ID from state: {self._current_position_id}")
        except Exception as e:
            logger.warning(f"Failed to load position from state: {e}")

    def _save_position_to_state(self, position_id: int) -> None:
        """Save position ID to strategy state."""
        self._current_position_id = str(position_id)
        logger.info(f"Updated position ID: {position_id}")

    def get_persistent_state(self) -> dict[str, Any]:
        """Get persistent state including position ID."""
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
            if "position_opened_at" not in state:
                state["position_opened_at"] = datetime.now(UTC).isoformat()
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persistent state including position ID."""
        super().load_persistent_state(state) if hasattr(super(), "load_persistent_state") else None
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
            logger.info(f"Restored position ID from state: {self._current_position_id}")
