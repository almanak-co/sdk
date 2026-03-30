"""
===============================================================================
DEMO: Curve 3pool Paper Trade -- StableSwap LP on Ethereum
===============================================================================

Vehicle for testing the paper trading engine with Curve StableSwap LP.
First paper trade for Curve on any chain. Validates that LP_OPEN and LP_CLOSE
work end-to-end on Ethereum 3pool (DAI/USDC/USDT) with PnL tracking.

PURPOSE:
--------
1. Validate paper trading pipeline with Curve StableSwap:
   - Anvil fork for Ethereum mainnet
   - LP_OPEN via add_liquidity to Curve 3pool
   - PnL journal entries tracking LP token value over ticks
   - LP_CLOSE via remove_liquidity for exit
2. Exercise stableswap-specific LP mechanics (no tick ranges, virtual_price).

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \
        -s demo_curve_paper_trade_ethereum \
        --chain ethereum \
        --max-ticks 5 \
        --tick-interval 60 \
        --foreground

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/curve_paper_trade_ethereum \
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. If no LP position and have USDC: open LP (add_liquidity to 3pool)
  2. Hold for N ticks to accumulate yield/PnL data points
  3. After hold_ticks: close LP (remove_liquidity from 3pool)
  4. Repeat cycle (paper trader runs multiple ticks)

This creates open -> hold -> close cycles across ticks, generating PnL
journal entries for the paper trader to track.
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_curve_paper_trade_ethereum",
    description="Paper trade: Curve 3pool StableSwap LP on Ethereum with PnL tracking",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trade", "curve", "stableswap", "ethereum", "lp", "3pool"],
    supported_chains=["ethereum"],
    supported_protocols=["curve"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="ethereum",
)
class CurvePaperTradeStrategy(IntentStrategy):
    """Curve 3pool StableSwap LP for paper trading.

    Cycles through open -> hold -> close to generate PnL data points.
    Designed for `almanak strat backtest paper`.

    Configuration (from config.json):
        pool: Curve pool name (default: "3pool")
        deposit_token: Token to deposit (default: "USDC")
        deposit_amount: Amount to deposit (default: "100")
        hold_ticks: Number of ticks to hold before closing (default: 3)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.pool = str(self.get_config("pool", "3pool"))
        if self.pool != "3pool":
            raise ValueError(
                f"CurvePaperTradeStrategy only supports pool='3pool', got {self.pool!r}"
            )
        self.deposit_token = str(self.get_config("deposit_token", "USDC")).upper()
        self.deposit_amount = Decimal(str(self.get_config("deposit_amount", "100")))
        self.hold_ticks = int(self.get_config("hold_ticks", 3))

        # Internal state
        self._has_position = False
        self._lp_token_balance = Decimal("0")
        self._lp_token_address: str | None = None  # LP token contract address
        self._ticks_held = 0
        self._cycles_completed = 0

        logger.info(
            f"CurvePaperTradeStrategy initialized: pool={self.pool}, "
            f"deposit={self.deposit_amount} {self.deposit_token}, "
            f"hold_ticks={self.hold_ticks}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Cycle through open -> hold -> close for PnL data generation."""
        # No position: try to open
        if not self._has_position:
            # Check balance
            try:
                bal = market.balance(self.deposit_token)
                if bal < self.deposit_amount:
                    return Intent.hold(
                        reason=f"Insufficient {self.deposit_token}: "
                        f"{bal} < {self.deposit_amount}"
                    )
            except (ValueError, KeyError):
                logger.warning(f"Could not verify {self.deposit_token} balance, holding")
                return Intent.hold(
                    reason=f"Cannot verify {self.deposit_token} balance, holding until available"
                )

            logger.info(
                f"Opening Curve 3pool LP: {self.deposit_amount} {self.deposit_token}"
            )
            return self._create_open_intent()

        # Has position: hold or close
        self._ticks_held += 1

        if self._ticks_held >= self.hold_ticks:
            logger.info(
                f"Closing Curve 3pool LP after {self._ticks_held} ticks "
                f"(cycle {self._cycles_completed + 1})"
            )
            return self._create_close_intent()

        return Intent.hold(
            reason=f"Holding Curve 3pool LP (tick {self._ticks_held}/{self.hold_ticks}, "
            f"cycle {self._cycles_completed + 1})"
        )

    # Supported deposit tokens: DAI (index 0) and USDC (index 1) in 3pool.
    # USDT (index 2) is NOT supported because LPOpenIntent only has amount0/amount1.
    _SUPPORTED_DEPOSIT_TOKENS = {"DAI", "USDC"}

    def _create_open_intent(self) -> Intent:
        """Create LP_OPEN intent for Curve 3pool.

        3pool coin order: DAI (index 0), USDC (index 1), USDT (index 2).
        LPOpenIntent only supports amount0 + amount1, so only DAI and USDC
        deposits are supported. USDT would require amount2 which doesn't exist.
        """
        token_upper = self.deposit_token.upper()
        if token_upper not in self._SUPPORTED_DEPOSIT_TOKENS:
            raise ValueError(
                f"Unsupported deposit_token '{self.deposit_token}' for 3pool. "
                f"Only {self._SUPPORTED_DEPOSIT_TOKENS} are supported "
                f"(USDT is index 2 but LPOpenIntent only has amount0/amount1)."
            )

        if token_upper == "DAI":
            amount0 = self.deposit_amount
            amount1 = Decimal("0")
        else:
            # USDC goes into amount1 (index 1 in 3pool)
            amount0 = Decimal("0")
            amount1 = self.deposit_amount

        return Intent.lp_open(
            pool=self.pool,
            amount0=amount0,
            amount1=amount1,
            # Required by LPOpenIntent validation, ignored by Curve compiler
            range_lower=Decimal("1"),
            range_upper=Decimal("1000000"),
            protocol="curve",
        )

    def _create_close_intent(self) -> Intent:
        """Create LP_CLOSE intent to remove liquidity from 3pool.

        Uses LP token amount from enrichment when available.  Falls back to
        the LP token address -- the Curve compiler accepts 0x-prefixed
        addresses and queries the on-chain balance automatically.
        """
        if self._lp_token_balance > 0:
            position_id = str(self._lp_token_balance)
        elif self._lp_token_address:
            position_id = self._lp_token_address
        else:
            return Intent.hold(
                reason="Cannot close LP: neither LP token balance nor address available from enrichment"
            )
        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="curve",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track LP lifecycle state after intent execution."""
        if success and intent.intent_type.value == "LP_OPEN":
            self._has_position = True
            self._ticks_held = 0

            # Store LP token address from enrichment (position_id = LP token address for Curve)
            if hasattr(result, "position_id") and result.position_id:
                self._lp_token_address = str(result.position_id)
                logger.info(f"LP token address: {self._lp_token_address}")

            # Extract LP token amount from enrichment.
            # For Curve, position_id is the LP token ADDRESS (not amount).
            # The LP token AMOUNT comes from extracted_data["liquidity"].
            if hasattr(result, "extracted_data") and result.extracted_data:
                lp_amount = (
                    result.extracted_data.get("liquidity")
                    or result.extracted_data.get("lp_tokens")
                )
                if lp_amount:
                    self._lp_token_balance = Decimal(str(lp_amount))
                    logger.info(f"LP token balance from enrichment: {self._lp_token_balance}")

            if self._lp_token_balance <= 0:
                logger.warning(
                    "No LP token balance from enrichment. "
                    "LP_CLOSE will hold until balance is available."
                )

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"Curve 3pool LP opened: {self.deposit_amount} {self.deposit_token}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.pool,
                        "lp_balance": str(self._lp_token_balance),
                        "deposit_token": self.deposit_token,
                    },
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            self._has_position = False
            self._lp_token_balance = Decimal("0")
            self._lp_token_address = None
            self._ticks_held = 0
            self._cycles_completed += 1

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_CLOSED,
                    description=f"Curve 3pool LP closed (cycle {self._cycles_completed})",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "cycle": self._cycles_completed},
                )
            )

        elif not success:
            logger.warning(f"Intent {intent.intent_type.value} failed: {result}")

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist state for crash recovery across paper trade ticks."""
        return {
            "has_position": self._has_position,
            "lp_token_balance": str(self._lp_token_balance),
            "lp_token_address": self._lp_token_address,
            "ticks_held": self._ticks_held,
            "cycles_completed": self._cycles_completed,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state on tick resumption."""
        raw = state.get("has_position", False)
        self._has_position = (
            raw.strip().lower() in {"1", "true", "yes"}
            if isinstance(raw, str)
            else bool(raw)
        )
        try:
            self._lp_token_balance = Decimal(str(state.get("lp_token_balance", "0")))
        except (ArithmeticError, TypeError, ValueError):
            logger.warning("Corrupted lp_token_balance in persisted state, resetting to 0")
            self._lp_token_balance = Decimal("0")
        self._lp_token_address = state.get("lp_token_address")
        if self._lp_token_balance > 0 or self._lp_token_address:
            self._has_position = True
        self._ticks_held = int(state.get("ticks_held", 0))
        self._cycles_completed = int(state.get("cycles_completed", 0))

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_curve_paper_trade_ethereum",
            "chain": self.chain,
            "pool": self.pool,
            "has_position": self._has_position,
            "lp_token_balance": str(self._lp_token_balance),
            "ticks_held": self._ticks_held,
            "cycles_completed": self._cycles_completed,
        }

    # =========================================================================
    # Teardown
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        if self._has_position:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="curve-3pool-paper-trade",
                    chain=self.chain,
                    protocol="curve",
                    value_usd=self.deposit_amount,
                    details={
                        "pool": self.pool,
                        "lp_balance": str(self._lp_token_balance),
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_curve_paper_trade_ethereum"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if not self._has_position:
            return []
        if self._lp_token_balance <= 0 and not self._lp_token_address:
            logger.warning("Teardown requested but neither LP balance nor address known, cannot close")
            return []
        return [self._create_close_intent()]
