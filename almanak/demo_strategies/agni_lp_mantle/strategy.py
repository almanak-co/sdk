"""
===============================================================================
Agni Finance LP Strategy - Concentrated Liquidity on Mantle
===============================================================================

Demonstrates concentrated liquidity position management on Agni Finance,
the primary Uniswap V3 fork on Mantle. Opens a WMNT/WETH LP position
with a configurable price range and monitors it.

AGNI FINANCE:
-------------
Agni Finance is a Uniswap V3 fork on Mantle with identical concentrated
liquidity mechanics. The SDK routes "agni" protocol intents through the
Uniswap V3 connector via protocol aliases.

- Position Manager: 0x218bf598D1453383e2F4AA7b14fFB9BfB102D637
- Same tick math, fee tiers, and NFT positions as Uniswap V3
- Fee tiers: 100 (0.01%), 500 (0.05%), 3000 (0.3%), 10000 (1%)

USAGE:
------
    # Test on Anvil (local Mantle fork)
    almanak strat run -d strategies/demo/agni_lp_mantle --network anvil --once

    # Run once
    almanak strat run -d strategies/demo/agni_lp_mantle --once

===============================================================================
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@dataclass
class AgniLPConfig:
    """Configuration for Agni Finance LP strategy."""

    pool: str = "WMNT/WETH/500"
    range_width_pct: float = 0.2
    amount0: str = "10"
    amount1: str = "0.005"
    force_action: str = ""
    position_id: str | None = None


@almanak_strategy(
    name="demo_agni_lp_mantle",
    description="LP position management on Agni Finance (Mantle) - WMNT/WETH",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lp", "liquidity", "agni", "mantle", "uniswap-v3-fork"],
    supported_chains=["mantle"],
    supported_protocols=["agni"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="mantle",
)
class AgniLPStrategy(IntentStrategy[AgniLPConfig]):
    """
    Agni Finance LP strategy for Mantle.

    Opens a concentrated liquidity position on the WMNT/WETH pool on Agni Finance.
    Monitors the position and holds. Supports teardown for safe position closure.

    Configuration (config.json):
        pool: "WMNT/WETH/500" (token0/token1/fee_tier)
        range_width_pct: 0.2 (20% total range width = +/-10% from current price)
        amount0: "10" (WMNT amount)
        amount1: "0.005" (WETH amount)
        force_action: "open" or "close" for testing
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WMNT"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "WETH"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 500

        self.range_width_pct = Decimal(str(self.config.range_width_pct))
        self.amount0 = Decimal(str(self.config.amount0))
        self.amount1 = Decimal(str(self.config.amount1))
        self.force_action = str(self.config.force_action).lower() if self.config.force_action else ""

        self._current_position_id: str | None = self.config.position_id
        self._load_position_from_state()

        logger.info(
            f"AgniLPStrategy initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
            + (f", position_id={self._current_position_id}" if self._current_position_id else "")
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Open LP position if none exists, otherwise hold and monitor."""
        # Handle close and monitor before price lookup (close doesn't need price)
        if self.force_action == "close":
            if not self._current_position_id:
                return Intent.hold(reason="Close requested but no position_id")
            logger.info(f"Forced action: CLOSE LP position {self._current_position_id}")
            return self._create_close_intent(self._current_position_id)

        if self._current_position_id and self.force_action != "open":
            return Intent.hold(reason=f"Position {self._current_position_id} exists - monitoring")

        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            current_price = token0_price_usd / token1_price_usd
            logger.debug(f"Current price: {current_price:.6f} {self.token1_symbol}/{self.token0_symbol}")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}")
            return Intent.hold(reason=f"Price data unavailable: {e}")

        if self.force_action == "open":
            if self._current_position_id:
                return Intent.hold(
                    reason=f"Open requested but position {self._current_position_id} is already tracked"
                )
            logger.info("Forced action: OPEN LP position")
            return self._create_open_intent(current_price)

        # Check balances before opening
        try:
            token0_bal = market.balance(self.token0_symbol)
            token1_bal = market.balance(self.token1_symbol)
            bal0 = token0_bal.balance if hasattr(token0_bal, "balance") else token0_bal
            bal1 = token1_bal.balance if hasattr(token1_bal, "balance") else token1_bal
            if bal0 < self.amount0:
                return Intent.hold(reason=f"Insufficient {self.token0_symbol}: {bal0} < {self.amount0}")
            if bal1 < self.amount1:
                return Intent.hold(reason=f"Insufficient {self.token1_symbol}: {bal1} < {self.amount1}")
        except (ValueError, KeyError):
            logger.warning("Could not verify balances, proceeding anyway")

        logger.info("No position found - opening new LP position")
        return self._create_open_intent(current_price)

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with price range centered on current price."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"range [{range_lower:.6f} - {range_upper:.6f}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="agni",
        )

    def _create_close_intent(self, position_id: str) -> Intent:
        """Create LP_CLOSE intent to close an existing position."""
        logger.info(f"LP_CLOSE: position={position_id}")
        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="agni",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position ID after LP open, clear after LP close."""
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = result.position_id if result else None
            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"LP position opened: position_id={position_id}")
            else:
                logger.warning("LP opened but could not extract position ID")

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info(f"LP position closed: {self._current_position_id}")
            self._current_position_id = None

    def _load_position_from_state(self) -> None:
        """Load position ID from persistent state if available."""
        state = self.get_persistent_state()
        if state and "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
            logger.info(f"Loaded position ID from state: {self._current_position_id}")

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist position ID across restarts."""
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore position ID from state."""
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        if state and "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
            logger.info(f"Restored position ID: {self._current_position_id}")

    def get_status(self) -> dict[str, Any]:
        """Status for monitoring dashboards."""
        return {
            "strategy": "demo_agni_lp_mantle",
            "chain": self.chain,
            "pool": self.pool,
            "position_id": self._current_position_id,
            "amounts": {"token0": str(self.amount0), "token1": str(self.amount1)},
        }

    # -------------------------------------------------------------------------
    # TEARDOWN
    # -------------------------------------------------------------------------

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Return open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._current_position_id:
            try:
                snapshot = self.create_market_snapshot()
                t0_usd = snapshot.price(self.token0_symbol)
                t1_usd = snapshot.price(self.token1_symbol)
            except Exception:  # noqa: BLE001
                t0_usd = Decimal("0")
                t1_usd = Decimal("0")

            estimated_value = self.amount0 * t0_usd + self.amount1 * t1_usd
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"agni-lp-{self._current_position_id}",
                    chain=self.chain,
                    protocol="agni",
                    value_usd=estimated_value,
                    details={
                        "nft_id": self._current_position_id,
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            total_value_usd=sum(p.value_usd for p in positions),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all LP positions."""
        intents: list[Intent] = []

        if self._current_position_id:
            logger.info(f"Teardown: closing LP position {self._current_position_id} (mode={mode.value})")
            intents.append(
                Intent.lp_close(
                    position_id=self._current_position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="agni",
                )
            )

        return intents
