"""
===============================================================================
DEMO: Aerodrome PnL Backtest -- RSI-Based LP on Base
===============================================================================

PnL backtesting vehicle for exercising the PnL backtester on Base with an
Aerodrome LP strategy. Opens LP positions when RSI signals oversold, closes
when overbought, and holds otherwise.

PURPOSE:
--------
1. Validate PnL backtesting on Base with Aerodrome (first Aerodrome PnL backtest):
   - CoinGecko WETH token pricing resolves correctly on Base
   - Aerodrome fee model (0.3% volatile pool) applied accurately
   - LP PnL tracking with fungible LP tokens (non-NFT, unlike Uniswap V3)
   - Equity curve generation with Base chain tokens
2. Exercise Aerodrome LP_OPEN / LP_CLOSE / SWAP in the PnL engine.

USAGE:
------
    # PnL backtest over 7 days
    almanak strat backtest pnl \
        -s demo_aerodrome_pnl_lp \
        --chain base \
        --start 2024-06-01 --end 2024-06-08 \
        --tokens "WETH,USDC" \
        --initial-capital 5000 \
        --chart --report

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/aerodrome_pnl_lp \
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. If idle and RSI < oversold threshold -> open LP in WETH/USDC volatile pool
  2. If LP open and RSI > overbought threshold -> close LP
  3. If LP open and RSI in neutral zone -> hold
  4. Otherwise -> hold
===============================================================================
"""

import logging
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


@almanak_strategy(
    name="demo_aerodrome_pnl_lp",
    description="PnL backtest demo -- Aerodrome RSI-based LP on Base",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "pnl-backtest", "lp", "aerodrome", "base", "backtesting"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="base",
)
class AerodromePnLLPStrategy(IntentStrategy):
    """Aerodrome LP strategy for PnL backtesting on Base.

    Configuration (config.json):
        pool: Pool descriptor "Token0/Token1" (default: WETH/USDC)
        stable: Pool type - false for volatile (default: false)
        amount0: Token0 amount to LP (default: 0.001 WETH)
        amount1: Token1 amount to LP (default: 3 USDC)
        rsi_period: RSI calculation window (default: 14)
        rsi_overbought: RSI threshold to close LP (default: 70)
        rsi_oversold: RSI threshold to open LP (default: 30)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool configuration
        pool = str(self.get_config("pool", "WETH/USDC"))
        parts = pool.split("/")
        self.token0 = parts[0] if len(parts) > 0 else "WETH"
        self.token1 = parts[1] if len(parts) > 1 else "USDC"
        self.stable = bool(self.get_config("stable", False))

        # LP amounts
        self.amount0 = Decimal(str(self.get_config("amount0", "0.001")))
        self.amount1 = Decimal(str(self.get_config("amount1", "3")))

        # RSI parameters
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_overbought = int(self.get_config("rsi_overbought", 70))
        self.rsi_oversold = int(self.get_config("rsi_oversold", 30))
        if self.rsi_period <= 0:
            raise ValueError("rsi_period must be greater than 0")

        # Internal state
        self._state = "idle"
        self._entry_price: Decimal | None = None
        self._tick_count = 0
        self._lp_position_id: str | None = None

        logger.info(
            f"AerodromePnLLP initialized: pool={self.token0}/{self.token1}, "
            f"stable={self.stable}, amounts={self.amount0} {self.token0} + "
            f"{self.amount1} {self.token1}, RSI({self.rsi_period}) "
            f"buy<{self.rsi_oversold} sell>{self.rsi_overbought}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-based LP decision for PnL backtesting."""
        self._tick_count += 1

        try:
            base_price = market.price(self.token0)
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get {self.token0} price: {e}")
            return Intent.hold(reason=f"Price data unavailable for {self.token0}: {e}")

        # Simulate RSI using tick count (PnL backtester replays prices,
        # actual RSI from indicators not available in backtest mode).
        # Use a simple heuristic: open on first tick, hold, close after
        # several ticks to ensure both LP_OPEN and LP_CLOSE are exercised.
        rsi = self._estimate_rsi()

        # State: idle -> open LP when RSI signals oversold
        if self._state == "idle":
            if rsi <= self.rsi_oversold:
                self._state = "opening"
                self._entry_price = base_price
                logger.info(
                    f"RSI={rsi} <= {self.rsi_oversold}: Opening LP "
                    f"{self.amount0} {self.token0} + {self.amount1} {self.token1} "
                    f"at price {base_price:.2f}"
                )
                pool_type = "stable" if self.stable else "volatile"
                return Intent.lp_open(
                    pool=f"{self.token0}/{self.token1}/{pool_type}",
                    amount0=self.amount0,
                    amount1=self.amount1,
                    range_lower=Decimal("1"),
                    range_upper=Decimal("1000000"),
                    protocol="aerodrome",
                )
            return Intent.hold(reason=f"RSI={rsi}, waiting for oversold (<={self.rsi_oversold})")

        # State: active -> close LP when RSI signals overbought
        if self._state == "active":
            if rsi >= self.rsi_overbought:
                self._state = "closing"
                entry_price = (
                    f"{self._entry_price:.2f}" if self._entry_price is not None else "unknown"
                )
                logger.info(
                    f"RSI={rsi} >= {self.rsi_overbought}: Closing LP "
                    f"(entry={entry_price}, current={base_price:.2f})"
                )
                pool_type = "stable" if self.stable else "volatile"
                # Use stored position_id from LP_OPEN result (PnL backtester
                # auto-generates IDs like LP_aerodrome_WETH_USDC_<timestamp>)
                close_id = self._lp_position_id or f"{self.token0}/{self.token1}/{pool_type}"
                return Intent.lp_close(
                    position_id=close_id,
                    pool=f"{self.token0}/{self.token1}/{pool_type}",
                    protocol="aerodrome",
                )
            return Intent.hold(
                reason=f"LP active, RSI={rsi} in neutral zone "
                f"({self.rsi_oversold}-{self.rsi_overbought})"
            )

        # Safety: auto-advance stuck transitional states. The PnL backtester
        # may not call on_intent_executed reliably, so opening/closing would
        # otherwise stick forever.
        if self._state in ("opening", "closing"):
            previous = self._state
            if self._state == "opening":
                self._state = "active"
            else:
                self._state = "idle"
                self._entry_price = None
            logger.warning(f"Stuck in '{previous}', auto-advancing to '{self._state}'")

        return Intent.hold(reason=f"Holding (state={self._state}, price={base_price:.2f})")

    def _estimate_rsi(self) -> int:
        """Estimate RSI from tick count for deterministic backtest behavior.

        Returns a synthetic RSI value that cycles through oversold -> neutral
        -> overbought to ensure both LP_OPEN and LP_CLOSE are exercised
        during the backtest window.
        """
        cycle_length = self.rsi_period * 3
        position_in_cycle = (self._tick_count - 1) % cycle_length

        if position_in_cycle < self.rsi_period:
            # First third of cycle: oversold (triggers LP_OPEN)
            return 20
        elif position_in_cycle < self.rsi_period * 2:
            # Middle third: neutral (hold)
            return 50
        else:
            # Last third: overbought (triggers LP_CLOSE)
            return 80

    def on_intent_executed(self, intent: Intent, success: bool, result: Any = None) -> None:
        """Handle intent execution results."""
        if not success:
            if self._state == "opening":
                self._state = "idle"
                self._entry_price = None
                logger.warning("LP_OPEN failed, reverting to idle")
            elif self._state == "closing":
                self._state = "active"
                logger.warning("LP_CLOSE failed, reverting to active")
            return

        if self._state == "opening":
            self._state = "active"
            # Capture the position_id from the PnL backtester for LP_CLOSE
            if result and hasattr(result, "position_id"):
                self._lp_position_id = result.position_id
            logger.info("LP opened successfully. State -> active")
        elif self._state == "closing":
            self._state = "idle"
            self._entry_price = None
            self._lp_position_id = None
            logger.info("LP closed. State -> idle")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aerodrome_pnl_lp",
            "chain": self.chain,
            "state": self._state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "tick_count": self._tick_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "tick_count": self._tick_count,
            "lp_position_id": self._lp_position_id,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        valid_states = {"idle", "opening", "active", "closing"}
        restored_state = str(state.get("state", "idle"))
        self._state = restored_state if restored_state in valid_states else "idle"

        ep = state.get("entry_price")
        self._entry_price = Decimal(ep) if ep else None

        try:
            self._tick_count = max(0, int(state.get("tick_count", 0)))
        except (TypeError, ValueError):
            self._tick_count = 0

        self._lp_position_id = state.get("lp_position_id")

    # =========================================================================
    # TEARDOWN
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
        if self._state in ("active", "opening", "closing"):
            pool_type = "stable" if self.stable else "volatile"
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"{self.token0}/{self.token1}/{pool_type}",
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=self.amount1 * Decimal("2"),
                    details={
                        "token0": self.token0,
                        "token1": self.token1,
                        "stable": self.stable,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_aerodrome_pnl_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if self._state not in ("active", "opening", "closing"):
            return []

        pool_type = "stable" if self.stable else "volatile"
        close_id = self._lp_position_id or f"{self.token0}/{self.token1}/{pool_type}"
        return [
            Intent.lp_close(
                position_id=close_id,
                pool=f"{self.token0}/{self.token1}/{pool_type}",
                protocol="aerodrome",
            )
        ]
