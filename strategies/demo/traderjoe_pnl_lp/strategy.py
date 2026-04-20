"""
===============================================================================
DEMO: TraderJoe V2 PnL Backtest -- LP Range Rebalancing on Avalanche
===============================================================================

PnL backtesting vehicle for exercising the PnL backtester on Avalanche
with a TraderJoe V2 Liquidity Book LP strategy. Opens LP positions within
a price range, rebalances when price moves beyond range, and holds otherwise.

PURPOSE:
--------
1. Validate PnL backtesting on Avalanche (first Avalanche PnL backtest):
   - CoinGecko WAVAX token pricing resolves correctly
   - TraderJoe V2 Liquidity Book fee model applied accurately
   - LP PnL tracking with bin-based positions
   - Equity curve generation with WAVAX denominations
2. Exercise TraderJoe V2 LP_OPEN / LP_CLOSE in the PnL engine.

USAGE:
------
    # PnL backtest over 6 months
    almanak strat backtest pnl \\
        -s demo_traderjoe_pnl_lp \\
        --chain avalanche \\
        --start 2024-01-01 --end 2024-06-01 \\
        --tokens "WAVAX,USDC" \\
        --initial-capital 10000 \\
        --chart --report

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/traderjoe_pnl_lp \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. If idle and have balance -> open LP in WAVAX/USDC range
  2. If LP open and price within range -> hold
  3. If LP open and price moved >8% from entry -> close and re-open at new range
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
    name="demo_traderjoe_pnl_lp",
    description="PnL backtest demo -- TraderJoe V2 LP range rebalancing on Avalanche",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "pnl-backtest", "lp", "traderjoe", "avalanche", "backtesting"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="avalanche",
)
class TraderJoePnLLPStrategy(IntentStrategy):
    """TraderJoe V2 LP strategy for PnL backtesting on Avalanche.

    Configuration (config.json):
        pool: Pool descriptor "TokenX/TokenY/BinStep" (default: WAVAX/USDC/20)
        range_width_pct: Width of LP range around current price (default: 0.10)
        amount_x: Token X amount to LP (default: 0.5 WAVAX)
        amount_y: Token Y amount to LP (default: 10 USDC)
        num_bins: Number of bins to distribute across (default: 11)
        rebalance_threshold_pct: Price move % triggering rebalance (default: 0.08)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Pool configuration
        pool = str(self.get_config("pool", "WAVAX/USDC/20"))
        parts = pool.split("/")
        self.token_x = parts[0] if len(parts) > 0 else "WAVAX"
        self.token_y = parts[1] if len(parts) > 1 else "USDC"
        self.bin_step = int(parts[2]) if len(parts) > 2 else 20

        # LP amounts
        self.amount_x = Decimal(str(self.get_config("amount_x", "0.5")))
        self.amount_y = Decimal(str(self.get_config("amount_y", "10")))
        self.num_bins = int(self.get_config("num_bins", 11))

        # Range and rebalance
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.10")))
        self.rebalance_threshold_pct = Decimal(str(self.get_config("rebalance_threshold_pct", "0.08")))

        # Internal state
        self._state = "idle"
        self._entry_price: Decimal | None = None
        self._position_bin_ids: list[int] = []
        self._rebalance_count = 0

        logger.info(
            f"TraderJoePnLLP initialized: pool={self.token_x}/{self.token_y}/{self.bin_step}, "
            f"amounts={self.amount_x} {self.token_x} + {self.amount_y} {self.token_y}, "
            f"bins={self.num_bins}, rebalance_threshold={self.rebalance_threshold_pct}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """LP range rebalancing decision for PnL backtesting."""
        try:
            base_price = market.price(self.token_x)
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get {self.token_x} price: {e}")
            return Intent.hold(reason=f"Price data unavailable for {self.token_x}: {e}")

        # State: idle -> open LP position
        if self._state == "idle":
            self._state = "opening"
            self._entry_price = base_price

            logger.info(
                f"Opening LP: {self.amount_x} {self.token_x} + {self.amount_y} {self.token_y} "
                f"at price {base_price:.2f}, range_width={self.range_width_pct}"
            )

            # Calculate range bounds from current price and width
            half_width = base_price * self.range_width_pct / Decimal("2")
            range_lower = base_price - half_width
            range_upper = base_price + half_width

            return Intent.lp_open(
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                amount0=self.amount_x,
                amount1=self.amount_y,
                range_lower=range_lower,
                range_upper=range_upper,
                protocol="traderjoe_v2",
            )

        # State: active -> check if rebalance needed
        if self._state == "active" and self._entry_price is not None:
            price_change_pct = abs(base_price - self._entry_price) / self._entry_price

            if price_change_pct >= self.rebalance_threshold_pct:
                self._state = "closing"
                logger.info(
                    f"Price moved {price_change_pct:.1%} from entry ({self._entry_price:.2f} -> {base_price:.2f}). "
                    f"Closing LP for rebalance."
                )

                return Intent.lp_close(
                    position_id="traderjoe_pnl_lp_0",
                    pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                    protocol="traderjoe_v2",
                )

            return Intent.hold(
                reason=f"LP active, price change {price_change_pct:.1%} < {self.rebalance_threshold_pct:.0%}"
            )

        # Safety: revert stuck transitional states (PnL backtester does not call
        # on_intent_executed, so opening/closing would otherwise stick forever)
        if self._state in ("opening", "closing"):
            previous = self._state
            if self._state == "opening":
                self._state = "active"
            else:
                self._state = "idle"
                self._entry_price = None
                self._position_bin_ids = []
                self._rebalance_count += 1
            logger.warning(f"Stuck in '{previous}', auto-advancing to '{self._state}'")

        return Intent.hold(reason=f"Holding (state={self._state}, price={base_price:.2f})")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any = None) -> None:
        """Handle intent execution results."""
        if not success:
            if self._state == "opening":
                self._state = "idle"
                self._entry_price = None
                self._position_bin_ids = []
                logger.warning("LP_OPEN failed, reverting to idle")
            elif self._state == "closing":
                self._state = "active"
                logger.warning("LP_CLOSE failed, reverting to active")
            return

        if self._state == "opening":
            self._state = "active"
            if result and hasattr(result, "bin_ids"):
                self._position_bin_ids = result.bin_ids
            logger.info(f"LP opened successfully. State -> active")

        elif self._state == "closing":
            self._state = "idle"
            self._entry_price = None
            self._position_bin_ids = []
            self._rebalance_count += 1
            logger.info(f"LP closed. Rebalance #{self._rebalance_count}. State -> idle")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_traderjoe_pnl_lp",
            "chain": self.chain,
            "state": self._state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "rebalance_count": self._rebalance_count,
            "bin_ids": self._position_bin_ids,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "position_bin_ids": self._position_bin_ids,
            "rebalance_count": self._rebalance_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._state = state.get("state", "idle")
        ep = state.get("entry_price")
        self._entry_price = Decimal(ep) if ep else None
        self._position_bin_ids = state.get("position_bin_ids", [])
        self._rebalance_count = state.get("rebalance_count", 0)

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
        if self._state in ("active", "opening"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="traderjoe_pnl_lp_0",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=self.amount_y * Decimal("2"),
                    details={
                        "token_x": self.token_x,
                        "token_y": self.token_y,
                        "bin_step": self.bin_step,
                        "bin_ids": self._position_bin_ids,
                        "rebalance_count": self._rebalance_count,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_traderjoe_pnl_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if self._state not in ("active", "opening"):
            return []

        return [
            Intent.lp_close(
                position_id="traderjoe_pnl_lp_0",
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                protocol="traderjoe_v2",
            )
        ]
