"""
===============================================================================
TraderJoe V2 LP Crisis Scenario Backtest — Range Rebalancing Under Stress
===============================================================================

Stress-tests TraderJoe V2 LP range rebalancing under historical crisis
conditions on Avalanche. First LP strategy used with crisis scenario
backtesting -- all 3 prior crisis backtests were swap/lending strategies.

CRISIS PARAMETERS:
- Wider LP range (15% vs 10%) to absorb larger price swings
- Lower rebalance threshold (6% vs 8%) for faster response to vol spikes
- Smaller position sizes to reduce impermanent loss exposure

USAGE:
------
    # Run against predefined crisis scenario
    almanak strat backtest scenario \
        -s demo_traderjoe_crisis_lp \
        --scenario ftx_collapse \
        --chain avalanche \
        --tokens WAVAX,USDC \
        --initial-capital 10000

    # Run against all 3 predefined scenarios
    almanak strat backtest scenario \
        -s demo_traderjoe_crisis_lp \
        --scenario black_thursday \
        --chain avalanche --tokens WAVAX,USDC

    almanak strat backtest scenario \
        -s demo_traderjoe_crisis_lp \
        --scenario terra_collapse \
        --chain avalanche --tokens WAVAX,USDC

    almanak strat backtest scenario \
        -s demo_traderjoe_crisis_lp \
        --scenario ftx_collapse \
        --chain avalanche --tokens WAVAX,USDC

    # With normal period comparison
    almanak strat backtest scenario \
        -s demo_traderjoe_crisis_lp \
        --scenario terra_collapse \
        --chain avalanche --tokens WAVAX,USDC \
        --compare-normal

KEY METRICS TO WATCH:
- Max drawdown during crisis (LP IL amplified by concentrated liquidity)
- Rebalance count (how often price escapes the range)
- Recovery time after crisis peak drawdown

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
    name="demo_traderjoe_crisis_lp",
    description="Crisis scenario stress test -- TraderJoe V2 LP rebalancing on Avalanche",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "crisis", "scenario-backtest", "lp", "traderjoe", "avalanche", "backtesting"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="avalanche",
)
class TraderJoeCrisisLPStrategy(IntentStrategy):
    """TraderJoe V2 LP strategy tuned for crisis scenario backtesting.

    Wider ranges and lower rebalance thresholds vs the standard PnL LP
    strategy to better handle the extreme volatility of crisis periods.

    Configuration (config.json):
        pool: Pool descriptor "TokenX/TokenY/BinStep" (default: WAVAX/USDC/20)
        range_width_pct: Width of LP range around current price (default: 0.15)
        amount_x: Token X amount to LP (default: 0.5 WAVAX)
        amount_y: Token Y amount to LP (default: 10 USDC)
        num_bins: Number of bins to distribute across (default: 11)
        rebalance_threshold_pct: Price move % triggering rebalance (default: 0.06)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
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

        # Range and rebalance — tuned for crisis volatility
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.15")))
        self.rebalance_threshold_pct = Decimal(str(self.get_config("rebalance_threshold_pct", "0.06")))

        # Internal state
        self._state = "idle"
        self._entry_price: Decimal | None = None
        self._position_bin_ids: list[int] = []
        self._rebalance_count = 0

        logger.info(
            "TraderJoeCrisisLP initialized: pool=%s/%s/%d, "
            "amounts=%s %s + %s %s, bins=%d, rebalance_threshold=%s",
            self.token_x,
            self.token_y,
            self.bin_step,
            self.amount_x,
            self.token_x,
            self.amount_y,
            self.token_y,
            self.num_bins,
            self.rebalance_threshold_pct,
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """LP range rebalancing with crisis-tuned parameters."""
        try:
            base_price = market.price(self.token_x)
        except (ValueError, KeyError) as e:
            logger.warning("Could not get %s price: %s", self.token_x, e)
            return Intent.hold(reason=f"Price data unavailable for {self.token_x}: {e}")

        # State: idle -> open LP position
        if self._state == "idle":
            self._state = "opening"
            self._entry_price = base_price

            half_width = base_price * self.range_width_pct / Decimal("2")
            range_lower = base_price - half_width
            range_upper = base_price + half_width

            logger.info(
                "Opening LP: %s %s + %s %s at price %.2f, range=[%.2f, %.2f]",
                self.amount_x,
                self.token_x,
                self.amount_y,
                self.token_y,
                base_price,
                range_lower,
                range_upper,
            )

            return Intent.lp_open(
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                amount0=self.amount_x,
                amount1=self.amount_y,
                range_lower=range_lower,
                range_upper=range_upper,
                protocol="traderjoe_v2",
                protocol_params={"bin_range": self.num_bins},
            )

        # State: active -> check rebalance
        if self._state == "active" and self._entry_price is not None:
            price_change_pct = abs(base_price - self._entry_price) / self._entry_price

            if price_change_pct >= self.rebalance_threshold_pct:
                self._state = "closing"
                logger.info(
                    "Price moved %.1f%% from entry (%.2f -> %.2f). Closing LP for rebalance #%d.",
                    float(price_change_pct * 100),
                    float(self._entry_price),
                    float(base_price),
                    self._rebalance_count + 1,
                )

                return Intent.lp_close(
                    position_id="traderjoe_crisis_lp_0",
                    pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                    protocol="traderjoe_v2",
                )

            return Intent.hold(
                reason=f"LP active, price change {price_change_pct:.1%} < {self.rebalance_threshold_pct:.0%}"
            )

        # Auto-advance stuck transitional states (PnL backtester doesn't call on_intent_executed)
        if self._state in ("opening", "closing"):
            previous = self._state
            if self._state == "opening":
                self._state = "active"
            else:
                self._state = "idle"
                self._entry_price = None
                self._position_bin_ids = []
                self._rebalance_count += 1
            logger.warning("Stuck in '%s', auto-advancing to '%s'", previous, self._state)

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
            logger.info("LP opened successfully. State -> active")

        elif self._state == "closing":
            self._state = "idle"
            self._entry_price = None
            self._position_bin_ids = []
            self._rebalance_count += 1
            logger.info("LP closed. Rebalance #%d. State -> idle", self._rebalance_count)

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_traderjoe_crisis_lp",
            "chain": self.chain,
            "state": self._state,
            "entry_price": str(self._entry_price) if self._entry_price else None,
            "rebalance_count": self._rebalance_count,
            "range_width_pct": str(self.range_width_pct),
            "rebalance_threshold_pct": str(self.rebalance_threshold_pct),
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
                    position_id="traderjoe_crisis_lp_0",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=self.amount_y * Decimal("2"),
                    details={
                        "token_x": self.token_x,
                        "token_y": self.token_y,
                        "bin_step": self.bin_step,
                        "rebalance_count": self._rebalance_count,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_traderjoe_crisis_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if self._state not in ("active", "opening"):
            return []

        return [
            Intent.lp_close(
                position_id="traderjoe_crisis_lp_0",
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                protocol="traderjoe_v2",
            )
        ]
