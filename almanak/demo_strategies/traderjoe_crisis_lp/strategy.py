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
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.persistence import safe_int_list

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
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
    default_chain="avalanche",
    quote_asset="USD",
)
class TraderJoeCrisisLPStrategy(IntentStrategy):
    """TraderJoe V2 LP strategy tuned for crisis scenario backtesting.

    Wider ranges and lower rebalance thresholds vs the standard PnL LP
    strategy to better handle the extreme volatility of crisis periods.

    Configuration (config.json):
        pool: Pool descriptor "TokenX/TokenY/BinStep" (default: WAVAX/USDC/20)
        range_width_pct: Width of LP range around current price (default: 0.15)
        amount_x: Token X amount to LP (default: 0.1 WAVAX)
        amount_y: Token Y amount to LP (default: 2 USDC)
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
        self.amount_x = Decimal(str(self.get_config("amount_x", "0.1")))
        self.amount_y = Decimal(str(self.get_config("amount_y", "2")))
        self.num_bins = int(self.get_config("num_bins", 11))

        # Range and rebalance — tuned for crisis volatility
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.15")))
        self.rebalance_threshold_pct = Decimal(str(self.get_config("rebalance_threshold_pct", "0.06")))

        # Minimum total inventory (USD) required to (re)open a position.
        self.min_position_usd = Decimal(str(self.get_config("min_position_usd", "50")))

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

        # State: idle -> balance inventory to ~50/50, then open LP position
        if self._state == "idle":
            # After a drift-close the wallet holds a skewed inventory (mostly
            # one token). Swap the heavy side back toward 50/50 BEFORE reopening
            # so the new range isn't lopsided. Stay in "idle" after a swap so the
            # next tick re-evaluates (now balanced) and opens.
            try:
                token_y_price = market.price(self.token_y)
                bx = market.balance(self.token_x, price=base_price)
                by = market.balance(self.token_y, price=token_y_price)
                bal_x = Decimal(str(bx.balance))
                bal_y = Decimal(str(by.balance))
                x_usd = Decimal(str(bx.balance_usd))
                y_usd = Decimal(str(by.balance_usd))
            except (ValueError, KeyError):
                return Intent.hold(reason="Cannot check balances")

            total_usd = x_usd + y_usd
            if total_usd < self.min_position_usd:
                return Intent.hold(
                    reason=f"Total ${total_usd:.2f} below min_position_usd ${self.min_position_usd:.2f}"
                )

            swap_intent = self._rebalance_swap_intent(x_usd, y_usd, total_usd)
            if swap_intent is not None:
                # Stay in "idle": next tick re-evaluates with balanced inventory.
                return swap_intent

            # Balanced enough -> open, deploying ~95% of each side (buffer for
            # gas/rounding) instead of the fixed config amounts.
            self._state = "opening"
            self._entry_price = base_price

            amount_x = bal_x * Decimal("0.95")
            amount_y = bal_y * Decimal("0.95")

            half_width = base_price * self.range_width_pct / Decimal("2")
            range_lower = base_price - half_width
            range_upper = base_price + half_width

            logger.info(
                "Opening LP: %s %s + %s %s at price %.2f, range=[%.2f, %.2f]",
                amount_x,
                self.token_x,
                amount_y,
                self.token_y,
                base_price,
                range_lower,
                range_upper,
            )

            return Intent.lp_open(
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                amount0=amount_x,
                amount1=amount_y,
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

                close_kwargs: dict[str, Any] = {}
                if self._position_bin_ids:
                    close_kwargs["protocol_params"] = {
                        "bin_ids": list(self._position_bin_ids)
                    }
                return Intent.lp_close(
                    position_id="traderjoe_crisis_lp_0",
                    pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                    protocol="traderjoe_v2",
                    **close_kwargs,
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

    def _rebalance_swap_intent(
        self, x_usd: Decimal, y_usd: Decimal, total_usd: Decimal
    ) -> Intent | None:
        """Swap the heavy side toward a ~50/50 USD split before (re)opening.

        Returns a SWAP intent when inventory is skewed beyond a 10% tolerance
        band, else None (balanced enough to open as-is).
        """
        half_usd = total_usd / Decimal("2")
        tolerance_usd = total_usd * Decimal("0.10")
        if x_usd - half_usd > tolerance_usd:
            logger.info(
                "Rebalance swap: %s -> %s ($%.2f to reach ~50/50)",
                self.token_x,
                self.token_y,
                float(x_usd - half_usd),
            )
            return Intent.swap(
                from_token=self.token_x,
                to_token=self.token_y,
                amount_usd=x_usd - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        if y_usd - half_usd > tolerance_usd:
            logger.info(
                "Rebalance swap: %s -> %s ($%.2f to reach ~50/50)",
                self.token_y,
                self.token_x,
                float(y_usd - half_usd),
            )
            return Intent.swap(
                from_token=self.token_y,
                to_token=self.token_x,
                amount_usd=y_usd - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        return None

    def on_intent_executed(self, intent: Intent, success: bool, result: Any = None) -> None:
        """Handle intent execution results."""
        # Only LP_OPEN/LP_CLOSE drive the LP state machine. A rebalance SWAP
        # adjusts wallet inventory only and must leave _state untouched so the
        # next tick re-evaluates (now balanced) from "idle".
        intent_type = getattr(getattr(intent, "intent_type", None), "value", None)
        if intent_type not in ("LP_OPEN", "LP_CLOSE"):
            return

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

        # Terminal state is keyed on the INTENT, not the prior _state. A teardown
        # LP_CLOSE fires while _state=="active" (it bypasses the decide()-driven
        # "active"->"closing" transition), so keying the reset on _state=="closing"
        # left the position phantom-open after teardown and failed post-teardown
        # verification (ALM-2807 Layer 2).
        if intent_type == "LP_OPEN":
            self._state = "active"
            bin_ids = getattr(result, "bin_ids", None) if result is not None else None
            if not bin_ids and result is not None:
                extracted = getattr(result, "extracted_data", None) or {}
                if isinstance(extracted, dict):
                    bin_ids = extracted.get("bin_ids")
            self._position_bin_ids = [int(b) for b in bin_ids] if bin_ids else []
            logger.info(
                "LP opened successfully (%d bins). State -> active",
                len(self._position_bin_ids),
            )

        else:  # LP_CLOSE — decide()-driven rebalance close OR teardown close
            was_rebalance = self._state == "closing"
            self._state = "idle"
            self._entry_price = None
            self._position_bin_ids = []
            if was_rebalance:
                self._rebalance_count += 1
                logger.info("LP closed. Rebalance #%d. State -> idle", self._rebalance_count)
            else:
                logger.info("LP closed (teardown). State -> idle")

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
        # safe_int_list: drop malformed entries with a warning rather
        # than aborting load_persistent_state on bad data (VIB-3757).
        self._position_bin_ids = safe_int_list(
            state.get("position_bin_ids"), name="position_bin_ids"
        )
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
            deployment_id=getattr(self, "deployment_id", "demo_traderjoe_crisis_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if self._state not in ("active", "opening"):
            return []

        close_kwargs: dict[str, Any] = {}
        if self._position_bin_ids:
            close_kwargs["protocol_params"] = {"bin_ids": list(self._position_bin_ids)}
        return [
            Intent.lp_close(
                position_id="traderjoe_crisis_lp_0",
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                protocol="traderjoe_v2",
                **close_kwargs,
            )
        ]
