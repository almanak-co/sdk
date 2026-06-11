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
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.persistence import safe_int_list

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
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"],
    default_chain="avalanche",
    quote_asset="USD",
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

        # Minimum total inventory (USD) required to (re)open a position.
        self.min_position_usd = Decimal(str(self.get_config("min_position_usd", "50")))

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

        # State: idle -> balance inventory to ~50/50, then open LP position
        if self._state == "idle":
            # After a drift-close the wallet holds a skewed inventory (mostly one
            # token), so swap the heavy side back toward 50/50 BEFORE reopening --
            # otherwise the new range opens lopsided. We stay in 'idle' while
            # swapping and only transition to 'opening' once balanced.
            try:
                token_y_price = market.price(self.token_y)
                bx = market.balance(self.token_x, price=base_price)
                by = market.balance(self.token_y, price=token_y_price)
                balance_x = Decimal(str(bx.balance))
                balance_y = Decimal(str(by.balance))
                usd_x = Decimal(str(bx.balance_usd))
                usd_y = Decimal(str(by.balance_usd))
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not read balances: {e}")
                return Intent.hold(reason=f"Cannot check balances: {e}")

            total_usd = usd_x + usd_y
            if total_usd < self.min_position_usd:
                return Intent.hold(
                    reason=f"Total ${total_usd:.2f} below min_position_usd ${self.min_position_usd:.2f}"
                )

            # Skewed beyond tolerance -> swap toward 50/50 and STAY in idle.
            swap_intent = self._rebalance_swap_intent(usd_x, usd_y, total_usd)
            if swap_intent is not None:
                return swap_intent

            # Balanced enough -> open LP deploying ~95% of each side.
            self._state = "opening"
            self._entry_price = base_price

            amount_x = balance_x * Decimal("0.95")
            amount_y = balance_y * Decimal("0.95")

            logger.info(
                f"Opening LP: {amount_x} {self.token_x} + {amount_y} {self.token_y} "
                f"at price {base_price:.2f}, range_width={self.range_width_pct}"
            )

            # Calculate range bounds from current price and width
            half_width = base_price * self.range_width_pct / Decimal("2")
            range_lower = base_price - half_width
            range_upper = base_price + half_width

            return Intent.lp_open(
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                amount0=amount_x,
                amount1=amount_y,
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

                close_kwargs: dict[str, Any] = {}
                if self._position_bin_ids:
                    close_kwargs["protocol_params"] = {"bin_ids": list(self._position_bin_ids)}
                return Intent.lp_close(
                    position_id="traderjoe_pnl_lp_0",
                    pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                    protocol="traderjoe_v2",
                    **close_kwargs,
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

    def _rebalance_swap_intent(
        self, usd_x: Decimal, usd_y: Decimal, total_usd: Decimal
    ) -> Intent | None:
        """Swap the heavy side toward a ~50/50 USD split before (re)opening.

        Returns a SWAP intent when inventory is skewed beyond a 10% tolerance
        band, else None (balanced enough to open as-is).
        """
        half_usd = total_usd / Decimal("2")
        tolerance_usd = total_usd * Decimal("0.10")
        if usd_x - half_usd > tolerance_usd:
            logger.info(
                f"Rebalance swap: {self.token_x} -> {self.token_y} "
                f"(${usd_x - half_usd:.2f} to reach ~50/50)"
            )
            return Intent.swap(
                from_token=self.token_x,
                to_token=self.token_y,
                amount_usd=usd_x - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        if usd_y - half_usd > tolerance_usd:
            logger.info(
                f"Rebalance swap: {self.token_y} -> {self.token_x} "
                f"(${usd_y - half_usd:.2f} to reach ~50/50)"
            )
            return Intent.swap(
                from_token=self.token_y,
                to_token=self.token_x,
                amount_usd=usd_y - half_usd,
                max_slippage=Decimal("0.01"),
                protocol="traderjoe_v2",
            )
        return None

    def on_intent_executed(self, intent: Intent, success: bool, result: Any = None) -> None:
        """Handle intent execution results."""
        # Rebalance SWAPs run while idle and must not advance the LP state
        # machine -- only LP_OPEN / LP_CLOSE drive opening/closing transitions.
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

        if self._state == "opening":
            self._state = "active"
            # ResultEnricher stores protocol-specific fields in extracted_data.
            # Some adapters also project them onto the result directly, but we
            # cannot rely on that for TraderJoe V2 bin IDs.
            bin_ids = None
            if result is not None:
                bin_ids = getattr(result, "bin_ids", None)
                if not bin_ids:
                    extracted = getattr(result, "extracted_data", None) or {}
                    bin_ids = extracted.get("bin_ids")
            if bin_ids:
                self._position_bin_ids = list(bin_ids)
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
            deployment_id=getattr(self, "deployment_id", "demo_traderjoe_pnl_lp"),
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
                position_id="traderjoe_pnl_lp_0",
                pool=f"{self.token_x}/{self.token_y}/{self.bin_step}",
                protocol="traderjoe_v2",
                **close_kwargs,
            )
        ]
