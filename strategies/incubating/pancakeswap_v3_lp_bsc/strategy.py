"""
===============================================================================
PancakeSwap V3 LP Strategy on BSC
===============================================================================

Concentrated liquidity LP on PancakeSwap V3 (BSC chain): open LP positions
in WBNB/USDT pool when RSI indicates range-bound market, hold otherwise.

This is the FIRST yailoop strategy to test:
1. PancakeSwap V3 connector for LP operations
2. BSC chain (Anvil fork, gateway, wallet funding)
3. PancakeSwap V3's non-standard 2500 fee tier (tick_spacing=50)

Decision Logic:
  - RSI in [40-60] + no position  -> LP_OPEN (range-bound = fee capture)
  - force_action="open"           -> LP_OPEN (testing shortcut)
  - Otherwise                     -> HOLD

Chain: BSC
Protocol: PancakeSwap V3 (Uniswap V3 fork)
Pool: WBNB/USDT 0.25% fee tier

USAGE:
    almanak strat run -d strategies/incubating/pancakeswap_v3_lp_bsc --network anvil --once
===============================================================================
"""

import logging
from dataclasses import dataclass, field
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
from almanak.framework.utils.log_formatters import format_token_amount_human

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class PancakeSwapLPConfig:
    """Configuration for PancakeSwap V3 LP strategy on BSC."""

    # Pool config -- MUST be in sorted-by-address order (USDT 0x55 < WBNB 0xbb)
    # because the compiler sorts tokens and doesn't invert price ranges.
    # amount0=USDT (token0), amount1=WBNB (token1)
    pool: str = "USDT/WBNB/2500"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount0: Decimal = field(default_factory=lambda: Decimal("60"))
    amount1: Decimal = field(default_factory=lambda: Decimal("0.1"))
    fee_tier: int = 2500

    # RSI signal parameters
    rsi_period: int = 14
    rsi_timeframe: str = "4h"
    rsi_open_lower: int = 40
    rsi_open_upper: int = 60

    # Testing
    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.range_width_pct, str):
            self.range_width_pct = Decimal(self.range_width_pct)
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.fee_tier, str):
            self.fee_tier = int(self.fee_tier)
        if isinstance(self.rsi_period, str):
            self.rsi_period = int(self.rsi_period)
        if isinstance(self.rsi_open_lower, str):
            self.rsi_open_lower = int(self.rsi_open_lower)
        if isinstance(self.rsi_open_upper, str):
            self.rsi_open_upper = int(self.rsi_open_upper)

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "fee_tier": self.fee_tier,
            "rsi_period": self.rsi_period,
            "rsi_timeframe": self.rsi_timeframe,
            "rsi_open_lower": self.rsi_open_lower,
            "rsi_open_upper": self.rsi_open_upper,
            "force_action": self.force_action,
        }


# =============================================================================
# Strategy
# =============================================================================


@almanak_strategy(
    name="pancakeswap_v3_lp_bsc",
    description="PancakeSwap V3 concentrated LP on BSC -- first test of PancakeSwap V3 connector and BSC chain",
    version="1.0.0",
    author="YAInnick Loop (Iteration 10)",
    tags=["incubating", "lp", "pancakeswap-v3", "bsc", "rsi"],
    supported_chains=["bnb"],
    supported_protocols=["pancakeswap_v3"],
    intent_types=["LP_OPEN", "HOLD"],
)
class PancakeSwapV3LPStrategy(IntentStrategy[PancakeSwapLPConfig]):
    """PancakeSwap V3 LP strategy on BSC.

    Opens concentrated liquidity positions on PancakeSwap V3 when RSI
    signals a range-bound market. Uses 2500 fee tier (PancakeSwap default).

    Primary purpose: stress-test PancakeSwap V3 connector and BSC chain
    infrastructure which have never been exercised in yailoop.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse pool config: TOKEN0/TOKEN1/FEE
        # Pool is in sorted-by-address order: USDT/WBNB (token0=USDT, token1=WBNB)
        pool_parts = self.config.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "USDT"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "WBNB"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else self.config.fee_tier

        # Position tracking
        self._position_id: int | None = None

        logger.info(
            f"PancakeSwapV3LPStrategy initialized: "
            f"pool={self.config.pool}, "
            f"range_width={self.config.range_width_pct * 100}%, "
            f"amounts={self.config.amount0} {self.token0_symbol} + {self.config.amount1} {self.token1_symbol}"
        )

    # =========================================================================
    # Decision Logic
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to open LP or hold based on RSI."""
        try:
            # Get current price for range calculation
            current_price = self._get_current_price(market)

            # Handle forced actions (for testing)
            force = self.config.force_action.lower() if self.config.force_action else ""
            if force == "open":
                logger.info("Forced action: OPEN LP position")
                return self._create_open_intent(current_price)

            # If we already have a position, hold
            if self._position_id:
                return Intent.hold(
                    reason=f"Position #{self._position_id} exists -- monitoring"
                )

            # Get RSI signal -- use BNB (unwrapped) for Binance OHLCV data.
            # Binance doesn't recognize "WBNB" -- only "BNB". Same issue class
            # as WETH->ETH mapping but not handled for BSC native token.
            rsi_symbol = "BNB" if self.token1_symbol == "WBNB" else self.token1_symbol
            try:
                rsi_data = market.rsi(
                    rsi_symbol,
                    period=self.config.rsi_period,
                    timeframe=self.config.rsi_timeframe,
                )
                rsi = float(rsi_data.value)
                logger.info(f"RSI({self.config.rsi_period}, {self.config.rsi_timeframe}): {rsi:.2f}")
            except Exception as e:
                logger.warning(f"Could not calculate RSI: {e}. Holding.")
                return Intent.hold(reason=f"RSI unavailable: {e}")

            # Log state
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description=f"RSI={rsi:.2f}, position={'#' + str(self._position_id) if self._position_id else 'NONE'}",
                    strategy_id=self.strategy_id,
                    details={"rsi": rsi, "position_id": self._position_id},
                )
            )

            # Decision: open LP if RSI in range-bound zone
            if self.config.rsi_open_lower <= rsi <= self.config.rsi_open_upper:
                # Verify balances
                try:
                    bal0 = market.balance(self.token0_symbol)
                    bal1 = market.balance(self.token1_symbol)
                    if bal0.balance < self.config.amount0:
                        return Intent.hold(
                            reason=f"Insufficient {self.token0_symbol}: {bal0.balance} < {self.config.amount0}"
                        )
                    if bal1.balance < self.config.amount1:
                        return Intent.hold(
                            reason=f"Insufficient {self.token1_symbol}: {bal1.balance} < {self.config.amount1}"
                        )
                except (ValueError, KeyError, AttributeError):
                    logger.warning("Could not verify balances, proceeding anyway")

                logger.info(f"RSI={rsi:.2f} range-bound -- opening LP for fee capture")
                return self._create_open_intent(current_price)

            # Not in range-bound zone
            return Intent.hold(reason=f"RSI={rsi:.2f} not in [{self.config.rsi_open_lower}-{self.config.rsi_open_upper}] -- waiting")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # Intent Creation
    # =========================================================================

    def _get_current_price(self, market: MarketSnapshot) -> Decimal:
        """Get current pool price as token1 per token0 (WBNB per USDT).

        The compiler expects range_lower/range_upper in sorted token order:
        token0=USDT, token1=WBNB, so price = WBNB_per_USDT = 1/BNB_price_usd.
        """
        try:
            # token0=USDT (~$1), token1=WBNB (~$606)
            price0_usd = market.price(self.token0_symbol)  # USDT price
            price1_usd = market.price(self.token1_symbol)  # WBNB price
            if price0_usd == Decimal("0"):
                logger.warning(f"{self.token0_symbol} price is zero, using default")
                return Decimal("0.00165")  # ~1/606
            # token1_per_token0 = WBNB per USDT = WBNB_USD / USDT_USD inverted
            # = USDT_USD / WBNB_USD = 1/606
            # Wait: price = token1_amount / token0_amount = how many WBNB per 1 USDT
            # = (1 USDT in USD) / (1 WBNB in USD) = price0_usd / price1_usd
            return price0_usd / price1_usd
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}, using default")
            return Decimal("0.00165")  # ~1/606

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with concentrated range around current price.

        Uses price-based range (not tick-based) and lets the compiler handle
        tick conversion. This exercises the compiler's tick alignment logic
        for PancakeSwap V3's 2500 fee tier.
        """
        half_width = self.config.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {format_token_amount_human(self.config.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.config.amount1, self.token1_symbol)}, "
            f"price range [{range_lower:.4f} - {range_upper:.4f}]"
        )

        return Intent.lp_open(
            pool=self.config.pool,
            amount0=self.config.amount0,
            amount1=self.config.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="pancakeswap_v3",
        )

    # =========================================================================
    # Lifecycle Hooks
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track position state after execution."""
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = result.position_id if result else None
            liquidity = result.extracted_data.get("liquidity") if result else None

            if position_id:
                self._position_id = int(position_id)
                logger.info(
                    f"LP position opened: position_id={position_id}, liquidity={liquidity}"
                )
            else:
                logger.warning("LP_OPEN succeeded but no position_id extracted from receipt")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"PancakeSwap V3 LP opened on {self.config.pool}",
                    strategy_id=self.strategy_id,
                    details={
                        "pool": self.config.pool,
                        "fee_tier": self.fee_tier,
                        "position_id": position_id,
                        "liquidity": liquidity,
                    },
                )
            )

    # =========================================================================
    # Status & Teardown
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "pancakeswap_v3_lp_bsc",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "position_id": self._position_id,
            },
        }

    def supports_teardown(self) -> bool:
        """Indicate that this strategy supports safe teardown."""
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._position_id:
            estimated_value = self.config.amount0 * Decimal("600") + self.config.amount1
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"pancakeswap-lp-{self._position_id}-{self.chain}",
                    chain=self.chain,
                    protocol="pancakeswap_v3",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "pool": self.config.pool,
                        "fee_tier": self.fee_tier,
                        "nft_position_id": self._position_id,
                    },
                )
            )

        total_value = sum(p.value_usd for p in positions)
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            total_value_usd=total_value,
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all LP positions."""
        intents: list[Intent] = []
        if self._position_id:
            logger.info(f"Generating teardown intent for LP position #{self._position_id}")
            intents.append(
                Intent.lp_close(
                    position_id=str(self._position_id),
                    pool=self.config.pool,
                    collect_fees=True,
                    protocol="pancakeswap_v3",
                )
            )
        return intents
