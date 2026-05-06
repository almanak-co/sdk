"""
SushiSwap V3 Swap + LP on Avalanche
=====================================

Kitchen Loop iteration 66 strategy. First SushiSwap V3 test on Avalanche.

Lifecycle steps (one per iteration):
1. SWAP: Buy WAVAX with USDC via SushiSwap V3 on Avalanche
2. LP_OPEN: Open WAVAX/USDC concentrated liquidity position
3. HOLD: Lifecycle complete

Coverage gaps filled:
- First SushiSwap V3 on Avalanche (tested on Arbitrum iters 7,32 and Base iter 50)
- Tests V1 router interface (SWAP_ROUTER_V1_PROTOCOLS) on 3rd chain
- Extends Avalanche protocol diversity (only TraderJoe V2, Uniswap V3, Aave V3, BENQI so far)

SushiSwap V3 deployment on Avalanche:
- SwapRouter: 0x717b7948AA264DeCf4D780aa6914482e5F46Da3e
- NonfungiblePositionManager: 0x18350b048AB366ed601fFDbC669110Ecb36016f3
- QuoterV2: 0xb1E835Dc2785b52265711e17fCCb0fd018226a6e
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="sushiswap_v3_avalanche",
    description="SushiSwap V3 swap + LP lifecycle on Avalanche (first SushiSwap V3 test on Avalanche)",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "swap", "lp", "sushiswap-v3", "avalanche"],
    supported_chains=["avalanche"],
    supported_protocols=["sushiswap_v3"],
    intent_types=["SWAP", "LP_OPEN", "HOLD"],
)
class SushiSwapV3AvalancheStrategy(IntentStrategy):
    """SushiSwap V3 swap + LP lifecycle on Avalanche.

    Tests SwapIntent and LPOpenIntent through SushiSwap V3 on a chain where
    SushiSwap V3 has never been exercised in the kitchen loop.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Swap config
        self.swap_from = self.get_config("swap_from", "USDC")
        self.swap_to = self.get_config("swap_to", "WAVAX")
        self.swap_amount_usd = Decimal(str(self.get_config("swap_amount_usd", "50")))

        # LP config
        self.pool = self.get_config("pool", "WAVAX/USDC/3000")
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 3000
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.30")))
        self.lp_amount0 = Decimal(str(self.get_config("lp_amount0", "1")))
        self.lp_amount1 = Decimal(str(self.get_config("lp_amount1", "50")))
        self.force_lp_only = self.get_config("force_lp_only", False)

        # State machine: idle -> swapping -> swapped -> opening_lp -> complete
        self._loop_state = "swapped" if self.force_lp_only else "idle"
        self._previous_stable_state = "idle"
        self._position_id: str | None = None

        logger.info(
            f"SushiSwapV3AvalancheStrategy initialized: chain={self.chain}, "
            f"swap={self.swap_from}->{self.swap_to} ${self.swap_amount_usd}, "
            f"pool={self.pool}, range_width={self.range_width_pct * 100:.0f}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a decision based on lifecycle state."""
        try:
            # Get prices
            try:
                wavax_price = market.price("WAVAX")
                usdc_price = market.price("USDC")
                logger.info(f"Prices: WAVAX=${wavax_price:.2f}, USDC=${usdc_price:.2f}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get prices: {e}")
                return Intent.hold(reason="Price data unavailable")

            # State: IDLE -> SWAP (buy WAVAX with USDC)
            if self._loop_state == "idle":
                logger.info("State: IDLE -> Swapping USDC to WAVAX via SushiSwap V3")
                self._previous_stable_state = self._loop_state
                self._loop_state = "swapping"
                return self._create_swap_intent()

            # State: SWAPPED -> LP_OPEN (open WAVAX/USDC LP)
            if self._loop_state == "swapped":
                logger.info("State: SWAPPED -> Opening WAVAX/USDC LP position")
                self._previous_stable_state = self._loop_state
                self._loop_state = "opening_lp"
                current_price = wavax_price / usdc_price
                return self._create_lp_open_intent(current_price)

            # State: COMPLETE -> HOLD
            if self._loop_state == "complete":
                return Intent.hold(
                    reason="Full lifecycle complete: swap -> LP open"
                    + (f" (position_id={self._position_id})" if self._position_id else "")
                )

            # Stuck in transitional state -- revert to last stable state
            if self._loop_state in ("swapping", "opening_lp"):
                revert_to = self._previous_stable_state
                logger.warning(
                    f"Stuck in transitional state '{self._loop_state}' -- reverting to '{revert_to}'"
                )
                self._loop_state = revert_to

            return Intent.hold(reason=f"Waiting for state transition (current: {self._loop_state})")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    def _create_swap_intent(self) -> Intent:
        """Create a SWAP intent: buy WAVAX with USDC via SushiSwap V3."""
        logger.info(
            f"SWAP intent: {self.swap_from} -> {self.swap_to}, "
            f"amount={format_usd(self.swap_amount_usd)} via SushiSwap V3 on Avalanche"
        )

        return Intent.swap(
            from_token=self.swap_from,
            to_token=self.swap_to,
            amount_usd=self.swap_amount_usd,
            max_slippage=Decimal("0.01"),
            protocol="sushiswap_v3",
        )

    def _create_lp_open_intent(self, current_price: Decimal) -> Intent:
        """Create an LP_OPEN intent: open WAVAX/USDC concentrated liquidity."""
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN intent: {format_token_amount_human(self.lp_amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.lp_amount1, self.token1_symbol)}, "
            f"range [{range_lower:.4f} - {range_upper:.4f}] {self.token1_symbol}/{self.token0_symbol}"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.lp_amount0,
            amount1=self.lp_amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="sushiswap_v3",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track execution results and advance the state machine."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SWAP":
                self._loop_state = "swapped"
                swap_amounts = getattr(result, "swap_amounts", None) if result else None
                if swap_amounts:
                    logger.info(
                        f"SWAP succeeded: {swap_amounts.amount_in_decimal:.6f} {swap_amounts.token_in} "
                        f"-> {swap_amounts.amount_out_decimal:.6f} {swap_amounts.token_out}"
                    )
                else:
                    logger.info("SWAP succeeded (swap_amounts not enriched)")

            elif intent_type == "LP_OPEN":
                self._loop_state = "complete"
                position_id = getattr(result, "position_id", None) if result else None
                if position_id:
                    self._position_id = str(position_id)
                    logger.info(f"LP_OPEN succeeded: position_id={position_id}")
                else:
                    logger.info("LP_OPEN succeeded (position_id not extracted)")
        else:
            error = getattr(result, "error", "unknown") if result else "unknown"
            logger.warning(f"{intent_type} FAILED: {error}")
            # Revert to previous stable state
            self._loop_state = self._previous_stable_state
            logger.info(f"Reverted to state: {self._loop_state}")

    def _estimate_position_value(self) -> Decimal:
        """Estimate position value using live prices, falling back to Decimal('0')."""
        try:
            market = self.create_market_snapshot()
            token0_price = market.price(self.token0_symbol)
            return self.lp_amount0 * token0_price + self.lp_amount1
        except Exception:
            return Decimal("0")

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._position_id:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._position_id,
                    chain=self.chain,
                    protocol="sushiswap_v3",
                    value_usd=self._estimate_position_value(),
                    details={"pool": self.pool},
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "sushiswap_v3_avalanche"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents: list[Intent] = []
        if self._position_id:
            intents.append(
                Intent.lp_close(
                    position_id=self._position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="sushiswap_v3",
                )
            )
        return intents
