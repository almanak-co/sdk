"""SushiSwap V3 Swap Lifecycle on Avalanche - Kitchen Loop Iteration 134.

Tests SushiSwap V3 DEX swap on Avalanche:
- First SushiSwap V3 swap lifecycle on Avalanche (5th chain after Arbitrum, Base, BSC, Polygon)
- Validates SushiSwap V3 SwapRouter at 0x717b7948AA264DeCf4D780aa6914482e5F46Da3e on Avalanche
- Tests both BUY and SELL directions via exactInputSingle
- Validates receipt parsing and swap_amounts enrichment with WAVAX native token handling

Linear ticket: VIB-1862

RESULT: FAIL -- SushiSwap V3 USDC/WAVAX pool on Avalanche has insufficient liquidity.
$100 trade: 54% price impact (correctly blocked by price impact guard).
$10 trade: compiled but reverted on-chain with 0x (pool nearly empty).
WETH.e/USDC pool does not exist at fee tier 3000.
Same pattern as Polygon (iter 122).

Lifecycle:
- Iteration 1: BUY (USDC -> WAVAX via SushiSwap V3)
- Iteration 2: SELL (WAVAX -> USDC via SushiSwap V3)
- Iteration 3+: HOLD
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="sushiswap_v3_swap_avalanche",
    description="Kitchen Loop iter 134: SushiSwap V3 swap lifecycle on Avalanche (BUY + SELL)",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "sushiswap_v3", "swap", "avalanche"],
    supported_chains=["avalanche"],
    supported_protocols=["sushiswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class SushiSwapV3SwapAvalancheStrategy(IntentStrategy):
    """SushiSwap V3 swap lifecycle on Avalanche.

    Exercises both BUY and SELL paths through SushiSwap V3 on Avalanche,
    validating compilation, execution, receipt parsing, and enrichment.
    Tests WAVAX native token handling on Avalanche's SushiSwap V3 deployment.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "100")))
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 1.0))
        self.base_token = self.get_config("base_token", "WAVAX")
        self.quote_token = self.get_config("quote_token", "USDC")
        self.fee_tier = int(self.get_config("fee_tier", 3000))

        # Lifecycle state
        self._iteration = 0
        self._buy_executed = False
        self._sell_executed = False

        logger.info(
            f"SushiSwapV3SwapAvalancheStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"slippage={self.max_slippage_pct}%, "
            f"pair={self.base_token}/{self.quote_token}, "
            f"fee_tier={self.fee_tier}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute a 3-iteration lifecycle: buy -> sell -> hold."""
        self._iteration += 1
        logger.info(f"Iteration {self._iteration}: buy_executed={self._buy_executed}, sell_executed={self._sell_executed}")

        try:
            # Log balances
            try:
                quote_bal = market.balance(self.quote_token)
                base_bal = market.balance(self.base_token)
                logger.info(
                    f"Balances: {self.quote_token}={quote_bal.balance} (${quote_bal.balance_usd:,.2f}), "
                    f"{self.base_token}={base_bal.balance} (${base_bal.balance_usd:,.2f})"
                )
            except Exception as e:
                logger.warning(f"Could not get balances: {e}")

            # Iteration 1: BUY (USDC -> WAVAX via SushiSwap V3)
            if not self._buy_executed:
                logger.info(f"BUY via SushiSwap V3: ${self.trade_size_usd} {self.quote_token} -> {self.base_token}")
                return self._create_swap_intent(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                )

            # Iteration 2: SELL (WAVAX -> USDC via SushiSwap V3)
            if not self._sell_executed:
                logger.info(f"SELL via SushiSwap V3: {self.base_token} -> {self.quote_token}")
                return self._create_swap_intent(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                )

            # Iteration 3+: HOLD
            logger.info("Lifecycle complete: both buy and sell executed via SushiSwap V3. Holding.")
            return Intent.hold(reason="Lifecycle complete: buy + sell both executed via SushiSwap V3 on Avalanche")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _create_swap_intent(self, from_token: str, to_token: str) -> Intent:
        """Create a swap intent routed through SushiSwap V3."""
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        return Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="sushiswap_v3",
        )

    def on_intent_executed(self, intent, success: bool, result):
        """Log execution results and update lifecycle state on success."""
        if success:
            intent_dict = intent.to_dict() if hasattr(intent, "to_dict") else {}
            from_token = intent_dict.get("from_token", "")
            if from_token.upper() == self.quote_token.upper() and not self._buy_executed:
                self._buy_executed = True
                logger.info("BUY phase completed successfully")
            elif from_token.upper() == self.base_token.upper() and not self._sell_executed:
                self._sell_executed = True
                logger.info("SELL phase completed successfully")
            logger.info("SushiSwap V3 swap executed successfully")
            if hasattr(result, "swap_amounts") and result.swap_amounts:
                sa = result.swap_amounts
                logger.info(
                    f"swap_amounts enriched: "
                    f"amount_in={sa.amount_in}, "
                    f"amount_out={sa.amount_out}, "
                    f"amount_in_decimal={sa.amount_in_decimal}, "
                    f"amount_out_decimal={sa.amount_out_decimal}, "
                    f"effective_price={sa.effective_price}"
                )
            else:
                logger.warning("swap_amounts NOT enriched on result")

            if hasattr(result, "extracted_data") and result.extracted_data:
                logger.info(f"extracted_data keys: {list(result.extracted_data.keys())}")
        else:
            error_msg = getattr(result, "error", "unknown") if result else "no result"
            logger.error(f"SushiSwap V3 swap FAILED: {error_msg}")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "sushiswap_v3_swap_avalanche",
            "chain": self.chain,
            "iteration": self._iteration,
            "buy_executed": self._buy_executed,
            "sell_executed": self._sell_executed,
            "fee_tier": self.fee_tier,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "iteration": self._iteration,
            "buy_executed": self._buy_executed,
            "sell_executed": self._sell_executed,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "iteration" in state:
            self._iteration = state["iteration"]
        if "buy_executed" in state:
            self._buy_executed = state["buy_executed"]
        if "sell_executed" in state:
            self._sell_executed = state["sell_executed"]

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._buy_executed and not self._sell_executed:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="sushiswap_v3_avalanche_wavax",
                    chain=self.chain,
                    protocol="sushiswap_v3",
                    value_usd=self.trade_size_usd,
                    details={"asset": self.base_token},
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "sushiswap_v3_swap_avalanche"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = (
            Decimal("0.03")
            if mode == TeardownMode.HARD
            else Decimal(str(self.max_slippage_pct)) / Decimal("100")
        )
        if not (self._buy_executed and not self._sell_executed):
            return []
        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="sushiswap_v3",
            )
        ]
