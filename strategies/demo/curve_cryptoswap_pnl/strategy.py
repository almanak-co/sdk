"""
===============================================================================
DEMO: Curve CryptoSwap PnL Backtest — RSI Swap Strategy on Ethereum
===============================================================================

RSI-based mean reversion strategy that executes swaps via Curve's tricrypto2
pool on Ethereum. Buys WETH when RSI is oversold, sells when overbought.
Designed as the vehicle for PnL backtesting on Ethereum with a non-standard
AMM (Curve CryptoSwap bonding curve vs Uniswap V3 constant product).

Curve tricrypto2 (0xD51a44d3FaE010294C616388b506AcdA1bfAAE46):
- Coins: USDT (index 0), WBTC (index 1), WETH (index 2)
- Pool type: Tricrypto (volatile 3-coin pool)
- Uses uint256 indices and CryptoSwap exchange selector (0x5b41b908)

USAGE:
------
    # PnL backtest (primary use case)
    almanak strat backtest pnl -d strategies/demo/curve_cryptoswap_pnl

    # Run on Anvil (single iteration)
    almanak strat run -d strategies/demo/curve_cryptoswap_pnl --network anvil --once

    # Dry run
    almanak strat run -d strategies/demo/curve_cryptoswap_pnl --once --dry-run
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
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_curve_cryptoswap_pnl",
    description="RSI swap strategy on Ethereum via Curve CryptoSwap — PnL backtest vehicle",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "trading", "rsi", "mean-reversion", "curve", "cryptoswap", "ethereum", "backtesting"],
    supported_chains=["ethereum"],
    supported_protocols=["curve"],
    intent_types=["SWAP", "HOLD"],
    default_chain="ethereum",
)
class CurveCryptoSwapPnLStrategy(IntentStrategy):
    """RSI-based swap strategy using Curve CryptoSwap on Ethereum.

    Trades USDT/WETH via the tricrypto2 pool. Uses the CryptoSwap exchange
    selector (uint256 indices) validated in iter 86 (PR #702).

    Configuration (config.json):
        trade_size_usd: Trade size in USD per signal (default: 100)
        rsi_period: RSI lookback period (default: 14)
        rsi_oversold: Buy threshold (default: 40)
        rsi_overbought: Sell threshold (default: 70)
        max_slippage_bps: Max slippage in basis points (default: 100)
        base_token: Token to trade (default: WETH)
        quote_token: Quote token (default: USDT)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "100")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "40")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDT")

        self._consecutive_holds = 0
        self._has_position = False

        logger.info(
            f"CurveCryptoSwapPnL initialized: trade_size={format_usd(self.trade_size_usd)}, "
            f"RSI({self.rsi_period}) [{self.rsi_oversold},{self.rsi_overbought}], "
            f"pair={self.base_token}/{self.quote_token} via Curve tricrypto2"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-based buy/sell/hold decision using Curve CryptoSwap."""
        try:
            base_price = market.price(self.base_token)
        except ValueError as e:
            logger.warning(f"Price unavailable: {e}")
            return Intent.hold(reason="Price data unavailable")

        if base_price <= 0:
            logger.warning(f"Invalid {self.base_token} price: {base_price}")
            return Intent.hold(reason=f"Invalid {self.base_token} price")

        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period)
        except ValueError as e:
            logger.warning(f"RSI unavailable: {e}")
            return Intent.hold(reason="RSI data unavailable")

        try:
            quote_balance = market.balance(self.quote_token)
            base_balance = market.balance(self.base_token)
        except ValueError as e:
            logger.warning(f"Balance unavailable: {e}")
            return Intent.hold(reason="Balance data unavailable")

        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        # OVERSOLD -> BUY WETH with USDT
        if rsi.value <= self.rsi_oversold:
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Oversold (RSI={rsi.value:.1f}) but insufficient "
                    f"{self.quote_token} ({format_usd(quote_balance.balance_usd)})"
                )

            logger.info(
                f"BUY: RSI={rsi.value:.2f} < {self.rsi_oversold} | "
                f"Buying {format_usd(self.trade_size_usd)} of {self.base_token} via Curve"
            )
            self._consecutive_holds = 0
            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=max_slippage,
                protocol="curve",
            )

        # OVERBOUGHT -> SELL WETH for USDT
        elif rsi.value >= self.rsi_overbought:
            min_base_to_sell = self.trade_size_usd / base_price
            if base_balance.balance < min_base_to_sell:
                return Intent.hold(
                    reason=f"Overbought (RSI={rsi.value:.1f}) but insufficient "
                    f"{self.base_token} ({base_balance.balance:.4f})"
                )

            logger.info(
                f"SELL: RSI={rsi.value:.2f} > {self.rsi_overbought} | "
                f"Selling {format_usd(self.trade_size_usd)} of {self.base_token} via Curve"
            )
            self._consecutive_holds = 0
            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount_usd=self.trade_size_usd,
                max_slippage=max_slippage,
                protocol="curve",
            )

        # NEUTRAL -> HOLD
        else:
            self._consecutive_holds += 1
            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} neutral [{self.rsi_oversold}-{self.rsi_overbought}] "
                f"(hold #{self._consecutive_holds})"
            )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track position state from execution results."""
        if not success:
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return

        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        if intent_type_val != "SWAP":
            return

        from_token = getattr(intent, "from_token", None)
        to_token = getattr(intent, "to_token", None)

        if from_token == self.quote_token and to_token == self.base_token:
            # BUY: acquired base token position
            self._has_position = True
            logger.info(f"Position opened: bought {self.base_token} with {self.quote_token}")
        elif from_token == self.base_token and to_token == self.quote_token:
            # SELL: exited base token position
            self._has_position = False
            logger.info(f"Position closed: sold {self.base_token} for {self.quote_token}")

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "has_position": self._has_position,
            "consecutive_holds": self._consecutive_holds,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "has_position" in state:
            self._has_position = bool(state["has_position"])
        if "consecutive_holds" in state:
            self._consecutive_holds = int(state["consecutive_holds"])
        logger.info(f"Restored state: has_position={self._has_position}")

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_position:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="curve_cryptoswap_pnl_base_token",
                    chain=self.chain,
                    protocol="curve",
                    value_usd=Decimal("0"),
                    details={
                        "asset": self.base_token,
                        "base_token": self.base_token,
                        "quote_token": self.quote_token,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_curve_cryptoswap_pnl"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        if not self._has_position:
            return []

        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.03")
        else:
            max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")
        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="curve",
            )
        ]

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_curve_cryptoswap_pnl",
            "chain": self.chain,
            "config": {
                "trade_size_usd": str(self.trade_size_usd),
                "rsi_period": self.rsi_period,
                "pair": f"{self.base_token}/{self.quote_token}",
            },
            "state": {
                "has_position": self._has_position,
                "consecutive_holds": self._consecutive_holds,
            },
        }
