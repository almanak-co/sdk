"""Berachain Swap Demo Strategy.

A simple RSI-based swap strategy on Berachain using Enso DEX aggregator.
Demonstrates the Almanak SDK's support for Berachain, an EVM-compatible L1
with Proof of Liquidity consensus.

BERACHAIN CONTEXT:
- Native gas token: BERA (wrapped as WBERA)
- Native stablecoin: HONEY (18 decimals)
- Key DEXs: Kodiak, BEX, Beraswap (all routed via Enso)
- Proof of Liquidity: Users earn BGT by providing liquidity

STRATEGY:
1. Monitors RSI of WBERA
2. RSI < 30 (oversold): Buy WBERA with HONEY via Enso
3. RSI > 70 (overbought): Sell WBERA for HONEY via Enso
4. Otherwise: Hold

To run:
    almanak strat run -d strategies/demo/berachain_swap --network anvil --once
"""

import logging
from datetime import UTC
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_berachain_swap",
    description="Tutorial: RSI-based trading on Berachain using Enso aggregator",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "tutorial", "rsi", "enso", "berachain", "trading"],
    supported_chains=["berachain"],
    supported_protocols=["enso"],
    intent_types=["SWAP", "HOLD"],
    default_chain="berachain",
)
class BerachainSwapStrategy(IntentStrategy):
    """RSI-based swap strategy for Berachain using Enso aggregator.

    Demonstrates Berachain support in the Almanak SDK with:
    - WBERA/HONEY trading pair (native wrapped token / native stablecoin)
    - Enso multi-DEX routing (Kodiak, BEX, Beraswap)
    - Standard RSI entry/exit signals
    - Full teardown support

    CONFIGURATION (from config.json):
        trade_size_usd (str): Amount to trade per signal
        rsi_oversold (int): RSI level that triggers buy (default: 30)
        rsi_overbought (int): RSI level that triggers sell (default: 70)
        max_slippage_pct (float): Maximum slippage percentage (default: 1.0)
        base_token (str): Token to trade (default: "WBERA")
        quote_token (str): Quote token (default: "HONEY")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(self.get_config("trade_size_usd", "100"))
        self.rsi_oversold = int(self.get_config("rsi_oversold", 30))
        self.rsi_overbought = int(self.get_config("rsi_overbought", 70))
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 1.0))
        self.base_token = self.get_config("base_token", "WBERA")
        self.quote_token = self.get_config("quote_token", "HONEY")
        self.force_action = self.get_config("force_action", None)
        self._trades_executed = 0

        logger.info(
            f"BerachainSwapStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make trading decision based on RSI."""
        # Handle forced actions (for testing)
        if self.force_action:
            if self.force_action == "buy":
                return self._create_buy_intent()
            elif self.force_action == "sell":
                return self._create_sell_intent()

        # Get RSI
        try:
            rsi_data = market.rsi(self.base_token)
            current_rsi = float(rsi_data.value)
        except ValueError:
            current_rsi = 50.0
            logger.warning(f"RSI unavailable for {self.base_token}, using {current_rsi}")

        # Trading decision
        if current_rsi < self.rsi_oversold:
            logger.info(
                f"BUY SIGNAL: RSI={current_rsi:.2f} < {self.rsi_oversold} "
                f"| Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
            )
            return self._create_buy_intent()

        elif current_rsi > self.rsi_overbought:
            logger.info(
                f"SELL SIGNAL: RSI={current_rsi:.2f} > {self.rsi_overbought} "
                f"| Selling {format_usd(self.trade_size_usd)} of {self.base_token}"
            )
            return self._create_sell_intent()

        else:
            return Intent.hold(reason=f"RSI {current_rsi:.2f} in neutral zone")

    def _create_buy_intent(self) -> Intent:
        """Buy base token with quote token via Enso."""
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def _create_sell_intent(self) -> Intent:
        """Sell base token for quote token via Enso."""
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_berachain_swap",
            "chain": self.chain,
            "config": {
                "trade_size_usd": str(self.trade_size_usd),
                "base_token": self.base_token,
                "quote_token": self.quote_token,
            },
            "state": {
                "trades_executed": self._trades_executed,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()
        if isinstance(self.config, dict):
            config_dict = self.config
        elif hasattr(self.config, "to_dict"):
            config_dict = self.config.to_dict()
        else:
            config_dict = {}

        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_berachain_swap"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="berachain_swap_token_0",
                    chain=self.chain,
                    protocol="enso",
                    value_usd=self.trade_size_usd,
                    details={
                        "asset": self.base_token,
                        "base_token": self.base_token,
                        "quote_token": self.quote_token,
                    },
                )
            ],
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal(str(self.max_slippage_pct)) / Decimal("100")

        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="enso",
            )
        ]
