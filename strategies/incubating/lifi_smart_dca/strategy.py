"""LiFi Smart DCA Strategy.

==============================================================================
WHAT IS LiFi?
==============================================================================

LiFi is a multi-chain DEX and bridge aggregator that finds optimal swap routes
across DEXs and bridges on 30+ chains:

BENEFITS:
1. Cross-chain swap support (bridge + swap in one transaction)
2. Multi-DEX routing for best prices
3. Automatic slippage protection
4. Single API for all DeFi swap operations

HOW IT WORKS:
1. You specify: token in, token out, amount, slippage
2. LiFi finds optimal route (single-chain or cross-chain)
3. Returns ready-to-execute transaction calldata
4. Same interface whether single-DEX, multi-hop, or cross-chain

==============================================================================
THIS STRATEGY
==============================================================================

A Smart DCA (Dollar-Cost Averaging) strategy that uses LiFi for execution:
1. Monitors the RSI of a target token
2. When RSI < oversold threshold: Buys target token using LiFi
3. When RSI > overbought threshold: Sells target token using LiFi
4. Otherwise: Holds
5. Uses `force_action` config for deterministic Anvil testing

==============================================================================
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
    name="lifi_smart_dca",
    description="Smart DCA strategy using LiFi aggregator for optimal swap routing",
    version="1.0.0",
    author="Almanak",
    tags=["incubating", "dca", "rsi", "lifi", "aggregator", "trading"],
    supported_chains=["arbitrum", "ethereum", "base", "optimism", "polygon", "avalanche", "bsc"],
    supported_protocols=["lifi"],
    intent_types=["SWAP", "HOLD"],
)
class LiFiSmartDCAStrategy(IntentStrategy):
    """Smart DCA strategy that buys/sells via LiFi aggregator based on RSI signals.

    CONFIGURATION (from config.json):
        target_token (str): Token to accumulate (e.g., "WETH")
        stable_token (str): Stablecoin for buying/selling (e.g., "USDC")
        base_buy_amount_usd (str): USD amount per DCA buy
        rsi_oversold (int): RSI level that triggers buy (default 30)
        rsi_overbought (int): RSI level that triggers sell (default 70)
        max_slippage_pct (float): Maximum slippage percentage (default 1.0)
        force_action (str|None): Force "buy" or "sell" for testing
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(self.config, dict):
            config_dict = self.config
        elif hasattr(self.config, "__dict__"):
            config_dict = {k: v for k, v in self.config.__dict__.items() if not k.startswith("_")}
        else:
            config_dict = {}

        # DCA parameters
        self.target_token = config_dict.get("target_token", "WETH")
        self.stable_token = config_dict.get("stable_token", "USDC")
        self.base_buy_amount_usd = Decimal(str(config_dict.get("base_buy_amount_usd", "5")))

        # RSI thresholds
        self.rsi_oversold = int(config_dict.get("rsi_oversold", 30))
        self.rsi_overbought = int(config_dict.get("rsi_overbought", 70))

        # Slippage (as percentage, e.g., 1.0 = 1%)
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 1.0))

        # Force action for testing
        self.force_action = config_dict.get("force_action", None)

        # Internal state
        self._trades_executed = 0
        self._total_invested = Decimal("0")
        self._total_received = Decimal("0")

        logger.info(
            f"LiFiSmartDCAStrategy initialized: "
            f"buy_amount=${self.base_buy_amount_usd}, "
            f"rsi_oversold={self.rsi_oversold}, "
            f"rsi_overbought={self.rsi_overbought}, "
            f"slippage={self.max_slippage_pct}%, "
            f"pair={self.target_token}/{self.stable_token}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a DCA trading decision based on RSI.

        DECISION FLOW:
        1. Check for forced action (for testing)
        2. Get current RSI for target token
        3. If RSI < oversold: BUY target token with stable via LiFi
        4. If RSI > overbought: SELL target token for stable via LiFi
        5. Otherwise: HOLD
        """
        try:
            # Handle forced actions (for testing)
            if self.force_action:
                logger.info(f"Force action requested: {self.force_action}")
                if self.force_action == "buy":
                    return self._create_buy_intent()
                elif self.force_action == "sell":
                    return self._create_sell_intent()
                else:
                    logger.warning(f"Unknown force_action: {self.force_action}")

            # Get RSI value
            try:
                rsi_data = market.rsi(self.target_token)
                current_rsi = float(rsi_data.value)
                logger.debug(f"Current RSI for {self.target_token}: {current_rsi:.2f}")
            except ValueError:
                current_rsi = 50.0
                logger.warning(f"RSI unavailable for {self.target_token}, using {current_rsi}")

            # Make trading decision
            if current_rsi < self.rsi_oversold:
                logger.info(
                    f"BUY SIGNAL: RSI={current_rsi:.2f} < {self.rsi_oversold} (oversold) "
                    f"| Buying {format_usd(self.base_buy_amount_usd)} of {self.target_token} via LiFi"
                )
                return self._create_buy_intent()

            elif current_rsi > self.rsi_overbought:
                logger.info(
                    f"SELL SIGNAL: RSI={current_rsi:.2f} > {self.rsi_overbought} (overbought) "
                    f"| Selling {format_usd(self.base_buy_amount_usd)} of {self.target_token} via LiFi"
                )
                return self._create_sell_intent()

            else:
                logger.debug(
                    f"RSI {current_rsi:.2f} in neutral zone [{self.rsi_oversold}-{self.rsi_overbought}] -> HOLD"
                )
                return Intent.hold(reason=f"RSI {current_rsi:.2f} in neutral zone")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_buy_intent(self) -> Intent:
        """Create a buy intent using LiFi aggregator.

        BUY: Convert stable token (USDC) to target token (WETH).
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"BUY via LiFi: {format_usd(self.base_buy_amount_usd)} {self.stable_token} -> {self.target_token}, "
            f"slippage={self.max_slippage_pct}%"
        )

        self._trades_executed += 1

        return Intent.swap(
            from_token=self.stable_token,
            to_token=self.target_token,
            amount_usd=self.base_buy_amount_usd,
            max_slippage=max_slippage,
            protocol="lifi",
        )

    def _create_sell_intent(self) -> Intent:
        """Create a sell intent using LiFi aggregator.

        SELL: Convert target token (WETH) to stable token (USDC).
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"SELL via LiFi: {self.target_token} -> {self.stable_token}, "
            f"slippage={self.max_slippage_pct}%"
        )

        self._trades_executed += 1

        return Intent.swap(
            from_token=self.target_token,
            to_token=self.stable_token,
            amount_usd=self.base_buy_amount_usd,
            max_slippage=max_slippage,
            protocol="lifi",
        )

    # =========================================================================
    # CALLBACKS
    # =========================================================================

    def on_intent_executed(self, intent, success: bool, result):
        """Track DCA statistics after each trade."""
        if success and hasattr(intent, "from_token"):
            if intent.from_token == self.stable_token:
                self._total_invested += self.base_buy_amount_usd
                logger.info(f"DCA buy executed. Total invested: {format_usd(self._total_invested)}")
            elif intent.from_token == self.target_token:
                self._total_received += self.base_buy_amount_usd
                logger.info(f"DCA sell executed. Total received: {format_usd(self._total_received)}")

    # =========================================================================
    # STATUS AND MONITORING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "lifi_smart_dca",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "...",
            "config": {
                "base_buy_amount_usd": str(self.base_buy_amount_usd),
                "rsi_oversold": self.rsi_oversold,
                "rsi_overbought": self.rsi_overbought,
                "max_slippage_pct": self.max_slippage_pct,
                "target_token": self.target_token,
                "stable_token": self.stable_token,
            },
            "state": {
                "trades_executed": self._trades_executed,
                "total_invested": str(self._total_invested),
                "total_received": str(self._total_received),
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

        positions: list[PositionInfo] = []
        estimated_value = self.base_buy_amount_usd

        positions.append(
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="lifi_dca_token_0",
                chain=self.chain,
                protocol="lifi",
                value_usd=estimated_value,
                details={
                    "asset": self.target_token,
                    "target_token": self.target_token,
                    "stable_token": self.stable_token,
                    "trades_executed": self._trades_executed,
                    "total_invested": str(self._total_invested),
                },
            )
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "lifi_smart_dca"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.03")
        else:
            max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        logger.info(
            f"Generating teardown intent: swap {self.target_token} -> "
            f"{self.stable_token} (mode={mode.value}, slippage={max_slippage})"
        )

        return [
            Intent.swap(
                from_token=self.target_token,
                to_token=self.stable_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="lifi",
            )
        ]


if __name__ == "__main__":
    print("LiFiSmartDCAStrategy loaded successfully!")
    print(f"Metadata: {LiFiSmartDCAStrategy.STRATEGY_METADATA}")
