"""Bollinger Band Volatility Trader on Base.

YAInnick Loop Iteration 4 -- stress-testing Bollinger Bands indicator
and swap routing on Base chain.

STRATEGY LOGIC:
---------------
Uses Bollinger Bands to detect volatility regimes and trade mean reversion:

1. SQUEEZE + NEAR LOWER BAND -> BUY (mean reversion entry in quiet market)
   - bandwidth < squeeze_bandwidth AND percent_b < buy_percent_b
   - Rationale: low volatility near the bottom = oversold in a calm market

2. EXPANSION + NEAR UPPER BAND -> SELL (profit-taking during vol spike)
   - bandwidth > expansion_bandwidth AND percent_b > sell_percent_b
   - Rationale: high volatility near the top = overbought during breakout

3. ABOVE UPPER BAND -> SELL (stop-out, price running away)
   - percent_b > 1.0 regardless of bandwidth
   - Rationale: extreme extension, revert to safety

4. Otherwise -> HOLD

WHAT THIS TESTS:
- Base chain (never tested in yailoop ideate before)
- Bollinger Bands indicator via market.bollinger_bands() (never tested)
- Default swap routing on Base
- Bandwidth and percent_b calculations from the data layer
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="bb_volatility_trader",
    description="Bollinger Band volatility trader - buys squeezes, sells expansions on Base",
    version="0.1.0",
    author="YAInnick Loop",
    tags=["incubating", "bollinger-bands", "volatility", "mean-reversion", "base"],
    supported_chains=["base"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class BBVolatilityTrader(IntentStrategy):
    """Bollinger Band volatility strategy for Base chain.

    Buys ETH during low-volatility dips (squeeze near lower band).
    Sells ETH during high-volatility spikes (expansion near upper band).

    Configuration (from config.json):
        trade_size_usd: Amount per trade in USD
        bb_period: Bollinger Band SMA period (default 20)
        bb_std_dev: Standard deviation multiplier (default 2.0)
        bb_timeframe: OHLCV candle timeframe (default "1h")
        buy_percent_b: Buy when percent_b below this (default 0.2)
        sell_percent_b: Sell when percent_b above this (default 0.8)
        squeeze_bandwidth: Bandwidth threshold for squeeze (default 0.04)
        expansion_bandwidth: Bandwidth threshold for expansion (default 0.10)
        max_slippage_pct: Max slippage percentage (default 1.0)
        base_token: Token to trade (default "WETH")
        quote_token: Quote token (default "USDC")
        force_action: Force "buy" or "sell" for testing (optional)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse config -- handle both dataclass and dict configs
        config_dict = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get"):
            config_dict = {k: getattr(self.config, k) for k in dir(self.config) if not k.startswith("_")}

        # Trading parameters
        self.trade_size_usd = Decimal(str(config_dict.get("trade_size_usd", "5")))
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 1.0))

        # Bollinger Band parameters
        self.bb_period = int(config_dict.get("bb_period", 20))
        self.bb_std_dev = float(config_dict.get("bb_std_dev", 2.0))
        self.bb_timeframe = str(config_dict.get("bb_timeframe", "1h"))

        # Signal thresholds
        self.buy_percent_b = float(config_dict.get("buy_percent_b", 0.2))
        self.sell_percent_b = float(config_dict.get("sell_percent_b", 0.8))
        self.squeeze_bandwidth = float(config_dict.get("squeeze_bandwidth", 0.04))
        self.expansion_bandwidth = float(config_dict.get("expansion_bandwidth", 0.10))

        # Token configuration
        self.base_token = str(config_dict.get("base_token", "WETH"))
        self.quote_token = str(config_dict.get("quote_token", "USDC"))

        # Force action for testing
        self.force_action = config_dict.get("force_action", None)
        if self.force_action:
            self.force_action = str(self.force_action).lower()

        # Internal state
        self._trades_executed = 0

        logger.info(
            f"BBVolatilityTrader initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"BB({self.bb_period}, {self.bb_std_dev}, {self.bb_timeframe}), "
            f"buy<%B={self.buy_percent_b}, sell>%B={self.sell_percent_b}, "
            f"squeeze<{self.squeeze_bandwidth}, expand>{self.expansion_bandwidth}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a trading decision based on Bollinger Bands.

        Decision flow:
        1. Check for forced action (for testing)
        2. Get Bollinger Bands for base token
        3. Classify volatility regime (squeeze vs expansion)
        4. Generate buy/sell/hold based on band position + volatility
        """
        try:
            # Step 1: Handle forced actions (for Anvil testing)
            if self.force_action == "buy":
                logger.info("Forced action: BUY (swap USDC -> WETH)")
                return self._create_buy_intent(market, reason="forced_buy")

            if self.force_action == "sell":
                logger.info("Forced action: SELL (swap WETH -> USDC)")
                return self._create_sell_intent(market, reason="forced_sell")

            # Step 2: Get Bollinger Bands
            try:
                bb = market.bollinger_bands(
                    self.base_token,
                    period=self.bb_period,
                    std_dev=self.bb_std_dev,
                    timeframe=self.bb_timeframe,
                )
            except Exception as e:
                logger.warning(f"Bollinger Bands unavailable: {e}")
                return Intent.hold(reason=f"Bollinger Bands unavailable: {e}")

            # Log the BB state
            logger.info(
                f"BB State: upper={bb.upper_band:.2f}, middle={bb.middle_band:.2f}, "
                f"lower={bb.lower_band:.2f}, bandwidth={bb.bandwidth:.4f}, "
                f"percent_b={bb.percent_b:.4f}"
            )

            # Step 3: Classify regime and make decision
            is_squeeze = bb.bandwidth < self.squeeze_bandwidth
            is_expansion = bb.bandwidth > self.expansion_bandwidth
            near_lower = bb.percent_b < self.buy_percent_b
            near_upper = bb.percent_b > self.sell_percent_b
            above_upper = bb.percent_b > 1.0

            # Rule 1: Extreme extension above upper band -> SELL (stop-out)
            if above_upper:
                logger.info(
                    f"SELL signal: price above upper band "
                    f"(percent_b={bb.percent_b:.4f} > 1.0)"
                )
                return self._create_sell_intent(market, reason="above_upper_band")

            # Rule 2: Expansion + near upper band -> SELL (take profit)
            if is_expansion and near_upper:
                logger.info(
                    f"SELL signal: volatility expansion + near upper band "
                    f"(bandwidth={bb.bandwidth:.4f} > {self.expansion_bandwidth}, "
                    f"percent_b={bb.percent_b:.4f} > {self.sell_percent_b})"
                )
                return self._create_sell_intent(market, reason="expansion_upper")

            # Rule 3: Squeeze + near lower band -> BUY (mean reversion)
            if is_squeeze and near_lower:
                logger.info(
                    f"BUY signal: volatility squeeze + near lower band "
                    f"(bandwidth={bb.bandwidth:.4f} < {self.squeeze_bandwidth}, "
                    f"percent_b={bb.percent_b:.4f} < {self.buy_percent_b})"
                )
                return self._create_buy_intent(market, reason="squeeze_lower")

            # Rule 4: Hold
            regime = "SQUEEZE" if is_squeeze else ("EXPANSION" if is_expansion else "NORMAL")
            return Intent.hold(
                reason=f"BB regime={regime}, bandwidth={bb.bandwidth:.4f}, "
                f"percent_b={bb.percent_b:.4f} -- no signal"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _create_buy_intent(self, market: MarketSnapshot, reason: str) -> Intent:
        """Create a swap intent to buy base token with quote token."""
        # Check balance first
        try:
            quote_bal = market.balance(self.quote_token)
            if quote_bal.balance < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.quote_token}: "
                    f"{quote_bal.balance} < {self.trade_size_usd}"
                )
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"Could not check {self.quote_token} balance: {e}")

        logger.info(
            f"BUY: {format_usd(self.trade_size_usd)} {self.quote_token} -> {self.base_token} "
            f"[reason={reason}]"
        )

        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=self.trade_size_usd,
            max_slippage=Decimal(str(self.max_slippage_pct / 100)),
        )

    def _create_sell_intent(self, market: MarketSnapshot, reason: str) -> Intent:
        """Create a swap intent to sell base token for quote token."""
        # Check balance first
        try:
            base_bal = market.balance(self.base_token)
            if base_bal.balance <= Decimal("0"):
                return Intent.hold(
                    reason=f"No {self.base_token} to sell"
                )
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"Could not check {self.base_token} balance: {e}")

        logger.info(
            f"SELL: {format_usd(self.trade_size_usd)} {self.base_token} -> {self.quote_token} "
            f"[reason={reason}]"
        )

        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=self.trade_size_usd,
            max_slippage=Decimal(str(self.max_slippage_pct / 100)),
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track executed trades."""
        if success and intent.intent_type.value == "SWAP":
            self._trades_executed += 1
            swap_amounts = getattr(result, "swap_amounts", None)
            if swap_amounts:
                logger.info(
                    f"Trade #{self._trades_executed} executed: "
                    f"in={swap_amounts.amount_in}, out={swap_amounts.amount_out}"
                )
            else:
                logger.info(f"Trade #{self._trades_executed} executed (no swap_amounts on result)")
        elif not success:
            logger.warning(f"Trade failed: {getattr(result, 'error', 'unknown')}")

    def get_status(self) -> dict[str, Any]:
        """Return current strategy status."""
        return {
            "strategy": "bb_volatility_trader",
            "chain": self.chain,
            "pair": f"{self.base_token}/{self.quote_token}",
            "trades_executed": self._trades_executed,
            "bb_params": {
                "period": self.bb_period,
                "std_dev": self.bb_std_dev,
                "timeframe": self.bb_timeframe,
            },
            "thresholds": {
                "buy_percent_b": self.buy_percent_b,
                "sell_percent_b": self.sell_percent_b,
                "squeeze_bandwidth": self.squeeze_bandwidth,
                "expansion_bandwidth": self.expansion_bandwidth,
            },
        }
