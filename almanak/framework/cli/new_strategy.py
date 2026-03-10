"""CLI command for scaffolding new strategies.

Usage:
    almanak new-strategy --template <template> --name <name> --chain <chain>

Example:
    almanak new-strategy --template dynamic_lp --name my_strategy --chain arbitrum
"""

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import click


class StrategyTemplate(StrEnum):
    """Available strategy templates."""

    DYNAMIC_LP = "dynamic_lp"
    MEAN_REVERSION = "mean_reversion"
    BOLLINGER = "bollinger"
    BASIS_TRADE = "basis_trade"
    LENDING_LOOP = "lending_loop"
    COPY_TRADER = "copy_trader"
    BLANK = "blank"


class SupportedChain(StrEnum):
    """Supported blockchain networks."""

    ETHEREUM = "ethereum"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BASE = "base"
    AVALANCHE = "avalanche"


@dataclass
class TemplateConfig:
    """Configuration for a strategy template."""

    name: str
    description: str
    default_protocol: str
    config_params: dict[str, str]


# Template configurations with sensible defaults
TEMPLATE_CONFIGS: dict[StrategyTemplate, TemplateConfig] = {
    StrategyTemplate.DYNAMIC_LP: TemplateConfig(
        name="Dynamic LP",
        description="Volatility-based LP strategy that adjusts position range based on market conditions",
        default_protocol="uniswap_v3",
        config_params={
            "volatility_factor": "2",
            "rebalance_threshold": "0.8",
            "time_window": "96",
            "granularity": "15m",
        },
    ),
    StrategyTemplate.MEAN_REVERSION: TemplateConfig(
        name="Mean Reversion",
        description="RSI-based trading strategy that buys oversold and sells overbought conditions",
        default_protocol="uniswap_v3",
        config_params={
            "rsi_period": "14",
            "rsi_oversold": "30",
            "rsi_overbought": "70",
            "trade_size_usd": "1000",
        },
    ),
    StrategyTemplate.BOLLINGER: TemplateConfig(
        name="Bollinger Bands",
        description="Volatility-aware mean reversion strategy using Bollinger Bands bandwidth and %B",
        default_protocol="uniswap_v3",
        config_params={
            "bb_period": "20",
            "bb_std_dev": "2.0",
            "bb_timeframe": "1h",
            "squeeze_threshold": "0.02",
            "buy_percent_b": "0.0",
            "sell_percent_b": "1.0",
            "trade_size_usd": "1000",
        },
    ),
    StrategyTemplate.BASIS_TRADE: TemplateConfig(
        name="Basis Trade",
        description="Spot+perp hedging strategy that captures funding rate arbitrage",
        default_protocol="gmx_v2",
        config_params={
            "spot_size_usd": "10000",
            "hedge_ratio": "1.0",
            "funding_threshold": "0.001",
        },
    ),
    StrategyTemplate.LENDING_LOOP: TemplateConfig(
        name="Lending Loop",
        description="Aave/Morpho leverage looping strategy for yield optimization",
        default_protocol="aave_v3",
        config_params={
            "initial_deposit_usd": "10000",
            "target_leverage": "2.0",
            "min_health_factor": "1.5",
        },
    ),
    StrategyTemplate.COPY_TRADER: TemplateConfig(
        name="Copy Trader",
        description="Copy trading strategy that monitors leader wallets and replicates trades",
        default_protocol="uniswap_v3",
        config_params={
            "fixed_usd": "100",
            "max_trade_usd": "1000",
            "max_slippage": "0.01",
        },
    ),
    StrategyTemplate.BLANK: TemplateConfig(
        name="Blank",
        description="Minimal strategy template for custom implementations",
        default_protocol="custom",
        config_params={},
    ),
}


def to_snake_case(name: str) -> str:
    """Convert a string to snake_case."""
    # Replace spaces and hyphens with underscores
    name = re.sub(r"[\s\-]+", "_", name)
    # Insert underscore before uppercase letters and convert to lowercase
    name = re.sub(r"([A-Z])", r"_\1", name).lower()
    # Remove leading underscores and collapse multiple underscores
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def to_pascal_case(name: str) -> str:
    """Convert a string to PascalCase."""
    snake = to_snake_case(name)
    return "".join(word.capitalize() for word in snake.split("_"))


def _get_template_decide_logic(template: StrategyTemplate, config: TemplateConfig) -> str:
    """Generate template-specific decide() logic."""
    if template == StrategyTemplate.MEAN_REVERSION:
        return """
            # Get RSI indicator
            try:
                rsi = market.rsi(self.base_token, period=self.rsi_period)
            except ValueError as e:
                logger.warning(f"Could not get RSI: {e}")
                return Intent.hold(reason="RSI data unavailable")

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Trading logic
            if rsi.value <= self.rsi_oversold:
                # Oversold - BUY signal
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Oversold (RSI={rsi.value:.1f}) but insufficient {self.quote_token}"
                    )
                logger.info(f"BUY SIGNAL: RSI={rsi.value:.2f} < {self.rsi_oversold}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                )

            elif rsi.value >= self.rsi_overbought:
                # Overbought - SELL signal
                base_price = market.price(self.base_token)
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(
                        reason=f"Overbought (RSI={rsi.value:.1f}) but insufficient {self.base_token}"
                    )
                logger.info(f"SELL SIGNAL: RSI={rsi.value:.2f} > {self.rsi_overbought}")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                )

            else:
                # Neutral zone
                return Intent.hold(
                    reason=f"RSI={rsi.value:.2f} in neutral zone [{self.rsi_oversold}-{self.rsi_overbought}]"
                )"""

    elif template == StrategyTemplate.BOLLINGER:
        return """
            # Get Bollinger Bands indicator
            try:
                bb = market.bollinger_bands(
                    self.base_token,
                    period=self.bb_period,
                    std_dev=self.bb_std_dev,
                    timeframe=self.bb_timeframe,
                )
            except ValueError as e:
                logger.warning(f"Could not get Bollinger Bands: {e}")
                return Intent.hold(reason="BB data unavailable")

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            # Log current BB state
            logger.info(
                f"BB: bandwidth={bb.bandwidth:.4f}, %B={bb.percent_b:.4f}, "
                f"upper={bb.upper_band:.2f}, mid={bb.middle_band:.2f}, lower={bb.lower_band:.2f}"
            )

            # Squeeze detection: low bandwidth means consolidation, skip trading
            if bb.bandwidth < self.squeeze_threshold:
                return Intent.hold(
                    reason=f"Squeeze detected (bandwidth={bb.bandwidth:.4f} < {self.squeeze_threshold})"
                )

            # Mean reversion: buy when price is at or below lower band
            if bb.percent_b <= self.buy_percent_b:
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Buy signal (%B={bb.percent_b:.2f}) but insufficient {self.quote_token}"
                    )
                logger.info(f"BUY SIGNAL: %B={bb.percent_b:.4f} <= {self.buy_percent_b}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                )

            # Mean reversion: sell when price is at or above upper band
            if bb.percent_b >= self.sell_percent_b:
                base_price = market.price(self.base_token)
                min_base_to_sell = self.trade_size_usd / base_price
                if base_balance.balance < min_base_to_sell:
                    return Intent.hold(
                        reason=f"Sell signal (%B={bb.percent_b:.2f}) but insufficient {self.base_token}"
                    )
                logger.info(f"SELL SIGNAL: %B={bb.percent_b:.4f} >= {self.sell_percent_b}")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                )

            # Neutral zone
            return Intent.hold(
                reason=f"%B={bb.percent_b:.4f} in neutral zone [{self.buy_percent_b}-{self.sell_percent_b}]"
            )"""

    elif template == StrategyTemplate.DYNAMIC_LP:
        return """
            # Get current price and volatility
            base_price = market.price(self.base_token)
            volatility = market.volatility(self.base_token, window=self.time_window)

            # Calculate dynamic range based on volatility
            range_width = volatility * Decimal(str(self.volatility_factor))
            lower_price = base_price * (Decimal("1") - range_width)
            upper_price = base_price * (Decimal("1") + range_width)

            # TODO: Check if current LP position needs rebalancing
            # For now, return hold
            return Intent.hold(
                reason=f"LP range: ${lower_price:.2f} - ${upper_price:.2f}, volatility: {volatility:.4f}"
            )"""

    elif template == StrategyTemplate.BASIS_TRADE:
        return """
            # Get spot price
            spot_price = market.price(self.base_token)

            # Get funding rate from perpetual market
            # TODO: Implement funding rate check
            funding_rate = Decimal("0.0001")  # Placeholder

            # Check funding threshold
            if abs(funding_rate) > self.funding_threshold:
                logger.info(f"Funding rate {funding_rate:.6f} exceeds threshold")
                # TODO: Implement hedged position logic
                return Intent.hold(reason=f"Funding rate: {funding_rate:.6f}")
            else:
                return Intent.hold(reason=f"Funding rate {funding_rate:.6f} below threshold")"""

    elif template == StrategyTemplate.LENDING_LOOP:
        return """
            # Get current health factor
            # TODO: Query Aave/Morpho for actual health factor
            health_factor = Decimal("2.0")  # Placeholder

            if health_factor < self.min_health_factor:
                logger.warning(f"Health factor {health_factor} below minimum {self.min_health_factor}")
                return Intent.hold(reason=f"Health factor too low: {health_factor}")

            # TODO: Implement leverage loop logic
            return Intent.hold(reason=f"Health factor: {health_factor}, target leverage: {self.target_leverage}")"""

    elif template == StrategyTemplate.COPY_TRADER:
        return """
            # Read leader signals from wallet activity provider
            signals = market.wallet_activity(action_types=self.action_types)

            if not signals:
                return Intent.hold(reason="No new leader activity")

            provider = getattr(self, "_wallet_activity_provider", None)

            for signal in signals:
                decision = self.policy_engine.evaluate(signal)
                if decision.action != "execute":
                    logger.info(f"Policy blocked signal {signal.signal_id}: {decision.skip_reason_code}")
                    if provider:
                        provider.consume_signals([signal.event_id])
                    continue

                result = self.intent_builder.build(signal)
                if result.intent is None:
                    logger.info(f"Could not map signal {signal.signal_id}: {result.reason_code}")
                    if provider:
                        provider.consume_signals([signal.event_id])
                    continue

                logger.info(f"Copy intent mapped: {signal.action_type} via {signal.protocol}")
                return result.intent

            return Intent.hold(reason="No actionable signals")"""

    else:  # BLANK template
        return """
            # Get market price
            # price = market.price("ETH")

            # Get wallet balance
            # balance = market.balance("USDC")

            # Implement your trading logic here
            # Example:
            # if some_condition:
            #     return Intent.swap(
            #         from_token="USDC",
            #         to_token="ETH",
            #         amount_usd=Decimal("100"),
            #     )

            return Intent.hold(reason="Strategy logic not implemented")"""


def _get_teardown_comment(template: StrategyTemplate) -> str:
    """Return a template-specific TODO hint for generate_teardown_intents()."""
    hints = {
        StrategyTemplate.MEAN_REVERSION: "Swap all holdings back to quote token",
        StrategyTemplate.BOLLINGER: "Swap all holdings back to quote token",
        StrategyTemplate.DYNAMIC_LP: "Close LP position, then swap tokens to quote",
        StrategyTemplate.BASIS_TRADE: "Close perp position, then swap to quote",
        StrategyTemplate.LENDING_LOOP: "Repay borrows, withdraw collateral, swap to quote",
        StrategyTemplate.COPY_TRADER: "Close all positions in order: perps -> borrows -> supplies -> LPs -> swaps",
        StrategyTemplate.BLANK: "Swap all holdings back to quote token",
    }
    return hints.get(template, "Close all positions and convert to stable")


def _get_template_init_params(template: StrategyTemplate, config: TemplateConfig) -> str:
    """Generate template-specific __init__ parameter extraction."""
    if template == StrategyTemplate.MEAN_REVERSION:
        return """
        # Trading parameters
        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "1000")))

        # RSI parameters
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", "70")))

        # Slippage protection (basis points)
        self.max_slippage_bps = int(get_config("max_slippage_bps", 50))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")"""

    elif template == StrategyTemplate.BOLLINGER:
        return """
        # Bollinger Bands parameters
        self.bb_period = int(get_config("bb_period", 20))
        self.bb_std_dev = float(get_config("bb_std_dev", 2.0))
        self.bb_timeframe = get_config("bb_timeframe", "1h")

        # Trading thresholds
        self.squeeze_threshold = float(get_config("squeeze_threshold", 0.02))
        self.buy_percent_b = float(get_config("buy_percent_b", 0.0))
        self.sell_percent_b = float(get_config("sell_percent_b", 1.0))

        # Trading parameters
        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "1000")))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 50))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")"""

    elif template == StrategyTemplate.DYNAMIC_LP:
        return """
        # LP parameters
        self.volatility_factor = float(get_config("volatility_factor", 2))
        self.rebalance_threshold = float(get_config("rebalance_threshold", 0.8))
        self.time_window = int(get_config("time_window", 96))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")"""

    elif template == StrategyTemplate.BASIS_TRADE:
        return """
        # Basis trade parameters
        self.spot_size_usd = Decimal(str(get_config("spot_size_usd", "10000")))
        self.hedge_ratio = Decimal(str(get_config("hedge_ratio", "1.0")))
        self.funding_threshold = Decimal(str(get_config("funding_threshold", "0.001")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")"""

    elif template == StrategyTemplate.LENDING_LOOP:
        return """
        # Lending loop parameters
        self.initial_deposit_usd = Decimal(str(get_config("initial_deposit_usd", "10000")))
        self.target_leverage = Decimal(str(get_config("target_leverage", "2.0")))
        self.min_health_factor = Decimal(str(get_config("min_health_factor", "1.5")))

        # Token configuration
        self.collateral_token = get_config("collateral_token", "WETH")
        self.borrow_token = get_config("borrow_token", "USDC")"""

    elif template == StrategyTemplate.COPY_TRADER:
        return """
        from almanak.framework.services.copy_intent_builder import CopyIntentBuilder
        from almanak.framework.services.copy_policy_engine import CopyPolicyEngine
        from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
        from almanak.framework.services.copy_trading_models import CopyTradingConfigV2

        # Copy trading config
        ct_config = get_config("copy_trading", {})
        self.copy_config = CopyTradingConfigV2.from_config(ct_config if isinstance(ct_config, dict) else {})
        self.action_types = self.copy_config.global_policy.action_types

        sizing_dict = self.copy_config.sizing.model_dump(mode="python")
        risk_dict = self.copy_config.risk.model_dump(mode="python")
        self.sizer = CopySizer(config=CopySizingConfig.from_config(sizing_dict, risk_dict))

        self.policy_engine = CopyPolicyEngine(config=self.copy_config)
        self.intent_builder = CopyIntentBuilder(config=self.copy_config, sizer=self.sizer)"""

    else:  # BLANK template
        return """
        # Add your configuration parameters here
        # Example:
        # self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "100")))
        pass"""


def generate_strategy_file(
    name: str,
    template: StrategyTemplate,
    chain: SupportedChain,
    output_dir: Path,
) -> str:
    """Generate the main strategy.py file content for v2 IntentStrategy."""
    class_name = to_pascal_case(name) + "Strategy"
    strategy_name = to_snake_case(name)
    config = TEMPLATE_CONFIGS[template]

    # Get template-specific code
    init_params = _get_template_init_params(template, config)
    decide_logic = _get_template_decide_logic(template, config)

    # Determine intent types based on template
    intent_types = {
        StrategyTemplate.MEAN_REVERSION: '["SWAP", "HOLD"]',
        StrategyTemplate.BOLLINGER: '["SWAP", "HOLD"]',
        StrategyTemplate.DYNAMIC_LP: '["LP_OPEN", "LP_CLOSE", "LP_REBALANCE", "HOLD"]',
        StrategyTemplate.BASIS_TRADE: '["SWAP", "PERP_OPEN", "PERP_CLOSE", "HOLD"]',
        StrategyTemplate.LENDING_LOOP: '["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"]',
        StrategyTemplate.COPY_TRADER: (
            '["SWAP", "LP_OPEN", "LP_CLOSE", "SUPPLY", "WITHDRAW", '
            '"BORROW", "REPAY", "PERP_OPEN", "PERP_CLOSE", "HOLD"]'
        ),
        StrategyTemplate.BLANK: '["SWAP", "HOLD"]',
    }

    decimal_import = "from decimal import Decimal"

    content = f'''"""
{config.name} Strategy: {name}

{config.description}

Generated by: almanak strat new
Template: {template.value}
Chain: {chain.value}
Created: {datetime.now().isoformat()}

Strategy Pattern:
-----------------
1. Inherit from IntentStrategy
2. Use @almanak_strategy decorator for metadata
3. Implement decide(market) method that returns an Intent
4. The framework handles compilation and execution
"""

import logging
{decimal_import}
from typing import Any, Optional

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.intents import Intent

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="{strategy_name}",
    description="{config.description}",
    version="1.0.0",
    author="Generated",
    tags=["generated", "{template.value}"],
    supported_chains=["{chain.value}"],
    supported_protocols=["{config.default_protocol}"],
    intent_types={intent_types[template]},
    default_chain="{chain.value}",
)
class {class_name}(IntentStrategy):
    """
    {config.description}

    Chain: {chain.value}
    Protocol: {config.default_protocol}

    Configuration Parameters:
    -------------------------
    See config.json for configurable parameters.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the strategy with configuration.

        The base class (IntentStrategy) handles:
        - self.config: Strategy configuration (dict or dataclass)
        - self.chain: The blockchain to operate on
        - self.wallet_address: The wallet executing trades
        """
        super().__init__(*args, **kwargs)

        # Helper to get config value from dict or object attributes
        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)
{init_params}

        logger.info(f"{class_name} initialized on {{self.chain}}")

    def decide(self, market: MarketSnapshot) -> Optional[Intent]:
        """
        Make a trading decision based on current market conditions.

        This is the core method of the strategy. It's called by the framework
        on each iteration with fresh market data.

        Parameters:
            market: MarketSnapshot containing:
                - market.price(token): Get current price in USD
                - market.rsi(token, period): Get RSI indicator
                - market.balance(token): Get wallet balance
                - market.chain: Current chain
                - market.wallet_address: Current wallet

        Returns:
            Intent: What action to take
                - Intent.swap(...): Execute a swap
                - Intent.hold(...): Do nothing
                - None: Also means hold
        """
        try:{decide_logic}

        except Exception as e:
            logger.exception(f"Error in decide(): {{e}}")
            return Intent.hold(reason=f"Error: {{str(e)}}")

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {{
            "strategy": "{strategy_name}",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else None,
        }}

    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # See: blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown."""
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Return all open positions for teardown preview.

        IMPORTANT: Query on-chain state here, not cached values.
        The framework calls this to show operators what will be closed.
        """
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list["PositionInfo"] = []

        # TODO: Add your open positions here. Example:
        # positions.append(
        #     PositionInfo(
        #         position_type=PositionType.TOKEN,
        #         position_id="{strategy_name}_token_0",
        #         chain=self.chain,
        #         protocol="{config.default_protocol}",
        #         value_usd=Decimal("0"),  # Query actual on-chain balance
        #         details={{"asset": "WETH"}},
        #     )
        # )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "{strategy_name}"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions.

        Teardown goal: {_get_teardown_comment(template)}

        Args:
            mode: TeardownMode.SOFT (normal slippage) or TeardownMode.HARD (emergency, 3% slippage)
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

        # TODO: Add teardown intents here. Example for a swap strategy:
        # intents.append(
        #     Intent.swap(
        #         from_token="WETH",
        #         to_token="USDC",
        #         amount="all",
        #         max_slippage=max_slippage,
        #         protocol="{config.default_protocol}",
        #     )
        # )

        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("{class_name}")
    print("=" * 60)
    print(f"Strategy Name: {{{class_name}.STRATEGY_NAME}}")
    print(f"Supported Chains: {{{class_name}.SUPPORTED_CHAINS}}")
    print(f"Supported Protocols: {{{class_name}.SUPPORTED_PROTOCOLS}}")
    print(f"Intent Types: {{{class_name}.INTENT_TYPES}}")
    print("\\nTo run this strategy:")
    print("  uv run almanak strat run --once")
'''

    return content


def generate_config_json(
    name: str,
    template: StrategyTemplate,
    chain: SupportedChain,
) -> str:
    """Generate config.json content for the strategy.

    This produces the runtime config file that load_strategy_config() reads.
    Structural metadata (strategy_id, chain) lives in the @almanak_strategy decorator,
    not here. Config.json is for tunable parameters only.
    """
    import json

    # Tunable parameters only - no structural metadata
    data: dict[str, object] = {}

    # Template-specific parameters (matching what __init__ reads via get_config)
    if template == StrategyTemplate.MEAN_REVERSION:
        data.update(
            {
                "base_token": "WETH",
                "quote_token": "USDC",
                "rsi_period": 14,
                "rsi_oversold": 30,
                "rsi_overbought": 70,
                "trade_size_usd": 1000,
                "max_slippage_bps": 50,
            }
        )
    elif template == StrategyTemplate.BOLLINGER:
        data.update(
            {
                "base_token": "WETH",
                "quote_token": "USDC",
                "bb_period": 20,
                "bb_std_dev": 2.0,
                "bb_timeframe": "1h",
                "squeeze_threshold": 0.02,
                "buy_percent_b": 0.0,
                "sell_percent_b": 1.0,
                "trade_size_usd": 1000,
                "max_slippage_bps": 50,
            }
        )
    elif template == StrategyTemplate.DYNAMIC_LP:
        data.update(
            {
                "base_token": "WETH",
                "quote_token": "USDC",
                "volatility_factor": 2,
                "rebalance_threshold": 0.8,
                "time_window": 96,
            }
        )
    elif template == StrategyTemplate.BASIS_TRADE:
        data.update(
            {
                "base_token": "WETH",
                "spot_size_usd": 10000,
                "hedge_ratio": 1.0,
                "funding_threshold": 0.001,
            }
        )
    elif template == StrategyTemplate.LENDING_LOOP:
        data.update(
            {
                "collateral_token": "WETH",
                "borrow_token": "USDC",
                "initial_deposit_usd": 10000,
                "target_leverage": 2.0,
                "min_health_factor": 1.5,
            }
        )
    elif template == StrategyTemplate.COPY_TRADER:
        data.update(
            {
                "copy_trading": {
                    "leaders": [
                        {"address": "0x_LEADER_WALLET_ADDRESS", "chain": chain.value},
                    ],
                    "sizing": {"mode": "fixed_usd", "fixed_usd": 100, "max_trade_usd": 1000},
                    "risk": {"max_slippage": 0.01},
                    "monitoring": {"poll_interval_seconds": 12, "lookback_blocks": 50},
                },
            }
        )
    # BLANK template: just strategy_id + chain (no extra params)

    return json.dumps(data, indent=4) + "\n"


def generate_test_file(
    name: str,
    template: StrategyTemplate,
    chain: SupportedChain,
) -> str:
    """Generate the test_strategy.py file content."""
    class_name = to_pascal_case(name) + "Strategy"

    content = f'''"""
Tests for {name} strategy.

Generated by: almanak strat new
Template: {template.value}
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock
from decimal import Decimal

from ..strategy import {class_name}


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {{
        "strategy_id": "test-strategy-001",
        "chain": "{chain.value}",
    }}


@pytest.fixture
def strategy(config: dict) -> {class_name}:
    """Create strategy instance for testing."""
    return {class_name}(
        config=config,
        chain=config.get("chain", "{chain.value}"),
        wallet_address="0x" + "1" * 40,
    )


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price.return_value = Decimal("2000")
    market.chain = "{chain.value}"
    market.wallet_address = "0x" + "1" * 40

    # Mock balance
    balance_mock = MagicMock()
    balance_mock.balance = Decimal("100")
    balance_mock.balance_usd = Decimal("100000")
    market.balance.return_value = balance_mock

    # Mock RSI
    rsi_mock = MagicMock()
    rsi_mock.value = Decimal("50")
    market.rsi.return_value = rsi_mock

    return market


class Test{class_name}:
    """Tests for {class_name} strategy."""

    def test_initialization(self, strategy: {class_name}) -> None:
        """Test strategy initialization."""
        assert strategy.chain == "{chain.value}"
        assert strategy.wallet_address == "0x" + "1" * 40

    def test_decide_returns_intent(self, strategy: {class_name}, mock_market: MagicMock) -> None:
        """Test that decide() returns an Intent."""
        result = strategy.decide(mock_market)

        # Should return some kind of Intent (swap or hold)
        assert result is None or hasattr(result, 'intent_type')

    def test_decide_handles_errors(self, strategy: {class_name}, mock_market: MagicMock) -> None:
        """Test that decide() handles errors gracefully."""
        # Cause an error by making price() raise
        mock_market.price.side_effect = ValueError("Price unavailable")

        result = strategy.decide(mock_market)

        # Should return hold on error, not raise
        assert result is not None
        assert "Error" in str(result.reason) or "hold" in str(result).lower()

    def test_get_status(self, strategy: {class_name}) -> None:
        """Test get_status returns expected fields."""
        status = strategy.get_status()

        assert "strategy" in status
        assert "chain" in status
'''

    return content


def generate_init_file(name: str) -> str:
    """Generate the __init__.py file content."""
    class_name = to_pascal_case(name) + "Strategy"

    content = f'''"""
{to_pascal_case(name)} Strategy Package.

Generated by: almanak strat new
"""

from .strategy import {class_name}

__all__ = [
    "{class_name}",
]
'''

    return content


def generate_env_file() -> str:
    """Generate the .env file with required environment variables."""
    return """# Required
ALMANAK_PRIVATE_KEY=

# RPC access (set one of these, or leave empty for free public RPCs)
# RPC_URL=https://your-rpc-provider.com/v1/your-key
# ALCHEMY_API_KEY=

# Optional
# ALMANAK_GATEWAY_PRIVATE_KEY=  # falls back to ALMANAK_PRIVATE_KEY if unset
# ENSO_API_KEY=
# COINGECKO_API_KEY=
# ALMANAK_API_KEY=
"""


def register_strategy_in_factory(
    name: str,
    strategies_dir: Path,
) -> None:
    """Register the new strategy in the strategy factory."""
    factory_file = strategies_dir / "__init__.py"
    class_name = f"Strategy{to_pascal_case(name)}"
    module_name = to_snake_case(name)

    # Read existing factory file or create new one
    if factory_file.exists():
        with open(factory_file) as f:
            content = f.read()
    else:
        content = '''"""
Strategy Factory - Auto-registers all available strategies.

Generated by: almanak new-strategy
"""

from typing import Type, Dict, Any

# Strategy registry - maps strategy names to their classes
STRATEGY_REGISTRY: Dict[str, Type[Any]] = {}


def register_strategy(name: str, strategy_class: Type[Any]) -> None:
    """Register a strategy class in the factory."""
    STRATEGY_REGISTRY[name] = strategy_class


def get_strategy(name: str) -> Type[Any]:
    """Get a strategy class by name."""
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Unknown strategy: {name}. Available: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[name]


def list_strategies() -> list[str]:
    """List all registered strategy names."""
    return list(STRATEGY_REGISTRY.keys())

'''

    # Add import and registration if not already present
    import_line = f"from .{module_name} import {class_name}"
    register_line = f'register_strategy("{module_name}", {class_name})'

    if import_line not in content:
        lines = content.split("\n")

        # Find position to insert import - after docstring and existing imports
        import_insert_pos = 0
        in_docstring = False

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Track docstring boundaries
            if stripped.startswith('"""') or stripped.startswith("'''"):
                if in_docstring:
                    in_docstring = False
                    import_insert_pos = i + 1
                elif stripped.count('"""') == 2 or stripped.count("'''") == 2:
                    # Single line docstring
                    import_insert_pos = i + 1
                else:
                    in_docstring = True
                continue

            if in_docstring:
                continue

            # After docstring, look for import section
            if stripped.startswith("from ") or stripped.startswith("import "):
                import_insert_pos = i + 1
            elif stripped and not stripped.startswith("#") and import_insert_pos > 0:
                # First non-import, non-comment line after imports
                break

        # Insert the import line
        lines.insert(import_insert_pos, import_line)

        # Add registration at the end of the file
        if register_line not in content:
            # Add a blank line if file doesn't end with one
            if lines and lines[-1].strip():
                lines.append("")
            lines.append(register_line)

        content = "\n".join(lines)

        with open(factory_file, "w") as fh:
            fh.write(content)


@click.command("new-strategy")
@click.option(
    "--template",
    "-t",
    type=click.Choice([t.value for t in StrategyTemplate]),
    default=StrategyTemplate.BLANK.value,
    help="Strategy template to use",
)
@click.option(
    "--name",
    "-n",
    required=True,
    help="Name for the new strategy (e.g., 'my_awesome_strategy')",
)
@click.option(
    "--chain",
    "-c",
    type=click.Choice([c.value for c in SupportedChain]),
    default=SupportedChain.ARBITRUM.value,
    help="Target blockchain network",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output directory (default: ./<name> in current working directory)",
)
def new_strategy(
    template: str,
    name: str,
    chain: str,
    output_dir: str | None,
) -> None:
    """
    Scaffold a new Almanak strategy from a template.

    This command generates a complete strategy directory structure with:
    - strategy.py: Main strategy implementation
    - config.json: Runtime configuration file
    - tests/test_strategy.py: Example test cases
    - __init__.py: Package initialization with exports

    Examples:

        almanak new-strategy --template dynamic_lp --name my_lp_strategy --chain arbitrum

        almanak new-strategy -t mean_reversion -n rsi_trader -c ethereum
    """
    template_enum = StrategyTemplate(template)
    chain_enum = SupportedChain(chain)
    snake_name = to_snake_case(name)

    # Determine output directory
    if output_dir:
        strategy_dir = Path(output_dir).resolve()
    else:
        # Default to current working directory / strategy name
        strategy_dir = Path.cwd() / snake_name

    # Check if directory already exists
    if strategy_dir.exists():
        click.echo(f"Error: Directory already exists: {strategy_dir}", err=True)
        raise click.Abort()

    # Create directory structure
    click.echo(f"Creating strategy: {snake_name}")
    click.echo(f"Template: {template_enum.value}")
    click.echo(f"Chain: {chain_enum.value}")
    click.echo(f"Output: {strategy_dir}")
    click.echo()

    try:
        # Create directories
        strategy_dir.mkdir(parents=True, exist_ok=True)
        tests_dir = strategy_dir / "tests"
        tests_dir.mkdir(exist_ok=True)

        # Generate files
        files_created: list[str] = []

        # strategy.py
        strategy_file = strategy_dir / "strategy.py"
        strategy_content = generate_strategy_file(name, template_enum, chain_enum, strategy_dir)
        with open(strategy_file, "w") as fh:
            fh.write(strategy_content)
        files_created.append("strategy.py")

        # config.json (runtime config read by load_strategy_config)
        config_json_file = strategy_dir / "config.json"
        config_json_content = generate_config_json(name, template_enum, chain_enum)
        with open(config_json_file, "w") as fh:
            fh.write(config_json_content)
        files_created.append("config.json")

        # __init__.py
        init_file = strategy_dir / "__init__.py"
        init_content = generate_init_file(name)
        with open(init_file, "w") as fh:
            fh.write(init_content)
        files_created.append("__init__.py")

        # tests/__init__.py
        tests_init = tests_dir / "__init__.py"
        with open(tests_init, "w") as fh:
            fh.write('"""Tests for the strategy."""\n')
        files_created.append("tests/__init__.py")

        # tests/test_strategy.py
        test_file = tests_dir / "test_strategy.py"
        test_content = generate_test_file(name, template_enum, chain_enum)
        with open(test_file, "w") as fh:
            fh.write(test_content)
        files_created.append("tests/test_strategy.py")

        # .env
        env_file = strategy_dir / ".env"
        env_content = generate_env_file()
        with open(env_file, "w") as fh:
            fh.write(env_content)
        files_created.append(".env")

        # AGENTS.md (per-strategy agent guide)
        from almanak.framework.cli.strategy_agent_guide import (
            StrategyGuideConfig,
            generate_strategy_agents_md,
        )

        guide_config = StrategyGuideConfig(
            strategy_name=snake_name,
            template_name=template_enum.value,
            chain=chain_enum.value,
            class_name=to_pascal_case(name) + "Strategy",
        )
        agents_md_file = strategy_dir / "AGENTS.md"
        agents_md_content = generate_strategy_agents_md(guide_config)
        with open(agents_md_file, "w") as fh:
            fh.write(agents_md_content)
        files_created.append("AGENTS.md")

        # Print success message
        click.echo("Files created:")
        for file_path in files_created:
            click.echo(f"  - {snake_name}/{file_path}")

        click.echo()
        click.echo("Next steps:")
        click.echo(f"  1. cd {snake_name}")
        click.echo("  2. Edit config.json to tune parameters")
        click.echo("  3. Implement your decide() method in strategy.py")
        click.echo("  4. Test locally: almanak strat run --once")
        click.echo("  5. Run tests: uv run pytest tests/")
        click.echo()
        click.echo("Backtesting:")
        click.echo("  almanak backtest pnl -s <name> --start 2024-01-01 --end 2024-06-01")
        click.echo("  almanak backtest sweep -s <name> --start ... --end ... --param 'key:v1,v2,v3'")
        click.echo("  almanak backtest optimize -s <name> --start ... --end ... --config-file cfg.json")
        click.echo()
        click.echo("AI agent support:")
        click.echo("  almanak agent install    # Teach your AI agent this SDK")

    except Exception as e:
        click.echo(f"Error creating strategy: {e}", err=True)
        # Clean up on failure
        if strategy_dir.exists():
            import shutil

            shutil.rmtree(strategy_dir)
        raise click.Abort() from e


if __name__ == "__main__":
    new_strategy()
