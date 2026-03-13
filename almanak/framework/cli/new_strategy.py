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

    BLANK = "blank"
    TA_SWAP = "ta_swap"
    DYNAMIC_LP = "dynamic_lp"
    LENDING_LOOP = "lending_loop"
    BASIS_TRADE = "basis_trade"
    VAULT_YIELD = "vault_yield"
    COPY_TRADER = "copy_trader"
    PERPS = "perps"
    MULTI_STEP = "multi_step"
    STAKING = "staking"


class SupportedChain(StrEnum):
    """Supported blockchain networks."""

    ETHEREUM = "ethereum"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BASE = "base"
    AVALANCHE = "avalanche"
    BSC = "bsc"
    SONIC = "sonic"
    PLASMA = "plasma"
    BLAST = "blast"
    MANTLE = "mantle"
    BERACHAIN = "berachain"
    SOLANA = "solana"
    MONAD = "monad"


@dataclass
class TemplateConfig:
    """Configuration for a strategy template."""

    name: str
    description: str
    default_protocol: str
    config_params: dict[str, str]


# Template configurations with sensible defaults
TEMPLATE_CONFIGS: dict[StrategyTemplate, TemplateConfig] = {
    StrategyTemplate.BLANK: TemplateConfig(
        name="Blank",
        description="Minimal strategy template for custom implementations",
        default_protocol="custom",
        config_params={},
    ),
    StrategyTemplate.TA_SWAP: TemplateConfig(
        name="TA Swap",
        description="Technical analysis swap strategy with configurable RSI, Bollinger Bands, or combined signals",
        default_protocol="uniswap_v3",
        config_params={
            "indicator": "rsi",
            "base_token": "WETH",
            "quote_token": "USDC",
        },
    ),
    StrategyTemplate.DYNAMIC_LP: TemplateConfig(
        name="Dynamic LP",
        description="Price-based LP range management with position tracking and rebalancing",
        default_protocol="uniswap_v3",
        config_params={
            "range_width_pct": "5",
            "rebalance_threshold_pct": "80",
        },
    ),
    StrategyTemplate.LENDING_LOOP: TemplateConfig(
        name="Lending Loop",
        description="Supply/borrow leverage loop with state machine and health monitoring",
        default_protocol="aave_v3",
        config_params={
            "collateral_token": "WETH",
            "borrow_token": "USDC",
        },
    ),
    StrategyTemplate.BASIS_TRADE: TemplateConfig(
        name="Basis Trade",
        description="Spot+perp delta-neutral strategy capturing funding rate arbitrage",
        default_protocol="gmx_v2",
        config_params={
            "base_token": "WETH",
            "perp_market": "ETH/USD",
        },
    ),
    StrategyTemplate.VAULT_YIELD: TemplateConfig(
        name="Vault Yield",
        description="ERC-4626 vault deposit/redeem strategy for optimized DeFi lending yield",
        default_protocol="metamorpho",
        config_params={
            "vault_address": "0x_SET_VAULT_ADDRESS",
            "deposit_token": "USDC",
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
    StrategyTemplate.PERPS: TemplateConfig(
        name="Perps",
        description="Perpetual futures trading with take-profit and stop-loss levels",
        default_protocol="gmx_v2",
        config_params={
            "market": "ETH/USD",
            "collateral_token": "USDC",
        },
    ),
    StrategyTemplate.MULTI_STEP: TemplateConfig(
        name="Multi Step",
        description="Atomic multi-step operations using IntentSequence for LP rebalancing",
        default_protocol="uniswap_v3",
        config_params={
            "pool_address": "0x_SET_POOL_ADDRESS",
            "base_token": "WETH",
            "quote_token": "USDC",
        },
    ),
    StrategyTemplate.STAKING: TemplateConfig(
        name="Staking",
        description="Liquid staking strategy with optional token swap before staking",
        default_protocol="lido",
        config_params={
            "stake_token": "ETH",
            "stake_amount": "1",
        },
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
    if template == StrategyTemplate.TA_SWAP:
        return """
            indicator = getattr(self, '_indicator', 'rsi')

            # Get balances
            try:
                quote_balance = market.balance(self.quote_token)
                base_balance = market.balance(self.base_token)
            except ValueError as e:
                logger.warning(f"Could not get balances: {e}")
                return Intent.hold(reason="Balance data unavailable")

            buy_signal = False
            sell_signal = False
            reason = ""

            # RSI analysis
            if indicator in ("rsi", "rsi_bb"):
                try:
                    rsi = market.rsi(self.base_token, period=self.rsi_period)
                    if rsi.value <= self.rsi_oversold:
                        buy_signal = True
                        reason = f"RSI oversold ({rsi.value:.1f})"
                    elif rsi.value >= self.rsi_overbought:
                        sell_signal = True
                        reason = f"RSI overbought ({rsi.value:.1f})"
                    else:
                        reason = f"RSI neutral ({rsi.value:.1f})"
                except ValueError as e:
                    logger.warning(f"RSI unavailable: {e}")
                    return Intent.hold(reason="RSI data unavailable")

            # Bollinger Bands analysis
            if indicator in ("bollinger", "rsi_bb"):
                try:
                    bb = market.bollinger_bands(self.base_token, period=self.bb_period, std_dev=self.bb_std_dev)
                    if bb.bandwidth < self.squeeze_threshold:
                        return Intent.hold(reason=f"BB squeeze (bandwidth={bb.bandwidth:.4f})")
                    bb_buy = bb.percent_b <= self.buy_percent_b
                    bb_sell = bb.percent_b >= self.sell_percent_b
                    if indicator == "bollinger":
                        buy_signal = bb_buy
                        sell_signal = bb_sell
                        reason = f"%B={bb.percent_b:.4f}"
                    elif indicator == "rsi_bb":
                        buy_signal = buy_signal and bb_buy
                        sell_signal = sell_signal and bb_sell
                        reason += f", %B={bb.percent_b:.4f}"
                except ValueError as e:
                    logger.warning(f"BB unavailable: {e}")
                    if indicator == "bollinger":
                        return Intent.hold(reason="BB data unavailable")

            if buy_signal and quote_balance.balance_usd >= self.trade_size_usd:
                logger.info(f"BUY: {reason}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                )
            elif sell_signal:
                base_price = market.price(self.base_token)
                min_sell = self.trade_size_usd / base_price if base_price > 0 else Decimal("0")
                if base_balance.balance >= min_sell:
                    logger.info(f"SELL: {reason}")
                    return Intent.swap(
                        from_token=self.base_token,
                        to_token=self.quote_token,
                        amount_usd=self.trade_size_usd,
                        max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                    )

            return Intent.hold(reason=reason or "No signal")"""

    elif template == StrategyTemplate.DYNAMIC_LP:
        return """
            base_price = market.price(self.base_token)
            range_pct = Decimal(str(self.range_width_pct)) / Decimal("100")
            lower_price = base_price * (Decimal("1") - range_pct)
            upper_price = base_price * (Decimal("1") + range_pct)

            # If we have an open position, check if rebalance needed
            if self._position_id is not None:
                rebalance_pct = Decimal(str(self.rebalance_threshold_pct)) / Decimal("100")
                if self._range_lower and self._range_upper:
                    range_size = self._range_upper - self._range_lower
                    dist_from_lower = base_price - self._range_lower
                    position_in_range = dist_from_lower / range_size if range_size > 0 else Decimal("0.5")
                    lower_bound = (Decimal("1") - rebalance_pct) / Decimal("2")
                    upper_bound = (Decimal("1") + rebalance_pct) / Decimal("2")
                    if position_in_range < lower_bound or position_in_range > upper_bound:
                        logger.info(f"Rebalance needed: price {base_price} at {position_in_range:.1%} of range")
                        return Intent.lp_close(
                            position_id=self._position_id,
                            pool=self.pool_address,
                            collect_fees=True,
                            protocol="uniswap_v3",
                        )
                return Intent.hold(reason=f"LP position {self._position_id} in range")

            # No position -- open one
            try:
                quote_balance = market.balance(self.quote_token)
            except ValueError:
                return Intent.hold(reason="Cannot check balance")

            if quote_balance.balance_usd < self.min_position_usd:
                return Intent.hold(reason=f"Insufficient {self.quote_token} for LP")

            logger.info(f"Opening LP: {lower_price:.2f} - {upper_price:.2f}")
            return Intent.lp_open(
                pool=self.pool_address,
                amount0=quote_balance.balance * Decimal("0.45"),
                amount1=Decimal("0"),
                range_lower=lower_price,
                range_upper=upper_price,
                protocol="uniswap_v3",
            )"""

    elif template == StrategyTemplate.LENDING_LOOP:
        return """
            # State machine: idle -> supplied -> borrowed -> monitoring
            if self._loop_state == "idle":
                # Check collateral balance
                try:
                    collateral_bal = market.balance(self.collateral_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check collateral balance")

                if collateral_bal.balance_usd < self.min_collateral_usd:
                    return Intent.hold(reason=f"Insufficient {self.collateral_token}")

                logger.info(f"Supplying {self.supply_amount} {self.collateral_token}")
                return Intent.supply(
                    protocol="aave_v3",
                    token=self.collateral_token,
                    amount=self.supply_amount,
                    use_as_collateral=True,
                )

            elif self._loop_state == "supplied":
                # Borrow against collateral
                logger.info(f"Borrowing {self.borrow_amount} {self.borrow_token}")
                return Intent.borrow(
                    protocol="aave_v3",
                    collateral_token=self.collateral_token,
                    collateral_amount=Decimal("0"),
                    borrow_token=self.borrow_token,
                    borrow_amount=self.borrow_amount,
                )

            elif self._loop_state == "borrowed":
                # Swap borrowed tokens back to collateral for loop
                logger.info(f"Swapping {self.borrow_token} -> {self.collateral_token}")
                return Intent.swap(
                    from_token=self.borrow_token,
                    to_token=self.collateral_token,
                    amount="all",
                    max_slippage=Decimal("0.005"),
                )

            elif self._loop_state == "monitoring":
                # Monitor health factor -- would repay if too low
                return Intent.hold(reason=f"Monitoring loop, target_leverage={self.target_leverage}")

            return Intent.hold(reason=f"Unknown state: {self._loop_state}")"""

    elif template == StrategyTemplate.BASIS_TRADE:
        return """
            spot_price = market.price(self.base_token)

            if self._trade_state == "idle":
                try:
                    quote_balance = market.balance(self.quote_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check balance")

                if quote_balance.balance_usd < self.spot_size_usd:
                    return Intent.hold(reason=f"Insufficient {self.quote_token}")

                # Buy spot
                logger.info(f"Opening basis: buying {self.base_token} spot at {spot_price}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.spot_size_usd,
                    max_slippage=Decimal("0.005"),
                )

            elif self._trade_state == "spot_bought":
                # Hedge with short perp
                logger.info(f"Hedging: opening short perp on {self.perp_market}")
                return Intent.perp_open(
                    market=self.perp_market,
                    collateral_token=self.quote_token,
                    collateral_amount=self.spot_size_usd * Decimal("0.1"),
                    size_usd=self.spot_size_usd * self.hedge_ratio,
                    is_long=False,
                    leverage=Decimal("10"),
                    protocol="gmx_v2",
                )

            elif self._trade_state == "hedged":
                # Monitor position -- collect funding
                return Intent.hold(reason="Basis trade active, collecting funding")

            return Intent.hold(reason=f"Unknown state: {self._trade_state}")"""

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

    elif template == StrategyTemplate.VAULT_YIELD:
        return """
            # Check available balance for deposit
            try:
                balance_info = market.balance(self.deposit_token)
                available = balance_info.balance
                available_usd = balance_info.balance_usd
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not check {self.deposit_token} balance: {e}")
                return Intent.hold(reason=f"Balance unavailable: {e}")

            if self._state == "idle":
                if available_usd < self.min_deposit_usd:
                    return Intent.hold(
                        reason=f"Insufficient {self.deposit_token}: ${available_usd:.2f} < ${self.min_deposit_usd}"
                    )
                # Deposit into vault
                pct = max(0, min(self.max_vault_allocation_pct, 100))
                max_deposit = available * Decimal(str(pct)) / Decimal("100")
                deposit_amount = min(self.deposit_amount, max_deposit)
                logger.info(f"DEPOSIT: {deposit_amount} {self.deposit_token} into vault")
                return Intent.vault_deposit(
                    protocol="metamorpho",
                    vault_address=self.vault_address,
                    amount=deposit_amount,
                    chain=self.chain,
                )

            elif self._state == "deposited":
                # Hold position -- yield accrues passively in the vault
                return Intent.hold(reason="Vault position active, earning yield")

            else:
                return Intent.hold(reason=f"Unknown state: {self._state}")"""

    elif template == StrategyTemplate.PERPS:
        return """
            entry_price = market.price(self.base_token)

            if self._position_state == "idle":
                try:
                    collateral_bal = market.balance(self.collateral_token)
                except ValueError:
                    return Intent.hold(reason="Cannot check balance")

                if collateral_bal.balance_usd < self.collateral_amount:
                    return Intent.hold(reason=f"Insufficient {self.collateral_token}")

                # Simple momentum: open long
                logger.info(f"Opening long {self.perp_market} at {entry_price}")
                return Intent.perp_open(
                    market=self.perp_market,
                    collateral_token=self.collateral_token,
                    collateral_amount=self.collateral_amount,
                    size_usd=self.position_size_usd,
                    is_long=True,
                    leverage=self.leverage,
                    protocol="gmx_v2",
                )

            elif self._position_state == "open":
                # Check TP/SL
                if self._entry_price:
                    pnl_pct = (entry_price - self._entry_price) / self._entry_price
                    if pnl_pct >= self.take_profit_pct:
                        logger.info(f"Take profit hit: {pnl_pct:.2%}")
                        return Intent.perp_close(
                            market=self.perp_market,
                            collateral_token=self.collateral_token,
                            is_long=True,
                            size_usd=self.position_size_usd,
                            protocol="gmx_v2",
                        )
                    elif pnl_pct <= -self.stop_loss_pct:
                        logger.info(f"Stop loss hit: {pnl_pct:.2%}")
                        return Intent.perp_close(
                            market=self.perp_market,
                            collateral_token=self.collateral_token,
                            is_long=True,
                            size_usd=self.position_size_usd,
                            protocol="gmx_v2",
                        )
                msg = f"Position open, PnL: {pnl_pct:.2%}" if self._entry_price else "Position open"
                return Intent.hold(reason=msg)

            return Intent.hold(reason=f"Unknown state: {self._position_state}")"""

    elif template == StrategyTemplate.MULTI_STEP:
        return """
            base_price = market.price(self.base_token)

            # If we have a position, check for rebalance
            if self._position_id is not None:
                range_pct = Decimal(str(self.range_width_pct)) / Decimal("100")
                new_lower = base_price * (Decimal("1") - range_pct)
                new_upper = base_price * (Decimal("1") + range_pct)

                # Check if price moved enough to rebalance
                if self._range_lower and self._range_upper:
                    mid = (self._range_lower + self._range_upper) / Decimal("2")
                    drift = abs(base_price - mid) / mid
                    if drift < Decimal("0.03"):
                        return Intent.hold(reason=f"Position in range, drift={drift:.2%}")

                # Atomic rebalance: close -> swap -> reopen
                logger.info(f"Rebalancing LP around {base_price}")
                return Intent.sequence([
                    Intent.lp_close(
                        position_id=self._position_id,
                        pool=self.pool_address,
                        collect_fees=True,
                        protocol="uniswap_v3",
                    ),
                    Intent.swap(
                        from_token=self.base_token,
                        to_token=self.quote_token,
                        amount="all",
                        max_slippage=Decimal("0.005"),
                    ),
                    # Re-open LP with post-swap balances
                    # TODO: Replace with actual token balances after the swap completes.
                    # In production, use on_intent_executed() to track swap output,
                    # then issue a separate lp_open intent with the real amounts.
                    Intent.lp_open(
                        pool=self.pool_address,
                        amount0=market.balance(self.base_token).balance,
                        amount1=market.balance(self.quote_token).balance,
                        range_lower=new_lower,
                        range_upper=new_upper,
                        protocol="uniswap_v3",
                    ),
                ], description="Atomic LP rebalance")

            # No position: just hold, waiting for setup
            return Intent.hold(reason="No LP position -- open one manually or via config")"""

    elif template == StrategyTemplate.STAKING:
        return """
            if self._stake_state == "idle":
                try:
                    token_balance = market.balance(self.stake_token)
                except ValueError:
                    return Intent.hold(reason=f"Cannot check {self.stake_token} balance")

                if token_balance.balance < self.stake_amount:
                    # Not enough stake token -- swap quote to get it
                    if self.swap_before_stake:
                        try:
                            quote_bal = market.balance(self.quote_token)
                        except ValueError:
                            return Intent.hold(reason=f"Cannot check {self.quote_token} balance")
                        stake_price = market.price(self.stake_token)
                        if stake_price <= 0:
                            return Intent.hold(reason=f"Invalid {self.stake_token} price: {stake_price}")
                        needed_usd = self.stake_amount * stake_price
                        if needed_usd > 0 and quote_bal.balance_usd >= needed_usd:
                            logger.info(f"Swapping {self.quote_token} -> {self.stake_token}")
                            return Intent.swap(
                                from_token=self.quote_token,
                                to_token=self.stake_token,
                                amount_usd=needed_usd,
                                max_slippage=Decimal("0.005"),
                            )
                    return Intent.hold(reason=f"Insufficient {self.stake_token}")

                logger.info(f"Staking {self.stake_amount} {self.stake_token}")
                return Intent.stake(
                    protocol=self.staking_protocol,
                    token_in=self.stake_token,
                    amount=self.stake_amount,
                )

            elif self._stake_state == "staked":
                return Intent.hold(reason="Staked, earning yield")

            return Intent.hold(reason=f"Unknown state: {self._stake_state}")"""

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
        StrategyTemplate.BLANK: "Swap all holdings back to quote token",
        StrategyTemplate.TA_SWAP: "Swap all holdings back to quote token",
        StrategyTemplate.DYNAMIC_LP: "Close LP position, then swap tokens to quote",
        StrategyTemplate.LENDING_LOOP: "Repay borrows, withdraw collateral, swap to quote",
        StrategyTemplate.BASIS_TRADE: "Close perp position, then swap to quote",
        StrategyTemplate.VAULT_YIELD: "Redeem all vault shares back to underlying token",
        StrategyTemplate.COPY_TRADER: "Close all copied positions in reverse order",
        StrategyTemplate.PERPS: "Close all perp positions",
        StrategyTemplate.MULTI_STEP: "Close LP position, swap back to quote",
        StrategyTemplate.STAKING: "Unstake and optionally swap back to quote",
    }
    return hints.get(template, "Close all positions and convert to stable")


def _get_template_init_params(template: StrategyTemplate, config: TemplateConfig) -> str:
    """Generate template-specific __init__ parameter extraction."""
    if template == StrategyTemplate.TA_SWAP:
        return """
        # Indicator mode: "rsi", "bollinger", or "rsi_bb" (combined)
        self._indicator = get_config("indicator", "rsi")

        # RSI parameters
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", "70")))

        # Bollinger Bands parameters
        self.bb_period = int(get_config("bb_period", 20))
        self.bb_std_dev = float(get_config("bb_std_dev", 2.0))
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
        self.pool_address = get_config("pool_address", "0x_SET_POOL_ADDRESS")
        self.range_width_pct = float(get_config("range_width_pct", 5))
        self.rebalance_threshold_pct = float(get_config("rebalance_threshold_pct", 80))
        self.min_position_usd = Decimal(str(get_config("min_position_usd", "500")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")

        # Position tracking (restored via load_persistent_state)
        self._position_id = None
        self._range_lower = None
        self._range_upper = None"""

    elif template == StrategyTemplate.LENDING_LOOP:
        return '''
        # Lending parameters
        self.supply_amount = Decimal(str(get_config("supply_amount", "1")))
        self.borrow_amount = Decimal(str(get_config("borrow_amount", "500")))
        self.target_leverage = Decimal(str(get_config("target_leverage", "2.0")))
        self.min_collateral_usd = Decimal(str(get_config("min_collateral_usd", "100")))

        # Token configuration
        self.collateral_token = get_config("collateral_token", "WETH")
        self.borrow_token = get_config("borrow_token", "USDC")

        # State machine: idle -> supplied -> borrowed -> monitoring
        self._loop_state = "idle"'''

    elif template == StrategyTemplate.BASIS_TRADE:
        return '''
        # Basis trade parameters
        self.spot_size_usd = Decimal(str(get_config("spot_size_usd", "10000")))
        self.hedge_ratio = Decimal(str(get_config("hedge_ratio", "1.0")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")
        self.perp_market = get_config("perp_market", "ETH/USD")

        # State machine: idle -> spot_bought -> hedged
        self._trade_state = "idle"'''

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

    elif template == StrategyTemplate.VAULT_YIELD:
        return '''
        # Vault parameters
        self.vault_address = get_config("vault_address", "0x_SET_VAULT_ADDRESS")
        self.deposit_token = get_config("deposit_token", "USDC")
        self.deposit_amount = Decimal(str(get_config("deposit_amount", "1000")))
        self.min_deposit_usd = Decimal(str(get_config("min_deposit_usd", "100")))
        self.max_vault_allocation_pct = int(get_config("max_vault_allocation_pct", 80))

        # State
        self._state = "idle"'''

    elif template == StrategyTemplate.PERPS:
        return """
        # Perps parameters
        self.perp_market = get_config("perp_market", "ETH/USD")
        self.collateral_token = get_config("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(get_config("collateral_amount", "100")))
        self.position_size_usd = Decimal(str(get_config("position_size_usd", "1000")))
        self.leverage = Decimal(str(get_config("leverage", "5")))
        self.take_profit_pct = Decimal(str(get_config("take_profit_pct", "0.05")))
        self.stop_loss_pct = Decimal(str(get_config("stop_loss_pct", "0.03")))

        # Token for price checks
        self.base_token = get_config("base_token", "ETH")

        # Position tracking (restored via load_persistent_state)
        self._position_state = "idle"
        self._entry_price = None"""

    elif template == StrategyTemplate.MULTI_STEP:
        return """
        # Multi-step LP parameters
        self.pool_address = get_config("pool_address", "0x_SET_POOL_ADDRESS")
        self.range_width_pct = float(get_config("range_width_pct", 5))
        self.min_position_usd = Decimal(str(get_config("min_position_usd", "500")))

        # Token configuration
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")

        # Position tracking
        self._position_id = None
        self._range_lower = None
        self._range_upper = None"""

    elif template == StrategyTemplate.STAKING:
        return '''
        # Staking parameters (stake_amount is the canonical amount)
        self.stake_token = get_config("stake_token", "ETH")
        self.stake_amount = Decimal(str(get_config("stake_amount", "1")))
        self.staking_protocol = get_config("staking_protocol", "lido")
        self.quote_token = get_config("quote_token", "USDC")
        self.swap_before_stake = get_config("swap_before_stake", True)

        # State tracking
        self._stake_state = "idle"'''

    else:  # BLANK template
        return """
        # Add your configuration parameters here
        # Example:
        # self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "100")))
        pass"""


def _get_template_callbacks(template: StrategyTemplate) -> str:
    """Generate on_intent_executed and persistence callbacks for stateful templates."""
    if template == StrategyTemplate.DYNAMIC_LP:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track LP position after open/close."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "LP_OPEN" and result:\n'
            "            self._position_id = getattr(result, 'position_id', None)\n"
            "            self._range_lower = getattr(intent, 'range_lower', None)\n"
            "            self._range_upper = getattr(intent, 'range_upper', None)\n"
            '            logger.info(f"LP opened: position_id={self._position_id}")\n'
            '        elif intent_type and intent_type.value == "LP_CLOSE":\n'
            "            self._position_id = None\n"
            "            self._range_lower = None\n"
            "            self._range_upper = None\n"
            '            logger.info("LP closed")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save position state for crash recovery."""\n'
            "        return {\n"
            '            "position_id": self._position_id,\n'
            '            "range_lower": str(self._range_lower) if self._range_lower else None,\n'
            '            "range_upper": str(self._range_upper) if self._range_upper else None,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore position state after restart."""\n'
            "        if state:\n"
            '            self._position_id = state.get("position_id")\n'
            '            rl = state.get("range_lower")\n'
            '            ru = state.get("range_upper")\n'
            "            self._range_lower = Decimal(rl) if rl else None\n"
            "            self._range_upper = Decimal(ru) if ru else None\n"
            "\n"
        )

    elif template == StrategyTemplate.LENDING_LOOP:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Advance state machine after intent execution."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "SUPPLY":\n'
            '            self._loop_state = "supplied"\n'
            '            logger.info("Supply confirmed -> supplied")\n'
            '        elif intent_type.value == "BORROW":\n'
            '            self._loop_state = "borrowed"\n'
            '            logger.info("Borrow confirmed -> borrowed")\n'
            '        elif intent_type.value == "SWAP":\n'
            '            self._loop_state = "monitoring"\n'
            '            logger.info("Swap confirmed -> monitoring")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save loop state."""\n'
            '        return {"loop_state": self._loop_state}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore loop state."""\n'
            "        if state:\n"
            '            self._loop_state = state.get("loop_state", "idle")\n'
            "\n"
        )

    elif template == StrategyTemplate.BASIS_TRADE:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Advance basis trade state machine."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "SWAP" and self._trade_state == "idle":\n'
            '            self._trade_state = "spot_bought"\n'
            '            logger.info("Spot bought -> spot_bought")\n'
            '        elif intent_type.value == "PERP_OPEN":\n'
            '            self._trade_state = "hedged"\n'
            '            logger.info("Perp opened -> hedged")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save trade state."""\n'
            '        return {"trade_state": self._trade_state}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore trade state."""\n'
            "        if state:\n"
            '            self._trade_state = state.get("trade_state", "idle")\n'
            "\n"
        )

    elif template == StrategyTemplate.VAULT_YIELD:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Update vault state after deposit/redeem."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "VAULT_DEPOSIT":\n'
            '            self._state = "deposited"\n'
            '            logger.info("Vault deposit confirmed -> deposited")\n'
            '        elif intent_type and intent_type.value == "VAULT_REDEEM":\n'
            '            self._state = "idle"\n'
            '            logger.info("Vault redeem confirmed -> idle")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save vault state."""\n'
            '        return {"state": self._state}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore vault state."""\n'
            "        if state:\n"
            '            self._state = state.get("state", "idle")\n'
            "\n"
        )

    elif template == StrategyTemplate.PERPS:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track perp position state."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "PERP_OPEN":\n'
            '            self._position_state = "open"\n'
            "            self._entry_price = getattr(result, 'entry_price', None)\n"
            '            logger.info(f"Perp opened at {self._entry_price}")\n'
            '        elif intent_type.value == "PERP_CLOSE":\n'
            '            self._position_state = "idle"\n'
            "            self._entry_price = None\n"
            '            logger.info("Perp closed -> idle")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save perp state."""\n'
            "        return {\n"
            '            "position_state": self._position_state,\n'
            '            "entry_price": str(self._entry_price) if self._entry_price else None,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore perp state."""\n'
            "        if state:\n"
            '            self._position_state = state.get("position_state", "idle")\n'
            '            ep = state.get("entry_price")\n'
            "            self._entry_price = Decimal(ep) if ep else None\n"
            "\n"
        )

    elif template == StrategyTemplate.STAKING:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track staking state."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            "        if not intent_type:\n"
            "            return\n"
            '        if intent_type.value == "STAKE":\n'
            '            self._stake_state = "staked"\n'
            '            logger.info("Staked -> staked")\n'
            '        elif intent_type.value == "UNSTAKE":\n'
            '            self._stake_state = "idle"\n'
            '            logger.info("Unstaked -> idle")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save stake state."""\n'
            '        return {"stake_state": self._stake_state}\n'
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore stake state."""\n'
            "        if state:\n"
            '            self._stake_state = state.get("stake_state", "idle")\n'
            "\n"
        )

    elif template == StrategyTemplate.MULTI_STEP:
        return (
            "    def on_intent_executed(self, intent, success: bool, result):\n"
            '        """Track LP position after open/close in multi-step sequence."""\n'
            "        if not success:\n"
            "            return\n"
            '        intent_type = getattr(intent, "intent_type", None)\n'
            '        if intent_type and intent_type.value == "LP_OPEN" and result:\n'
            "            self._position_id = getattr(result, 'position_id', None)\n"
            "            self._range_lower = getattr(intent, 'range_lower', None)\n"
            "            self._range_upper = getattr(intent, 'range_upper', None)\n"
            '            logger.info(f"LP opened: position_id={self._position_id}")\n'
            '        elif intent_type and intent_type.value == "LP_CLOSE":\n'
            "            self._position_id = None\n"
            "            self._range_lower = None\n"
            "            self._range_upper = None\n"
            '            logger.info("LP closed")\n'
            "\n"
            "    def get_persistent_state(self):\n"
            '        """Save position state for crash recovery."""\n'
            "        return {\n"
            '            "position_id": self._position_id,\n'
            '            "range_lower": str(self._range_lower) if self._range_lower else None,\n'
            '            "range_upper": str(self._range_upper) if self._range_upper else None,\n'
            "        }\n"
            "\n"
            "    def load_persistent_state(self, state):\n"
            '        """Restore position state after restart."""\n'
            "        if state:\n"
            '            self._position_id = state.get("position_id")\n'
            '            rl = state.get("range_lower")\n'
            '            ru = state.get("range_upper")\n'
            "            self._range_lower = Decimal(rl) if rl else None\n"
            "            self._range_upper = Decimal(ru) if ru else None\n"
            "\n"
        )

    # Templates without state callbacks (BLANK, TA_SWAP, COPY_TRADER)
    return ""


def _build_strategy_content(
    name: str,
    template: StrategyTemplate,
    chain: SupportedChain,
    output_dir: Path,
) -> str:
    """Build the strategy.py file content for v2 IntentStrategy."""
    class_name = to_pascal_case(name) + "Strategy"
    strategy_name = to_snake_case(name)
    config = TEMPLATE_CONFIGS[template]

    # Get template-specific code
    init_params = _get_template_init_params(template, config)
    decide_logic = _get_template_decide_logic(template, config)
    callbacks_str = _get_template_callbacks(template)

    # Determine intent types based on template
    intent_types = {
        StrategyTemplate.BLANK: '["SWAP", "HOLD"]',
        StrategyTemplate.TA_SWAP: '["SWAP", "HOLD"]',
        StrategyTemplate.DYNAMIC_LP: '["LP_OPEN", "LP_CLOSE", "HOLD"]',
        StrategyTemplate.LENDING_LOOP: '["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"]',
        StrategyTemplate.BASIS_TRADE: '["SWAP", "PERP_OPEN", "PERP_CLOSE", "HOLD"]',
        StrategyTemplate.VAULT_YIELD: '["VAULT_DEPOSIT", "VAULT_REDEEM", "HOLD"]',
        StrategyTemplate.COPY_TRADER: (
            '[\n        "SWAP", "LP_OPEN", "LP_CLOSE", "SUPPLY", "WITHDRAW",\n'
            '        "BORROW", "REPAY", "PERP_OPEN", "PERP_CLOSE", "HOLD",\n    ]'
        ),
        StrategyTemplate.PERPS: '["PERP_OPEN", "PERP_CLOSE", "HOLD"]',
        StrategyTemplate.MULTI_STEP: '["LP_OPEN", "LP_CLOSE", "SWAP", "HOLD"]',
        StrategyTemplate.STAKING: '["STAKE", "UNSTAKE", "SWAP", "HOLD"]',
    }

    teardown_comment = _get_teardown_comment(template)

    part1 = f'''"""
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
from decimal import Decimal  # noqa: F401
from typing import Any, Optional

# Core strategy framework imports
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

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

'''

    part2 = f'''    # -------------------------------------------------------------------------
    # TEARDOWN (required) - implement so operators can safely close positions
    # Without these methods, operator close-requests are silently ignored.
    # See: blueprints/14-teardown-system.md
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown."""
        return True

    def get_open_positions(self):
        """Return all open positions for teardown preview.

        IMPORTANT: Query on-chain state here, not cached values.
        The framework calls this to show operators what will be closed.
        """
        from datetime import UTC, datetime

        from almanak.framework.teardown import (
            PositionInfo,  # noqa: F401
            PositionType,  # noqa: F401
            TeardownPositionSummary,
        )

        positions = []

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

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        """Generate intents to close all positions.

        Teardown goal: {teardown_comment}

        Args:
            mode: TeardownMode.SOFT (normal slippage) or TeardownMode.HARD (emergency, 3% slippage)
        """
        from almanak.framework.teardown import TeardownMode  # noqa: F401

        intents: list[Intent] = []

        # max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")

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

    return part1 + callbacks_str + part2


def generate_strategy_file(
    name: str,
    template: StrategyTemplate,
    chain: SupportedChain,
    output_dir: Path,
) -> str:
    """Generate the main strategy.py file content for v2 IntentStrategy."""
    return _build_strategy_content(name, template, chain, output_dir)


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
    if template == StrategyTemplate.TA_SWAP:
        data.update(
            {
                "indicator": "rsi",
                "base_token": "WETH",
                "quote_token": "USDC",
                "rsi_period": 14,
                "rsi_oversold": 30,
                "rsi_overbought": 70,
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
                "pool_address": "0x_SET_POOL_ADDRESS",
                "base_token": "WETH",
                "quote_token": "USDC",
                "range_width_pct": 5,
                "rebalance_threshold_pct": 80,
                "min_position_usd": 500,
            }
        )
    elif template == StrategyTemplate.LENDING_LOOP:
        data.update(
            {
                "collateral_token": "WETH",
                "borrow_token": "USDC",
                "supply_amount": 1,
                "borrow_amount": 500,
                "target_leverage": 2.0,
                "min_collateral_usd": 100,
            }
        )
    elif template == StrategyTemplate.BASIS_TRADE:
        data.update(
            {
                "base_token": "WETH",
                "quote_token": "USDC",
                "perp_market": "ETH/USD",
                "spot_size_usd": 10000,
                "hedge_ratio": 1.0,
            }
        )
    elif template == StrategyTemplate.VAULT_YIELD:
        data.update(
            {
                "vault_address": "0x_SET_VAULT_ADDRESS",
                "deposit_token": "USDC",
                "deposit_amount": 1000,
                "min_deposit_usd": 100,
                "max_vault_allocation_pct": 80,
            }
        )
    elif template == StrategyTemplate.COPY_TRADER:
        data.update(
            {
                "copy_trading": {
                    "leaders": [{"address": "0x_LEADER_WALLET_ADDRESS", "chain": chain.value}],
                    "sizing": {"mode": "fixed_usd", "fixed_usd": 100},
                    "risk": {"max_trade_usd": 1000, "max_slippage": 0.01},
                },
            }
        )
    elif template == StrategyTemplate.PERPS:
        data.update(
            {
                "perp_market": "ETH/USD",
                "collateral_token": "USDC",
                "collateral_amount": 100,
                "position_size_usd": 1000,
                "leverage": 5,
                "take_profit_pct": 0.05,
                "stop_loss_pct": 0.03,
                "base_token": "ETH",
            }
        )
    elif template == StrategyTemplate.MULTI_STEP:
        data.update(
            {
                "pool_address": "0x_SET_POOL_ADDRESS",
                "base_token": "WETH",
                "quote_token": "USDC",
                "range_width_pct": 5,
                "min_position_usd": 500,
            }
        )
    elif template == StrategyTemplate.STAKING:
        data.update(
            {
                "stake_token": "ETH",
                "stake_amount": 1,
                "staking_protocol": "lido",
                "quote_token": "USDC",
                "swap_before_stake": True,
            }
        )
    # BLANK: empty config

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


def generate_pyproject_toml(
    name: str,
) -> str:
    """Generate pyproject.toml for a self-contained strategy Python project.

    The generated file is a lean manifest for the hosted platform.
    The platform handles lockfile generation during cloud Docker builds.
    """
    from almanak._version import __version__

    snake_name = to_snake_case(name)

    return f"""[project]
name = "{snake_name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "almanak>={__version__}",
]

[tool.almanak.run]
interval = 60
"""


def generate_gitignore() -> str:
    """Generate .gitignore for a strategy directory."""
    return """.venv/
__pycache__/
*.pyc
.env
*.db
*.db-journal
.pytest_cache/
.coverage
dist/
build/
*.egg-info/
.DS_Store
"""


def generate_python_version() -> str:
    """Generate .python-version file matching the Dockerfile base image."""
    return "3.12\n"


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

        almanak new-strategy -t ta_swap -n rsi_trader -c ethereum
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

        # pyproject.toml
        pyproject_file = strategy_dir / "pyproject.toml"
        pyproject_content = generate_pyproject_toml(name)
        with open(pyproject_file, "w") as fh:
            fh.write(pyproject_content)
        files_created.append("pyproject.toml")

        # .python-version
        python_version_file = strategy_dir / ".python-version"
        with open(python_version_file, "w") as fh:
            fh.write(generate_python_version())
        files_created.append(".python-version")

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

        # .gitignore
        gitignore_file = strategy_dir / ".gitignore"
        with open(gitignore_file, "w") as fh:
            fh.write(generate_gitignore())
        files_created.append(".gitignore")

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
        click.echo()
        click.echo(f"Created strategy '{snake_name}' in {strategy_dir}")
        click.echo()
        click.echo("Files:")
        click.echo("  strategy.py          - Strategy implementation")
        click.echo("  config.json          - Runtime configuration")
        click.echo("  pyproject.toml       - Dependencies and metadata")
        click.echo("  .env                 - Environment variables (edit this)")
        click.echo("  .gitignore           - Git ignore rules")
        click.echo("  AGENTS.md            - AI agent guide")
        click.echo("  tests/               - Test scaffold")
        click.echo()
        click.echo("Next steps:")
        click.echo(f"  cd {strategy_dir}")
        click.echo("  almanak strat run --once --dry-run")

    except Exception as e:
        click.echo(f"Error creating strategy: {e}", err=True)
        # Clean up on failure
        if strategy_dir.exists():
            import shutil

            shutil.rmtree(strategy_dir)
        raise click.Abort() from e


if __name__ == "__main__":
    new_strategy()
