"""Cross-DEX Spot Arbitrage Strategy - Atomic arbitrage using flash loans.

This strategy identifies price differences across DEXs (Uniswap V3, Curve, Enso)
and executes atomic arbitrage trades using flash loans for capital efficiency.

Key Features:
    - Multi-DEX price comparison using MultiDexPriceService
    - Flash loan execution for capital-free arbitrage
    - Automatic provider selection (Aave/Balancer)
    - Configurable profit thresholds and gas limits
    - Support for multiple token pairs

Example:
    If USDC/WETH price differs between Uniswap V3 and Curve:
    1. Flash loan USDC from Balancer (0% fee)
    2. Swap USDC -> WETH on cheaper DEX
    3. Swap WETH -> USDC on expensive DEX
    4. Repay flash loan, keep profit

    All steps execute atomically - if any step fails, entire trade reverts.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.connectors.flash_loan import (
    FlashLoanSelectionResult,
    FlashLoanSelector,
    SelectionPriority,
)
from almanak.framework.intents import Intent, IntentCompiler, StateMachineConfig
from almanak.framework.intents.vocabulary import (
    DecideResult,
    FlashLoanCallbackIntent,
    FlashLoanIntent,
)
from almanak.framework.strategies import (
    BalanceProvider,
    IntentStrategy,
    MarketSnapshot,
    NotificationCallback,
    PriceOracle,
    RiskGuardConfig,
    RSIProvider,
    almanak_strategy,
)
from almanak.gateway.data.price import (
    DexQuote,
    MultiDexPriceService,
)

from .config import CrossDexArbConfig

logger = logging.getLogger(__name__)


class ArbState(str, Enum):
    """State of the arbitrage strategy."""

    SCANNING = "scanning"  # Looking for opportunities
    OPPORTUNITY_FOUND = "opportunity_found"  # Found profitable opportunity
    EXECUTING = "executing"  # Executing flash loan arbitrage
    COOLDOWN = "cooldown"  # Waiting after trade


@dataclass
class ArbitrageOpportunity:
    """Represents a cross-DEX arbitrage opportunity.

    Attributes:
        token_in: Input token for arbitrage
        token_out: Output token for arbitrage
        amount_in: Flash loan amount
        buy_dex: DEX to buy from (cheaper)
        sell_dex: DEX to sell to (expensive)
        buy_quote: Quote from buy DEX
        sell_quote: Quote from sell DEX
        gross_profit_bps: Gross profit in basis points
        gross_profit_usd: Gross profit in USD
        net_profit_usd: Net profit after gas
        flash_loan_provider: Flash loan provider to use
        flash_loan_fee: Flash loan fee amount
        timestamp: When opportunity was found
    """

    token_in: str
    token_out: str
    amount_in: Decimal
    buy_dex: str
    sell_dex: str
    buy_quote: DexQuote
    sell_quote: DexQuote
    gross_profit_bps: int
    gross_profit_usd: Decimal
    net_profit_usd: Decimal
    flash_loan_provider: str
    flash_loan_fee: Decimal
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "buy_dex": self.buy_dex,
            "sell_dex": self.sell_dex,
            "buy_output": str(self.buy_quote.amount_out),
            "sell_output": str(self.sell_quote.amount_out),
            "gross_profit_bps": self.gross_profit_bps,
            "gross_profit_usd": str(self.gross_profit_usd),
            "net_profit_usd": str(self.net_profit_usd),
            "flash_loan_provider": self.flash_loan_provider,
            "flash_loan_fee": str(self.flash_loan_fee),
            "timestamp": self.timestamp.isoformat(),
        }


@almanak_strategy(
    name="cross_dex_arb",
    description="Cross-DEX spot arbitrage using flash loans",
    version="1.0.0",
    author="Almanak",
    tags=["arbitrage", "flash_loan", "dex", "atomic", "defi"],
    supported_chains=["ethereum", "arbitrum", "optimism", "polygon", "base"],
    supported_protocols=["uniswap_v3", "curve", "enso", "aave_v3", "balancer"],
    intent_types=["FLASH_LOAN", "SWAP", "HOLD"],
)
class CrossDexArbStrategy(IntentStrategy[CrossDexArbConfig]):
    """Cross-DEX Spot Arbitrage Strategy using Intent pattern.

    This strategy scans for price discrepancies across DEXs and executes
    atomic arbitrage using flash loans when profitable opportunities are found.

    Key Simplifications:
    - No manual state machine - framework handles execution flow
    - No action bundle construction - IntentCompiler handles TX building
    - Only implements decide() with core business logic

    Arbitrage Flow:
    1. Scan token pairs for price differences across DEXs
    2. Calculate profitability after gas and flash loan fees
    3. Execute if profit > threshold:
       - Flash loan input token
       - Swap on cheaper DEX to get intermediate token
       - Swap on expensive DEX to get more input token back
       - Repay flash loan + fee
       - Keep profit
    """

    STRATEGY_NAME = "cross_dex_arb"

    def __init__(
        self,
        config: CrossDexArbConfig,
        risk_guard_config: RiskGuardConfig | None = None,
        notification_callback: NotificationCallback | None = None,
        compiler: IntentCompiler | None = None,
        state_machine_config: StateMachineConfig | None = None,
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
        price_service: MultiDexPriceService | None = None,
        flash_loan_selector: FlashLoanSelector | None = None,
    ) -> None:
        """Initialize the Cross-DEX Arbitrage strategy.

        Args:
            config: Strategy configuration
            risk_guard_config: Risk management configuration
            notification_callback: Callback for notifications
            compiler: Intent compiler
            state_machine_config: State machine configuration
            price_oracle: Price data provider
            rsi_provider: RSI data provider
            balance_provider: Balance data provider
            price_service: Multi-DEX price service for quotes
            flash_loan_selector: Flash loan provider selector
        """
        super().__init__(
            config=config,
            chain=config.chain,
            wallet_address=config.wallet_address,
            risk_guard_config=risk_guard_config,
            notification_callback=notification_callback,
            compiler=compiler,
            state_machine_config=state_machine_config,
            price_oracle=price_oracle,
            rsi_provider=rsi_provider,
            balance_provider=balance_provider,
        )
        # Price service for cross-DEX quotes
        self._price_service = price_service or MultiDexPriceService(
            chain=config.chain,
            cache_ttl_seconds=config.opportunity_cache_seconds,
            dexs=config.dexs,
        )
        # Flash loan selector - convert string priority to enum
        try:
            priority = SelectionPriority(config.flash_loan_priority)
        except ValueError:
            priority = SelectionPriority.FEE
        self._flash_loan_selector = flash_loan_selector or FlashLoanSelector(
            chain=config.chain,
            default_priority=priority,
        )
        # State tracking
        self._state = ArbState.SCANNING
        self._current_opportunity: ArbitrageOpportunity | None = None

    def _run_async(self, coro: Any) -> Any:
        """Helper to run async code synchronously.

        Args:
            coro: Coroutine to run

        Returns:
            Result of the coroutine
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, coro)
                    return future.result()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)

    def decide(self, market: MarketSnapshot) -> DecideResult:
        """Decide the next action based on current market state.

        This method:
        1. Checks if strategy is paused or in cooldown
        2. Scans for arbitrage opportunities
        3. Executes flash loan arbitrage if profitable

        Returns:
            Intent to execute or hold
        """
        if self.config.pause_strategy:
            return Intent.hold(reason="Strategy paused")

        # Check cooldown
        if not self._can_trade():
            remaining = self._cooldown_remaining()
            return Intent.hold(reason=f"Trade cooldown: {remaining}s remaining")

        # Update state
        self._update_state()

        # State-based decision making
        if self._state == ArbState.SCANNING:
            return self._handle_scanning(market)
        elif self._state == ArbState.OPPORTUNITY_FOUND:
            return self._handle_opportunity(market)
        elif self._state == ArbState.COOLDOWN:
            return Intent.hold(reason="In cooldown period")

        return Intent.hold(reason="Unknown state")

    def _can_trade(self) -> bool:
        """Check if trading is allowed based on cooldown.

        Returns:
            True if trading is allowed
        """
        if self.config.last_trade_timestamp is None:
            return True

        elapsed = int(time.time()) - self.config.last_trade_timestamp
        return elapsed >= self.config.trade_cooldown_seconds

    def _cooldown_remaining(self) -> int:
        """Get remaining cooldown time in seconds.

        Returns:
            Seconds remaining in cooldown
        """
        if self.config.last_trade_timestamp is None:
            return 0

        elapsed = int(time.time()) - self.config.last_trade_timestamp
        remaining = self.config.trade_cooldown_seconds - elapsed
        return max(0, remaining)

    def _update_state(self) -> None:
        """Update strategy state based on current conditions."""
        if self._current_opportunity is not None:
            # Check if opportunity is still fresh
            age = (datetime.now(UTC) - self._current_opportunity.timestamp).total_seconds()
            if age > self.config.opportunity_cache_seconds:
                logger.info("Opportunity expired, returning to scanning")
                self._current_opportunity = None
                self._state = ArbState.SCANNING
            else:
                self._state = ArbState.OPPORTUNITY_FOUND
        elif not self._can_trade():
            self._state = ArbState.COOLDOWN
        else:
            self._state = ArbState.SCANNING

    def _handle_scanning(self, market: MarketSnapshot) -> DecideResult:
        """Handle scanning state - look for arbitrage opportunities.

        Args:
            market: Current market snapshot

        Returns:
            Intent to execute or hold
        """
        logger.debug("Scanning for arbitrage opportunities...")

        # Scan all token pairs
        opportunity = self._find_best_opportunity()

        if opportunity is None:
            return Intent.hold(reason="No profitable arbitrage opportunity found")

        # Found opportunity - store and proceed
        self._current_opportunity = opportunity
        self._state = ArbState.OPPORTUNITY_FOUND
        self.config.last_opportunity_found = (
            f"{opportunity.token_in}/{opportunity.token_out} "
            f"{opportunity.buy_dex}->{opportunity.sell_dex} "
            f"+{opportunity.gross_profit_bps}bps"
        )

        logger.info(
            f"Found arbitrage opportunity: "
            f"{opportunity.token_in}->{opportunity.token_out}, "
            f"buy on {opportunity.buy_dex}, sell on {opportunity.sell_dex}, "
            f"profit: {opportunity.gross_profit_bps}bps (${opportunity.net_profit_usd:.2f} net)"
        )

        return self._create_arbitrage_intent(opportunity)

    def _handle_opportunity(self, market: MarketSnapshot) -> DecideResult:
        """Handle opportunity found state - execute arbitrage.

        Args:
            market: Current market snapshot

        Returns:
            Intent to execute
        """
        if self._current_opportunity is None:
            self._state = ArbState.SCANNING
            return Intent.hold(reason="Opportunity expired")

        return self._create_arbitrage_intent(self._current_opportunity)

    def _find_best_opportunity(self) -> ArbitrageOpportunity | None:
        """Find the best arbitrage opportunity across all token pairs.

        Returns:
            Best opportunity or None if none profitable
        """
        best_opportunity: ArbitrageOpportunity | None = None
        best_profit_usd = Decimal("0")

        tokens = self.config.tokens

        # Check each token pair
        for i, token_in in enumerate(tokens):
            for token_out in tokens[i + 1 :]:
                # Check both directions
                for t_in, t_out in [(token_in, token_out), (token_out, token_in)]:
                    opportunity = self._check_opportunity(t_in, t_out)
                    if opportunity and opportunity.net_profit_usd > best_profit_usd:
                        best_opportunity = opportunity
                        best_profit_usd = opportunity.net_profit_usd

        return best_opportunity

    def _check_opportunity(self, token_in: str, token_out: str) -> ArbitrageOpportunity | None:
        """Check for arbitrage opportunity between two tokens.

        Args:
            token_in: Input token
            token_out: Output token

        Returns:
            ArbitrageOpportunity if profitable, None otherwise
        """
        try:
            # Get prices from all DEXs
            prices = self._run_async(
                self._price_service.get_prices_across_dexs(
                    token_in=token_in,
                    token_out=token_out,
                    amount_in=self.config.default_trade_size_usd,
                )
            )

            if not prices.quotes or len(prices.quotes) < 2:
                return None

            # Find best buy (highest output) and best sell (for reverse)
            quotes = list(prices.quotes.values())
            best_buy = max(quotes, key=lambda q: q.amount_out)
            worst_buy = min(quotes, key=lambda q: q.amount_out)

            # Calculate spread
            if worst_buy.amount_out == 0:
                return None

            spread_bps = int((best_buy.amount_out - worst_buy.amount_out) / worst_buy.amount_out * 10000)

            # Check if spread is profitable
            if spread_bps < self.config.min_profit_bps:
                return None

            # Get reverse quotes to complete the arbitrage cycle
            reverse_prices = self._run_async(
                self._price_service.get_prices_across_dexs(
                    token_in=token_out,
                    token_out=token_in,
                    amount_in=best_buy.amount_out,
                )
            )

            if not reverse_prices.quotes:
                return None

            # Find best reverse swap
            reverse_quotes = list(reverse_prices.quotes.values())
            best_sell = max(reverse_quotes, key=lambda q: q.amount_out)

            # Skip if buy and sell DEX are the same (no arb)
            if best_buy.dex == best_sell.dex:
                # Find second best sell
                other_sells = [q for q in reverse_quotes if q.dex != best_buy.dex]
                if not other_sells:
                    return None
                best_sell = max(other_sells, key=lambda q: q.amount_out)

            # Calculate actual profit
            amount_in = self.config.default_trade_size_usd
            final_amount = best_sell.amount_out

            # Get flash loan info
            flash_loan_result = self._get_flash_loan_info(token_in, amount_in)
            if flash_loan_result is None:
                return None

            # Calculate profit
            total_repay = amount_in + flash_loan_result.fee_amount
            gross_profit = final_amount - total_repay
            gross_profit_bps = int(gross_profit / amount_in * 10000)

            # Estimate USD profit (assuming stablecoin or convert)
            gross_profit_usd = self._estimate_usd_value(gross_profit, token_in)
            net_profit_usd = gross_profit_usd - self.config.estimated_gas_cost_usd

            # Check profitability
            if not self.config.is_profitable(gross_profit_usd, gross_profit_bps):
                return None

            return ArbitrageOpportunity(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                buy_dex=best_buy.dex,
                sell_dex=best_sell.dex,
                buy_quote=best_buy,
                sell_quote=best_sell,
                gross_profit_bps=gross_profit_bps,
                gross_profit_usd=gross_profit_usd,
                net_profit_usd=net_profit_usd,
                flash_loan_provider=flash_loan_result.provider or "aave",
                flash_loan_fee=flash_loan_result.fee_amount,
                timestamp=datetime.now(UTC),
            )

        except Exception as e:
            logger.debug(f"Error checking opportunity {token_in}/{token_out}: {e}")
            return None

    def _get_flash_loan_info(self, token: str, amount: Decimal) -> FlashLoanSelectionResult | None:
        """Get flash loan provider info.

        Args:
            token: Token to borrow
            amount: Amount to borrow

        Returns:
            FlashLoanSelectionResult or None if unavailable
        """
        try:
            if self.config.flash_loan_provider == "auto":
                return self._flash_loan_selector.select_provider(
                    token=token,
                    amount=amount,
                    priority=self.config.flash_loan_priority,
                )
            else:
                return self._flash_loan_selector.select_provider(
                    token=token,
                    amount=amount,
                    priority=self.config.flash_loan_priority,
                )
        except Exception as e:
            logger.warning(f"Failed to get flash loan info for {token}: {e}")
            return None

    def _estimate_usd_value(self, amount: Decimal, token: str) -> Decimal:
        """Estimate USD value of a token amount.

        Args:
            amount: Token amount
            token: Token symbol

        Returns:
            Estimated USD value
        """
        # Stablecoins are 1:1
        stables = {"USDC", "USDT", "DAI", "FRAX"}
        if token in stables:
            return amount

        # Approximate prices for common tokens
        prices = {
            "WETH": Decimal("2500"),
            "ETH": Decimal("2500"),
            "WBTC": Decimal("45000"),
            "ARB": Decimal("0.80"),
            "OP": Decimal("1.50"),
        }

        price = prices.get(token, Decimal("1"))
        return amount * price

    def _create_arbitrage_intent(self, opportunity: ArbitrageOpportunity) -> FlashLoanIntent:
        """Create flash loan intent for arbitrage execution.

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            FlashLoanIntent with swap callbacks
        """
        logger.info(
            f"Creating arbitrage intent: "
            f"flash loan {opportunity.amount_in} {opportunity.token_in}, "
            f"swap on {opportunity.buy_dex}, reverse on {opportunity.sell_dex}"
        )

        # Create swap callbacks
        # Step 1: Buy token_out on cheaper DEX
        # Calculate slippage-protected swap using max_slippage from config
        buy_swap: FlashLoanCallbackIntent = Intent.swap(
            from_token=opportunity.token_in,
            to_token=opportunity.token_out,
            amount=opportunity.amount_in,
            max_slippage=Decimal(self.config.max_slippage_bps) / Decimal("10000"),
            protocol=opportunity.buy_dex,
            chain=self.config.chain,
        )

        # Step 2: Sell token_out on expensive DEX to get token_in back
        sell_swap: FlashLoanCallbackIntent = Intent.swap(
            from_token=opportunity.token_out,
            to_token=opportunity.token_in,
            amount="all",  # Use all output from previous swap
            max_slippage=Decimal(self.config.max_slippage_bps) / Decimal("10000"),
            protocol=opportunity.sell_dex,
            chain=self.config.chain,
        )

        # Create flash loan intent
        flash_loan_intent = Intent.flash_loan(
            provider=opportunity.flash_loan_provider,  # type: ignore
            token=opportunity.token_in,
            amount=opportunity.amount_in,
            callback_intents=[buy_swap, sell_swap],
            chain=self.config.chain,
        )

        # Update tracking
        self._record_trade()

        return flash_loan_intent

    def _record_trade(self) -> None:
        """Record trade execution for tracking."""
        self.config.last_trade_timestamp = int(time.time())
        self.config.total_trades += 1

        if self._current_opportunity:
            self.config.total_profit_usd += self._current_opportunity.net_profit_usd

        # Clear opportunity
        self._current_opportunity = None
        self._state = ArbState.COOLDOWN

    # Public methods for external access

    def get_state(self) -> ArbState:
        """Get current strategy state."""
        return self._state

    def get_current_opportunity(self) -> ArbitrageOpportunity | None:
        """Get current arbitrage opportunity if any."""
        return self._current_opportunity

    def get_stats(self) -> dict[str, Any]:
        """Get strategy statistics.

        Returns:
            Dictionary with strategy stats
        """
        return {
            "state": self._state.value,
            "total_trades": self.config.total_trades,
            "total_profit_usd": str(self.config.total_profit_usd),
            "last_trade_timestamp": self.config.last_trade_timestamp,
            "last_opportunity_found": self.config.last_opportunity_found,
            "cooldown_remaining": self._cooldown_remaining(),
        }

    def scan_opportunities(self) -> list[ArbitrageOpportunity]:
        """Manually scan for all arbitrage opportunities.

        Returns:
            List of all profitable opportunities found
        """
        opportunities: list[ArbitrageOpportunity] = []
        tokens = self.config.tokens

        for i, token_in in enumerate(tokens):
            for token_out in tokens[i + 1 :]:
                for t_in, t_out in [(token_in, token_out), (token_out, token_in)]:
                    opportunity = self._check_opportunity(t_in, t_out)
                    if opportunity:
                        opportunities.append(opportunity)

        # Sort by profit
        opportunities.sort(key=lambda o: o.net_profit_usd, reverse=True)
        return opportunities

    def clear_state(self) -> None:
        """Clear strategy state and statistics."""
        self._state = ArbState.SCANNING
        self._current_opportunity = None
        self.config.last_trade_timestamp = None
        self.config.last_opportunity_found = None
        self.config.total_profit_usd = Decimal("0")
        self.config.total_trades = 0

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Flash loan strategies are atomic - no persistent positions.
        Each trade executes and settles within a single transaction.

        Returns:
            True - this strategy can be safely torn down (trivially)
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        Flash loan arbitrage is atomic - no persistent positions.
        Trades execute and settle within a single transaction.

        Returns:
            TeardownPositionSummary with no positions
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            TeardownPositionSummary,
        )

        # No persistent positions - flash loans are atomic
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "cross_dex_arb"),
            timestamp=datetime.now(UTC),
            positions=[],
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        Flash loan arbitrage has no persistent positions.
        Nothing to tear down.

        Args:
            mode: TeardownMode (SOFT or HARD)

        Returns:
            Empty list - no positions to close
        """
        logger.info(
            f"Teardown requested for flash loan strategy (mode={mode.value}). No persistent positions to close."
        )
        return []


__all__ = [
    "CrossDexArbStrategy",
    "ArbState",
    "ArbitrageOpportunity",
]
