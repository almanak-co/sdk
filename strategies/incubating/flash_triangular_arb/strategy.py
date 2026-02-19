"""Flash Loan Triangular Arbitrage Strategy - Atomic multi-hop arbitrage using flash loans.

This strategy identifies triangular arbitrage opportunities across DEXs
(e.g., ETH->USDC->WBTC->ETH) and executes them atomically using flash loans.

Key Features:
    - Multi-hop path finding (3-4 tokens)
    - Flash loan execution for capital-free arbitrage
    - Automatic provider selection (Aave/Balancer)
    - Configurable profit thresholds and gas limits
    - Support for multiple token paths

Example:
    If ETH->USDC->WBTC->ETH yields profit:
    1. Flash loan ETH from Balancer (0% fee)
    2. Swap ETH -> USDC on best DEX
    3. Swap USDC -> WBTC on best DEX
    4. Swap WBTC -> ETH on best DEX
    5. Repay flash loan, keep profit

    All steps execute atomically - if any step fails, entire trade reverts.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from itertools import permutations
from typing import Any, Literal

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

from .config import FlashTriangularArbConfig

logger = logging.getLogger(__name__)


class TriangularArbState(str, Enum):
    """State of the triangular arbitrage strategy."""

    SCANNING = "scanning"  # Looking for opportunities
    OPPORTUNITY_FOUND = "opportunity_found"  # Found profitable opportunity
    EXECUTING = "executing"  # Executing flash loan arbitrage
    COOLDOWN = "cooldown"  # Waiting after trade


@dataclass
class SwapLeg:
    """Represents a single swap leg in a triangular arbitrage path.

    Attributes:
        from_token: Input token for this leg
        to_token: Output token for this leg
        dex: DEX to execute the swap on
        amount_in: Input amount
        amount_out: Expected output amount
        price_impact_bps: Price impact in basis points
    """

    from_token: str
    to_token: str
    dex: str
    amount_in: Decimal
    amount_out: Decimal
    price_impact_bps: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_token": self.from_token,
            "to_token": self.to_token,
            "dex": self.dex,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "price_impact_bps": self.price_impact_bps,
        }


@dataclass
class TriangularOpportunity:
    """Represents a triangular arbitrage opportunity.

    Attributes:
        path: Token path (e.g., ["ETH", "USDC", "WBTC", "ETH"])
        legs: Swap legs for each step
        flash_loan_token: Token to flash loan (first token in path)
        flash_loan_amount: Amount to flash loan
        flash_loan_provider: Flash loan provider to use
        flash_loan_fee: Flash loan fee amount
        gross_profit: Gross profit in loan token terms
        gross_profit_bps: Gross profit in basis points
        gross_profit_usd: Gross profit in USD
        net_profit_usd: Net profit after gas costs
        total_price_impact_bps: Cumulative price impact
        timestamp: When opportunity was found
    """

    path: list[str]
    legs: list[SwapLeg]
    flash_loan_token: str
    flash_loan_amount: Decimal
    flash_loan_provider: str
    flash_loan_fee: Decimal
    gross_profit: Decimal
    gross_profit_bps: int
    gross_profit_usd: Decimal
    net_profit_usd: Decimal
    total_price_impact_bps: int
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "path": self.path,
            "legs": [leg.to_dict() for leg in self.legs],
            "flash_loan_token": self.flash_loan_token,
            "flash_loan_amount": str(self.flash_loan_amount),
            "flash_loan_provider": self.flash_loan_provider,
            "flash_loan_fee": str(self.flash_loan_fee),
            "gross_profit": str(self.gross_profit),
            "gross_profit_bps": self.gross_profit_bps,
            "gross_profit_usd": str(self.gross_profit_usd),
            "net_profit_usd": str(self.net_profit_usd),
            "total_price_impact_bps": self.total_price_impact_bps,
            "timestamp": self.timestamp.isoformat(),
        }

    @property
    def path_str(self) -> str:
        """Get string representation of path."""
        return " -> ".join(self.path)


@almanak_strategy(
    name="flash_triangular_arb",
    description="Flash loan triangular arbitrage across DEXs",
    version="1.0.0",
    author="Almanak",
    tags=["arbitrage", "flash_loan", "triangular", "dex", "atomic", "defi"],
    supported_chains=["ethereum", "arbitrum", "optimism", "polygon", "base"],
    supported_protocols=["uniswap_v3", "curve", "enso", "aave_v3", "balancer"],
    intent_types=["FLASH_LOAN", "SWAP", "HOLD"],
)
class FlashTriangularArbStrategy(IntentStrategy[FlashTriangularArbConfig]):
    """Flash Loan Triangular Arbitrage Strategy using Intent pattern.

    This strategy scans for triangular price discrepancies across DEXs and
    executes atomic arbitrage using flash loans when profitable opportunities
    are found.

    Key Simplifications:
    - No manual state machine - framework handles execution flow
    - No action bundle construction - IntentCompiler handles TX building
    - Only implements decide() with core business logic

    Triangular Arbitrage Flow:
    1. Generate all valid token paths (A -> B -> C -> A)
    2. For each path, get quotes from multiple DEXs
    3. Calculate profitability after gas and flash loan fees
    4. Execute if profit > threshold:
       - Flash loan first token
       - Execute swap chain
       - Repay flash loan + fee
       - Keep profit
    """

    STRATEGY_NAME = "flash_triangular_arb"

    def __init__(
        self,
        config: FlashTriangularArbConfig,
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
        """Initialize the Flash Triangular Arbitrage strategy.

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
        self._state = TriangularArbState.SCANNING
        self._current_opportunity: TriangularOpportunity | None = None
        # Cache for token paths
        self._token_paths: list[list[str]] = []
        self._paths_generated = False

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
        2. Generates token paths if not already done
        3. Scans for triangular arbitrage opportunities
        4. Executes flash loan arbitrage if profitable

        Returns:
            Intent to execute or hold
        """
        if self.config.pause_strategy:
            return Intent.hold(reason="Strategy paused")

        # Check cooldown
        if not self._can_trade():
            remaining = self._cooldown_remaining()
            return Intent.hold(reason=f"Trade cooldown: {remaining}s remaining")

        # Generate paths if not done
        if not self._paths_generated:
            self._generate_token_paths()

        # Update state
        self._update_state()

        # State-based decision making
        if self._state == TriangularArbState.SCANNING:
            return self._handle_scanning(market)
        elif self._state == TriangularArbState.OPPORTUNITY_FOUND:
            return self._handle_opportunity(market)
        elif self._state == TriangularArbState.COOLDOWN:
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
                self._state = TriangularArbState.SCANNING
            else:
                self._state = TriangularArbState.OPPORTUNITY_FOUND
        elif not self._can_trade():
            self._state = TriangularArbState.COOLDOWN
        else:
            self._state = TriangularArbState.SCANNING

    def _generate_token_paths(self) -> None:
        """Generate all valid triangular paths from configured tokens.

        Creates paths like [ETH, USDC, WBTC, ETH] for triangular arbitrage.
        """
        tokens = self.config.tokens
        paths: list[list[str]] = []

        # For triangular: we need exactly 3 hops, meaning 4 tokens in path
        # where first and last are the same
        if self.config.min_hops <= 3 <= self.config.max_hops:
            for tri_perm in permutations(tokens, 3):
                # Create triangular path: A -> B -> C -> A
                tri_path: list[str] = list(tri_perm) + [tri_perm[0]]
                paths.append(tri_path)

        # For quadrilateral: 4 hops, 5 tokens in path
        if self.config.max_hops >= 4 and len(tokens) >= 4:
            for quad_perm in permutations(tokens, 4):
                # Create quadrilateral path: A -> B -> C -> D -> A
                quad_path: list[str] = list(quad_perm) + [quad_perm[0]]
                paths.append(quad_path)

        # Limit paths to evaluate
        self._token_paths = paths[: self.config.max_paths_to_evaluate]
        self._paths_generated = True

        logger.info(f"Generated {len(self._token_paths)} token paths for triangular arbitrage")

    def _handle_scanning(self, market: MarketSnapshot) -> DecideResult:
        """Handle scanning state - look for triangular arbitrage opportunities.

        Args:
            market: Current market snapshot

        Returns:
            Intent to execute or hold
        """
        logger.debug("Scanning for triangular arbitrage opportunities...")

        # Scan all token paths
        opportunity = self._find_best_opportunity()

        if opportunity is None:
            return Intent.hold(reason="No profitable triangular arbitrage opportunity found")

        # Found opportunity - store and proceed
        self._current_opportunity = opportunity
        self._state = TriangularArbState.OPPORTUNITY_FOUND
        self.config.last_opportunity_found = f"{opportunity.path_str} +{opportunity.gross_profit_bps}bps"

        logger.info(
            f"Found triangular opportunity: {opportunity.path_str}, "
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
            self._state = TriangularArbState.SCANNING
            return Intent.hold(reason="Opportunity expired")

        return self._create_arbitrage_intent(self._current_opportunity)

    def _find_best_opportunity(self) -> TriangularOpportunity | None:
        """Find the best triangular arbitrage opportunity across all paths.

        Returns:
            Best opportunity or None if none profitable
        """
        best_opportunity: TriangularOpportunity | None = None
        best_profit_usd = Decimal("0")

        for path in self._token_paths:
            opportunity = self._evaluate_path(path)
            if opportunity and opportunity.net_profit_usd > best_profit_usd:
                best_opportunity = opportunity
                best_profit_usd = opportunity.net_profit_usd

        return best_opportunity

    def _evaluate_path(self, path: list[str]) -> TriangularOpportunity | None:
        """Evaluate a single triangular path for profitability.

        Args:
            path: Token path (e.g., ["ETH", "USDC", "WBTC", "ETH"])

        Returns:
            TriangularOpportunity if profitable, None otherwise
        """
        if len(path) < 4:
            return None

        try:
            # First token is what we flash loan
            flash_loan_token = path[0]
            initial_amount = self.config.default_trade_size_usd

            # Get flash loan info first
            flash_loan_result = self._get_flash_loan_info(flash_loan_token, initial_amount)
            if flash_loan_result is None or not flash_loan_result.is_success:
                return None

            # Simulate swaps through the path
            legs: list[SwapLeg] = []
            current_amount = initial_amount
            total_price_impact = 0

            for i in range(len(path) - 1):
                from_token = path[i]
                to_token = path[i + 1]

                # Get best quote for this leg
                quote = self._get_best_quote(from_token, to_token, current_amount)
                if quote is None:
                    return None

                leg = SwapLeg(
                    from_token=from_token,
                    to_token=to_token,
                    dex=quote.dex,
                    amount_in=current_amount,
                    amount_out=quote.amount_out,
                    price_impact_bps=quote.price_impact_bps,
                )
                legs.append(leg)

                current_amount = quote.amount_out
                total_price_impact += quote.price_impact_bps

            # Check total price impact
            if total_price_impact > self.config.max_total_slippage_bps:
                return None

            # Calculate profit
            # Final amount should be > initial amount + flash loan fee
            total_repay = initial_amount + flash_loan_result.fee_amount
            gross_profit = current_amount - total_repay

            # If negative profit, skip
            if gross_profit <= Decimal("0"):
                return None

            gross_profit_bps = int((gross_profit / initial_amount) * Decimal("10000"))

            # Estimate USD profit
            gross_profit_usd = self._estimate_usd_value(gross_profit, flash_loan_token)
            net_profit_usd = gross_profit_usd - self.config.estimated_gas_cost_usd

            # Check profitability
            if not self.config.is_profitable(gross_profit_usd, gross_profit_bps):
                return None

            return TriangularOpportunity(
                path=path,
                legs=legs,
                flash_loan_token=flash_loan_token,
                flash_loan_amount=initial_amount,
                flash_loan_provider=flash_loan_result.provider or "aave",
                flash_loan_fee=flash_loan_result.fee_amount,
                gross_profit=gross_profit,
                gross_profit_bps=gross_profit_bps,
                gross_profit_usd=gross_profit_usd,
                net_profit_usd=net_profit_usd,
                total_price_impact_bps=total_price_impact,
                timestamp=datetime.now(UTC),
            )

        except Exception as e:
            logger.debug(f"Error evaluating path {' -> '.join(path)}: {e}")
            return None

    def _get_best_quote(self, from_token: str, to_token: str, amount_in: Decimal) -> DexQuote | None:
        """Get the best quote for a swap from any DEX.

        Args:
            from_token: Input token
            to_token: Output token
            amount_in: Input amount

        Returns:
            Best DexQuote or None if unavailable
        """
        try:
            prices = self._run_async(
                self._price_service.get_prices_across_dexs(
                    token_in=from_token,
                    token_out=to_token,
                    amount_in=amount_in,
                )
            )

            if not prices.quotes:
                return None

            # Find best output
            quotes = list(prices.quotes.values())
            best_quote = max(quotes, key=lambda q: q.amount_out)

            return best_quote

        except Exception as e:
            logger.debug(f"Error getting quote {from_token}->{to_token}: {e}")
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

    def _create_arbitrage_intent(self, opportunity: TriangularOpportunity) -> FlashLoanIntent:
        """Create flash loan intent for triangular arbitrage execution.

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            FlashLoanIntent with swap callbacks
        """
        logger.info(
            f"Creating triangular arbitrage intent: "
            f"flash loan {opportunity.flash_loan_amount} {opportunity.flash_loan_token}, "
            f"path: {opportunity.path_str}"
        )

        # Create swap callbacks for each leg
        callbacks: list[FlashLoanCallbackIntent] = []

        for i, leg in enumerate(opportunity.legs):
            # Use "all" for subsequent swaps to chain amounts
            swap_amount: Decimal | Literal["all"] = leg.amount_in if i == 0 else "all"

            swap_intent: FlashLoanCallbackIntent = Intent.swap(
                from_token=leg.from_token,
                to_token=leg.to_token,
                amount=swap_amount,
                max_slippage=Decimal(self.config.max_slippage_bps) / Decimal("10000"),
                protocol=leg.dex,
                chain=self.config.chain,
            )
            callbacks.append(swap_intent)

        # Create flash loan intent with all swaps as callbacks
        flash_loan_intent = Intent.flash_loan(
            provider=opportunity.flash_loan_provider,  # type: ignore
            token=opportunity.flash_loan_token,
            amount=opportunity.flash_loan_amount,
            callback_intents=callbacks,
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
        self._state = TriangularArbState.COOLDOWN

    # Public methods for external access

    def get_state(self) -> TriangularArbState:
        """Get current strategy state."""
        return self._state

    def get_current_opportunity(self) -> TriangularOpportunity | None:
        """Get current arbitrage opportunity if any."""
        return self._current_opportunity

    def get_token_paths(self) -> list[list[str]]:
        """Get all generated token paths."""
        if not self._paths_generated:
            self._generate_token_paths()
        return self._token_paths.copy()

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
            "paths_count": len(self._token_paths),
        }

    def scan_opportunities(self) -> list[TriangularOpportunity]:
        """Manually scan for all triangular arbitrage opportunities.

        Returns:
            List of all profitable opportunities found
        """
        if not self._paths_generated:
            self._generate_token_paths()

        opportunities: list[TriangularOpportunity] = []

        for path in self._token_paths:
            opportunity = self._evaluate_path(path)
            if opportunity:
                opportunities.append(opportunity)

        # Sort by profit
        opportunities.sort(key=lambda o: o.net_profit_usd, reverse=True)
        return opportunities

    def clear_state(self) -> None:
        """Clear strategy state and statistics."""
        self._state = TriangularArbState.SCANNING
        self._current_opportunity = None
        self.config.last_trade_timestamp = None
        self.config.last_opportunity_found = None
        self.config.total_profit_usd = Decimal("0")
        self.config.total_trades = 0

    def regenerate_paths(self) -> None:
        """Force regeneration of token paths."""
        self._paths_generated = False
        self._token_paths = []
        self._generate_token_paths()

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Flash loan strategies are atomic - no persistent positions.
        Each triangular arbitrage executes and settles within a single transaction.

        Returns:
            True - this strategy can be safely torn down (trivially)
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        Flash loan triangular arbitrage is atomic - no persistent positions.
        All trades execute and settle within a single transaction.

        Returns:
            TeardownPositionSummary with no positions
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            TeardownPositionSummary,
        )

        # No persistent positions - flash loans are atomic
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "flash_triangular_arb"),
            timestamp=datetime.now(UTC),
            positions=[],
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        Flash loan triangular arbitrage has no persistent positions.
        Nothing to tear down.

        Args:
            mode: TeardownMode (SOFT or HARD)

        Returns:
            Empty list - no positions to close
        """
        logger.info(
            f"Teardown requested for flash loan triangular arb (mode={mode.value}). No persistent positions to close."
        )
        return []


__all__ = [
    "FlashTriangularArbStrategy",
    "TriangularArbState",
    "TriangularOpportunity",
    "SwapLeg",
]
