"""Stablecoin Peg Arbitrage Strategy - Profit from stablecoin depeg events.

This strategy monitors stablecoin prices (USDC, USDT, DAI, FRAX) and detects
depeg events where a stablecoin trades below or above its $1.00 peg. When a
significant depeg is detected, it executes Curve swaps to profit from the
expected peg restoration.

Key Features:
    - Monitors multiple stablecoins for depeg events
    - Uses Curve pools for low-slippage stablecoin swaps
    - Configurable depeg thresholds and profit targets
    - Automatic trade sizing based on depeg severity

Strategy Logic:
    1. Monitor stablecoin prices vs USD peg
    2. Detect depeg events exceeding threshold (default 50 bps)
    3. For depeg below peg: Buy the depegged stablecoin (it's cheap)
    4. For depeg above peg: Sell the overvalued stablecoin
    5. Wait for peg restoration and profit from price convergence

Example:
    If USDC trades at $0.995 (-50 bps):
    1. Swap USDT -> USDC via Curve 3pool (buy cheap USDC)
    2. Expected profit when USDC returns to $1.00: ~50 bps
    3. Net profit after gas and Curve fees
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.intents import Intent, IntentCompiler, StateMachineConfig
from almanak.framework.intents.vocabulary import (
    DecideResult,
    SwapIntent,
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

from .config import StablecoinPegArbConfig

logger = logging.getLogger(__name__)


class PegArbState(str, Enum):
    """State of the peg arbitrage strategy."""

    MONITORING = "monitoring"  # Monitoring stablecoin prices
    OPPORTUNITY_FOUND = "opportunity_found"  # Found depeg opportunity
    EXECUTING = "executing"  # Executing Curve swap
    COOLDOWN = "cooldown"  # Waiting after trade


class DepegDirection(str, Enum):
    """Direction of the depeg event."""

    BELOW_PEG = "below_peg"  # Price < $1.00 (buy opportunity)
    ABOVE_PEG = "above_peg"  # Price > $1.00 (sell opportunity)


@dataclass
class DepegOpportunity:
    """Represents a stablecoin depeg arbitrage opportunity.

    Attributes:
        depegged_token: The stablecoin that has depegged
        stable_token: The stablecoin to swap from/to (maintains peg)
        direction: Whether depeg is below or above peg
        current_price: Current price of depegged token
        depeg_bps: Depeg in basis points
        trade_amount: Recommended trade amount in USD
        expected_profit_bps: Expected profit in basis points
        expected_profit_usd: Expected profit in USD (before gas)
        curve_pool: Recommended Curve pool for execution
        timestamp: When opportunity was found
    """

    depegged_token: str
    stable_token: str
    direction: DepegDirection
    current_price: Decimal
    depeg_bps: int
    trade_amount: Decimal
    expected_profit_bps: int
    expected_profit_usd: Decimal
    curve_pool: str
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "depegged_token": self.depegged_token,
            "stable_token": self.stable_token,
            "direction": self.direction.value,
            "current_price": str(self.current_price),
            "depeg_bps": self.depeg_bps,
            "trade_amount": str(self.trade_amount),
            "expected_profit_bps": self.expected_profit_bps,
            "expected_profit_usd": str(self.expected_profit_usd),
            "curve_pool": self.curve_pool,
            "timestamp": self.timestamp.isoformat(),
        }


# Curve pool token mappings
CURVE_POOL_TOKENS: dict[str, list[str]] = {
    "3pool": ["DAI", "USDC", "USDT"],
    "frax_usdc": ["FRAX", "USDC"],
    "frax_3crv": ["FRAX", "DAI", "USDC", "USDT"],
    "lusd_3crv": ["LUSD", "DAI", "USDC", "USDT"],
    "susd": ["DAI", "USDC", "USDT", "sUSD"],
    "rai_3crv": ["RAI", "DAI", "USDC", "USDT"],
}


def get_pool_for_tokens(token_a: str, token_b: str) -> str | None:
    """Get the best Curve pool for a token pair.

    Args:
        token_a: First token
        token_b: Second token

    Returns:
        Pool name or None if no pool supports the pair
    """
    for pool, tokens in CURVE_POOL_TOKENS.items():
        if token_a in tokens and token_b in tokens:
            return pool
    return None


@almanak_strategy(
    name="stablecoin_peg_arb",
    description="Stablecoin peg arbitrage using Curve pools",
    version="1.0.0",
    author="Almanak",
    tags=["arbitrage", "stablecoin", "curve", "depeg", "defi"],
    supported_chains=["ethereum", "arbitrum"],
    supported_protocols=["curve"],
    intent_types=["SWAP", "HOLD"],
)
class StablecoinPegArbStrategy(IntentStrategy[StablecoinPegArbConfig]):
    """Stablecoin Peg Arbitrage Strategy using Intent pattern.

    This strategy monitors stablecoin prices and executes Curve swaps when
    depeg opportunities are detected.

    Key Simplifications:
    - No manual state machine - framework handles execution flow
    - No action bundle construction - IntentCompiler handles TX building
    - Only implements decide() with core business logic

    Arbitrage Flow:
    1. Monitor stablecoin prices (USDC, USDT, DAI, FRAX)
    2. Detect depeg events (price deviation > threshold)
    3. Identify best swap route via Curve pools
    4. Execute swap to profit from peg restoration
    """

    STRATEGY_NAME = "stablecoin_peg_arb"

    def __init__(
        self,
        config: StablecoinPegArbConfig,
        risk_guard_config: RiskGuardConfig | None = None,
        notification_callback: NotificationCallback | None = None,
        compiler: IntentCompiler | None = None,
        state_machine_config: StateMachineConfig | None = None,
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
    ) -> None:
        """Initialize the Stablecoin Peg Arbitrage strategy.

        Args:
            config: Strategy configuration
            risk_guard_config: Risk management configuration
            notification_callback: Callback for notifications
            compiler: Intent compiler
            state_machine_config: State machine configuration
            price_oracle: Price data provider
            rsi_provider: RSI data provider
            balance_provider: Balance data provider
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
        # State tracking
        self._state = PegArbState.MONITORING
        self._current_opportunity: DepegOpportunity | None = None

        # Price cache for depeg detection
        self._price_cache: dict[str, tuple[Decimal, datetime]] = {}

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
        2. Monitors stablecoin prices for depeg events
        3. Executes Curve swap if profitable opportunity found

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
        if self._state == PegArbState.MONITORING:
            return self._handle_monitoring(market)
        elif self._state == PegArbState.OPPORTUNITY_FOUND:
            return self._handle_opportunity(market)
        elif self._state == PegArbState.COOLDOWN:
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
            if age > self.config.opportunity_expiry_seconds:
                logger.info("Opportunity expired, returning to monitoring")
                self._current_opportunity = None
                self._state = PegArbState.MONITORING
            else:
                self._state = PegArbState.OPPORTUNITY_FOUND
        elif not self._can_trade():
            self._state = PegArbState.COOLDOWN
        else:
            self._state = PegArbState.MONITORING

    def _handle_monitoring(self, market: MarketSnapshot) -> DecideResult:
        """Handle monitoring state - look for depeg opportunities.

        Args:
            market: Current market snapshot

        Returns:
            Intent to execute or hold
        """
        logger.debug("Monitoring stablecoin prices for depeg...")

        # Check all stablecoins for depeg
        opportunity = self._find_best_opportunity(market)

        if opportunity is None:
            return Intent.hold(reason="No depeg opportunity found")

        # Found opportunity - store and proceed
        self._current_opportunity = opportunity
        self._state = PegArbState.OPPORTUNITY_FOUND
        self.config.last_opportunity_found = (
            f"{opportunity.depegged_token} {opportunity.direction.value} "
            f"{opportunity.depeg_bps}bps -> swap via {opportunity.curve_pool}"
        )

        logger.info(
            f"Found depeg opportunity: "
            f"{opportunity.depegged_token} at ${opportunity.current_price} "
            f"({opportunity.direction.value}, {opportunity.depeg_bps}bps), "
            f"expected profit: {opportunity.expected_profit_bps}bps "
            f"(${opportunity.expected_profit_usd:.2f})"
        )

        return self._create_swap_intent(opportunity)

    def _handle_opportunity(self, market: MarketSnapshot) -> DecideResult:
        """Handle opportunity found state - execute swap.

        Args:
            market: Current market snapshot

        Returns:
            Intent to execute
        """
        if self._current_opportunity is None:
            self._state = PegArbState.MONITORING
            return Intent.hold(reason="Opportunity expired")

        return self._create_swap_intent(self._current_opportunity)

    def _find_best_opportunity(self, market: MarketSnapshot) -> DepegOpportunity | None:
        """Find the best depeg opportunity across all stablecoins.

        Args:
            market: Current market snapshot

        Returns:
            Best opportunity or None if none profitable
        """
        best_opportunity: DepegOpportunity | None = None
        best_profit_bps = 0

        # Get prices for all configured stablecoins
        prices: dict[str, Decimal] = {}
        for token in self.config.stablecoins:
            try:
                price = self._get_stablecoin_price(market, token)
                prices[token] = price
                self._price_cache[token] = (price, datetime.now(UTC))
            except Exception as e:
                logger.debug(f"Failed to get price for {token}: {e}")
                continue

        # Find depegged stablecoins
        for token, price in prices.items():
            if not self.config.is_opportunity(price):
                continue

            # Calculate depeg details
            depeg_bps = self.config.calculate_depeg_bps(price)
            direction = DepegDirection.BELOW_PEG if price < self.config.peg_target else DepegDirection.ABOVE_PEG

            # Find a stable token to swap with
            stable_token = self._find_stable_counterparty(prices, token)
            if stable_token is None:
                continue

            # Find Curve pool for this pair
            curve_pool = get_pool_for_tokens(token, stable_token)
            if curve_pool is None:
                continue

            # Calculate expected profit
            # Profit comes from buying cheap (depegged) token or selling expensive token
            # Expected profit = depeg_bps minus fees (Curve ~4 bps + gas)
            curve_fee_bps = 4  # Curve pools typically 4 bps
            expected_profit_bps = depeg_bps - curve_fee_bps

            # Calculate trade size and USD profit
            trade_amount = min(
                self.config.default_trade_size_usd,
                self.config.max_trade_size_usd,
            )
            expected_profit_usd = trade_amount * Decimal(expected_profit_bps) / Decimal("10000")

            # Check profitability
            if not self.config.is_profitable(expected_profit_usd, expected_profit_bps):
                continue

            # Track best opportunity
            if expected_profit_bps > best_profit_bps:
                best_profit_bps = expected_profit_bps
                best_opportunity = DepegOpportunity(
                    depegged_token=token,
                    stable_token=stable_token,
                    direction=direction,
                    current_price=price,
                    depeg_bps=depeg_bps,
                    trade_amount=trade_amount,
                    expected_profit_bps=expected_profit_bps,
                    expected_profit_usd=expected_profit_usd,
                    curve_pool=curve_pool,
                    timestamp=datetime.now(UTC),
                )

        return best_opportunity

    def _get_stablecoin_price(self, market: MarketSnapshot, token: str) -> Decimal:
        """Get the current price of a stablecoin.

        Args:
            market: Current market snapshot
            token: Stablecoin symbol

        Returns:
            Price in USD
        """
        # Use market price (not pegged) to detect depegs
        return market.price(token, "USD")

    def _find_stable_counterparty(self, prices: dict[str, Decimal], depegged_token: str) -> str | None:
        """Find a stablecoin that maintains its peg to use as counterparty.

        Args:
            prices: Map of token -> price
            depegged_token: The token that has depegged

        Returns:
            Token symbol of stable counterparty or None
        """
        for token, price in prices.items():
            if token == depegged_token:
                continue
            # Check if this token is close to peg (within 10 bps)
            depeg_bps = self.config.calculate_depeg_bps(price)
            if depeg_bps <= self.config.min_depeg_bps:
                return token

        return None

    def _create_swap_intent(self, opportunity: DepegOpportunity) -> SwapIntent:
        """Create swap intent for depeg arbitrage execution.

        Args:
            opportunity: The depeg opportunity

        Returns:
            SwapIntent for Curve swap
        """
        logger.info(
            f"Creating swap intent: "
            f"{opportunity.stable_token} -> {opportunity.depegged_token} "
            f"via {opportunity.curve_pool}"
        )

        # Determine swap direction based on depeg
        if opportunity.direction == DepegDirection.BELOW_PEG:
            # Depegged token is cheap - buy it
            # Swap stable_token -> depegged_token
            from_token = opportunity.stable_token
            to_token = opportunity.depegged_token
        else:
            # Depegged token is expensive - sell it
            # Swap depegged_token -> stable_token
            from_token = opportunity.depegged_token
            to_token = opportunity.stable_token

        # Create swap intent
        swap_intent = Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount=opportunity.trade_amount,
            max_slippage=Decimal(self.config.max_slippage_bps) / Decimal("10000"),
            protocol="curve",
            chain=self.config.chain,
        )

        # Update tracking
        self._record_trade()

        return swap_intent

    def _record_trade(self) -> None:
        """Record trade execution for tracking."""
        self.config.last_trade_timestamp = int(time.time())
        self.config.total_trades += 1

        if self._current_opportunity:
            self.config.total_profit_usd += (
                self._current_opportunity.expected_profit_usd - self.config.estimated_gas_cost_usd
            )

        # Clear opportunity
        self._current_opportunity = None
        self._state = PegArbState.COOLDOWN

    # Public methods for external access

    def get_state(self) -> PegArbState:
        """Get current strategy state."""
        return self._state

    def get_current_opportunity(self) -> DepegOpportunity | None:
        """Get current depeg opportunity if any."""
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
            "cached_prices": {token: str(price) for token, (price, _) in self._price_cache.items()},
        }

    def scan_depegs(self, market: MarketSnapshot) -> list[dict[str, Any]]:
        """Manually scan for all depeg events.

        Args:
            market: Current market snapshot

        Returns:
            List of depeg information for all tokens
        """
        depegs: list[dict[str, Any]] = []

        for token in self.config.stablecoins:
            try:
                price = self._get_stablecoin_price(market, token)
                depeg_bps = self.config.calculate_depeg_bps(price)
                direction = "below_peg" if price < self.config.peg_target else "above_peg"
                is_opportunity = self.config.is_opportunity(price)

                depegs.append(
                    {
                        "token": token,
                        "price": str(price),
                        "depeg_bps": depeg_bps,
                        "direction": direction,
                        "is_opportunity": is_opportunity,
                    }
                )
            except Exception as e:
                depegs.append(
                    {
                        "token": token,
                        "error": str(e),
                    }
                )

        return depegs

    def clear_state(self) -> None:
        """Clear strategy state and statistics."""
        self._state = PegArbState.MONITORING
        self._current_opportunity = None
        self._price_cache.clear()
        self.config.last_trade_timestamp = None
        self.config.last_opportunity_found = None
        self.config.total_profit_usd = Decimal("0")
        self.config.total_trades = 0

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Stablecoin peg arb holds depegged tokens waiting for peg restoration.
        Teardown swaps any non-USDC stablecoins to USDC.

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        For peg arb, positions are depegged stablecoin holdings.
        These are stablecoins bought at a discount waiting for peg restoration.

        Returns:
            TeardownPositionSummary with stablecoin position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        position_idx = 0

        # Stablecoin positions are TOKEN type
        # The strategy may be holding various stablecoins
        for token in self.config.stablecoins:
            if token == "USDC":
                continue  # USDC is target, no need to close

            # In production, would query actual balances
            # For now, estimate based on trade size
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"stablecoin_peg_arb_token_{position_idx}",
                    chain=self.config.chain,
                    protocol="curve",
                    value_usd=self.config.default_trade_size_usd,  # 1:1 for stables
                    details={
                        "asset": token,
                        "token": token,
                        "is_stablecoin": True,
                        "amount": str(self.config.default_trade_size_usd),
                    },
                )
            )
            position_idx += 1

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "stablecoin_peg_arb"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        For peg arb, teardown means swapping all non-USDC stablecoins to USDC.

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage tolerance

        Returns:
            List of SWAP intents for stablecoin conversion
        """
        from almanak.framework.teardown import TeardownMode

        intents: list = []

        # Determine slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.01")  # 1% for stablecoins in emergency
        else:
            max_slippage = Decimal(str(self.config.max_slippage_bps)) / Decimal("10000")

        # Swap all non-USDC stablecoins to USDC
        for token in self.config.stablecoins:
            if token == "USDC":
                continue

            logger.info(f"Generating teardown: swap {token} -> USDC (mode={mode.value})")
            intents.append(
                Intent.swap(
                    from_token=token,
                    to_token="USDC",
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="curve",  # Best for stablecoin swaps
                    chain=self.config.chain,
                )
            )

        return intents


__all__ = [
    "StablecoinPegArbStrategy",
    "PegArbState",
    "DepegDirection",
    "DepegOpportunity",
    "CURVE_POOL_TOKENS",
    "get_pool_for_tokens",
]
