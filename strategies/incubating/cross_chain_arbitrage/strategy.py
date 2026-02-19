"""Cross-Chain Arbitrage Strategy Implementation.

This strategy monitors prices across multiple chains and executes arbitrage
when price spreads exceed a configurable threshold after accounting for:
- Bridge fees
- Swap slippage
- Gas costs
- Bridge latency risk

The strategy uses the MultiChainMarketSnapshot for cross-chain price comparison
and Intent.sequence() for atomic multi-step operations.

Key Features:
    - Multi-chain price monitoring (Arbitrum, Optimism, Base)
    - Bridge fee and latency accounting
    - Configurable profit thresholds
    - Emergency handling for price changes during bridging
    - Support for multi-Anvil fork testing

Example Flow (when ETH is cheaper on Optimism):
    1. Swap USDC -> ETH on Optimism (cheaper chain)
    2. Bridge ETH from Optimism -> Arbitrum
    3. Swap ETH -> USDC on Arbitrum (expensive chain)

The sequence uses amount="all" pattern to chain outputs between steps.
"""

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.intents import Intent, IntentSequence
from almanak.framework.intents.vocabulary import DecideResult
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.strategies.intent_strategy import MarketSnapshot, MultiChainMarketSnapshot

from .config import CrossChainArbConfig

logger = logging.getLogger(__name__)


class ArbState(str, Enum):
    """State of the cross-chain arbitrage strategy."""

    MONITORING = "monitoring"  # Monitoring for price spreads
    OPPORTUNITY_FOUND = "opportunity_found"  # Found profitable spread
    EXECUTING = "executing"  # Executing arbitrage sequence
    COOLDOWN = "cooldown"  # Waiting after trade


@dataclass
class CrossChainOpportunity:
    """Represents a cross-chain arbitrage opportunity.

    Attributes:
        buy_chain: Chain to buy on (cheaper price)
        sell_chain: Chain to sell on (more expensive price)
        token: Token being arbitraged
        raw_spread_bps: Raw price spread in basis points
        net_profit_bps: Net profit after all fees
        estimated_profit_usd: Estimated profit in USD
        bridge_provider: Bridge to use for transfer
        bridge_fee_bps: Bridge fee in basis points
        bridge_latency_seconds: Expected bridge latency
        buy_price: Price on buy chain
        sell_price: Price on sell chain
        timestamp: When opportunity was detected
    """

    buy_chain: str
    sell_chain: str
    token: str
    raw_spread_bps: int
    net_profit_bps: int
    estimated_profit_usd: Decimal
    bridge_provider: str | None
    bridge_fee_bps: int
    bridge_latency_seconds: int
    buy_price: Decimal
    sell_price: Decimal
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "buy_chain": self.buy_chain,
            "sell_chain": self.sell_chain,
            "token": self.token,
            "raw_spread_bps": self.raw_spread_bps,
            "net_profit_bps": self.net_profit_bps,
            "estimated_profit_usd": str(self.estimated_profit_usd),
            "bridge_provider": self.bridge_provider,
            "bridge_fee_bps": self.bridge_fee_bps,
            "bridge_latency_seconds": self.bridge_latency_seconds,
            "buy_price": str(self.buy_price),
            "sell_price": str(self.sell_price),
            "timestamp": self.timestamp.isoformat(),
        }


@almanak_strategy(
    name="cross_chain_arbitrage",
    description="Cross-chain arbitrage with bridge fee and latency accounting",
    version="2.0.0",
    author="Almanak",
    tags=["arbitrage", "multi-chain", "cross-chain", "bridge", "defi"],
    supported_chains=["ethereum", "arbitrum", "optimism", "base"],
    supported_protocols=["uniswap_v3", "across", "stargate"],
    intent_types=["SWAP", "BRIDGE", "HOLD"],
)
class CrossChainArbitrageStrategy(IntentStrategy[CrossChainArbConfig]):
    """Cross-Chain Arbitrage Strategy with profitability calculations.

    This strategy implements cross-chain arbitrage by:
    1. Monitoring price spreads across configured chains
    2. Calculating net profitability after bridge fees and slippage
    3. Executing atomic swap -> bridge -> swap sequences
    4. Tracking trade statistics and performance

    The strategy requires a MultiChainMarketSnapshot at runtime to access
    cross-chain price and balance data.

    Key Improvements over v1:
    - Bridge fee accounting (0.1% - 0.5% depending on provider)
    - Bridge latency risk assessment
    - Net profit calculations after all fees
    - Volatility-based pausing
    - Improved state management

    Configuration:
        - min_spread_bps: Minimum raw spread to consider (default 50 = 0.5%)
        - min_spread_after_fees_bps: Minimum profit after fees (default 10 = 0.1%)
        - bridge_provider: Preferred bridge or None for auto-select
        - account_for_bridge_fees: Whether to include bridge fees in calculations
        - account_for_bridge_latency: Whether to assess latency risk
    """

    STRATEGY_NAME = "cross_chain_arbitrage"

    def __init__(
        self,
        config: CrossChainArbConfig,
        chain: str = "arbitrum",
        wallet_address: str = "",
        **kwargs: Any,
    ) -> None:
        """Initialize the strategy.

        Args:
            config: Strategy configuration
            chain: Primary chain for the strategy
            wallet_address: Wallet address for transactions
            **kwargs: Additional arguments for base class
        """
        super().__init__(
            config=config,
            chain=chain,
            wallet_address=wallet_address,
            **kwargs,
        )
        self._state = ArbState.MONITORING
        self._current_opportunity: CrossChainOpportunity | None = None
        self._last_execution_time: float | None = None

    def decide(self, market: MarketSnapshot) -> DecideResult:
        """Make trading decision based on cross-chain price comparison.

        This method:
        1. Checks if strategy is paused or in cooldown
        2. Compares prices across configured chains
        3. Calculates profitability including bridge fees
        4. Returns arbitrage sequence if profitable

        Args:
            market: MarketSnapshot (expected MultiChainMarketSnapshot at runtime)

        Returns:
            DecideResult: Intent sequence for arbitrage or HoldIntent
        """
        if self.config.pause_strategy:
            return Intent.hold(reason="Strategy paused")

        # Check cooldown
        if not self._can_trade():
            remaining = self._cooldown_remaining()
            return Intent.hold(reason=f"Trade cooldown: {remaining}s remaining")

        # Cast to MultiChainMarketSnapshot for cross-chain access
        multi_chain_market: MultiChainMarketSnapshot = market  # type: ignore[assignment]

        # Update state
        self._update_state()

        # State-based decision
        if self._state == ArbState.MONITORING:
            return self._handle_monitoring(multi_chain_market)
        elif self._state == ArbState.OPPORTUNITY_FOUND:
            return self._handle_opportunity(multi_chain_market)
        elif self._state == ArbState.COOLDOWN:
            return Intent.hold(reason="In cooldown period")

        return Intent.hold(reason="Unknown state")

    def _can_trade(self) -> bool:
        """Check if trading is allowed based on cooldown.

        Returns:
            True if trading is allowed
        """
        if self._last_execution_time is None:
            return True

        elapsed = time.time() - self._last_execution_time
        return elapsed >= self.config.cooldown_seconds

    def _cooldown_remaining(self) -> int:
        """Get remaining cooldown time in seconds.

        Returns:
            Seconds remaining in cooldown
        """
        if self._last_execution_time is None:
            return 0

        elapsed = time.time() - self._last_execution_time
        remaining = self.config.cooldown_seconds - elapsed
        return max(0, int(remaining))

    def _update_state(self) -> None:
        """Update strategy state based on current conditions."""
        if self._current_opportunity is not None:
            # Check if opportunity is still fresh (within 30 seconds)
            age = (datetime.now(UTC) - self._current_opportunity.timestamp).total_seconds()
            if age > 30:
                logger.info("Opportunity expired, returning to monitoring")
                self._current_opportunity = None
                self._state = ArbState.MONITORING
            else:
                self._state = ArbState.OPPORTUNITY_FOUND
        elif not self._can_trade():
            self._state = ArbState.COOLDOWN
        else:
            self._state = ArbState.MONITORING

    def _handle_monitoring(self, market: MultiChainMarketSnapshot) -> DecideResult:
        """Handle monitoring state - look for price spreads.

        Args:
            market: Multi-chain market snapshot

        Returns:
            Intent to execute or hold
        """
        logger.debug("Monitoring for cross-chain arbitrage opportunities...")

        # Find best opportunity across all chain pairs
        opportunity = self._find_best_opportunity(market)

        if opportunity is None:
            return Intent.hold(reason="No profitable cross-chain opportunity found")

        # Found opportunity
        self._current_opportunity = opportunity
        self._state = ArbState.OPPORTUNITY_FOUND
        self.config.last_opportunity_found = (
            f"{opportunity.token} {opportunity.buy_chain}->{opportunity.sell_chain} "
            f"+{opportunity.net_profit_bps}bps net"
        )

        logger.info(
            f"Found cross-chain opportunity: "
            f"{opportunity.token} from {opportunity.buy_chain} to {opportunity.sell_chain}, "
            f"raw spread: {opportunity.raw_spread_bps}bps, "
            f"net profit: {opportunity.net_profit_bps}bps (${opportunity.estimated_profit_usd:.2f})"
        )

        return self._build_arbitrage_sequence(opportunity)

    def _handle_opportunity(self, market: MultiChainMarketSnapshot) -> DecideResult:
        """Handle opportunity found state.

        Args:
            market: Multi-chain market snapshot

        Returns:
            Intent to execute
        """
        if self._current_opportunity is None:
            self._state = ArbState.MONITORING
            return Intent.hold(reason="Opportunity expired")

        # Re-verify the opportunity is still valid
        opportunity = self._verify_opportunity(market, self._current_opportunity)
        if opportunity is None:
            self._current_opportunity = None
            self._state = ArbState.MONITORING
            return Intent.hold(reason="Opportunity no longer profitable")

        return self._build_arbitrage_sequence(opportunity)

    def _find_best_opportunity(self, market: MultiChainMarketSnapshot) -> CrossChainOpportunity | None:
        """Find the best cross-chain arbitrage opportunity.

        Scans all configured chain pairs for profitable spreads.

        Args:
            market: Multi-chain market snapshot

        Returns:
            Best opportunity or None if none profitable
        """
        best_opportunity: CrossChainOpportunity | None = None
        best_net_profit = 0

        chains = self.config.chains
        token = self.config.quote_token

        # Check all chain pairs
        for i, chain_a in enumerate(chains):
            for chain_b in chains[i + 1 :]:
                # Check both directions
                opportunity = self._check_chain_pair(market, chain_a, chain_b, token)
                if opportunity and opportunity.net_profit_bps > best_net_profit:
                    best_opportunity = opportunity
                    best_net_profit = opportunity.net_profit_bps

        return best_opportunity

    def _check_chain_pair(
        self,
        market: MultiChainMarketSnapshot,
        chain_a: str,
        chain_b: str,
        token: str,
    ) -> CrossChainOpportunity | None:
        """Check for arbitrage opportunity between two chains.

        Args:
            market: Multi-chain market snapshot
            chain_a: First chain
            chain_b: Second chain
            token: Token to check

        Returns:
            CrossChainOpportunity if profitable, None otherwise
        """
        try:
            # Get price difference
            spread = market.price_difference(token, chain_a=chain_a, chain_b=chain_b)

            if spread is None:
                return None

            # Convert spread to basis points (spread is decimal, e.g. 0.005 = 50 bps)
            raw_spread_bps = int(abs(spread) * 10000)

            # Quick check: skip if raw spread is below minimum
            if raw_spread_bps < self.config.min_spread_bps:
                return None

            # Determine buy/sell chains
            # Positive spread means chain_a > chain_b, so buy on chain_b
            if spread > 0:
                buy_chain = chain_b
                sell_chain = chain_a
            else:
                buy_chain = chain_a
                sell_chain = chain_b

            # Get bridge info
            bridge_provider = self.config.bridge_provider
            bridge_fee_bps = self.config.get_bridge_fee_bps(bridge_provider)
            bridge_latency = self.config.get_bridge_latency_seconds(bridge_provider)

            # Check latency constraint
            if bridge_latency > self.config.max_bridge_latency_seconds:
                logger.debug(f"Bridge latency {bridge_latency}s exceeds max {self.config.max_bridge_latency_seconds}s")
                return None

            # Calculate net profit
            net_profit_bps = self.config.calculate_net_profit_bps(raw_spread_bps, bridge_provider)

            # Check profitability
            if net_profit_bps < self.config.min_spread_after_fees_bps:
                return None

            # Estimate USD profit
            estimated_profit = self.config.estimate_profit_usd(
                raw_spread_bps,
                self.config.trade_amount_usd,
                bridge_provider,
            )

            # Skip if profit is negative after gas
            if estimated_profit <= Decimal("0"):
                return None

            # Get prices for tracking
            buy_price = market.price(token, chain=buy_chain)
            sell_price = market.price(token, chain=sell_chain)

            return CrossChainOpportunity(
                buy_chain=buy_chain,
                sell_chain=sell_chain,
                token=token,
                raw_spread_bps=raw_spread_bps,
                net_profit_bps=net_profit_bps,
                estimated_profit_usd=estimated_profit,
                bridge_provider=bridge_provider,
                bridge_fee_bps=bridge_fee_bps,
                bridge_latency_seconds=bridge_latency,
                buy_price=buy_price,
                sell_price=sell_price,
                timestamp=datetime.now(UTC),
            )

        except Exception as e:
            logger.debug(f"Error checking chain pair {chain_a}/{chain_b}: {e}")
            return None

    def _verify_opportunity(
        self,
        market: MultiChainMarketSnapshot,
        opportunity: CrossChainOpportunity,
    ) -> CrossChainOpportunity | None:
        """Verify that an opportunity is still valid.

        Args:
            market: Multi-chain market snapshot
            opportunity: Previously found opportunity

        Returns:
            Updated opportunity if still valid, None otherwise
        """
        # Re-check the specific chain pair
        return self._check_chain_pair(
            market,
            opportunity.buy_chain,
            opportunity.sell_chain,
            opportunity.token,
        )

    def _build_arbitrage_sequence(self, opportunity: CrossChainOpportunity) -> IntentSequence:
        """Build the arbitrage intent sequence.

        Creates a 3-step sequence:
        1. Buy token on cheaper chain
        2. Bridge token to more expensive chain
        3. Sell token on more expensive chain

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            IntentSequence for execution
        """
        logger.info(
            f"Building arbitrage sequence: "
            f"buy {opportunity.token} on {opportunity.buy_chain}, "
            f"bridge to {opportunity.sell_chain}, sell for profit"
        )

        # Record execution
        self._record_trade(opportunity)

        return Intent.sequence(
            [
                # Step 1: Buy token on cheaper chain
                Intent.swap(
                    from_token=self.config.base_token,
                    to_token=opportunity.token,
                    amount_usd=self.config.trade_amount_usd,
                    max_slippage=self.config.max_slippage_swap,
                    protocol="uniswap_v3",
                    chain=opportunity.buy_chain,
                ),
                # Step 2: Bridge token to sell chain
                Intent.bridge(
                    token=opportunity.token,
                    amount="all",  # Use all output from step 1
                    from_chain=opportunity.buy_chain,
                    to_chain=opportunity.sell_chain,
                    max_slippage=self.config.max_slippage_bridge,
                    preferred_bridge=opportunity.bridge_provider,
                ),
                # Step 3: Sell token on expensive chain
                Intent.swap(
                    from_token=opportunity.token,
                    to_token=self.config.base_token,
                    amount="all",  # Use all output from step 2
                    max_slippage=self.config.max_slippage_swap,
                    protocol="uniswap_v3",
                    chain=opportunity.sell_chain,
                ),
            ],
            description=(
                f"Cross-chain arbitrage: {opportunity.buy_chain} -> {opportunity.sell_chain} "
                f"({opportunity.net_profit_bps}bps net, ${opportunity.estimated_profit_usd:.2f})"
            ),
        )

    def _record_trade(self, opportunity: CrossChainOpportunity) -> None:
        """Record trade execution for tracking.

        Args:
            opportunity: The executed opportunity
        """
        self._last_execution_time = time.time()
        self.config.last_trade_timestamp = int(self._last_execution_time)
        self.config.total_trades += 1
        self.config.total_profit_usd += opportunity.estimated_profit_usd
        self._current_opportunity = None
        self._state = ArbState.COOLDOWN

    def _check_balance(
        self,
        market: MultiChainMarketSnapshot,
        chain: str,
        token: str,
        required_amount: Decimal,
    ) -> bool:
        """Check if balance is sufficient on a chain.

        Args:
            market: Multi-chain market snapshot
            chain: Chain to check
            token: Token to check
            required_amount: Required amount

        Returns:
            True if balance is sufficient
        """
        try:
            balance = market.balance(token, chain=chain)
            if balance is None:
                return False
            return balance.balance >= required_amount
        except Exception:
            return False

    # Public methods for external access

    def get_state(self) -> ArbState:
        """Get current strategy state."""
        return self._state

    def get_current_opportunity(self) -> CrossChainOpportunity | None:
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
            "failed_trades": self.config.failed_trades,
            "total_profit_usd": str(self.config.total_profit_usd),
            "last_trade_timestamp": self.config.last_trade_timestamp,
            "last_opportunity_found": self.config.last_opportunity_found,
            "cooldown_remaining": self._cooldown_remaining(),
            "chains": self.config.chains,
            "quote_token": self.config.quote_token,
            "base_token": self.config.base_token,
        }

    def calculate_expected_fees(
        self,
        bridge: str | None = None,
    ) -> dict[str, int]:
        """Calculate expected fees for a trade.

        Args:
            bridge: Bridge provider name

        Returns:
            Dictionary with fee breakdown in basis points
        """
        bridge_fee = self.config.get_bridge_fee_bps(bridge)
        swap_slippage = int(self.config.max_slippage_swap * 10000) * 2
        bridge_slippage = int(self.config.max_slippage_bridge * 10000)

        return {
            "bridge_fee_bps": bridge_fee,
            "swap_slippage_bps": swap_slippage,
            "bridge_slippage_bps": bridge_slippage,
            "total_fees_bps": bridge_fee + swap_slippage + bridge_slippage,
        }

    def clear_state(self) -> None:
        """Clear strategy state and statistics."""
        self._state = ArbState.MONITORING
        self._current_opportunity = None
        self._last_execution_time = None
        self.config.last_trade_timestamp = None
        self.config.last_opportunity_found = None
        self.config.total_profit_usd = Decimal("0")
        self.config.total_trades = 0
        self.config.failed_trades = 0

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Cross-chain arbitrage may have intermediate token holdings
        on different chains if execution was interrupted.

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        For cross-chain arb, positions are token holdings on each chain.
        If interrupted mid-execution, may have tokens on source or dest chain.

        Returns:
            TeardownPositionSummary with token position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        position_idx = 0

        # Track potential holdings on each chain
        for chain in self.config.chains:
            for token in self.config.tokens:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"cross_chain_arb_token_{position_idx}",
                        chain=chain,
                        protocol="enso",  # Main swap/bridge provider
                        value_usd=self.config.trade_amount_usd,
                        details={
                            "asset": token,
                            "token": token,
                            "amount": str(self.config.trade_amount_usd),
                        },
                    )
                )
                position_idx += 1

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "cross_chain_arbitrage"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        For cross-chain arb, teardown converts all tokens on all chains to USDC.

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage tolerance

        Returns:
            List of SWAP intents for each chain
        """
        from almanak.framework.teardown import TeardownMode

        intents: list = []

        # Slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.03")  # 3% emergency
        else:
            max_slippage = Decimal(str(self.config.max_slippage_bps)) / Decimal("10000")

        # Swap all non-USDC tokens to USDC on each chain
        for chain in self.config.chains:
            for token in self.config.tokens:
                if token == "USDC":
                    continue

                logger.info(f"Generating teardown: swap {token} -> USDC on {chain} (mode={mode.value})")
                intents.append(
                    Intent.swap(
                        from_token=token,
                        to_token="USDC",
                        amount="all",
                        max_slippage=max_slippage,
                        protocol="enso",  # Best routing
                        chain=chain,
                    )
                )

        return intents


# Keep backward compatibility with old config name
CrossChainArbitrageConfig = CrossChainArbConfig


__all__ = [
    "CrossChainArbitrageStrategy",
    "CrossChainArbitrageConfig",
    "CrossChainArbConfig",
    "CrossChainOpportunity",
    "ArbState",
]
