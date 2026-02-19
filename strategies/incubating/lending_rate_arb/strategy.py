"""Lending Rate Arbitrage Strategy - Captures rate differentials across lending protocols.

This strategy monitors lending rates across Aave V3, Morpho Blue, and Compound V3,
and automatically moves capital to capture rate differentials when the spread exceeds
a configurable threshold.

Key Features:
    - Monitors supply APY across multiple protocols
    - Identifies best yield opportunities per token
    - Executes atomic withdraw -> supply sequences
    - Configurable minimum spread threshold
    - Position tracking for rebalancing decisions

Example:
    If USDC supply APY is:
    - Aave V3: 4.2%
    - Morpho Blue: 5.1%
    - Compound V3: 3.8%

    And capital is currently in Compound V3, the strategy will:
    1. Withdraw USDC from Compound V3
    2. Supply USDC to Morpho Blue
    (Only if spread > min_spread_bps threshold)
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.rates import (
    BestRateResult,
    LendingRate,
    RateMonitor,
    RateSide,
)
from almanak.framework.intents import Intent, IntentCompiler, StateMachineConfig
from almanak.framework.intents.vocabulary import HoldIntent, IntentSequence
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

from .config import LendingRateArbConfig

# Type alias for rate provider callback
RateProvider = Callable[[str, str, str], LendingRate | None]
BestRateProvider = Callable[[str, str, list[str] | None], BestRateResult | None]


logger = logging.getLogger(__name__)


@dataclass
class TokenPosition:
    """Tracks position for a single token across protocols."""

    token: str
    protocol: str
    amount: Decimal
    apy_percent: Decimal
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass
class RebalanceOpportunity:
    """Represents an opportunity to rebalance for better yield."""

    token: str
    from_protocol: str
    to_protocol: str
    from_apy: Decimal
    to_apy: Decimal
    spread_bps: int
    amount: Decimal

    @property
    def spread_percent(self) -> Decimal:
        """Get spread as percentage."""
        return Decimal(self.spread_bps) / Decimal("100")


@almanak_strategy(
    name="lending_rate_arb",
    description="Arbitrage lending rate differences across protocols",
    version="1.0.0",
    author="Almanak",
    tags=["lending", "arbitrage", "rates", "yield"],
    supported_chains=["ethereum", "arbitrum", "optimism", "polygon", "base"],
    supported_protocols=["aave_v3", "morpho_blue", "compound_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD", "SEQUENCE"],
)
class LendingRateArbStrategy(IntentStrategy[LendingRateArbConfig]):
    """Lending Rate Arbitrage Strategy using Intent pattern.

    This strategy monitors lending rates across multiple protocols and
    moves capital to capture rate differentials. It uses the Intent framework
    for simplified execution management.

    Key Simplifications:
    - No manual state machine - framework handles PREPARING/VALIDATING/SADFLOW
    - No action bundle construction - IntentCompiler handles TX building
    - Only implements decide() with core business logic
    """

    # STRATEGY_NAME is set by @almanak_strategy decorator (lowercase)
    STRATEGY_NAME = "lending_rate_arb"

    def __init__(
        self,
        config: LendingRateArbConfig,
        risk_guard_config: RiskGuardConfig | None = None,
        notification_callback: NotificationCallback | None = None,
        compiler: IntentCompiler | None = None,
        state_machine_config: StateMachineConfig | None = None,
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
        rate_monitor: RateMonitor | None = None,
    ) -> None:
        """Initialize the Lending Rate Arbitrage strategy.

        Args:
            config: Strategy configuration
            risk_guard_config: Risk management configuration
            notification_callback: Callback for notifications
            compiler: Intent compiler
            state_machine_config: State machine configuration
            price_oracle: Price data provider
            rsi_provider: RSI data provider
            balance_provider: Balance data provider
            rate_monitor: Lending rate monitor for fetching APYs
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
        # Rate monitor for fetching lending rates
        self._rate_monitor = rate_monitor or RateMonitor(chain=config.chain)
        # Track positions per token per protocol
        self._positions: dict[str, dict[str, Decimal]] = {}
        # Track last known APYs
        self._last_apys: dict[str, dict[str, Decimal]] = {}

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
                # If there's already a running loop, create a new one in a thread
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, coro)
                    return future.result()
            return loop.run_until_complete(coro)
        except RuntimeError:
            # No event loop, create one
            return asyncio.run(coro)

    def _get_best_rate(self, token: str, protocols: list[str] | None = None) -> BestRateResult | None:
        """Get best lending rate for a token across protocols.

        Args:
            token: Token symbol
            protocols: Protocols to compare (default: all configured)

        Returns:
            BestRateResult or None if failed
        """
        try:
            return self._run_async(
                self._rate_monitor.get_best_lending_rate(
                    token=token,
                    side=RateSide.SUPPLY,
                    protocols=protocols or self.config.protocols,
                )
            )
        except Exception as e:
            logger.warning(f"Failed to get best rate for {token}: {e}")
            return None

    def decide(self, market: MarketSnapshot) -> HoldIntent | IntentSequence | None:
        """Decide whether to rebalance positions based on rate differentials.

        This method:
        1. Checks if strategy is paused
        2. Fetches current rates for all tokens across protocols
        3. Identifies best yield opportunities
        4. Executes rebalance if spread exceeds threshold

        Returns:
            Intent to execute (sequence of withdraw->supply) or hold
        """
        if self.config.pause_strategy:
            return Intent.hold(reason="Strategy paused")

        # Find best rebalance opportunity across all tokens
        best_opportunity = self._find_best_opportunity(market)

        if best_opportunity is None:
            return Intent.hold(reason="No profitable rebalance opportunities")

        # Check if spread meets minimum threshold
        if best_opportunity.spread_bps < self.config.min_spread_bps:
            return Intent.hold(
                reason=f"Spread {best_opportunity.spread_bps}bps below threshold {self.config.min_spread_bps}bps"
            )

        # Check if amount meets minimum rebalance threshold
        if best_opportunity.amount < self.config.rebalance_threshold_usd:
            return Intent.hold(
                reason=f"Amount ${best_opportunity.amount} below threshold ${self.config.rebalance_threshold_usd}"
            )

        logger.info(
            f"Executing rebalance: {best_opportunity.token} "
            f"from {best_opportunity.from_protocol} ({best_opportunity.from_apy:.2f}%) "
            f"to {best_opportunity.to_protocol} ({best_opportunity.to_apy:.2f}%) "
            f"spread={best_opportunity.spread_bps}bps amount=${best_opportunity.amount}"
        )

        # Execute atomic withdraw -> supply sequence
        return Intent.sequence(
            [
                Intent.withdraw(
                    protocol=best_opportunity.from_protocol,
                    token=best_opportunity.token,
                    amount=best_opportunity.amount,
                    withdraw_all=False,
                    chain=self.config.chain,
                ),
                Intent.supply(
                    protocol=best_opportunity.to_protocol,
                    token=best_opportunity.token,
                    amount="all",  # Use output from withdraw
                    use_as_collateral=True,  # Required for most protocols
                    chain=self.config.chain,
                ),
            ],
            description=f"Rebalance {best_opportunity.token}: {best_opportunity.from_protocol} -> {best_opportunity.to_protocol}",
        )

    def _find_best_opportunity(self, market: MarketSnapshot) -> RebalanceOpportunity | None:
        """Find the best rebalance opportunity across all tokens.

        Scans all configured tokens and protocols to find the opportunity
        with the highest spread between current position and best rate.

        Returns:
            Best RebalanceOpportunity or None if no opportunities exist
        """
        best: RebalanceOpportunity | None = None

        for token in self.config.tokens:
            opportunity = self._find_opportunity_for_token(market, token)
            if opportunity is None:
                continue

            if best is None or opportunity.spread_bps > best.spread_bps:
                best = opportunity

        return best

    def _find_opportunity_for_token(self, market: MarketSnapshot, token: str) -> RebalanceOpportunity | None:
        """Find rebalance opportunity for a specific token.

        Compares current position's APY with best available APY across protocols.

        Args:
            market: Current market snapshot
            token: Token symbol to check

        Returns:
            RebalanceOpportunity if profitable rebalance exists, None otherwise
        """
        # Get current position for this token
        current_protocol, current_amount = self._get_current_position(market, token)

        if current_protocol is None or current_amount <= 0:
            # No position in this token - check if we should open one
            return self._find_new_position_opportunity(market, token)

        # Get best rate across protocols
        best_result = self._get_best_rate(token, self.config.protocols)

        if best_result is None or best_result.best_rate is None:
            return None

        best_protocol = best_result.best_rate.protocol
        best_apy = best_result.best_rate.apy_percent

        # If already in best protocol, no opportunity
        if best_protocol == current_protocol:
            return None

        # Get current protocol's APY
        current_apy = Decimal("0")
        for rate in best_result.all_rates:
            if rate.protocol == current_protocol:
                current_apy = rate.apy_percent
                break

        # Calculate spread
        spread_bps = int((best_apy - current_apy) * Decimal("100"))

        if spread_bps <= 0:
            return None

        return RebalanceOpportunity(
            token=token,
            from_protocol=current_protocol,
            to_protocol=best_protocol,
            from_apy=current_apy,
            to_apy=best_apy,
            spread_bps=spread_bps,
            amount=current_amount,
        )

    def _find_new_position_opportunity(self, market: MarketSnapshot, token: str) -> RebalanceOpportunity | None:
        """Find opportunity to open a new position in best protocol.

        Checks if we have idle balance that could be earning yield.

        Args:
            market: Current market snapshot
            token: Token symbol to check

        Returns:
            RebalanceOpportunity if we have balance to deploy, None otherwise
        """
        # Check if we have balance in wallet
        try:
            balance_info = market.balance(token)
            balance = balance_info.balance if balance_info else Decimal("0")
        except Exception:
            balance = Decimal("0")

        if balance <= 0:
            return None

        # Get best rate
        best_result = self._get_best_rate(token, self.config.protocols)

        if best_result is None or best_result.best_rate is None:
            return None

        # This is a new position opportunity
        # Spread is effectively the APY since we're earning 0% in wallet
        spread_bps = int(best_result.best_rate.apy_percent * Decimal("100"))

        return RebalanceOpportunity(
            token=token,
            from_protocol="wallet",  # Special marker for wallet balance
            to_protocol=best_result.best_rate.protocol,
            from_apy=Decimal("0"),
            to_apy=best_result.best_rate.apy_percent,
            spread_bps=spread_bps,
            amount=balance,
        )

    def _get_current_position(self, market: MarketSnapshot, token: str) -> tuple[str | None, Decimal]:
        """Get current position for a token.

        Checks config's current_positions first, then falls back to
        querying each protocol for balance.

        Args:
            market: Current market snapshot
            token: Token symbol

        Returns:
            Tuple of (protocol_name, amount) or (None, 0) if no position
        """
        # Check config's position tracking first
        if token in self.config.current_positions:
            positions = self.config.current_positions[token]
            # Return largest position
            if positions:
                max_proto = max(positions.keys(), key=lambda p: positions[p])
                return max_proto, positions[max_proto]

        # Fall back to internal tracking
        if token in self._positions:
            positions = self._positions[token]
            if positions:
                max_proto = max(positions.keys(), key=lambda p: positions[p])
                return max_proto, positions[max_proto]

        return None, Decimal("0")

    def update_position(self, token: str, protocol: str, amount: Decimal) -> None:
        """Update tracked position after successful execution.

        Called by framework after intent execution succeeds.

        Args:
            token: Token symbol
            protocol: Protocol where position is held
            amount: New position amount
        """
        if token not in self._positions:
            self._positions[token] = {}

        if amount > 0:
            self._positions[token][protocol] = amount
        elif protocol in self._positions[token]:
            del self._positions[token][protocol]

        logger.info(f"Updated position: {token} on {protocol} = {amount}")

    def get_positions(self) -> dict[str, dict[str, Decimal]]:
        """Get all current positions.

        Returns:
            Dict mapping token -> {protocol -> amount}
        """
        return self._positions.copy()

    def get_rates_snapshot(self) -> dict[str, dict[str, Decimal]]:
        """Get current rates for all tokens across protocols.

        Useful for monitoring and debugging.

        Returns:
            Dict mapping token -> {protocol -> apy_percent}
        """
        rates: dict[str, dict[str, Decimal]] = {}

        for token in self.config.tokens:
            rates[token] = {}
            result = self._get_best_rate(token, self.config.protocols)
            if result is not None:
                for rate in result.all_rates:
                    rates[token][rate.protocol] = rate.apy_percent

        self._last_apys = rates
        return rates

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Lending strategies have SUPPLY positions that need to be withdrawn.

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        For lending rate arb, positions are SUPPLY positions across protocols.
        Collects positions from both config and internal tracking.

        Returns:
            TeardownPositionSummary with supply position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        position_idx = 0

        # Collect positions from config
        for token, protocol_amounts in self.config.current_positions.items():
            for protocol, amount in protocol_amounts.items():
                if amount > 0:
                    positions.append(
                        PositionInfo(
                            position_type=PositionType.SUPPLY,
                            position_id=f"lending_rate_arb_supply_{position_idx}",
                            chain=self.config.chain,
                            protocol=protocol,
                            value_usd=amount,  # Assuming stablecoins, 1:1
                            details={
                                "asset": token,
                                "token": token,
                                "protocol": protocol,
                                "amount": str(amount),
                            },
                        )
                    )
                    position_idx += 1

        # Also check internal position tracking
        for token, protocol_amounts in self._positions.items():
            for protocol, amount in protocol_amounts.items():
                # Avoid duplicates
                existing = any(p.details.get("asset") == token and p.protocol == protocol for p in positions)
                if not existing and amount > 0:
                    positions.append(
                        PositionInfo(
                            position_type=PositionType.SUPPLY,
                            position_id=f"lending_rate_arb_supply_{position_idx}",
                            chain=self.config.chain,
                            protocol=protocol,
                            value_usd=amount,
                            details={
                                "asset": token,
                                "token": token,
                                "protocol": protocol,
                                "amount": str(amount),
                            },
                        )
                    )
                    position_idx += 1

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "lending_rate_arb"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        For lending strategies, teardown means withdrawing all supplied tokens.
        Order: SUPPLY positions (withdraw all).

        Args:
            mode: TeardownMode (SOFT or HARD) - affects urgency

        Returns:
            List of WITHDRAW intents for all supply positions
        """

        intents: list = []

        # Collect all positions
        all_positions: dict[str, dict[str, Decimal]] = {}

        # From config
        for token, protocol_amounts in self.config.current_positions.items():
            if token not in all_positions:
                all_positions[token] = {}
            for protocol, amount in protocol_amounts.items():
                all_positions[token][protocol] = amount

        # From internal tracking
        for token, protocol_amounts in self._positions.items():
            if token not in all_positions:
                all_positions[token] = {}
            for protocol, amount in protocol_amounts.items():
                if protocol not in all_positions[token]:
                    all_positions[token][protocol] = amount

        # Generate withdraw intents
        for token, protocol_amounts in all_positions.items():
            for protocol, amount in protocol_amounts.items():
                if amount > 0:
                    logger.info(f"Generating teardown: withdraw {token} from {protocol} (mode={mode.value})")
                    intents.append(
                        Intent.withdraw(
                            protocol=protocol,
                            token=token,
                            amount=amount,
                            withdraw_all=True,
                            chain=self.config.chain,
                        )
                    )

        return intents


__all__ = ["LendingRateArbStrategy", "TokenPosition", "RebalanceOpportunity"]
