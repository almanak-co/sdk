"""LST Basis Trading Strategy - Capture LST premium/discount opportunities.

This strategy monitors Liquid Staking Token (LST) prices relative to ETH
and executes swaps when significant premiums or discounts are detected.

Key Features:
    - Monitors stETH, rETH, cbETH prices vs ETH
    - Detects premium/discount opportunities
    - Executes swaps when spread exceeds threshold
    - Uses Curve or Uniswap V3 for optimal execution

Strategy Logic:
    1. Monitor LST/ETH prices (market price vs fair value)
    2. Detect premium (LST > fair value) or discount (LST < fair value)
    3. For premium: Sell LST for ETH (expect price to converge down)
    4. For discount: Buy LST with ETH (expect price to converge up)
    5. Profit from mean reversion of basis spread

Example:
    If stETH trades at 0.995 ETH (0.5% discount vs fair value of 1.0):
    1. Swap ETH -> stETH via Curve stETH pool
    2. Expected profit when stETH returns to fair value: ~50 bps
    3. Net profit after gas and swap fees
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

from .config import LSTBasisConfig

logger = logging.getLogger(__name__)


class LSTBasisState(str, Enum):
    """State of the LST basis trading strategy."""

    MONITORING = "monitoring"  # Monitoring LST prices
    OPPORTUNITY_FOUND = "opportunity_found"  # Found basis opportunity
    EXECUTING = "executing"  # Executing swap
    COOLDOWN = "cooldown"  # Waiting after trade


class BasisDirection(str, Enum):
    """Direction of the basis trade."""

    PREMIUM = "premium"  # LST at premium - sell LST for ETH
    DISCOUNT = "discount"  # LST at discount - buy LST with ETH


@dataclass
class LSTTokenInfo:
    """Information about an LST token.

    Attributes:
        symbol: Token symbol (stETH, rETH, cbETH)
        name: Full token name
        protocol: Staking protocol (Lido, Rocket Pool, Coinbase)
        fair_value_ratio: Base fair value ratio vs ETH (before staking rewards)
        curve_pool: Curve pool for this LST (if available)
        estimated_apy: Estimated staking APY for value accrual
    """

    symbol: str
    name: str
    protocol: str
    fair_value_ratio: Decimal
    curve_pool: str | None
    estimated_apy: Decimal


# LST token information
LST_TOKEN_INFO: dict[str, LSTTokenInfo] = {
    "stETH": LSTTokenInfo(
        symbol="stETH",
        name="Lido Staked ETH",
        protocol="Lido",
        fair_value_ratio=Decimal("1.0"),  # stETH rebases, so 1:1 ratio
        curve_pool="steth",
        estimated_apy=Decimal("0.035"),  # ~3.5% APY
    ),
    "wstETH": LSTTokenInfo(
        symbol="wstETH",
        name="Wrapped Staked ETH",
        protocol="Lido",
        fair_value_ratio=Decimal("1.15"),  # wstETH accumulates value
        curve_pool=None,  # Trade via stETH or Uniswap
        estimated_apy=Decimal("0.035"),
    ),
    "rETH": LSTTokenInfo(
        symbol="rETH",
        name="Rocket Pool ETH",
        protocol="Rocket Pool",
        fair_value_ratio=Decimal("1.08"),  # rETH accumulates value
        curve_pool="reth",
        estimated_apy=Decimal("0.032"),  # ~3.2% APY
    ),
    "cbETH": LSTTokenInfo(
        symbol="cbETH",
        name="Coinbase Staked ETH",
        protocol="Coinbase",
        fair_value_ratio=Decimal("1.05"),  # cbETH accumulates value
        curve_pool="cbeth",
        estimated_apy=Decimal("0.030"),  # ~3.0% APY
    ),
    "frxETH": LSTTokenInfo(
        symbol="frxETH",
        name="Frax Ether",
        protocol="Frax",
        fair_value_ratio=Decimal("1.0"),  # frxETH is pegged 1:1
        curve_pool="frxeth",
        estimated_apy=Decimal("0.040"),  # ~4.0% APY (via sfrxETH)
    ),
}


@dataclass
class LSTBasisOpportunity:
    """Represents an LST basis trading opportunity.

    Attributes:
        lst_token: The LST token (stETH, rETH, cbETH)
        direction: Premium (sell LST) or Discount (buy LST)
        market_price: Current market price in ETH
        fair_value: Fair value based on staking rewards
        spread_bps: Spread in basis points
        trade_amount_eth: Trade amount in ETH
        expected_profit_bps: Expected profit in basis points
        expected_profit_usd: Expected profit in USD (before gas)
        swap_protocol: Recommended swap protocol
        timestamp: When opportunity was found
    """

    lst_token: str
    direction: BasisDirection
    market_price: Decimal
    fair_value: Decimal
    spread_bps: int
    trade_amount_eth: Decimal
    expected_profit_bps: int
    expected_profit_usd: Decimal
    swap_protocol: str
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "lst_token": self.lst_token,
            "direction": self.direction.value,
            "market_price": str(self.market_price),
            "fair_value": str(self.fair_value),
            "spread_bps": self.spread_bps,
            "trade_amount_eth": str(self.trade_amount_eth),
            "expected_profit_bps": self.expected_profit_bps,
            "expected_profit_usd": str(self.expected_profit_usd),
            "swap_protocol": self.swap_protocol,
            "timestamp": self.timestamp.isoformat(),
        }


@almanak_strategy(
    name="lst_basis",
    description="LST basis trading for premium/discount opportunities",
    version="1.0.0",
    author="Almanak",
    tags=["arbitrage", "lst", "staking", "basis", "defi"],
    supported_chains=["ethereum"],
    supported_protocols=["curve", "uniswap_v3", "lido", "rocket_pool", "coinbase"],
    intent_types=["SWAP", "HOLD"],
)
class LSTBasisStrategy(IntentStrategy[LSTBasisConfig]):
    """LST Basis Trading Strategy using Intent pattern.

    This strategy monitors LST prices relative to ETH and executes swaps
    when significant premiums or discounts are detected.

    Key Simplifications:
    - No manual state machine - framework handles execution flow
    - No action bundle construction - IntentCompiler handles TX building
    - Only implements decide() with core business logic

    Trading Flow:
    1. Monitor LST/ETH prices (stETH, rETH, cbETH)
    2. Calculate fair value based on staking rewards/exchange rate
    3. Detect premium/discount opportunities
    4. Execute swap when spread exceeds threshold
    """

    STRATEGY_NAME = "lst_basis"

    def __init__(
        self,
        config: LSTBasisConfig,
        risk_guard_config: RiskGuardConfig | None = None,
        notification_callback: NotificationCallback | None = None,
        compiler: IntentCompiler | None = None,
        state_machine_config: StateMachineConfig | None = None,
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
    ) -> None:
        """Initialize the LST Basis Trading strategy.

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
        self._state = LSTBasisState.MONITORING
        self._current_opportunity: LSTBasisOpportunity | None = None

        # Price cache for basis calculation
        self._price_cache: dict[str, tuple[Decimal, datetime]] = {}

        # Fair value cache (updated periodically)
        self._fair_value_cache: dict[str, tuple[Decimal, datetime]] = {}

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
        2. Monitors LST prices for basis opportunities
        3. Executes swap if profitable opportunity found

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
        if self._state == LSTBasisState.MONITORING:
            return self._handle_monitoring(market)
        elif self._state == LSTBasisState.OPPORTUNITY_FOUND:
            return self._handle_opportunity(market)
        elif self._state == LSTBasisState.COOLDOWN:
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
                self._state = LSTBasisState.MONITORING
            else:
                self._state = LSTBasisState.OPPORTUNITY_FOUND
        elif not self._can_trade():
            self._state = LSTBasisState.COOLDOWN
        else:
            self._state = LSTBasisState.MONITORING

    def _handle_monitoring(self, market: MarketSnapshot) -> DecideResult:
        """Handle monitoring state - look for basis opportunities.

        Args:
            market: Current market snapshot

        Returns:
            Intent to execute or hold
        """
        logger.debug("Monitoring LST prices for basis opportunities...")

        # Check all LST tokens for opportunities
        opportunity = self._find_best_opportunity(market)

        if opportunity is None:
            return Intent.hold(reason="No basis opportunity found")

        # Found opportunity - store and proceed
        self._current_opportunity = opportunity
        self._state = LSTBasisState.OPPORTUNITY_FOUND
        self.config.last_opportunity_found = (
            f"{opportunity.lst_token} {opportunity.direction.value} {opportunity.spread_bps}bps"
        )

        logger.info(
            f"Found basis opportunity: "
            f"{opportunity.lst_token} {opportunity.direction.value}, "
            f"market={opportunity.market_price:.4f} fair={opportunity.fair_value:.4f}, "
            f"spread={opportunity.spread_bps}bps, "
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
            self._state = LSTBasisState.MONITORING
            return Intent.hold(reason="Opportunity expired")

        return self._create_swap_intent(self._current_opportunity)

    def _find_best_opportunity(self, market: MarketSnapshot) -> LSTBasisOpportunity | None:
        """Find the best basis opportunity across all LST tokens.

        Args:
            market: Current market snapshot

        Returns:
            Best opportunity or None if none profitable
        """
        best_opportunity: LSTBasisOpportunity | None = None
        best_profit_bps = 0

        for lst_token in self.config.lst_tokens:
            if lst_token not in LST_TOKEN_INFO:
                logger.warning(f"Unknown LST token: {lst_token}")
                continue

            opportunity = self._check_token_opportunity(market, lst_token)
            if opportunity and opportunity.expected_profit_bps > best_profit_bps:
                best_opportunity = opportunity
                best_profit_bps = opportunity.expected_profit_bps

        return best_opportunity

    def _check_token_opportunity(self, market: MarketSnapshot, lst_token: str) -> LSTBasisOpportunity | None:
        """Check for basis opportunity for a specific LST token.

        Args:
            market: Current market snapshot
            lst_token: LST token to check

        Returns:
            LSTBasisOpportunity if profitable, None otherwise
        """
        try:
            # Get market price (LST/ETH)
            market_price = self._get_lst_price(market, lst_token)
            self._price_cache[lst_token] = (market_price, datetime.now(UTC))

            # Get fair value
            fair_value = self._get_fair_value(market, lst_token)
            self._fair_value_cache[lst_token] = (fair_value, datetime.now(UTC))

            # Calculate spread
            spread_bps = self.config.calculate_spread_bps(market_price, fair_value)

            # Check if this is an opportunity
            if not self.config.is_opportunity(spread_bps):
                return None

            # Determine direction
            if spread_bps > 0:
                direction = BasisDirection.PREMIUM
            else:
                direction = BasisDirection.DISCOUNT

            # Calculate expected profit
            # Profit = |spread_bps| - swap_fee - slippage
            swap_fee_bps = 4 if LST_TOKEN_INFO[lst_token].curve_pool else 30  # Curve ~4bps, Uni ~30bps
            expected_profit_bps = abs(spread_bps) - swap_fee_bps

            # Calculate trade size
            trade_amount_eth = min(
                self.config.default_trade_size_eth,
                self.config.max_trade_size_eth,
            )

            # Calculate USD profit (estimate ETH price at $2500)
            eth_price_usd = self._get_eth_price_usd(market)
            trade_amount_usd = trade_amount_eth * eth_price_usd
            expected_profit_usd = trade_amount_usd * Decimal(expected_profit_bps) / Decimal("10000")

            # Check profitability
            if not self.config.is_profitable(expected_profit_usd, expected_profit_bps):
                return None

            # Select swap protocol
            token_info = LST_TOKEN_INFO[lst_token]
            swap_protocol = token_info.curve_pool if token_info.curve_pool else "uniswap_v3"

            return LSTBasisOpportunity(
                lst_token=lst_token,
                direction=direction,
                market_price=market_price,
                fair_value=fair_value,
                spread_bps=spread_bps,
                trade_amount_eth=trade_amount_eth,
                expected_profit_bps=expected_profit_bps,
                expected_profit_usd=expected_profit_usd,
                swap_protocol=swap_protocol,
                timestamp=datetime.now(UTC),
            )

        except Exception as e:
            logger.debug(f"Error checking opportunity for {lst_token}: {e}")
            return None

    def _get_lst_price(self, market: MarketSnapshot, lst_token: str) -> Decimal:
        """Get the current market price of LST in ETH.

        Args:
            market: Current market snapshot
            lst_token: LST token symbol

        Returns:
            Price in ETH
        """
        # Get LST/ETH price from market
        # For rebasing tokens (stETH), this should be ~1.0
        # For accumulating tokens (rETH, cbETH), this grows over time
        try:
            return market.price(lst_token, "ETH")
        except Exception:
            # Fallback to manual calculation via USD prices
            lst_usd = market.price(lst_token, "USD")
            eth_usd = market.price("ETH", "USD")
            if eth_usd == Decimal("0"):
                return Decimal("1.0")
            return lst_usd / eth_usd

    def _get_fair_value(self, market: MarketSnapshot, lst_token: str) -> Decimal:
        """Get the fair value of LST in ETH based on staking rewards.

        For rebasing tokens (stETH), fair value is always 1.0.
        For accumulating tokens (rETH, cbETH), fair value grows with staking rewards.

        Args:
            market: Current market snapshot
            lst_token: LST token symbol

        Returns:
            Fair value in ETH
        """
        token_info = LST_TOKEN_INFO.get(lst_token)
        if token_info is None:
            return Decimal("1.0")

        # For rebasing tokens, fair value is always 1:1
        if lst_token in ("stETH", "frxETH"):
            return Decimal("1.0")

        # For accumulating tokens, use the base ratio
        # In production, this would query the on-chain exchange rate
        # from the staking contract (e.g., rETH.getExchangeRate())
        return token_info.fair_value_ratio

    def _get_eth_price_usd(self, market: MarketSnapshot) -> Decimal:
        """Get ETH price in USD.

        Args:
            market: Current market snapshot

        Returns:
            ETH price in USD
        """
        try:
            return market.price("ETH", "USD")
        except Exception:
            return Decimal("2500")  # Default fallback

    def _create_swap_intent(self, opportunity: LSTBasisOpportunity) -> SwapIntent:
        """Create swap intent for basis trade execution.

        Args:
            opportunity: The basis opportunity

        Returns:
            SwapIntent for the trade
        """
        logger.info(
            f"Creating swap intent: "
            f"{opportunity.direction.value} {opportunity.lst_token} "
            f"via {opportunity.swap_protocol}"
        )

        # Determine swap direction based on basis direction
        if opportunity.direction == BasisDirection.PREMIUM:
            # LST at premium - sell LST for ETH
            from_token = opportunity.lst_token
            to_token = "ETH"
            # Convert ETH amount to LST amount
            amount = opportunity.trade_amount_eth / opportunity.market_price
        else:
            # LST at discount - buy LST with ETH
            from_token = "ETH"
            to_token = opportunity.lst_token
            amount = opportunity.trade_amount_eth

        # Create swap intent
        swap_intent = Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount=amount,
            max_slippage=Decimal(self.config.max_slippage_bps) / Decimal("10000"),
            protocol=opportunity.swap_protocol,
            chain=self.config.chain,
        )

        # Update tracking
        self._record_trade(opportunity)

        return swap_intent

    def _record_trade(self, opportunity: LSTBasisOpportunity) -> None:
        """Record trade execution for tracking.

        Args:
            opportunity: The opportunity being traded
        """
        self.config.last_trade_timestamp = int(time.time())
        self.config.total_trades += 1

        # Calculate profit in ETH
        profit_eth = opportunity.trade_amount_eth * Decimal(opportunity.expected_profit_bps) / Decimal("10000")
        self.config.total_profit_eth += profit_eth
        self.config.total_profit_usd += opportunity.expected_profit_usd - self.config.estimated_gas_cost_usd

        # Clear opportunity
        self._current_opportunity = None
        self._state = LSTBasisState.COOLDOWN

    # Public methods for external access

    def get_state(self) -> LSTBasisState:
        """Get current strategy state."""
        return self._state

    def get_current_opportunity(self) -> LSTBasisOpportunity | None:
        """Get current basis opportunity if any."""
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
            "total_profit_eth": str(self.config.total_profit_eth),
            "last_trade_timestamp": self.config.last_trade_timestamp,
            "last_opportunity_found": self.config.last_opportunity_found,
            "cooldown_remaining": self._cooldown_remaining(),
            "cached_prices": {token: str(price) for token, (price, _) in self._price_cache.items()},
            "cached_fair_values": {token: str(fv) for token, (fv, _) in self._fair_value_cache.items()},
        }

    def scan_basis(self, market: MarketSnapshot) -> list[dict[str, Any]]:
        """Manually scan for all basis opportunities.

        Args:
            market: Current market snapshot

        Returns:
            List of basis information for all LST tokens
        """
        basis_data: list[dict[str, Any]] = []

        for lst_token in self.config.lst_tokens:
            if lst_token not in LST_TOKEN_INFO:
                continue

            try:
                market_price = self._get_lst_price(market, lst_token)
                fair_value = self._get_fair_value(market, lst_token)
                spread_bps = self.config.calculate_spread_bps(market_price, fair_value)

                direction = "premium" if spread_bps > 0 else "discount" if spread_bps < 0 else "fair"
                is_opportunity = self.config.is_opportunity(spread_bps)

                token_info = LST_TOKEN_INFO[lst_token]
                basis_data.append(
                    {
                        "token": lst_token,
                        "protocol": token_info.protocol,
                        "market_price": str(market_price),
                        "fair_value": str(fair_value),
                        "spread_bps": spread_bps,
                        "direction": direction,
                        "is_opportunity": is_opportunity,
                        "curve_pool": token_info.curve_pool,
                    }
                )
            except Exception as e:
                basis_data.append(
                    {
                        "token": lst_token,
                        "error": str(e),
                    }
                )

        return basis_data

    def clear_state(self) -> None:
        """Clear strategy state and statistics."""
        self._state = LSTBasisState.MONITORING
        self._current_opportunity = None
        self._price_cache.clear()
        self._fair_value_cache.clear()
        self.config.last_trade_timestamp = None
        self.config.last_opportunity_found = None
        self.config.total_profit_usd = Decimal("0")
        self.config.total_profit_eth = Decimal("0")
        self.config.total_trades = 0

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        LST basis trading holds LST tokens (stETH, rETH, cbETH) waiting
        for basis convergence. Teardown swaps all LSTs back to ETH.

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        For LST basis, positions are LST token holdings.

        Returns:
            TeardownPositionSummary with LST position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        position_idx = 0

        # Track potential LST holdings
        for lst_symbol in self.config.lst_tokens:
            # In production, would query actual balances
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"lst_basis_token_{position_idx}",
                    chain=self.config.chain,
                    protocol="curve",  # Main LST swap venue
                    value_usd=self.config.default_trade_size_usd,
                    details={
                        "asset": lst_symbol,
                        "token": lst_symbol,
                        "is_lst": True,
                        "base_token": "ETH",
                        "amount": str(self.config.default_trade_size_usd),
                    },
                )
            )
            position_idx += 1

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "lst_basis"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        For LST basis, teardown means swapping all LST holdings to ETH.

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage tolerance

        Returns:
            List of SWAP intents to convert LSTs to ETH
        """
        from almanak.framework.teardown import TeardownMode

        intents: list = []

        # Slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.02")  # 2% for LSTs in emergency
        else:
            max_slippage = Decimal(str(self.config.max_slippage_bps)) / Decimal("10000")

        # Swap each LST to WETH
        for lst_symbol in self.config.lst_tokens:
            logger.info(f"Generating teardown: swap {lst_symbol} -> WETH (mode={mode.value})")
            intents.append(
                Intent.swap(
                    from_token=lst_symbol,
                    to_token="WETH",
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="curve",  # Best for LST swaps
                    chain=self.config.chain,
                )
            )

        return intents


__all__ = [
    "LSTBasisStrategy",
    "LSTBasisState",
    "BasisDirection",
    "LSTBasisOpportunity",
    "LST_TOKEN_INFO",
    "LSTTokenInfo",
]
