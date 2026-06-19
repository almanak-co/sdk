"""Portfolio tracker for paper trading sessions.

This module provides the PaperPortfolioTracker class that tracks portfolio
state during paper trading, including token balances, trades, errors,
and performance metrics.

Classes:
    - PaperPortfolioTracker: Tracks paper trading portfolio state
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.core.chains import DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradingSummary,
)

logger = logging.getLogger(__name__)


class MissingPriceError(ValueError):
    """Raised when a held (non-zero) token has no supplied USD price.

    VIB-3164: portfolio valuation must not silently substitute a price
    (e.g. force a "stablecoin" to $1) or silently drop an unpriced holding.
    Either path corrupts the reported portfolio value / PnL. The caller must
    supply the missing prices.
    """

    def __init__(self, tokens: list[str]) -> None:
        self.tokens = list(tokens)
        super().__init__(
            "Cannot value portfolio: no USD price supplied for held token(s) "
            f"{sorted(self.tokens)}. Provide prices for every non-zero holding "
            "(stablecoins included -- do not assume $1)."
        )


def _trade_intent_type(trade: PaperTrade) -> str:
    intent_type = getattr(trade, "intent_type", "") or ""
    if intent_type:
        return intent_type.upper()
    if isinstance(trade.intent, dict):
        return str(trade.intent.get("type", "")).upper()
    return ""


def _has_negative_token_flows(trade: PaperTrade) -> bool:
    return any(amount < 0 for token_flows in (trade.tokens_in, trade.tokens_out) for amount in token_flows.values())


def _swap_has_zero_inflow(trade: PaperTrade, intent_type: str) -> bool:
    if intent_type != "SWAP" or not trade.tokens_out:
        return False
    return not any(amount > 0 for amount in trade.tokens_in.values())


@dataclass
class PaperPortfolioTracker:
    """Tracks paper trading portfolio state.

    This class maintains the current state of a paper trading session,
    including token balances, executed trades, errors, and provides
    methods to calculate PnL and generate session summaries.

    Attributes:
        deployment_id: Identifier of the strategy being tracked
        chain: Target blockchain (default: arbitrum)
        initial_balances: Token balances at session start
        current_balances: Current token balances
        trades: List of successful trades
        errors: List of trade errors
        session_started: When the session was started
        total_gas_used: Cumulative gas used
        total_gas_cost_usd: Cumulative gas cost in USD

    Example:
        tracker = PaperPortfolioTracker(deployment_id="my_strategy")
        tracker.start_session({"ETH": Decimal("10"), "USDC": Decimal("10000")})

        # Record trades as they happen
        tracker.record_trade(paper_trade)

        # Get PnL with current prices
        pnl = tracker.get_pnl_usd({"ETH": Decimal("2000"), "USDC": Decimal("1")})

        # Get session summary
        summary = tracker.get_summary()
    """

    deployment_id: str
    chain: str = DEFAULT_CHAIN
    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    current_balances: dict[str, Decimal] = field(default_factory=dict)
    trades: list[PaperTrade] = field(default_factory=list)
    errors: list[PaperTradeError] = field(default_factory=list)
    session_started: datetime | None = None
    total_gas_used: int = 0
    total_gas_cost_usd: Decimal = Decimal("0")

    def start_session(
        self,
        initial_balances: dict[str, Decimal],
        chain: str = DEFAULT_CHAIN,
    ) -> None:
        """Start a new paper trading session.

        Initializes the session with the given token balances.
        Must be called before recording trades.

        Args:
            initial_balances: Starting token balances {token_symbol: amount}
            chain: Target blockchain (default: arbitrum)

        Example:
            tracker.start_session({
                "ETH": Decimal("10"),
                "USDC": Decimal("10000"),
                "WBTC": Decimal("0.5"),
            })
        """
        self.session_started = datetime.now(UTC)
        self.chain = chain

        # Store initial balances
        self.initial_balances = {k: Decimal(str(v)) for k, v in initial_balances.items()}

        # Initialize current balances as copy of initial
        self.current_balances = {k: Decimal(str(v)) for k, v in initial_balances.items()}

        # Reset tracking state
        self.trades = []
        self.errors = []
        self.total_gas_used = 0
        self.total_gas_cost_usd = Decimal("0")

    def record_trade(self, trade: PaperTrade) -> None:
        """Record a successful trade and update balances.

        Updates current_balances based on the trade's token flows
        and accumulates gas usage statistics.

        Args:
            trade: The PaperTrade to record

        Example:
            trade = PaperTrade(
                timestamp=datetime.now(timezone.utc),
                block_number=12345,
                intent={"type": "SWAP"},
                tx_hash="0x...",
                gas_used=150000,
                gas_cost_usd=Decimal("0.50"),
                tokens_in={"WETH": Decimal("1")},
                tokens_out={"USDC": Decimal("2000")},
            )
            tracker.record_trade(trade)
        """
        intent_type = _trade_intent_type(trade)
        if _has_negative_token_flows(trade):
            logger.error(
                "[paper-trading] SANITY GUARD: Trade has negative token flows "
                "(tokens_in=%s, tokens_out=%s). Rejecting trade to prevent balance corruption.",
                trade.tokens_in,
                trade.tokens_out,
            )
            return

        # VIB-2551: reject swap zero-amount inflows before mutating balances.
        if _swap_has_zero_inflow(trade, intent_type):
            logger.error(
                "[paper-trading] SANITY GUARD: Swap trade has zero/empty inflows "
                "(tokens_in=%s, tokens_out=%s). Rejecting trade to prevent balance corruption.",
                trade.tokens_in,
                trade.tokens_out,
            )
            return

        # Add trade to list
        self.trades.append(trade)

        # Update gas tracking
        self.total_gas_used += trade.gas_used
        self.total_gas_cost_usd += trade.gas_cost_usd

        # Update balances from token flows
        # tokens_out = tokens we sent (decrease balance)
        for token, amount in trade.tokens_out.items():
            current = self.current_balances.get(token, Decimal("0"))
            self.current_balances[token] = current - amount

        # tokens_in = tokens we received (increase balance)
        for token, amount in trade.tokens_in.items():
            current = self.current_balances.get(token, Decimal("0"))
            self.current_balances[token] = current + amount

        # Clean up zero balances
        self._cleanup_zero_balances()

    def record_error(self, error: PaperTradeError) -> None:
        """Record a trade error.

        Errors don't affect balances but are tracked for reporting.

        Args:
            error: The PaperTradeError to record

        Example:
            error = PaperTradeError(
                timestamp=datetime.now(timezone.utc),
                intent={"type": "SWAP"},
                error_type=PaperTradeErrorType.REVERT,
                error_message="Slippage exceeded",
            )
            tracker.record_error(error)
        """
        self.errors.append(error)

    def get_pnl_usd(self, current_prices: dict[str, Decimal]) -> Decimal:
        """Calculate current PnL in USD.

        Compares current portfolio value to initial portfolio value
        using the provided current prices.

        Args:
            current_prices: Current token prices in USD {token_symbol: price_usd}

        Returns:
            Net PnL in USD (positive = profit, negative = loss)

        Example:
            prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}
            pnl = tracker.get_pnl_usd(prices)
            # Returns: Decimal("500") if portfolio gained $500
        """
        initial_value = self._calculate_portfolio_value(self.initial_balances, current_prices)
        current_value = self._calculate_portfolio_value(self.current_balances, current_prices)

        # PnL = current value - initial value
        return current_value - initial_value

    def get_summary(self) -> PaperTradingSummary:
        """Generate a paper trading session summary.

        Creates a PaperTradingSummary with all session statistics
        including trade counts, gas usage, and error breakdown.

        Returns:
            PaperTradingSummary with session statistics

        Example:
            summary = tracker.get_summary()
            print(summary.summary())  # Human-readable summary
        """
        # Calculate duration
        if self.session_started is not None:
            duration = datetime.now(UTC) - self.session_started
        else:
            duration = timedelta(seconds=0)

        # Build error summary by type
        error_summary: dict[str, int] = {}
        for error in self.errors:
            error_type_key = error.error_type.value
            error_summary[error_type_key] = error_summary.get(error_type_key, 0) + 1

        # Calculate trade counts
        total_trades = len(self.trades) + len(self.errors)
        successful_trades = len(self.trades)
        failed_trades = len(self.errors)

        return PaperTradingSummary(
            deployment_id=self.deployment_id,
            start_time=self.session_started or datetime.now(UTC),
            duration=duration,
            total_trades=total_trades,
            successful_trades=successful_trades,
            failed_trades=failed_trades,
            chain=self.chain,
            initial_balances=dict(self.initial_balances),
            final_balances=dict(self.current_balances),
            total_gas_used=self.total_gas_used,
            total_gas_cost_usd=self.total_gas_cost_usd,
            pnl_usd=None,  # Requires prices to calculate
            error_summary=error_summary,
            trades=list(self.trades),
            errors=list(self.errors),
        )

    def get_summary_with_pnl(self, current_prices: dict[str, Decimal]) -> PaperTradingSummary:
        """Generate a session summary with PnL calculated.

        Like get_summary() but also calculates and includes the PnL
        using the provided current prices.

        Args:
            current_prices: Current token prices in USD {token_symbol: price_usd}

        Returns:
            PaperTradingSummary with pnl_usd populated

        Example:
            prices = {"ETH": Decimal("2000"), "USDC": Decimal("1")}
            summary = tracker.get_summary_with_pnl(prices)
            print(f"PnL: ${summary.pnl_usd}")
        """
        summary = self.get_summary()
        summary.pnl_usd = self.get_pnl_usd(current_prices)
        return summary

    def get_token_balance(self, token: str) -> Decimal:
        """Get current balance of a specific token.

        Args:
            token: Token symbol (e.g., "ETH", "USDC")

        Returns:
            Current token balance, or Decimal("0") if not held
        """
        return self.current_balances.get(token, Decimal("0"))

    def get_all_balances(self) -> dict[str, Decimal]:
        """Get all current token balances.

        Returns:
            Dictionary of {token_symbol: amount} for all held tokens
        """
        return dict(self.current_balances)

    def get_balance_change(self, token: str) -> Decimal:
        """Get the change in balance for a token since session start.

        Args:
            token: Token symbol

        Returns:
            Change in balance (positive = gained, negative = lost)
        """
        initial = self.initial_balances.get(token, Decimal("0"))
        current = self.current_balances.get(token, Decimal("0"))
        return current - initial

    def get_trade_count(self) -> int:
        """Get total number of successful trades."""
        return len(self.trades)

    def get_error_count(self) -> int:
        """Get total number of errors."""
        return len(self.errors)

    def is_session_active(self) -> bool:
        """Check if a session has been started."""
        return self.session_started is not None

    def _calculate_portfolio_value(
        self,
        balances: dict[str, Decimal],
        prices: dict[str, Decimal],
    ) -> Decimal:
        """Calculate total portfolio value in USD.

        VIB-3164 (Empty != Zero): a held token with no supplied price is
        *unmeasured*, not worth $0 and not worth $1. Silently dropping it
        understates portfolio value and PnL; silently force-pricing a
        "stablecoin" at $1 hides depegs. This method therefore raises
        ``MissingPriceError`` listing the unpriced held tokens so the caller
        supplies the missing prices instead of receiving a wrong number.

        A token with a *zero balance* needs no price (it contributes nothing
        regardless), so it is exempt.

        Args:
            balances: Token balances to value
            prices: Token prices in USD

        Returns:
            Total value in USD

        Raises:
            MissingPriceError: If any token with a non-zero balance has no price.
        """
        total = Decimal("0")
        missing: list[str] = []

        for token, amount in balances.items():
            if token in prices:
                total += amount * prices[token]
            elif amount == Decimal("0"):
                # Zero balance contributes nothing; no price needed.
                continue
            else:
                # No price for a non-zero holding -> unmeasured. Do NOT force $1
                # (even for stablecoins -- depegs are real) and do NOT silently
                # drop it (that understates the portfolio).
                missing.append(token)

        if missing:
            raise MissingPriceError(missing)

        return total

    def _cleanup_zero_balances(self) -> None:
        """Remove tokens with an *exactly zero* balance.

        VIB-3164 (Empty != Zero): a negative balance is a *measured* value
        (e.g. a short / borrow position or an accounting discrepancy worth
        surfacing), not nothing. Only exact zeros are pruned; negatives are
        retained so they remain visible to PnL and debugging.
        """
        tokens_to_remove = [token for token, amount in self.current_balances.items() if amount == Decimal("0")]
        for token in tokens_to_remove:
            del self.current_balances[token]

    def to_dict(self) -> dict[str, Any]:
        """Serialize tracker state to dictionary.

        Useful for persistence and debugging.

        Returns:
            Dictionary representation of tracker state
        """
        return {
            "deployment_id": self.deployment_id,
            "chain": self.chain,
            "session_started": self.session_started.isoformat() if self.session_started else None,
            "initial_balances": {k: str(v) for k, v in self.initial_balances.items()},
            "current_balances": {k: str(v) for k, v in self.current_balances.items()},
            "total_gas_used": self.total_gas_used,
            "total_gas_cost_usd": str(self.total_gas_cost_usd),
            "trade_count": len(self.trades),
            "error_count": len(self.errors),
            "trades": [t.to_dict() for t in self.trades],
            "errors": [e.to_dict() for e in self.errors],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperPortfolioTracker":
        """Deserialize tracker state from dictionary.

        Args:
            data: Dictionary with serialized tracker state

        Returns:
            PaperPortfolioTracker instance
        """
        tracker = cls(
            deployment_id=data["deployment_id"],
            chain=data.get("chain", LEGACY_SERIALIZED_CHAIN),
        )

        # Restore session state
        if data.get("session_started"):
            tracker.session_started = datetime.fromisoformat(data["session_started"])

        # Restore balances
        tracker.initial_balances = {k: Decimal(v) for k, v in data.get("initial_balances", {}).items()}
        tracker.current_balances = {k: Decimal(v) for k, v in data.get("current_balances", {}).items()}

        # Restore gas tracking
        tracker.total_gas_used = data.get("total_gas_used", 0)
        tracker.total_gas_cost_usd = Decimal(data.get("total_gas_cost_usd", "0"))

        # Restore trades and errors
        tracker.trades = [PaperTrade.from_dict(t) for t in data.get("trades", [])]
        tracker.errors = [PaperTradeError.from_dict(e) for e in data.get("errors", [])]

        return tracker


__all__ = ["PaperPortfolioTracker"]
