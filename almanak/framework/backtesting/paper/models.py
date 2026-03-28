"""Data models for paper trading.

This module defines the core data structures used by the Paper Trader
to record trade executions, errors, and session summaries.

Models:
    - PaperTrade: Record of a successful paper trade execution
    - PaperTradeError: Record of a failed paper trade attempt
    - PaperTradeErrorType: Types of errors that can occur during paper trading
    - PaperTradingSummary: Summary of a paper trading session
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any


class PaperTradeErrorType(StrEnum):
    """Types of errors that can occur during paper trading.

    Categories:
        - Network errors: RPC_ERROR, TIMEOUT_ERROR
        - Transaction errors: SIMULATION_FAILED, REVERT, OUT_OF_GAS
        - Strategy errors: INTENT_INVALID, INSUFFICIENT_BALANCE
        - System errors: FORK_ERROR, INTERNAL_ERROR
    """

    # Network errors
    RPC_ERROR = "rpc_error"  # RPC call failed
    TIMEOUT_ERROR = "timeout_error"  # Transaction timed out

    # Transaction errors
    SIMULATION_FAILED = "simulation_failed"  # Pre-execution simulation failed
    REVERT = "revert"  # Transaction reverted on-chain
    OUT_OF_GAS = "out_of_gas"  # Transaction ran out of gas

    # Strategy errors
    INTENT_INVALID = "intent_invalid"  # Intent validation failed
    INSUFFICIENT_BALANCE = "insufficient_balance"  # Not enough tokens/ETH

    # System errors
    FORK_ERROR = "fork_error"  # Anvil fork error
    INTERNAL_ERROR = "internal_error"  # Internal system error

    # Unknown
    UNKNOWN = "unknown"  # Unknown error type


@dataclass
class PaperTrade:
    """Record of a successful paper trade execution.

    This dataclass captures all details of a trade executed on the Anvil fork,
    including the transaction hash, gas usage, and token flows.

    Attributes:
        timestamp: When the trade was executed (wall clock time)
        block_number: Fork block number where the trade was executed
        intent: The intent object that was executed (serialized)
        tx_hash: Transaction hash from the fork execution
        gas_used: Actual gas used by the transaction
        gas_cost_usd: Gas cost in USD at current ETH price
        tokens_in: Dict of tokens received {token_symbol: amount}
        tokens_out: Dict of tokens sent {token_symbol: amount}
        protocol: Protocol used for the trade (uniswap_v3, aave_v3, etc.)
        intent_type: Type of intent (SWAP, LP_OPEN, etc.)
        execution_time_ms: Time taken to execute the trade in milliseconds
        eth_price_usd: ETH price used for gas cost calculation
        metadata: Additional trade-specific metadata
        expected_amount_out: Expected output amount from quote/simulation
        actual_amount_out: Actual output amount received from receipt
        actual_slippage_bps: Actual slippage in basis points (expected vs actual)
        token_prices_usd: Token prices in USD at execution time for PnL calculation
    """

    timestamp: datetime
    block_number: int
    intent: dict[str, Any]  # Serialized intent
    tx_hash: str
    gas_used: int
    gas_cost_usd: Decimal
    tokens_in: dict[str, Decimal]
    tokens_out: dict[str, Decimal]
    protocol: str = ""
    intent_type: str = ""
    execution_time_ms: int = 0
    eth_price_usd: Decimal = Decimal("0")
    metadata: dict[str, Any] = field(default_factory=dict)
    # Slippage tracking fields
    expected_amount_out: Decimal | None = None
    actual_amount_out: Decimal | None = None
    actual_slippage_bps: int | None = None
    # Token prices at execution time for PnL calculation
    token_prices_usd: dict[str, Decimal] = field(default_factory=dict)

    @property
    def net_token_flow_usd(self) -> Decimal:
        """Get net token flow in USD (received - sent).

        Calculates the net value change from this trade using token prices
        captured at execution time. Convention matches receipt_utils.py and
        engine.py: tokens_in = received (transfers TO wallet),
        tokens_out = sent (transfers FROM wallet).

        Returns:
            Positive value indicates profit (received more than sent),
            negative value indicates loss (sent more than received).
            Returns Decimal("0") if token_prices_usd is empty.
        """
        if not self.token_prices_usd:
            return Decimal("0")

        # Calculate USD value of tokens received (tokens_in = transfers TO wallet)
        tokens_in_usd = Decimal("0")
        for token, amount in self.tokens_in.items():
            price = self.token_prices_usd.get(token.upper(), Decimal("0"))
            tokens_in_usd += amount * price

        # Calculate USD value of tokens sent (tokens_out = transfers FROM wallet)
        tokens_out_usd = Decimal("0")
        for token, amount in self.tokens_out.items():
            price = self.token_prices_usd.get(token.upper(), Decimal("0"))
            tokens_out_usd += amount * price

        # Net flow: what we received minus what we sent
        return tokens_in_usd - tokens_out_usd

    @property
    def net_pnl_usd(self) -> Decimal:
        """Get net PnL in USD including gas costs.

        Calculates the total profit/loss from this trade by taking the
        net token flow and subtracting gas costs.

        Returns:
            Positive value indicates profit (after gas costs),
            negative value indicates loss.
            This is the true per-trade PnL that affects portfolio value.
        """
        return self.net_token_flow_usd - self.gas_cost_usd

    @property
    def gas_gwei(self) -> int:
        """Calculate gas price in Gwei from gas_used and gas_cost_usd.

        Returns:
            Gas price in Gwei, or 0 if ETH price is unavailable
        """
        if self.eth_price_usd == 0 or self.gas_used == 0:
            return 0
        # gas_cost_usd = gas_used * gas_price_wei * eth_price / 1e18
        # gas_price_gwei = gas_cost_usd * 1e9 / (gas_used * eth_price)
        gas_price_gwei = self.gas_cost_usd * Decimal("1000000000") / (Decimal(self.gas_used) * self.eth_price_usd)
        return int(gas_price_gwei)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "block_number": self.block_number,
            "intent": self.intent,
            "tx_hash": self.tx_hash,
            "gas_used": self.gas_used,
            "gas_cost_usd": str(self.gas_cost_usd),
            "tokens_in": {k: str(v) for k, v in self.tokens_in.items()},
            "tokens_out": {k: str(v) for k, v in self.tokens_out.items()},
            "protocol": self.protocol,
            "intent_type": self.intent_type,
            "execution_time_ms": self.execution_time_ms,
            "eth_price_usd": str(self.eth_price_usd),
            "metadata": self.metadata,
            "expected_amount_out": str(self.expected_amount_out) if self.expected_amount_out is not None else None,
            "actual_amount_out": str(self.actual_amount_out) if self.actual_amount_out is not None else None,
            "actual_slippage_bps": self.actual_slippage_bps,
            "token_prices_usd": {k: str(v) for k, v in self.token_prices_usd.items()},
            # Computed PnL values for convenience
            "net_token_flow_usd": str(self.net_token_flow_usd),
            "net_pnl_usd": str(self.net_pnl_usd),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperTrade":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized PaperTrade data

        Returns:
            PaperTrade instance
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            block_number=data["block_number"],
            intent=data["intent"],
            tx_hash=data["tx_hash"],
            gas_used=data["gas_used"],
            gas_cost_usd=Decimal(data["gas_cost_usd"]),
            tokens_in={k: Decimal(v) for k, v in data.get("tokens_in", {}).items()},
            tokens_out={k: Decimal(v) for k, v in data.get("tokens_out", {}).items()},
            protocol=data.get("protocol", ""),
            intent_type=data.get("intent_type", ""),
            execution_time_ms=data.get("execution_time_ms", 0),
            eth_price_usd=Decimal(data.get("eth_price_usd", "0")),
            metadata=data.get("metadata", {}),
            expected_amount_out=Decimal(data["expected_amount_out"])
            if data.get("expected_amount_out") is not None
            else None,
            actual_amount_out=Decimal(data["actual_amount_out"]) if data.get("actual_amount_out") is not None else None,
            actual_slippage_bps=data.get("actual_slippage_bps"),
            token_prices_usd={k: Decimal(v) for k, v in data.get("token_prices_usd", {}).items()},
        )


@dataclass
class PaperTradeError:
    """Record of a failed paper trade attempt.

    This dataclass captures details of trade execution failures,
    including the error type and message for debugging.

    Attributes:
        timestamp: When the error occurred (wall clock time)
        intent: The intent that failed to execute (serialized)
        error_type: Category of the error
        error_message: Detailed error message
        block_number: Fork block number when error occurred (if available)
        tx_hash: Transaction hash if the error occurred after submission
        revert_reason: Decoded revert reason (if available)
        gas_used: Gas used before the error (for reverts)
        metadata: Additional error context
    """

    timestamp: datetime
    intent: dict[str, Any]  # Serialized intent
    error_type: PaperTradeErrorType
    error_message: str
    block_number: int | None = None
    tx_hash: str | None = None
    revert_reason: str | None = None
    gas_used: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_recoverable(self) -> bool:
        """Check if this error is potentially recoverable.

        Recoverable errors (can retry):
        - RPC_ERROR, TIMEOUT_ERROR: Network issues
        - FORK_ERROR: Can reset fork

        Non-recoverable errors (should not retry):
        - REVERT, SIMULATION_FAILED: Transaction logic failed
        - INSUFFICIENT_BALANCE, INTENT_INVALID: Strategy issue
        - OUT_OF_GAS, INTERNAL_ERROR: System issue
        """
        recoverable_types = {
            PaperTradeErrorType.RPC_ERROR,
            PaperTradeErrorType.TIMEOUT_ERROR,
            PaperTradeErrorType.FORK_ERROR,
        }
        return self.error_type in recoverable_types

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "intent": self.intent,
            "error_type": self.error_type.value,
            "error_message": self.error_message,
            "block_number": self.block_number,
            "tx_hash": self.tx_hash,
            "revert_reason": self.revert_reason,
            "gas_used": self.gas_used,
            "is_recoverable": self.is_recoverable,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperTradeError":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized PaperTradeError data

        Returns:
            PaperTradeError instance
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            intent=data["intent"],
            error_type=PaperTradeErrorType(data["error_type"]),
            error_message=data["error_message"],
            block_number=data.get("block_number"),
            tx_hash=data.get("tx_hash"),
            revert_reason=data.get("revert_reason"),
            gas_used=data.get("gas_used"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PaperTradingSummary:
    """Summary of a paper trading session.

    This dataclass provides an overview of the paper trading session,
    including trade counts, timing, and basic performance metrics.

    Attributes:
        strategy_id: Identifier of the strategy being tested
        start_time: When the session started
        duration: How long the session ran
        total_trades: Total number of trades attempted
        successful_trades: Number of successful trades
        failed_trades: Number of failed trades
        end_time: When the session ended (computed)
        chain: Target blockchain
        initial_balances: Starting token balances
        final_balances: Ending token balances
        total_gas_used: Total gas consumed
        total_gas_cost_usd: Total gas cost in USD
        pnl_usd: Estimated PnL in USD (if available)
        error_summary: Count of errors by type
        trades: List of successful trades
        errors: List of trade errors
    """

    strategy_id: str
    start_time: datetime
    duration: timedelta
    total_trades: int
    successful_trades: int
    failed_trades: int
    chain: str = "arbitrum"
    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    final_balances: dict[str, Decimal] = field(default_factory=dict)
    total_gas_used: int = 0
    total_gas_cost_usd: Decimal = Decimal("0")
    pnl_usd: Decimal | None = None
    valuation_source: str = "simple"
    error_summary: dict[str, int] = field(default_factory=dict)
    trades: list[PaperTrade] = field(default_factory=list)
    errors: list[PaperTradeError] = field(default_factory=list)

    @property
    def end_time(self) -> datetime:
        """Get the session end time."""
        return self.start_time + self.duration

    @property
    def success_rate(self) -> Decimal:
        """Calculate the success rate as a decimal (0.0 to 1.0)."""
        if self.total_trades == 0:
            return Decimal("1")  # No trades = 100% success (nothing failed)
        return Decimal(self.successful_trades) / Decimal(self.total_trades)

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        return self.duration.total_seconds()

    @property
    def duration_minutes(self) -> float:
        """Get duration in minutes."""
        return self.duration.total_seconds() / 60

    @property
    def duration_hours(self) -> float:
        """Get duration in hours."""
        return self.duration.total_seconds() / 3600

    @property
    def trades_per_hour(self) -> Decimal:
        """Calculate average trades per hour."""
        hours = Decimal(str(self.duration_hours))
        if hours == 0:
            return Decimal("0")
        return Decimal(self.total_trades) / hours

    @property
    def avg_gas_per_trade(self) -> int:
        """Calculate average gas used per successful trade."""
        if self.successful_trades == 0:
            return 0
        return self.total_gas_used // self.successful_trades

    def summary(self) -> str:
        """Generate a human-readable summary.

        Returns:
            Multi-line string with formatted session summary
        """
        lines = [
            "=" * 70,
            "PAPER TRADING SESSION SUMMARY",
            "=" * 70,
            "",
            "SESSION INFO",
            "-" * 70,
            f"Strategy:           {self.strategy_id}",
            f"Chain:              {self.chain}",
            f"Start Time:         {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"End Time:           {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Duration:           {self.duration_minutes:.1f} minutes",
            "",
            "TRADE STATISTICS",
            "-" * 70,
            f"Total Trades:       {self.total_trades}",
            f"Successful Trades:  {self.successful_trades}",
            f"Failed Trades:      {self.failed_trades}",
            f"Success Rate:       {self.success_rate * 100:.1f}%",
            f"Trades/Hour:        {self.trades_per_hour:.2f}",
            "",
            "GAS USAGE",
            "-" * 70,
            f"Total Gas Used:     {self.total_gas_used:,}",
            f"Avg Gas/Trade:      {self.avg_gas_per_trade:,}",
            f"Total Gas Cost:     ${self.total_gas_cost_usd:,.2f}",
        ]

        if self.pnl_usd is not None:
            lines.extend(
                [
                    "",
                    "PERFORMANCE",
                    "-" * 70,
                    f"Estimated PnL:      ${self.pnl_usd:,.2f}",
                ]
            )

        if self.initial_balances:
            lines.extend(
                [
                    "",
                    "INITIAL BALANCES",
                    "-" * 70,
                ]
            )
            for token, amount in self.initial_balances.items():
                lines.append(f"  {token}: {amount:,.6f}")

        if self.final_balances:
            lines.extend(
                [
                    "",
                    "FINAL BALANCES",
                    "-" * 70,
                ]
            )
            for token, amount in self.final_balances.items():
                lines.append(f"  {token}: {amount:,.6f}")

        if self.error_summary:
            lines.extend(
                [
                    "",
                    "ERROR SUMMARY",
                    "-" * 70,
                ]
            )
            for error_type, count in self.error_summary.items():
                lines.append(f"  {error_type}: {count}")

        lines.append("=" * 70)

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "strategy_id": self.strategy_id,
            "start_time": self.start_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "end_time": self.end_time.isoformat(),
            "total_trades": self.total_trades,
            "successful_trades": self.successful_trades,
            "failed_trades": self.failed_trades,
            "success_rate": str(self.success_rate),
            "chain": self.chain,
            "initial_balances": {k: str(v) for k, v in self.initial_balances.items()},
            "final_balances": {k: str(v) for k, v in self.final_balances.items()},
            "total_gas_used": self.total_gas_used,
            "total_gas_cost_usd": str(self.total_gas_cost_usd),
            "pnl_usd": str(self.pnl_usd) if self.pnl_usd is not None else None,
            "valuation_source": self.valuation_source,
            "error_summary": self.error_summary,
            "trades": [t.to_dict() for t in self.trades],
            "errors": [e.to_dict() for e in self.errors],
            "trades_per_hour": str(self.trades_per_hour),
            "avg_gas_per_trade": self.avg_gas_per_trade,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperTradingSummary":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized PaperTradingSummary data

        Returns:
            PaperTradingSummary instance
        """
        # Parse trades
        trades = [PaperTrade.from_dict(t) for t in data.get("trades", [])]

        # Parse errors
        errors = [PaperTradeError.from_dict(e) for e in data.get("errors", [])]

        return cls(
            strategy_id=data["strategy_id"],
            start_time=datetime.fromisoformat(data["start_time"]),
            duration=timedelta(seconds=data["duration_seconds"]),
            total_trades=data["total_trades"],
            successful_trades=data["successful_trades"],
            failed_trades=data["failed_trades"],
            chain=data.get("chain", "arbitrum"),
            initial_balances={k: Decimal(v) for k, v in data.get("initial_balances", {}).items()},
            final_balances={k: Decimal(v) for k, v in data.get("final_balances", {}).items()},
            total_gas_used=data.get("total_gas_used", 0),
            total_gas_cost_usd=Decimal(data.get("total_gas_cost_usd", "0")),
            pnl_usd=Decimal(data["pnl_usd"]) if data.get("pnl_usd") is not None else None,
            valuation_source=data.get("valuation_source", "simple"),
            error_summary=data.get("error_summary", {}),
            trades=trades,
            errors=errors,
        )


__all__ = [
    "PaperTradeErrorType",
    "PaperTrade",
    "PaperTradeError",
    "PaperTradingSummary",
]
