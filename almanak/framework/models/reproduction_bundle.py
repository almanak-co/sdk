"""Reproduction Bundle model for failure replay.

The ReproductionBundle captures all state and context needed to reproduce
a failure locally, enabling developers to debug and fix issues efficiently.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


@dataclass
class TransactionReceipt:
    """Simplified transaction receipt for bundle storage."""

    transaction_hash: str
    block_number: int
    block_hash: str
    status: int  # 0 = failed, 1 = success
    gas_used: int
    effective_gas_price: int
    logs: list[dict[str, Any]] = field(default_factory=list)
    contract_address: str | None = None
    revert_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "status": self.status,
            "gas_used": self.gas_used,
            "effective_gas_price": self.effective_gas_price,
            "logs": self.logs,
            "contract_address": self.contract_address,
            "revert_reason": self.revert_reason,
        }


@dataclass
class ActionBundle:
    """Represents a bundle of actions/transactions to execute.

    Attributes:
        intent_type: Intent type string (e.g., "SWAP", "LP_OPEN").
        transactions: List of transaction data dicts.
        metadata: Public metadata (safe for logging/serialization).
        sensitive_data: Private data excluded from standard serialization (e.g.,
            additional signer keypairs for Solana multi-sign transactions).
            NOTE: When execution goes through the gateway, sensitive_data transits
            over gRPC (see _sensitive_data handling in execution_service.py).
            It is stripped from to_dict() and must never appear in logs, metrics,
            or persistent storage. All logging paths must guard against leaking
            this field.
    """

    intent_type: str  # e.g., "SWAP", "LP_OPEN", "BORROW"
    transactions: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    sensitive_data: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization.

        Note: sensitive_data is intentionally excluded.
        """
        return {
            "intent_type": self.intent_type,
            "transactions": self.transactions,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ActionBundle":
        """Create an ActionBundle from a dictionary.

        Args:
            data: Dictionary containing bundle data.

        Returns:
            An ActionBundle instance.
        """
        return cls(
            intent_type=data.get("intent_type", "UNKNOWN"),
            transactions=data.get("transactions", []),
            metadata=data.get("metadata", {}),
        )


@dataclass
class MarketData:
    """Market data snapshot at time of failure."""

    timestamp: datetime
    token_prices: dict[str, Decimal] = field(default_factory=dict)
    pool_liquidity: dict[str, Decimal] = field(default_factory=dict)
    gas_price: int | None = None
    base_fee: int | None = None
    priority_fee: int | None = None
    oracle_prices: dict[str, Decimal] = field(default_factory=dict)
    oracle_timestamps: dict[str, datetime] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "token_prices": {k: str(v) for k, v in self.token_prices.items()},
            "pool_liquidity": {k: str(v) for k, v in self.pool_liquidity.items()},
            "gas_price": self.gas_price,
            "base_fee": self.base_fee,
            "priority_fee": self.priority_fee,
            "oracle_prices": {k: str(v) for k, v in self.oracle_prices.items()},
            "oracle_timestamps": {k: v.isoformat() for k, v in self.oracle_timestamps.items()},
        }


@dataclass
class TimelineEventSnapshot:
    """Snapshot of a timeline event for bundle storage."""

    timestamp: datetime
    event_type: str
    description: str
    tx_hash: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "description": self.description,
            "tx_hash": self.tx_hash,
            "metadata": self.metadata,
        }


@dataclass
class ReproductionBundle:
    """Bundle containing all state needed to reproduce a failure locally.

    The ReproductionBundle captures:
    - Strategy and chain context
    - Persistent state and config at time of failure
    - The action that failed
    - Transaction details and receipt
    - Market conditions
    - Recent timeline events leading up to failure

    This enables developers to:
    - Spin up a local fork at the exact block
    - Load the exact state that existed
    - Re-execute the failed action step by step
    - Debug with full context

    Attributes:
        strategy_id: Unique identifier for the strategy
        failure_timestamp: When the failure occurred
        block_number: Block number at time of failure
        chain: Chain identifier (e.g., "arbitrum", "ethereum")
        persistent_state: Strategy's persistent state snapshot
        config: Strategy configuration snapshot
        action_bundle: The action bundle that was being executed
        transaction_hash: Hash of the failed transaction (if any)
        receipt: Transaction receipt (if available)
        market_data: Market conditions at time of failure
        events_before: Timeline events leading up to failure
        tenderly_trace_url: Tenderly trace URL for debugging (if available)
        revert_reason: Decoded revert reason (if available)
        bundle_id: Unique identifier for this bundle
        created_at: When this bundle was created
    """

    # Required identifiers
    strategy_id: str
    failure_timestamp: datetime
    block_number: int
    chain: str

    # State snapshots
    persistent_state: dict[str, Any]
    config: dict[str, Any]

    # Action context
    action_bundle: ActionBundle | None = None
    transaction_hash: str | None = None
    receipt: TransactionReceipt | None = None

    # Market context
    market_data: MarketData | None = None

    # Events leading up to failure
    events_before: list[TimelineEventSnapshot] = field(default_factory=list)

    # Debug info
    tenderly_trace_url: str | None = None
    revert_reason: str | None = None

    # Bundle metadata
    bundle_id: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        """Generate bundle_id if not provided."""
        if not self.bundle_id:
            # Generate a unique bundle ID based on strategy, timestamp, and block
            ts = self.failure_timestamp.strftime("%Y%m%d_%H%M%S")
            self.bundle_id = f"{self.strategy_id}_{ts}_{self.block_number}"

    def to_replay_command(self) -> str:
        """Generate a CLI command to replay this failure locally.

        Returns:
            A CLI command string that can be used to replay the failure.
        """
        cmd_parts = [
            "almanak",
            "replay",
            f"--bundle {self.bundle_id}",
        ]

        # Add chain and block info for clarity
        cmd_parts.append(f"--chain {self.chain}")
        cmd_parts.append(f"--block {self.block_number}")

        # Add verbose flag recommendation for debugging
        cmd_parts.append("--verbose")

        return " ".join(cmd_parts)

    def to_dict(self) -> dict[str, Any]:
        """Convert the bundle to a dictionary for serialization."""
        return {
            "bundle_id": self.bundle_id,
            "strategy_id": self.strategy_id,
            "failure_timestamp": self.failure_timestamp.isoformat(),
            "block_number": self.block_number,
            "chain": self.chain,
            "persistent_state": self.persistent_state,
            "config": self.config,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "transaction_hash": self.transaction_hash,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "market_data": self.market_data.to_dict() if self.market_data else None,
            "events_before": [e.to_dict() for e in self.events_before],
            "tenderly_trace_url": self.tenderly_trace_url,
            "revert_reason": self.revert_reason,
            "created_at": self.created_at.isoformat(),
            "replay_command": self.to_replay_command(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReproductionBundle":
        """Create a ReproductionBundle from a dictionary.

        Args:
            data: Dictionary containing bundle data.

        Returns:
            A ReproductionBundle instance.
        """
        # Parse action_bundle
        action_bundle = None
        if data.get("action_bundle"):
            action_bundle = ActionBundle.from_dict(data["action_bundle"])

        # Parse receipt
        receipt = None
        if data.get("receipt"):
            r = data["receipt"]
            receipt = TransactionReceipt(
                transaction_hash=r["transaction_hash"],
                block_number=r["block_number"],
                block_hash=r["block_hash"],
                status=r["status"],
                gas_used=r["gas_used"],
                effective_gas_price=r["effective_gas_price"],
                logs=r.get("logs", []),
                contract_address=r.get("contract_address"),
                revert_reason=r.get("revert_reason"),
            )

        # Parse market_data
        market_data = None
        if data.get("market_data"):
            md = data["market_data"]
            market_data = MarketData(
                timestamp=datetime.fromisoformat(md["timestamp"]),
                token_prices={k: Decimal(v) for k, v in md.get("token_prices", {}).items()},
                pool_liquidity={k: Decimal(v) for k, v in md.get("pool_liquidity", {}).items()},
                gas_price=md.get("gas_price"),
                base_fee=md.get("base_fee"),
                priority_fee=md.get("priority_fee"),
                oracle_prices={k: Decimal(v) for k, v in md.get("oracle_prices", {}).items()},
                oracle_timestamps={k: datetime.fromisoformat(v) for k, v in md.get("oracle_timestamps", {}).items()},
            )

        # Parse events_before
        events_before = [
            TimelineEventSnapshot(
                timestamp=datetime.fromisoformat(e["timestamp"]),
                event_type=e["event_type"],
                description=e["description"],
                tx_hash=e.get("tx_hash"),
                metadata=e.get("metadata", {}),
            )
            for e in data.get("events_before", [])
        ]

        return cls(
            bundle_id=data["bundle_id"],
            strategy_id=data["strategy_id"],
            failure_timestamp=datetime.fromisoformat(data["failure_timestamp"]),
            block_number=data["block_number"],
            chain=data["chain"],
            persistent_state=data["persistent_state"],
            config=data["config"],
            action_bundle=action_bundle,
            transaction_hash=data.get("transaction_hash"),
            receipt=receipt,
            market_data=market_data,
            events_before=events_before,
            tenderly_trace_url=data.get("tenderly_trace_url"),
            revert_reason=data.get("revert_reason"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC),
        )


# Type alias for failure hook function
FailureHookFn = Callable[[ReproductionBundle], None]

# Storage for registered hooks (would typically be configured elsewhere)
_failure_hooks: list[FailureHookFn] = []


def register_failure_hook(hook: FailureHookFn) -> None:
    """Register a hook to be called when a failure occurs.

    Args:
        hook: A callable that takes a ReproductionBundle.
    """
    _failure_hooks.append(hook)


def unregister_failure_hook(hook: FailureHookFn) -> None:
    """Unregister a previously registered failure hook.

    Args:
        hook: The hook to remove.
    """
    if hook in _failure_hooks:
        _failure_hooks.remove(hook)


def get_failure_hooks() -> list[FailureHookFn]:
    """Get all registered failure hooks.

    Returns:
        List of registered failure hook functions.
    """
    return list(_failure_hooks)


def clear_failure_hooks() -> None:
    """Clear all registered failure hooks."""
    _failure_hooks.clear()


@dataclass
class FailureContext:
    """Context provided to the on_failure hook for bundle generation.

    This dataclass captures all the information needed to generate
    a reproduction bundle from a failure.
    """

    strategy_id: str
    chain: str
    error: Exception
    persistent_state: dict[str, Any]
    config: dict[str, Any]
    action_bundle: ActionBundle | None = None
    transaction_hash: str | None = None
    receipt: TransactionReceipt | None = None
    market_data: MarketData | None = None
    events_before: list[TimelineEventSnapshot] = field(default_factory=list)
    block_number: int | None = None
    tenderly_api_key: str | None = None


# Tenderly chain mappings for trace URLs
TENDERLY_CHAIN_SLUGS: dict[str, str] = {
    "ethereum": "mainnet",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "polygon": "polygon",
    "base": "base",
    "avalanche": "avalanche",
    "bsc": "bsc",
}


def _generate_tenderly_trace_url(
    chain: str,
    tx_hash: str,
    api_key: str | None = None,
) -> str | None:
    """Generate a Tenderly trace URL for the transaction.

    Args:
        chain: Chain identifier.
        tx_hash: Transaction hash.
        api_key: Tenderly API key (optional, for private traces).

    Returns:
        Tenderly trace URL or None if chain not supported.
    """
    chain_slug = TENDERLY_CHAIN_SLUGS.get(chain.lower())
    if not chain_slug:
        return None

    # Public trace URL (works without API key)
    return f"https://dashboard.tenderly.co/tx/{chain_slug}/{tx_hash}"


def _extract_revert_reason(
    receipt: TransactionReceipt | None,
    error: Exception,
) -> str | None:
    """Extract revert reason from receipt or error.

    Args:
        receipt: Transaction receipt (may contain revert reason).
        error: The exception that was raised.

    Returns:
        Revert reason string or None.
    """
    # First check receipt
    if receipt and receipt.revert_reason:
        return receipt.revert_reason

    # Check error message for common patterns
    error_str = str(error)

    # Common revert reason patterns
    patterns = [
        "execution reverted: ",
        "revert: ",
        "Error: ",
        "VM Exception while processing transaction: ",
    ]

    for pattern in patterns:
        if pattern in error_str:
            idx = error_str.find(pattern)
            return error_str[idx + len(pattern) :].strip()

    return None


def on_failure(context: FailureContext) -> ReproductionBundle:
    """Generate a reproduction bundle from a failure context.

    This is the main hook function that should be called when a failure
    occurs. It generates a bundle with all available context and calls
    any registered failure hooks.

    Args:
        context: The failure context containing all available information.

    Returns:
        The generated ReproductionBundle.
    """
    # Get current block number if not provided
    block_number = context.block_number or 0

    # Generate Tenderly trace URL if transaction hash available
    tenderly_url = None
    if context.transaction_hash:
        tenderly_url = _generate_tenderly_trace_url(
            context.chain,
            context.transaction_hash,
            context.tenderly_api_key,
        )

    # Extract revert reason
    revert_reason = _extract_revert_reason(context.receipt, context.error)

    # Create the bundle
    bundle = ReproductionBundle(
        strategy_id=context.strategy_id,
        failure_timestamp=datetime.now(UTC),
        block_number=block_number,
        chain=context.chain,
        persistent_state=context.persistent_state,
        config=context.config,
        action_bundle=context.action_bundle,
        transaction_hash=context.transaction_hash,
        receipt=context.receipt,
        market_data=context.market_data,
        events_before=context.events_before,
        tenderly_trace_url=tenderly_url,
        revert_reason=revert_reason,
    )

    # Call all registered hooks
    for hook in _failure_hooks:
        try:
            hook(bundle)
        except Exception:
            # Don't let hook failures prevent bundle generation
            pass

    return bundle
