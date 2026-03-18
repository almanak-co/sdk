"""Execution Orchestrator for coordinating transaction signing, simulation, and submission.

This module provides the ExecutionOrchestrator class that coordinates the full
execution flow from ActionBundle to confirmed transactions.

The orchestrator:
1. Validates transactions via RiskGuard
2. Simulates transactions (if enabled)
3. Assigns sequential nonces
4. Signs transactions via Signer
5. Submits transactions via Submitter
6. Polls for receipts
7. Parses receipts for results

At each step, events are emitted for observability.

Example:
    from almanak.framework.execution import LocalKeySigner, PublicMempoolSubmitter, DirectSimulator
    from almanak.framework.execution.orchestrator import ExecutionOrchestrator

    signer = LocalKeySigner(private_key="0x...")
    submitter = PublicMempoolSubmitter(rpc_url="https://...")
    simulator = DirectSimulator()

    orchestrator = ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain="arbitrum",
    )

    result = await orchestrator.execute(action_bundle)
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.types import HexStr, TxParams

from almanak.framework.execution.config import CHAIN_IDS
from almanak.framework.execution.signer.safe.base import SafeSigner

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..models.reproduction_bundle import ActionBundle
from ..strategies.base import RiskGuard, RiskGuardResult
from ..utils.log_formatters import _emojis_enabled, format_gas_cost, format_tx_hash
from .extracted_data import LPCloseData, SwapAmounts

if TYPE_CHECKING:
    from .outcome import ExecutionOutcome

from .interfaces import (
    ExecutionError,
    GasEstimationError,
    InsufficientFundsError,
    NonceError,
    SignedTransaction,
    Signer,
    SimulationResult,
    Simulator,
    SubmissionError,
    Submitter,
    TransactionReceipt,
    TransactionRevertedError,
    TransactionType,
    UnsignedTransaction,
    _sanitize_logs,
)
from .revert_diagnostics import build_verbose_revert_report
from .session import (
    ExecutionPhase as SessionPhase,
)
from .session import (
    ExecutionSession,
    TransactionState,
    TransactionStatus,
    create_session,
)
from .session_store import ExecutionSessionStore

logger = logging.getLogger(__name__)


# =============================================================================
# Enums and Constants
# =============================================================================


# Import ExecutionEventType from events module (canonical source)
from .events import ExecutionEventType


class ExecutionPhase(StrEnum):
    """Current phase of execution."""

    VALIDATION = "VALIDATION"
    SIMULATION = "SIMULATION"
    NONCE_ASSIGNMENT = "NONCE_ASSIGNMENT"
    SIGNING = "SIGNING"
    SUBMISSION = "SUBMISSION"
    CONFIRMATION = "CONFIRMATION"
    COMPLETE = "COMPLETE"


# Chain-specific gas multipliers - imported from shared constants module
from almanak.framework.execution.gas.constants import (
    CHAIN_GAS_BUFFERS,
    CHAIN_GAS_COST_CAPS_NATIVE,
    CHAIN_GAS_PRICE_CAPS_GWEI,
    DEFAULT_GAS_BUFFER,
    DEFAULT_GAS_PRICE_CAP_GWEI,
)

GAS_BUFFER_MULTIPLIERS = CHAIN_GAS_BUFFERS


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TransactionResult:
    """Result of a single transaction execution.

    Attributes:
        tx_hash: Transaction hash
        success: Whether transaction succeeded
        receipt: Transaction receipt
        gas_used: Actual gas used
        gas_cost_wei: Total gas cost in wei
        logs: Event logs from the transaction
        error: Error message if failed
    """

    tx_hash: str
    success: bool
    receipt: TransactionReceipt | None = None
    gas_used: int = 0
    gas_cost_wei: int = 0
    logs: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tx_hash": self.tx_hash,
            "success": self.success,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "gas_used": self.gas_used,
            "gas_cost_wei": str(self.gas_cost_wei),
            "logs": _sanitize_logs(self.logs),
            "error": self.error,
        }


@dataclass
class ExecutionResult:
    """Complete result of an execution attempt.

    Attributes:
        success: Whether all transactions succeeded
        phase: Phase where execution completed or failed
        transaction_results: Results for each transaction
        simulation_result: Simulation result (if simulation was run)
        total_gas_used: Sum of gas used across all transactions
        total_gas_cost_wei: Sum of gas costs across all transactions
        error: Error message if failed
        error_phase: Phase where error occurred
        started_at: When execution started
        completed_at: When execution completed
        correlation_id: Unique identifier for this execution

        position_id: LP position ID for LP_OPEN intents (NFT tokenId), populated by ResultEnricher
        swap_amounts: Swap execution data for SWAP intents
        lp_close_data: LP close data for LP_CLOSE intents
        bin_ids: TraderJoe V2 bin IDs for LP positions
        extracted_data: Flexible dict for protocol-specific extracted data
        extraction_warnings: Non-fatal warnings from extraction process
    """

    success: bool
    phase: ExecutionPhase
    transaction_results: list[TransactionResult] = field(default_factory=list)
    simulation_result: SimulationResult | None = None
    total_gas_used: int = 0
    total_gas_cost_wei: int = 0
    error: str | None = None
    error_phase: ExecutionPhase | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    correlation_id: str = ""

    # === Gas Estimation Warnings ===
    gas_warnings: list[str] = field(default_factory=list)

    # === Enriched Data (populated by ResultEnricher) ===
    position_id: int | None = None
    swap_amounts: SwapAmounts | None = None
    lp_close_data: LPCloseData | None = None
    bin_ids: list[int] | None = None  # TraderJoe V2 LP bin IDs
    extracted_data: dict[str, Any] = field(default_factory=dict)
    extraction_warnings: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Generate correlation_id if not provided."""
        if not self.correlation_id:
            import uuid

            self.correlation_id = str(uuid.uuid4())

    def get_extracted(self, key: str, expected_type: type | None = None, default: Any = None) -> Any:
        """Get extracted data with optional type checking.

        Provides safe access to protocol-specific extracted data with
        optional type validation.

        Args:
            key: Data key to retrieve from extracted_data
            expected_type: Optional type to validate against
            default: Default value if key not found or wrong type

        Returns:
            Extracted value or default

        Example:
            tick_lower = result.get_extracted("tick_lower", int, 0)
            liquidity = result.get_extracted("liquidity")
        """
        value = self.extracted_data.get(key)
        if value is None:
            return default
        if expected_type is not None and not isinstance(value, expected_type):
            return default
        return value

    def to_outcome(self) -> "ExecutionOutcome":
        """Convert to chain-agnostic ExecutionOutcome.

        Returns:
            ExecutionOutcome with EVM-specific fields mapped to common shape.
        """
        from almanak.framework.execution.outcome import ExecutionOutcome

        return ExecutionOutcome(
            success=self.success,
            tx_ids=[tr.tx_hash for tr in self.transaction_results if tr.tx_hash],
            receipts=[tr.receipt.to_dict() if tr.receipt else {} for tr in self.transaction_results],
            total_fee_native=Decimal(self.total_gas_cost_wei),
            error=self.error,
            chain_family="EVM",
            position_id=self.position_id,
            swap_amounts=self.swap_amounts,
            lp_close_data=self.lp_close_data,
            extracted_data=self.extracted_data,
            extraction_warnings=self.extraction_warnings,
        )

    @property
    def effective_price(self) -> Decimal | None:
        """Convenience accessor for swap effective price."""
        return self.swap_amounts.effective_price if self.swap_amounts else None

    @property
    def slippage_bps(self) -> int | None:
        """Convenience accessor for swap slippage in basis points."""
        return self.swap_amounts.slippage_bps if self.swap_amounts else None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "phase": self.phase.value,
            "transaction_results": [tr.to_dict() for tr in self.transaction_results],
            "simulation_result": self.simulation_result.to_dict() if self.simulation_result else None,
            "total_gas_used": self.total_gas_used,
            "total_gas_cost_wei": str(self.total_gas_cost_wei),
            "error": self.error,
            "error_phase": self.error_phase.value if self.error_phase else None,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "correlation_id": self.correlation_id,
            # Gas warnings
            "gas_warnings": self.gas_warnings,
            # Enriched data
            "position_id": self.position_id,
            "swap_amounts": self.swap_amounts.to_dict() if self.swap_amounts else None,
            "lp_close_data": self.lp_close_data.to_dict() if self.lp_close_data else None,
            "extracted_data": self.extracted_data,
            "extraction_warnings": self.extraction_warnings,
        }


@dataclass
class ExecutionContext:
    """Context for the current execution.

    Attributes:
        strategy_id: Strategy identifier
        intent_id: Intent identifier (for session tracking)
        chain: Blockchain network
        wallet_address: Address executing transactions
        correlation_id: Unique identifier for this execution
        session_id: Execution session identifier (for crash recovery)
        simulation_enabled: Whether to simulate before execution
        dry_run: If True, don't actually submit transactions
    """

    strategy_id: str = "unknown"
    intent_id: str = ""
    chain: str = "arbitrum"
    wallet_address: str = ""
    correlation_id: str = ""
    session_id: str = ""
    simulation_enabled: bool = False
    intent_description: str = ""  # Human-readable description of the intent
    dry_run: bool = False
    protocol: str | None = None  # Resolved protocol (for result enrichment)

    def __post_init__(self) -> None:
        """Generate correlation_id if not provided."""
        if not self.correlation_id:
            import uuid

            self.correlation_id = str(uuid.uuid4())


# =============================================================================
# Transaction Risk Configuration
# =============================================================================


@dataclass
class TransactionRiskConfig:
    """Configuration for transaction-level risk validation.

    Provides configurable limits for the ExecutionOrchestrator's risk guard.
    All checks are enforced BEFORE transactions are submitted to prevent
    unauthorized or excessive operations.

    Attributes:
        max_value_eth: Maximum ETH value per transaction (default 10 ETH)
        max_value_per_token: Per-token maximum values in token units
        allowed_contracts: Whitelist of contract addresses (None = allow all)
        block_contract_deployment: Whether to block contract deployments
        max_gas_price_gwei: Maximum acceptable gas price (0 = no limit)
        max_gas_cost_native: Max gas cost in native token per tx (0 = no limit)
        max_gas_cost_usd: Max gas cost in USD per tx (0 = no limit).
            Requires native_token_price_usd to be set for conversion.
        native_token_price_usd: Current native token price in USD, set by
            the runner before execution. Required for max_gas_cost_usd check.
        max_slippage_bps: Maximum acceptable slippage in basis points for swap
            executions (0 = no limit). Checked post-execution against actual
            slippage extracted from swap receipts. Acts as a circuit breaker
            independent of protocol-level minOut parameters.
        max_daily_volume_eth: Maximum daily volume in ETH (0 = no limit)
    """

    max_value_eth: Decimal = Decimal("10")  # 10 ETH per tx default
    max_value_per_token: dict[str, Decimal] = field(default_factory=dict)
    allowed_contracts: set[str] | None = None  # None = allow all
    block_contract_deployment: bool = True
    max_gas_price_gwei: int = 0  # 0 = no limit
    max_gas_cost_native: float = 0.0  # Max gas cost in native token per tx (0 = no limit)
    max_gas_cost_usd: float = 0.0  # Max gas cost in USD per tx (0 = no limit)
    native_token_price_usd: float = 0.0  # Current native token price for USD conversion
    max_slippage_bps: int = 0  # Max acceptable swap slippage in bps (0 = no limit)
    max_daily_volume_eth: Decimal = Decimal("0")  # 0 = no limit

    # Track daily volume for limit enforcement
    _daily_volume_wei: int = field(default=0, init=False, repr=False)
    _daily_volume_date: str = field(default="", init=False, repr=False)

    @classmethod
    def default(cls) -> "TransactionRiskConfig":
        """Create default risk configuration suitable for production."""
        return cls(
            max_value_eth=Decimal("10"),  # 10 ETH per transaction
            block_contract_deployment=True,
            max_gas_price_gwei=DEFAULT_GAS_PRICE_CAP_GWEI,
            max_daily_volume_eth=Decimal("100"),  # 100 ETH daily limit
        )

    @classmethod
    def for_chain(cls, chain: str) -> "TransactionRiskConfig":
        """Create chain-specific risk configuration with recommended defaults.

        Uses CHAIN_GAS_PRICE_CAPS_GWEI and CHAIN_GAS_COST_CAPS_NATIVE for
        the specified chain, falling back to generic defaults.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
        """
        chain_lower = chain.lower()
        return cls(
            max_value_eth=Decimal("10"),
            block_contract_deployment=True,
            max_gas_price_gwei=CHAIN_GAS_PRICE_CAPS_GWEI.get(chain_lower, DEFAULT_GAS_PRICE_CAP_GWEI),
            max_gas_cost_native=CHAIN_GAS_COST_CAPS_NATIVE.get(chain_lower, 0.0),
            max_daily_volume_eth=Decimal("100"),
        )

    @classmethod
    def permissive(cls) -> "TransactionRiskConfig":
        """Create permissive configuration for testing."""
        return cls(
            max_value_eth=Decimal("1000"),  # 1000 ETH
            block_contract_deployment=False,
            max_gas_price_gwei=0,  # No limit
            max_daily_volume_eth=Decimal("0"),  # No daily limit
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "max_value_eth": str(self.max_value_eth),
            "max_value_per_token": {k: str(v) for k, v in self.max_value_per_token.items()},
            "allowed_contracts": list(self.allowed_contracts) if self.allowed_contracts else None,
            "block_contract_deployment": self.block_contract_deployment,
            "max_gas_price_gwei": self.max_gas_price_gwei,
            "max_gas_cost_native": self.max_gas_cost_native,
            "max_gas_cost_usd": self.max_gas_cost_usd,
            "native_token_price_usd": self.native_token_price_usd,
            "max_slippage_bps": self.max_slippage_bps,
            "max_daily_volume_eth": str(self.max_daily_volume_eth),
        }


# =============================================================================
# Event Callback Type
# =============================================================================


EventCallback = Callable[[ExecutionEventType, dict[str, Any]], None]


# =============================================================================
# Intent Description Generator
# =============================================================================


def _get_token_symbol(token_data: Any) -> str:
    """Extract token symbol from various formats."""
    if isinstance(token_data, dict):
        symbol = token_data.get("symbol") or token_data.get("name") or ""
        return str(symbol)
    elif isinstance(token_data, str):
        return token_data
    return ""


def _get_token_decimals(token_data: Any) -> int:
    """Extract token decimals from token data dict."""
    if isinstance(token_data, dict):
        return token_data.get("decimals", 18)
    return 18  # Default to 18 decimals (ETH standard)


def _format_amount(amount: Any, token_data: Any = None) -> str:
    """Format amount for display, converting from wei if needed.

    Args:
        amount: Raw amount (possibly in wei)
        token_data: Token dict containing decimals info
    """
    if not amount:
        return ""
    try:
        if isinstance(amount, str):
            amount = Decimal(amount)
        if isinstance(amount, int | float | Decimal):
            amount = Decimal(str(amount))

            # If amount is very large (likely wei), convert using decimals
            if amount > 1_000_000_000 and token_data:
                decimals = _get_token_decimals(token_data)
                amount = amount / Decimal(10**decimals)

            # Format based on magnitude
            if amount >= 1000:
                return f"{amount:,.0f}"
            elif amount >= 1:
                return f"{amount:.4f}".rstrip("0").rstrip(".")
            elif amount >= 0.0001:
                return f"{amount:.6f}".rstrip("0").rstrip(".")
            else:
                return f"{amount:.8f}".rstrip("0").rstrip(".")
    except Exception:
        pass
    return str(amount)


def _generate_intent_description(action_bundle: "ActionBundle") -> str:
    """Generate a human-readable description of an intent.

    Args:
        action_bundle: The action bundle containing intent details

    Returns:
        Human-readable description like "Supply 0.002 WETH as collateral on Aave V3"
    """
    intent_type = action_bundle.intent_type.upper()
    metadata = action_bundle.metadata or {}

    # Extract common fields - handle both dict and string formats
    protocol = metadata.get("protocol", "")
    chain = metadata.get("chain", "")

    # Generate description based on intent type
    if intent_type == "SWAP":
        from_token_data = metadata.get("from_token", {})
        to_token_data = metadata.get("to_token", {})
        from_token = _get_token_symbol(from_token_data)
        to_token = _get_token_symbol(to_token_data)
        amount = _format_amount(metadata.get("from_amount", metadata.get("amount", "")), from_token_data)

        if amount and from_token and to_token:
            desc = f"Swap {amount} {from_token} → {to_token}"
        elif from_token and to_token:
            desc = f"Swap {from_token} → {to_token}"
        else:
            desc = "Swap tokens"
        if protocol:
            desc += f" via {protocol}"
        return desc

    elif intent_type == "SUPPLY":
        supply_token_data = metadata.get("supply_token", {})
        supply_token = _get_token_symbol(supply_token_data)
        amount = _format_amount(metadata.get("supply_amount", ""), supply_token_data)

        if amount and supply_token:
            desc = f"Supply {amount} {supply_token} as collateral"
        elif supply_token:
            desc = f"Supply {supply_token} as collateral"
        else:
            desc = "Supply collateral"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "BORROW":
        borrow_token_data = metadata.get("borrow_token", {})
        collateral_token_data = metadata.get("collateral_token", {})
        borrow_token = _get_token_symbol(borrow_token_data)
        collateral_token = _get_token_symbol(collateral_token_data)
        borrow_amount = _format_amount(metadata.get("borrow_amount", ""), borrow_token_data)

        if borrow_amount and borrow_token:
            desc = f"Borrow {borrow_amount} {borrow_token}"
        elif borrow_token:
            desc = f"Borrow {borrow_token}"
        else:
            desc = "Borrow tokens"
        if collateral_token:
            desc += f" against {collateral_token} collateral"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "REPAY":
        repay_token_data = metadata.get("repay_token", {})
        repay_token = _get_token_symbol(repay_token_data)
        amount = _format_amount(metadata.get("repay_amount", ""), repay_token_data)

        if amount and repay_token:
            desc = f"Repay {amount} {repay_token}"
        elif repay_token:
            desc = f"Repay {repay_token} debt"
        else:
            desc = "Repay debt"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "WITHDRAW":
        withdraw_token_data = metadata.get("withdraw_token", {})
        withdraw_token = _get_token_symbol(withdraw_token_data)
        amount = _format_amount(metadata.get("withdraw_amount", ""), withdraw_token_data)

        if amount and withdraw_token:
            desc = f"Withdraw {amount} {withdraw_token}"
        elif withdraw_token:
            desc = f"Withdraw {withdraw_token}"
        else:
            desc = "Withdraw from protocol"
        if protocol:
            desc += f" from {protocol}"
        return desc

    elif intent_type == "LP_OPEN":
        token0 = _get_token_symbol(metadata.get("token0", ""))
        token1 = _get_token_symbol(metadata.get("token1", ""))
        pool = metadata.get("pool", "")

        if token0 and token1:
            desc = f"Open LP: {token0}/{token1}"
        elif pool:
            desc = f"Open LP: {pool}"
        else:
            desc = "Open LP position"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "LP_CLOSE":
        token0 = _get_token_symbol(metadata.get("token0", ""))
        token1 = _get_token_symbol(metadata.get("token1", ""))

        if token0 and token1:
            desc = f"Close LP: {token0}/{token1}"
        else:
            desc = "Close LP position"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "PERP_OPEN":
        direction = metadata.get("direction", "long")
        if isinstance(direction, str):
            direction = direction.lower()
        market = metadata.get("market", "")
        leverage = metadata.get("leverage", "")
        collateral_token_data = metadata.get("collateral_token", {})
        collateral_token = _get_token_symbol(collateral_token_data)
        collateral_amount = _format_amount(metadata.get("collateral_amount", ""), collateral_token_data)

        if collateral_amount and collateral_token:
            desc = f"Open {direction}: {collateral_amount} {collateral_token}"
        elif market:
            desc = f"Open {direction}: {market}"
        else:
            desc = f"Open {direction} position"
        if leverage:
            desc += f" ({leverage}x)"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "PERP_CLOSE":
        market = metadata.get("market", "")
        if market:
            desc = f"Close position: {market}"
        else:
            desc = "Close perpetual position"
        if protocol:
            desc += f" on {protocol}"
        return desc

    elif intent_type == "BRIDGE":
        token_data = metadata.get("token", {})
        token = _get_token_symbol(token_data)
        amount = _format_amount(metadata.get("amount", ""), token_data)
        from_chain = metadata.get("from_chain", "")
        to_chain = metadata.get("to_chain", chain)

        if amount and token:
            desc = f"Bridge {amount} {token}"
        elif token:
            desc = f"Bridge {token}"
        else:
            desc = "Bridge tokens"
        if from_chain and to_chain:
            desc += f": {from_chain} → {to_chain}"
        elif to_chain:
            desc += f" to {to_chain}"
        if protocol:
            desc += f" via {protocol}"
        return desc

    elif intent_type == "HOLD":
        reason = metadata.get("reason", "")
        if reason:
            return f"Hold: {reason}"
        return "Hold position"

    # Default: use intent type
    return f"{intent_type.replace('_', ' ').title()}"


# =============================================================================
# Execution Orchestrator
# =============================================================================


class ExecutionOrchestrator:
    """Orchestrates the full transaction execution flow.

    The ExecutionOrchestrator coordinates:
    - RiskGuard validation
    - Transaction simulation (optional)
    - Nonce assignment
    - Transaction signing
    - Transaction submission
    - Receipt polling and parsing

    Events are emitted at each step for observability.

    Example:
        orchestrator = ExecutionOrchestrator(
            signer=signer,
            submitter=submitter,
            simulator=simulator,
            chain="arbitrum",
        )

        result = await orchestrator.execute(action_bundle)
        if result.success:
            print(f"All transactions confirmed: {result.transaction_results}")
        else:
            print(f"Execution failed at {result.error_phase}: {result.error}")
    """

    def __init__(
        self,
        signer: Signer,
        submitter: Submitter,
        simulator: Simulator,
        chain: str = "arbitrum",
        rpc_url: str | None = None,
        risk_guard: RiskGuard | None = None,
        event_callback: EventCallback | None = None,
        gas_buffer_multiplier: float | None = None,
        tx_timeout_seconds: float | None = None,
        session_store: ExecutionSessionStore | None = None,
        tx_risk_config: TransactionRiskConfig | None = None,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            signer: Signer implementation for transaction signing
            submitter: Submitter implementation for transaction submission
            simulator: Simulator implementation for pre-execution simulation
            chain: Target blockchain network
            rpc_url: RPC URL for nonce queries (optional if submitter provides)
            risk_guard: RiskGuard for validation (uses default if not provided)
            event_callback: Optional callback for execution events
            gas_buffer_multiplier: Gas buffer multiplier (uses chain default if not provided)
            tx_timeout_seconds: Timeout for transaction confirmation. If None, uses
                chain-specific default (300s for Ethereum L1, 120s for L2s).
            session_store: Optional ExecutionSessionStore for crash recovery checkpoints
            tx_risk_config: Transaction risk configuration (uses default if not provided)
        """
        self.signer = signer
        self.submitter = submitter
        self.simulator = simulator
        self.chain = chain
        self.rpc_url = rpc_url
        self.risk_guard = risk_guard or RiskGuard()
        self._event_callback = event_callback

        # Use chain-specific timeout if not explicitly provided
        from almanak.framework.execution.gas.constants import CHAIN_TX_TIMEOUTS, DEFAULT_TX_TIMEOUT_SECONDS

        self.tx_timeout_seconds = (
            tx_timeout_seconds
            if tx_timeout_seconds is not None
            else float(CHAIN_TX_TIMEOUTS.get(chain.lower(), DEFAULT_TX_TIMEOUT_SECONDS))
        )
        self._session_store = session_store
        self.tx_risk_config = tx_risk_config or TransactionRiskConfig.default()

        # Set gas buffer multiplier based on chain
        if gas_buffer_multiplier is not None:
            self.gas_buffer_multiplier = gas_buffer_multiplier
        else:
            self.gas_buffer_multiplier = GAS_BUFFER_MULTIPLIERS.get(chain.lower(), DEFAULT_GAS_BUFFER)

        # Web3 instance for nonce queries (lazy initialized)
        self._web3: AsyncWeb3 | None = None

        # Local nonce tracker: maps address -> next expected nonce.
        # Prevents nonce collisions when multiple tool calls execute rapidly.
        self._local_nonce: dict[str, int] = {}

        logger.info(
            f"ExecutionOrchestrator initialized: chain={chain}, "
            f"wallet={signer.address[:10]}..., "
            f"gas_buffer={self.gas_buffer_multiplier}x, "
            f"session_store={'enabled' if session_store else 'disabled'}"
        )

    def reset_nonce_cache(self, wallet_address: str | None = None) -> None:
        """Reset the local nonce cache, forcing fresh on-chain query on next execution.

        Call this after a nonce-related error to recover from nonce drift.

        Args:
            wallet_address: Specific address to reset. If None, clears all cached nonces.
        """
        if wallet_address:
            key = wallet_address.lower()
            if key in self._local_nonce:
                logger.info(f"Reset nonce cache for {key[:10]}... (was {self._local_nonce[key]})")
                del self._local_nonce[key]
        else:
            if self._local_nonce:
                logger.info(f"Cleared nonce cache for {len(self._local_nonce)} address(es)")
            self._local_nonce.clear()

    async def _get_web3(self) -> AsyncWeb3:
        """Get or create Web3 instance."""
        if self._web3 is None:
            if self.rpc_url:
                self._web3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
                # Inject POA middleware for chains like Polygon, Avalanche, BSC
                from almanak.gateway.utils.rpc_provider import is_poa_chain

                if is_poa_chain(self.chain):
                    try:
                        from web3.middleware import ExtraDataToPOAMiddleware

                        poa_mw = ExtraDataToPOAMiddleware
                    except ImportError:
                        from web3.middleware import geth_poa_middleware  # type: ignore[attr-defined]

                        poa_mw = geth_poa_middleware
                    self._web3.middleware_onion.inject(poa_mw, layer=0)
                    logger.debug(f"Injected POA middleware for chain={self.chain}")
            else:
                raise ExecutionError("RPC URL required for nonce queries")
        return self._web3

    def _emit_event(
        self,
        event_type: ExecutionEventType,
        context: ExecutionContext,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Emit an execution event.

        Args:
            event_type: Type of event
            context: Execution context
            details: Additional event details
        """
        event_details = details or {}
        event_details["correlation_id"] = context.correlation_id

        # Map execution event type to timeline event type
        timeline_event_type_map: dict[ExecutionEventType, TimelineEventType] = {
            ExecutionEventType.VALIDATING: TimelineEventType.CUSTOM,
            ExecutionEventType.RISK_BLOCKED: TimelineEventType.RISK_GUARD_TRIGGERED,
            ExecutionEventType.SIMULATING: TimelineEventType.CUSTOM,
            ExecutionEventType.SIMULATION_FAILED: TimelineEventType.CUSTOM,
            ExecutionEventType.SIGNING: TimelineEventType.CUSTOM,
            ExecutionEventType.SUBMITTING: TimelineEventType.CUSTOM,
            ExecutionEventType.TX_SENT: TimelineEventType.TRANSACTION_SUBMITTED,
            ExecutionEventType.WAITING: TimelineEventType.CUSTOM,
            ExecutionEventType.TX_CONFIRMED: TimelineEventType.TRANSACTION_CONFIRMED,
            ExecutionEventType.TX_REVERTED: TimelineEventType.TRANSACTION_REVERTED,
            ExecutionEventType.EXECUTION_SUCCESS: TimelineEventType.CUSTOM,
            ExecutionEventType.EXECUTION_FAILED: TimelineEventType.TRANSACTION_FAILED,
        }

        timeline_event_type = timeline_event_type_map.get(event_type, TimelineEventType.CUSTOM)

        # Generate human-readable description based on event type
        intent_desc = context.intent_description or "Unknown action"
        if event_type == ExecutionEventType.EXECUTION_SUCCESS:
            description = f"✓ {intent_desc}"
        elif event_type == ExecutionEventType.EXECUTION_FAILED:
            error_msg = event_details.get("error", "Unknown error")
            description = f"✗ {intent_desc} failed: {error_msg[:50]}"
        elif event_type == ExecutionEventType.TX_CONFIRMED:
            tx_hash = event_details.get("tx_hash", "")[:10]
            description = f"✓ Transaction confirmed ({tx_hash}...)"
        elif event_type == ExecutionEventType.TX_REVERTED:
            reason = (event_details.get("revert_reason") or "Unknown reason")[:40]
            description = f"✗ Transaction reverted: {reason}"
        elif event_type == ExecutionEventType.TX_SENT:
            tx_hash = event_details.get("tx_hash", "")[:10]
            description = f"→ Transaction sent ({tx_hash}...)"
        elif event_type == ExecutionEventType.VALIDATING:
            description = f"Validating: {intent_desc}"
        elif event_type == ExecutionEventType.SIMULATING:
            description = f"Simulating: {intent_desc}"
        elif event_type == ExecutionEventType.SIGNING:
            tx_count = event_details.get("tx_count", 0)
            description = f"Signing {tx_count} transaction(s)"
        elif event_type == ExecutionEventType.SUBMITTING:
            tx_count = event_details.get("tx_count", 0)
            description = f"Submitting {tx_count} transaction(s)"
        elif event_type == ExecutionEventType.WAITING:
            description = "Waiting for confirmation..."
        elif event_type == ExecutionEventType.RISK_BLOCKED:
            violations = event_details.get("violations", [])
            description = f"⚠ Risk blocked: {'; '.join(violations[:2])}"
        elif event_type == ExecutionEventType.SIMULATION_FAILED:
            reason = event_details.get("revert_reason", "Unknown")[:40]
            description = f"Simulation failed: {reason}"
        else:
            description = f"{intent_desc}"

        # Create timeline event
        timeline_event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=timeline_event_type,
            description=description,
            strategy_id=context.strategy_id,
            chain=context.chain,
            details={
                "execution_event": event_type.value,
                "intent_description": intent_desc,
                **event_details,
            },
        )

        # Add to timeline
        add_event(timeline_event)

        # Call custom callback if provided
        if self._event_callback:
            try:
                self._event_callback(event_type, event_details)
            except Exception as e:
                logger.warning(f"Event callback failed: {e}")

        logger.debug(f"Event emitted: {event_type.value} for {context.strategy_id}")

    def _create_session(
        self,
        context: ExecutionContext,
        action_bundle: ActionBundle,
    ) -> ExecutionSession | None:
        """Create an execution session for crash recovery tracking.

        Args:
            context: Execution context with strategy_id and intent_id
            action_bundle: ActionBundle to snapshot for replay

        Returns:
            ExecutionSession if session_store is configured, None otherwise
        """
        if self._session_store is None:
            return None

        # Create session with PREPARING phase
        session = create_session(
            strategy_id=context.strategy_id,
            intent_id=context.intent_id or context.correlation_id,
            session_id=context.session_id if context.session_id else None,
        )

        # Store action bundle snapshot for potential replay
        session.set_action_bundle(action_bundle.to_dict())

        # Persist the session
        self._session_store.save(session)

        # Update context with session_id
        context.session_id = session.session_id

        logger.debug(f"Created execution session {session.session_id} for strategy {context.strategy_id}")

        return session

    def _checkpoint_session(
        self,
        session: ExecutionSession | None,
        phase: SessionPhase,
        transactions: list[TransactionState] | None = None,
        error: str | None = None,
    ) -> None:
        """Checkpoint session state at a phase transition.

        Args:
            session: ExecutionSession to checkpoint (may be None if store not configured)
            phase: New execution phase
            transactions: Updated transaction states
            error: Error message if any
        """
        if session is None or self._session_store is None:
            return

        session.set_phase(phase)

        if transactions is not None:
            session.transactions = transactions

        if error is not None:
            session.set_error(error)

        self._session_store.save(session)

        logger.debug(f"Checkpointed session {session.session_id} at phase {phase.value}")

    def _complete_session(
        self,
        session: ExecutionSession | None,
        success: bool,
        error: str | None = None,
    ) -> None:
        """Mark a session as complete.

        Args:
            session: ExecutionSession to complete (may be None if store not configured)
            success: Whether execution succeeded
            error: Error message if failed
        """
        if session is None or self._session_store is None:
            return

        if error is not None:
            session.set_error(error)

        session.mark_complete(success)
        self._session_store.save(session)

        logger.info(f"Completed session {session.session_id} (success={success}, phase={session.phase.value})")

    async def execute(
        self,
        action_bundle: ActionBundle,
        context: ExecutionContext | None = None,
    ) -> ExecutionResult:
        """Execute an ActionBundle through the full execution pipeline.

        This method:
        1. Validates transactions via RiskGuard
        2. Simulates transactions (if enabled)
        3. Assigns sequential nonces
        4. Signs all transactions
        5. Submits transactions
        6. Polls for and parses receipts

        Args:
            action_bundle: The ActionBundle to execute
            context: Optional execution context

        Returns:
            ExecutionResult with complete execution details
        """
        # Initialize context
        if context is None:
            context = ExecutionContext(
                wallet_address=self.signer.address,
                chain=self.chain,
            )
        else:
            if not context.wallet_address:
                context.wallet_address = self.signer.address
            if not context.chain:
                context.chain = self.chain

        # Generate human-readable intent description
        if not context.intent_description:
            context.intent_description = _generate_intent_description(action_bundle)

        result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.VALIDATION,
            correlation_id=context.correlation_id,
        )

        # Create execution session for crash recovery (PREPARING phase)
        session = self._create_session(context, action_bundle)

        try:
            # Step 0: Refresh deferred transactions (LiFi, Enso) with fresh route data
            from .deferred_refresh import refresh_deferred_bundle

            action_bundle = refresh_deferred_bundle(action_bundle, context.wallet_address)

            # Step 1: Build unsigned transactions from ActionBundle
            # Gas prices are set as placeholders; _update_gas_prices() sets final values.
            unsigned_txs = await self._build_unsigned_transactions(action_bundle, context)

            if not unsigned_txs:
                intent_type = (action_bundle.intent_type or "").upper()
                # HOLD intents legitimately produce 0 transactions
                if intent_type == "HOLD":
                    result.success = True
                    result.phase = ExecutionPhase.COMPLETE
                    result.completed_at = datetime.now(UTC)
                    self._complete_session(session, success=True)
                    self._emit_event(
                        ExecutionEventType.EXECUTION_SUCCESS,
                        context,
                        {"message": "No transactions to execute (HOLD intent)", "intent_type": intent_type},
                    )
                    return result

                # For all other intent types (LP_CLOSE, SWAP, etc.), 0 transactions
                # means nothing happened -- this is a false positive if reported as
                # SUCCESS. Mark as failed so the strategy knows the action didn't execute.
                error_msg = (
                    f"Empty ActionBundle: {intent_type} compiled to 0 transactions. "
                    f"Nothing was executed. This usually means no position was found to close "
                    f"or the compiler could not build the required transactions."
                )
                logger.warning(error_msg)
                result.success = False
                result.error = error_msg
                result.error_phase = ExecutionPhase.COMPLETE
                result.phase = ExecutionPhase.COMPLETE
                result.completed_at = datetime.now(UTC)
                self._complete_session(session, success=False, error=error_msg)
                self._emit_event(
                    ExecutionEventType.EXECUTION_FAILED,
                    context,
                    {"error": error_msg, "message": error_msg, "intent_type": intent_type},
                )
                return result

            # Step 1.5: Pre-flight token balance check
            # Prevents sending doomed transactions that can hang Anvil forks
            # via expensive upstream RPC calls during gas estimation / simulation.
            await self._check_token_balance_before_submit(action_bundle, context)

            # Step 1.6: On-chain gas estimation fallback (when simulation is disabled)
            if not context.simulation_enabled:
                unsigned_txs, gas_warnings = await self._maybe_estimate_gas_limits(unsigned_txs, context)
                if gas_warnings:
                    result.gas_warnings = gas_warnings

            # Step 2: Validate via RiskGuard
            self._emit_event(
                ExecutionEventType.VALIDATING,
                context,
                {"tx_count": len(unsigned_txs)},
            )

            validation_result = await self._validate_transactions(unsigned_txs, context)
            if not validation_result.passed:
                result.error = f"RiskGuard blocked: {'; '.join(validation_result.violations)}"
                result.error_phase = ExecutionPhase.VALIDATION
                self._complete_session(session, success=False, error=result.error)
                self._emit_event(
                    ExecutionEventType.RISK_BLOCKED,
                    context,
                    {"violations": validation_result.violations},
                )
                return result

            # Step 2.5: Pre-flight balance check (VIB-521)
            # Prevents wasting gas on approvals when the wallet can't cover the
            # final action (e.g., LP mint). Extracts required token amounts from
            # the ActionBundle metadata and checks on-chain balances.
            preflight_error = await self._preflight_balance_check(action_bundle, context)
            if preflight_error:
                result.error = preflight_error
                result.error_phase = ExecutionPhase.VALIDATION
                self._complete_session(session, success=False, error=result.error)
                self._emit_event(
                    ExecutionEventType.RISK_BLOCKED,
                    context,
                    {"violations": [preflight_error]},
                )
                return result

            result.phase = ExecutionPhase.SIMULATION

            # Step 3: Simulate (if enabled)
            if context.simulation_enabled:
                self._emit_event(
                    ExecutionEventType.SIMULATING,
                    context,
                    {"tx_count": len(unsigned_txs)},
                )

                # Build state overrides for Safe wallet simulation
                # Safe wallets hold the tokens, but EOA signs - simulator needs to
                # see the Safe's balance, not the EOA's
                state_overrides = None
                if isinstance(self.signer, SafeSigner):
                    safe_address = self.signer.address
                    # Override Safe wallet ETH balance for simulation
                    # This ensures simulation sees the Safe can pay for gas
                    state_overrides = {
                        safe_address: {
                            "balance": hex(10 * 10**18)  # 10 ETH for simulation
                        }
                    }
                    logger.info(
                        "Using state overrides for Safe wallet simulation",
                        extra={
                            "safe_address": safe_address,
                            "eoa_address": self.signer.eoa_address,
                        },
                    )

                simulation_result = await self.simulator.simulate(
                    unsigned_txs, context.chain, state_overrides=state_overrides
                )
                result.simulation_result = simulation_result

                if not simulation_result.success:
                    result.error = f"Simulation failed: {simulation_result.revert_reason or 'Unknown reason'}"
                    result.error_phase = ExecutionPhase.SIMULATION
                    self._complete_session(session, success=False, error=result.error)
                    self._emit_event(
                        ExecutionEventType.SIMULATION_FAILED,
                        context,
                        {"revert_reason": simulation_result.revert_reason},
                    )
                    return result

                # Update gas estimates from simulation if available
                if simulation_result.gas_estimates:
                    source = simulation_result.simulator_name or "unknown"
                    for i, gas_estimate in enumerate(simulation_result.gas_estimates):
                        if i < len(unsigned_txs):
                            buffered = int(gas_estimate * self.gas_buffer_multiplier)
                            logger.info(
                                f"Gas estimate tx[{i}]: raw={gas_estimate:,} "
                                f"buffered={buffered:,} (x{self.gas_buffer_multiplier}) "
                                f"source={source}"
                            )
                            unsigned_txs[i] = self._update_gas_estimate(unsigned_txs[i], gas_estimate)

            # Step 3.5: Update gas prices from network
            unsigned_txs = await self._update_gas_prices(unsigned_txs)

            # Step 3.6: Validate gas prices against configured cap
            # This must run AFTER _update_gas_prices() sets real values.
            gas_price_result = self._validate_gas_prices(unsigned_txs)
            if not gas_price_result.passed:
                result.error = f"Gas price cap exceeded: {'; '.join(gas_price_result.violations)}"
                result.error_phase = ExecutionPhase.VALIDATION
                self._complete_session(session, success=False, error=result.error)
                self._emit_event(
                    ExecutionEventType.RISK_BLOCKED,
                    context,
                    {"violations": gas_price_result.violations},
                )
                return result

            result.phase = ExecutionPhase.NONCE_ASSIGNMENT

            # Step 4: Assign nonces
            unsigned_txs = await self._assign_nonces(unsigned_txs, context)

            result.phase = ExecutionPhase.SIGNING

            # Step 5: Sign transactions
            self._emit_event(
                ExecutionEventType.SIGNING,
                context,
                {"tx_count": len(unsigned_txs)},
            )

            if isinstance(self.signer, SafeSigner):
                signed_txs = await self._sign_safe_batch(unsigned_txs, context)
            else:
                signed_txs = await self.signer.sign_batch(unsigned_txs, context.chain)

            # Checkpoint session after signing (SIGNING phase with nonces)
            tx_states = [
                TransactionState(
                    nonce=tx.nonce if tx.nonce is not None else 0,
                    status=TransactionStatus.PENDING,
                )
                for tx in unsigned_txs
            ]
            self._checkpoint_session(session, SessionPhase.SIGNING, transactions=tx_states)

            result.phase = ExecutionPhase.SUBMISSION

            # Step 6: Dry run check
            if context.dry_run:
                result.success = True
                result.phase = ExecutionPhase.COMPLETE
                result.completed_at = datetime.now(UTC)
                self._complete_session(session, success=True)
                self._emit_event(
                    ExecutionEventType.EXECUTION_SUCCESS,
                    context,
                    {"message": "Dry run completed", "tx_count": len(signed_txs)},
                )
                return result

            # Step 7+8: Submit transactions and collect receipts.
            #
            # Use sequential submit-and-confirm for multi-TX bundles with EOA
            # signers to avoid hitting RPC in-flight TX limits (e.g. Alchemy's
            # 2-TX limit for delegated accounts on Base).  Safe signers bundle
            # atomically via MultiSend into a single on-chain TX, so they are
            # exempt and use the faster parallel path.
            self._emit_event(
                ExecutionEventType.SUBMITTING,
                context,
                {"tx_count": len(signed_txs)},
            )

            use_sequential = len(signed_txs) >= 2 and not isinstance(self.signer, SafeSigner)

            if use_sequential:
                # --- Sequential path: submit TX -> confirm -> submit next ---
                from .submitter.public import PublicMempoolSubmitter

                if isinstance(self.submitter, PublicMempoolSubmitter):
                    logger.info(f"Using sequential submit for {len(signed_txs)} TXs (avoids RPC in-flight limits)")
                    try:
                        submission_results, receipts = await self.submitter.submit_sequential(
                            signed_txs, receipt_timeout=self.tx_timeout_seconds
                        )
                    except Exception as exc:
                        # Record partial tx_hashes from already-confirmed TXs
                        # so retry logic can detect them and avoid duplicate swaps.
                        partial = getattr(exc, "partial_results", [])
                        for i, sub in enumerate(partial):
                            if sub.submitted and session and i < len(session.transactions):
                                session.transactions[i].tx_hash = sub.tx_hash
                                session.transactions[i].status = TransactionStatus.SUBMITTED
                                session.transactions[i].submitted_at = datetime.now(UTC)
                        if partial:
                            self._checkpoint_session(
                                session,
                                SessionPhase.SUBMITTED,
                                transactions=session.transactions if session else None,
                            )
                        raise
                else:
                    # Non-public submitter fallback: submit all, then confirm
                    submission_results = await self.submitter.submit(signed_txs)
                    use_sequential = False  # fall through to parallel receipt path
            else:
                submission_results = await self.submitter.submit(signed_txs)

            if not use_sequential:
                # --- Parallel path (single TX, Safe signer, or fallback) ---
                # Check for submission failures
                failed_submissions = [r for r in submission_results if not r.submitted]
                if failed_submissions:
                    first_error = failed_submissions[0].error or "Unknown submission error"
                    result.error = f"Submission failed: {first_error}"
                    result.error_phase = ExecutionPhase.SUBMISSION
                    self._complete_session(session, success=False, error=result.error)
                    self._emit_event(
                        ExecutionEventType.EXECUTION_FAILED,
                        context,
                        {"error": first_error, "failed_count": len(failed_submissions)},
                    )
                    return result

            # Emit TX_SENT events and update transaction states
            for i, submission in enumerate(submission_results):
                self._emit_event(
                    ExecutionEventType.TX_SENT,
                    context,
                    {"tx_hash": submission.tx_hash},
                )
                # Update transaction state with tx_hash
                if session and i < len(session.transactions):
                    session.transactions[i].tx_hash = submission.tx_hash
                    session.transactions[i].status = TransactionStatus.SUBMITTED
                    session.transactions[i].submitted_at = datetime.now(UTC)

            # Checkpoint SUBMITTED phase with tx_hash and nonce
            self._checkpoint_session(
                session,
                SessionPhase.SUBMITTED,
                transactions=session.transactions if session else None,
            )

            result.phase = ExecutionPhase.CONFIRMATION

            # Checkpoint CONFIRMING phase
            self._checkpoint_session(session, SessionPhase.CONFIRMING)

            if not use_sequential:
                # Parallel receipt polling (single TX or Safe bundles)
                self._emit_event(
                    ExecutionEventType.WAITING,
                    context,
                    {"tx_count": len(submission_results)},
                )
                tx_hashes = [r.tx_hash for r in submission_results]
                receipts = await self.submitter.get_receipts(tx_hashes, timeout=self.tx_timeout_seconds)

            # Step 9: Process receipts
            for _i, receipt in enumerate(receipts):
                tx_result = TransactionResult(
                    tx_hash=receipt.tx_hash,
                    success=receipt.success,
                    receipt=receipt,
                    gas_used=receipt.gas_used,
                    gas_cost_wei=receipt.gas_cost_wei,
                    logs=receipt.logs,
                )

                result.transaction_results.append(tx_result)
                result.total_gas_used += receipt.gas_used
                result.total_gas_cost_wei += receipt.gas_cost_wei

                # Update session transaction state
                if session:
                    session.update_transaction(
                        tx_hash=receipt.tx_hash,
                        status=(TransactionStatus.CONFIRMED if receipt.success else TransactionStatus.FAILED),
                        gas_used=receipt.gas_used,
                        block_number=receipt.block_number,
                    )

                if receipt.success:
                    self._emit_event(
                        ExecutionEventType.TX_CONFIRMED,
                        context,
                        {
                            "tx_hash": receipt.tx_hash,
                            "block_number": receipt.block_number,
                            "gas_used": receipt.gas_used,
                        },
                    )
                else:
                    self._emit_event(
                        ExecutionEventType.TX_REVERTED,
                        context,
                        {
                            "tx_hash": receipt.tx_hash,
                            "block_number": receipt.block_number,
                            "gas_used": receipt.gas_used,
                        },
                    )

            # Check for any reverted transactions
            reverted = [tr for tr in result.transaction_results if not tr.success]
            if reverted:
                first_reverted = reverted[0]
                result.error_phase = ExecutionPhase.CONFIRMATION

                # Build and log verbose revert report for debugging
                verbose_report = build_verbose_revert_report(
                    context=context,
                    action_bundle=action_bundle,
                    transaction_results=result.transaction_results,
                    intent=getattr(action_bundle, "intent", None),
                    raw_error=first_reverted.error,
                    started_at=result.started_at,
                )

                result.error = f"{verbose_report.format()}"
                self._complete_session(session, success=False, error=result.error)
                self._emit_event(
                    ExecutionEventType.EXECUTION_FAILED,
                    context,
                    {
                        "error": result.error,
                        "reverted_count": len(reverted),
                        "verbose_report": verbose_report.to_dict(),
                    },
                )

                # Log user-friendly failure summary
                intent_type = action_bundle.intent_type if action_bundle else "UNKNOWN"
                tx_hash_fmt = format_tx_hash(first_reverted.tx_hash)
                logger.error(f"FAILED: {intent_type} - Transaction reverted at {tx_hash_fmt}")
                logger.error(f"   Reverted: {len(reverted)}/{len(result.transaction_results)} transactions")

                return result

            # Success!
            result.success = True
            result.phase = ExecutionPhase.COMPLETE
            result.completed_at = datetime.now(UTC)

            # Update local nonce cache ONLY after confirmed on-chain success.
            # This prevents nonce drift when transactions fail. (VIB-1449)
            confirmed_count = len([tr for tr in result.transaction_results if tr.success])
            if confirmed_count > 0:
                wallet_key = context.wallet_address.lower()
                # Set nonce to chain_nonce + confirmed_count (or use the
                # highest confirmed nonce + 1 from the transaction results).
                web3 = await self._get_web3()
                fresh_nonce = await web3.eth.get_transaction_count(
                    web3.to_checksum_address(context.wallet_address), "pending"
                )
                self._local_nonce[wallet_key] = fresh_nonce
                logger.debug(f"Updated local nonce cache to {fresh_nonce} after {confirmed_count} confirmed TXs")

            # Mark session complete on success
            self._complete_session(session, success=True)

            self._emit_event(
                ExecutionEventType.EXECUTION_SUCCESS,
                context,
                {
                    "tx_count": len(result.transaction_results),
                    "total_gas_used": result.total_gas_used,
                    "total_gas_cost_wei": str(result.total_gas_cost_wei),
                },
            )

            # Log user-friendly execution summary
            intent_type = action_bundle.intent_type if action_bundle else "UNKNOWN"
            tx_hashes = [format_tx_hash(tr.tx_hash) for tr in result.transaction_results]
            gas_fmt = format_gas_cost(result.total_gas_used)

            ok_prefix = "✅" if _emojis_enabled() else "[OK]"
            logger.info(f"{ok_prefix} EXECUTED: {intent_type} completed successfully")
            logger.info(f"   Txs: {len(result.transaction_results)} ({', '.join(tx_hashes)}) | {gas_fmt}")

            return result

        except NonceError as e:
            result.error = str(e)
            result.error_phase = ExecutionPhase.NONCE_ASSIGNMENT
            self._complete_session(session, success=False, error=str(e))
            self._emit_event(
                ExecutionEventType.EXECUTION_FAILED,
                context,
                {"error": str(e), "error_type": "NonceError"},
            )
            return result

        except InsufficientFundsError as e:
            result.error = str(e)
            result.error_phase = result.phase
            self._complete_session(session, success=False, error=str(e))
            self._emit_event(
                ExecutionEventType.EXECUTION_FAILED,
                context,
                {"error": str(e), "error_type": "InsufficientFundsError"},
            )
            return result

        except GasEstimationError as e:
            result.error = str(e)
            result.error_phase = result.phase
            self._complete_session(session, success=False, error=str(e))
            self._emit_event(
                ExecutionEventType.EXECUTION_FAILED,
                context,
                {"error": str(e), "error_type": "GasEstimationError"},
            )
            return result

        except TransactionRevertedError as e:
            result.error_phase = ExecutionPhase.CONFIRMATION

            # Build and log verbose revert report for debugging
            verbose_report = build_verbose_revert_report(
                context=context,
                action_bundle=action_bundle,
                transaction_results=result.transaction_results,
                intent=getattr(action_bundle, "intent", None),
                raw_error=str(e),
                started_at=result.started_at,
            )

            result.error = f"{verbose_report.format()}"
            self._complete_session(session, success=False, error=result.error)

            self._emit_event(
                ExecutionEventType.TX_REVERTED,
                context,
                {
                    "tx_hash": e.tx_hash,
                    "revert_reason": e.revert_reason,
                    "verbose_report": verbose_report.to_dict(),
                },
            )

            # Log user-friendly failure summary
            intent_type = action_bundle.intent_type if action_bundle else "UNKNOWN"
            tx_hash_fmt = format_tx_hash(e.tx_hash)
            logger.error(f"FAILED: {intent_type} - Transaction reverted at {tx_hash_fmt}")
            logger.error(f"   Reason: {e.revert_reason or 'Unknown'}")

            return result

        except SubmissionError as e:
            result.error = str(e)
            result.error_phase = ExecutionPhase.SUBMISSION

            # Preserve submitted tx_hashes on timeout so the runner can check
            # if transactions confirmed before blindly retrying (which could
            # cause duplicate swaps).
            if not result.transaction_results and session and session.transactions:
                for tx_state in session.transactions:
                    if tx_state.tx_hash:
                        result.transaction_results.append(
                            TransactionResult(
                                tx_hash=tx_state.tx_hash,
                                success=False,
                                error="timeout_waiting_for_receipt",
                            )
                        )

            self._complete_session(session, success=False, error=str(e))
            self._emit_event(
                ExecutionEventType.EXECUTION_FAILED,
                context,
                {"error": str(e), "error_type": "SubmissionError"},
            )
            return result

        except ExecutionError as e:
            result.error = str(e)
            result.error_phase = result.phase
            self._complete_session(session, success=False, error=str(e))
            self._emit_event(
                ExecutionEventType.EXECUTION_FAILED,
                context,
                {"error": str(e), "error_type": "ExecutionError"},
            )
            return result

        except Exception as e:
            logger.exception(f"Unexpected execution error: {e}")
            result.error = f"Unexpected error: {e}"
            result.error_phase = result.phase
            self._complete_session(session, success=False, error=str(e))
            self._emit_event(
                ExecutionEventType.EXECUTION_FAILED,
                context,
                {"error": str(e), "error_type": type(e).__name__},
            )
            return result

    async def _build_unsigned_transactions(
        self,
        action_bundle: ActionBundle,
        context: ExecutionContext,
    ) -> list[UnsignedTransaction]:
        """Build unsigned transactions from an ActionBundle.

        Builds transactions with placeholder gas prices (0). The actual network
        gas prices are set later by _update_gas_prices() using the 2x base fee
        EIP-1559 formula, before nonce assignment and signing.

        Args:
            action_bundle: ActionBundle containing transaction data
            context: Execution context

        Returns:
            List of UnsignedTransaction objects
        """
        unsigned_txs: list[UnsignedTransaction] = []

        # Get chain ID for the target chain (use canonical CHAIN_IDS constant)
        chain_id = CHAIN_IDS.get(context.chain.lower(), 42161)

        # Gas prices are set as placeholders here; _update_gas_prices() overwrites
        # them with accurate network values (2x base fee EIP-1559 formula) later
        # in the execute() flow, before nonce assignment and signing.

        logger.debug(f"Building {len(action_bundle.transactions)} unsigned transactions for chain {context.chain}")

        for tx_data in action_bundle.transactions:
            # Parse transaction data from ActionBundle format
            to_address = tx_data.get("to", "")
            value = int(tx_data.get("value", "0"))
            data = tx_data.get("data", "0x")
            gas_estimate = tx_data.get("gas_estimate", 100000)

            # Apply gas buffer
            buffered_gas = int(gas_estimate * self.gas_buffer_multiplier)

            # Build unsigned transaction (EIP-1559 by default)
            unsigned_tx = UnsignedTransaction(
                to=to_address,
                value=value,
                data=data,
                chain_id=chain_id,
                gas_limit=buffered_gas,
                tx_type=TransactionType.EIP_1559,
                from_address=context.wallet_address,
                max_fee_per_gas=0,
                max_priority_fee_per_gas=0,
                metadata={
                    "tx_type": tx_data.get("tx_type", "unknown"),
                    "description": tx_data.get("description", ""),
                    "intent_type": action_bundle.intent_type,
                },
            )

            unsigned_txs.append(unsigned_tx)

        return unsigned_txs

    async def _validate_transactions(
        self,
        unsigned_txs: list[UnsignedTransaction],
        context: ExecutionContext,
    ) -> RiskGuardResult:
        """Validate transactions via RiskGuard (pre-gas-price checks).

        Validates all transactions against configured risk limits:
        - Maximum transaction value (per-tx ETH limit)
        - Allowed contract addresses (whitelist if configured)
        - Contract deployment blocking
        - Daily volume limits

        Note: Gas price cap validation is done separately in _validate_gas_prices()
        which runs AFTER _update_gas_prices() sets real network values.

        Args:
            unsigned_txs: Transactions to validate
            context: Execution context

        Returns:
            RiskGuardResult indicating validation status
        """
        violations: list[str] = []
        config = self.tx_risk_config

        # Get today's date for daily volume tracking
        from datetime import date

        today = date.today().isoformat()
        if config._daily_volume_date != today:
            # Reset daily volume on new day
            config._daily_volume_wei = 0
            config._daily_volume_date = today

        total_value_wei = 0

        for i, tx in enumerate(unsigned_txs):
            # 1. Check maximum value per transaction
            max_value_wei = int(config.max_value_eth * 10**18)
            if tx.value > max_value_wei:
                value_eth = Decimal(tx.value) / Decimal(10**18)
                violations.append(
                    f"Transaction {i}: value {value_eth:.4f} ETH exceeds limit {config.max_value_eth} ETH"
                )

            total_value_wei += tx.value

            # 2. Check contract deployment blocking
            if tx.to is None and config.block_contract_deployment:
                violations.append(f"Transaction {i}: Contract deployment blocked by risk guard")

            # 3. Check allowed contracts whitelist
            if config.allowed_contracts is not None and tx.to is not None:
                # Normalize to lowercase for comparison
                normalized_to = tx.to.lower()
                allowed_normalized = {addr.lower() for addr in config.allowed_contracts}
                if normalized_to not in allowed_normalized:
                    violations.append(f"Transaction {i}: Contract {tx.to[:10]}... not in allowed whitelist")

            # 4. Gas price limits are checked separately in _validate_gas_prices()
            # after _update_gas_prices() sets real values from the network.

        # 5. Check daily volume limits
        if config.max_daily_volume_eth > 0:
            max_daily_wei = int(config.max_daily_volume_eth * 10**18)
            projected_daily = config._daily_volume_wei + total_value_wei
            if projected_daily > max_daily_wei:
                current_eth = Decimal(config._daily_volume_wei) / Decimal(10**18)
                total_eth = Decimal(total_value_wei) / Decimal(10**18)
                violations.append(
                    f"Daily volume limit exceeded: current {current_eth:.4f} ETH + "
                    f"this batch {total_eth:.4f} ETH would exceed limit "
                    f"{config.max_daily_volume_eth} ETH"
                )

        # Update daily volume tracking if validation passes
        if not violations:
            config._daily_volume_wei += total_value_wei
            logger.debug(
                f"Risk guard passed: {len(unsigned_txs)} txs, "
                f"value={Decimal(total_value_wei) / Decimal(10**18):.4f} ETH, "
                f"daily_total={Decimal(config._daily_volume_wei) / Decimal(10**18):.4f} ETH"
            )
        else:
            logger.warning(f"Risk guard BLOCKED: {len(violations)} violations - {violations}")

        return RiskGuardResult(
            passed=len(violations) == 0,
            violations=violations,
        )

    def _validate_gas_prices(
        self,
        unsigned_txs: list[UnsignedTransaction],
    ) -> RiskGuardResult:
        """Validate gas prices and total gas cost against configured caps.

        This runs AFTER _update_gas_prices() sets real network gas prices,
        ensuring caps are checked against actual values rather than placeholders.

        Checks:
            1. Per-tx gas price cap (max_gas_price_gwei)
            2. Per-tx total gas cost cap in native token (max_gas_cost_native)
            3. Per-tx total gas cost cap in USD (max_gas_cost_usd)

        Args:
            unsigned_txs: Transactions with final gas prices set

        Returns:
            RiskGuardResult indicating validation status
        """
        config = self.tx_risk_config
        check_gas_price = config.max_gas_price_gwei > 0
        check_gas_cost = config.max_gas_cost_native > 0
        check_gas_cost_usd = config.max_gas_cost_usd > 0 and config.native_token_price_usd > 0

        if config.max_gas_cost_usd > 0 and config.native_token_price_usd <= 0:
            logger.warning(
                "max_gas_cost_usd is set but native_token_price_usd is not available; "
                "USD gas guard is disabled for this execution"
            )

        if not check_gas_price and not check_gas_cost and not check_gas_cost_usd:
            return RiskGuardResult(passed=True, violations=[])

        violations: list[str] = []
        max_gas_wei = config.max_gas_price_gwei * 10**9 if check_gas_price else 0
        max_cost_wei = int(config.max_gas_cost_native * 10**18) if check_gas_cost else 0

        for i, tx in enumerate(unsigned_txs):
            tx_gas_price = tx.max_fee_per_gas or tx.gas_price or 0

            if check_gas_price and tx_gas_price > max_gas_wei:
                tx_gas_gwei = tx_gas_price / 10**9
                violations.append(
                    f"Transaction {i}: gas price {tx_gas_gwei:.1f} gwei exceeds limit {config.max_gas_price_gwei} gwei"
                )

            if tx_gas_price > 0:
                gas_limit = tx.gas_limit or 0
                estimated_cost_wei = gas_limit * tx_gas_price
                cost_native = estimated_cost_wei / 10**18

                if check_gas_cost and estimated_cost_wei > max_cost_wei:
                    violations.append(
                        f"Transaction {i}: estimated gas cost {cost_native:.6f} native "
                        f"exceeds limit {config.max_gas_cost_native} native "
                        f"(gas_limit={gas_limit:,} * gas_price={tx_gas_price / 10**9:.2f} gwei)"
                    )

                if check_gas_cost_usd:
                    cost_usd = cost_native * config.native_token_price_usd
                    if cost_usd > config.max_gas_cost_usd:
                        violations.append(
                            f"Transaction {i}: estimated gas cost ${cost_usd:.2f} USD "
                            f"exceeds limit ${config.max_gas_cost_usd:.2f} USD "
                            f"(native_cost={cost_native:.6f} * price=${config.native_token_price_usd:.2f})"
                        )

        if violations:
            logger.warning(f"Gas guard BLOCKED: {violations}")

        return RiskGuardResult(
            passed=len(violations) == 0,
            violations=violations,
        )

    async def _preflight_balance_check(
        self,
        action_bundle: ActionBundle,
        context: ExecutionContext,
    ) -> str | None:
        """Check wallet has sufficient token balances before submitting any transactions.

        Prevents wasting gas on approval txs when the final action (mint, swap)
        will revert due to insufficient balance. Extracts required amounts from
        the ActionBundle metadata and checks on-chain balances.

        This is a best-effort optimisation -- balances can change between this check
        and actual submission (MEV, concurrent strategies, etc.). It is NOT a
        substitute for on-chain revert protection.

        Args:
            action_bundle: The bundle with metadata containing token amounts.
            context: Execution context with wallet address.

        Returns:
            Error message string if balance is insufficient, None if OK.
        """
        if not self.rpc_url:
            return None  # Can't check without RPC

        metadata = action_bundle.metadata or {}
        intent_type = (action_bundle.intent_type or "").upper()
        protocol = (metadata.get("protocol") or "").lower()
        wallet = context.wallet_address or self.signer.address

        # Protocols that store lending amounts as wei in metadata.
        # All others (morpho, morpho_blue, compound_v3) use human-readable.
        _WEI_LENDING_PROTOCOLS = {"aave_v3", "spark"}

        # Extract required tokens and amounts from metadata based on intent type
        # Each requirement: (symbol, address, amount_wei, decimals or None, is_native)
        requirements: list[tuple[str, str, int, int | None, bool]] = []

        def _parse_amount_wei(raw_amount: str, token_info: dict, is_wei: bool = True) -> int | None:
            """Parse a metadata amount string to wei. Returns None if unparseable.

            Args:
                raw_amount: The raw amount string from metadata.
                token_info: Token dict with optional 'decimals' key.
                is_wei: If True, the value is already in wei (SWAP/LP and Aave/Spark lending).
                    If False, the value is human-readable and needs conversion (Morpho/Compound).
            """
            try:
                val = Decimal(raw_amount)
                if val <= 0:
                    return None
                token_decimals = token_info.get("decimals")
                if is_wei:
                    if val != int(val):
                        # Fractional values are invalid on wei-coded paths.
                        # Return None to fail open rather than guess the format.
                        return None
                    return int(val)
                else:
                    # Human-readable -> convert to wei
                    if token_decimals is not None and token_decimals >= 0:
                        return int(val * Decimal(10**token_decimals))
                    return None  # Can't convert without decimals
            except Exception:
                return None

        def _add_requirement(token_info: dict, raw_amount: str | None, is_wei: bool = True) -> None:
            if not token_info or not raw_amount:
                return
            amount_wei = _parse_amount_wei(raw_amount, token_info, is_wei=is_wei)
            if amount_wei is not None and amount_wei > 0:
                requirements.append(
                    (
                        token_info.get("symbol", "?"),
                        token_info.get("address", ""),
                        amount_wei,
                        token_info.get("decimals"),
                        bool(token_info.get("is_native")),
                    )
                )

        try:
            if intent_type == "SWAP":
                _add_requirement(metadata.get("from_token", {}), metadata.get("amount_in"))

            elif intent_type == "LP_OPEN":
                token0 = metadata.get("token0") or metadata.get("token_x") or {}
                token1 = metadata.get("token1") or metadata.get("token_y") or {}
                amount0 = metadata.get("amount0_desired") or metadata.get("amount_x")
                amount1 = metadata.get("amount1_desired") or metadata.get("amount_y")
                _add_requirement(token0, amount0)
                _add_requirement(token1, amount1)

            elif intent_type == "SUPPLY":
                is_wei = protocol in _WEI_LENDING_PROTOCOLS
                _add_requirement(metadata.get("supply_token", {}), metadata.get("supply_amount"), is_wei=is_wei)

            elif intent_type == "REPAY":
                # Skip full-repay (MAX_UINT256 sentinel) -- wallet can't hold 2^256 tokens
                if not metadata.get("repay_full"):
                    is_wei = protocol in _WEI_LENDING_PROTOCOLS
                    _add_requirement(metadata.get("repay_token", {}), metadata.get("repay_amount"), is_wei=is_wei)

            elif intent_type == "BORROW":
                # Check collateral token balance for borrow intents
                is_wei = protocol in _WEI_LENDING_PROTOCOLS
                _add_requirement(metadata.get("collateral_token", {}), metadata.get("collateral_amount"), is_wei=is_wei)

        except (ValueError, TypeError) as e:
            logger.debug(f"Pre-flight balance check: could not parse metadata: {e}")
            return None  # Don't block on parse errors

        if not requirements:
            return None  # Nothing to check (HOLD, LP_CLOSE, etc.)

        # Check on-chain balances
        # balanceOf(address) selector = 0x70a08231
        erc20_balance_of = bytes.fromhex("70a08231")

        try:
            web3 = await self._get_web3()

            shortfalls: list[str] = []
            check_failures = 0
            for symbol, address, required_wei, decimals, is_native in requirements:
                if not address and not is_native:
                    continue
                try:
                    if is_native:
                        balance_wei = int(
                            await asyncio.wait_for(
                                web3.eth.get_balance(web3.to_checksum_address(wallet)),
                                timeout=5.0,
                            )
                        )
                    else:
                        data = erc20_balance_of + bytes.fromhex(wallet[2:].lower().zfill(64))
                        raw = await asyncio.wait_for(
                            web3.eth.call({"to": web3.to_checksum_address(address), "data": data}),
                            timeout=5.0,
                        )
                        balance_wei = int(raw.hex(), 16)

                    if balance_wei < required_wei:
                        if decimals is not None:
                            required_human = Decimal(required_wei) / Decimal(10**decimals)
                            actual_human = Decimal(balance_wei) / Decimal(10**decimals)
                            shortfalls.append(
                                f"Insufficient {symbol}: have {actual_human:.6f}, need {required_human:.6f}"
                            )
                        else:
                            shortfalls.append(f"Insufficient {symbol}: have {balance_wei} wei, need {required_wei} wei")
                except Exception as e:
                    check_failures += 1
                    logger.debug(f"Pre-flight: could not check {symbol} balance: {e}")
                    # Don't block on individual check failures

            if check_failures > 0:
                logger.warning(
                    f"Pre-flight balance check: could not verify {check_failures} of "
                    f"{len(requirements)} token balance(s)"
                )

            if shortfalls:
                msg = (
                    f"Pre-flight balance check failed: {'; '.join(shortfalls)}. "
                    f"No transactions submitted (saved gas on approvals)."
                )
                logger.warning(msg)
                return msg

        except Exception as e:
            logger.debug(f"Pre-flight balance check skipped: {e}")

        return None

    async def _sign_safe_batch(
        self,
        unsigned_txs: list[UnsignedTransaction],
        context: ExecutionContext,
    ) -> list[SignedTransaction]:
        """Sign transactions via Safe wallet (requires Web3 for on-chain calls).

        For a single transaction, uses sign_with_web3(). For multiple transactions,
        bundles them into an atomic MultiSend via sign_bundle_with_web3().

        Args:
            unsigned_txs: Transactions to sign through the Safe
            context: Execution context

        Returns:
            List of signed transactions (single element for MultiSend bundles)
        """
        assert isinstance(self.signer, SafeSigner)

        web3 = await self._get_web3()
        eoa_address = web3.to_checksum_address(self.signer.eoa_address)
        chain_nonce = await web3.eth.get_transaction_count(eoa_address, "pending")
        local_nonce = self._local_nonce.get(eoa_address.lower(), 0)
        eoa_nonce = max(chain_nonce, local_nonce)

        if len(unsigned_txs) == 1:
            signed = await self.signer.sign_with_web3(unsigned_txs[0], web3, eoa_nonce)
        else:
            # Atomic MultiSend bundle -- all txs succeed or fail together
            signed = await self.signer.sign_bundle_with_web3(unsigned_txs, web3, eoa_nonce, context.chain)

        # NOTE: Do NOT update _local_nonce here. Updated only after
        # confirmed on-chain success in execute(). (VIB-1449)
        return [signed]

    async def _assign_nonces(
        self,
        unsigned_txs: list[UnsignedTransaction],
        context: ExecutionContext,
    ) -> list[UnsignedTransaction]:
        """Assign sequential nonces to transactions.

        Args:
            unsigned_txs: Transactions to assign nonces
            context: Execution context

        Returns:
            Transactions with nonces assigned
        """
        if not unsigned_txs:
            return unsigned_txs

        # Get current nonce -- use max of chain nonce and local tracker
        # to avoid "nonce too low" when rapid tool calls overlap.
        web3 = await self._get_web3()
        wallet_addr = web3.to_checksum_address(context.wallet_address)
        chain_nonce = await web3.eth.get_transaction_count(wallet_addr, "pending")
        local_nonce = self._local_nonce.get(wallet_addr.lower(), 0)
        current_nonce = max(chain_nonce, local_nonce)

        # Assign sequential nonces
        result_txs: list[UnsignedTransaction] = []
        for i, tx in enumerate(unsigned_txs):
            # Create new transaction with nonce assigned
            tx_with_nonce = UnsignedTransaction(
                to=tx.to,
                value=tx.value,
                data=tx.data,
                chain_id=tx.chain_id,
                gas_limit=tx.gas_limit,
                nonce=current_nonce + i,
                tx_type=tx.tx_type,
                from_address=tx.from_address,
                max_fee_per_gas=tx.max_fee_per_gas,
                max_priority_fee_per_gas=tx.max_priority_fee_per_gas,
                gas_price=tx.gas_price,
                metadata=tx.metadata,
            )
            result_txs.append(tx_with_nonce)

        # NOTE: Do NOT update _local_nonce here. The nonce cache is only
        # updated after confirmed on-chain success (in execute()). Optimistic
        # pre-update caused nonce drift when transactions failed — the cache
        # kept the inflated value while on-chain nonce stayed unchanged,
        # leading to "nonce too high" on subsequent calls. (VIB-1449)
        logger.debug(
            f"Assigned nonces {current_nonce} to {current_nonce + len(unsigned_txs) - 1} (chain={chain_nonce}, local={local_nonce})"
        )

        return result_txs

    def _update_gas_estimate(
        self,
        tx: UnsignedTransaction,
        gas_estimate: int,
    ) -> UnsignedTransaction:
        """Update a transaction's gas estimate from simulation or eth_estimateGas.

        This is the SINGLE point where gas buffer is applied. Simulators
        (Tenderly, Alchemy) and eth_estimateGas return raw gas_used values;
        the buffer multiplier (e.g. 1.5x for Arbitrum) is applied here only.

        Args:
            tx: Transaction to update
            gas_estimate: Raw gas estimate (unbuffered) from simulation

        Returns:
            Transaction with buffered gas limit
        """
        # Guard against zero/negative gas estimates (RPC error, rate limiting, timeout).
        # Fall back to the compiler-provided gas limit instead of crashing.
        if gas_estimate <= 0:
            compiler_gas = tx.gas_limit or 0
            if compiler_gas > 0:
                logger.warning(f"Gas estimation returned {gas_estimate}, using compiler estimate: {compiler_gas:,}")
                return tx
            logger.warning(
                f"Gas estimation returned {gas_estimate} and no compiler estimate available, using default 300,000"
            )
            gas_estimate = 300_000

        # Single point of gas buffer application -- simulators return raw gas_used
        buffered_gas = int(gas_estimate * self.gas_buffer_multiplier)

        return UnsignedTransaction(
            to=tx.to,
            value=tx.value,
            data=tx.data,
            chain_id=tx.chain_id,
            gas_limit=buffered_gas,
            nonce=tx.nonce,
            tx_type=tx.tx_type,
            from_address=tx.from_address,
            max_fee_per_gas=tx.max_fee_per_gas,
            max_priority_fee_per_gas=tx.max_priority_fee_per_gas,
            gas_price=tx.gas_price,
            metadata=tx.metadata,
        )

    async def _maybe_estimate_gas_limits(
        self,
        unsigned_txs: list[UnsignedTransaction],
        context: ExecutionContext,
    ) -> tuple[list[UnsignedTransaction], list[str]]:
        """Estimate gas limits via RPC when simulation is disabled.

        Uses eth_estimateGas for best-effort estimation. If estimation fails,
        falls back to the existing compiler-provided gas limit.

        Returns:
            Tuple of (updated transactions, gas estimation warnings).
            Warnings capture estimation failures (e.g., STF reverts) for surfacing
            in revert diagnostics.
        """
        gas_warnings: list[str] = []

        if not self.rpc_url:
            return unsigned_txs, gas_warnings

        # Safe transactions are executed via Safe modules; skip direct estimation.
        if isinstance(self.signer, SafeSigner):
            return unsigned_txs, gas_warnings

        try:
            web3 = await self._get_web3()
        except ExecutionError:
            return unsigned_txs, gas_warnings

        is_multi_tx_bundle = len(unsigned_txs) > 1

        updated: list[UnsignedTransaction] = []
        for idx, tx in enumerate(unsigned_txs):
            if not tx.to:
                updated.append(tx)
                continue

            # For multi-TX bundles, only estimate gas for the first transaction.
            # Subsequent TXs depend on state changes from prior TXs (e.g., approve must
            # execute before addLiquidity), so eth_estimateGas against the current
            # chain state will always revert. The compiler-provided gas limit is the
            # correct estimate for dependent transactions (approve ~65k, action uses
            # connector defaults). Attempting estimation wastes an RPC round-trip.
            if is_multi_tx_bundle and idx > 0:
                updated.append(tx)
                continue

            try:
                tx_params: dict[str, Any] = {
                    "from": web3.to_checksum_address(context.wallet_address),
                    "to": web3.to_checksum_address(tx.to),
                    "value": int(tx.value),
                    "data": tx.data,
                }

                if tx.max_fee_per_gas is not None:
                    tx_params["maxFeePerGas"] = int(tx.max_fee_per_gas)
                if tx.max_priority_fee_per_gas is not None:
                    tx_params["maxPriorityFeePerGas"] = int(tx.max_priority_fee_per_gas)
                if tx.tx_type == TransactionType.EIP_1559:
                    tx_params["type"] = 2

                gas_estimate = int(
                    await asyncio.wait_for(
                        web3.eth.estimate_gas(cast(TxParams, tx_params)),
                        timeout=15.0,
                    )
                )
                buffered = int(gas_estimate * self.gas_buffer_multiplier)

                # Never go below compiler-provided gas limit. On-chain estimation
                # for multi-step bundles (e.g. LP close: decrease→collect→burn) can
                # underestimate later txs because it estimates against pre-execution
                # state. For example, collect estimated before decrease sees 0 tokens
                # owed (cheap), but actual collect after decrease transfers tokens
                # (expensive). The compiler default accounts for worst-case.
                compiler_limit = tx.gas_limit or 0
                if buffered < compiler_limit:
                    logger.warning(
                        f"Gas estimate tx[{idx}]: raw={gas_estimate:,} "
                        f"buffered={buffered:,} (x{self.gas_buffer_multiplier}) "
                        f"< compiler={compiler_limit:,}, using compiler limit"
                    )
                    updated.append(tx)
                else:
                    logger.info(
                        f"Gas estimate tx[{idx}]: raw={gas_estimate:,} "
                        f"buffered={buffered:,} (x{self.gas_buffer_multiplier}) "
                        f"source=eth_estimateGas"
                    )
                    updated.append(self._update_gas_estimate(tx, gas_estimate))
            except Exception as e:
                error_str = str(e)
                # Gas estimation for non-first TXs in multi-step bundles (approve+supply,
                # decrease+collect+burn, etc.) will often fail because they depend on state
                # changes from prior TXs that haven't been executed yet. This is expected
                # and the compiler-provided gas limit is always used as fallback.
                is_multi_tx_dependent = idx > 0
                is_known_pattern = any(
                    code in error_str for code in ("STF", "allowance", "TRANSFER_FROM_FAILED", "ds-math-sub")
                )
                is_expected = is_multi_tx_dependent or is_known_pattern
                warning_msg = f"tx {idx + 1}/{len(unsigned_txs)}: {e}"
                gas_warnings.append(warning_msg)
                if is_expected:
                    logger.debug(
                        f"Gas estimation reverted for {warning_msg} "
                        "(expected for multi-step bundles). Using compiler-provided gas limit."
                    )
                else:
                    logger.debug(f"Gas estimation failed for {warning_msg}. Using compiler-provided gas limit.")
                updated.append(tx)

        return updated, gas_warnings

    async def get_current_nonce(self, address: str | None = None) -> int:
        """Get the current nonce for an address.

        Args:
            address: Address to query (defaults to signer address)

        Returns:
            Current nonce for the address
        """
        addr = address or self.signer.address
        web3 = await self._get_web3()
        checksum_addr = web3.to_checksum_address(addr)
        return await web3.eth.get_transaction_count(checksum_addr, "pending")

    async def get_gas_price(self) -> dict[str, int]:
        """Get current gas prices from the network.

        Returns:
            Dict with max_fee_per_gas and max_priority_fee_per_gas
        """
        web3 = await self._get_web3()

        # Get latest base fee
        latest_block = await web3.eth.get_block("latest")
        base_fee = latest_block.get("baseFeePerGas", 0)

        # Get max priority fee suggestion
        try:
            max_priority_fee = int(await web3.eth.max_priority_fee)
        except Exception:
            max_priority_fee = 1_000_000_000  # 1 gwei fallback

        # Get base_fee as int
        base_fee_int = int(base_fee) if base_fee else 0

        # EIP-1559 recommendation: 2x base fee ensures inclusion even if base fee
        # doubles in the next block (each block can increase base fee by up to 12.5%,
        # so 2x covers ~6 consecutive full blocks of increases). Priority fee is added
        # on top as the miner tip.
        max_fee = base_fee_int * 2 + max_priority_fee

        return {
            "max_fee_per_gas": max_fee,
            "max_priority_fee_per_gas": max_priority_fee,
            "base_fee_per_gas": base_fee_int,
        }

    async def _update_gas_prices(
        self,
        unsigned_txs: list[UnsignedTransaction],
    ) -> list[UnsignedTransaction]:
        """Update transactions with current network gas prices.

        Fetches current gas prices from the network and updates all
        transactions with accurate values instead of placeholders.

        Args:
            unsigned_txs: Transactions with placeholder gas prices

        Returns:
            Transactions updated with network gas prices
        """
        # Fetch current gas prices
        gas_prices = await self.get_gas_price()
        max_fee = gas_prices["max_fee_per_gas"]
        max_priority_fee = gas_prices["max_priority_fee_per_gas"]

        logger.debug(
            f"Updating gas prices: max_fee={max_fee / 10**9:.4f} gwei, priority_fee={max_priority_fee / 10**9:.4f} gwei"
        )

        # Update each transaction
        result_txs = []
        for tx in unsigned_txs:
            updated_tx = UnsignedTransaction(
                to=tx.to,
                value=tx.value,
                data=tx.data,
                chain_id=tx.chain_id,
                gas_limit=tx.gas_limit,
                nonce=tx.nonce,
                tx_type=tx.tx_type,
                from_address=tx.from_address,
                max_fee_per_gas=max_fee,
                max_priority_fee_per_gas=max_priority_fee,
                gas_price=tx.gas_price,
                metadata=tx.metadata,
            )
            result_txs.append(updated_tx)

        return result_txs

    async def _check_token_balance_before_submit(
        self,
        action_bundle: ActionBundle,
        context: ExecutionContext,
    ) -> None:
        """Check ERC20 token balance before submitting swap transactions.

        For SWAP intents, verifies the wallet has sufficient input tokens.
        Raises InsufficientFundsError if balance is too low.
        """
        if (action_bundle.intent_type or "").upper() != "SWAP":
            return

        metadata = action_bundle.metadata or {}
        from_token = metadata.get("from_token", {})
        amount_in_str = metadata.get("amount_in")

        if not from_token or not amount_in_str:
            return  # Can't check without metadata

        token_address = from_token.get("address")
        is_native = from_token.get("is_native", False)

        if not token_address or is_native:
            return  # Skip for native tokens

        try:
            amount_in = int(amount_in_str)
        except (ValueError, TypeError):
            return  # Can't parse amount

        if not self.rpc_url:
            return  # No RPC URL configured

        try:
            web3 = await self._get_web3()
            balance = await asyncio.wait_for(
                web3.eth.call(
                    {
                        "to": web3.to_checksum_address(token_address),
                        "data": HexStr("0x70a08231" + context.wallet_address[2:].lower().zfill(64)),
                    }
                ),
                timeout=10.0,
            )
            balance_int = int.from_bytes(balance, "big")
        except Exception as e:  # noqa: BLE001
            # Balance check is best-effort - don't block execution on RPC errors
            logger.debug(f"Pre-submission balance check failed: {e}")
            return

        if balance_int < amount_in:
            token_symbol = from_token.get("symbol", "ERC20")
            raise InsufficientFundsError(
                required=amount_in,
                available=balance_int,
                token=token_symbol,
            )

    def set_event_callback(self, callback: EventCallback | None) -> None:
        """Set the event callback.

        Args:
            callback: Callback function for execution events
        """
        self._event_callback = callback


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "ExecutionOrchestrator",
    "ExecutionResult",
    "ExecutionContext",
    "ExecutionPhase",
    "ExecutionEventType",
    "TransactionResult",
    "EventCallback",
    "GAS_BUFFER_MULTIPLIERS",
]
