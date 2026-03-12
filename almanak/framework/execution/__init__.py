"""Execution Layer for Transaction Signing, Simulation, and Submission.

This module provides the infrastructure for executing blockchain transactions,
supporting multiple signing backends (local EOA, cloud KMS) and submission
methods (public mempool, Flashbots, direct RPC).

Key Components:
    - GatewayExecutionOrchestrator: Gateway-backed orchestrator for strategy containers
    - ExecutionOrchestrator: Direct orchestrator (gateway-internal use only)
    - Signer: Abstract base class for transaction signing
    - Submitter: Abstract base class for transaction submission
    - Simulator: Abstract base class for transaction simulation

Architecture:
    Strategy containers use GatewayExecutionOrchestrator which routes execution
    requests to the gateway sidecar via gRPC. The gateway handles actual signing,
    simulation, and submission using the direct components below.

    Gateway-internal components (not for strategy container use):
    1. Signer implementations:
       - LocalKeySigner: Signs with local private key
       - SafeSigner: Signs via Safe wallet

    2. Submitter implementations:
       - PublicMempoolSubmitter: Standard eth_sendRawTransaction

    3. Simulator implementations:
       - DirectSimulator: Pass-through for trusted environments
       - TenderlySimulator: Full simulation via Tenderly API
       - AlchemySimulator: Simulation via Alchemy RPC API

Example (Strategy Container):
    from almanak.framework.execution import GatewayExecutionOrchestrator
    from almanak.framework.gateway_client import GatewayClient

    with GatewayClient() as client:
        orchestrator = GatewayExecutionOrchestrator(
            client=client,
            chain="arbitrum",
            wallet_address="0x...",
        )
        result = await orchestrator.execute(action_bundle)
"""

# Chain-family execution strategy (EVM / Solana abstraction)
# Chain Executor (multi-chain support)
from almanak.framework.execution.chain_executor import (
    ChainExecutor,
    ChainExecutorConfig,
    TransactionExecutionResult,
)
from almanak.framework.execution.chain_strategy import ChainExecutionStrategy

# CLOB Handler for off-chain order execution (Polymarket)
from almanak.framework.execution.clob_handler import (
    ClobActionHandler,
    ClobExecutionResult,
    ClobFill,
    ClobOrderState,
    ClobOrderStatus,
)

# Configuration
from almanak.framework.execution.config import (
    CHAIN_IDS,
    SUPPORTED_PROTOCOLS,
    ConfigurationError,
    DataFreshnessPolicy,
    LocalRuntimeConfig,
    MissingEnvironmentVariableError,
    MultiChainRuntimeConfig,
)

# Events and payloads
from almanak.framework.execution.events import (
    ERROR_RECOVERY_MAP,
    EXECUTION_TO_TIMELINE_MAP,
    ExecutionEvent,
    ExecutionEventType,
    ExecutionFailedPayload,
    SwapResultPayload,
    TransactionConfirmedPayload,
    TransactionSentPayload,
    get_recovery_info,
)
from almanak.framework.execution.evm_strategy import EvmExecutionStrategy

# Extracted Data Models (for Result Enrichment)
from almanak.framework.execution.extracted_data import (
    BorrowData,
    LPCloseData,
    LPOpenData,
    PerpData,
    StakeData,
    SupplyData,
    SwapAmounts,
)

# Gateway-backed Orchestrator (for strategy containers)
from almanak.framework.execution.gateway_orchestrator import (
    GatewayExecutionOrchestrator,
    GatewayExecutionResult,
)

# Handler Registry for protocol-agnostic execution routing
from almanak.framework.execution.handler_registry import (
    ExecutionHandler,
    ExecutionHandlerRegistry,
)
from almanak.framework.execution.interfaces import (
    # Enums
    Chain,
    # Exceptions
    ExecutionError,
    GasEstimationError,
    InsufficientFundsError,
    NonceError,
    SignedTransaction,
    # Abstract base classes
    Signer,
    SigningError,
    SimulationError,
    SimulationResult,
    Simulator,
    SubmissionError,
    SubmissionResult,
    Submitter,
    TransactionReceipt,
    TransactionRevertedError,
    TransactionType,
    # Data classes
    UnsignedTransaction,
)

# Multi-Chain Orchestrator
from almanak.framework.execution.multichain import (
    ExecutionStatus,
    IntentExecutionResult,
    MultiChainExecutionError,
    MultiChainExecutionResult,
    MultiChainOrchestrator,
)

# Orchestrator
from almanak.framework.execution.orchestrator import (
    GAS_BUFFER_MULTIPLIERS,
    EventCallback,
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.execution.outcome import ExecutionOutcome

# Plan structures for cross-chain execution
from almanak.framework.execution.plan import (
    PlanBundle,
    PlanStep,
    RemediationAction,
    StepArtifacts,
    StepStatus,
)

# Plan executor for deterministic execution
from almanak.framework.execution.plan_executor import (
    DEFAULT_STALE_QUOTE_THRESHOLD_SECONDS,
    ExecutionPath,
    PlanExecutionResult,
    PlanExecutionStatus,
    PlanExecutor,
    PlanExecutorConfig,
    PlanReconciliation,
    QuoteRefreshInfo,
    QuoteRefreshResult,
    ReconciliationStatus,
    RehydrationResult,
    RehydrationStatus,
    RemediationHandlingResult,
    StepExecutionResult,
    StepReconciliation,
    StepRehydrationResult,
)

# Receipt Parser Registry
from almanak.framework.execution.receipt_registry import (
    ParserNotFoundError,
    ReceiptParser,
    ReceiptParserError,
    ReceiptParserRegistry,
    extract_position_id,
    get_parser,
    is_parser_available,
    list_parsers,
    register_parser,
)

# Remediation state machine and operator cards
from almanak.framework.execution.remediation import (
    OperatorCard,
    OperatorCardPriority,
    OperatorCardStatus,
    RemediationResult,
    RemediationState,
    RemediationStateMachine,
    RemediationStateRecord,
    RemediationTrigger,
)

# Result Enricher (automatic receipt parsing)
from almanak.framework.execution.result_enricher import (
    ResultEnricher,
    enrich_result,
    get_enricher,
)

# Revert Diagnostics (verbose error reporting)
from almanak.framework.execution.revert_diagnostics import (
    ActionDetails,
    IntentDetails,
    RevertDiagnostic,
    TransactionDetails,
    VerboseRevertReport,
    build_verbose_revert_report,
    decode_calldata_selector,
)

# Cross-chain risk guard
from almanak.framework.execution.risk_guards import (
    BridgeHistoryEntry,
    ChainBalance,
    CrossChainRiskConfig,
    CrossChainRiskGuard,
    CrossChainRiskResult,
    InFlightTransfer,
    RiskContext,
    RiskViolation,
)

# Signer implementations
from almanak.framework.execution.signer import LocalKeySigner

# Simulator implementations
from almanak.framework.execution.simulator import (
    AlchemySimulator,
    DirectSimulator,
    FallbackSimulator,
    SimulationConfig,
    TenderlySimulator,
    create_simulator,
    is_local_rpc,
)
from almanak.framework.execution.solana import (
    AccountMeta,
    SignedSolanaTransaction,
    SolanaExecutionPlanner,
    SolanaInstruction,
    SolanaTransaction,
    SolanaTransactionReceipt,
)

# Submitter implementations
from almanak.framework.execution.submitter import PublicMempoolSubmitter

__all__ = [
    # Enums
    "Chain",
    "TransactionType",
    # Data classes
    "UnsignedTransaction",
    "SignedTransaction",
    "SimulationResult",
    "SubmissionResult",
    "TransactionReceipt",
    # Abstract base classes
    "Signer",
    "Submitter",
    "Simulator",
    # Signer implementations
    "LocalKeySigner",
    # Submitter implementations
    "PublicMempoolSubmitter",
    # Simulator implementations
    "DirectSimulator",
    "TenderlySimulator",
    "AlchemySimulator",
    "FallbackSimulator",
    "SimulationConfig",
    "create_simulator",
    "is_local_rpc",
    # Orchestrator
    "ExecutionOrchestrator",
    "ExecutionResult",
    "ExecutionContext",
    "ExecutionPhase",
    "TransactionResult",
    "EventCallback",
    "GAS_BUFFER_MULTIPLIERS",
    # Gateway-backed Orchestrator (for strategy containers)
    "GatewayExecutionOrchestrator",
    "GatewayExecutionResult",
    # Chain Executor (multi-chain support)
    "ChainExecutor",
    "ChainExecutorConfig",
    "TransactionExecutionResult",
    # Multi-Chain Orchestrator
    "MultiChainOrchestrator",
    "MultiChainExecutionResult",
    "IntentExecutionResult",
    "ExecutionStatus",
    "MultiChainExecutionError",
    # Events and payloads
    "ExecutionEventType",
    "TransactionSentPayload",
    "TransactionConfirmedPayload",
    "SwapResultPayload",
    "ExecutionFailedPayload",
    "ExecutionEvent",
    "EXECUTION_TO_TIMELINE_MAP",
    "ERROR_RECOVERY_MAP",
    "get_recovery_info",
    # Configuration
    "LocalRuntimeConfig",
    "MultiChainRuntimeConfig",
    "ConfigurationError",
    "MissingEnvironmentVariableError",
    "DataFreshnessPolicy",
    "CHAIN_IDS",
    "SUPPORTED_PROTOCOLS",
    # Exceptions
    "ExecutionError",
    "SigningError",
    "SimulationError",
    "SubmissionError",
    "TransactionRevertedError",
    "InsufficientFundsError",
    "NonceError",
    "GasEstimationError",
    # Plan structures
    "StepStatus",
    "RemediationAction",
    "StepArtifacts",
    "PlanStep",
    "PlanBundle",
    # Plan executor
    "PlanExecutor",
    "PlanExecutorConfig",
    "ReconciliationStatus",
    "QuoteRefreshResult",
    "QuoteRefreshInfo",
    "StepReconciliation",
    "PlanReconciliation",
    "RehydrationStatus",
    "RehydrationResult",
    "StepRehydrationResult",
    "PlanExecutionStatus",
    "PlanExecutionResult",
    "StepExecutionResult",
    "RemediationHandlingResult",
    "ExecutionPath",
    "DEFAULT_STALE_QUOTE_THRESHOLD_SECONDS",
    # Cross-chain risk guard
    "CrossChainRiskGuard",
    "CrossChainRiskConfig",
    "CrossChainRiskResult",
    "RiskViolation",
    "RiskContext",
    "ChainBalance",
    "InFlightTransfer",
    "BridgeHistoryEntry",
    # Remediation state machine and operator cards
    "RemediationState",
    "RemediationTrigger",
    "OperatorCardStatus",
    "OperatorCardPriority",
    "RemediationResult",
    "OperatorCard",
    "RemediationStateRecord",
    "RemediationStateMachine",
    # Receipt Parser Registry
    "ReceiptParserRegistry",
    "ReceiptParser",
    "get_parser",
    "register_parser",
    "list_parsers",
    "is_parser_available",
    "extract_position_id",
    "ReceiptParserError",
    "ParserNotFoundError",
    # CLOB Handler (off-chain order execution)
    "ClobActionHandler",
    "ClobExecutionResult",
    "ClobFill",
    "ClobOrderState",
    "ClobOrderStatus",
    # Handler Registry (protocol-agnostic routing)
    "ExecutionHandler",
    "ExecutionHandlerRegistry",
    # Extracted Data Models (for Result Enrichment)
    "SwapAmounts",
    "LPCloseData",
    "LPOpenData",
    "BorrowData",
    "SupplyData",
    "PerpData",
    "StakeData",
    # Result Enricher (automatic receipt parsing)
    "ResultEnricher",
    "enrich_result",
    "get_enricher",
    # Revert Diagnostics (verbose error reporting)
    "RevertDiagnostic",
    "VerboseRevertReport",
    "TransactionDetails",
    "IntentDetails",
    "ActionDetails",
    "build_verbose_revert_report",
    "decode_calldata_selector",
    # Chain-family execution strategy (EVM / Solana)
    "ChainExecutionStrategy",
    "EvmExecutionStrategy",
    "ExecutionOutcome",
    # Solana types and planner
    "SolanaExecutionPlanner",
    "AccountMeta",
    "SolanaInstruction",
    "SolanaTransaction",
    "SignedSolanaTransaction",
    "SolanaTransactionReceipt",
]
