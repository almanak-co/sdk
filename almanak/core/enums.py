from enum import Enum


class CoSigners(Enum):
    NONE = "NONE"
    EULITH = "EULITH"
    FIREBLOCKS = "FIREBLOCKS"


class ActionType(Enum):
    APPROVE = "APPROVE"
    WRAP = "WRAP"
    UNWRAP = "UNWRAP"
    TRANSFER = "TRANSFER"
    SWAP = "SWAP"
    OPEN_LP_POSITION = "OPEN_LP_POSITION"
    CLOSE_LP_POSITION = "CLOSE_LP_POSITION"
    PROPOSE_VAULT_VALUATION = "PROPOSE_VAULT_VALUATION"
    SETTLE_VAULT_DEPOSIT = "SETTLE_VAULT_DEPOSIT"
    SETTLE_VAULT_REDEEM = "SETTLE_VAULT_REDEEM"
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"
    SUPPLY = "SUPPLY"
    BORROW = "BORROW"
    REPAY = "REPAY"
    VAULT_REALLOCATE = "VAULT_REALLOCATE"
    VAULT_MANAGE = "VAULT_MANAGE"
    CUSTOM = "CUSTOM"


class TransactionType(Enum):
    APPROVE = "APPROVE"
    WRAP = "WRAP"
    UNWRAP = "UNWRAP"
    TRANSFER = "TRANSFER"
    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    INCREASE_LIQUIDITY = "INCREASE_LIQUIDITY"
    DECREASE_LIQUIDITY = "DECREASE_LIQUIDITY"
    CLOSE_POSITION_MULTICALL = "CLOSE_POSITION_MULTICALL"
    PROPOSE_VAULT_VALUATION = "PROPOSE_VAULT_VALUATION"
    SETTLE_VAULT_DEPOSIT = "SETTLE_VAULT_DEPOSIT"
    SETTLE_VAULT_REDEEM = "SETTLE_VAULT_REDEEM"
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"
    SUPPLY = "SUPPLY"
    BORROW = "BORROW"
    REPAY = "REPAY"


class SwapSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class Chain(Enum):
    ETHEREUM = "ETHEREUM"
    ARBITRUM = "ARBITRUM"
    OPTIMISM = "OPTIMISM"
    BASE = "BASE"
    AVALANCHE = "AVALANCHE"
    POLYGON = "POLYGON"
    BSC = "BSC"
    SONIC = "SONIC"
    PLASMA = "PLASMA"
    BLAST = "BLAST"
    LINEA = "LINEA"
    MANTLE = "MANTLE"
    BERACHAIN = "BERACHAIN"
    SOLANA = "SOLANA"
    MONAD = "MONAD"
    XLAYER = "XLAYER"
    ZEROG = "ZEROG"  # Preview support — 0G Chain (AI L1)


class ChainFamily(Enum):
    """Execution substrate family. Routes to chain-family-specific code paths."""

    EVM = "EVM"
    SOLANA = "SOLANA"


class CommitmentLevel(Enum):
    """Solana transaction commitment level.

    PROCESSED: Confirmed by 1 validator. Fastest but can be rolled back.
    CONFIRMED: Confirmed by supermajority. Good default for strategies.
    FINALIZED: Rooted in the chain. Slowest but irreversible.
    """

    PROCESSED = "processed"
    CONFIRMED = "confirmed"
    FINALIZED = "finalized"


class ATAPolicy(Enum):
    """Policy for Solana Associated Token Account creation during execution.

    AUTO_CREATE: Planner will create ATAs as needed (~0.002 SOL each).
    REQUIRE_EXISTING: Planner will fail if required ATAs don't exist.
    """

    AUTO_CREATE = "AUTO_CREATE"
    REQUIRE_EXISTING = "REQUIRE_EXISTING"


class AtomicityRequirement(Enum):
    """Controls how multi-instruction sequences are executed on Solana.

    ATOMIC: All instructions in a single transaction. Reverts if any fails.
    BEST_EFFORT: Execute as many as possible. Failed instructions don't revert prior successes.
    SEQUENTIAL_REQUIRED: Instructions execute in strict order, each in its own transaction.
    """

    ATOMIC = "ATOMIC"
    BEST_EFFORT = "BEST_EFFORT"
    SEQUENTIAL_REQUIRED = "SEQUENTIAL_REQUIRED"


class Protocol(Enum):
    UNISWAP_V3 = "UNISWAP_V3"
    UNISWAP_V4 = "UNISWAP_V4"
    PANCAKESWAP_V3 = "PANCAKESWAP_V3"
    SUSHISWAP_V3 = "SUSHISWAP_V3"
    TRADERJOE_V2 = "TRADERJOE_V2"
    AERODROME = "AERODROME"
    AGNI_FINANCE = "AGNI_FINANCE"
    CURVE = "CURVE"
    BALANCER = "BALANCER"
    VAULT = "VAULT"
    ENSO = "ENSO"
    PENDLE = "PENDLE"
    METAMORPHO = "METAMORPHO"
    SILO_V2 = "SILO_V2"
    LIFI = "LIFI"
    BENQI = "BENQI"
    # JOE_LEND retained for historical ActionBundle deserialization only —
    # protocol wound down by governance (VIB-3960). Compiler dispatch
    # short-circuits to CompilationStatus.FAILED with a deprecation message;
    # adapter constructor raises JoeLendDeprecatedError. Full enum removal
    # is part of the July full-cleanup ticket (VIB-3963).
    JOE_LEND = "JOE_LEND"
    EULER_V2 = "EULER_V2"


class Network(Enum):
    MAINNET = "MAINNET"
    ANVIL = "ANVIL"
    SEPOLIA = "SEPOLIA"


class NodeProvider(Enum):
    ALCHEMY = "ALCHEMY"
    TENDERLY = "TENDERLY"


class ExecutionStatus(Enum):
    CREATED = "CREATED"
    BUILT = "BUILT"
    SIGNED = "SIGNED"
    # In executioner module
    PRE_SEND = "PRE_SEND"  # Immediately before sending the bundle/transaction
    SENT = "SENT"  # Immediately after sending the bundle/transaction
    SUCCESS = "SUCCESS"  # Successfully executed bundle/transaction
    FAILED = "FAILED"  # Failed to execute bundle/transaction
    NOT_INCLUDED = "NOT_INCLUDED"  # Bundle/transaction was not included in the blockchain
    CANCELLED = "CANCELLED"  # Bundle/transaction was cancelled
    PARTIAL_EXECUTION = "PARTIAL_EXECUTION"  # Not all transactions in the bundle were executed successfully
    RECEIPT_UNKNOWN = "RECEIPT_UNKNOWN"  # bundle/transaction receipt cannot be obtained
    RECEIPT_PARSED_FAILURE = "RECEIPT_PARSED_FAILURE"  # Failed to parse the receipt successfully
    UNKNOWN = "UNKNOWN"  # The transactions in the bundle have statuses that are not expected
