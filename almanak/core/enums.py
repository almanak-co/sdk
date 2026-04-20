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
    MANTLE = "MANTLE"
    BERACHAIN = "BERACHAIN"
    SOLANA = "SOLANA"
    MONAD = "MONAD"


class ChainFamily(Enum):
    """Execution substrate family. Routes to chain-family-specific code paths."""

    EVM = "EVM"
    SOLANA = "SOLANA"


# Authoritative mapping — every Chain MUST have a family
CHAIN_FAMILY_MAP: dict[Chain, "ChainFamily"] = {
    Chain.ETHEREUM: ChainFamily.EVM,
    Chain.ARBITRUM: ChainFamily.EVM,
    Chain.OPTIMISM: ChainFamily.EVM,
    Chain.BASE: ChainFamily.EVM,
    Chain.AVALANCHE: ChainFamily.EVM,
    Chain.POLYGON: ChainFamily.EVM,
    Chain.BSC: ChainFamily.EVM,
    Chain.SONIC: ChainFamily.EVM,
    Chain.PLASMA: ChainFamily.EVM,
    Chain.BLAST: ChainFamily.EVM,
    Chain.MANTLE: ChainFamily.EVM,
    Chain.BERACHAIN: ChainFamily.EVM,
    Chain.MONAD: ChainFamily.EVM,
    Chain.SOLANA: ChainFamily.SOLANA,
}


def get_chain_family(chain: Chain) -> ChainFamily:
    """Get the execution family for a chain.

    Args:
        chain: Chain enum value

    Returns:
        ChainFamily for the given chain

    Raises:
        KeyError: If chain has no mapped family
    """
    return CHAIN_FAMILY_MAP[chain]


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
    LIFI = "LIFI"


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
