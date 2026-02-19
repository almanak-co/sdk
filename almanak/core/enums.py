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


class Protocol(Enum):
    UNISWAP_V3 = "UNISWAP_V3"
    PANCAKESWAP_V3 = "PANCAKESWAP_V3"
    SUSHISWAP_V3 = "SUSHISWAP_V3"
    TRADERJOE_V2 = "TRADERJOE_V2"
    AERODROME = "AERODROME"
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
