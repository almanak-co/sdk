"""Gateway request validation utilities.

This module provides reusable validators for gateway service inputs.
All user-provided inputs should be validated before processing to prevent
injection attacks and ensure data integrity.
"""

import re
from typing import Any

# Allowed chains for all gateway operations
ALLOWED_CHAINS = frozenset(
    {
        "ethereum",
        "arbitrum",
        "base",
        "optimism",
        "polygon",
        "avalanche",
        "bsc",
        "bnb",
        "sonic",
        "plasma",
        "linea",
        "blast",
        "mantle",
        "berachain",
        "monad",
    }
)

# RPC methods that are safe to expose to strategy containers
# Excludes debug_*, admin_*, personal_*, and other sensitive methods
ALLOWED_RPC_METHODS = frozenset(
    {
        # State queries
        "eth_call",
        "eth_getBalance",
        "eth_getTransactionCount",
        "eth_getCode",
        "eth_getStorageAt",
        # Transaction queries
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
        "eth_getTransactionByBlockHashAndIndex",
        "eth_getTransactionByBlockNumberAndIndex",
        # Block queries
        "eth_blockNumber",
        "eth_getBlockByNumber",
        "eth_getBlockByHash",
        "eth_getBlockTransactionCountByHash",
        "eth_getBlockTransactionCountByNumber",
        # Logs and filters
        "eth_getLogs",
        "eth_newFilter",
        "eth_newBlockFilter",
        "eth_getFilterChanges",
        "eth_getFilterLogs",
        "eth_uninstallFilter",
        # Gas and fees
        "eth_gasPrice",
        "eth_estimateGas",
        "eth_feeHistory",
        "eth_maxPriorityFeePerGas",
        # Chain info
        "eth_chainId",
        "net_version",
        "net_listening",
        "web3_clientVersion",
        # Transaction submission (signing happens in gateway)
        "eth_sendRawTransaction",
    }
)

# Maximum sizes
MAX_STRATEGY_ID_LENGTH = 128
MAX_SYMBOL_LENGTH = 20
MAX_TOKEN_ID_LENGTH = 64
MAX_STATE_SIZE_BYTES = 1 * 1024 * 1024  # 1MB
MAX_BATCH_SIZE = 100
MAX_GRAPHQL_QUERY_LENGTH = 10000
MAX_GRAPHQL_DEPTH = 10

# Validation patterns
ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
STRATEGY_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_:-]{1,128}$")
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{1,20}$")
TOKEN_ID_PATTERN = re.compile(r"^[a-z0-9-]{1,64}$")
TX_HASH_PATTERN = re.compile(r"^0x[a-fA-F0-9]{64}$")


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


def validate_chain(chain: str, field: str = "chain") -> str:
    """Validate chain name against allowlist.

    Args:
        chain: Chain name to validate
        field: Field name for error messages

    Returns:
        Normalized chain name (lowercase)

    Raises:
        ValidationError: If chain is not in allowlist
    """
    if not chain:
        raise ValidationError(field, "required")

    chain = chain.lower().strip()
    if chain not in ALLOWED_CHAINS:
        allowed = ", ".join(sorted(ALLOWED_CHAINS))
        raise ValidationError(field, f"'{chain}' not allowed. Valid: {allowed}")

    return chain


def validate_address(address: str, field: str = "address") -> str:
    """Validate Ethereum address format.

    Args:
        address: Address to validate
        field: Field name for error messages

    Returns:
        Address (unchanged)

    Raises:
        ValidationError: If address format is invalid
    """
    if not address:
        raise ValidationError(field, "required")

    if not ADDRESS_PATTERN.match(address):
        raise ValidationError(field, "invalid address format (expected 0x + 40 hex chars)")

    return address


def validate_strategy_id(strategy_id: str, field: str = "strategy_id") -> str:
    """Validate strategy ID format.

    Strategy IDs must be alphanumeric with dashes and underscores,
    max 128 characters.

    Args:
        strategy_id: Strategy ID to validate
        field: Field name for error messages

    Returns:
        Strategy ID (unchanged)

    Raises:
        ValidationError: If format is invalid
    """
    if not strategy_id:
        raise ValidationError(field, "required")

    if not STRATEGY_ID_PATTERN.match(strategy_id):
        raise ValidationError(
            field, f"invalid format (alphanumeric, colons, dashes, underscores, max {MAX_STRATEGY_ID_LENGTH} chars)"
        )

    return strategy_id


def validate_rpc_method(method: str, field: str = "method") -> str:
    """Validate RPC method against allowlist.

    Only safe read methods and transaction submission are allowed.
    Dangerous methods (debug_*, admin_*, personal_*) are blocked.

    Args:
        method: RPC method name to validate
        field: Field name for error messages

    Returns:
        Method name (unchanged)

    Raises:
        ValidationError: If method is not allowed
    """
    if not method:
        raise ValidationError(field, "required")

    if method not in ALLOWED_RPC_METHODS:
        raise ValidationError(field, f"'{method}' is not allowed")

    return method


def validate_symbol(symbol: str, field: str = "symbol") -> str:
    """Validate trading pair symbol.

    Symbols must be uppercase alphanumeric, max 20 characters.
    Example: BTCUSDT, ETHUSDC

    Args:
        symbol: Symbol to validate
        field: Field name for error messages

    Returns:
        Normalized symbol (uppercase)

    Raises:
        ValidationError: If format is invalid
    """
    if not symbol:
        raise ValidationError(field, "required")

    symbol = symbol.upper().strip()
    if not SYMBOL_PATTERN.match(symbol):
        raise ValidationError(field, f"invalid format (uppercase alphanumeric, max {MAX_SYMBOL_LENGTH} chars)")

    return symbol


def validate_token_id(token_id: str, field: str = "token_id") -> str:
    """Validate CoinGecko token ID.

    Token IDs must be lowercase alphanumeric with dashes, max 64 characters.
    Example: ethereum, bitcoin, usd-coin

    Args:
        token_id: Token ID to validate
        field: Field name for error messages

    Returns:
        Normalized token ID (lowercase)

    Raises:
        ValidationError: If format is invalid
    """
    if not token_id:
        raise ValidationError(field, "required")

    token_id = token_id.lower().strip()
    if not TOKEN_ID_PATTERN.match(token_id):
        raise ValidationError(
            field, f"invalid format (lowercase alphanumeric with dashes, max {MAX_TOKEN_ID_LENGTH} chars)"
        )

    return token_id


def validate_tx_hash(tx_hash: str, field: str = "tx_hash") -> str:
    """Validate transaction hash format.

    Args:
        tx_hash: Transaction hash to validate
        field: Field name for error messages

    Returns:
        Transaction hash (unchanged)

    Raises:
        ValidationError: If format is invalid
    """
    if not tx_hash:
        raise ValidationError(field, "required")

    if not TX_HASH_PATTERN.match(tx_hash):
        raise ValidationError(field, "invalid format (expected 0x + 64 hex chars)")

    return tx_hash


def validate_state_size(data: bytes, field: str = "data") -> bytes:
    """Validate state data size.

    Args:
        data: State data bytes to validate
        field: Field name for error messages

    Returns:
        Data bytes (unchanged)

    Raises:
        ValidationError: If size exceeds limit
    """
    if len(data) > MAX_STATE_SIZE_BYTES:
        raise ValidationError(field, f"size {len(data)} exceeds maximum {MAX_STATE_SIZE_BYTES} bytes")

    return data


def validate_batch_size(items: list[Any], field: str = "batch") -> list[Any]:
    """Validate batch request size.

    Args:
        items: List of batch items to validate
        field: Field name for error messages

    Returns:
        Items list (unchanged)

    Raises:
        ValidationError: If batch size exceeds limit
    """
    if len(items) > MAX_BATCH_SIZE:
        raise ValidationError(field, f"size {len(items)} exceeds maximum {MAX_BATCH_SIZE} items")

    return items


def validate_graphql_query(query: str, field: str = "query") -> str:
    """Validate GraphQL query.

    Checks:
    - Query length limit
    - No introspection queries (security measure)

    Args:
        query: GraphQL query string to validate
        field: Field name for error messages

    Returns:
        Query string (unchanged)

    Raises:
        ValidationError: If validation fails
    """
    if not query:
        raise ValidationError(field, "required")

    if len(query) > MAX_GRAPHQL_QUERY_LENGTH:
        raise ValidationError(field, f"length {len(query)} exceeds maximum {MAX_GRAPHQL_QUERY_LENGTH} chars")

    # Block introspection queries (potential information disclosure)
    query_lower = query.lower()
    if "__schema" in query_lower or "__type" in query_lower:
        raise ValidationError(field, "introspection queries are not allowed")

    return query


def validate_positive_int(value: int, field: str, max_value: int | None = None) -> int:
    """Validate positive integer.

    Args:
        value: Integer to validate
        field: Field name for error messages
        max_value: Optional maximum value

    Returns:
        Value (unchanged)

    Raises:
        ValidationError: If validation fails
    """
    if value < 0:
        raise ValidationError(field, "must be non-negative")

    if max_value is not None and value > max_value:
        raise ValidationError(field, f"exceeds maximum {max_value}")

    return value
