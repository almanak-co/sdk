"""CLI command for scaffolding new protocol integrations.

Usage:
    almanak new-protocol --name <name> --type <type>

Example:
    almanak new-protocol --name compound_v3 --type lending
    almanak new-protocol --name dydx --type perps
"""

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import click


class ProtocolType(StrEnum):
    """Types of DeFi protocols."""

    DEX = "dex"
    PERPS = "perps"
    LENDING = "lending"
    YIELD = "yield"


class SupportedChain(StrEnum):
    """Supported blockchain networks."""

    ETHEREUM = "ethereum"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BASE = "base"
    AVALANCHE = "avalanche"


@dataclass
class ProtocolTypeConfig:
    """Configuration for a protocol type template."""

    name: str
    description: str
    operations: list[str]
    event_types: list[str]
    example_config_params: dict[str, str]


# Protocol type configurations with standard operations
PROTOCOL_TYPE_CONFIGS: dict[ProtocolType, ProtocolTypeConfig] = {
    ProtocolType.DEX: ProtocolTypeConfig(
        name="Decentralized Exchange",
        description="Swap tokens and provide liquidity on AMM/order-book DEXes",
        operations=["swap", "add_liquidity", "remove_liquidity", "get_quote", "get_pool_info"],
        event_types=["SWAP", "LIQUIDITY_ADDED", "LIQUIDITY_REMOVED", "SYNC"],
        example_config_params={
            "default_slippage_bps": "50",
            "default_deadline_seconds": "300",
        },
    ),
    ProtocolType.PERPS: ProtocolTypeConfig(
        name="Perpetual Futures",
        description="Trade perpetual futures with leverage",
        operations=[
            "open_position",
            "close_position",
            "increase_position",
            "decrease_position",
            "cancel_order",
            "get_position",
            "get_order",
        ],
        event_types=[
            "POSITION_OPENED",
            "POSITION_CLOSED",
            "POSITION_INCREASED",
            "POSITION_DECREASED",
            "ORDER_CREATED",
            "ORDER_EXECUTED",
            "ORDER_CANCELLED",
            "LIQUIDATION",
        ],
        example_config_params={
            "default_slippage_bps": "50",
            "default_leverage": "1",
            "execution_fee": "1000000000000000",
        },
    ),
    ProtocolType.LENDING: ProtocolTypeConfig(
        name="Lending Protocol",
        description="Supply, borrow, and manage collateral on lending protocols",
        operations=[
            "supply",
            "withdraw",
            "borrow",
            "repay",
            "get_user_position",
            "get_reserve_data",
            "get_health_factor",
        ],
        event_types=[
            "SUPPLY",
            "WITHDRAW",
            "BORROW",
            "REPAY",
            "LIQUIDATION_CALL",
            "FLASH_LOAN",
            "RESERVE_DATA_UPDATED",
        ],
        example_config_params={
            "default_slippage_bps": "50",
            "interest_rate_mode": "2",  # Variable rate
        },
    ),
    ProtocolType.YIELD: ProtocolTypeConfig(
        name="Yield Protocol",
        description="Deposit and manage positions in yield-generating protocols",
        operations=[
            "deposit",
            "withdraw",
            "claim_rewards",
            "get_position",
            "get_vault_info",
            "get_apy",
        ],
        event_types=[
            "DEPOSIT",
            "WITHDRAW",
            "REWARDS_CLAIMED",
            "STRATEGY_UPDATED",
            "HARVEST",
        ],
        example_config_params={
            "default_slippage_bps": "50",
            "auto_compound": "true",
        },
    ),
}


# Default gas estimates per operation category
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    # DEX
    "swap": 200000,
    "add_liquidity": 350000,
    "remove_liquidity": 300000,
    "get_quote": 0,
    "get_pool_info": 0,
    # Perps
    "open_position": 800000,
    "close_position": 600000,
    "increase_position": 700000,
    "decrease_position": 500000,
    "cancel_order": 200000,
    "get_position": 0,
    "get_order": 0,
    # Lending
    "supply": 200000,
    "withdraw": 200000,
    "borrow": 300000,
    "repay": 200000,
    "get_user_position": 0,
    "get_reserve_data": 0,
    "get_health_factor": 0,
    # Yield
    "deposit": 250000,
    "claim_rewards": 150000,
    "get_apy": 0,
    "get_vault_info": 0,
}


def to_snake_case(name: str) -> str:
    """Convert a string to snake_case."""
    # Replace spaces and hyphens with underscores
    name = re.sub(r"[\s\-]+", "_", name)
    # Insert underscore before uppercase letters and convert to lowercase
    name = re.sub(r"([A-Z])", r"_\1", name).lower()
    # Remove leading underscores and collapse multiple underscores
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def to_pascal_case(name: str) -> str:
    """Convert a string to PascalCase."""
    snake = to_snake_case(name)
    return "".join(word.capitalize() for word in snake.split("_"))


def to_upper_snake_case(name: str) -> str:
    """Convert a string to UPPER_SNAKE_CASE."""
    return to_snake_case(name).upper()


def generate_adapter_file(
    name: str,
    protocol_type: ProtocolType,
    chains: list[SupportedChain],
) -> str:
    """Generate the main adapter.py file content."""
    pascal_name = to_pascal_case(name)
    upper_name = to_upper_snake_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    # Generate chain addresses dict
    chain_addresses = ",\n".join(
        f'    "{chain.value}": {{\n'
        f'        "router": "0x0000000000000000000000000000000000000000",  # TODO: Add real address\n'
        f'        "factory": "0x0000000000000000000000000000000000000000",  # TODO: Add real address\n'
        f"    }}"
        for chain in chains
    )

    # Generate operation methods
    operation_methods = []
    for op in config.operations:
        method_name = op
        is_read_only = DEFAULT_GAS_ESTIMATES.get(op, 0) == 0
        gas_estimate = DEFAULT_GAS_ESTIMATES.get(op, 200000)

        if is_read_only:
            # Read-only method
            method = f'''
    def {method_name}(
        self,
        **kwargs: Any,
    ) -> {pascal_name}Result:
        """
        {op.replace("_", " ").title()} operation.

        This is a read-only operation that queries protocol state.

        Args:
            **kwargs: Operation-specific parameters

        Returns:
            {pascal_name}Result with operation data
        """
        logger.info(f"Executing {method_name} on {{self.config.chain}}")

        # TODO: Implement {method_name} logic
        # 1. Query protocol state
        # 2. Parse and return result

        return {pascal_name}Result(
            success=True,
            data={{}},
            error=None,
        )'''
        else:
            # State-changing method
            method = f'''
    def {method_name}(
        self,
        **kwargs: Any,
    ) -> TransactionResult:
        """
        {op.replace("_", " ").title()} operation.

        This operation modifies protocol state and requires a transaction.

        Args:
            **kwargs: Operation-specific parameters

        Returns:
            TransactionResult with transaction data
        """
        logger.info(f"Executing {method_name} on {{self.config.chain}}")

        # TODO: Implement {method_name} logic
        # 1. Validate parameters
        # 2. Build transaction data
        # 3. Return transaction result

        tx_data = self._build_{method_name}_tx(**kwargs)

        return TransactionResult(
            success=True,
            transaction=tx_data,
            gas_estimate={gas_estimate},
            error=None,
        )

    def _build_{method_name}_tx(
        self,
        **kwargs: Any,
    ) -> TransactionData:
        """Build transaction data for {method_name}."""
        addresses = {upper_name}_ADDRESSES[self.config.chain]

        # TODO: Build actual transaction data
        return TransactionData(
            to=addresses["router"],
            value=0,
            data="0x",  # TODO: Encode function call
            gas_estimate={gas_estimate},
            description="{op.replace("_", " ").title()} transaction",
        )'''
        operation_methods.append(method)

    operations_code = "\n".join(operation_methods)

    # Generate gas estimates dict
    gas_estimates_items = []
    for op in config.operations:
        gas = DEFAULT_GAS_ESTIMATES.get(op, 200000)
        if gas > 0:
            gas_estimates_items.append(f'    "{op}": {gas},')
    gas_estimates_code = "\n".join(gas_estimates_items)

    # Generate config params
    config_params = []
    for param, default in config.example_config_params.items():
        # Determine type from default value
        if default.lower() in ("true", "false"):
            config_params.append(f"    {param}: bool = {default.capitalize()}")
        elif default.replace(".", "").replace("-", "").isdigit():
            if "." in default:
                config_params.append(f"    {param}: float = {default}")
            else:
                config_params.append(f"    {param}: int = {default}")
        else:
            config_params.append(f'    {param}: str = "{default}"')
    config_params_code = "\n".join(config_params) if config_params else "    pass"

    content = f'''"""{pascal_name} Protocol Adapter.

This module provides the {pascal_name}Adapter class for interacting with
{config.name} protocol.

{config.description}

Supported chains:
{chr(10).join(f"- {chain.value.title()}" for chain in chains)}

Example:
    from almanak.framework.connectors.{to_snake_case(name)} import {pascal_name}Adapter, {pascal_name}Config

    config = {pascal_name}Config(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = {pascal_name}Adapter(config)

    # Execute operation
    result = adapter.{config.operations[0]}(...)

Generated by: almanak new-protocol
Protocol type: {protocol_type.value}
Created: {datetime.now().isoformat()}
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Contract addresses per chain
{upper_name}_ADDRESSES: dict[str, dict[str, str]] = {{
{chain_addresses}
}}

# Gas estimates for operations
{upper_name}_GAS_ESTIMATES: dict[str, int] = {{
{gas_estimates_code}
}}


# =============================================================================
# Enums
# =============================================================================

# TODO: Add protocol-specific enums here
# Example:
# class {pascal_name}OrderType(Enum):
#     """Order types for {pascal_name}."""
#     MARKET = "MARKET"
#     LIMIT = "LIMIT"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class {pascal_name}Config:
    """Configuration for {pascal_name}Adapter.

    Attributes:
        chain: Target blockchain
        wallet_address: Address executing transactions
{chr(10).join(f"        {param}: {config.example_config_params[param]}" for param in config.example_config_params)}
    """

    chain: str
    wallet_address: str
{config_params_code}

    def __post_init__(self) -> None:
        """Validate configuration."""
        supported_chains = list({upper_name}_ADDRESSES.keys())
        if self.chain not in supported_chains:
            raise ValueError(
                f"Unsupported chain: {{self.chain}}. "
                f"Supported: {{supported_chains}}"
            )

        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(
                f"Invalid wallet address format: {{self.wallet_address}}. "
                "Must be a 42-character hex string starting with 0x"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "chain": self.chain,
            "wallet_address": self.wallet_address,
{chr(10).join(f'            "{param}": self.{param},' for param in config.example_config_params)}
        }}


@dataclass
class TransactionData:
    """Transaction data ready to be submitted."""

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "to": self.to,
            "value": self.value,
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
        }}


@dataclass
class TransactionResult:
    """Result of a transaction-building operation."""

    success: bool
    transaction: Optional[TransactionData] = None
    gas_estimate: int = 0
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "success": self.success,
            "transaction": self.transaction.to_dict() if self.transaction else None,
            "gas_estimate": self.gas_estimate,
            "error": self.error,
        }}


@dataclass
class {pascal_name}Result:
    """Result of a read operation."""

    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "success": self.success,
            "data": self.data,
            "error": self.error,
        }}


# =============================================================================
# Adapter Class
# =============================================================================


class {pascal_name}Adapter:
    """
    Adapter for interacting with {config.name} protocol.

    {config.description}

    Example:
        adapter = {pascal_name}Adapter(config)
        result = adapter.{config.operations[0]}(...)
    """

    def __init__(self, config: {pascal_name}Config) -> None:
        """
        Initialize the adapter.

        Args:
            config: Adapter configuration
        """
        self.config = config
        self.chain = config.chain
        self._addresses = {upper_name}_ADDRESSES[self.chain]

        logger.info(
            f"Initialized {pascal_name}Adapter on {{self.chain}} "
            f"for wallet {{config.wallet_address[:10]}}..."
        )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"{pascal_name}Adapter("
            f"chain={{self.chain}}, "
            f"wallet={{self.config.wallet_address[:10]}}...)"
        )

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def router_address(self) -> str:
        """Get router contract address."""
        return self._addresses["router"]

    @property
    def factory_address(self) -> str:
        """Get factory contract address."""
        return self._addresses["factory"]

    # -------------------------------------------------------------------------
    # Public Methods
    # -------------------------------------------------------------------------
{operations_code}

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    def _encode_function_call(
        self,
        selector: str,
        params: list[Any],
    ) -> str:
        """
        Encode a function call with parameters.

        Args:
            selector: 4-byte function selector (e.g., "0x12345678")
            params: List of parameters to encode

        Returns:
            Encoded calldata as hex string
        """
        # TODO: Implement proper ABI encoding
        # This is a placeholder - use proper ABI encoding library
        data = selector

        for param in params:
            if isinstance(param, str) and param.startswith("0x"):
                # Address - pad to 32 bytes
                data += param[2:].lower().zfill(64)
            elif isinstance(param, int):
                # Integer - pad to 32 bytes
                data += hex(param)[2:].zfill(64)
            elif isinstance(param, bool):
                # Boolean - encode as uint256
                data += "1".zfill(64) if param else "0".zfill(64)

        return data

    def _get_gas_estimate(self, operation: str) -> int:
        """Get gas estimate for an operation."""
        return {upper_name}_GAS_ESTIMATES.get(operation, 200000)
'''

    return content


def generate_receipt_parser_file(
    name: str,
    protocol_type: ProtocolType,
) -> str:
    """Generate the receipt_parser.py file content."""
    pascal_name = to_pascal_case(name)
    to_upper_snake_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    # Generate event type enum values
    event_type_values = "\n".join(f'    {event} = "{event}"' for event in config.event_types)

    # Generate event topic mappings (placeholder hashes)
    event_topics = "\n".join(
        f'    "{event}": "0x' + "0" * 64 + '",  # TODO: Add real topic hash' for event in config.event_types
    )

    content = f'''"""{pascal_name} Receipt Parser.

This module provides the {pascal_name}ReceiptParser class for parsing
transaction receipts from {pascal_name} protocol.

Parses the following event types:
{chr(10).join(f"- {event}" for event in config.event_types)}

Example:
    from almanak.framework.connectors.{to_snake_case(name)} import {pascal_name}ReceiptParser

    parser = {pascal_name}ReceiptParser()
    result = parser.parse_receipt(receipt)

    for event in result.events:
        print(f"Event: {{event.event_type}}")

Generated by: almanak new-protocol
Protocol type: {protocol_type.value}
Created: {datetime.now().isoformat()}
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Event topic hashes (keccak256 of event signatures)
EVENT_TOPICS: dict[str, str] = {{
{event_topics}
}}

# Reverse mapping for lookup
TOPIC_TO_EVENT: dict[str, str] = {{v: k for k, v in EVENT_TOPICS.items()}}


# =============================================================================
# Enums
# =============================================================================


class {pascal_name}EventType(Enum):
    """Event types emitted by {pascal_name} protocol."""

{event_type_values}
    UNKNOWN = "UNKNOWN"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TransactionReceipt:
    """Raw transaction receipt data."""

    transaction_hash: str
    block_number: int
    block_hash: str
    status: int  # 1 = success, 0 = failure
    gas_used: int
    logs: list[dict[str, Any]] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Check if transaction succeeded."""
        return self.status == 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "status": self.status,
            "success": self.success,
            "gas_used": self.gas_used,
            "logs": self.logs,
        }}


@dataclass
class {pascal_name}Event:
    """Parsed event from {pascal_name} protocol."""

    event_type: {pascal_name}EventType
    transaction_hash: str
    log_index: int
    block_number: int
    timestamp: datetime
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "event_type": self.event_type.value,
            "transaction_hash": self.transaction_hash,
            "log_index": self.log_index,
            "block_number": self.block_number,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
        }}


@dataclass
class ParseResult:
    """Result of parsing a transaction receipt."""

    success: bool
    transaction_hash: str
    events: list[{pascal_name}Event] = field(default_factory=list)
    error: Optional[str] = None
    gas_used: int = 0

    @property
    def event_count(self) -> int:
        """Get number of parsed events."""
        return len(self.events)

    def get_events_by_type(
        self, event_type: {pascal_name}EventType
    ) -> list[{pascal_name}Event]:
        """Get all events of a specific type."""
        return [e for e in self.events if e.event_type == event_type]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {{
            "success": self.success,
            "transaction_hash": self.transaction_hash,
            "events": [e.to_dict() for e in self.events],
            "event_count": self.event_count,
            "error": self.error,
            "gas_used": self.gas_used,
        }}


# =============================================================================
# Parser Class
# =============================================================================


class {pascal_name}ReceiptParser:
    """
    Parser for {pascal_name} protocol transaction receipts.

    Parses transaction logs and extracts typed events with their data.

    Example:
        parser = {pascal_name}ReceiptParser()
        result = parser.parse_receipt(receipt)

        for event in result.events:
            if event.event_type == {pascal_name}EventType.{config.event_types[0]}:
                print(f"Found {config.event_types[0]} event")
    """

    def __init__(self) -> None:
        """Initialize the receipt parser."""
        logger.info("Initialized {pascal_name}ReceiptParser")

    def parse_receipt(
        self,
        receipt: TransactionReceipt,
        timestamp: Optional[datetime] = None,
    ) -> ParseResult:
        """
        Parse a transaction receipt.

        Args:
            receipt: Transaction receipt to parse
            timestamp: Optional timestamp (uses current time if not provided)

        Returns:
            ParseResult with extracted events
        """
        if timestamp is None:
            timestamp = datetime.now()

        events: list[{pascal_name}Event] = []

        try:
            for log_index, log in enumerate(receipt.logs):
                event = self._parse_log(
                    log=log,
                    transaction_hash=receipt.transaction_hash,
                    log_index=log_index,
                    block_number=receipt.block_number,
                    timestamp=timestamp,
                )
                if event is not None:
                    events.append(event)

            return ParseResult(
                success=receipt.success,
                transaction_hash=receipt.transaction_hash,
                events=events,
                error=None if receipt.success else "Transaction reverted",
                gas_used=receipt.gas_used,
            )

        except Exception as e:
            logger.error(f"Error parsing receipt: {{e}}")
            return ParseResult(
                success=False,
                transaction_hash=receipt.transaction_hash,
                events=[],
                error=str(e),
                gas_used=receipt.gas_used,
            )

    def _parse_log(
        self,
        log: dict[str, Any],
        transaction_hash: str,
        log_index: int,
        block_number: int,
        timestamp: datetime,
    ) -> Optional[{pascal_name}Event]:
        """
        Parse a single log entry.

        Args:
            log: Log entry from receipt
            transaction_hash: Transaction hash
            log_index: Index of log in receipt
            block_number: Block number
            timestamp: Block timestamp

        Returns:
            Parsed event or None if not recognized
        """
        topics = log.get("topics", [])
        if not topics:
            return None

        # Get first topic (event signature hash)
        topic0 = topics[0] if topics else None
        if topic0 is None:
            return None

        # Normalize topic format
        if isinstance(topic0, bytes):
            topic0 = "0x" + topic0.hex()
        elif not topic0.startswith("0x"):
            topic0 = "0x" + topic0

        # Look up event type
        event_name = TOPIC_TO_EVENT.get(topic0.lower())
        if event_name is None:
            return None

        try:
            event_type = {pascal_name}EventType(event_name)
        except ValueError:
            event_type = {pascal_name}EventType.UNKNOWN

        # Parse event data based on type
        data = self._parse_event_data(event_type, log)

        return {pascal_name}Event(
            event_type=event_type,
            transaction_hash=transaction_hash,
            log_index=log_index,
            block_number=block_number,
            timestamp=timestamp,
            data=data,
        )

    def _parse_event_data(
        self,
        event_type: {pascal_name}EventType,
        log: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Parse event-specific data from log.

        Args:
            event_type: Type of event
            log: Log entry

        Returns:
            Parsed event data
        """
        topics = log.get("topics", [])
        data = log.get("data", "0x")

        # Remove "0x" prefix if present
        if isinstance(data, str) and data.startswith("0x"):
            data = data[2:]

        # TODO: Implement event-specific parsing
        # Each event type has different data layout
        # Parse indexed params from topics[1:] and non-indexed from data

        return {{
            "raw_topics": [t.hex() if isinstance(t, bytes) else t for t in topics],
            "raw_data": data,
        }}

    # -------------------------------------------------------------------------
    # Event-Specific Parsing Methods
    # -------------------------------------------------------------------------

    # TODO: Add parsing methods for each event type
    # Example:
    # def _parse_{config.event_types[0].lower()}_event(
    #     self, topics: list[str], data: str
    # ) -> dict[str, Any]:
    #     \"\"\"Parse {config.event_types[0]} event data.\"\"\"
    #     return {{}}


# =============================================================================
# Utility Functions
# =============================================================================


def decode_uint256(data: str, offset: int = 0) -> int:
    """Decode a uint256 from hex data."""
    start = offset * 64
    end = start + 64
    return int(data[start:end], 16)


def decode_address(data: str, offset: int = 0) -> str:
    """Decode an address from hex data."""
    start = offset * 64 + 24  # Address is last 20 bytes of 32-byte slot
    end = start + 40
    return "0x" + data[start:end]


def decode_bool(data: str, offset: int = 0) -> bool:
    """Decode a bool from hex data."""
    return decode_uint256(data, offset) != 0
'''

    return content


def generate_sdk_wrapper_file(
    name: str,
    protocol_type: ProtocolType,
) -> str:
    """Generate the sdk.py wrapper file content."""
    pascal_name = to_pascal_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    content = f'''"""{pascal_name} SDK Wrapper.

This module provides a high-level SDK wrapper for the {pascal_name} adapter,
offering async support, caching, and additional convenience methods.

Example:
    from almanak.framework.connectors.{to_snake_case(name)} import {pascal_name}SDK

    sdk = {pascal_name}SDK(config)
    async with sdk:
        result = await sdk.{config.operations[0]}_async(...)

Generated by: almanak new-protocol
Protocol type: {protocol_type.value}
Created: {datetime.now().isoformat()}
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional
import logging

from .adapter import (
    {pascal_name}Adapter,
    {pascal_name}Config,
    {pascal_name}Result,
    TransactionResult,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Default cache TTL for read operations
DEFAULT_CACHE_TTL_SECONDS = 30


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CacheEntry:
    """Cached result with expiration."""

    data: Any
    expires_at: datetime

    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return datetime.now() > self.expires_at


@dataclass
class {pascal_name}SDKConfig:
    """Configuration for {pascal_name}SDK.

    Attributes:
        adapter_config: Configuration for the underlying adapter
        cache_ttl_seconds: TTL for cached read operations
        max_retries: Maximum number of retries for failed operations
        retry_delay_seconds: Delay between retries
    """

    adapter_config: {pascal_name}Config
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS
    max_retries: int = 3
    retry_delay_seconds: float = 1.0


# =============================================================================
# SDK Class
# =============================================================================


class {pascal_name}SDK:
    """
    High-level SDK wrapper for {pascal_name} protocol.

    Provides:
    - Async operation support
    - Result caching for read operations
    - Automatic retries
    - Connection management

    Example:
        sdk = {pascal_name}SDK(config)
        async with sdk:
            result = await sdk.{config.operations[0]}_async(...)
    """

    def __init__(
        self,
        config: {pascal_name}SDKConfig | {pascal_name}Config,
    ) -> None:
        """
        Initialize the SDK.

        Args:
            config: SDK or adapter configuration
        """
        if isinstance(config, {pascal_name}Config):
            self._sdk_config = {pascal_name}SDKConfig(adapter_config=config)
        else:
            self._sdk_config = config

        self._adapter = {pascal_name}Adapter(self._sdk_config.adapter_config)
        self._cache: dict[str, CacheEntry] = {{}}
        self._initialized = False

        logger.info(
            f"Initialized {pascal_name}SDK on {{self._adapter.chain}} "
            f"with {{self._sdk_config.cache_ttl_seconds}}s cache TTL"
        )

    # -------------------------------------------------------------------------
    # Context Manager
    # -------------------------------------------------------------------------

    async def __aenter__(self) -> "{pascal_name}SDK":
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: Any,
        exc_val: Any,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.close()

    async def initialize(self) -> None:
        """Initialize SDK connections."""
        if self._initialized:
            return

        # TODO: Initialize any async connections (websockets, etc.)
        self._initialized = True
        logger.info("{pascal_name}SDK initialized")

    async def close(self) -> None:
        """Close SDK connections."""
        if not self._initialized:
            return

        # Clear cache
        self._cache.clear()

        # TODO: Close any async connections
        self._initialized = False
        logger.info("{pascal_name}SDK closed")

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def adapter(self) -> {pascal_name}Adapter:
        """Get underlying adapter."""
        return self._adapter

    @property
    def chain(self) -> str:
        """Get target chain."""
        return self._adapter.chain

    # -------------------------------------------------------------------------
    # Cache Methods
    # -------------------------------------------------------------------------

    def _get_cached(self, key: str) -> Optional[Any]:
        """Get cached value if not expired."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry.is_expired:
            del self._cache[key]
            return None
        return entry.data

    def _set_cached(self, key: str, data: Any) -> None:
        """Set cached value with TTL."""
        expires_at = datetime.now() + timedelta(
            seconds=self._sdk_config.cache_ttl_seconds
        )
        self._cache[key] = CacheEntry(data=data, expires_at=expires_at)

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()

    # -------------------------------------------------------------------------
    # Async Wrapper Methods
    # -------------------------------------------------------------------------

    async def _run_with_retry(
        self,
        operation: str,
        func: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Run an operation with automatic retries.

        Args:
            operation: Operation name for logging
            func: Function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Operation result
        """
        last_error: Optional[Exception] = None

        for attempt in range(self._sdk_config.max_retries):
            try:
                result = await asyncio.to_thread(func, *args, **kwargs)
                return result
            except Exception as e:
                last_error = e
                logger.warning(
                    f"{{operation}} attempt {{attempt + 1}}/{{self._sdk_config.max_retries}} "
                    f"failed: {{e}}"
                )
                if attempt < self._sdk_config.max_retries - 1:
                    await asyncio.sleep(self._sdk_config.retry_delay_seconds)

        raise last_error or RuntimeError(f"{{operation}} failed after retries")

    # TODO: Add async wrapper methods for each adapter operation
    # Example:
    # async def {config.operations[0]}_async(self, **kwargs: Any) -> TransactionResult:
    #     \"\"\"Async version of {config.operations[0]}.\"\"\"
    #     return await self._run_with_retry(
    #         "{config.operations[0]}",
    #         self._adapter.{config.operations[0]},
    #         **kwargs,
    #     )
'''

    return content


def generate_test_file(
    name: str,
    protocol_type: ProtocolType,
    chains: list[SupportedChain],
) -> str:
    """Generate the test_adapter.py file content."""
    pascal_name = to_pascal_case(name)
    to_snake_case(name)
    upper_name = to_upper_snake_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    # Generate test methods for each operation
    test_methods = []
    for op in config.operations:
        method = f'''
    def test_{op}(self, adapter: {pascal_name}Adapter) -> None:
        """Test {op.replace("_", " ")} operation."""
        # TODO: Add proper test implementation
        result = adapter.{op}()
        assert result is not None'''
        test_methods.append(method)

    test_methods_code = "\n".join(test_methods)

    content = f'''"""Tests for {pascal_name} Adapter.

Generated by: almanak new-protocol
Protocol type: {protocol_type.value}
Created: {datetime.now().isoformat()}
"""

import pytest
from decimal import Decimal
from datetime import datetime

from ..adapter import (
    {pascal_name}Adapter,
    {pascal_name}Config,
    {pascal_name}Result,
    TransactionResult,
    TransactionData,
    {upper_name}_ADDRESSES,
    {upper_name}_GAS_ESTIMATES,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> {pascal_name}Config:
    """Create test configuration."""
    return {pascal_name}Config(
        chain="{chains[0].value}",
        wallet_address="0x" + "1" * 40,
    )


@pytest.fixture
def adapter(config: {pascal_name}Config) -> {pascal_name}Adapter:
    """Create adapter instance for testing."""
    return {pascal_name}Adapter(config)


# =============================================================================
# Config Tests
# =============================================================================


class Test{pascal_name}Config:
    """Tests for {pascal_name}Config."""

    def test_valid_config(self) -> None:
        """Test valid configuration creation."""
        config = {pascal_name}Config(
            chain="{chains[0].value}",
            wallet_address="0x" + "a" * 40,
        )
        assert config.chain == "{chains[0].value}"
        assert config.wallet_address == "0x" + "a" * 40

    def test_invalid_chain(self) -> None:
        """Test invalid chain raises error."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            {pascal_name}Config(
                chain="invalid_chain",
                wallet_address="0x" + "a" * 40,
            )

    def test_invalid_wallet_address(self) -> None:
        """Test invalid wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            {pascal_name}Config(
                chain="{chains[0].value}",
                wallet_address="invalid_address",
            )

    def test_to_dict(self, config: {pascal_name}Config) -> None:
        """Test configuration serialization."""
        data = config.to_dict()
        assert data["chain"] == "{chains[0].value}"
        assert "wallet_address" in data


# =============================================================================
# Adapter Tests
# =============================================================================


class Test{pascal_name}Adapter:
    """Tests for {pascal_name}Adapter."""

    def test_initialization(self, adapter: {pascal_name}Adapter) -> None:
        """Test adapter initialization."""
        assert adapter.chain == "{chains[0].value}"
        assert adapter.router_address is not None

    def test_repr(self, adapter: {pascal_name}Adapter) -> None:
        """Test string representation."""
        repr_str = repr(adapter)
        assert "{pascal_name}Adapter" in repr_str
        assert "{chains[0].value}" in repr_str

    def test_properties(self, adapter: {pascal_name}Adapter) -> None:
        """Test adapter properties."""
        assert adapter.router_address in {upper_name}_ADDRESSES["{chains[0].value}"].values()
{test_methods_code}


# =============================================================================
# Data Class Tests
# =============================================================================


class TestTransactionData:
    """Tests for TransactionData."""

    def test_creation(self) -> None:
        """Test transaction data creation."""
        tx = TransactionData(
            to="0x" + "a" * 40,
            value=0,
            data="0x12345678",
            gas_estimate=200000,
            description="Test transaction",
        )
        assert tx.to == "0x" + "a" * 40
        assert tx.gas_estimate == 200000

    def test_to_dict(self) -> None:
        """Test serialization."""
        tx = TransactionData(
            to="0x" + "a" * 40,
            value=100,
            data="0x",
            gas_estimate=100000,
            description="Test",
        )
        data = tx.to_dict()
        assert data["to"] == "0x" + "a" * 40
        assert data["value"] == 100


class TestTransactionResult:
    """Tests for TransactionResult."""

    def test_success_result(self) -> None:
        """Test successful result."""
        tx = TransactionData(
            to="0x" + "a" * 40,
            value=0,
            data="0x",
            gas_estimate=200000,
            description="Test",
        )
        result = TransactionResult(
            success=True,
            transaction=tx,
            gas_estimate=200000,
        )
        assert result.success is True
        assert result.transaction is not None

    def test_failure_result(self) -> None:
        """Test failed result."""
        result = TransactionResult(
            success=False,
            error="Operation failed",
        )
        assert result.success is False
        assert result.error == "Operation failed"


class Test{pascal_name}Result:
    """Tests for {pascal_name}Result."""

    def test_success_result(self) -> None:
        """Test successful result."""
        result = {pascal_name}Result(
            success=True,
            data={{"key": "value"}},
        )
        assert result.success is True
        assert result.data["key"] == "value"

    def test_to_dict(self) -> None:
        """Test serialization."""
        result = {pascal_name}Result(
            success=True,
            data={{"test": 123}},
        )
        data = result.to_dict()
        assert data["success"] is True
        assert data["data"]["test"] == 123


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_addresses_exist(self) -> None:
        """Test address constants exist for all chains."""
        assert "{chains[0].value}" in {upper_name}_ADDRESSES
        assert "router" in {upper_name}_ADDRESSES["{chains[0].value}"]

    def test_gas_estimates_exist(self) -> None:
        """Test gas estimate constants exist."""
        assert len({upper_name}_GAS_ESTIMATES) > 0
'''

    return content


def generate_receipt_parser_test_file(
    name: str,
    protocol_type: ProtocolType,
) -> str:
    """Generate the test_receipt_parser.py file content."""
    pascal_name = to_pascal_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    content = f'''"""Tests for {pascal_name} Receipt Parser.

Generated by: almanak new-protocol
Protocol type: {protocol_type.value}
Created: {datetime.now().isoformat()}
"""

import pytest
from datetime import datetime

from ..receipt_parser import (
    {pascal_name}ReceiptParser,
    {pascal_name}Event,
    {pascal_name}EventType,
    TransactionReceipt,
    ParseResult,
    EVENT_TOPICS,
    decode_uint256,
    decode_address,
    decode_bool,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> {pascal_name}ReceiptParser:
    """Create parser instance for testing."""
    return {pascal_name}ReceiptParser()


@pytest.fixture
def success_receipt() -> TransactionReceipt:
    """Create a successful transaction receipt."""
    return TransactionReceipt(
        transaction_hash="0x" + "a" * 64,
        block_number=12345678,
        block_hash="0x" + "b" * 64,
        status=1,
        gas_used=200000,
        logs=[],
    )


@pytest.fixture
def failed_receipt() -> TransactionReceipt:
    """Create a failed transaction receipt."""
    return TransactionReceipt(
        transaction_hash="0x" + "c" * 64,
        block_number=12345679,
        block_hash="0x" + "d" * 64,
        status=0,
        gas_used=100000,
        logs=[],
    )


# =============================================================================
# Receipt Tests
# =============================================================================


class TestTransactionReceipt:
    """Tests for TransactionReceipt."""

    def test_success_property(self, success_receipt: TransactionReceipt) -> None:
        """Test success property on successful receipt."""
        assert success_receipt.success is True

    def test_failure_property(self, failed_receipt: TransactionReceipt) -> None:
        """Test success property on failed receipt."""
        assert failed_receipt.success is False

    def test_to_dict(self, success_receipt: TransactionReceipt) -> None:
        """Test serialization."""
        data = success_receipt.to_dict()
        assert data["status"] == 1
        assert data["success"] is True
        assert data["gas_used"] == 200000


# =============================================================================
# Parser Tests
# =============================================================================


class Test{pascal_name}ReceiptParser:
    """Tests for {pascal_name}ReceiptParser."""

    def test_initialization(self, parser: {pascal_name}ReceiptParser) -> None:
        """Test parser initialization."""
        assert parser is not None

    def test_parse_empty_receipt(
        self,
        parser: {pascal_name}ReceiptParser,
        success_receipt: TransactionReceipt,
    ) -> None:
        """Test parsing receipt with no logs."""
        result = parser.parse_receipt(success_receipt)

        assert result.success is True
        assert result.event_count == 0
        assert result.error is None

    def test_parse_failed_receipt(
        self,
        parser: {pascal_name}ReceiptParser,
        failed_receipt: TransactionReceipt,
    ) -> None:
        """Test parsing failed receipt."""
        result = parser.parse_receipt(failed_receipt)

        assert result.success is False
        assert "reverted" in result.error.lower() if result.error else False

    def test_parse_with_timestamp(
        self,
        parser: {pascal_name}ReceiptParser,
        success_receipt: TransactionReceipt,
    ) -> None:
        """Test parsing with explicit timestamp."""
        timestamp = datetime(2024, 1, 15, 12, 0, 0)
        result = parser.parse_receipt(success_receipt, timestamp=timestamp)

        assert result.success is True


# =============================================================================
# Event Tests
# =============================================================================


class Test{pascal_name}Event:
    """Tests for {pascal_name}Event."""

    def test_creation(self) -> None:
        """Test event creation."""
        event = {pascal_name}Event(
            event_type={pascal_name}EventType.{config.event_types[0]},
            transaction_hash="0x" + "a" * 64,
            log_index=0,
            block_number=12345678,
            timestamp=datetime.now(),
            data={{"key": "value"}},
        )

        assert event.event_type == {pascal_name}EventType.{config.event_types[0]}
        assert event.log_index == 0

    def test_to_dict(self) -> None:
        """Test serialization."""
        event = {pascal_name}Event(
            event_type={pascal_name}EventType.{config.event_types[0]},
            transaction_hash="0x" + "a" * 64,
            log_index=1,
            block_number=12345678,
            timestamp=datetime.now(),
        )

        data = event.to_dict()
        assert data["event_type"] == "{config.event_types[0]}"
        assert data["log_index"] == 1


class Test{pascal_name}EventType:
    """Tests for {pascal_name}EventType enum."""

    def test_all_event_types_exist(self) -> None:
        """Test all expected event types exist."""
        expected_types = {config.event_types}

        for event_type in expected_types:
            assert hasattr({pascal_name}EventType, event_type)

    def test_unknown_type(self) -> None:
        """Test UNKNOWN event type exists."""
        assert {pascal_name}EventType.UNKNOWN.value == "UNKNOWN"


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult."""

    def test_event_count(self) -> None:
        """Test event count property."""
        result = ParseResult(
            success=True,
            transaction_hash="0x" + "a" * 64,
            events=[
                {pascal_name}Event(
                    event_type={pascal_name}EventType.{config.event_types[0]},
                    transaction_hash="0x" + "a" * 64,
                    log_index=0,
                    block_number=12345678,
                    timestamp=datetime.now(),
                )
            ],
        )

        assert result.event_count == 1

    def test_get_events_by_type(self) -> None:
        """Test filtering events by type."""
        events = [
            {pascal_name}Event(
                event_type={pascal_name}EventType.{config.event_types[0]},
                transaction_hash="0x" + "a" * 64,
                log_index=0,
                block_number=12345678,
                timestamp=datetime.now(),
            ),
            {pascal_name}Event(
                event_type={pascal_name}EventType.UNKNOWN,
                transaction_hash="0x" + "a" * 64,
                log_index=1,
                block_number=12345678,
                timestamp=datetime.now(),
            ),
        ]

        result = ParseResult(
            success=True,
            transaction_hash="0x" + "a" * 64,
            events=events,
        )

        filtered = result.get_events_by_type({pascal_name}EventType.{config.event_types[0]})
        assert len(filtered) == 1
        assert filtered[0].event_type == {pascal_name}EventType.{config.event_types[0]}


# =============================================================================
# Utility Function Tests
# =============================================================================


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_decode_uint256(self) -> None:
        """Test uint256 decoding."""
        # 100 in hex, padded to 64 chars
        data = "0" * 62 + "64"
        result = decode_uint256(data, 0)
        assert result == 100

    def test_decode_address(self) -> None:
        """Test address decoding."""
        # Address padded to 64 chars (12 bytes of zeros + 20 byte address)
        data = "0" * 24 + "a" * 40
        result = decode_address(data, 0)
        assert result == "0x" + "a" * 40

    def test_decode_bool_true(self) -> None:
        """Test bool decoding - true."""
        data = "0" * 63 + "1"
        result = decode_bool(data, 0)
        assert result is True

    def test_decode_bool_false(self) -> None:
        """Test bool decoding - false."""
        data = "0" * 64
        result = decode_bool(data, 0)
        assert result is False


# =============================================================================
# Constants Tests
# =============================================================================


class TestEventTopics:
    """Tests for event topic constants."""

    def test_event_topics_exist(self) -> None:
        """Test event topics are defined."""
        assert len(EVENT_TOPICS) > 0

    def test_topic_format(self) -> None:
        """Test topics are valid hex strings."""
        for event_name, topic in EVENT_TOPICS.items():
            assert topic.startswith("0x"), f"{{event_name}} topic should start with 0x"
            assert len(topic) == 66, f"{{event_name}} topic should be 66 chars (0x + 64)"
'''

    return content


def generate_init_file(
    name: str,
    protocol_type: ProtocolType,
) -> str:
    """Generate the __init__.py file content."""
    pascal_name = to_pascal_case(name)
    upper_name = to_upper_snake_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    content = f'''"""{pascal_name} Protocol Connector.

This module provides an adapter for interacting with {config.name},
supporting {", ".join(config.operations[:3])} and more.

{config.description}

Example:
    from almanak.framework.connectors.{to_snake_case(name)} import {pascal_name}Adapter, {pascal_name}Config

    config = {pascal_name}Config(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = {pascal_name}Adapter(config)

    result = adapter.{config.operations[0]}(...)

Generated by: almanak new-protocol
Protocol type: {protocol_type.value}
Created: {datetime.now().isoformat()}
"""

from .adapter import (
    {pascal_name}Adapter,
    {pascal_name}Config,
    {pascal_name}Result,
    TransactionResult,
    TransactionData,
    {upper_name}_ADDRESSES,
    {upper_name}_GAS_ESTIMATES,
)
from .receipt_parser import (
    {pascal_name}ReceiptParser,
    {pascal_name}Event,
    {pascal_name}EventType,
    TransactionReceipt,
    ParseResult,
    EVENT_TOPICS,
)
from .sdk import (
    {pascal_name}SDK,
    {pascal_name}SDKConfig,
)

__all__ = [
    # Adapter
    "{pascal_name}Adapter",
    "{pascal_name}Config",
    "{pascal_name}Result",
    "TransactionResult",
    "TransactionData",
    # Receipt Parser
    "{pascal_name}ReceiptParser",
    "{pascal_name}Event",
    "{pascal_name}EventType",
    "TransactionReceipt",
    "ParseResult",
    "EVENT_TOPICS",
    # SDK
    "{pascal_name}SDK",
    "{pascal_name}SDKConfig",
    # Constants
    "{upper_name}_ADDRESSES",
    "{upper_name}_GAS_ESTIMATES",
]
'''

    return content


def generate_readme_file(
    name: str,
    protocol_type: ProtocolType,
    chains: list[SupportedChain],
) -> str:
    """Generate the README.md file content."""
    pascal_name = to_pascal_case(name)
    snake_name = to_snake_case(name)
    config = PROTOCOL_TYPE_CONFIGS[protocol_type]

    content = f"""# {pascal_name} Connector

{config.description}

## Overview

This connector provides a standardized interface for interacting with {pascal_name} protocol,
a {config.name.lower()}.

## Supported Chains

{chr(10).join(f"- {chain.value.title()}" for chain in chains)}

## Operations

{chr(10).join(f"- `{op}`: {op.replace('_', ' ').title()}" for op in config.operations)}

## Installation

The connector is included in the Almanak Strategy Framework. No additional installation required.

## Usage

### Basic Usage

```python
from almanak.framework.connectors.{snake_name} import {pascal_name}Adapter, {pascal_name}Config

# Configure the adapter
config = {pascal_name}Config(
    chain="arbitrum",
    wallet_address="0xYourWalletAddress",
)

# Create adapter instance
adapter = {pascal_name}Adapter(config)

# Execute operations
result = adapter.{config.operations[0]}(
    # Add operation-specific parameters
)

if result.success:
    print("Operation successful!")
    print(result.to_dict())
else:
    print(f"Operation failed: {{result.error}}")
```

### Using the SDK (Async)

```python
from almanak.framework.connectors.{snake_name} import {pascal_name}SDK, {pascal_name}Config

config = {pascal_name}Config(
    chain="arbitrum",
    wallet_address="0xYourWalletAddress",
)

async def main():
    sdk = {pascal_name}SDK(config)
    async with sdk:
        # Operations with automatic retries and caching
        result = await sdk.{config.operations[0]}_async(...)

asyncio.run(main())
```

### Parsing Transaction Receipts

```python
from almanak.framework.connectors.{snake_name} import (
    {pascal_name}ReceiptParser,
    {pascal_name}EventType,
    TransactionReceipt,
)

parser = {pascal_name}ReceiptParser()

# Parse a transaction receipt
receipt = TransactionReceipt(
    transaction_hash="0x...",
    block_number=12345678,
    block_hash="0x...",
    status=1,
    gas_used=200000,
    logs=[...],
)

result = parser.parse_receipt(receipt)

for event in result.events:
    if event.event_type == {pascal_name}EventType.{config.event_types[0]}:
        print(f"Found {config.event_types[0]} event: {{event.data}}")
```

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `chain` | str | - | Target blockchain (required) |
| `wallet_address` | str | - | Wallet address for transactions (required) |
{chr(10).join(f"| `{param}` | varies | {default} | {param.replace('_', ' ').title()} |" for param, default in config.example_config_params.items())}

## Event Types

{chr(10).join(f"- `{event}`: {event.replace('_', ' ').title()} event" for event in config.event_types)}

## Testing

Run the test suite:

```bash
# Run all tests
pytest src/connectors/{snake_name}/tests/ -v

# Run specific test file
pytest src/connectors/{snake_name}/tests/test_adapter.py -v

# Run with coverage
pytest src/connectors/{snake_name}/tests/ --cov=src.connectors.{snake_name}
```

## Development

### Adding New Operations

1. Add operation method to `adapter.py`
2. Add corresponding event type to `receipt_parser.py`
3. Update SDK wrapper in `sdk.py`
4. Add tests

### Adding New Chains

1. Add chain addresses to `{to_upper_snake_case(name)}_ADDRESSES` in `adapter.py`
2. Update chain validation in `{pascal_name}Config.__post_init__()`
3. Add chain-specific tests

## Generated By

This connector was scaffolded using:

```bash
almanak new-protocol --name {name} --type {protocol_type.value}
```

Generated: {datetime.now().isoformat()}
"""

    return content


@click.command("new-protocol")
@click.option(
    "--name",
    "-n",
    required=True,
    help="Name for the new protocol (e.g., 'compound_v3', 'dydx')",
)
@click.option(
    "--type",
    "-t",
    "protocol_type",
    type=click.Choice([t.value for t in ProtocolType]),
    required=True,
    help="Type of protocol (dex, perps, lending, yield)",
)
@click.option(
    "--chain",
    "-c",
    "chains",
    type=click.Choice([c.value for c in SupportedChain]),
    multiple=True,
    default=["arbitrum"],
    help="Target chain(s) (can specify multiple)",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output directory (default: src/connectors/<name>)",
)
def new_protocol(
    name: str,
    protocol_type: str,
    chains: tuple[str, ...],
    output_dir: str | None,
) -> None:
    """
    Scaffold a new protocol connector from a template.

    This command generates a complete connector directory structure with:
    - adapter.py: Main protocol adapter class
    - receipt_parser.py: Transaction receipt parser
    - sdk.py: High-level SDK wrapper with async support
    - tests/: Comprehensive test suite
    - README.md: Documentation

    The connector follows Almanak's standard patterns for protocol integration.

    Examples:

        almanak new-protocol --name compound_v3 --type lending

        almanak new-protocol -n gmx_v3 -t perps -c arbitrum -c avalanche

        almanak new-protocol --name curve --type dex --chain ethereum --chain polygon
    """
    protocol_type_enum = ProtocolType(protocol_type)
    chain_enums = [SupportedChain(c) for c in chains]
    snake_name = to_snake_case(name)
    pascal_name = to_pascal_case(name)

    # Determine output directory
    if output_dir:
        protocol_dir = Path(output_dir)
    else:
        # Default to src/connectors/<name>
        src_dir = Path(__file__).parent.parent
        connectors_dir = src_dir / "connectors"
        protocol_dir = connectors_dir / snake_name

    # Check if directory already exists
    if protocol_dir.exists():
        click.echo(f"Error: Directory already exists: {protocol_dir}", err=True)
        raise click.Abort()

    # Print header
    click.echo(f"Creating protocol connector: {snake_name}")
    click.echo(f"Type: {protocol_type_enum.value} ({PROTOCOL_TYPE_CONFIGS[protocol_type_enum].name})")
    click.echo(f"Chains: {', '.join(c.value for c in chain_enums)}")
    click.echo(f"Output: {protocol_dir}")
    click.echo()

    try:
        # Create directories
        protocol_dir.mkdir(parents=True, exist_ok=True)
        tests_dir = protocol_dir / "tests"
        tests_dir.mkdir(exist_ok=True)

        files_created: list[str] = []

        # Generate adapter.py
        adapter_file = protocol_dir / "adapter.py"
        adapter_content = generate_adapter_file(name, protocol_type_enum, chain_enums)
        with open(adapter_file, "w") as f:
            f.write(adapter_content)
        files_created.append("adapter.py")

        # Generate receipt_parser.py
        parser_file = protocol_dir / "receipt_parser.py"
        parser_content = generate_receipt_parser_file(name, protocol_type_enum)
        with open(parser_file, "w") as f:
            f.write(parser_content)
        files_created.append("receipt_parser.py")

        # Generate sdk.py
        sdk_file = protocol_dir / "sdk.py"
        sdk_content = generate_sdk_wrapper_file(name, protocol_type_enum)
        with open(sdk_file, "w") as f:
            f.write(sdk_content)
        files_created.append("sdk.py")

        # Generate __init__.py
        init_file = protocol_dir / "__init__.py"
        init_content = generate_init_file(name, protocol_type_enum)
        with open(init_file, "w") as f:
            f.write(init_content)
        files_created.append("__init__.py")

        # Generate tests/__init__.py
        tests_init = tests_dir / "__init__.py"
        with open(tests_init, "w") as f:
            f.write(f'"""Tests for {pascal_name} connector."""\n')
        files_created.append("tests/__init__.py")

        # Generate tests/test_adapter.py
        test_adapter_file = tests_dir / "test_adapter.py"
        test_adapter_content = generate_test_file(name, protocol_type_enum, chain_enums)
        with open(test_adapter_file, "w") as f:
            f.write(test_adapter_content)
        files_created.append("tests/test_adapter.py")

        # Generate tests/test_receipt_parser.py
        test_parser_file = tests_dir / "test_receipt_parser.py"
        test_parser_content = generate_receipt_parser_test_file(name, protocol_type_enum)
        with open(test_parser_file, "w") as f:
            f.write(test_parser_content)
        files_created.append("tests/test_receipt_parser.py")

        # Generate README.md
        readme_file = protocol_dir / "README.md"
        readme_content = generate_readme_file(name, protocol_type_enum, chain_enums)
        with open(readme_file, "w") as f:
            f.write(readme_content)
        files_created.append("README.md")

        # Print success message
        click.echo("Files created:")
        for file_path in files_created:
            click.echo(f"  - {snake_name}/{file_path}")

        click.echo()
        click.echo("Next steps:")
        click.echo(f"  1. cd {protocol_dir}")
        click.echo("  2. Update contract addresses in adapter.py")
        click.echo("  3. Implement operation methods with real protocol logic")
        click.echo("  4. Add event topic hashes to receipt_parser.py")
        click.echo("  5. Run tests: pytest tests/ -v")
        click.echo("  6. Add connector to src/connectors/__init__.py")
        click.echo()

        # Show operations to implement
        config = PROTOCOL_TYPE_CONFIGS[protocol_type_enum]
        click.echo(f"Operations to implement ({len(config.operations)} total):")
        for op in config.operations:
            click.echo(f"  - {op}()")

        click.echo()
        click.echo(f"Event types to parse ({len(config.event_types)} total):")
        for event in config.event_types:
            click.echo(f"  - {event}")

    except Exception as e:
        click.echo(f"Error creating connector: {e}", err=True)
        # Clean up on failure
        if protocol_dir.exists():
            import shutil

            shutil.rmtree(protocol_dir)
        raise click.Abort() from e


if __name__ == "__main__":
    new_protocol()
