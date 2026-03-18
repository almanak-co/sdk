"""Receipt Parser Registry.

This module provides a centralized registry for protocol receipt parsers,
enabling automatic parser lookup based on protocol name.

The registry uses lazy loading to avoid circular imports and reduce startup time.
Parser classes are imported and instantiated only when requested.

Example:
    from almanak.framework.execution.receipt_registry import get_parser, ReceiptParserRegistry

    # Get a parser for a protocol
    parser = get_parser("spark")
    result = parser.parse_receipt(receipt)

    # Use the registry class for more control
    registry = ReceiptParserRegistry()
    registry.register("custom", MyCustomParser)
    parser = registry.get("custom")

Registered Protocols:
    DEX/AMM:
    - uniswap_v3: UniswapV3ReceiptParser
    - uniswap_v4: UniswapV4ReceiptParser (V4 PoolManager singleton)
    - pancakeswap_v3: PancakeSwapV3ReceiptParser (Uniswap V3 fork on BSC)
    - sushiswap_v3: SushiSwapV3ReceiptParser (Uniswap V3 fork)
    - aerodrome: AerodromeReceiptParser (Base DEX)
    - traderjoe_v2: TraderJoeV2ReceiptParser (Avalanche DEX)
    - curve: CurveReceiptParser (Stablecoin DEX)

    Lending:
    - aave_v3: AaveV3ReceiptParser
    - spark: SparkReceiptParser (Aave V3 fork for DAI)
    - morpho_blue / morpho: MorphoBlueReceiptParser
    - compound_v3: CompoundV3ReceiptParser
    - benqi: BenqiReceiptParser (Compound V2 fork on Avalanche)

    Perpetuals:
    - gmx_v2: GMXv2ReceiptParser (Arbitrum perps)

    Staking:
    - lido: LidoReceiptParser (ETH liquid staking)
    - ethena: EthenaReceiptParser (USDe/sUSDe yield)

    Aggregators:
    - enso: EnsoReceiptParser (Enso intent-based routing)

    Solana Lending:
    - kamino: KaminoReceiptParser (Kamino Finance lending)

    Solana LP:
    - raydium_clmm / raydium: RaydiumReceiptParser (Raydium CLMM LP)

    Yield / Structured Products:
    - pendle: PendleReceiptParser (Pendle yield trading)

    Prediction Markets:
    - polymarket: PolymarketReceiptParser (Polygon prediction markets)

    Vault Protocols:
    - lagoon: LagoonReceiptParser (Lagoon ERC-7540 vault settlements)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# =============================================================================
# Protocol for Receipt Parsers
# =============================================================================


class ReceiptParser(Protocol):
    """Protocol defining the receipt parser interface.

    All receipt parsers must implement this interface to be compatible
    with the registry.
    """

    def parse_receipt(self, receipt: dict[str, Any]) -> Any:
        """Parse a transaction receipt.

        Args:
            receipt: Transaction receipt dict containing 'logs', 'transactionHash',
                     'blockNumber', etc.

        Returns:
            ParseResult with extracted events and data (protocol-specific type)
        """
        ...


# =============================================================================
# Receipt Parser Registry
# =============================================================================


class ReceiptParserRegistry:
    """Registry for protocol receipt parsers.

    The registry provides lazy loading of parser classes and supports
    both built-in parsers and custom parser registration.

    Built-in parsers are loaded from the connectors package when first
    requested. Custom parsers can be registered at any time.

    Example:
        registry = ReceiptParserRegistry()

        # Get a built-in parser
        spark_parser = registry.get("spark")

        # Register a custom parser
        registry.register("my_protocol", MyProtocolReceiptParser)

        # Check available protocols
        protocols = registry.list_protocols()
    """

    # Mapping of protocol names to lazy loader functions
    _BUILTIN_LOADERS: dict[str, tuple[str, str]] = {
        # Format: protocol_name -> (module_path, class_name)
        # DEX / AMM Protocols
        "uniswap_v3": (
            "almanak.framework.connectors.uniswap_v3.receipt_parser",
            "UniswapV3ReceiptParser",
        ),
        "uniswap_v4": (
            "almanak.framework.connectors.uniswap_v4.receipt_parser",
            "UniswapV4ReceiptParser",
        ),
        "pancakeswap_v3": (
            "almanak.framework.connectors.pancakeswap_v3.receipt_parser",
            "PancakeSwapV3ReceiptParser",
        ),
        "aerodrome": (
            "almanak.framework.connectors.aerodrome.receipt_parser",
            "AerodromeReceiptParser",
        ),
        "traderjoe_v2": (
            "almanak.framework.connectors.traderjoe_v2.receipt_parser",
            "TraderJoeV2ReceiptParser",
        ),
        "sushiswap_v3": (
            "almanak.framework.connectors.sushiswap_v3.receipt_parser",
            "SushiSwapV3ReceiptParser",
        ),
        "curve": (
            "almanak.framework.connectors.curve.receipt_parser",
            "CurveReceiptParser",
        ),
        # Lending Protocols
        "aave_v3": (
            "almanak.framework.connectors.aave_v3.receipt_parser",
            "AaveV3ReceiptParser",
        ),
        "spark": (
            "almanak.framework.connectors.spark.receipt_parser",
            "SparkReceiptParser",
        ),
        "morpho_blue": (
            "almanak.framework.connectors.morpho_blue.receipt_parser",
            "MorphoBlueReceiptParser",
        ),
        "morpho": (
            "almanak.framework.connectors.morpho_blue.receipt_parser",
            "MorphoBlueReceiptParser",
        ),  # Alias for morpho_blue
        "compound_v3": (
            "almanak.framework.connectors.compound_v3.receipt_parser",
            "CompoundV3ReceiptParser",
        ),
        "benqi": (
            "almanak.framework.connectors.benqi.receipt_parser",
            "BenqiReceiptParser",
        ),
        # Perpetuals
        "drift": (
            "almanak.framework.connectors.drift.receipt_parser",
            "DriftReceiptParser",
        ),
        "gmx_v2": (
            "almanak.framework.connectors.gmx_v2.receipt_parser",
            "GMXv2ReceiptParser",
        ),
        # Staking Protocols
        "lido": (
            "almanak.framework.connectors.lido.receipt_parser",
            "LidoReceiptParser",
        ),
        "ethena": (
            "almanak.framework.connectors.ethena.receipt_parser",
            "EthenaReceiptParser",
        ),
        # Aggregators
        "enso": (
            "almanak.framework.connectors.enso.receipt_parser",
            "EnsoReceiptParser",
        ),
        "lifi": (
            "almanak.framework.connectors.lifi.receipt_parser",
            "LiFiReceiptParser",
        ),
        # Yield / Structured Products
        "pendle": (
            "almanak.framework.connectors.pendle.receipt_parser",
            "PendleReceiptParser",
        ),
        # Prediction Markets
        "polymarket": (
            "almanak.framework.connectors.polymarket.receipt_parser",
            "PolymarketReceiptParser",
        ),
        # Vault Protocols
        "lagoon": (
            "almanak.framework.connectors.lagoon.receipt_parser",
            "LagoonReceiptParser",
        ),
        "metamorpho": (
            "almanak.framework.connectors.morpho_vault.receipt_parser",
            "MetaMorphoReceiptParser",
        ),
        # Solana Aggregators
        "jupiter": (
            "almanak.framework.connectors.jupiter.receipt_parser",
            "JupiterReceiptParser",
        ),
        # Solana Lending
        "kamino": (
            "almanak.framework.connectors.kamino.receipt_parser",
            "KaminoReceiptParser",
        ),
        "kamino_klend": (
            "almanak.framework.connectors.kamino.receipt_parser",
            "KaminoReceiptParser",
        ),  # Alias for kamino
        "jupiter_lend": (
            "almanak.framework.connectors.jupiter_lend.receipt_parser",
            "JupiterLendReceiptParser",
        ),
        # Solana LP
        "raydium_clmm": (
            "almanak.framework.connectors.raydium.receipt_parser",
            "RaydiumReceiptParser",
        ),
        "raydium": (
            "almanak.framework.connectors.raydium.receipt_parser",
            "RaydiumReceiptParser",
        ),  # Alias for raydium_clmm
        # Solana DLMM LP
        "meteora_dlmm": (
            "almanak.framework.connectors.meteora.receipt_parser",
            "MeteoraReceiptParser",
        ),
        "meteora": (
            "almanak.framework.connectors.meteora.receipt_parser",
            "MeteoraReceiptParser",
        ),  # Alias for meteora_dlmm
        # Solana CLMM LP (Orca Whirlpools)
        "orca_whirlpools": (
            "almanak.framework.connectors.orca.receipt_parser",
            "OrcaReceiptParser",
        ),
        "orca": (
            "almanak.framework.connectors.orca.receipt_parser",
            "OrcaReceiptParser",
        ),  # Alias for orca_whirlpools
    }

    def __init__(self) -> None:
        """Initialize the registry."""
        # Cache for instantiated parsers
        self._parsers: dict[str, ReceiptParser] = {}
        # Custom registered parser classes
        self._custom_classes: dict[str, type[ReceiptParser]] = {}

    def get(
        self,
        protocol: str,
        **kwargs: Any,
    ) -> ReceiptParser:
        """Get a receipt parser for a protocol.

        Args:
            protocol: Protocol name (e.g., "spark", "lido", "ethena", "pancakeswap_v3")
            **kwargs: Additional arguments to pass to parser constructor

        Returns:
            Receipt parser instance

        Raises:
            ValueError: If protocol is not registered
        """
        # Normalize protocol name (resolve aliases like "agni" -> "uniswap_v3")
        from almanak.framework.connectors.protocol_aliases import normalize_protocol

        protocol_lower = normalize_protocol(kwargs.get("chain", ""), protocol)

        # Check cache first (only for parsers without custom kwargs)
        if not kwargs and protocol_lower in self._parsers:
            return self._parsers[protocol_lower]

        # Try custom registered parser
        if protocol_lower in self._custom_classes:
            parser = self._custom_classes[protocol_lower](**kwargs)
            if not kwargs:
                self._parsers[protocol_lower] = parser
            return parser

        # Try built-in parser
        if protocol_lower in self._BUILTIN_LOADERS:
            parser = self._load_builtin(protocol_lower, **kwargs)
            if not kwargs:
                self._parsers[protocol_lower] = parser
            return parser

        raise ValueError(f"Unknown protocol: {protocol}. Available protocols: {', '.join(self.list_protocols())}")

    def register(
        self,
        protocol: str,
        parser_class: type[ReceiptParser],
    ) -> None:
        """Register a custom receipt parser.

        Args:
            protocol: Protocol name
            parser_class: Parser class (not instance)

        Raises:
            TypeError: If parser_class is not a class
        """
        if not isinstance(parser_class, type):
            raise TypeError(
                f"Expected a class, got {type(parser_class).__name__}. "
                "Use parser_class=MyParser, not parser_class=MyParser()"
            )

        protocol_lower = protocol.lower()
        self._custom_classes[protocol_lower] = parser_class

        # Clear cached instance if exists
        if protocol_lower in self._parsers:
            del self._parsers[protocol_lower]

        logger.debug(f"Registered custom receipt parser for protocol: {protocol}")

    def unregister(self, protocol: str) -> bool:
        """Unregister a custom receipt parser.

        Args:
            protocol: Protocol name

        Returns:
            True if parser was unregistered, False if not found
        """
        protocol_lower = protocol.lower()
        removed = protocol_lower in self._custom_classes

        if protocol_lower in self._custom_classes:
            del self._custom_classes[protocol_lower]

        if protocol_lower in self._parsers:
            del self._parsers[protocol_lower]

        return removed

    def list_protocols(self) -> list[str]:
        """List all available protocol names.

        Returns:
            List of registered protocol names
        """
        protocols = set(self._BUILTIN_LOADERS.keys())
        protocols.update(self._custom_classes.keys())
        return sorted(protocols)

    def is_registered(self, protocol: str) -> bool:
        """Check if a protocol is registered.

        Args:
            protocol: Protocol name

        Returns:
            True if protocol is registered
        """
        protocol_lower = protocol.lower()
        return protocol_lower in self._BUILTIN_LOADERS or protocol_lower in self._custom_classes

    def clear_cache(self) -> None:
        """Clear the parser instance cache.

        Useful for testing or when parser configuration changes.
        """
        self._parsers.clear()

    def _load_builtin(self, protocol: str, **kwargs: Any) -> ReceiptParser:
        """Load a built-in parser class.

        Args:
            protocol: Protocol name (must be in _BUILTIN_LOADERS)
            **kwargs: Arguments to pass to parser constructor

        Returns:
            Parser instance
        """
        import importlib

        module_path, class_name = self._BUILTIN_LOADERS[protocol]

        try:
            module = importlib.import_module(module_path)
            parser_class = getattr(module, class_name)
            return parser_class(**kwargs)
        except ImportError as e:
            raise ValueError(f"Failed to import parser for protocol {protocol}: {e}") from e
        except AttributeError as e:
            raise ValueError(f"Parser class {class_name} not found in {module_path}: {e}") from e


# =============================================================================
# Module-Level Convenience Functions
# =============================================================================

# Global registry instance
_default_registry = ReceiptParserRegistry()


def get_parser(
    protocol: str,
    **kwargs: Any,
) -> ReceiptParser:
    """Get a receipt parser for a protocol.

    This is a convenience function that uses the default registry.

    Args:
        protocol: Protocol name (e.g., "spark", "lido", "ethena", "pancakeswap_v3")
        **kwargs: Additional arguments to pass to parser constructor

    Returns:
        Receipt parser instance

    Raises:
        ValueError: If protocol is not registered

    Example:
        parser = get_parser("spark")
        result = parser.parse_receipt(receipt)

        # With custom configuration
        parser = get_parser("spark", pool_addresses={"0x..."})
    """
    return _default_registry.get(protocol, **kwargs)


def register_parser(
    protocol: str,
    parser_class: type[ReceiptParser],
) -> None:
    """Register a custom receipt parser in the default registry.

    Args:
        protocol: Protocol name
        parser_class: Parser class (not instance)

    Example:
        register_parser("my_protocol", MyProtocolReceiptParser)
    """
    _default_registry.register(protocol, parser_class)


def list_parsers() -> list[str]:
    """List all available protocol names in the default registry.

    Returns:
        List of registered protocol names

    Example:
        protocols = list_parsers()
        # ['ethena', 'lido', 'pancakeswap_v3', 'spark']
    """
    return _default_registry.list_protocols()


def is_parser_available(protocol: str) -> bool:
    """Check if a parser is available for a protocol.

    Args:
        protocol: Protocol name

    Returns:
        True if parser is available
    """
    return _default_registry.is_registered(protocol)


def extract_position_id(
    result: Any,
    protocol: str,
    chain: str | None = None,
) -> int | None:
    """Extract LP position ID from an execution result.

    This is a high-level utility that automatically selects the right parser
    based on the protocol and extracts the position ID from transaction receipts.

    Supports protocols that create NFT positions:
        - uniswap_v3: Extracts tokenId from NonfungiblePositionManager Transfer events
        - pancakeswap_v3: Same as uniswap_v3 (Uniswap V3 fork)

    Args:
        result: ExecutionResult from orchestrator, or a dict with 'transaction_results'
                or 'logs' field, or a raw receipt dict
        protocol: Protocol name (e.g., "uniswap_v3", "pancakeswap_v3")
        chain: Chain name for protocol-specific address lookups. Required for
               correct behavior; omitting it defaults to "arbitrum" with a warning.

    Returns:
        Position ID (NFT tokenId) if found, None otherwise

    Example:
        # From ExecutionResult (in on_intent_executed callback)
        def on_intent_executed(self, intent, success, result):
            if success and intent.intent_type.value == "LP_OPEN":
                position_id = extract_position_id(result, protocol="uniswap_v3", chain="arbitrum")
                if position_id:
                    self.current_position_id = position_id

        # From raw receipt
        position_id = extract_position_id(receipt, protocol="uniswap_v3", chain="base")
    """
    if chain is None:
        logger.warning(
            "extract_position_id() called without explicit chain parameter, "
            "defaulting to 'arbitrum'. Pass chain= explicitly to silence this warning."
        )
        chain = "arbitrum"
    try:
        # Get the parser
        parser = get_parser(protocol, chain=chain)

        # Check if parser supports position ID extraction
        if not hasattr(parser, "extract_position_id"):
            logger.warning(f"Parser for {protocol} does not support position ID extraction")
            return None

        # Handle different input types
        receipts_to_check: list[dict[str, Any]] = []

        # Case 1: ExecutionResult with transaction_results
        if hasattr(result, "transaction_results"):
            for tx_result in result.transaction_results:
                if tx_result.success and tx_result.receipt:
                    # Convert receipt to dict if needed
                    receipt = tx_result.receipt
                    if hasattr(receipt, "to_dict"):
                        receipt = receipt.to_dict()
                    elif hasattr(receipt, "logs"):
                        # Receipt object with logs attribute
                        receipt = {"logs": receipt.logs}
                    receipts_to_check.append(receipt)

        # Case 2: Dict with transaction_results
        elif isinstance(result, dict) and "transaction_results" in result:
            for tx_result in result["transaction_results"]:
                if tx_result.get("success") and tx_result.get("receipt"):
                    receipts_to_check.append(tx_result["receipt"])

        # Case 3: Dict with logs (raw receipt)
        elif isinstance(result, dict) and "logs" in result:
            receipts_to_check.append(result)

        # Case 4: List of logs directly
        elif isinstance(result, list):
            receipts_to_check.append({"logs": result})

        # Try to extract position ID from each receipt
        for receipt in receipts_to_check:
            position_id = parser.extract_position_id(receipt)
            if position_id is not None:
                return position_id

        return None

    except ValueError as e:
        # Parser not found
        logger.warning(f"Cannot extract position ID: {e}")
        return None
    except Exception as e:
        logger.warning(f"Failed to extract position ID: {e}")
        return None


# =============================================================================
# Error Classes
# =============================================================================


class ReceiptParserError(Exception):
    """Base exception for receipt parser errors."""

    pass


class ParserNotFoundError(ReceiptParserError):
    """Raised when a parser is not found for a protocol."""

    def __init__(self, protocol: str, available: list[str]) -> None:
        self.protocol = protocol
        self.available = available
        super().__init__(f"No receipt parser found for protocol: {protocol}. Available: {', '.join(available)}")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Registry class
    "ReceiptParserRegistry",
    # Protocol type
    "ReceiptParser",
    # Convenience functions
    "get_parser",
    "register_parser",
    "list_parsers",
    "is_parser_available",
    "extract_position_id",
    # Exceptions
    "ReceiptParserError",
    "ParserNotFoundError",
]
