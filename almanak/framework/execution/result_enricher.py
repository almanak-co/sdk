"""Result Enricher for Automatic Receipt Parsing.

This module implements the ResultEnricher component that automatically extracts
intent-specific data from transaction receipts and attaches it to ExecutionResult.

The design follows "Framework Orchestrates, Protocols Execute":
- The framework (ResultEnricher) determines WHAT to extract based on intent type
- The protocols (ReceiptParsers) determine HOW to extract the data

This enables strategy authors to access extracted data directly via:
    result.position_id  # Instead of manual parsing

Example:
    enricher = ResultEnricher(parser_registry)
    enriched_result = enricher.enrich(result, intent, context)

    # Strategy can now use:
    if enriched_result.position_id:
        track_position(enriched_result.position_id)
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from .extracted_data import LPCloseData, SwapAmounts
from .receipt_registry import ReceiptParserRegistry

if TYPE_CHECKING:
    from .orchestrator import ExecutionContext, ExecutionResult

logger = logging.getLogger(__name__)

# Mapping from TransactionReceipt.to_dict() snake_case keys to web3-style camelCase keys.
# All receipt parsers expect camelCase (transactionHash, gasUsed, blockNumber).
_SNAKE_TO_CAMEL = {
    "tx_hash": "transactionHash",
    "gas_used": "gasUsed",
    "block_number": "blockNumber",
    "block_hash": "blockHash",
    "from_address": "from",
    "to_address": "to",
    "contract_address": "contractAddress",
    "effective_gas_price": "effectiveGasPrice",
}


class ResultEnricher:
    """Enriches ExecutionResult with intent-specific extracted data.

    This component implements the "Framework Orchestrates, Protocols Execute"
    pattern. It determines WHAT to extract based on intent type, and delegates
    HOW to extract to protocol-specific parsers.

    Key Design Principles:
    1. Fail-Safe: Extraction errors are logged but never crash execution
    2. Type-Safe: Core fields are strongly typed
    3. Extensible: New protocols can be added without framework changes
    4. Zero Cognitive Load: Data "just appears" on result

    Example:
        enricher = ResultEnricher()

        # In StrategyRunner after execution:
        result = await orchestrator.execute(bundle)
        if result.success:
            result = enricher.enrich(result, intent, context)

        # Strategy callback receives enriched result:
        strategy.on_intent_executed(intent, success=True, result=result)
        # Strategy can use result.position_id directly!
    """

    # Extraction specifications per intent type
    # Maps intent type to list of fields to extract
    EXTRACTION_SPECS: dict[str, list[str]] = {
        # === DEX / AMM ===
        "SWAP": ["swap_amounts"],
        # === Liquidity Providing ===
        "LP_OPEN": ["position_id", "tick_lower", "tick_upper", "liquidity", "bin_ids"],
        "LP_CLOSE": [
            "lp_close_data",
            "amount0_collected",
            "amount1_collected",
            "fees0",
            "fees1",
        ],
        # === LP Fee Collection ===
        "LP_COLLECT_FEES": ["fees0", "fees1", "bin_ids"],
        # === Lending ===
        # Singular forms used by EVM parsers (Aave, Morpho, etc.)
        # Plural forms used by Solana parsers (Jupiter Lend, Kamino)
        "BORROW": ["borrow_amount", "borrow_amounts", "borrow_rate", "debt_token"],
        "REPAY": ["repay_amount", "repay_amounts", "remaining_debt"],
        "SUPPLY": ["supply_amount", "supply_amounts", "a_token_received", "supply_rate"],
        "WITHDRAW": ["withdraw_amount", "withdraw_amounts", "a_token_burned"],
        # === Perpetuals ===
        "PERP_OPEN": [
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            "leverage",
        ],
        "PERP_CLOSE": [
            "realized_pnl",
            "exit_price",
            "fees_paid",
            "collateral_returned",
        ],
        # === Staking ===
        "STAKE": ["stake_amount", "shares_received", "stake_token"],
        "UNSTAKE": ["unstake_amount", "underlying_received", "cooldown_end"],
        # === Flash Loans ===
        "FLASH_LOAN": ["loan_amount", "fee_paid", "loan_token"],
        # === Prediction Markets ===
        "PREDICTION_BUY": ["outcome_tokens_received", "cost_basis", "market_id"],
        "PREDICTION_SELL": ["outcome_tokens_sold", "proceeds", "market_id"],
        "PREDICTION_REDEEM": ["redemption_amount", "payout", "market_id"],
        # === Cross-Chain ===
        "BRIDGE": [
            "source_tx_hash",
            "bridge_id",
            "destination_chain",
            "expected_amount",
        ],
        "ENSURE_BALANCE": ["amount_transferred", "source_chain"],
        # === Vault Operations (MetaMorpho ERC-4626) ===
        "VAULT_DEPOSIT": ["deposit_data"],
        "VAULT_REDEEM": ["redeem_data"],
        # === No-Op ===
        "HOLD": [],  # No extraction needed
    }

    def __init__(self, parser_registry: ReceiptParserRegistry | None = None) -> None:
        """Initialize the ResultEnricher.

        Args:
            parser_registry: Registry for protocol parsers.
                If not provided, uses the default global registry.
        """
        self.parser_registry = parser_registry or ReceiptParserRegistry()

    def enrich(
        self,
        result: ExecutionResult,
        intent: Any,
        context: ExecutionContext,
    ) -> ExecutionResult:
        """Enrich execution result with intent-specific extracted data.

        This method extracts relevant data from transaction receipts based
        on the intent type and attaches it to the ExecutionResult.

        IMPORTANT: This method NEVER raises exceptions. All errors are
        logged as warnings and added to result.extraction_warnings.

        Args:
            result: Raw execution result from orchestrator
            intent: The intent that was executed
            context: Execution context with chain info

        Returns:
            Enriched ExecutionResult (same instance, mutated)

        Example:
            result = enricher.enrich(result, intent, context)
            # result.position_id is now populated (if LP_OPEN)
            # result.swap_amounts is now populated (if SWAP)
        """
        # Don't enrich failed executions
        if not result.success:
            logger.debug("Enrichment skipped: execution failed")
            return result

        # Get intent type
        intent_type = self._get_intent_type(intent)
        if intent_type not in self.EXTRACTION_SPECS:
            logger.debug(f"Enrichment skipped: no extraction spec for intent type '{intent_type}'")
            return result

        # Get extraction spec
        spec = self.EXTRACTION_SPECS[intent_type]
        if not spec:
            return result  # No fields to extract (e.g., HOLD)

        # Get protocol from intent, falling back to context (intent may be frozen with protocol=None)
        intent_protocol = self._get_protocol(intent)
        context_protocol = getattr(context, "protocol", None)
        protocol = intent_protocol or context_protocol
        if not protocol:
            logger.debug(f"Enrichment skipped: protocol=None on both intent and context (intent_type={intent_type})")
            return result
        logger.debug(
            f"Enrichment: intent_type={intent_type}, protocol={protocol} "
            f"(from={'intent' if intent_protocol else 'context'}), "
            f"chain={context.chain}, fields={spec}"
        )

        # Get parser for protocol
        try:
            parser = self.parser_registry.get(protocol, chain=context.chain)
        except ValueError as e:
            warning = f"Parser not found for {protocol}: {e}"
            logger.info(warning)
            result.extraction_warnings.append(warning)
            return result
        logger.debug(f"Enrichment: using parser {type(parser).__name__} for protocol={protocol}")

        # Collect receipts from successful transactions
        receipts = self._collect_receipts(result)
        if not receipts:
            logger.debug(
                f"Enrichment skipped: no receipts in execution result (intent_type={intent_type}, protocol={protocol})"
            )
            return result
        logger.debug(f"Enrichment: found {len(receipts)} receipt(s) to process")

        # Install a temporary parse_receipt cache to avoid redundant parsing.
        # Without this, each extract_* method calls parse_receipt() independently,
        # meaning the same receipt is parsed N times for N extraction fields
        # (e.g., 5x for PERP_OPEN with position_id, size_delta, collateral, entry_price, leverage).
        self._install_parse_cache(parser)
        try:
            # Extract each field in the spec
            for field in spec:
                self._extract_field(result, parser, receipts, field, intent_type)
        finally:
            self._remove_parse_cache(parser)

        # Log enrichment summary with actual extracted values
        extracted_parts = []
        missing_fields = []
        for f in spec:
            if self._has_extracted(result, f):
                val = self._get_extracted_value(result, f)
                extracted_parts.append(f"{f}={val}")
            else:
                missing_fields.append(f)
        if extracted_parts:
            logger.info(
                f"Enriched {intent_type} result: {', '.join(extracted_parts)} "
                f"(protocol={protocol}, chain={context.chain})"
            )
        if missing_fields:
            logger.debug(
                f"Enrichment: fields not extracted for {intent_type}: {', '.join(missing_fields)} "
                f"(protocol={protocol}, parser={type(parser).__name__})"
            )

        return result

    def _extract_field(
        self,
        result: ExecutionResult,
        parser: Any,
        receipts: list[dict[str, Any]],
        field: str,
        intent_type: str,
    ) -> None:
        """Extract a single field from receipts and attach to result.

        Args:
            result: ExecutionResult to populate
            parser: Protocol receipt parser
            receipts: List of transaction receipts
            field: Field name to extract
            intent_type: Type of intent being processed
        """
        method_name = f"extract_{field}"

        # Check capability declaration if parser declares SUPPORTED_EXTRACTIONS
        supported = getattr(parser, "SUPPORTED_EXTRACTIONS", None)
        if isinstance(supported, list | tuple | set | frozenset) and field not in supported:
            warning = (
                f"Parser {type(parser).__name__} does not declare support for '{field}' (expected by {intent_type})"
            )
            logger.info(warning)
            result.extraction_warnings.append(warning)
            return

        # Check if parser has this extraction method
        if not hasattr(parser, method_name):
            logger.debug(
                f"Enrichment: parser {type(parser).__name__} has no method '{method_name}' "
                f"(field={field}, intent_type={intent_type})"
            )
            return

        extract_method = getattr(parser, method_name)

        # Try each receipt until we find the data
        for receipt in receipts:
            try:
                value = extract_method(receipt)
                if value is not None:
                    self._attach_to_result(result, field, value, intent_type)
                    logger.debug(f"Enrichment: extracted {field}={type(value).__name__} from receipt")
                    return  # Found it, stop looking
            except Exception as e:
                warning = f"Failed to extract {field}: {e}"
                logger.info(warning)
                result.extraction_warnings.append(warning)

        # If we get here, no receipt yielded a value
        logger.debug(
            f"Enrichment: {field} returned None from all {len(receipts)} receipt(s) "
            f"(parser={type(parser).__name__}, intent_type={intent_type})"
        )

    def _attach_to_result(
        self,
        result: ExecutionResult,
        field: str,
        value: Any,
        intent_type: str,
    ) -> None:
        """Attach extracted value to appropriate result field.

        Core typed fields are set directly on the result.
        All values are also added to extracted_data dict.

        Args:
            result: ExecutionResult to populate
            field: Field name
            value: Extracted value
            intent_type: Type of intent
        """
        # Core typed fields - set directly on result
        if field == "position_id" and isinstance(value, int | str):
            if isinstance(value, str):
                # Accept hex addresses (e.g. Curve LP token addresses) as valid position IDs
                is_hex_address = bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", value))
                if not is_hex_address:
                    try:
                        parsed = Decimal(value)
                        if not parsed.is_finite():
                            logger.warning(f"Ignoring non-finite string position_id {value!r}")
                            result.extracted_data[field] = value
                            return
                    except InvalidOperation:
                        logger.warning(f"Ignoring invalid string position_id {value!r}: not a valid decimal or address")
                        result.extracted_data[field] = value
                        return
            result.position_id = value
        elif field == "swap_amounts" and isinstance(value, SwapAmounts):
            result.swap_amounts = value
        elif field == "lp_close_data" and isinstance(value, LPCloseData):
            result.lp_close_data = value

        # Always add to extracted_data for full access
        result.extracted_data[field] = value

    def _has_extracted(self, result: ExecutionResult, field: str) -> bool:
        """Check if a field was successfully extracted.

        Args:
            result: ExecutionResult to check
            field: Field name

        Returns:
            True if field was extracted
        """
        # Check core fields
        if field == "position_id":
            return result.position_id is not None
        if field == "swap_amounts":
            return result.swap_amounts is not None
        if field == "lp_close_data":
            return result.lp_close_data is not None

        # Check extracted_data
        return field in result.extracted_data

    def _get_extracted_value(self, result: ExecutionResult, field: str) -> Any:
        """Get the extracted value for a field, formatted for logging.

        Args:
            result: ExecutionResult to read
            field: Field name

        Returns:
            The extracted value (summarized for complex types)
        """
        if field == "position_id":
            return result.position_id
        if field == "swap_amounts" and result.swap_amounts:
            sa = result.swap_amounts
            return f"{sa.amount_in_decimal} -> {sa.amount_out_decimal}"
        if field == "lp_close_data" and result.lp_close_data:
            return f"amount0={result.lp_close_data.amount0_collected}, amount1={result.lp_close_data.amount1_collected}"
        val = result.extracted_data.get(field)
        return str(val)[:100] if val is not None else val

    def _get_intent_type(self, intent: Any) -> str:
        """Get intent type string from intent object.

        Args:
            intent: Intent object

        Returns:
            Intent type string (e.g., "SWAP", "LP_OPEN")
        """
        # Try intent_type attribute (IntentType enum)
        if hasattr(intent, "intent_type"):
            intent_type = intent.intent_type
            # Handle enum
            if hasattr(intent_type, "value"):
                return str(intent_type.value).upper()
            return str(intent_type).upper()

        # Fallback: derive from class name (e.g., SwapIntent -> SWAP)
        class_name = type(intent).__name__
        if class_name.endswith("Intent"):
            class_name = class_name[:-6]  # Remove "Intent" suffix

        # Convert CamelCase to UPPER_SNAKE
        # LPOpen -> LP_OPEN, PerpClose -> PERP_CLOSE
        # Insert underscore only before capitals that start a new word (uppercase followed by lowercase)
        # This keeps acronyms like "LP" together instead of splitting to "L_P"
        normalized = re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", class_name)
        return normalized.upper()

    def _get_protocol(self, intent: Any) -> str | None:
        """Get protocol from intent.

        Args:
            intent: Intent object

        Returns:
            Protocol name or None
        """
        return getattr(intent, "protocol", None)

    @staticmethod
    def _install_parse_cache(parser: Any) -> None:
        """Install a temporary cache on the parser's parse_receipt method.

        This wraps the parser's parse_receipt() so repeated calls with the same
        receipt return the cached result. The cache key is the receipt's
        transactionHash (or id() as fallback for receipts without a hash).

        This is critical for performance: PERP_OPEN enrichment calls 5
        extract_* methods, each internally calling parse_receipt(). Without
        caching, the same receipt is parsed 5x per TX.
        """
        if not hasattr(parser, "parse_receipt"):
            return

        original = parser.parse_receipt
        # Guard against double-wrapping (e.g., if enrich() is called recursively)
        if getattr(original, "_is_cached_wrapper", False):
            return

        cache: dict[str, Any] = {}

        def cached_parse_receipt(receipt: dict[str, Any]) -> Any:
            # Use transactionHash as key, fall back to id() for hashability
            tx_hash = receipt.get("transactionHash") or receipt.get("tx_hash")
            if tx_hash is None:
                tx_hash = id(receipt)
            key = str(tx_hash)
            if key not in cache:
                cache[key] = original(receipt)
            return cache[key]

        cached_parse_receipt._is_cached_wrapper = True  # type: ignore[attr-defined]
        cached_parse_receipt._original = original  # type: ignore[attr-defined]
        parser.parse_receipt = cached_parse_receipt

    @staticmethod
    def _remove_parse_cache(parser: Any) -> None:
        """Remove the temporary parse_receipt cache, restoring the original method."""
        current = getattr(parser, "parse_receipt", None)
        if current is not None and getattr(current, "_is_cached_wrapper", False):
            parser.parse_receipt = current._original

    def _collect_receipts(self, result: ExecutionResult) -> list[dict[str, Any]]:
        """Collect receipts from successful transaction results.

        Args:
            result: ExecutionResult containing transaction results

        Returns:
            List of receipt dicts
        """
        receipts: list[dict[str, Any]] = []

        for tx_result in result.transaction_results:
            if not tx_result.success:
                continue
            if not tx_result.receipt:
                continue

            receipt = tx_result.receipt

            # Convert to dict if needed
            receipt_dict: dict[str, Any]
            if hasattr(receipt, "to_dict"):
                receipt_dict = receipt.to_dict()
            elif hasattr(receipt, "logs"):
                # Receipt object with logs attribute — also propagate 'from' / 'from_address'
                # for Transfer-event-based decimal resolution in extract_swap_amounts.
                receipt_dict = {"logs": receipt.logs}
                for attr in ("from_address", "status"):
                    if hasattr(receipt, attr):
                        receipt_dict[attr] = getattr(receipt, attr)
            elif isinstance(receipt, dict):
                receipt_dict = receipt
            else:
                continue  # Unknown format

            # Add camelCase aliases so receipt parsers work regardless of
            # which key convention (snake_case vs camelCase) the receipt uses.
            for snake_key, camel_key in _SNAKE_TO_CAMEL.items():
                if snake_key in receipt_dict and camel_key not in receipt_dict:
                    receipt_dict[camel_key] = receipt_dict[snake_key]

            receipts.append(receipt_dict)

        return receipts


# =============================================================================
# Module-level singleton for convenience
# =============================================================================

_default_enricher: ResultEnricher | None = None


def get_enricher() -> ResultEnricher:
    """Get the default ResultEnricher instance.

    Returns:
        Singleton ResultEnricher instance
    """
    global _default_enricher
    if _default_enricher is None:
        _default_enricher = ResultEnricher()
    return _default_enricher


def enrich_result(
    result: ExecutionResult,
    intent: Any,
    context: ExecutionContext,
) -> ExecutionResult:
    """Enrich an execution result using the default enricher.

    Convenience function that uses the singleton ResultEnricher.

    Args:
        result: Raw execution result from orchestrator
        intent: The intent that was executed
        context: Execution context with chain info

    Returns:
        Enriched ExecutionResult

    Example:
        result = await orchestrator.execute(bundle)
        result = enrich_result(result, intent, context)
    """
    return get_enricher().enrich(result, intent, context)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "ResultEnricher",
    "get_enricher",
    "enrich_result",
]
