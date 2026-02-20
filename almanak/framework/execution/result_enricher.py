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
from typing import TYPE_CHECKING, Any

from .extracted_data import LPCloseData, SwapAmounts
from .receipt_registry import ReceiptParserRegistry

if TYPE_CHECKING:
    from .orchestrator import ExecutionContext, ExecutionResult

logger = logging.getLogger(__name__)


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
        # === Lending ===
        "BORROW": ["borrow_amount", "borrow_rate", "debt_token"],
        "REPAY": ["repay_amount", "remaining_debt"],
        "SUPPLY": ["supply_amount", "a_token_received", "supply_rate"],
        "WITHDRAW": ["withdraw_amount", "a_token_burned"],
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
            return result

        # Get intent type
        intent_type = self._get_intent_type(intent)
        if intent_type not in self.EXTRACTION_SPECS:
            logger.debug(f"No extraction spec for intent type: {intent_type}")
            return result

        # Get extraction spec
        spec = self.EXTRACTION_SPECS[intent_type]
        if not spec:
            return result  # No fields to extract (e.g., HOLD)

        # Get protocol from intent, falling back to context (intent may be frozen with protocol=None)
        protocol = self._get_protocol(intent) or getattr(context, "protocol", None)
        if not protocol:
            logger.debug(f"No protocol specified on intent or context: {intent_type}")
            return result

        # Get parser for protocol
        try:
            parser = self.parser_registry.get(protocol, chain=context.chain)
        except ValueError as e:
            warning = f"Parser not found for {protocol}: {e}"
            logger.info(warning)
            result.extraction_warnings.append(warning)
            return result

        # Collect receipts from successful transactions
        receipts = self._collect_receipts(result)
        if not receipts:
            logger.debug("No receipts to extract from")
            return result

        # Extract each field in the spec
        for field in spec:
            self._extract_field(result, parser, receipts, field, intent_type)

        # Log enrichment summary
        extracted_fields = [f for f in spec if self._has_extracted(result, f)]
        if extracted_fields:
            logger.info(
                f"Enriched {intent_type} result with: {', '.join(extracted_fields)} "
                f"(protocol={protocol}, chain={context.chain})"
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
            return

        extract_method = getattr(parser, method_name)

        # Try each receipt until we find the data
        for receipt in receipts:
            try:
                value = extract_method(receipt)
                if value is not None:
                    self._attach_to_result(result, field, value, intent_type)
                    return  # Found it, stop looking
            except Exception as e:
                warning = f"Failed to extract {field}: {e}"
                logger.info(warning)
                result.extraction_warnings.append(warning)

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
        if field == "position_id" and isinstance(value, int):
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
                # Receipt object with logs attribute
                receipt_dict = {"logs": receipt.logs}
            elif isinstance(receipt, dict):
                receipt_dict = receipt
            else:
                continue  # Unknown format

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
