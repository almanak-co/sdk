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
import warnings
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from .extract_result import (
    CriticalAccountingError,
    ExtractError,
    ExtractMissing,
    ExtractOk,
)
from .extracted_data import LPCloseData, ProtocolFees, SwapAmounts
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

# One-shot deprecation tracking for un-migrated parsers. Keyed by
# (parser_class_name, field) so we warn exactly once per (parser, field) pair
# instead of spamming on every receipt.
_LEGACY_WARNED: set[tuple[str, str]] = set()


def _legacy_warn(parser: Any, field: str) -> None:
    """Emit a one-shot DeprecationWarning for parsers that still return raw values.

    VIB-3159 migrates receipt parsers to the three-variant ExtractResult
    contract. Parsers that still return raw values / None keep working via
    backward-compat wrapping, but callers cannot distinguish "no event" from
    "parse error" — which is the ghost-position failure mode this ticket
    closes. The warning identifies which parser still needs migration.
    """
    key = (type(parser).__name__, field)
    if key in _LEGACY_WARNED:
        return
    _LEGACY_WARNED.add(key)
    warnings.warn(
        f"Receipt parser {type(parser).__name__}.extract_{field}() returns a raw value "
        f"instead of ExtractOk/ExtractMissing/ExtractError (VIB-3159). Parse errors and "
        f"missing events are indistinguishable — migrate to the tagged variant.",
        DeprecationWarning,
        stacklevel=3,
    )


class ResultEnricher:
    """Enriches ExecutionResult with intent-specific extracted data.

    This component implements the "Framework Orchestrates, Protocols Execute"
    pattern. It determines WHAT to extract based on intent type, and delegates
    HOW to extract to protocol-specific parsers.

    Key Design Principles:
    1. Fail-Closed (live): Parse errors raise CriticalAccountingError so the
       runner cannot proceed on a stale / ghost view of on-chain state.
       Paper / backtest callers opt into permissive mode via live_mode=False,
       which downgrades ExtractError to a structured warning + counter.
       "No event of this type" results (ExtractMissing) are benign in both
       modes and never raise.
    2. Type-Safe: Core fields are strongly typed.
    3. Extensible: New protocols can be added without framework changes.
    4. Zero Cognitive Load: Data "just appears" on result.
    5. Three-variant contract: migrated parsers return ExtractOk /
       ExtractMissing / ExtractError so "no event" and "parse error" are
       distinguishable. Legacy parsers keep working via backward-compat
       wrapping (see _legacy_warn / _invoke_extract).

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
    #
    # VIB-3204: ``protocol_fees`` is added to every intent type that charges
    # protocol-level fees (DEX fees, origination fees, perp open/close fees,
    # vault fees). Parsers that don't implement the extractor simply yield
    # ``None`` and the field is skipped — no warning is emitted for missing
    # methods when the parser doesn't declare SUPPORTED_EXTRACTIONS.
    EXTRACTION_SPECS: dict[str, list[str]] = {
        # === DEX / AMM ===
        "SWAP": ["swap_amounts", "protocol_fees"],
        # === Liquidity Providing ===
        "LP_OPEN": ["position_id", "tick_lower", "tick_upper", "liquidity", "bin_ids", "protocol_fees"],
        "LP_CLOSE": [
            "lp_close_data",
            "amount0_collected",
            "amount1_collected",
            "fees0",
            "fees1",
            "protocol_fees",
        ],
        # === LP Fee Collection ===
        "LP_COLLECT_FEES": ["fees0", "fees1", "bin_ids", "protocol_fees"],
        # === Lending ===
        # Singular forms used by EVM parsers (Aave, Morpho, etc.)
        # Plural forms used by Solana parsers (Jupiter Lend, Kamino)
        "BORROW": ["borrow_amount", "borrow_amounts", "borrow_rate", "debt_token", "protocol_fees"],
        "REPAY": ["repay_amount", "repay_amounts", "remaining_debt", "protocol_fees"],
        "SUPPLY": ["supply_amount", "supply_amounts", "a_token_received", "supply_rate", "protocol_fees"],
        "WITHDRAW": ["withdraw_amount", "withdraw_amounts", "a_token_burned", "protocol_fees"],
        # === Perpetuals ===
        "PERP_OPEN": [
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            "leverage",
            "protocol_fees",
        ],
        "PERP_CLOSE": [
            "realized_pnl",
            "exit_price",
            "fees_paid",
            "collateral_returned",
            "protocol_fees",
        ],
        # === Staking ===
        "STAKE": ["stake_amount", "shares_received", "wsteth_received", "stake_token", "protocol_fees"],
        "UNSTAKE": ["unstake_amount", "underlying_received", "cooldown_end", "protocol_fees"],
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
        "VAULT_DEPOSIT": ["deposit_data", "protocol_fees"],
        "VAULT_REDEEM": ["redeem_data", "protocol_fees"],
        # === No-Op ===
        "HOLD": [],  # No extraction needed
    }

    def __init__(
        self,
        parser_registry: ReceiptParserRegistry | None = None,
        *,
        live_mode: bool = True,
    ) -> None:
        """Initialize the ResultEnricher.

        Args:
            parser_registry: Registry for protocol parsers. If not provided,
                uses the default global registry.
            live_mode: When True (default), an ExtractError from a parser
                is converted into CriticalAccountingError and raised —
                accounting failures must not be silently treated as "no
                event". When False (paper / backtest), the error is logged
                and counted on result.extraction_warnings but does not halt
                execution. Default True is a deliberate fail-closed choice —
                paper trading entry points must opt into permissive mode.
        """
        self.parser_registry = parser_registry or ReceiptParserRegistry()
        self.live_mode = live_mode
        # Counter for ExtractError occurrences in non-live mode. Exposed so
        # monitoring / paper engines can surface the total.
        self.extract_error_count: int = 0

    def enrich(
        self,
        result: ExecutionResult,
        intent: Any,
        context: ExecutionContext,
        *,
        bundle_metadata: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """Enrich execution result with intent-specific extracted data.

        This method extracts relevant data from transaction receipts based
        on the intent type and attaches it to the ExecutionResult.

        IMPORTANT (VIB-3159): In live mode this method FAILS CLOSED. Parsers
        that return ExtractError — or raise — cause CriticalAccountingError
        to propagate. Paper / backtest callers must construct the enricher
        with live_mode=False to downgrade those errors to warnings + a
        counter. Benign "no event of this type" results (ExtractMissing)
        never raise in either mode.

        Args:
            result: Raw execution result from orchestrator
            intent: The intent that was executed
            context: Execution context with chain info
            bundle_metadata: Optional ActionBundle.metadata dict from the
                compiler. Used to thread compiler-side quote data (e.g.,
                ``expected_output_human`` for VIB-3203 realized-slippage
                calculation) through to the extract_* methods.

        Returns:
            Enriched ExecutionResult (same instance, mutated)

        Raises:
            CriticalAccountingError: when live_mode is True and a parser
                returns ExtractError (or raises). This intentionally uses
                BaseException as its base so generic except-Exception
                handlers do not swallow it.

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

        # VIB-2581: Skip enrichment for Solana chains when no Solana-specific parser
        # exists. Without this guard, Solana TXs (with string instruction logs) get routed
        # to EVM parsers (expecting dict logs with 'topics'), producing 40+ warnings like
        # "Failed to parse log: 'str' object has no attribute 'get'".
        chain_str = str(getattr(context, "chain", "")).lower()
        is_solana = "solana" in chain_str

        # Get parser for protocol
        try:
            parser = self.parser_registry.get(protocol, chain=context.chain)
        except ValueError as e:
            warning = f"Parser not found for {protocol}: {e}"
            logger.info(warning)
            result.extraction_warnings.append(warning)
            return result

        # Guard: don't run EVM parsers on Solana receipts
        parser_name = type(parser).__name__.lower()
        solana_parsers = {
            "jupiterreceiptparser",
            "kaminoreceiptparser",
            "raydiumreceiptparser",
            "meteorareceiptparser",
            "orcareceiptparser",
            "jupiterlendreceiptparser",
        }
        if is_solana and parser_name not in solana_parsers:
            logger.debug(
                f"Enrichment skipped: EVM parser {type(parser).__name__} is not compatible "
                f"with Solana chain receipts (protocol={protocol})"
            )
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
                self._extract_field(
                    result, parser, receipts, field, intent_type, protocol, bundle_metadata=bundle_metadata
                )
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
        protocol: str | None = None,
        *,
        bundle_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Extract a single field from receipts and attach to result.

        Handles the three-variant ExtractResult contract (VIB-3159):
          * ExtractOk      -> attach to result
          * ExtractMissing -> no-op (benign "no event of this type")
          * ExtractError   -> raise CriticalAccountingError in live mode,
                              warn + count in paper mode

        Un-migrated parsers returning raw None / value are wrapped via
        _invoke_extract with a one-shot DeprecationWarning. This keeps
        the remaining ~32 parsers working until they are migrated (see
        docs/internal/vib-3159-followup.md).

        A raised exception from a parser is always treated as ExtractError.
        Under the legacy contract the error was logged and swallowed,
        producing the ghost-position failure mode this ticket addresses.

        Migrated parsers expose a second method ``extract_{field}_result``
        that returns the tagged ``ExtractResult``. We prefer it when present
        so existing raw-returning public methods keep their signatures for
        the strategies / tests that call them directly.
        """
        method_name = f"extract_{field}"
        result_method_name = f"{method_name}_result"

        # Check capability declaration if parser declares SUPPORTED_EXTRACTIONS
        supported = getattr(parser, "SUPPORTED_EXTRACTIONS", None)
        if isinstance(supported, list | tuple | set | frozenset) and field not in supported:
            warning = (
                f"Parser {type(parser).__name__} does not declare support for '{field}' (expected by {intent_type})"
            )
            logger.info(warning)
            result.extraction_warnings.append(warning)
            return

        # Prefer the migrated tagged-variant method when present. This lets
        # the raw public method keep its legacy return type for existing
        # callers (strategies, tests) while the enricher gets the richer
        # signal. We check the *class hierarchy* (not the instance) to avoid
        # matching auto-generated attributes on unittest.mock.Mock() which
        # would otherwise claim every ``extract_{field}_result`` exists.
        if self._class_has_method(parser, result_method_name):
            extract_method = getattr(parser, result_method_name)
        elif hasattr(parser, method_name):
            extract_method = getattr(parser, method_name)
        else:
            logger.debug(
                f"Enrichment: parser {type(parser).__name__} has no method '{method_name}' "
                f"(field={field}, intent_type={intent_type})"
            )
            return

        # Build field-specific extraction kwargs. VIB-3203: thread
        # ``expected_out`` (human Decimal) from the compiler's ActionBundle
        # metadata to swap_amounts extractors so parsers can compute realized
        # slippage_bps. Parsers that do not accept the kwarg degrade to the
        # legacy behavior (slippage_bps=None) via the TypeError fallback in
        # _invoke_extract.
        extract_kwargs = self._build_extract_kwargs(field, bundle_metadata)

        # Iterate receipts. Remember any ExtractError and keep looking — the
        # data might land in a later receipt (multi-tx bundle). Only escalate
        # if no receipt produced Ok.
        last_error: ExtractError | None = None
        for receipt in receipts:
            variant = self._invoke_extract(extract_method, parser, receipt, field, extract_kwargs)

            if isinstance(variant, ExtractOk):
                attached = self._attach_to_result(result, field, variant.value, intent_type)
                if attached:
                    logger.debug(f"Enrichment: extracted {field}={type(variant.value).__name__} from receipt")
                    return
                # Value rejected by type-check (see _attach_to_result). Keep
                # scanning subsequent receipts — a later one may produce
                # a valid value for this field.
                continue
            if isinstance(variant, ExtractError):
                last_error = variant
                continue
            # ExtractMissing — benign, continue to next receipt.

        if last_error is not None:
            self._handle_extract_error(result, last_error, field, intent_type, parser, protocol)
            return

        logger.debug(
            f"Enrichment: {field} missing from all {len(receipts)} receipt(s) "
            f"(parser={type(parser).__name__}, intent_type={intent_type})"
        )

    @staticmethod
    def _class_has_method(obj: Any, name: str) -> bool:
        """Return True if ``name`` is defined on ``type(obj)`` or a base class.

        Unlike ``hasattr(obj, name)`` this does not match attributes that
        were auto-generated on the instance (``unittest.mock.Mock`` in
        particular exposes every attribute lookup as a fresh Mock), so it
        is safe to use for "did the parser author actually implement the
        tagged variant?" checks.
        """
        return any(name in klass.__dict__ for klass in type(obj).__mro__)

    @staticmethod
    def _build_extract_kwargs(
        field: str,
        bundle_metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Compute per-field extra kwargs for ``extract_<field>`` methods.

        VIB-3203 — swap_amounts extractors can consume an ``expected_out``
        Decimal (human units) to compute realized ``slippage_bps`` from
        ``(expected_out - actual_out) / expected_out``. The value comes from
        the compiler's ``ActionBundle.metadata["expected_output_human"]``.

        VIB-3204 — protocol_fees extractors for DEX swap intents can consume
        a ``fee_tier_bps`` int so they can compute the swap fee without
        re-reading on-chain pool metadata. Sourced from
        ``ActionBundle.metadata["selected_fee_tier"]``.

        Returns a mapping that can be passed directly as ``**kwargs`` to
        the extract method. Parsers that do not accept the kwarg fall back
        to positional-only invocation via :meth:`_invoke_extract` (TypeError
        fallback).
        """
        if not bundle_metadata:
            return {}
        if field == "swap_amounts":
            raw = bundle_metadata.get("expected_output_human")
            if raw is None:
                return {}
            try:
                expected_out = Decimal(str(raw))
            except (InvalidOperation, TypeError, ValueError):
                logger.debug("Could not coerce expected_output_human=%r to Decimal; skipping", raw)
                return {}
            if not expected_out.is_finite() or expected_out <= 0:
                return {}
            return {"expected_out": expected_out}
        if field == "protocol_fees":
            raw_tier = bundle_metadata.get("selected_fee_tier")
            if raw_tier in (None, ""):
                return {}
            try:
                return {"fee_tier_bps": int(str(raw_tier))}
            except (TypeError, ValueError):
                logger.debug("Could not coerce selected_fee_tier=%r to int; skipping", raw_tier)
                return {}
        return {}

    def _invoke_extract(
        self,
        extract_method: Any,
        parser: Any,
        receipt: dict[str, Any],
        field: str,
        extract_kwargs: dict[str, Any] | None = None,
    ) -> ExtractOk[Any] | ExtractMissing | ExtractError:
        """Call an extract_* method and normalize the return to a variant.

        Migrated parsers already return ExtractOk/Missing/Error. Legacy
        parsers return raw None / value; we wrap those with a one-shot
        deprecation warning. Exceptions from either kind become
        ExtractError — a raised exception is always accounting-critical.

        ``extract_kwargs`` carry optional field-specific hints (e.g.,
        ``expected_out`` for swap_amounts — VIB-3203). Parsers that do not
        accept a given kwarg degrade to the legacy no-kwarg call via the
        TypeError fallback.
        """
        kwargs = extract_kwargs or {}
        try:
            if kwargs:
                try:
                    raw = extract_method(receipt, **kwargs)
                except TypeError as exc:
                    # Parser signature doesn't accept the kwarg (yet). This is an
                    # expected back-compat path — the ticket only wires 5 of the
                    # swap parsers in Phase A; the rest keep the legacy
                    # "slippage_bps=None" behavior. Distinguish this from a real
                    # crash by checking the exception message mentions the kwarg.
                    if any(k in str(exc) for k in kwargs):
                        raw = extract_method(receipt)
                    else:
                        raise
            else:
                raw = extract_method(receipt)
        except CriticalAccountingError:
            # Never swallow a fail-closed signal raised by a nested enricher.
            raise
        except Exception as exc:  # noqa: BLE001 — crash is accounting-critical
            return ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc)

        if isinstance(raw, ExtractOk | ExtractMissing | ExtractError):
            return raw

        _legacy_warn(parser, field)
        if raw is None:
            return ExtractMissing(reason="legacy None return")
        return ExtractOk(value=raw)

    def _handle_extract_error(
        self,
        result: ExecutionResult,
        err: ExtractError,
        field: str,
        intent_type: str,
        parser: Any,
        protocol: str | None = None,
    ) -> None:
        """Route an ExtractError per live/paper-mode policy.

        In live mode we raise CriticalAccountingError (inherits BaseException)
        so the runner's generic except-Exception handler cannot swallow it.
        In paper mode we log, increment a counter, and attach a structured
        warning so monitoring can still catch the problem.

        ``protocol`` is the resolved protocol slug (from the intent/context)
        and is what downstream consumers actually filter on; the parser class
        name stays in the human-readable message for diagnostics.
        """
        parser_name = type(parser).__name__
        message = f"Extraction failed for {field} (intent={intent_type}, parser={parser_name}): {err.error}"

        if self.live_mode:
            logger.error(message)
            raise CriticalAccountingError(
                message,
                field_name=field,
                intent_type=intent_type,
                protocol=protocol,
                original=err.exception,
            )

        self.extract_error_count += 1
        logger.warning(f"{message} (paper mode — surfaced as warning, not raised)")
        result.extraction_warnings.append(f"ExtractError[{field}]: {err.error}")

    def _attach_to_result(
        self,
        result: ExecutionResult,
        field: str,
        value: Any,
        intent_type: str,
    ) -> bool:
        """Attach extracted value to appropriate result field.

        Core typed fields are set directly on the result.
        All values are also added to extracted_data dict.

        Returns:
            ``True`` when the value was accepted and attached. ``False``
            when the value was rejected (e.g. wrong type for a typed
            field); the caller should treat this as if the receipt did
            not produce a valid value and continue scanning subsequent
            receipts in the bundle rather than stopping.

        Args:
            result: ExecutionResult to populate
            field: Field name
            value: Extracted value
            intent_type: Type of intent
        """
        # Core typed fields - set directly on result
        if field == "position_id" and isinstance(value, int | str):
            if isinstance(value, str):
                # Accept hex addresses (40-char, e.g. Curve LP token addresses) and bytes32
                # hashes (64-char, e.g. Aster Perps tradeHash) as valid position IDs.
                is_hex_address = bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", value))
                is_bytes32 = bool(re.fullmatch(r"0x[a-fA-F0-9]{64}", value))
                if not (is_hex_address or is_bytes32):
                    try:
                        parsed = Decimal(value)
                        if not parsed.is_finite():
                            logger.warning(f"Ignoring non-finite string position_id {value!r}")
                            result.extracted_data[field] = value
                            return True
                    except InvalidOperation:
                        logger.warning(f"Ignoring invalid string position_id {value!r}: not a valid decimal or address")
                        result.extracted_data[field] = value
                        return True
            result.position_id = value
        elif field == "swap_amounts" and isinstance(value, SwapAmounts):
            result.swap_amounts = value
        elif field == "lp_close_data" and isinstance(value, LPCloseData):
            result.lp_close_data = value
        elif field == "protocol_fees" and not isinstance(value, ProtocolFees):
            # VIB-3204 audit fix (CodeRabbit multi-receipt): rejecting the
            # value is correct, but the caller MUST continue scanning the
            # remaining receipts in a multi-tx bundle rather than treat
            # this rejection as "attached successfully" and stop. Return
            # False so _extract_field keeps looking.
            logger.warning(
                "Enrichment: parser returned non-ProtocolFees value for 'protocol_fees' "
                f"(type={type(value).__name__}); ignoring and continuing receipt scan"
            )
            return False

        # Always add to extracted_data for full access
        result.extracted_data[field] = value
        return True

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

    The default enricher is constructed with live_mode=True — callers that
    need paper/backtest semantics must construct their own enricher with
    live_mode=False (see backtesting/paper/engine.py).

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
    *,
    live_mode: bool | None = None,
    bundle_metadata: dict[str, Any] | None = None,
) -> ExecutionResult:
    """Enrich an execution result using the default enricher.

    Convenience function that uses the singleton ResultEnricher when
    live_mode is None. When live_mode is passed explicitly, a fresh
    enricher is constructed with that mode so the caller doesn't mutate
    the shared singleton.

    Args:
        result: Raw execution result from orchestrator
        intent: The intent that was executed
        context: Execution context with chain info
        live_mode: Optional override. None = use singleton default (live).
        bundle_metadata: Optional ActionBundle.metadata dict from the
            compiler. VIB-3203: carries ``expected_output_human`` so
            swap_amounts extractors can compute realized ``slippage_bps``.

    Returns:
        Enriched ExecutionResult

    Example:
        # live / default
        result = enrich_result(result, intent, context)

        # paper / backtest
        result = enrich_result(result, intent, context, live_mode=False)
    """
    if live_mode is None:
        return get_enricher().enrich(result, intent, context, bundle_metadata=bundle_metadata)
    enricher = ResultEnricher(live_mode=live_mode)
    return enricher.enrich(result, intent, context, bundle_metadata=bundle_metadata)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "CriticalAccountingError",
    "ExtractError",
    "ExtractMissing",
    "ExtractOk",
    "ResultEnricher",
    "enrich_result",
    "get_enricher",
]
