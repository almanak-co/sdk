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
from .extracted_data import BridgeData, LPCloseData, ProtocolFees, SwapAmounts
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
        "LP_OPEN": ["position_id", "tick_lower", "tick_upper", "liquidity", "bin_ids", "protocol_fees", "lp_open_data"],
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
        "WITHDRAW": ["withdraw_amount", "withdraw_amounts", "a_token_burned", "protocol_fees", "redemption_amounts"],
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
            # VIB-3497: funding fee USD at close. Parsers that implement
            # extract_funding_fee_usd return a Decimal; those that don't
            # (or return None) propagate as "unavailable" in attribution.
            "funding_fee_usd",
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
        # VIB-3226: BRIDGE enrichment returns a typed ``BridgeData`` struct
        # describing the *source-chain* deposit. Destination-chain settlement
        # is observed asynchronously (``EnsoStateProvider``) — the enricher
        # does not block on it. ``bridge_data`` carries the individual
        # scalars (source_tx_hash, destination_chain, expected_amount_out)
        # as typed fields; legacy scalar keys are intentionally NOT in the
        # spec — no caller reads ``result.extracted_data["source_tx_hash"]``
        # and the bridge parsers explicitly do not implement
        # ``extract_source_tx_hash`` etc., so including them here would only
        # generate spurious SUPPORTED_EXTRACTIONS warnings on every bridge
        # execution.
        "BRIDGE": [
            "bridge_data",
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
                returns ExtractError (or raises). Inherits from Exception
                so the strategy runner's recovery path in run_iteration can
                catch it and return ACCOUNTING_FAILED (VIB-3180).

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

        # VIB-3706: Off-chain extraction for Polymarket CLOB orders.
        # PREDICTION_BUY / PREDICTION_SELL submit off-chain via the CLOB API
        # — there are no on-chain receipts to parse. The fill data lives on
        # ``result.prediction_fill`` (set by the runner's CLOB branch in
        # _single_chain_execute_clob). We pull the spec fields from there
        # plus ``bundle_metadata["market_id"]`` BEFORE the on-chain receipt
        # collection runs, so a missing receipt cannot silently drop the
        # enrichment data the strategy needs to book the position.
        # PREDICTION_REDEEM stays on the on-chain CTF receipt path because
        # redemption is an on-chain merge call.
        offchain_extracted: set[str] = set()
        if intent_type in ("PREDICTION_BUY", "PREDICTION_SELL"):
            offchain_extracted = self._extract_offchain_prediction_fields(result, intent, intent_type, bundle_metadata)

        # Get protocol from intent, falling back to context (intent may be frozen with protocol=None)
        intent_protocol = self._get_protocol(intent)
        context_protocol = getattr(context, "protocol", None)
        protocol = intent_protocol or context_protocol

        # VIB-3226: BridgeIntent does not carry a protocol — the adapter is
        # selected by the compiler and recorded in ActionBundle.metadata as
        # ``"bridge": "<Name>"``. Fall back to that when nothing else is set
        # so BRIDGE enrichment works without requiring the runner to thread
        # the bridge adapter name through ExecutionContext.
        if not protocol and intent_type == "BRIDGE" and bundle_metadata:
            bridge_name = bundle_metadata.get("bridge")
            if bridge_name:
                protocol = str(bridge_name).lower()

        # VIB-3706: When off-chain extraction has already populated some
        # fields (PREDICTION_BUY/SELL CLOB path), we still want to fall
        # through to the summary log even if the on-chain receipt path is
        # unavailable (no protocol resolvable, no parser registered, no
        # receipts). Track parser availability without short-circuiting.
        parser: Any = None

        if not protocol:
            logger.debug(f"Enrichment: protocol=None on both intent and context (intent_type={intent_type})")
        else:
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
                parser = None

            if parser is not None:
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
                    parser = None
                else:
                    logger.debug(f"Enrichment: using parser {type(parser).__name__} for protocol={protocol}")

        # Collect receipts and run the on-chain extraction pass when we have
        # both a usable parser and at least one receipt. Off-chain enrichment
        # (above) is already attached to ``result`` regardless.
        if parser is not None:
            receipts = self._collect_receipts(result)
            if not receipts:
                if not offchain_extracted:
                    logger.debug(
                        f"Enrichment skipped: no receipts in execution result "
                        f"(intent_type={intent_type}, protocol={protocol})"
                    )
                    return result
            else:
                logger.debug(f"Enrichment: found {len(receipts)} receipt(s) to process")

                # On-chain extraction skips fields already populated off-chain so
                # the CLOB-authoritative values are not overwritten by speculative
                # log parsing. For non-prediction intents ``offchain_extracted`` is
                # empty and the full spec runs as before.
                onchain_spec = [f for f in spec if f not in offchain_extracted]

                # Install a temporary parse_receipt cache to avoid redundant parsing.
                # Without this, each extract_* method calls parse_receipt() independently,
                # meaning the same receipt is parsed N times for N extraction fields
                # (e.g., 5x for PERP_OPEN with position_id, size_delta, collateral, entry_price, leverage).
                self._install_parse_cache(parser)
                try:
                    # Extract each field in the (possibly filtered) on-chain spec
                    for field in onchain_spec:
                        self._extract_field(
                            result, parser, receipts, field, intent_type, protocol, bundle_metadata=bundle_metadata
                        )
                finally:
                    self._remove_parse_cache(parser)
        elif not offchain_extracted:
            # No parser AND no off-chain extraction — nothing to log, return.
            return result

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
            parser_label = type(parser).__name__ if parser is not None else "offchain"
            logger.debug(
                f"Enrichment: fields not extracted for {intent_type}: {', '.join(missing_fields)} "
                f"(protocol={protocol}, parser={parser_label})"
            )

        return result

    def _extract_offchain_prediction_fields(
        self,
        result: ExecutionResult,
        intent: Any,
        intent_type: str,
        bundle_metadata: dict[str, Any] | None,
    ) -> set[str]:
        """Extract Polymarket CLOB fill data for PREDICTION_BUY / PREDICTION_SELL.

        VIB-3706 introduced this off-chain path because Polymarket CLOB
        orders submit off-chain and produce no on-chain receipts; the runner
        attaches a :class:`PredictionFill` to ``result.prediction_fill`` in
        :meth:`StrategyRunner._single_chain_execute_clob`.

        VIB-3708: rather than read ``prediction_fill`` directly here (which
        forks parsing logic between the enricher and the parser), this
        method now constructs an ``OrderResponse``-shaped dict from
        ``prediction_fill`` + ``bundle_metadata`` + ``extracted_data["order_id"]``
        and routes it through
        :meth:`PolymarketReceiptParser.parse_order_response` to obtain a
        typed :class:`TradeResult`. The resulting fields are then mapped to
        the spec keys (``outcome_tokens_received`` / ``cost_basis`` /
        ``market_id`` for BUY, ``outcome_tokens_sold`` / ``proceeds`` /
        ``market_id`` for SELL).

        Single source of truth: any future edge case (partial fills,
        explicit fees, fee-adjusted cost basis) is handled inside the
        parser, not duplicated here.

        Fallback: if the parser raises or returns an unsuccessful
        ``TradeResult``, the method falls back to reading ``prediction_fill``
        directly (the VIB-3706 behavior) and emits a warning. This
        preserves VIB-3706's data-preservation guarantee — a parser bug
        cannot silently drop the only fill data the strategy will ever see.

        When ``prediction_fill`` is missing or unfilled (rejected order or
        resting GTC), the method attaches ``market_id`` if available and
        emits a structured ``extraction_warnings`` entry so downstream
        accounting cannot silently mistake a no-op for a fill. The data
        flow is deliberately one-way: this method writes into
        ``extracted_data`` and ``extraction_warnings`` only; it never
        raises.

        Args:
            result: ExecutionResult to mutate.
            intent: The PredictionBuyIntent / PredictionSellIntent. Used as
                a fallback source of ``market_id`` when bundle_metadata is
                absent or incomplete.
            intent_type: Either ``"PREDICTION_BUY"`` or ``"PREDICTION_SELL"``.
            bundle_metadata: ``ActionBundle.metadata`` from the polymarket
                adapter. The compiler always sets ``market_id`` here (see
                ``polymarket/adapter.py``).

        Returns:
            Set of spec field names successfully populated. Used by the
            caller so the on-chain receipt pass (if any) does not overwrite
            CLOB-authoritative values.
        """
        # Resolve market_id with bundle_metadata-then-intent fallback. The
        # adapter always writes market_id into metadata in the BUY/SELL
        # compile paths, but be defensive in case a bespoke compile path
        # ever omits it.
        market_id: str | None = None
        if bundle_metadata:
            raw_mid = bundle_metadata.get("market_id")
            if raw_mid is not None and raw_mid != "":
                market_id = str(raw_mid)
        if market_id is None:
            intent_mid = getattr(intent, "market_id", None)
            if intent_mid is not None and intent_mid != "":
                market_id = str(intent_mid)

        prediction_fill = getattr(result, "prediction_fill", None)

        # Field labels per spec (BUY vs SELL). PREDICTION_REDEEM is not
        # routed here — it stays on the on-chain CTF receipt path.
        if intent_type == "PREDICTION_BUY":
            shares_field = "outcome_tokens_received"
            value_field = "cost_basis"
        else:  # PREDICTION_SELL
            shares_field = "outcome_tokens_sold"
            value_field = "proceeds"

        extracted: set[str] = set()

        # Always attach market_id when we can — even an unfilled order needs
        # it for downstream identification.
        if market_id is not None:
            result.extracted_data["market_id"] = market_id
            extracted.add("market_id")
        else:
            warning = f"Enrichment incomplete: {intent_type} has no market_id (missing from bundle_metadata and intent)"
            logger.warning(warning)
            result.extraction_warnings.append(warning)

        if prediction_fill is None:
            warning = f"Enrichment incomplete: {intent_type} has no prediction_fill data, order may have been rejected"
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        # Build the OrderResponse-shaped dict the parser expects. Side is
        # derived from intent_type so the parser receives a complete view
        # even if the runner did not echo it back on PredictionFill.
        order_dict = self._build_clob_order_dict(
            intent_type=intent_type,
            prediction_fill=prediction_fill,
            market_id=market_id,
            order_id_fallback=result.extracted_data.get("order_id"),
        )

        # Try the parser-routed path first. Any failure (parser raises, or
        # returns success=False) drops to the direct prediction_fill
        # fallback below — VIB-3706's data-preservation guarantee must
        # survive a parser bug.
        trade_result = None
        try:
            from almanak.framework.connectors.polymarket.receipt_parser import (
                PolymarketReceiptParser,
            )

            parser = PolymarketReceiptParser()
            trade_result = parser.parse_order_response(order_dict)
        except Exception as exc:
            warning = (
                f"Enrichment fallback: {intent_type} parser.parse_order_response "
                f"raised ({type(exc).__name__}: {exc}); falling back to direct prediction_fill read"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            trade_result = None

        if trade_result is None or not trade_result.success:
            if trade_result is not None and not trade_result.success:
                warning = (
                    f"Enrichment fallback: {intent_type} parser returned unsuccessful "
                    f"TradeResult (error={trade_result.error}); falling back to direct prediction_fill read"
                )
                logger.warning(warning)
                result.extraction_warnings.append(warning)
            extracted |= self._extract_from_prediction_fill_direct(
                result, intent_type, prediction_fill, shares_field, value_field
            )
            return extracted

        # Map the parser's TradeResult onto the spec keys. The parser's
        # ``filled_size`` and ``avg_price`` are the canonical post-parse
        # values — derive shares + USD value from them so any future parser
        # adjustment (e.g. fee-adjusted basis) flows here automatically.
        filled_shares = trade_result.filled_size
        avg_price = trade_result.avg_price

        if filled_shares <= 0:
            # Zero-fill = order rejected (IOC unmatched) or resting (GTC live).
            # Surface as a structured warning so the strategy / accounting
            # cannot silently book a position from a no-op submission. The
            # parser preserves the lifecycle status string so the warning
            # carries the same diagnostics as the direct-read path.
            status = trade_result.status or "unknown"
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill has "
                f"filled_shares=0 (status={status}); no fill to extract"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        # Successful fill — populate shares + USD value fields.
        result.extracted_data[shares_field] = filled_shares
        extracted.add(shares_field)

        if avg_price is None or avg_price <= 0:
            # Filled but no average price — should not happen for a
            # non-zero fill that the parser successfully parsed, but treat
            # as an accounting gap rather than fabricating $0.
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill.filled_shares={filled_shares} "
                f"but avg_fill_price is missing or zero — cannot compute {value_field}"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        # cost_basis (BUY) / proceeds (SELL) = filled_shares * avg_price in
        # USDC. Polymarket prices are 0.01 tick, sizes are share-count
        # Decimals — straight Decimal multiplication preserves precision.
        usd_value = filled_shares * avg_price
        result.extracted_data[value_field] = usd_value
        extracted.add(value_field)

        # VIB-3710: surface gateway-side setup_tx gas + operator fee_pusd onto
        # extracted_data so the prediction handler can fold them into a
        # fully-loaded cost basis. Only meaningful for BUY (the gateway never
        # submits setup_txs on a SELL — allowances are already in place from
        # the first BUY) but kept symmetric so a SELL that did wrap (rare
        # edge case) still attributes its gas correctly.
        gas_extracted = self._extract_offchain_prediction_costs(
            result=result,
            intent_type=intent_type,
            prediction_fill=prediction_fill,
            bundle_metadata=bundle_metadata,
        )
        extracted |= gas_extracted

        return extracted

    def _extract_offchain_prediction_costs(
        self,
        *,
        result: ExecutionResult,
        intent_type: str,
        prediction_fill: Any,
        bundle_metadata: dict[str, Any] | None,
    ) -> set[str]:
        """Extract gateway setup_tx gas + operator fee_pusd from prediction_fill.

        VIB-3710: writes the following keys onto ``result.extracted_data`` when
        present:

          - ``setup_tx_count`` (int): number of approval / wrap txs the gateway
            submitted before this order.
          - ``gas_cost_native_wei`` (Decimal): aggregate MATIC wei spent on
            setup transactions. Always present when ``setup_tx_count > 0``.
          - ``gas_cost_usd`` (Decimal | None): same value converted via the
            compiler-resolved MATIC USD price. None when the price could not
            be resolved (a structured warning is appended to
            ``extraction_warnings``).
          - ``fee_pusd`` (Decimal): operator fee. Only written when the fill
            carried a non-None ``fee_pusd``.

        Spec-field-set returned by this method is informational — the keys
        above are NOT in EXTRACTION_SPECS (they are loaded-cost extras, not
        intent-required fields), so the on-chain receipt pass cannot
        accidentally clobber them.
        """
        extracted: set[str] = set()

        setup_txs = getattr(prediction_fill, "setup_txs", None) or ()
        if setup_txs:
            total_wei = Decimal("0")
            for tx in setup_txs:
                try:
                    total_wei += Decimal(str(getattr(tx, "total_cost_wei", "0") or "0"))
                except (InvalidOperation, ValueError, ArithmeticError):
                    continue
            result.extracted_data["setup_tx_count"] = len(setup_txs)
            result.extracted_data["gas_cost_native_wei"] = total_wei
            extracted.add("setup_tx_count")
            extracted.add("gas_cost_native_wei")

            # Resolve MATIC USD price from compiler bundle_metadata. Missing
            # or unparseable price degrades gracefully — gas_cost_usd stays
            # None, the basis row records gas_cost_usd=None, and the
            # accounting handler can still record everything else without
            # fabricating a USD figure from nothing.
            matic_price: Decimal | None = None
            if bundle_metadata:
                raw_price = bundle_metadata.get("native_token_price_usd")
                if raw_price not in (None, ""):
                    try:
                        candidate = Decimal(str(raw_price))
                        if candidate > 0:
                            matic_price = candidate
                    except (InvalidOperation, ValueError, ArithmeticError):
                        matic_price = None

            if matic_price is not None:
                gas_cost_usd = (total_wei / Decimal(10**18)) * matic_price
                result.extracted_data["gas_cost_usd"] = gas_cost_usd
                extracted.add("gas_cost_usd")
            else:
                # None signals "unknown" — distinct from Decimal("0") (which
                # would mean "we measured zero gas"). The handler treats None
                # as gas_cost_usd=0 in the basis sum but logs the gap.
                warning = (
                    f"Enrichment incomplete: {intent_type} setup_tx gas attributed "
                    f"to native units (gas_cost_native_wei={total_wei}) but "
                    "MATIC USD price was not resolvable; gas_cost_usd omitted"
                )
                logger.warning(warning)
                result.extraction_warnings.append(warning)

        fee_pusd = getattr(prediction_fill, "fee_pusd", None)
        if fee_pusd is not None:
            try:
                fee_decimal = Decimal(str(fee_pusd))
                if fee_decimal >= 0:
                    result.extracted_data["fee_pusd"] = fee_decimal
                    extracted.add("fee_pusd")
            except (InvalidOperation, ValueError, ArithmeticError):
                pass

        return extracted

    @staticmethod
    def _build_clob_order_dict(
        intent_type: str,
        prediction_fill: Any,
        market_id: str | None,
        order_id_fallback: str | None,
    ) -> dict[str, Any]:
        """Construct an OrderResponse-shaped dict for parse_order_response.

        Mirrors the CLOB API response shape documented on
        :meth:`PolymarketReceiptParser.parse_order_response` — populated from
        the runner-attached :class:`PredictionFill` plus compiler-side
        bundle_metadata. The parser tolerates missing fields, but we
        provide them all so log messages and edge cases line up with
        production responses.

        ``side`` is derived from intent_type because PredictionFill does
        not echo it; ``createdAt`` is intentionally omitted because the
        runner does not capture submission time on the fill struct (the
        parser handles a missing timestamp gracefully).
        """
        # Order ID: prefer the value the runner stamped on extracted_data
        # (set in StrategyRunner._single_chain_execute_clob from
        # clob_result.order_id) and fall back to PredictionFill.order_id.
        order_id = order_id_fallback or getattr(prediction_fill, "order_id", None)
        side = "BUY" if intent_type == "PREDICTION_BUY" else "SELL"

        # Numeric fields — pass through as strings so the parser can do its
        # own Decimal coercion uniformly with real CLOB responses.
        filled_shares_raw = getattr(prediction_fill, "filled_shares", Decimal("0"))
        requested_shares_raw = getattr(prediction_fill, "requested_shares", Decimal("0"))
        avg_fill_price_raw = getattr(prediction_fill, "avg_fill_price", None)
        status = getattr(prediction_fill, "status", None) or "UNKNOWN"

        order_dict: dict[str, Any] = {
            "orderID": order_id,
            "status": status,
            "side": side,
            # ``size`` is the *requested* size on a CLOB order; the parser
            # does not currently use it for value derivation, but it is
            # part of the documented shape — populate so edge cases that
            # later read it (e.g. partial-fill detection) work.
            "size": str(requested_shares_raw),
            "filledSize": str(filled_shares_raw),
        }
        if avg_fill_price_raw is not None:
            order_dict["avgPrice"] = str(avg_fill_price_raw)
            # parse_order_response falls back to ``price`` when avgPrice is
            # missing; mirror avgPrice here so the fallback path also
            # produces the same value if avgPrice is ever stripped.
            order_dict["price"] = str(avg_fill_price_raw)
        if market_id is not None:
            order_dict["market"] = market_id
        return order_dict

    @staticmethod
    def _extract_from_prediction_fill_direct(
        result: ExecutionResult,
        intent_type: str,
        prediction_fill: Any,
        shares_field: str,
        value_field: str,
    ) -> set[str]:
        """Direct prediction_fill -> extracted_data fallback (VIB-3706 path).

        Used only when the parser-routed path fails. Mirrors the original
        VIB-3706 logic exactly so the user-visible result is identical to
        the pre-3708 behavior on a parser bug.
        """
        extracted: set[str] = set()

        try:
            filled_shares = Decimal(str(prediction_fill.filled_shares))
        except (InvalidOperation, TypeError, ValueError) as exc:
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill.filled_shares "
                f"could not be coerced to Decimal: {exc}"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        if filled_shares <= 0:
            status = getattr(prediction_fill, "status", None) or "unknown"
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill has "
                f"filled_shares=0 (status={status}); no fill to extract"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        result.extracted_data[shares_field] = filled_shares
        extracted.add(shares_field)

        avg_price_raw = getattr(prediction_fill, "avg_fill_price", None)
        if avg_price_raw is None:
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill.filled_shares={filled_shares} "
                f"but avg_fill_price is None — cannot compute {value_field}"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        try:
            avg_fill_price = Decimal(str(avg_price_raw))
        except (InvalidOperation, TypeError, ValueError) as exc:
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill.avg_fill_price "
                f"could not be coerced to Decimal: {exc}"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        usd_value = filled_shares * avg_fill_price
        result.extracted_data[value_field] = usd_value
        extracted.add(value_field)
        return extracted

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
        if field == "bridge_data":
            # VIB-3226: bridge receipts typically do not carry the user-facing
            # symbol or canonical chain names — they encode chain IDs and token
            # addresses. The compiler writes the resolved intent shape into
            # ``ActionBundle.metadata`` (see compiler._compile_bridge), so we
            # thread those hints into the parser to keep the typed output
            # stable and avoid re-deriving them at parse time.
            kwargs: dict[str, Any] = {}
            for key in ("from_chain", "to_chain", "token", "amount", "bridge"):
                val = bundle_metadata.get(key)
                if val is not None and val != "":
                    kwargs[key] = val
            # Expected output (post-fee) from the compiler quote — optional,
            # parsers that do not accept it fall back via TypeError handling.
            out_amount = bundle_metadata.get("output_amount")
            if out_amount is not None:
                kwargs["expected_amount_out"] = out_amount
            return kwargs
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

        In live mode we raise CriticalAccountingError (inherits Exception).
        See the module docstring for the VIB-3180 rationale on why this is
        Exception (not BaseException) and where it is caught.
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
        elif field == "bridge_data":
            # VIB-3226: reject anything that is not BridgeData so a broken
            # parser cannot silently populate ``result.bridge_data`` with a
            # dict / None-fielded struct. Matches the ProtocolFees pattern:
            # return False so the enricher keeps scanning subsequent receipts
            # in a multi-tx bundle (approve + deposit is the common shape).
            if not isinstance(value, BridgeData):
                logger.warning(
                    "Enrichment: parser returned non-BridgeData value for 'bridge_data' "
                    f"(type={type(value).__name__}); ignoring and continuing receipt scan"
                )
                return False
            result.bridge_data = value
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
        if field == "bridge_data":
            return getattr(result, "bridge_data", None) is not None

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
        bd = getattr(result, "bridge_data", None)
        if field == "bridge_data" and bd is not None:
            return f"{bd.amount_sent} {bd.token_symbol} {bd.source_chain}->{bd.destination_chain} via {bd.bridge_name}"
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
