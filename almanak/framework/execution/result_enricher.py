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
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from almanak.core.chains._helpers import is_solana_chain

from .extract_result import (
    CriticalAccountingError,
    ExtractError,
    ExtractMissing,
    ExtractOk,
)
from .extracted_data import AsyncOrderData, BridgeData, LPCloseData, LPOpenData, ProtocolFees, SwapAmounts
from .receipt_registry import ReceiptParserRegistry

if TYPE_CHECKING:
    from .orchestrator import ExecutionContext, ExecutionResult

logger = logging.getLogger(__name__)


def _is_primitive_money_legs(value: Any) -> bool:
    """Type guard for the connector-declared ``PrimitiveMoneyLegs``.

    Deferred import keeps the framework -> connector boundary intact: the
    connector value type must never load at framework-module import time
    (mirrors ``ledger._declared_money_legs``).
    """
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

    return isinstance(value, PrimitiveMoneyLegs)


def stamp_trading_wallet(receipt: dict[str, Any], wallet: str) -> dict[str, Any]:
    """Stamp the effective trading wallet onto a receipt copy (VIB-6043).

    Thin deferred-import wrapper around the connector-side helper (same
    framework -> connector boundary discipline as
    :func:`_is_primitive_money_legs`): the shared resolver lives with the
    receipt-parser base infrastructure that consumes it
    (``almanak/connectors/_strategy_base/base/receipt_wallet.py``), and must
    not load at framework-module import time.
    """
    from almanak.connectors._strategy_base.base.receipt_wallet import (
        stamp_trading_wallet as _stamp,
    )

    return _stamp(receipt, wallet)


# Typed fields are mirrored to top-level slots and ``extracted_data``.
# Rejecting the wrong type must not end a multi-receipt scan:
# a later receipt may contain a valid value.


_STRICT_TYPED_FIELDS: dict[str, tuple[str, Callable[[Any], bool], str]] = {
    "lp_close_data": ("lp_close_data", lambda v: isinstance(v, LPCloseData), "LPCloseData"),
    "bridge_data": ("bridge_data", lambda v: isinstance(v, BridgeData), "BridgeData"),
    "protocol_fees": ("protocol_fees", lambda v: isinstance(v, ProtocolFees), "ProtocolFees"),
    "async_orders": (
        "async_orders",
        lambda v: isinstance(v, list) and bool(v) and all(isinstance(order, AsyncOrderData) for order in v),
        "non-empty list[AsyncOrderData]",
    ),
    "bin_ids": (
        "bin_ids",
        # ``bool`` is an ``int`` subclass, so exclude it from bin ids.
        lambda v: isinstance(v, list) and all(isinstance(b, int) and not isinstance(b, bool) for b in v),
        "list[int]",
    ),
    "primitive_money_legs": ("primitive_money_legs", _is_primitive_money_legs, "PrimitiveMoneyLegs"),
}

# Receipt parsers consume web3-style camelCase keys.

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

# Warn once per parser and field to avoid receipt-level log spam.


_LEGACY_WARNED: set[tuple[str, str]] = set()


# Aggregate fields inspect every receipt before choosing a value.
# Split liquidity closes emit principal on decrease and principal plus
# fees on collect, so first-match semantics can lose accrued fees.
# Tagged candidates prefer the configured source; untagged candidates
# retain first-found semantics.


_AGGREGATE_FIELDS: dict[str, str] = {
    "lp_close_data": "collect",
}

# Some money legs span transactions and must be parsed from the union
# of their logs. This avoids treating an intermediate asset as the
# intent output. Parsers filter by contract and topic, so sibling logs
# remain inert for unrelated events.


_MERGED_RECEIPT_FIELDS: frozenset[str] = frozenset({"primitive_money_legs"})


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
        f"instead of ExtractOk/ExtractMissing/ExtractError. Parse errors and "
        f"missing events are indistinguishable — migrate to the tagged variant.",
        DeprecationWarning,
        stacklevel=3,
    )


def _receipt_parser_kwarg_keys(kwarg_name: str) -> frozenset[str]:
    """Receipt-parser keys whose connector declares ``kwarg_name`` in its manifest.

    Derived from each connector's manifest ``receipt_parser_kwargs`` declaration
    (VIB-4851 C3) — keyed by ``receipt_parser_keys`` (canonical name + aliases +
    explicit receipt-parser protocols) so fork keys resolve like the parser
    registry itself does.

    Recomputed per call — a cheap filter over the registry's cached manifest
    tuple — so test-side ``CONNECTOR_REGISTRY.clear()`` is honoured; a
    module-level cache here would serve stale sets after a registry reset.
    """
    # Connector discovery must never run at module import.
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    return frozenset(
        key
        for connector in CONNECTOR_REGISTRY.all()
        if kwarg_name in connector.receipt_parser_kwargs
        for key in connector.receipt_parser_keys
    )


def _pool_key_lookup_protocols() -> frozenset[str]:
    """Receipt-parser keys whose connector declares the ``pool_key_lookup`` kwarg.

    The Uniswap V4 parser resolves ``ModifyLiquidity.pool_id`` -> canonical
    ``PoolKey`` via the gateway (VIB-4477 T08).
    """
    return _receipt_parser_kwarg_keys("pool_key_lookup")


def _pool_meta_lookup_protocols() -> frozenset[str]:
    """Receipt-parser keys whose connector declares the ``pool_meta_lookup`` kwarg.

    The Curve parser resolves an uncurated pool's coin addresses / symbols /
    pool_type from the on-chain MetaRegistry on a static-registry miss
    (VIB-5628).
    """
    return _receipt_parser_kwarg_keys("pool_meta_lookup")


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

    # Protocol fees apply across money-bearing intents. Parsers that do not
    # declare an extractor omit the field without a missing-method warning.

    EXTRACTION_SPECS: dict[str, list[str]] = {
        "SWAP": ["swap_amounts", "protocol_fees"],
        # Keep ``bin_ids`` protocol-specific until ``LPOpenData`` owns them.
        "LP_OPEN": ["position_id", "tick_lower", "tick_upper", "liquidity", "protocol_fees", "lp_open_data"],
        "LP_CLOSE": [
            "lp_close_data",
            "amount0_collected",
            "amount1_collected",
            "fees0",
            "fees1",
            "protocol_fees",
        ],
        "LP_COLLECT_FEES": ["fees0", "fees1", "protocol_fees"],
        # EVM parsers use singular amounts; Solana parsers use plural amounts.
        "BORROW": ["borrow_amount", "borrow_amounts", "borrow_rate", "debt_token", "protocol_fees"],
        "REPAY": ["repay_amount", "repay_amounts", "remaining_debt", "protocol_fees"],
        "SUPPLY": ["supply_amount", "supply_amounts", "a_token_received", "supply_rate", "protocol_fees"],
        "WITHDRAW": ["withdraw_amount", "withdraw_amounts", "a_token_burned", "protocol_fees", "redemption_amounts"],
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
            # Funding fees are USD Decimals; absent parser data stays unmeasured.
            "funding_fee_usd",
        ],
        "STAKE": ["stake_amount", "shares_received", "wsteth_received", "stake_token", "protocol_fees"],
        "UNSTAKE": ["unstake_amount", "underlying_received", "cooldown_end", "protocol_fees"],
        "FLASH_LOAN": ["loan_amount", "fee_paid", "loan_token"],
        "PREDICTION_BUY": ["outcome_tokens_received", "cost_basis", "market_id"],
        "PREDICTION_SELL": ["outcome_tokens_sold", "proceeds", "market_id"],
        "PREDICTION_REDEEM": ["redemption_amount", "payout", "market_id"],
        # Bridge enrichment describes the source-chain deposit; destination
        # settlement is asynchronous. Keep the individual values inside the
        # typed ``BridgeData`` rather than requesting unsupported scalar fields.
        "BRIDGE": [
            "bridge_data",
        ],
        "ENSURE_BALANCE": ["amount_transferred", "source_chain"],
        "VAULT_DEPOSIT": ["deposit_data", "protocol_fees"],
        "VAULT_REDEEM": ["redeem_data", "protocol_fees"],
        "HOLD": [],
        # Canceling a pending order is a cash movement captured by wallet
        # balance deltas, not a position or PnL extraction.
        "PERP_CANCEL_ORDER": [],
        # Withdrawing free venue margin is an asynchronous cash movement, not
        # a trade; wallet balance deltas capture the settled amount and fees.
        "PERP_WITHDRAW": [],
    }

    # Protocol overlays append fields after the generic spec with
    # order-preserving deduplication. Base fields always run first, and
    # ``protocol=None`` leaves the base spec unchanged.

    EXTRACTION_SPECS_BY_PROTOCOL: dict[str, dict[str, list[str]]] = {
        "traderjoe_v2": {
            # TraderJoe positions need the canonical LBPair address carried by
            # structured LP data; descriptor-shaped keys are not V3 fee tiers.
            # Fee-only collections therefore request principal-zero close data.
            # LP opens and closes also expose declared money legs so accounting
            # records measured token notionals instead of a fabricated zero basis.
            # ``bin_ids`` remains available to position-tracking consumers.
            "LP_OPEN": ["bin_ids", "primitive_money_legs"],
            "LP_COLLECT_FEES": ["bin_ids", "lp_close_data"],
            "LP_CLOSE": ["primitive_money_legs"],
        },
        # A V4 fee-only collection emits a zero-liquidity modification rather
        # than a principal burn. Structured close data supplies the canonical
        # PoolId while preserving the generic fee fields.
        "uniswap_v4": {
            "LP_COLLECT_FEES": ["lp_close_data"],
        },
        # Morpho collateral events are distinct from loan-side supply and
        # withdrawal events. Surface their exact on-chain asset amounts under
        # collateral-specific fields for downstream scaling.
        "morpho_blue": {
            "SUPPLY": ["supply_collateral_amount"],
            "WITHDRAW": ["withdraw_collateral_amount"],
        },
        # Compound collateral supply is distinct from base-asset supply and
        # needs its own amount field; base-asset receipts remain unchanged.
        "compound_v3": {
            "SUPPLY": ["supply_collateral_amount"],
        },
        # Lido declares typed input and output money legs. Keep this overlay
        # protocol-specific so unmigrated staking parsers do not warn.
        "lido": {
            "STAKE": ["primitive_money_legs"],
        },
        # New connector-specific fields belong in the parser's
        # ``EXTRA_EXTRACTIONS_BY_INTENT`` declaration, not this legacy table.
    }

    # Removals apply after additive overlays so structurally absent fields
    # cannot produce recurring capability warnings.
    # Aerodrome Classic is fungible and tickless; collected amounts and fees
    # live inside structured close data. Slipstream carries ticks and close
    # amounts in structured data but retains standalone fee extractors.
    # V3 forks likewise carry all close amounts and fees in ``lp_close_data``.
    # Keep structured close data: absent values remain unmeasured and must
    # never be replaced with measured zero.
    # Frozensets avoid rebuilding a membership set for every enrichment.

    EXTRACTION_SPECS_REMOVE_BY_PROTOCOL: dict[str, dict[str, frozenset[str]]] = {
        "aerodrome": {
            "LP_OPEN": frozenset({"lp_open_data", "tick_lower", "tick_upper"}),
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
        },
        "aerodrome_slipstream": {
            "LP_OPEN": frozenset({"tick_lower", "tick_upper"}),
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected"}),
        },
        # Alias normalization occurs before lookup, so each canonical V3 fork
        # needs its own removal entry.
        "uniswap_v3": {
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
        },
        "sushiswap_v3": {
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
        },
        "pancakeswap_v3": {
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
        },
        "agni_finance": {
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
        },
    }

    @staticmethod
    def _canonicalise_protocol(protocol: str | None, context: Any) -> str | None:
        """Normalize a protocol alias (e.g. ``trader-joe-v2``) to canonical form.

        ``ReceiptParserRegistry.get`` already normalises aliases internally;
        we mirror that here so the overlay lookup (`EXTRACTION_SPECS_BY_PROTOCOL`)
        sees the same key. ``None`` / empty input passes through unchanged.
        """
        if not protocol:
            return protocol
        from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

        return normalize_protocol(str(getattr(context, "chain", "") or ""), protocol)

    @staticmethod
    def _merge_spec_with_overlay(intent_type: str, protocol: str | None) -> list[str]:
        """Return effective extraction spec for (intent_type, protocol).

        Two-phase merge:

        1. **Additive** — ``EXTRACTION_SPECS_BY_PROTOCOL`` overlay fields are
           appended at the tail of the base spec with order-preserving dedup.
           Base fields always come first (preserves the VIB-4320 semantics).
        2. **Subtractive** — ``EXTRACTION_SPECS_REMOVE_BY_PROTOCOL`` fields are
           removed from the merged spec. Applied last so a remove entry can
           drop both base AND overlay fields per-protocol if needed (VIB-4434
           W2).

        ``protocol`` is expected to be already canonicalised via
        ``normalize_protocol(chain, protocol)`` by the caller (see ``enrich``).
        Passing a raw alias here would silently miss the overlay and was the
        regression Codex flagged on PR #2269.
        """
        base = list(ResultEnricher.EXTRACTION_SPECS.get(intent_type, []))
        if protocol is None:
            return base
        overlay = ResultEnricher.EXTRACTION_SPECS_BY_PROTOCOL.get(protocol, {}).get(intent_type, [])
        merged = list(base)
        seen = set(base)
        for field in overlay:
            if field not in seen:
                merged.append(field)
                seen.add(field)
        # Removal values are already frozensets; avoid per-call conversion.

        removed = ResultEnricher.EXTRACTION_SPECS_REMOVE_BY_PROTOCOL.get(protocol, {}).get(intent_type)
        if removed:
            merged = [field for field in merged if field not in removed]
        return merged

    @staticmethod
    def _with_parser_extra_extractions(spec: list[str], parser: Any, intent_type: str) -> list[str]:
        """Append a parser's CONNECTOR-DECLARED per-intent extraction fields.

        A receipt parser may publish ``EXTRA_EXTRACTIONS_BY_INTENT`` —
        ``{intent_type: (field, ...)}`` — naming extra fields it can extract beyond
        the generic :data:`EXTRACTION_SPECS` base (e.g. the US-009
        ``primitive_money_legs`` seam for a PT redeem WITHDRAW). The framework reads
        it generically so connector-specific field choices live in the connector,
        not this enricher (the alternative — a per-protocol overlay — names the
        protocol here; ``test_connector_descriptor`` forbids that for migrated
        connectors). Additive with order-preserving dedup, mirroring
        :meth:`_merge_spec_with_overlay`; each field is still gated by the parser's
        ``SUPPORTED_EXTRACTIONS`` at extraction time, so a stray declaration cannot
        force an unsupported extract.
        """
        extra = getattr(parser, "EXTRA_EXTRACTIONS_BY_INTENT", None)
        if not isinstance(extra, dict):
            return spec
        fields = extra.get(intent_type)
        if not fields:
            return spec
        merged = list(spec)
        seen = set(spec)
        for field in fields:
            if field not in seen:
                merged.append(field)
                seen.add(field)
        return merged

    @staticmethod
    def _with_parser_extraction_removals(spec: list[str], parser: Any, intent_type: str) -> list[str]:
        """Drop a parser's CONNECTOR-DECLARED per-intent non-applicable fields.

        Subtractive sibling of :meth:`_with_parser_extra_extractions`
        (VIB-5896). A receipt parser may publish
        ``EXTRACTION_REMOVALS_BY_INTENT`` — ``{intent_type: frozenset(field,
        ...)}`` — naming base-spec fields that structurally do not exist for its
        venue (e.g. Curve StableSwap is tickless, so the V3-shaped
        ``tick_lower``/``tick_upper`` LP_OPEN expectations would only produce
        the chronic "parser does not declare support" WARN; its LP_CLOSE flat
        fields ship inside ``lp_close_data``, same as the V3 forks). Declared
        connector-side so the venue-shape knowledge lives in the connector, not
        as a protocol-named entry in this framework's
        ``EXTRACTION_SPECS_REMOVE_BY_PROTOCOL`` (which the coupling /
        literal-dispatch ratchets rightly flag for migrated connectors).
        Applied after the additive merges, mirroring the two-phase
        ``_merge_spec_with_overlay`` semantics.
        """
        removals = getattr(parser, "EXTRACTION_REMOVALS_BY_INTENT", None)
        if not isinstance(removals, dict):
            return spec
        fields = removals.get(intent_type)
        if not fields:
            return spec
        return [field for field in spec if field not in fields]

    def __init__(
        self,
        parser_registry: ReceiptParserRegistry | None = None,
        *,
        live_mode: bool = True,
        pool_key_lookup: Any = None,
        pool_meta_lookup: Any = None,
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
            pool_key_lookup: VIB-4477 (T08). Sync ``(pool_id_hex, chain) ->
                PoolKey | None`` callable injected into the Uniswap V4 receipt
                parser so ``extract_lp_close_data`` can resolve V4
                ``ModifyLiquidity.pool_id`` back to its canonical PoolKey via
                the gateway. ``None`` (default) skips the wiring — V4
                LP_CLOSE events then drop with a structured
                ``missing_pool_key_lookup`` warning (Empty != Zero per
                blueprint 27, the parser fails loud rather than misattribute).
                The strategy runner builds this from connector-owned runner
                hooks bound to its ``GatewayClient``.
            pool_meta_lookup: VIB-5628. Sync ``(pool_address, chain) ->
                CurvePoolMetadata | None`` callable injected into the Curve
                receipt parser so the leg-labelling helpers can resolve exact
                pool coin addresses / symbols / pool_type from the on-chain
                MetaRegistry. ``None`` (default) skips the wiring — pool legs
                then degrade to ``[]`` / ``""`` (Empty != Zero,
                never fabricates). The strategy runner builds this from the
                Curve gateway bridge bound to its ``GatewayClient``.
        """
        self.parser_registry = parser_registry or ReceiptParserRegistry()
        self.live_mode = live_mode
        self._pool_key_lookup = pool_key_lookup
        self._pool_meta_lookup = pool_meta_lookup

        self.extract_error_count: int = 0

    def enrich(  # noqa: C901
        self,
        result: ExecutionResult,
        intent: Any,
        context: ExecutionContext,
        *,
        bundle_metadata: dict[str, Any] | None = None,
        additional_receipts: tuple[dict[str, Any], ...] = (),
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
            additional_receipts: Successful receipts produced after the
                submission bundle completed, such as a keeper execution for
                an asynchronous order. These supplement, rather than replace,
                the original submission receipts.

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

        if not result.success:
            logger.debug("Enrichment skipped: execution failed")
            return result

        intent_type = self._get_intent_type(intent)
        if intent_type not in self.EXTRACTION_SPECS:
            logger.debug(f"Enrichment skipped: no extraction spec for intent type '{intent_type}'")
            return result

        # An empty base spec is intentionally terminal and skips all enrichment.

        base_spec = self.EXTRACTION_SPECS[intent_type]
        if not base_spec:
            return result

        # CLOB orders have no on-chain receipt. Attach their fill fields before
        # receipt collection so missing receipts cannot discard authoritative
        # off-chain data; redemptions remain on the on-chain path.

        offchain_extracted: set[str] = set()
        if intent_type in ("PREDICTION_BUY", "PREDICTION_SELL"):
            offchain_extracted = self._extract_offchain_prediction_fields(
                result, intent, intent_type, bundle_metadata, context
            )

        intent_protocol = self._get_protocol(intent)
        context_protocol = getattr(context, "protocol", None)
        protocol = intent_protocol or context_protocol

        # Bridge intents obtain their compiler-selected adapter from metadata
        # when neither the intent nor context identifies a protocol.

        if not protocol and intent_type == "BRIDGE" and bundle_metadata:
            bridge_name = bundle_metadata.get("bridge")
            if bridge_name:
                protocol = str(bridge_name).lower()

        # Canonicalize before overlay lookup, then append overlay fields after
        # base fields so extraction order remains stable.

        protocol = self._canonicalise_protocol(protocol, context)

        spec = self._merge_spec_with_overlay(intent_type, protocol)

        # Off-chain fields still reach summary logging when no parser or receipt
        # is available, so parser availability cannot short-circuit them.

        parser: Any = None

        if not protocol:
            logger.debug(f"Enrichment: protocol=None on both intent and context (intent_type={intent_type})")
        else:
            logger.debug(
                f"Enrichment: intent_type={intent_type}, protocol={protocol} "
                f"(from={'intent' if intent_protocol else 'context'}), "
                f"chain={context.chain}, fields={spec}"
            )

            # Never route Solana instruction-string logs through EVM parsers, which
            # expect mapping logs with topics.

            chain_str = str(getattr(context, "chain", "")).lower()
            is_solana = is_solana_chain(chain_str)

            parser_kwargs = self._build_parser_kwargs(protocol, context.chain)
            try:
                parser = self.parser_registry.get(protocol, **parser_kwargs)
            except ValueError as e:
                warning = f"Parser not found for {protocol}: {e}"
                logger.info(warning)
                result.extraction_warnings.append(warning)
                parser = None

            if parser is not None:
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

        if parser is not None:
            # Stamp the effective trading wallet on copies of every receipt. Under
            # Safe execution, ``receipt['from']`` is the agent EOA, not the Safe.

            trading_wallet = str(getattr(context, "wallet_address", "") or "")
            receipts = self._collect_receipts(result, trading_wallet)
            receipts.extend(self._collect_additional_receipts(additional_receipts, trading_wallet))
            if not receipts:
                if not offchain_extracted:
                    logger.debug(
                        f"Enrichment skipped: no receipts in execution result "
                        f"(intent_type={intent_type}, protocol={protocol})"
                    )
                    return result
            else:
                logger.debug(f"Enrichment: found {len(receipts)} receipt(s) to process")

                # Apply parser-declared additions before removals. Subtractive-last
                # ordering matches the protocol overlay contract.

                spec = self._with_parser_extra_extractions(spec, parser, intent_type)

                spec = self._with_parser_extraction_removals(spec, parser, intent_type)

                # CLOB-authoritative fields must not be overwritten by speculative
                # on-chain parsing.

                onchain_spec = [f for f in spec if f not in offchain_extracted]

                # Cache parsed receipts across field extractors; parsing once per field
                # would repeatedly decode the same logs.

                self._install_parse_cache(parser)
                try:
                    for field in onchain_spec:
                        self._extract_field(
                            result, parser, receipts, field, intent_type, protocol, bundle_metadata=bundle_metadata
                        )
                finally:
                    self._remove_parse_cache(parser)
        elif not offchain_extracted:
            return result

        # Pure V4 mints emit no Swap event, so use the compile-time tick only
        # when the parser left it unmeasured. Receipt-derived values always win.
        # The mint itself cannot move price, though an interleaving transaction
        # can make the compile-time value stale. Capability gating keeps this
        # fallback inert for other protocols and intent types.

        self._fill_v4_lp_open_current_tick_from_metadata(result, bundle_metadata)

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
        context: ExecutionContext | None = None,
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

        Fallback: if no parser is registered for the protocol, or if the
        parser lacks ``parse_order_response``, the method falls back to
        reading ``prediction_fill`` directly (the VIB-3706 behavior) in
        ALL modes and emits a warning — protocol not yet covered is not a
        parser bug.  If the parser is present but ``parse_order_response``
        raises (a parser bug on data the framework is about to book), the
        behavior is mode-aware: in live mode this raises
        :class:`~almanak.framework.execution.extract_result.CriticalAccountingError`
        (VIB-3159 fail-closed contract, same policy as
        ``_handle_extract_error``); in paper / backtest mode it keeps
        VIB-3706's warn-and-fallback so a parser bug cannot silently drop
        the only fill data the strategy will ever see. A structured
        ``TradeResult(success=False)`` from the parser (its deliberate
        "could not parse" signal) still falls back in all modes — unchanged.

        When ``prediction_fill`` is missing or unfilled (rejected order or
        resting GTC), the method attaches ``market_id`` if available and
        emits a structured ``extraction_warnings`` entry so downstream
        accounting cannot silently mistake a no-op for a fill. The data
        flow is deliberately one-way: this method writes into
        ``extracted_data`` and ``extraction_warnings`` only; it raises
        ``CriticalAccountingError`` only for a live-mode parser crash, and
        never raises in paper / backtest mode.

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

        if intent_type == "PREDICTION_BUY":
            shares_field = "outcome_tokens_received"
            value_field = "cost_basis"
        else:
            shares_field = "outcome_tokens_sold"
            value_field = "proceeds"

        extracted: set[str] = set()

        # Retain market identity even when the order has no fill.

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

        order_dict = self._build_clob_order_dict(
            intent_type=intent_type,
            prediction_fill=prediction_fill,
            market_id=market_id,
            order_id_fallback=result.extracted_data.get("order_id"),
        )

        # Resolve the parser through the registry; connector imports do not
        # belong at the framework boundary.

        offchain_protocol = (
            (bundle_metadata or {}).get("protocol")
            or self._get_protocol(intent)
            or getattr(context, "protocol", None)
            or ""
        )

        trade_result = self._parse_prediction_order_response(
            result=result,
            offchain_protocol=offchain_protocol,
            order_dict=order_dict,
            intent_type=intent_type,
            value_field=value_field,
        )

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

        filled_shares = trade_result.filled_size
        avg_price = trade_result.avg_price

        if filled_shares <= 0:
            # A zero fill is not a position. Surface it so accounting cannot book
            # rejected, unmatched, or resting orders as completed trades.

            status = trade_result.status or "unknown"
            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill has "
                f"filled_shares=0 (status={status}); no fill to extract"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        result.extracted_data[shares_field] = filled_shares
        extracted.add(shares_field)

        if avg_price is None or avg_price <= 0:
            # A filled order without a positive average price is unmeasured; never
            # fabricate a zero-dollar value.

            warning = (
                f"Enrichment incomplete: {intent_type} prediction_fill.filled_shares={filled_shares} "
                f"but avg_fill_price is missing or zero — cannot compute {value_field}"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return extracted

        # Prices are USDC per share; Decimal multiplication preserves the
        # 0.01 price tick and fractional share precision.

        usd_value = filled_shares * avg_price
        result.extracted_data[value_field] = usd_value
        extracted.add(value_field)

        # Gateway setup gas and operator fees belong in loaded cost basis. Keep
        # the path symmetric for rare sells that also require setup.

        gas_extracted = self._extract_offchain_prediction_costs(
            result=result,
            intent_type=intent_type,
            prediction_fill=prediction_fill,
            bundle_metadata=bundle_metadata,
        )
        extracted |= gas_extracted

        return extracted

    def _parse_prediction_order_response(
        self,
        result: ExecutionResult,
        offchain_protocol: str,
        order_dict: dict[str, Any],
        intent_type: str,
        value_field: str,
    ) -> Any:
        """Acquire the receipt parser and call parse_order_response; return the TradeResult or None.

        Failure handling is class-aware:
          (a)  no parser registered for the protocol (ValueError from the
               registry, including offchain_protocol == "") -> direct
               prediction_fill fallback in ALL modes; the protocol may
               legitimately have no parser yet. Mirrors the main on-chain
               path's ValueError handling above.
          (a') parser exists but exposes no parse_order_response -> same
               fallback in ALL modes (capability missing, not a crash).
          (b)  parse_order_response raised -> a parser bug on data the
               framework is about to book. Live mode fails closed with
               CriticalAccountingError (VIB-3159 contract, same policy as
               _handle_extract_error); paper / backtest keeps the VIB-3706
               warn-and-fallback so a parser bug cannot silently drop the
               only fill data the strategy will ever see.

        A structured TradeResult(success=False) from the parser (caller
        handles it) still falls back in all modes — the parser deliberately
        reported unparseable data rather than crashing.

        Args:
            result: ExecutionResult whose extraction_warnings list is appended.
            offchain_protocol: Protocol key used to look up the parser.
            order_dict: OrderResponse-shaped dict passed to parse_order_response.
            intent_type: ``"PREDICTION_BUY"`` or ``"PREDICTION_SELL"`` (for warnings).
            value_field: Field name for CriticalAccountingError in live mode.

        Returns:
            The TradeResult on success, None for every fallback-worthy class.

        Raises:
            CriticalAccountingError: live mode only, when parse_order_response raises.
        """
        parser = None
        try:
            parser = self.parser_registry.get(offchain_protocol)
        except ValueError as exc:
            warning = (
                f"Enrichment fallback: {intent_type} has no receipt parser registered "
                f"for protocol '{offchain_protocol}' ({exc}); falling back to direct prediction_fill read"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)

        if parser is None:
            return None

        parse_method = getattr(parser, "parse_order_response", None)
        if not callable(parse_method):
            warning = (
                f"Enrichment fallback: {intent_type} parser {type(parser).__name__} "
                f"has no parse_order_response; falling back to direct prediction_fill read"
            )
            logger.warning(warning)
            result.extraction_warnings.append(warning)
            return None

        try:
            return parse_method(order_dict)
        except Exception as exc:
            message = (
                f"Enrichment fallback: {intent_type} parser.parse_order_response "
                f"raised ({type(exc).__name__}: {exc}); falling back to direct prediction_fill read"
            )
            if self.live_mode:
                # Live fills fail closed when the parser crashes because the framework
                # cannot certify the value it is about to book.

                logger.error(message)
                raise CriticalAccountingError(
                    message,
                    field_name=value_field,
                    intent_type=intent_type,
                    protocol=offchain_protocol or None,
                    original=exc,
                ) from exc
            logger.warning(message)
            result.extraction_warnings.append(message)
            return None

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
            setup transactions. Present only when every ``setup_txs`` entry
            carries a parseable ``total_cost_wei``; omitted (unmeasured)
            otherwise (a structured warning is appended to
            ``extraction_warnings``).
          - ``gas_cost_usd`` (Decimal | None): same value converted via the
            compiler-resolved MATIC USD price. Additionally omitted whenever
            ``gas_cost_native_wei`` is omitted. None (omitted) when the price
            could not be resolved (a structured warning is appended to
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
            result.extracted_data["setup_tx_count"] = len(setup_txs)
            extracted.add("setup_tx_count")

            total_wei = Decimal("0")
            malformed_index: int | None = None
            for idx, tx in enumerate(setup_txs):
                raw_cost = getattr(tx, "total_cost_wei", None)
                if raw_cost in (None, ""):
                    malformed_index = idx
                    break
                try:
                    total_wei += Decimal(str(raw_cost))
                except (InvalidOperation, ValueError, ArithmeticError):
                    malformed_index = idx
                    break

            if malformed_index is not None:
                # A partial setup-gas sum is unmeasured, not zero. If any transaction
                # cost is malformed, omit both native and USD aggregates.

                bad_tx = setup_txs[malformed_index]
                warning = (
                    f"Enrichment incomplete: {intent_type} setup_txs[{malformed_index}] "
                    f"(tx_hash={getattr(bad_tx, 'tx_hash', None) or '<unknown>'}) has "
                    f"malformed total_cost_wei={getattr(bad_tx, 'total_cost_wei', None)!r}; "
                    f"gas_cost_native_wei and gas_cost_usd omitted (unmeasured)"
                )
                logger.warning(warning)
                result.extraction_warnings.append(warning)
            else:
                result.extracted_data["gas_cost_native_wei"] = total_wei
                extracted.add("gas_cost_native_wei")

                # Missing or invalid native-token price leaves USD gas unmeasured while
                # preserving independently measured cost fields.

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
                    # ``None`` means unknown; ``Decimal('0')`` means measured zero gas.

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

        order_id = order_id_fallback or getattr(prediction_fill, "order_id", None)
        side = "BUY" if intent_type == "PREDICTION_BUY" else "SELL"

        # Stringify numeric fields so the parser owns Decimal coercion.

        filled_shares_raw = getattr(prediction_fill, "filled_shares", Decimal("0"))
        requested_shares_raw = getattr(prediction_fill, "requested_shares", Decimal("0"))
        avg_fill_price_raw = getattr(prediction_fill, "avg_fill_price", None)
        status = getattr(prediction_fill, "status", None) or "UNKNOWN"

        order_dict: dict[str, Any] = {
            "orderID": order_id,
            "status": status,
            "side": side,
            # ``size`` is requested size, distinct from the measured fill size.
            "size": str(requested_shares_raw),
            "filledSize": str(filled_shares_raw),
        }
        if avg_fill_price_raw is not None:
            order_dict["avgPrice"] = str(avg_fill_price_raw)

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

        supported = getattr(parser, "SUPPORTED_EXTRACTIONS", None)
        if isinstance(supported, list | tuple | set | frozenset) and field not in supported:
            warning = (
                f"Parser {type(parser).__name__} does not declare support for '{field}' (expected by {intent_type})"
            )
            logger.info(warning)
            result.extraction_warnings.append(warning)
            return

        # Inspect the class hierarchy for tagged extractors. Instance lookup
        # would mistake dynamically generated Mock attributes for methods.

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

        # Framework metadata supplies human-unit expected output and other
        # generic hints; parser hooks may add connector-specific kwargs.
        # Hook failures are extraction errors because silently dropping their
        # accounting inputs would hide data loss.

        try:
            extract_kwargs = self._build_extract_kwargs_for_parser(
                parser,
                field,
                bundle_metadata,
                intent_type=intent_type,
            )
        except CriticalAccountingError:
            raise
        except Exception as exc:  # noqa: BLE001 - malformed parser hook is extraction-critical
            self._handle_extract_error(
                result,
                ExtractError(error=f"{type(exc).__name__}: {exc}", exception=exc),
                field,
                intent_type,
                parser,
                protocol,
            )
            return

        # Continue after ExtractError because a later receipt may succeed; only
        # escalate when none does. Aggregate fields collect every ExtractOk
        # before applying preferred-source selection.

        aggregate_preferred = _AGGREGATE_FIELDS.get(field)
        # Fields whose legs span transactions receive one ordered union of logs
        # instead of per-receipt extraction.

        if field in _MERGED_RECEIPT_FIELDS and len(receipts) > 1:
            receipts = [self._merge_receipt_logs(receipts)]
        candidates: list[Any] = []
        last_error: ExtractError | None = None
        for receipt in receipts:
            variant = self._invoke_extract(extract_method, parser, receipt, field, extract_kwargs)

            if isinstance(variant, ExtractOk):
                if aggregate_preferred is not None:
                    candidates.append(variant.value)
                    continue
                attached = self._attach_to_result(result, field, variant.value, intent_type)
                if attached:
                    logger.debug(f"Enrichment: extracted {field}={type(variant.value).__name__} from receipt")
                    return
                # A rejected type is not terminal; a later receipt may be valid.

                continue
            if isinstance(variant, ExtractError):
                last_error = variant
                continue

        if aggregate_preferred is not None and candidates:
            chosen = self._select_preferred_aggregate(candidates, aggregate_preferred)
            attached = self._attach_to_result(result, field, chosen, intent_type)
            if attached:
                chosen_source = getattr(chosen, "source", None)
                logger.debug(
                    f"Enrichment: extracted {field}={type(chosen).__name__} "
                    f"(aggregated across {len(candidates)} candidate(s), "
                    f"chosen source={chosen_source!r}, preferred={aggregate_preferred!r})"
                )
                return

        extraction_error = last_error or self._required_extraction_missing_error(
            parser=parser,
            field=field,
            intent_type=intent_type,
            receipt_count=len(receipts),
        )
        if extraction_error is not None:
            self._handle_extract_error(
                result,
                extraction_error,
                field,
                intent_type,
                parser,
                protocol,
            )
            return

        logger.debug(
            f"Enrichment: {field} missing from all {len(receipts)} receipt(s) "
            f"(parser={type(parser).__name__}, intent_type={intent_type})"
        )

    @staticmethod
    def _required_extraction_missing_error(
        *,
        parser: Any,
        field: str,
        intent_type: str,
        receipt_count: int,
    ) -> ExtractError | None:
        """Build a fail-closed error for a connector-declared required field."""
        required = getattr(parser, "REQUIRED_EXTRACTIONS_BY_INTENT", None)
        required_fields = required.get(intent_type, ()) if isinstance(required, dict) else ()
        if field not in required_fields:
            return None
        return ExtractError(
            error=(
                f"required extraction missing from all {receipt_count} receipt(s); "
                "the submitted intent cannot be tracked to terminal settlement"
            )
        )

    @staticmethod
    def _derive_lp_close_fees_from_siblings(chosen: Any, candidates: list[Any]) -> None:
        """Override ``fees0/fees1`` on a chosen ``collect``-tagged LP close
        candidate from a sibling ``decrease_liquidity`` candidate.

        Fires when ALL of:
          1. ``chosen`` is tagged ``source="collect"``.
          2. A non-self sibling tagged ``source="decrease_liquidity"`` is in
             ``candidates`` with populated ``amount{0,1}_collected``.

        Derivation: ``fees{i} = max(collect.amount{i}_collected -
        decrease.amount{i}_collected, 0)``. Clamped at zero to absorb
        pre-existing ``tokensOwed`` dust where decrease > collect.

        **Always overrides** when a decrease sibling exists — the parser's
        collect-only attribution (``fees = collect_amount``, treating the
        whole transfer as fees because no Burn was in the same receipt) is
        correct semantics for LP_COLLECT_FEES and the
        no-liquidity-but-owed-tokens scenario (compiler skips the decrease
        step when ``liquidity == 0``), but WRONG for split-tx LP_CLOSE
        where the principal lives in the decrease sibling receipt. The
        aggregator is the only layer that can tell them apart, so it
        always overrides when a sibling is present. See
        ``docs/internal/lp-close-may20.md`` §6.3.

        Mutates ``chosen`` in place. Falls back from
        ``object.__setattr__`` to direct attribute assignment on TypeError
        so frozen-dataclass instances still receive the derived values.
        """
        if getattr(chosen, "source", None) != "collect":
            return
        decrease_sib = next(
            (c for c in candidates if c is not chosen and getattr(c, "source", None) == "decrease_liquidity"),
            None,
        )
        if decrease_sib is None:
            # Without a decrease sibling, collect-only fee attribution is
            # authoritative; split closes must subtract principal below.
            return

        ResultEnricher._derive_one_fee(chosen, decrease_sib, "fees0", "amount0_collected")
        ResultEnricher._derive_one_fee(chosen, decrease_sib, "fees1", "amount1_collected")

    @staticmethod
    def _derive_one_fee(chosen: Any, decrease_sib: Any, fee_field: str, amount_field: str) -> None:
        """Set ``chosen.<fee_field> = max(chosen.<amount_field> - decrease_sib.<amount_field>, 0)``
        when both amount fields are populated. Always overrides any prior
        ``chosen.<fee_field>`` value — the caller has already decided this
        is the split-tx LP_CLOSE branch where the parser's single-receipt
        attribution is wrong."""
        c_amt = getattr(chosen, amount_field, None)
        d_amt = getattr(decrease_sib, amount_field, None)
        if c_amt is None or d_amt is None:
            return
        derived = max(c_amt - d_amt, 0)
        try:
            object.__setattr__(chosen, fee_field, derived)
        except (AttributeError, TypeError):
            setattr(chosen, fee_field, derived)

    @staticmethod
    def _select_preferred_aggregate(candidates: list[Any], preferred_source: str) -> Any:
        """Pick the preferred-``source`` candidate from a multi-receipt aggregate,
        backfilling complementary fields from sibling candidates.

        VIB-4310 — Slipstream LP close emits ``DecreaseLiquidity`` in receipt #1
        and ``Collect`` in receipt #2. The Collect amounts are the truth on
        transfer (principal + accrued fees); the DecreaseLiquidity amounts are
        principal-only.

        Naive "pick preferred wholesale" loses fields the preferred candidate
        cannot populate from its source receipt — most importantly
        ``liquidity_removed``, which only DecreaseLiquidity carries. Codex
        pushback on PR #2256: dropping it would write ``LP_CLOSE`` ledger rows
        with ``liquidity=None`` even though the value was parsed from
        receipt #1. Backfill any field that is ``None`` on the chosen
        candidate from the first sibling that populated it.

        Behaviour:
        * Pick the first candidate whose ``source`` matches ``preferred_source``;
          fall back to the first candidate when no tagged match exists
          (un-tagged single-tx parsers).
        * **LP_CLOSE fee derivation** (lp-close-may20.md): when both a
          ``"collect"``-tagged and a ``"decrease_liquidity"``-tagged candidate
          are present and the chosen (collect) candidate has
          ``fees0/1 is None``, derive
          ``fees{0,1} = collect.amount{0,1}_collected - decrease.amount{0,1}_collected``
          (clamped at zero). This is the only layer that has both sibling
          receipts visible and can disentangle principal from accrued fees on
          UniswapV3-fork split-tx closes (decreaseLiquidity + collect emitted
          as separate transactions). Without this derivation, a guard-only
          parser fix would silently drop real mainnet fees from the LP_CLOSE
          accounting event.
        * For each remaining ``None`` / empty-string field on the chosen
          candidate, look for a sibling with a populated value and adopt it.
          Non-``None`` fields on the chosen candidate are authoritative — never
          overwritten.
        """
        chosen: Any | None = None
        for candidate in candidates:
            if getattr(candidate, "source", None) == preferred_source:
                chosen = candidate
                break
        if chosen is None:
            chosen = candidates[0]

        ResultEnricher._derive_lp_close_fees_from_siblings(chosen, candidates)

        # Fill only empty fields from siblings; the preferred candidate remains
        # authoritative for every populated value.
        siblings = [c for c in candidates if c is not chosen]
        if not siblings:
            return chosen

        from dataclasses import fields, is_dataclass, replace

        if not is_dataclass(chosen):
            return chosen

        backfills: dict[str, Any] = {}
        for f in fields(chosen):
            current = getattr(chosen, f.name)
            if current is not None and current != "":
                continue
            for sibling in siblings:
                sibling_value = getattr(sibling, f.name, None)
                if sibling_value is not None and sibling_value != "":
                    backfills[f.name] = sibling_value
                    break

        if not backfills:
            return chosen
        try:
            # ``is_dataclass`` also accepts bare types, so replacement may fail at
            # runtime even though the selected value is dynamically typed.

            return replace(chosen, **backfills)  # type: ignore[type-var]
        except TypeError:
            # Mutable or non-replaceable dataclasses receive the same backfills
            # through direct assignment.

            for name, value in backfills.items():
                setattr(chosen, name, value)
            return chosen

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
        *,
        intent_type: str | None = None,
    ) -> dict[str, Any]:
        """Compute framework-owned extra kwargs for ``extract_<field>`` methods.

        VIB-3203 — swap_amounts extractors can consume an ``expected_out``
        Decimal (human units) to compute realized ``slippage_bps`` from
        ``(expected_out - actual_out) / expected_out``. The value comes from
        the compiler's ``ActionBundle.metadata["expected_output_human"]``.

        VIB-3204 — protocol_fees extractors for DEX swap intents can consume
        a ``fee_tier_bps`` int so they can compute the swap fee without
        re-reading on-chain pool metadata. Sourced from
        ``ActionBundle.metadata["selected_fee_tier"]``.

        Returns framework-generic kwargs only. Connector-specific parser
        kwargs are appended by :meth:`_build_extract_kwargs_for_parser`.
        """
        if field == "async_orders" and intent_type:
            # Intent type is authoritative for async order kind; event payloads may
            # be dynamically encoded and cannot safely define lifecycle semantics.

            return {"intent_type": intent_type}
        if not bundle_metadata:
            return {}
        if field == "swap_amounts":
            kwargs: dict[str, Any] = {}
            raw = bundle_metadata.get("expected_output_human")
            if raw is not None:
                try:
                    expected_out = Decimal(str(raw))
                    if expected_out.is_finite() and expected_out > 0:
                        kwargs["expected_out"] = expected_out
                except (InvalidOperation, TypeError, ValueError):
                    logger.debug("Could not coerce expected_output_human=%r to Decimal; skipping", raw)
            return kwargs
        if field == "protocol_fees":
            return ResultEnricher._build_protocol_fees_kwargs(bundle_metadata)
        if field == "bridge_data":
            # Bridge receipts encode ids and addresses, not stable user-facing names.
            # Thread compiler-resolved intent values rather than re-deriving them.

            bridge_kwargs: dict[str, Any] = {}
            for key in ("from_chain", "to_chain", "token", "amount", "bridge"):
                val = bundle_metadata.get(key)
                if val is not None and val != "":
                    bridge_kwargs[key] = val
            # Expected output is post-fee and optional for legacy parsers.

            out_amount = bundle_metadata.get("output_amount")
            if out_amount is not None:
                bridge_kwargs["expected_amount_out"] = out_amount
            return bridge_kwargs
        return {}

    def _build_extract_kwargs_for_parser(
        self,
        parser: Any,
        field: str,
        bundle_metadata: dict[str, Any] | None,
        *,
        intent_type: str | None = None,
    ) -> dict[str, Any]:
        """Merge framework-generic kwargs with optional parser-owned kwargs.

        Parser-owned hooks may only add disjoint kwargs. Framework-owned
        metadata such as ``expected_out`` and fee hints must not be shadowed by
        connector hooks because those values are compiler/orchestrator facts.
        """
        kwargs = self._build_extract_kwargs(field, bundle_metadata, intent_type=intent_type)
        parser_kwargs = self._build_parser_extract_kwargs(parser, field, bundle_metadata)
        if not parser_kwargs:
            return kwargs
        duplicate_keys = kwargs.keys() & parser_kwargs.keys()
        if duplicate_keys:
            keys = ", ".join(sorted(duplicate_keys))
            raise ValueError(
                f"{type(parser).__name__}.build_extract_kwargs() must not return "
                f"framework-owned extraction kwarg(s): {keys}"
            )
        return {**kwargs, **parser_kwargs}

    @staticmethod
    def _build_parser_extract_kwargs(
        parser: Any,
        field: str,
        bundle_metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Ask a parser for connector-specific extraction kwargs when it opts in."""
        if not bundle_metadata or not ResultEnricher._class_has_method(parser, "build_extract_kwargs"):
            return {}

        raw_kwargs = parser.build_extract_kwargs(field=field, bundle_metadata=bundle_metadata)
        if raw_kwargs is None:
            return {}
        if not isinstance(raw_kwargs, dict):
            raise TypeError(
                f"{type(parser).__name__}.build_extract_kwargs() must return dict[str, Any] or None, "
                f"got {type(raw_kwargs).__name__}"
            )
        return dict(raw_kwargs)

    @staticmethod
    def _build_protocol_fees_kwargs(bundle_metadata: dict[str, Any]) -> dict[str, Any]:
        """Compose ``extract_protocol_fees`` kwargs from compiler metadata.

        Two values feed this signature today:

        * ``fee_tier_bps`` — DEX pool fee tier (VIB-3204), sourced from
          ``ActionBundle.metadata["selected_fee_tier"]``.
        * ``protocol_fee_usd`` — aggregator integrator fee in USD
          (VIB-3210), sourced from
          ``ActionBundle.metadata["protocol_fee_usd"]``. LiFi captures this
          at compile time from ``quote.estimate.total_fee_usd``; Enso does
          not have a USD-denominated quote field yet, so the key stays
          unset until adapter-side USD conversion ships.

        Extracted from ``_build_extract_kwargs`` so the outer function stays
        under the CRAP threshold as new fields land.
        """
        kwargs: dict[str, Any] = {}
        raw_tier = bundle_metadata.get("selected_fee_tier")
        if raw_tier not in (None, ""):
            try:
                kwargs["fee_tier_bps"] = int(str(raw_tier))
            except (TypeError, ValueError):
                logger.debug(
                    "Could not coerce selected_fee_tier=%r to int; skipping",
                    raw_tier,
                )
        raw_fee_usd = bundle_metadata.get("protocol_fee_usd")
        if raw_fee_usd not in (None, ""):
            try:
                fee_usd = Decimal(str(raw_fee_usd))
                if fee_usd.is_finite():
                    # Thread finite values including negatives. The parser validates sign,
                    # so filtering here would conceal corrupted upstream data.

                    kwargs["protocol_fee_usd"] = fee_usd
            except (InvalidOperation, TypeError, ValueError):
                logger.debug(
                    "Could not coerce protocol_fee_usd=%r to Decimal; skipping",
                    raw_fee_usd,
                )
        return kwargs

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
                    # Retry without optional kwargs only when the signature rejected their
                    # names; an unrelated TypeError remains extraction-critical.

                    if any(k in str(exc) for k in kwargs):
                        raw = extract_method(receipt)
                    else:
                        raise
            else:
                raw = extract_method(receipt)
        except CriticalAccountingError:
            # Never swallow a nested fail-closed signal.
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

    @staticmethod
    def _fill_v4_lp_open_current_tick_from_metadata(
        result: ExecutionResult,
        bundle_metadata: dict[str, Any] | None,
    ) -> None:
        """VIB-4636 — fill V4 ``LPOpenData.current_tick`` from compiler metadata.

        V4 pure-mint receipts (PositionManager-mediated) emit no Swap event,
        so ``UniswapV4ReceiptParser.extract_lp_open_data`` leaves
        ``current_tick=None``. The adapter stamps the compile-time tick on
        ``ActionBundle.metadata["compile_time_current_tick"]``; the mint
        itself never moves price, so that value is correct for post-mint
        accounting (caveat: an interleaving tx between compile and mint
        could move the pool — same caveat applies to the V3 slot0 fallback,
        which queries at a slightly different block than the mint).

        Authoritative on-chain extraction always wins: this only fires when
        the parser left ``current_tick`` ``None``. No-ops on every other
        shape (no ``lp_open_data``, missing metadata key, already populated).
        """
        if not bundle_metadata:
            return
        compile_tick = bundle_metadata.get("compile_time_current_tick")
        if compile_tick is None:
            return
        lp_open = result.extracted_data.get("lp_open_data")
        if not isinstance(lp_open, LPOpenData):
            return
        if lp_open.current_tick is not None:
            return
        try:
            tick_value = int(compile_tick)
        except (TypeError, ValueError):
            logger.warning(
                "V4 LP_OPEN current_tick fallback: ignoring non-integer "
                "bundle metadata value compile_time_current_tick=%r",
                compile_tick,
            )
            return
        source = bundle_metadata.get("compile_time_current_tick_source", "unknown")
        logger.info(
            "filled V4 LP_OPEN current_tick from compile-time metadata (tick=%d source=%s)",
            tick_value,
            source,
        )
        import dataclasses

        result.extracted_data["lp_open_data"] = dataclasses.replace(lp_open, current_tick=tick_value)

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

        if field == "position_id" and isinstance(value, int | str):
            if not self._attach_position_id(result, value):
                # Invalid string ids remain in ``extracted_data`` but do not occupy the
                # validated top-level slot or trigger redundant receipt scans.

                return True
        elif field == "swap_amounts" and isinstance(value, SwapAmounts):
            self._attach_swap_amounts(result, value)
        elif field in _STRICT_TYPED_FIELDS:
            # Strict typed slots reject invalid values and continue scanning later
            # receipts. Accepted values are exposed both to callbacks and the
            # generic extracted-data consumers.

            attr, validator, type_label = _STRICT_TYPED_FIELDS[field]
            if not validator(value):
                logger.warning(
                    f"Enrichment: parser returned non-{type_label} value for {field!r} "
                    f"(type={type(value).__name__}); ignoring and continuing receipt scan"
                )
                return False
            setattr(result, attr, value)

        # Keep typed values mirrored for generic consumers such as declared
        # money-leg dispatch.

        result.extracted_data[field] = value
        return True

    def _attach_position_id(self, result: ExecutionResult, value: int | str) -> bool:
        """Set ``result.position_id`` from a validated int/str value.

        Returns ``False`` (without setting the slot) for a string id that is
        neither a hex address / bytes32 hash nor a finite decimal — in that
        case the raw value is still mirrored into ``extracted_data`` so no
        information is lost. Returns ``True`` once the top-level slot is set.
        """
        if isinstance(value, str):
            # Pool addresses and bytes32 trade hashes are valid position ids.

            is_hex_address = bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", value))
            is_bytes32 = bool(re.fullmatch(r"0x[a-fA-F0-9]{64}", value))
            if not (is_hex_address or is_bytes32):
                try:
                    parsed = Decimal(value)
                    if not parsed.is_finite():
                        logger.warning(f"Ignoring non-finite string position_id {value!r}")
                        result.extracted_data["position_id"] = value
                        return False
                except InvalidOperation:
                    logger.warning(f"Ignoring invalid string position_id {value!r}: not a valid decimal or address")
                    result.extracted_data["position_id"] = value
                    return False
        result.position_id = value
        return True

    @staticmethod
    def _attach_swap_amounts(result: ExecutionResult, value: SwapAmounts) -> None:
        """Set ``result.swap_amounts`` and surface unresolved-decimal warnings."""
        result.swap_amounts = value
        # Unresolved decimals remain unmeasured even if a legacy 18-decimal
        # display fallback exists; downstream money rows exclude those amounts.

        unresolved_sides = [
            side
            for side, ok in (
                ("token_in", value.amount_in_decimal_resolved),
                ("token_out", value.amount_out_decimal_resolved),
            )
            if not ok
        ]
        if unresolved_sides:
            result.extraction_warnings.append(
                f"swap_amounts decimals unresolved for {', '.join(unresolved_sides)}; "
                "decimal amounts use the legacy 18-decimal fallback and are "
                "excluded from ledger/sidecar amounts (VIB-3164)"
            )

    def _has_extracted(self, result: ExecutionResult, field: str) -> bool:
        """Check if a field was successfully extracted.

        Args:
            result: ExecutionResult to check
            field: Field name

        Returns:
            True if field was extracted
        """

        if field == "position_id":
            return result.position_id is not None
        if field == "swap_amounts":
            return result.swap_amounts is not None
        if field == "lp_close_data":
            return result.lp_close_data is not None
        if field == "bridge_data":
            return getattr(result, "bridge_data", None) is not None

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
            # Render unresolved decimal amounts as unknown, not measured zero.

            in_str = f"{sa.amount_in_decimal}" if sa.amount_in_decimal is not None else "?"
            out_str = f"{sa.amount_out_decimal}" if sa.amount_out_decimal is not None else "?"
            return f"{in_str} -> {out_str}"
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

        if hasattr(intent, "intent_type"):
            intent_type = intent.intent_type

            if hasattr(intent_type, "value"):
                return str(intent_type.value).upper()
            return str(intent_type).upper()

        class_name = type(intent).__name__
        if class_name.endswith("Intent"):
            class_name = class_name[:-6]

        # Split CamelCase only at new words so acronyms such as ``LP`` remain
        # intact in upper-snake intent names.

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
        # Recursive enrichment must not wrap the parser twice.
        if getattr(original, "_is_cached_wrapper", False):
            return

        cache: dict[tuple, Any] = {}

        def cached_parse_receipt(receipt: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
            # Include positional and keyword context in the cache key. The same
            # receipt can produce different parses under different extractor hints.

            tx_hash = receipt.get("transactionHash") or receipt.get("tx_hash")
            if tx_hash is None:
                tx_hash = id(receipt)
            kwarg_key = tuple(sorted((k, str(v)) for k, v in kwargs.items()))
            arg_key = tuple(str(a) for a in args)
            key = (str(tx_hash), arg_key, kwarg_key)
            if key not in cache:
                cache[key] = original(receipt, *args, **kwargs)
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

    def _build_parser_kwargs(self, protocol: str, chain: str) -> dict[str, Any]:
        """Build kwargs for ReceiptParserRegistry.get(protocol, **kwargs).

        VIB-4477 (T08): thread ``pool_key_lookup`` into parsers whose
        connector declares it in ``receipt_parser_kwargs`` (the V4 parser,
        which resolves ``ModifyLiquidity.pool_id`` -> canonical ``PoolKey``
        via the gateway; VIB-4851 C3 moved the opt-in onto the manifest).
        Without this, V4 LP_CLOSE events drop with a structured
        ``missing_pool_key_lookup`` warning and the lp_accounting pipeline
        never sees V4 events. The kwarg is only sent to declaring parsers to
        keep other parsers' caching behaviour unchanged --
        ``ReceiptParserRegistry.get`` bypasses its protocol cache when any
        kwarg is provided (see ``_load_builtin``).
        """
        kwargs: dict[str, Any] = {"chain": chain}
        if self._pool_key_lookup is not None and protocol.lower() in _pool_key_lookup_protocols():
            kwargs["pool_key_lookup"] = self._pool_key_lookup
        # Inject lookup callbacks only into connectors that declare them; kwargs
        # bypass the registry's protocol-only cache.

        if self._pool_meta_lookup is not None and protocol.lower() in _pool_meta_lookup_protocols():
            kwargs["pool_meta_lookup"] = self._pool_meta_lookup
        return kwargs

    @staticmethod
    def _merge_receipt_logs(receipts: list[dict[str, Any]]) -> dict[str, Any]:
        """Union the ``logs`` of every receipt into one synthetic receipt (VIB-5416).

        A multi-transaction intent (e.g. a Lido wrapped STAKE: submit ETH→stETH,
        then wrap stETH→wstETH) splits its money legs across txs. A receipt parser
        that scans logs by contract address + event topic can declare the whole
        intent's legs (ETH input + wstETH output) ONLY if it sees every tx's logs
        at once. This concatenates the per-tx ``logs`` in order and carries the
        first receipt's scalar context (``from_address`` / ``status`` / their
        camelCase aliases) so the parser's address-based wrap/unwrap disambiguation
        still works. Does not mutate the input receipts.

        The merged receipt is stamped with a SYNTHETIC ``transactionHash`` derived
        from every constituent tx hash. The enricher installs a ``parse_receipt``
        cache keyed on ``transactionHash`` (``_install_parse_cache``); without a
        distinct key the merged call would inherit the first tx's hash and the
        cache would return that tx's STALE per-tx parse (e.g. ``wraps=0`` for a
        wrapped STAKE), silently defeating the merge. The key is set-unique (a
        function of all constituent hashes) so it collides with neither any single
        tx nor a different intent's merge, while staying stable within one
        enrichment so caching still elides repeat parses of the merged receipt.
        """
        merged: dict[str, Any] = {}
        # Preserve the first receipt's scalar context for parsers that infer
        # direction from sender or status.

        if receipts and isinstance(receipts[0], dict):
            for key, value in receipts[0].items():
                if key != "logs":
                    merged[key] = value
        all_logs: list[Any] = []
        constituent_hashes: list[str] = []
        for receipt in receipts:
            if not isinstance(receipt, dict):
                continue
            logs = receipt.get("logs")
            if isinstance(logs, list):
                all_logs.extend(logs)
            tx_hash = receipt.get("transactionHash") or receipt.get("tx_hash")
            constituent_hashes.append(str(tx_hash) if tx_hash is not None else "")
        merged["logs"] = all_logs
        # A set-unique synthetic hash prevents merged logs from reusing a stale
        # single-receipt cache entry.
        synthetic_hash = "merged:" + "|".join(constituent_hashes)
        merged["transactionHash"] = synthetic_hash
        merged["tx_hash"] = synthetic_hash
        return merged

    def _collect_receipts(self, result: ExecutionResult, trading_wallet: str = "") -> list[dict[str, Any]]:
        """Collect receipts from successful transaction results.

        Args:
            result: ExecutionResult containing transaction results
            trading_wallet: VIB-6043. The effective execution address for this
                run (``ExecutionContext.wallet_address`` — the **Safe** under
                Safe / Zodiac execution, the EOA otherwise). Stamped onto every
                receipt under ``TRADING_WALLET_KEY`` so receipt parsers resolve
                the strategy's money legs against the address that actually
                holds the tokens instead of ``receipt["from"]`` (the agent EOA
                that merely signs ``execTransactionWithRole``). Empty leaves
                the receipt unstamped and parsers fall back exactly as before.

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

            receipt_dict: dict[str, Any]
            if hasattr(receipt, "to_dict"):
                receipt_dict = receipt.to_dict()
            elif hasattr(receipt, "logs"):
                # Preserve sender data used for Transfer-based decimal resolution.

                receipt_dict = {"logs": receipt.logs}
                for attr in ("from_address", "status"):
                    if hasattr(receipt, attr):
                        receipt_dict[attr] = getattr(receipt, attr)
            elif isinstance(receipt, dict):
                receipt_dict = receipt
            else:
                continue

            # Supply camelCase aliases without overwriting parser-ready values.

            for snake_key, camel_key in _SNAKE_TO_CAMEL.items():
                if snake_key in receipt_dict and camel_key not in receipt_dict:
                    receipt_dict[camel_key] = receipt_dict[snake_key]

            # Stamp a copy; the ledger and persistence path retain the receipt.

            receipts.append(stamp_trading_wallet(receipt_dict, trading_wallet))

        return receipts

    @staticmethod
    def _collect_additional_receipts(
        receipts: tuple[dict[str, Any], ...], trading_wallet: str = ""
    ) -> list[dict[str, Any]]:
        """Normalize successful post-submission receipts for enrichment.

        ``trading_wallet`` is stamped exactly as in :meth:`_collect_receipts`
        (VIB-6043) — a keeper-executed order receipt is parsed by the same
        parsers and needs the same Safe-aware wallet.
        """
        collected: list[dict[str, Any]] = []
        for receipt in receipts:
            if not isinstance(receipt, dict):
                continue
            status_raw = receipt.get("status", 1)
            try:
                status = int(status_raw, 0) if isinstance(status_raw, str) else int(status_raw)
            except (TypeError, ValueError):
                logger.warning("Ignoring post-submission receipt with invalid status %r", status_raw)
                continue
            if status != 1:
                logger.warning("Ignoring failed post-submission receipt with status %r", status_raw)
                continue
            receipt_dict = dict(receipt)
            receipt_dict["status"] = status
            for snake_key, camel_key in _SNAKE_TO_CAMEL.items():
                if snake_key in receipt_dict and camel_key not in receipt_dict:
                    receipt_dict[camel_key] = receipt_dict[snake_key]
            for numeric_key in (
                "blockNumber",
                "cumulativeGasUsed",
                "effectiveGasPrice",
                "gasUsed",
                "transactionIndex",
            ):
                raw = receipt_dict.get(numeric_key)
                if not isinstance(raw, str):
                    continue
                try:
                    receipt_dict[numeric_key] = int(raw, 0)
                except ValueError:
                    logger.warning(
                        "Ignoring post-submission receipt with invalid %s %r",
                        numeric_key,
                        raw,
                    )
                    break
            else:
                collected.append(stamp_trading_wallet(receipt_dict, trading_wallet))
        return collected


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


__all__ = [
    "CriticalAccountingError",
    "ExtractError",
    "ExtractMissing",
    "ExtractOk",
    "ResultEnricher",
    "enrich_result",
    "get_enricher",
]
