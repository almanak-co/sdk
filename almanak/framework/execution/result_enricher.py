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
from .extracted_data import BridgeData, LPCloseData, LPOpenData, ProtocolFees, SwapAmounts
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


# Strictly-typed enriched fields that get a real top-level slot on
# ``ExecutionResult`` (read directly by strategy callbacks) AND a mirror in
# ``extracted_data``. Each entry is ``(result_attr, type_validator, type_label)``.
# A value failing its validator is rejected with a warning and the enricher
# keeps scanning the bundle (``return False``) rather than treating the
# rejection as a terminal attach. ``primitive_money_legs`` / ``bin_ids`` slots
# were the VIB-159 gap: declared / expected but never assigned to the result.
_STRICT_TYPED_FIELDS: dict[str, tuple[str, Callable[[Any], bool], str]] = {
    "lp_close_data": ("lp_close_data", lambda v: isinstance(v, LPCloseData), "LPCloseData"),
    "bridge_data": ("bridge_data", lambda v: isinstance(v, BridgeData), "BridgeData"),
    "protocol_fees": ("protocol_fees", lambda v: isinstance(v, ProtocolFees), "ProtocolFees"),
    "bin_ids": (
        "bin_ids",
        # ``bool`` is an ``int`` subclass in Python, so an explicit
        # ``not isinstance(b, bool)`` guard is required to keep ``[True]`` /
        # ``[False]`` from masquerading as valid bin identifiers.
        lambda v: isinstance(v, list) and all(isinstance(b, int) and not isinstance(b, bool) for b in v),
        "list[int]",
    ),
    "primitive_money_legs": ("primitive_money_legs", _is_primitive_money_legs, "PrimitiveMoneyLegs"),
}

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


# VIB-4310 — Fields whose extraction must scan ALL receipts in a bundle and
# select the preferred-``source``-tagged variant rather than returning on
# first ExtractOk. Two-transaction protocol flows (e.g. Aerodrome Slipstream
# ``decreaseLiquidity`` → ``collect``) emit complementary data across separate
# receipts: receipt #1 carries DecreaseLiquidity (principal unlocked), receipt
# #2 carries Collect (principal + accrued fees actually transferred).
# First-match semantics return the decrease-sourced extraction and silently
# drop accrued fees from the registry payload.
#
# The map's value is the ``source`` tag this aggregator prefers. Parser-side
# producers (see ``AerodromeSlipstreamReceiptParser.extract_lp_close_data``)
# stamp every emitted value with the source it was decoded from. Producers
# that leave ``source=None`` (single-tx parsers) are unaffected — the picker
# falls back to first-found semantics for un-tagged candidates.
_AGGREGATE_FIELDS: dict[str, str] = {
    "lp_close_data": "collect",
}

# VIB-5416 — fields whose extractor must see the UNION of all the intent's
# transaction logs, not a single tx. A multi-transaction intent can split its
# money legs across txs: a Lido wrapped STAKE submits ETH→stETH in tx 1 and wraps
# stETH→wstETH in a later tx, so neither receipt alone carries BOTH the ETH input
# leg AND the wstETH output leg. The per-receipt first-OK loop would attach the
# tx-1 (ETH→stETH) legs and stop, mislabelling the ledger ``token_out`` as the
# intermediate ``stETH`` while the wallet (and the teardown swap-back) actually
# holds ``wstETH`` — which strands the teardown swap as ``untracked_token``
# (VIB-5416). Extracting from the merged-logs receipt lets the parser declare the
# true ETH→wstETH legs. Parsers filter logs by contract+topic, so the extra logs
# from sibling txs are inert for single-tx intents.
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
    # Deferred import: connector discovery must never run at module import.
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
        # NOTE: Do NOT re-add bin_ids here without first migrating LPOpenData per
        # VIB-4320 follow-up. TJ V2 bin_ids is in EXTRACTION_SPECS_BY_PROTOCOL.
        "LP_OPEN": ["position_id", "tick_lower", "tick_upper", "liquidity", "protocol_fees", "lp_open_data"],
        "LP_CLOSE": [
            "lp_close_data",
            "amount0_collected",
            "amount1_collected",
            "fees0",
            "fees1",
            "protocol_fees",
        ],
        # === LP Fee Collection ===
        # NOTE: Do NOT re-add bin_ids here without first migrating LPOpenData per
        # VIB-4320 follow-up. TJ V2 bin_ids is in EXTRACTION_SPECS_BY_PROTOCOL.
        "LP_COLLECT_FEES": ["fees0", "fees1", "protocol_fees"],
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
        # PERP_CANCEL_ORDER (VIB-5568) — cancel a pending order, recover collateral.
        # No position/PnL fields to extract: the OrderVault refund (committed
        # collateral + unspent exec fee) is a wallet-balance delta captured by the
        # portfolio snapshot, and the ORDER_CANCELLED event is surfaced by the GMX
        # parser's order_events. Empty spec (like HOLD) → enrichment short-circuits.
        "PERP_CANCEL_ORDER": [],
        # PERP_WITHDRAW (VIB-5617) — withdraw free margin off the venue's off-chain
        # account back to L1 (Hyperliquid: a CoreWriter spotSend HyperCore→HyperEVM
        # USDC bridge). A cash movement, not a trade: no position/PnL fields to
        # extract. The credited amount (net of the ~$1 HyperCore withdraw fee) is a
        # wallet-balance delta captured by the portfolio snapshot; the HyperCore
        # settlement is async off-EVM (the EVM tx only emits RawAction). Empty spec
        # (like HOLD / PERP_CANCEL_ORDER) → enrichment short-circuits.
        "PERP_WITHDRAW": [],
    }

    # VIB-4320 — Per-protocol overlay appended onto the generic ``EXTRACTION_SPECS``
    # for protocol-specific fields that are not implemented by every parser. Keeps
    # the generic spec protocol-neutral (no Uniswap-V3 / PancakeSwap-V3 warnings
    # for ``bin_ids``) while preserving the flat ``extracted_data["bin_ids"]``
    # contract for TraderJoe V2 consumers (LPPositionTracker + leveraged_lp demo).
    #
    # Overlay semantics: ``_merge_spec_with_overlay`` appends overlay fields at the
    # tail of the base spec with order-preserving dedup. Base fields always come
    # first; ``protocol=None`` returns the base spec unchanged.
    #
    # Follow-up VIB-4344 — Uniswap/PancakeSwap V3 ``LP_COLLECT_FEES`` still warns
    # for ``fees0`` / ``fees1`` because those parsers do not implement
    # ``extract_fees0`` / ``extract_fees1`` yet; the right fix is to implement
    # them (not move them into per-protocol overlays). Out of scope for VIB-4320.
    EXTRACTION_SPECS_BY_PROTOCOL: dict[str, dict[str, list[str]]] = {
        "traderjoe_v2": {
            # VIB-4634 — ``lp_open_data`` / ``lp_close_data`` carry the
            # canonical LBPair ``pool_address`` (stamped by the receipt parser
            # from the DepositedToBins / WithdrawnFromBins / ClaimedFees
            # emitter — the LBPair itself emits those events). Without it the
            # LP accounting handler drops every TraderJoe V2 LP event because
            # the ``tokenX/tokenY/<binStep>`` position-key descriptor is
            # rejected as a Uniswap-V3 fee tier. ``lp_open_data`` is already in
            # the base LP_OPEN spec; LP_CLOSE already carries ``lp_close_data``.
            # LP_COLLECT_FEES has neither in the base spec, so the LBPair
            # address must be added here for the fee-harvest path (the parser's
            # ``extract_lp_close_data`` emits a principal-zero LPCloseData
            # carrying only the pool_address for a ClaimedFees-only receipt).
            # VIB-5414 — declare the LP_OPEN money legs too (the symmetric mirror of
            # the LP_CLOSE entry below). ``extract_primitive_money_legs`` now returns
            # two INPUT legs (the deposited token0/token1 notional) for a
            # ``DepositedToBins`` receipt, so the LP handler can compute a MEASURED
            # ``cost_basis_usd`` instead of ``0`` — which kept the gateway state
            # manager degrading the snapshot HIGH→ESTIMATED for the whole hold (same
            # class as the Uniswap-V3 ``deployed_capital=0`` family VIB-3883/3894).
            # ``bin_ids`` stays for the LPPositionTracker / leveraged_lp consumers.
            "LP_OPEN": ["bin_ids", "primitive_money_legs"],
            "LP_COLLECT_FEES": ["bin_ids", "lp_close_data"],
            # VIB-5221 (US-011) — declare the LP_CLOSE money legs as a typed
            # ``PrimitiveMoneyLegs`` (``TraderJoeV2ReceiptParser.extract_primitive_money_legs``).
            # The enricher's generic ``extracted_data[field] = value`` lands it at
            # ``extracted_data["primitive_money_legs"]`` — the exact key the US-009
            # ledger dispatcher (``_declared_money_legs``) already prefers over the
            # legacy guesser. This makes TJ V2 LP_CLOSE's token0/token1 + amounts a
            # property of the typed contract instead of the #2894 intent-pool-
            # descriptor threading (blueprint 27 §6.6 / 05 §7). Additive overlay:
            # the base LP_CLOSE spec (lp_close_data / fees / protocol_fees) is kept.
            "LP_CLOSE": ["primitive_money_legs"],
        },
        # VIB-4637 — a Uniswap V4 fees-only ``LP_COLLECT_FEES`` compiles to
        # ``DECREASE_LIQUIDITY(liquidity=0) + TAKE_PAIR``, so the PoolManager
        # emits a zero-delta ``ModifyLiquidity`` and NO principal-removing
        # burn. The base ``LP_COLLECT_FEES`` spec (``fees0`` / ``fees1`` /
        # ``protocol_fees``) carries no ``pool_address``, so the LP accounting
        # handler had nothing to resolve and dropped the event entirely (the
        # ``tokenX/tokenY/<fee>`` V4 position-key tail is rejected as a V3
        # fee-tier descriptor). Adding ``lp_close_data`` routes the receipt
        # through ``UniswapV4ReceiptParser.extract_lp_close_data``, whose
        # fees-only branch stamps the canonical 32-byte V4 PoolId on a
        # principal-zero ``LPCloseData`` so the handler books the event.
        # Mirrors the TraderJoe V2 collect overlay (VIB-4634).
        # The overlay is additive (``_merge_spec_with_overlay``): the base
        # ``LP_COLLECT_FEES`` fields (``fees0`` / ``fees1`` / ``protocol_fees``)
        # are kept; only ``lp_close_data`` is appended.
        "uniswap_v4": {
            "LP_COLLECT_FEES": ["lp_close_data"],
        },
        # Morpho Blue isolated markets emit ``SupplyCollateral`` for the
        # collateral leg of a market — a distinct on-chain event from the
        # loan-side ``Supply``. The generic spec only asks for
        # ``supply_amount`` (loan-side); collateral receipts return ``None``.
        # This overlay surfaces the collateral amount so downstream
        # lending-accounting can book the typed event with the true on-chain
        # assets value. See MorphoMay15 §6.2 (F2). VIB-4635 wires the symmetric
        # ``WITHDRAW`` leg: collateral withdrawals route through
        # ``withdrawCollateral(...)`` and emit ``WithdrawCollateral`` (not the
        # loan-side ``Withdraw``), so the generic ``withdraw_amount`` key is
        # absent. The Morpho parser now exposes
        # ``extract_withdraw_collateral_amount``, surfaced here as
        # ``withdraw_collateral_amount`` so the lending handler can scale it.
        "morpho_blue": {
            "SUPPLY": ["supply_collateral_amount"],
            "WITHDRAW": ["withdraw_collateral_amount"],
        },
        # Compound V3 collateral supplies route through
        # ``Comet.supplyCollateral(asset, amount)`` and emit ``SupplyCollateral``
        # — a distinct on-chain event from the base-asset ``Supply``. The generic
        # spec only asks for ``supply_amount`` (base-asset leg); a collateral
        # receipt has no ``Supply`` event, so that extractor returns ``None`` and
        # the persisted ``LendingAccountingEvent.amount_token`` came back ``None``
        # even though the supplied amount is known exactly on-chain (VIB-4633
        # Finding A). This overlay surfaces the collateral amount as
        # ``supply_collateral_amount`` (via the Compound parser's
        # ``extract_supply_collateral_amount``); the lending handler's existing
        # ``_COLLATERAL_FALLBACK_BY_INTENT["SUPPLY"]`` then scales it. Mirrors the
        # morpho_blue collateral overlay above. Base-asset ``Comet.supply()`` is
        # unaffected — it still populates ``supply_amount``.
        "compound_v3": {
            "SUPPLY": ["supply_collateral_amount"],
        },
        # VIB-5220 — Lido is the first connector migrated onto the
        # ``PrimitiveMoneyLeg`` contract. Its parser's
        # ``extract_primitive_money_legs`` returns a typed ``PrimitiveMoneyLegs``
        # (INPUT=ETH staked, OUTPUT=stETH/wstETH minted); this overlay surfaces it
        # under ``extracted_data["primitive_money_legs"]`` — the seam the US-009
        # ledger dispatcher (``_extract_tokens_and_amounts``) prefers over the
        # legacy intent-attribute guesser. Kept under the lido overlay (not the
        # generic STAKE spec) so other staking connectors that have not yet
        # migrated (ethena / gimo) emit no spurious SUPPORTED_EXTRACTIONS warning.
        "lido": {
            "STAKE": ["primitive_money_legs"],
        },
        # NOTE: prefer the connector-owned ``EXTRA_EXTRACTIONS_BY_INTENT`` parser
        # attribute (merged generically by ``_with_parser_extra_extractions``) for
        # NEW connector-specific extractions — it keeps the protocol name out of
        # this framework file (guarded by ``test_connector_descriptor``). This
        # per-protocol overlay table is the older, not-yet-migrated mechanism.
    }

    # VIB-4434 W2 — Per-protocol REMOVE table; companion to
    # ``EXTRACTION_SPECS_BY_PROTOCOL``. Fields listed here are *removed* from
    # the effective spec after the additive overlay is applied. Use this when
    # a protocol legitimately does not expose a base field, so the SUPPORTED_
    # EXTRACTIONS capability check inside ``_extract_field`` would otherwise
    # emit a chronic info-warning on every receipt.
    #
    # Narrowing dimensions (Aerodrome Classic + Slipstream, Uniswap V3 forks,
    # LP_OPEN and LP_CLOSE):
    #
    # * ``"aerodrome"`` — Classic V1 (Solidly fork) — fungible LP, no NFT, no
    #   ticks, no structured ``lp_open_data`` and no standalone ``amount0_collected`` /
    #   ``amount1_collected`` / ``fees0`` / ``fees1`` extractors (those collected
    #   amounts live INSIDE ``lp_close_data`` only, on the Solidly burn path).
    # * ``"aerodrome_slipstream"`` — CL — Slipstream DOES extract
    #   ``lp_open_data`` (``AerodromeSlipstreamReceiptParser.extract_lp_open_data``)
    #   and the ticks ship inside that struct. There is no standalone
    #   ``extract_tick_lower`` / ``extract_tick_upper`` method, so without
    #   narrowing those two flat fields would trigger false info-warnings on
    #   every Slipstream LP_OPEN even though ticks are extracted via the
    #   structured path. Keep ``lp_open_data`` (the V3-style struct). For
    #   LP_CLOSE, the amounts ship via ``lp_close_data`` (not as standalone
    #   ``amount0_collected`` / ``amount1_collected``), so those flat fields
    #   are narrowed too. ``fees0`` / ``fees1`` remain in the Slipstream
    #   SUPPORTED_EXTRACTIONS set (Slipstream-only standalone extractors).
    # * ``"uniswap_v3"`` / ``"sushiswap_v3"`` / ``"pancakeswap_v3"`` — V3
    #   concentrated-liquidity forks. LP_CLOSE data ships entirely via
    #   ``lp_close_data`` (Burn + Collect path); the standalone flat fields
    #   ``amount0_collected`` / ``amount1_collected`` / ``fees0`` / ``fees1``
    #   are NOT declared in SUPPORTED_EXTRACTIONS for any of these parsers and
    #   are NOT standalone ``extract_*`` methods — they live inside the
    #   ``LPCloseData`` struct. Removing them from the effective spec silences
    #   the chronic info-warnings on every LP_CLOSE without losing any data
    #   (VIB-4805). Empty ≠ Zero — ``lp_close_data`` itself remains in the
    #   spec and carries all fee/amount fields.
    #
    # Values are ``frozenset[str]`` rather than ``list[str]`` because
    # ``_merge_spec_with_overlay`` only needs O(1) membership tests against
    # the merged spec — storing as frozenset removes a per-call
    # ``set(...)`` conversion that otherwise fires on every receipt
    # enrichment (Gemini perf tip on PR #2331).
    #
    # Existing TraderJoe V2 additive overlay (``bin_ids``) is unchanged.
    EXTRACTION_SPECS_REMOVE_BY_PROTOCOL: dict[str, dict[str, frozenset[str]]] = {
        "aerodrome": {
            "LP_OPEN": frozenset({"lp_open_data", "tick_lower", "tick_upper"}),
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected", "fees0", "fees1"}),
        },
        "aerodrome_slipstream": {
            "LP_OPEN": frozenset({"tick_lower", "tick_upper"}),
            "LP_CLOSE": frozenset({"amount0_collected", "amount1_collected"}),
        },
        # VIB-4805: V3 concentrated-liquidity forks — LP_CLOSE flat fields
        # ship inside lp_close_data; no standalone extractors exist. Covers the
        # full UNISWAP_V3_FORKS set (protocol_aliases.py) — each fork keeps its
        # own protocol slug at overlay-lookup time, so each needs its own entry.
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
        # ``EXTRACTION_SPECS_REMOVE_BY_PROTOCOL`` values are already ``frozenset[str]``
        # (Gemini perf tip on PR #2331) so no per-call set conversion is needed.
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
                receipt parser so the leg-labelling helpers can resolve an
                UNCURATED pool's coin addresses / symbols / pool_type from the
                on-chain MetaRegistry on a static ``CURVE_POOLS`` miss. ``None``
                (default) skips the wiring — uncurated-pool legs then degrade to
                ``[]`` / ``""`` (the legacy static-only path; Empty != Zero,
                never fabricates). The strategy runner builds this from the
                Curve gateway bridge bound to its ``GatewayClient``.
        """
        self.parser_registry = parser_registry or ReceiptParserRegistry()
        self.live_mode = live_mode
        self._pool_key_lookup = pool_key_lookup
        self._pool_meta_lookup = pool_meta_lookup
        # Counter for ExtractError occurrences in non-live mode. Exposed so
        # monitoring / paper engines can surface the total.
        self.extract_error_count: int = 0

    def enrich(  # noqa: C901
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

        # Get extraction spec. The merged spec (base + per-protocol overlay) is
        # computed once ``protocol`` is resolved below; for now we only need to
        # short-circuit on the protocol-neutral HOLD case where base is empty.
        base_spec = self.EXTRACTION_SPECS[intent_type]
        if not base_spec:
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
            offchain_extracted = self._extract_offchain_prediction_fields(
                result, intent, intent_type, bundle_metadata, context
            )

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

        # VIB-4320: canonicalise the protocol so the overlay lookup uses the
        # same key ``ReceiptParserRegistry.get`` would resolve to. See
        # ``_canonicalise_protocol`` for the alias-mapping rationale.
        protocol = self._canonicalise_protocol(protocol, context)

        # VIB-4320: merge generic spec with per-protocol overlay. Base fields
        # always come first; overlay fields (e.g. TraderJoe V2 ``bin_ids``)
        # are appended at the tail. ``protocol=None`` returns the base spec
        # unchanged, preserving today's behaviour for unresolvable protocols.
        spec = self._merge_spec_with_overlay(intent_type, protocol)

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

                # Merge the resolved parser's connector-DECLARED per-intent extra
                # extractions (e.g. the US-009 ``primitive_money_legs`` seam) — kept
                # connector-side, not as a protocol-named overlay in this framework.
                spec = self._with_parser_extra_extractions(spec, parser, intent_type)

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

        # VIB-4636 — V4 LP_OPEN current_tick fallback from compiler metadata.
        # The V4 receipt parser reads current_tick from a Swap event in the
        # mint receipt; pure NPM.mint receipts (the canonical PositionManager-
        # mediated path) carry none, so the parser leaves current_tick=None
        # and the persisted accounting_events payload would lose in_range.
        # The V4 adapter stamps ``compile_time_current_tick`` from the same
        # sqrtPriceX96 it sized liquidity against — the V4 mint itself never
        # moves price, so the compile-time tick is correct for post-mint
        # accounting unless an interleaving tx moves the pool. Use it as a
        # fallback only; on-chain extraction always wins. Capability-gated,
        # not protocol-name-gated: the helper no-ops unless the V4 adapter
        # stamped ``compile_time_current_tick`` AND the parser left an
        # ``LPOpenData.current_tick`` of None, so an unconditional call is
        # safe for every intent type / protocol / paper-mode receipt.
        self._fill_v4_lp_open_current_tick_from_metadata(result, bundle_metadata)

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

        # VIB-4989: route through the receipt-parser registry, keyed on the
        # bundle's resolved protocol (the main path already does this) -- no
        # direct connector import and no hardcoded venue name.
        offchain_protocol = (
            (bundle_metadata or {}).get("protocol")
            or self._get_protocol(intent)
            or getattr(context, "protocol", None)
            or ""
        )
        # Try the parser-routed path. Failure handling is class-aware — see
        # _parse_prediction_order_response for the (a/a'/b) classification.
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
                # Fail closed: a crashing parser on a live fill means
                # the framework cannot certify what it is about to
                # book. Mirrors _handle_extract_error's live branch.
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
            # setup_tx_count is measured independently of the per-tx cost
            # fields — always stamp it.
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
                # Empty != Zero (blueprint 27): a partial sum stamped as
                # measured is worse than an honest unmeasured marker. One
                # malformed entry makes the whole aggregate unmeasured -
                # omit gas_cost_native_wei AND gas_cost_usd (same
                # key-omission representation as the unresolvable-price
                # branch below) and warn once naming the bad tx.
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
        # _invoke_extract. Connector-owned parsers may add parser-specific
        # kwargs through ``build_extract_kwargs`` without framework changes.
        # A buggy hook is treated as ExtractError: these kwargs feed
        # accounting-relevant extraction, so silently ignoring hook failures
        # would hide parser-owned data loss.
        try:
            extract_kwargs = self._build_extract_kwargs_for_parser(parser, field, bundle_metadata)
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

        # Iterate receipts. Remember any ExtractError and keep looking — the
        # data might land in a later receipt (multi-tx bundle). Only escalate
        # if no receipt produced Ok.
        #
        # For aggregate fields (see ``_AGGREGATE_FIELDS``), we collect every
        # ExtractOk across receipts and select the preferred-``source``
        # variant once the loop completes. VIB-4310.
        aggregate_preferred = _AGGREGATE_FIELDS.get(field)
        # VIB-5416 — holistic money-leg fields must see every tx's logs at once
        # (the intent's input and output legs can land in different txs). Collapse
        # the per-tx receipts into one merged-logs receipt so the extractor sees
        # the whole intent. No-op for a single-tx intent.
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
                # Value rejected by type-check (see _attach_to_result). Keep
                # scanning subsequent receipts — a later one may produce
                # a valid value for this field.
                continue
            if isinstance(variant, ExtractError):
                last_error = variant
                continue
            # ExtractMissing — benign, continue to next receipt.

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

        if last_error is not None:
            self._handle_extract_error(result, last_error, field, intent_type, parser, protocol)
            return

        logger.debug(
            f"Enrichment: {field} missing from all {len(receipts)} receipt(s) "
            f"(parser={type(parser).__name__}, intent_type={intent_type})"
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
            # LP_COLLECT_FEES / no-liquidity-but-owed: parser's
            # ``fees = collect_amount`` attribution is correct.
            return
        # Split-tx LP_CLOSE: override parser's collect-only attribution.
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

        # LP_CLOSE fee derivation — see helper docstring.
        ResultEnricher._derive_lp_close_fees_from_siblings(chosen, candidates)

        # Backfill ``None`` fields from siblings. Use replace() if the
        # dataclass is frozen; otherwise direct attribute assignment is fine.
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
            # ``is_dataclass`` returns True for both instances and the bare
            # dataclass type; mypy can't narrow ``chosen: Any`` to "instance,
            # not type", so silence the type-var complaint. The TypeError
            # fallback below catches the runtime "applied to a type, not an
            # instance" case.
            return replace(chosen, **backfills)  # type: ignore[type-var]
        except TypeError:
            # Non-frozen / non-replace-able dataclass: fall back to direct
            # attribute assignment. Preserves the contract (chosen returned
            # with backfills applied) without forcing the field model to
            # be replace()-compatible.
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
            # VIB-3226: bridge receipts typically do not carry the user-facing
            # symbol or canonical chain names — they encode chain IDs and token
            # addresses. The bridge compiler writes the resolved intent shape into
            # ``ActionBundle.metadata`` (see BridgeCompiler.compile_bridge), so we
            # thread those hints into the parser to keep the typed output
            # stable and avoid re-deriving them at parse time.
            bridge_kwargs: dict[str, Any] = {}
            for key in ("from_chain", "to_chain", "token", "amount", "bridge"):
                val = bundle_metadata.get(key)
                if val is not None and val != "":
                    bridge_kwargs[key] = val
            # Expected output (post-fee) from the compiler quote — optional,
            # parsers that do not accept it fall back via TypeError handling.
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
    ) -> dict[str, Any]:
        """Merge framework-generic kwargs with optional parser-owned kwargs.

        Parser-owned hooks may only add disjoint kwargs. Framework-owned
        metadata such as ``expected_out`` and fee hints must not be shadowed by
        connector hooks because those values are compiler/orchestrator facts.
        """
        kwargs = self._build_extract_kwargs(field, bundle_metadata)
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
                    # Always thread the value through, including negatives.
                    # The parser fail-fasts on negative (CodeRabbit pushback
                    # on PR #2256): silently dropping a negative here would
                    # let upstream sign corruption hide. End-to-end fail-fast
                    # means the kwargs builder is a pure threader; the parser
                    # is the validator.
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
        # Core typed fields - set directly on result.
        if field == "position_id" and isinstance(value, int | str):
            if not self._attach_position_id(result, value):
                # Non-finite / non-decimal string id: already mirrored into
                # extracted_data by the helper; treat as accepted (return True)
                # so the caller does not keep scanning. Skip the top-level slot.
                return True
        elif field == "swap_amounts" and isinstance(value, SwapAmounts):
            self._attach_swap_amounts(result, value)
        elif field in _STRICT_TYPED_FIELDS:
            # VIB-4310 / VIB-3226 / VIB-3204 / VIB-159 — strictly-typed slots
            # (lp_close_data / bridge_data / protocol_fees / bin_ids /
            # primitive_money_legs). Reject anything of the wrong type with a
            # warning and ``return False`` so the enricher keeps scanning the
            # remaining receipts in a multi-tx bundle, rather than treating the
            # rejection as a terminal "attached". On a valid value, set the
            # top-level attribute so the strategy callback can read it directly
            # (the #159 fix: these previously fell through to extracted_data
            # only, leaving ``result.bin_ids`` etc. unreachable / None).
            attr, validator, type_label = _STRICT_TYPED_FIELDS[field]
            if not validator(value):
                logger.warning(
                    f"Enrichment: parser returned non-{type_label} value for {field!r} "
                    f"(type={type(value).__name__}); ignoring and continuing receipt scan"
                )
                return False
            setattr(result, attr, value)

        # Always add to extracted_data for full access. The ledger dispatcher's
        # ``primitive_money_legs`` fallback (VIB-5212/5218) reads this entry, so
        # it MUST remain even though the value now also has a top-level slot.
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
            # Accept hex addresses (40-char, e.g. Curve LP token addresses) and bytes32
            # hashes (64-char, e.g. Aster Perps tradeHash) as valid position IDs.
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
        # VIB-3164 / Empty != Zero: a parser that could not resolve token
        # decimals stamps the *_decimal_resolved flags False. Surface that
        # on the result so operators see it without grepping parser logs.
        # Non-fatal by design — ledger/sidecar already gate amounts on
        # the flags and record the "parser didn't emit" sentinel.
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
            # ``amount_*_decimal`` may be None when the receipt parser could
            # not resolve token decimals (Empty != zero invariant —
            # docs/internal/blueprints/27-accounting.md). Render as "?" rather than the
            # literal "None" to keep logs readable.
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

        cache: dict[tuple, Any] = {}

        def cached_parse_receipt(receipt: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
            # Use transactionHash + a deterministic kwargs signature as the
            # key. Context kwargs MUST be part of the cache key. The receipt
            # itself is identical for every extract_* call within one
            # enrichment, but two extract_* calls may pass different kwargs,
            # and we must not return the wrong cached result.
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
        # VIB-5628: thread the Curve dynamic-pool-meta lookup into parsers whose
        # connector declares it (the Curve parser). Same declare-only discipline
        # as pool_key_lookup — other parsers' cache behaviour stays unchanged.
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
        # Preserve first-receipt scalar context (sender / status + aliases) so
        # parsers that key on it (e.g. swap/wrap direction) behave as before.
        # Defensive: guard against a None / non-dict first receipt.
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
        # Override the inherited first-tx hash with a set-unique synthetic key so
        # the parse cache never returns a stale per-tx result for the merged call.
        synthetic_hash = "merged:" + "|".join(constituent_hashes)
        merged["transactionHash"] = synthetic_hash
        merged["tx_hash"] = synthetic_hash
        return merged

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
