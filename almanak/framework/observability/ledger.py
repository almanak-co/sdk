"""Transaction Ledger -- structured trade records.

Every executed intent produces a LedgerEntry that captures the trade in a
structured, queryable format.  This replaces grepping through timeline event
``details`` dicts for post-mortem trade analysis.

The ledger is populated by ``StrategyRunner`` after result enrichment and
stored alongside timeline events in the gateway state store.

Phase 5k -- helper extraction layout
------------------------------------
``build_ledger_entry`` is composed from small phase helpers that each
return a piece of the final ``LedgerEntry``:

    alpha  _extract_intent_type          : enum-or-string dispatch
    beta   _extract_tokens_and_amounts   : dispatch between sub-helpers:
             _extract_from_declared_legs      (VIB-5218 / US-009: a connector-
                                               DECLARED PrimitiveMoneyLegs on the
                                               result — preferred over every guess
                                               when present; no-op until US-010/011
                                               migrate the first connectors)
             _extract_from_swap_amounts       (SwapAmounts + intent fallback
                                               for empty token sides)
             _extract_from_lp_open           (LP_OPEN: LPOpenData amounts +
                                               intent token0/token1 lookup)
             _extract_from_lp_close          (LP_CLOSE: LPCloseData collected
                                               amounts + currency0/1 symbols,
                                               VIB-5132)
             _extract_from_intent_fallback    (LEGACY guesser — intent-attr
                                               precedence chain from_token >
                                               borrow_token > supply_token > token;
                                               amount > borrow_amount >
                                               supply_amount — amount_usd is
                                               deliberately excluded, VIB-5060.
                                               Emits a WARN + ledger_intent_
                                               fallback_total metric per money row
                                               so its shrink is trackable, VIB-5218).
    gamma  _extract_tx_and_gas           : first tx_hash + total gas + gas USD
    delta  _coalesce_error               : failure + empty-error -> result.error
    epsilon _build_extracted_data_json   : serialize + multi-tx augmentation

ORDERING (VIB-5132): the beta token/amount extraction is now DEFERRED until
AFTER the LP-close native-leg stamps (``_stamp_v4_lp_close_native_principal`` /
``_stamp_lp_close_native_amounts``) run, because ``_extract_from_lp_close``
reads ``LPCloseData.amount{0,1}_collected`` — values the stamps fill for a
native-ETH leg that emits no ERC-20 Transfer. Extracting before the stamps
would read the pre-stamp ``None`` legs and re-emit empty amount_in/out. For
every non-LP-close intent type the deferral is behaviourally identical (the
stamps are no-ops outside LP_CLOSE).

The SQLite INSERT at ``backends/sqlite.py:2291-2322`` names 21 columns and
pairs each with a specific ``entry.<attribute>`` read.  The refactor
preserves every LedgerEntry field-value semantic byte-identical so the
write contract is unaffected.
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class LedgerEntry:
    """A single structured trade record.

    Attributes:
        id: Unique entry identifier (UUID).
        cycle_id: Correlation ID for the decide->execute cycle.
        deployment_id: Deployment that produced this trade.
        timestamp: When the trade was executed.
        intent_type: Intent type (SWAP, LP_OPEN, BORROW, etc.).
        token_in: Input token symbol or address.
        amount_in: Input amount (human-readable decimal string).
        token_out: Output token symbol or address.
        amount_out: Output amount (human-readable decimal string).
        effective_price: Execution price (out/in), if applicable.
        slippage_bps: Actual slippage in basis points.
        gas_used: Gas consumed by the transaction.
        gas_usd: Gas cost in USD.
        tx_hash: On-chain transaction hash.
        chain: Chain where the trade executed.
        protocol: Protocol used (e.g. uniswap_v3, aave_v3).
        success: Whether the trade succeeded.
        error: Error message if the trade failed.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cycle_id: str = ""
    deployment_id: str = ""
    execution_mode: str = ""  # Phase 4: "live", "paper", "dry_run" (VIB-2837)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    intent_type: str = ""
    token_in: str = ""
    amount_in: str = ""
    token_out: str = ""
    amount_out: str = ""
    effective_price: str = ""
    slippage_bps: float | None = None
    gas_used: int = 0
    gas_usd: str = ""
    tx_hash: str = ""
    chain: str = ""
    protocol: str = ""
    success: bool = True
    error: str = ""
    extracted_data_json: str = ""
    price_inputs_json: str = ""  # token prices at execution time — enables audit-grade replay (VIB-3480)
    pre_state_json: str = ""  # on-chain state before execution (VIB-3480)
    post_state_json: str = ""  # on-chain state after execution (VIB-3480)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary for storage."""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LedgerEntry":
        """Deserialize from a dictionary."""
        ts = data.get("timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        elif ts is None:
            ts = datetime.now(UTC)
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            cycle_id=data.get("cycle_id", ""),
            deployment_id=data["deployment_id"],
            execution_mode=data.get("execution_mode", ""),
            timestamp=ts,
            intent_type=data.get("intent_type", ""),
            token_in=data.get("token_in", ""),
            amount_in=data.get("amount_in", ""),
            token_out=data.get("token_out", ""),
            amount_out=data.get("amount_out", ""),
            effective_price=data.get("effective_price", ""),
            slippage_bps=data.get("slippage_bps"),
            gas_used=data.get("gas_used", 0),
            gas_usd=data.get("gas_usd", ""),
            tx_hash=data.get("tx_hash", ""),
            chain=data.get("chain", ""),
            protocol=data.get("protocol", ""),
            success=data.get("success", True),
            error=data.get("error", ""),
            extracted_data_json=data.get("extracted_data_json", ""),
            price_inputs_json=data.get("price_inputs_json", ""),
            pre_state_json=data.get("pre_state_json", ""),
            post_state_json=data.get("post_state_json", ""),
        )


def lenient_ledger_decimal(value: Any) -> Decimal:
    """Lenient parse of a ledger numeric column to a FINITE Decimal.

    The dashboard quant aggregations must never crash (or zero every tile by
    failing one aggregate query) because a single on-disk row carries a
    degenerate numeric value. Contract (VIB-5059 Phase 1, shared verbatim by
    the SQLite custom SQL aggregate, the Postgres numeric-literal guard, and
    the Python reference aggregation in ``quant_aggregations``):

    - ``None`` / ``""`` → ``Decimal("0")`` (absent contributes nothing).
    - Unparsable text → ``Decimal("0")`` (legacy ``_to_decimal`` behavior).
    - Non-finite numerics (``NaN`` / ``Infinity``) → ``Decimal("0")``. This is
      the one documented divergence from the legacy per-row loop, which let a
      single ``NaN`` row poison a lifetime SUM into ``NaN`` — a defect, not a
      contract (pinned by the VIB-5059-p1sql UAT card, D3.F6).
    """
    if value is None or value == "":
        return Decimal("0")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


@dataclass(frozen=True)
class LedgerQuantStats:
    """SQL-side aggregate of everything the dashboard quant tiles need from
    ``transaction_ledger`` (VIB-5059 Phase 1).

    Replaces the bulk full-width ledger fetch in the dashboard quant-input
    load: the per-row Python loops in ``quant_aggregations`` consumed only
    these counts, this one sum, and the first-action anchor — so the stores
    compute them with targeted ``COUNT``/``SUM`` queries that transfer O(1)
    rows and never select the pre/post-state JSON blobs.

    Zero-row semantics (parity with the legacy loops over an empty list —
    Empty ≠ Zero applies per field):

    - counts → ``0`` (``len([]) == 0``).
    - ``gas_usd_sum`` → ``Decimal("0")`` (the legacy ``CostStack`` default).
    - ``first_action_wallet_value_usd`` → ``None`` (unmeasured — triggers the
      portfolio-metrics fallback exactly as the legacy anchor walk did; never
      coerced to ``0``).

    Frozen: the quant-input cache (PR #2731) shares one loaded object across
    the three tile RPCs, which must treat it as read-only.
    """

    total: int = 0
    with_tx_hash: int = 0
    with_cycle_id: int = 0
    with_price_inputs: int = 0
    with_pre_post_state: int = 0
    with_positive_gas_usd: int = 0
    gas_usd_sum: Decimal = Decimal("0")
    # Wallet USD value at the strategy's first action (VIB-3914 anchor).
    # Computed from the bounded anchor-candidate walk, not from the
    # aggregate query; ``None`` = no ledger row carries a usable anchor.
    first_action_wallet_value_usd: Decimal | None = None


def _extract_intent_type(intent: Any) -> str:
    """Phase alpha -- normalize intent_type to a string.

    Supports both enum-like payloads (``.value`` present) and raw strings.
    Missing ``intent_type`` attribute maps to ``""``.
    """
    if not hasattr(intent, "intent_type"):
        return ""
    it = intent.intent_type
    return it.value if hasattr(it, "value") else str(it)


# Tuple returned by the token/amount phase -- kept as a plain tuple to avoid
# another tiny dataclass and to match the positional assignment style in
# the final LedgerEntry(...) constructor.
_TokensAndAmounts = tuple[str, str, str, str, str, float | None]


def _measured_amount_to_row(value: "Decimal | str | None") -> str:
    """The single sanctioned conversion of a raw extracted amount to its
    ``transaction_ledger`` row string, routed through ``MeasuredMoney`` so the
    three Empty≠Zero states can never be conflated at this boundary (VIB-5214,
    US-005 gate (b)).

    This BANS the ad-hoc ``str(x) if x is not None else ""`` idiom the extraction
    helpers used to type amounts. That idiom let the ``intent_fallback`` path
    launder a non-measured value into the ledger: an ABSENT field (or a
    fabricated non-numeric placeholder — the #2885 / #2895 bug class) was booked
    as a value-bearing string, and a measured ``Decimal("0")`` could not be told
    apart from "no value" by the bare ``is not None`` check. Routing through
    :meth:`MeasuredMoney.from_raw` makes the state explicit and impossible to
    misclassify.

    Serialization back to the existing row form (byte-compatible for the
    Decimal-typed amounts these helpers produce — see the VIB-5214 unit tests):

    - **measured** (a real ``Decimal``, INCLUDING ``Decimal("0")`` — measured
      zero is a value) → ``str(value)``. For a canonical ``Decimal`` this is
      byte-identical to the legacy ``str(amt)``; ``Decimal`` ↔ ``str`` round-trips.
    - **unmeasured** (``None``) / **absent** (``""`` / whitespace-only) → ``""``,
      the existing row form for "no amount".
    - a value OUTSIDE the ``Decimal | str | None`` money domain (e.g. a stray
      ``int`` / ``float``), or a non-numeric string (e.g. an unresolved ``"all"``
      placeholder) → unmeasured ``""``. It is NOT a measured number, so
      Empty≠Zero forbids booking it as one. ``from_raw`` raises on these; we map
      the raise to the unmeasured row form rather than crash the ledger write.
    """
    from almanak.framework.accounting.measured import MeasuredMoney

    try:
        money = MeasuredMoney.from_raw(value)
    except (ValueError, TypeError):
        # Outside the documented money domain (non-numeric string / int / float):
        # not a measured value -> unmeasured row form. Never launder to a number.
        return ""
    return str(money.value) if money.is_measured else ""


def _extract_from_swap_amounts(swap_amounts: Any, intent: Any) -> _TokensAndAmounts:
    """Phase beta-primary -- all fields from ``result.swap_amounts``.

    Token sides fall back to ``intent.from_token`` / ``intent.to_token`` when
    the swap_amounts side is falsy (empty string). Amount sides use
    ``is not None`` checks instead of truthiness so measured-zero amounts
    (``Decimal("0")``) are preserved as ``"0"`` rather than silently dropped
    to ``""`` -- issue #1768 (sibling of #1709 / #1710 fixed in #1751).

    The ``amount_*_decimal_resolved`` flags on ``SwapAmounts`` disambiguate
    a measured ``Decimal(0)`` from an unresolvable-decimals sentinel —
    historically both were recorded as ``"0"`` on the ledger, silently
    corrupting PnL attribution and portfolio accounting for tokens whose
    decimals failed to resolve (issue #1778, Codex finding on PR #1774).
    An unresolved amount now falls through to the same ``""`` that the
    truthiness path used to produce before #1768, but without conflating
    the measured-zero case. Parsers that do not populate the flag default
    to ``True`` (``SwapAmounts`` default) -- preserves existing behavior
    byte-for-byte for the many connectors that have not been audited yet.
    """
    token_in = swap_amounts.token_in or getattr(intent, "from_token", "") or ""
    token_out = swap_amounts.token_out or getattr(intent, "to_token", "") or ""
    amt_in = getattr(swap_amounts, "amount_in_decimal", None)
    amt_out = getattr(swap_amounts, "amount_out_decimal", None)
    amt_in_resolved = getattr(swap_amounts, "amount_in_decimal_resolved", True)
    amt_out_resolved = getattr(swap_amounts, "amount_out_decimal_resolved", True)
    # An unresolved-decimals leg is UNMEASURED (Empty != Zero): pass ``None`` so
    # the sanctioned conversion records ``""`` rather than a measured value.
    amount_in = _measured_amount_to_row(amt_in if amt_in_resolved else None)
    amount_out = _measured_amount_to_row(amt_out if amt_out_resolved else None)
    # ``effective_price`` mirrors ``amount_in`` / ``amount_out``: an
    # unresolved input-decimals row carries ``effective_price=None`` from
    # the parser, but we also gate on ``amt_in_resolved`` so any future
    # parser that emits a non-None ``effective_price`` while
    # ``amount_in_decimal_resolved=False`` still falls through to ``""``
    # (the "Empty != zero" invariant — issue #1778).
    if swap_amounts.effective_price is not None and amt_in_resolved:
        effective_price = str(swap_amounts.effective_price)
    else:
        effective_price = ""
    return (
        token_in,
        token_out,
        amount_in,
        amount_out,
        effective_price,
        swap_amounts.slippage_bps,
    )


def _intent_fallback_token_in(intent: Any) -> str:
    """The legacy intent-attr ``token_in`` precedence chain (pure, no side effects).

    Token precedence:
        ``from_token > borrow_token > supply_token > token``

    Extracted so the lending helper can REUSE the precedence chain to resolve a
    token symbol WITHOUT triggering the fallback's WARN+metric observability
    (VIB-5218): when the lending lane resolves the row, the intent-fallback
    guesser did NOT produce it, so it must not be counted against the fallback.
    """
    return (
        getattr(intent, "from_token", "")
        or getattr(intent, "borrow_token", "")
        or getattr(intent, "supply_token", "")
        or getattr(intent, "token", "")
        or ""
    )


def _record_intent_fallback_money_row(intent_type: str, token_in: str, amount_in: str) -> None:
    """Emit the WARN + metric for a fallback-attributed money row (VIB-5218).

    The two halves of the fallback-observability contract: a structured WARNING
    (human-readable, names the intent type for diagnosis) and the
    ``ledger_intent_fallback_total`` counter (operator-dashboard, tracks the
    shrink as connectors migrate to declared ``PrimitiveMoneyLeg`` sets). Called
    only when the fallback yields a money-bearing row — see
    :func:`_extract_from_intent_fallback`.
    """
    logger.warning(
        "ledger intent_fallback produced a money row (intent_type=%s, token_in=%s, amount_in=%s): "
        "no connector-declared PrimitiveMoneyLeg available, used the legacy intent-attribute guesser "
        "(VIB-5218). Migrate the connector to declare money legs (blueprint 05 §7 / 27 §6.6).",
        intent_type or "<unknown>",
        token_in or "<empty>",
        amount_in or "<empty>",
    )
    # Lazy import (house style — mirrors decimal_guards' record_raw_wei_suspected
    # call site) so the observability module's import graph stays unchanged.
    from almanak.framework.observability.metrics import record_ledger_intent_fallback

    record_ledger_intent_fallback(intent_type=intent_type)


def _extract_from_intent_fallback(intent: Any, *, intent_type: str = "") -> _TokensAndAmounts:
    """Phase beta-fallback -- no declared legs / no swap_amounts; walk the intent-attr chain.

    Token precedence:
        ``from_token > borrow_token > supply_token > token``
    Amount precedence:
        ``amount > borrow_amount > supply_amount``

    Supports swap-style (from_token/to_token), lending
    (borrow_token/supply_token) and generic (token/amount) intents.

    ``intent.amount_usd`` is deliberately NOT in the amount chain (VIB-5060):
    ``transaction_ledger.amount_in`` is human units of ``token_in`` (the
    VIB-5036 contract), and a USD clip size is the wrong unit for that column
    — a failed $2 WBTC sell used to land as ``amount_in="2.00"`` /
    ``token_in="WBTC"`` (~$126k notional on the trade tape). When only USD
    sizing is known the token amount is unmeasured: leave ``""``
    (Empty != Zero).

    VIB-5218 — this is the LEGACY guesser the dispatcher now prefers a
    connector-declared ``PrimitiveMoneyLeg`` set over. When it produces a
    money-bearing row (a non-empty ``token_in`` or ``amount_in``) it emits a
    WARN + ``ledger_intent_fallback_total`` metric (tagged with ``intent_type``)
    so the fallback's usage is measurable and its shrink trackable as connectors
    migrate. ``intent_type`` is threaded by the caller for the diagnostic label;
    it defaults to ``""`` so the historical positional-only call sites and the
    direct unit tests keep working.
    """
    token_in = _intent_fallback_token_in(intent)
    token_out = getattr(intent, "to_token", "") or ""
    # First NON-None link wins — ``or``-truthiness would collapse a measured
    # ``Decimal("0")`` into the unmeasured ``""`` sentinel (Empty != Zero;
    # the same measured-zero preservation the swap_amounts path got in
    # issue #1768).
    amt = None
    for attr in ("amount", "borrow_amount", "supply_amount"):
        value = getattr(intent, attr, None)
        if value is not None:
            amt = value
            break
    # VIB-5214 — route through the sanctioned MeasuredMoney conversion so an
    # unresolved amount (``None``, ``""``, or a non-numeric placeholder) is
    # recorded as UNMEASURED/ABSENT (``""``) and can NEVER be laundered into a
    # value-bearing string or a measured ``Decimal("0")``. A real ``Decimal``
    # (incl. measured zero) still serializes to its canonical string.
    amount_in = _measured_amount_to_row(amt)
    # VIB-5218 — only a money-bearing row counts: an all-empty fallback (a truly
    # money-less intent) is not the patch-hub signal we track.
    if token_in or amount_in:
        _record_intent_fallback_money_row(intent_type, token_in, amount_in)
    return (token_in, token_out, amount_in, "", "", None)


def _lp_amount_to_human(raw: Any, token: str, chain: str) -> str | None:
    """Scale a raw on-chain LP amount (smallest unit) to a human Decimal string.

    Mirrors the canonical resolver pattern in :func:`_extract_from_lending`
    (resolve token decimals, divide by ``10 ** decimals``).  Returns ``None``
    when the value is missing or a NON-ZERO amount's token / chain / decimals
    cannot be resolved, so the caller can fall back to the intent's
    (already-human) amount.  A raw integer is therefore NEVER persisted to
    ``transaction_ledger.amount_in/out`` (VIB-5036 — the contract is human
    units, matching SWAP / lending / the L2 golden-fixture ``amount_in_human``).

    A measured zero is returned as ``"0"`` without resolving decimals (0 scaled
    is 0) so Empty != Zero is preserved even when tokens are unknown.
    """
    if raw is None:
        return None
    try:
        d = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if d == 0:
        return "0"
    if not token or not chain:
        return None
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        # skip_gateway/log_errors: this is the accounting write hot path —
        # never risk a gateway round-trip stall on a best-effort scale; we pass
        # a symbol (not an address), so the gateway lookup is a no-op anyway.
        token_info = resolver.resolve(token, chain, log_errors=False, skip_gateway=True)
        # Validate decimals explicitly (mirrors _scale_fee /
        # _scale_raw_amount_to_human) rather than relying on the except below.
        decimals = getattr(token_info, "decimals", None)
        if decimals is None or decimals < 0:
            return None
        return str(d / Decimal(10**decimals))
    except Exception as exc:
        logger.debug(
            "ledger LP_OPEN: failed to scale raw amount=%s token=%s chain=%s: %s",
            raw,
            token,
            chain,
            exc,
        )
        return None


def _extract_from_lp_open(intent: Any, result: Any, chain: str = "") -> _TokensAndAmounts:
    """Phase beta-lp-open -- LP_OPEN has no swap_amounts; pull amounts from
    ``LPOpenData`` in ``result.extracted_data`` and tokens from the intent.

    Field mapping:
    - ``token_in``  : ``intent.token0`` -> ``intent.from_token`` -> ``""``
    - ``token_out`` : ``intent.token1`` -> ``intent.to_token``  -> ``""``
    - ``amount_in`` : ``LPOpenData.amount0`` (raw int) is the on-chain actual
                      deposit for token0, **scaled to human units** via the
                      token resolver; falls back to ``intent.amount0`` (an
                      already-human ``Decimal``) when the on-chain actual is
                      absent or its decimals cannot be resolved.
    - ``amount_out``: same logic for ``amount1``.

    ``LPOpenData.amount0`` / ``amount1`` are raw integer values (smallest
    unit).  VIB-5036: we scale them to human units before persistence so
    ``transaction_ledger`` carries the same human-form contract as SWAP and
    lending rows (and the parallel ``accounting_events``).  A raw integer is
    never written; on a decimals-resolve miss we prefer the intent's human
    amount, else leave ``""`` (Empty != Zero).
    """
    # Token resolution: prefer explicit token0/token1 attrs (test/legacy callers),
    # then parse symbols from the pool string (e.g. "WETH/USDC"), then fallback.
    token_in = getattr(intent, "token0", "") or getattr(intent, "from_token", "") or ""
    token_out = getattr(intent, "token1", "") or getattr(intent, "to_token", "") or ""
    if not token_in or not token_out:
        pool_str = (getattr(intent, "pool", "") or "").strip()
        if "/" in pool_str:
            pool_parts = [p.strip() for p in pool_str.split("/")]
            if not token_in and pool_parts:
                token_in = pool_parts[0]
            if not token_out and len(pool_parts) > 1:
                token_out = pool_parts[1]

    # Prefer on-chain actuals from LPOpenData; fall back to intent amounts.
    extracted_data = getattr(result, "extracted_data", None) or {} if result else {}
    lp_open_data = extracted_data.get("lp_open_data") if isinstance(extracted_data, dict) else None

    if lp_open_data is not None:
        # On-chain actuals are raw integers (smallest unit) -> scale to human.
        # Per-side fallback to the intent's (already-human) amount when the
        # on-chain actual is absent OR its decimals cannot be resolved, so a
        # raw integer is never persisted (VIB-5036).
        raw0 = getattr(lp_open_data, "amount0", None)
        raw1 = getattr(lp_open_data, "amount1", None)
        amount_in = _lp_amount_to_human(raw0, token_in, chain)
        if amount_in is None:
            amount_in = _measured_amount_to_row(getattr(intent, "amount0", None))
        amount_out = _lp_amount_to_human(raw1, token_out, chain)
        if amount_out is None:
            amount_out = _measured_amount_to_row(getattr(intent, "amount1", None))
    else:
        amount_in = _measured_amount_to_row(getattr(intent, "amount0", None))
        amount_out = _measured_amount_to_row(getattr(intent, "amount1", None))

    return (token_in, token_out, amount_in, amount_out, "", None)


def _resolve_lp_close_symbol(currency: Any, chain: str) -> str:
    """Map a V4 PoolKey currency address to a ledger token symbol.

    The V4 receipt parser emits ``amount0_collected`` / ``amount1_collected`` in
    PoolKey-sorted order (``int(currency0, 16) < int(currency1, 16)``), which may
    be the OPPOSITE of the user's intent order. Resolving the symbol FROM the
    currency address (rather than the intent's ``token0`` / ``token1``) keeps the
    ledger ``token_in`` / ``amount_in`` pair aligned with the leg the amount
    actually belongs to (the same by-address discipline VIB-4426 applied to the
    LP accounting handler's ``_v4_realign_token_pair``).

    A native sentinel — V4's zero address (``0x0…0``) or the ERC-7528 marker
    (``0xEeee…``) — maps to the chain's native symbol so ``_lp_amount_to_human``
    scales it at native (18-dp) decimals. A real ERC-20 address resolves through
    the token resolver (``skip_gateway=True`` — accounting write hot path, never
    risk a gateway round-trip stall). Returns ``""`` when ``currency`` is absent
    or unresolvable, so the caller falls back to the intent's token symbol with
    the SAME precedence ``_extract_from_lp_open`` uses (V3 closes leave
    ``currency0`` / ``currency1`` ``None`` and rely entirely on that fallback).
    """
    if not currency:
        return ""
    cur = str(currency).lower()
    if cur in (_V4_NATIVE_CURRENCY, _ERC7528_NATIVE_CURRENCY):
        if not chain:
            return ""
        from almanak.framework.accounting.gas_pricing import native_token_for_chain

        return native_token_for_chain(chain) or ""
    if not chain:
        return ""
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        # Pass the normalised lowercase string ``cur`` (not the raw ``currency``,
        # which may be ``HexBytes`` / non-str) so the resolver's address handling
        # never trips on a missing ``.startswith`` (Gemini).
        token_info = resolver.resolve(cur, chain, log_errors=False, skip_gateway=True)
        return getattr(token_info, "symbol", "") or ""
    except Exception as exc:  # noqa: BLE001 — fail-open to intent fallback
        logger.debug(
            "ledger LP_CLOSE: failed to resolve currency=%s on chain=%s to a symbol: %s",
            currency,
            chain,
            exc,
        )
        return ""


def _resolve_lp_close_tokens(intent: Any, currency0: Any, currency1: Any, chain: str) -> tuple[str, str]:
    """Resolve ``(token_in, token_out)`` symbols for an LP_CLOSE, address-aligned.

    ADDRESS-ALIGNMENT is the money-correctness invariant (codex P2 / CodeRabbit).
    V4 ``amount{0,1}_collected`` are PoolKey-sorted (``int(currency0,16) <
    int(currency1,16)``); the intent's ``token0`` / ``token1`` (and the ``pool``
    string) are USER-ordered and may be the OPPOSITE. Pairing a PoolKey-index
    amount with a user-ordered symbol would scale it by the WRONG token's decimals
    and persist a materially wrong ledger amount — worse than the honest empty it
    had pre-fix. So when a V4 PoolKey currency is PRESENT we resolve the symbol
    FROM THE ADDRESS only; the intent / pool-string fallback applies PER LEG ONLY
    when that leg's currency is ABSENT (``None`` — V3-style close data, no PoolKey
    re-sort). An unresolved present-currency leg stays ``""`` so the caller's
    ``_lp_amount_to_human`` returns ``None`` for a non-zero raw and the leg stays
    unmeasured (Empty != Zero) rather than carrying a misordered symbol.
    """
    pool_parts = [p.strip() for p in (getattr(intent, "pool", "") or "").strip().split("/")]
    if len(pool_parts) <= 1:
        pool_parts = []
    # Per-leg resolution (written once for both legs to keep it auditable): the
    # PoolKey-sorted index ``idx`` ties the symbol to the same-index collected
    # amount. ``_resolve_lp_close_symbol`` returns "" for an absent (``None``)
    # currency, so ``currency is None`` is exactly the leg that may take the
    # user-ordered intent / pool fallback; a present-but-unresolved leg keeps "".
    resolved: list[str] = []
    legs = ((currency0, ("token0", "from_token"), 0), (currency1, ("token1", "to_token"), 1))
    for currency, intent_attrs, idx in legs:
        symbol = _resolve_lp_close_symbol(currency, chain)
        if currency is None and not symbol:
            for attr in intent_attrs:
                symbol = getattr(intent, attr, "") or ""
                if symbol:
                    break
            if not symbol and idx < len(pool_parts):
                symbol = pool_parts[idx]
        resolved.append(symbol)
    return resolved[0], resolved[1]


def _extract_from_lp_close(intent: Any, result: Any, chain: str = "") -> _TokensAndAmounts | None:
    """Phase beta-lp-close -- the symmetric twin of :func:`_extract_from_lp_open`.

    An LP_CLOSE has no ``swap_amounts`` for a **V4 native** leg: the ETH side is
    returned to the wallet as raw ETH (TAKE_PAIR, no ERC-20 Transfer), so the
    receipt parser emits no ``SwapAmounts`` and the row historically fell through
    to ``_extract_from_intent_fallback`` → ``amount_in=""`` / ``amount_out=""``.
    The measured proceeds live on ``LPCloseData.amount{0,1}_collected`` (VIB-5117
    stamps the native principal there), but nothing read them back into the
    ledger's top-level amount columns (VIB-5132). This helper closes that gap.

    Field mapping (mirrors LP_OPEN's amount0->amount_in / amount1->amount_out
    convention for lane symmetry):
    - ``token_in``  : ``currency0`` symbol -> ``intent.token0`` -> ``from_token``
                      -> pool-string ``[0]`` -> ``""``
    - ``token_out`` : ``currency1`` symbol -> ``intent.token1`` -> ``to_token``
                      -> pool-string ``[1]`` -> ``""``
    - ``amount_in`` : ``LPCloseData.amount0_collected`` (raw int) **scaled to
                      human units** via ``_lp_amount_to_human``.
    - ``amount_out``: same logic for ``amount1_collected``.

    VIB-5036: amounts are scaled to human units before persistence so
    ``transaction_ledger`` carries the same human-form contract as SWAP, lending,
    and LP_OPEN rows. **Empty != Zero**: a ``None`` collected leg (UNMEASURED —
    e.g. a native leg whose runner stamp never ran) stays ``""``; a measured
    ``0`` becomes the human ``"0"`` (both via ``_lp_amount_to_human``).

    Returns ``None`` when there is no ``LPCloseData`` on the result (no V4/LP
    close data to read), or when it yields neither a token nor an amount, so the
    caller falls back to ``_extract_from_intent_fallback``.
    """
    extracted_data = getattr(result, "extracted_data", None) or {} if result else {}
    lp_close_data = extracted_data.get("lp_close_data") if isinstance(extracted_data, dict) else None
    if lp_close_data is None:
        return None

    # Address-aligned symbol resolution (see ``_resolve_lp_close_tokens``): a V4
    # PoolKey currency resolves FROM ITS ADDRESS (amounts are PoolKey-sorted); the
    # user-ordered intent / pool fallback is used only for a currency-ABSENT leg.
    currency0 = getattr(lp_close_data, "currency0", None)
    currency1 = getattr(lp_close_data, "currency1", None)
    token_in, token_out = _resolve_lp_close_tokens(intent, currency0, currency1, chain)

    # Scale on-chain raw collected amounts (smallest unit) to human. ``None``
    # (unmeasured) stays ``""`` (Empty != Zero); a measured ``0`` scales to "0".
    # Token is address-aligned (above), so the decimals are the leg's own.
    raw0 = getattr(lp_close_data, "amount0_collected", None)
    raw1 = getattr(lp_close_data, "amount1_collected", None)
    # ``_lp_amount_to_human`` returns a human str (incl. "0" for measured zero)
    # or ``None`` (UNMEASURED). Route through the sanctioned conversion so the
    # ``None`` leg records the absent/unmeasured ``""`` row form (Empty != Zero).
    amount_in = _measured_amount_to_row(_lp_amount_to_human(raw0, token_in, chain))
    amount_out = _measured_amount_to_row(_lp_amount_to_human(raw1, token_out, chain))

    # Yielded nothing usable -> let the caller take the intent-attr fallback.
    if not any((token_in, token_out, amount_in, amount_out)):
        return None
    return (token_in, token_out, amount_in, amount_out, "", None)


def _extract_from_lending(
    intent: Any,
    result: Any,
    intent_type: str,
    chain: str,
) -> _TokensAndAmounts:
    """Phase beta-lending -- REPAY / WITHDRAW receipt-resolved amount (VIB-3939).

    REPAY with ``repay_full=True`` and WITHDRAW with ``withdraw_all=True``
    submit ``uint256.max`` to the protocol so Aave (or any compatible lending
    pool) repays / withdraws "all available". The intent's ``amount`` stays
    at its ``Decimal("0")`` default, so the intent-attr fallback writes
    ``amount_in=""`` to the ledger — the audit (2026-05-03 LP+looping
    Anvil run, Finding #7) called this out as the reason
    ``transaction_ledger.amount_in`` was empty on the residual REPAY and
    final WITHDRAW even though the resolved amount was visible in
    ``accounting_events.payload.amount_token``.

    The receipt parser already extracts the *resolved* amount from the
    on-chain ``Repay`` / ``Withdraw`` event (Aave V3 emits the actual
    repaid / withdrawn amount, not the user-supplied uint256.max sentinel)
    and pushes it onto ``result.extracted_data["repay_amount"]`` /
    ``["withdraw_amount"]`` as a raw integer. This helper consumes that
    raw int and converts it to human units via the token resolver, the
    same pattern used by ``accounting/category_handlers/lending_handler.py``
    (``_extract_amount_human``).

    Receipt-resolved value wins when present — Aave can repay strictly
    less than the user-requested amount when the wallet balance is below
    it, so even partial REPAY/WITHDRAW are more accurately captured from
    the receipt than from the intent.

    Falls back to ``_extract_from_intent_fallback`` when the receipt
    didn't yield a resolved amount (parser absent, parse failed, or
    receipt simply not a lending event). This preserves the historical
    behavior for non-uint256.max cases where the receipt extraction is
    a no-op and the intent attribute path was already correct.
    """
    extracted = getattr(result, "extracted_data", None) if result else None
    raw: Any = None
    if isinstance(extracted, dict):
        # DELEVERAGE is structurally a repay — the receipt parser writes
        # ``repay_amount``, not a separate ``deleverage_amount`` (Codex X2
        # 2026-05-04 review). Mirror lending_handler's intent → key map.
        key = "withdraw_amount" if intent_type == "WITHDRAW" else "repay_amount"
        raw = extracted.get(key)

    if raw is None:
        return _extract_from_intent_fallback(intent, intent_type=intent_type)

    try:
        raw_int = int(raw)
    except (TypeError, ValueError):
        # Receipt produced a non-int value (shouldn't happen for these
        # extractors, but fail-open to the intent-attr path rather than
        # pretending we have a value). Empty != zero, so don't substitute.
        return _extract_from_intent_fallback(intent, intent_type=intent_type)

    # CodeRabbit 2026-05-04: reuse the existing intent-attr precedence chain
    # (``from_token`` > ``borrow_token`` > ``supply_token`` > ``token``) so
    # connectors that name the lending asset under ``borrow_token`` /
    # ``supply_token`` (rather than the generic ``token``) still get
    # decimals resolved. VIB-5218 — call the PURE precedence helper, not
    # ``_extract_from_intent_fallback``: the lending lane (not the fallback
    # guesser) is producing this row, so it must NOT trip the fallback's
    # WARN+metric observability.
    token_in = _intent_fallback_token_in(intent)

    # Convert raw on-chain integer to human units. Without a chain or a
    # resolvable token we can't scale safely — Empty != zero, so leave
    # ``amount_in=""`` rather than write the unscaled raw int (which
    # would be 18 orders of magnitude wrong for a 6-decimal stablecoin).
    if not token_in or not chain:
        return (token_in, "", "", "", "", None)

    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        # ``resolver.resolve`` is typed ``-> ResolvedToken`` and raises
        # ``TokenNotFoundError`` / ``TokenResolutionError`` on failure (never
        # returns None). The except below is the only honest failure path.
        token_info = resolver.resolve(token_in, chain=chain)
        scaled = Decimal(raw_int) / Decimal(10**token_info.decimals)
    except Exception as exc:
        logger.debug(
            "ledger %s: failed to scale raw amount=%s for token=%s chain=%s: %s",
            intent_type,
            raw_int,
            token_in,
            chain,
            exc,
        )
        return (token_in, "", "", "", "", None)

    return (token_in, "", _measured_amount_to_row(scaled), "", "", None)


# VIB-5218 — the reserved ``result.extracted_data`` key (and forward-compatible
# typed attribute name) a connector uses to DECLARE its money legs for the ledger.
# Connectors deposit a ``PrimitiveMoneyLegs`` here from their extraction path
# (blueprint 05 §7) the same way they deposit ``lp_open_data`` / ``lp_close_data``;
# the dispatcher prefers it over the legacy guesser when present. No connector
# populates it yet (US-010 / US-011 migrate the first ones), so the prefer-if-
# present gate is a no-op for every canonical protocol today — their ledger rows
# stay byte-identical.
_PRIMITIVE_MONEY_LEGS_KEY = "primitive_money_legs"


def _declared_money_legs(result: Any) -> Any:
    """Return the connector-DECLARED ``PrimitiveMoneyLegs`` for this result, or None.

    Prefer-if-present (VIB-5218 / US-009): a connector that has been migrated to
    the ``PrimitiveMoneyLeg`` contract (US-008) declares its money legs from its
    extraction path, either as a typed ``result.primitive_money_legs`` attribute
    or under ``result.extracted_data["primitive_money_legs"]`` (the same flexible
    dict that already carries ``lp_open_data`` / ``lp_close_data``). This resolver
    looks in both places and returns the typed object so the dispatcher can map it
    onto the flat ledger columns instead of guessing.

    Strictly typed: anything that is not a ``PrimitiveMoneyLegs`` is ignored
    (returns ``None`` → the dispatcher falls back to the legacy path) rather than
    crashing the accounting write hot path on a malformed declaration. No connector
    declares legs yet, so this returns ``None`` for every canonical protocol and
    the legacy dispatch is reached unchanged (byte-compatible rows).
    """
    if not result:
        return None
    legs = getattr(result, _PRIMITIVE_MONEY_LEGS_KEY, None)
    if legs is None:
        extracted = getattr(result, "extracted_data", None)
        if isinstance(extracted, dict):
            legs = extracted.get(_PRIMITIVE_MONEY_LEGS_KEY)
    if legs is None:
        return None
    # Deferred import: connector value types must never load at module import
    # (the framework -> connector boundary; mirrors ``_fungible_lp_protocols``).
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

    if isinstance(legs, PrimitiveMoneyLegs):
        return legs
    logger.debug(
        "ledger: ignoring non-PrimitiveMoneyLegs value under %r (got %s); falling back to legacy extraction.",
        _PRIMITIVE_MONEY_LEGS_KEY,
        type(legs).__name__,
    )
    return None


def _leg_amount_to_row(amount: Any) -> str:
    """Serialize a leg's ``MeasuredMoney`` amount to its ``transaction_ledger`` row form.

    Measured (a real ``Decimal``, INCLUDING measured zero) → canonical ``str``;
    unmeasured / absent → ``""`` (Empty != Zero, §10.10). Mirrors the row
    semantics of :func:`_measured_amount_to_row` but for an already-typed
    ``MeasuredMoney`` (the contract carries the three states by construction, so
    no re-classification is needed).
    """
    return str(amount.value) if amount.is_measured else ""


def _extract_from_declared_legs(legs: Any) -> _TokensAndAmounts:
    """Project a connector-declared ``PrimitiveMoneyLegs`` onto the flat ledger tuple.

    The role → column mapping (blueprint 27 §6.6): ``INPUT`` / ``PRINCIPAL`` →
    ``token_in`` / ``amount_in``; ``OUTPUT`` → ``token_out`` / ``amount_out``.
    Slot assignment honours the documented per-primitive projection while staying
    a single general rule:

    * Two-sided action (SWAP, STAKE: one input + one output) → first input → in,
      first output → out.
    * Same-role pair (LP_OPEN: two inputs; LP_CLOSE: two outputs) → the two legs
      fill the in / out slots positionally, for lane symmetry with
      :func:`_extract_from_lp_open` / :func:`_extract_from_lp_close`
      (leg0 → in, leg1 → out).
    * One-sided action (PERP_OPEN / REPAY / WITHDRAW: a single principal/input) →
      in slot only; out stays empty.

    ``effective_price`` / ``slippage_bps`` are trade-quality metadata, explicitly
    out of scope for the money-leg contract (§6.6), so they are ``""`` / ``None``.
    Amounts carry Empty != Zero by construction (``MeasuredMoney``), so an
    unmeasured / absent leg projects to ``""`` — never a fabricated zero.

    HARDENING (VIB-5220 — Lido STAKE is the first real consumer of this path,
    which US-009 shipped dormant): the flat ledger tuple carries exactly TWO money
    slots (in / out). The canonical 1- and 2-leg primitives above always fit. A
    connector that declares MORE money-bearing legs than fit would have the surplus
    SILENTLY dropped here — a money-leg loss that must be observable, not silent
    (the Empty != Zero spirit). When that happens we emit a WARN naming the dropped
    legs so the lossiness surfaces instead of corrupting the trade tape quietly;
    the canonical projection itself is unchanged.
    """
    from almanak.connectors._strategy_base.primitive_money_leg import MoneyLegRole

    inputs = [leg for leg in legs.legs if leg.role in (MoneyLegRole.INPUT, MoneyLegRole.PRINCIPAL)]
    outputs = [leg for leg in legs.legs if leg.role is MoneyLegRole.OUTPUT]

    if inputs and outputs:
        in_leg: Any = inputs[0]
        out_leg: Any = outputs[0]
    elif inputs:
        in_leg = inputs[0]
        out_leg = inputs[1] if len(inputs) > 1 else None
    elif outputs:
        in_leg = outputs[0]
        out_leg = outputs[1] if len(outputs) > 1 else None
    else:
        in_leg = out_leg = None

    # Observability: surface any money-bearing leg the 2-slot projection could not
    # carry (e.g. an INPUT + OUTPUT + PRINCIPAL declaration) rather than dropping
    # it silently. ``in_leg`` / ``out_leg`` are the two assigned legs; any other
    # leg is dropped. Identity comparison so duplicate-valued legs are not masked.
    assigned = [leg for leg in (in_leg, out_leg) if leg is not None]
    dropped = [leg for leg in legs.legs if all(leg is not a for a in assigned)]
    if dropped:
        logger.warning(
            "ledger declared-legs projection dropped %d money leg(s) that do not fit the "
            "flat (in/out) ledger tuple (assigned=%s, dropped=%s). The connector declared more "
            "money-bearing legs than the trade tape can carry (VIB-5220).",
            len(dropped),
            [(leg.role.value, leg.token) for leg in assigned],
            [(leg.role.value, leg.token) for leg in dropped],
        )

    token_in = in_leg.token if in_leg is not None else ""
    token_out = out_leg.token if out_leg is not None else ""
    amount_in = _leg_amount_to_row(in_leg.amount) if in_leg is not None else ""
    amount_out = _leg_amount_to_row(out_leg.amount) if out_leg is not None else ""
    return (token_in, token_out, amount_in, amount_out, "", None)


def _extract_tokens_and_amounts(
    intent: Any,
    result: Any,
    chain: str = "",
) -> _TokensAndAmounts:
    """Phase beta -- dispatch between connector-DECLARED legs, SwapAmounts,
    LP_OPEN, PERP_OPEN, lending (REPAY/WITHDRAW), and the intent-attr fallback.

    VIB-5218 (US-009) — control-flow inversion: when a connector DECLARES its
    money legs (a ``PrimitiveMoneyLegs`` on the result, US-008), that typed fact
    is authoritative and drives every column — checked FIRST, before any guess.
    No connector declares legs yet (US-010 / US-011 migrate the first ones), so
    for every canonical protocol this gate is a no-op and the legacy dispatch
    below runs unchanged (byte-identical rows).

    Legacy dispatch (unchanged): a truthy ``result.swap_amounts`` drives every
    field (used by SWAP, LP_CLOSE, and anything whose receipt parser emits
    SwapAmounts). LP_OPEN intents carry amounts in ``LPOpenData`` and have no
    ``from_token`` / ``to_token``, so they get a dedicated extraction path.
    PERP_OPEN collateral lives at ``intent.collateral_token`` /
    ``intent.collateral_amount``, not the standard from_token/to_token chain.
    REPAY/WITHDRAW route through the lending helper so the receipt-resolved amount
    (post-uint256.max decoding by Aave) lands on ``transaction_ledger.amount_in``
    (VIB-3939). Everything else walks the intent-attr precedence chain, which now
    emits a WARN + ``ledger_intent_fallback_total`` metric when it produces a
    money row so the fallback's shrink is trackable (VIB-5218).
    """
    declared_legs = _declared_money_legs(result)
    if declared_legs is not None:
        return _extract_from_declared_legs(declared_legs)
    swap_amounts = getattr(result, "swap_amounts", None) if result else None
    if swap_amounts:
        return _extract_from_swap_amounts(swap_amounts, intent)
    intent_type = _extract_intent_type(intent)
    if intent_type == "LP_OPEN":
        return _extract_from_lp_open(intent, result, chain)
    if intent_type == "LP_CLOSE":
        # VIB-5132 — a V4 native close emits no ERC-20 Transfer for its ETH leg,
        # so there is no ``swap_amounts`` (handled above) and the row used to fall
        # through to the intent-attr fallback (amount_in/out=""). Read the measured
        # proceeds off ``LPCloseData`` instead; fall back only when it yields
        # nothing (no LPCloseData / no token or amount). MUST run after the
        # ``swap_amounts`` short-circuit so V3 ERC-20 closes are untouched.
        lp_close = _extract_from_lp_close(intent, result, chain)
        if lp_close is not None:
            return lp_close
    if intent_type == "PERP_OPEN":
        token_in = getattr(intent, "collateral_token", "") or ""
        amount_in = _measured_amount_to_row(getattr(intent, "collateral_amount", None))
        return (token_in, "", amount_in, "", "", None)
    if intent_type in ("REPAY", "WITHDRAW", "DELEVERAGE"):
        # DELEVERAGE is structurally a repay (closes borrow exposure) — the
        # writer routes it through the same lending receipt path, so the
        # ledger row must too. Without DELEVERAGE here, ``Intent.deleverage(
        # repay_full=True)``'s default ``Decimal("0")`` falls through to the
        # intent-attr fallback and lands ``amount_in=""`` despite the receipt
        # carrying the resolved repaid amount.
        return _extract_from_lending(intent, result, intent_type, chain)
    return _extract_from_intent_fallback(intent, intent_type=intent_type)


def _extract_tx_and_gas(
    result: Any,
    *,
    chain: str = "",
    price_oracle: dict[str, Any] | None = None,
) -> tuple[str, int, str]:
    """Phase gamma -- (tx_hash, gas_used, gas_usd) from the result envelope.

    - ``tx_hash`` = ``result.transaction_results[0].tx_hash or ""`` when the
      list is non-empty; empty-list or missing attr -> ``""``.
    - ``gas_used`` = ``result.total_gas_used or 0`` (None coalesces to 0).
    - ``gas_usd``: when the result carries a pre-computed ``gas_cost_usd``
      (e.g. ResultEnricher's prediction-handler path that already did the
      conversion against a compiler-resolved price) we honour it. Otherwise
      we compute it from ``result.total_gas_cost_wei × native_token_usd``
      via :func:`compute_gas_usd` — this closes the swap/LP/perp/vault gap
      where ``transaction_ledger.gas_usd`` was always ``""`` because no
      writer multiplied the wei figure by the native-token USD price
      (April 30 audit item #3).

    The function returns ``""`` when the oracle cannot resolve the native
    token's price — distinct from ``"0"`` (measured-zero gas) and aligned
    with the lending lane's ``_amount_to_usd`` which returns ``None`` on
    missing prices.  A single WARN is logged at the call site (not here)
    so we never spam the loop.
    """
    if not result:
        return ("", 0, "")

    tx_hash = ""
    tx_results = getattr(result, "transaction_results", None)
    if tx_results:
        # VIB-4087 — parent ``transaction_ledger.tx_hash`` MUST point at
        # the ACTION transaction, not an APPROVAL or INCIDENTAL leg.
        # Pre-fix the writer always picked tx_results[0], which for a
        # SUPPLY = approve+supply bundle pointed at the approval — making
        # the parent tx_hash useless for "what was the action?" audits.
        # ``_classify_sub_tx_role`` uses the same Approval-event-signature
        # heuristic ``_build_sub_transactions`` does, so the parent and
        # the sub_transactions[role=ACTION] entry are guaranteed to agree.
        action_tx = next(
            (tr for tr in tx_results if _classify_sub_tx_role(tr) == "ACTION"),
            None,
        )
        chosen = action_tx if action_tx is not None else tx_results[0]
        tx_hash = chosen.tx_hash or ""

    gas_used = getattr(result, "total_gas_used", 0) or 0

    # 1. Honour a pre-computed gas_cost_usd if the upstream writer set one
    #    (today: ResultEnricher's prediction-handler path; tomorrow: any
    #    enricher that has its own price source). This preserves backward
    #    compatibility for the small set of intent types that already had
    #    gas_usd populated, AND lets the universal swap/LP path below take
    #    over for everyone else.
    gas_cost_usd_attr = getattr(result, "gas_cost_usd", None)
    if gas_cost_usd_attr is not None:
        return (tx_hash, gas_used, str(gas_cost_usd_attr))

    # 2. Universal path: compute gas_usd from total_gas_cost_wei × native USD
    #    price. We import lazily so the observability module stays free of
    #    the accounting package as a hard import-time dependency (the layers
    #    are siblings; cycles here are easy to introduce by accident).
    from almanak.framework.accounting.gas_pricing import compute_gas_usd

    gas_cost_wei = getattr(result, "total_gas_cost_wei", None)
    gas_cost = compute_gas_usd(
        gas_cost_wei=gas_cost_wei,
        chain=chain,
        price_oracle=price_oracle,
    )
    if gas_cost is None:
        return (tx_hash, gas_used, "")
    return (tx_hash, gas_used, str(gas_cost))


def _coalesce_error(success: bool, error: str, result: Any) -> str:
    """Phase delta -- if the caller said "failed" and supplied no error
    string, fall back to ``result.error`` (coalescing None -> "").

    Caller-supplied error always wins; success=True skips the branch.
    """
    if not success and not error and result:
        return getattr(result, "error", "") or ""
    return error


# VIB-4087 — ERC20 Approval event topic (`keccak("Approval(address,address,uint256)")`).
# A receipt that emits this is an APPROVAL transaction; non-Approval-emitting
# receipts are classified as ACTION. INCIDENTAL is reserved for future
# heuristic refinement (e.g. nonce-bump-only transactions).
_ERC20_APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"


def _classify_sub_tx_role(tx_result: Any) -> str:
    """VIB-4087 — APPROVAL / ACTION / INCIDENTAL classification.

    Heuristic: a receipt whose logs contain ONLY Approval events is an
    APPROVAL transaction. A receipt with at least one non-Approval event
    (Transfer, Mint, Swap, Burn, Supply, Borrow, etc.) is an ACTION.
    Receipts with no logs default to ACTION (safe default — never
    silently elevate an unclassified leg to APPROVAL).

    Why "only-Approval" instead of "any-Approval"? ERC20 and ERC721 emit
    the same canonical ``Approval(address,address,uint256)`` topic
    (``0x8c5be1e5...``) — the same signature is reused for ERC721's
    ``Approval(owner, approved, tokenId)``. An LP_OPEN that mints a
    Uniswap V3 NFT therefore emits an ERC721 Approval as a side-effect
    of the mint, and the naive "any-Approval-event => APPROVAL" rule
    misclassified the action transaction as APPROVAL — the very bug
    that breaks the parent-tx_hash invariant.

    Why event-signature classification (not function-selector)? The
    TransactionResult envelope carries the receipt + logs but not the
    original calldata. Function selector lives on the unsigned
    transaction, which is gone by the time ledger writing runs.
    """
    receipt = getattr(tx_result, "receipt", None)
    logs = []
    if receipt is not None:
        # logs may be on the TransactionResult directly or nested on the receipt.
        receipt_logs = getattr(receipt, "logs", None)
        if receipt_logs:
            logs = receipt_logs
    if not logs:
        logs = getattr(tx_result, "logs", []) or []
    if not logs:
        # No logs at all (e.g. value-transfer or revert handled upstream)
        # — default to ACTION rather than fabricate APPROVAL.
        return "ACTION"

    saw_approval = False
    saw_other = False
    for log in logs:
        # log entries are dicts with a "topics" list whose first entry is the event signature.
        if not isinstance(log, dict):
            topics = getattr(log, "topics", None) or []
        else:
            topics = log.get("topics") or []
        if not topics:
            # An event with no topics is technically valid but never an
            # Approval — count it as "other" to flip the row to ACTION.
            saw_other = True
            continue
        first = topics[0]
        # Topic values arrive in three shapes across providers:
        #   * ``HexBytes`` / ``bytes`` — ``.hex()`` returns no leading "0x"
        #   * ``str`` already prefixed (``"0x..."`` / ``"0X..."``)
        #   * ``str`` without prefix (some custom RPC clients)
        # Normalise to a single ``0x...`` lowercase form before comparing,
        # otherwise either the unprefixed-bytes case or the upper-case-prefix
        # case slips through and the row gets misclassified as ACTION.
        first_str = first.hex() if hasattr(first, "hex") else str(first)
        first_str = first_str.lower()
        if not first_str.startswith("0x"):
            first_str = f"0x{first_str}"
        if first_str == _ERC20_APPROVAL_TOPIC:
            saw_approval = True
        else:
            saw_other = True

    # Only-Approval ⇒ pure ERC20 / ERC721 approve() call ⇒ APPROVAL.
    # Approval-plus-other or other-only ⇒ ACTION. This correctly classifies:
    #   - approve(WETH, router) → only Approval ⇒ APPROVAL
    #   - mint NFT (emits Transfer + Approval(0x0,owner,tokenId)) ⇒ ACTION
    #   - swap (emits Swap + Approval reset) ⇒ ACTION
    if saw_approval and not saw_other:
        return "APPROVAL"
    return "ACTION"


def _function_selector_from_receipt(tx_result: Any) -> str:
    """Best-effort 4-byte selector. Pre VIB-4087 the selector lived only
    on the original unsigned-transaction calldata and is not preserved on
    TransactionResult; until that plumbing lands, we leave it empty. The
    sub-transactions JSON contract still includes the key so consumers
    can rely on the shape; readers treat ``""`` as unknown."""
    return ""


def _build_sub_transactions(tx_results: list[Any]) -> list[dict[str, Any]]:
    """VIB-4087 — build the typed ``sub_transactions`` array.

    See `_classify_sub_tx_role` for the role-detection contract. The
    returned shape is the contract documented on VIB-4087 and pinned by
    `accounting_regression_assert.gate_sub_transactions`.
    """
    out: list[dict[str, Any]] = []
    for tr in tx_results:
        receipt = getattr(tr, "receipt", None)
        # status: prefer receipt.status (1/0) when present; otherwise infer
        # from the result's success flag. Both forms map to "success" / "failure".
        status: str
        if receipt is not None and getattr(receipt, "status", None) is not None:
            status = "success" if int(receipt.status) == 1 else "failure"
        else:
            status = "success" if getattr(tr, "success", True) else "failure"
        target_contract = getattr(receipt, "to_address", None) if receipt is not None else None
        out.append(
            {
                "tx_hash": getattr(tr, "tx_hash", "") or "",
                "target_contract": str(target_contract or ""),
                "function_selector": _function_selector_from_receipt(tr),
                "gas_used": getattr(tr, "gas_used", 0) or 0,
                "status": status,
                "role": _classify_sub_tx_role(tr),
            }
        )
    return out


def _build_extracted_data_json(result: Any) -> str:
    """Phase epsilon -- serialize ``result.extracted_data`` with type tags,
    and for every result with a transaction list augment the payload with a
    ``sub_transactions`` array capturing each leg's hash, target, gas,
    status, and role (APPROVAL / ACTION / INCIDENTAL).

    Returns ``""`` when the result lacks extracted_data (attribute absent or
    dict empty). VIB-4087: the ``sub_transactions`` array is always emitted
    when the result has at least one transaction — even single-tx results
    — so consumers can rely on the key existing on every successful row.
    Pre-fix only multi-tx results were augmented (``all_tx_results``), and
    operators couldn't tell "single tx" from "missing data."

    Defensive ``try/except`` around the augmentation keeps the original
    serialization on any JSON decode failure (today unreachable from prod,
    but the safety net is cheap and matches the pre-refactor contract).
    """
    if not result:
        return ""

    extracted = getattr(result, "extracted_data", None) or {}
    tx_results = getattr(result, "transaction_results", None) or []

    # No extracted payload AND no tx receipts → nothing to record.
    if not extracted and not tx_results:
        return ""

    extracted_data_json = serialize_extracted_data(extracted) if extracted else ""

    if not tx_results:
        # No tx receipts (e.g., a value-transfer or compile-failure path that
        # set extracted_data without executing) — return what we have.
        return extracted_data_json

    # VIB-4087 — always emit sub_transactions when tx_results exist, even on
    # rows where the connector didn't populate extracted_data. Operators
    # rely on the key being present to distinguish "single tx" from
    # "missing data," so a payload-less successful row must still get the
    # sub_transactions array (otherwise the audit trail loses the
    # APPROVAL/ACTION/INCIDENTAL leg breakdown for that intent class).
    try:
        parsed = json.loads(extracted_data_json) if extracted_data_json else {}
    except (json.JSONDecodeError, TypeError):
        return extracted_data_json  # keep existing serialization on failure

    if not isinstance(parsed, dict):
        # Defensive: serialize_extracted_data should always produce a dict
        # but if a stub returned a list/scalar, fall through without
        # augmentation rather than crashing.
        return extracted_data_json

    parsed["sub_transactions"] = _build_sub_transactions(tx_results)
    # Back-compat: keep ``all_tx_results`` for any reader still on the
    # pre-VIB-4087 schema. Strictly cheaper than coordinating a removal.
    if len(tx_results) > 1:
        parsed["all_tx_results"] = [
            {
                "tx_hash": getattr(tr, "tx_hash", "") or "",
                "gas_used": getattr(tr, "gas_used", 0) or 0,
                "success": getattr(tr, "success", True),
            }
            for tr in tx_results
        ]
    try:
        return json.dumps(parsed)
    except (TypeError, ValueError):
        return extracted_data_json


def _fungible_lp_protocols() -> frozenset[str]:
    """Fungible-LP (ERC20 LP-token) venues where ``position_id`` is not an NFT id.

    Derived from each connector's manifest ``fungible_lp`` flag (VIB-4851 C2).
    On Curve the close intent's ``position_id`` is overloaded as the LP-token
    *amount* to burn (a human-decimal string), so stamping it as a per-position
    discriminator (VIB-4275) would write a bogus amount-shaped ``position_id``
    onto the fungible-LP close event — which has no co-leg to disambiguate
    (one balance per pool). VIB-4968.

    Recomputed per call — a cheap filter over the registry's cached manifest
    tuple — so test-side ``CONNECTOR_REGISTRY.clear()`` is honoured; a
    module-level cache here would serve stale sets after a registry reset.
    """
    # Deferred import: connector discovery must never run at module import.
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    return frozenset(connector.name for connector in CONNECTOR_REGISTRY.with_fungible_lp())


def _stamp_lp_close_discriminator(intent: Any, result: Any, intent_type: str, protocol: str = "") -> None:
    """Stamp the close intent's ``position_id`` onto ``result.extracted_data["lp_close_data"]`` (VIB-4275).

    The close RECEIPT does not re-emit the closing NFT's token id (a Burn /
    DecreaseLiquidity event carries no NFT id), so receipt parsers leave
    ``LPCloseData.position_id`` as ``None``. The close INTENT, however, names
    exactly which position is being closed (``LPCloseIntent.position_id`` is a
    required field). This is the single correct capture point: the runner holds
    both the intent and the enriched result here, just before serialization into
    the existing ``transaction_ledger.extracted_data_json`` JSON column (SDK-
    owned; no new DB column, no Postgres DDL).

    No-op unless this is an LP_CLOSE / LP_COLLECT_FEES with a usable
    ``position_id`` on the intent and an ``LPCloseData`` already present on the
    result. ``LPCloseData`` is frozen, so the stamped copy is written back via
    :func:`dataclasses.replace`. Idempotent: a discriminator already present on
    the close data (e.g. a future parser that learns to emit it) is preserved.

    Skipped entirely for fungible-LP venues
    (:func:`_fungible_lp_protocols`) where the close intent's
    ``position_id`` is overloaded as a burn *amount*, not an NFT id (VIB-4968).
    """
    if intent_type not in ("LP_CLOSE", "LP_COLLECT_FEES"):
        return
    if (protocol or "").lower() in _fungible_lp_protocols():
        return
    raw = getattr(intent, "position_id", None)
    # Uniformly ignore the degenerate 0 / "0" id across stamp + both resolvers:
    # never stamp a discriminator the resolver will discard (gemini review on #2459).
    if raw is None or raw == "" or raw == 0 or str(raw).strip() == "0":
        return
    extracted = getattr(result, "extracted_data", None) if result else None
    if not isinstance(extracted, dict):
        return
    close_data = extracted.get("lp_close_data")
    if close_data is None or not hasattr(close_data, "position_id"):
        return
    # Preserve an already-stamped discriminator (Empty ≠ Zero — do not clobber
    # a real parser-emitted value with the intent's).
    if getattr(close_data, "position_id", None):
        return
    import dataclasses

    try:
        extracted["lp_close_data"] = dataclasses.replace(close_data, position_id=str(raw))
    except (TypeError, ValueError):
        # Defensive: a non-dataclass duck-typed close-data stub (tests) — leave
        # it untouched rather than raise on the ledger-write path.
        return


def _stamp_v4_lp_close_fees(
    result: Any,
    intent_type: str,
    fees: tuple[int, int] | None,
) -> None:
    """Stamp PRE-close-measured V4 uncollected fees onto ``LPCloseData`` (VIB-4482).

    Uniswap V4's ``ModifyLiquidity`` burn event carries no amounts and bundles
    fees into the single withdrawal Transfer, so the close RECEIPT cannot
    separate fees from principal (unlike V3, which differences Collect−Burn
    legs in the same receipt). The receipt parser therefore leaves
    ``LPCloseData.fees0/fees1 = None`` (honest "unmeasured", Empty ≠ Zero).

    The runner reads ``tokens_owed0/tokens_owed1`` on-chain *before* the burn
    submits (a post-burn read returns zero liquidity → zero fees) via the
    gateway ``QueryV4PositionState`` RPC and threads the raw-int pair here. This
    is the single correct stamp point: the runner holds the enriched result just
    before it is serialized into ``transaction_ledger.extracted_data_json``, and
    every downstream LP consumer (the accounting handler's ``_resolve_lp_amounts``
    and the ``position_events`` builder) already reads ``lp_close_data.fees0/1``.

    ``fees`` is ``(tokens_owed0, tokens_owed1)`` in PoolKey-currency0/1 order —
    the same order ``LPCloseData.currency0`` / ``amount0_collected`` use, because
    both the gateway read and the parser derive ordering from the same canonical
    PoolKey — so ``fees0 ↔ tokens_owed0 ↔ currency0`` align with no transpose.

    Precision bound (inherent, accepted): ``tokens_owed0/1`` is read at the
    pre-execute block while ``amount0_collected`` is measured at the burn block.
    If the pool accrues additional fees in the gap (intervening swaps before the
    burn lands), the stamped fees slightly UNDER-state the fee subset actually
    inside ``amount0_collected``, so a downstream ``il_usd = cost_basis −
    fees_total`` marginally over-states principal-only LP value. Unavoidable
    without a same-block read; small in practice; never violates Empty ≠ Zero
    and never double-counts.

    No-op unless this is an ``LP_CLOSE`` / ``LP_COLLECT_FEES`` carrying a usable
    ``(int, int)`` pair and a V4-shaped ``LPCloseData`` (``currency0`` populated —
    the connector-agnostic capability signal, not a protocol-string match). Like
    :func:`_stamp_lp_close_discriminator`, the frozen dataclass is replaced via
    :func:`dataclasses.replace` (which re-runs ``__post_init__``, correctly
    deriving ``fee_separation_method="SEPARATE"`` / ``fee_confidence="EXACT"`` —
    honest, since the fees are exact on-chain reads). ``fees = None`` (the read
    was unavailable / failed) leaves ``fees0/fees1 = None`` untouched — never
    fabricates a zero. ``Decimal("0")`` / ``0`` is only written when the gateway
    *measured* zero owed fees. Idempotent: a parser that somehow emitted measured
    fees is preserved (no clobber).
    """
    if fees is None:
        return
    if intent_type not in ("LP_CLOSE", "LP_COLLECT_FEES"):
        return
    extracted = getattr(result, "extracted_data", None) if result else None
    if not isinstance(extracted, dict):
        return
    # Re-read AFTER ``_stamp_lp_close_discriminator`` (which also replaces
    # ``extracted["lp_close_data"]``) so this stamp operates on the latest copy.
    close_data = extracted.get("lp_close_data")
    if close_data is None or not hasattr(close_data, "fees0"):
        return
    # Capability-gate on the V4 PoolKey data shape rather than a hard-coded
    # protocol string (blueprint 22 / scan-coupling): only the V4 receipt parser
    # populates ``currency0``/``currency1`` on ``LPCloseData`` (V3 leaves them
    # None), and the ``(tokens_owed0, tokens_owed1)`` pair only exists for a V4
    # position — so currency0 presence is the precise "this is a V4 close" signal.
    if getattr(close_data, "currency0", None) is None:
        return
    # Preserve a parser-measured fee pair (Empty ≠ Zero — do not clobber a real
    # parser-emitted value with the gateway read).
    if getattr(close_data, "fees0", None) is not None or getattr(close_data, "fees1", None) is not None:
        return
    raw0, raw1 = fees
    import dataclasses

    try:
        # Reset the fee taxonomy to the ``"UNKNOWN"`` sentinel alongside the new
        # fee pair so ``__post_init__`` RE-DERIVES it: the parser stamped
        # ``BUNDLED`` (its honest "couldn't separate" verdict) when it emitted
        # ``fees0=None``, and ``replace`` would otherwise carry that stale
        # ``BUNDLED`` forward. The gateway-measured fees ARE separated and exact,
        # so re-deriving correctly yields ``SEPARATE`` / ``EXACT`` — the honest
        # taxonomy for an on-chain ``tokens_owed`` read.
        extracted["lp_close_data"] = dataclasses.replace(
            close_data,
            fees0=int(raw0),
            fees1=int(raw1),
            fee_separation_method="UNKNOWN",
            fee_confidence="UNKNOWN",
        )
    except (TypeError, ValueError):
        # Defensive: a non-dataclass duck-typed close-data stub (tests) — leave
        # it untouched rather than raise on the ledger-write path.
        return


def _stamp_lp_open_native_amounts(
    result: Any,
    intent_type: str,
    amounts: tuple[int | None, int | None] | None,
) -> None:
    """Stamp a runner-measured native-leg amount onto ``LPOpenData``.

    Connector-agnostic native-leg fill (VIB-4483 V4 concentrated pools, VIB-5121
    Fluid fungible DEX LP). A native-ETH LP leg deposits via ``msg.value`` — there
    is NO ERC-20 Transfer for that leg, so the mint RECEIPT cannot measure it and
    the receipt parser leaves it ``None`` (honest "unmeasured", Empty ≠ Zero —
    never a fabricated zero). The runner measures the native amount AFTER the tx
    lands (V4: a post-mint position-state gateway read + concentrated-liquidity
    math; Fluid + any fungible native-leg LP: a block-pinned wallet
    native-balance bracket, gas-separated) and threads the raw-int pair here. This
    is the single correct stamp point: the runner holds the enriched result just
    before it is serialised into ``transaction_ledger.extracted_data_json``, and
    the LP accounting handler reads ``lp_open_data.amount0/amount1`` straight off
    it (``lp_accounting.build_lp_accounting_event``).

    ``amounts`` is ``(amount0, amount1)`` in the SAME currency0/1 order
    ``LPOpenData.amount0``/``currency0`` use (the runner capture and the parser
    derive ordering from the same canonical token pair).

    Symmetric with :func:`_stamp_v4_lp_close_fees`:

    * ``amounts = None`` (read unavailable / failed / not a native pool) → leaves
      ``LPOpenData`` untouched. The native leg stays ``None`` (honest unmeasured),
      never fabricates a zero.
    * Fills ONLY a leg the parser left ``None`` (the unmeasured native leg). A
      leg the parser already MEASURED from a Transfer (the ERC-20 side, or a
      genuine ``0`` for an out-of-range ERC-20 leg) is preserved — the gateway
      read never clobbers a measured value (Empty ≠ Zero idempotence).
    * No-op unless this is an ``LP_OPEN`` carrying ``LPOpenData`` with
      ``currency0`` populated — the connector-agnostic capability signal (a
      parser that resolves token legs by ADDRESS), not a protocol-string match.

    Precision bound (inherent, accepted): the native amount is measured just
    after the tx block while the ERC-20 leg is measured at the tx block. For a
    same-cycle open there are no intervening balance changes for the wallet, so
    the measured native amount reflects the same deposit; the ``int()`` floor /
    balance bracket drops at most sub-wei. Mirrors the close-fee stamp's
    documented gap-read bound.
    """
    if amounts is None:
        return
    if intent_type != "LP_OPEN":
        return
    extracted = getattr(result, "extracted_data", None) if result else None
    if not isinstance(extracted, dict):
        return
    open_data = extracted.get("lp_open_data")
    if open_data is None or not hasattr(open_data, "amount0"):
        return
    # Capability-gate on the by-address data shape (currency0/currency1 populated
    # by parsers that resolve legs by address — V4, fluid_dex_lp) rather than a
    # hard-coded protocol string.
    if getattr(open_data, "currency0", None) is None:
        return

    raw0, raw1 = amounts
    # Fill only the legs the parser left unmeasured (``None``). A measured leg
    # (the ERC-20 side, or a genuine measured ``0``) is preserved — never clobber.
    new0 = getattr(open_data, "amount0", None)
    new1 = getattr(open_data, "amount1", None)
    changed = False
    if new0 is None and raw0 is not None:
        new0 = int(raw0)
        changed = True
    if new1 is None and raw1 is not None:
        new1 = int(raw1)
        changed = True
    if not changed:
        return

    import dataclasses

    try:
        extracted["lp_open_data"] = dataclasses.replace(open_data, amount0=new0, amount1=new1)
    except (TypeError, ValueError):
        # Defensive: a non-dataclass duck-typed open-data stub (tests) — leave it
        # untouched rather than raise on the ledger-write path.
        return


# The V4 native-currency sentinel (the zero address) — the same value the
# connector parser and the runner's open-side eligibility gate use. Kept as a
# local plain-string constant (not a connector import) to honour the framework →
# connector boundary; it is the well-known zero address, not connector logic.
_V4_NATIVE_CURRENCY = "0x" + "0" * 40

# The ERC-7528 / Fluid SmartLending native sentinel — the non-V4 native marker
# the fungible-LP close stamp (VIB-5121) recognizes. Framework-owned (no connector
# import), mirrors ``strategy_runner._ERC7528_NATIVE_SENTINEL``. Written in EIP-55
# checksum form (the production-address checksum gate requires it) and lowercased
# for the comparison (currencies are compared ``.lower()``-ed below).
_ERC7528_NATIVE_CURRENCY = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()


def _stamp_v4_lp_close_native_principal(
    result: Any,
    intent_type: str,
    amounts: tuple[int | None, int | None] | None,
) -> None:
    """Stamp PRE-burn-measured V4 native-leg close PRINCIPAL onto ``LPCloseData`` (VIB-5117).

    A native-ETH V4 pool (``PoolKey.currency == 0x0``) returns its ETH leg to the
    wallet as raw ETH via ``TAKE_PAIR`` on close — there is NO ERC-20 Transfer for
    that leg, so the burn RECEIPT cannot measure it and the V4 receipt parser
    leaves that leg's ``amount{0,1}_collected = None`` (honest "unmeasured",
    Empty ≠ Zero — never a fabricated zero). The runner derives the principal from
    the freshly-read pre-burn position state (``liquidity`` + ``sqrt_price_x96`` +
    ticks → the framework's concentrated-liquidity math) and threads the raw-int
    pair here. This is the single correct stamp point: the runner holds the
    enriched result just before it is serialised into
    ``transaction_ledger.extracted_data_json``, and the LP accounting handler reads
    ``lp_close_data.amount{0,1}_collected`` straight off it
    (``lp_accounting.build_lp_accounting_event`` → ``received_value_usd`` →
    ``realized_pnl_usd``).

    ``amounts`` is ``(amount0, amount1)`` in PoolKey-currency0/1 order — the same
    order ``LPCloseData.amount0_collected``/``currency0`` use, because both the
    gateway read and the parser derive ordering from the same canonical PoolKey.

    The exact close-side mirror of :func:`_stamp_v4_lp_open_native_amounts`:

    * ``amounts = None`` (read unavailable / failed / not a native pool) → leaves
      ``LPCloseData`` untouched. The native leg stays ``None`` (honest unmeasured),
      never fabricates a zero.
    * Fills ONLY a leg the parser left ``None`` (the unmeasured native leg). A
      leg the parser already MEASURED from a Transfer (the ERC-20 side, or a
      genuine ``0`` for an unobserved ERC-20 leg) is preserved — the gateway read
      never clobbers a measured value (Empty ≠ Zero idempotence).
    * No-op unless this is an ``LP_CLOSE`` carrying a V4-shaped ``LPCloseData``
      (``currency0`` populated — the connector-agnostic capability signal, not a
      protocol-string match).

    Precision bound (inherent, accepted): the position state is read just before
    the burn while the ERC-20 leg is measured at the burn block. For a same-cycle
    close there are no intervening liquidity changes, so the derived native amount
    reflects the burned position; the ``int()`` floor drops at most sub-wei.
    Mirrors the open-stamp / close-fee gap-read bound.
    """
    if amounts is None:
        return
    if intent_type != "LP_CLOSE":
        return
    extracted = getattr(result, "extracted_data", None) if result else None
    if not isinstance(extracted, dict):
        return
    # Re-read AFTER ``_stamp_lp_close_discriminator`` / ``_stamp_v4_lp_close_fees``
    # (which also replace ``extracted["lp_close_data"]``) so this stamp operates on
    # the latest copy.
    close_data = extracted.get("lp_close_data")
    if close_data is None or not hasattr(close_data, "amount0_collected"):
        return
    # Capability-gate on the V4 PoolKey data shape (currency0/currency1 populated
    # only by the V4 parser) rather than a hard-coded protocol string.
    if getattr(close_data, "currency0", None) is None:
        return

    raw0, raw1 = amounts
    # Fill only the legs the parser left unmeasured (``None``). A measured leg
    # (the ERC-20 side, or a genuine measured ``0``) is preserved — never clobber.
    # Defense-in-depth (mirrors the open-side ``_native_v4_open_eligible`` rigor):
    # gate the derived fill on the leg's currency being the native sentinel. The
    # V4 parser leaves a leg ``None`` ONLY for the native currency (an unobserved
    # ERC-20 leg is a measured ``0``), so today every ``None`` leg here is already
    # native — but checking the currency keeps this correct-by-construction should
    # a future parser change ever emit a ``None`` ERC-20 leg: a derived value must
    # never land on an ERC-20 leg whose true amount comes from its Transfer.
    cur0 = (getattr(close_data, "currency0", None) or "").lower()
    cur1 = (getattr(close_data, "currency1", None) or "").lower()
    new0 = getattr(close_data, "amount0_collected", None)
    new1 = getattr(close_data, "amount1_collected", None)
    changed = False
    if new0 is None and raw0 is not None and cur0 == _V4_NATIVE_CURRENCY:
        new0 = int(raw0)
        changed = True
    if new1 is None and raw1 is not None and cur1 == _V4_NATIVE_CURRENCY:
        new1 = int(raw1)
        changed = True
    if not changed:
        return

    import dataclasses

    try:
        extracted["lp_close_data"] = dataclasses.replace(close_data, amount0_collected=new0, amount1_collected=new1)
    except (TypeError, ValueError):
        # Defensive: a non-dataclass duck-typed close-data stub (tests) — leave it
        # untouched rather than raise on the ledger-write path.
        return


def _stamp_lp_close_native_amounts(
    result: Any,
    intent_type: str,
    amounts: tuple[int | None, int | None] | None,
) -> None:
    """Stamp a runner-measured FUNGIBLE native-leg RETURNED amount onto ``LPCloseData`` (VIB-5121).

    The FLUID / fungible-LP close-side twin of the open-side native fill. A
    fungible-pool native-ETH leg (e.g. Fluid SmartLending fSL5 FLUID/ETH) is
    returned to the wallet via an internal call that emits NO ERC-20 Transfer, so
    the log-based parser leaves the corresponding ``amountN_collected`` ``None``
    (Empty ≠ Zero). There is no position state to read on a fungible pool, so the
    runner measures the returned native amount from a block-pinned wallet
    native-balance bracket (gas-separated: ``returned = post − pre + gas``) and
    threads the raw-int pair here.

    Per-connector measurement (rule-of-three: only V4 + Fluid exist today, so the
    two stamps stay distinct — see :func:`_stamp_v4_lp_close_native_principal`,
    VIB-5117): this stamp handles the FUNGIBLE native case ONLY and MUST NOT touch
    a V4 concentrated leg. V4 is measured more accurately from pre-burn position
    state (no gas confounding) and is stamped by the V4-specific function above.
    The currency-sentinel gate below makes that boundary correct-by-construction:
    a leg is filled here only when its currency is a NON-V4 native sentinel (the
    ERC-7528 ``0xEeee…`` marker). Unification behind one injected-strategy stamp
    is deferred until a 3rd native-leg connector lands (VIB-5135).

    ``amounts`` is ``(amount0_collected, amount1_collected)`` in the same
    currency0/1 order ``LPCloseData`` uses.

    * ``amounts = None`` → leaves ``LPCloseData`` untouched (native leg stays
      ``None`` — honest unmeasured, never a fabricated zero).
    * Fills ONLY a leg the parser left ``None`` whose currency is a NON-V4 native
      sentinel; a parser-measured leg, an ERC-20 leg, or a V4 native leg is left
      untouched (never clobbered; V4 is the V4 stamp's job — Empty ≠ Zero).
    * No-op unless this is a close-like intent (``LP_CLOSE`` / ``LP_COLLECT_FEES``)
      carrying ``LPCloseData`` with ``currency0`` populated. ``LP_COLLECT_FEES`` is
      included for parity with :func:`_stamp_lp_close_discriminator` /
      :func:`_stamp_v4_lp_close_fees`: a fungible fee-collect that returns a native
      leg with no ERC-20 Transfer is measured by the same runner balance bracket
      (``_capture_native_lp_close_amounts_safe`` does not gate on intent_type), so
      dropping it here would discard a measured native amount (Empty ≠ Zero).
    """
    if amounts is None:
        return
    if intent_type not in ("LP_CLOSE", "LP_COLLECT_FEES"):
        return
    extracted = getattr(result, "extracted_data", None) if result else None
    if not isinstance(extracted, dict):
        return
    # Re-read AFTER any earlier ``lp_close_data`` replace (discriminator / close
    # fees / the V4 native-principal stamp) so this stamp operates on the latest
    # copy.
    close_data = extracted.get("lp_close_data")
    if close_data is None or not hasattr(close_data, "amount0_collected"):
        return
    if getattr(close_data, "currency0", None) is None:
        return

    raw0, raw1 = amounts
    # NARROW to the FUNGIBLE native case: fill a ``None`` leg ONLY when its
    # currency is a non-V4 native sentinel. A V4 native leg (``0x0``) is the V4
    # stamp's responsibility (measured from position state); never stamp it here
    # from a balance bracket. Mirrors the V4 stamp's defense-in-depth currency
    # gate (rule-of-three: per-connector measurement, no cross-claim).
    cur0 = (getattr(close_data, "currency0", None) or "").lower()
    cur1 = (getattr(close_data, "currency1", None) or "").lower()
    new0 = getattr(close_data, "amount0_collected", None)
    new1 = getattr(close_data, "amount1_collected", None)
    changed = False
    if new0 is None and raw0 is not None and cur0 == _ERC7528_NATIVE_CURRENCY:
        new0 = int(raw0)
        changed = True
    if new1 is None and raw1 is not None and cur1 == _ERC7528_NATIVE_CURRENCY:
        new1 = int(raw1)
        changed = True
    if not changed:
        return

    import dataclasses

    try:
        extracted["lp_close_data"] = dataclasses.replace(close_data, amount0_collected=new0, amount1_collected=new1)
    except (TypeError, ValueError):
        # Defensive: a non-dataclass duck-typed close-data stub (tests).
        return


def build_ledger_entry(
    *,
    deployment_id: str,
    cycle_id: str,
    intent: Any,
    result: Any,
    chain: str = "",
    success: bool = True,
    error: str = "",
    price_oracle: dict[str, Any] | None = None,
    pre_state: dict[str, Any] | None = None,
    post_state: dict[str, Any] | None = None,
    v4_lp_close_fees: tuple[int, int] | None = None,
    # Native-leg open fill. ``lp_open_native_amounts`` is the connector-merged
    # OPEN result (V4 position-state OR Fluid balance-bracket — the runner picks
    # per connector and threads ONE value). VIB-5117's V4-close principal stays
    # its own param; VIB-5121 adds the Fluid-close balance-bracket param. Per
    # connector measurement (rule-of-three — unify at the 3rd connector, VIB-5135).
    lp_open_native_amounts: tuple[int | None, int | None] | None = None,
    v4_lp_close_native_principal: tuple[int | None, int | None] | None = None,
    lp_close_native_amounts: tuple[int | None, int | None] | None = None,
) -> LedgerEntry:
    """Build a LedgerEntry from an intent and its execution result.

    Extracts structured trade data from the enriched result object
    (swap_amounts, lp_close_data, etc.) so callers don't need to
    know the extraction details. Sequences the phase helpers
    alpha -> beta -> gamma -> delta -> epsilon; see module docstring.

    ``price_oracle`` is a flat ``{symbol: usd_price}`` dict — typically
    ``state.price_oracle`` from the strategy_runner.  When supplied, the
    gamma phase computes ``gas_usd`` from ``total_gas_cost_wei × native_usd``
    via ``accounting.gas_pricing.compute_gas_usd``.  Without it,
    ``gas_usd`` falls back to ``""`` (which is what the column held before
    the April 30 audit identified the gap).  A WARN is logged when the
    oracle is supplied but cannot resolve the native-token price — once
    per ledger write, never per helper invocation.

    ``pre_state`` / ``post_state`` (Accounting-AttemptNo17 §3 D3): typed
    snapshots of protocol state captured by the runner BEFORE submission and
    AFTER confirmation. When supplied, they're serialized to JSON and stored
    on ``LedgerEntry.pre_state_json`` / ``post_state_json`` (VIB-3480 columns
    that have been universally NULL since the columns were added). The runner
    is the only correct capture point — see Accounting-AttemptNo17 §3 D3 and
    docs/internal/connector-pre-post-audit.md.

    ``price_oracle`` is also serialized to ``price_inputs_json`` so every
    ledger row carries the oracle snapshot used at execution time. Auditors
    grep ``price_inputs_json`` to filter "exposure by oracle" — the
    ``oracle_source`` field on each priced asset is required (G12).
    """
    intent_type = _extract_intent_type(intent)
    # VIB-5132 — token/amount extraction is DEFERRED to after the LP-close native
    # stamps below. ``_extract_from_lp_close`` reads ``LPCloseData.amount{0,1}_
    # collected``, and the native-ETH legs are populated by
    # ``_stamp_v4_lp_close_native_principal`` / ``_stamp_lp_close_native_amounts``
    # (VIB-5117 / VIB-5121). Extracting here (the historical position) would read
    # the PRE-stamp ``None`` legs and re-emit empty amount_in/out. Nothing between
    # this point and the deferred call consumes the extraction tuple (verified:
    # tx/gas, the gas WARN, error coalescing, and the stamps depend only on
    # ``result`` / ``intent`` / ``intent_type``).
    tx_hash, gas_used, gas_usd = _extract_tx_and_gas(
        result,
        chain=chain,
        price_oracle=price_oracle,
    )
    if (
        result is not None
        and gas_usd == ""
        and (getattr(result, "total_gas_cost_wei", None) or 0) > 0
        and price_oracle is not None
    ):
        # Conversion is empty even though the oracle was supplied. Two
        # paths reach here today:
        #   (a) the native token's USD price genuinely isn't in the oracle —
        #       the original case this WARN was added for;
        #   (b) the chain is non-EVM (``solana``) — the helper fails closed
        #       on lamport vs wei unit mismatch by design.
        # The original WARN message named (a) as the cause; emitting it on
        # (b) would mislead operators ("missing SOL price" when the SOL
        # price IS in the oracle but the unit conversion path isn't yet
        # supported). Gate the WARN on (a) only.
        from almanak.framework.accounting.gas_pricing import native_token_for_chain

        native_symbol = native_token_for_chain(chain)
        oracle_has_native = any(
            price_oracle.get(key) is not None for key in (native_symbol.upper(), native_symbol, native_symbol.lower())
        )
        if not oracle_has_native:
            logger.warning(
                "ledger gas_usd unavailable: chain=%s native_token=%s missing from price_oracle "
                "(deployment_id=%s, cycle_id=%s); transaction_ledger.gas_usd will be empty for this row",
                chain,
                native_symbol,
                deployment_id,
                cycle_id,
            )
    final_error = _coalesce_error(success, error, result)
    # VIB-4275 — stamp the close intent's per-position discriminator onto
    # ``lp_close_data`` BEFORE serialization. The close RECEIPT cannot carry the
    # token id (a Burn emits no NFT id), but the close INTENT
    # (``LPCloseIntent.position_id``) knows exactly which NFT is being closed.
    # The close-side accounting resolver reads this back off ``extracted_data_json``
    # to attribute a co-pool close to its OWN prior open.
    protocol = getattr(intent, "protocol", "") or ""
    _stamp_lp_close_discriminator(intent, result, intent_type, protocol)
    # VIB-4482 (P-V1-A) — stamp PRE-close-measured V4 uncollected fees onto
    # ``lp_close_data`` so the LP accounting handler emits measured fees instead
    # of the receipt-parser's honest-but-blank ``None`` (V4 bundles fees into the
    # withdrawal Transfer; they are unrecoverable from the receipt). The runner
    # reads them on-chain before the burn submits and threads the raw-int pair.
    _stamp_v4_lp_close_fees(result, intent_type, v4_lp_close_fees)
    # VIB-4483 (V4) / VIB-5121 (fungible) — stamp the runner-measured native-ETH
    # OPEN leg amount onto ``lp_open_data``. A native leg deposits via msg.value
    # (no ERC-20 Transfer), so the receipt parser left it None. The runner
    # measures it after the tx lands per connector (V4: post-mint position-state
    # read + CL math; Fluid + any fungible native-leg LP: a block-pinned wallet
    # native-balance bracket, gas-separated) and threads the merged result here;
    # this stamp fills only the unmeasured native leg (never clobbers a measured
    # ERC-20 leg).
    _stamp_lp_open_native_amounts(result, intent_type, lp_open_native_amounts)
    # VIB-5117 — V4 close: stamp the PRE-burn-measured native-ETH PRINCIPAL onto
    # ``lp_close_data``. A native-ETH V4 LP_CLOSE returns its ETH leg as raw ETH
    # (no ERC-20 Transfer), so the receipt parser left ``amount{0,1}_collected``
    # None. The runner reads pre-burn position state + CL math (V4-specific
    # measurement — kept exactly per VIB-5117). Without it the close records 0
    # proceeds and understates realized PnL by the full native principal.
    _stamp_v4_lp_close_native_principal(result, intent_type, v4_lp_close_native_principal)
    # VIB-5121 — Fluid/fungible close twin: stamp the runner-measured native-ETH
    # RETURNED leg (measured from a balance bracket, gated to the non-V4 native
    # sentinel) onto ``lp_close_data``. Distinct from the V4 stamp above by
    # currency gate — per-connector measurement (rule-of-three, VIB-5135).
    _stamp_lp_close_native_amounts(result, intent_type, lp_close_native_amounts)
    # VIB-5132 — extract tokens/amounts NOW, after the LP-close native stamps, so
    # ``_extract_from_lp_close`` reads the POST-stamp ``LPCloseData.amount{0,1}_
    # collected`` (the native ETH leg is filled by the stamps above). For every
    # non-LP-close intent type this is behaviourally identical to extracting at
    # the top of the function (the stamps are no-ops outside LP_CLOSE).
    (
        token_in,
        token_out,
        amount_in,
        amount_out,
        effective_price,
        slippage_bps,
    ) = _extract_tokens_and_amounts(intent, result, chain=chain)
    extracted_data_json = _build_extracted_data_json(result)

    # ─── VIB-3480 columns finally populated (Accounting-AttemptNo17 §3 D3) ──
    # Until this PR, pre_state_json / post_state_json / price_inputs_json
    # were declared on the dataclass + DDL but no writer ever filled them.
    # That's the canonical leaf-fix anti-pattern §0 names. The runner is the
    # single capture point — see docs/internal/connector-pre-post-audit.md.

    def _safe_json(d: dict[str, Any] | None) -> str:
        # ``None`` = no state was captured (callers default to passing
        # ``None`` when pre/post capture didn't run). ``{}`` = the runner
        # captured an explicitly empty snapshot — e.g. "wallet had nothing
        # of interest after the close" — and we want that recorded as the
        # JSON object ``{}``, not collapsed to ``""`` which is
        # indistinguishable from the unmeasured case downstream.
        if d is None:
            return ""
        try:
            # Decimals + datetimes need a default — match serialize_extracted_data.
            return json.dumps(d, default=str)
        except (TypeError, ValueError):
            return ""

    price_inputs_json = ""
    if price_oracle:
        # Shape per AttemptNo17 §1.2 G12: {symbol: {price_usd, oracle_source,
        # fetched_at, confidence}}. The runner may pass the new shape directly
        # OR the legacy flat {symbol: price} shape. Normalize to the new shape.
        normalised: dict[str, Any] = {}
        for sym, val in price_oracle.items():
            if isinstance(val, dict) and "price_usd" in val:
                normalised[sym] = val
            else:
                normalised[sym] = {
                    "price_usd": str(val) if val is not None else None,
                    "oracle_source": "unknown",
                    "fetched_at": "",
                    "confidence": "ESTIMATED",
                }
        price_inputs_json = _safe_json(normalised)

    pre_state_json = _safe_json(pre_state)
    post_state_json = _safe_json(post_state)

    entry = LedgerEntry(
        cycle_id=cycle_id,
        deployment_id=deployment_id,
        intent_type=intent_type,
        token_in=token_in,
        amount_in=amount_in,
        token_out=token_out,
        amount_out=amount_out,
        effective_price=effective_price,
        slippage_bps=slippage_bps,
        gas_used=gas_used,
        gas_usd=gas_usd,
        tx_hash=tx_hash,
        chain=chain,
        protocol=protocol,
        success=success,
        error=final_error,
        extracted_data_json=extracted_data_json,
        price_inputs_json=price_inputs_json,
        pre_state_json=pre_state_json,
        post_state_json=post_state_json,
    )

    # W1-5 decimal-unit soft-fail guard (VIB-4780).  Runs after the entry is
    # fully constructed so amount_in/amount_out are resolved.  Soft-fail only:
    # logs a WARNING, never raises, never mutates the entry.
    #
    # Token decimals are resolved LAZILY — only when at least one of the
    # guarded fields is integer-shaped (the raw-wei tell).  This keeps the
    # happy path (legitimate Decimal strings like ``"0.001130"``) free of
    # any resolver init cost or noise.
    from almanak.framework.accounting.decimal_guards import (
        _check_decimal_unit_soft_fail,
        _is_integer_shaped,
    )

    chain_lc = (chain or "").lower()
    token_symbols_map: dict[str, str] = {}
    token_decimals_map: dict[str, int] = {}

    needs_decimals_lookup = any(_is_integer_shaped(v) for v in (entry.amount_in, entry.amount_out))

    if needs_decimals_lookup:
        try:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            resolver = get_token_resolver()
            for side, sym in (("in", token_in), ("out", token_out)):
                if not sym:
                    continue
                token_symbols_map[side] = sym
                try:
                    info = resolver.resolve(sym, chain=chain_lc)
                except Exception:
                    info = None
                if info is not None and info.decimals is not None:
                    token_decimals_map[side] = info.decimals
        except Exception:  # pragma: no cover - resolver path is best-effort
            # Resolver unavailable — fall through; the magnitude rule will
            # still run inside the guard without decimals plumbed.
            pass

    _check_decimal_unit_soft_fail(
        {"amount_in": entry.amount_in, "amount_out": entry.amount_out},
        event_id=entry.id,
        event_type=entry.intent_type,
        chain=chain_lc or None,
        token_decimals_map=token_decimals_map or None,
        token_symbols_map=token_symbols_map or None,
    )

    return entry


def serialize_extracted_data(extracted_data: dict[str, Any]) -> str:
    """Serialize extracted_data dict with type tags for round-trip fidelity.

    Each value that has a ``to_dict()`` method (the typed dataclasses like
    SwapAmounts, LPOpenData, PerpData, etc.) is serialized with a ``_type``
    tag so ``deserialize_extracted_data()`` can reconstruct the original type.
    """
    serializable: dict[str, Any] = {}
    for key, val in extracted_data.items():
        if hasattr(val, "to_dict"):
            d = val.to_dict()
            d["_type"] = type(val).__name__
            serializable[key] = d
        elif isinstance(val, Decimal):
            serializable[key] = {"_type": "Decimal", "value": str(val)}
        elif isinstance(val, datetime):
            serializable[key] = {"_type": "datetime", "value": val.isoformat()}
        elif isinstance(val, Enum):
            serializable[key] = {"_type": "Enum", "name": type(val).__name__, "value": val.value}
        else:
            serializable[key] = val
    try:
        return json.dumps(serializable, default=str)
    except (TypeError, ValueError):
        return ""


def deserialize_extracted_data(json_str: str) -> dict[str, Any]:
    """Deserialize extracted_data JSON back into typed objects where possible.

    Values tagged with ``_type`` are reconstructed into their original types.
    Unknown types are returned as plain dicts.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}

    # Lazy import to avoid circular deps
    from almanak.framework.execution.extracted_data import (
        BorrowData,
        LPCloseData,
        LPOpenData,
        PerpData,
        StakeData,
        SupplyData,
        SwapAmounts,
    )

    type_map: dict[str, type] = {
        "SwapAmounts": SwapAmounts,
        "LPOpenData": LPOpenData,
        "LPCloseData": LPCloseData,
        "BorrowData": BorrowData,
        "SupplyData": SupplyData,
        "PerpData": PerpData,
        "StakeData": StakeData,
    }

    result: dict[str, Any] = {}
    for key, val in data.items():
        if not isinstance(val, dict) or "_type" not in val:
            result[key] = val
            continue

        type_name = val.pop("_type")
        if type_name == "Decimal":
            result[key] = Decimal(val["value"])
        elif type_name == "datetime":
            result[key] = datetime.fromisoformat(val["value"])
        elif type_name == "Enum":
            result[key] = val  # Return as dict; caller knows the enum type
        elif type_name in type_map:
            try:
                cls = type_map[type_name]
                # Convert string values back to appropriate types
                result[key] = _reconstruct_dataclass(cls, val)
            except (TypeError, ValueError):
                val["_type"] = type_name
                result[key] = val
        else:
            val["_type"] = type_name
            result[key] = val

    return result


def _reconstruct_dataclass(cls: type, data: dict[str, Any]) -> Any:
    """Reconstruct a frozen dataclass from a dict of string values."""
    import dataclasses
    import inspect

    fields = {f.name: f for f in dataclasses.fields(cls)}
    sig = inspect.signature(cls)
    kwargs: dict[str, Any] = {}

    for name, _param in sig.parameters.items():
        if name not in data:
            continue
        val = data[name]
        if val is None:
            kwargs[name] = None
            continue

        f = fields.get(name)
        if f is None:
            continue

        # Infer type from field annotation string
        ann = str(f.type)
        if "Decimal" in ann:
            kwargs[name] = Decimal(val)
        elif "int" in ann and "str" not in ann:
            try:
                kwargs[name] = int(val)
            except (ValueError, TypeError):
                kwargs[name] = val
        elif "float" in ann:
            try:
                kwargs[name] = float(val)
            except (ValueError, TypeError):
                kwargs[name] = val
        elif "bool" in ann:
            if isinstance(val, bool):
                kwargs[name] = val
            elif isinstance(val, str):
                kwargs[name] = val.lower() in ("true", "1", "yes")
            else:
                kwargs[name] = bool(val)
        elif "SlippageSource" in ann:
            # VIB-4087 — string round-trip back to the StrEnum so consumers
            # that compare ``swap_amounts.slippage_source == SlippageSource.RECEIPT_DECODED``
            # see the typed instance, not a bare string.
            from almanak.framework.execution.extracted_data import SlippageSource

            try:
                kwargs[name] = SlippageSource(val) if isinstance(val, str) else val
            except ValueError:
                # Unknown enum value (older serialised payload, hand-crafted
                # JSON, etc.) — degrade to NONE rather than raise. The
                # contract is "slippage_source is always a known value at
                # read time"; rejecting would break replay of older DBs.
                kwargs[name] = SlippageSource.NONE
        else:
            kwargs[name] = val

    return cls(**kwargs)
