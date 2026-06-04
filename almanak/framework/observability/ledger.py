"""Transaction Ledger -- structured trade records.

Every executed intent produces a LedgerEntry that captures the trade in a
structured, queryable format.  This replaces grepping through timeline event
``details`` dicts for post-mortem trade analysis.

The ledger is populated by ``StrategyRunner`` after result enrichment and
stored alongside timeline events in the gateway state store.

Phase 5k -- helper extraction layout
------------------------------------
``build_ledger_entry`` is composed from small phase helpers that each
return a piece of the final ``LedgerEntry``.  The helpers run in a fixed
order; ordering is NOT load-bearing (unlike the position-events pipeline)
because none of the helpers depend on output of earlier phases:

    alpha  _extract_intent_type          : enum-or-string dispatch
    beta   _extract_tokens_and_amounts   : dispatch between three sub-helpers:
             _extract_from_swap_amounts       (SwapAmounts + intent fallback
                                               for empty token sides)
             _extract_from_lp_open           (LP_OPEN: LPOpenData amounts +
                                               intent token0/token1 lookup)
             _extract_from_intent_fallback    (intent-attr precedence chain
                                               from_token > borrow_token >
                                               supply_token > token;
                                               amount > borrow_amount >
                                               supply_amount > amount_usd).
    gamma  _extract_tx_and_gas           : first tx_hash + total gas + gas USD
    delta  _coalesce_error               : failure + empty-error -> result.error
    epsilon _build_extracted_data_json   : serialize + multi-tx augmentation

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
from decimal import Decimal
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
    amount_in = str(amt_in) if amt_in is not None and amt_in_resolved else ""
    amount_out = str(amt_out) if amt_out is not None and amt_out_resolved else ""
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


def _extract_from_intent_fallback(intent: Any) -> _TokensAndAmounts:
    """Phase beta-fallback -- no swap_amounts; walk the intent-attr chain.

    Token precedence:
        ``from_token > borrow_token > supply_token > token``
    Amount precedence:
        ``amount > borrow_amount > supply_amount > amount_usd``

    Supports swap-style (from_token/to_token), lending
    (borrow_token/supply_token) and generic (token/amount) intents.
    """
    token_in = (
        getattr(intent, "from_token", "")
        or getattr(intent, "borrow_token", "")
        or getattr(intent, "supply_token", "")
        or getattr(intent, "token", "")
        or ""
    )
    token_out = getattr(intent, "to_token", "") or ""
    amt = (
        getattr(intent, "amount", None)
        or getattr(intent, "borrow_amount", None)
        or getattr(intent, "supply_amount", None)
        or getattr(intent, "amount_usd", None)
    )
    amount_in = str(amt) if amt is not None else ""
    return (token_in, token_out, amount_in, "", "", None)


def _extract_from_lp_open(intent: Any, result: Any) -> _TokensAndAmounts:
    """Phase beta-lp-open -- LP_OPEN has no swap_amounts; pull amounts from
    ``LPOpenData`` in ``result.extracted_data`` and tokens from the intent.

    Field mapping:
    - ``token_in``  : ``intent.token0`` -> ``intent.from_token`` -> ``""``
    - ``token_out`` : ``intent.token1`` -> ``intent.to_token``  -> ``""``
    - ``amount_in`` : ``LPOpenData.amount0`` (raw int) is the on-chain actual
                      deposit for token0; falls back to ``intent.amount0``
                      (Decimal from the intent, the user-requested amount).
    - ``amount_out``: same logic for ``amount1``.

    ``LPOpenData.amount0`` / ``amount1`` are raw integer values (smallest
    unit).  We store them as strings directly so that accounting consumers
    see the on-chain amount.  When only the intent amounts are available the
    human-readable Decimal string is more useful.
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
        # Per-side fallback: if one side is missing from LPOpenData, fall back
        # to the corresponding intent amount rather than leaving it empty.
        raw0 = getattr(lp_open_data, "amount0", None)
        raw1 = getattr(lp_open_data, "amount1", None)
        if raw0 is None:
            raw0 = getattr(intent, "amount0", None)
        if raw1 is None:
            raw1 = getattr(intent, "amount1", None)
        amount_in = str(raw0) if raw0 is not None else ""
        amount_out = str(raw1) if raw1 is not None else ""
    else:
        intent_amt0 = getattr(intent, "amount0", None)
        intent_amt1 = getattr(intent, "amount1", None)
        amount_in = str(intent_amt0) if intent_amt0 is not None else ""
        amount_out = str(intent_amt1) if intent_amt1 is not None else ""

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
        return _extract_from_intent_fallback(intent)

    try:
        raw_int = int(raw)
    except (TypeError, ValueError):
        # Receipt produced a non-int value (shouldn't happen for these
        # extractors, but fail-open to the intent-attr path rather than
        # pretending we have a value). Empty != zero, so don't substitute.
        return _extract_from_intent_fallback(intent)

    # CodeRabbit 2026-05-04: reuse the existing intent-attr precedence chain
    # (``from_token`` > ``borrow_token`` > ``supply_token`` > ``token``) so
    # connectors that name the lending asset under ``borrow_token`` /
    # ``supply_token`` (rather than the generic ``token``) still get
    # decimals resolved. ``_extract_from_intent_fallback`` returns the full
    # 6-tuple; we only need ``token_in`` here.
    token_in = _extract_from_intent_fallback(intent)[0]

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

    return (token_in, "", str(scaled), "", "", None)


def _extract_tokens_and_amounts(
    intent: Any,
    result: Any,
    chain: str = "",
) -> _TokensAndAmounts:
    """Phase beta -- dispatch between SwapAmounts, LP_OPEN, PERP_OPEN,
    lending (REPAY/WITHDRAW), and intent-attr fallback.

    A truthy ``result.swap_amounts`` drives every field (used by SWAP,
    LP_CLOSE, and anything whose receipt parser emits SwapAmounts). LP_OPEN
    intents carry amounts in ``LPOpenData`` and have no ``from_token`` /
    ``to_token``, so they get a dedicated extraction path. PERP_OPEN collateral
    lives at ``intent.collateral_token`` / ``intent.collateral_amount``, not the
    standard from_token/to_token chain. REPAY/WITHDRAW route through the
    lending helper so the receipt-resolved amount (post-uint256.max
    decoding by Aave) lands on ``transaction_ledger.amount_in`` (VIB-3939).
    Everything else walks the intent-attr precedence chain.
    """
    swap_amounts = getattr(result, "swap_amounts", None) if result else None
    if swap_amounts:
        return _extract_from_swap_amounts(swap_amounts, intent)
    intent_type = _extract_intent_type(intent)
    if intent_type == "LP_OPEN":
        return _extract_from_lp_open(intent, result)
    if intent_type == "PERP_OPEN":
        token_in = getattr(intent, "collateral_token", "") or ""
        collateral_amount = getattr(intent, "collateral_amount", None)
        amount_in = str(collateral_amount) if collateral_amount is not None else ""
        return (token_in, "", amount_in, "", "", None)
    if intent_type in ("REPAY", "WITHDRAW", "DELEVERAGE"):
        # DELEVERAGE is structurally a repay (closes borrow exposure) — the
        # writer routes it through the same lending receipt path, so the
        # ledger row must too. Without DELEVERAGE here, ``Intent.deleverage(
        # repay_full=True)``'s default ``Decimal("0")`` falls through to the
        # intent-attr fallback and lands ``amount_in=""`` despite the receipt
        # carrying the resolved repaid amount.
        return _extract_from_lending(intent, result, intent_type, chain)
    return _extract_from_intent_fallback(intent)


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


# Fungible-LP (ERC20 LP-token) venues where ``LPCloseIntent.position_id`` is
# NOT an NFT token id. On Curve the close intent's ``position_id`` is overloaded
# as the LP-token *amount* to burn (a human-decimal string), so stamping it as a
# per-position discriminator (VIB-4275) would write a bogus amount-shaped
# ``position_id`` onto the fungible-LP close event — which has no co-leg to
# disambiguate (one balance per pool). VIB-4968.
_FUNGIBLE_LP_NO_DISCRIMINATOR_PROTOCOLS = frozenset({"curve"})


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
    (:data:`_FUNGIBLE_LP_NO_DISCRIMINATOR_PROTOCOLS`) where the close intent's
    ``position_id`` is overloaded as a burn *amount*, not an NFT id (VIB-4968).
    """
    if intent_type not in ("LP_CLOSE", "LP_COLLECT_FEES"):
        return
    if (protocol or "").lower() in _FUNGIBLE_LP_NO_DISCRIMINATOR_PROTOCOLS:
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
    (
        token_in,
        token_out,
        amount_in,
        amount_out,
        effective_price,
        slippage_bps,
    ) = _extract_tokens_and_amounts(intent, result, chain=chain)
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
