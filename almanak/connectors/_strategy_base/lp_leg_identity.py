"""Shared LP leg-identity primitives for receipt parsers (VIB-6053).

A ledger / position row is a set of ``(token, amount)`` pairs, and it is correct
only when BOTH halves come from the same layer's leg ordering. Two legitimate
orderings exist and ``amount0`` names both:

* **intent layer** — the first symbol of the user's pool label (``"WETH/USDC/3000"``),
  the strategy-facing contract;
* **receipt layer** — the venue's own canonical leg index (V3-family pool
  ``token0()``/``token1()``, V4 PoolKey, Curve ``coins(i)``, TraderJoe
  ``tokenX``/``tokenY`` — which is explicitly NOT address-sorted).

Pairing an amount from one layer with a symbol from the other transposes the row
and then scales it by the WRONG token's decimals (VIB-5851 / VIB-5983 / VIB-5988 /
VIB-6053 — a $100 position recorded as $26.5bn). Three consumer-side compensators
shipped and the class survived all three, because a compensator re-sorts one half
without knowing where the other half came from.

This module is the producer-side fix: helpers that let a receipt parser emit leg
**identity bound to its own amounts**, so no consumer ever has to infer it.

Two rules the helpers here exist to enforce:

* **Token-keyed, never magnitude-sorted.** Ordering token amounts by raw integer
  magnitude is decimals-blind (2.9e16 WETH wei "outranks" 5.5e7 USDC) and was the
  direct cause of VIB-6045. :func:`transfers_by_token` accumulates by token
  address; nothing here ever sorts by value.
* **Never reorder amounts to compensate.** Identity travels WITH the amount, in
  the venue's own canonical order. A parser that cannot determine identity emits
  ``None`` — it must not guess and must not re-sort.

This module must not import any concrete connector.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.base import HexDecoder

logger = logging.getLogger(__name__)

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Mint / burn counterparty — a Transfer to or from the zero address is an LP-token
# mint or burn, never a pool-coin movement.
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _normalize_address(value: Any) -> str:
    """Lowercase hex address from a str / bytes / HexBytes log field (``""`` if absent)."""
    if value is None:
        return ""
    if isinstance(value, bytes | bytearray):
        return "0x" + bytes(value).hex()
    text = str(value).strip().lower()
    return text if text.startswith("0x") else ""


def _log_field(log_entry: Any, key: str) -> Any:
    """Read ``key`` from a dict-like or attribute-like log entry."""
    if hasattr(log_entry, "get"):
        return log_entry.get(key)
    return getattr(log_entry, key, None)


def _first_topic(topics: Any) -> str:
    if not topics:
        return ""
    first = topics[0]
    if isinstance(first, bytes | bytearray):
        return "0x" + bytes(first).hex()
    return str(first).strip().lower()


def log_emitter_address(log_entry: Any) -> str:
    """Lowercase address of the contract that emitted ``log_entry`` (``""`` if absent).

    The V3-family pool emits Burn AND Collect, so its own log's emitter is the pool
    — the counterparty a leg-identity transfer scan needs. Shared because three
    parsers captured it with the same four-line str/bytes dance, and one of them
    only captured it on Burn: a V3 close is a SPLIT-TX sequence
    (decreaseLiquidity -> collect -> burn), so on the collect-only receipt the
    Burn-derived address is empty and the scan silently found no counterparty
    (VIB-6053 — the close half of the fix regressed on exactly that shape).
    """
    return _normalize_address(_log_field(log_entry, "address"))


def transfers_by_token(
    logs: Any,
    *,
    to_address: str | None = None,
    from_address: str | None = None,
    skip_zero_address: bool = True,
) -> dict[str, int]:
    """Accumulate ERC-20 ``Transfer`` values per token address.

    The token-keyed replacement for the per-connector transfer scans that
    reimplemented this five different ways, one of which (Aerodrome's Path B)
    ordered legs by raw integer magnitude across tokens with different decimals —
    a decimals-blind sort that made leg assignment arbitrary (VIB-6045).

    Filters are ANDed and matched case-insensitively; pass ``to_address`` for a
    deposit scan (wallet → pool) and ``from_address`` for a withdrawal scan
    (pool → wallet). With neither, every Transfer in the receipt is accumulated.

    Values ACCUMULATE per token (``+=``), so a leg split across several transfers
    — a router hop, a fee-on-transfer top-up — is summed rather than last-write-wins.
    Callers that must avoid double-counting a router pass-through should filter by
    counterparty rather than rely on overwrite semantics.

    ``skip_zero_address`` drops mint/burn transfers (LP-token supply changes),
    which are never pool-coin movements.

    Returns ``{lowercase_token_address: summed_raw_value}``. Never raises: a
    malformed log is skipped, not fatal — this runs on the accounting write path.
    """
    want_to = (to_address or "").strip().lower()
    want_from = (from_address or "").strip().lower()
    totals: dict[str, int] = {}

    for entry in logs or []:
        try:
            topics = _log_field(entry, "topics") or []
            if _first_topic(topics) != TRANSFER_TOPIC or len(topics) < 3:
                continue
            src = HexDecoder.topic_to_address(topics[1])
            dst = HexDecoder.topic_to_address(topics[2])
            if skip_zero_address and (src == _ZERO_ADDRESS or dst == _ZERO_ADDRESS):
                continue
            if want_to and dst != want_to:
                continue
            if want_from and src != want_from:
                continue
            token = _normalize_address(_log_field(entry, "address"))
            if not token:
                continue
            data = _log_field(entry, "data")
            value = HexDecoder.decode_uint256(data, 0)
        except Exception:  # noqa: BLE001 — accounting write path: skip, never raise
            logger.debug("lp leg identity: skipping undecodable Transfer log", exc_info=True)
            continue
        totals[token] = totals.get(token, 0) + int(value)

    return totals


def currencies_for_amounts(
    token_totals: dict[str, int],
    amount0: int | None,
    amount1: int | None,
) -> tuple[str | None, str | None]:
    """Bind ``(currency0, currency1)`` to a venue's own ``(amount0, amount1)``.

    The venue event (a V3 ``Mint`` / ``Burn`` / ``Collect``) reports amounts in the
    pool's canonical slot order but names no tokens; the receipt's Transfer logs
    name tokens but carry no slot. This joins them **by value**, which is exact for
    the V3 family: the mint callback transfers precisely ``amount0Owed`` /
    ``amount1Owed``, and a collect transfers precisely the collected amounts.

    Rules, in order:

    * a slot whose amount is ``None`` (unmeasured) or ``0`` gets ``None`` — there
      is no transfer to bind it to, and a measured ``0`` scales to ``0`` under any
      decimals, so leaving it unidentified is lossless;
    * a slot whose amount matches exactly one token's total binds to that token;
    * if both slots carry the same non-zero amount (degenerate tie), the two
      candidate tokens are assigned in ascending address order — the V3-family
      pool convention — so the result stays deterministic;
    * anything else (no match, or an ambiguous multi-match) yields ``None`` for
      that slot. **Fail closed: never guess.**

    Returned addresses are lowercase. ``None`` means "identity not determinable
    from this receipt" and MUST NOT be substituted with a label-order guess.
    """
    by_value: dict[int, list[str]] = {}
    for token, total in token_totals.items():
        by_value.setdefault(int(total), []).append(token)

    def _bind(amount: int | None, taken: str | None) -> str | None:
        if amount is None or int(amount) == 0:
            return None
        candidates = [t for t in by_value.get(int(amount), []) if t != taken]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            # Degenerate tie (both legs moved the same raw amount). Ascending
            # address order is the V3-family pool convention, so the assignment is
            # deterministic rather than dict-iteration-order dependent.
            return sorted(candidates, key=lambda a: int(a, 16))[0]
        return None

    currency0 = _bind(amount0, taken=None)
    currency1 = _bind(amount1, taken=currency0)
    return currency0, currency1


def slot_moved_money(amount: Any) -> bool:
    """True when a slot carries a measured, non-zero amount.

    A slot that moved nothing needs no identity: ``0`` scales to ``0`` under any
    decimals, so mis-attributing it is arithmetically harmless. A slot that DID
    move money and has no identity is the defect class this module exists to end.

    Unparseable input counts as "did not move" rather than raising — this runs on
    the accounting write path, where an exception would abort the write. Note this
    is *permissive*, not fail-closed: a malformed amount makes
    :func:`identity_is_complete` treat the slot as needing no identity. That is
    safe only because the same ``int(raw)`` conversion in
    ``lp_handler._to_human_from_raw`` yields ``None`` for the identical input, so a
    slot this predicate waves through is still booked unmeasured rather than
    mis-scaled.

    ``OverflowError`` is caught alongside the parse errors: ``int(float("inf"))``
    raises it rather than ``ValueError``, and it would otherwise escape a function
    whose contract is that it never raises.
    """
    if amount is None:
        return False
    try:
        return int(amount) != 0
    except (TypeError, ValueError, OverflowError):
        return False


def identity_is_complete(
    currency0: str | None,
    currency1: str | None,
    amount0: Any,
    amount1: Any,
) -> bool:
    """True when every slot that MOVED money carries its own identity.

    :func:`currencies_for_amounts` returns ``None`` for a slot in three different
    situations — the amount was unmeasured, the amount was ``0`` (identity moot),
    or the value-join was ambiguous (identity undeterminable). Consumers that test
    ``currency0 and currency1`` cannot tell those apart, so they read a routine
    single-sided close (one real leg + one zero leg) as a *failed* observation and
    a genuinely failed observation as a *successful* one. That conflation is
    VIB-6471 (partial stamp suppresses realignment) and VIB-6476 (present-but-
    unresolvable stamp suppresses realignment) — one root cause, two symptoms:

        **an observation's presence is being treated as its success.**

    This predicate restores the distinction WITHOUT changing how ``currency0`` /
    ``currency1`` are stored or serialized: the amounts already sit beside the
    currencies at every consumer, so the third state is *derived* rather than
    persisted. That matters — the stored fields are also read by the runner's
    native-leg stamp (``strategy_runner._receipt_token_legs`` and the V4 native
    top-off), where changing their meaning would re-break VIB-5117 / VIB-5121.

    Vacuously ``True`` when neither slot moved money; callers that need a usable
    pair must additionally require at least one bound currency.
    """
    for currency, amount in ((currency0, amount0), (currency1, amount1)):
        if slot_moved_money(amount) and not currency:
            return False
    return True
