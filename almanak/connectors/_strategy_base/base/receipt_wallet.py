"""Effective trading-wallet resolution for receipt parsers (VIB-6043).

Why this exists
===============

Receipt parsers have to know *which address is the strategy* before they can
tell which ERC-20 ``Transfer`` legs of a transaction are the strategy's money
legs (token-in / token-out discovery, decimals resolution, amount extraction).
Historically every parser answered that question with ``receipt["from"]``.

That is correct for **plain EOA execution only**. Under Safe + Zodiac Roles
execution — the mode the hosted platform runs exclusively — the transaction is
signed by the **agent EOA** (it calls ``execTransactionWithRole`` on the Roles
modifier) while every token ``Transfer`` moves on the **Safe**. Matching logs
against ``receipt["from"]`` then matches nothing:

* token addresses resolve to ``("", "")``,
* decimals resolve to ``None``,
* amount extraction returns ``None``.

Downstream that lands in one of two wrong places: a fail-closed parser raises
``CriticalAccountingError`` (money moved on-chain, **zero** ledger rows, the
circuit breaker eventually emergency-stops the strategy — the Curve blackout in
VIB-6043), or a fail-open parser writes a success ledger row with **empty**
measured amounts (an Empty != Zero violation — the PancakeSwap V3 flavour).

Resolution order
================

1. **Framework stamp** — ``ResultEnricher`` stamps the effective execution
   address (``ExecutionContext.wallet_address``, which is the Safe address in
   Safe mode and the EOA otherwise) onto the receipt dict under
   :data:`TRADING_WALLET_KEY` before handing it to a parser. This is the
   authoritative answer: it comes from the execution configuration that
   actually signed the transaction, not from a heuristic.
2. **``receipt["from"]``** — the legacy behaviour, correct for plain EOA
   execution and preserved verbatim for it.

Empty != Zero: when nothing resolves this returns ``""``. It never fabricates
an address and never guesses from ``Transfer`` participants.

Why there is no "derive the Safe from the receipt's own events" step
====================================================================

A Safe emits ``ExecutionFromModuleSuccess(address)`` (module / Zodiac
execution) or ``ExecutionSuccess(bytes32,uint256)`` (``execTransaction``) from
its own address, so it is tempting to recover the Safe from an unstamped
receipt by looking for those topics. **That would be authentication by event
signature, and event signatures are not authentication**: any contract can emit
a log whose topic0 collides, and a receipt containing exactly one such emitter
would silently replace the sender with that contract — turning "the wallet is
wrong" into "the wallet is wrong AND attacker-chosen", with the traded legs
attributed to it. Two independent reviewers flagged this on PR #3439, and they
were right.

The corroborations available inside a receipt (require the emitter to also move
tokens; require it to match the tx target) narrow the surface but do not close
it — a router or pool is both a Transfer counterparty and, for a
``execTransaction``-shaped call, the tx target.

So the resolver takes only what it can trust. :func:`safe_wallet_from_receipt`
remains available for a caller that already knows, out of band, that it holds a
Safe-executed receipt (diagnostics, evidence tooling), and is documented there
as a heuristic — but it is deliberately NOT part of the default resolution
order. A caller with no execution context keeps the pre-existing behaviour
rather than gaining a spoofable one; the correct fix for such a caller is to
thread its execution address, exactly as the enricher and the teardown lane now
do.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

#: Reserved receipt key carrying the effective trading wallet.
#:
#: Stamped by ``almanak.framework.execution.result_enricher.ResultEnricher``
#: (and any other framework caller that knows the execution address) onto the
#: receipt dict passed to a connector receipt parser. Namespaced with an
#: ``almanak_`` prefix so it can never collide with an EVM receipt field.
TRADING_WALLET_KEY: Final[str] = "almanak_trading_wallet"

#: ``keccak256("ExecutionFromModuleSuccess(address)")`` — emitted by a Safe when
#: an enabled module (Zodiac Roles) executes through it.
_EXECUTION_FROM_MODULE_SUCCESS_TOPIC: Final[str] = "0x6895c13664aa4f67288b25d7a21d7aaa34916e355fb9b6fae0a139a9085becb8"

#: ``keccak256("ExecutionSuccess(bytes32,uint256)")`` — emitted by a Safe on a
#: successful ``execTransaction`` (safe_direct mode).
_EXECUTION_SUCCESS_TOPIC: Final[str] = "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e"

_SAFE_EXECUTION_TOPICS: Final[frozenset[str]] = frozenset(
    {_EXECUTION_FROM_MODULE_SUCCESS_TOPIC, _EXECUTION_SUCCESS_TOPIC}
)

#: ``keccak256("Transfer(address,address,uint256)")`` — used to corroborate a
#: Safe candidate by requiring it to actually move tokens in the same receipt.
_ERC20_TRANSFER_TOPIC: Final[str] = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


#: The only characters a normalised address payload may contain. Used instead of
#: ``int(text, 16)`` because **Python accepts ``_`` as a digit separator**:
#: ``int("0x" + "a" * 38 + "_b", 16)`` succeeds, so an underscore-bearing 42-char
#: string would pass a length + ``int()`` check and be treated as a usable address.
#: It would then be stamped as the authoritative trading wallet, and every parser
#: would resolve money legs against an address that cannot match anything —
#: a silent no-signal, which is the failure class this module exists to prevent.
_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789abcdef")


def normalize_wallet_address(value: Any) -> str:
    """Lower-cased ``0x``-prefixed 20-byte address, or ``""`` when not one.

    Accepts the shapes receipts arrive in: ``str``, ``bytes`` / ``HexBytes``,
    or anything with a ``hex()`` method.

    **Normative, not yet universal.** Anything deciding whether an address may
    be attributed to, keyed on, or stamped onto a receipt MUST call this rather
    than reimplementing the check — that is the rule, and the
    attribution/stamp pair now satisfies it. It is *not* a claim that every
    address helper in the tree already routes here: `gmx_v2/runner_hooks.py`
    still carries its own `len == 42` + `int(value, 16)` copy (same hole this
    function closes, on a different path), and `enso/receipt_parser.py`,
    `lp_leg_identity.py` and `base/receipt_parser.py` define weaker
    module-local variants for their own purposes. Migrating those is follow-up
    work on VIB-6049, not something this docstring should imply is done.
    Two independent copies would drift — and the specific way they drift is
    dangerous: if attribution accepted a value the stamp rejected (or vice
    versa), a signal could be attributed to the leader while its money legs were
    resolved against the *signer*, producing an amount-correct, wrongly-attributed
    result. Sharing one function makes them equal by construction rather than by
    coincidence (VIB-6049).
    """
    if value is None:
        return ""
    if isinstance(value, bytes | bytearray):
        text = "0x" + bytes(value).hex()
    elif isinstance(value, str):
        text = value
    else:
        hex_method = getattr(value, "hex", None)
        if not callable(hex_method):
            return ""
        try:
            text = hex_method()
        except Exception:  # noqa: BLE001 — malformed receipt field, not an address
            return ""
        if not isinstance(text, str):
            # e.g. a HexBytes-like whose hex() returns bytes — malformed here.
            return ""
        if not text.startswith("0x"):
            text = "0x" + text
    text = text.strip().lower()
    if not text.startswith("0x"):
        return ""
    if len(text) != 42:
        return ""
    # NOT ``int(text, 16)`` — see ``_HEX_DIGITS``: that accepts ``_`` separators.
    if not _HEX_DIGITS.issuperset(text[2:]):
        return ""
    return text


#: Alias for this module's own four internal call sites, which referenced the
#: private name before it became the shared acceptance test. No other module
#: imports it — the identically-named helpers in ``enso/``, ``gmx_v2/``,
#: ``lp_leg_identity.py`` and ``receipt_parser.py`` are independent module-local
#: definitions, not this one.
_normalize_address = normalize_wallet_address


def _normalize_topic(value: Any) -> str:
    """Lower-cased ``0x``-prefixed 32-byte topic, or ``""``."""
    if value is None:
        return ""
    if isinstance(value, bytes | bytearray):
        text = "0x" + bytes(value).hex()
    elif isinstance(value, str):
        text = value
    else:
        hex_method = getattr(value, "hex", None)
        if not callable(hex_method):
            return ""
        try:
            text = hex_method()
        except Exception:  # noqa: BLE001 — malformed receipt field, not a topic
            return ""
        if not isinstance(text, str):
            return ""
        if not text.startswith("0x"):
            text = "0x" + text
    return text.strip().lower()


def stamp_trading_wallet(receipt: Mapping[str, Any], wallet: str | None) -> dict[str, Any]:
    """Return a copy of ``receipt`` carrying ``wallet`` as the trading wallet.

    Never mutates the input (receipts are shared with the ledger / persistence
    path). The stamp is **authoritative**: a missing or malformed ``wallet``
    REMOVES any pre-existing stamp rather than leaving a stale one in place —
    otherwise a receipt that inherited a stamp from elsewhere (the enricher's
    multi-tx merge copies the first receipt's scalar fields) would keep
    outranking ``receipt["from"]`` with the wrong address. Unstamped then means
    exactly the pre-VIB-6043 behaviour.
    """
    stamped = dict(receipt)
    normalized = _normalize_address(wallet)
    if normalized:
        stamped[TRADING_WALLET_KEY] = normalized
    else:
        stamped.pop(TRADING_WALLET_KEY, None)
    return stamped


def _erc20_transfer_participants(logs: list[Any]) -> set[str]:
    """Every address that sent or received an ERC-20 ``Transfer`` in ``logs``."""
    participants: set[str] = set()
    for log in logs:
        if not isinstance(log, Mapping):
            continue
        topics = log.get("topics")
        if not isinstance(topics, list | tuple) or len(topics) < 3:
            continue
        if _normalize_topic(topics[0]) != _ERC20_TRANSFER_TOPIC:
            continue
        for topic in (topics[1], topics[2]):
            # Indexed address topics are left-padded to 32 bytes.
            padded = _normalize_topic(topic)
            if len(padded) == 66:
                participants.add("0x" + padded[-40:])
    return participants


def safe_wallet_from_receipt(receipt: Mapping[str, Any]) -> str:
    """HEURISTIC: the likely Safe behind a receipt, from its own events.

    **Not part of :func:`resolve_trading_wallet`'s resolution order, and not
    safe to use as an identity check** — see the module docstring. A matching
    topic proves only that *some* contract emitted a log with that signature
    hash. Provided for callers that already know out of band that they hold a
    Safe-executed receipt (diagnostics, evidence tooling, tests) and want the
    likely address without re-deriving it.

    Returns the lower-cased address of the single emitter of a Safe execution
    event (``ExecutionFromModuleSuccess`` / ``ExecutionSuccess``) that also
    participates in an ERC-20 ``Transfer`` in the same receipt, or ``""`` when
    there is none, when two distinct emitters make it ambiguous, or when the
    emitter moved no tokens.
    """
    logs = receipt.get("logs")
    if not isinstance(logs, list):
        return ""
    emitters: set[str] = set()
    for log in logs:
        if not isinstance(log, Mapping):
            continue
        topics = log.get("topics")
        if not isinstance(topics, list | tuple) or not topics:
            continue
        if _normalize_topic(topics[0]) not in _SAFE_EXECUTION_TOPICS:
            continue
        emitter = _normalize_address(log.get("address"))
        if emitter:
            emitters.add(emitter)
    if len(emitters) != 1:
        return ""
    candidate = next(iter(emitters))
    if candidate not in _erc20_transfer_participants(logs):
        return ""
    return candidate


def resolve_trading_wallet(receipt: Mapping[str, Any]) -> str:
    """Effective trading wallet for ``receipt`` — the Safe under Safe execution.

    See the module docstring for the full rationale and resolution order.
    Returns a lower-cased ``0x`` address, or ``""`` when the receipt carries no
    usable wallet at all (Empty != Zero — callers keep their own
    unmeasured/degraded handling; this helper never invents an address).
    """
    if not isinstance(receipt, Mapping):
        return ""

    stamped = _normalize_address(receipt.get(TRADING_WALLET_KEY))
    if stamped:
        return stamped

    # NOTE: deliberately no event-derived Safe step here — see the module
    # docstring. An event signature is not authentication.
    return _normalize_address(receipt.get("from") or receipt.get("from_address"))


__all__ = [
    "TRADING_WALLET_KEY",
    "normalize_wallet_address",
    "resolve_trading_wallet",
    "safe_wallet_from_receipt",
    "stamp_trading_wallet",
]
