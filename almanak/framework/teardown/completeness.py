"""Teardown completeness enforcement — every KNOWN open position needs a closing intent (VIB-5469 / TD-11).

> **A tracked-open position with NO closing intent must make the teardown FAIL
> LOUD, not silently half-unwind.**

Teardown's two surfaces are produced independently: ``get_open_positions()``
enumerates what is open (reconciled against the ``position_registry`` WARM read
path, TD-01) and ``generate_teardown_intents()`` builds the closing plan. Nothing
structurally guarantees the second covers the first. A strategy whose teardown
returns ``[]`` (VIB-5417: spark teardown unimplemented) or repays a borrow but
never withdraws the collateral (ALM-2900) lands every emitted intent on-chain and
then reports a clean ``COMPLETED`` while a tracked position is still open — the
silent half-unwind this module exists to stop.

This is a **pre-execution INTENT-COVERAGE check**, deliberately distinct from the
on-chain post-teardown verification (TD-15 / VIB-5473):

* Coverage answers *"did the plan even try to close this position?"* — a pure,
  structural property of ``(positions, intents)`` computed before anything
  executes.
* On-chain verification answers *"is it actually closed on-chain?"* — measured
  after execution.

A position can be *covered* (an intent targets it) yet still fail on-chain
verification (the intent reverted); and a teardown can execute every emitted
intent successfully yet be *incomplete* (a position had no intent at all). Both
must fail the teardown; this module owns the first.

**Inverted failure semantics (blueprint 14 §Teardown).** Teardown's first job is
to remove on-chain risk. The coverage gap is computed up front, but the lanes
still execute every intent they DO have (risk reduction) and fold the
completeness failure into the result **after** — a missing-intent position never
blocks the risk-reducing intents that exist. The result surfaces
``verification_status=FAILED`` / ``success=False`` loudly.

Scope guard: only position types teardown can structurally reason about are
enforced. ``PREDICTION`` / ``CEX`` (no generic closing-intent vocabulary here)
are treated as covered so the check never fabricates a false failure for a
position it cannot evaluate — Empty ≠ Zero applied to *enforceability*.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownAssetPolicy,
    TeardownPositionSummary,
)

logger = logging.getLogger(__name__)


# Position types teardown has a generic closing-intent vocabulary for. A type
# NOT in this set is skipped by the coverage check (treated as covered) — the
# check must never FAIL a position it cannot structurally reason about, which
# would turn an un-modelled venue into a false teardown failure.
_ENFORCEABLE_TYPES: frozenset[PositionType] = frozenset(
    {
        PositionType.PERP,
        PositionType.BORROW,
        PositionType.SUPPLY,
        PositionType.LP,
        PositionType.STAKE,
        PositionType.VAULT,
        PositionType.TOKEN,
    }
)

# Detail keys that may carry the token symbol/address a position is denominated
# in. Mirrors the lookup ``full_close._close_intent_for_position`` uses so the
# coverage check reads the same identity the close builder reads. ``address`` is
# included because the STAKE / TOKEN close builder resolves its swap token via
# ``_first(details, "asset", "token", "address")`` — an address-keyed held token
# yields a valid ``SWAP(from_token=<address>)``, so the coverage check must see
# that same address or it would falsely fail a legitimate close (VIB-5469).
# ``asset_symbol`` is included because a registry-sourced lending position
# (``registry_enumeration._position_info_from_lending_registry_row`` /
# ``_position_info_from_pendle_registry_row``) deliberately stores its reserve
# symbol under ``details["asset_symbol"]`` (NOT ``details["asset"]``, to avoid
# the PortfolioValuer wallet-overlap special-casing). Without it the coverage
# check reads ``{}`` tokens for those positions and falsely fails a legitimate
# WITHDRAW/REPAY/SWAP close on a restart-only registry enumeration (VIB-5523).
# This is safe: completeness and PortfolioValuer are different consumers — the
# valuer keys on ``details["asset"]`` and is untouched by adding the symbol key
# here, so the deliberate separation that keeps the valuer from double-counting
# these legs as wallet tokens is preserved.
_TOKEN_DETAIL_KEYS: tuple[str, ...] = (
    "asset",
    "asset_symbol",
    "token",
    "collateral_token",
    "borrow_token",
    "debt_token",
    "supply_token",
    "token_in",
    "staked_token",
    "address",
    "token0",
    "token1",
    # A Pendle PT/YT held as a generic TOKEN carries its identity under
    # ``pt_token`` / ``pt_symbol`` (the producer-side symbol used by the
    # ``protocol="pendle"`` swap-back exit). Recognising them lets a legitimate
    # PT close SWAP be credited instead of false-failing the teardown as a
    # silent strand (VIB-5590). Additive/lenient: a token key only ever WIDENS
    # the match set, and a match still requires the intent's token to equal one
    # of these — no unrelated position type carries a pt_* detail, so this
    # cannot false-match another position.
    "pt_token",
    "pt_symbol",
)

# The EXACT key order ``full_close._close_intent_for_position`` uses to resolve
# the swap-close token for a STAKE / TOKEN position (its
# ``_first(details, "asset", "asset_symbol", "token", "address", "pt_token",
# "pt_symbol")`` call). ``full_close`` emits NO closing intent when that token
# already equals the consolidation target (nothing to swap). Mirroring the same
# ordered first-truthy resolution — NOT the wider ``_TOKEN_DETAIL_KEYS`` set —
# is what lets the completeness gate credit the SAME no-op close ``full_close``
# makes, and only that one (VIB-5494 Item 1).
_SWAP_CLOSE_TOKEN_KEYS: tuple[str, ...] = (
    "asset",
    "asset_symbol",
    "token",
    "address",
    "pt_token",
    "pt_symbol",
)

# Detail markers that mean a STAKE / TOKEN position is NOT a plain held wallet
# token but a vault share or a lending leg reported under the generic type — its
# close is a VAULT_REDEEM / WITHDRAW / REPAY, never a swap. The target no-op
# credit must NOT apply to these: a supplied-collateral leg whose asset happens
# to equal the target is STILL on-chain in the protocol after teardown (the
# wallet does NOT hold the target), so crediting it a no-op would convert a
# caught strand into a silent one — the exact false-NEGATIVE this ticket forbids.
_NON_SWAP_CLOSE_MARKERS: tuple[str, ...] = (
    "vault_address",
    "vault",
    "market_id",
    "collateral_token",
    "borrow_token",
    "debt_token",
    "supply_token",
)


def _swap_close_token(position: PositionInfo) -> str | None:
    """The upper-cased swap-close token full_close resolves for a STAKE/TOKEN.

    First truthy value among :data:`_SWAP_CLOSE_TOKEN_KEYS`, in that exact order
    (mirrors ``full_close._close_intent_for_position``); ``None`` when none is
    present — the same "missing details → hand-roll" case full_close skips loud.
    """
    details = position.details or {}
    for key in _SWAP_CLOSE_TOKEN_KEYS:
        value = details.get(key)
        if value:
            return str(value).upper()
    return None


def _is_noop_target_close(position: PositionInfo, consolidation_target_token: str | None) -> bool:
    """True when full_close emits NO intent for this position because its held
    token already equals the consolidation target (VIB-5494 Item 1).

    ``full_close`` drops the swap for a STAKE / TOKEN position whose denominating
    token already IS the consolidation target (``token == target_token → return
    None``): there is nothing to swap and the wallet ends holding exactly the
    desired final asset. Without this the coverage check would see that position
    with no matching intent and force the teardown to FAIL — able to lock a
    deployment into a recurring failed-teardown loop on the no-intents gate.

    Fail-SAFE against false-negatives:

    * Only credited when a target is actually threaded (``TARGET_TOKEN`` policy —
      the caller passes ``None`` for entry-token / keep-outputs, preserving
      today's strict behaviour).
    * Only for STAKE / TOKEN, and only when the position carries NO vault /
      lending identity (:data:`_NON_SWAP_CLOSE_MARKERS`) — a collateral leg or
      vault share reported as TOKEN is never no-op'd, because it stays on-chain
      after teardown even when its asset equals the target.
    * The credit is valid by the end-state invariant: held token == final
      consolidation target ⇒ the wallet holds exactly the target ⇒ not a strand,
      independent of which ``target_token`` ``full_close`` itself was passed.
    """
    if not consolidation_target_token:
        return False
    if position.position_type not in (PositionType.STAKE, PositionType.TOKEN):
        return False
    details = position.details or {}
    if any(details.get(marker) for marker in _NON_SWAP_CLOSE_MARKERS):
        return False
    leg = str(details.get("leg") or "").lower()
    if leg in ("collateral", "debt"):
        return False
    token = _swap_close_token(position)
    return token is not None and token == consolidation_target_token.upper()


def resolve_consolidation_noop_target(
    asset_policy: TeardownAssetPolicy | str | None,
    target_token: str | None,
    *,
    chain: str | None = None,
) -> str | None:
    """The single token every residual is consolidated INTO, or ``None`` when the
    policy has no single target so no no-op credit should apply (VIB-5494 Item 1).

    Returns the target ONLY for the ``TARGET_TOKEN`` policy.
    ``ENTRY_TOKEN`` (per-position entry assets) and ``KEEP_OUTPUTS`` (no terminal
    swaps) have no single "already done" token, so they return ``None`` and the
    completeness gate keeps its strict, fail-safe behaviour for them. Callers
    thread the result into :func:`check_intent_coverage`.

    *chain* (VIB-5727) resolves the "no preference" sentinel to the same token
    the consolidation phase will actually target. Threading it is not cosmetic:
    this gate credits a position already denominated in the target as a no-op
    close, and without the real target that credit is lost — which is exactly
    the recurring failed-teardown loop VIB-5494 Item 1 fixed. Chain unknown →
    the legacy ``USDC`` answer, so the gate is never *less* accurate than before.
    """
    # Narrow explicitly for mypy (the enum constructor is typed to accept only
    # ``str``). Behaviour is identical to ``TeardownAssetPolicy(asset_policy)``
    # with a caught ``ValueError``/``TypeError``: a member passes through, a valid
    # string resolves, and None / unknown-string degrade to ``None`` → the strict
    # fail-safe gate (the None-degrades-to-strict path is load-bearing for the
    # false-negative safety, so it must stay identical).
    if isinstance(asset_policy, TeardownAssetPolicy):
        policy = asset_policy
    elif isinstance(asset_policy, str):
        try:
            policy = TeardownAssetPolicy(asset_policy)
        except (ValueError, TypeError):
            return None
    else:
        return None
    if policy != TeardownAssetPolicy.TARGET_TOKEN:
        return None

    from almanak.framework.teardown.consolidation import resolve_chain_target_token

    # Same resolution the consolidation phase uses, so the no-op credit is
    # granted for the token that will actually be targeted (VIB-5727). An
    # explicit-but-unresolvable target yields None here → strict gate, which is
    # the fail-safe direction (no false "already done" credit).
    resolved, _warnings = resolve_chain_target_token(target_token, chain)
    return resolved


# Position types where two distinct positions ALWAYS need two distinct on-chain
# closes, so a single under-specified intent must not blanket-cover several
# (VIB-5494 Item 2). TOKEN / STAKE are deliberately excluded: they are closed by
# a fungible token swap, so one ``SWAP(from=<token>, amount=all)`` legitimately
# empties every same-token holding — there is no multi-position ambiguity to
# guard, and requiring disambiguation there would false-FAIL a valid plan.
_TYPES_REQUIRING_DISAMBIGUATION: frozenset[PositionType] = frozenset(
    {
        PositionType.LP,
        PositionType.PERP,
        PositionType.VAULT,
        PositionType.BORROW,
        PositionType.SUPPLY,
    }
)


@dataclass(frozen=True)
class CompletenessReport:
    """Outcome of an intent-coverage check over a KNOWN open-position set.

    Attributes:
        uncovered: The tracked-open positions that NO closing intent targets.
            Empty ⇒ every enforceable position has at least one closing intent.
        total_enforceable: How many positions were eligible for the check
            (enforceable types). Informational — distinguishes "nothing to
            enforce" from "all covered".
    """

    uncovered: tuple[PositionInfo, ...] = ()
    total_enforceable: int = 0

    @property
    def complete(self) -> bool:
        """``True`` when every enforceable tracked-open position has a closing intent."""
        return not self.uncovered

    def error_message(self) -> str:
        """Operator-facing, fail-loud description of the coverage gap."""
        parts = [
            f"{p.position_type.value} {p.position_id} ({p.protocol}" + (f"/{p.chain}" if p.chain else "") + ")"
            for p in self.uncovered
        ]
        return (
            "Teardown completeness check FAILED: "
            f"{len(self.uncovered)} tracked-open position(s) have NO closing intent and would be "
            f"stranded — {', '.join(parts)}. Teardown must not report success while a known "
            "position is left open (VIB-5469 / ALM-2900). Verify on-chain and re-run teardown."
        )


def _as_position_list(positions: Any) -> list[PositionInfo]:
    """Coerce a summary / iterable / None into a list of ``PositionInfo``."""
    if positions is None:
        return []
    # A lone PositionInfo is not a list/summary; wrap it rather than dropping it
    # — silently ignoring a single known-open position would defeat the gate.
    if isinstance(positions, PositionInfo):
        return [positions]
    if isinstance(positions, TeardownPositionSummary):
        return list(positions.positions or [])
    if isinstance(positions, Iterable):
        return [p for p in positions if isinstance(p, PositionInfo)]
    return []


def _position_tokens(position: PositionInfo) -> set[str]:
    """Upper-cased token symbols/addresses a position may be denominated in."""
    tokens: set[str] = set()
    details = position.details or {}
    for key in _TOKEN_DETAIL_KEYS:
        value = details.get(key)
        if value:
            tokens.add(str(value).upper())
    return tokens


def _field(intent: Any, name: str) -> Any:
    """Read ``name`` from an intent, supporting both objects and dict stand-ins.

    Production teardown intents are ``BaseIntent`` objects, but some callers /
    tests carry dict-shaped intents (``Intent.to_dict`` uses ``type`` for the
    intent type). Reading both keeps the structural coverage check robust and
    never false-fails on a dict representation.
    """
    if isinstance(intent, dict):
        return intent.get(name)
    return getattr(intent, name, None)


def _intent_type(intent: Any) -> str:
    """The intent's ``IntentType`` value as an upper-cased string (``""`` if none).

    Robust to the resume path: intents serialized into ``pending_intents_json``
    can store the intent type as the enum's fully-qualified ``str`` form
    (``"IntentType.SWAP"``) rather than its bare ``.value`` (``"SWAP"``). Taking
    the segment after the final ``.`` normalizes both forms so a resumed teardown
    matches coverage identically to a freshly-built one (VIB-5469). Bare values
    carry no dot, so this is a no-op for the common case.
    """
    itype = _field(intent, "intent_type")
    if itype is None and isinstance(intent, dict):
        itype = intent.get("type")
    if itype is None:
        return ""
    return str(getattr(itype, "value", itype)).upper().split(".")[-1]


def _attr_str(intent: Any, name: str) -> str | None:
    """A string intent attribute, upper-cased; ``None`` when absent/empty."""
    value = _field(intent, name)
    if value is None or value == "":
        return None
    return str(value).upper()


def _chain_compatible(position: PositionInfo, intent: Any) -> bool:
    """True unless BOTH carry a chain and they differ.

    Lenient by design: an intent that omits ``chain`` defaults to the strategy's
    primary chain at compile time, so a missing chain on either side must not
    cause a false "uncovered" (which would falsely fail a working teardown). When
    both are explicitly set, a same-symbol token on a different chain is a real
    distinct position, so a mismatch is NOT coverage.
    """
    pos_chain = str(position.chain or "").lower()
    intent_chain = str(_field(intent, "chain") or "").lower()
    if not pos_chain or not intent_chain:
        return True
    return pos_chain == intent_chain


def _protocol_compatible(position: PositionInfo, intent: Any) -> bool:
    """True unless BOTH carry a protocol and they differ.

    Lenient by design (same contract as :func:`_chain_compatible`): a missing
    protocol on either side defaults to compatible so a legitimately-targeted
    close is never falsely flagged uncovered. When BOTH are set, the SAME token
    on a DIFFERENT protocol is a distinct lending/perp position (e.g. Aave-USDC
    vs Morpho-USDC borrow, or an ETH/USD perp on two venues), so a single
    closing intent must NOT cover both — this closes the same-token /
    same-market cross-protocol silent-strand gap (VIB-5469).
    """
    pos_protocol = str(position.protocol or "").lower()
    intent_protocol = str(_field(intent, "protocol") or "").lower()
    if not pos_protocol or not intent_protocol:
        return True
    return pos_protocol == intent_protocol


def _market_compatible(position: PositionInfo, intent: Any) -> bool:
    """True unless BOTH carry an isolated-market id and they differ.

    Lenient, like :func:`_protocol_compatible`. Two isolated markets on the same
    protocol/token (e.g. two Morpho Blue ``market_id`` vaults) are distinct
    positions, so a REPAY/WITHDRAW naming one market must not cover the other.
    Most lending protocols (Aave V3 / Spark) carry no ``market_id`` on either
    side → always compatible, so this is a no-op there.
    """
    details = position.details or {}
    pos_market = str(details.get("market_id") or details.get("market") or "").lower()
    intent_market = str(_field(intent, "market_id") or "").lower()
    if not pos_market or not intent_market:
        return True
    return pos_market == intent_market


def _intent_token_matches(intent: Any, position: PositionInfo, attr: str) -> bool:
    """True when ``intent.<attr>`` matches one of the position's token symbols."""
    token = _attr_str(intent, attr)
    if token is None:
        return False
    return token in _position_tokens(position)


def _covers_lending_leg(intent: Any, position: PositionInfo, token_attr: str) -> bool:
    """A lending REPAY/WITHDRAW covers a BORROW/SUPPLY leg only when the token,
    protocol, AND isolated-market id all line up (each lenient on absence).

    Token identity alone is insufficient: the same token can be borrowed /
    supplied across distinct protocols or isolated markets, and a single closing
    intent must not be credited against a leg it does not actually target
    (VIB-5469 — Aave-USDC + Morpho-USDC silent strand).
    """
    return (
        _intent_token_matches(intent, position, token_attr)
        and _protocol_compatible(position, intent)
        and _market_compatible(position, intent)
    )


def _position_is_long(position: PositionInfo) -> bool | None:
    """The perp position's side as a bool (``True`` long / ``False`` short).

    ``None`` when the side is unknown — the lenient case. Reads the typed
    ``direction`` field first, then falls back to ``details`` (``is_long`` bool
    or a ``side`` / ``direction`` string) so hand-rolled position summaries that
    only stamp the detail still disambiguate long vs short.
    """
    direction = position.direction
    if not direction:
        details = position.details or {}
        flag = details.get("is_long")
        if isinstance(flag, bool):
            return flag
        direction = details.get("side") or details.get("direction")
    if not direction:
        return None
    normalized = str(direction).upper()
    if normalized in ("LONG", "BUY"):
        return True
    if normalized in ("SHORT", "SELL"):
        return False
    return None


def _intent_is_long(intent: Any) -> bool | None:
    """The close intent's side as a bool, or ``None`` when unspecified.

    Production ``PerpCloseIntent`` always carries a typed ``is_long`` bool; dict
    stand-ins may instead carry a ``direction`` / ``side`` string or omit the
    side entirely (then ``None`` → lenient).
    """
    flag = _field(intent, "is_long")
    if isinstance(flag, bool):
        return flag
    direction = _field(intent, "direction") or _field(intent, "side")
    if direction:
        normalized = str(direction).upper()
        if normalized in ("LONG", "BUY"):
            return True
        if normalized in ("SHORT", "SELL"):
            return False
    return None


def _side_compatible(position: PositionInfo, intent: Any) -> bool:
    """True unless BOTH carry a perp side and they differ.

    Lenient by design: only an explicit long-vs-short MISMATCH breaks coverage.
    A long and a short on the SAME market each need their own close, so one
    ``PERP_CLOSE`` must not cover both (VIB-5469).
    """
    pos_side = _position_is_long(position)
    intent_side = _intent_is_long(intent)
    if pos_side is None or intent_side is None:
        return True
    return pos_side == intent_side


def _position_identity(position: PositionInfo, detail_keys: tuple[str, ...]) -> str:
    """The position's lower-cased identity string for id-based matching.

    First truthy value among ``detail_keys`` wins; falls back to
    ``position_id``. ``""`` when neither yields anything — the lenient case.
    """
    details = position.details or {}
    for key in detail_keys:
        value = details.get(key)
        if value:
            return str(value).lower()
    return str(position.position_id or "").lower()


def _lenient_identity_match(
    intent: Any,
    position: PositionInfo,
    intent_attr: str,
    detail_keys: tuple[str, ...],
) -> bool:
    """Match a position's identity against ``intent.<intent_attr>`` by id.

    Shared structural matcher for the id-keyed position types (market / vault,
    blueprint 14 §Completeness enforcement). Lenient by design: when EITHER side
    omits its identity the match defaults to ``True`` so a legitimately-targeted
    close is never falsely flagged uncovered. Only when BOTH sides name an
    identity does a mismatch count as "not coverage".
    """
    identity = _position_identity(position, detail_keys)
    intent_identity = str(_field(intent, intent_attr) or "").lower()
    if identity and intent_identity:
        return identity == intent_identity
    return True


def _identity_present_and_matches(
    intent: Any,
    position: PositionInfo,
    intent_attr: str,
    detail_keys: tuple[str, ...],
) -> bool:
    """STRICT sibling of :func:`_lenient_identity_match` (VIB-5494 Item 2).

    Returns ``True`` only when BOTH sides name an identity AND they are equal —
    the intent must POSITIVELY carry this position's own identity, never default
    to covered on absence. Used only for the multi-position disambiguation guard,
    so a single id-less intent cannot blanket-cover several distinct positions.
    """
    identity = _position_identity(position, detail_keys)
    intent_identity = str(_field(intent, intent_attr) or "").lower()
    return bool(identity) and bool(intent_identity) and identity == intent_identity


def _norm_identity(value: Any) -> str:
    """Normalise an id/market for strict identity comparison (Empty ≠ Zero).

    ``None`` (absent / unmeasured) → ``""``; a MEASURED value → its stripped,
    lower-cased string. Critically this preserves a measured ``0`` as ``"0"``
    (present), unlike ``str(value or "")`` — an ERC-721 token id of ``0`` or a
    sequential-index lending ``market_id`` of ``0`` is a real identity, not
    "absent" (VIB-5494 / Gemini review). An ABSENT id still yields ``""`` and
    takes the exact same lenient/strict branch it does today.
    """
    return "" if value is None else str(value).strip().lower()


def _lp_carries_identity(intent: Any, position: PositionInfo) -> bool:
    """An LP_CLOSE strictly identifies an LP only by its ``position_id`` (the pool
    is shared across NFTs, so a pool-only match is lenient, not disambiguating).

    Empty ≠ Zero: a measured ``position_id`` of ``0`` is a valid ERC-721 token id,
    so two id-0 positions must still be able to match (``str(id or "")`` would
    coerce ``0`` → ``""`` and false-FAIL them) — Gemini finding, completeness.py:546.
    """
    intent_id = _norm_identity(_field(intent, "position_id"))
    return bool(intent_id) and intent_id == _norm_identity(position.position_id)


def _perp_carries_identity(intent: Any, position: PositionInfo) -> bool:
    """A perp close/cancel strictly identifies a PERP by its ``market``. A pending
    -order / free-margin residual is matched on its order_key / kind in
    ``_covers_perp`` — inherently specific, not a market-wide default.

    Empty ≠ Zero: a market / position_id of ``0`` is a measured identity, so it is
    preserved (not coalesced to absent) when resolving the position's market.
    """
    kind = str((position.details or {}).get("kind") or "").lower()
    if kind in ("pending_order", "hypercore_cash"):
        return True
    market = (position.details or {}).get("market")
    pos_market = _norm_identity(market if market is not None else position.position_id)
    intent_market = _norm_identity(_field(intent, "market"))
    return bool(intent_market) and intent_market == pos_market


def _vault_carries_identity(intent: Any, position: PositionInfo) -> bool:
    """A VAULT_REDEEM strictly identifies a VAULT by its ``vault_address``."""
    return _identity_present_and_matches(intent, position, "vault_address", ("vault_address", "address", "vault"))


def _lending_carries_identity(intent: Any, position: PositionInfo) -> bool:
    """A repay/withdraw strictly identifies a lending leg by naming this leg's
    ``protocol`` (+ isolated-market ``market_id`` when the leg carries one).

    Token is already matched by ``_covers``; protocol (+ market) is the sibling
    disambiguator, so a protocol/market-less intent cannot blanket-cover two
    distinct legs. (Cross-chain-only siblings that share protocol are a known
    residual — an explicit chain still mismatches via ``_chain_compatible``; a
    chain-omitted one is logged, and TD-15 on-chain verification backstops.)
    """
    intent_protocol = str(_field(intent, "protocol") or "").lower()
    if not intent_protocol or intent_protocol != str(position.protocol or "").lower():
        return False
    details = position.details or {}
    # Empty ≠ Zero: a sequential-index ``market_id`` of ``0`` is a MEASURED
    # isolated-market identity, not "market-less". ``str(market_id or market or
    # "")`` would coerce ``0`` → falsy → the leg would fall through to the lenient
    # ``return True`` and be blanket-covered by a market-less intent — the exact
    # multi-position false-POSITIVE Item 2 exists to prevent (Gemini finding,
    # completeness.py:584). Preserve the measured 0 via explicit None handling.
    market_id = details.get("market_id")
    pos_market = _norm_identity(market_id if market_id is not None else details.get("market"))
    if not pos_market:
        return True
    intent_market = _norm_identity(_field(intent, "market_id"))
    return bool(intent_market) and intent_market == pos_market


# Per-type STRICT identity predicate (VIB-5494 Item 2) — the disambiguating
# identity a covering intent must POSITIVELY carry so a single under-specified
# intent cannot blanket-cover several distinct same-type positions. Flat dispatch
# mirrors ``_COVERAGE_HANDLERS``; only the types in
# ``_TYPES_REQUIRING_DISAMBIGUATION`` need an entry (TOKEN/STAKE are fungible
# token swaps — inherently single-target — so they never need disambiguation).
_IDENTITY_HANDLERS = {
    PositionType.LP: _lp_carries_identity,
    PositionType.PERP: _perp_carries_identity,
    PositionType.VAULT: _vault_carries_identity,
    PositionType.BORROW: _lending_carries_identity,
    PositionType.SUPPLY: _lending_carries_identity,
}


def _intent_carries_position_identity(intent: Any, position: PositionInfo) -> bool:
    """Does a KNOWN-covering ``intent`` name THIS position's own disambiguating
    identity (VIB-5494 Item 2)?

    Precondition: ``_covers(intent, position)`` is already ``True`` (so type /
    token / side / protocol / chain compatibility all hold). This adds the one
    thing the lenient matchers waive — a POSITIONAL identity match — so an intent
    that omits (or names a sibling's) id / market / pool / vault does NOT qualify.
    Consulted only when ≥2 positions of the same type exist; below that the
    lenient default is preserved (and merely logged). A type with no entry
    (TOKEN / STAKE) returns ``True`` — no multi-position ambiguity to guard.
    """
    handler = _IDENTITY_HANDLERS.get(position.position_type)
    return handler(intent, position) if handler is not None else True


def _covers_borrow(intent: Any, position: PositionInfo, itype: str) -> bool:
    # DELEVERAGE is structurally a repay (emergency forced repay with risk-event
    # context — same on-chain path, same ``token`` field; see DeleverageIntent).
    # A BORROW closed by an HF-guard deleverage instead of a routine repay must
    # still count as covered, else the gate false-FAILs the unwind (VIB-5469).
    return itype in ("REPAY", "DELEVERAGE") and _covers_lending_leg(intent, position, "token")


def _covers_supply(intent: Any, position: PositionInfo, itype: str) -> bool:
    return itype == "WITHDRAW" and _covers_lending_leg(intent, position, "token")


def _covers_lp(intent: Any, position: PositionInfo, itype: str) -> bool:
    if itype != "LP_CLOSE":
        return False
    pos_id = str(position.position_id or "").lower()
    intent_id = str(_field(intent, "position_id") or "").lower()
    if pos_id and intent_id and pos_id == intent_id:
        return True
    pool = str((position.details or {}).get("pool") or "").lower()
    intent_pool = str(_field(intent, "pool") or "").lower()
    if pool and intent_pool and pool == intent_pool:
        return True
    # An LP_CLOSE on the same chain with no contradicting id/pool is accepted
    # (a strategy commonly tracks one LP and closes it by live liquidity).
    return not intent_id and not intent_pool


# Residual ``kind`` markers (set by teardown residual discovery) for off-position
# committed capital. An UNVERIFIED residual is a fail-closed sentinel with NO
# cancellable identity — no intent can cover it, so it keeps the completeness gate
# failing loud (Empty ≠ Zero). A ``pending_order`` residual is NOT in this set: it
# IS coverable, but only by a key-matched PERP_CANCEL_ORDER (VIB-5568) — see
# ``_covers_perp``. Keying on the generic ``kind`` marker keeps this protocol-agnostic.
_UNCOVERABLE_PERP_KINDS: frozenset[str] = frozenset({"residual_unverified"})


def _covers_perp(intent: Any, position: PositionInfo, itype: str) -> bool:
    kind = str((position.details or {}).get("kind") or "").lower()
    # A pending (unfilled) ORDER residual (VIB-5116) is committed-but-unfilled
    # collateral, NOT an open position. It is covered ONLY by a PERP_CANCEL_ORDER
    # (VIB-5568) whose ``order_key`` MATCHES the residual's — never by a PERP_CLOSE
    # (which closes an open position, not a pending order), and never by a cancel
    # for a DIFFERENT order. The key match is the fund-safety anchor: a real open
    # long + a pending increase on the SAME market+side must not let one cancel (or
    # one close) falsely cover the other and let the strand pass the gate. When the
    # matching cancel executed and completeness passes, the on-chain post-condition
    # still re-reads the OrderVault (count == 0) as the authoritative backstop.
    # A HyperCore free-margin residual (VIB-5617) is unencumbered cash parked on
    # the venue's off-chain ledger, NOT an open position. It is recovered — and
    # thus covered — ONLY by a PERP_WITHDRAW (the CoreWriter spotSend HyperCore→L1
    # bridge), never by a PERP_CLOSE (which closes a position, not a cash balance)
    # and never by a cancel. Keyed on the generic ``kind`` marker so it stays
    # protocol-agnostic. NOTE: no hyperliquid teardown path emits this residual kind
    # today (HyperCore free margin is not yet enumerated as a PositionInfo), so this
    # branch is inert until that discovery lands — it makes the verb completeness-
    # ready (a PERP_WITHDRAW recovering surfaced cash passes the gate) rather than
    # latently uncovered, without fabricating coverage for anything currently
    # surfaced. Fail-safe: only credits when a residual explicitly carries this kind.
    if kind == "hypercore_cash":
        return itype == "PERP_WITHDRAW"
    if kind == "pending_order":
        if itype != "PERP_CANCEL_ORDER":
            return False
        # Normalise the 0x prefix on both sides so a prefixed key never mismatches an
        # unprefixed one (both are 0x-prefixed in practice, but this is a fund-safety
        # comparison — a false mismatch would leave a recovered order marked uncovered
        # → teardown FAILED). removeprefix strips only the leading 0x. (Gemini review.)
        intent_key = str(_field(intent, "order_key") or "").lower().removeprefix("0x")
        res_key = (
            str((position.details or {}).get("order_key") or position.position_id or "").lower().removeprefix("0x")
        )
        return bool(intent_key) and bool(res_key) and intent_key == res_key
    if kind in _UNCOVERABLE_PERP_KINDS:
        return False
    # Market alone is insufficient: a long and a short on the SAME market — or
    # the same market on two perp venues — are distinct positions, so one close
    # must not cross-cover the other. Protocol + side are lenient on absence
    # (only an explicit mismatch breaks coverage), so framework-built closes
    # (which always stamp protocol + is_long) catch the real long/short strand
    # while under-specified hand-rolled intents preserve today's behaviour.
    return (
        itype == "PERP_CLOSE"
        and _protocol_compatible(position, intent)
        and _side_compatible(position, intent)
        and _lenient_identity_match(intent, position, "market", ("market",))
    )


def _covers_vault(intent: Any, position: PositionInfo, itype: str) -> bool:
    # Detail keys mirror ``full_close._close_intent_for_position`` which resolves
    # the redeem target via ``_first(details, "vault_address", "address", "vault")
    # or position_id`` — ``address`` must be here or an address-keyed vault whose
    # ``position_id`` is a logical name (e.g. "metamorpho_eth_yield") would read a
    # mismatched identity and falsely fail a legitimate redeem (VIB-5469).
    return itype == "VAULT_REDEEM" and _lenient_identity_match(
        intent, position, "vault_address", ("vault_address", "address", "vault")
    )


def _covers_stake(intent: Any, position: PositionInfo, itype: str) -> bool:
    if itype == "UNSTAKE" and _intent_token_matches(intent, position, "token_in"):
        return True
    # A cooldown-complete Ethena (sUSDe) teardown closes the STAKE with a CLAIM
    # of the underlying — ``Intent.claim(protocol="ethena", token="USDe")``
    # (blueprint 14 §Withdrawal Queues and Cooldowns). Without this a strategy in
    # the claim phase would false-FAIL completeness. Additive: CLAIM is only ever
    # credited when its ``token`` matches one the position is denominated in.
    if itype == "CLAIM" and _intent_token_matches(intent, position, "token"):
        return True
    return itype == "SWAP" and _intent_token_matches(intent, position, "from_token")


def _covers_token(intent: Any, position: PositionInfo, itype: str) -> bool:
    # VIB-5494 Item 1 (fixed): ``full_close`` emits NO intent for a held
    # TOKEN/STAKE position whose token already equals the consolidation target
    # (nothing to swap). That no-op is now credited BEFORE this handler runs, in
    # ``check_intent_coverage`` via ``_is_noop_target_close`` (which threads the
    # consolidation target), so such a position is treated as covered instead of
    # false-failing the teardown. This handler still covers the cases where a
    # closing intent DOES exist.
    #
    # A held token is closed by swapping it away; a collateral leg reported as
    # TOKEN is closed by a withdraw. Accept either referencing the token.
    if itype == "SWAP" and _intent_token_matches(intent, position, "from_token"):
        return True
    if itype in ("WITHDRAW", "REPAY") and _covers_lending_leg(intent, position, "token"):
        return True
    # A held TOKEN moved off-chain (closed by bridging) emits BRIDGE(token=…), and
    # a wrapped-native held token (WETH/WMATIC) closed by unwrapping emits
    # UNWRAP_NATIVE(token=…). Both reference the held token directly, so recognize
    # them as coverage — additive/safe, matched only on the token (VIB-5469).
    if itype in ("BRIDGE", "UNWRAP_NATIVE") and _intent_token_matches(intent, position, "token"):
        return True
    # VIB-5573: an ERC-4626 vault position a strategy reports as PositionType.TOKEN
    # rather than PositionType.VAULT (e.g. the metamorpho_base_yield demo, which
    # types its vault position TOKEN for USD-pegged valuation simplicity — see its
    # get_open_positions) is closed by a VAULT_REDEEM. Credit it by the SAME
    # vault-address identity match _covers_vault uses. Safe against false-coverage:
    # a VAULT_REDEEM always carries vault_address and a position always has an id,
    # so this is ALWAYS a strict address match — a plain held token (whose id is
    # not a vault address) can never be leniently covered. Without this a real
    # on-chain redeem that closes the position is flagged uncovered and the
    # completeness gate FAILs the whole teardown (the E2E-caught metamorpho case).
    if itype == "VAULT_REDEEM" and _lenient_identity_match(
        intent, position, "vault_address", ("vault_address", "address", "vault")
    ):
        return True
    return itype == "UNSTAKE" and _intent_token_matches(intent, position, "token_in")


# Per-position-type coverage predicate. Keeping each predicate small (and the
# dispatch flat) keeps the structural matcher auditable and under the CRAP gate.
_COVERAGE_HANDLERS = {
    PositionType.BORROW: _covers_borrow,
    PositionType.SUPPLY: _covers_supply,
    PositionType.LP: _covers_lp,
    PositionType.PERP: _covers_perp,
    PositionType.VAULT: _covers_vault,
    PositionType.STAKE: _covers_stake,
    PositionType.TOKEN: _covers_token,
}


def _covers(intent: Any, position: PositionInfo) -> bool:
    """Does ``intent`` plausibly close ``position``? (structural pre-exec test).

    Conservative: matches by position type → expected closing intent vocabulary
    plus a token/identity correspondence. Leans toward "covered" on ambiguity so
    the check never falsely fails a legitimate plan, while still catching the
    clear gaps it targets (no intent at all / repay-without-withdraw).
    """
    if not _chain_compatible(position, intent):
        return False
    handler = _COVERAGE_HANDLERS.get(position.position_type)
    if handler is None:
        return False
    return handler(intent, position, _intent_type(intent))


def _position_is_covered(
    position: PositionInfo,
    intent_list: list[Any],
    same_type_count: int,
) -> bool:
    """Whether ``position`` has a closing intent — with the VIB-5494 Item 2
    multi-position disambiguation guard applied.

    * No covering intent → uncovered.
    * Covered by an intent that POSITIVELY names this position's identity → covered.
    * Covered ONLY by a lenient default (the intent omitted the disambiguating
      id / market / pool / vault / protocol): accepted when this is the sole
      position of its type (today's behaviour, logged); REJECTED when ≥2
      positions of a disambiguation-requiring type exist, since one
      under-specified intent must not blanket-cover several distinct positions.
    """
    covering = [i for i in intent_list if _covers(i, position)]
    if not covering:
        return False
    if any(_intent_carries_position_identity(i, position) for i in covering):
        return True
    # Covered only via a lenient default (no positional identity on any intent).
    if position.position_type in _TYPES_REQUIRING_DISAMBIGUATION and same_type_count >= 2:
        logger.warning(
            "🛑 Teardown completeness: %s %s is covered only by an under-specified intent that "
            "omits its id/market/pool identity, and %d positions of this type exist — a single "
            "lenient intent cannot disambiguate multiple distinct positions; requiring an "
            "identity-scoped closing intent (VIB-5494).",
            position.position_type.value,
            position.position_id,
            same_type_count,
        )
        return False
    logger.info(
        "Teardown completeness: %s %s covered by a LENIENT default (intent omitted a "
        "disambiguating identity); accepted as the only enforceable position of its type (VIB-5494).",
        position.position_type.value,
        position.position_id,
    )
    return True


def check_intent_coverage(
    positions: TeardownPositionSummary | Iterable[PositionInfo] | None,
    intents: Iterable[Any] | None,
    *,
    consolidation_target_token: str | None = None,
) -> CompletenessReport:
    """Verify every KNOWN open position has at least one closing intent targeting it.

    Pure and side-effect free — the lanes own when to FAIL on the result.

    Args:
        positions: The KNOWN open-position set (registry-reconciled enumeration,
            TD-01) — NOT a wallet-wide sweep. A summary, an iterable of
            ``PositionInfo``, or ``None`` (treated as "nothing known").
        intents: The closing intents the strategy / framework built.
        consolidation_target_token: The token Phase-2 consolidation swaps residual
            holdings INTO (VIB-5494 Item 1). When set (``TARGET_TOKEN`` policy — see
            :func:`resolve_consolidation_noop_target`), a held STAKE / TOKEN position
            whose denominating token already equals this target is treated as
            covered: ``full_close`` emits no swap for it (nothing to do) and the
            wallet ends holding exactly the target. ``None`` (default) preserves the
            strict behaviour for entry-token / keep-outputs policies.

    Returns:
        A :class:`CompletenessReport`. ``complete`` is ``True`` when every
        enforceable position is covered (or there is nothing to enforce). The
        ``uncovered`` tuple names the tracked positions with no closing intent.
    """
    pos_list = _as_position_list(positions)
    # Drop ``None`` intents up front — a sparse / partially-built intent list must
    # not raise inside ``_covers`` (attribute access on ``None``) and false-FAIL.
    intent_list = [i for i in (intents or []) if i is not None]

    enforceable = [p for p in pos_list if p.position_type in _ENFORCEABLE_TYPES]
    # Item 1: a STAKE/TOKEN position already denominated in the consolidation
    # target needs no closing intent (full_close no-ops it); drop it from the
    # positions the coverage loop must find an intent for. It still counts toward
    # ``total_enforceable`` — it is enforceable-type and satisfied, not absent.
    active = [p for p in enforceable if not _is_noop_target_close(p, consolidation_target_token)]

    # Item 2: per-type population — the multi-position disambiguation guard fires
    # only when ≥2 positions of a type are being enforced.
    type_counts: dict[PositionType, int] = {}
    for p in active:
        type_counts[p.position_type] = type_counts.get(p.position_type, 0) + 1

    uncovered = [p for p in active if not _position_is_covered(p, intent_list, type_counts[p.position_type])]

    if uncovered:
        logger.error(
            "🛑 Teardown completeness: %d/%d tracked-open position(s) have no closing intent: %s",
            len(uncovered),
            len(enforceable),
            ", ".join(f"{p.position_type.value}:{p.position_id}" for p in uncovered),
        )

    return CompletenessReport(uncovered=tuple(uncovered), total_enforceable=len(enforceable))


__all__ = ["CompletenessReport", "check_intent_coverage", "resolve_consolidation_noop_target"]
