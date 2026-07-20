"""Detection + headline derivation for leveraged-lending strategies (VIB-4975).

Background — the cascade
------------------------
``strat pnl`` takes its Gross / Net PnL headline verbatim from
``PortfolioMetrics`` (``strat_pnl.py:_populate_gross_net_pnl``):

    gross = metrics.pnl_before_gas
          = total_value_usd − initial_value_usd − deposits_usd + withdrawals_usd

(VIB-2475: ``pnl_before_gas`` is now ``Decimal | None`` — when
``total_value_usd`` is *unmeasured* the headline is left ``None`` upstream in
``strat_pnl._populate_gross_net_pnl`` before this module's verdict is consulted,
so the cascade below describes the *measured* case only.)

(VIB-5866: ``deposits_usd`` / ``withdrawals_usd`` are ``Decimal | None`` on the
same terms.  An **absent** flow does NOT mean "no capital moved" — that
assumption is what books an external deposit as profit.  ``Decimal("0")`` is a
measured zero; ``None`` is unmeasured and propagates through ``pnl_before_gas``
to a suppressed headline, so the B-open derivation below is skipped rather than
computed off a fabricated zero flow.)

Under VIB-3614 ``total_value_usd`` is **positive-position-scoped**: it counts
Aave **collateral** (SUPPLY) but EXCLUDES the wallet balance and does NOT
subtract **debt** (BORROW).  That scope is correct for the dashboard but wrong
for the ``strat pnl`` headline on a leveraged-lending (looping) strategy:

* **Closed / torn down** — teardown returns the collateral to the *wallet*, so
  ``total_value_usd → 0``.  The headline becomes ``0 − initial`` ≈ ``−initial``
  — a confident, wrong −100%-of-capital figure even though the capital is
  sitting safely in the wallet (true cost ≈ gas).
* **Open** — ``total_value_usd`` includes the *re-supplied borrowed* collateral
  but the borrowed-principal liability is never netted, so taking on leverage
  manufactures a phantom positive gain.

This module is read-only detection (mirrors ``swap_class_fallback.py``):
given the latest ``PortfolioSnapshot`` and the ``transaction_ledger`` rows, it
decides whether the strategy is leveraged-lending and, if so, whether it is in
the **open** or **closed** state.  The renderer applies the verdict.

Three outcomes
--------------
* **open** — debt-netted lending NAV is available (live SUPPLY/BORROW positions
  in the snapshot).  ``strat pnl`` re-derives the headline from the debt-netted
  NAV instead of the positive-position-scoped ``total_value_usd`` so re-supplied
  borrowed collateral is not booked as profit (B-open).
* **closed** — leveraged historically, **and** the strategy's deployed value has
  genuinely collapsed back to the wallet (no live non-lending position of real
  value remains).  The debt-netted NAV is $0 and cannot recognise wallet-held
  value, so the headline is **suppressed** (shown ``unavailable``) until the
  scoped wallet/cash baseline lands (VIB-4976, design).  Better an honest "—"
  than a confident, wrong −$8.00 (B3).
* **none** — not leveraged, OR leveraged historically but still holding live
  non-lending value (e.g. a ``borrow → swap → LP`` carry whose LP leg is still
  open).  In that case ``total_value_usd`` is NOT ~0, the verbatim headline is
  meaningful, and we leave it untouched (the −initial artifact only arises when
  value genuinely returned to the wallet).

Why ledger-based detection matters
----------------------------------
A torn-down loop has **no lending positions in the latest snapshot**, so a
snapshot-only check would miss it and let the −100% artifact through.  The
strategy is recognised as leveraged-lending when *either* the snapshot carries
a live BORROW (debt) position *or* the ledger carries a historical successful
BORROW intent.  This keeps the suppression scoped to genuinely leveraged
strategies — a non-leveraged spot / LP strategy never borrowed, so its
headline is left untouched (regression guard).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.accounting.lending_nav import compute_lending_nav

# Lifecycle state of a leveraged-lending strategy as seen by ``strat pnl``.
_STATE_OPEN = "open"
_STATE_CLOSED = "closed"
_STATE_NONE = "none"

# Canonical ``PositionType`` string values (StrEnum) the detector reasons over.
# Exact equality — NOT ``endswith`` — so a future ``FLASH_BORROW`` / similar
# type can never silently match (mirrors ``compute_lending_nav``'s own style).
_BORROW = "BORROW"
_SUPPLY = "SUPPLY"
# Wallet pseudo-position (a TOKEN wrapper around a raw wallet balance).  A live
# TOKEN position is NOT deployed value — it is exactly the capital that returned
# to the wallet, so it must NOT count as "live non-lending value remains".
_WALLET_PSEUDO = "TOKEN"
# Lending legs net out via the debt-netted NAV; they are not "other" value.
_LENDING_TYPES = frozenset({_SUPPLY, _BORROW})


def _position_type_str(pos: Any) -> str | None:
    """Read ``pos.position_type`` as an upper-cased string, or ``None``.

    Defensive: a malformed legacy position that raises on attribute access can
    never crash the detector.
    """
    try:
        return str(pos.position_type).upper()
    except Exception:  # pragma: no cover - defensive
        return None


def _position_value(pos: Any) -> Decimal | None:
    """Read ``pos.value_usd`` as a ``Decimal``, or ``None`` when unmeasured.

    Empty≠Zero: an absent / unparsable ``value_usd`` is ``None`` (unmeasured),
    never coerced to ``Decimal("0")``.
    """
    raw = getattr(pos, "value_usd", None)
    if raw is None:
        return None
    if isinstance(raw, Decimal):
        return raw
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):  # pragma: no cover - defensive
        return None


def _snapshot_has_borrow_position(snapshot: Any) -> bool:
    """Return ``True`` iff the snapshot carries a live BORROW (debt) position.

    A live BORROW leg is the strongest "this strategy currently holds leverage"
    signal.
    """
    positions = getattr(snapshot, "positions", None) or []
    return any(_position_type_str(pos) == _BORROW for pos in positions)


def _snapshot_has_lending_position(snapshot: Any) -> bool:
    """Return ``True`` iff the snapshot carries any live SUPPLY or BORROW leg.

    Used to distinguish open (lending positions live) from closed (gone) for a
    strategy already known to be leveraged.
    """
    positions = getattr(snapshot, "positions", None) or []
    return any(_position_type_str(pos) in _LENDING_TYPES for pos in positions)


def _snapshot_has_live_non_lending_value(snapshot: Any) -> bool:
    """Return ``True`` iff a live non-lending position of real positive value remains.

    "Real value" excludes:

    * lending legs (SUPPLY/BORROW) — they net out via the debt-netted NAV and
      are handled by the OPEN path, not "other" value;
    * wallet pseudo-positions (TOKEN) — a TOKEN leg IS the capital that returned
      to the wallet, so counting it here would defeat the whole point of the
      closed-state suppression.

    A live LP / VAULT / PERP / STAKE / PREDICTION position with a measured
    positive ``value_usd`` means the strategy's deployed value did NOT collapse
    to the wallet — ``total_value_usd`` is meaningfully non-zero and the
    verbatim headline is correct.  In that case we must NOT suppress.

    Empty≠Zero: an unmeasured ``value_usd`` does not count as "real value"
    (we cannot assert the strategy still holds value we could not measure); the
    decision then falls through to the genuine-collapse suppression, which is
    the conservative honest-"unavailable" outcome.
    """
    positions = getattr(snapshot, "positions", None) or []
    for pos in positions:
        ptype = _position_type_str(pos)
        if ptype is None or ptype in _LENDING_TYPES or ptype == _WALLET_PSEUDO:
            continue
        value = _position_value(pos)
        if value is not None and value > 0:
            return True
    return False


def _count_unmeasured_borrow_legs(snapshot: Any) -> int:
    """Count live BORROW legs whose ``value_usd`` is unmeasured (``None``).

    ``compute_lending_nav`` silently skips such legs from ``gross_debt`` (it
    logs at INFO but the returned summary cannot be distinguished from a
    fully-measured one).  An unmeasured borrow leg makes the debt-netted NAV
    *overstate* (debt under-counted) → the open headline would over-report — the
    exact phantom-gain failure this PR exists to kill.  We count them here so
    the OPEN path can route to honest suppression instead (Empty≠Zero).
    """
    positions = getattr(snapshot, "positions", None) or []
    n = 0
    for pos in positions:
        if _position_type_str(pos) == _BORROW and _position_value(pos) is None:
            n += 1
    return n


def _ledger_has_successful_borrow(ledger_entries: list[Any] | None) -> bool:
    """Return ``True`` iff some successful BORROW intent is in the ledger.

    "Successful" is strict identity (``entry.success is True``) so a malformed
    truthy non-bool row can never silently upgrade a non-leveraged strategy
    into the leveraged path (Empty≠Zero at the read site).  A historical
    BORROW is what proves a *torn-down* loop was leveraged even though the
    latest snapshot has no lending positions left.

    A ``None`` / empty ledger means no rows to inspect → no historical borrow.
    Guarded explicitly so a caller passing ``None`` can't raise ``TypeError``.
    """
    if not ledger_entries:
        return False
    for entry in ledger_entries:
        if getattr(entry, "success", None) is not True:
            continue
        intent_type = (getattr(entry, "intent_type", "") or "").upper()
        if intent_type == _BORROW:
            return True
    return False


@dataclass(frozen=True)
class LeveragedLendingVerdict:
    """Verdict from :func:`detect_leveraged_lending`.

    Attributes:
        is_leveraged_lending:
            ``True`` when the strategy borrowed at any point AND the verdict
            actually scopes the headline (open or closed).  ``False`` for
            non-leveraged strategies, and also for the leveraged-but-still-holds
            -live-non-lending-value case (state ``"none"``) — in both the
            headline is left exactly as ``PortfolioMetrics`` produced it.
        state:
            ``"open"`` (live SUPPLY/BORROW positions → derive headline from
            debt-netted NAV), ``"closed"`` (borrowed historically, deployed
            value genuinely collapsed to the wallet → suppress headline), or
            ``"none"`` (not leveraged, or leveraged but real live non-lending
            value remains so the verbatim headline stands).
        net_lending_nav_usd:
            Debt-netted lending NAV from the snapshot (Σ SUPPLY value −
            Σ |BORROW value|).  Populated only for the ``"open"`` state; the
            B-open headline is ``net_lending_nav_usd − initial − deposits +
            withdrawals`` (NOT the NAV itself — see the Codex note in the
            ticket), and is derived only when ``initial`` and BOTH flows are
            measured (VIB-5866).  ``None`` otherwise (unmeasured / not
            applicable).
        reason:
            Single-line plain-English explanation for the closed-state
            suppression notice.  Empty string when the headline is not
            suppressed.
    """

    is_leveraged_lending: bool
    state: str
    net_lending_nav_usd: Decimal | None
    reason: str


# Suppression reasons — single-line, operator-facing.
_REASON_COLLAPSED = (
    "leveraged-lending strategy with no live lending positions and no other "
    "deployed value — the collateral has been recovered to the wallet, so the "
    "positive-position-scoped total_value_usd (VIB-3614) collapses to ~0 and "
    "the headline would read ≈ −initial (a false −100%). Capital returned to "
    "the wallet is not yet recognised as held value; that requires a scoped "
    "wallet/cash baseline (VIB-4976). Headline suppressed rather than shown "
    "wrong (VIB-4975)."
)
_REASON_UNMEASURED_DEBT = (
    "leveraged-lending strategy with a live BORROW leg whose value_usd is "
    "unmeasured — the debt-netted NAV would under-count the debt and re-report "
    "a phantom leverage gain (Empty≠Zero). Headline suppressed rather than "
    "shown over-reported (VIB-4975)."
)


def detect_leveraged_lending(
    snapshot: Any,
    ledger_entries: list[Any] | None,
) -> LeveragedLendingVerdict:
    """Classify a strategy's leveraged-lending state for the ``strat pnl`` headline.

    Args:
        snapshot: The latest ``PortfolioSnapshot`` (or ``None``).
        ledger_entries: ``LedgerEntry`` rows for the deployment (any order), or
            ``None`` / empty when there are no rows.

    Returns:
        :class:`LeveragedLendingVerdict`.  ``is_leveraged_lending=False`` (state
        ``"none"``) for any strategy that never borrowed, AND for a leveraged
        strategy that still holds live non-lending value — both keep the
        verbatim ``PortfolioMetrics`` headline.
    """
    has_live_borrow = _snapshot_has_borrow_position(snapshot)
    has_historical_borrow = _ledger_has_successful_borrow(ledger_entries)

    # Not leveraged → leave the headline exactly as-is (regression guard).
    if not has_live_borrow and not has_historical_borrow:
        return LeveragedLendingVerdict(False, _STATE_NONE, None, "")

    # OPEN: the snapshot still carries live SUPPLY/BORROW legs, so the
    # debt-netted lending NAV is meaningful — derive the headline from it.
    if _snapshot_has_lending_position(snapshot):
        # Empty≠Zero guard: an unmeasured BORROW leg makes compute_lending_nav
        # under-count the debt → the netted NAV overstates → phantom gain. Route
        # to honest suppression instead of a confident over-reported number.
        if _count_unmeasured_borrow_legs(snapshot) > 0:
            return LeveragedLendingVerdict(True, _STATE_CLOSED, None, _REASON_UNMEASURED_DEBT)
        try:
            nav = compute_lending_nav(snapshot)
        except Exception:  # pragma: no cover - compute_lending_nav is shape-tolerant
            nav = None
        if nav is not None:
            net_nav = nav.gross_supply_value_usd - nav.gross_debt_value_usd
            return LeveragedLendingVerdict(True, _STATE_OPEN, net_nav, "")

    # No live lending NAV to net.  Before suppressing, check whether the
    # strategy still holds live NON-lending value (e.g. a borrow→swap→LP carry
    # whose LP leg is still open).  If so, total_value_usd is genuinely
    # non-zero and the verbatim headline is meaningful — leave it untouched.
    # The −initial artifact only arises when deployed value truly collapsed
    # back to the wallet.
    if _snapshot_has_live_non_lending_value(snapshot):
        return LeveragedLendingVerdict(False, _STATE_NONE, None, "")

    # CLOSED: leveraged historically (or a live borrow with no SUPPLY, an
    # inconsistent shape we refuse to value) and deployed value genuinely
    # collapsed to the wallet.  Suppress until the scoped wallet/cash baseline
    # lands (VIB-4976).
    return LeveragedLendingVerdict(True, _STATE_CLOSED, None, _REASON_COLLAPSED)
