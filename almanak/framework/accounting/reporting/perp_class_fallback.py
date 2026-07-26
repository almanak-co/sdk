"""Closed perp-primitive headline suppression for ``strat pnl`` (VIB-5952).

The perp sibling of the leveraged-lending *closed* case (VIB-4975,
``leveraged_lending.py``) and the swap-primitive *closed* case (VIB-5788,
``swap_class_fallback.py``).  Same cascade, third primitive:

A perpetuals strategy (``gmx_v2`` et al.) deploys capital by committing
**collateral** into a PERP_OPEN order, then on close/teardown the position is
settled and the collateral is returned to the *wallet*.  Under VIB-3614
``total_value_usd`` is positive-**position**-scoped, so once the perp position
is closed it collapses to ~0, while the lifecycle baseline
``initial_value_usd`` still reflects the committed collateral.  The verbatim
headline (``total - initial - deposits + withdrawals``) then reads ~ ``-initial``
— a confident, wrong near-total loss even though the collateral is sitting
safely back in the wallet (true cost ~ gas + funding + price drift).

The perp-specific twist (the reason VIB-5866's unmeasured-flow guard does not
always catch this): on GMX the settlement transfer that returns the collateral
is executed by the protocol **keeper**, in a transaction OUTSIDE the strategy's
own tx set.  The capital-flow classifier cannot attribute that inflow, so it is
flagged ``unclassified`` and the committed-collateral outflow is left un-netted.
When the classifier marks the flows *unmeasured* (``None``), the upstream
``_populate_gross_net_pnl`` already leaves the headline unavailable (Empty≠Zero,
VIB-5866) — but when the flows are a *measured* zero (the pre-settlement /
inside-txset case, or a venue whose settlement is not keeper-mediated) the
verbatim headline computes to ~ ``-initial`` and renders as a confident-wrong
near-total loss.  On the real 20260722-0913-gmx-perp-avalanche run this printed
net_pnl_usd = -$6.00 for a $6 collateral round trip whose true wallet PnL was
-$0.025.

The two existing detectors miss this shape:
  * ``leveraged_lending.detect_leveraged_lending`` requires a BORROW leg — a
    perp strategy commits collateral, it does not borrow, so it returns
    state ``none``.
  * ``swap_class_fallback.detect_closed_swap_primitive`` requires a successful
    SWAP in the ledger — a pure perp deployment (PERP_OPEN / PERP_CLOSE, no
    spot swap) never trips it.

Recovering the true near-zero number needs a **strategy-attributed** wallet
baseline (the ambient ``wallet_total_value_usd`` / ``available_cash_usd`` are
shared across every deployment on the wallet and carry no per-strategy
attribution — a read-side sum was BUILT AND REJECTED as unsound, see
``docs/internal/accounting/VIB-4976-scoped-wallet-cash-lifecycle-pnl-design.md``
section 7b).  Until that schema-gated baseline lands (VIB-4927), the correct
behaviour — identical to VIB-4975 / VIB-5788 — is to **suppress** the headline:
an honest ``unavailable`` over a confident wrong number.  Empty≠Zero is HARD LAW
here (blueprint 27 §10.10): an unmeasured ``total_value_usd`` never fires this
suppression (there is no confident-wrong number to suppress — the upstream path
already left it unavailable), and a fabricated number is never substituted.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

# A live position whose type is NOT the wallet pseudo-position (TOKEN) means the
# strategy still holds genuinely deployed value (an LP/VAULT/PERP/SUPPLY/... leg),
# so ``total_value_usd`` is meaningfully non-zero and the headline is NOT the
# -initial artifact — do not suppress in that case.  A live PERP leg in
# particular means the position is still open, so the headline stands.
_WALLET_PSEUDO_TYPE = "TOKEN"

# Ledger intent types that identify a perp-primitive deployment.  Either a
# successful open or a successful close is sufficient evidence that this
# deployment traded perps (a close with no recorded open still proves the
# primitive; an open with no close is caught by the live-position rule below).
_PERP_INTENTS = frozenset({"PERP_OPEN", "PERP_CLOSE"})

# A borrow routes the deployment to the leveraged-lending path
# (``leveraged_lending.py``), never here — mirrors the swap detector's guard.
_BORROW = "BORROW"

# The deployed-value-collapse threshold: the position-scoped ``total_value_usd``
# has fallen below 1% of the lifecycle baseline, i.e. essentially all of the
# committed collateral has returned to the wallet.  A relative floor (not an
# absolute one) keeps the detection scale-free across a $6 pool wallet and a
# $10M deployment alike.  Matches ``swap_class_fallback._COLLAPSE_FRACTION``.
_COLLAPSE_FRACTION = Decimal("0.01")


@dataclass(frozen=True)
class ClosedPerpPrimitiveDetection:
    """Verdict from :func:`detect_closed_perp_primitive`.

    Attributes:
        suppressed:
            ``True`` when the closed perp-primitive fingerprint holds and
            ``strat pnl`` should suppress the ~ ``-initial`` headline.
        reason:
            Single-line operator-facing explanation, empty when not suppressed.
    """

    suppressed: bool
    reason: str


_CLOSED_PERP_REASON = (
    "perp-primitive strategy fully closed to the wallet: the committed "
    "collateral was returned to wallet cash on close/teardown, so the "
    "positive-position-scoped total_value_usd (VIB-3614) collapsed to ~0 while "
    "the lifecycle baseline initial_value_usd still reflects the committed "
    "collateral - the verbatim headline would read ~ -initial (a false "
    "near-total loss) even though the collateral is in the wallet (true cost ~ "
    "gas + funding). On GMX the settlement transfer is keeper-executed outside "
    "the strategy's tx set, so the capital-flow classifier cannot net the "
    "returned collateral against the committed-collateral outflow. Recognising "
    "the recovered wallet cash needs a strategy-attributed wallet baseline "
    "(VIB-4976 design / VIB-4927, schema-gated); the ambient wallet total is "
    "not per-strategy attributable, so a read-side number would be confidently "
    "wrong. Headline suppressed rather than shown wrong (VIB-5952; mirrors the "
    "VIB-4975 leveraged-lending and VIB-5788 swap-primitive closed states)."
)


def _snapshot_attr(obj: Any, name: str) -> Any:
    """Read ``obj.name`` defensively, returning ``None`` on any access error."""
    try:
        return getattr(obj, name, None)
    except Exception:  # pragma: no cover - defensive
        return None


def _to_decimal_or_none(raw: Any) -> Decimal | None:
    """Parse ``raw`` to a ``Decimal``, or ``None`` when unmeasured/unparsable.

    Empty≠Zero (blueprint 27 §10.10): an absent / ``None`` / unparsable / empty
    value is ``None`` (unmeasured), never coerced to ``Decimal("0")``.
    """
    if raw is None:
        return None
    candidate = raw if isinstance(raw, Decimal) else None
    if candidate is None:
        text = str(raw).strip()
        if not text:
            return None
        try:
            candidate = Decimal(text)
        except (InvalidOperation, ValueError, TypeError):
            return None
    # A non-finite Decimal (NaN / ±Infinity) would raise InvalidOperation the
    # moment it is compared downstream — treat it as unmeasured (Empty≠Zero).
    if not candidate.is_finite():
        return None
    return candidate


def _ledger_has_successful_intent(ledger_entries: list[Any] | None, intent_types: frozenset[str]) -> bool:
    """Return ``True`` iff a successful ledger entry of any ``intent_types`` exists.

    "Successful" is strict identity (``entry.success is True``) so a malformed
    truthy non-bool row can never silently upgrade the classification
    (Empty≠Zero at the read site). ``None`` / empty ledger -> ``False``.
    """
    if not ledger_entries:
        return False
    for entry in ledger_entries:
        if _snapshot_attr(entry, "success") is not True:
            continue
        # str() the row's intent_type: it may be a non-str (enum / int / None);
        # `(x or "").upper()` still raises AttributeError on a truthy non-str.
        if str(_snapshot_attr(entry, "intent_type") or "").upper() in intent_types:
            return True
    return False


def _snapshot_has_live_deployed_value(snapshot: Any) -> bool:
    """Return ``True`` iff a live NON-wallet position of positive value remains.

    Wallet pseudo-positions (``TOKEN``) ARE the collateral that returned to the
    wallet, so they must NOT count as "still deployed". Any other live position
    type (PERP / LP / VAULT / SUPPLY / BORROW / STAKE / ...) with a measured
    positive ``value_usd`` means the strategy's deployed value did NOT collapse
    to the wallet — a live PERP leg in particular means the position is still
    open, so ``total_value_usd`` is genuinely non-zero and the verbatim headline
    is meaningful.  We must not suppress.

    Empty≠Zero: an unmeasured ``value_usd`` does not count as "live value" (we
    cannot assert value we could not measure); the decision then falls through
    to suppression, the conservative honest-``unavailable`` outcome.
    """
    positions = _snapshot_attr(snapshot, "positions") or []
    for pos in positions:
        try:
            ptype = str(pos.position_type).upper()
        except Exception:  # pragma: no cover - defensive
            continue
        if ptype == _WALLET_PSEUDO_TYPE:
            continue
        value = _to_decimal_or_none(_snapshot_attr(pos, "value_usd"))
        if value is not None and value > 0:
            return True
    return False


def _wallet_retains_value(snapshot: Any) -> bool:
    """Return ``True`` iff the wallet still holds measured value post-close.

    Distinguishes "closed into the wallet" (the -initial artifact this
    suppression targets) from a genuine wipe-to-zero, where the loss is real and
    the headline should stand. Uses the wallet-inclusive snapshot fields, which
    are fine as a *presence* check here — the unsoundness proven in VIB-4976 is
    about *attributing a strategy's share* of a shared wallet, not about whether
    the wallet holds anything at all.
    """
    for field_name in ("wallet_total_value_usd", "available_cash_usd"):
        value = _to_decimal_or_none(_snapshot_attr(snapshot, field_name))
        if value is not None and value > 0:
            return True
    return False


def detect_closed_perp_primitive(
    snapshot: Any,
    ledger_entries: list[Any] | None,
    metrics: Any,
) -> ClosedPerpPrimitiveDetection:
    """Detect a fully-closed perp-primitive deployment (VIB-5952).

    Fires (suppress) when ALL hold:

    1. ``metrics`` carries a positive baseline (``initial_value_usd`` > 0) and a
       MEASURED ``total_value_usd`` that has collapsed below
       ``_COLLAPSE_FRACTION`` of that baseline (committed collateral returned to
       the wallet). An unmeasured ``total_value_usd`` (``None``) does not fire —
       the upstream headline is already left unmeasured (Empty≠Zero), so there
       is no confident-wrong number to suppress.
    2. The deployment is perp-primitive and non-leveraged-lending: >=1
       successful PERP_OPEN or PERP_CLOSE in the ledger, and NO successful
       BORROW (a borrowed deployment is the leveraged-lending path's job —
       ``leveraged_lending.py``).
    3. No live non-wallet position of positive value remains in the snapshot (a
       still-open PERP leg means the position was not closed — do not suppress).
    4. The wallet still holds measured value (closed-into-wallet, not a genuine
       wipe-to-zero).

    Args:
        snapshot: The latest ``PortfolioSnapshot`` (or ``None``).
        ledger_entries: ``LedgerEntry`` rows for the deployment (any order), or
            ``None`` / empty.
        metrics: The ``PortfolioMetrics`` row whose headline is at risk.

    Returns:
        :class:`ClosedPerpPrimitiveDetection`.
    """
    if metrics is None:
        return ClosedPerpPrimitiveDetection(False, "")

    initial = _to_decimal_or_none(_snapshot_attr(metrics, "initial_value_usd"))
    total = _to_decimal_or_none(_snapshot_attr(metrics, "total_value_usd"))
    # Rule 1 — positive baseline and a measured, collapsed current value.
    if initial is None or initial <= 0 or total is None:
        return ClosedPerpPrimitiveDetection(False, "")
    if total >= initial * _COLLAPSE_FRACTION:
        return ClosedPerpPrimitiveDetection(False, "")

    # Rule 2 — perp-primitive, non-leveraged-lending.
    if not _ledger_has_successful_intent(ledger_entries, _PERP_INTENTS):
        return ClosedPerpPrimitiveDetection(False, "")
    if _ledger_has_successful_intent(ledger_entries, frozenset({_BORROW})):
        return ClosedPerpPrimitiveDetection(False, "")

    # Rule 3 — no genuinely-deployed (non-wallet) value remains (position closed).
    if _snapshot_has_live_deployed_value(snapshot):
        return ClosedPerpPrimitiveDetection(False, "")

    # Rule 4 — collateral returned to the wallet (not a real wipe-to-zero).
    if not _wallet_retains_value(snapshot):
        return ClosedPerpPrimitiveDetection(False, "")

    return ClosedPerpPrimitiveDetection(True, _CLOSED_PERP_REASON)
