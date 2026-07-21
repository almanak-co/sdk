"""VIB-5865 — the ``WalletDeltaLane`` taxonomy seam (completeness + disjointness).

The teardown swap-back clamp reconstructs "how much of this balance is provably
ours" from history. Before this seam a primitive was visible to that
reconstruction iff someone had remembered to add it to
``accounting.basis._REPLAY_DISPATCH``; every other wallet-moving verb was
SILENTLY invisible and its proceeds stranded (``untracked_token`` /
``degraded=False``).

These tests are the structural guards that make that impossible to re-introduce:

  1. ``PrimitiveRecord`` has NO default for ``wallet_delta`` → a row added
     without a reviewed declaration fails at IMPORT with a ``TypeError``.
  2. Lane / dispatch disjointness — ``EVENT_REPLAY`` iff a replay handler exists.
  3. ``LEDGER_PROJECTION`` == the ``NO_ACCOUNTING`` category set, i.e. the
     generalized ``basis._is_ledger_projected_row`` predicate is EXACTLY the old
     category-based predicate on every taxonomy row (behaviour-preserving by
     construction in this PR).
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.basis import _REPLAY_DISPATCH, _is_ledger_projected_row
from almanak.framework.primitives.taxonomy import TAXONOMY
from almanak.framework.primitives.types import (
    AccountingCategory,
    EventKind,
    LifecyclePhase,
    Primitive,
    PrimitiveRecord,
    WalletDeltaLane,
)

# Dispatch keys that are deliberately NOT taxonomy rows: ``WALLET_MOVEMENT`` is
# the EPHEMERAL synthetic event type emitted only by the clamp's ledger
# projection (``basis.synthetic_wallet_movement_events``) and never persisted, so
# it has no intent/accounting-event identity to declare a lane for.
_SYNTHETIC_DISPATCH_KEYS = {"WALLET_MOVEMENT"}


def _rows_in_lane(lane: WalletDeltaLane) -> set[str]:
    return {name for name, rec in TAXONOMY.items() if rec.wallet_delta is lane}


# ---------------------------------------------------------------------------
# 1. Declaration is mandatory (import-time enforcement)
# ---------------------------------------------------------------------------


def test_primitive_record_requires_wallet_delta() -> None:
    """Constructing a record without ``wallet_delta`` must raise ``TypeError``.

    This is the whole point of the seam: the taxonomy table is built at import
    time, so an undeclared row cannot reach production — it cannot even be
    imported.
    """
    kwargs = {
        "intent_type": "SWAP",
        "primitive": Primitive.SWAP,
        "accounting_category": AccountingCategory.SWAP,
        "position_type": None,
        "event_kind": EventKind.NONE,
        "is_async": False,
        "lifecycle_phase": LifecyclePhase.ATOMIC,
        "required_lifecycle": (),
    }
    with pytest.raises(TypeError):
        PrimitiveRecord(**kwargs)  # type: ignore[arg-type]
    # …and the same kwargs WITH the declaration construct fine.
    assert PrimitiveRecord(**kwargs, wallet_delta=WalletDeltaLane.EVENT_REPLAY).wallet_delta is (
        WalletDeltaLane.EVENT_REPLAY
    )


def test_every_taxonomy_row_declares_a_lane() -> None:
    """No row may be missing / mistyped its declaration."""
    assert TAXONOMY, "taxonomy must not be empty"
    for name, rec in TAXONOMY.items():
        assert isinstance(rec.wallet_delta, WalletDeltaLane), f"{name} has no WalletDeltaLane"


# ---------------------------------------------------------------------------
# 2. Lane / replay-dispatch disjointness
# ---------------------------------------------------------------------------


def test_event_replay_rows_have_a_replay_handler() -> None:
    """A row declaring EVENT_REPLAY MUST be folded by a ``_REPLAY_DISPATCH`` handler.

    MUTATION CHECK: deleting any entry from ``_REPLAY_DISPATCH`` (e.g. ``SWAP``)
    fails this test — the declaration would then claim a measured lane that the
    fold cannot deliver, which is exactly the silent-strand shape VIB-5865 exists
    to prevent.
    """
    missing = sorted(_rows_in_lane(WalletDeltaLane.EVENT_REPLAY) - set(_REPLAY_DISPATCH))
    assert not missing, f"declared EVENT_REPLAY but no replay handler: {missing}"


def test_non_replay_rows_have_no_replay_handler() -> None:
    """NONE / LEDGER_PROJECTION / UNMEASURED rows must NOT be in the dispatch table.

    MUTATION CHECK: flipping any NONE row's declaration to EVENT_REPLAY, or
    adding a dispatch entry for an UNMEASURED row without moving its
    declaration, fails one of these two tests — the lanes cannot drift apart.
    """
    non_replay = (
        _rows_in_lane(WalletDeltaLane.NONE)
        | _rows_in_lane(WalletDeltaLane.LEDGER_PROJECTION)
        | _rows_in_lane(WalletDeltaLane.UNMEASURED)
    )
    overlap = sorted(non_replay & set(_REPLAY_DISPATCH))
    assert not overlap, f"declared non-replay but a replay handler exists: {overlap}"


def test_replay_dispatch_keys_are_declared_or_synthetic() -> None:
    """Every dispatch key is either an EVENT_REPLAY taxonomy row or a known synthetic type."""
    undeclared = sorted(set(_REPLAY_DISPATCH) - set(TAXONOMY) - _SYNTHETIC_DISPATCH_KEYS)
    assert not undeclared, f"replay handler for an unknown/undeclared type: {undeclared}"


def test_lanes_partition_the_taxonomy() -> None:
    """Every row lands in exactly one lane and every lane is exercised."""
    total = sum(len(_rows_in_lane(lane)) for lane in WalletDeltaLane)
    assert total == len(TAXONOMY)
    for lane in WalletDeltaLane:
        assert _rows_in_lane(lane), f"lane {lane} has no rows — declarations drifted"


# ---------------------------------------------------------------------------
# 3. Ledger-projection predicate parity (behaviour-preserving BY CONSTRUCTION)
# ---------------------------------------------------------------------------


def test_ledger_projection_lane_equals_no_accounting_category() -> None:
    """The declared LEDGER_PROJECTION set is EXACTLY the NO_ACCOUNTING set.

    PR-1 changes no measured-lane mechanics: generalising the predicate from
    "category is NO_ACCOUNTING" to "declaration is LEDGER_PROJECTION" is a no-op
    only while these two sets coincide. A future PR that moves a row between
    lanes MUST update this test deliberately (and carry its own evidence).
    """
    by_category = {
        name for name, rec in TAXONOMY.items() if rec.accounting_category is AccountingCategory.NO_ACCOUNTING
    }
    assert _rows_in_lane(WalletDeltaLane.LEDGER_PROJECTION) == by_category


def test_ledger_predicate_matches_old_category_predicate_on_every_row() -> None:
    """Whole-taxonomy parity of the OLD (category) vs NEW (lane) ledger predicate.

    Drives the real production predicate over a synthetic SUCCESSFUL ledger row
    for every intent string in the table, and compares against the pre-VIB-5865
    category check. Any divergence is a behaviour change in a teardown
    fund-safety lane.
    """
    for name, rec in TAXONOMY.items():
        row = {"success": True, "intent_type": name, "token_in": "WETH", "token_out": "USDC"}
        old = rec.accounting_category is AccountingCategory.NO_ACCOUNTING
        assert _is_ledger_projected_row(row) is old, f"{name}: predicate diverged"


def test_ledger_predicate_rejects_failed_and_unknown_rows() -> None:
    """Unchanged guards: a failed tx moved nothing; an unknown intent is not our lane."""
    assert _is_ledger_projected_row({"success": False, "intent_type": "STAKE"}) is False
    assert _is_ledger_projected_row({"success": True, "intent_type": "NOT_A_REAL_INTENT"}) is False
    assert _is_ledger_projected_row({"success": True, "intent_type": ""}) is False


# ---------------------------------------------------------------------------
# 4. Reviewed judgment calls — pinned so a silent flip is caught
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["LP_SNAPSHOT", "VAULT_SNAPSHOT", "LIQUIDATION_RISK_UPDATE", "CLOSE"])
def test_declared_no_wallet_movement_rows(name: str) -> None:
    """The four reviewed NONE rows (each carries a justification comment in the table).

    ``CLOSE`` is the lending aggregate marker: its fungible movement is carried by
    the constituent WITHDRAW / REPAY events (EVENT_REPLAY), so declaring it
    UNMEASURED would poison tokens those measured legs just reconstructed.
    """
    assert TAXONOMY[name].wallet_delta is WalletDeltaLane.NONE


@pytest.mark.parametrize(
    "name",
    [
        "VAULT_REDEEM",
        "PERP_CLOSE",
        "BRIDGE",
        "TRANSFER",
        "SETTLE_REDEEM",
        "LP_REBALANCE",
        "PENDLE_LP_CLOSE",
        "PREDICTION_BUY",
        "PREDICTION_SELL",
    ],
)
def test_known_blind_primitives_are_declared_unmeasured(name: str) -> None:
    """The primitives the clamp is still blind to must fail CLOSED, not silently.

    VIB-5865 PR-2 moved LP_OPEN / LP_CLOSE / LP_COLLECT_FEES OUT of this list
    into EVENT_REPLAY (they now have a real measured fold — see
    ``test_lp_replay_fold_vib5865.py``). ``LP_REBALANCE`` stays UNMEASURED: the
    event type is reserved and no handler emits it, so there is no payload shape
    to fold. ``PENDLE_LP_*`` stays until its connector-owned extractor lands.

    VIB-5865 PR-4 (evidence-pinned, see
    ``tests/reports/vib5865-pr4-transfer-prediction-evidence.md``): BRIDGE /
    TRANSFER and PREDICTION_BUY / PREDICTION_SELL stay UNMEASURED —
    * BRIDGE / TRANSFER: the ledger ``amount_in`` (and the
      ``TransferAccountingEvent.amount`` derived from it) is INTENT-requested,
      not receipt-measured; the measured ``amount_sent`` lives only in
      ``bridge_data`` / ``extracted_data_json``. Folding a requested amount is
      the anti-pattern the LP trace disproved — no guessing on a fund-safety lane.
    * PREDICTION_BUY / SELL: the measured fill (shares / cost_basis / proceeds)
      lives in ``extracted_data_json``; the ledger token/amount columns are EMPTY,
      so a LEDGER_PROJECTION flip would project nothing AND would break the
      LEDGER_PROJECTION == NO_ACCOUNTING parity invariants (category is
      PREDICTION). A real fold needs a ``_replay_prediction`` handler (follow-up).
    This test is the guard that a future silent / guess flip is caught.
    """
    assert TAXONOMY[name].wallet_delta is WalletDeltaLane.UNMEASURED


@pytest.mark.parametrize("name", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
def test_lp_family_is_measured_by_replay(name: str) -> None:
    """VIB-5865 PR-2: the LP family folds through ``_replay_lp`` (the headline fix)."""
    assert TAXONOMY[name].wallet_delta is WalletDeltaLane.EVENT_REPLAY
