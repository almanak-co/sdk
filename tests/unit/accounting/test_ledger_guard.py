"""Write-time Empty != Zero guard for transaction_ledger (VIB-6043 leg 2).

The rule: a ledger row must never claim a clean success while its measured
money columns are empty. The guard downgrades and marks such a row instead of
rejecting it — rejecting would manufacture the other half of VIB-6043 (money
on-chain, zero rows).
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal

import pytest

from almanak.framework.accounting.ledger_guard import (
    DEGRADED_PREFIX,
    REQUIRED_MONEY_SLOTS,
    LedgerDegradationReason,
    SlotPolicy,
    apply_degradation,
    classify_ledger_row,
    is_degraded,
)
from almanak.framework.observability.ledger import LedgerEntry


def swap_row(**overrides) -> LedgerEntry:
    base = {
        "intent_type": "SWAP",
        "success": True,
        "tx_hash": "0x" + "ab" * 32,
        "chain": "arbitrum",
        "protocol": "pancakeswap_v3",
        "token_in": "USDC",
        "token_out": "WETH",
        "amount_in": "50",
        "amount_out": "0.0265",
        "gas_used": 263_322,
    }
    base.update(overrides)
    return LedgerEntry(**base)


#: Two distinct 32-byte hashes, in the shape ``transaction_ledger.tx_hash``
#: actually holds (``0x`` + 64 hex chars — see ``observability.ledger``, which
#: copies the receipt's ``transactionHash``). Distinct so a test can tell which
#: row a query matched.
_HASH_OPEN = "0x" + "1a" * 32
_HASH_CLOSE = "0x" + "2b" * 32


def _lifecycle_ledger(rows: list[tuple[str, int, str, str]]):
    """An in-memory ``transaction_ledger`` for ``_assert_fixture_lifecycle``.

    Rows are ``(intent_type, success, error, tx_hash)`` and all share one
    deployment. ``tx_hash`` is a COLUMN here, not a decoration: the landed
    predicate's degraded arm requires it, and the real table has always had it
    — these fixtures simply had not projected it. A helper rather than three
    copies of the DDL, so the next column the predicate needs is added once.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE transaction_ledger "
        "(intent_type TEXT, success INTEGER, error TEXT, tx_hash TEXT, deployment_id TEXT)"
    )
    conn.executemany(
        "INSERT INTO transaction_ledger VALUES (?, ?, ?, ?, 'deployment:abc')",
        rows,
    )
    return conn


# --- what fires -------------------------------------------------------------


@pytest.mark.parametrize(
    "missing",
    [
        {"amount_out": ""},
        {"amount_in": ""},
        {"amount_in": "", "amount_out": ""},
        {"amount_out": "   "},
        {"amount_out": None},
    ],
)
def test_success_swap_row_with_an_unmeasured_amount_is_degraded(missing):
    """The exact PancakeSwap-under-Safe shape: tx hash + gas + empty amounts."""
    entry = swap_row(**missing)
    verdict = classify_ledger_row(entry)
    assert verdict is not None
    assert verdict.reason is LedgerDegradationReason.AMOUNTS_UNMEASURED


def test_degradation_marks_the_row_without_inventing_numbers():
    entry = swap_row(amount_out="")
    apply_degradation(entry, classify_ledger_row(entry))

    assert entry.success is False
    assert entry.error.startswith(DEGRADED_PREFIX)
    assert "amount_out" in entry.error
    # Chain reality is preserved — this is a books verdict, not a claim the tx failed.
    assert entry.tx_hash
    assert entry.gas_used == 263_322
    assert entry.intent_type == "SWAP"
    # Empty != Zero: the unmeasured column stays unmeasured. Never "0".
    assert entry.amount_out == ""
    assert entry.amount_in == "50"

    marker = json.loads(entry.extracted_data_json)["accounting_degradation"]
    assert marker["reason"] == "amounts_unmeasured"
    assert marker["columns"] == ["amount_out"]
    assert marker["chain_success"] is True


# --- what must NOT fire -----------------------------------------------------


def test_measured_zero_is_legal():
    """``Decimal("0")`` is a measurement. Rejecting it would be the mirror bug."""
    assert classify_ledger_row(swap_row(amount_in="0", amount_out="0")) is None
    assert classify_ledger_row(swap_row(amount_out="0.000000")) is None


def test_clean_row_is_untouched():
    assert classify_ledger_row(swap_row()) is None


def test_non_success_rows_are_not_double_marked():
    """Slippage / reconciliation / FAILED rows already carry their own verdict."""
    entry = swap_row(success=False, amount_out="", error="slippage breach at 2.0%")
    assert classify_ledger_row(entry) is None


def test_guard_is_idempotent():
    entry = swap_row(amount_out="")
    apply_degradation(entry, classify_ledger_row(entry))
    first_error = entry.error
    assert is_degraded(entry)
    # Re-classifying a marked row (retry, INSERT OR REPLACE) must be a no-op.
    assert classify_ledger_row(entry) is None
    assert entry.error == first_error


@pytest.mark.parametrize("intent_type", ["SETTLE_DEPOSIT", "SETTLE_REDEEM", "TRANSFER", "PERP_OPEN", ""])
def test_intent_types_without_a_money_slot_contract_are_untouched(intent_type):
    """Settlement rows legitimately book unmeasured amounts by design.

    Perp / lending stay out on purpose — per-primitive leg semantics belong to
    the PrimitiveMoneyLegs seam, not a hand-grown framework table. This table
    covers only the two shapes that have actually bitten us in production.
    """
    assert classify_ledger_row(swap_row(intent_type=intent_type, amount_out="")) is None


def test_intent_type_matching_is_case_insensitive():
    assert classify_ledger_row(swap_row(intent_type="swap", amount_out="")) is not None


# --- structure --------------------------------------------------------------


def test_existing_error_text_is_preserved():
    entry = swap_row(amount_out="", error="upstream note")
    apply_degradation(entry, classify_ledger_row(entry))
    assert entry.error.startswith(DEGRADED_PREFIX)
    assert "upstream note" in entry.error


def test_unparsable_extracted_data_is_kept_not_dropped():
    entry = swap_row(amount_out="", extracted_data_json="not-json{{")
    apply_degradation(entry, classify_ledger_row(entry))
    payload = json.loads(entry.extracted_data_json)
    assert payload["accounting_degradation"]["reason"] == "amounts_unmeasured"
    assert payload["_unparsed_extracted_data"].startswith("not-json")


def test_existing_extracted_data_keys_survive():
    entry = swap_row(amount_out="", extracted_data_json=json.dumps({"swap_amounts": {"a": 1}}))
    apply_degradation(entry, classify_ledger_row(entry))
    payload = json.loads(entry.extracted_data_json)
    assert payload["swap_amounts"] == {"a": 1}
    assert "accounting_degradation" in payload


def test_swap_contract_matches_the_accountant_test_invariant():
    """The guard and Accountant Test cell G1 must not drift apart.

    G1 fails a success row when ``intent_type == "SWAP"`` and either amount is
    blank — i.e. SWAP is an ALL contract over both amounts. If this table ever
    disagrees, one of the two silently stops being enforcement.
    """
    assert REQUIRED_MONEY_SLOTS["SWAP"] == (("amount_in", "amount_out"), SlotPolicy.ALL)


# --- LP rows: VIB-6051 (curve LP_CLOSE with ALL money columns empty) --------


def lp_row(intent_type="LP_CLOSE", **overrides) -> LedgerEntry:
    base = {
        "intent_type": intent_type,
        "success": True,
        "tx_hash": "0x" + "cd" * 32,
        "chain": "arbitrum",
        "protocol": "curve",
        "token_in": "USDC",
        "token_out": "USDT",
        "amount_in": "25",
        "amount_out": "25",
        "gas_used": 180_000,
    }
    base.update(overrides)
    return LedgerEntry(**base)


@pytest.mark.parametrize("intent_type", ["LP_CLOSE", "LP_OPEN"])
def test_lp_row_with_every_money_column_empty_is_degraded(intent_type):
    """VIB-6051: curve LP_CLOSE rows persisted with all four money columns
    empty while ``position_events`` booked the value correctly — the books
    disagreed with themselves about a position that really closed.
    """
    entry = lp_row(intent_type, token_in="", amount_in="", token_out="", amount_out="")
    verdict = classify_ledger_row(entry)
    assert verdict is not None
    assert verdict.reason is LedgerDegradationReason.AMOUNTS_UNMEASURED
    assert set(verdict.columns) == {"amount_in", "amount_out"}


@pytest.mark.parametrize("intent_type", ["LP_CLOSE", "LP_OPEN"])
@pytest.mark.parametrize("measured", [{"amount_in": ""}, {"amount_out": ""}])
def test_single_sided_lp_leg_is_legitimate_and_not_degraded(intent_type, measured):
    """ANY, not ALL — a one-sided add or a close returning a single token is
    a real shape, and degrading it would make the guard the bug.
    """
    assert classify_ledger_row(lp_row(intent_type, **measured)) is None


def test_lp_measured_zero_is_legal():
    assert classify_ledger_row(lp_row(amount_in="0", amount_out="0")) is None


def test_lp_contract_is_any_not_all():
    for intent_type in ("LP_OPEN", "LP_CLOSE"):
        slots, policy = REQUIRED_MONEY_SLOTS[intent_type]
        assert slots == ("amount_in", "amount_out")
        assert policy is SlotPolicy.ANY


# --- anti-laundering: the downgrade must not hide the defect from the audit ---


def test_accountant_test_matches_degraded_rows_via_the_shared_rule():
    """G1 matches degraded rows through ``ledger_guard``, not its own copy.

    This replaced a prefix-equality assertion. The Accountant Test no longer
    holds a prefix of its own to compare — it calls the shared ``degraded``
    rule — so identity of the delegated function is both the stronger check
    and the only one still meaningful.
    """
    from almanak.framework.accounting import accountant_test as at
    from almanak.framework.accounting.ledger_guard import degraded

    assert at._degraded_rule is degraded
    assert degraded(f"{DEGRADED_PREFIX}amounts_unmeasured: x") is True
    assert degraded("execution reverted") is False
    assert degraded(None) is False


def test_g1_fails_on_a_chain_landed_degraded_row():
    """Downgrading to success=False must NOT make cell G1 go green.

    Without this clause the degraded row drops out of G1's ``successful`` scan
    and the scorecard would report a clean money trail while a swap that moved
    real value has no measured amounts — the fix would have become a way to
    hide the bug.
    """
    from almanak.framework.accounting.accountant_test import _cell_g1_money_trail

    entry = swap_row(amount_out="")
    apply_degradation(entry, classify_ledger_row(entry))

    rows = [
        {
            "intent_type": entry.intent_type,
            "success": entry.success,
            "tx_hash": entry.tx_hash,
            "amount_in": entry.amount_in,
            "amount_out": entry.amount_out,
            "error": entry.error,
        }
    ]
    result = _cell_g1_money_trail(rows, [], {}, "lp")
    assert result.status == "FAIL"
    assert "degraded" in result.diagnostic.lower()
    assert entry.tx_hash in result.diagnostic


# ---------------------------------------------------------------------------
# Empty != Zero, for NON-STRING money values (VIB-6043 leg 2 review).
#
# ``_is_unmeasured`` previously read ``not str(value or "").strip()``. Every
# numeric zero is falsy, so ``value or ""`` collapsed ``Decimal("0")`` / ``0`` /
# ``0.0`` to ``""`` and reported a MEASURED zero as unmeasured — the exact
# mirror-image of the bug this module exists to prevent, and a violation of
# AGENTS.md §Accounting.
#
# Latent rather than live (callers currently stringify first), but the type
# hints are unenforced and ``build_ledger_entry`` produces
# ``Decimal | str | None``, so the predicate must be correct on its own terms.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "measured",
    [
        "0",
        "0.000000",
        "0E-18",
        Decimal("0"),
        Decimal("0.00"),
        Decimal("0E-18"),
        0,
        0.0,
    ],
    ids=repr,
)
def test_a_measured_zero_is_never_degraded(measured):
    """A zero someone actually measured is a measurement, whatever its type."""
    entry = swap_row(amount_in=measured, amount_out=measured)
    assert classify_ledger_row(entry) is None, f"measured zero {measured!r} was wrongly degraded"


@pytest.mark.parametrize("unmeasured", ["", "   ", "\t", "\n", None], ids=repr)
def test_every_unmeasured_spelling_still_fires(unmeasured):
    """The guard must not have been loosened into uselessness by the above."""
    entry = swap_row(amount_out=unmeasured)
    degradation = classify_ledger_row(entry)
    assert degradation is not None, f"unmeasured {unmeasured!r} escaped the guard"
    assert "amount_out" in degradation.columns


def test_a_measured_zero_does_not_launder_an_unmeasured_other_leg():
    """A measured zero on one leg must not excuse an unmeasured other leg."""
    entry = swap_row(amount_in=Decimal("0"), amount_out="")
    degradation = classify_ledger_row(entry)
    assert degradation is not None
    assert degradation.columns == ("amount_out",)


def test_g1_fails_on_a_degraded_row_that_has_no_tx_hash():
    """The laundering hole: degrading a hash-less row must not turn G1 green.

    ``classify_ledger_row`` fires on ANY success row with unmeasured money
    slots — it does not require a tx hash. G1's degraded clause was originally
    gated on ``tx_hash``, and the downgrade to ``success=False`` simultaneously
    removed the row from the ``successful`` scan that ``missing_hash`` reads.
    A row with success=True, no hash and no amounts — the Safe/bundle
    under-measured shape VIB-6043 exists for — therefore escaped BOTH clauses:
    it FAILED G1 before this PR and PASSED after it.

    A fix that makes the scorecard quieter than no fix at all is worse than
    no fix. This pins the regression.
    """
    from almanak.framework.accounting.accountant_test import _cell_g1_money_trail

    entry = swap_row(tx_hash="", amount_in="", amount_out="")
    apply_degradation(entry, classify_ledger_row(entry))
    assert entry.success is False, "precondition: the guard fired"

    rows = [
        {
            "intent_type": entry.intent_type,
            "success": entry.success,
            "tx_hash": entry.tx_hash,
            "amount_in": entry.amount_in,
            "amount_out": entry.amount_out,
            "error": entry.error,
        }
    ]
    result = _cell_g1_money_trail(rows, [], {}, "lp")
    assert result.status == "FAIL", (
        "a degraded row with no tx_hash escaped G1 — the downgrade laundered "
        "a defect the pre-guard scorecard caught"
    )


def test_g1_still_fails_the_hashless_row_before_the_guard_runs():
    """Baseline for the test above: this shape FAILS G1 on main today."""
    from almanak.framework.accounting.accountant_test import _cell_g1_money_trail

    rows = [
        {
            "intent_type": "SWAP",
            "success": True,
            "tx_hash": "",
            "amount_in": "",
            "amount_out": "",
            "error": "",
        }
    ]
    assert _cell_g1_money_trail(rows, [], {}, "lp").status == "FAIL"


def test_fixture_lifecycle_counts_a_degraded_step_as_having_happened():
    """A step that LANDED but was degraded still completed the round trip.

    ``_assert_fixture_lifecycle`` reads ``WHERE success=1``. A degraded
    ``LP_CLOSE`` — the VIB-6051 shape this guard targets — would drop out of
    ``actual`` and raise ``FixtureLifecycleError``, so NO cells score at all:
    the audit tool would abort on exactly the input it exists to measure, and
    G1 would never get the chance to report the real defect.

    The degraded ``LP_CLOSE`` carries a REAL hash, because the lane it stands
    for does: a Safe / bundle execution that confirmed on-chain and whose
    parser yielded no amounts. A degraded row *without* a hash is a different
    animal — a no-op — and is pinned separately below.
    """
    from almanak.framework.accounting.accountant_test import _assert_fixture_lifecycle
    from almanak.framework.primitives.types import Primitive

    conn = _lifecycle_ledger(
        [
            ("LP_OPEN", 1, "", _HASH_OPEN),
            # Landed, but the guard downgraded it — still a completed step.
            ("LP_CLOSE", 0, f"{DEGRADED_PREFIX}amounts_unmeasured: intent=LP_CLOSE", _HASH_CLOSE),
        ]
    )

    # Must not raise — both scoped and unscoped forms.
    _assert_fixture_lifecycle(conn, Primitive.LP)
    _assert_fixture_lifecycle(conn, Primitive.LP, deployment_id="deployment:abc")


def test_a_hashless_degraded_step_did_not_complete_the_lifecycle():
    """The NO-OP: the marker without a hash is not a completed round trip.

    ``apply_degradation`` stamps ``chain_success: True`` by construction — it
    never consults a receipt — so the marker alone cannot stand in for one. An
    ``LP_CLOSE`` against an already-empty position compiles to
    ``transactions=[]`` / ``metadata["no_op"]=True``, the orchestrator books
    ``result.success=True``, the runner writes ``tx_hash=""``, and the absent
    receipt leaves both money slots unmeasured — so the guard marks it. Reading
    that as a completed close makes ``_assert_fixture_lifecycle`` certify a
    round trip that never closed anything.

    Same two rows as the test above, one field different. That is the whole
    delta, and it is the difference between "the parser lost the numbers" and
    "no transaction was ever submitted".
    """
    from almanak.framework.accounting.accountant_test import (
        FixtureLifecycleError,
        _assert_fixture_lifecycle,
    )
    from almanak.framework.primitives.types import Primitive

    conn = _lifecycle_ledger(
        [
            ("LP_OPEN", 1, "", _HASH_OPEN),
            ("LP_CLOSE", 0, f"{DEGRADED_PREFIX}amounts_unmeasured: intent=LP_CLOSE", ""),
        ]
    )

    with pytest.raises(FixtureLifecycleError):
        _assert_fixture_lifecycle(conn, Primitive.LP)
    with pytest.raises(FixtureLifecycleError):
        _assert_fixture_lifecycle(conn, Primitive.LP, deployment_id="deployment:abc")


def test_fixture_lifecycle_still_rejects_a_genuinely_absent_step():
    """The degraded arm must not turn the lifecycle guard into a rubber stamp."""
    from almanak.framework.accounting.accountant_test import (
        FixtureLifecycleError,
        _assert_fixture_lifecycle,
    )
    from almanak.framework.primitives.types import Primitive

    conn = _lifecycle_ledger([("LP_OPEN", 1, "", _HASH_OPEN)])

    with pytest.raises(FixtureLifecycleError):
        _assert_fixture_lifecycle(conn, Primitive.LP)


def test_a_reverted_row_is_not_counted_as_a_completed_lifecycle_step():
    """Only degraded rows get the new arm — a genuine failure still misses.

    The reverted row carries a hash: a transaction that reverted was still
    mined, so ``tx_hash`` is populated (the runner writes the receipt's hash
    with ``success=0``). Dropping the hash here would let the row be excluded
    for the *wrong* reason and stop discriminating the degraded arm at all.
    """
    from almanak.framework.accounting.accountant_test import (
        FixtureLifecycleError,
        _assert_fixture_lifecycle,
    )
    from almanak.framework.primitives.types import Primitive

    conn = _lifecycle_ledger(
        [
            ("LP_OPEN", 1, "", _HASH_OPEN),
            ("LP_CLOSE", 0, "execution reverted", _HASH_CLOSE),
        ]
    )

    with pytest.raises(FixtureLifecycleError):
        _assert_fixture_lifecycle(conn, Primitive.LP)


def test_landed_readers_delegate_to_the_one_rule_rather_than_mirroring_it():
    """Stronger than "the mirrors agree": there are no mirrors.

    This replaces a test that asserted the reporting path's *copy* of the
    prefix equalled the writer's. Prefix agreement was never the real risk —
    three readers each had their own **predicate**, and they disagreed:

    * ``ledger_guard``          ``success == 1``
    * ``accountant_test``       ``bool(success)``    -> the string ``"0"`` was LANDED
    * ``swap_class_fallback``   ``success is True``  -> the integer ``1`` was NOT

    `bool(...)` is an Empty != Zero violation inside the helper meant to
    enforce Empty != Zero. `is True` drops genuine rows, because ``sqlite3``
    returns ``1`` (int) for a BOOLEAN column. Asserting identity of the
    delegated function is what makes a fourth divergence impossible rather
    than merely detectable.
    """
    from almanak.framework.accounting import accountant_test as at
    from almanak.framework.accounting.ledger_guard import landed
    from almanak.framework.accounting.reporting import swap_class_fallback as scf

    assert at._landed_rule is landed
    assert scf._landed_rule is landed


def test_the_one_rule_accepts_both_true_spellings_and_no_malformed_ones():
    """Pins the semantics all three copies disagreed about.

    ``True`` and ``1`` are the two real spellings of true (object-sourced and
    SQLite-sourced). Everything else is malformed and must not be guessed at.

    The clean arm is asserted with NO ``tx_hash`` argument at all — that is the
    contract, not an omission. Readers whose SELECT has no hash column in scope
    must keep working, so ``success == 1`` may never consult it.
    """
    from almanak.framework.accounting.ledger_guard import landed

    assert landed(True, "") is True
    assert landed(1, "") is True          # sqlite3 BOOLEAN — `is True` rejected this
    assert landed("0", "") is False       # `bool(...)` accepted this
    assert landed("1", "") is False
    assert landed(2, "") is False
    assert landed(None, "") is False
    # The degraded arm — marker AND a hash. The marker alone is stamped by
    # construction and a NO-OP bundle carries it with nothing on chain.
    assert landed(0, f"{DEGRADED_PREFIX}x", _HASH_CLOSE) is True
    assert landed(0, f"{DEGRADED_PREFIX}x") is False


@pytest.mark.parametrize(
    "tx_hash",
    [None, "", "   ", "\t", "\n"],
    ids=repr,
)
def test_the_degraded_arm_needs_a_tx_hash_not_just_the_marker(tx_hash):
    """VIB-6043: the marker is not on-chain evidence on its own.

    ``apply_degradation`` writes ``chain_success: True`` unconditionally — it
    never reads a receipt — so it records what the framework believed, not what
    the chain did. A NO-OP ``LP_CLOSE`` (already-empty position ⇒
    ``transactions=[]`` / ``metadata["no_op"]=True`` ⇒ ``result.success=True``
    ⇒ ledger row with ``tx_hash=""`` and no receipt ⇒ both money slots
    unmeasured ⇒ marker) satisfied every other condition, so ``landed`` said
    True for a routine idempotent teardown where nothing was submitted.

    Whitespace counts as absent for the same reason ``_is_unmeasured`` treats a
    blank money column as unmeasured: it is the writer's spelling of "nothing",
    not a value.
    """
    from almanak.framework.accounting.ledger_guard import landed

    assert landed(0, f"{DEGRADED_PREFIX}amounts_unmeasured: intent=LP_CLOSE", tx_hash) is False


def test_the_degraded_arm_lands_when_a_real_hash_backs_it():
    """The lane the guard exists for is untouched.

    A Safe / bundle swap that confirmed on-chain and whose parser yielded no
    amounts always carries a hash — so the conjunct costs that lane nothing,
    which is the whole reason a hash is the right discriminator.
    """
    from almanak.framework.accounting.ledger_guard import landed

    assert landed(0, f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP", _HASH_CLOSE) is True


def test_a_clean_success_lands_without_any_tx_hash_in_scope():
    """The clean arm must never acquire a hash dependency.

    ``accountant_test`` and the ship-gate both call the rule from places whose
    projection may not include ``tx_hash``; gating ``success == 1`` on it would
    silently empty their populations — the mirror of the bug being fixed.
    """
    from almanak.framework.accounting.ledger_guard import landed

    assert landed(1, "") is True
    assert landed(1, "", None) is True
    assert landed(1, "", "") is True
    assert landed(True, "", "   ") is True


def test_a_degraded_swap_still_counts_as_having_run_for_the_reporting_path():
    """VIB-6043 leg 2: a degraded-but-landed swap must not vanish from Rule 3.

    ``detect_stale_post_teardown_snapshot`` suppresses a headline PnL that
    would compare stale state against itself (VIB-4906 / VIB-4907). It keys on
    "did a swap run between these snapshots?" — so a swap the guard downgraded
    must still count, or the wrong number gets published.

    The degraded swap carries a hash: it is the Safe/bundle lane, a swap that
    confirmed on-chain and whose parser yielded no amounts. A no-op carries the
    same marker and no hash, and must NOT count — a swap that was never
    submitted is not an intervening swap, and treating it as one suppresses a
    headline PnL that should have been published.
    """
    from types import SimpleNamespace

    from almanak.framework.accounting.reporting.swap_class_fallback import _landed_on_chain

    degraded = SimpleNamespace(
        success=False,
        error=f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP",
        tx_hash=_HASH_CLOSE,
    )
    assert _landed_on_chain(degraded) is True

    no_op = SimpleNamespace(
        success=False,
        error=f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP",
        tx_hash="",
    )
    assert _landed_on_chain(no_op) is False

    # And the discipline this must NOT loosen:
    assert _landed_on_chain(SimpleNamespace(success=True, error="", tx_hash=_HASH_OPEN)) is True
    assert (
        _landed_on_chain(
            SimpleNamespace(success=False, error="execution reverted", tx_hash=_HASH_OPEN)
        )
        is False
    )
    # A legacy truthy non-bool must still never upgrade a malformed row.
    assert _landed_on_chain(SimpleNamespace(success="1", error="", tx_hash=_HASH_OPEN)) is False


def test_g10_treats_a_degraded_row_as_landed_not_reverted():
    """A clean row + a degraded row in one cycle is not a partial revert."""
    from almanak.framework.accounting.accountant_test import _cell_g10_multi_tx_atomicity

    rows = [
        {
            "cycle_id": "c1",
            "intent_type": "SWAP",
            "tx_hash": "0x" + "11" * 32,
            "success": True,
            "error": "",
        },
        {
            "cycle_id": "c1",
            "intent_type": "LP_OPEN",
            "tx_hash": "0x" + "22" * 32,
            "success": False,
            "error": f"{DEGRADED_PREFIX}amounts_unmeasured: intent=LP_OPEN",
        },
    ]
    result = _cell_g10_multi_tx_atomicity(rows, [], [])
    assert result.status != "FAIL", f"degraded row misreported as a revert: {result.diagnostic}"


def test_g10_still_fails_a_genuinely_mixed_cycle():
    """The degraded arm must not blind G10 to a real partial unwind."""
    from almanak.framework.accounting.accountant_test import _cell_g10_multi_tx_atomicity

    rows = [
        {
            "cycle_id": "c1",
            "intent_type": "SWAP",
            "tx_hash": "0x" + "11" * 32,
            "success": True,
            "error": "",
        },
        {
            "cycle_id": "c1",
            "intent_type": "LP_OPEN",
            "tx_hash": "0x" + "22" * 32,
            "success": False,
            "error": "execution reverted",
        },
    ]
    assert _cell_g10_multi_tx_atomicity(rows, [], []).status == "FAIL"


@pytest.mark.parametrize(
    "bogus",
    [False, True, [], {}, ()],
    ids=["False", "True", "empty-list", "empty-dict", "empty-tuple"],
)
def test_an_illegal_money_value_type_fails_closed(bogus):
    """An unreadable money column is degraded, not waved through.

    ``bool`` is a subclass of ``int``, so ``False`` would read as a measured
    zero if the numeric check ran first. Degrading is the conservative
    direction: it never fabricates a number, it only marks the row as
    not-cleanly-successful — which is what an unreadable money column
    deserves. Treating it as measured would let a malformed value ride
    through as a clean success.
    """
    entry = swap_row(amount_out=bogus)
    degradation = classify_ledger_row(entry)
    assert degradation is not None, f"illegal money value {bogus!r} was accepted as measured"
    assert "amount_out" in degradation.columns


def test_g11_does_not_count_a_degraded_row_as_a_failed_intent():
    """A landed-but-degraded row did NOT fail.

    Keying G11 on the framework verdict flips a run where **nothing reverted**
    from SKIP to PASS — the cell would assert the failed-intent writer
    contract was exercised when zero intents failed. Same defect shape as the
    G1 and G10 holes, one cell over in the same file.
    """
    from almanak.framework.accounting.accountant_test import _cell_g11_failed_intents

    clean = {
        "intent_type": "SWAP",
        "success": True,
        "error": "",
        "gas_usd": "0.42",
        "gas_used": 1000,
        "tx_hash": "0x" + "aa" * 32,
    }
    degraded = dict(clean, success=False, error=f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP")

    assert _cell_g11_failed_intents([clean]).status == "SKIP"
    assert _cell_g11_failed_intents([degraded]).status == "SKIP", (
        "a degraded row was counted as a failed intent — G11 now claims the "
        "failed-intent writer contract was exercised when nothing reverted"
    )

    # And the same shape with unmeasured gas must not produce a false FAIL
    # whose diagnostic says 'failed intents' about a tx that never failed.
    degraded_nogas = dict(degraded, gas_usd="")
    assert _cell_g11_failed_intents([degraded_nogas]).status == "SKIP"


def test_g11_still_evaluates_a_genuinely_failed_intent():
    """The degraded arm must not blind G11 to a real revert."""
    from almanak.framework.accounting.accountant_test import _cell_g11_failed_intents

    reverted = {
        "intent_type": "SWAP",
        "success": False,
        "error": "execution reverted",
        "gas_usd": "0.42",
        "gas_used": 1000,
        "tx_hash": "0x" + "bb" * 32,
    }
    assert _cell_g11_failed_intents([reverted]).status == "PASS"

    # A real revert with gas burned but no gas_usd is still a books failure.
    assert _cell_g11_failed_intents([dict(reverted, gas_usd="")]).status == "FAIL"


def test_degraded_prefix_match_is_not_a_like_wildcard_match():
    """``_`` is a LIKE wildcard; the lifecycle query must match exactly.

    A naive ``LIKE 'accounting_degraded:%'`` also matches
    ``accounting-degraded:``, so the query uses substr instead.
    """
    from almanak.framework.accounting.accountant_test import (
        FixtureLifecycleError,
        _assert_fixture_lifecycle,
    )
    from almanak.framework.primitives.types import Primitive

    conn = _lifecycle_ledger(
        [
            ("LP_OPEN", 1, "", _HASH_OPEN),
            # A near-miss string that a wildcard LIKE would wrongly accept. It
            # carries a REAL hash so the hash conjunct cannot be what excludes
            # it — the prefix match has to do the work, which is what this test
            # measures.
            ("LP_CLOSE", 0, "accounting-degraded:not-our-marker", _HASH_CLOSE),
        ]
    )
    with pytest.raises(FixtureLifecycleError):
        _assert_fixture_lifecycle(conn, Primitive.LP)


# --- the two forms of the rule must agree (VIB-6043) ------------------------


def test_row_landed_reads_tx_hash_and_fails_loudly_when_it_is_absent():
    """A dropped column must RAISE, not silently change the answer.

    The two columns fail in OPPOSITE directions, which is why neither may be
    softened to ``.get``: a missing ``error`` collapses the rule to the
    pre-guard ``success == 1``, and a missing ``tx_hash`` collapses the degraded
    arm to always-False, silently emptying the reader's landed population of
    exactly the rows this module exists for. Both are invisible without a raise.
    """
    from almanak.framework.accounting.ledger_guard import row_landed

    deg = f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP"
    assert row_landed({"success": 0, "error": deg, "tx_hash": _HASH_CLOSE}) is True
    assert row_landed({"success": 0, "error": deg, "tx_hash": ""}) is False

    with pytest.raises((IndexError, KeyError)):
        row_landed({"success": 0, "error": deg})
    with pytest.raises((IndexError, KeyError)):
        row_landed({"success": 0, "tx_hash": _HASH_CLOSE})


def test_the_sql_and_python_forms_agree_row_for_row():
    """The drift this module exists to prevent, tested rather than argued.

    ``landed_sql`` and ``row_landed`` are two spellings of one definition, read
    by different consumers — the ship-gate and the matrix harness filter in
    SQLite, the scorecard and the demo scorer filter in Python. A divergence
    means they disagree about what landed, which is worse than either being
    wrong alone because it reads as corroboration.

    The whitespace rows are the ones that would actually split them: SQLite's
    one-argument ``trim`` strips only literal spaces, so a tab- or
    newline-padded hash would land in SQL and not in Python unless
    ``landed_sql`` passes the same character set ``str.strip`` removes.
    """
    from almanak.framework.accounting import ledger_guard as lg

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE transaction_ledger "
        "(id INTEGER PRIMARY KEY, success INTEGER, error TEXT, tx_hash TEXT)"
    )
    deg = f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP"
    rows = [
        (1, 1, "", _HASH_OPEN),                       # clean
        (2, 1, "", ""),                               # clean, no hash — still landed
        (3, 1, "", None),                             # clean, NULL hash — still landed
        (4, 0, deg, _HASH_CLOSE),                     # degraded + hash — landed
        (5, 0, deg, ""),                              # no-op
        (6, 0, deg, None),                            # no-op, NULL
        (7, 0, deg, "   "),                           # no-op, spaces
        (8, 0, deg, "\t"),                            # no-op, tab
        (9, 0, deg, "\n"),                            # no-op, newline
        (10, 0, deg, "\r"),                           # no-op, carriage return
        (11, 0, deg, "\x0b\x0c"),                     # no-op, vertical tab / form feed
        (12, 0, "execution reverted", _HASH_CLOSE),   # a real revert
        (13, 0, "accounting-degraded:nope", _HASH_CLOSE),  # LIKE-wildcard near miss
        (14, 0, None, _HASH_CLOSE),                   # NULL error
        (15, 2, "", _HASH_CLOSE),                     # malformed success
    ]
    conn.executemany("INSERT INTO transaction_ledger VALUES (?, ?, ?, ?)", rows)

    sql_landed = {
        r["id"]
        for r in conn.execute(
            f"SELECT id FROM transaction_ledger WHERE {lg.landed_sql()}",
            lg.LANDED_PARAMS,
        )
    }
    py_landed = {
        r["id"] for r in conn.execute("SELECT * FROM transaction_ledger") if lg.row_landed(r)
    }

    assert sql_landed == py_landed, (
        "the SQL and Python forms of the landed rule disagree on "
        f"{sorted(sql_landed ^ py_landed)} — a SQL reader and a Python reader "
        "would classify the same row differently"
    )
    # Pinned explicitly too: agreeing on the WRONG set would satisfy the line
    # above, and "they agree" is only worth anything if the shared answer is
    # also correct.
    assert sql_landed == {1, 2, 3, 4}


def test_the_sql_form_agrees_with_python_through_a_table_alias():
    """The aliased fragment must qualify EVERY column it names.

    ``landed_sql("t")`` now emits three column references instead of two; a
    ``tx_hash`` left unqualified would either raise on a join or, worse,
    silently resolve to the wrong table's column.
    """
    from almanak.framework.accounting import ledger_guard as lg

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE transaction_ledger "
        "(id INTEGER PRIMARY KEY, success INTEGER, error TEXT, tx_hash TEXT)"
    )
    # A second table carrying a CONFLICTING tx_hash for the same id: if the
    # fragment left any column unqualified, this row would decide the verdict.
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY, tx_hash TEXT, success INTEGER, error TEXT)")
    deg = f"{DEGRADED_PREFIX}amounts_unmeasured: intent=SWAP"
    conn.executemany(
        "INSERT INTO transaction_ledger VALUES (?, ?, ?, ?)",
        [(1, 0, deg, _HASH_CLOSE), (2, 0, deg, "")],
    )
    conn.executemany(
        "INSERT INTO other VALUES (?, ?, ?, ?)",
        [(1, "", 0, ""), (2, _HASH_CLOSE, 1, deg)],
    )

    aliased = {
        r["id"]
        for r in conn.execute(
            "SELECT t.id FROM transaction_ledger t JOIN other o ON o.id = t.id "
            f"WHERE {lg.landed_sql('t')}",
            lg.LANDED_PARAMS,
        )
    }
    assert aliased == {1}


def test_the_sql_and_python_forms_agree_on_EVERY_whitespace_character() -> None:
    """The prior mirror test could not split the two forms.

    `_SQL_STRIP_CHARS` covered 6 characters; `str.strip()` removes 29. That left
    23 unicode whitespace characters (NBSP, the EN/EM quad family, U+2028/9,
    ideographic space, the C1 separators) LANDED to SQL and NOT LANDED to
    Python — a divergence in the one invariant this module exists to hold.

    The old test exercised only `' '`, tab, newline, CR and `\x0b\x0c`, so it
    passed on a set that provably cannot split them: vacuity class 4, the
    observation window. This sweeps every character `str.isspace()` accepts.
    """
    import sqlite3

    from almanak.framework.accounting.ledger_guard import (
        DEGRADED_PREFIX,
        LANDED_PARAMS,
        landed,
        landed_sql,
    )

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t(success INT, error TEXT, tx_hash TEXT)")
    marker = f"{DEGRADED_PREFIX}amounts_unmeasured: x"

    divergent = []
    for code in range(0x110000):
        ch = chr(code)
        if not ch.isspace():
            continue
        conn.execute("DELETE FROM t")
        conn.execute("INSERT INTO t VALUES(0, ?, ?)", (marker, ch))
        via_sql = bool(conn.execute(f"SELECT 1 FROM t WHERE {landed_sql()}", LANDED_PARAMS).fetchone())
        via_py = landed(0, marker, ch)
        if via_sql != via_py:
            divergent.append(repr(ch))

    assert divergent == [], f"SQL and Python disagree on {len(divergent)} whitespace-only hashes: {divergent[:8]}"


class TestApplyDegradationNeverDestroysTheAuditTrail:
    """Valid JSON that is not an OBJECT used to be silently dropped.

    `apply_degradation` parsed `extracted_data_json` and kept it only when it
    was a dict. A top-level ARRAY parses without raising, so the `except` arm —
    whose comment promises "losing forensic data to make room for a marker
    would be its own accounting bug" — never fired, and `payload` stayed `{}`.
    The blob was overwritten by the marker alone.

    That destroys the `sub_transactions` audit trail CI gate G8 reads. Reachable
    exactly where it matters: the gateway `SaveLedgerEntry` /
    `SaveLedgerAndRegistry` RPCs, a boundary added on the stated premise that
    the caller may be untrusted or differently versioned — the producer most
    likely to send a shape this function did not expect.
    """

    @staticmethod
    def _entry(raw: str):  # type: ignore[no-untyped-def]
        class _E:
            success = True
            error = ""
            extracted_data_json = ""

        e = _E()
        e.extracted_data_json = raw
        return e

    @staticmethod
    def _degrade(entry) -> dict:  # type: ignore[no-untyped-def]
        import json as _json

        from almanak.framework.accounting.ledger_guard import (
            LedgerDegradation,
            LedgerDegradationReason,
            apply_degradation,
        )

        apply_degradation(
            entry,
            LedgerDegradation(
                reason=list(LedgerDegradationReason)[0], columns=("amount_in",), detail="t"
            ),
        )
        return _json.loads(entry.extracted_data_json)

    def test_a_top_level_array_is_preserved_not_dropped(self) -> None:
        import json as _json

        raw = _json.dumps([{"sub_transactions": [{"tx_hash": "0xa", "status": "success"}]}])
        out = self._degrade(self._entry(raw))
        assert "accounting_degradation" in out, "the marker must still be stamped"
        assert "_unparsed_extracted_data" in out, (
            "valid-but-non-object JSON was DROPPED — the sub_transactions audit "
            "trail that gate G8 reads is gone"
        )
        assert "sub_transactions" in str(out["_unparsed_extracted_data"])

    def test_a_dict_payload_is_still_merged_in_place(self) -> None:
        """The normal case must not regress into the side-key path."""
        import json as _json

        raw = _json.dumps({"sub_transactions": [{"tx_hash": "0xa", "status": "success"}]})
        out = self._degrade(self._entry(raw))
        assert "sub_transactions" in out, "a dict payload must stay merged at the top level"
        assert "_unparsed_extracted_data" not in out
        assert "accounting_degradation" in out

    def test_a_non_json_blob_is_still_preserved(self) -> None:
        out = self._degrade(self._entry("not json at all"))
        assert out["_unparsed_extracted_data"] == "not json at all"
