"""Verdict-transition matrix for VIB-6043 leg 2 — the anti-narrowing gate.

This PR flips ledger rows from ``success=1`` to ``success=0``. Twice during its
development a fix intended to remove a *vacuous green* instead **narrowed a
gate** — once turning a genuine FAIL into a SKIP (G9's slippage contradiction),
once restoring the pre-sweep predicate through a silent ``except`` (the demo
scorer's ``_landed``). Both were caught by external review. Review caught two
and missed two, which is a coin flip.

So this module catches that class **by construction** instead. It enumerates
every ledger row shape that matters, runs every accounting validation surface
against each, and pins the resulting verdict. The rule it enforces:

    **No adverse verdict may become PASS, and no FAIL may become SKIP,
    without an explicit, named justification.**

"Adverse" spans FAIL, PARTIAL, SKIP and CRASH — every state in which a reader
does not ship. ``PASS -> FAIL`` and ``SKIP -> FAIL`` are coverage *gains* and
need no signature; ``N/A -> PASS`` is a question becoming applicable, also a
gain. Only coverage **loss** needs a signature, and every signature is reviewed
here in one place rather than re-derived from a diff.

**The rule started narrower and that was itself a defect.** Version one was
scoped ``if pre == "FAIL"``. It therefore *recorded* ``CRASH -> PASS`` for G9
on ``malformed_json_row`` and did not trip on it — the matrix built to catch
narrowing by construction let a narrowing through by construction. A fourth
auditor found the underlying bug on seven committed fixture DBs, where a gate
that used to abort the run instead printed ``PASS :: 2 landed swap rows:
slippage_source consistent`` having read none of them. A crash is not a
verdict, but it is not a green light either, and treating it as a state with
no coverage to lose is exactly how the hole opened.

The ``pre`` column was MEASURED, not asserted: each value was produced by
loading the ``origin/main`` copy of the same module and running the same gate
against the same fixture. The generator lives in the PR description; the
numbers are frozen here so the test needs no git at runtime.

**One shape has ``pre = N/A`` on every surface, deliberately.**
``noop_degraded`` carries the degradation marker with ``tx_hash=""`` — a row
``main`` cannot emit, because the marker did not exist there. Running main's
gates against it would measure their opinion of an input they can never
receive: a different quantity from every other ``pre`` in this column, printed
in the same column with no marker, which is precisely the "claims more than it
measured" defect this PR is about. ``N/A`` is ranked WITH ``SKIP`` in
:data:`_STRENGTH`, so recording it costs nothing in rigour — the two
``N/A -> PASS`` cells on that shape still had to be signed in
:data:`JUSTIFIED_LOSSES` like any other coverage loss.

**What the matrix shows, in one line**: on every ``landed_degraded*`` shape,
G1 / G3 / G8 move ``PASS -> FAIL``. The degraded row is now caught where it
previously sailed through. That is the entire point of the change, stated as a
measurement.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sqlite3
from pathlib import Path

import pytest

import scripts.ci.accounting_regression_assert as gate
from almanak.framework.accounting import accountant_test as at
from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX as DEG

_REPO_ROOT = Path(__file__).resolve().parents[3]


# Loaded by PATH, not by importing the sibling test module.
#
# ``tests/unit/accounting`` has no ``__init__.py`` and this repo's pytest.ini
# sets ``--import-mode=append``, under which a test module is imported
# top-level with no parent package — so ``from .test_landed_population_readers
# import ...`` raises ImportError at COLLECTION. Every local run of this file
# had passed ``--import-mode=importlib``, which hid it: the merge gate itself
# would not have collected under the mode CI actually uses.
#
# The duplicated loader is stdlib import boilerplate, not a rule with
# semantics; the one-home discipline in this PR is about the LANDED predicate,
# and inventing a shared test package to avoid six lines would add more
# collection fragility than it removes.
def _load_script(basename: str):
    """Import a ``scripts/qa/*.py`` module (no ``__init__.py`` in that dir)."""
    path = _REPO_ROOT / "scripts" / "qa" / f"{basename}.py"
    spec = importlib.util.spec_from_file_location(f"_matrix_{basename}", path)
    assert spec and spec.loader, path
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


scorer = _load_script("score_demos_anvil")

_HA = "0x" + "a" * 64
_HB = "0x" + "b" * 64
_SUBTX = json.dumps(
    {
        "sub_transactions": [
            {
                "tx_hash": _HA,
                "target_contract": "",
                "function_selector": "",
                "gas_used": 210000,
                "status": "success",
                "role": "ACTION",
            }
        ]
    }
)


def _row(**kw) -> dict:
    d = {
        "id": 1,
        "timestamp": "2026-01-01T00:30:00",
        "deployment_id": "d",
        "cycle_id": "c",
        "intent_type": "SWAP",
        "token_in": "USDC",
        "amount_in": "100.0",
        "token_out": "WETH",
        "amount_out": "0.03",
        "gas_used": 210000,
        "gas_usd": "0.42",
        "tx_hash": _HA,
        "success": 1,
        "error": "",
        "slippage_bps": 5.0,
        "effective_price": "3333",
        "extracted_data_json": _SUBTX,
        "price_inputs_json": "{}",
        "pre_state_json": "{}",
        "post_state_json": "{}",
    }
    d.update(kw)
    return d


_DEG_ERR = f"{DEG}unmeasured_money_slots: amount_in"


def _noop_degraded_row() -> dict:
    """A NO-OP ``LP_CLOSE`` row, built by running the REAL writer over it.

    Not hand-written. The field values below are the ones production produces
    for an ``LP_CLOSE`` against an already-empty position, and ``success`` /
    ``error`` / ``extracted_data_json`` are then whatever
    ``classify_ledger_row`` + ``apply_degradation`` actually stamp — the same
    two functions the runner and the gateway's ``SaveLedgerEntry`` both call.
    A dict written by hand here would be a claim about the writer; this is the
    writer's output.

    Provenance of each input:

    * ``transactions=[]`` + ``metadata["no_op"]=True`` —
      ``uniswap_v3/compiler.py`` (``no_op = not transactions``; curve and
      aerodrome emit the same shape).
    * ``result.success = True`` — ``orchestrator.py`` treats a no-op bundle as
      a legitimate success and returns before the empty-bundle failure path.
    * ``tx_hash=""``, ``gas_used=0``, ``gas_usd=""``, empty money columns —
      nothing was submitted, so there is no receipt to copy any of them from.
      Empty != Zero: they are unmeasured, never fabricated zeros.
    """
    from almanak.framework.accounting.ledger_guard import apply_degradation, classify_ledger_row
    from almanak.framework.observability.ledger import LedgerEntry

    entry = LedgerEntry(
        intent_type="LP_CLOSE",
        success=True,
        tx_hash="",
        gas_used=0,
        gas_usd="",
        amount_in="",
        amount_out="",
        chain="arbitrum",
        protocol="uniswap_v3",
    )
    degradation = classify_ledger_row(entry)
    assert degradation is not None, (
        "fixture precondition: the guard must actually fire on a no-op row — "
        "if it stops, this shape no longer represents anything"
    )
    apply_degradation(entry, degradation)
    assert entry.success is False and entry.error.startswith(DEG)

    return _row(
        intent_type="LP_CLOSE",
        token_in="",
        amount_in="",
        token_out="",
        amount_out="",
        gas_used=0,
        gas_usd="",
        tx_hash="",
        success=int(entry.success),
        error=entry.error,
        slippage_bps=None,
        effective_price="",
        extracted_data_json=entry.extracted_data_json,
    )


#: Every ledger row shape whose treatment this PR could plausibly change.
SHAPES: dict[str, list[dict]] = {
    "clean_measured": [_row()],
    # the pre-fix bug shape: booked clean, money never measured
    "success_unmeasured": [_row(amount_in="", amount_out="")],
    "landed_degraded": [_row(success=0, error=_DEG_ERR, amount_in="", amount_out="")],
    "landed_degraded_no_gas": [_row(success=0, error=_DEG_ERR, gas_usd="")],
    "landed_degraded_no_subtx": [_row(success=0, error=_DEG_ERR, extracted_data_json="{}")],
    "reverted": [_row(success=0, error="execution reverted: STF", tx_hash="", gas_usd="")],
    # LANDED, but downgraded with free-form prose and NO machine-readable marker
    "slippage_downgrade": [_row(success=0, error="Slippage circuit breaker: actual slippage 412 bps")],
    "ledger_empty": [],
    "all_reverted": [
        _row(success=0, error="execution reverted", tx_hash="", gas_usd=""),
        _row(id=2, success=0, error="execution reverted", tx_hash="", gas_usd=""),
    ],
    "slippage_contradiction_landed": [
        _row(slippage_bps=50.0, extracted_data_json=json.dumps({"swap_amounts": {"slippage_source": "NONE"}}))
    ],
    "slippage_contradiction_reverted": [
        _row(
            success=0,
            error="execution reverted",
            tx_hash="",
            slippage_bps=50.0,
            extracted_data_json=json.dumps({"swap_amounts": {"slippage_source": "NONE"}}),
        )
    ],
    # G9: json valid, swap_amounts present, slippage_source ABSENT. NOT IN
    # yields NULL here, so this row escaped the offender scan while still
    # counting toward the denominator.
    "empty_swap_amounts": [_row(extracted_data_json=json.dumps({"swap_amounts": {}}))],
    "malformed_json_row": [_row(), _row(id=2, tx_hash=_HB, extracted_data_json="}{ not json")],
    # The shape the tx_hash conjunct newly distinguishes: the marker WITHOUT a
    # hash. Same books verdict as ``landed_degraded``, but nothing was ever
    # submitted, so it must not be read as a transaction that happened.
    "noop_degraded": [_noop_degraded_row()],
}

_DDL_LEDGER = """
CREATE TABLE transaction_ledger (
  id INTEGER PRIMARY KEY, timestamp TEXT, deployment_id TEXT, cycle_id TEXT,
  intent_type TEXT, token_in TEXT, amount_in TEXT, token_out TEXT, amount_out TEXT,
  gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, success INTEGER, error TEXT,
  slippage_bps REAL, effective_price TEXT, extracted_data_json TEXT,
  price_inputs_json TEXT, pre_state_json TEXT, post_state_json TEXT);
"""
_DDL_REST = """
CREATE TABLE accounting_outbox (id INTEGER PRIMARY KEY, status TEXT);
CREATE TABLE accounting_events (id INTEGER PRIMARY KEY, event_type TEXT,
  payload_json TEXT, confidence TEXT, unavailable_reason TEXT);
CREATE TABLE position_events (id INTEGER PRIMARY KEY, event_type TEXT, value_usd TEXT,
  position_id TEXT, position_type TEXT, token0 TEXT, token1 TEXT, amount0 TEXT,
  amount1 TEXT, gas_usd TEXT, tick_lower INTEGER, tick_upper INTEGER,
  liquidity TEXT, in_range INTEGER, attribution_json TEXT, fees_token0 TEXT,
  fees_token1 TEXT, timestamp TEXT);
CREATE TABLE portfolio_snapshots (id INTEGER PRIMARY KEY, total_value_usd TEXT,
  available_cash_usd TEXT, deployed_capital_usd TEXT, confidence TEXT,
  unavailable_reason TEXT, value_confidence TEXT, positions_json TEXT,
  wallet_balances_json TEXT, iteration_number INTEGER, timestamp TEXT);
CREATE TABLE strategy_state (id INTEGER PRIMARY KEY, state_data TEXT);
CREATE TABLE teardown_requests (id INTEGER PRIMARY KEY, status TEXT,
  positions_closed INTEGER, positions_total INTEGER, started_at TEXT, updated_at TEXT);
"""


def _db(rows, path: Path | None = None, *, teardown: bool = False) -> sqlite3.Connection:
    con = sqlite3.connect(str(path) if path else ":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(_DDL_LEDGER + _DDL_REST)
    for r in rows:
        cols = ",".join(r)
        con.execute(
            f"INSERT INTO transaction_ledger ({cols}) VALUES ({','.join('?' * len(r))})",
            tuple(r.values()),
        )
    if teardown:
        con.execute(
            "INSERT INTO teardown_requests (status, positions_closed, positions_total,"
            " started_at, updated_at) VALUES ('completed',1,1,?,?)",
            ("2026-01-01T00:00:00", "2026-01-01T01:00:00"),
        )
    con.commit()
    return con


def _v(fn, *a) -> str:
    """A crash is a verdict too — the pre-PR gate really did abort on some
    shapes, and hiding that behind an exception would understate the change."""
    try:
        return fn(*a).status
    except Exception as exc:  # noqa: BLE001 - deliberately broad; see docstring
        return f"CRASH({type(exc).__name__})"


def _verdicts(shape: str, tmp_path: Path) -> dict[str, str]:
    rows = SHAPES[shape]
    out = {
        "G3": _v(gate.gate_gas_tracking, _db(rows)),
        "G5": _v(gate.gate_teardown_trail, _db(rows, teardown=True), "lp"),
        "G8": _v(gate.gate_sub_transactions, _db(rows)),
        "G9": _v(gate.gate_slippage_source, _db(rows)),
        "G1": _v(at._cell_g1_money_trail, [dict(r) for r in rows], [], {}, "lp"),
        "G10": _v(at._cell_g10_multi_tx_atomicity, [dict(r) for r in rows], [], []),
        "G11": _v(at._cell_g11_failed_intents, [dict(r) for r in rows]),
    }
    db = tmp_path / shape / "almanak_state.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    _db(rows, db).close()
    try:
        s = scorer.score_db(db)
        out |= {"Q1": s["Q1"], "Q2": s["Q2"], "Q3": s["Q3"]}
    except Exception as exc:  # noqa: BLE001
        out |= dict.fromkeys(("Q1", "Q2", "Q3"), f"CRASH({type(exc).__name__})")
    return out


#: (pre-PR, post-PR) verdict per shape per surface. ``pre`` was measured by
#: running the ``origin/main`` copy of each module against the same fixture.
MATRIX: dict[str, dict[str, tuple[str, str]]] = {
    "clean_measured": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "PASS"),
        "G9": ("PASS", "FAIL"),
        "G1": ("FAIL", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("SKIP", "SKIP"),
        "Q1": ("PASS", "PASS"),
        "Q2": ("PASS", "PASS"),
        "Q3": ("PASS", "PASS"),
    },
    "success_unmeasured": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "PASS"),
        "G9": ("PASS", "FAIL"),
        "G1": ("FAIL", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("SKIP", "SKIP"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("FAIL", "FAIL"),
        "Q3": ("PASS", "PASS"),
    },
    "landed_degraded": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "PASS"),
        "G9": ("PASS", "FAIL"),
        "G1": ("PASS", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("PASS", "SKIP"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("N/A", "FAIL"),
        "Q3": ("N/A", "PASS"),
    },
    "landed_degraded_no_gas": {
        "G3": ("PASS", "FAIL"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "PASS"),
        "G9": ("PASS", "FAIL"),
        "G1": ("PASS", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("FAIL", "SKIP"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("N/A", "PASS"),
        "Q3": ("N/A", "FAIL"),
    },
    "landed_degraded_no_subtx": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "FAIL"),
        "G9": ("PASS", "FAIL"),
        "G1": ("PASS", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("PASS", "SKIP"),
        "Q1": ("FAIL", "PASS"),
        "Q2": ("N/A", "PASS"),
        "Q3": ("N/A", "PASS"),
    },
    "reverted": {
        "G3": ("PASS", "SKIP"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "SKIP"),
        "G9": ("PASS", "SKIP"),
        "G1": ("PASS", "PASS"),
        "G10": ("PASS", "PASS"),
        "G11": ("FAIL", "FAIL"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("N/A", "N/A"),
        "Q3": ("N/A", "N/A"),
    },
    "slippage_downgrade": {
        "G3": ("PASS", "SKIP"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "SKIP"),
        "G9": ("PASS", "SKIP"),
        "G1": ("PASS", "PASS"),
        "G10": ("PASS", "PASS"),
        "G11": ("PASS", "PASS"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("N/A", "N/A"),
        "Q3": ("N/A", "N/A"),
    },
    "ledger_empty": {
        "G3": ("PASS", "SKIP"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "SKIP"),
        "G9": ("PASS", "SKIP"),
        "G1": ("FAIL", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("SKIP", "SKIP"),
        "Q1": ("N/A", "N/A"),
        "Q2": ("N/A", "N/A"),
        "Q3": ("N/A", "N/A"),
    },
    "all_reverted": {
        "G3": ("PASS", "SKIP"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "SKIP"),
        "G9": ("PASS", "SKIP"),
        "G1": ("PASS", "PASS"),
        "G10": ("PASS", "PASS"),
        "G11": ("FAIL", "FAIL"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("N/A", "N/A"),
        "Q3": ("N/A", "N/A"),
    },
    "slippage_contradiction_landed": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("FAIL", "FAIL"),
        "G9": ("FAIL", "FAIL"),
        "G1": ("FAIL", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("SKIP", "SKIP"),
        "Q1": ("PASS", "PASS"),
        "Q2": ("PASS", "PASS"),
        "Q3": ("PASS", "PASS"),
    },
    "slippage_contradiction_reverted": {
        "G3": ("PASS", "SKIP"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("PASS", "SKIP"),
        "G9": ("FAIL", "FAIL"),
        "G1": ("PASS", "PASS"),
        "G10": ("PASS", "PASS"),
        "G11": ("PASS", "PASS"),
        "Q1": ("FAIL", "FAIL"),
        "Q2": ("N/A", "N/A"),
        "Q3": ("N/A", "N/A"),
    },
    "empty_swap_amounts": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("FAIL", "FAIL"),
        "G9": ("PASS", "FAIL"),
        "G1": ("FAIL", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("SKIP", "SKIP"),
        "Q1": ("PASS", "PASS"),
        "Q2": ("PASS", "PASS"),
        "Q3": ("PASS", "PASS"),
    },
    "malformed_json_row": {
        "G3": ("PASS", "PASS"),
        "G5": ("FAIL", "FAIL"),
        "G8": ("CRASH(OperationalError)", "FAIL"),
        "G9": ("CRASH(OperationalError)", "FAIL"),
        "G1": ("FAIL", "FAIL"),
        "G10": ("PASS", "PASS"),
        "G11": ("SKIP", "SKIP"),
        "Q1": ("PASS", "PASS"),
        "Q2": ("PASS", "PASS"),
        "Q3": ("PASS", "PASS"),
    },
    # ``pre`` IS "N/A" FOR EVERY SURFACE ON THIS SHAPE, AND THAT IS A MEASURED
    # STATEMENT, NOT A PLACEHOLDER. The degradation marker did not exist before
    # this PR, so ``main`` cannot emit this row at all: running main's gates
    # against it would measure what they would say about an input they can never
    # receive, which is a different quantity from every other ``pre`` in this
    # column and would be a fabricated baseline wearing a measurement. "Not
    # applicable" is the honest verdict, and the strength model already ranks
    # N/A WITH SKIP rather than with PASS, so it cannot launder a gap: the two
    # ``N/A -> PASS`` cells below still had to be signed.
    #
    # ``post`` was measured by running every surface against the fixture.
    "noop_degraded": {
        "G3": ("N/A", "SKIP"),
        "G5": ("N/A", "FAIL"),
        "G8": ("N/A", "SKIP"),
        "G9": ("N/A", "SKIP"),
        "G1": ("N/A", "FAIL"),
        "G10": ("N/A", "PASS"),
        "G11": ("N/A", "PASS"),
        "Q1": ("N/A", "FAIL"),
        "Q2": ("N/A", "N/A"),
        "Q3": ("N/A", "N/A"),
    },
}


#: Signed coverage losses, keyed by the **exact** transition
#: ``(shape, surface, pre, post)`` and carrying ``(compensating_surface,
#: reason)``.
#:
#: Keyed by the exact pre/post pair on purpose. An earlier version keyed on
#: ``(shape, surface)`` alone, which made every waiver a permanent pair-level
#: bypass: a signature covering ``FAIL -> SKIP`` silently authorised a later
#: ``FAIL -> PASS`` at the same cell, because presence of the key was the whole
#: check. A waiver must license one transition, not a cell.
#:
#: ``compensating_surface`` names the surface that MUST still FAIL on that
#: shape. An earlier version only required *some* other surface to fail, so an
#: unrelated G1/G5 failure satisfied a waiver whose entire argument rested on
#: G3 or G8 — and the waiver would have kept passing after the surface it
#: named regressed.
JUSTIFIED_LOSSES: dict[tuple[str, str, str, str], tuple[str, str]] = {
    ("landed_degraded_no_gas", "G11", "FAIL", "SKIP"): (
        "G3",
        "G11 asks 'were FAILED intents recorded properly?'. Pre-PR a "
        "landed-degraded row was misclassified AS a failed intent, so the cell "
        "evaluated it and FAILed — a verdict about a row that never failed. "
        "_row_landed now classifies it correctly, leaving zero failed intents, "
        "so SKIP is the honest answer. The underlying defect (missing gas_usd) "
        "is still caught on this same shape by G3, which moves PASS -> FAIL.",
    ),
    ("landed_degraded_no_subtx", "Q1", "FAIL", "PASS"): (
        "G8",
        "Pre-PR Q1 SKIPPED the degraded row inside its loop, leaving its "
        "`complete` list EMPTY, and fell through to `else: FAIL`. That FAIL was "
        "vacuous — it failed because it examined nothing, not because it found "
        "anything. Post-PR it examines the row and finds tx_hash, gas and both "
        "amounts present, which is correct for this shape: the missing thing is "
        "sub_transactions, which Q1 does not check. G8 catches it on this same "
        "shape, moving PASS -> FAIL.",
    ),
    ("landed_degraded", "Q3", "N/A", "PASS"): (
        "G1",
        "Q3 (gas per tx) scored N/A pre-PR because the degraded row was "
        "filtered out and no rows remained to grade — the question did not "
        "apply because its population had been emptied, which is the defect. "
        "It now grades the row and finds gas present, correctly. N/A is ranked "
        "WITH SKIP rather than with PASS precisely so this transition has to be "
        "argued rather than assumed; the money defect on this shape is still "
        "caught by G1 (PASS -> FAIL).",
    ),
    ("landed_degraded_no_gas", "Q2", "N/A", "PASS"): (
        "G3",
        "Same shape of transition as above: Q2 (swap costs) had no rows to "
        "grade once the degraded row was excluded. It now grades it and the "
        "swap-cost fields are present, so PASS is backed by an inspection. The "
        "actual defect on this shape — missing gas_usd — is caught by G3 "
        "(PASS -> FAIL), and by Q3 which moves N/A -> FAIL.",
    ),
    ("landed_degraded_no_subtx", "Q2", "N/A", "PASS"): (
        "G8",
        "As above — Q2's population was empty pre-PR and is now graded. The "
        "missing sub_transactions on this shape are outside Q2's remit and are "
        "caught by G8 (PASS -> FAIL).",
    ),
    ("landed_degraded_no_subtx", "Q3", "N/A", "PASS"): (
        "G8",
        "As above — Q3's population was empty pre-PR and is now graded, finding "
        "gas present. The defect on this shape is caught by G8 (PASS -> FAIL).",
    ),
    ("noop_degraded", "G10", "N/A", "PASS"): (
        "G1",
        "G10 asks 'did every intent in a cycle reach the same ON-CHAIN outcome?' "
        "and explicitly skips rows with no tx_hash, because a row that never "
        "reached the chain has no on-chain outcome to be consistent with. A "
        "no-op has no hash, so G10's population is empty and PASS is vacuous "
        "for the correct reason — the question genuinely does not apply, rather "
        "than its population having been filtered out by a bad predicate. The "
        "shape is caught by G1 (N/A -> FAIL), which is the surface that owns "
        "'this row claims a money movement it cannot evidence'.",
    ),
    ("noop_degraded", "G11", "N/A", "PASS"): (
        "G1",
        "G11 asks 'were failed intents RECORDED properly?', not 'did anything "
        "fail?'. Post-fix the no-op is not landed, so it enters G11's "
        "population, and G11's only check is gas_usd-present-when-gas_used>0: "
        "gas_used is 0 because nothing was submitted, so the row is correctly "
        "recorded and PASS is backed by an inspection. Noted honestly: G11 "
        "labels it a 'failed intent' when it is really a no-op — a naming "
        "imprecision in G11's population, not a missed defect, and tracked "
        "rather than papered over. G1 (N/A -> FAIL) catches the shape.",
    ),
}


def _transitions():
    for shape, gates in MATRIX.items():
        for gid, (pre, post) in gates.items():
            yield shape, gid, pre, post


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_verdicts_match_the_recorded_matrix(shape: str, tmp_path: Path) -> None:
    """Pin the CURRENT verdict of every surface on every shape.

    This is the tripwire. Any future edit that changes what a gate reports for
    any of these shapes fails here and must update the matrix deliberately,
    rather than the change passing unnoticed because no existing test happened
    to cover that combination.
    """
    actual = _verdicts(shape, tmp_path)
    expected = {gid: post for gid, (_pre, post) in MATRIX[shape].items()}
    assert actual == expected


#: **Coverage strength** of a verdict: how much adverse signal it carries.
#: A transition that LOWERS strength removes coverage and needs a signature.
#:
#: Modelled as a strength ordering rather than a list of forbidden pairs,
#: because enumeration lost three times on this file. Version one caught only
#: ``FAIL -> PASS/SKIP``; version two added CRASH and PARTIAL; each round an
#: unenumerated state slipped through. With an ordering, a state cannot slip by
#: not being thought of — it can only slip by being *ranked wrong*, which is a
#: single visible decision per verdict instead of a combinatorial one.
_STRENGTH: dict[str, int] = {
    "FAIL": 3,  # evaluated; defect found; blocks the release
    "CRASH": 2,  # blocks the release, but produced no verdict at all
    "PARTIAL": 2,  # evaluated; partial defect (demo scorer)
    "SKIP": 1,  # blocks (exit 2), but evaluated nothing
    "N/A": 1,  # did not evaluate — ranked WITH SKIP, not with PASS, because
    # "not applicable" can mean "filtered out before evaluation",
    # which is the exact defect class this file audits
    "XFAIL": 0,  # tolerated by the ratchet; does not block
    "XPASS": 0,
    "PASS": 0,  # evaluated; nothing wrong
}


#: Verdicts recorded as ``CRASH(ExceptionName)``. Normalised to a single rank
#: rather than prefix-matched inside :func:`_strength`, so the "fail closed on
#: anything unranked" property has no soft edge: a prefix test would silently
#: accept ``CRASHED``, ``CRASH_LATER`` or any typo as a known verdict.
_CRASH_RE = re.compile(r"^CRASH\([A-Za-z_][A-Za-z0-9_]*\)$")


def _strength(verdict: str) -> int:
    """Rank a verdict, **failing closed** on anything unrecognised.

    An unknown verdict is not silently treated as neutral. Neutral-by-default
    is how every previous version of this rule leaked: a state nobody
    enumerated scored as "no coverage to lose". Adding a verdict to the system
    must be a deliberate act that includes ranking it.
    """
    key = "CRASH" if _CRASH_RE.match(verdict) else verdict
    if key not in _STRENGTH:
        raise AssertionError(
            f"unranked verdict {verdict!r} — add it to _STRENGTH with a "
            "deliberate strength rather than letting it default to neutral"
        )
    return _STRENGTH[key]


def _is_loss(pre: str, post: str) -> bool:
    """A transition loses coverage when it lowers adverse-signal strength."""
    return _strength(post) < _strength(pre)


def test_no_unjustified_coverage_loss() -> None:
    """**The rule**, and it is deliberately wider than it first was.

    Two transitions count as coverage loss:

    * **any ship-blocking verdict -> PASS.** A run that previously could not
      ship now can.
    * **FAIL -> SKIP.** Both block shipping, but a known defect has become
      "did not look", which is a real loss of signal.

    The first clause is the one that matters most, and it exists because of a
    hole in the FIRST version of this test. That version was scoped
    ``if pre == "FAIL"``, so it recorded ``CRASH -> PASS`` for G9 on
    ``malformed_json_row`` and **did not trip on it** — the matrix built to
    catch narrowing by construction let a narrowing through by construction.
    An external auditor found it on seven committed fixture DBs, where a gate
    that used to abort the run now printed
    ``PASS :: 2 landed swap rows: slippage_source consistent`` having read
    none of them.

    A crash is not a verdict; it is the absence of one. But it is not a green
    light either, and treating it as a state with no coverage to lose is how
    the hole opened.
    """
    unjustified = [
        (shape, gid, pre, post)
        for shape, gid, pre, post in _transitions()
        if _is_loss(pre, post) and (shape, gid, pre, post) not in JUSTIFIED_LOSSES
    ]
    assert not unjustified, (
        "coverage lost without justification — a gate that used to stop this "
        f"shape from shipping no longer does: {unjustified}"
    )


def test_every_justified_loss_is_still_covered_by_another_surface() -> None:
    """A justification is only acceptable if the shape stays caught somewhere.

    Losing a FAIL is defensible when the gate was answering the wrong question
    (both current entries are that case). It is NOT defensible if the shape
    then sails through every surface — that is how a defect class goes dark.
    So each justified loss must be paired with at least one other surface that
    FAILs on the same shape.
    """
    for (shape, gid, _pre, _post), (compensator, _why) in JUSTIFIED_LOSSES.items():
        assert compensator != gid, f"{shape}/{gid} cannot compensate for itself"
        assert compensator in MATRIX[shape], (
            f"{shape}/{gid} names {compensator} as its compensating surface, but that surface is not in the matrix"
        )
        c_pre, c_post = MATRIX[shape][compensator]
        assert c_post == "FAIL", (
            f"{shape}/{gid} was signed off because {compensator} still catches "
            f"this shape — but {compensator} now reports {c_post}. The waiver's "
            "argument no longer holds; re-verify before re-signing."
        )
        # ...and it must have GAINED that coverage, not merely be permanently
        # red. G5 FAILs on every shape in this matrix, so "is currently FAIL"
        # would let a future waiver point at a cell that never caught anything
        # and never will — compensation by coincidence.
        assert _strength(c_post) > _strength(c_pre), (
            f"{shape}/{gid} names {compensator} as compensating, but "
            f"{compensator} went {c_pre} -> {c_post}: it did not GAIN coverage "
            "on this shape, so it cannot be what makes the loss acceptable."
        )


#: The matrix's committed dimensions. Deleting a shape or a surface from BOTH
#: catalogs at once is otherwise invisible — every consistency check still
#: passes on the smaller set, and coverage silently shrinks. Changing these
#: numbers must be a deliberate, reviewed act.
_EXPECTED_SHAPES = 14
_EXPECTED_SURFACES = 10


def test_matrix_dimensions_are_pinned() -> None:
    """Coverage cannot shrink by deletion.

    Every other check here is *relative* — SHAPES vs MATRIX, waivers vs
    transitions — so removing a row or a column from both sides keeps them all
    green while the matrix measures strictly less. Pinning the absolute
    dimensions is what makes a deletion visible.
    """
    surfaces = {gid for gates in MATRIX.values() for gid in gates}
    assert len(MATRIX) == _EXPECTED_SHAPES, (
        f"{len(MATRIX)} shapes, expected {_EXPECTED_SHAPES} — if this is a "
        "deliberate addition or removal, update _EXPECTED_SHAPES in the same commit"
    )
    assert len(surfaces) == _EXPECTED_SURFACES, f"{len(surfaces)} surfaces, expected {_EXPECTED_SURFACES} — same rule"


def test_every_recorded_shape_is_actually_exercised() -> None:
    """``MATRIX`` and ``SHAPES`` must name the same shapes.

    ``test_verdicts_match_the_recorded_matrix`` parametrises over ``SHAPES``,
    so a shape recorded in ``MATRIX`` but missing from ``SHAPES`` is **never
    run** — its verdicts sit in the table looking authoritative while nothing
    checks them, and its transitions are still consulted by the loss rule.
    That is a matrix cell that asserts nothing, which is the same defect this
    whole PR is about, one level up.
    """
    assert set(MATRIX) == set(SHAPES), {
        "recorded but never exercised": sorted(set(MATRIX) - set(SHAPES)),
        "exercised but not recorded": sorted(set(SHAPES) - set(MATRIX)),
    }


def test_every_shape_records_every_surface() -> None:
    """No surface may be silently absent from a shape's row.

    A missing cell is not a neutral omission: the loss rule iterates the
    recorded transitions, so an unrecorded (shape, surface) pair simply never
    gets checked for narrowing.
    """
    surfaces = {gid for gates in MATRIX.values() for gid in gates}
    for shape, gates in MATRIX.items():
        assert set(gates) == surfaces, f"{shape} is missing {surfaces - set(gates)}"


def test_no_stale_justifications() -> None:
    """Every signed loss must correspond to a loss that is actually recorded.

    A justification whose transition no longer exists — because the verdict
    changed and the entry was left behind — is a standing licence for a FUTURE
    narrowing at that exact (shape, surface) to pass unnoticed. Removing the
    entry has to be part of fixing the transition.
    """
    real_losses = {(shape, gid, pre, post) for shape, gid, pre, post in _transitions() if _is_loss(pre, post)}
    stale = set(JUSTIFIED_LOSSES) - real_losses
    assert not stale, (
        "justification recorded for a transition that is not a loss — delete it, "
        f"or it silently pre-authorises a future one: {sorted(stale)}"
    )


def test_justifications_are_not_placeholders() -> None:
    """A one-word reason is not a reason. Force the signature to be real."""
    for key, (_compensator, why) in JUSTIFIED_LOSSES.items():
        assert len(why) > 120, f"{key}: justification too thin to review"


def test_the_degraded_shapes_are_caught_where_they_used_to_pass() -> None:
    """The affirmative case, and the reason this PR exists.

    Stated as a measurement rather than an argument: on every landed-degraded
    shape, at least one surface moves PASS -> FAIL. Before this PR a row that
    moved money with unmeasured amounts sailed through; now it is caught.
    """
    for shape in ("landed_degraded", "landed_degraded_no_gas", "landed_degraded_no_subtx"):
        gained = [gid for gid, (pre, post) in MATRIX[shape].items() if pre in ("PASS", "SKIP") and post == "FAIL"]
        assert gained, f"{shape} gained no coverage — the sweep did nothing here"


def test_no_surface_crashes_any_more() -> None:
    """Pre-PR, two surfaces ABORTED on a malformed-JSON row, taking the whole
    run with them. A crash is not a verdict; the report must survive bad data."""
    for shape, gid, _pre, post in _transitions():
        assert not post.startswith("CRASH"), f"{shape}/{gid} still crashes: {post}"
