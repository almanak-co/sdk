"""The ``success``-reader sweep for VIB-6043 leg 2, at the QA/gate layer.

The write-time guard flips a would-be-``success=1`` ledger row whose money
columns were never measured to ``success=0`` plus an ``accounting_degraded:``
marker. That is more honest per-row, but it moves the row **out of the
population every downstream reader keyed on ``success``** — so each such reader
is a potential false green.

``accountant_test.py`` was swept in the same PR (``_row_landed``). These two
scripts read the *same* SQLite files asking the *same* question and were not,
which is what this module covers:

* ``scripts/qa/score_demos_anvil.py`` — the 7 Quant Questions demo scorer.
  **This one actively regressed**: a degraded swap was skipped by the very
  check that looks for unmeasured amounts, flipping Q1/Q2 PARTIAL -> PASS.
* ``scripts/ci/accounting_regression_assert.py`` — the operator ship-gate.
  G3/G8/G9 measured the ``success=1`` population and G8 returned a *passing*
  result on an empty one (the VIB-6146 vacuous-green shape).

Every assertion here is mutation-verified: each test states, in its docstring
or via an explicit control case, what it would have reported against the
pre-sweep predicate. An assertion that holds on both the fixed and the broken
code would not be a test.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sqlite3
from pathlib import Path

import pytest

import scripts.ci.accounting_regression_assert as gate
from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_script(basename: str):
    """Import a ``scripts/qa/*.py`` module (no ``__init__.py`` in that dir)."""
    path = _REPO_ROOT / "scripts" / "qa" / f"{basename}.py"
    spec = importlib.util.spec_from_file_location(f"_{basename}", path)
    assert spec and spec.loader, path
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


scorer = _load_script("score_demos_anvil")


def _literal_constant(rel_path: str, name: str) -> str:
    """Read a module-level string constant WITHOUT importing the module.

    ``run_accounting_matrix`` is a harness with import-time side effects, so
    executing it to read one constant would be both slow and unsafe in a unit
    test. Parsing the AST gets the pinned literal with no execution at all.
    """
    tree = ast.parse((_REPO_ROOT / rel_path).read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if name in targets and isinstance(node.value, ast.Constant):
                return node.value.value
    raise AssertionError(f"{name} not found as a module-level literal in {rel_path}")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

_LEDGER_DDL = """
CREATE TABLE transaction_ledger (
  id INTEGER PRIMARY KEY, timestamp TEXT, deployment_id TEXT, cycle_id TEXT,
  intent_type TEXT, token_in TEXT, amount_in TEXT, token_out TEXT, amount_out TEXT,
  gas_used INTEGER, gas_usd TEXT, tx_hash TEXT, success INTEGER, error TEXT,
  slippage_bps REAL, effective_price TEXT,
  extracted_data_json TEXT, price_inputs_json TEXT,
  pre_state_json TEXT, post_state_json TEXT
);
"""

_OTHER_TABLES_DDL = """
CREATE TABLE accounting_outbox (id INTEGER PRIMARY KEY, status TEXT);
CREATE TABLE accounting_events (id INTEGER PRIMARY KEY, event_type TEXT, payload_json TEXT,
    confidence TEXT, unavailable_reason TEXT);
CREATE TABLE position_events (id INTEGER PRIMARY KEY, event_type TEXT, value_usd TEXT,
    position_id TEXT, position_type TEXT, token0 TEXT, token1 TEXT, amount0 TEXT, amount1 TEXT,
    gas_usd TEXT, tick_lower INTEGER, tick_upper INTEGER, liquidity TEXT,
    in_range INTEGER, attribution_json TEXT, extra_json TEXT);
CREATE TABLE portfolio_snapshots (id INTEGER PRIMARY KEY, total_value_usd TEXT,
    available_cash_usd TEXT, deployed_capital_usd TEXT, confidence TEXT,
    unavailable_reason TEXT, value_confidence TEXT, positions_json TEXT);
CREATE TABLE strategy_state (id INTEGER PRIMARY KEY, state_data TEXT);
CREATE TABLE teardown_requests (id INTEGER PRIMARY KEY, status TEXT,
    positions_closed INTEGER, positions_total INTEGER, started_at TEXT, updated_at TEXT);
"""

_SUBTX = '{"sub_transactions":[{"tx_hash":"%s","gas_used":21000,"status":"success","role":"ACTION"}]}'

_HASH_A = "0x" + "a" * 64
_HASH_B = "0x" + "b" * 64
_HASH_C = "0x" + "c" * 64


def _conn(tmp_path: Path, *, extra_tables: bool = False) -> sqlite3.Connection:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "almanak_state.db"
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    con.executescript(_LEDGER_DDL)
    if extra_tables:
        con.executescript(_OTHER_TABLES_DDL)
    return con


def _insert(
    con: sqlite3.Connection,
    *,
    rid: int,
    intent_type: str = "SWAP",
    amount_in: str = "100.0",
    amount_out: str = "0.03",
    gas_usd: str = "0.42",
    tx_hash: str = _HASH_A,
    success: int = 1,
    error: str = "",
    extracted: str | None = None,
    slippage_bps: float | None = None,
    effective_price: str | None = None,
) -> None:
    con.execute(
        "INSERT INTO transaction_ledger (id, timestamp, intent_type, "
        "token_in, amount_in, token_out, amount_out, gas_used, gas_usd, "
        "tx_hash, success, error, extracted_data_json, "
        "slippage_bps, effective_price) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            rid,
            f"2026-01-01T00:0{rid}:00",
            intent_type,
            "USDC",
            amount_in,
            "WETH",
            amount_out,
            210000,
            gas_usd,
            tx_hash,
            success,
            error,
            extracted if extracted is not None else _SUBTX % tx_hash,
            slippage_bps,
            effective_price,
        ),
    )
    con.commit()


def _degraded_error(reason: str = "unmeasured_money_slots") -> str:
    return f"{DEGRADED_PREFIX}{reason}: amount_in,amount_out"


# ---------------------------------------------------------------------------
# A. the mirrored constant
# ---------------------------------------------------------------------------


#: Every module permitted to answer "did this transaction land?" or "is this
#: row degraded?", with the ``ledger_guard`` symbol it must resolve to.
#:
#: An explicit registry, not an inferred one. Three duplicate predicates hid
#: for a whole review cycle precisely because "who are the readers?" was not
#: answerable — it had to be re-derived by grep each time, and grep missed.
LANDED_READERS: dict[str, str] = {
    "almanak.framework.accounting.accountant_test": "_landed_rule",
    "almanak.framework.accounting.reporting.swap_class_fallback": "_landed_rule",
}

#: **BLIND SPOT — read before trusting this registry.**
#:
#: Everything below is a *Python* guard: the AST matcher parses ``.py`` files
#: and the identity check imports modules. **Shell callers are structurally
#: invisible to it.** ``scripts/ci/accounting_regression_check.sh`` queries
#: ``transaction_ledger`` directly with ``sqlite3``, and for a full review
#: cycle it polled ``success=1`` for the entry intent — the hang/false-red twin
#: of the ``run_accounting_matrix`` readiness bug — while every check here
#: stayed green. That shell now *renders* the predicate from ``ledger_guard``
#: instead of inlining one, but nothing in this file would notice if it
#: stopped.
#:
#: So: a green run of this module means "no PYTHON reader re-implements the
#: rule". It does not mean "no reader re-implements the rule". Any new
#: non-Python consumer of ``transaction_ledger.success`` needs its own check,
#: and ``test_shell_harness_does_not_inline_the_predicate`` below is the
#: minimum version of one.

#: Modules that legitimately hold the SQL form rather than the Python one.
LANDED_SQL_READERS = (
    "scripts/ci/accounting_regression_assert.py",
    "scripts/qa/run_accounting_matrix.py",
    "almanak/framework/accounting/accountant_test.py",
)

#: A ``success`` comparison against a literal, in any spacing SQL permits.
#: Deliberately NOT anchored to ``OR`` — the whole point is that a lone
#: ``success=1`` in one reverted call site is the invisible case.
_SUCCESS_COMPARISON = re.compile(r"\bsuccess\s*(?:=|==|IS)\s*[01]\b", re.IGNORECASE)


def _sql_string_literals(tree: ast.AST) -> list[str]:
    """Every string constant in ``tree`` except docstrings.

    Docstrings are excluded because these modules legitimately discuss
    ``success=1`` in prose while querying via ``landed_sql()``; a raw-text
    scan would fire on the explanation of the very rule it is enforcing.

    f-string segments ARE included: ``ast.JoinedStr`` holds its literal parts
    as ``Constant`` nodes, so replacing ``{landed_sql()}`` with an inline
    ``success=1`` is caught.
    """
    docstring_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                docstring_ids.add(id(first.value))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstring_ids
    ]


def _ledger_guard_aliases(tree: ast.AST) -> set[str]:
    """Every local name bound to a ``ledger_guard`` import in this module.

    Without this, aliasing defeats the matcher: ``from ...ledger_guard import
    DEGRADED_PREFIX as _P`` then ``x.startswith(_P)`` looks like an unrelated
    variable. Resolving the module's own imports is what makes the guard
    immune to renaming — a name-based matcher is only as good as the names it
    knows about, so it has to learn them from the file.
    """
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "ledger_guard" in node.module:
            aliases |= {a.asname or a.name for a in node.names}
    return aliases


def _local_landed_predicates(tree: ast.AST) -> list[int]:
    """Line numbers of expressions that RE-IMPLEMENT the landed/degraded rule.

    Structural, not textual. Matches ``<anything>.startswith(<anything>)``
    where the argument mentions a degraded-prefix-ish name — the shape of a
    hand-rolled marker test — regardless of quoting, concatenation, f-strings,
    or whether the prefix came from the shared constant. A text search sees
    none of those; the AST sees all of them.
    """
    hits: list[int] = []
    aliases = _ledger_guard_aliases(tree)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "startswith"):
            continue
        for arg in node.args:
            names = {n.id for n in ast.walk(arg) if isinstance(n, ast.Name)}
            names |= {n.attr for n in ast.walk(arg) if isinstance(n, ast.Attribute)}
            consts = {c.value for c in ast.walk(arg) if isinstance(c, ast.Constant) and isinstance(c.value, str)}
            # Substring both ways: the whole prefix in one literal, OR a piece
            # of it (``"accounting_degraded" + ":"``). Concatenation is the
            # cheapest way past a naive literal comparison.
            marker_literal = any(c and (c in DEGRADED_PREFIX or DEGRADED_PREFIX in c) and len(c) > 4 for c in consts)
            if names & aliases or any("DEGRADED_PREFIX" in n for n in names) or marker_literal:
                hits.append(node.lineno)
    return hits


def test_no_module_re_implements_the_landed_rule() -> None:
    """AST guard: the rule may be *called*, never re-built locally.

    Replaces a text search for ``= "accounting_degraded:"``. That search
    policed the CONSTANT only, and passed while three modules carried their own
    PREDICATE — which had already drifted into three different semantics
    (``== 1`` / ``bool(...)`` / ``is True``). A stronger regex would still be a
    regex; single quotes, concatenation, an f-string, or a helper built on the
    imported constant all defeat it. None of them defeat this.
    """
    offenders: list[str] = []
    for path in (*(_REPO_ROOT / "almanak").rglob("*.py"), *(_REPO_ROOT / "scripts").rglob("*.py")):
        if path.name == "ledger_guard.py":
            continue  # the one home
        try:
            tree = ast.parse(path.read_text(), filename=str(path))
        except SyntaxError:  # pragma: no cover - not our file to fix
            continue
        for lineno in _local_landed_predicates(tree):
            offenders.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}")
    assert not offenders, (
        "the landed/degraded rule was re-implemented instead of imported from "
        f"ledger_guard.landed()/degraded(): {offenders}"
    )


def test_the_ast_guard_can_actually_see_a_violation() -> None:
    """Anti-vacuity: prove the matcher fires on every spelling it must catch.

    A guard that matches nothing passes for free. Each source below is a real
    way someone would re-implement the rule, including the ones a text search
    misses.
    """
    spellings = {
        "double quotes": 'x.startswith("accounting_degraded:")',
        "single quotes": "x.startswith('accounting_degraded:')",
        "shared constant": "x.startswith(DEGRADED_PREFIX)",
        "aliased constant": "x.startswith(_LEDGER_DEGRADED_PREFIX)",
        "module attribute": "x.startswith(ledger_guard.DEGRADED_PREFIX)",
        "f-string": 'x.startswith(f"{DEGRADED_PREFIX}")',
        "concatenation": 'x.startswith("accounting_degraded" + ":")',
        "aliased import": (
            "from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX as _P\nx.startswith(_P)"
        ),
    }
    for label, src in spellings.items():
        assert _local_landed_predicates(ast.parse(src)), f"matcher is blind to {label}"

    # and it must NOT fire on unrelated startswith calls
    assert not _local_landed_predicates(ast.parse('x.startswith("0x")'))
    assert not _local_landed_predicates(ast.parse("path.startswith(prefix)"))


def test_every_registered_reader_resolves_to_the_shared_rule() -> None:
    """Presence of an import is not enough — assert IDENTITY.

    The drift that started this existed between two modules that both *looked*
    consolidated. Only object identity proves a reader cannot have its own
    semantics.
    """
    import importlib

    from almanak.framework.accounting.ledger_guard import landed

    for module_path, attr in LANDED_READERS.items():
        mod = importlib.import_module(module_path)
        assert hasattr(mod, attr), f"{module_path} lost its {attr} binding"
        assert getattr(mod, attr) is landed, (
            f"{module_path}.{attr} is not ledger_guard.landed — a reader has re-acquired its own predicate"
        )


def test_shell_harness_does_not_inline_the_predicate() -> None:
    """The one non-Python reader, checked textually because it must be.

    An AST matcher cannot parse bash. This is a deliberately narrow, explicit
    check for the single shell file that reads ``transaction_ledger`` — not a
    general solution, and the blind-spot note above says so. It exists because
    this file's polls silently asked for ``success=1`` through six audit
    rounds: a landed-but-degraded entry intent never satisfied the wait, so the
    harness burned its timeout, killed a healthy strategy, and reported that
    the intent never landed.
    """
    src = (_REPO_ROOT / "scripts/ci/accounting_regression_check.sh").read_text()
    assert "landed_sql" in src, "the shell harness must RENDER the predicate from ledger_guard, not carry its own"
    offenders = [
        f"{i}: {line.strip()}"
        for i, line in enumerate(src.splitlines(), 1)
        if "success=1" in line and not line.strip().startswith("#")
    ]
    assert not offenders, f"the shell harness inlines a success=1 filter again: {offenders}"


def test_sql_readers_use_the_shared_fragment_not_their_own() -> None:
    """The SQL form has the same one-home rule as the Python form.

    ``run_accounting_matrix`` inlined it twice, in a readiness poll where drift
    does not answer wrong — it HANGS, because a degraded LP_CLOSE that landed
    never satisfies a ``success=1`` wait.
    """
    for rel in LANDED_SQL_READERS:
        src = (_REPO_ROOT / rel).read_text()
        assert "landed_sql(" in src, f"{rel} does not use the shared SQL fragment"
        assert "success=1 OR" not in src, f"{rel} still inlines the SQL predicate"

        # PER-CALL-SITE, not per-file. The two assertions above are file-level
        # substrings, so reverting ONE of several call sites to a literal is
        # invisible to them: ``landed_sql(`` still appears (from the sites left
        # alone) and a bare ``success=1`` carries no ``OR`` suffix. Measured
        # before this was added — reverting a single ``{landed_sql()}`` in
        # run_accounting_matrix.py to ``success=1`` left the suite fully green.
        #
        # That is the readiness poll whose own docstring says drift there does
        # not answer wrong, it HANGS: a degraded LP_CLOSE that landed never
        # satisfies a ``success=1`` wait, so the harness burns its deadline and
        # reports a healthy strategy as "did not land".
        #
        # Scan SQL string literals rather than raw text so prose is exempt —
        # several of these files legitimately DISCUSS ``success=1`` in comments
        # and docstrings. f-string literal segments are ``Constant`` nodes too,
        # so a ``{landed_sql()}`` swapped for an inline literal is caught.
        inlined = [
            literal
            for literal in _sql_string_literals(ast.parse(src, filename=rel))
            if _SUCCESS_COMPARISON.search(literal)
        ]
        assert not inlined, (
            f"{rel} inlines a success comparison in SQL instead of calling landed_sql(); "
            f"every ledger-population query must use the one shared fragment. Found: {inlined}"
        )


def test_landed_sql_prefix_match_is_not_a_like_wildcard(tmp_path: Path) -> None:
    """``_`` is a single-char LIKE wildcard and the prefix contains one.

    A naive ``LIKE 'accounting_degraded:%'`` also matches ``accounting-degraded:``
    and ``accountingXdegraded:``. Those are not our marker, and treating a
    foreign error string as "landed" would pull genuinely-reverted rows into
    the measured population. Mutation control: swapping ``substr`` for ``LIKE``
    makes the hyphen row match and this test fail.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1, success=0, error="accounting-degraded:not_ours: x")
    _insert(con, rid=2, success=0, error="accountingXdegraded:not_ours: x")
    _insert(con, rid=3, success=0, error=_degraded_error())

    landed = con.execute(
        f"SELECT id FROM transaction_ledger WHERE {gate.landed_sql()}",
        gate.LANDED_PARAMS,
    ).fetchall()
    assert [r["id"] for r in landed] == [3]


def test_landed_sql_excludes_a_plain_reverted_row(tmp_path: Path) -> None:
    """A real revert must stay OUT — the sweep widens the population, it does
    not abolish it. If everything counted as landed, G3/G8 would start failing
    on reverted rows that legitimately have no gas or sub-transactions."""
    con = _conn(tmp_path)
    _insert(con, rid=1, success=0, error="execution reverted: STF")
    _insert(con, rid=2, success=0, error="")
    assert (
        con.execute(
            f"SELECT COUNT(*) FROM transaction_ledger WHERE {gate.landed_sql()}",
            gate.LANDED_PARAMS,
        ).fetchone()[0]
        == 0
    )


# ---------------------------------------------------------------------------
# B. GateResult / SKIP semantics
# ---------------------------------------------------------------------------


def test_skipped_gate_is_not_green() -> None:
    """A gate that inspected nothing has not verified its invariant.

    Pre-sweep, G8's empty-population branch returned ``passed=True``, which
    ``status`` rendered as PASS — indistinguishable in the report from a gate
    that actually checked rows.
    """
    r = gate._skipped("G8", "Sub-transactions stamped", "skipped: no landed rows")
    assert r.status == "SKIP"
    assert r.passed is False


def test_skip_outranks_xfail() -> None:
    """A known-blocked gate that also had nothing to inspect reports SKIP.

    XPASS on an empty population would read as "the blocked gate started
    working", which is a stronger and wronger claim than "nothing ran".
    """
    r = gate.GateResult("L4", "n", passed=True, detail="", xfail=True, skipped=True)
    assert r.status == "SKIP"


# ---------------------------------------------------------------------------
# C. G3 — gas tracking
# ---------------------------------------------------------------------------


def test_g3_fails_on_a_degraded_row_missing_gas(tmp_path: Path) -> None:
    """A degraded row burned real gas, so missing ``gas_usd`` is a true defect.

    Mutation control below: the same DB under the pre-sweep ``success=1``
    predicate finds zero rows and reports PASS.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1, success=0, error=_degraded_error(), gas_usd="")

    assert gate.gate_gas_tracking(con).status == "FAIL"

    pre_sweep = con.execute(
        "SELECT id FROM transaction_ledger WHERE success=1 AND (gas_usd IS NULL OR gas_usd='')"
    ).fetchall()
    assert pre_sweep == [], "control: the old predicate saw no offender — hence the sweep"


def test_g3_skips_rather_than_passes_on_an_empty_ledger(tmp_path: Path) -> None:
    """Zero rows means zero evidence. PASS here is the vacuous green."""
    con = _conn(tmp_path)
    res = gate.gate_gas_tracking(con)
    assert res.status == "SKIP"
    assert "ledger is empty" in res.detail


def test_g3_still_passes_a_clean_measured_row(tmp_path: Path) -> None:
    """The sweep must not manufacture failures on healthy data.

    Note on what this does and does not prove: a clean ``success=1`` row is in
    BOTH the old and new populations, so the PASS itself does not discriminate
    the predicate — only the detail wording does. It is a guard against
    over-reach, not a sweep test. The population semantics are pinned by
    ``test_g3_fails_on_a_degraded_row_missing_gas`` and its control case.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1)
    res = gate.gate_gas_tracking(con)
    assert res.status == "PASS"
    assert "1 landed tx" in res.detail


def test_g3_counts_a_degraded_row_in_its_measured_population(tmp_path: Path) -> None:
    """Discriminates on the COUNT, not on the wording of the detail string.

    One clean row plus one properly-measured degraded row: the old predicate
    sees 1, the new one sees 2. Unlike the test above, this fails against the
    pre-sweep predicate for a semantic reason rather than a cosmetic one.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1)
    _insert(con, rid=2, tx_hash=_HASH_B, success=0, error=_degraded_error())
    res = gate.gate_gas_tracking(con)
    assert res.status == "PASS"
    assert "2 landed tx" in res.detail


# ---------------------------------------------------------------------------
# D. G8 — sub-transactions, and the vacuous-green shape it used to have
# ---------------------------------------------------------------------------


def test_g8_no_longer_reports_pass_when_it_checked_nothing(tmp_path: Path) -> None:
    """The exact VIB-6146 shape: every row degraded => ``success=1`` count 0.

    Pre-sweep this returned ``GateResult(..., True, "skipped: ...")`` — a PASS.
    Post-sweep the degraded rows ARE the landed population, so the gate
    actually evaluates them and finds the missing array.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1, success=0, error=_degraded_error(), extracted="{}")

    assert con.execute("SELECT COUNT(*) FROM transaction_ledger WHERE success=1").fetchone()[0] == 0, (
        "control: pre-sweep population is empty, which is what made it skip"
    )

    res = gate.gate_sub_transactions(con)
    assert res.status == "FAIL"
    assert "missing sub_transactions" in res.detail


def test_g8_skip_distinguishes_empty_ledger_from_all_reverted(tmp_path: Path) -> None:
    """SKIP must stay actionable: "harness never ran" and "everything
    reverted" need different operator responses, so the detail says which."""
    empty_res = gate.gate_sub_transactions(_conn(tmp_path / "a"))
    assert empty_res.status == "SKIP"
    assert "ledger is empty" in empty_res.detail

    reverted = _conn(tmp_path / "b")
    _insert(reverted, rid=1, success=0, error="execution reverted")
    reverted_res = gate.gate_sub_transactions(reverted)
    assert reverted_res.status == "SKIP"
    assert "among 1 ledger rows" in reverted_res.detail
    assert "ledger is empty" not in reverted_res.detail


def test_g8_passes_when_a_degraded_row_is_properly_stamped(tmp_path: Path) -> None:
    """Being degraded is not itself a G8 offence — G8 asks about sub-tx
    stamping. A degraded row that stamped correctly must pass, or the sweep
    would make ``accounting_degraded`` a self-fulfilling gate failure.

    The detail string is asserted, not just the status: at HEAD this same DB
    also reported PASS, but *vacuously* — via the ``success_count == 0`` skip,
    having inspected nothing. Status alone cannot tell those two apart, so an
    assertion on status alone would pass against both the fixed and the broken
    code and would not be a test.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1, success=0, error=_degraded_error())
    res = gate.gate_sub_transactions(con)
    assert res.status == "PASS"
    assert res.detail.startswith("1 landed rows:"), (
        "PASS must be backed by an inspected row, not by an empty population"
    )


def test_g8_parent_hash_check_spans_degraded_rows(tmp_path: Path) -> None:
    """The parent/ACTION mismatch check must also see degraded rows."""
    con = _conn(tmp_path)
    _insert(
        con,
        rid=1,
        success=0,
        error=_degraded_error(),
        tx_hash=_HASH_A,
        extracted=_SUBTX % _HASH_B,  # parent points at no ACTION sub-tx
    )
    res = gate.gate_sub_transactions(con)
    assert res.status == "FAIL"
    assert "matches no ACTION sub-tx" in res.detail


# ---------------------------------------------------------------------------
# D2. G5 — the teardown ledger check that had never run
# ---------------------------------------------------------------------------


def test_g5_teardown_ledger_check_actually_executes(tmp_path: Path) -> None:
    """G5's per-row teardown check was dead code, and this is the only test
    that would have noticed.

    It reads ``started_at`` from the ``teardown_requests`` row and guards with
    ``"started_at" in tr.keys()``. The projection listed only
    ``status``/``positions_*``, so the guard was ALWAYS false and the gas_usd /
    pre_state_json / post_state_json check — the VIB-3918 regression it exists
    to catch — never executed once.

    This also pins the parameter ordering of the one landed query that carries
    an extra positional argument, ``{"started_at": ..., **LANDED_PARAMS}``. A mis-ordered
    tuple there would not raise; it would silently mis-scope the gate. Three
    rows discriminate all three behaviours at once.
    """
    con = _conn(tmp_path, extra_tables=True)
    con.execute(
        "INSERT INTO teardown_requests (status, positions_closed, positions_total,"
        " started_at, updated_at) VALUES ('completed',1,1,?,?)",
        ("2026-01-01T00:00:00", "2026-01-01T01:00:00"),
    )
    # Every row carries a tx_hash: (a) and (b) are REAL degraded teardown legs
    # (a Safe/bundle close that confirmed and whose parser yielded no amounts),
    # and (c) reverted on-chain, which still mines a transaction. Row (d) is the
    # no-op — same marker, no hash, nothing submitted.
    #
    # (a) degraded, inside the teardown window, missing pre_state_json -> caught
    con.execute(
        "INSERT INTO transaction_ledger (id, timestamp, intent_type, gas_usd,"
        " tx_hash, pre_state_json, post_state_json, success, error) "
        "VALUES (1,'2026-01-01T00:30:00','LP_CLOSE','0.42',?,'','{}',0,?)",
        (_HASH_A, _degraded_error()),
    )
    # (b) degraded but BEFORE teardown started -> excluded by the timestamp bound
    con.execute(
        "INSERT INTO transaction_ledger (id, timestamp, intent_type, gas_usd,"
        " tx_hash, pre_state_json, post_state_json, success, error) "
        "VALUES (2,'2025-12-01T00:00:00','SWAP','',?,NULL,NULL,0,?)",
        (_HASH_B, _degraded_error()),
    )
    # (c) genuinely reverted, inside the window -> excluded by the landed predicate
    con.execute(
        "INSERT INTO transaction_ledger (id, timestamp, intent_type, gas_usd,"
        " tx_hash, pre_state_json, post_state_json, success, error) "
        "VALUES (3,'2026-01-01T00:40:00','SWAP','',?,NULL,NULL,0,'execution reverted')",
        (_HASH_C,),
    )
    # (d) NO-OP: the marker with no hash, inside the window, and missing both
    # state snapshots. Excluded because nothing was submitted — a teardown leg
    # that never happened cannot be missing a pre-state for it.
    con.execute(
        "INSERT INTO transaction_ledger (id, timestamp, intent_type, gas_usd,"
        " tx_hash, pre_state_json, post_state_json, success, error) "
        "VALUES (4,'2026-01-01T00:45:00','LP_CLOSE','','',NULL,NULL,0,?)",
        (_degraded_error(),),
    )
    con.commit()

    detail = gate.gate_teardown_trail(con, "lp").detail

    assert "teardown ledger 1(LP_CLOSE) missing ['pre_state_json']" in detail
    assert "ledger 2(" not in detail, "timestamp bound not applied — wrong param order?"
    assert "ledger 3(" not in detail, "a reverted row leaked into the landed population"
    assert "ledger 4(" not in detail, (
        "a no-op (marker, no tx_hash) leaked into the landed population — the "
        "gate would report a teardown leg that was never submitted as missing "
        "the state snapshots it could not have"
    )


# ---------------------------------------------------------------------------
# E. G9 — slippage source
# ---------------------------------------------------------------------------


def test_g9_spans_degraded_rows(tmp_path: Path) -> None:
    """A degraded swap with a bogus slippage_source is still a bad row."""
    con = _conn(tmp_path)
    _insert(
        con,
        rid=1,
        success=0,
        error=_degraded_error(),
        extracted='{"swap_amounts":{"slippage_source":"MADE_UP"}}',
    )
    assert gate.gate_slippage_source(con).status == "FAIL"

    assert (
        con.execute(
            "SELECT COUNT(*) FROM transaction_ledger WHERE success=1 "
            "AND json_extract(extracted_data_json,'$.swap_amounts') IS NOT NULL"
        ).fetchone()[0]
        == 0
    ), "control: the old predicate could not see this row"


def test_g9_skips_rather_than_passes_on_an_empty_population(tmp_path: Path) -> None:
    """G9 was left reporting a vacuous PASS while the commit claimed otherwise.

    G3 and G8 got the SKIP treatment; G9 did not, so an empty or fully-reverted
    ledger produced ``PASS :: slippage_source consistent with slippage_bps`` —
    a gate announcing an invariant it had never evaluated. Found by an external
    auditor after the claim had already been written into the commit message
    and the PR body, which is exactly why the claim needed a test.
    """
    empty = _conn(tmp_path / "empty")
    res = gate.gate_slippage_source(empty)
    assert res.status == "SKIP"
    assert "ledger is empty" in res.detail

    # rows exist, but none are landed swap rows
    reverted = _conn(tmp_path / "reverted")
    _insert(reverted, rid=1, success=0, error="execution reverted")
    assert gate.gate_slippage_source(reverted).status == "SKIP"


def test_g9_passes_only_when_backed_by_an_inspected_row(tmp_path: Path) -> None:
    """A PASS must name the population it inspected, so PASS and SKIP can
    never be confused in the report."""
    con = _conn(tmp_path)
    _insert(con, rid=1, extracted='{"swap_amounts":{"slippage_source":"RECEIPT_DECODED"}}')
    res = gate.gate_slippage_source(con)
    assert res.status == "PASS"
    assert res.detail.startswith("1 swap rows verified:")


def test_a_gate_that_explodes_is_reported_not_fatal(tmp_path: Path) -> None:
    """A malformed-JSON row must not abort the whole run.

    ``json_extract`` raises ``OperationalError: malformed JSON`` for the entire
    statement when any scanned row is unparsable. Unhandled, the operator gets
    a stack trace instead of a report and the remaining gates never execute —
    the same "the gate told you nothing" outcome the SKIP work exists to
    prevent, through a different door. Reproduced by an auditor against a
    fixture committed to this repo.

    FAIL rather than SKIP: an empty population is legitimate, a malformed row
    is a real defect in the data under test.
    """
    con = _conn(tmp_path, extra_tables=True)
    # LP6 reads position_events.attribution_json, which this PR did not touch
    # and which carries no json_valid guard — a realistic residual raiser.
    con.execute(
        "INSERT INTO position_events (id, event_type, position_type, position_id,"
        " token0, token1, value_usd, attribution_json) "
        "VALUES (1,'CLOSE','LP','p1','WETH','USDC','100','}{ not json')"
    )
    con.commit()

    with pytest.raises(sqlite3.OperationalError):
        gate.gate_lp_close_completeness(con)  # raw call still raises...

    res = gate._safe_gate(gate.gate_lp_close_completeness, con)  # ...wrapped, it reports
    assert res.status == "FAIL"
    assert res.gid == "LP6"
    assert "OperationalError" in res.detail

    # and the run as a whole survives: other gates still produce results
    assert gate._safe_gate(gate.gate_gas_tracking, con).status in {"PASS", "FAIL", "SKIP"}


def test_slippage_inconsistency_query_ignores_unparsable_rows(tmp_path: Path) -> None:
    """Unparsable JSON must not crash G9, and must not hide a good verdict."""
    con = _conn(tmp_path)
    _insert(con, rid=1, extracted='{"swap_amounts":{"slippage_source":"RECEIPT_DECODED"}}')
    _insert(con, rid=2, tx_hash=_HASH_B, success=0, error="execution reverted", extracted="}{ not json")
    res = gate.gate_slippage_source(con)
    assert res.status == "PASS"
    assert res.detail.startswith("1 swap rows verified:")


def test_g9_skip_cannot_suppress_a_real_inconsistency_failure(tmp_path: Path) -> None:
    """The empty-population SKIP must not swallow a FAIL that predates it.

    ``slippage_source=NONE`` with non-zero ``slippage_bps`` is a defect in the
    ROW — a connector emitting slippage without saying where it came from — and
    that query was universally scoped before this sweep. An earlier revision of
    my own fix returned SKIP *before* evaluating it, so a reverted row carrying
    the contradiction went FAIL -> SKIP: narrowing a gate while claiming only
    to be fixing vacuous greens. Caught by an auditor verifying the fix, not
    the original code.
    """
    con = _conn(tmp_path)
    _insert(
        con,
        rid=1,
        success=0,
        error="execution reverted",
        slippage_bps=50.0,
        extracted='{"swap_amounts":{"slippage_source":"NONE"}}',
    )
    res = gate.gate_slippage_source(con)
    assert res.status == "FAIL"
    assert "slippage_source=NONE but non-zero slippage_bps" in res.detail


def test_skip_detail_makes_no_causal_claim_about_a_subset_population(
    tmp_path: Path,
) -> None:
    """G9's population is a SUBSET of the ledger, so the "reverted or unmarked
    downgrade" clause cannot be asserted about the rows it did not consider.

    With one clean ``success=1 SUPPLY`` row, the first version of this message
    told the operator that row had "reverted or carries an unmarked framework
    downgrade". It had done neither.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1, intent_type="SUPPLY")
    detail = gate.gate_slippage_source(con).detail
    assert "among 1 ledger rows" in detail
    assert "reverted" not in detail
    assert "unmarked framework downgrade" not in detail


def test_g8_treats_unparsable_json_as_missing_not_as_a_crash(tmp_path: Path) -> None:
    """A row whose blob cannot be read has no usable sub_transactions array —
    which is what G8 looks for. Crashing loses the other nine gates; passing
    would be a false green."""
    con = _conn(tmp_path)
    _insert(con, rid=1)
    _insert(con, rid=2, tx_hash=_HASH_B, extracted="}{ not json")
    res = gate.gate_sub_transactions(con)
    assert res.status == "FAIL"
    assert "1 landed rows missing sub_transactions array" in res.detail


def test_empty_population_detail_does_not_claim_rows_reverted(tmp_path: Path) -> None:
    """A ``success=0`` row with no marker may be a slippage-breach downgrade
    that LANDED, not a revert. The SKIP text must not assert it reverted —
    that would be the same over-claim this module exists to fix, one level up.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1, success=0, error="Slippage circuit breaker: actual slippage 412 bps")
    detail = gate.gate_gas_tracking(con).detail
    assert "reverted" in detail  # it is still one of the possibilities
    assert "all 1 ledger rows reverted" not in detail
    assert "unmarked framework downgrade" in detail


# ---------------------------------------------------------------------------
# F. the report never lets SKIP read as verified
# ---------------------------------------------------------------------------


def test_main_warns_that_skipped_gates_asserted_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A skipped gate must never be silently folded into the failure tally.

    Two properties, both load-bearing for the report being honest:

    1. The empty-population gates render as SKIP, not PASS. Pre-sweep G8
       rendered PASS here — a gate that inspected zero rows was visually
       indistinguishable from one that inspected a clean run.
    2. Skips are counted and named *separately*, so "N failures" can never be
       read as "everything else was verified".
    """
    con = _conn(tmp_path, extra_tables=True)
    con.close()

    monkeypatch.setattr(
        "sys.argv",
        ["prog", "--strategy", "lp", "--db", str(tmp_path / "almanak_state.db")],
    )
    rc = gate.main()
    out = capsys.readouterr().out

    def _status_of(gid: str) -> str:
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[1] == gid:
                return parts[2]
        raise AssertionError(f"{gid} not in report:\n{out}")

    # Every universally-quantified gate over the ledger must SKIP here.
    # Enumerated explicitly rather than asserting a skip *count*: an earlier
    # version of this test asserted "2 skipped", which silently ratcheted in a
    # G9 that was still reporting a vacuous PASS. A count assertion turns a
    # missing fix into a passing test — the precise failure mode this file is
    # about. (Caught by an external auditor, not by this suite.)
    for gid in ("G3", "G8", "G9"):
        assert _status_of(gid) == "SKIP", f"{gid} must not report a verdict it did not test"

    # the tally separates them, and the failure count excludes them
    n_fail = sum(1 for ln in out.splitlines() if " FAIL " in ln)
    n_skip = sum(1 for ln in out.splitlines() if " SKIP " in ln)
    assert f"result: {n_fail} failures, {n_skip} skipped" in out
    assert "asserted NOTHING" in out
    assert "does NOT mean these invariants hold" in out

    # The warning is invisible to a machine. `... && ship` reads the exit code,
    # so a run containing SKIPs must not exit 0 — that is the vacuous green
    # surviving in machine-readable form. This DB also has real failures, so
    # FAIL(1) wins over SKIP(2); the skip-only case is covered below.
    assert rc == 1


def test_skips_alone_exit_nonzero_but_distinguishable_from_failures() -> None:
    """Exit 2 = "nothing failed, but something asserted nothing".

    0 would let a release script treat an uninspected gate as verified; 1 would
    make an incomplete harness run indistinguishable from an accounting
    regression. The third code is what keeps both true at once.
    """
    ok = gate.GateResult("G3", "n", passed=True, detail="")
    skip = gate._skipped("G8", "n", "skipped: ...")
    fail = gate.GateResult("G9", "n", passed=False, detail="")

    assert gate._exit_code([ok]) == 0
    assert gate._exit_code([ok, skip]) == 2
    assert gate._exit_code([ok, skip, fail]) == 1
    assert gate._exit_code([ok, fail]) == 1


# ---------------------------------------------------------------------------
# G. score_demos_anvil — the regression this PR actually introduced
# ---------------------------------------------------------------------------


def _score_one(tmp_path: Path, *, degraded_shape: bool) -> dict:
    """Two bookings of ONE on-chain reality: a clean swap plus a swap that
    moved money whose amounts were never measured."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "almanak_state.db"
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    con.executescript(_LEDGER_DDL)
    # a clean, fully-measured swap — Q2 needs slippage + effective price to
    # count a row as OK, so this row is what makes PARTIAL distinguishable
    # from FAIL when the second row is unmeasured.
    _insert(con, rid=1, tx_hash=_HASH_A, slippage_bps=5.0, effective_price="3333.3")
    if degraded_shape:
        _insert(
            con,
            rid=2,
            tx_hash=_HASH_B,
            amount_in="",
            amount_out="",
            success=0,
            error=_degraded_error(),
        )
    else:
        _insert(con, rid=2, tx_hash=_HASH_B, amount_in="", amount_out="", success=1)
    con.close()
    return scorer.score_db(db)


def test_degraded_booking_does_not_quieten_the_quant_questions(tmp_path: Path) -> None:
    """The regression, stated as an invariant.

    The same on-chain reality must score the same however the framework books
    it. Pre-sweep, ``if not r["success"]: continue`` skipped the degraded row
    inside the loop that checks for unmeasured amounts, so Q1 and Q2 went
    PARTIAL -> PASS purely because the guard downgraded the row.
    """
    pre = _score_one(tmp_path / "pre", degraded_shape=False)
    post = _score_one(tmp_path / "post", degraded_shape=True)

    for q in ("Q1", "Q2", "Q3"):
        assert pre[q] == post[q], f"{q} changed with the booking, not with reality"
    assert post["Q1"] == "PARTIAL"
    assert post["Q2"] == "PARTIAL"
    assert post["ledger_success"] == 2


def test_scorer_landed_helper_rejects_a_real_revert() -> None:
    """Widening to "landed" must not swallow genuine failures.

    Row shapes match what ``score_db``'s SELECT hands the helper — a mapping
    over the ledger columns — so ``tx_hash`` is present on every one, as it is
    on every row of the real table. The last two rows are the discrimination
    that matters: identical marker, and only the hash tells a Safe/bundle close
    that confirmed apart from a no-op that submitted nothing.
    """
    assert scorer._landed({"success": 0, "error": "execution reverted", "tx_hash": _HASH_A}) is False
    assert scorer._landed({"success": 0, "error": None, "tx_hash": _HASH_A}) is False
    assert scorer._landed({"success": 1, "error": "", "tx_hash": _HASH_A}) is True
    assert scorer._landed({"success": 0, "error": _degraded_error(), "tx_hash": _HASH_A}) is True
    assert scorer._landed({"success": 0, "error": _degraded_error(), "tx_hash": ""}) is False


def test_scorer_landed_helper_fails_loudly_when_the_error_column_is_absent() -> None:
    """A missing ``error`` column must RAISE, not silently mean "not landed".

    This replaces a test that asserted the opposite. The old ``_landed`` caught
    the lookup and returned ``False``, which an auditor showed was unreachable
    for its stated reason (``score_db``'s SELECT names ``error``, so an older
    DB dies at ``cur.execute`` long before this helper runs) and harmful for
    the reachable one: a future edit dropping ``error`` from the projection
    would degrade this helper to ``bool(row["success"])`` — the exact
    pre-sweep predicate — restoring the laundering bug with no error, no note,
    and a green suite.

    The old test also discriminated nothing: it passed under every mutation of
    the predicate, because it only ever exercised the fallback it was pinning.

    ``tx_hash`` is held to the same standard for the mirror-image reason: if a
    future projection drops it, a silent fallback would turn the degraded arm
    permanently off and every genuinely landed-but-degraded row — the lane the
    guard exists for — would quietly leave the scorer's population.
    """
    with pytest.raises((IndexError, KeyError)):
        scorer._landed({"success": 0, "tx_hash": _HASH_A})
    with pytest.raises((IndexError, KeyError)):
        scorer._landed({"success": 0, "error": _degraded_error()})


def test_scorer_landed_helper_matches_the_gate_predicate_on_the_same_row(
    tmp_path: Path,
) -> None:
    """The Python helper and the SQL predicate must agree row-for-row.

    They are two mirrors of one definition; a divergence means the demo
    scorer and the ship-gate disagree about what landed, which is worse than
    either being wrong alone because it looks like corroboration.

    Row 6 (``success=2``) is the case that caught them apart: the SQL says
    ``success=1`` while the helper used Python truthiness, so a non-boolean
    value landed in one mirror and not the other.

    Rows 7-9 are the ``tx_hash`` conjunct: same marker as row 2, no usable
    hash. SQLite's one-argument ``trim`` strips only spaces, so row 9's tab is
    the case that would have split the mirrors if ``landed_sql`` had used it.
    """
    con = _conn(tmp_path)
    _insert(con, rid=1)  # clean
    _insert(con, rid=2, success=0, error=_degraded_error())  # degraded
    _insert(con, rid=3, success=0, error="execution reverted")  # revert
    _insert(con, rid=4, success=0, error="accounting-degraded:nope")  # near-miss
    _insert(con, rid=5, success=0, error=None)  # no error
    _insert(con, rid=6, success=2, error="")  # non-boolean
    _insert(con, rid=7, success=0, error=_degraded_error(), tx_hash="")  # no-op
    _insert(con, rid=8, success=0, error=_degraded_error(), tx_hash=None)  # no-op, NULL
    _insert(con, rid=9, success=0, error=_degraded_error(), tx_hash="\t")  # no-op, blank

    sql_landed = {
        r["id"]
        for r in con.execute(
            f"SELECT id FROM transaction_ledger WHERE {gate.landed_sql()}",
            gate.LANDED_PARAMS,
        )
    }
    py_landed = {
        r["id"] for r in con.execute("SELECT id, success, error, tx_hash FROM transaction_ledger") if scorer._landed(r)
    }
    assert sql_landed == py_landed == {1, 2}
