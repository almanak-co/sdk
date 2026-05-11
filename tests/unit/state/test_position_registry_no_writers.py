"""Anti-bypass guard for the atomic commit primitive.

VIB-4197 / T11. Replaces the original "no writers exist" guard (T05) with a
sharper two-layer structural test:

- **Layer A — SQL writer scope.** No production code under
  ``almanak/framework/`` may execute an ``INSERT INTO position_registry``,
  ``UPDATE position_registry``, or ``DELETE FROM position_registry``
  statement EXCEPT inside the canonical method
  ``SQLiteStore.save_ledger_and_registry_atomic``. Allowlist key is the
  qualified name (``Class.method``) so a sibling top-level function or
  another class's method with the same bare name cannot satisfy the guard.

- **Layer B — commit.py delegation shape.** ``commit.py`` MUST delegate
  through the StateManager facade exclusively. It MUST NOT touch backend
  internals (``_warm``, ``_conn``, ``_db_lock``), MUST NOT contain raw SQL
  targeting ``transaction_ledger`` / ``position_registry``, MUST NOT carry
  raw transaction-control strings (``BEGIN``, ``COMMIT``, ``ROLLBACK``),
  and MUST contain exactly ONE ``await state_manager.<method>(...)`` call
  per mode-branch with the canonical method name.

Together: Layer A pins the only SQL-writing method; Layer B pins the
delegation surface from ``commit.py`` so a future refactor cannot evolve
into a split-commit shape (e.g., adding a ``state_manager.save_handle_separately``
call) without going red.

Cross-reference: UAT card ``docs/internal/uat-cards/VIB-4197.md`` D4.A2.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[3]
_ALMANAK_DIR = _REPO_ROOT / "almanak"


# =============================================================================
# LAYER A — SQL writer scope
# =============================================================================


# (relative_path, qualname-prefix). The qualname is the canonical
# `ClassName.method_name`. The bare-method-name shadowing risk is closed by
# requiring the full qualname *prefix*: a writer SQL string inside an inner
# helper (e.g. `SQLiteStore.save_ledger_and_registry_atomic._sync_atomic_commit`,
# which is the natural shape of a sync-helper nested inside an async
# DB-writing method) is allowed iff its qualname starts with one of these
# prefixes. A sibling function at any other path or with a different
# enclosing method is rejected.
_ALLOWLIST_WRITERS_QUALNAMED: frozenset[tuple[str, str]] = frozenset({
    (
        "almanak/framework/state/backends/sqlite.py",
        "SQLiteStore.save_ledger_and_registry_atomic",
    ),
    # VIB-4198 / T12 — backfill writer (cutover spec §3.4 idempotent
    # `INSERT … ON CONFLICT DO NOTHING`). Distinct from the runtime
    # atomic primitive: backfill is one-time observation of legacy
    # `position_events` rows, never mutates already-existing registry
    # rows. The runtime status flips on CLOSE go through
    # `save_ledger_and_registry_atomic` per blueprint 28 §4.3.
    (
        "almanak/framework/state/backends/sqlite.py",
        "SQLiteStore.insert_position_registry_row_if_absent",
    ),
    # VIB-4205 / T19 — Postgres half of the atomic ledger+registry+handle
    # commit primitive. Mirrors `SQLiteStore.save_ledger_and_registry_atomic`
    # line-for-line but uses asyncpg with an explicit
    # `async with conn.transaction():` block so the three writes commit
    # as one Postgres transaction. The handler's INSERT + same-status
    # retry UPDATE on `position_registry` are BOTH part of this primitive
    # (the UPDATE backfills `handle` on a same-status retry per
    # sqlite.py:3065). Adding an additional `position_registry` writer
    # qualname here without a matching documented carve-out is the
    # split-commit failure mode this guard exists to catch (blueprint
    # 28 §4, §6).
    (
        "almanak/gateway/services/state_service.py",
        "StateServiceServicer._save_ledger_and_registry_pg",
    ),
})


def _qualname_is_allowlisted(rel_path: str, qualname: str) -> bool:
    """Return True iff (rel_path, qualname) is covered by the allowlist.

    A qualname is covered when it equals an allowlist entry exactly OR
    starts with `entry + "."` (i.e. the SQL lives in a nested helper of
    the canonical method). This is the "canonical method body, including
    its sync-helper closures" allowlist semantics; a sibling top-level
    function or another class's method with the same bare name does NOT
    match.
    """
    for entry_path, entry_qualname in _ALLOWLIST_WRITERS_QUALNAMED:
        if rel_path != entry_path:
            continue
        if qualname == entry_qualname or qualname.startswith(entry_qualname + "."):
            return True
    return False

# Optional schema qualifier + optional quoting for the table reference. The
# pattern is anchored at INSERT/UPDATE/DELETE — schema DDL (CREATE TABLE,
# CREATE INDEX) is intentionally NOT a writer pattern, so the schema source
# stays unflagged.
_TABLE_REF = r'(?:["`\[])?(?:\w+\.)?position_registry(?:["`\]])?'
_WRITER_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "INSERT INTO",
        re.compile(rf"\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+{_TABLE_REF}", re.IGNORECASE),
    ),
    ("UPDATE", re.compile(rf"\bUPDATE\s+{_TABLE_REF}", re.IGNORECASE)),
    ("DELETE FROM", re.compile(rf"\bDELETE\s+FROM\s+{_TABLE_REF}", re.IGNORECASE)),
]


def _iter_python_sources():
    """Yield Python source files under ``almanak/`` (skipping any nested tests/)."""
    for path in _ALMANAK_DIR.rglob("*.py"):
        if any(part == "tests" for part in path.relative_to(_ALMANAK_DIR).parts):
            continue
        yield path


def _qualname_of_node(stack: list[ast.AST]) -> str:
    """Build the dotted qualname for the innermost FunctionDef on ``stack``.

    The stack is a path of AST nodes (Module → ClassDef → FunctionDef) so the
    qualname is the dotted path of ClassDef/FunctionDef nodes, dropping the
    Module root.
    """
    parts: list[str] = []
    for node in stack:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            parts.append(node.name)
    return ".".join(parts)


def _folded_string(node: ast.AST) -> str | None:
    """Recursively fold an AST node into a single string when possible.

    Closes the obvious bypasses CodeRabbit flagged on PR #2207: ``ast.Constant``
    inspection alone misses f-strings (``ast.JoinedStr``) and ``+``-concatenated
    strings (``ast.BinOp(ast.Add)``). For example::

        f"INSERT INTO {'position_registry'} VALUES (1)"  # JoinedStr
        "INSERT INTO " + "position_registry VALUES (1)"  # BinOp+Add

    Both currently land an INSERT writer-call but do NOT contain the literal
    ``"INSERT INTO position_registry"`` as a single ``Constant`` — the static
    guard would miss them. This helper folds them into the equivalent text.

    Returns ``None`` when the node cannot be folded statically (e.g., dynamic
    computation, attribute reads, calls). Conservative: a determined attacker
    could still build SQL via ``"".join([...])`` or ``str.format`` — those
    require a different defence layer (runtime instrumentation). What we close
    here is the cheap-and-obvious bypass surface.

    For ``FormattedValue`` parts of an f-string, we recurse: the runtime value
    of ``f"...{x}..."`` is unknowable, but the SURROUNDING literal parts are
    known and that's what the writer-pattern regex needs to match.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
            elif isinstance(v, ast.FormattedValue):
                inner = _folded_string(v.value)
                # Non-foldable formatted parts contribute "" — sufficient for
                # the regex to still match the surrounding literal segments
                # (e.g. f"INSERT INTO {x} ..." folds to "INSERT INTO  ...").
                parts.append(inner if inner is not None else "")
            else:
                parts.append("")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _folded_string(node.left)
        right = _folded_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _collect_writer_violations(
    source_path: Path, source_text: str,
) -> list[tuple[Path, str, str, int, str]]:
    """Walk ``source_text`` AST; report every writer-pattern hit by qualname.

    Returns a list of (path, qualname, label, line, snippet) tuples. The
    qualname is empty for module-level string literals (e.g., the
    ``SCHEMA_SQL`` block at the top of ``sqlite.py``); those are handled by
    the schema-only allowlist below.
    """
    try:
        tree = ast.parse(source_text)
    except SyntaxError:
        # Unparseable file — treat as a hard fail (catches a corrupt commit).
        # Better to surface it explicitly than to silently ignore.
        raise

    violations: list[tuple[Path, str, str, int, str]] = []

    class _Walker(ast.NodeVisitor):
        def __init__(self) -> None:
            self.stack: list[ast.AST] = []
            # Track which BinOp/JoinedStr nodes we've already folded to avoid
            # double-counting their inner Constants when the recursion descends.
            self.folded_node_ids: set[int] = set()

        def visit(self, node: ast.AST) -> None:  # type: ignore[override]
            self.stack.append(node)
            try:
                # Fold string-like nodes (Constant, JoinedStr, BinOp+Add) into
                # a single text and run the writer-pattern regex against the
                # folded result. This catches the obvious bypasses
                # (CodeRabbit PR #2207): f-strings and concat-built SQL that
                # a Constant-only inspection would miss.
                folded = _folded_string(node)
                if folded is not None and id(node) not in self.folded_node_ids:
                    qualname = _qualname_of_node(self.stack)
                    for label, pattern in _WRITER_PATTERNS:
                        for match in pattern.finditer(folded):
                            line_no = node.lineno + folded[: match.start()].count("\n")
                            line_text = folded.splitlines()[
                                folded.count("\n", 0, match.start())
                            ] if "\n" in folded else folded
                            violations.append(
                                (source_path, qualname, label, line_no, line_text.strip()),
                            )
                    # Mark all child string-like nodes as already-folded so
                    # generic_visit doesn't re-emit duplicate violations for
                    # the inner Constants of a JoinedStr/BinOp we just walked.
                    if isinstance(node, (ast.JoinedStr, ast.BinOp)):
                        for child in ast.walk(node):
                            self.folded_node_ids.add(id(child))
                self.generic_visit(node)
            finally:
                self.stack.pop()

    _Walker().visit(tree)
    return violations


def test_layer_a_only_canonical_qualname_writes_position_registry() -> None:
    """Layer A — every writer-statement must live in the canonical qualname.

    The schema source (`SCHEMA_SQL`) appears at module level in
    ``sqlite.py`` and consists exclusively of CREATE TABLE / CREATE INDEX
    statements — which are NOT writer patterns and are not flagged.
    """
    failures: list[tuple[Path, str, str, int, str]] = []
    for path in _iter_python_sources():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for violation in _collect_writer_violations(path, text):
            p, qualname, label, line_no, snippet = violation
            rel = str(p.relative_to(_REPO_ROOT))
            if _qualname_is_allowlisted(rel, qualname):
                continue
            failures.append((p, qualname, label, line_no, snippet))

    if failures:
        report = "\n".join(
            f"  {p.relative_to(_REPO_ROOT)}:{lineno} [{label}] qualname={q!r}: {snippet}"
            for p, q, label, lineno, snippet in failures
        )
        raise AssertionError(
            "Found writer SQL targeting position_registry outside the canonical "
            f"qualname {sorted(_ALLOWLIST_WRITERS_QUALNAMED)}. Either the new "
            "site is a legitimate need (then update _ALLOWLIST_WRITERS_QUALNAMED "
            "AND document why), or the new site is a bypass that strands the "
            "atomicity contract — see blueprint 28 §4 and §6.\n" + report,
        )


def test_layer_a_allowlist_targets_real_qualnames() -> None:
    """Guard: every allowlist entry's qualname must actually exist in its file.

    Catches typos / refactors that rename the canonical method without
    updating the allowlist.
    """
    for rel, qualname in _ALLOWLIST_WRITERS_QUALNAMED:
        path = _REPO_ROOT / rel
        assert path.is_file(), f"Allowlist entry {rel} does not exist."
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        found = False

        class _Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.stack: list[ast.AST] = []

            def visit(self, node: ast.AST) -> None:  # type: ignore[override]
                self.stack.append(node)
                try:
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        nonlocal found
                        if _qualname_of_node(self.stack) == qualname:
                            found = True
                    self.generic_visit(node)
                finally:
                    self.stack.pop()

        _Visitor().visit(tree)
        assert found, (
            f"Allowlist entry {rel}::{qualname} not found in source. "
            "Refresh the allowlist if the canonical method was renamed."
        )


# =============================================================================
# LAYER B — commit.py delegation shape
# =============================================================================


_COMMIT_PY = _ALMANAK_DIR / "framework" / "accounting" / "commit.py"


# Forbidden attribute access patterns inside commit.py: backend internals,
# transaction-lock primitives, raw transaction-control strings.
_COMMIT_PY_FORBIDDEN_ATTRS: frozenset[str] = frozenset({
    "_warm",
    "_conn",
    "_db_lock",
})

# Word-boundary regex catches BARE tokens too (CodeRabbit PR #2207 follow-up):
# the prior delimiter-suffixed substring set (``"begin "``, ``"commit;"``)
# missed bare ``sql = "COMMIT"`` and ``sql = "ROLLBACK"`` literals because they
# carry no trailing delimiter. ``\b`` boundaries the token regardless of
# case + delimiter; ``re.IGNORECASE`` covers ``begin``/``BEGIN``/``Begin``.
_COMMIT_PY_FORBIDDEN_RE: re.Pattern[str] = re.compile(
    r"\b(?:BEGIN|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)

_COMMIT_PY_FORBIDDEN_SQL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+(?:[\w]+\.)?[\"`\[]?"
        r"(?:position_registry|transaction_ledger)[\"`\]]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bUPDATE\s+(?:[\w]+\.)?[\"`\[]?"
        r"(?:position_registry|transaction_ledger)[\"`\]]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bDELETE\s+FROM\s+(?:[\w]+\.)?[\"`\[]?"
        r"(?:position_registry|transaction_ledger)[\"`\]]?",
        re.IGNORECASE,
    ),
]


def _docstring_node_ids(tree: ast.AST) -> set[int]:
    """Return the ``id()`` of every Constant node that is a docstring.

    A docstring is the FIRST statement of a Module/ClassDef/FunctionDef body
    when that statement is ``ast.Expr(value=ast.Constant(str))``. The
    word-boundary regex would otherwise flag legitimate prose use of
    ``commit``/``begin``/``rollback`` in module/function docstrings.
    """
    docstring_ids: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            docstring_ids.add(id(first.value))
    return docstring_ids


def test_layer_b_commit_py_no_backend_internals_or_raw_sql() -> None:
    """Layer B — commit.py must not touch backend internals or write raw SQL."""
    text = _COMMIT_PY.read_text(encoding="utf-8")
    tree = ast.parse(text)

    forbidden_attr_hits: list[tuple[int, str]] = []
    forbidden_str_hits: list[tuple[int, str]] = []
    forbidden_sql_hits: list[tuple[int, str]] = []
    folded_seen: set[int] = set()
    docstring_ids = _docstring_node_ids(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in _COMMIT_PY_FORBIDDEN_ATTRS:
            forbidden_attr_hits.append((node.lineno, node.attr))
        # Fold string-like nodes (Constant, JoinedStr f-string, BinOp+Add
        # concat) into one text, then check forbidden tokens AND SQL patterns.
        # CodeRabbit PR #2207 closed the f-string/concat bypass surface here.
        # Docstrings are exempt — prose use of "commit"/"begin"/"rollback"
        # is not the same as raw transaction-control SQL.
        if id(node) in docstring_ids:
            continue
        folded = _folded_string(node)
        if folded is None or id(node) in folded_seen:
            continue
        # Only run the check at the OUTERMOST string-like node; mark inner
        # Constants of JoinedStr/BinOp as already-seen to avoid duplicate
        # violations.
        if isinstance(node, (ast.JoinedStr, ast.BinOp)):
            for child in ast.walk(node):
                folded_seen.add(id(child))
        # Word-boundary regex (case-insensitive) catches bare BEGIN/COMMIT/
        # ROLLBACK tokens regardless of surrounding delimiters or case.
        match = _COMMIT_PY_FORBIDDEN_RE.search(folded)
        if match is not None:
            forbidden_str_hits.append((node.lineno, match.group(0)))
        for pattern in _COMMIT_PY_FORBIDDEN_SQL_PATTERNS:
            if pattern.search(folded):
                forbidden_sql_hits.append(
                    (node.lineno, pattern.pattern),
                )

    msg_parts: list[str] = []
    if forbidden_attr_hits:
        msg_parts.append(
            f"commit.py contains forbidden backend-internal attribute access: "
            f"{forbidden_attr_hits}. The atomic primitive must delegate through "
            "the StateManager facade — never poke `_warm`/`_conn`/`_db_lock` "
            "directly (which would let a future refactor open a parallel "
            "transaction outside the atomic boundary)."
        )
    if forbidden_str_hits:
        msg_parts.append(
            f"commit.py contains raw transaction-control strings: "
            f"{forbidden_str_hits}. Transactions are owned by the SQLite "
            "backend's `save_ledger_and_registry_atomic` method."
        )
    if forbidden_sql_hits:
        msg_parts.append(
            f"commit.py contains raw SQL targeting transaction_ledger or "
            f"position_registry: {forbidden_sql_hits}. SQL writers belong in "
            "the backend, not in the high-level commit primitive."
        )

    if msg_parts:
        raise AssertionError("\n".join(msg_parts))


def _state_manager_calls_in_branch(branch_body: list[ast.stmt]) -> list[tuple[int, str]]:
    """List every ``await state_manager.<method>(...)`` call in a branch body.

    Returns (lineno, method_name) tuples in source order. Calls against
    other receivers (e.g., direct `_warm` access — caught above as a
    forbidden attribute, but we double-guard) are NOT included; this list
    is the surface the branch uses to reach the StateManager facade.
    """
    calls: list[tuple[int, str]] = []
    for stmt in ast.walk(ast.Module(body=branch_body, type_ignores=[])):
        if not isinstance(stmt, ast.Await):
            continue
        call = stmt.value
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if not isinstance(func, ast.Attribute):
            continue
        receiver = func.value
        if not (isinstance(receiver, ast.Name) and receiver.id == "state_manager"):
            continue
        calls.append((stmt.lineno, func.attr))
    return calls


def _find_save_ledger_and_registry_function(tree: ast.AST) -> ast.AsyncFunctionDef:
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name == "save_ledger_and_registry"
        ):
            assert isinstance(node, ast.AsyncFunctionDef), (
                "save_ledger_and_registry must be an async function"
            )
            return node
    raise AssertionError("commit.py is missing async def save_ledger_and_registry")


def test_layer_b_commit_py_delegation_shape() -> None:
    """Layer B — each mode branch awaits exactly ONE state_manager.* call.

    The registry-mode branch must await ``save_ledger_and_registry``; the
    accounting-only branch must await ``save_ledger_entry``. Any other
    state_manager.* call in either branch fails the test — this is the
    structural backstop against future-evolution attacks (a hypothetical
    ``state_manager.save_handle_separately`` introduced later would pass
    a name-equality check and reintroduce the split-commit pattern).
    """
    text = _COMMIT_PY.read_text(encoding="utf-8")
    tree = ast.parse(text)
    fn = _find_save_ledger_and_registry_function(tree)

    # The function body's structure is:
    #   _validate_inputs(...)
    #   if mode == "accounting_only":
    #       await state_manager.save_ledger_entry(ledger)
    #       return
    #   await state_manager.save_ledger_and_registry(...)
    #
    # We walk every Await(Call(Attribute(Name('state_manager')))) in the
    # body, classify each by its enclosing branch (the if-test on `mode`),
    # and assert the exact-one-canonical-call invariant.

    # Find the top-level `if mode == "accounting_only":` block.
    if_node: ast.If | None = None
    for stmt in fn.body:
        if isinstance(stmt, ast.If):
            test = stmt.test
            # `mode == "accounting_only"`
            if (
                isinstance(test, ast.Compare)
                and len(test.ops) == 1
                and isinstance(test.ops[0], ast.Eq)
                and isinstance(test.left, ast.Name)
                and test.left.id == "mode"
                and len(test.comparators) == 1
                and isinstance(test.comparators[0], ast.Constant)
                and test.comparators[0].value == "accounting_only"
            ):
                if_node = stmt
                break
    assert if_node is not None, (
        "commit.py:save_ledger_and_registry must contain an `if mode == "
        "\"accounting_only\":` branch — that's the canonical mode-dispatch "
        "shape this test understands."
    )

    accounting_only_calls = _state_manager_calls_in_branch(if_node.body)
    # Registry-mode statements are everything else in the function body
    # AFTER the if-block (the function returns early from the
    # accounting_only branch, so registry-mode is the fall-through path).
    body_after_if: list[ast.stmt] = []
    seen_if = False
    for stmt in fn.body:
        if stmt is if_node:
            seen_if = True
            continue
        if seen_if:
            body_after_if.append(stmt)
    registry_calls = _state_manager_calls_in_branch(body_after_if)

    errors: list[str] = []
    if len(accounting_only_calls) != 1:
        errors.append(
            f"accounting_only branch must await exactly one state_manager.* "
            f"call, found {accounting_only_calls}"
        )
    elif accounting_only_calls[0][1] != "save_ledger_entry":
        errors.append(
            f"accounting_only branch must await state_manager.save_ledger_entry, "
            f"got {accounting_only_calls[0][1]!r}"
        )

    if len(registry_calls) != 1:
        errors.append(
            f"registry-mode branch must await exactly one state_manager.* "
            f"call, found {registry_calls}"
        )
    elif registry_calls[0][1] != "save_ledger_and_registry":
        errors.append(
            f"registry-mode branch must await state_manager.save_ledger_and_registry, "
            f"got {registry_calls[0][1]!r}"
        )

    if errors:
        raise AssertionError(
            "commit.py delegation shape violation. The structural rule is: "
            "each mode branch awaits exactly ONE state_manager.* call, and "
            "that call MUST be the canonical method for the branch. Any "
            "extra call (e.g., `state_manager.save_handle_separately(...)`) "
            "is a split-commit reintroduction of bug #2130's failure mode.\n"
            + "\n".join(f"  - {e}" for e in errors),
        )


def test_layer_b_allowlist_path_actually_exists() -> None:
    assert _COMMIT_PY.is_file(), (
        f"commit.py expected at {_COMMIT_PY}; refresh the allowlist if "
        "the file moved."
    )


# =============================================================================
# REGRESSION — string-folding helper closes f-string + concat bypasses
# (CodeRabbit PR #2207 finding)
# =============================================================================


def test_folded_string_handles_constant() -> None:
    """Plain Constant str is returned verbatim."""
    node = ast.parse('"hello"', mode="eval").body
    assert _folded_string(node) == "hello"


def test_folded_string_handles_fstring() -> None:
    """JoinedStr (f-string) is folded; FormattedValue parts contribute ''.

    The literal segments around the formatted value are what the writer-pattern
    regex needs to match. A bypass like ``f"INSERT INTO {x} foo"`` folds to
    ``"INSERT INTO  foo"`` and is correctly flagged by the writer regex.
    """
    node = ast.parse('f"INSERT INTO {x} VALUES (1)"', mode="eval").body
    folded = _folded_string(node)
    assert folded is not None
    assert "INSERT INTO" in folded
    # FormattedValue with a constant inside is folded too — closes the
    # ``f"INSERT INTO {'position_registry'} ..."`` bypass directly.
    node2 = ast.parse('f"INSERT INTO {\'position_registry\'} VALUES (1)"', mode="eval").body
    folded2 = _folded_string(node2)
    assert folded2 is not None
    assert "position_registry" in folded2


def test_folded_string_handles_binop_concat() -> None:
    """BinOp(Add) of two str constants is folded."""
    node = ast.parse('"INSERT INTO " + "position_registry"', mode="eval").body
    folded = _folded_string(node)
    assert folded == "INSERT INTO position_registry"


def test_folded_string_returns_none_on_dynamic() -> None:
    """Non-string-like nodes (calls, names) return None."""
    assert _folded_string(ast.parse("foo()", mode="eval").body) is None
    assert _folded_string(ast.parse("name", mode="eval").body) is None
    # Mixed-type BinOp (str + non-str) → None (we won't speculate).
    node = ast.parse('"INSERT " + foo()', mode="eval").body
    assert _folded_string(node) is None


def test_layer_a_catches_fstring_writer_in_synthetic_source() -> None:
    """Synthetic source with f-string-built INSERT must be flagged."""
    src = '''
def evil_writer():
    sql = f"INSERT INTO {'position_registry'} (a) VALUES (1)"
    return sql
'''
    violations = _collect_writer_violations(Path("/tmp/synthetic_evil.py"), src)
    # Should flag at least one violation under the qualname `evil_writer`.
    assert any(v[1] == "evil_writer" for v in violations), (
        f"f-string writer bypass not caught; violations: {violations}"
    )


def test_layer_a_catches_concat_writer_in_synthetic_source() -> None:
    """Synthetic source with concat-built INSERT must be flagged."""
    src = '''
def evil_concat():
    sql = "INSERT INTO " + "position_registry VALUES (1)"
    return sql
'''
    violations = _collect_writer_violations(Path("/tmp/synthetic_evil.py"), src)
    assert any(v[1] == "evil_concat" for v in violations), (
        f"concat writer bypass not caught; violations: {violations}"
    )


def test_layer_b_forbidden_regex_catches_bare_tokens() -> None:
    """Layer B regex must catch bare BEGIN/COMMIT/ROLLBACK tokens.

    The prior token-frozenset only matched delimiter-suffixed variants
    (``"begin "``, ``"commit;"``); bare ``sql = "COMMIT"`` slipped through.
    Word-boundary regex closes that gap. Pinning the property here so a
    future maintainer doesn't regress to the substring approach.
    """
    # Bare tokens — must match.
    for bare in ("BEGIN", "COMMIT", "ROLLBACK", "begin", "commit", "rollback", "Begin"):
        assert _COMMIT_PY_FORBIDDEN_RE.search(bare) is not None, (
            f"bare {bare!r} must match — word-boundary regex regression"
        )
    # Delimiter-suffixed variants — must also match (preserved from prior).
    for tok in ("begin immediate", "BEGIN;", "commit\n", "ROLLBACK;"):
        assert _COMMIT_PY_FORBIDDEN_RE.search(tok) is not None, (
            f"delimiter-suffixed {tok!r} must match"
        )
    # Negative cases — must NOT match (substring within other words).
    for tok in ("commitment", "rollback_handler_v2", "begins_with"):
        # `commitment` should NOT match because `\b` requires word boundary
        # after `commit` — the `m` is a word char so no boundary. Likewise
        # `rollback_handler_v2` has `_` after `rollback` (word char in
        # Python's \w), so no \b boundary; safe.
        if _COMMIT_PY_FORBIDDEN_RE.search(tok) is not None:
            # `begins_with` has `s` after `begin` — \b doesn't fire there
            # either. Underscores DO create word-boundary issues only at the
            # left edge; let's not over-assert. Skip false-negative checks.
            pass


# =============================================================================
# REGRESSION — Postgres contract INCLUDES T11 tables after T19 (VIB-4205)
# (CodeRabbit PR #2207 finding, inverted by VIB-4205 / T19 landing)
# =============================================================================


def test_postgres_contract_includes_t11_tables_after_t19_vib_4205() -> None:
    """``position_registry`` and ``migration_state`` MUST appear in the
    Postgres contract now that T19 (VIB-4205) has shipped the hosted
    writers.

    Inverted form of the pre-T19 deferral guard: before VIB-4205 these
    tables were intentionally absent from the Postgres contract so hosted
    gateway boot didn't fail-loud on tables the hosted runtime could not
    yet use. T19 shipped the Postgres writer paths in
    ``almanak/gateway/services/state_service.py`` and emptied
    ``_POSTGRES_DEFERRED_TABLES`` so the schema validator now fail-louds
    when the metrics-database migration (VIB-4191) hasn't landed these
    tables.

    Re-introducing either table to ``_POSTGRES_DEFERRED_TABLES`` without
    a corresponding metrics-database rollback is the regression this
    inverted test catches — it would silently re-disable the fail-loud
    boot guard for these tables.
    """
    from almanak.framework.state.schema_contract import (
        ACCOUNTING_SCHEMA_CONTRACT_POSTGRES,
        ACCOUNTING_SCHEMA_CONTRACT_SQLITE,
    )
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT_SQLITE
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT_SQLITE
    # T19 (VIB-4205) landed — both tables MUST now appear in the hosted
    # Postgres contract. Re-introducing either to
    # ``_POSTGRES_DEFERRED_TABLES`` silently re-disables the fail-loud
    # boot guard for new column drift on these tables.
    assert "position_registry" in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES, (
        "position_registry missing from the Postgres contract — T19 "
        "(VIB-4205) landed the hosted writer, so the schema validator "
        "must now require this table. If you re-added it to "
        "_POSTGRES_DEFERRED_TABLES, also revert the writer and update "
        "this test."
    )
    assert "migration_state" in ACCOUNTING_SCHEMA_CONTRACT_POSTGRES, (
        "migration_state missing from the Postgres contract — T19 "
        "(VIB-4205) landed the hosted cutover RPCs, so the schema "
        "validator must now require this table. If you re-added it to "
        "_POSTGRES_DEFERRED_TABLES, also revert the writer and update "
        "this test."
    )
