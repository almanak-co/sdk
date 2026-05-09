"""VIB-4165 (T5 of VIB-4160) — placeholder ``IntentType`` fail-fast contract.

These five enums (``LIQUIDATE``, ``OPEN_CDP``, ``MINT_STABLE``, ``REPAY_STABLE``,
``CLOSE_CDP``) exist *without real connectors* so future code paths cannot
silently smuggle CDP / liquidation / stablecoin-mint operations through
``BORROW`` / ``REPAY`` / ``SUPPLY`` and pollute lending accounting before the
real connectors land in P1.

The compiler MUST raise ``NotImplementedError`` on each — without this test,
the "placeholder" designation silently rots into "accepted" exactly as
warned by §"Locked design decisions" #5 of the primitives-refactor-20260508
PRD.

Hard Ratification Condition #5 of the ratified primitives refactor design.
"""

from __future__ import annotations

import ast
import inspect
import re
import textwrap

import pytest

from almanak.framework.intents.compiler import (
    _PLACEHOLDER_INTENT_TYPES,
    IntentCompiler,
    _raise_if_placeholder_intent,
)
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives.taxonomy import UnknownIntentTypeError, record_for

# ---------------------------------------------------------------------------
# The 5 placeholder values declared by VIB-4165 — kept as a literal list (not
# derived from ``_PLACEHOLDER_INTENT_TYPES``) so that the equality test in
# ``test_placeholder_set_is_exactly_the_5_p0_values`` cannot tautologically
# pass when the production set drifts.
# ---------------------------------------------------------------------------
PLACEHOLDERS: list[IntentType] = [
    IntentType.LIQUIDATE,
    IntentType.OPEN_CDP,
    IntentType.MINT_STABLE,
    IntentType.REPAY_STABLE,
    IntentType.CLOSE_CDP,
]


# ---------------------------------------------------------------------------
# §4.1 — placeholder set has the exact 5 values (anti-drift, reverse direction)
# ---------------------------------------------------------------------------
def test_placeholder_set_is_exactly_the_5_p0_values() -> None:
    """Anti-drift: the production set MUST equal exactly these 5 values.

    Catches: a 6th value sneaking in, a 5th vanishing, a typo replacing one
    with a real intent type. All three are equally bad.
    """
    assert _PLACEHOLDER_INTENT_TYPES == frozenset(PLACEHOLDERS), (
        "VIB-4165 placeholder set must contain exactly LIQUIDATE, OPEN_CDP, "
        "MINT_STABLE, REPAY_STABLE, CLOSE_CDP. Any drift requires a "
        "deliberate update to BOTH the production set in compiler.py AND "
        "this test (and the UAT card)."
    )


# ---------------------------------------------------------------------------
# §4.2 — placeholder guard raises NotImplementedError naming the placeholder
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("placeholder", PLACEHOLDERS, ids=lambda p: p.value)
def test_placeholder_compiler_raises_not_implemented(placeholder: IntentType) -> None:
    """The compiler-level fail-fast guard MUST raise ``NotImplementedError``.

    The exception message MUST contain the placeholder's enum value so a
    caller / log reader can identify the offender without reading the helper
    source.
    """
    with pytest.raises(NotImplementedError) as exc_info:
        _raise_if_placeholder_intent(placeholder)
    msg = str(exc_info.value)
    assert placeholder.value in msg, (
        f"NotImplementedError must name the placeholder; got: {msg!r}"
    )


# ---------------------------------------------------------------------------
# §4.3 — each placeholder has a TAXONOMY row
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("placeholder", PLACEHOLDERS, ids=lambda p: p.value)
def test_placeholder_has_taxonomy_row(placeholder: IntentType) -> None:
    """Each placeholder MUST have a TAXONOMY row.

    Without a row, ``record_for(...)`` would raise ``UnknownIntentTypeError``
    on any placeholder lookup — the asymmetric pattern that VIB-4159 / 4161 /
    4164 went out of their way to forbid (parity invariant: every
    ``IntentType`` value has a ``PrimitiveRecord``).
    """
    try:
        record = record_for(placeholder.value)
    except UnknownIntentTypeError as exc:  # pragma: no cover - regression guard
        pytest.fail(
            f"VIB-4165: placeholder {placeholder.value!r} is missing from "
            f"TAXONOMY in almanak/framework/primitives/taxonomy.py. "
            f"record_for() raised {exc!r}."
        )
    assert record.intent_type == placeholder.value


# ---------------------------------------------------------------------------
# §4.4 — non-placeholder intent types do NOT raise (reverse direction)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "intent_type",
    [t for t in IntentType if t not in set(PLACEHOLDERS)],
    ids=lambda t: t.value,
)
def test_non_placeholder_intent_type_does_not_raise(intent_type: IntentType) -> None:
    """The guard MUST NOT raise for any of the 24 real intent types.

    Symmetric coverage with §4.1 — both forward (placeholder→accepted) and
    reverse (real→placeholder) drift are caught.
    """
    # No assertion needed: a NotImplementedError would fail the test by
    # propagation. The call returning normally IS the assertion.
    _raise_if_placeholder_intent(intent_type)


# ---------------------------------------------------------------------------
# §4.5 — IntentCompiler.compile() invokes the guard (call-site preservation)
# ---------------------------------------------------------------------------
def _compile_source_lines() -> tuple[list[str], int]:
    """Return ``(source_lines, starting_lineno)`` for ``IntentCompiler.compile``."""
    return inspect.getsourcelines(IntentCompiler.compile)


def test_compiler_compile_invokes_placeholder_guard() -> None:
    """Static check: ``IntentCompiler.compile`` MUST reference the helper by name.

    Catches future refactors that silently remove the fail-fast call site
    (the helper still exists and still raises, but ``compile`` never invokes
    it — the silent-failure mode this card exists to prevent).

    This check is brittle to renames — that is intentional. If the helper is
    renamed, this test fails loudly and the test author has to consciously
    update both sides. The alternative (inferring the call dynamically) would
    require building a real ``IntentCompiler`` with gateway client, chain,
    etc. — orders of magnitude more setup and many more moving parts that
    could mask a regression here.
    """
    src_lines, _ = _compile_source_lines()
    src = "".join(src_lines)
    assert "_raise_if_placeholder_intent" in src, (
        "IntentCompiler.compile() must call _raise_if_placeholder_intent to "
        "fail-fast on placeholder IntentType values (VIB-4165). The helper "
        "exists in almanak/framework/intents/compiler.py but the dispatch "
        "method does not reference it — exactly the silent-failure mode HRC-5 "
        "of the primitives refactor PRD was created to prevent."
    )


# ---------------------------------------------------------------------------
# §6 F6 defense-in-depth — IntentStateMachine.__init__ guard
#
# The compiler's outer try/except catches ``Exception`` and could silently
# convert the helper's ``NotImplementedError`` to ``CompilationResult.FAILED``
# if a future refactor moved the call site inside the try block. The compiler
# is the primary check, but the state machine is the second-most-likely
# entry point: any caller who skips compilation and constructs an
# ``IntentStateMachine`` directly would otherwise hit
# ``ValueError("missing state machine wiring")`` rather than the intended
# ``NotImplementedError``. The state-machine constructor calls the same guard
# so the failure mode is uniform regardless of entry point.
# ---------------------------------------------------------------------------
def test_intent_state_machine_init_invokes_placeholder_guard() -> None:
    """``IntentStateMachine.__init__`` MUST also call ``_raise_if_placeholder_intent``.

    Defense-in-depth: even if a caller bypasses ``IntentCompiler.compile``
    (e.g. constructs an ``IntentStateMachine`` directly with an ad-hoc Intent
    whose ``intent_type`` is a placeholder), the ``NotImplementedError`` must
    still surface — not the generic ``ValueError("missing state machine
    wiring")`` that ``get_preparing_state`` would otherwise raise.

    Static check: ``IntentStateMachine.__init__`` source references the helper
    by name, AND the helper call appears BEFORE the call to
    ``get_preparing_state`` (so the right error class wins).
    """
    from almanak.framework.intents.state_machine import IntentStateMachine

    src_lines, _ = inspect.getsourcelines(IntentStateMachine.__init__)
    src = "".join(src_lines)
    assert "_raise_if_placeholder_intent" in src, (
        "IntentStateMachine.__init__ must call _raise_if_placeholder_intent "
        "to fail-fast on placeholder IntentType values that bypass the "
        "compiler (VIB-4165). The helper exists in compiler.py but the "
        "state machine constructor does not invoke it — placeholder intents "
        "would surface as ValueError instead of NotImplementedError."
    )

    # Match the call site (the first non-comment occurrence). A bare
    # ``_raise_if_placeholder_intent(`` and ``get_preparing_state(`` both
    # appear unambiguously at the assignment / expression level — comments
    # would have ``# `` / ``"""`` prefixes which we filter out.
    call_re = re.compile(r"^(?!\s*(?:#|\"\"\"|'''))[^#\"']*\b{name}\s*\(")

    def _first_call_site(name: str) -> int:
        rx = re.compile(rf"^(?!\s*#)(?!\s*\"\"\")(?!\s*''')[^#]*\b{name}\s*\(")
        return next((i for i, line in enumerate(src_lines) if rx.search(line)), -1)

    helper_idx = _first_call_site("_raise_if_placeholder_intent")
    preparing_idx = _first_call_site("get_preparing_state")

    # ``call_re`` is unused but documents the regex shape for the reader; the
    # local helper above does the real work and skips comment lines.
    assert call_re  # noqa: SIM101

    assert helper_idx >= 0 and preparing_idx >= 0, (
        "expected both _raise_if_placeholder_intent and get_preparing_state "
        "call sites (not comments) in IntentStateMachine.__init__"
    )
    assert helper_idx < preparing_idx, (
        f"_raise_if_placeholder_intent (call site at relative line "
        f"{helper_idx}) must run BEFORE get_preparing_state (call site at "
        f"relative line {preparing_idx}) in __init__ — otherwise placeholder "
        f"intents surface as ValueError instead of NotImplementedError. "
        f"VIB-4165 HRC-5 defense in depth."
    )


def test_compiler_compile_helper_call_is_not_inside_a_swallowing_try() -> None:
    """§6 F6 strengthening: the helper call MUST NOT be wrapped in a try block
    that catches ``Exception`` (which would convert ``NotImplementedError``
    into a silent ``CompilationResult.FAILED``).

    The check uses Python's AST so it is immune to formatting variations:
    single-line ``try:`` statements (``try: ...``), comments containing the
    word ``try``, conditional ``try`` blocks closed BEFORE the helper call,
    and anything else regex-based heuristics would miss.

    Algorithm: parse ``IntentCompiler.compile`` into an AST, locate every
    ``Call`` node whose function is named ``_raise_if_placeholder_intent``,
    and confirm none of them have a ``Try`` node as an ancestor inside the
    function body. ``Try`` blocks that close before the helper call are
    irrelevant — only blocks the call is *lexically inside* would swallow
    the exception.
    """
    src_lines, start_lineno = _compile_source_lines()
    src = textwrap.dedent("".join(src_lines))
    tree = ast.parse(src)
    func_def = tree.body[0]
    assert isinstance(func_def, ast.FunctionDef), (
        "Expected IntentCompiler.compile to be a FunctionDef; got "
        f"{type(func_def).__name__}"
    )

    # Walk the function body AST tracking whether we are currently inside a
    # ``Try`` node. ``ast.walk`` flattens the tree and loses parent context,
    # so use a manual traversal that carries an "inside Try" flag.
    helper_call_locations: list[tuple[int, bool]] = []  # (lineno, inside_try)

    def _visit(node: ast.AST, *, inside_try: bool) -> None:
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_raise_if_placeholder_intent"
        ):
            helper_call_locations.append((node.lineno, inside_try))
        # ``Try`` nodes have several child lists; the body + handlers + else +
        # finalbody all "wrap" their statements in the swallowing semantics
        # because the matching ``except Exception`` would catch a raise from
        # any of them. Treat all four as inside_try=True.
        if isinstance(node, ast.Try):
            for child in (*node.body, *node.handlers, *node.orelse, *node.finalbody):
                _visit(child, inside_try=True)
            return
        for child in ast.iter_child_nodes(node):
            _visit(child, inside_try=inside_try)

    _visit(func_def, inside_try=False)

    assert helper_call_locations, (
        "Expected at least one call to _raise_if_placeholder_intent in "
        "IntentCompiler.compile() — see "
        "test_compiler_compile_invokes_placeholder_guard above. The AST "
        "walker found zero Call nodes; either the helper was renamed or "
        "the call site was removed."
    )

    nested_calls = [(lineno, inside) for lineno, inside in helper_call_locations if inside]
    assert not nested_calls, (
        "VIB-4165 / HRC-5: _raise_if_placeholder_intent is lexically nested "
        "inside a try block within IntentCompiler.compile(). The outer try "
        "block catches `Exception` and would silently convert the helper's "
        "NotImplementedError into CompilationResult.FAILED — the literal "
        "silent-failure mode this guard was created to prevent. Move the "
        "helper call ABOVE the try block.\n\n"
        f"Offending call site(s) at function-relative lines: "
        f"{[ln for ln, _ in nested_calls]} (function source starts at module "
        f"line {start_lineno})."
    )
