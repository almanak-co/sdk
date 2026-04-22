"""Characterization tests for ``_ast_scan_strategy_file``.

These tests pin down the current externally-observable behaviour of the AST
scanner so the Phase 7.5 refactor to an ``ast.NodeVisitor`` subclass can be
validated bit-for-bit against the pre-refactor baseline. They intentionally
build minimal strategy source strings (no ``strat new`` scaffold dependency)
so the contract is easy to read and reason about.

The scanner's observable contract, as exercised here:

1. Flags every placeholder string literal it sees, regardless of where the
   literal lives (module-level constant, inside a method body, inside a
   class attribute, etc.).
2. Tracks whether ``PositionInfo`` is imported via either ``from ... import
   PositionInfo`` or a dotted ``import ... PositionInfo`` tail.
3. Resolves the strategy class either by exact name match (when the loader
   supplies ``target_class_name``) or falls back to the first class whose
   bases reference a known strategy base (``IntentStrategy``,
   ``StatelessStrategy``, ``Strategy``, ``StrategyBase``). Base names are
   resolved from ``Name``, ``Attribute``, and ``Subscript[Name]`` nodes.
4. Flags empty ``generate_teardown_intents`` bodies (``pass``, bare docstring,
   ``return None`` / ``return []`` / ``return ()`` / combinations).
5. Emits ``missing_teardown_intents`` when the method is absent AND the
   strategy does not inherit ``StatelessStrategy``.
6. Emits ``missing_get_open_positions`` only when ``PositionInfo`` is
   imported AND the method is absent AND the strategy does not inherit
   ``StatelessStrategy``.
7. Surfaces syntax errors and read failures as ERROR findings with the
   expected codes (``syntax_error`` / ``read_failed``), and returns
   ``(None, <default facts>)`` so downstream template heuristics are safe.
8. Does not emit ``missing_*`` findings when the file has no strategy class
   at all — the loader is responsible for that message.
9. Preserves finding emission order: placeholder findings follow source
   order (since the walker is a single ``ast.walk`` pass).

Any future refactor must preserve every assertion in this file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.framework.cli.check import (
    CheckReport,
    Finding,
    Layer,
    Severity,
    _ast_scan_strategy_file,
)


# ---------------------------------------------------------------------------
# Tiny helpers — keep the test intent visible at the call site
# ---------------------------------------------------------------------------


def _write(strategy_dir: Path, source: str) -> Path:
    """Write ``source`` to ``strategy_dir/strategy.py`` and return the path."""
    strategy_dir.mkdir(parents=True, exist_ok=True)
    strategy_file = strategy_dir / "strategy.py"
    strategy_file.write_text(source, encoding="utf-8")
    return strategy_file


def _scan(strategy_file: Path, target: str | None = None) -> tuple[CheckReport, dict]:
    """Run the scanner and return ``(report, facts)`` for assertions."""
    report = CheckReport(strategy_dir=str(strategy_file.parent))
    _tree, facts = _ast_scan_strategy_file(strategy_file, report, target_class_name=target)
    return report, facts


def _codes(report: CheckReport) -> list[str]:
    return [f.code for f in report.findings]


# ---------------------------------------------------------------------------
# 1. Placeholder literal detection
# ---------------------------------------------------------------------------


def test_flags_placeholder_address_at_module_level(tmp_path: Path) -> None:
    """A ``0x_SET_*`` module-level constant must produce a placeholder finding."""
    strategy_file = _write(
        tmp_path / "p1",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'VAULT = "0x_SET_VAULT_ADDRESS"\n\n'
        'class S(IntentStrategy):\n'
        '    STRATEGY_NAME = "s"\n'
        '    def decide(self, market): return None\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report, facts = _scan(strategy_file)

    placeholder = [f for f in report.findings if f.code == "placeholder_address"]
    assert placeholder, f"expected placeholder_address, got: {_codes(report)}"
    assert placeholder[0].severity == Severity.ERROR
    assert placeholder[0].layer == Layer.AST
    assert placeholder[0].file == str(strategy_file)
    assert placeholder[0].line is not None
    assert "0x_SET_" in placeholder[0].message
    # Class-name fact is still populated — placeholder detection does not short-circuit.
    assert facts["class_name"] == "S"


def test_flags_placeholder_inside_method_body(tmp_path: Path) -> None:
    """Placeholders hidden inside method bodies must still be surfaced."""
    strategy_file = _write(
        tmp_path / "p2",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def decide(self, market):\n'
        '        addr = "REPLACE_ME"\n'
        '        return addr\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report, _ = _scan(strategy_file)
    hits = [f for f in report.findings if f.code == "placeholder_address"]
    assert len(hits) == 1
    assert "REPLACE_ME" in hits[0].message


def test_flags_multiple_placeholders_in_source_order(tmp_path: Path) -> None:
    """Ordering: multiple placeholders must be emitted in source-walk order.

    ``ast.walk`` yields nodes breadth-first, but both placeholders in this
    fixture sit at the same depth, so the stable contract is "relative line
    numbers are non-decreasing". We pin the exact observed ordering here so a
    refactor that switches to ``ast.NodeVisitor`` (depth-first) does not
    silently re-order the emitted list.
    """
    strategy_file = _write(
        tmp_path / "p3",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'A = "0x_SET_A"\n'
        'B = "0xDEADBEEF"\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report, _ = _scan(strategy_file)
    hits = [f for f in report.findings if f.code == "placeholder_address"]
    assert len(hits) == 2
    # Both at module scope — source order must be preserved.
    assert hits[0].line is not None and hits[1].line is not None
    assert hits[0].line < hits[1].line
    assert "0x_SET_" in hits[0].message
    assert "0xDEADBEEF" in hits[1].message


# ---------------------------------------------------------------------------
# 2. PositionInfo import tracking drives the missing_get_open_positions warning
# ---------------------------------------------------------------------------


def test_missing_get_open_positions_when_position_info_imported_from(tmp_path: Path) -> None:
    """``from ... import PositionInfo`` -> warning if method is missing."""
    strategy_file = _write(
        tmp_path / "pi1",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n'
        'from almanak.framework.teardown import PositionInfo  # noqa: F401\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report, facts = _scan(strategy_file)
    assert facts["imports_position_info"] is True
    assert "missing_get_open_positions" in _codes(report)


def test_missing_get_open_positions_when_position_info_imported_as_dotted(tmp_path: Path) -> None:
    """Dotted ``import x.y.PositionInfo`` also counts as a PositionInfo import.

    The scanner checks ``alias.name.endswith("PositionInfo")`` on ``ast.Import``
    nodes. We pin that behaviour here so the refactor cannot quietly narrow it.
    """
    strategy_file = _write(
        tmp_path / "pi2",
        'import almanak.framework.teardown.PositionInfo  # type: ignore  # noqa: F401\n'
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report, facts = _scan(strategy_file)
    # The import statement itself is syntactically valid; we don't care whether
    # it resolves at runtime — the scanner reads static AST only.
    assert facts["imports_position_info"] is True
    assert "missing_get_open_positions" in _codes(report)


def test_no_missing_get_open_positions_when_position_info_not_imported(tmp_path: Path) -> None:
    """Without the import, the heuristic must not fire."""
    strategy_file = _write(
        tmp_path / "pi3",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report, facts = _scan(strategy_file)
    assert facts["imports_position_info"] is False
    assert "missing_get_open_positions" not in _codes(report)


# ---------------------------------------------------------------------------
# 3. Empty teardown body detection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        "        pass\n",
        "        return []\n",
        "        return ()\n",
        "        return None\n",
        "        return\n",
        '        """docstring only."""\n',
        '        """docstring."""\n        pass\n',
    ],
    ids=[
        "pass",
        "return_empty_list",
        "return_empty_tuple",
        "return_none_explicit",
        "return_bare",
        "docstring_only",
        "docstring_and_pass",
    ],
)
def test_empty_teardown_variants_are_flagged(tmp_path: Path, body: str) -> None:
    """Every "effectively empty" teardown body must be flagged."""
    strategy_file = _write(
        tmp_path / f"et_{hash(body) & 0xFFFF:x}",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        f"{body}",
    )
    report, facts = _scan(strategy_file)
    assert facts["overrides_generate_teardown_intents"] is True
    assert facts["teardown_body_empty"] is True
    assert "empty_teardown_intents" in _codes(report)


def test_non_trivial_teardown_is_not_flagged(tmp_path: Path) -> None:
    """A teardown with a real statement body must not be flagged as empty."""
    strategy_file = _write(
        tmp_path / "nt",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        intents = [1, 2, 3]\n'
        '        return intents\n',
    )
    report, facts = _scan(strategy_file)
    assert facts["overrides_generate_teardown_intents"] is True
    assert facts["teardown_body_empty"] is False
    assert "empty_teardown_intents" not in _codes(report)


# ---------------------------------------------------------------------------
# 4. Missing-method findings and the StatelessStrategy opt-out
# ---------------------------------------------------------------------------


def test_missing_teardown_method_emits_warning(tmp_path: Path) -> None:
    """No ``generate_teardown_intents`` on an IntentStrategy -> warning."""
    strategy_file = _write(
        tmp_path / "mt",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    STRATEGY_NAME = "s"\n'
        '    def decide(self, market): return None\n',
    )
    report, facts = _scan(strategy_file)
    assert facts["overrides_generate_teardown_intents"] is False
    assert "missing_teardown_intents" in _codes(report)
    missing = [f for f in report.findings if f.code == "missing_teardown_intents"][0]
    assert missing.severity == Severity.WARNING
    assert missing.layer == Layer.AST


def test_stateless_strategy_suppresses_missing_teardown_and_positions(tmp_path: Path) -> None:
    """``StatelessStrategy`` subclasses inherit valid defaults — silence the warnings."""
    strategy_file = _write(
        tmp_path / "ss",
        'from almanak.framework.strategies.stateless_strategy import StatelessStrategy\n'
        'from almanak.framework.teardown import PositionInfo  # noqa: F401\n\n'
        'class Signal(StatelessStrategy):\n'
        '    def decide(self, market): return None\n',
    )
    report, facts = _scan(strategy_file)
    assert facts["inherits_stateless"] is True
    assert "missing_teardown_intents" not in _codes(report)
    assert "missing_get_open_positions" not in _codes(report)


def test_stateless_detected_via_attribute_base(tmp_path: Path) -> None:
    """``class X(pkg.StatelessStrategy)`` (Attribute base) must also count."""
    strategy_file = _write(
        tmp_path / "ss2",
        'import almanak.framework.strategies.stateless_strategy as ss\n\n'
        'class Signal(ss.StatelessStrategy):\n'
        '    def decide(self, market): return None\n',
    )
    _report, facts = _scan(strategy_file)
    assert facts["inherits_stateless"] is True


# ---------------------------------------------------------------------------
# 5. Class resolution — target_class_name vs fallback
# ---------------------------------------------------------------------------


def test_target_class_name_locks_onto_exact_class(tmp_path: Path) -> None:
    """When a target name is supplied, the scanner must use that exact class."""
    strategy_file = _write(
        tmp_path / "tcn",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class Helper(IntentStrategy):\n'
        '    pass\n\n'
        'class Real(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    _report, facts = _scan(strategy_file, target="Real")
    assert facts["class_name"] == "Real"
    assert facts["overrides_generate_teardown_intents"] is True


def test_fallback_picks_first_class_with_known_strategy_base(tmp_path: Path) -> None:
    """Without a target, the first class inheriting a known base wins."""
    strategy_file = _write(
        tmp_path / "fb",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class Unrelated:\n'
        '    pass\n\n'
        'class Real(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    _report, facts = _scan(strategy_file)
    assert facts["class_name"] == "Real"


def test_fallback_ignores_nested_classes(tmp_path: Path) -> None:
    """A nested strategy class must not outrank a later top-level strategy class.

    The pre-refactor ``ast.walk`` traversal is breadth-first, so top-level
    classes are visited before any nested class. The post-refactor
    ``NodeVisitor`` is depth-first; without explicit nesting awareness a
    nested ``IntentStrategy`` inside ``Wrapper`` would win the fallback over
    the real top-level class. This test pins the correct behaviour: only
    top-level classes are considered for fallback resolution.
    """
    strategy_file = _write(
        tmp_path / "nested",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class Wrapper:\n'
        '    class Nested(IntentStrategy):\n'
        '        pass\n\n'
        'class Real(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    _report, facts = _scan(strategy_file)
    assert facts["class_name"] == "Real"


def test_target_class_name_resolves_even_when_nested(tmp_path: Path) -> None:
    """An exact ``target_class_name`` match wins regardless of nesting.

    The loader may legitimately pick a nested class (via dotted lookup /
    ``__qualname__`` resolution). When it does, the scanner should lock
    onto that class and not fall through to a top-level sibling.
    """
    strategy_file = _write(
        tmp_path / "nested_target",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class Wrapper:\n'
        '    class Nested(IntentStrategy):\n'
        '        def generate_teardown_intents(self, mode=None, market=None):\n'
        '            return [1]\n\n'
        'class Other(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    _report, facts = _scan(strategy_file, target="Nested")
    assert facts["class_name"] == "Nested"


def test_no_strategy_class_emits_no_missing_method_findings(tmp_path: Path) -> None:
    """A module without any strategy class must NOT emit missing_* findings.

    The loader is responsible for the "no class" error; the AST scanner
    simply declines to emit the downstream heuristics.
    """
    strategy_file = _write(
        tmp_path / "nc",
        'from almanak.framework.teardown import PositionInfo  # noqa: F401\n\n'
        'x = 1\n',
    )
    report, facts = _scan(strategy_file)
    assert facts["class_name"] is None
    assert "missing_teardown_intents" not in _codes(report)
    assert "missing_get_open_positions" not in _codes(report)


# ---------------------------------------------------------------------------
# 6. Error handling — malformed Python and unreadable files
# ---------------------------------------------------------------------------


def test_syntax_error_is_reported_and_facts_default(tmp_path: Path) -> None:
    """Malformed Python must surface ``syntax_error`` and return default facts."""
    strategy_file = _write(tmp_path / "se", 'def broken(:\n    pass\n')
    report, facts = _scan(strategy_file)
    codes = _codes(report)
    assert "syntax_error" in codes
    syntax = [f for f in report.findings if f.code == "syntax_error"][0]
    assert syntax.severity == Severity.ERROR
    assert syntax.layer == Layer.AST
    assert syntax.file == str(strategy_file)
    # Default facts survive so the caller can still run template heuristics.
    assert facts["class_name"] is None
    assert facts["imports_position_info"] is False
    assert facts["overrides_get_open_positions"] is False
    assert facts["overrides_generate_teardown_intents"] is False
    assert facts["teardown_body_empty"] is False
    assert facts["has_on_intent_executed"] is False


def test_read_failure_is_reported(tmp_path: Path) -> None:
    """A path that cannot be read surfaces ``read_failed``."""
    # Point at a directory-as-file so ``read_text`` raises IsADirectoryError.
    bogus = tmp_path / "adir"
    bogus.mkdir()
    report = CheckReport(strategy_dir=str(tmp_path))
    tree, facts = _ast_scan_strategy_file(bogus, report)
    assert tree is None
    codes = [f.code for f in report.findings]
    assert "read_failed" in codes
    read_fail = [f for f in report.findings if f.code == "read_failed"][0]
    assert read_fail.severity == Severity.ERROR
    assert read_fail.layer == Layer.AST
    assert read_fail.file == str(bogus)
    # Default facts preserved.
    assert facts["class_name"] is None


# ---------------------------------------------------------------------------
# 7. Edge cases: empty file, comments-only file, on_intent_executed fact
# ---------------------------------------------------------------------------


def test_empty_file_parses_but_no_strategy_class(tmp_path: Path) -> None:
    """An empty file is valid Python and must not raise."""
    strategy_file = _write(tmp_path / "ef", "")
    report, facts = _scan(strategy_file)
    # No findings from the AST scanner itself — the loader handles "no class".
    assert facts["class_name"] is None
    # Specifically: no missing_* findings and no placeholder spam.
    assert "missing_teardown_intents" not in _codes(report)
    assert "missing_get_open_positions" not in _codes(report)
    assert "placeholder_address" not in _codes(report)


def test_comments_only_file_parses(tmp_path: Path) -> None:
    """A comments-only file is valid Python and must not raise."""
    strategy_file = _write(
        tmp_path / "co",
        "# just comments\n"
        "# no code at all\n"
        "# not even a class\n",
    )
    report, facts = _scan(strategy_file)
    assert facts["class_name"] is None
    assert not _codes(report)


def test_has_on_intent_executed_fact(tmp_path: Path) -> None:
    """``on_intent_executed`` presence must be reflected in facts."""
    strategy_file = _write(
        tmp_path / "oie",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def on_intent_executed(self, intent, receipt): pass\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    _report, facts = _scan(strategy_file)
    assert facts["has_on_intent_executed"] is True


# ---------------------------------------------------------------------------
# 8. Return-shape contract: always ``(tree_or_None, facts_dict)``
# ---------------------------------------------------------------------------


def test_return_shape_on_clean_file(tmp_path: Path) -> None:
    """The scanner must return an ``ast.Module`` + the fact dict on success."""
    import ast as _ast

    strategy_file = _write(
        tmp_path / "rs",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return [1]\n',
    )
    report = CheckReport(strategy_dir=str(strategy_file.parent))
    tree, facts = _ast_scan_strategy_file(strategy_file, report)
    assert isinstance(tree, _ast.Module)
    # Every documented fact key is present, even when False / None.
    assert set(facts.keys()) >= {
        "imports_position_info",
        "overrides_get_open_positions",
        "overrides_generate_teardown_intents",
        "teardown_body_empty",
        "has_on_intent_executed",
        "class_name",
    }


def test_finding_instances_are_appended_to_report(tmp_path: Path) -> None:
    """Findings are pushed onto the shared ``CheckReport``, not returned."""
    strategy_file = _write(
        tmp_path / "fi",
        'from almanak.framework.strategies.intent_strategy import IntentStrategy\n'
        'ADDR = "0x_SET_X"\n\n'
        'class S(IntentStrategy):\n'
        '    def generate_teardown_intents(self, mode=None, market=None):\n'
        '        return []\n',
    )
    report = CheckReport(strategy_dir=str(strategy_file.parent))
    _tree, _facts = _ast_scan_strategy_file(strategy_file, report)
    # At minimum: placeholder + empty teardown.
    codes = [f.code for f in report.findings]
    assert "placeholder_address" in codes
    assert "empty_teardown_intents" in codes
    # Every appended finding is a real Finding dataclass (not a dict).
    assert all(isinstance(f, Finding) for f in report.findings)
