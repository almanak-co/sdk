"""Tests for the test-file generator itself (``generate_test_file``).

These tests verify that:

1. Every template emits syntactically valid Python.
2. Every template emits ruff-clean Python (line length 120, same as the SDK).
3. The emitted test suite, when scaffolded end-to-end with ``almanak strat new``,
   executes successfully (pytest passes) for every template.
4. The expected test classes and test methods are present, keyed to template
   capability (stateful templates must have state-machine tests, etc).
5. Breaking the generated strategy in specific ways makes the emitted tests
   fail cleanly with actionable assertion messages.

These are generator-level integration tests. They intentionally scaffold a
fresh strategy into a temporary directory and run pytest against it.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.new_strategy import (
    _TEMPLATE_TEST_SPECS,
    StrategyTemplate,
    generate_strategy_file,
    generate_test_file,
    new_strategy,
)

# Staking template only supports Ethereum; every other template works on Arbitrum.
_TEMPLATE_CHAINS: dict[StrategyTemplate, str] = {
    t: "ethereum" if t == StrategyTemplate.STAKING else "arbitrum"
    for t in StrategyTemplate
}

STATEFUL_TEMPLATES = [
    t for t, spec in _TEMPLATE_TEST_SPECS.items() if spec.has_callbacks
]


# ---------------------------------------------------------------------------
# Structural checks on the emitted test module
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template", list(StrategyTemplate), ids=lambda t: t.value)
def test_generated_test_file_is_valid_python(template: StrategyTemplate) -> None:
    """The emitted test_strategy.py parses as valid Python for every template."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    ast.parse(code)


@pytest.mark.parametrize("template", list(StrategyTemplate), ids=lambda t: t.value)
def test_generated_test_file_passes_ruff(template: StrategyTemplate, tmp_path: Path) -> None:
    """The emitted test_strategy.py is ruff-clean at SDK line length (120).

    Note: we create a sibling ``strategy.py`` so ruff's isort correctly
    classifies ``from strategy import ...`` as first-party / local.
    """
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    # Write the test file inside a ``tests/`` subdir, with strategy.py in the parent
    # dir -- mirrors the real scaffolded layout so ruff classifies imports correctly.
    (tmp_path / "strategy.py").write_text("class GenProbeStrategy: ...\n")
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    test_path = tests_dir / "test_strategy.py"
    test_path.write_text(code)

    result = subprocess.run(
        [
            "uv", "run", "ruff", "check", str(test_path),
            "--select", "E,W,F,I",
            "--line-length", "120",
            # Treat ``strategy`` as first-party so isort groups it correctly.
            "--config", "lint.isort.known-first-party=['strategy']",
        ],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[3]),
    )
    assert result.returncode == 0, (
        f"ruff check failed for generated test file of {template.value}:\n"
        f"{result.stdout}\n{result.stderr}"
    )


@pytest.mark.parametrize("template", list(StrategyTemplate), ids=lambda t: t.value)
def test_generated_test_file_has_basics_class(template: StrategyTemplate) -> None:
    """Every emitted test file has a Test...Basics class with the core assertions."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    tree = ast.parse(code)
    class_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    assert any(n.endswith("Basics") for n in class_names), (
        f"Expected a *Basics test class in {template.value}, got: {class_names}"
    )
    assert any(n.endswith("EdgeCases") for n in class_names), (
        f"Expected a *EdgeCases test class in {template.value}, got: {class_names}"
    )
    assert any(n.endswith("Teardown") for n in class_names), (
        f"Expected a *Teardown test class in {template.value}, got: {class_names}"
    )


@pytest.mark.parametrize("template", STATEFUL_TEMPLATES, ids=lambda t: t.value)
def test_generated_test_file_has_state_machine_class_for_stateful_templates(
    template: StrategyTemplate,
) -> None:
    """Stateful templates emit StateMachine + Persistence test classes."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    tree = ast.parse(code)
    class_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    assert any(n.endswith("StateMachine") for n in class_names), (
        f"Stateful template {template.value} must emit a *StateMachine class, got: {class_names}"
    )
    assert any(n.endswith("Persistence") for n in class_names), (
        f"Stateful template {template.value} must emit a *Persistence class, got: {class_names}"
    )


def test_blank_template_skips_state_machine_class() -> None:
    """BLANK template is stateless; emitted tests must NOT include state machine class."""
    code = generate_test_file(
        name="Gen Probe",
        template=StrategyTemplate.BLANK,
        chain="arbitrum",
    )
    tree = ast.parse(code)
    class_names = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    assert not any(n.endswith("StateMachine") for n in class_names), (
        f"BLANK template must not emit a state machine test class, got: {class_names}"
    )
    assert not any(n.endswith("Persistence") for n in class_names), (
        f"BLANK template must not emit a persistence test class, got: {class_names}"
    )


@pytest.mark.parametrize("template", list(StrategyTemplate), ids=lambda t: t.value)
def test_generated_test_file_has_edge_case_methods(template: StrategyTemplate) -> None:
    """Every template's emitted tests include zero-balance and zero-price edge cases."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    assert "test_decide_with_zero_balance_does_not_raise" in code, (
        f"zero-balance edge test missing in {template.value}"
    )
    assert "test_decide_with_zero_price_does_not_raise" in code, (
        f"zero-price edge test missing in {template.value}"
    )


@pytest.mark.parametrize("template", STATEFUL_TEMPLATES, ids=lambda t: t.value)
def test_generated_test_file_has_persistence_round_trip(
    template: StrategyTemplate,
) -> None:
    """Stateful templates emit the save -> fresh instance -> load round-trip test."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    assert "test_load_persistent_state_round_trip" in code, (
        f"persistence round-trip missing in {template.value}"
    )


@pytest.mark.parametrize("template", STATEFUL_TEMPLATES, ids=lambda t: t.value)
def test_generated_test_file_has_on_intent_executed_failure_test(
    template: StrategyTemplate,
) -> None:
    """Stateful templates test that on_intent_executed(success=False) does not mutate state."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    assert "test_on_intent_executed_ignores_failures" in code, (
        f"failure-mode callback test missing in {template.value}"
    )


@pytest.mark.parametrize("template", list(StrategyTemplate), ids=lambda t: t.value)
def test_generated_test_file_tests_teardown_soft_and_hard(
    template: StrategyTemplate,
) -> None:
    """Every emitted test file tests both SOFT and HARD teardown modes."""
    code = generate_test_file(
        name="Gen Probe",
        template=template,
        chain=_TEMPLATE_CHAINS[template],
    )
    assert "test_generate_teardown_intents_soft_returns_list" in code
    assert "test_generate_teardown_intents_hard_returns_list" in code


# ---------------------------------------------------------------------------
# End-to-end: scaffold a strategy and run the emitted pytest suite
# ---------------------------------------------------------------------------


def _scaffold_and_run_pytest(template: StrategyTemplate, chain: str) -> subprocess.CompletedProcess:
    """Scaffold a strategy via the CLI and return the pytest result object.

    This is the highest-level confidence check: the whole flow that a real
    user would run (``almanak strat new`` -> ``pytest``) must succeed.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "emitted_strat"
        runner = CliRunner()
        result = runner.invoke(
            new_strategy,
            [
                "--template", template.value,
                "--name", "emitted_strat",
                "--chain", chain,
                "--output-dir", str(target),
            ],
            env={"CI": ""},
        )
        assert result.exit_code == 0, f"scaffold failed: {result.output}"

        # Run pytest inside the scaffolded strategy directory with strategy.py on sys.path.
        env = os.environ.copy()
        env["PYTHONPATH"] = str(target) + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=short", "-p", "no:cacheprovider"],
            cwd=str(target),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return proc


@pytest.mark.parametrize(
    "template",
    [
        StrategyTemplate.BLANK,
        StrategyTemplate.TA_SWAP,
        StrategyTemplate.DYNAMIC_LP,
        StrategyTemplate.LENDING_LOOP,
        StrategyTemplate.BASIS_TRADE,
        StrategyTemplate.VAULT_YIELD,
        StrategyTemplate.COPY_TRADER,
        StrategyTemplate.PERPS,
        StrategyTemplate.MULTI_STEP,
        StrategyTemplate.STAKING,
    ],
    ids=lambda t: t.value,
)
def test_emitted_tests_pass_for_each_template(template: StrategyTemplate) -> None:
    """End-to-end: scaffolded strategy + emitted tests = green pytest.

    This is the single most important test in this module: if a user runs
    ``almanak strat new`` and then ``pytest``, they must get a passing suite.
    """
    chain = _TEMPLATE_CHAINS[template]
    proc = _scaffold_and_run_pytest(template, chain)
    assert proc.returncode == 0, (
        f"Emitted tests failed for {template.value}:\n"
        f"STDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
    )


# ---------------------------------------------------------------------------
# Breakage tests: verify emitted tests catch real regressions
# ---------------------------------------------------------------------------


def test_emitted_tests_catch_missing_teardown_method() -> None:
    """If the strategy removes generate_teardown_intents(), emitted tests must fail.

    This validates that the emitted suite is not a smoke test -- breaking the
    teardown contract makes the tests fail loudly with an actionable message.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "broken_strat"
        runner = CliRunner()
        result = runner.invoke(
            new_strategy,
            [
                "--template", "ta_swap",
                "--name", "broken_strat",
                "--chain", "arbitrum",
                "--output-dir", str(target),
            ],
            env={"CI": ""},
        )
        assert result.exit_code == 0, result.output

        # Break the strategy: replace generate_teardown_intents with something
        # that returns a non-list, violating the teardown contract.
        strategy_py = target / "strategy.py"
        src = strategy_py.read_text()
        broken = src.replace(
            "def generate_teardown_intents(self, mode=None, market=None) -> list[AnyIntent]:",
            "def generate_teardown_intents(self, mode=None, market=None):",
            1,
        ).replace(
            "        intents: list[AnyIntent] = []",
            '        return "not a list"  # intentional break for testing',
            1,
        )
        assert broken != src, "Test setup error: substitution did not apply"
        strategy_py.write_text(broken)

        env = os.environ.copy()
        env["PYTHONPATH"] = str(target) + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=no", "-p", "no:cacheprovider"],
            cwd=str(target),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

        # Tests must fail
        assert proc.returncode != 0, (
            f"Expected emitted tests to catch the broken teardown contract, "
            f"but they passed:\n{proc.stdout}"
        )
        # And the failure must be on a teardown test, not a random crash
        assert (
            "generate_teardown_intents_soft_returns_list" in proc.stdout
            or "generate_teardown_intents_hard_returns_list" in proc.stdout
            or "Must return a list" in proc.stdout
        ), (
            f"Expected failure in teardown test; got output:\n{proc.stdout}"
        )


def test_emitted_tests_catch_decide_that_raises() -> None:
    """If decide() stops handling errors, the error-handling test must fail.

    Break: replace the entire decide() body with ``raise RuntimeError``. The
    emitted error-handling test must catch the propagating exception (via the
    ``assert did not raise / returns HOLD`` contract).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "broken_strat"
        runner = CliRunner()
        result = runner.invoke(
            new_strategy,
            [
                "--template", "ta_swap",
                "--name", "broken_strat",
                "--chain", "arbitrum",
                "--output-dir", str(target),
            ],
            env={"CI": ""},
        )
        assert result.exit_code == 0, result.output

        # Break the strategy: replace decide() with one that always raises.
        # The emitted error-handling test must catch the unhandled exception.
        strategy_py = target / "strategy.py"
        src = strategy_py.read_text()

        # Find the decide() function and replace its body with ``raise``.
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "decide":
                # Blank out the body in the source string using line numbers.
                lines = src.splitlines()
                # Body starts on node.body[0].lineno (1-indexed) and ends at
                # node.end_lineno (inclusive). We replace everything in between
                # with a single ``raise RuntimeError(...)`` statement.
                body_start = node.body[0].lineno - 1
                body_end = node.end_lineno  # exclusive after slicing
                indent = " " * (node.col_offset + 4)
                replacement = [
                    f'{indent}raise RuntimeError("intentional break for testing")',
                ]
                broken_lines = (
                    lines[:body_start]
                    + replacement
                    + lines[body_end:]
                )
                broken = "\n".join(broken_lines) + "\n"
                break
        else:
            pytest.fail("Test setup error: could not find decide() in strategy.py")

        assert broken != src, "Test setup error: substitution did not apply"
        strategy_py.write_text(broken)

        env = os.environ.copy()
        env["PYTHONPATH"] = str(target) + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=no", "-p", "no:cacheprovider"],
            cwd=str(target),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

        assert proc.returncode != 0, (
            f"Expected emitted error-handling test to catch the broken decide(), "
            f"but tests passed:\n{proc.stdout}"
        )


# ---------------------------------------------------------------------------
# Unit tests for the _TEMPLATE_TEST_SPECS mapping
# ---------------------------------------------------------------------------


def test_every_stateful_template_has_transitions() -> None:
    """Every stateful template must define at least one state transition to test."""
    for template in STATEFUL_TEMPLATES:
        spec = _TEMPLATE_TEST_SPECS[template]
        assert len(spec.transitions) >= 1, (
            f"Template {template.value} has has_callbacks=True but no transitions"
        )


def test_every_stateful_template_has_persistent_state_sample() -> None:
    """Every stateful template provides a sample state for round-trip testing."""
    for template in STATEFUL_TEMPLATES:
        spec = _TEMPLATE_TEST_SPECS[template]
        assert spec.persistent_state_sample is not None, (
            f"Template {template.value} missing persistent_state_sample"
        )
        assert isinstance(spec.persistent_state_sample, dict)
        assert len(spec.persistent_state_sample) > 0, (
            f"Template {template.value} has empty persistent_state_sample"
        )


def test_templates_with_teardown_intents_have_position_setup() -> None:
    """If a template claims to generate teardown intents, it must tell us how to set up a position."""
    for template, spec in _TEMPLATE_TEST_SPECS.items():
        if spec.has_teardown_intents:
            assert spec.position_setup, (
                f"Template {template.value} has has_teardown_intents=True but no position_setup"
            )


# ---------------------------------------------------------------------------
# Regression: make sure generator output still works with older callsites
# ---------------------------------------------------------------------------


def test_generate_test_file_signature_is_stable() -> None:
    """generate_test_file(name, template, chain) -> str -- caller contract is stable."""
    out = generate_test_file("My Strat", StrategyTemplate.BLANK, "arbitrum")
    assert isinstance(out, str)
    assert "class TestMyStratStrategyBasics" in out
    assert "class TestMyStratStrategyEdgeCases" in out
    assert "class TestMyStratStrategyTeardown" in out


def test_strategy_class_name_matches_between_test_file_and_strategy_file() -> None:
    """The test file imports the exact class name emitted by generate_strategy_file."""
    name = "Hyphen-And Space Name"
    strategy_code = generate_strategy_file(
        name=name,
        template=StrategyTemplate.TA_SWAP,
        chain="arbitrum",
        output_dir=Path("/tmp"),
    )
    test_code = generate_test_file(
        name=name,
        template=StrategyTemplate.TA_SWAP,
        chain="arbitrum",
    )
    # Derive expected class name from the strategy file's class definition
    strategy_tree = ast.parse(strategy_code)
    strategy_classes = [
        n for n in ast.walk(strategy_tree) if isinstance(n, ast.ClassDef)
    ]
    assert len(strategy_classes) >= 1
    emitted_class_name = strategy_classes[0].name

    # Test file must import this class
    assert f"from strategy import {emitted_class_name}" in test_code
