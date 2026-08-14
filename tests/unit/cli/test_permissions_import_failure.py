"""``almanak strat permissions`` must fail legibly when a connector's hints are broken.

VIB-6018 made ``get_permission_hints`` fail closed rather than substitute empty
hints. That is the right direction, but it has a blast radius the user cannot
guess: ``_derive_membership_sets`` folds ``get_permission_hints`` over EVERY
connector slug, so one broken module stops manifest generation for all of them —
including strategies that do not use that connector.

Left unhandled, that reaches the operator as a raw traceback. A bare
``ModuleNotFoundError: No module named 'x'`` reads as a broken install, and the
tempting workaround — hand-writing a manifest — produces an incomplete one that
reverts ``unauthorized`` under Safe. So the CLI must convert BOTH raise paths
into an actionable ``ClickException``:

* ``PermissionHintsError`` — module present, export malformed;
* ``ImportError`` / ``ModuleNotFoundError`` — a broken import *inside* an
  existing hints module. This is the shape the branch is named for and the more
  common one, and it was initially left uncaught.

The logger-restoration assertion matters because the command lowers the compiler
logger to CRITICAL for the duration of the sweep. If an exception skipped the
restore, every later command in the same process would run with compiler logging
silently suppressed.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.permissions import permissions
from almanak.framework.permissions.hints import PermissionHintsError

_COMPILER_LOGGER = "almanak.framework.intents.compiler"

# A REAL demo strategy, not a synthesised one: the CLI's loader requires a
# concrete IntentStrategy subclass, and hand-rolling a minimal stand-in only
# tests the stand-in. spark_lender is a simple single-chain EVM lender.
_STRATEGY_DIR = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies" / "spark_lender"


@pytest.fixture
def strategy_dir() -> Path:
    assert (_STRATEGY_DIR / "strategy.py").exists(), f"demo strategy moved: {_STRATEGY_DIR}"
    return _STRATEGY_DIR


@pytest.mark.parametrize(
    ("exc", "label"),
    [
        (PermissionHintsError("almanak.connectors.curve.permission_hints exports no usable hints"), "malformed"),
        (ModuleNotFoundError("No module named 'some_dep'", name="some_dep"), "nested-import"),
        (ImportError("cannot import name 'CURVE_TEST_POOLS'"), "nested-symbol"),
    ],
)
def test_broken_hints_produce_an_actionable_cli_error(
    strategy_dir: Path, monkeypatch: pytest.MonkeyPatch, exc: Exception, label: str
) -> None:
    """Every raise path becomes a non-zero ClickException naming the remedy."""
    import almanak.framework.permissions.generator as generator

    def boom(*_args: object, **_kwargs: object) -> object:
        raise exc

    monkeypatch.setattr(generator, "generate_manifest", boom)

    result = CliRunner().invoke(permissions, ["-d", str(strategy_dir)])

    assert result.exit_code != 0, f"{label}: expected a failure exit code"
    combined = result.output + str(result.exception or "")
    # The remedy, not just the raw error: the fault may be in a connector the
    # user never named, and hand-writing a manifest must be discouraged.
    assert "permission_hints" in combined, f"{label}: message does not name the failing surface:\n{combined}"
    assert "hand-write" in combined or "hand-written" in combined, (
        f"{label}: message does not warn against hand-writing a manifest:\n{combined}"
    )
    assert "Traceback (most recent call last)" not in result.output, (
        f"{label}: raw traceback leaked to the user instead of a ClickException"
    )


def test_compiler_logger_is_restored_after_a_hints_failure(strategy_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The command lowers the compiler logger; an error must not strand it there."""
    import almanak.framework.permissions.generator as generator

    compiler_logger = logging.getLogger(_COMPILER_LOGGER)
    original_level = compiler_logger.level
    try:
        compiler_logger.setLevel(logging.INFO)
        before = compiler_logger.level

        def boom(*_args: object, **_kwargs: object) -> object:
            raise PermissionHintsError("broken")

        monkeypatch.setattr(generator, "generate_manifest", boom)
        CliRunner().invoke(permissions, ["-d", str(strategy_dir)])

        assert compiler_logger.level == before, (
            "the compiler logger was left at CRITICAL after a failure — every later "
            "command in this process would run with compiler logging suppressed"
        )
    finally:
        # Logger levels are process-global and this one is not restored by the
        # command under test, so leaking INFO here would silently change what
        # later tests in the same xdist worker observe.
        compiler_logger.setLevel(original_level)
