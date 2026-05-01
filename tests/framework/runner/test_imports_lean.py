"""Regression guard: importing the strategy runner must not pull in heavy
modules that the deployed strategy container has no need for at startup.

The deployed V2 strategy image is built by Cloud Build and then has a fixed
set of packages stripped out at the end of the build (see
``platform/packages/backend/src/templates/Dockerfile.strategy`` and the
companion ``strip-list-strategy.txt``). This test asserts the cause that
justifies that strip: importing the runner never reaches into streamlit /
plotly / matplotlib / optuna / dashboard / backtesting at module load, so
removing those packages from the deployed venv is safe.

This test is the contract for the strategy strip list. If a future PR adds
a module-level import of one of the forbidden packages, this test fails in
CI before the strip can break in production. Either:
  - move the import to function scope, or
  - extend the lazy dispatch map in the affected ``__init__.py``, or
  - if the dep is genuinely needed at runtime, also remove it from
    ``platform/packages/backend/src/templates/strip-list-strategy.txt``.

Companion tests:
- ``tests/gateway/test_imports_lean.py`` — same pattern for the gateway sidecar.
- ``tests/framework/dashboard/test_imports_lean.py`` — same pattern for the
  Streamlit dashboard.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

# UI / backtest packages — none of these belong in the strategy runtime,
# which talks to the gateway over gRPC and has no UI surface. Backtesting
# is offline-only (parameter sweeps, paper trading) and runs on the user's
# laptop, never inside a deployed strategy pod.
_FORBIDDEN_THIRD_PARTY = (
    "streamlit",
    "plotly",
    "matplotlib",
    "altair",
    "pyarrow",
    "optuna",
    "tqdm",
    "sqlalchemy",
    "alembic",
    "simple_term_menu",
)

# Framework subpackages the runner has no business loading at module level.
# ``backtesting`` is offline-only; ``dashboard`` is the Streamlit UI surface;
# ``deployment`` and ``testing`` are scaffolding code.
_FORBIDDEN_FRAMEWORK_SUBPACKAGES = (
    "almanak.framework.backtesting",
    "almanak.framework.dashboard",
    "almanak.framework.deployment",
    "almanak.framework.testing",
)


def _import_runner_in_subprocess() -> set[str]:
    """Import the strategy runner in a fresh subprocess and return ``sys.modules`` keys.

    A subprocess is required because pytest itself loads many modules (numpy
    / pandas via plugins, optuna via backtest tests) and we'd otherwise see
    false positives. ``ALMANAK_STRATEGIES_DIR`` is forced to a non-existent
    path so the ``_auto_discover_strategies`` side-effect in
    ``framework/strategies/__init__.py`` is a no-op (mirrors the deployed
    strategy container, which has no ``./strategies`` directory at SDK
    install time).
    """
    script = textwrap.dedent(
        """
        import json
        import sys
        from almanak.framework.runner.strategy_runner import StrategyRunner  # noqa: F401
        sys.stdout.write(json.dumps(sorted(sys.modules)))
        """
    )
    env = os.environ.copy()
    env["ALMANAK_STRATEGIES_DIR"] = "/nonexistent_strategies_dir_for_lean_import_test"
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return set(json.loads(result.stdout))


def _check_absent(loaded: set[str], forbidden: tuple[str, ...], category: str) -> list[str]:
    return [f"{category}: {mod} is in sys.modules" for mod in forbidden if mod in loaded]


def test_strategy_runner_import_does_not_pull_heavy_modules() -> None:
    loaded = _import_runner_in_subprocess()

    failures: list[str] = []
    failures.extend(_check_absent(loaded, _FORBIDDEN_THIRD_PARTY, "third-party"))
    failures.extend(_check_absent(loaded, _FORBIDDEN_FRAMEWORK_SUBPACKAGES, "framework subpackage"))

    if failures:
        msg_lines = [
            "Importing almanak.framework.runner.strategy_runner pulled in modules",
            "the deployed strategy container does not need at startup. The most",
            "likely culprit is a new module-level import in the runner or in one",
            "of the lazy __init__.py files. Either move the import to function",
            "scope, or extend the lazy dispatch map.",
            "",
            "If the new import is genuinely required at runtime, also update",
            "platform/packages/backend/src/templates/strip-list-strategy.txt",
            "so the package stays in the deployed image.",
            "",
            *failures,
        ]
        raise AssertionError("\n".join(msg_lines))
