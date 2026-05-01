"""Regression guard: importing the Streamlit dashboard must not pull in
heavy modules that the deployed dashboard container has no need for.

The deployed V2 dashboard image is built by Cloud Build and then has a fixed
set of packages stripped out at the end of the build (see
``platform/packages/backend/src/templates/Dockerfile.dashboard`` and the
companion ``strip-list-dashboard.txt``). This test asserts the cause that
justifies that strip: the dashboard reads strategy state via the gateway's
DashboardService gRPC endpoint and renders Streamlit + Plotly. It does not
need optuna, matplotlib, the backtesting framework, the execution / intent
stack, or any protocol connector adapters at module-import time.

This test is the contract for the dashboard strip list. If a future PR adds
a module-level import of one of the forbidden packages, this test fails in
CI before the strip can break in production. Either:
  - move the import to function scope, or
  - extend the lazy dispatch map in the affected ``__init__.py``, or
  - if the dep is genuinely needed at runtime, also remove it from
    ``platform/packages/backend/src/templates/strip-list-dashboard.txt``.

Companion tests:
- ``tests/gateway/test_imports_lean.py`` — same pattern for the gateway sidecar.
- ``tests/framework/runner/test_imports_lean.py`` — same pattern for the
  strategy runner.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

# Backtesting / non-Plotly UI deps. ``streamlit``, ``plotly``, ``altair``,
# and ``pyarrow`` are intentionally NOT here — the dashboard renders with
# all of them at runtime (streamlit pulls altair + pyarrow).
_FORBIDDEN_THIRD_PARTY = (
    "matplotlib",
    "optuna",
    "tqdm",
    "sqlalchemy",
    "alembic",
    "simple_term_menu",
)

# The dashboard is a read-only view over gateway state. It pulls model
# types from ``almanak.framework.{models,api}`` (e.g. TimelineEvent) and
# talks to the gateway via ``almanak.framework.gateway_client``, but it
# should never load the execution stack, intent compiler, runner,
# backtesting framework, or strategy auto-discovery — all of those are
# strategy-side concerns.
_FORBIDDEN_FRAMEWORK_SUBPACKAGES = (
    "almanak.framework.backtesting",
    "almanak.framework.execution",
    "almanak.framework.intents",
    "almanak.framework.runner",
    "almanak.framework.deployment",
    "almanak.framework.testing",
    "almanak.framework.strategies",
)


def _import_dashboard_in_subprocess() -> set[str]:
    """Import the Streamlit dashboard module in a fresh subprocess.

    A subprocess is required to avoid pollution from pytest plugins. The
    dashboard module imports ``streamlit`` at module level — that is fine
    because streamlit is installed in the deployed dashboard image and
    intentionally retained.
    """
    script = textwrap.dedent(
        """
        import json
        import sys
        import almanak.framework.dashboard.app  # noqa: F401
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


def test_dashboard_import_does_not_pull_heavy_modules() -> None:
    loaded = _import_dashboard_in_subprocess()

    failures: list[str] = []
    failures.extend(_check_absent(loaded, _FORBIDDEN_THIRD_PARTY, "third-party"))
    failures.extend(_check_absent(loaded, _FORBIDDEN_FRAMEWORK_SUBPACKAGES, "framework subpackage"))

    if failures:
        msg_lines = [
            "Importing almanak.framework.dashboard.app pulled in modules the",
            "deployed dashboard container does not need at startup. The most",
            "likely culprit is a new module-level import in the dashboard pages",
            "or in one of the lazy __init__.py files. Either move the import to",
            "function scope, or extend the lazy dispatch map.",
            "",
            "If the new import is genuinely required at runtime, also update",
            "platform/packages/backend/src/templates/strip-list-dashboard.txt",
            "so the package stays in the deployed image.",
            "",
            *failures,
        ]
        raise AssertionError("\n".join(msg_lines))
