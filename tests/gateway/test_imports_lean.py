"""Regression guard: importing the gateway server must not pull in heavy
framework / Python modules that the gateway sidecar does not need at startup.

Background: ``almanak/__init__.py`` and ``almanak/framework/__init__.py``
historically eagerly re-exported the entire SDK public surface. Importing any
gateway module therefore loaded pandas, pyarrow, numpy, web3 internals, every
connector adapter, the backtesting / deployment / A-B-testing machinery, and
streamlit / plotly / matplotlib. The gateway sidecar OOM'd at the 512 Mi k8s
limit on release 2.15.1rc2 as a result. Both ``__init__.py`` layers and a
handful of intermediate subpackage inits (``api``, ``data``, ``connectors``)
are now lazy via PEP 562 ``__getattr__``.

This test asserts the *cause* (forbidden modules absent from ``sys.modules``)
not the *symptom* (process RSS), because RSS is too sensitive to Python
version, glibc allocator, and CI runner to track reliably.

If this test fails, the most likely culprit is a new module-level
``from almanak.framework.X import Y`` (or similar) added under
``almanak/gateway/`` or under one of the lazy ``__init__.py`` files. Look at
the failure message for which module slipped in, and either move the import
to function scope or extend the lazy dispatch map.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

# Modules whose presence in ``sys.modules`` after gateway import indicates the
# lazy boundary was breached. Grouped for clearer failure messages.
_FORBIDDEN_THIRD_PARTY = (
    "pandas",
    "pyarrow",
    "numpy",
    "streamlit",
    "plotly",
    "matplotlib",
    "altair",
    "optuna",
)
_FORBIDDEN_FRAMEWORK_SUBPACKAGES = (
    # These are the strategy / dashboard / deployment-side surfaces that the
    # gateway has no business loading.
    "almanak.framework.backtesting",
    "almanak.framework.deployment",
    "almanak.framework.testing",
    "almanak.framework.dashboard",
    # ``almanak.framework.data.market_snapshot`` IS pulled in by the polymarket
    # adapter so ``typing.get_type_hints(PolymarketAdapter.compile_intent)``
    # resolves the ``MarketSnapshot`` annotation at runtime. The pandas import
    # inside that module is now deferred to function scope, so loading the
    # module costs ~17 MB without pulling pandas / pyarrow / numpy — those
    # remain in the third-party forbidden set above.
)
# VIB-4835: each connector's gateway-side provider lives at
# ``almanak.connectors.<protocol>.gateway.provider`` and is loaded eagerly
# by ``_gateway_registry.py`` at gateway boot. Python's import machinery
# initialises the parent package ``almanak.connectors.<protocol>`` as a
# side effect — so the *package* will always appear in ``sys.modules``
# after gateway boot. What the lean-imports test guards is the
# strategy-side **heavy submodules** (``.adapter``, ``.sdk``,
# ``.compiler``, ``.receipt_parser``) — those must remain unloaded
# because every connector's ``__init__.py`` is PEP 562 lazy.
_FORBIDDEN_CONNECTOR_SUBMODULES = (
    ".adapter",
    ".sdk",
    ".compiler",
    ".receipt_parser",
    ".client",
    ".models",
)
_CONNECTOR_PACKAGES_TO_CHECK = (
    "almanak.connectors.aave_v3",
    "almanak.connectors.uniswap_v3",
    "almanak.connectors.morpho_blue",
    "almanak.connectors.compound_v3",
    "almanak.connectors.curve",
    "almanak.connectors.aerodrome",
    "almanak.connectors.gmx_v2",
    "almanak.connectors.hyperliquid",
    "almanak.connectors.traderjoe_v2",
    "almanak.connectors.pancakeswap_v3",
    "almanak.connectors.spark",
    "almanak.connectors.lido",
    "almanak.connectors.ethena",
    "almanak.connectors.gimo",
    "almanak.connectors.across",
    "almanak.connectors.stargate",
    "almanak.connectors.enso",
    "almanak.connectors.pendle",
    # ``polymarket`` is intentionally NOT in this list. Its gateway-side
    # service (``almanak.connectors.polymarket.gateway.service``) imports
    # several of the connector's strategy-side modules — ``exceptions``,
    # ``signer``, ``models`` — because the CLOB protocol's signing and
    # EIP-712 envelopes are shared between strategy and gateway code.
    # That sharing predates VIB-4835 and is legitimately part of the
    # gateway-side import graph for this connector. Mark polymarket as a
    # known exception rather than expanding the forbidden-submodules list
    # with carve-outs.
)


def _import_gateway_in_subprocess() -> set[str]:
    """Import the gateway server in a fresh subprocess and return ``sys.modules`` keys.

    A subprocess is required because pytest itself loads many modules
    (numpy / pandas via plugins) and we'd otherwise see false positives.
    ``ALMANAK_STRATEGIES_DIR`` is forced to a non-existent path so the
    ``_auto_discover_strategies`` side-effect in
    ``framework/strategies/__init__.py`` is a no-op (mirrors the gateway
    container, which has no ``./strategies`` directory).
    """
    script = textwrap.dedent(
        """
        import json
        import sys
        import almanak.gateway.server  # noqa: F401
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
    import json

    return set(json.loads(result.stdout))


def _check_absent(loaded: set[str], forbidden: tuple[str, ...], category: str) -> list[str]:
    return [f"{category}: {mod} is in sys.modules" for mod in forbidden if mod in loaded]


def test_gateway_import_does_not_pull_heavy_modules() -> None:
    loaded = _import_gateway_in_subprocess()

    failures: list[str] = []
    failures.extend(_check_absent(loaded, _FORBIDDEN_THIRD_PARTY, "third-party"))
    failures.extend(_check_absent(loaded, _FORBIDDEN_FRAMEWORK_SUBPACKAGES, "framework subpackage"))
    # For each connector package, assert the strategy-side heavy
    # submodules (adapter / sdk / compiler / receipt_parser / client /
    # models) are NOT loaded. The package itself is allowed to be in
    # ``sys.modules`` because the gateway-side ``gateway.provider`` lives
    # under it and Python initialises the parent package on import.
    for pkg in _CONNECTOR_PACKAGES_TO_CHECK:
        for suffix in _FORBIDDEN_CONNECTOR_SUBMODULES:
            mod = pkg + suffix
            if mod in loaded:
                failures.append(f"connector-submodule: {mod} is in sys.modules")

    if failures:
        msg_lines = [
            "Importing almanak.gateway.server pulled in modules the gateway sidecar must not eagerly load.",
            "The likely culprit is a new module-level import in almanak/gateway/ or in a lazy __init__.py file.",
            "Either move the import to function scope, or extend the lazy dispatch map.",
            "",
            *failures,
        ]
        raise AssertionError("\n".join(msg_lines))


def _import_gateway_with_dashboard_handler_lazies_in_subprocess() -> set[str]:
    """Import the gateway server, then trigger the function-scope lazy imports
    that ``DashboardServiceServicer.GetPnLSummary`` / ``GetCostStack`` /
    ``GetAuditPosture`` perform at runtime.

    Function-scope imports still execute the parent package's ``__init__.py``
    the first time they fire, so a barrel re-export inside
    ``almanak/framework/dashboard/__init__.py`` that pulls a streamlit-using
    submodule will surface here even though the server-startup import path
    (covered by ``test_gateway_import_does_not_pull_heavy_modules``) stays
    clean. This is the path that broke production in 2.15.1-rc12 (VIB-4048).
    """
    script = textwrap.dedent(
        """
        import json
        import sys
        # Step 1: server startup — same as the lean-import baseline test.
        import almanak.gateway.server  # noqa: F401
        # Step 2: replicate the function-scope lazy imports inside the
        # DashboardServiceServicer handlers in
        # almanak/gateway/services/dashboard_service.py. Keep this list in
        # sync with the handlers' ``from almanak.framework.dashboard...``
        # imports — if a handler grows a new lazy import, add it here.
        from almanak.framework.dashboard.quant_aggregations import (  # noqa: F401
            _detect_primitive,
            compute_audit_trail,
            compute_cost_stack,
            compute_pnl_summary,
            compute_reconciliation,
            evaluate_posture,
        )
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
    import json

    return set(json.loads(result.stdout))


# UI third-party packages forbidden on the gateway runtime path. Pulled out of
# ``_FORBIDDEN_THIRD_PARTY`` so the runtime-path test can apply a tighter
# subset (e.g. pandas IS allowed at runtime via numerical helpers) without
# loosening the startup-path guarantees above.
_FORBIDDEN_UI_AT_RUNTIME = (
    "streamlit",
    "plotly",
    "altair",
    "pydeck",
)


def test_gateway_dashboard_handler_lazies_do_not_pull_streamlit() -> None:
    """Regression guard for VIB-4048.

    ``2.15.1-rc12`` shipped with an eager ``from .sections import
    render_pnl_section`` in ``almanak/framework/dashboard/__init__.py``.
    The gateway's function-scope ``from almanak.framework.dashboard
    .quant_aggregations import compute_pnl_summary`` ran the package init,
    which loaded ``sections``, which imported ``streamlit``, which
    ``ModuleNotFoundError``'d in the deployed image (streamlit is in
    ``strip-list-gateway.txt``). Every ``GetPnLSummary`` / ``GetCostStack``
    RPC failed in production while the existing
    ``test_gateway_import_does_not_pull_heavy_modules`` test stayed green
    because the server-startup path never traversed
    ``framework.dashboard``. This test simulates the runtime trigger so
    the same class of regression fails in CI before reaching production.
    """
    loaded = _import_gateway_with_dashboard_handler_lazies_in_subprocess()
    failures = _check_absent(loaded, _FORBIDDEN_UI_AT_RUNTIME, "third-party (runtime path)")

    if failures:
        msg_lines = [
            "Triggering the gateway's dashboard-handler lazy imports pulled",
            "in UI modules the gateway sidecar image strips. The most likely",
            "culprit is a new module-level streamlit/plotly import reachable",
            "from almanak/framework/dashboard/__init__.py — either via a new",
            "eager re-export in the package barrel, or via a streamlit-free",
            "submodule that grew a transitive UI dependency.",
            "",
            "Fix: keep streamlit-using names in the _LAZY_IMPORTS map in",
            "almanak/framework/dashboard/__init__.py, behind PEP 562",
            "__getattr__ resolution.",
            "",
            *failures,
        ]
        raise AssertionError("\n".join(msg_lines))
