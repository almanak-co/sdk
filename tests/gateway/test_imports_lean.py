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
_FORBIDDEN_CONNECTORS = (
    # The gateway only needs the polymarket connector at module level (via
    # polymarket_service). Every other connector adapter must remain unloaded
    # until a strategy actually uses it.
    "almanak.framework.connectors.aave_v3",
    "almanak.framework.connectors.uniswap_v3",
    "almanak.framework.connectors.morpho_blue",
    "almanak.framework.connectors.compound_v3",
    "almanak.framework.connectors.curve",
    "almanak.framework.connectors.aerodrome",
    "almanak.framework.connectors.gmx_v2",
    "almanak.framework.connectors.hyperliquid",
    "almanak.framework.connectors.traderjoe_v2",
    "almanak.framework.connectors.pancakeswap_v3",
    "almanak.framework.connectors.spark",
    "almanak.framework.connectors.lido",
    "almanak.framework.connectors.ethena",
    "almanak.framework.connectors.gimo",
    "almanak.framework.connectors.across",
    "almanak.framework.connectors.stargate",
    "almanak.framework.connectors.enso",
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
    failures.extend(_check_absent(loaded, _FORBIDDEN_CONNECTORS, "connector"))

    if failures:
        msg_lines = [
            "Importing almanak.gateway.server pulled in modules the gateway sidecar must not eagerly load.",
            "The likely culprit is a new module-level import in almanak/gateway/ or in a lazy __init__.py file.",
            "Either move the import to function scope, or extend the lazy dispatch map.",
            "",
            *failures,
        ]
        raise AssertionError("\n".join(msg_lines))
