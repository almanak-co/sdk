"""Lean-import budget for VIB-4062 ``almanak.framework.market``.

PRD §4.8 — the gateway sidecar must not pay heavy import costs to load the
public market surface. Importing ``almanak.framework.market`` and the public
classes MUST NOT pull in pandas, numpy, scipy, matplotlib, plotly,
streamlit, optuna, web3, or heavy connector packages.

Mirrors the pattern of ``tests/gateway/test_imports_lean.py``.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import textwrap

FORBIDDEN_MODULES = (
    "pandas",
    "pyarrow",
    "numpy",
    "scipy",
    "matplotlib",
    "plotly",
    "streamlit",
    "optuna",
    "web3",
    "solana",
    "anchorpy",
)


def test_market_package_lean_import_in_subprocess():
    """Run the import in a clean subprocess to avoid pollution from earlier
    test modules that legitimately use these dependencies.
    """
    code = textwrap.dedent(
        """
        import sys
        import almanak.framework.market
        from almanak.framework.market import (
            MarketSnapshot,
            MultiChainMarketSnapshot,
            TokenBalance,
            PriceData,
            RSIData,
            ChainNotConfiguredError,
            AmbiguousChainError,
        )
        forbidden = (
            "pandas", "pyarrow", "numpy", "scipy", "matplotlib", "plotly",
            "streamlit", "optuna", "web3", "solana", "anchorpy",
        )
        leaked = sorted(m for m in forbidden if m in sys.modules)
        if leaked:
            print("LEAKED:" + ",".join(leaked))
            raise SystemExit(1)
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Lean-import test failed.\n"
        f"stdout: {result.stdout!r}\n"
        f"stderr: {result.stderr!r}"
    )
    assert "OK" in result.stdout
