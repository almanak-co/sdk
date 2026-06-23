"""Completeness test for feasibility-preflight registration (VIB-5374 / RC-2).

Registration of a venue's feasibility gate IS overriding ``preflight`` on its
connector compiler — there is no separate manifest field. This test pins that the
four connectors the RC-2 batch gates (Pendle maturity, GMX exec-fee, Stargate
native-fee, Euler LTV) actually ship a non-default ``preflight``, so a future
refactor that drops the override is caught here rather than silently re-opening
the "doomed intent reverts on-chain" gap.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.base.compiler import BaseProtocolCompiler
from almanak.connectors._strategy_base.bridge_compiler import BridgeCompiler
from almanak.connectors.euler_v2.compiler import EulerV2Compiler
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.connectors.pendle.compiler import PendleCompiler

_GATED_COMPILERS = (
    PendleCompiler,
    GMXV2Compiler,
    BridgeCompiler,
    EulerV2Compiler,
)


def test_gated_compilers_override_preflight():
    """Each gated connector compiler overrides the default base ``preflight``."""
    for cls in _GATED_COMPILERS:
        assert cls.preflight is not BaseProtocolCompiler.preflight, (
            f"{cls.__name__} declares a feasibility-gated intent but does not override preflight()"
        )


def test_gated_compilers_expose_stable_error_prefix():
    """Each gated compiler exposes a stable, non-empty, 'revert'-free error prefix."""
    prefixes = {
        PendleCompiler.MATURITY_ERROR_PREFIX,
        GMXV2Compiler.NATIVE_FEE_ERROR_PREFIX,
        BridgeCompiler.STARGATE_NATIVE_FEE_ERROR_PREFIX,
        EulerV2Compiler.BORROW_INFEASIBLE_ERROR_PREFIX,
    }
    assert len(prefixes) == 4  # all distinct
    for prefix in prefixes:
        assert prefix and "revert" not in prefix.lower()


def test_non_gated_compiler_keeps_default_preflight():
    """A connector that does NOT gate feasibility keeps the FEASIBLE default."""
    from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler

    assert UniswapV3Compiler.preflight is BaseProtocolCompiler.preflight
