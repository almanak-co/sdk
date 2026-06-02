"""Permission discovery hints for Camelot (Algebra V3 fork on Arbitrum).

Camelot's Phase-2 fold (``docs/internal/plans/camelot-compiler-connector-folding-plan.md``)
ships a SWAP-only ``CamelotCompiler`` that subclasses ``UniswapV3Compiler``.
LP / collect-fees paths are explicit fail-closed stubs, so no synthetic LP
discovery runs and no LP-pair override is needed. Algebra V1.9 sets fees
dynamically per pool, so there is no fixed fee-tier to override. The
swap-router permission is generated generically by synthetic-intent
discovery from the SWAP intent type — no static permissions or market IDs
required.

The only non-default hint is ``synthetic_discovery_intents={"SWAP"}`` (VIB-4928)
— Camelot participates in synthetic SWAP discovery only. It is a Solidly-style
Algebra router (no native-in msg.value auto-wrap path tested today), so
``supports_native_in_swap`` stays False. Revisit only if Camelot ever grows
LP / collect-fees / lending support, or if synthetic LP discovery routes
through Arbitrum's default ``(USDC, WETH)`` pair and that pair lacks Camelot
liquidity (today it does — VIB-1636 Algebra V3 SwapRouter on Arbitrum).
"""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    synthetic_discovery_intents=frozenset({"SWAP"}),
)
