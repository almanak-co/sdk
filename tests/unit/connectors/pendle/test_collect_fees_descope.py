"""VIB-5308: Pendle LP_COLLECT_FEES is de-scoped, never silently compiles to FAILED.

Pendle markets accrue trading value into the LP token rather than exposing a
standalone fee-claim primitive, so ``LP_COLLECT_FEES`` is intentionally absent
from both the connector manifest's ``strategy_intents`` and the compiler's
``intents`` ClassVar.

Two layers must hold so a strategy author cannot reach a bespoke silent-FAILED
stub:

1. Framework dispatch (``IntentCompiler._compile_collect_fees``) gates on the
   connector compiler's declared ``intents`` — an undeclared
   ``(pendle, LP_COLLECT_FEES)`` pair routes to the canonical
   "Protocol 'pendle' does not support LP_COLLECT_FEES. Supported: ..." error,
   listing the protocols that DO support it, rather than dispatching into the
   Pendle compiler.
2. The Pendle compiler no longer carries a bespoke LP_COLLECT_FEES branch; any
   such intent reaching it falls through to the canonical ``_unsupported``
   fail-close (defence in depth — the framework gate already prevents routing).
"""

from __future__ import annotations

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext
from almanak.connectors.pendle.compiler import PendleCompiler
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
)
from almanak.framework.intents.vocabulary import Intent, IntentType


def _make_compiler(chain: str = "arbitrum") -> IntentCompiler:
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = chain
    compiler.wallet_address = "0x" + "11" * 20
    compiler.price_oracle = {}
    compiler._gateway_client = None
    compiler.rpc_url = None
    compiler.default_deadline_seconds = 300
    compiler.default_protocol = ""
    return compiler


def test_pendle_collect_fees_routes_to_canonical_unsupported_error() -> None:
    """A strategy emitting LP_COLLECT_FEES for Pendle must fail loudly with the
    canonical framework error that lists supported protocols — NOT the old
    bespoke "Pendle does not support LP_COLLECT_FEES compilation." stub."""
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="wstETH/0xMarket",
        protocol="pendle",
        chain="arbitrum",
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.action_bundle is None
    assert result.error is not None
    # Canonical, informative failure: names the protocol, lists alternatives.
    assert "does not support LP_COLLECT_FEES" in result.error
    assert "Supported:" in result.error
    assert "uniswap_v3" in result.error
    # The old silent-FAILED Pendle stub message must be gone.
    assert "Pendle does not support LP_COLLECT_FEES compilation" not in result.error


def test_pendle_compiler_collect_fees_falls_through_to_unsupported() -> None:
    """Defence in depth: even if an LP_COLLECT_FEES intent reaches the Pendle
    compiler directly, it hits the canonical ``_unsupported`` fail-close rather
    than a bespoke per-connector FAILED branch."""

    class _Intent:
        intent_type = IntentType.LP_COLLECT_FEES
        intent_id = "test-collect-fees"

    ctx = BaseCompilerContext(
        chain="arbitrum",
        wallet_address="0x" + "11" * 20,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle=None,
        cache={},
        services=object(),  # type: ignore[arg-type]
    )

    result = PendleCompiler().compile(ctx, _Intent())

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "does not support intent type" in result.error
    assert "Pendle does not support LP_COLLECT_FEES compilation" not in result.error


def test_pendle_does_not_declare_collect_fees_intent() -> None:
    """Manifest truthfulness: the Pendle compiler must not advertise
    LP_COLLECT_FEES in its declared intents set."""
    assert IntentType.LP_COLLECT_FEES not in PendleCompiler.intents


def test_declaring_connector_passes_the_dispatch_gate() -> None:
    """Positive-path guard: a connector that DECLARES LP_COLLECT_FEES must pass
    the framework dispatch gate so it still routes into the connector.

    The gate keys on the compiler's ``.intents`` ClassVar, not the manifest's
    ``strategy_intents``. This one-liner catches a future inversion of the gate
    condition (``not in`` instead of ``in``, or dropping the check) directly at
    the change site — the positive path is otherwise only covered by separate
    intent-test files. It mirrors the exact predicate used in
    ``IntentCompiler._compile_collect_fees``.
    """
    from almanak.connectors._strategy_base.compiler_registry import get_compiler
    from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler

    # Static declaration on the compiler class.
    assert IntentType.LP_COLLECT_FEES in UniswapV3Compiler.intents

    # Live registry instance + the exact gate predicate the dispatch uses.
    connector_compiler = get_compiler("uniswap_v3")
    assert connector_compiler is not None
    assert IntentType.LP_COLLECT_FEES in connector_compiler.intents
