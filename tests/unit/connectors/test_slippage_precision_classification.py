"""Compiler-boundary classification for canonical slippage precision refusals."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.slippage import SlippagePrecisionError
from almanak.connectors.jupiter.compiler import JupiterCompiler
from almanak.connectors.uniswap_v4.compiler import UniswapV4Compiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import SwapIntent


def _sub_bp_swap(*, chain: str, protocol: str) -> SwapIntent:
    return SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("1"),
        max_slippage=Decimal("0.00005"),
        chain=chain,
        protocol=protocol,
    )


def _assert_safety_refusal(result) -> None:
    assert result.status == CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert result.is_transient is False
    assert result.transactions == []
    assert "basis point" in (result.error or "")


def test_uniswap_v4_sub_bp_tolerance_is_a_safety_refusal() -> None:
    ctx = SimpleNamespace(chain="ethereum")

    result = UniswapV4Compiler().compile_swap(
        ctx,  # type: ignore[arg-type]
        _sub_bp_swap(chain="ethereum", protocol="uniswap_v4"),
    )

    _assert_safety_refusal(result)


def test_jupiter_propagated_precision_error_is_a_safety_refusal() -> None:
    compiler = JupiterCompiler()
    adapter = MagicMock()
    adapter.compile_swap_intent.side_effect = SlippagePrecisionError(
        "max_slippage=0.00005 is positive but finer than one basis point"
    )
    ctx = SimpleNamespace(price_oracle={})

    with patch.object(compiler, "_get_adapter", return_value=adapter):
        result = compiler.compile_swap(
            ctx,  # type: ignore[arg-type]
            _sub_bp_swap(chain="solana", protocol="jupiter"),
        )

    _assert_safety_refusal(result)
