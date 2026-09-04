"""Branch-focused TraderJoe V2 LP open and fee collection compiler tests."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.traderjoe_v2.compiler import (
    TraderJoeV2Compiler,
    _ResolvedLBPair,
    _TraderJoeV2CompileImpl,
)
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import (
    CompilationResult,
    CompilationStatus,
    TokenInfo,
    TransactionData,
)
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPOpenIntent

WALLET = "0x" + "11" * 20
WAVAX = "0x" + "22" * 20
USDC = "0x" + "33" * 20
ROUTER = "0x" + "44" * 20
PAIR = "0x" + "55" * 20


def _impl() -> _TraderJoeV2CompileImpl:
    compiler = IntentCompiler(
        chain="avalanche",
        wallet_address=WALLET,
        rpc_url="http://anvil:8545",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    ctx = compiler._build_compiler_context("traderjoe_v2", TraderJoeV2Compiler())
    return _TraderJoeV2CompileImpl(ctx)


def _pair() -> _ResolvedLBPair:
    return _ResolvedLBPair(
        token_x=TokenInfo("WAVAX", WAVAX, 18),
        token_y=TokenInfo("USDC", USDC, 6),
        token_x_symbol="WAVAX",
        token_y_symbol="USDC",
        bin_step=20,
        gateway_client=None,
        rpc_url="http://anvil:8545",
    )


def _open_intent() -> LPOpenIntent:
    return LPOpenIntent(
        pool="WAVAX/USDC/20",
        amount0=Decimal("1.25"),
        amount1=Decimal("200"),
        range_lower=Decimal("10"),
        range_upper=Decimal("100"),
        protocol="traderjoe_v2",
        chain="avalanche",
    )


def _collect_intent(pool: str = "WAVAX/USDC/20") -> CollectFeesIntent:
    return CollectFeesIntent(pool=pool, protocol="traderjoe_v2", chain="avalanche")


def _adapter_tx(*, data: str | bytes = "0x225b20b9", gas: int | None = 200_000) -> SimpleNamespace:
    return SimpleNamespace(to=PAIR, value=0, data=data, gas=gas)


def test_lp_open_symbolic_pair_preserves_approval_and_action_order() -> None:
    impl = _impl()
    intent = _open_intent()
    approval_x = TransactionData(WAVAX, 0, "0xaaaa", 10, "approve x", "approve")
    approval_y = TransactionData(USDC, 0, "0xbbbb", 20, "approve y", "approve")
    adapter = MagicMock()
    adapter.build_add_liquidity_transaction.return_value = _adapter_tx(data=b"\x12\x34", gas=None)

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair_for_open", return_value=_pair()) as resolve,
        patch.object(impl, "_resolve_traderjoe_v2_lp_router", return_value=ROUTER),
        patch.object(impl, "_build_approve_tx", side_effect=[[approval_x], [approval_y]]) as approve,
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config") as config_cls,
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_lp_open_traderjoe_v2(intent)

    assert result.status is CompilationStatus.SUCCESS, result.error
    resolve.assert_called_once_with(intent)
    assert [tx.tx_type for tx in result.transactions] == ["approve", "approve", "traderjoe_v2_add_liquidity"]
    assert result.total_gas_estimate == 400_030
    assert result.action_bundle is not None
    assert [tx["tx_type"] for tx in result.action_bundle.transactions] == [
        "approve",
        "approve",
        "traderjoe_v2_add_liquidity",
    ]
    assert result.action_bundle.metadata["amount_x"] == str(int(Decimal("1.25") * 10**18))
    assert result.action_bundle.metadata["amount_y"] == str(int(Decimal("200") * 10**6))
    assert approve.call_args_list[0].args == (WAVAX, ROUTER, int(Decimal("1.25") * 10**18))
    assert approve.call_args_list[1].args == (USDC, ROUTER, int(Decimal("200") * 10**6))
    assert config_cls.call_args.kwargs["rpc_url"] == "http://anvil:8545"
    assert adapter.build_add_liquidity_transaction.call_args.kwargs == {
        "token_x": WAVAX,
        "token_y": USDC,
        "amount_x": Decimal("1.25"),
        "amount_y": Decimal("200"),
        "bin_step": 20,
        "bin_range": 5,
        "id_slippage": 5,
    }


def test_lp_open_returns_router_configuration_failure_before_adapter_work() -> None:
    impl = _impl()
    intent = _open_intent()
    failure = CompilationResult(
        status=CompilationStatus.FAILED,
        error="TraderJoe V2 not configured",
        intent_id=intent.intent_id,
    )

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair_for_open", return_value=_pair()),
        patch.object(impl, "_resolve_traderjoe_v2_lp_router", return_value=failure),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
    ):
        result = impl._compile_lp_open_traderjoe_v2(intent)

    assert result is failure
    adapter_cls.assert_not_called()


def test_lp_open_contains_resolution_exceptions() -> None:
    impl = _impl()
    with patch.object(impl, "_resolve_symbolic_lb_pair_for_open", side_effect=RuntimeError("pair read failed")):
        result = impl._compile_lp_open_traderjoe_v2(_open_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "pair read failed"


def test_collect_fees_preserves_malformed_pool_error() -> None:
    impl = _impl()
    intent = _collect_intent("WAVAX")

    with patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls:
        result = impl._compile_collect_fees_traderjoe_v2(intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Invalid pool format for TraderJoe V2: WAVAX. Expected: TOKEN_X/TOKEN_Y/BIN_STEP"
    adapter_cls.assert_not_called()


def test_collect_fees_returns_symbolic_pair_resolution_failure() -> None:
    impl = _impl()
    intent = _collect_intent()
    failure = CompilationResult(
        status=CompilationStatus.FAILED,
        error="Unknown tokens for pool WAVAX/USDC/20 on avalanche",
        intent_id=intent.intent_id,
    )

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair", return_value=failure),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
    ):
        result = impl._compile_collect_fees_traderjoe_v2(intent)

    assert result is failure
    adapter_cls.assert_not_called()


@pytest.mark.parametrize("position", [None, SimpleNamespace(bin_ids=[])], ids=["missing", "empty-bin-list"])
def test_collect_fees_returns_successful_noop_without_position(position: object | None) -> None:
    impl = _impl()
    adapter = MagicMock()
    adapter.get_position.return_value = position

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair", return_value=_pair()),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_collect_fees_traderjoe_v2(_collect_intent())

    assert result.status is CompilationStatus.SUCCESS
    assert result.transactions == []
    assert result.warnings == ["No LP position found for fee collection"]
    assert result.action_bundle is not None
    assert result.action_bundle.transactions == []
    assert result.action_bundle.metadata["warning"] == "No position found"
    adapter.build_collect_fees_transaction.assert_not_called()


def test_collect_fees_returns_successful_noop_when_builder_declines_position() -> None:
    impl = _impl()
    position = SimpleNamespace(bin_ids=[8_388_607], pool_address=PAIR)
    adapter = MagicMock()
    adapter.get_position.return_value = position
    adapter.build_collect_fees_transaction.return_value = None

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair", return_value=_pair()),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_collect_fees_traderjoe_v2(_collect_intent())

    assert result.status is CompilationStatus.SUCCESS
    assert result.transactions == []
    assert result.warnings == ["No LP position found for fee collection"]
    adapter.build_collect_fees_transaction.assert_called_once_with(
        token_x=WAVAX,
        token_y=USDC,
        bin_step=20,
        position=position,
    )


@pytest.mark.parametrize(
    ("fee_tx", "expected_gas"),
    [
        (_adapter_tx(), 200_000),
        (_adapter_tx(data=b"\x22\x5b\x20\xb9", gas=None), 200_000),
    ],
    ids=["adapter-gas-and-string-data", "fallback-gas-and-bytes-data"],
)
def test_collect_fees_success_reuses_exact_position(fee_tx: SimpleNamespace, expected_gas: int) -> None:
    impl = _impl()
    position = SimpleNamespace(bin_ids=[8_388_606, 8_388_608], pool_address=PAIR)
    adapter = MagicMock()
    adapter.get_position.return_value = position
    adapter.build_collect_fees_transaction.return_value = fee_tx

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair", return_value=_pair()),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config") as config_cls,
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_collect_fees_traderjoe_v2(_collect_intent())

    assert result.status is CompilationStatus.SUCCESS, result.error
    assert result.total_gas_estimate == expected_gas
    assert len(result.transactions) == 1
    assert result.transactions[0].data == fee_tx.data
    assert result.transactions[0].description == "Collect fees from TraderJoe V2: WAVAX/USDC"
    assert result.action_bundle is not None
    assert result.action_bundle.metadata == {
        "pool": "WAVAX/USDC/20",
        "protocol": "traderjoe_v2",
        "chain": "avalanche",
        "bin_ids": [8_388_606, 8_388_608],
    }
    adapter.get_position.assert_called_once_with(WAVAX, USDC, 20)
    adapter.build_collect_fees_transaction.assert_called_once_with(
        token_x=WAVAX,
        token_y=USDC,
        bin_step=20,
        position=position,
    )
    assert config_cls.call_args.kwargs["rpc_url"] == "http://anvil:8545"


def test_collect_fees_contains_adapter_exceptions() -> None:
    impl = _impl()
    adapter = MagicMock()
    adapter.get_position.side_effect = RuntimeError("position lookup failed")

    with (
        patch.object(impl, "_resolve_symbolic_lb_pair", return_value=_pair()),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_collect_fees_traderjoe_v2(_collect_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "position lookup failed"
