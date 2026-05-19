from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.fluid.compiler import FluidCompiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent, LPOpenIntent, SwapIntent


def _ctx(*, rpc_url: str | None = "http://localhost:8545", gateway_client: object | None = None) -> BaseCompilerContext:
    return BaseCompilerContext(
        chain="arbitrum",
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url=rpc_url,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=gateway_client,
        price_oracle={},
        cache={},
        services=MagicMock(),
    )


def test_compile_swap_fails_with_existing_disabled_message() -> None:
    intent = SwapIntent(from_token="USDC", to_token="USDT", amount=Decimal("1"), protocol="fluid")

    result = FluidCompiler().compile_swap(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.action_bundle is None
    assert result.error is not None
    assert "Fluid DEX connector is disabled" in result.error


def test_compile_lp_open_fails_with_existing_phase_one_message() -> None:
    intent = LPOpenIntent(
        pool="0x1111111111111111111111111111111111111111",
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="fluid",
    )

    result = FluidCompiler().compile_lp_open(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.action_bundle is None
    assert result.error is not None
    assert "Fluid DEX LP_OPEN is not supported in phase 1" in result.error


def test_compile_lp_close_rejects_invalid_position_id() -> None:
    intent = LPCloseIntent(
        position_id="not-int",
        pool="0x1111111111111111111111111111111111111111",
        protocol="fluid",
    )

    result = FluidCompiler().compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Invalid Fluid position ID (must be integer): not-int"


def test_compile_lp_close_requires_pool_address() -> None:
    intent = LPCloseIntent(position_id="123", pool="USDC/USDT", protocol="fluid")

    result = FluidCompiler().compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Fluid LP_CLOSE requires pool address in pool field. Got pool=USDC/USDT"


def test_compile_lp_close_builds_action_bundle_with_rpc_url() -> None:
    intent = LPCloseIntent(
        position_id="123",
        pool="0x1111111111111111111111111111111111111111",
        protocol="fluid",
    )
    adapter = MagicMock()
    adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x3333333333333333333333333333333333333333",
        value=0,
        data="0xabcdef",
        gas=250_000,
        description="Remove Fluid liquidity",
    )

    with (
        patch("almanak.framework.connectors.fluid.FluidAdapter", return_value=adapter) as adapter_cls,
        patch("almanak.framework.connectors.fluid.FluidConfig") as config_cls,
    ):
        result = FluidCompiler().compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.SUCCESS
    assert result.total_gas_estimate == 250_000
    assert result.transactions[0].tx_type == "fluid_operate_close"
    assert result.action_bundle.metadata == {
        "dex_address": "0x1111111111111111111111111111111111111111",
        "nft_id": 123,
        "protocol": "fluid",
        "chain": "arbitrum",
    }
    config_cls.assert_called_once_with(
        chain="arbitrum",
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url="http://localhost:8545",
        gateway_client=None,
    )
    adapter_cls.assert_called_once_with(config_cls.return_value)
    adapter.build_remove_liquidity_transaction.assert_called_once_with(
        dex_address="0x1111111111111111111111111111111111111111",
        nft_id=123,
    )


def test_compile_lp_close_prefers_connected_gateway_client_over_rpc_url() -> None:
    gateway_client = SimpleNamespace(is_connected=True)
    intent = LPCloseIntent(
        position_id="123",
        pool="0x1111111111111111111111111111111111111111",
        protocol="fluid",
    )
    adapter = MagicMock()
    adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x3333333333333333333333333333333333333333",
        value=0,
        data="0xabcdef",
        gas=250_000,
        description="Remove Fluid liquidity",
    )

    with (
        patch("almanak.framework.connectors.fluid.FluidAdapter", return_value=adapter),
        patch("almanak.framework.connectors.fluid.FluidConfig") as config_cls,
    ):
        result = FluidCompiler().compile_lp_close(_ctx(gateway_client=gateway_client), intent)

    assert result.status is CompilationStatus.SUCCESS
    config_cls.assert_called_once_with(
        chain="arbitrum",
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url=None,
        gateway_client=gateway_client,
    )


def test_compile_lp_close_falls_back_to_rpc_when_gateway_disconnected() -> None:
    gateway_client = SimpleNamespace(is_connected=False)
    intent = LPCloseIntent(
        position_id="123",
        pool="0x1111111111111111111111111111111111111111",
        protocol="fluid",
    )
    adapter = MagicMock()
    adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x3333333333333333333333333333333333333333",
        value=0,
        data="0xabcdef",
        gas=250_000,
        description="Remove Fluid liquidity",
    )

    with (
        patch("almanak.framework.connectors.fluid.FluidAdapter", return_value=adapter),
        patch("almanak.framework.connectors.fluid.FluidConfig") as config_cls,
    ):
        result = FluidCompiler().compile_lp_close(
            _ctx(rpc_url="http://localhost:8545", gateway_client=gateway_client), intent
        )

    assert result.status is CompilationStatus.SUCCESS
    config_cls.assert_called_once_with(
        chain="arbitrum",
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url="http://localhost:8545",
        gateway_client=None,
    )


def test_compile_lp_close_fails_without_gateway_or_rpc_url() -> None:
    intent = LPCloseIntent(
        position_id="123",
        pool="0x1111111111111111111111111111111111111111",
        protocol="fluid",
    )

    result = FluidCompiler().compile_lp_close(_ctx(rpc_url=None, gateway_client=None), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.action_bundle is None
    assert result.error == "Connected gateway_client or RPC URL required for Fluid DEX adapter."


def test_compile_lp_close_surfaces_adapter_exception() -> None:
    intent = LPCloseIntent(
        position_id="123",
        pool="0x1111111111111111111111111111111111111111",
        protocol="fluid",
    )
    adapter = MagicMock()
    adapter.build_remove_liquidity_transaction.side_effect = ValueError(
        "Fluid pool has smart-debt enabled; refusing to compile LP_CLOSE"
    )

    with (
        patch("almanak.framework.connectors.fluid.FluidAdapter", return_value=adapter),
        patch("almanak.framework.connectors.fluid.FluidConfig"),
    ):
        result = FluidCompiler().compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.action_bundle is None
    assert result.error == "Fluid pool has smart-debt enabled; refusing to compile LP_CLOSE"


def test_compile_collect_fees_reports_unsupported() -> None:
    intent = CollectFeesIntent(pool="0x1111111111111111111111111111111111111111", protocol="fluid")

    result = FluidCompiler().compile_collect_fees(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == "Fluid does not support LP_COLLECT_FEES compilation."
