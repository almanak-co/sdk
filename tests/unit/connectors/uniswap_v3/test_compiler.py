from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.connectors.base.compiler import CLCompilerContext
from almanak.framework.connectors.uniswap_v3.compiler import MAX_UINT128, UniswapV3Compiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import CollectFeesIntent


class _Adapter:
    def __init__(self, position_manager: str = "0x1111111111111111111111111111111111111111") -> None:
        self.position_manager = position_manager
        self.collect_args = None

    def get_position_manager_address(self) -> str:
        return self.position_manager

    def get_collect_calldata(self, *, token_id: int, recipient: str, amount0_max: int, amount1_max: int) -> bytes:
        self.collect_args = {
            "token_id": token_id,
            "recipient": recipient,
            "amount0_max": amount0_max,
            "amount1_max": amount1_max,
        }
        return b"\x12\x34\x56\x78"


def _ctx(adapter: _Adapter) -> CLCompilerContext:
    return CLCompilerContext(
        chain="arbitrum",
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle={},
        cache={},
        services=MagicMock(),
        protocol="pancakeswap_v3",
        default_swap_adapter_factory=MagicMock(),
        lp_adapter_factory=lambda _protocol: adapter,
        swap_pool_selection_mode="auto",
        fixed_swap_fee_tier=None,
        default_deadline_seconds=1200,
        default_lp_slippage=Decimal("0.005"),
        max_price_impact_pct=Decimal("0.05"),
        using_placeholders=False,
    )


def test_uniswap_v3_compiler_collect_fees_builds_collect_bundle():
    adapter = _Adapter()
    intent = CollectFeesIntent(
        pool="WETH/USDC/500",
        protocol="pancakeswap_v3",
        protocol_params={"position_id": 123},
    )

    result = UniswapV3Compiler().compile_collect_fees(_ctx(adapter), intent)

    assert result.status is CompilationStatus.SUCCESS
    assert result.total_gas_estimate > 0
    assert result.transactions[0].to == adapter.position_manager
    assert result.transactions[0].data == "0x12345678"
    assert result.action_bundle.metadata["token_id"] == 123
    assert result.action_bundle.metadata["protocol"] == "pancakeswap_v3"
    assert adapter.collect_args == {
        "token_id": 123,
        "recipient": "0x2222222222222222222222222222222222222222",
        "amount0_max": MAX_UINT128,
        "amount1_max": MAX_UINT128,
    }


def test_uniswap_v3_compiler_collect_fees_requires_position_id():
    intent = CollectFeesIntent(pool="WETH/USDC/500", protocol="pancakeswap_v3")

    result = UniswapV3Compiler().compile_collect_fees(_ctx(_Adapter()), intent)

    assert result.status is CompilationStatus.FAILED
    assert "requires 'position_id'" in result.error


def test_uniswap_v3_compiler_collect_fees_rejects_invalid_position_id():
    intent = CollectFeesIntent(
        pool="WETH/USDC/500",
        protocol="pancakeswap_v3",
        protocol_params={"position_id": "not-int"},
    )

    result = UniswapV3Compiler().compile_collect_fees(_ctx(_Adapter()), intent)

    assert result.status is CompilationStatus.FAILED
    assert "Invalid position_id" in result.error


def test_uniswap_v3_compiler_collect_fees_rejects_unknown_position_manager():
    intent = CollectFeesIntent(
        pool="WETH/USDC/500",
        protocol="pancakeswap_v3",
        protocol_params={"position_id": 123},
    )
    adapter = _Adapter(position_manager="0x0000000000000000000000000000000000000000")

    result = UniswapV3Compiler().compile_collect_fees(_ctx(adapter), intent)

    assert result.status is CompilationStatus.FAILED
    assert "Unknown position manager" in result.error
