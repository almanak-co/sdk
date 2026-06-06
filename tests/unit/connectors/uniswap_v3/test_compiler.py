from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.connectors._strategy_base.base.compiler import CLAdapterFactoryContext, CLCompilerContext
from almanak.connectors._strategy_base.swap_quote_registry import SwapQuoteResult, SwapQuoteUnavailable
from almanak.connectors.uniswap_v3.adapter import UniswapV3LPAdapter
from almanak.connectors.uniswap_v3.compiler import MAX_UINT128, UniswapV3Compiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo
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


def _ctx(adapter: object, *, rpc_url: str | None = None) -> CLCompilerContext:
    return CLCompilerContext(
        chain="arbitrum",
        wallet_address="0x2222222222222222222222222222222222222222",
        rpc_url=rpc_url,
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


def _swap_intent() -> MagicMock:
    intent = MagicMock()
    intent.from_token = "USDC"
    intent.to_token = "WETH"
    intent.max_slippage = Decimal("0.01")
    intent.max_price_impact = Decimal("0.05")
    intent.intent_id = "swap-1"
    return intent


def test_uniswap_v3_compiler_owns_lp_adapter_factory() -> None:
    factory = UniswapV3Compiler().build_lp_adapter_factory(
        CLAdapterFactoryContext(
            chain="arbitrum",
            rpc_url=None,
            rpc_timeout=10.0,
            gateway_client=None,
            swap_pool_selection_mode="auto",
            fixed_swap_fee_tier=None,
        )
    )

    adapter = factory("pancakeswap_v3")

    assert isinstance(adapter, UniswapV3LPAdapter)
    assert adapter.chain == "arbitrum"
    assert adapter.protocol == "pancakeswap_v3"


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


def test_uniswap_v3_swap_slippage_uses_quoter_directly_when_above_oracle() -> None:
    min_output, quoted_for_metrics, clamped_expected = UniswapV3Compiler._apply_swap_slippage_and_impact(
        ctx=_ctx(_Adapter()),
        intent=_swap_intent(),
        oracle_estimate=1_000,
        quoter_amount=1_200,
    )

    assert min_output == 1_188
    assert quoted_for_metrics == 1_200
    assert clamped_expected == 1_200


def test_uniswap_v3_swap_price_impact_guard_skips_canonical_local_rpc() -> None:
    result = UniswapV3Compiler._apply_swap_slippage_and_impact(
        ctx=_ctx(_Adapter(), rpc_url="http://0.0.0.0:8545"),
        intent=_swap_intent(),
        oracle_estimate=1_000,
        quoter_amount=100,
    )

    assert not isinstance(result, CompilationResult)
    assert result == (99, 100, 100)


def test_uniswap_v3_swap_price_impact_guard_fails_nonlocal_rpc() -> None:
    result = UniswapV3Compiler._apply_swap_slippage_and_impact(
        ctx=_ctx(_Adapter(), rpc_url="https://arb.example.invalid"),
        intent=_swap_intent(),
        oracle_estimate=1_000,
        quoter_amount=100,
    )

    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert "Price impact too high" in result.error


def test_uniswap_v3_local_rpc_detection_delegates_to_canonical_helper() -> None:
    assert UniswapV3Compiler._is_local_anvil_rpc("http://0.0.0.0:8545")
    assert UniswapV3Compiler._is_local_anvil_rpc("http://[::1]:8545")
    assert not UniswapV3Compiler._is_local_anvil_rpc("https://arb.example.invalid")


def test_uniswap_v3_swap_quote_registry_result_stamps_adapter_selection(monkeypatch) -> None:
    registry = MagicMock()
    registry.quote_swap.return_value = SwapQuoteResult(
        amount_out=1_234,
        source="uniswap_v3_quoter",
        metadata={
            "fee_tier": 500,
            "fee_selection": {
                "mode": "auto",
                "source": "quoter_best_quote",
                "selected_fee_tier": 500,
                "candidate_fee_tiers": [500, 3000],
            },
        },
    )
    monkeypatch.setattr("almanak.connectors.uniswap_v3.compiler.SWAP_QUOTE_REGISTRY", registry)
    adapter = MagicMock()

    amount_out = UniswapV3Compiler._quote_swap_via_registry(
        ctx=_ctx(adapter),
        protocol="uniswap_v3",
        from_token=TokenInfo("USDC", "0xaf88d065e77c8cc2239327c5edb3a432268e5831", 6),
        to_token=TokenInfo("WETH", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", 18),
        actual_from_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        actual_to_token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        amount_in=1_000_000,
        adapter=adapter,
    )

    assert amount_out == 1_234
    request = registry.quote_swap.call_args.args[1]
    assert request.protocol == "uniswap_v3"
    assert request.amount_in == 1_000_000
    adapter.apply_external_quote_selection.assert_called_once_with(
        fee_tier=500,
        amount_out=1_234,
        source="uniswap_v3_quoter",
        fee_selection={
            "mode": "auto",
            "source": "quoter_best_quote",
            "selected_fee_tier": 500,
            "candidate_fee_tiers": [500, 3000],
        },
    )


def test_uniswap_v3_swap_quote_registry_unavailable_falls_back(monkeypatch) -> None:
    registry = MagicMock()
    registry.quote_swap.side_effect = SwapQuoteUnavailable("missing quoter")
    monkeypatch.setattr("almanak.connectors.uniswap_v3.compiler.SWAP_QUOTE_REGISTRY", registry)
    adapter = MagicMock()

    amount_out = UniswapV3Compiler._quote_swap_via_registry(
        ctx=_ctx(adapter),
        protocol="uniswap_v3",
        from_token=TokenInfo("USDC", "0xaf88d065e77c8cc2239327c5edb3a432268e5831", 6),
        to_token=TokenInfo("WETH", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", 18),
        actual_from_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        actual_to_token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        amount_in=1_000_000,
        adapter=adapter,
    )

    assert amount_out is None
    adapter.apply_external_quote_selection.assert_not_called()
