"""Tests for connector-owned swap quote providers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from eth_abi import encode as abi_encode

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors._strategy_base.rpc import decode_uint256
from almanak.connectors._strategy_base.swap_quote_registry import (
    SLIPPAGE_REFERENCE_STABLE_PARITY,
    SLIPPAGE_REFERENCE_UNSUPPORTED,
    SLIPPAGE_REFERENCE_V3_SPOT,
    SwapQuoteConnector,
    SwapQuoteRegistry,
    SwapQuoteRequest,
    SwapQuoteResult,
    SwapQuoteUnavailable,
)

USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"


def _slipstream_generation(factory: str):
    from almanak.connectors.aerodrome.addresses import slipstream_deployment_for_factory

    deployment = slipstream_deployment_for_factory("base", factory)
    assert deployment is not None, factory
    return deployment


def _slipstream_resolution(*matches, unreachable=()):
    """A ``resolve_slipstream_pool_key`` outcome with the given (deployment, pool) hits."""
    from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
    from almanak.connectors.aerodrome.pool_validation import SlipstreamKeyMatch, SlipstreamKeyResolution

    return SlipstreamKeyResolution(
        chain="base",
        token_a=USDC_BASE,
        token_b=WETH_BASE,
        tick_spacing=100,
        matches=tuple(SlipstreamKeyMatch(deployment=d, pool_address=pool) for d, pool in matches),
        unreachable=tuple(unreachable),
        reviewed=slipstream_lp_deployments("base"),
    )


def _aerodrome_ctx() -> SimpleNamespace:
    return SimpleNamespace(
        wallet_address="0x1234567890123456789012345678901234567890",
        rpc_url="http://anvil.local",
        gateway_client=None,
        token_resolver=None,
    )


def test_swap_quote_request_freezes_extra_mapping() -> None:
    extra = {"stable": False}

    request = SwapQuoteRequest(
        chain="base",
        protocol="aerodrome",
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        amount_in=100,
        extra=extra,
    )

    extra["stable"] = True
    assert request.extra["stable"] is False
    with pytest.raises(TypeError):
        request.extra["new"] = "value"  # type: ignore[index]


def test_swap_quote_result_freezes_metadata_mapping() -> None:
    metadata = {"fee_tier": 3000}

    result = SwapQuoteResult(amount_out=100, source="test", metadata=metadata)

    metadata["fee_tier"] = 500
    assert result.metadata["fee_tier"] == 3000
    with pytest.raises(TypeError):
        result.metadata["new"] = "value"  # type: ignore[index]


def test_swap_quote_registry_dispatches_declared_alias_without_duplicates() -> None:
    class AliasConnector(SwapQuoteConnector):
        protocol = ProtocolName("canonical")
        protocol_aliases = (ProtocolName("alias"),)
        kind = ProtocolKind.LP

        def quote_swap(self, ctx, request):  # noqa: ARG002
            return SwapQuoteResult(amount_out=1, source="alias")

    registry = SwapQuoteRegistry()
    connector = AliasConnector()
    registry.register(connector)

    assert registry.get("canonical") is connector
    assert registry.get("alias") is connector
    assert registry.all() == (connector,)


def test_decode_uint256_rejects_short_response() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        decode_uint256(b"\x01")


def test_decode_uint256_reads_first_word() -> None:
    assert decode_uint256(abi_encode(["uint256", "bool"], [123, True])) == 123


def test_uniswap_v4_provider_uses_shared_eth_call(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.uniswap_v4 import sdk as v4_sdk
    from almanak.connectors.uniswap_v4.swap_quote_provider import UniswapV4SwapQuoteConnector

    calls: list[dict[str, object]] = []

    def fake_eth_call(**kwargs):
        calls.append(kwargs)
        return abi_encode(["uint256", "uint256"], [49_000_000_000_000_000, 123_456])

    monkeypatch.setattr(v4_sdk, "eth_call", fake_eth_call)

    provider = UniswapV4SwapQuoteConnector()
    result = provider.quote_swap(
        SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None),
        SwapQuoteRequest(
            chain="base",
            protocol="uniswap_v4",
            token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            token_out="0x4200000000000000000000000000000000000006",
            amount_in=100_000_000,
            token_in_decimals=6,
            token_out_decimals=18,
            fee_tier=3000,
        ),
    )

    assert isinstance(provider, SwapQuoteConnector)
    assert isinstance(result, SwapQuoteResult)
    assert result.amount_out == 49_000_000_000_000_000
    assert result.gas_estimate == 123_456
    assert result.source == "uniswap_v4_quoter"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_UNSUPPORTED
    assert calls == [
        {
            "chain": "base",
            "to": "0x0d5e0F971ED27FBfF6c2837bf31316121532048D",
            "data": calls[0]["data"],
            "rpc_url": "http://anvil.local",
            "gateway_client": None,
            "timeout": v4_sdk.V4_QUOTER_DIRECT_RPC_TIMEOUT_SECONDS,
        }
    ]
    assert str(calls[0]["data"]).startswith(v4_sdk.QUOTE_EXACT_INPUT_SINGLE_SELECTOR)


def test_uniswap_v4_provider_preserves_explicit_zero_numeric_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.uniswap_v4 import sdk as v4_sdk
    from almanak.connectors.uniswap_v4.swap_quote_provider import UniswapV4SwapQuoteConnector

    calls: list[dict[str, object]] = []

    class FakeSDK:
        def __init__(self, **kwargs):
            calls.append({"init": kwargs})

        def get_quote(self, **kwargs):
            calls.append({"quote": kwargs})
            return SimpleNamespace(amount_out=1, gas_estimate=2, fee_tier=kwargs["fee_tier"])

    monkeypatch.setattr(v4_sdk, "UniswapV4SDK", FakeSDK)

    result = UniswapV4SwapQuoteConnector().quote_swap(
        SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None),
        SwapQuoteRequest(
            chain="base",
            protocol="uniswap_v4",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in=100,
            token_in_decimals=0,
            token_out_decimals=0,
            fee_tier=0,
        ),
    )

    assert result.metadata["fee_tier"] == 0
    assert calls[1]["quote"]["fee_tier"] == 0
    assert calls[1]["quote"]["token_in_decimals"] == 0
    assert calls[1]["quote"]["token_out_decimals"] == 0


def test_uniswap_v3_provider_uses_default_swap_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.uniswap_v3 import swap_quote_provider
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    created: list[dict[str, object]] = []

    class FakeAdapter:
        last_fee_selection = {"selected_fee_tier": 500}

        def __init__(self, *args, **kwargs):
            created.append({"args": args, "kwargs": kwargs})

        def select_fee_tier(self, token_in: str, token_out: str, amount_in: int) -> int:
            assert token_in == "0x1111111111111111111111111111111111111111"
            assert token_out == "0x2222222222222222222222222222222222222222"
            assert amount_in == 100_000_000
            return 500

        def get_quoted_amount_out(self) -> int:
            return 48_000_000_000_000_000

    monkeypatch.setattr(swap_quote_provider, "DefaultSwapAdapter", FakeAdapter)

    result = UniswapV3SwapQuoteConnector().quote_swap(
        SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None, rpc_timeout=7.0),
        SwapQuoteRequest(
            chain="base",
            protocol="uniswap_v3",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in=100_000_000,
            fee_tier=500,
        ),
    )

    assert result.amount_out == 48_000_000_000_000_000
    assert result.source == "uniswap_v3_quoter"
    assert result.metadata["fee_tier"] == 500
    assert result.metadata["pool_key"] == 500
    assert result.metadata["pool_key_kind"] == "fee_tier"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_V3_SPOT
    assert result.metadata["fee_selection"] == {"selected_fee_tier": 500}
    assert created[0]["kwargs"]["rpc_url"] == "http://anvil.local"
    assert created[0]["kwargs"]["pool_selection_mode"] == "fixed"


def test_uniswap_v3_provider_rejects_wrong_protocol() -> None:
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    with pytest.raises(SwapQuoteUnavailable, match="cannot quote curve"):
        UniswapV3SwapQuoteConnector().quote_swap(
            SimpleNamespace(),
            SwapQuoteRequest(
                chain="base",
                protocol="curve",
                token_in="0x1111111111111111111111111111111111111111",
                token_out="0x2222222222222222222222222222222222222222",
                amount_in=100,
            ),
        )


def test_uniswap_v3_provider_uses_auto_pool_selection_without_fee(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.uniswap_v3 import swap_quote_provider
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    created: list[dict[str, object]] = []

    class FakeAdapter:
        last_fee_selection = {"selected_fee_tier": 3000}

        def __init__(self, *args, **kwargs):
            created.append(kwargs)

        def select_fee_tier(self, token_in: str, token_out: str, amount_in: int) -> int:
            return 3000

        def get_quoted_amount_out(self) -> int:
            return 1

    monkeypatch.setattr(swap_quote_provider, "DefaultSwapAdapter", FakeAdapter)

    result = UniswapV3SwapQuoteConnector().quote_swap(
        SimpleNamespace(
            rpc_url="http://anvil.local", gateway_client=None, swap_pool_selection_mode="highest-liquidity"
        ),
        SwapQuoteRequest(
            chain="base",
            protocol="uniswap_v3",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in=100,
        ),
    )

    assert result.metadata["fee_tier"] == 3000
    assert created[0]["pool_selection_mode"] == "highest-liquidity"
    assert created[0]["fixed_fee_tier"] is None


def test_uniswap_v3_provider_reads_fee_from_exact_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors._strategy_base import rpc, v3_pool_validation
    from almanak.connectors.uniswap_v3 import swap_quote_provider
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    pool = "0x3333333333333333333333333333333333333333"
    rpc_calls: list[dict[str, object]] = []
    created: list[dict[str, object]] = []

    def fake_eth_call_uint256(**kwargs):
        rpc_calls.append(kwargs)
        return 500

    class FakeAdapter:
        last_fee_selection = {"selected_fee_tier": 500}

        def __init__(self, *args, **kwargs):
            created.append({"args": args, "kwargs": kwargs})

        def select_fee_tier(self, token_in: str, token_out: str, amount_in: int) -> int:  # noqa: ARG002
            return 500

        def get_quoted_amount_out(self) -> int:
            return 48_000_000_000_000_000

    monkeypatch.setattr(rpc, "eth_call_uint256", fake_eth_call_uint256)
    monkeypatch.setattr(
        v3_pool_validation,
        "validate_v3_pool",
        lambda **kwargs: SimpleNamespace(exists=True, pool_address=pool),
    )
    monkeypatch.setattr(swap_quote_provider, "DefaultSwapAdapter", FakeAdapter)

    result = UniswapV3SwapQuoteConnector().quote_swap(
        SimpleNamespace(rpc_url=None, gateway_client=object(), rpc_timeout=7.0),
        SwapQuoteRequest(
            chain="base",
            protocol="sushiswap_v3",
            pool_address=pool,
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in=100_000_000,
        ),
    )

    assert rpc_calls[0]["to"] == pool
    assert created[0]["args"][1] == "sushiswap_v3"
    assert created[0]["kwargs"]["pool_selection_mode"] == "fixed"
    assert created[0]["kwargs"]["fixed_fee_tier"] == 500
    assert result.source == "sushiswap_v3_quoter"
    assert result.metadata["pool_address"] == pool


def test_uniswap_v3_provider_rejects_pool_route_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors._strategy_base import v3_pool_validation
    from almanak.connectors.uniswap_v3 import swap_quote_provider
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    monkeypatch.setattr(
        v3_pool_validation,
        "validate_v3_pool",
        lambda **kwargs: SimpleNamespace(
            exists=True,
            pool_address="0x4444444444444444444444444444444444444444",
        ),
    )
    monkeypatch.setattr(
        swap_quote_provider,
        "DefaultSwapAdapter",
        lambda *args, **kwargs: pytest.fail("adapter must not quote a mismatched pool"),
    )

    with pytest.raises(SwapQuoteUnavailable, match="does not match .* route for fee tier 500"):
        UniswapV3SwapQuoteConnector().quote_swap(
            SimpleNamespace(rpc_url=None, gateway_client=object()),
            SwapQuoteRequest(
                chain="base",
                protocol="uniswap_v3",
                pool_address="0x3333333333333333333333333333333333333333",
                token_in="0x1111111111111111111111111111111111111111",
                token_out="0x2222222222222222222222222222222222222222",
                amount_in=100,
                fee_tier=500,
            ),
        )


def test_uniswap_v3_provider_wraps_pool_fee_read_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors._strategy_base import rpc
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    def fail_fee_read(**kwargs):
        raise RuntimeError("rpc down")

    monkeypatch.setattr(rpc, "eth_call_uint256", fail_fee_read)

    with pytest.raises(SwapQuoteUnavailable, match="rpc down"):
        UniswapV3SwapQuoteConnector().quote_swap(
            SimpleNamespace(rpc_url=None, gateway_client=object()),
            SwapQuoteRequest(
                chain="base",
                protocol="uniswap_v3",
                pool_address="0x3333333333333333333333333333333333333333",
                token_in="0x1111111111111111111111111111111111111111",
                token_out="0x2222222222222222222222222222222222222222",
                amount_in=100,
            ),
        )


def test_uniswap_v3_provider_wraps_fee_selection_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.uniswap_v3 import swap_quote_provider
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    class FakeAdapter:
        last_fee_selection = {}

        def __init__(self, *args, **kwargs):
            pass

        def select_fee_tier(self, token_in: str, token_out: str, amount_in: int) -> int:
            raise RuntimeError("quoter down")

    monkeypatch.setattr(swap_quote_provider, "DefaultSwapAdapter", FakeAdapter)

    with pytest.raises(SwapQuoteUnavailable, match="quoter down"):
        UniswapV3SwapQuoteConnector().quote_swap(
            SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None),
            SwapQuoteRequest(
                chain="base",
                protocol="uniswap_v3",
                token_in="0x1111111111111111111111111111111111111111",
                token_out="0x2222222222222222222222222222222222222222",
                amount_in=100,
            ),
        )


def test_uniswap_v3_provider_requires_quoted_amount(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.uniswap_v3 import swap_quote_provider
    from almanak.connectors.uniswap_v3.swap_quote_provider import UniswapV3SwapQuoteConnector

    class FakeAdapter:
        last_fee_selection = {"selected_fee_tier": 3000}

        def __init__(self, *args, **kwargs):
            pass

        def select_fee_tier(self, token_in: str, token_out: str, amount_in: int) -> int:
            return 3000

        def get_quoted_amount_out(self) -> None:
            return None

    monkeypatch.setattr(swap_quote_provider, "DefaultSwapAdapter", FakeAdapter)

    with pytest.raises(SwapQuoteUnavailable, match="returned no amount"):
        UniswapV3SwapQuoteConnector().quote_swap(
            SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None),
            SwapQuoteRequest(
                chain="base",
                protocol="uniswap_v3",
                token_in="0x1111111111111111111111111111111111111111",
                token_out="0x2222222222222222222222222222222222222222",
                amount_in=100,
            ),
        )


def test_curve_provider_uses_shared_eth_call_uint256(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.curve import adapter as curve_adapter
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    calls: list[dict[str, object]] = []

    def fake_eth_call_uint256(**kwargs):
        calls.append(kwargs)
        return 99_500_000

    monkeypatch.setattr(curve_adapter, "eth_call_uint256", fake_eth_call_uint256)

    provider = CurveSwapQuoteConnector()
    result = provider.quote_swap(
        SimpleNamespace(
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://anvil.local",
            gateway_client=None,
            token_resolver=None,
        ),
        SwapQuoteRequest(
            chain="base",
            protocol="curve",
            pool_address="0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f",
            token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            token_out="0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
            token_in_symbol="USDC",
            token_out_symbol="USDbC",
            amount_in=100_000_000,
        ),
    )

    assert result.amount_out == 99_500_000
    assert result.source == "curve_pool_get_dy"
    assert result.metadata["pool_address"] == "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"
    assert result.metadata["pool_type"] == "stableswap"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_STABLE_PARITY
    assert len(calls) == 1
    assert calls[0]["chain"] == "base"
    assert calls[0]["to"] == "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f"
    assert calls[0]["rpc_url"] == "http://anvil.local"
    assert str(calls[0]["data"]).startswith(curve_adapter.GET_DY_SELECTOR)


def test_curve_provider_quotes_with_resolved_addresses(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.curve import adapter as curve_adapter
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    calls: list[dict[str, object]] = []

    def fake_quote_swap_output(self, **kwargs):
        calls.append(kwargs)
        return 99_500_000

    monkeypatch.setattr(curve_adapter.CurveAdapter, "quote_swap_output", fake_quote_swap_output)

    result = CurveSwapQuoteConnector().quote_swap(
        SimpleNamespace(
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://anvil.local",
            gateway_client=None,
            token_resolver=None,
        ),
        SwapQuoteRequest(
            chain="base",
            protocol="curve",
            pool_address="0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f",
            token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            token_out="0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
            token_in_symbol="USDC",
            token_out_symbol="USDbC",
            amount_in=100_000_000,
        ),
    )

    assert result.amount_out == 99_500_000
    assert calls == [
        {
            "pool_address": "0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f",
            "token_in": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "token_out": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
            "amount_in_wei": 100_000_000,
        }
    ]


def test_curve_crypto_pool_does_not_claim_stable_parity(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.curve import adapter as curve_adapter
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    monkeypatch.setattr(curve_adapter.CurveAdapter, "quote_swap_output", lambda self, **kwargs: 999)

    result = CurveSwapQuoteConnector().quote_swap(
        SimpleNamespace(
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://anvil.local",
            gateway_client=None,
            token_resolver=None,
        ),
        SwapQuoteRequest(
            chain="base",
            protocol="curve",
            pool_address="0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59",
            token_in="0x4200000000000000000000000000000000000006",
            token_out="0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
            amount_in=1_000,
        ),
    )

    assert result.metadata["pool_type"] == "cryptoswap"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_UNSUPPORTED


def test_curve_unmarked_stable_pool_does_not_claim_stable_parity(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.curve import adapter as curve_adapter
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    monkeypatch.setattr(curve_adapter.CurveAdapter, "quote_swap_output", lambda self, **kwargs: 999)

    result = CurveSwapQuoteConnector().quote_swap(
        SimpleNamespace(
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://anvil.local",
            gateway_client=None,
            token_resolver=None,
        ),
        SwapQuoteRequest(
            chain="ethereum",
            protocol="curve",
            pool_address="0xDC24316b9AE028F1497c275EB9192a3Ea0f67022",
            token_in="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
            token_out="0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
            amount_in=1_000,
        ),
    )

    assert result.metadata["pool_type"] == "stableswap"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_UNSUPPORTED


def test_curated_stable_parity_pool_set_matches_reviewed_pools() -> None:
    """Pin the exact curated set: only reviewed pegged StableSwap pools claim parity."""
    from almanak.connectors.curve.adapter import _STABLE_PARITY_POOLS

    assert {pool.lower() for pools in _STABLE_PARITY_POOLS.values() for pool in pools} == {
        "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7",  # ethereum 3pool
        "0xdcef968d416a41cdac0ed8702fac8128a64241a2",  # ethereum FRAX/USDC
        "0x7f90122bf0700f9e7e1f688fe926940e8839f353",  # arbitrum 2pool
        "0xf6c5f01c7f3148891ad0e19df78743d31e390d1f",  # base 4pool
        "0x1337bedc9d22ecbe766df105c9623922a27963ec",  # optimism 3pool
        "0x03771e24b7c9172d163bf447490b142a15be3485",  # optimism crvUSD/USDC
        "0x5bc930b8f81f4ceee3e3527159c3bdf453bcaae9",  # polygon frxUSD/USDT
    }


def test_curve_provider_wraps_adapter_initialization_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.curve import adapter as curve_adapter
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    class BrokenCurveAdapter:
        def __init__(self, *args, **kwargs) -> None:
            raise ValueError("bad curve config")

    monkeypatch.setattr(curve_adapter, "CurveAdapter", BrokenCurveAdapter)

    with pytest.raises(SwapQuoteUnavailable, match="bad curve config"):
        CurveSwapQuoteConnector().quote_swap(
            SimpleNamespace(
                wallet_address="0x1234567890123456789012345678901234567890",
                rpc_url="http://anvil.local",
                gateway_client=None,
                token_resolver=None,
            ),
            SwapQuoteRequest(
                chain="base",
                protocol="curve",
                pool_address="0xf6C5F01C7F3148891ad0e19DF78743D31E390D1f",
                token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                token_out="0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
                amount_in=100_000_000,
            ),
        )


def test_aerodrome_provider_uses_adapter_quote(monkeypatch: pytest.MonkeyPatch) -> None:
    """A symbolic CL key is asked of every reviewed generation; the unique owner's quoter answers."""
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome import pool_validation as aerodrome_pool_validation
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    current = _slipstream_generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
    calls: list[dict[str, object]] = []
    resolver_calls: list[tuple] = []

    def fake_quote_swap_output(self, **kwargs):
        calls.append(kwargs)
        return 47_000_000_000_000_000

    def fake_resolve(chain, token_a, token_b, tick_spacing, rpc_url, gateway_client=None):
        resolver_calls.append((chain, token_a, token_b, tick_spacing, rpc_url, gateway_client))
        return _slipstream_resolution((current, "0x" + "ab" * 20))

    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", fake_quote_swap_output)
    monkeypatch.setattr(aerodrome_pool_validation, "resolve_slipstream_pool_key", fake_resolve)

    result = AerodromeSwapQuoteConnector().quote_swap(
        _aerodrome_ctx(),
        SwapQuoteRequest(
            chain="base",
            protocol="aerodrome",
            token_in=USDC_BASE,
            token_out=WETH_BASE,
            amount_in=100_000_000,
            extra={"tick_spacing": 100, "use_cl": True},
        ),
    )

    assert result.amount_out == 47_000_000_000_000_000
    assert result.source == "aerodrome_cl_quoter"
    assert resolver_calls == [("base", USDC_BASE, WETH_BASE, 100, "http://anvil.local", None)]
    assert result.metadata["pool_key"] == 100
    assert result.metadata["pool_key_kind"] == "tick_spacing"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_V3_SPOT
    assert calls == [
        {
            "token_in": USDC_BASE,
            "token_out": WETH_BASE,
            "amount_in_wei": 100_000_000,
            "stable": False,
            "tick_spacing": 100,
            "use_cl": True,
            "require_onchain": True,
            "deployment": current,
        }
    ]
    assert result.metadata["slipstream_deployment"] == "current"
    assert result.metadata["quoter"] == current.quoter


def test_aerodrome_provider_pinned_pool_selects_the_generation_from_its_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact pool address decides the generation by its own factory(); no symbolic scan runs."""
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome import pool_validation as aerodrome_pool_validation
    from almanak.connectors.aerodrome.pool_validation import SlipstreamPoolBinding
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    legacy = _slipstream_generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")
    pool = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"
    calls: list[dict[str, object]] = []
    reads: list[tuple] = []

    def fake_quote_swap_output(self, **kwargs):
        calls.append(kwargs)
        return 1

    def fake_read(pool_address, rpc_url, *, chain=None, gateway_client=None):
        reads.append((pool_address, rpc_url, chain, gateway_client))
        return SlipstreamPoolBinding(
            token0=WETH_BASE.lower(), token1=USDC_BASE.lower(), tick_spacing=50, factory=legacy.factory.lower()
        )

    def forbidden_resolve(*args, **kwargs):
        raise AssertionError("a pinned pool must not trigger the symbolic scan")

    validations: list[dict[str, object]] = []

    def fake_validate(chain, token_a, token_b, tick_spacing, rpc_url, gateway_client=None, deployment=None):
        validations.append({"tokens": (token_a, token_b), "tick_spacing": tick_spacing, "deployment": deployment})
        return PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool, factory=deployment.factory
        )

    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", fake_quote_swap_output)
    monkeypatch.setattr(aerodrome_pool_validation, "read_slipstream_cl_pool_binding", fake_read)
    monkeypatch.setattr(aerodrome_pool_validation, "validate_aerodrome_cl_pool", fake_validate)
    monkeypatch.setattr(aerodrome_pool_validation, "resolve_slipstream_pool_key", forbidden_resolve)

    result = AerodromeSwapQuoteConnector().quote_swap(
        _aerodrome_ctx(),
        SwapQuoteRequest(
            chain="base",
            protocol="aerodrome_slipstream",
            pool_address=pool,
            token_in=USDC_BASE,
            token_out=WETH_BASE,
            amount_in=100_000_000,
        ),
    )

    assert reads == [(pool, "http://anvil.local", "base", None)]
    # The pool's tuple is round-tripped through the generation it claims, only.
    assert validations == [{"tokens": (WETH_BASE.lower(), USDC_BASE.lower()), "tick_spacing": 50, "deployment": legacy}]
    assert calls[0]["deployment"] == legacy
    # The exact pool supplies the discriminator when the request omits it.
    assert calls[0]["tick_spacing"] == 50
    assert result.metadata["tick_spacing"] == 50
    assert result.metadata["pool_key"] == 50
    assert result.metadata["pool_key_kind"] == "tick_spacing"
    assert result.metadata["slippage_reference"] == SLIPPAGE_REFERENCE_V3_SPOT
    assert result.metadata["pool_address"] == pool
    assert result.metadata["slipstream_deployment"] == "legacy"
    assert result.metadata["quoter"] == legacy.quoter


def test_aerodrome_provider_refuses_an_ambiguous_symbolic_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome import pool_validation as aerodrome_pool_validation
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    current = _slipstream_generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
    legacy = _slipstream_generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")
    quoted: list[dict[str, object]] = []

    monkeypatch.setattr(
        aerodrome_adapter.AerodromeAdapter, "quote_swap_output", lambda self, **kw: quoted.append(kw) or 1
    )
    monkeypatch.setattr(
        aerodrome_pool_validation,
        "resolve_slipstream_pool_key",
        lambda *a, **k: _slipstream_resolution((current, "0x" + "ab" * 20), (legacy, "0x" + "cd" * 20)),
    )

    with pytest.raises(SwapQuoteUnavailable, match="Ambiguous Aerodrome Slipstream pool key"):
        AerodromeSwapQuoteConnector().quote_swap(
            _aerodrome_ctx(),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome",
                token_in=USDC_BASE,
                token_out=WETH_BASE,
                amount_in=100_000_000,
                extra={"tick_spacing": 100, "use_cl": True},
            ),
        )
    assert quoted == []


def test_aerodrome_provider_refuses_an_unreachable_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    """A lone hit cannot prove uniqueness while another reviewed factory is unreadable."""
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome import pool_validation as aerodrome_pool_validation
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    current = _slipstream_generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
    legacy = _slipstream_generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")

    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", lambda self, **kw: 1)
    monkeypatch.setattr(
        aerodrome_pool_validation,
        "resolve_slipstream_pool_key",
        lambda *a, **k: _slipstream_resolution((current, "0x" + "ab" * 20), unreachable=(legacy,)),
    )

    with pytest.raises(SwapQuoteUnavailable, match="legacy"):
        AerodromeSwapQuoteConnector().quote_swap(
            _aerodrome_ctx(),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome",
                token_in=USDC_BASE,
                token_out=WETH_BASE,
                amount_in=100_000_000,
                extra={"tick_spacing": 100, "use_cl": True},
            ),
        )


def test_aerodrome_symbolic_slipstream_requires_tick_spacing() -> None:
    """A symbolic Slipstream route without a discriminator fails closed before any RPC."""
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    with pytest.raises(SwapQuoteUnavailable, match="require tick spacing"):
        AerodromeSwapQuoteConnector().quote_swap(
            _aerodrome_ctx(),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome_slipstream",
                token_in=USDC_BASE,
                token_out=WETH_BASE,
                amount_in=100_000_000,
            ),
        )


def test_aerodrome_rejects_non_positive_tick_spacing() -> None:
    """A non-positive tick spacing is rejected before pool resolution."""
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    with pytest.raises(SwapQuoteUnavailable, match="Invalid tick spacing 0"):
        AerodromeSwapQuoteConnector().quote_swap(
            _aerodrome_ctx(),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome_slipstream",
                token_in=USDC_BASE,
                token_out=WETH_BASE,
                amount_in=100_000_000,
                extra={"tick_spacing": 0},
            ),
        )


def test_aerodrome_provider_refuses_a_pinned_pool_on_an_unreviewed_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome import pool_validation as aerodrome_pool_validation
    from almanak.connectors.aerodrome.pool_validation import SlipstreamPoolBinding
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    unreviewed = "0x" + "ab" * 20
    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", lambda self, **kw: 1)
    monkeypatch.setattr(
        aerodrome_pool_validation,
        "read_slipstream_cl_pool_binding",
        lambda *a, **k: SlipstreamPoolBinding(
            token0=WETH_BASE.lower(), token1=USDC_BASE.lower(), tick_spacing=100, factory=unreviewed
        ),
    )

    with pytest.raises(SwapQuoteUnavailable, match=f"unreviewed Slipstream factory {unreviewed}"):
        AerodromeSwapQuoteConnector().quote_swap(
            _aerodrome_ctx(),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome",
                pool_address="0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59",
                token_in=USDC_BASE,
                token_out=WETH_BASE,
                amount_in=100_000_000,
            ),
        )


@pytest.mark.parametrize(
    ("binding_tokens", "factory_answer", "needle"),
    [
        ((WETH_BASE.lower(), "0x" + "cc" * 20), "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59", "holds"),
        ((WETH_BASE.lower(), USDC_BASE.lower()), "0x" + "dd" * 20, "is not the legacy Slipstream factory's pool"),
    ],
)
def test_aerodrome_provider_refuses_a_pinned_pool_that_does_not_authenticate(
    monkeypatch: pytest.MonkeyPatch, binding_tokens, factory_answer, needle
) -> None:
    """A pinned pool must hold the requested pair AND be the pool its claimed factory returns for that tuple."""
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome import pool_validation as aerodrome_pool_validation
    from almanak.connectors.aerodrome.pool_validation import SlipstreamPoolBinding
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    legacy = _slipstream_generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")
    pool = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"
    quotes: list[dict[str, object]] = []
    monkeypatch.setattr(
        aerodrome_adapter.AerodromeAdapter, "quote_swap_output", lambda self, **kw: quotes.append(kw) or 1
    )
    monkeypatch.setattr(
        aerodrome_pool_validation,
        "read_slipstream_cl_pool_binding",
        lambda *a, **k: SlipstreamPoolBinding(
            token0=binding_tokens[0], token1=binding_tokens[1], tick_spacing=100, factory=legacy.factory.lower()
        ),
    )
    monkeypatch.setattr(
        aerodrome_pool_validation,
        "validate_aerodrome_cl_pool",
        lambda *a, **k: PoolValidationResult(
            exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=factory_answer, factory=legacy.factory
        ),
    )

    with pytest.raises(SwapQuoteUnavailable, match=needle):
        AerodromeSwapQuoteConnector().quote_swap(
            _aerodrome_ctx(),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome",
                pool_address=pool,
                token_in=USDC_BASE,
                token_out=WETH_BASE,
                amount_in=100_000_000,
            ),
        )
    assert quotes == [], "no quote may be produced for an unauthenticated pin"


def test_aerodrome_provider_honors_explicit_classic_route(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    calls: list[dict[str, object]] = []

    def fake_quote_swap_output(self, **kwargs):
        calls.append(kwargs)
        return 47_000_000_000_000_000

    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", fake_quote_swap_output)

    result = AerodromeSwapQuoteConnector().quote_swap(
        SimpleNamespace(
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://anvil.local",
            gateway_client=None,
            token_resolver=None,
        ),
        SwapQuoteRequest(
            chain="base",
            protocol="aerodrome",
            token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            token_out="0x4200000000000000000000000000000000000006",
            amount_in=100_000_000,
            extra={"stable": True, "tick_spacing": 200, "use_cl": False},
        ),
    )

    assert result.amount_out == 47_000_000_000_000_000
    assert result.source == "aerodrome_router_getAmountsOut"
    assert result.metadata == {
        "stable": True,
        "use_cl": False,
        "tick_spacing": 200,
        "slippage_reference": SLIPPAGE_REFERENCE_UNSUPPORTED,
    }
    assert calls[0]["stable"] is True
    assert calls[0]["tick_spacing"] == 200
    assert calls[0]["use_cl"] is False
    assert calls[0]["deployment"] is None


def test_aerodrome_provider_wraps_quote_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    def fake_quote_swap_output(self, **kwargs):
        raise ValueError("router quote unavailable")

    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", fake_quote_swap_output)

    with pytest.raises(SwapQuoteUnavailable, match="router quote unavailable"):
        AerodromeSwapQuoteConnector().quote_swap(
            SimpleNamespace(
                wallet_address="0x1234567890123456789012345678901234567890",
                rpc_url="http://anvil.local",
                gateway_client=None,
                token_resolver=None,
            ),
            SwapQuoteRequest(
                chain="optimism",
                protocol="aerodrome",
                token_in="0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
                token_out="0x4200000000000000000000000000000000000006",
                amount_in=1_000_000,
            ),
        )


def test_aerodrome_provider_wraps_invalid_tick_spacing() -> None:
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    with pytest.raises(SwapQuoteUnavailable, match="Aerodrome quote unavailable"):
        AerodromeSwapQuoteConnector().quote_swap(
            SimpleNamespace(
                wallet_address="0x1234567890123456789012345678901234567890",
                rpc_url="http://anvil.local",
                gateway_client=None,
                token_resolver=None,
            ),
            SwapQuoteRequest(
                chain="base",
                protocol="aerodrome",
                token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                token_out="0x4200000000000000000000000000000000000006",
                amount_in=100_000_000,
                extra={"tick_spacing": "bad"},
            ),
        )


def test_aerodrome_provider_defaults_to_classic_without_a_reviewed_cl_router(monkeypatch: pytest.MonkeyPatch) -> None:
    """Velodrome on Optimism has no reviewed Slipstream generation, so ``use_cl`` defaults off."""
    from almanak.connectors.aerodrome import adapter as aerodrome_adapter
    from almanak.connectors.aerodrome.swap_quote_provider import AerodromeSwapQuoteConnector

    calls: list[dict[str, object]] = []

    def fake_quote_swap_output(self, **kwargs):
        calls.append(kwargs)
        return 1_000_000

    monkeypatch.setattr(aerodrome_adapter.AerodromeAdapter, "quote_swap_output", fake_quote_swap_output)

    result = AerodromeSwapQuoteConnector().quote_swap(
        SimpleNamespace(
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://anvil.local",
            gateway_client=None,
            token_resolver=None,
        ),
        SwapQuoteRequest(
            chain="optimism",
            protocol="aerodrome",
            token_in="0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            token_out="0x4200000000000000000000000000000000000006",
            amount_in=1_000_000,
        ),
    )

    assert result.source == "aerodrome_router_getAmountsOut"
    assert result.metadata["use_cl"] is False
    assert calls == [
        {
            "token_in": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            "token_out": "0x4200000000000000000000000000000000000006",
            "amount_in_wei": 1_000_000,
            "stable": False,
            "tick_spacing": 100,
            "use_cl": False,
            "require_onchain": True,
            "deployment": None,
        }
    ]


def test_curve_provider_resolves_pool_when_address_omitted() -> None:
    """ALM-2896: pool_address is now optional — the provider resolves it from
    the connector's own pool registry. An unknown pair (no Curve pool) raises
    SwapQuoteUnavailable with a 'No Curve pool' message rather than demanding a
    caller-supplied pool_address.
    """
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    provider = CurveSwapQuoteConnector()

    with pytest.raises(SwapQuoteUnavailable, match="No Curve pool"):
        provider.quote_swap(
            SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None),
            SwapQuoteRequest(
                chain="base",
                protocol="curve",
                token_in="USDC",
                token_out="0x000000000000000000000000000000000000dEaD",
                amount_in=100_000_000,
            ),
        )


def test_agni_pool_reader_uses_mantle_factory() -> None:
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE
    from almanak.connectors.uniswap_v3.connector import CONNECTOR
    from almanak.connectors.uniswap_v3.pool_reader import (
        AGNI_POOL_DATA_SPEC,
        AGNI_POOL_READER_SPEC,
        POOL_DATA_SPECS,
        POOL_READER_SPEC,
    )
    from almanak.framework.data.pools.reader import PoolReaderRegistry

    assert AGNI_POOL_DATA_SPEC in POOL_DATA_SPECS
    assert AGNI_POOL_DATA_SPEC.price_reader is AGNI_POOL_READER_SPEC
    assert AGNI_POOL_READER_SPEC.protocol == "agni_finance"
    assert AGNI_POOL_READER_SPEC.factory_addresses == {
        chain: addresses["factory"] for chain, addresses in AGNI_FINANCE.items() if "factory" in addresses
    }
    assert AGNI_POOL_READER_SPEC.factory_addresses["mantle"] == AGNI_FINANCE["mantle"]["factory"]
    assert 2500 in AGNI_POOL_READER_SPEC.candidate_pool_keys
    assert AGNI_POOL_READER_SPEC.reader_kind == "v3_slot0"
    assert set(CONNECTOR.supported_chains_for_protocol("uniswap_v3")) <= set(POOL_READER_SPEC.factory_addresses)

    registry = PoolReaderRegistry(rpc_call=lambda *args: b"")
    assert "agni_finance" in registry.supported_protocols
    assert "agni_finance" in registry.protocols_for_chain("mantle")
