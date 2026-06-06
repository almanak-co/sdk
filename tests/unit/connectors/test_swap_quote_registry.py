"""Tests for connector-owned swap quote providers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from eth_abi import encode as abi_encode

from almanak.connectors._strategy_base.rpc import decode_uint256
from almanak.connectors._strategy_base.swap_quote_registry import (
    SwapQuoteConnector,
    SwapQuoteRequest,
    SwapQuoteResult,
    SwapQuoteUnavailable,
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
    assert calls == [
        {
            "chain": "base",
            "to": "0x0d5e0F971ED27FBfF6c2837bf31316121532048D",
            "data": calls[0]["data"],
            "rpc_url": "http://anvil.local",
            "gateway_client": None,
            "timeout": 10.0,
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
            extra={"tick_spacing": 100, "use_cl": True},
        ),
    )

    assert result.amount_out == 47_000_000_000_000_000
    assert result.source == "aerodrome_cl_quoter"
    assert calls == [
        {
            "token_in": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "token_out": "0x4200000000000000000000000000000000000006",
            "amount_in_wei": 100_000_000,
            "stable": False,
            "tick_spacing": 100,
            "use_cl": True,
            "require_onchain": True,
        }
    ]


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
    assert result.metadata == {"stable": True, "use_cl": False, "tick_spacing": 200}
    assert calls[0]["stable"] is True
    assert calls[0]["tick_spacing"] == 200
    assert calls[0]["use_cl"] is False


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


def test_aerodrome_provider_defaults_to_classic_without_cl_quoter(monkeypatch: pytest.MonkeyPatch) -> None:
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
        }
    ]


def test_curve_provider_requires_pool_address() -> None:
    from almanak.connectors.curve.swap_quote_provider import CurveSwapQuoteConnector

    provider = CurveSwapQuoteConnector()

    with pytest.raises(SwapQuoteUnavailable, match="pool_address"):
        provider.quote_swap(
            SimpleNamespace(rpc_url="http://anvil.local", gateway_client=None),
            SwapQuoteRequest(
                chain="base",
                protocol="curve",
                token_in="USDC",
                token_out="USDbC",
                amount_in=100_000_000,
            ),
        )
