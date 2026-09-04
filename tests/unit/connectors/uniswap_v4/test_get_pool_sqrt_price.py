from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY, PoolKey, UniswapV4SDK
from almanak.framework.observability.metrics import OnchainReadFallbackReason

CHAIN = "base"
LOW_CURRENCY = "0x4200000000000000000000000000000000000006"
HIGH_CURRENCY = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"


def _slot0_response(
    sqrt_price_x96: int,
    *,
    tick: int = 0,
    protocol_fee: int = 0,
    lp_fee: int = 3000,
) -> str:
    values = (sqrt_price_x96, tick % (1 << 256), protocol_fee, lp_fee)
    return "0x" + "".join(format(value, "064x") for value in values)


def _expected_calldata(pool_key: PoolKey) -> str:
    encoded = encode(
        ["address", "address", "uint24", "int24", "address"],
        [
            pool_key.currency0,
            pool_key.currency1,
            pool_key.fee,
            pool_key.tick_spacing,
            pool_key.hooks,
        ],
    )
    return "0xc815641c" + keccak(encoded).hex()


@pytest.fixture
def fallback_recorder(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    recorder = MagicMock()
    monkeypatch.setattr(
        "almanak.framework.observability.metrics.record_onchain_read_fallback",
        recorder,
    )
    return recorder


def _assert_fallback(recorder: MagicMock, reason: OnchainReadFallbackReason) -> None:
    recorder.assert_called_once_with(
        protocol="uniswap_v4",
        chain=CHAIN,
        call="getSlot0",
        reason=reason,
    )


@pytest.mark.parametrize(
    ("first_currency", "second_currency", "expected_currency0", "expected_currency1"),
    [
        (HIGH_CURRENCY, LOW_CURRENCY, LOW_CURRENCY.lower(), HIGH_CURRENCY.lower()),
        (HIGH_CURRENCY, NATIVE_CURRENCY, NATIVE_CURRENCY, HIGH_CURRENCY.lower()),
    ],
)
def test_queries_state_view_with_canonical_currency_order(
    monkeypatch: pytest.MonkeyPatch,
    fallback_recorder: MagicMock,
    first_currency: str,
    second_currency: str,
    expected_currency0: str,
    expected_currency1: str,
) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://default.local")
    pool_key = PoolKey(first_currency, second_currency, fee=500, tick_spacing=10)
    rpc_call = MagicMock(return_value=_slot0_response(2**96, tick=-17, protocol_fee=4, lp_fee=500))
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", rpc_call)

    result = sdk.get_pool_sqrt_price(pool_key, rpc_url="http://override.local")

    assert result == 2**96
    assert (pool_key.currency0, pool_key.currency1) == (expected_currency0, expected_currency1)
    rpc_call.assert_called_once_with(
        chain=CHAIN,
        to=sdk.addresses["state_view"],
        data=_expected_calldata(pool_key),
        rpc_url="http://override.local",
        gateway_client=None,
        gateway_raise_on_error=True,
        timeout=10.0,
    )
    fallback_recorder.assert_not_called()


def test_connected_gateway_is_the_only_transport(fallback_recorder: MagicMock) -> None:
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.eth_call.return_value = _slot0_response(2**96)
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://direct-fallback.local", gateway_client=gateway)
    pool_key = PoolKey(HIGH_CURRENCY, LOW_CURRENCY, fee=3000, tick_spacing=60)

    with patch("requests.post") as direct_rpc:
        result = sdk.get_pool_sqrt_price(pool_key)

    assert result == 2**96
    gateway.eth_call.assert_called_once_with(
        chain=CHAIN,
        to=sdk.addresses["state_view"],
        data=_expected_calldata(pool_key),
        raise_on_error=True,
    )
    direct_rpc.assert_not_called()
    fallback_recorder.assert_not_called()


def test_gateway_rpc_error_returns_none_without_direct_fallback(fallback_recorder: MagicMock) -> None:
    gateway = MagicMock()
    gateway.is_connected = True
    gateway.eth_call.side_effect = RuntimeError("upstream 503")
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://direct-fallback.local", gateway_client=gateway)
    pool_key = PoolKey(LOW_CURRENCY, HIGH_CURRENCY, fee=3000, tick_spacing=60)

    with patch("requests.post") as direct_rpc:
        result = sdk.get_pool_sqrt_price(pool_key)

    assert result is None
    direct_rpc.assert_not_called()
    _assert_fallback(fallback_recorder, OnchainReadFallbackReason.RPC_CALL_FAILED)


def test_no_read_transport_preserves_none_fallback(fallback_recorder: MagicMock) -> None:
    sdk = UniswapV4SDK(chain=CHAIN)
    pool_key = PoolKey(LOW_CURRENCY, HIGH_CURRENCY, fee=3000, tick_spacing=60)

    assert sdk.get_pool_sqrt_price(pool_key) is None
    _assert_fallback(fallback_recorder, OnchainReadFallbackReason.EMPTY_RESULT)


def test_empty_rpc_result_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    fallback_recorder: MagicMock,
) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", lambda **_kwargs: None)

    assert sdk.get_pool_sqrt_price(PoolKey(LOW_CURRENCY, HIGH_CURRENCY, 3000, 60)) is None
    _assert_fallback(fallback_recorder, OnchainReadFallbackReason.EMPTY_RESULT)


@pytest.mark.parametrize(
    "response",
    [
        "0x" + "00" * 31,
        "0x" + "zz" * 128,
        _slot0_response(1 << 160),
    ],
)
def test_malformed_slot0_response_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    fallback_recorder: MagicMock,
    response: str,
) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", lambda **_kwargs: response)

    assert sdk.get_pool_sqrt_price(PoolKey(LOW_CURRENCY, HIGH_CURRENCY, 3000, 60)) is None
    _assert_fallback(fallback_recorder, OnchainReadFallbackReason.DECODE_FAILED)


def test_zero_slot0_response_is_uninitialized_pool(
    monkeypatch: pytest.MonkeyPatch,
    fallback_recorder: MagicMock,
) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v4.sdk.eth_call_hex",
        lambda **_kwargs: _slot0_response(0, lp_fee=0),
    )

    assert sdk.get_pool_sqrt_price(PoolKey(LOW_CURRENCY, HIGH_CURRENCY, 3000, 60)) is None
    _assert_fallback(fallback_recorder, OnchainReadFallbackReason.POOL_UNINITIALIZED)


def test_pool_key_encoding_error_still_raises(
    monkeypatch: pytest.MonkeyPatch,
    fallback_recorder: MagicMock,
) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    rpc_call = MagicMock()
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", rpc_call)
    invalid_pool_key = PoolKey(LOW_CURRENCY, HIGH_CURRENCY, fee=1 << 24, tick_spacing=60)

    with pytest.raises(ValueError, match="uint24 out of range"):
        sdk.get_pool_sqrt_price(invalid_pool_key)

    rpc_call.assert_not_called()
    fallback_recorder.assert_not_called()
