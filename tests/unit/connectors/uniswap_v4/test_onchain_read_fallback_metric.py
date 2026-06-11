"""VIB-5052: money-path on-chain read fallback observability.

The VIB-5038 ``getSlot0`` selector regression survived ~2 months in production
because a hard on-chain read failure was silently converted to an estimated
sqrtPrice with NO metric. This file is the regression guard for the contract:

1. ``record_onchain_read_fallback`` increments ``onchain_read_fallback_total``
   with ``{protocol, chain, call, reason}`` and coerces / rejects the reason.
2. Every return-None fallback path in
   :meth:`UniswapV4SDK.get_pool_sqrt_price` increments the counter exactly once
   with the right ``reason`` label.
3. The success path does NOT increment the counter.

The counter is read off ``FRAMEWORK_REGISTRY`` via ``get_sample_value`` and
diffed against a baseline captured inside each test so parallel test workers do
not race each other on the shared registry.
"""

from __future__ import annotations

import pytest

from almanak.connectors.uniswap_v4.sdk import PoolKey, UniswapV4SDK, _tick_to_sqrt_ratio_x96
from almanak.framework.observability.metrics import (
    FRAMEWORK_REGISTRY,
    OnchainReadFallbackReason,
    record_onchain_read_fallback,
)

CHAIN = "base"
PROTOCOL = "uniswap_v4"
CALL = "getSlot0"


def _counter_value(*, protocol: str, chain: str, call: str, reason: str) -> float:
    """Return the current counter sample for the label set, or 0.0."""
    value = FRAMEWORK_REGISTRY.get_sample_value(
        "onchain_read_fallback_total",
        {"protocol": protocol, "chain": chain, "call": call, "reason": reason},
    )
    return value if value is not None else 0.0


def _make_pool_key() -> PoolKey:
    return PoolKey(
        currency0="0x4200000000000000000000000000000000000006",  # WETH (base)
        currency1="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC (base)
        fee=3000,
        tick_spacing=60,
        hooks="0x0000000000000000000000000000000000000000",
    )


# =============================================================================
# Helper-level contract
# =============================================================================


def test_helper_increments_with_labels() -> None:
    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.RPC_CALL_FAILED.value
    )
    record_onchain_read_fallback(
        protocol=PROTOCOL,
        chain=CHAIN,
        call=CALL,
        reason=OnchainReadFallbackReason.RPC_CALL_FAILED,
    )
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.RPC_CALL_FAILED.value
    )
    assert after == pytest.approx(before + 1.0)


def test_helper_accepts_string_reason() -> None:
    """String reasons are coerced via the enum (terse call sites)."""
    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.EMPTY_RESULT.value
    )
    record_onchain_read_fallback(protocol=PROTOCOL, chain=CHAIN, call=CALL, reason="empty_result")
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.EMPTY_RESULT.value
    )
    assert after == pytest.approx(before + 1.0)


def test_helper_rejects_unknown_reason() -> None:
    """An unknown reason string fails fast rather than polluting cardinality."""
    with pytest.raises(ValueError):
        record_onchain_read_fallback(protocol=PROTOCOL, chain=CHAIN, call=CALL, reason="not_a_real_reason")


def test_helper_lowercases_protocol_and_chain() -> None:
    before = _counter_value(
        protocol="uniswap_v4", chain="base", call=CALL, reason=OnchainReadFallbackReason.DECODE_FAILED.value
    )
    record_onchain_read_fallback(
        protocol="Uniswap_V4",
        chain="BASE",
        call=CALL,
        reason=OnchainReadFallbackReason.DECODE_FAILED,
    )
    after = _counter_value(
        protocol="uniswap_v4", chain="base", call=CALL, reason=OnchainReadFallbackReason.DECODE_FAILED.value
    )
    assert after == pytest.approx(before + 1.0)


def test_helper_empty_labels_default_to_unknown() -> None:
    before = _counter_value(
        protocol="unknown", chain="unknown", call="unknown", reason=OnchainReadFallbackReason.READER_UNAVAILABLE.value
    )
    record_onchain_read_fallback(
        protocol="",
        chain="",
        call="",
        reason=OnchainReadFallbackReason.READER_UNAVAILABLE,
    )
    after = _counter_value(
        protocol="unknown", chain="unknown", call="unknown", reason=OnchainReadFallbackReason.READER_UNAVAILABLE.value
    )
    assert after == pytest.approx(before + 1.0)


# =============================================================================
# get_pool_sqrt_price fallback paths (the VIB-5038 root-cause sites)
# =============================================================================


def test_reader_unavailable_increments(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setitem(sdk.addresses, "state_view", "")  # no StateView address

    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.READER_UNAVAILABLE.value
    )
    result = sdk.get_pool_sqrt_price(_make_pool_key())
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.READER_UNAVAILABLE.value
    )

    assert result is None
    assert after == pytest.approx(before + 1.0)


def test_rpc_call_failed_increments(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")

    def _boom(**_kwargs: object) -> str:
        raise RuntimeError("execution reverted: selector mismatch")

    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", _boom)

    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.RPC_CALL_FAILED.value
    )
    result = sdk.get_pool_sqrt_price(_make_pool_key())
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.RPC_CALL_FAILED.value
    )

    assert result is None
    assert after == pytest.approx(before + 1.0)


def test_empty_result_increments(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", lambda **_kwargs: None)

    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.EMPTY_RESULT.value
    )
    result = sdk.get_pool_sqrt_price(_make_pool_key())
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.EMPTY_RESULT.value
    )

    assert result is None
    assert after == pytest.approx(before + 1.0)


def test_decode_failed_increments(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", lambda **_kwargs: "0xdeadbeef")

    def _bad_decode(_hex: str) -> object:
        raise ValueError("malformed slot0 response")

    monkeypatch.setattr("almanak.connectors.uniswap_v4.hooks.decode_slot0_response", _bad_decode)

    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.DECODE_FAILED.value
    )
    result = sdk.get_pool_sqrt_price(_make_pool_key())
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.DECODE_FAILED.value
    )

    assert result is None
    assert after == pytest.approx(before + 1.0)


def test_pool_uninitialized_increments(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", lambda **_kwargs: "0x00")

    class _UninitState:
        exists = False
        sqrt_price_x96 = 0
        tick = 0

    monkeypatch.setattr(
        "almanak.connectors.uniswap_v4.hooks.decode_slot0_response",
        lambda _hex: _UninitState(),
    )

    before = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.POOL_UNINITIALIZED.value
    )
    result = sdk.get_pool_sqrt_price(_make_pool_key())
    after = _counter_value(
        protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=OnchainReadFallbackReason.POOL_UNINITIALIZED.value
    )

    assert result is None
    assert after == pytest.approx(before + 1.0)


def test_success_path_does_not_increment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A healthy on-chain read must NOT touch any fallback counter."""
    sdk = UniswapV4SDK(chain=CHAIN, rpc_url="http://anvil.local")
    monkeypatch.setattr("almanak.connectors.uniswap_v4.sdk.eth_call_hex", lambda **_kwargs: "0x01")

    good_sqrt = _tick_to_sqrt_ratio_x96(0)

    class _GoodState:
        exists = True
        sqrt_price_x96 = good_sqrt
        tick = 0

    monkeypatch.setattr(
        "almanak.connectors.uniswap_v4.hooks.decode_slot0_response",
        lambda _hex: _GoodState(),
    )

    baselines = {
        reason: _counter_value(protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=reason.value)
        for reason in OnchainReadFallbackReason
    }

    result = sdk.get_pool_sqrt_price(_make_pool_key())

    assert result == good_sqrt
    for reason in OnchainReadFallbackReason:
        after = _counter_value(protocol=PROTOCOL, chain=CHAIN, call=CALL, reason=reason.value)
        assert after == pytest.approx(baselines[reason]), f"fallback counter for {reason} moved on success path"
