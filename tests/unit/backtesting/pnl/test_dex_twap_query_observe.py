"""Branch coverage for DEXTWAPDataProvider._query_observe / _query_observe_sync.

Both variants import ``Web3`` lazily inside the call, so the tests patch the
``web3.Web3`` seam and script ``eth.call`` with hand-encoded ABI payloads.
Covered per variant: the missing-rpc-url early exit, observe() calldata
encoding, signed int56 tick decoding plus offset-based array parsing, the
short-response guard, RPC exception wrapping, and the response/request
length-mismatch path (IndexError swallowed into ``None``).

The higher-level calculate_twap flow is covered by
test_dex_twap_calculate.py; the tick math by test_dex_twap.py.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.price.dex_twap import (
    OBSERVE_SELECTOR,
    DEXTWAPDataProvider,
    TWAPObservation,
)

POOL = "0x" + "22" * 20


def _encode_observe_response(
    tick_cumulatives: list[int], seconds_per_liquidity: list[int]
) -> bytes:
    """ABI-encode observe()'s (int56[], uint160[]) return payload."""
    offset1 = 64
    offset2 = offset1 + 32 + 32 * len(tick_cumulatives)
    out = offset1.to_bytes(32, "big") + offset2.to_bytes(32, "big")
    out += len(tick_cumulatives).to_bytes(32, "big")
    for tick in tick_cumulatives:
        out += tick.to_bytes(32, "big", signed=True)
    out += len(seconds_per_liquidity).to_bytes(32, "big")
    for value in seconds_per_liquidity:
        out += value.to_bytes(32, "big")
    return out


def _web3_returning(result=None, call_exc=None, calls=None):
    """Fake ``web3.Web3`` class whose instance serves scripted eth.call results."""
    web3_cls = MagicMock()
    instance = web3_cls.return_value
    instance.to_checksum_address.side_effect = lambda address: address

    def _call(tx):
        if calls is not None:
            calls.append(tx)
        if call_exc is not None:
            raise call_exc
        return result

    instance.eth.call.side_effect = _call
    return web3_cls


@pytest.fixture
def provider() -> DEXTWAPDataProvider:
    return DEXTWAPDataProvider(chain="ethereum", rpc_url="http://gateway.invalid/rpc")


@pytest.fixture(params=["async", "sync"])
def variant(request):
    """Run either query variant through the same assertions."""

    def run(provider, pool_address, seconds_agos):
        if request.param == "async":
            return asyncio.run(provider._query_observe(pool_address, seconds_agos))
        return provider._query_observe_sync(pool_address, seconds_agos)

    return run


class TestQueryObserve:
    def test_no_rpc_url_returns_none_without_touching_web3(self, variant):
        offline = DEXTWAPDataProvider(chain="ethereum")
        web3_cls = _web3_returning()
        with patch("web3.Web3", web3_cls):
            assert variant(offline, POOL, [1800, 0]) is None
        web3_cls.assert_not_called()

    def test_happy_path_decodes_signed_ticks_and_liquidity(self, variant, provider):
        payload = _encode_observe_response([-1_000_000, -940_000], [111, 222])
        with patch("web3.Web3", _web3_returning(result=payload)):
            before = int(datetime.now(UTC).timestamp())
            observations = variant(provider, POOL, [1800, 0])
            after = int(datetime.now(UTC).timestamp())

        assert observations is not None
        assert len(observations) == 2
        assert all(isinstance(obs, TWAPObservation) for obs in observations)
        # int56 values are sign-extended into 32-byte words; decode must be signed.
        assert [obs.tick_cumulative for obs in observations] == [-1_000_000, -940_000]
        assert [obs.seconds_per_liquidity_cumulative for obs in observations] == [111, 222]
        assert all(obs.initialized for obs in observations)
        # Timestamps are synthesized as now - seconds_ago.
        assert before - 1800 <= observations[0].block_timestamp <= after - 1800
        assert before <= observations[1].block_timestamp <= after

    def test_encodes_observe_calldata(self, variant, provider):
        calls: list[dict] = []
        payload = _encode_observe_response([5, 10], [1, 2])
        with patch("web3.Web3", _web3_returning(result=payload, calls=calls)):
            variant(provider, POOL, [1800, 0])

        (tx,) = calls
        assert tx["to"] == POOL  # checksum passthrough in this harness
        expected = (
            OBSERVE_SELECTOR
            + (32).to_bytes(32, "big").hex()  # array offset
            + (2).to_bytes(32, "big").hex()  # array length
            + (1800).to_bytes(32, "big").hex()
            + (0).to_bytes(32, "big").hex()
        )
        assert tx["data"] == expected

    def test_short_response_returns_none(self, variant, provider):
        with patch("web3.Web3", _web3_returning(result=b"\x00" * 63)):
            assert variant(provider, POOL, [1800, 0]) is None

    def test_rpc_error_returns_none(self, variant, provider):
        with patch("web3.Web3", _web3_returning(call_exc=RuntimeError("rpc down"))):
            assert variant(provider, POOL, [1800, 0]) is None

    def test_fewer_accumulators_than_requested_returns_none(self, variant, provider):
        # Two seconds_agos requested but the pool answers with one-element
        # arrays: the IndexError is swallowed by the error wrapper.
        payload = _encode_observe_response([5], [1])
        with patch("web3.Web3", _web3_returning(result=payload)):
            assert variant(provider, POOL, [1800, 0]) is None
