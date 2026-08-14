"""Live verification for the test-only Curve pool catalog.

These checks intentionally use external RPC and therefore live outside the
unit tree. They catch catalog coin-order drift without making unit collection
depend on credentials or network availability.
"""

import os

import httpx
import pytest

from tests.support.curve_adapter import CURVE_TEST_POOLS

_CHAIN_RPC = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{key}",
    "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{key}",
    "base": "https://base-mainnet.g.alchemy.com/v2/{key}",
    "optimism": "https://opt-mainnet.g.alchemy.com/v2/{key}",
    "polygon": "https://polygon-mainnet.g.alchemy.com/v2/{key}",
}
_COINS_SELECTOR = "0xc6610657"


class _ConfirmedRevert:
    """Sentinel type for an RPC-confirmed out-of-range contract revert."""


_REVERTED = _ConfirmedRevert()


def _query_coin_address(rpc_url: str, pool_address: str, index: int) -> str | _ConfirmedRevert:
    """Query ``coins(index)`` and distinguish reverts from RPC failures."""
    data = _COINS_SELECTOR + hex(index)[2:].zfill(64)
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": pool_address, "data": data}, "latest"],
        "id": 1,
    }
    try:
        response = httpx.post(rpc_url, json=payload, timeout=10)
    except httpx.RequestError:
        raise RuntimeError(f"RPC transport failed querying {pool_address} coins({index})") from None
    if response.is_error:
        raise RuntimeError(f"RPC returned HTTP {response.status_code} querying {pool_address} coins({index})")
    try:
        body = response.json()
    except ValueError:
        raise RuntimeError(f"RPC returned invalid JSON querying {pool_address} coins({index})") from None

    error = body.get("error")
    if error is not None:
        message = str(error.get("message", "")) if isinstance(error, dict) else str(error)
        if "revert" in message.lower():
            return _REVERTED
        code = error.get("code") if isinstance(error, dict) else None
        raise RuntimeError(f"RPC error {code!r} querying {pool_address} coins({index}): {message}")

    result = body.get("result")
    if not isinstance(result, str) or not result.startswith("0x") or len(result) < 66:
        raise RuntimeError(f"RPC returned malformed coins({index}) data for {pool_address}")
    first_word = result[2:66]
    return "0x" + first_word[24:]


@pytest.mark.integration
class TestCurvePoolsCoinOrderOnChain:
    """Verify fixture coin order against each deployed pool."""

    @pytest.fixture(autouse=True)
    def _require_alchemy_key(self) -> None:
        key = os.environ.get("ALCHEMY_API_KEY")
        if not key:
            pytest.skip("ALCHEMY_API_KEY not set")
        self.alchemy_key = key

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, name) for chain, pools in CURVE_TEST_POOLS.items() for name in pools if chain in _CHAIN_RPC],
    )
    def test_coin_addresses_match_onchain(self, chain: str, pool_name: str) -> None:
        pool = CURVE_TEST_POOLS[chain][pool_name]
        rpc_url = _CHAIN_RPC[chain].format(key=self.alchemy_key)

        for index, expected_address in enumerate(pool["coin_addresses"]):
            onchain_address = _query_coin_address(rpc_url, pool["address"], index)
            assert onchain_address is not _REVERTED, f"{chain}/{pool_name}: coins({index}) unexpectedly reverted"
            assert isinstance(onchain_address, str)
            assert onchain_address.lower() == expected_address.lower(), (
                f"{chain}/{pool_name}: coin order mismatch at index {index}: "
                f"fixture has {expected_address}, on-chain has {onchain_address}"
            )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, name) for chain, pools in CURVE_TEST_POOLS.items() for name in pools if chain in _CHAIN_RPC],
    )
    def test_no_extra_coins_onchain(self, chain: str, pool_name: str) -> None:
        pool = CURVE_TEST_POOLS[chain][pool_name]
        rpc_url = _CHAIN_RPC[chain].format(key=self.alchemy_key)
        extra_address = _query_coin_address(rpc_url, pool["address"], pool["n_coins"])

        if extra_address is _REVERTED:
            return
        assert extra_address == "0x" + "0" * 40, (
            f"{chain}/{pool_name}: unexpected extra coin {extra_address} at index {pool['n_coins']}"
        )
