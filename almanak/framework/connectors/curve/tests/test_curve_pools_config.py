"""Tests for CURVE_POOLS configuration integrity.

Validates that CURVE_POOLS entries are internally consistent and that coin
addresses match on-chain coins() for all configured pools. Catches the class
of bug where config coin order diverges from on-chain contract state (VIB-1677,
VIB-580, iter 114 BUG-1).

Unit tests (no RPC): internal consistency checks.
Integration tests (requires RPC): on-chain coins() verification.
"""

import logging
import os
import re

import httpx
import pytest

from almanak.framework.connectors.curve.adapter import CURVE_POOLS

_logger = logging.getLogger(__name__)

# =============================================================================
# Unit Tests: Internal Consistency (no RPC required)
# =============================================================================


class TestCurvePoolsConsistency:
    """Verify CURVE_POOLS config entries are internally consistent."""

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_n_coins_matches_coins_list(self, chain: str, pool_name: str) -> None:
        """n_coins must equal len(coins) for every pool."""
        pool = CURVE_POOLS[chain][pool_name]
        assert len(pool["coins"]) == pool["n_coins"], (
            f"{chain}/{pool_name}: coins has {len(pool['coins'])} entries but n_coins={pool['n_coins']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_n_coins_matches_coin_addresses(self, chain: str, pool_name: str) -> None:
        """coin_addresses length must equal n_coins for every pool."""
        pool = CURVE_POOLS[chain][pool_name]
        assert len(pool["coin_addresses"]) == pool["n_coins"], (
            f"{chain}/{pool_name}: coin_addresses has {len(pool['coin_addresses'])} entries "
            f"but n_coins={pool['n_coins']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_pool_address_is_valid_hex(self, chain: str, pool_name: str) -> None:
        """Pool address must be a valid 0x-prefixed hex address."""
        pool = CURVE_POOLS[chain][pool_name]
        assert re.match(r"^0x[0-9a-fA-F]{40}$", pool["address"]), (
            f"{chain}/{pool_name}: invalid pool address {pool['address']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_coin_addresses_are_valid_hex(self, chain: str, pool_name: str) -> None:
        """All coin addresses must be valid 0x-prefixed hex addresses."""
        pool = CURVE_POOLS[chain][pool_name]
        for i, addr in enumerate(pool["coin_addresses"]):
            assert re.match(r"^0x[0-9a-fA-F]{40}$", addr), f"{chain}/{pool_name}: invalid coin_address[{i}] {addr}"

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_no_duplicate_coin_addresses(self, chain: str, pool_name: str) -> None:
        """No pool should have duplicate coin addresses."""
        pool = CURVE_POOLS[chain][pool_name]
        addrs = [a.lower() for a in pool["coin_addresses"]]
        assert len(addrs) == len(set(addrs)), f"{chain}/{pool_name}: duplicate coin_addresses found"

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_pool_type_is_valid(self, chain: str, pool_name: str) -> None:
        """pool_type must be one of the known types."""
        pool = CURVE_POOLS[chain][pool_name]
        valid_types = {"stableswap", "cryptoswap", "tricrypto"}
        assert pool["pool_type"] in valid_types, f"{chain}/{pool_name}: unknown pool_type '{pool['pool_type']}'"

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools],
    )
    def test_lp_token_is_valid_hex(self, chain: str, pool_name: str) -> None:
        """LP token address must be a valid 0x-prefixed hex address."""
        pool = CURVE_POOLS[chain][pool_name]
        assert re.match(r"^0x[0-9a-fA-F]{40}$", pool["lp_token"]), (
            f"{chain}/{pool_name}: invalid lp_token address {pool['lp_token']}"
        )


# =============================================================================
# Integration Tests: On-chain Verification (requires RPC)
# =============================================================================

# Chain -> Alchemy RPC URL template
_CHAIN_RPC = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{key}",
    "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{key}",
    "base": "https://base-mainnet.g.alchemy.com/v2/{key}",
    "optimism": "https://opt-mainnet.g.alchemy.com/v2/{key}",
}

# coins(uint256) selector
_COINS_SELECTOR = "0xc6610657"


def _query_coin_address(rpc_url: str, pool_address: str, index: int) -> str | None:
    """Query coins(index) on a Curve pool contract via JSON-RPC."""
    # Encode coins(uint256) call: selector + index padded to 32 bytes
    data = _COINS_SELECTOR + hex(index)[2:].zfill(64)
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": pool_address, "data": data}, "latest"],
        "id": 1,
    }
    try:
        resp = httpx.post(rpc_url, json=payload, timeout=10)
        result = resp.json().get("result")
        if not result or result == "0x" or len(result) < 66:
            return None
        # Extract address from 32-byte return value (last 20 bytes)
        return "0x" + result[-40:]
    except (httpx.RequestError, KeyError, ValueError) as e:
        _logger.debug("RPC call failed for %s coins(%d): %s", pool_address, index, e)
        return None


@pytest.mark.integration
class TestCurvePoolsCoinOrderOnChain:
    """Verify CURVE_POOLS coin order matches on-chain coins() calls.

    Requires ALCHEMY_API_KEY environment variable.
    Catches bugs like iter 114 BUG-1 (crvusd_usdc coin order reversed).
    """

    @pytest.fixture(autouse=True)
    def _require_alchemy_key(self):
        key = os.environ.get("ALCHEMY_API_KEY")
        if not key:
            pytest.skip("ALCHEMY_API_KEY not set")
        self.alchemy_key = key

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools if chain in _CHAIN_RPC],
    )
    def test_coin_addresses_match_onchain(self, chain: str, pool_name: str) -> None:
        """Each coin_address[i] must match on-chain coins(i)."""
        pool = CURVE_POOLS[chain][pool_name]
        rpc_url = _CHAIN_RPC[chain].format(key=self.alchemy_key)

        for i, expected_addr in enumerate(pool["coin_addresses"]):
            onchain_addr = _query_coin_address(rpc_url, pool["address"], i)
            assert onchain_addr is not None, f"{chain}/{pool_name}: coins({i}) RPC call returned None"
            assert onchain_addr.lower() == expected_addr.lower(), (
                f"{chain}/{pool_name}: coin order mismatch at index {i}! "
                f"Config has {expected_addr} but on-chain coins({i}) = {onchain_addr}. "
                f"This is the same class of bug as iter 114 BUG-1 (VIB-1667)."
            )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_POOLS.items() for pool_name in pools if chain in _CHAIN_RPC],
    )
    def test_no_extra_coins_onchain(self, chain: str, pool_name: str) -> None:
        """Verify coins(n_coins) returns zero/reverts (no unconfigured coins)."""
        pool = CURVE_POOLS[chain][pool_name]
        rpc_url = _CHAIN_RPC[chain].format(key=self.alchemy_key)
        n_coins = pool["n_coins"]

        extra_addr = _query_coin_address(rpc_url, pool["address"], n_coins)
        if extra_addr is not None:
            # Some pools revert, some return zero address
            assert extra_addr == "0x" + "0" * 40, (
                f"{chain}/{pool_name}: coins({n_coins}) returned {extra_addr} -- "
                f"pool may have more coins than n_coins={n_coins}"
            )
