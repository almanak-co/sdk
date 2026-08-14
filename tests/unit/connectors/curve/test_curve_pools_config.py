"""Tests for CURVE_TEST_POOLS configuration integrity.

Validates that CURVE_TEST_POOLS entries are internally consistent. Live
on-chain verification lives under ``tests/integration`` so this unit module is
always hermetic.
"""

import re

import pytest

from tests.support.curve_adapter import CURVE_TEST_POOLS

# =============================================================================
# Unit Tests: Internal Consistency (no RPC required)
# =============================================================================


class TestCurvePoolsConsistency:
    """Verify CURVE_TEST_POOLS config entries are internally consistent."""

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_n_coins_matches_coins_list(self, chain: str, pool_name: str) -> None:
        """n_coins must equal len(coins) for every pool."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        assert len(pool["coins"]) == pool["n_coins"], (
            f"{chain}/{pool_name}: coins has {len(pool['coins'])} entries but n_coins={pool['n_coins']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_n_coins_matches_coin_addresses(self, chain: str, pool_name: str) -> None:
        """coin_addresses length must equal n_coins for every pool."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        assert len(pool["coin_addresses"]) == pool["n_coins"], (
            f"{chain}/{pool_name}: coin_addresses has {len(pool['coin_addresses'])} entries "
            f"but n_coins={pool['n_coins']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_n_coins_matches_coin_decimals(self, chain: str, pool_name: str) -> None:
        """coin_decimals must be explicit and aligned with pool coin order."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        assert len(pool["coin_decimals"]) == pool["n_coins"], (
            f"{chain}/{pool_name}: coin_decimals has {len(pool['coin_decimals'])} entries but n_coins={pool['n_coins']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_pool_address_is_valid_hex(self, chain: str, pool_name: str) -> None:
        """Pool address must be a valid 0x-prefixed hex address."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        assert re.match(r"^0x[0-9a-fA-F]{40}$", pool["address"]), (
            f"{chain}/{pool_name}: invalid pool address {pool['address']}"
        )

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_coin_addresses_are_valid_hex(self, chain: str, pool_name: str) -> None:
        """All coin addresses must be valid 0x-prefixed hex addresses."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        for i, addr in enumerate(pool["coin_addresses"]):
            assert re.match(r"^0x[0-9a-fA-F]{40}$", addr), f"{chain}/{pool_name}: invalid coin_address[{i}] {addr}"

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_no_duplicate_coin_addresses(self, chain: str, pool_name: str) -> None:
        """No pool should have duplicate coin addresses."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        addrs = [a.lower() for a in pool["coin_addresses"]]
        assert len(addrs) == len(set(addrs)), f"{chain}/{pool_name}: duplicate coin_addresses found"

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_pool_type_is_valid(self, chain: str, pool_name: str) -> None:
        """pool_type must be one of the known types."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        valid_types = {"stableswap", "cryptoswap", "tricrypto"}
        assert pool["pool_type"] in valid_types, f"{chain}/{pool_name}: unknown pool_type '{pool['pool_type']}'"

    @pytest.mark.parametrize(
        "chain,pool_name",
        [(chain, pool_name) for chain, pools in CURVE_TEST_POOLS.items() for pool_name in pools],
    )
    def test_lp_token_is_valid_hex(self, chain: str, pool_name: str) -> None:
        """LP token address must be a valid 0x-prefixed hex address."""
        pool = CURVE_TEST_POOLS[chain][pool_name]
        assert re.match(r"^0x[0-9a-fA-F]{40}$", pool["lp_token"]), (
            f"{chain}/{pool_name}: invalid lp_token address {pool['lp_token']}"
        )
