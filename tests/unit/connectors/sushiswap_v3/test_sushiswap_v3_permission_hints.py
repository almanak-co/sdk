"""Tests for sushiswap_v3 permission_hints module.

Verifies the bsc synthetic LP pair override (#1902) is wired correctly so
permission discovery emits USDT + WBNB approves on bsc, not the framework
default (USDC + bridged ETH).
"""

from almanak.framework.connectors.sushiswap_v3.permission_hints import (
    PERMISSION_HINTS,
)


def test_bsc_synthetic_lp_pair_is_usdt_wbnb():
    """The bsc override must point at the canonical liquid pair."""
    assert "bsc" in PERMISSION_HINTS.synthetic_lp_pair
    pair = PERMISSION_HINTS.synthetic_lp_pair["bsc"]
    # USDT (BSC), WBNB
    assert pair[0] == "0x55d398326f99059fF775485246999027B3197955"
    assert pair[1] == "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"


def test_pair_is_a_two_tuple():
    pair = PERMISSION_HINTS.synthetic_lp_pair["bsc"]
    # Must be a tuple specifically (immutable) — `len(pair) == 2` alone passes
    # for lists, sets, etc., which would silently weaken the contract.
    assert isinstance(pair, tuple)
    assert len(pair) == 2
