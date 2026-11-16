"""Lending grants cannot widen Aerodrome's correlated pool and cash limits."""

import pytest

from tests.strategy_fork.aave_v3_authority import assert_permissions as lending_permissions
from tests.strategy_fork.aave_v3_authority import wrapped
from tests.strategy_fork.aerodrome_authority import swap_attacks


def assert_permissions(node, config):
    result = lending_permissions(node, config)
    target = config.target("trade")
    profile = config.accounts.binding(target.account).profile
    for label, destination, inner in swap_attacks(target):
        call = wrapped(profile, destination, inner)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        result["rejected"].append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return result
