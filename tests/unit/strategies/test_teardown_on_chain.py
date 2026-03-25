"""Static analysis: detect cached-state teardown anti-pattern.

Scans get_open_positions() methods in incubating strategies for references
to self._* cached instance variables without corresponding gateway/RPC calls,
which indicates the method is using cached state instead of on-chain queries.

The IntentStrategy contract requires: "MUST query on-chain state, not cache."
See VIB-219 for the tracking ticket.
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent.parent

# Attributes that indicate cached state (not on-chain)
CACHED_STATE_ATTRS = {
    "_supplied_amount",
    "_borrowed_amount",
    "_has_position",
    "_position_size_usd",
    "_position_id",
    "_lp_token_id",
    "_position_value",
    "_collateral_amount",
    "_debt_amount",
}

# Attributes/calls that indicate on-chain queries
ON_CHAIN_INDICATORS = {
    "gateway_client",
    "get_token_balance",
    "get_position",
    "market",
    "get_balance",
    "rpc_call",
    "balanceOf",
    "get_portfolio",
}

# Strategies with known cached-state teardown (grandfathered, tracked in VIB-219).
# Remove entries as they are fixed. Adding new entries is NOT allowed — fix the
# strategy instead. When this set is empty, the xfail marker can be removed.
KNOWN_VIOLATIONS: set[str] = {
    "strategies/incubating/aave_supply_base/strategy.py",
    "strategies/incubating/aave_uniswap_leverage_polygon/strategy.py",
    "strategies/incubating/aave_v3_lending_bsc/strategy.py",
    "strategies/incubating/aave_v3_lending_mantle/strategy.py",
    "strategies/incubating/aerodrome_swap_aave_supply_base/strategy.py",
    "strategies/incubating/agni_swap_aave_supply_mantle/strategy.py",
    "strategies/incubating/benqi_leverage_loop/strategy.py",
    "strategies/incubating/curve_aave_yield_optimism/strategy.py",
    "strategies/incubating/curve_aave_yield_pipeline/strategy.py",
    "strategies/incubating/curve_stableswap_lp_base/strategy.py",
    "strategies/incubating/curve_swap_aave_supply_optimism/strategy.py",
    "strategies/incubating/edge_lp_arb_weth_usdc/strategy.py",
    "strategies/incubating/edge_lp_bsc_usdt_wbnb/strategy.py",
    "strategies/incubating/edge_lp_polygon_wmatic_usdc/strategy.py",
    "strategies/incubating/lifi_aave_optimism/strategy.py",
    "strategies/incubating/momentum_accumulation/strategy.py",
    "strategies/incubating/morpho_blue_yield_rotator/strategy.py",
    "strategies/incubating/pancakeswap_aave_base/strategy.py",
    "strategies/incubating/pancakeswap_aave_bsc/strategy.py",
    "strategies/incubating/pancakeswap_aave_ethereum/strategy.py",
    "strategies/incubating/pancakeswap_v3_ethereum/strategy.py",
    "strategies/incubating/pancakeswap_v3_lp_bsc/strategy.py",
    "strategies/incubating/sushiswap_limit_order_lp/strategy.py",
    "strategies/incubating/sushiswap_v3_avalanche/strategy.py",
    "strategies/incubating/sushiswap_v3_base/strategy.py",
    "strategies/incubating/sushiswap_v3_ethereum/strategy.py",
    "strategies/incubating/sushiswap_v3_lp_base/strategy.py",
    "strategies/incubating/sushiswap_v3_lp_bsc/strategy.py",
    "strategies/incubating/sushiswap_v3_optimism/strategy.py",
    "strategies/incubating/sushiswap_v3_polygon/strategy.py",
    "strategies/incubating/uniswap_aave_yield_polygon/strategy.py",
    "strategies/incubating/uniswap_v3_avalanche/strategy.py",
    "strategies/incubating/uniswap_v3_lp_bsc/strategy.py",
    "strategies/incubating/uniswap_v3_mantle/strategy.py",
    "strategies/incubating/velodrome_lp_optimism/strategy.py",
}


def _find_strategies_with_cached_teardown():
    """Find strategies whose get_open_positions uses cached state.

    Uses proper AST walking to find self.<attr> references rather than
    string matching on ast.dump() (which is CPython-version-specific).
    """
    violations = []
    strategies_dir = REPO_ROOT / "strategies" / "incubating"
    if not strategies_dir.exists():
        return violations

    for strategy_file in sorted(strategies_dir.glob("*/strategy.py")):
        source = strategy_file.read_text()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if not (isinstance(node, ast.FunctionDef) and node.name == "get_open_positions"):
                continue

            # Walk the function body looking for self.<cached_attr> references
            has_cached = False
            has_onchain = False
            for child in ast.walk(node):
                if isinstance(child, ast.Attribute):
                    if child.attr in CACHED_STATE_ATTRS:
                        has_cached = True
                    if child.attr in ON_CHAIN_INDICATORS:
                        has_onchain = True
                elif isinstance(child, ast.Name) and child.id in ON_CHAIN_INDICATORS:
                    has_onchain = True

            if has_cached and not has_onchain:
                rel_path = str(strategy_file.relative_to(REPO_ROOT))
                violations.append(rel_path)

    return violations


# Use the explicit KNOWN_VIOLATIONS set as the baseline (not a dynamic snapshot).
# This ensures the regression guard catches NEW violations introduced in PRs.
_BASELINE_VIOLATIONS = sorted(KNOWN_VIOLATIONS)


@pytest.mark.xfail(reason="Known anti-pattern in incubating strategies (VIB-219)", strict=False)
def test_no_cached_state_in_teardown():
    """Detect strategies using cached state in get_open_positions().

    This test scans incubating strategy files and flags those that reference
    cached instance variables (self._supplied_amount, etc.) in get_open_positions()
    without also referencing on-chain query methods.

    When all incubating strategies are fixed to query on-chain state, this test
    will start passing and the xfail marker can be removed.
    """
    violations = _find_strategies_with_cached_teardown()
    assert not violations, (
        f"{len(violations)} strategies use cached state in get_open_positions() "
        f"without on-chain queries:\n" + "\n".join(f"  - {v}" for v in violations)
    )


def test_no_new_cached_state_violations():
    """Ensure no NEW strategies introduce the cached-state teardown anti-pattern.

    This test is strict: it fails CI if a strategy not in the known baseline
    introduces cached state in get_open_positions(). Existing violations are
    grandfathered (tracked in test_no_cached_state_in_teardown above).
    """
    current = set(_find_strategies_with_cached_teardown())
    baseline = set(_BASELINE_VIOLATIONS)
    new_violations = current - baseline
    assert not new_violations, (
        f"{len(new_violations)} NEW strategies introduce cached state in "
        f"get_open_positions() (not in baseline):\n"
        + "\n".join(f"  - {v}" for v in sorted(new_violations))
        + "\n\nFix: query on-chain state instead of using self._* cached variables."
    )
