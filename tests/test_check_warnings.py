"""Check findings: measured gas limits, gas amount units and lower bounds, and market gas
prices. Every finding is advisory; none refuses a check."""

from pathlib import Path

import pytest

from almanak.chains import POLYGON
from almanak.check_warnings import (
    MIN_LIVE_DECISION_INTERVAL_SECONDS,
    OVERSIZED_PRICE_FACTOR,
    SUGGESTED_SPIKE_HEADROOM,
    WINDOW_BLOCKS,
    decision_interval_warnings,
    gas_amount_warnings,
    gas_references,
    market_gas_warnings,
    offline_warnings,
    significant_ceiling,
)
from almanak.configuration.unresolved import read_inputs

USDC = "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

CONFIG = """
strategy:
  decision_interval: 1h
  allocations:
    position:
      market: pool
      account: fund
      capital: {{asset: {usdc}, quantity: {{base_units: 200000000}}}}
      entry_swap_bps: 10000
      hold_seconds: 1d
      slippage_bps: 100
      deadline_seconds: 300s
      minimum_native_balance: {{base_units: 0}}{shape}
  gas:
    base:
      accounts: [fund]
      chain: eip155:8453
      source: network_rpc_fee_history
      reward_percentile: 50
      base_fee_multiplier_bps: 20000
      max_gas_price_wei: 1000000000
      max_gas_limit: {limit}
      gas_limit_buffer_bps: 2500
      max_transaction_fee: {fee}
{budget}      exit_reserve: {reserve}
markets:
  pool:
    connector: uniswap_v3
    chain: eip155:8453
    pool: '0xd0b53d9277642d899df5c87a3966a349a798f224'
accounts:
  fund:
    chain: eip155:8453
"""


def inputs(
    tmp_path: Path,
    *,
    limit=500000,
    fee="{base_units: 1500000000000000}",
    budget="{base_units: 6000000000000000}",
    reserve="{base_units: 3000000000000000}",
    shape="",
):
    path = tmp_path / "config.yaml"
    path.write_text(
        CONFIG.format(
            usdc=USDC,
            limit=limit,
            fee=fee,
            budget="" if budget is None else f"      total_budget: {budget}\n",
            reserve=reserve,
            shape=shape,
        )
    )
    return read_inputs(path).inputs


def by_code(warnings, code):
    return [w for w in warnings if w["code"] == code]


def test_references_carry_fork_receipt_provenance():
    swap = next(
        r
        for r in gas_references()
        if (r["connector"], r["chain"], r["action"]) == ("uniswap_v3", "eip155:8453", "swap")
    )
    assert swap["profile"] == "safe_roles"
    assert swap["gas_used"] >= 400289
    assert swap["source"]["kind"] == "fork_receipt"
    assert swap["source"]["transaction"].startswith("0x")


def test_gas_limit_below_measured_swap_plus_buffer_warns(tmp_path):
    [finding] = by_code(offline_warnings(inputs(tmp_path)), "gas_limit")
    assert finding["field"] == "strategy.gas.base.max_gas_limit"
    assert "uniswap_v3 swap" in finding["message"]
    assert f"at least {required_limit()}" in finding["message"]


def test_gas_limit_covering_the_buffered_reference_is_silent(tmp_path):
    assert not by_code(offline_warnings(inputs(tmp_path, limit=required_limit())), "gas_limit")


def test_gas_amount_above_ten_native_units_warns(tmp_path):
    # A quoted decimal is whole units: 1.5e15 ETH, not 1.5e15 wei.
    config = inputs(tmp_path, limit=1500000, fee="'1500000000000000'")
    [finding] = by_code(offline_warnings(config), "gas_amount_units")
    assert finding["field"] == "strategy.gas.base.max_transaction_fee"
    policy = config.strategy.gas["base"]
    assert by_code(gas_amount_warnings("base", policy), "gas_amount_units") == [finding]


def test_the_native_unit_ceiling_follows_the_chain(tmp_path):
    # 12 POL is a few dollars of Polygon gas; base units written as a decimal still warn.
    policy = inputs(tmp_path, limit=1500000).strategy.gas["base"]
    polygon = policy.model_copy(update={"chain": POLYGON.chain})
    budget = polygon.model_copy(update={"total_budget": "12"})
    assert not by_code(gas_amount_warnings("base", budget), "gas_amount_units")
    at_ceiling = polygon.model_copy(update={"total_budget": "10000"})
    assert not by_code(gas_amount_warnings("base", at_ceiling), "gas_amount_units")
    over = polygon.model_copy(update={"total_budget": "10001"})
    [finding] = by_code(gas_amount_warnings("base", over), "gas_amount_units")
    assert "10001 whole native units" in finding["message"]
    assert "a gas amount above 10000 is implausible" in finding["message"]
    slip = polygon.model_copy(update={"total_budget": "12000000000000000000"})
    assert by_code(gas_amount_warnings("base", slip), "gas_amount_units")


def test_plausible_gas_amounts_are_silent(tmp_path):
    assert not by_code(offline_warnings(inputs(tmp_path, limit=1500000)), "gas_amount_units")


def test_decision_interval_below_the_live_minimum_warns(tmp_path):
    config = inputs(tmp_path)
    assert decision_interval_warnings(config) == []
    for seconds, warns in ((5, True), (MIN_LIVE_DECISION_INTERVAL_SECONDS, False)):
        strategy = config.strategy.model_copy(update={"decision_interval": seconds})
        short = config.model_copy(update={"strategy": strategy})
        findings = decision_interval_warnings(short)
        assert [w["code"] for w in findings] == (["decision_interval"] if warns else [])
        # The author-file warnings shared with fork checks never include it.
        assert not by_code(offline_warnings(short), "decision_interval")


def required_limit() -> int:
    """The largest Base uniswap_v3 measurement plus the policy's 25% buffer."""
    measured = max(
        max(r["gas_used"], r.get("gas_estimate", 0))
        for r in gas_references()
        if (r["connector"], r["chain"]) == ("uniswap_v3", "eip155:8453")
    )
    return -(-measured * 12500 // 10000)


def test_reference_file_is_one_row_per_key():
    keys = [
        (r["connector"], r["chain"], r["profile"], r["action"], r.get("variant"))
        for r in gas_references()
    ]
    assert len(keys) == len(set(keys))


GAS_BOUNDS = ("gas_fee_ceiling", "gas_budget", "gas_exit_reserve")
FEE = 1_500_000_000_000_000


def bounds(config):
    return [w for w in offline_warnings(config) if w["code"] in GAS_BOUNDS]


def test_consistent_gas_amounts_have_no_lower_bound_findings(tmp_path):
    # A trade enters and exits in at least two transactions each: 2 x fee + reserve.
    assert bounds(inputs(tmp_path)) == []


def test_a_budget_that_fits_one_fee_warns_with_the_entry_transaction_count(tmp_path):
    # 1450c70b: total_budget equal to max_transaction_fee ran out after four transactions.
    config = inputs(tmp_path, budget=f"{{base_units: {FEE}}}", reserve="{base_units: 0}")
    [finding] = by_code(bounds(config), "gas_budget")
    assert finding["field"] == "strategy.gas.base.total_budget"
    message = finding["message"]
    assert message.startswith(f"total_budget {FEE} wei (0.0015 native) is below 2 entry")
    assert f"= {2 * FEE} wei" in message and "paid by fund" in message


def test_a_liquidity_allocation_needs_more_entry_and_exit_transactions(tmp_path):
    shape = "\n      range: {lower: -200000, upper: -190000}"
    config = inputs(tmp_path, shape=shape)
    budget, reserve = bounds(config)
    reserved = 3 * 10**15
    assert budget["field"] == "strategy.gas.base.total_budget"
    assert "below 4 entry transaction(s)" in budget["message"]
    assert f"at least {4 * FEE + reserved} wei" in budget["message"]
    assert reserve["field"] == "strategy.gas.base.exit_reserve"
    # Close, approve, swap back, then revoke the mint's residual allowance on each currency.
    assert "below 5 exit transaction(s)" in reserve["message"]
    assert f"at least {5 * FEE} wei" in reserve["message"]


def test_an_exit_reserve_below_one_fee_per_exit_transaction_warns(tmp_path):
    reserve = f"{{base_units: {FEE}}}"
    [finding] = bounds(inputs(tmp_path, reserve=reserve))
    assert finding["code"] == "gas_exit_reserve"
    assert "below 2 exit transaction(s)" in finding["message"]


def test_a_fee_ceiling_below_gas_limit_times_price_ceiling_warns(tmp_path):
    # 4f9705fe: max_transaction_fee under max_gas_limit x max_gas_price_wei.
    config = inputs(tmp_path, limit=2_000_000)
    [finding] = bounds(config)
    assert finding["code"] == "gas_fee_ceiling"
    assert finding["field"] == "strategy.gas.base.max_transaction_fee"
    assert "max_gas_limit 2000000 x max_gas_price_wei 1000000000" in finding["message"]
    assert f"at most {FEE // 2_000_000} wei" in finding["message"]


def test_without_a_total_budget_only_the_lifetime_cap_check_goes(tmp_path):
    assert bounds(inputs(tmp_path, budget=None)) == []
    reserve = f"{{base_units: {FEE}}}"
    [finding] = bounds(inputs(tmp_path, budget=None, reserve=reserve))
    assert finding["code"] == "gas_exit_reserve"
    [ceiling] = bounds(inputs(tmp_path, budget=None, limit=2_000_000))
    assert ceiling["code"] == "gas_fee_ceiling"
    config = inputs(tmp_path, budget=None)
    assert gas_amount_warnings("base", config.strategy.gas["base"]) == []


def resolved_policy(tmp_path, **values):
    return inputs(tmp_path, **values).strategy.gas["base"].resolve("base")


def test_a_price_ceiling_below_the_current_quote_warns_online(tmp_path):
    # 6f654439: 0.02 gwei on Arbitrum is below the quote with the author's own multiplier.
    policy = resolved_policy(tmp_path)
    [finding] = market_gas_warnings((policy,), lambda _: 1_200_000_000)
    assert finding["code"] == "gas_price"
    assert finding["field"] == "strategy.gas.base.max_gas_price_wei"
    message = finding["message"]
    assert "max_gas_price_wei 1000000000 is below the current gas price" in message
    assert "base_fee_multiplier_bps 20000 is 1200000000 wei" in message


def test_a_fee_ceiling_below_the_gas_limit_at_the_current_quote_warns_online(tmp_path):
    policy = resolved_policy(tmp_path, limit=2_000_000)
    [finding] = market_gas_warnings((policy,), lambda _: 1_000_000_000)
    assert finding["code"] == "gas_fee_quote"
    assert f"pays for at most {FEE // 1_000_000_000} gas now" in finding["message"]
    assert market_gas_warnings((resolved_policy(tmp_path),), lambda _: 10**9) == []


def on_chain(policy, chain):
    from almanak.caip import AssetType

    native = {"eip155:56": "slip44:714", "eip155:43114": "slip44:9005"}.get(chain, "slip44:60")
    return policy.model_copy(update={"asset": AssetType(f"{chain}/{native}")})


def ethereum_policy(tmp_path, **values):
    """The test policy on Ethereum, where no L1 data fee applies."""
    return on_chain(resolved_policy(tmp_path, **values), "eip155:1")


def oversized(policy, price, window=None, l1=None):
    return market_gas_warnings(
        (policy,),
        lambda _: price,
        (lambda _: price) if window is None else (lambda _: window),
        None if l1 is None else (lambda _: l1),
    )


def test_a_fee_far_above_the_executable_fee_warns_online(tmp_path):
    # 500k gas at a 0.01 gwei window bid costs 5e12 wei; the 1.5e15 fee is 300x that.
    [finding] = oversized(ethereum_policy(tmp_path), 10**7)
    assert finding["code"] == "gas_ceiling_oversized"
    assert finding["field"] == "strategy.gas.base.max_transaction_fee"
    message = finding["message"]
    suggested = 500_000 * 10**7 * SUGGESTED_SPIKE_HEADROOM
    assert f"max_transaction_fee {FEE} wei (0.0015 native) is 300x the executable fee" in message
    assert "5000000000000 wei (0.000005 native) (max_gas_limit 500000 x 10000000 wei)" in message
    assert f"highest bid over the last {WINDOW_BLOCKS} eip155:1 blocks" in message
    assert "base_fee_multiplier_bps 20000 is 10000000 wei" in message
    assert f"max_transaction_fee {suggested} wei (0.000025 native)" in message
    assert "spike beyond 5x makes transactions, exits included, wait" in message
    assert "nothing is lost" in message
    assert "entry_transactions x max_transaction_fee + exit_reserve" in message
    assert "exit transactions x the new fee" in message
    assert "omitted max_gas_price_wei follows the fee" in message
    assert "L1" not in message


def test_the_trigger_is_the_fee_not_the_price_ceiling(tmp_path):
    # The 1 gwei ceiling is only 2x a 0.5 gwei quote, but the fee pays 1000x the gas.
    fee = f"{{base_units: {1000 * 500_000 * 5 * 10**8}}}"
    [finding] = oversized(ethereum_policy(tmp_path, fee=fee, budget=None), 5 * 10**8)
    assert "is 1000x the executable fee" in finding["message"]
    # The ceiling is 1000x a 0.001 gwei quote, but the fee pays for 6x the gas at it.
    modest = ethereum_policy(tmp_path, fee=f"{{base_units: {6 * 500_000 * 10**6}}}")
    assert oversized(modest, 10**6) == []


def test_a_fee_within_the_factor_of_the_executable_fee_is_silent(tmp_path):
    policy = ethereum_policy(tmp_path)
    at_factor = FEE // (OVERSIZED_PRICE_FACTOR * 500_000)
    assert oversized(policy, at_factor) == []
    [finding] = oversized(policy, at_factor - 1)
    assert finding["code"] == "gas_ceiling_oversized"


def test_a_spike_inside_the_window_raises_the_bid(tmp_path):
    # A 0.01 gwei quote now, but the window's highest bid was 0.2 gwei: 15x, not 300x.
    policy = ethereum_policy(tmp_path)
    assert oversized(policy, 10**7, window=2 * 10**8) == []
    [finding] = oversized(policy, 10**7, window=5 * 10**7)
    assert "is 60x the executable fee" in finding["message"]
    assert "max_gas_limit 500000 x 50000000 wei" in finding["message"]


def test_an_oversized_warning_never_joins_a_too_low_one(tmp_path):
    # The fee pays 300x 500k gas at 0.01 gwei, but only 1M of the 2M gas limit.
    policy = ethereum_policy(tmp_path, limit=2_000_000, fee="{base_units: 10000000000000}")
    [finding] = oversized(policy, 10**7)
    assert finding["code"] == "gas_fee_quote"
    [low] = oversized(ethereum_policy(tmp_path), 2 * 10**9, window=2 * 10**9)
    assert low["code"] == "gas_price"


def test_the_op_stack_l1_fee_is_part_of_the_executable_fee(tmp_path):
    # Generous sizing on Base: 5x the execution and 5x a large L1 data fee bound.
    policy = resolved_policy(tmp_path)
    l1 = 2 * 10**14
    generous = significant_ceiling(5 * (500_000 * 6 * 10**6 + l1))
    sized = on_chain(resolved_policy(tmp_path, fee=f"{{base_units: {generous}}}"), "eip155:8453")
    assert generous > OVERSIZED_PRICE_FACTOR * 500_000 * 6 * 10**6
    assert oversized(sized, 6 * 10**6, l1=l1) == []
    # The 1.5e15 fee is 7x 3e12 + 2e14 with the bound, and 500x without it.
    assert oversized(policy, 6 * 10**6, l1=l1) == []
    [finding] = oversized(policy, 6 * 10**6, l1=10**12)
    message = finding["message"]
    executable = 500_000 * 6 * 10**6 + 10**12
    assert f"is {FEE // executable}x the executable fee {executable} wei" in message
    assert "+ 1000000000000 wei OP Stack L1 data and operator fee bound" in message
    suggested = significant_ceiling(5 * 500_000 * 6 * 10**6 + 5 * 10**12)
    assert f"max_transaction_fee {suggested} wei" in message
    assert "500000 x 6000000 wei x 5 + 5 x 1000000000000 wei L1" in message


def test_an_l1_fee_counts_only_on_op_stack_chains(tmp_path):
    policy = resolved_policy(tmp_path)
    for chain, op_stack in (("eip155:8453", True), ("eip155:10", True), ("eip155:4663", False)):
        findings = oversized(on_chain(policy, chain), 10**7, l1=10**12)
        assert ("L1 data and operator fee" in findings[0]["message"]) is op_stack, chain


def test_an_unreadable_l1_fee_skips_the_oversized_warning_on_op_stack_chains(tmp_path):
    from almanak.gateway.ethereum import DataError

    def refused(_):
        raise DataError("unsupported_fee_revision", "unverified OP fee contract revision")

    policy = resolved_policy(tmp_path)
    assert market_gas_warnings((policy,), lambda _: 10**7, lambda _: 10**7, refused) == []
    assert market_gas_warnings((policy,), lambda _: 10**7, lambda _: 10**7) == []


def test_an_unknown_chain_has_no_l1_fee(tmp_path):
    def unread(_):
        raise AssertionError("no L1 fee is read off OP Stack")

    policy = on_chain(resolved_policy(tmp_path), "eip155:999999")
    [finding] = market_gas_warnings((policy,), lambda _: 10**7, lambda _: 10**7, unread)
    assert "is 300x the executable fee" in finding["message"]


def test_a_failed_window_read_falls_back_to_the_single_block_quote(tmp_path):
    from almanak.gateway.ethereum import DataError

    def refused(_):
        raise DataError("provider", "block range too large")

    policy = ethereum_policy(tmp_path)
    [finding] = market_gas_warnings((policy,), lambda _: 10**7, refused)
    message = finding["message"]
    assert "the fee history window read failed: block range too large" in message
    assert f"single-block quote instead of the highest bid over the last {WINDOW_BLOCKS}" in message
    assert "is 300x the executable fee" in message
    [unread] = market_gas_warnings((policy,), lambda _: 10**7)
    assert "no fee history window was read" in unread["message"]


def test_a_suggestion_not_below_the_fee_is_not_made(tmp_path, monkeypatch):
    # A headroom above the factor would suggest raising the fee, never a warning's aim.
    monkeypatch.setattr("almanak.check_warnings.SUGGESTED_SPIKE_HEADROOM", 400)
    assert oversized(ethereum_policy(tmp_path), 10**7) == []


def test_a_suggestion_rounds_up_to_two_significant_digits():
    assert significant_ceiling(123_456) == 130_000
    assert significant_ceiling(120_000) == 120_000
    assert significant_ceiling(7) == 7


def test_a_failed_fee_quote_is_a_finding_never_a_refusal(tmp_path):
    from almanak.gateway.ethereum import DataError

    def refused(_):
        raise DataError("malformed", "fee history is incomplete or inconsistent")

    [finding] = market_gas_warnings((resolved_policy(tmp_path),), refused)
    assert finding["code"] == "gas_price"
    assert "not checked" in finding["message"] and "fee history" in finding["message"]


ROOT = Path(__file__).parents[1]
GAS_CODES = ("gas_limit", "gas_amount_units", *GAS_BOUNDS)


def reference_gas_configs() -> list[Path]:
    import yaml

    return [
        path
        for path in sorted((ROOT / "strategies").glob("*/config*.yaml"))
        if (yaml.safe_load(path.read_text()).get("strategy") or {}).get("gas")
    ]


# Avalanche gas blocks predating the consistency findings: 1000 gwei x 2M gas exceeds their
# 0.2 AVAX fee ceiling. Legacy snapshots, lifecycle fixtures and the fork rehearsal policy
# pin these values, so they are corrected separately; an entry must go once it is clean.
KNOWN_INCONSISTENT = frozenset(
    {
        "strategies/aave_v4_avalanche_borrowing/config.yaml",
        "strategies/aave_v4_multichain_borrowing/config.yaml",
        "strategies/uniswap_v4_avalanche_lp/config.yaml",
        "strategies/uniswap_v4_avalanche_trading/config.yaml",
    }
)


def test_every_reference_gas_policy_is_internally_consistent():
    # Authors copy reference gas blocks, so each must pass its own check.
    configs = reference_gas_configs()
    assert configs
    assert KNOWN_INCONSISTENT <= {str(path.relative_to(ROOT)) for path in configs}
    for path in configs:
        name = str(path.relative_to(ROOT))
        findings = [w for w in offline_warnings(read_inputs(path).inputs) if w["code"] in GAS_CODES]
        if name in KNOWN_INCONSISTENT:
            assert findings, f"{name} is consistent now; remove it from KNOWN_INCONSISTENT"
        else:
            assert findings == [], f"{name}: {findings}"


def test_every_reference_decision_interval_is_live_safe():
    # Agents copy reference intervals; a shorter one draws the live interval warning.
    import yaml

    from almanak.configuration.values import seconds

    configs = sorted((ROOT / "strategies").glob("*/config*.yaml"))
    assert len(configs) > 100
    short = {
        str(path.relative_to(ROOT)): interval
        for path in configs
        if (interval := seconds(yaml.safe_load(path.read_text())["strategy"]["decision_interval"]))
        < MIN_LIVE_DECISION_INTERVAL_SECONDS
    }
    assert short == {}


# Before these, the only references with gas were Avalanche and Ethereum, and
# an agent copied Avalanche's 1000 gwei x 2M gas onto Arbitrum. Each L2 keeps its own.
L2_GAS_REFERENCES = {
    "uniswap_v3_base_lp": "eip155:8453",
    "uniswap_v3_arbitrum_lp": "eip155:42161",
    "uniswap_v3_optimism_lp": "eip155:10",
}
# A few dollars of gas at any plausible ETH price.
MAX_L2_BUDGET_WEI = 2 * 10**15


def test_l2_references_carry_their_own_chain_gas_policy():
    import yaml

    for name, chain in L2_GAS_REFERENCES.items():
        path = ROOT / "strategies" / name / "config.yaml"
        [(key, policy)] = read_inputs(path).inputs.strategy.gas.items()
        assert str(policy.chain) == chain
        resolved = policy.resolve(key)
        assert resolved.max_gas_price_wei <= 10**9, name
        assert resolved.total_budget is not None
        assert resolved.total_budget.amount <= MAX_L2_BUDGET_WEI, name
        operator = yaml.safe_load(
            (ROOT / "examples/operators" / f"{name}.production.yaml").read_text()
        )
        [payer] = operator["production"]["payers"]
        assert payer["max_gas_price_wei"] == resolved.max_gas_price_wei
        assert payer["max_gas_limit"] == resolved.max_gas_limit
        assert payer["max_transaction_fee"]["amount"] == resolved.max_transaction_fee.amount
        assert payer["total_budget"]["amount"] == resolved.total_budget.amount


V4_CONFIG = """
strategy:
  decision_interval: 1h
  allocations:
    lp:
      market: pool
      account: fund
      kind: {kind}
      capital: {{asset: {usdc}, quantity: {quantity}}}{bps}{shape}
      hold_seconds: 1d
      slippage_bps: 100
      deadline_seconds: 300s
      minimum_native_balance: '0'
markets:
  pool:
    connector: uniswap_v4
    chain: eip155:8453
    currency0: eip155:8453/erc20:0x4200000000000000000000000000000000000006
    currency1: {usdc}
    fee: 3000
    tick_spacing: 60
    hooks: '0x0000000000000000000000000000000000000000'
accounts:
  fund:
    chain: eip155:8453
"""
V4_RANGE = """
      range: {lower: -887220, upper: 887220}
      collect_seconds: 6h"""
V4_BAND = """
      band: {lower: -887220, upper: 887220, width_bps: 500}
      collect_seconds: 6h"""


def v4_inputs(tmp_path, *, kind="liquidity", bps=None, quantity="'1000'", shape=V4_RANGE):
    path = tmp_path / "config.yaml"
    path.write_text(
        V4_CONFIG.format(
            kind=kind,
            usdc=USDC,
            quantity=quantity,
            bps="" if bps is None else f"\n      entry_swap_bps: {bps}",
            shape=shape if kind == "liquidity" else "",
        )
    )
    return read_inputs(path).inputs


def partial_entry(config) -> dict:
    [finding] = by_code(offline_warnings(config), "entry_budget_partial")
    return finding


def test_a_partial_v4_liquidity_entry_quote_warns_with_the_idle_capital(tmp_path):
    finding = partial_entry(v4_inputs(tmp_path, bps=5000))
    assert finding["field"] == "strategy.allocations.lp.entry_swap_bps"
    message = finding["message"]
    assert "sizes the Uniswap V4 liquidity entry quote from 50% of capital" in message
    assert "usual single entry swap then open_liquidity" in message
    assert f"about 500 {USDC} of capital is not swapped into the pair" in message
    assert "swaps repeatedly before minting is not limited" in message
    assert "Set entry_swap_bps to 10000" in message
    config = v4_inputs(tmp_path, bps=2500, quantity="{base_units: 1000000000}")
    assert f"750000000 base units of {USDC}" in partial_entry(config)["message"]


def test_the_idle_capital_rounds_like_the_runtime_entry_budget(tmp_path):
    # Execution sizes the budget as capital * bps // 10000 base units, so the idle
    # remainder rounds up; quoted decimals are not scaled offline and read as approximate.
    config = v4_inputs(tmp_path, bps=5000, quantity="{base_units: 1000001}")
    assert f" 500001 base units of {USDC}" in partial_entry(config)["message"]
    config = v4_inputs(tmp_path, bps=3333, quantity="{base_units: 7}")
    assert f" 5 base units of {USDC}" in partial_entry(config)["message"]
    config = v4_inputs(tmp_path, bps=3333, quantity="'1000.003'")
    assert f"about 666.7020001 {USDC}" in partial_entry(config)["message"]


def test_a_banded_v4_liquidity_allocation_is_silent(tmp_path):
    # A band recentres through rebalance_quote, sized against all inventory, and mints
    # all of it, so entry_swap_bps does not leave capital idle.
    config = v4_inputs(tmp_path, bps=5000, shape=V4_BAND)
    assert config.strategy.allocations["lp"].band is not None
    assert not by_code(offline_warnings(config), "entry_budget_partial")


def test_a_full_or_no_swap_v4_liquidity_entry_budget_is_silent(tmp_path):
    # Zero offers no entry quote: the allocation deposits without an entry swap.
    for bps in (None, 10000, 0):
        config = v4_inputs(tmp_path, bps=bps)
        assert not by_code(offline_warnings(config), "entry_budget_partial")


def test_a_v4_trading_entry_slice_is_silent(tmp_path):
    config = v4_inputs(tmp_path, kind="trading", bps=5000)
    assert not by_code(offline_warnings(config), "entry_budget_partial")


def test_a_v3_pool_liquidity_swap_share_is_silent(tmp_path):
    shape = "\n      range: {lower: -887220, upper: 887220}"
    config = inputs(tmp_path, shape=shape).model_copy(deep=True)
    position = config.strategy.allocations["position"]
    assert position.range is not None
    config.strategy.allocations["position"] = position.model_copy(update={"entry_swap_bps": 5000})
    assert not by_code(offline_warnings(config), "entry_budget_partial")


def test_shipped_uniswap_v4_references_deploy_all_capital():
    root = Path(__file__).parents[1] / "strategies"
    paths = [p for p in sorted(root.glob("*/config.yaml")) if "uniswap_v4" in p.read_text()]
    assert paths
    for path in paths:
        assert not by_code(offline_warnings(read_inputs(path).inputs), "entry_budget_partial")


def test_v4_editor_schema_states_the_author_input_description():
    import json

    from almanak.connectors.uniswap_v4.author_inputs import ENTRY_SWAP_BPS

    path = Path(__file__).parents[1] / "docs/schemas/positions/uniswap_v4.schema.json"
    field = json.loads(path.read_text())["properties"]["entry_swap_bps"]
    assert field["description"] == ENTRY_SWAP_BPS


NATIVE_GAS_CONFIG = """
strategy:
  decision_interval: 1h
  allocations:
    lp:
      market: pool
      account: fund
      range: {{lower: -198960, upper: -197760}}
      capital: {{asset: {capital}, quantity: {{base_units: 50000000000000000}}}}
      hold_seconds: 1d
      collect_seconds: 6h
      slippage_bps: 100
      deadline_seconds: 300s
      minimum_native_balance: {{base_units: 0}}
      kind: liquidity
  gas:
    base:
      accounts: [fund]
      chain: eip155:1
      max_transaction_fee: {{base_units: 1500000000000000}}
      exit_reserve: {{base_units: 0}}
markets:
  pool:
    connector: uniswap_v4
    chain: eip155:1
    currency0: {currency0}
    currency1: {currency1}
    fee: 3000
    tick_spacing: 60
accounts:
  fund:
    chain: eip155:1
"""
WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
ETH_USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


@pytest.mark.parametrize("native", [True, False])
def test_native_pool_gas_measurements_bound_only_strategies_that_use_a_native_pool(
    tmp_path, native
):
    currency0, currency1 = ("eip155:1/slip44:60", ETH_USDC) if native else (ETH_USDC, WETH)
    path = tmp_path / "config.yaml"
    path.write_text(
        NATIVE_GAS_CONFIG.format(capital=WETH, currency0=currency0, currency1=currency1)
    )
    found = by_code(offline_warnings(read_inputs(path).inputs), "gas_limit")
    if native:
        # A native mint adds the PositionManager's unwrap and wrap: above the 1.2M default.
        assert found and "mint" in found[0]["message"]
    else:
        assert not found


def test_trading_on_a_native_pool_keeps_the_plain_gas_measurements(tmp_path):
    from almanak.check_warnings import policy_variants

    path = tmp_path / "config.yaml"
    path.write_text(
        NATIVE_GAS_CONFIG.format(capital=WETH, currency0="eip155:1/slip44:60", currency1=ETH_USDC)
    )
    config = read_inputs(path).inputs
    assert policy_variants(config, ("fund",)) == {("uniswap_v4", "native")}
    allocation = config.strategy.allocations["lp"]
    config.strategy.allocations["lp"] = allocation.model_copy(update={"kind": "trading"})
    assert policy_variants(config, ("fund",)) == set()


def test_a_local_measurement_of_one_connector_never_hides_another_connectors_native_action():
    from almanak.caip import ChainId
    from almanak.check_warnings import largest_reference, measured_gas

    base = ChainId("eip155:8453")
    plain = largest_reference({"uniswap_v3", "uniswap_v4"}, base)
    native = largest_reference({"uniswap_v3", "uniswap_v4"}, base, {("uniswap_v4", "native")})
    assert native.get("variant") == "native" and native["action"] == "mint"
    assert measured_gas(native) > measured_gas(plain)
    # A native variant of another connector leaves a plain V4 market on its plain rows.
    elsewhere = largest_reference({"uniswap_v4"}, base, {("uniswap_v3", "native")})
    assert elsewhere.get("variant") is None


def test_many_entry_slices_warn_with_the_fees_they_add_to_the_entry_minimum(tmp_path):
    config = inputs(tmp_path)
    assert not by_code(offline_warnings(config), "gas_entry_slices")
    path = tmp_path / "config.yaml"
    path.write_text(path.read_text().replace("entry_swap_bps: 10000", "entry_swap_bps: 1000"))
    # Ten slices are the limit; one more is flagged.
    assert not by_code(offline_warnings(read_inputs(path).inputs), "gas_entry_slices")
    path.write_text(path.read_text().replace("entry_swap_bps: 1000", "entry_swap_bps: 300"))
    [finding] = by_code(offline_warnings(read_inputs(path).inputs), "gas_entry_slices")
    assert finding["field"] == "strategy.allocations.position.entry_swap_bps"
    message = finding["message"]
    assert "entry_swap_bps 300 enters in at least 34 slices" in message
    assert f"= {34 * FEE} wei" in message and "strategy.gas.base" in message


ROBINHOOD_NATIVE_LP = """
strategy:
  decision_interval: 60s
  allocations:
    position:
      market: pool
      account: fund
      band: {{width_bps: 500}}
      capital: {{asset: {usdg}, quantity: '1000'}}
      slippage_bps: 100
      deadline_seconds: 300
      minimum_native_balance: {{base_units: 0}}
      kind: liquidity
  gas:
    fund_payer:
      accounts: [fund]
      chain: eip155:4663
      max_gas_limit: {limit}
      max_transaction_fee: {{base_units: 200000000000000}}
      exit_reserve: {{base_units: 0}}
markets:
  pool:
    connector: uniswap_v4
    chain: eip155:4663
    currency0: eip155:4663/slip44:60
    currency1: {usdg}
    fee: 500
    tick_spacing: 10
    hooks: '0x0000000000000000000000000000000000000000'
accounts:
  fund:
    chain: eip155:4663
"""
USDG = "eip155:4663/erc20:0x5fc5360d0400a0fd4f2af552add042d716f1d168"
# The fork gate refused a native ETH/USDG mint at max_gas_limit 1,375,684, the floor
# `check` once recommended: its buffered eth_estimateGas was 1,592,333 (rig m_rh_v4lp).
ROBINHOOD_NATIVE_MINT_GATE = 1_592_333


def robinhood_native_findings(tmp_path, limit):
    path = tmp_path / "config.yaml"
    path.write_text(ROBINHOOD_NATIVE_LP.format(usdg=USDG, limit=limit))
    return by_code(offline_warnings(read_inputs(path).inputs), "gas_limit")


def test_a_native_mint_floor_covers_its_refund_inflated_estimate(tmp_path):
    [finding] = robinhood_native_findings(tmp_path, 1_375_684)
    message = finding["message"]
    floor = int(message.rsplit("at least ", 1)[1].rstrip("."))
    assert floor >= ROBINHOOD_NATIVE_MINT_GATE
    assert "used 1100547; its estimate includes a storage refund" in message
    # The recommended floor, and the 1.8M both rig arms then passed with, are silent.
    assert not robinhood_native_findings(tmp_path, floor)
    assert not robinhood_native_findings(tmp_path, 1_800_000)


def test_a_measured_estimate_or_a_plain_row_keeps_its_own_gas():
    from almanak.check_warnings import executable_gas

    native = {"variant": "native", "gas_used": 1_000_000}
    assert executable_gas(native) == 1_250_000
    assert executable_gas(native | {"gas_estimate": 1_100_000}) == 1_100_000
    assert executable_gas({"gas_used": 1_000_000}) == 1_000_000
