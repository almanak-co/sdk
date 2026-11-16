"""Decoded offline regressions for pristine BNB Pancake authority probes."""

import json
from pathlib import Path
from types import SimpleNamespace

from eth_abi import decode

from almanak.caip import AccountReference
from almanak.configuration.composed import Author as ComposedAuthor
from almanak.connectors.composed.fork import ComposedFork
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.pancakeswap_v3 import abi
from almanak.connectors.pancakeswap_v3.fork import PancakeFork
from almanak.models import Account, AccountBinding, Chain, Eoa, SafeRoles, StrategyAccounts
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import frozen_effective, harness_directory
from tests.strategy_fork import pancake_bnb_authority as authority
from tests.strategy_fork.bnb_v3_authority import lending_permissions
from tests.test_bnb_v3_pristine_authority import (
    bind_node,
    decode_approval,
    lending_policy,
    selector,
    unwrap,
)

ROOT = Path(__file__).parents[1]
INSTANCE = "0x" + "00" * 32


def safe_config(reference):
    if reference == "bnb_aave_pancake_borrow_and_trade":
        author = ComposedAuthor.model_validate_json(
            json.dumps(frozen_effective(reference, "config.yaml"))
        )
    else:
        author = read_author(harness_directory(reference) / "config.yaml")
    fork = author.fork_config()
    chain = Chain(chain_id=fork.origin.chain.chain_id, environment="anvil:" + INSTANCE)

    def account(number):
        return Account(chain=chain, address=AccountReference(f"0x{number:040x}"))

    profile = SafeRoles(
        kind="safe_roles",
        fund=account(1),
        operator=account(2),
        roles_module=account(3).address,
        role_key="0x" + "11" * 32,
    )
    bindings = (AccountBinding(name="fund", profile=profile),)
    factory = ComposedFork if fork.connector is None else PancakeFork
    return factory.prepare(fork, bindings, chain, INSTANCE, fork.origin)


def decode_swap(inner):
    assert inner[:4] == selector(f"exactInputSingle({abi.SWAP_PARAMS})")
    (values,) = decode((abi.SWAP_PARAMS,), inner[4:])
    return tuple(value.lower() if isinstance(value, str) else value for value in values)


def decode_tuple(inner, signature, type_):
    assert inner[:4] == selector(f"{signature}({type_})")
    (values,) = decode((type_,), inner[4:])
    return tuple(value.lower() if isinstance(value, str) else value for value in values)


def decode_exit(inner):
    calls = decode_multicall(inner)
    assert len(calls) in {2, 3}
    decrease = decode_tuple(calls[0], "decreaseLiquidity", abi.DECREASE_PARAMS)
    collect = decode_tuple(calls[1], "collect", abi.COLLECT_PARAMS)
    burn = None
    if len(calls) == 3:
        assert calls[2][:4] == selector("burn(uint256)")
        (burn,) = decode(("uint256",), calls[2][4:])
    return decrease, collect, burn


def decode_multicall(inner):
    assert inner[:4] == selector("multicall(bytes[])")
    (calls,) = decode(("bytes[]",), inner[4:])
    return calls


def exchange_policy(config, allocation_name):
    target = config.target(allocation_name)
    profile = config.accounts.binding(target.account).profile
    swaps = {authority._swap_values(target, direction) for direction in (0, 1)}
    # Ticks and deposit amounts are gateway checks (blueprint 27 section 4.3): the policy
    # binds the pool, fee, recipient and deadline only.
    scoped_mint = (
        authority._mint(target, (1, 1))[0][:3] + authority._mint(target, (1, 1))[0][7:]
        if target.range is not None
        else None
    )

    def policy(call):
        destination, value, inner, operation, entrypoint = unwrap(call, profile)
        if value or operation:
            return False
        if destination in {asset.reference for asset in target.assets}:
            spender, amount = decode_approval(inner)
            spenders = {target.contracts.router}
            if target.range is not None:
                spenders.add(target.contracts.position_manager)
            # Scope-only approvals (blueprint 27): the exact spender with any amount.
            return entrypoint == "direct" and spender in spenders and amount > 0
        if destination == target.contracts.router:
            return entrypoint == "return_data" and decode_swap(inner) in swaps
        if target.range is None or destination != target.contracts.position_manager:
            return False
        if inner[:4] == selector(f"mint({abi.MINT_PARAMS})"):
            values = decode_tuple(inner, "mint", abi.MINT_PARAMS)
            return entrypoint == "return_data" and values[:3] + values[7:] == scoped_mint
        if inner[:4] == selector(f"increaseLiquidity({abi.INCREASE_PARAMS})"):
            return entrypoint == "return_data" and decode_tuple(
                inner, "increaseLiquidity", abi.INCREASE_PARAMS
            ) == (2**200, 1, 1, 0, 0, 2**64)
        if inner[:4] == selector(f"collect({abi.COLLECT_PARAMS})"):
            values = decode_tuple(inner, "collect", abi.COLLECT_PARAMS)
            return entrypoint == "return_data" and values[1:] == (
                target.fund.address,
                abi.MAX128,
                abi.MAX128,
            )
        if inner[:4] == selector("multicall(bytes[])"):
            try:
                decrease, collect, _ = decode_exit(inner)
            except AssertionError:
                return False
            return (
                entrypoint == "return_data"
                and decrease[1:] == (1, 0, 0, 2**64)
                and collect[1:] == (target.fund.address, abi.MAX128, abi.MAX128)
            )
        return False

    return profile, policy


def exchange_node(config, allocation_name):
    target = config.target(allocation_name)
    profile, policy = exchange_policy(config, allocation_name)
    return bind_node(
        profile,
        policy,
        {asset.reference for asset in target.assets}
        | {target.contracts.router, target.contracts.position_manager},
    )


def assert_swap_controls(config, allocation_name, result):
    target = config.target(allocation_name)
    profile = config.accounts.binding(target.account).profile
    controls = {item["case"]: item for item in result["allowed_controls"]}
    for direction, asset in enumerate(target.assets):
        for label, amount in (("", 1), ("any_amount_", 2**256 - 1)):
            approval = controls[f"router_approval_{label}{direction}"]
            destination, _, inner, _, entrypoint = unwrap(approval["call"], profile)
            assert (destination, entrypoint, decode_approval(inner)) == (
                asset.reference,
                "direct",
                (target.contracts.router, amount),
            )
        swap = controls[f"configured_swap{direction}_template"]
        assert swap["status"] == "authorized_inner_refusal"
        destination, _, inner, _, entrypoint = unwrap(swap["call"], profile)
        assert (destination, entrypoint, decode_swap(inner)) == (
            target.contracts.router,
            "return_data",
            authority._swap_values(target, direction),
        )
    rejected = {item["case"]: item for item in result["rejected"]}
    for direction in range(len(target.assets)):
        baseline = authority._swap_values(target, direction)
        changes = {
            "other_input": (0, profile.operator.address),
            "other_output": (1, profile.operator.address),
            "other_fee": (2, target.pool.fee + 1),
            "other_recipient": (3, profile.operator.address),
            "zero_input": (5, 0),
            "price_limit": (7, 1),
        }
        for suffix, (index, replacement) in changes.items():
            _, _, inner, _, _ = unwrap(rejected[f"swap{direction}_{suffix}"]["call"], profile)
            changed = list(baseline)
            changed[index] = replacement
            assert decode_swap(inner) == tuple(changed)
        _, _, inner, _, _ = unwrap(
            rejected[f"router_approval_other_spender_{direction}"]["call"], profile
        )
        assert decode_approval(inner) == (profile.operator.address, 1)
    assert not any("above" in label for label in rejected)


def test_actual_trading_reference_probes_both_direct_swap_directions():
    config = safe_config("pancakeswap_v3_bnb_trading")
    result = authority.exchange_permissions(exchange_node(config, "position"), config, "position")
    assert result["stage"] == "before_runtime_entry"
    assert_swap_controls(config, "position", result)


def test_actual_lp_reference_covers_mint_and_broad_exit_shapes():
    config = safe_config("pancakeswap_v3_bnb_lp")
    target = config.target("position")
    profile = config.accounts.binding(target.account).profile
    result = authority.exchange_permissions(exchange_node(config, "position"), config, "position")
    assert_swap_controls(config, "position", result)
    liquidity = result["liquidity"]
    controls = {item["case"]: item for item in liquidity["allowed_controls"]}
    for index, asset in enumerate(target.assets):
        for label, amount in (("one", 1), ("any_amount", 2**256 - 1)):
            approval = controls[f"position_manager_approval_{index}_{label}"]
            destination, _, inner, _, entrypoint = unwrap(approval["call"], profile)
            assert (destination, entrypoint, decode_approval(inner)) == (
                asset.reference,
                "direct",
                (target.contracts.position_manager, amount),
            )
    for label, amounts in (
        ("configured_mint_template", (1, 1)),
        ("configured_mint_zero_amount0_template", (0, 1)),
        ("configured_mint_zero_amount1_template", (1, 0)),
        ("mint_large_deposit_template", (2**128 - 1, 2**128 - 1)),
    ):
        assert controls[label]["status"] == "authorized_inner_refusal"
        _, _, inner, _, entrypoint = unwrap(controls[label]["call"], profile)
        assert entrypoint == "return_data"
        assert decode_tuple(inner, "mint", abi.MINT_PARAMS)[5:7] == amounts
    _, _, inner, _, _ = unwrap(controls["mint_other_range_template"]["call"], profile)
    spacing = target.pool.tick_spacing
    assert decode_tuple(inner, "mint", abi.MINT_PARAMS)[3:5] == (
        target.range.lower + spacing,
        target.range.upper + spacing,
    )
    _, _, inner, _, _ = unwrap(controls["broad_arbitrary_nft_increase_template"]["call"], profile)
    assert decode_tuple(inner, "increaseLiquidity", abi.INCREASE_PARAMS) == (
        2**200,
        1,
        1,
        0,
        0,
        2**64,
    )
    _, _, inner, _, _ = unwrap(controls["broad_arbitrary_nft_collect_template"]["call"], profile)
    assert decode_tuple(inner, "collect", abi.COLLECT_PARAMS) == (
        2**200,
        target.fund.address,
        abi.MAX128,
        abi.MAX128,
    )
    _, _, inner, _, _ = unwrap(
        controls["broad_arbitrary_nft_partial_decrease_template"]["call"], profile
    )
    assert decode_exit(inner) == (
        (2**200, 1, 0, 0, 2**64),
        (2**200, target.fund.address, abi.MAX128, abi.MAX128),
        None,
    )
    _, _, inner, _, _ = unwrap(controls["broad_arbitrary_nft_close_template"]["call"], profile)
    assert decode_exit(inner)[2] == 2**200
    _, _, inner, _, _ = unwrap(controls["broad_uncorrelated_nft_close_template"]["call"], profile)
    decrease, collect, burn = decode_exit(inner)
    assert decrease[0] == collect[0] == 2**200 and burn == 2**200 + 1
    rejected = {item["case"]: item for item in liquidity["rejected"]}
    assert {"close_wrong_order", "close_extra_call", "collect_other_recipient"} <= rejected.keys()
    baseline_mint = authority._mint(target, (1, 1))[0]
    for label in ("mint_other_asset", "mint_other_fee", "mint_other_recipient"):
        _, _, inner, _, _ = unwrap(rejected[label]["call"], profile)
        assert decode_tuple(inner, "mint", abi.MINT_PARAMS) != baseline_mint
    assert not any("above" in label or "range" in label for label in rejected)
    _, _, inner, _, _ = unwrap(rejected["collect_other_recipient"]["call"], profile)
    assert decode_tuple(inner, "collect", abi.COLLECT_PARAMS)[1] == profile.operator.address
    _, _, inner, _, _ = unwrap(rejected["close_wrong_order"]["call"], profile)
    wrong_order = decode_multicall(inner)
    assert tuple(call[:4] for call in wrong_order) == (
        selector(f"collect({abi.COLLECT_PARAMS})"),
        selector(f"decreaseLiquidity({abi.DECREASE_PARAMS})"),
        selector("burn(uint256)"),
    )
    _, _, inner, _, _ = unwrap(rejected["close_extra_call"]["call"], profile)
    assert len(decode_multicall(inner)) == 4


def test_actual_composed_reference_probes_lending_and_both_pancake_directions():
    composed = safe_config("bnb_aave_pancake_borrow_and_trade")
    lending = composed.component(ConnectorId("aave_v3"))
    exchange = composed.component(ConnectorId("pancakeswap_v3"))
    profile, policy = lending_policy(lending)
    targets = {
        lending.allocation("collateral").reserve.pool,
        *(allocation.reserve.asset.reference for allocation in lending.allocations),
    }
    lending_result = lending_permissions(bind_node(profile, policy, targets), lending)
    exchange_result = authority.exchange_permissions(
        exchange_node(exchange, "trade"), exchange, "trade"
    )
    assert {item["case"] for item in lending_result["allowed_controls"]} >= {
        "configured_supply_template",
        "configured_borrow_template",
        "configured_repay_template",
        "configured_withdraw_template",
    }
    assert_swap_controls(exchange, "trade", exchange_result)


def test_lp_hold_separates_gateway_ownership_and_eoa_non_applicability(monkeypatch, tmp_path):
    config = safe_config("pancakeswap_v3_bnb_lp")
    target = config.target("position")
    owned = SimpleNamespace(target=target)
    state = SimpleNamespace(config=config, owned=(owned,))
    journal = SimpleNamespace(path=tmp_path / "state.sqlite", load=lambda: state)
    process = SimpleNamespace(poll=lambda: None)
    monkeypatch.setattr(
        authority,
        "foreign_nft_gateway_refusal",
        lambda actual, value: {
            "status": "rejected",
            "same_state": actual is state and value is owned,
        },
    )
    authority.retain_lp_gateway_ownership(None, journal, process, "position")
    binding = config.accounts.binding("fund")
    accounts = StrategyAccounts(
        bindings=(
            binding.model_copy(update={"profile": Eoa(kind="eoa", fund=binding.profile.fund)}),
        )
    )
    state.config = config.model_copy(update={"accounts": accounts})
    authority.retain_lp_gateway_ownership(None, journal, process, "position")
    observations = json.loads((tmp_path / "permission-budget-observations.json").read_text())
    assert observations[0]["gateway_foreign_nft"]["same_state"] is True
    assert observations[0]["quota_claim"].startswith("none")
    assert observations[1]["status"] == "not_applicable"
