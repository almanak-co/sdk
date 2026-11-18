"""Decoded offline regressions for pristine BNB V3 authority probes."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.caip import AccountReference
from almanak.connectors.composed.fork import ComposedFork
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3 import abi
from almanak.connectors.uniswap_v3.fork import UniswapFork
from almanak.models import Account, AccountBinding, Chain, Eoa, SafeRoles, StrategyAccounts
from scripts.legacy_configuration.author import read_author
from tests.fork.test_bnb_v3_reference_authority import _verified_partitions
from tests.strategy_fork import bnb_v3_authority as authority
from tests.strategy_fork.robinhood_v3_authority import (
    ROLES_CONDITION_VIOLATION,
    RPC_ERROR_PREFIX,
)

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures/author/candidates"
INSTANCE = "0x" + "00" * 32


def condition_error(status):
    data = ROLES_CONDITION_VIOLATION + encode(("uint8", "bytes32"), (status, bytes(32))).hex()
    return RuntimeError(
        RPC_ERROR_PREFIX + repr({"code": 3, "message": "execution reverted", "data": data})
    )


def selector(signature):
    return keccak(text=signature)[:4]


def safe_config(reference):
    fork = read_author(FIXTURES / reference / "config.yaml").fork_config()
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
    factory = ComposedFork if fork.connector is None else UniswapFork
    return factory.prepare(fork, bindings, chain, INSTANCE, fork.origin)


def unwrap(call, profile):
    assert call["from"] == profile.operator.address
    assert call["to"] == profile.roles_module
    payload = bytes.fromhex(call["data"][2:])
    direct = "execTransactionWithRole(address,uint256,bytes,uint8,bytes32,bool)"
    returning = "execTransactionWithRoleReturnData(address,uint256,bytes,uint8,bytes32,bool)"
    if payload[:4] == selector(direct):
        entrypoint = "direct"
    elif payload[:4] == selector(returning):
        entrypoint = "return_data"
    else:
        raise AssertionError("unexpected Roles entrypoint")
    target, value, inner, operation, role, should_revert = decode(
        ("address", "uint256", "bytes", "uint8", "bytes32", "bool"), payload[4:]
    )
    assert target == target.lower()
    assert role == bytes.fromhex(profile.role_key[2:])
    assert should_revert is (entrypoint == "direct")
    return target, value, inner, operation, entrypoint


def decode_approval(inner):
    assert inner[:4] == selector("approve(address,uint256)")
    return decode(("address", "uint256"), inner[4:])


def decode_swap(inner):
    assert inner[:4] == selector("multicall(uint256,bytes[])")
    deadline, calls = decode(("uint256", "bytes[]"), inner[4:])
    decoded = []
    for call in calls:
        assert call[:4] == selector(f"exactInputSingle({abi.SWAP_PARAMS})")
        (values,) = decode((abi.SWAP_PARAMS,), call[4:])
        decoded.append(
            tuple(value.lower() if isinstance(value, str) else value for value in values)
        )
    return deadline, tuple(decoded)


def decode_mint(inner):
    assert inner[:4] == selector(f"mint({abi.MINT_PARAMS})")
    (values,) = decode((abi.MINT_PARAMS,), inner[4:])
    return tuple(value.lower() if isinstance(value, str) else value for value in values)


def decode_collect(inner):
    assert inner[:4] == selector(f"collect({abi.COLLECT_PARAMS})")
    (values,) = decode((abi.COLLECT_PARAMS,), inner[4:])
    return tuple(value.lower() if isinstance(value, str) else value for value in values)


def decode_increase(inner):
    assert inner[:4] == selector(f"increaseLiquidity({abi.INCREASE_PARAMS})")
    (values,) = decode((abi.INCREASE_PARAMS,), inner[4:])
    return tuple(values)


def decode_multicall(inner):
    assert inner[:4] == selector("multicall(bytes[])")
    (calls,) = decode(("bytes[]",), inner[4:])
    return calls


def decode_exit(inner):
    calls = decode_multicall(inner)
    assert len(calls) in {2, 3}
    assert calls[0][:4] == selector(f"decreaseLiquidity({abi.DECREASE_PARAMS})")
    assert calls[1][:4] == selector(f"collect({abi.COLLECT_PARAMS})")
    (decrease,) = decode((abi.DECREASE_PARAMS,), calls[0][4:])
    burn = None
    if len(calls) == 3:
        assert calls[2][:4] == selector("burn(uint256)")
        (burn,) = decode(("uint256",), calls[2][4:])
    return tuple(decrease), decode_collect(calls[1]), burn


def decode_call(inner, signature, types):
    assert inner[:4] == selector(signature)
    return decode(types, inner[4:])


class PolicyNode:
    def __init__(self, policy, allowed_targets):
        self.policy = policy
        self.allowed_targets = allowed_targets
        self.calls = []

    def _validate_origin(self):
        pass

    def rpc(self, method, params):
        assert method == "eth_call" and params[1] == "latest"
        call = params[0]
        self.calls.append(call)
        if not self.policy(call):
            destination, value, _, operation, _ = self.decoded(call)
            status = (
                1
                if operation
                else 4
                if value
                else 2
                if destination not in self.allowed_targets
                else 7
            )
            raise condition_error(status)
        _, _, _, _, entrypoint = self.decoded(call)
        if entrypoint == "return_data":
            return "0x" + encode(("bool", "bytes"), (False, b"\xde\xad\xbe\xef")).hex()
        return "0x"

    def decoded(self, call):
        raise AssertionError("test must bind the profile-aware decoder")


def exchange_policy(config, allocation_name):
    target = config.target(allocation_name)
    profile = config.accounts.binding(target.account).profile
    valid_swap = authority._swap_values(target, 1)
    valid_mint = (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        target.range.lower if target.range else None,
        target.range.upper if target.range else None,
        1,
        1,
        0,
        0,
        target.fund.address,
        2**64,
    )

    def policy(call):
        destination, value, inner, operation, entrypoint = unwrap(call, profile)
        if value or operation:
            return False
        if destination in {asset.reference for asset in target.assets}:
            spender, amount = decode_approval(inner)
            valid_spenders = {target.contracts.position_manager}
            if destination == target.funding_currency.reference:
                valid_spenders.add(target.contracts.router)
            # Scope-only approvals (blueprint 27): the exact spender with any amount.
            return entrypoint == "direct" and spender in valid_spenders and amount > 0
        if destination == target.contracts.router:
            return entrypoint == "return_data" and decode_swap(inner) == (
                2**64,
                (valid_swap,),
            )
        if target.range is None or destination != target.contracts.position_manager:
            return False
        if inner[:4] == selector(f"mint({abi.MINT_PARAMS})"):
            # Ticks and deposit amounts are gateway checks (blueprint 27 section 4.3).
            values = decode_mint(inner)
            scope = values[:3] + values[7:]
            return entrypoint == "return_data" and scope == valid_mint[:3] + valid_mint[7:]
        if inner[:4] == selector(f"increaseLiquidity({abi.INCREASE_PARAMS})"):
            return entrypoint == "return_data" and decode_increase(inner) == (
                2**200,
                1,
                1,
                0,
                0,
                2**64,
            )
        if inner[:4] == selector(f"collect({abi.COLLECT_PARAMS})"):
            values = decode_collect(inner)
            return entrypoint == "return_data" and values[1:] == (
                target.fund.address,
                abi.MAX128,
                abi.MAX128,
            )
        if inner[:4] == selector("multicall(bytes[])"):
            try:
                decrease, collect, burn = decode_exit(inner)
            except AssertionError:
                return False
            return (
                entrypoint == "return_data"
                and decrease == (2**200, 1, 0, 0, 2**64)
                and collect == (2**200, target.fund.address, abi.MAX128, abi.MAX128)
                and burn in {None, 2**200}
            )
        return False

    return profile, policy


def bind_node(profile, policy, allowed_targets):
    node = PolicyNode(policy, allowed_targets)
    node.decoded = lambda call: unwrap(call, profile)
    return node


def test_pristine_trading_calls_bind_every_configured_swap_field():
    config = safe_config("uniswap_v3_bnb_trading")
    target = config.target("position")
    profile, policy = exchange_policy(config, "position")
    node = bind_node(
        profile,
        policy,
        {asset.reference for asset in target.assets}
        | {target.contracts.router, target.contracts.position_manager},
    )

    result = authority.exchange_permissions(node, config, "position")

    assert result["stage"] == "before_runtime_entry"
    controls = {item["case"]: item for item in result["allowed_controls"]}
    assert controls["configured_swap_template"]["status"] == "authorized_inner_refusal"
    assert controls["configured_swap_template"]["inner_success"] is False
    for label, amount in (("router_approval", 1), ("router_approval_any_amount", 2**256 - 1)):
        destination, _, inner, _, entrypoint = unwrap(controls[label]["call"], profile)
        assert (destination, entrypoint, decode_approval(inner)) == (
            target.funding_currency.reference,
            "direct",
            (target.contracts.router, amount),
        )
    expected = authority._swap_values(target, 1)
    destination, _, inner, _, entrypoint = unwrap(
        controls["configured_swap_template"]["call"], profile
    )
    assert (destination, entrypoint, decode_swap(inner)) == (
        target.contracts.router,
        "return_data",
        (2**64, (expected,)),
    )
    assert_swap_rejections(config, result)


def assert_swap_rejections(config, result):
    target = config.target("position" if config.connector else "trade")
    profile = config.accounts.binding(target.account).profile
    baseline = authority._swap_values(target, 1)
    rejected = {item["case"]: item["call"] for item in result["rejected"]}
    assert {item["condition_status"] for item in result["rejected"]} <= authority.PREDICATE_STATUSES
    changes = {
        "swap_other_input": (0, target.counter_currency.reference),
        "swap_other_output": (1, target.funding_currency.reference),
        "swap_other_fee": (2, target.pool.fee + 1),
        "swap_other_recipient": (3, profile.submitter.address),
        "swap_zero_input": (4, 0),
        "swap_price_limit": (6, 1),
    }
    for label, (index, replacement) in changes.items():
        destination, _, inner, _, entrypoint = unwrap(rejected[label], profile)
        changed = list(baseline)
        changed[index] = replacement
        assert (destination, entrypoint, decode_swap(inner)) == (
            target.contracts.router,
            "direct",
            (2**64, (tuple(changed),)),
        )
    _, _, inner, _, _ = unwrap(rejected["swap_extra_nested_call"], profile)
    assert decode_swap(inner) == (2**64, (baseline, baseline))
    _, _, inner, _, _ = unwrap(rejected["router_approval_other_spender"], profile)
    assert decode_approval(inner) == (profile.submitter.address, 1)
    assert not any("above" in label for label in rejected)


def test_pristine_lp_separates_mint_predicates_from_broad_nft_scope():
    config = safe_config("uniswap_v3_bnb_lp")
    target = config.target("position")
    profile, policy = exchange_policy(config, "position")
    node = bind_node(
        profile,
        policy,
        {asset.reference for asset in target.assets}
        | {target.contracts.router, target.contracts.position_manager},
    )

    result = authority.exchange_permissions(node, config, "position")

    assert_swap_rejections(config, result)
    liquidity = result["liquidity"]
    controls = {item["case"]: item for item in liquidity["allowed_controls"]}
    for index, asset in enumerate(target.assets):
        for label, amount in (("one", 1), ("any_amount", 2**256 - 1)):
            approval = controls[f"position_manager_approval_{index}_{label}"]
            assert approval["status"] == "executed"
            destination, _, inner, _, entrypoint = unwrap(approval["call"], profile)
            assert (destination, entrypoint, decode_approval(inner)) == (
                asset.reference,
                "direct",
                (target.contracts.position_manager, amount),
            )
    _, _, inner, _, entrypoint = unwrap(controls["configured_mint_template"]["call"], profile)
    mint = decode_mint(inner)
    assert entrypoint == "return_data"
    assert mint[:5] == (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        target.range.lower,
        target.range.upper,
    )
    assert mint[5:] == (1, 1, 0, 0, target.fund.address, 2**64)
    for label, amounts in (
        ("configured_mint_zero_amount0_template", (0, 1)),
        ("configured_mint_zero_amount1_template", (1, 0)),
    ):
        assert controls[label]["status"] == "authorized_inner_refusal"
        _, _, inner, _, entrypoint = unwrap(controls[label]["call"], profile)
        assert entrypoint == "return_data"
        assert decode_mint(inner)[5:7] == amounts
    # Another aligned range and a uint128 deposit pass Roles; the gateway refuses them.
    spacing = target.pool.tick_spacing
    _, _, inner, _, _ = unwrap(controls["mint_other_range_template"]["call"], profile)
    assert decode_mint(inner)[3:5] == (target.range.lower + spacing, target.range.upper + spacing)
    _, _, inner, _, _ = unwrap(controls["mint_large_deposit_template"]["call"], profile)
    assert decode_mint(inner)[5:7] == (2**128 - 1, 2**128 - 1)
    _, _, inner, _, entrypoint = unwrap(
        controls["broad_arbitrary_nft_increase_template"]["call"], profile
    )
    assert entrypoint == "return_data" and decode_increase(inner) == (
        2**200,
        1,
        1,
        0,
        0,
        2**64,
    )
    _, _, inner, _, entrypoint = unwrap(
        controls["broad_arbitrary_nft_collect_template"]["call"], profile
    )
    collect = decode_collect(inner)
    assert entrypoint == "return_data" and collect == (
        2**200,
        target.fund.address,
        abi.MAX128,
        abi.MAX128,
    )
    _, _, inner, _, entrypoint = unwrap(
        controls["broad_arbitrary_nft_partial_decrease_template"]["call"], profile
    )
    assert entrypoint == "return_data" and decode_exit(inner) == (
        (2**200, 1, 0, 0, 2**64),
        (2**200, target.fund.address, abi.MAX128, abi.MAX128),
        None,
    )
    _, _, inner, _, entrypoint = unwrap(
        controls["broad_arbitrary_nft_close_template"]["call"], profile
    )
    assert entrypoint == "return_data" and decode_exit(inner) == (
        (2**200, 1, 0, 0, 2**64),
        (2**200, target.fund.address, abi.MAX128, abi.MAX128),
        2**200,
    )
    rejected = {item["case"]: item["call"] for item in liquidity["rejected"]}
    expected_labels = {
        "mint_other_asset",
        "mint_other_fee",
        "mint_other_recipient",
        "collect_other_recipient",
        "position_manager_approval_other_spender_0",
        "position_manager_approval_other_spender_1",
        "close_wrong_order",
    }
    assert set(rejected) == expected_labels
    for label in {"mint_other_asset", "mint_other_fee", "mint_other_recipient"}:
        _, _, malformed, _, _ = unwrap(rejected[label], profile)
        assert decode_mint(malformed) != mint
    _, _, malformed, _, _ = unwrap(rejected["collect_other_recipient"], profile)
    assert decode_collect(malformed)[1] == profile.submitter.address
    _, _, malformed, _, _ = unwrap(rejected["close_wrong_order"], profile)
    calls = decode_multicall(malformed)
    assert tuple(call[:4] for call in calls) == (
        selector(f"collect({abi.COLLECT_PARAMS})"),
        selector(f"decreaseLiquidity({abi.DECREASE_PARAMS})"),
        selector("burn(uint256)"),
    )
    for index, asset in enumerate(target.assets):
        destination, _, inner, _, _ = unwrap(
            rejected[f"position_manager_approval_other_spender_{index}"], profile
        )
        assert (destination, decode_approval(inner)) == (
            asset.reference,
            (profile.submitter.address, 1),
        )


def lending_policy(config):
    collateral = config.allocation("collateral")
    loan = config.allocation("loan")
    profile = config.accounts.binding(collateral.account).profile
    valid = {inner for _, _, inner in authority._lending_control_calls(config)}
    approval_assets = {collateral.reserve.asset.reference, loan.reserve.asset.reference}

    def policy(call):
        destination, value, inner, operation, entrypoint = unwrap(call, profile)
        if value or operation:
            return False
        if destination in approval_assets:
            return entrypoint == "direct" and decode_approval(inner) == (
                collateral.reserve.pool,
                1,
            )
        return (
            destination == collateral.reserve.pool
            and entrypoint == "return_data"
            and "0x" + inner.hex() in valid
        )

    return profile, policy


def test_composed_pristine_lending_and_swap_controls_use_real_components():
    composed = safe_config("bnb_aave_borrow_and_trade_v3")
    lending = composed.component(ConnectorId("aave_v3"))
    exchange = composed.component(ConnectorId("uniswap_v3"))
    profile, policy = lending_policy(lending)
    lending_targets = {
        lending.allocation("collateral").reserve.pool,
        *(allocation.reserve.asset.reference for allocation in lending.allocations),
    }
    lending_result = authority.lending_permissions(
        bind_node(profile, policy, lending_targets), lending
    )
    exchange_profile, exchange_check = exchange_policy(exchange, "trade")
    exchange_result = authority.exchange_permissions(
        bind_node(
            exchange_profile,
            exchange_check,
            {asset.reference for asset in exchange.target("trade").assets}
            | {
                exchange.target("trade").contracts.router,
                exchange.target("trade").contracts.position_manager,
            },
        ),
        exchange,
        "trade",
    )

    controls = {item["case"]: item for item in lending_result["allowed_controls"]}
    assert set(controls) == {
        "supply_approval",
        "repay_approval",
        "configured_supply_template",
        "configured_borrow_template",
        "configured_repay_template",
        "configured_withdraw_template",
        "configured_collateral_template",
    }
    assert controls["supply_approval"]["status"] == "executed"
    assert controls["repay_approval"]["status"] == "executed"
    assert all(
        item["status"] == "authorized_inner_refusal"
        for label, item in controls.items()
        if not label.endswith("_approval")
    )
    collateral = lending.allocation("collateral")
    loan = lending.allocation("loan")
    fund = profile.fund.address
    for label, allocation in (
        ("supply_approval", collateral),
        ("repay_approval", loan),
    ):
        destination, _, inner, _, entrypoint = unwrap(controls[label]["call"], profile)
        assert (destination, entrypoint, decode_approval(inner)) == (
            allocation.reserve.asset.reference,
            "direct",
            (allocation.reserve.pool, 1),
        )
    expected_controls = {
        "configured_supply_template": (
            "supply(address,uint256,address,uint16)",
            ("address", "uint256", "address", "uint16"),
            (collateral.reserve.asset.reference, 1, fund, 0),
        ),
        "configured_borrow_template": (
            "borrow(address,uint256,uint256,uint16,address)",
            ("address", "uint256", "uint256", "uint16", "address"),
            (loan.reserve.asset.reference, 1, 2, 0, fund),
        ),
        "configured_repay_template": (
            "repay(address,uint256,uint256,address)",
            ("address", "uint256", "uint256", "address"),
            (loan.reserve.asset.reference, 1, 2, fund),
        ),
        "configured_withdraw_template": (
            "withdraw(address,uint256,address)",
            ("address", "uint256", "address"),
            (collateral.reserve.asset.reference, 1, fund),
        ),
        "configured_collateral_template": (
            "setUserUseReserveAsCollateral(address,bool)",
            ("address", "bool"),
            (collateral.reserve.asset.reference, True),
        ),
    }
    for label, (signature, types, values) in expected_controls.items():
        destination, _, inner, _, entrypoint = unwrap(controls[label]["call"], profile)
        assert destination == collateral.reserve.pool and entrypoint == "return_data"
        assert decode_call(inner, signature, types) == values
    assert {item["case"] for item in lending_result["rejected"]} >= {
        "supply_other_asset",
        "supply_other_recipient",
        "borrow_other_asset",
        "borrow_other_recipient",
        "repay_other_fund",
        "withdraw_other_recipient",
        "collateral_other_asset",
        "approval_other_spender",
    }
    statuses = {item["case"]: item["condition_status"] for item in lending_result["rejected"]}
    assert statuses["delegatecall"] == 1
    assert statuses["native_value"] == 4
    assert statuses["other_target"] == 2
    assert {
        status
        for label, status in statuses.items()
        if label not in {"delegatecall", "native_value", "other_target"}
    } <= authority.PREDICATE_STATUSES
    assert_swap_rejections(composed, exchange_result)


def test_lp_hold_retains_gateway_boundary_and_marks_eoa_not_applicable(monkeypatch, tmp_path):
    config = safe_config("uniswap_v3_bnb_lp")
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
            "retained_nft": "retained",
            "same_state": actual is state and value is owned,
        },
    )

    authority.retain_lp_gateway_ownership(None, journal, process, "position")
    binding = config.accounts.binding("fund")
    eoa = Eoa(kind="eoa", fund=binding.profile.fund)
    accounts = StrategyAccounts(bindings=(binding.model_copy(update={"profile": eoa}),))
    state.config = config.model_copy(update={"accounts": accounts})
    authority.retain_lp_gateway_ownership(None, journal, process, "position")

    observations = json.loads((tmp_path / "permission-budget-observations.json").read_text())
    assert observations[0]["gateway_foreign_nft"]["same_state"] is True
    assert observations[0]["quota_claim"].startswith("none")
    assert observations[1] == {
        "stage": "paused_and_resumed_lp_ownership",
        "profile": "eoa",
        "status": "not_applicable",
        "reason": "EOA execution has no installed Roles authority",
    }


@pytest.mark.parametrize("error", (RuntimeError("gateway unavailable: 503"), TimeoutError()))
def test_pristine_controls_fail_closed_on_transport_errors(error):
    config = safe_config("uniswap_v3_bnb_trading")

    class ErrorNode:
        def _validate_origin(self):
            pass

        def rpc(self, _method, _params):
            raise error

    with pytest.raises((AssertionError, TimeoutError)):
        authority.exchange_permissions(ErrorNode(), config, "position")


def test_predicate_probe_refuses_a_target_level_condition_status():
    class ErrorNode:
        def rpc(self, _method, _params):
            raise condition_error(2)

    with pytest.raises(AssertionError, match="unexpected Roles condition status 2"):
        authority._denied(ErrorNode(), "predicate", {})


def permission_verification(partitions=None, *, manifest="manifest-digest"):
    if partitions is None:
        partitions = [{"account": "fund"}]
    return {"manifest": manifest, "partitions": partitions}


def permission_manifest(partitions=None):
    if partitions is None:
        partitions = (SimpleNamespace(account="fund"),)
    return SimpleNamespace(digest="manifest-digest", partitions=partitions)


def test_installed_verification_binds_every_manifest_partition():
    assert _verified_partitions(permission_manifest(), permission_verification()) == ("fund",)


@pytest.mark.parametrize(
    ("manifest", "result", "message"),
    (
        (permission_manifest(()), permission_verification([]), "no account partitions"),
        (
            permission_manifest((SimpleNamespace(account="fund"),) * 2),
            permission_verification(),
            "repeats an account partition",
        ),
        (permission_manifest(), permission_verification([]), "cover every manifest partition"),
        (
            permission_manifest(),
            permission_verification(manifest="other-manifest"),
            "different manifest identity",
        ),
        (
            permission_manifest(),
            permission_verification([{"account": "other"}]),
            "unknown account partition",
        ),
        (
            permission_manifest(),
            permission_verification([{"account": "fund", "quotas": {}}]),
            "reported Roles allowances",
        ),
    ),
)
def test_installed_verification_refuses_incomplete_or_quota_state(manifest, result, message):
    with pytest.raises(AssertionError, match=message):
        _verified_partitions(manifest, result)
