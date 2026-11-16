"""Offline regressions for the private Robinhood Uniswap V3 hold probes."""

import importlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.caip import AccountReference, AssetId
from almanak.connectors import composition
from almanak.connectors.composed.fork import ComposedFork
from almanak.connectors.concentrated_math import sqrt_at_tick
from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.domain import PoolObservation
from almanak.connectors.uniswap_v3.fork import UniswapFork
from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.positions import Allowance, Ownership, Position
from almanak.connectors.uniswap_v3.support import candidate_support
from almanak.models import (
    Account,
    AccountBinding,
    Asset,
    AssetAmount,
    Block,
    Chain,
    Eoa,
    SafeRoles,
    StrategyAccounts,
)
from scripts.legacy_configuration.author import read_author
from tests.strategy_fork import robinhood_v3_authority as authority
from tests.strategy_fork.scenarios.uniswap_v3_robinhood_trading import assertions

ROOT = Path(__file__).parents[1]
CANDIDATES = ROOT / "tests/fixtures/author/candidates"
INSTANCE = "0x" + "00" * 32


class PolicyNode:
    """Decode every wrapped call before choosing its exact simulated outcome."""

    def __init__(self, policy):
        self.policy = policy
        self.calls = []

    def rpc(self, method, params):
        assert method == "eth_call" and params[1] == "latest"
        call = params[0]
        self.calls.append(call)
        outcome = self.policy(call)
        if outcome == "executed":
            return "0x"
        if outcome == "roles_denied":
            payload = {
                "code": 3,
                "message": "execution reverted",
                "data": authority.ROLES_CONDITION_VIOLATION + "00" * 64,
            }
            raise RuntimeError(authority.RPC_ERROR_PREFIX + repr(payload))
        if isinstance(outcome, tuple) and len(outcome) == 2:
            success, returned = outcome
            return "0x" + encode(("bool", "bytes"), (success, returned)).hex()
        raise AssertionError(f"unknown decoded call outcome: {outcome}")


def selector(signature):
    return keccak(text=signature)[:4]


def unwrap(call, profile):
    assert call["from"] == profile.operator.address
    assert call["to"] == profile.roles_module
    raw = bytes.fromhex(call["data"][2:])
    direct = selector("execTransactionWithRole(address,uint256,bytes,uint8,bytes32,bool)")
    returned = selector(
        "execTransactionWithRoleReturnData(address,uint256,bytes,uint8,bytes32,bool)"
    )
    assert raw[:4] in {direct, returned}
    target, value, inner, operation, role, revert = decode(
        ("address", "uint256", "bytes", "uint8", "bytes32", "bool"), raw[4:]
    )
    assert "0x" + role.hex() == profile.role_key
    entrypoint = "return_data" if raw[:4] == returned else "direct"
    assert revert is (entrypoint == "direct")
    return target.lower(), value, inner, operation, entrypoint


def decode_approval(inner):
    assert inner[:4] == selector("approve(address,uint256)")
    spender, amount = decode(("address", "uint256"), inner[4:])
    return spender.lower(), amount


def decode_swap(inner):
    assert inner[:4] == selector("multicall(uint256,bytes[])")
    deadline, calls = decode(("uint256", "bytes[]"), inner[4:])
    decoded = []
    for call in calls:
        if call[:4] != selector(f"exactInputSingle({authority.abi.SWAP_PARAMS})"):
            decoded.append(None)
            continue
        (values,) = decode((authority.abi.SWAP_PARAMS,), call[4:])
        decoded.append(
            tuple(value.lower() if isinstance(value, str) else value for value in values)
        )
    return deadline, tuple(decoded)


def v3_policy(config, state):
    target = config.target("position" if config.connector else "trade")
    profile = config.accounts.binding(target.account).profile
    reverse = target.counter_currency
    owned = next(value for value in state.owned if value.target == target)
    reverse_amount = next(
        value.amount for value in (owned.inventory0, owned.inventory1) if value.asset == reverse
    )
    valid_swap = (
        reverse.reference,
        target.funding_currency.reference,
        target.pool.fee,
        target.fund.address,
        reverse_amount,
        1,
        0,
    )

    def policy(call):
        destination, value, inner, operation, entrypoint = unwrap(call, profile)
        if value or operation:
            return "roles_denied"
        if destination in {asset.reference for asset in target.assets}:
            assert entrypoint == "direct"
            spender, amount = decode_approval(inner)
            valid_spenders = {target.contracts.router, target.contracts.position_manager}
            if spender not in valid_spenders:
                return "roles_denied"
            # Scope-only grants (blueprint 27): the exact spender with any amount.
            return "executed" if amount >= 0 else "roles_denied"
        if destination == target.contracts.router:
            deadline, calls = decode_swap(inner)
            if deadline == 2**64 and calls == (valid_swap,):
                assert entrypoint == "return_data"
                refusal = authority.abi.calldata("Error(string)", ("string",), ("STF",))
                return False, refusal
            assert entrypoint == "direct"
            return "roles_denied"
        if destination == target.contracts.position_manager:
            assert entrypoint == "direct"
            if inner[:4] == selector(f"collect({authority.abi.COLLECT_PARAMS})"):
                (values,) = decode((authority.abi.COLLECT_PARAMS,), inner[4:])
                valid = (
                    int(owned.position.nft.token_id),
                    target.fund.address,
                    authority.abi.MAX128,
                    authority.abi.MAX128,
                )
                return "executed" if values == valid else "roles_denied"
            if inner[:4] == selector("multicall(bytes[])"):
                (calls,) = decode(("bytes[]",), inner[4:])
                expected = bytes.fromhex(
                    authority.abi.call(
                        authority.Close(
                            target=target,
                            nft=owned.position.nft,
                            liquidity=owned.position.liquidity,
                            minimum0=AssetAmount(asset=target.assets[0], amount=0),
                            minimum1=AssetAmount(asset=target.assets[1], amount=0),
                            deadline=2**64,
                        )
                    ).data[2:]
                )
                (expected_calls,) = decode(("bytes[]",), expected[4:])
                return "executed" if calls == expected_calls else "roles_denied"
        raise AssertionError("probe emitted an unexpected target or selector")

    return policy


def assert_trade_call_differences(config, state, result):
    target = config.target("position" if config.connector else "trade")
    profile = config.accounts.binding(target.account).profile
    reverse = target.counter_currency
    owned = next(value for value in state.owned if value.target == target)
    amount = next(
        value.amount for value in (owned.inventory0, owned.inventory1) if value.asset == reverse
    )
    baseline = (
        reverse.reference,
        target.funding_currency.reference,
        target.pool.fee,
        target.fund.address,
        amount,
        1,
        0,
    )
    for index, approved in enumerate((1, 2**256 - 1)):
        approval = result["allowed_controls"][index]["call"]
        destination, value, inner, operation, entrypoint = unwrap(approval, profile)
        assert (destination, value, operation, entrypoint) == (reverse.reference, 0, 0, "direct")
        assert decode_approval(inner) == (target.contracts.router, approved)
    if not amount:
        return
    reverse_call = result["allowed_controls"][2]["call"]
    destination, value, inner, operation, entrypoint = unwrap(reverse_call, profile)
    assert (destination, value, operation, entrypoint) == (
        target.contracts.router,
        0,
        0,
        "return_data",
    )
    assert decode_swap(inner) == (2**64, (baseline,))
    rejected = {item["case"]: item["call"] for item in result["rejected"]}
    changes = {
        "reverse_swap_other_output": (1, reverse.reference),
        "reverse_swap_other_fee": (2, target.pool.fee + 1),
        "reverse_swap_other_recipient": (3, profile.submitter.address),
        "reverse_swap_zero_input": (4, 0),
        "reverse_swap_price_limit": (6, 1),
    }
    for label, (index, expected) in changes.items():
        destination, value, inner, operation, entrypoint = unwrap(rejected[label], profile)
        deadline, calls = decode_swap(inner)
        changed = list(baseline)
        changed[index] = expected
        assert (destination, value, operation, entrypoint, deadline, calls) == (
            target.contracts.router,
            0,
            0,
            "direct",
            2**64,
            (tuple(changed),),
        )
    _, _, inner, _, entrypoint = unwrap(rejected["reverse_swap_extra_nested_call"], profile)
    assert entrypoint == "direct"
    assert decode_swap(inner) == (2**64, (baseline, baseline))
    destination, _, inner, _, entrypoint = unwrap(
        rejected["router_approval_other_spender"], profile
    )
    assert (destination, entrypoint) == (reverse.reference, "direct")
    assert decode_approval(inner) == (profile.submitter.address, 1)


def assert_liquidity_call_differences(config, state, result):
    target = config.target("position")
    profile = config.accounts.binding(target.account).profile
    owned = state.owned[0]
    token = int(owned.position.nft.token_id)
    exit_result = result["liquidity_exit"]
    controls = {item["case"]: item["call"] for item in exit_result["allowed_controls"]}
    destination, _, inner, _, entrypoint = unwrap(
        controls["position_manager_approval_any_amount"], profile
    )
    assert destination == target.assets[0].reference
    assert decode_approval(inner) == (target.contracts.position_manager, 2**256 - 1)
    destination, _, inner, _, entrypoint = unwrap(controls["retained_nft_collect"], profile)
    assert destination == target.contracts.position_manager
    (collect,) = decode((authority.abi.COLLECT_PARAMS,), inner[4:])
    assert collect == (token, target.fund.address, authority.abi.MAX128, authority.abi.MAX128)
    destination, _, close_inner, _, entrypoint = unwrap(controls["retained_nft_close"], profile)
    assert destination == target.contracts.position_manager
    assert close_inner[:4] == selector("multicall(bytes[])")

    rejected = {item["case"]: item["call"] for item in exit_result["rejected"]}
    destination, _, inner, _, entrypoint = unwrap(
        rejected["position_manager_approval_other_spender"], profile
    )
    assert destination == target.assets[0].reference
    assert decode_approval(inner) == (profile.submitter.address, 1)
    assert not any("above" in label for label in rejected)
    _, _, inner, _, entrypoint = unwrap(rejected["collect_other_recipient"], profile)
    (collect,) = decode((authority.abi.COLLECT_PARAMS,), inner[4:])
    assert collect == (
        token,
        profile.submitter.address,
        authority.abi.MAX128,
        authority.abi.MAX128,
    )
    _, _, inner, _, entrypoint = unwrap(rejected["collect_nonmax_amount"], profile)
    (collect,) = decode((authority.abi.COLLECT_PARAMS,), inner[4:])
    assert collect == (
        token,
        target.fund.address,
        authority.abi.MAX128 - 1,
        authority.abi.MAX128,
    )
    _, _, inner, _, entrypoint = unwrap(rejected["close_extra_nested_call"], profile)
    (calls,) = decode(("bytes[]",), inner[4:])
    assert len(calls) == 2 and calls[0] == calls[1] == close_inner
    _, value, inner, _, entrypoint = unwrap(rejected["close_native_value"], profile)
    assert value == 1 and inner == close_inner


def morpho_policy(config):
    component = config.component(authority.ConnectorId("morpho_blue"))
    profile = component.accounts.binding("fund").profile
    loan = component.allocation("loan")
    market = loan.reserve.market
    repay_signature = (
        f"repay({authority.morpho_operations.PARAMS_ABI},uint256,uint256,address,bytes)"
    )
    withdraw_signature = (
        f"withdrawCollateral({authority.morpho_operations.PARAMS_ABI},uint256,address,address)"
    )

    def policy(call):
        destination, value, inner, operation, entrypoint = unwrap(call, profile)
        if value or operation:
            return "roles_denied"
        if destination == loan.reserve.asset.reference:
            assert entrypoint == "direct"
            spender, _ = decode_approval(inner)
            # The approval grant pins the core and admits any amount.
            return "executed" if spender == market.core else "roles_denied"
        if destination != market.core:
            raise AssertionError("Morpho probe emitted an unexpected target")
        if inner[:4] == selector(repay_signature):
            values = decode(
                (authority.morpho_operations.PARAMS_ABI, "uint256", "uint256", "address", "bytes"),
                inner[4:],
            )
            valid = values[0] == market.parameters and values[1:] == (
                1,
                0,
                profile.fund.address,
                b"",
            )
            if valid:
                assert entrypoint == "return_data"
                return False, b"\xde\xad\xbe\xef"
            assert entrypoint == "direct"
            return "roles_denied"
        if inner[:4] == selector(withdraw_signature):
            values = decode(
                (authority.morpho_operations.PARAMS_ABI, "uint256", "address", "address"),
                inner[4:],
            )
            valid = values == (
                market.parameters,
                1,
                profile.fund.address,
                profile.fund.address,
            )
            if valid:
                assert entrypoint == "return_data"
                return True, b""
            assert entrypoint == "direct"
            return "roles_denied"
        raise AssertionError("Morpho probe emitted an unexpected selector")

    return policy


def assert_morpho_call_differences(config, result):
    component = config.component(authority.ConnectorId("morpho_blue"))
    profile = component.accounts.binding("fund").profile
    loan = component.allocation("loan")
    market = loan.reserve.market
    controls = {item["case"]: item["call"] for item in result["allowed_controls"]}
    _, _, _, _, repay_entrypoint = unwrap(controls["retained_debt_repay"], profile)
    _, _, _, _, withdraw_entrypoint = unwrap(controls["retained_collateral_withdraw"], profile)
    assert (repay_entrypoint, withdraw_entrypoint) == ("return_data", "return_data")
    destination, _, inner, _, entrypoint = unwrap(controls["bounded_morpho_approval"], profile)
    assert (destination, entrypoint) == (loan.reserve.asset.reference, "direct")
    assert decode_approval(inner) == (market.core, 1)

    rejected = {item["case"]: item["call"] for item in result["rejected"]}
    repay_types = (
        authority.morpho_operations.PARAMS_ABI,
        "uint256",
        "uint256",
        "address",
        "bytes",
    )
    _, _, inner, _, entrypoint = unwrap(rejected["repay_other_owner"], profile)
    repay = decode(repay_types, inner[4:])
    assert repay[1:] == (1, 0, profile.submitter.address, b"")
    _, _, inner, _, entrypoint = unwrap(rejected["repay_nonempty_callback"], profile)
    repay = decode(repay_types, inner[4:])
    assert repay[1:] == (1, 0, profile.fund.address, b"\x01")
    _, _, inner, _, entrypoint = unwrap(rejected["repay_assets_and_shares"], profile)
    repay = decode(repay_types, inner[4:])
    assert repay[1:] == (1, 1, profile.fund.address, b"")
    _, _, inner, _, entrypoint = unwrap(rejected["repay_zero"], profile)
    repay = decode(repay_types, inner[4:])
    assert repay[1:] == (0, 0, profile.fund.address, b"")
    withdraw_types = (
        authority.morpho_operations.PARAMS_ABI,
        "uint256",
        "address",
        "address",
    )
    _, _, inner, _, entrypoint = unwrap(rejected["withdraw_other_receiver"], profile)
    withdraw = decode(withdraw_types, inner[4:])
    assert withdraw[1:] == (1, profile.fund.address, profile.submitter.address)
    _, _, inner, _, entrypoint = unwrap(rejected["withdraw_other_owner"], profile)
    withdraw = decode(withdraw_types, inner[4:])
    assert withdraw[1:] == (1, profile.submitter.address, profile.fund.address)
    destination, _, inner, _, entrypoint = unwrap(
        rejected["morpho_approval_other_spender"], profile
    )
    assert destination == loan.reserve.asset.reference
    assert decode_approval(inner) == (profile.submitter.address, 1)
    assert not any("above" in case for case in rejected)


def safe_binding(chain):
    def account(number):
        return Account(chain=chain, address=AccountReference(f"0x{number:040x}"))

    profile = SafeRoles(
        kind="safe_roles",
        fund=account(1),
        operator=account(2),
        roles_module=account(3).address,
        role_key="0x" + "11" * 32,
    )
    return AccountBinding(name="fund", profile=profile)


def install_candidate_support(monkeypatch):
    connector_module = importlib.import_module("almanak.connectors.uniswap_v3.connector")
    native_connector = connector_module.connector

    def qualified_connector():
        return replace(native_connector(), chain_support=candidate_support())

    original_connector_for = composition.connector_for

    def connector_for(config):
        if getattr(config, "connector", None) == "uniswap_v3":
            return qualified_connector()
        return original_connector_for(config)

    monkeypatch.setattr(connector_module, "connector", qualified_connector)
    monkeypatch.setattr(composition, "connector_for", connector_for)


def resolved(monkeypatch, name):
    install_candidate_support(monkeypatch)
    fork = read_author(CANDIDATES / name / "config.yaml").fork_config()
    chain = Chain(chain_id=fork.origin.chain.chain_id, environment="anvil:" + INSTANCE)
    binding = safe_binding(chain)
    if name.startswith("robinhood_morpho"):
        return ComposedFork.prepare(fork, (binding,), chain, INSTANCE, fork.origin)
    return UniswapFork.prepare(fork, (binding,), chain, INSTANCE, fork.origin)


def exchange_state(config, *, lp=False, counter_amount=None):
    target = config.target("position")
    inventories = tuple(
        AssetAmount(
            asset=asset,
            amount=(
                counter_amount
                if counter_amount is not None and asset == target.counter_currency
                else index + 1
            ),
        )
        for index, asset in enumerate(target.assets)
    )
    position = None
    if lp:
        nft = AssetId(f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}/17")
        position = Position(
            nft=nft,
            liquidity=10,
            owed0=0,
            owed1=0,
            fee_growth=(0, 0),
        )
    owned = Ownership(
        target=target,
        inventory0=inventories[0],
        inventory1=inventories[1],
        position=position,
    )
    return SimpleNamespace(config=config, owned=(owned,), outcomes=(), mode="running")


def test_trading_hold_probes_reverse_swap_scope_and_valid_controls(monkeypatch):
    config = resolved(monkeypatch, "uniswap_v3_robinhood_trading")
    state = exchange_state(config)
    node = PolicyNode(v3_policy(config, state))

    result = authority.exchange_permissions(node, state, "position")
    assert_trade_call_differences(config, state, result)

    assert "quota_basis" not in result
    assert [item["case"] for item in result["allowed_controls"]] == [
        "router_approval",
        "router_approval_any_amount",
        "reverse_swap_scope",
    ]
    reverse = result["allowed_controls"][2]
    assert reverse["status"] == "authorized_inner_refusal"
    assert reverse["roles_entrypoint"] == "execTransactionWithRoleReturnData"
    assert reverse["inner_success"] is False
    assert reverse["inner_return_data"] == authority._inner(
        authority.abi.calldata("Error(string)", ("string",), ("STF",))
    )
    assert {item["case"] for item in result["rejected"]} == {
        "reverse_swap_other_output",
        "reverse_swap_other_fee",
        "reverse_swap_other_recipient",
        "reverse_swap_zero_input",
        "reverse_swap_price_limit",
        "reverse_swap_extra_nested_call",
        "router_approval_other_spender",
    }
    assert result["not_isolated"] == []
    assert result["insufficiencies"] == []
    assert result["qualification_status"] == "complete"


def test_lp_hold_probes_reverse_and_exit_without_claiming_entry_predicates(
    monkeypatch,
):
    config = resolved(monkeypatch, "uniswap_v3_robinhood_lp")
    state = exchange_state(config, lp=True)
    node = PolicyNode(v3_policy(config, state))
    monkeypatch.setattr(
        authority,
        "foreign_nft_gateway_refusal",
        lambda state, owned: {"status": "rejected", "retained_nft": str(owned.position.nft)},
    )

    result = authority.exchange_permissions(node, state, "position")
    assert_trade_call_differences(config, state, result)
    assert_liquidity_call_differences(config, state, result)

    exit_result = result["liquidity_exit"]
    assert [item["case"] for item in exit_result["allowed_controls"]] == [
        "position_manager_approval_any_amount",
        "retained_nft_collect",
        "retained_nft_close",
    ]
    assert exit_result["gateway_foreign_nft"]["retained_nft"] == str(state.owned[0].position.nft)
    assert result["not_isolated"] == []
    assert "unrestricted" in exit_result["accepted_onchain_scope"]


def test_lp_hold_retains_incomplete_status_when_no_counter_asset_dust(monkeypatch, tmp_path):
    config = resolved(monkeypatch, "uniswap_v3_robinhood_lp")
    state = exchange_state(config, lp=True, counter_amount=0)
    node = PolicyNode(v3_policy(config, state))
    monkeypatch.setattr(
        authority,
        "foreign_nft_gateway_refusal",
        lambda state, owned: {"status": "rejected", "retained_nft": str(owned.position.nft)},
    )

    result = authority.exchange_permissions(node, state, "position")
    assert_trade_call_differences(config, state, result)
    assert_liquidity_call_differences(config, state, result)

    assert [item["case"] for item in result["allowed_controls"]] == [
        "router_approval",
        "router_approval_any_amount",
    ]
    assert result["insufficiencies"] == [
        "no retained counter-asset wallet inventory; reverse swap admission was not probed"
    ]
    assert result["qualification_status"] == "incomplete"
    assert [item["case"] for item in result["liquidity_exit"]["allowed_controls"]] == [
        "position_manager_approval_any_amount",
        "retained_nft_collect",
        "retained_nft_close",
    ]
    journal = SimpleNamespace(path=tmp_path / "state.sqlite", load=lambda: state)
    authority.exercise_hold(node, journal, SimpleNamespace(poll=lambda: None), "position")
    retained = json.loads((tmp_path / "permission-budget-observations.json").read_text())
    assert retained[0]["status"] == "incomplete"


def test_foreign_nft_gateway_probe_keeps_retained_checkpoint(monkeypatch):
    config = resolved(monkeypatch, "uniswap_v3_robinhood_lp")
    state = exchange_state(config, lp=True)
    owned = state.owned[0]
    allocation = config.allocation("position")
    block = Block(
        chain=owned.target.fund.chain,
        number=1,
        hash="0x" + "22" * 32,
        timestamp=100,
    )
    tick = allocation.range.lower
    pool = PoolObservation(
        source=owned.target.pool,
        block=block,
        currency0=Asset(
            chain=block.chain,
            identifier=owned.target.assets[0],
            decimals=config.asset_metadata[owned.target.assets[0]].decimals,
        ),
        currency1=Asset(
            chain=block.chain,
            identifier=owned.target.assets[1],
            decimals=config.asset_metadata[owned.target.assets[1]].decimals,
        ),
        sqrt_price_x96=sqrt_at_tick(tick),
        tick=tick,
        liquidity=100,
        protocol_fee=0,
        unlocked=True,
    )
    before = Snapshot(
        target=owned.target,
        pool=pool,
        owned=owned,
        wallet0=owned.inventory0,
        wallet1=owned.inventory1,
        allowances=tuple(
            Allowance(asset=asset, spender=spender, amount=0)
            for asset in owned.target.assets
            for spender in (
                owned.target.contracts.router,
                owned.target.contracts.position_manager,
            )
        ),
        native_balance=AssetAmount(asset=allocation.native_asset, amount=0),
        payer=config.accounts.binding("fund").profile.submitter,
        position=owned.position,
        quotes_included=False,
    )
    outcome = SimpleNamespace(
        operation=SimpleNamespace(target=owned.target), after=before, success=True
    )
    state.outcomes = (outcome,)
    state.capabilities = declared(config)
    retained = owned.position.nft

    result = authority.foreign_nft_gateway_refusal(state, owned)

    assert result["status"] == "rejected"
    assert result["retained_nft"] == str(retained)
    assert owned.position.nft == retained
    assert result["foreign_nft"] != result["retained_nft"]


def test_composed_hold_uses_morpho_exit_controls_and_marks_entry_limit(monkeypatch):
    config = resolved(monkeypatch, "robinhood_morpho_borrow_and_trade_v3")
    collateral = SimpleNamespace(target=config.target("collateral"), collateral=10)
    loan = SimpleNamespace(target=config.target("loan"), borrow_shares=20)
    state = SimpleNamespace(config=config, owned=(collateral, loan))
    node = PolicyNode(morpho_policy(config))

    result = authority.morpho_exit_permissions(node, state)
    assert_morpho_call_differences(config, result)

    assert [item["case"] for item in result["allowed_controls"]] == [
        "retained_debt_repay",
        "retained_collateral_withdraw",
        "bounded_morpho_approval",
    ]
    assert result["allowed_controls"][0]["status"] == "authorized_inner_refusal"
    assert result["allowed_controls"][0]["inner_return_data"] == "0xdeadbeef"
    assert result["allowed_controls"][1]["status"] == "inner_executed"
    assert {item["case"] for item in result["rejected"]} >= {
        "repay_other_owner",
        "repay_nonempty_callback",
        "withdraw_other_receiver",
        "withdraw_other_owner",
        "repay_zero",
        "morpho_approval_other_spender",
    }
    assert "entry-consumed non-refilling quotas" in result["not_isolated"][0]


class ErrorNode:
    def __init__(self, error):
        self.error = error

    def rpc(self, _method, _params):
        raise self.error


class ResultNode:
    def __init__(self, result):
        self.result = result

    def rpc(self, _method, _params):
        return self.result


def test_valid_control_refuses_transport_and_unknown_authorization_errors():
    call = {"from": "0x1", "to": "0x2", "data": "0x"}
    with pytest.raises(AssertionError, match="did not execute"):
        authority._allowed(ErrorNode(RuntimeError("gateway unavailable: 503")), "control", call)
    with pytest.raises(AssertionError, match="did not execute"):
        authority._allowed(ErrorNode(RuntimeError("0xdeadbeef")), "control", call)
    with pytest.raises(TimeoutError):
        authority._allowed(ErrorNode(TimeoutError("timed out")), "control", call)


def test_denied_control_requires_structured_roles_revert():
    call = {"from": "0x1", "to": "0x2", "data": "0x"}
    with pytest.raises(AssertionError, match="unstructured RPC error"):
        authority._denied(
            ErrorNode(RuntimeError(f"transport included {authority.ROLES_CONDITION_VIOLATION}")),
            "malformed",
            call,
        )
    other = {"code": 3, "message": "execution reverted", "data": "0xdeadbeef"}
    with pytest.raises(AssertionError, match="did not return a Roles ConditionViolation"):
        authority._denied(
            ErrorNode(RuntimeError(authority.RPC_ERROR_PREFIX + repr(other))),
            "malformed",
            call,
        )


def test_return_data_control_separates_roles_admission_from_inner_refusal():
    returned = b"\xde\xad\xbe\xef"
    payload = "0x" + encode(("bool", "bytes"), (False, returned)).hex()
    result = authority._allowed(
        ResultNode(payload),
        "reverse",
        {},
        return_data=True,
    )

    assert result["status"] == "authorized_inner_refusal"
    assert result["inner_success"] is False
    assert result["inner_return_data"] == "0xdeadbeef"


@pytest.mark.parametrize("payload", ("bad", "0x", "0x01"))
def test_return_data_control_refuses_malformed_results(payload):
    with pytest.raises(AssertionError, match="malformed Roles data"):
        authority._allowed(ResultNode(payload), "control", {}, return_data=True)


def test_return_data_control_refuses_noncanonical_result():
    raw = encode(("bool", "bytes"), (False, b"refusal")) + bytes(32)
    with pytest.raises(AssertionError, match="noncanonical Roles data"):
        authority._allowed(ResultNode("0x" + raw.hex()), "control", {}, return_data=True)


def test_eoa_hold_retains_explicit_not_applicable_observation(monkeypatch, tmp_path):
    config = resolved(monkeypatch, "uniswap_v3_robinhood_trading")
    binding = config.accounts.binding("fund")
    eoa = Eoa(kind="eoa", fund=binding.profile.fund)
    accounts = StrategyAccounts(bindings=(binding.model_copy(update={"profile": eoa}),))
    state = SimpleNamespace(config=config.model_copy(update={"accounts": accounts}))
    journal = SimpleNamespace(path=tmp_path / "state.sqlite", load=lambda: state)
    process = SimpleNamespace(poll=lambda: None)

    assertions.exercise_hold(None, journal, process)

    observations = json.loads((tmp_path / "permission-budget-observations.json").read_text())
    assert observations == [
        {
            "stage": "paused_and_resumed_hold",
            "profile": "eoa",
            "status": "not_applicable",
            "reason": "EOA execution has no installed Roles authority",
        }
    ]
