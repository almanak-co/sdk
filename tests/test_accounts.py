import json

import pytest
from eth_abi import decode
from pydantic import ValidationError

from almanak.caip import ChainId
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.models import (
    Account,
    AccountBinding,
    Chain,
    Eoa,
    SafeRoles,
    StrategyAccounts,
)

ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="unit")
SECOND_CHAIN = Chain(chain_id=ChainId("eip155:10"), environment="unit")


def account(number, chain=ETHEREUM):
    return Account(chain=chain, address=f"0x{number:040x}")


def safe(number, chain=ETHEREUM, operator=3):
    return SafeRoles(
        kind="safe_roles",
        fund=account(number, chain),
        operator=account(operator, chain),
        roles_module=account(number + 100, chain).address,
        role_key="0x" + "11" * 32,
    )


@pytest.fixture
def accounts():
    return StrategyAccounts(
        bindings=(
            AccountBinding(name="lending", profile=safe(1)),
            AccountBinding(name="liquidity", profile=safe(2)),
            AccountBinding(name="second_chain", profile=safe(1, SECOND_CHAIN)),
            AccountBinding(name="eoa", profile=Eoa(kind="eoa", fund=account(4))),
        )
    )


def contract_call(fund, value=123):
    return ContractCall(fund=fund, to=account(9).address, value=value, data="0x12345678abcd")


def test_one_strategy_binds_multiple_funds_and_chains_without_default_accounts(accounts):
    assert len(accounts.bindings) == 4
    assert accounts.binding("lending").profile.fund != accounts.binding("second_chain").profile.fund
    assert StrategyAccounts.model_validate_json(accounts.model_dump_json()) == accounts
    with pytest.raises(KeyError, match="unknown account binding"):
        accounts.binding("default")
    with pytest.raises(ValidationError):
        accounts.bindings = ()


@pytest.mark.parametrize("duplicate", ["name", "fund"])
def test_ambiguous_bindings_are_rejected(duplicate):
    first = AccountBinding(name="first", profile=safe(1))
    second = AccountBinding(
        name="first" if duplicate == "name" else "second",
        profile=safe(2) if duplicate == "name" else safe(1, operator=4),
    )
    with pytest.raises(ValidationError):
        StrategyAccounts(bindings=(first, second))


def test_empty_bindings_allow_read_only_use_without_an_execution_fallback():
    accounts = StrategyAccounts(bindings=())
    with pytest.raises(KeyError):
        prepare_call(accounts, "missing", contract_call(account(1)))


def test_fork_and_mainnet_accounts_with_identical_addresses_remain_distinct():
    other = Chain(chain_id=ChainId("eip155:1"), environment="other-fork")
    bindings = StrategyAccounts(
        bindings=(
            AccountBinding(name="first", profile=safe(1)),
            AccountBinding(name="other", profile=safe(1, other)),
        )
    )
    assert (
        bindings.binding("first").profile.submitter != bindings.binding("other").profile.submitter
    )


@pytest.mark.parametrize("value", [0, 123])
def test_zodiac_keeps_fund_native_value_separate_from_operator_gas(accounts, value):
    profile = accounts.binding("lending").profile
    call = contract_call(profile.fund, value)
    prepared = prepare_call(accounts, "lending", call)
    assert prepared.call == call
    assert prepared.call.fund != prepared.submitter
    assert prepared.submitter == profile.operator
    assert prepared.to == profile.roles_module
    assert prepared.value == 0
    assert prepared.data.startswith("0xc6fe8747")
    target, inner_value, data, operation, role, should_revert = decode(
        ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
        bytes.fromhex(prepared.data[10:]),
    )
    assert (target, inner_value, data) == (call.to, value, bytes.fromhex(call.data[2:]))
    assert operation == 0
    assert role == bytes.fromhex(profile.role_key[2:])
    assert should_revert is True


def test_eoa_uses_the_same_call_with_its_own_funds_and_submission(accounts):
    profile = accounts.binding("eoa").profile
    call = contract_call(profile.fund)
    prepared = prepare_call(accounts, "eoa", call)
    assert prepared.call == call
    assert prepared.submitter == profile.operator == call.fund
    assert (prepared.to, prepared.value, prepared.data) == (call.to, call.value, call.data)


def test_shared_operator_shares_outer_nonce_scope_but_not_fund_identity(accounts):
    lending = prepare_call(
        accounts, "lending", contract_call(accounts.binding("lending").profile.fund)
    )
    lp = prepare_call(
        accounts, "liquidity", contract_call(accounts.binding("liquidity").profile.fund)
    )
    second = prepare_call(
        accounts, "second_chain", contract_call(accounts.binding("second_chain").profile.fund)
    )
    assert lending.submitter == lp.submitter
    assert lending.call.fund != lp.call.fund
    assert lending.to != lp.to
    assert lending.submitter.address == second.submitter.address
    assert lending.submitter != second.submitter


@pytest.mark.parametrize("wrong_scope", ["fund", "chain", "environment"])
def test_call_cannot_use_a_binding_for_another_scope(accounts, wrong_scope):
    fund = accounts.binding("lending").profile.fund
    if wrong_scope == "fund":
        fund = account(2)
    elif wrong_scope == "chain":
        fund = account(1, SECOND_CHAIN)
    else:
        fund = account(1, Chain(chain_id=ChainId("eip155:1"), environment="another-fork"))
    with pytest.raises(ValueError, match="account/chain/environment"):
        prepare_call(accounts, "lending", contract_call(fund))


def test_unknown_binding_never_falls_back_to_a_matching_eoa(accounts):
    with pytest.raises(KeyError):
        prepare_call(accounts, "missing", contract_call(accounts.binding("eoa").profile.fund))


def test_operator_rotation_does_not_migrate_fund_ownership(accounts):
    original = accounts.binding("lending")
    rotated = AccountBinding(name=original.name, profile=safe(1, operator=4))
    revised = StrategyAccounts(bindings=(rotated,))
    call = contract_call(original.profile.fund)
    before = prepare_call(accounts, "lending", call)
    after = prepare_call(revised, "lending", call)
    assert before.call == after.call
    assert before.data == after.data
    assert before.submitter != after.submitter
    assert accounts.binding("lending") == original


@pytest.mark.parametrize(
    "change",
    [
        "same_operator",
        "wrong_chain",
        "wrong_environment",
        "zero_module",
        "fund_module",
        "missing_role",
    ],
)
def test_invalid_safe_authority_configuration_is_rejected(change):
    raw = safe(1).model_dump()
    if change == "same_operator":
        raw["operator"] = raw["fund"]
    elif change == "wrong_chain":
        raw["operator"]["chain"]["chain_id"] = 10
    elif change == "wrong_environment":
        raw["operator"]["chain"]["environment"] = "other"
    elif change == "zero_module":
        raw["roles_module"] = "0x" + "00" * 20
    elif change == "fund_module":
        raw["roles_module"] = raw["fund"]["address"]
    else:
        del raw["role_key"]
    with pytest.raises(ValidationError):
        SafeRoles.model_validate(raw)


def test_legacy_submitter_field_requires_explicit_profile_migration():
    raw = safe(1).model_dump()
    raw["submitter"] = raw.pop("operator")
    with pytest.raises(ValidationError):
        SafeRoles.model_validate(raw)


@pytest.mark.parametrize("change", ["odd_hex", "negative_value", "overflow", "delegatecall"])
def test_malformed_or_unsupported_calls_are_rejected(change):
    raw = contract_call(account(1)).model_dump(mode="json")
    if change == "odd_hex":
        raw["data"] = "0x123"
    elif change == "negative_value":
        raw["value"] = -1
    elif change == "overflow":
        raw["value"] = 2**256
    else:
        raw["operation"] = 1
    with pytest.raises(ValidationError):
        ContractCall.model_validate_json(json.dumps(raw))
