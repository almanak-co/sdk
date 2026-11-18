"""Unsigned V3 calls retain fund recipients and finite token approvals."""

import gzip
import json
from pathlib import Path

import pytest
from eth_abi import decode
from eth_utils import keccak
from pydantic import ValidationError

from almanak.caip import AssetType
from almanak.connectors.aave_v3.domain import PositionObservation, Target
from almanak.connectors.aave_v3.math import UINT256_MAX
from almanak.connectors.aave_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
    plan_call,
)
from almanak.gateway.accounts import prepare_call
from almanak.models import AccountBinding, AssetAmount, SafeRoles, StrategyAccounts


@pytest.fixture
def target():
    fixture = Path(__file__).parent / "fixtures/aave-v3-ethereum-revision11-reads.json.gz"
    raw = json.loads(gzip.decompress(fixture.read_bytes()))
    position = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    return Target(
        allocation="collateral",
        account="vault",
        fund=position.fund,
        reserve=position.reserve.source,
    )


@pytest.mark.parametrize(
    "operation,fields,signature,types,amount_index,fund_index",
    [
        (
            SupplyOperation,
            {},
            "supply(address,uint256,address,uint16)",
            ("address", "uint256", "address", "uint16"),
            1,
            2,
        ),
        (
            BorrowOperation,
            {},
            "borrow(address,uint256,uint256,uint16,address)",
            ("address", "uint256", "uint256", "uint16", "address"),
            1,
            4,
        ),
        (
            WithdrawOperation,
            {"full": False},
            "withdraw(address,uint256,address)",
            ("address", "uint256", "address"),
            1,
            2,
        ),
        (
            WithdrawOperation,
            {"full": True},
            "withdraw(address,uint256,address)",
            ("address", "uint256", "address"),
            1,
            2,
        ),
        (
            RepayOperation,
            {"full": False},
            "repay(address,uint256,uint256,address)",
            ("address", "uint256", "uint256", "address"),
            1,
            3,
        ),
        (
            RepayOperation,
            {"full": True},
            "repay(address,uint256,uint256,address)",
            ("address", "uint256", "uint256", "address"),
            1,
            3,
        ),
    ],
)
def test_pool_call_asset_recipient_and_variable_debt_mode(
    target, operation, fields, signature, types, amount_index, fund_index
):
    call = plan_call(
        operation(target=target, amount=AssetAmount(asset=target.asset, amount=123), **fields)
    )
    data = bytes.fromhex(call.data[2:])
    assert data[:4] == keccak(text=signature)[:4]
    values = decode(types, data[4:])
    assert values[0] == target.asset.reference
    assert values[fund_index] == target.fund.address
    # Full withdrawal and full repayment send the Pool sentinel; partial amounts are exact.
    full = operation in (WithdrawOperation, RepayOperation) and fields.get("full")
    assert values[amount_index] == (UINT256_MAX if full else 123)
    assert (call.to, call.fund, call.value) == (target.reserve.pool, target.fund, 0)
    if operation in (BorrowOperation, RepayOperation):
        assert values[2] == 2


def test_cleanup_and_collateral_calls_cannot_select_other_spenders_or_assets(target):
    operations = (
        ApproveOperation(target=target, amount=AssetAmount(asset=target.asset, amount=456)),
        RevokeOperation(target=target),
    )
    for operation, expected in zip(operations, (456, 0), strict=True):
        call = plan_call(operation)
        assert call.to == target.asset.reference
        assert decode(("address", "uint256"), bytes.fromhex(call.data[10:])) == (
            target.reserve.pool,
            expected,
        )
    for enabled in (True, False):
        call = plan_call(CollateralOperation(target=target, enabled=enabled))
        assert call.to == target.reserve.pool
        assert decode(("address", "bool"), bytes.fromhex(call.data[10:])) == (
            target.asset.reference,
            enabled,
        )


@pytest.mark.parametrize("amount", [0, UINT256_MAX, UINT256_MAX + 1])
def test_finite_approval_cannot_use_unlimited_sentinel(target, amount):
    with pytest.raises(ValueError):
        ApproveOperation(target=target, amount=AssetAmount(asset=target.asset, amount=amount))


def test_operation_cannot_override_asset_or_recipient(target):
    other = AssetType(f"{target.asset.chain}/erc20:0x{'12' * 20}")
    with pytest.raises(ValueError, match="target reserve asset"):
        SupplyOperation(target=target, amount=AssetAmount(asset=other, amount=1))
    with pytest.raises(ValidationError, match="Extra inputs"):
        BorrowOperation.model_validate(
            {
                "target": target,
                "amount": AssetAmount(asset=target.asset, amount=1),
                "on_behalf_of": "0x" + "12" * 20,
            }
        )


def test_safe_wrapper_keeps_borrowed_proceeds_and_debt_in_fund(target):
    operator = target.fund.model_copy(update={"address": "0x" + "12" * 20})
    profile = SafeRoles(
        kind="safe_roles",
        fund=target.fund,
        operator=operator,
        roles_module="0x" + "34" * 20,
        role_key="0x" + "56" * 32,
    )
    accounts = StrategyAccounts(bindings=(AccountBinding(name=target.account, profile=profile),))
    call = plan_call(
        BorrowOperation(target=target, amount=AssetAmount(asset=target.asset, amount=123))
    )
    prepared = prepare_call(accounts, target.account, call)
    assert prepared.submitter == operator
    assert prepared.to == profile.roles_module
    outer = decode(
        ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
        bytes.fromhex(prepared.data[10:]),
    )
    assert (outer[0], outer[1], outer[3], outer[5]) == (target.reserve.pool, 0, 0, True)
    inner = decode(("address", "uint256", "uint256", "uint16", "address"), outer[2][4:])
    assert inner[4] == target.fund.address
    assert inner[4] != operator.address
