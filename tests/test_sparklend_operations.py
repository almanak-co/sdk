"""Unsigned shared-ABI calls retain SparkLend identity and fund ownership."""

import pytest
from eth_abi import decode
from eth_utils import keccak
from pydantic import TypeAdapter

from almanak.connectors.aave_v3 import operations as aave
from almanak.connectors.pool_lending_operations import BorrowOperation as UnboundBorrow
from almanak.connectors.sparklend import operations as spark
from almanak.connectors.sparklend.domain import Reserve, Target
from almanak.models import AssetAmount
from tests.test_aave_v3_operations import target as target


@pytest.fixture
def spark_target(target):
    return Target(
        allocation=target.allocation,
        account=target.account,
        fund=target.fund,
        reserve=Reserve(
            chain=target.fund.chain,
            pool="0xc13e21b648a5ee794902342038ff3adab66be987",
            asset=target.asset,
        ),
    )


@pytest.mark.parametrize(
    "kind,fields,signature,types,fund_index",
    [
        ("supply", {}, "supply", ("address", "uint256", "address", "uint16"), 2),
        ("borrow", {}, "borrow", ("address", "uint256", "uint256", "uint16", "address"), 4),
        ("withdraw", {"full": False}, "withdraw", ("address", "uint256", "address"), 2),
        ("withdraw", {"full": True}, "withdraw", ("address", "uint256", "address"), 2),
        ("repay", {"full": False}, "repay", ("address", "uint256", "uint256", "address"), 3),
        ("repay", {"full": True}, "repay", ("address", "uint256", "uint256", "address"), 3),
    ],
)
def test_pool_operation_preserves_exact_spark_target(
    spark_target, kind, fields, signature, types, fund_index
):
    target = spark_target
    operation = TypeAdapter(spark.SparkOperation).validate_python(
        {
            "kind": kind,
            "target": target,
            "amount": AssetAmount(asset=target.asset, amount=123),
            **fields,
        }
    )
    call = spark.plan_call(operation)
    assert call.to == target.reserve.pool and call.fund == target.fund
    assert call.data[2:10] == keccak(text=f"{signature}({','.join(types)})")[:4].hex()
    decoded = decode(types, bytes.fromhex(call.data[10:]))
    assert decoded[0] == target.asset.reference and decoded[fund_index] == target.fund.address
    assert operation.target.reserve.kind == "sparklend"
    assert TypeAdapter(spark.SparkOperation).validate_json(operation.model_dump_json()) == operation
    with pytest.raises(ValueError):
        TypeAdapter(aave.AaveOperation).validate_json(operation.model_dump_json())


def test_connector_target_cannot_be_erased_through_the_shared_operation_base(spark_target, target):
    aave_target, target = target, spark_target
    amount = AssetAmount(asset=target.asset, amount=1)
    with pytest.raises(ValueError):
        UnboundBorrow(target=target, amount=amount)
    with pytest.raises(ValueError):
        spark.BorrowOperation(target=aave_target, amount=amount)
    with pytest.raises(ValueError):
        aave.BorrowOperation(target=target, amount=amount)


def test_approvals_and_collateral_remain_spark_pool_scoped(spark_target):
    target = spark_target
    operation = spark.ApproveOperation(
        target=target, amount=AssetAmount(asset=target.asset, amount=2)
    )
    call = spark.plan_call(operation)
    assert call.to == target.asset.reference
    assert decode(("address", "uint256"), bytes.fromhex(call.data[10:])) == (target.reserve.pool, 2)
    call = spark.plan_call(spark.CollateralOperation(target=target, enabled=False))
    assert call.to == target.reserve.pool
    assert decode(("address", "bool"), bytes.fromhex(call.data[10:])) == (
        target.asset.reference,
        False,
    )
