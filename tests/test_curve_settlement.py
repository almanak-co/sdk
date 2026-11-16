"""Adversarial receipt and return checks are component proofs, not fork execution."""

import gzip
import json

import pytest
from eth_abi import encode

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve import receipts as logs
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.curve.models import Capabilities, Capability, Target
from almanak.connectors.curve.permissions import MAXIMUM, derive, finite_shares
from almanak.connectors.curve.simulation import validate_simulation
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import any_amount, bounded, either, equal, matches
from almanak.models import ZERO_ADDRESS, Account, AssetAmount, SafeRoles
from tests.retained_fixtures import retained


@pytest.fixture
def target():
    path = retained("2026-09-07-rollout/curve-initial-discovery") / "curve-reader-capture.json.gz"
    captured = json.loads(gzip.decompress(path.read_bytes()))
    pool = CurvePool.model_validate_json(json.dumps(captured["source"]))
    return Target(
        pool=pool,
        allocation="Main",
        account="Fund",
        fund=Account(chain=pool.chain, address="0x" + "12" * 20),
        funding_asset=pool.coins[1],
        trading_asset=pool.coins[0],
        liquidity_enabled=True,
        single_coin_exit=pool.coins[1],
    )


def amounts(target, values):
    return AssetAmounts(
        tuple(AssetAmount(asset=a, amount=v) for a, v in zip(target.assets, values, strict=True))
    )


def event(emitter, signature, addresses, types, values):
    return {
        "address": emitter,
        "topics": [signature, *("0x" + encode(("address",), (a,)).hex() for a in addresses)],
        "data": "0x" + encode(types, values).hex(),
    }


def transfer(asset, sender, receiver, amount):
    return event(asset.reference, logs.TRANSFER, (sender, receiver), ("uint256",), (amount,))


def swap_receipt(target):
    operation = ops.Swap(
        target=target,
        amount=AssetAmount(asset=target.assets[1], amount=100),
        minimum=AssetAmount(asset=target.assets[0], amount=90),
        deadline=1,
    )
    receipt = {
        "logs": [
            event(
                target.pool.pool,
                logs.SWAP,
                (target.fund.address,),
                ("int128", "uint256", "int128", "uint256"),
                (1, 100, 0, 95),
            ),
            transfer(target.assets[1], target.fund.address, target.pool.pool, 100),
            transfer(target.assets[0], target.pool.pool, target.fund.address, 95),
        ]
    }
    return operation, receipt


def test_swap_effects_retain_ordered_caip_assets_account_and_share_scope(target):
    operation, receipt = swap_receipt(target)
    result = logs.movement(receipt, operation)
    result.validate_target(target)
    assert tuple(v.amount for v in result.asset_changes) == (95, -100)
    assert all(v.key.account == target.account for v in result.asset_changes)
    assert result.share_change.amount == 0
    assert result.share_change.key.asset == target.pool.shares_asset
    validate_simulation(operation, "0x" + encode(("uint256",), (95,)).hex())


@pytest.mark.parametrize("attack", ["recipient", "amount", "extra_action", "extra_shares"])
def test_swap_rejects_unattributed_transfers_and_additional_pool_actions(target, attack):
    operation, receipt = swap_receipt(target)
    if attack == "recipient":
        receipt["logs"][2]["topics"][2] = "0x" + encode(("address",), (ZERO_ADDRESS,)).hex()
    elif attack == "amount":
        receipt["logs"][2]["data"] = "0x" + encode(("uint256",), (94,)).hex()
    elif attack == "extra_action":
        receipt["logs"].append(receipt["logs"][0])
    else:
        receipt["logs"].append(
            transfer(target.pool.shares_asset, ZERO_ADDRESS, target.fund.address, 1)
        )
    with pytest.raises(DataError):
        logs.movement(receipt, operation)


def test_proportional_removal_has_empty_fee_array_and_explicit_zero_transfer(target):
    operation = ops.Close(
        target=target,
        shares=AssetAmount(asset=target.pool.shares_asset, amount=10),
        minimums=amounts(target, (0, 7)),
        deadline=1,
    )
    receipt = {
        "logs": [
            event(
                target.pool.pool,
                logs.REMOVE,
                (target.fund.address,),
                ("uint256[]", "uint256[]", "uint256"),
                ((0, 8), (), 100),
            ),
            transfer(target.assets[0], target.pool.pool, target.fund.address, 0),
            transfer(target.assets[1], target.pool.pool, target.fund.address, 8),
            transfer(target.pool.shares_asset, target.fund.address, ZERO_ADDRESS, 10),
        ]
    }
    result = logs.movement(receipt, operation)
    assert result.share_change.amount == -10
    assert tuple(v.amount for v in result.asset_changes) == (0, 8)
    validate_simulation(operation, "0x" + encode(("uint256[]",), ((0, 8),)).hex())
    with pytest.raises(DataError, match="exit minima"):
        validate_simulation(operation, "0x" + encode(("uint256[]",), ((8,),)).hex())


def test_single_coin_removal_burns_only_retained_shares_and_selected_output(target):
    operation = ops.Decrease(
        target=target,
        shares=AssetAmount(asset=target.pool.shares_asset, amount=10),
        minimums=amounts(target, (0, 7)),
        output_asset=target.assets[1],
        deadline=1,
    )
    receipt = {
        "logs": [
            event(
                target.pool.pool,
                logs.ONE,
                (target.fund.address,),
                ("int128", "uint256", "uint256", "uint256"),
                (1, 10, 8, 100),
            ),
            transfer(target.assets[1], target.pool.pool, target.fund.address, 8),
            transfer(target.pool.shares_asset, target.fund.address, ZERO_ADDRESS, 10),
        ]
    }
    assert logs.movement(receipt, operation).share_change.amount == -10
    validate_simulation(operation, "0x" + encode(("uint256",), (8,)).hex())
    wrong = operation.model_copy(
        update={"shares": AssetAmount(asset=target.pool.shares_asset, amount=11)}
    )
    with pytest.raises(DataError, match="share burn"):
        logs.movement(receipt, wrong)


def test_addition_requires_exact_inputs_and_minted_share_minimum(target):
    operation = ops.Mint(
        target=target,
        amounts=amounts(target, (10, 0)),
        minimum_shares=AssetAmount(asset=target.pool.shares_asset, amount=8),
        deadline=1,
    )
    receipt = {
        "logs": [
            event(
                target.pool.pool,
                logs.ADD,
                (target.fund.address,),
                ("uint256[]", "uint256[]", "uint256", "uint256"),
                ((10, 0), (1, 0), 999, 100),
            ),
            transfer(target.assets[0], target.fund.address, target.pool.pool, 10),
            transfer(target.pool.shares_asset, ZERO_ADDRESS, target.fund.address, 9),
        ]
    }
    assert logs.movement(receipt, operation).share_change.amount == 9
    validate_simulation(operation, "0x" + encode(("uint256",), (9,)).hex())
    with pytest.raises(DataError, match="minimum"):
        validate_simulation(operation, "0x" + encode(("uint256",), (7,)).hex())


@pytest.mark.parametrize("raw", ["0x01", "0x", "invalid", "0x" + "00" * 64])
def test_simulation_rejects_noncanonical_protocol_return(target, raw):
    operation, _ = swap_receipt(target)
    with pytest.raises(DataError):
        validate_simulation(operation, raw)


@pytest.mark.parametrize("count", [2, 3, 8])
def test_permission_tree_fits_roles_and_carries_scope_without_amounts(target, count):
    coins = target.assets + tuple(
        AssetType(f"{target.pool.chain.chain_id}/erc20:0x{index:040x}")
        for index in range(100, 100 + count - 2)
    )
    target = target.model_copy(update={"pool": target.pool.model_copy(update={"coins": coins})})
    profile = SafeRoles(
        kind="safe_roles",
        fund=target.fund,
        operator=Account(chain=target.fund.chain, address="0x" + "34" * 20),
        roles_module="0x" + "56" * 20,
        role_key="0x" + "ab" * 32,
    )
    grants = derive(profile, Capabilities(requirements=(Capability(target=target),)))
    assert all(len(grant.conditions) <= 256 for grant in grants)
    assert not any(c.operator == 28 for grant in grants for c in grant.conditions)
    # Scope-only authority (blueprint 27): any nonzero uint128 swap input and any uint128
    # addition amounts, while the pool, coin indices, nonzero minimum and Safe receiver
    # stay pinned.
    swap = next(g for g in grants if g.signature.startswith("exchange("))
    for branch in swap.condition.children:
        assert branch.children[2] == bounded(MAXIMUM)
        assert branch.children[3] == finite_shares()
        assert branch.children[-1] == equal("address", target.fund.address)
    add = next(g for g in grants if g.signature.startswith("add_liquidity("))
    deposit = either(equal("uint256", 0), bounded(MAXIMUM))
    assert add.condition.children[0].children == (deposit,) * count
    assert add.condition.children[1] == finite_shares()
    approvals = [g for g in grants if g.signature == "approve(address,uint256)"]
    assert {g.target for g in approvals} == {a.reference for a in target.assets}
    for grant in approvals:
        assert grant.condition == matches(equal("address", target.pool.pool), any_amount())
    assert not any(
        g.target == target.pool.pool and g.signature.startswith("approve(") for g in grants
    )
    assert not any("admin" in g.signature or "permit" in g.signature for g in grants)
