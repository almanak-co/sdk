"""Shared execution wiring replays mined Crypto receipts without provider access."""

import copy
import json
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.curve import abi, liquidity_quotes, receipts
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve.models import Target
from almanak.connectors.curve.reader import read_pool
from almanak.connectors.curve.simulation import validate_simulation
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.curve.tricrypto_receipts import CLAIM
from almanak.gateway.ethereum import DataError
from almanak.models import Account, AssetAmount
from tests.test_curve_settlement import amounts
from tests.test_curve_tricrypto_observation import observation as observation
from tests.test_curve_tricrypto_transitions import CASES


def inputs(case):
    pool = TriCryptoObservation.model_validate_json(json.dumps(case["observation"]))
    args = case.get("inputs", case)
    mode = case.get("mode", "single")
    target = Target(
        pool=pool.source,
        allocation="Main",
        account="Fund",
        fund=Account(chain=pool.source.chain, address=case["transaction"]["from"]),
        funding_asset=pool.source.coins[0],
        trading_asset=pool.source.coins[1],
        liquidity_enabled=True,
        single_coin_exit=pool.source.coins[args.get("index", 0)],
    )
    if mode == "exchange":
        operation = ops.Swap(
            target=target,
            deadline=case["timestamp"] + 100,
            amount=AssetAmount(asset=target.assets[args["input_index"]], amount=args["amount"]),
            minimum=AssetAmount(asset=target.assets[args["output_index"]], amount=1),
        )
        types = ("uint256", "uint256", "uint256", "uint256", "bool", "address")
        name, minimum_index = "exchange", 3
    elif mode.startswith("add_"):
        operation = ops.Mint(
            target=target,
            deadline=case["timestamp"] + 100,
            amounts=amounts(target, args["amounts"]),
            minimum_shares=AssetAmount(asset=target.pool.shares_asset, amount=1),
        )
        types = ("uint256[3]", "uint256", "bool", "address")
        name, minimum_index = "add_liquidity", 1
    else:
        operation = ops.Close(
            target=target,
            deadline=case["timestamp"] + 100,
            shares=AssetAmount(asset=target.pool.shares_asset, amount=args["shares"]),
            minimums=amounts(target, (0, 0, 0)),
            output_asset=None if mode == "proportional" else target.assets[args["index"]],
        )
        types = (
            ("uint256", "uint256[3]", "bool", "address", "bool")
            if mode == "proportional"
            else ("uint256", "uint256", "uint256", "bool", "address")
        )
        name = "remove_liquidity" if mode == "proportional" else "remove_liquidity_one_coin"
        minimum_index = None
    return pool, operation, types, name, minimum_index


@pytest.mark.parametrize("case", CASES)
def test_native_calldata_and_receipts_use_shared_protocol_boundary(case):
    pool, operation, types, name, minimum_index = inputs(case)
    native = bytes.fromhex(case["transaction"]["input"][2:])
    expected_selector = keccak(text=name + "(" + ",".join(types) + ")")[:4]
    assert native[:4] == expected_selector
    expected = list(decode(types, native[4:]))
    # Native kernel probes used zero minima; admitted entries require positive bounds.
    if minimum_index is not None:
        assert expected[minimum_index] == 0
        expected[minimum_index] = 1
    encoded = abi.call(operation)
    assert encoded.to == case["transaction"]["to"]
    assert encoded.data == "0x" + (expected_selector + encode(types, expected)).hex()
    result = receipts.movement(case["receipt"], operation, SimpleNamespace(pool=pool))
    assert tuple(v.amount for v in result.asset_changes) == tuple(case["predicted"]["amounts"])
    assert (
        result.share_change.amount
        == case["predicted"]["state"]["total_supply"]
        - pool.total_supply
        - case["predicted"]["admin_shares"]
    )
    if isinstance(operation, ops.Swap):
        output = next(v.amount for v in result.asset_changes if v.amount > 0)
    elif isinstance(operation, ops.Addition):
        output = result.share_change.amount
    elif operation.output_asset is None:
        output = tuple(v.amount for v in result.asset_changes)
    else:
        output = result.asset_changes[operation.target.assets.index(operation.output_asset)].amount
    returns = ("uint256[3]",) if name == "remove_liquidity" else ("uint256",)
    validate_simulation(operation, "0x" + encode(returns, (output,)).hex())


def test_shared_reader_selects_captured_crypto_revision(observation):
    original, reader, block = observation
    assert read_pool(reader, original.source, block) == original


@pytest.mark.parametrize("attack", ["duplicate", "receiver", "transfer", "missing_state"])
def test_admin_claim_cannot_hide_unattributed_transfers(attack):
    case = next(c for c in CASES if any(log["topics"][0] == CLAIM for log in c["receipt"]["logs"]))
    pool, operation, *_ = inputs(case)
    receipt = copy.deepcopy(case["receipt"])
    before = SimpleNamespace(pool=pool)
    claim = next(log for log in receipt["logs"] if log["topics"][0] == CLAIM)
    if attack == "duplicate":
        receipt["logs"].append(claim)
    elif attack == "receiver":
        claim["topics"][1] = "0x" + encode(("address",), (operation.target.fund.address,)).hex()
    elif attack == "transfer":
        transfer = next(log for log in receipt["logs"] if log["topics"][0] == receipts.TRANSFER)
        transfer["data"] = "0x" + encode(("uint256",), (123,)).hex()
    else:
        before = None
    with pytest.raises(DataError):
        receipts.movement(receipt, operation, before)


def test_family_return_encoding_cannot_be_interchanged():
    case = next(c for c in CASES if c.get("mode") == "proportional")
    _, operation, *_ = inputs(case)
    with pytest.raises(DataError, match="malformed protocol data"):
        validate_simulation(operation, "0x" + encode(("uint256[]",), ((0, 0, 0),)).hex())


@pytest.mark.parametrize("case", [c for c in CASES if c.get("mode", "single") != "exchange"])
def test_liquidity_bounds_retain_native_fee_and_admin_semantics(case):
    pool, operation, *_ = inputs(case)
    pool = pool.model_copy(
        update={"block": pool.block.model_copy(update={"timestamp": case["timestamp"]})}
    )
    if isinstance(operation, ops.Addition):
        assert (
            liquidity_quotes.minted(pool, tuple(v.amount for v in operation.amounts.root))
            == case["actual_lp_change"]
        )
    else:
        index = (
            None
            if operation.output_asset is None
            else operation.target.assets.index(operation.output_asset)
        )
        assert liquidity_quotes.removed(pool, operation.shares.amount, index) == tuple(
            case["predicted"]["amounts"]
        )


def test_fee_receiver_cannot_be_adopted_as_the_strategy_fund():
    from almanak.connectors.curve.observations import Snapshot
    from almanak.connectors.curve.positions import Ownership

    pool, operation, *_ = inputs(CASES[0])
    target = operation.target.model_copy(
        update={
            "fund": operation.target.fund.model_copy(update={"address": pool.accounting.receiver})
        }
    )
    empty = amounts(target, (0, 0, 0))
    with pytest.raises(ValueError, match="fee receiver must be distinct"):
        Snapshot(
            target=target,
            pool=pool,
            owned=Ownership(target=target, inventory=empty),
            wallets=empty,
            allowances=(),
            native_balance=None,
            payer=None,
            position=None,
        )
