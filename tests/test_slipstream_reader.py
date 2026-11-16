"""Pinned Slipstream admission, mutable fee context and exact clone refusals."""

import gzip
import json
from pathlib import Path

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.aerodrome_slipstream.deployments import (
    require_no_discount,
    revisions,
    verify_fee_modules,
    verify_periphery,
    verify_pool,
)
from almanak.connectors.aerodrome_slipstream.domain import (
    Contracts,
    PoolObservation,
    PoolPriceRequest,
    SlipstreamPool,
)
from almanak.connectors.aerodrome_slipstream.reader import quote, read_pool
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import ZERO_ADDRESS, AssetAmount

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "slipstream-base-reader.json.gz").read_bytes()
    )
)


class Replay(EthereumReader):
    def __init__(self, row=CAPTURE):
        self.source = SlipstreamPool.model_validate_json(json.dumps(row["source"]))
        super().__init__(self.source.chain, "http://unused.invalid")
        self.records = {
            json.dumps([r["method"], r["params"]], sort_keys=True): r["result"]
            for r in row["requests"]
        }
        self.seen = set()

    def _rpc(self, method, params, *, allow_none=False):
        key = json.dumps([method, params], sort_keys=True)
        assert key in self.records, "test cannot acquire unrecorded provider data"
        self.seen.add(key)
        return self.records[key]

    def change_call(self, address, signature, types, values, *, argument=None):
        selector = "0x" + keccak(text=signature)[:4].hex()
        matches = [
            key
            for key in self.records
            if '"eth_call"' in key
            and address in key
            and selector in key
            and (argument is None or argument in key)
        ]
        assert len(matches) == 1
        self.records[matches[0]] = "0x" + encode(types, values).hex()


def setup(reader):
    contracts = Contracts.model_validate_json(json.dumps(CAPTURE["contracts"]))
    return reader.pin_block(CAPTURE["block"]["number"]), contracts


def test_pinned_reader_replays_revisions_spot_units_and_exact_quotes():
    with Replay() as reader:
        block, contracts = setup(reader)
        source = reader.source
        identities = verify_pool(reader, source, block) + verify_periphery(
            reader, source, contracts, block
        )
        assert [i.model_dump(mode="json") for i in identities] == CAPTURE["identities"]
        observed = read_pool(reader, source, block)
        assert observed == PoolObservation.model_validate_json(json.dumps(CAPTURE["observed"]))
        direct = PoolPriceRequest(source=source, base=source.currency0, quote=source.currency1)
        reverse = PoolPriceRequest(source=source, base=source.currency1, quote=source.currency0)
        assert observed.spot_price(direct) * observed.spot_price(reverse) == 1
        assert "fee" not in source.model_dump() and observed.swap_fee == 332
        for retained in CAPTURE["quotes"]:
            amount = AssetAmount.model_validate_json(json.dumps(retained["amount"]))
            assert quote(reader, source, contracts, block, amount, source.currency1) == tuple(
                retained["result"]
            )
        require_no_discount(reader, source, block, "0x0000000000000000000000000000000000000001")
        assert reader.seen == set(reader.records)


@pytest.mark.parametrize(
    "identity", CAPTURE["identities"], ids=lambda identity: identity["address"]
)
def test_unknown_runtime_or_clone_is_refused(identity):
    with Replay() as reader:
        block, contracts = setup(reader)
        key = next(k for k in reader.records if '"eth_getCode"' in k and identity["address"] in k)
        code = bytearray.fromhex(reader.records[key][2:])
        code[0] ^= 1
        reader.records[key] = "0x" + code.hex()
        with pytest.raises(DataError, match="code differs"):
            verify_pool(reader, reader.source, block)
            verify_periphery(reader, reader.source, contracts, block)


@pytest.mark.parametrize(
    "signature,types,values",
    [
        ("factory()", ("address",), (ZERO_ADDRESS,)),
        ("token0()", ("address",), (ZERO_ADDRESS,)),
        ("token1()", ("address",), (ZERO_ADDRESS,)),
        ("tickSpacing()", ("int24",), (100,)),
        ("maxLiquidityPerTick()", ("uint128",), (1,)),
    ],
)
def test_pool_storage_cannot_substitute_a_market(signature, types, values):
    with Replay() as reader:
        block, _ = setup(reader)
        reader.change_call(reader.source.pool, signature, types, values)
        with pytest.raises(DataError, match="storage differs|spacing or maximum"):
            read_pool(reader, reader.source, block)


def test_factory_registration_and_manager_association_are_checked():
    with Replay() as reader:
        block, contracts = setup(reader)
        reader.change_call(
            reader.source.factory, "getPool(address,address,int24)", ("address",), (ZERO_ADDRESS,)
        )
        with pytest.raises(DataError, match="factory registration"):
            verify_pool(reader, reader.source, block)
    with Replay() as reader:
        block, contracts = setup(reader)
        reader.change_call(reader.source.pool, "nft()", ("address",), (reader.source.factory,))
        with pytest.raises(DataError, match="another NFT manager"):
            verify_periphery(reader, reader.source, contracts, block)


@pytest.mark.parametrize("role", ["swap_fee_module", "unstaked_fee_module", "position_manager"])
def test_compiled_dependencies_must_belong_to_this_factory(role):
    with Replay() as reader:
        block, contracts = setup(reader)
        address = CAPTURE["observed"].get(role, CAPTURE["contracts"].get(role))
        key = next(k for k in reader.records if '"eth_getCode"' in k and address in k)
        raw = bytearray.fromhex(reader.records[key][2:])
        for slot in revisions()[role]["immutables"]["factory"]:
            raw[slot["start"] : slot["start"] + slot["length"]] = bytes(slot["length"])
        reader.records[key] = "0x" + raw.hex()
        with pytest.raises(DataError, match="immutables differ"):
            verify_pool(reader, reader.source, block)
            verify_periphery(reader, reader.source, contracts, block)


@pytest.mark.parametrize("submitter", [ZERO_ADDRESS, "0x0000000000000000000000000000000000000001"])
def test_zero_origin_and_actual_submitter_discounts_are_refused(submitter):
    with Replay() as reader:
        block, _ = setup(reader)
        module = CAPTURE["observed"]["swap_fee_module"]
        encoded = encode(("address",), (submitter,)).hex()
        reader.change_call(module, "discounted(address)", ("uint24",), (1,), argument=encoded)
        with pytest.raises(DataError, match="discounts are unsupported"):
            require_no_discount(reader, reader.source, block, submitter)


def test_absent_fee_modules_use_only_the_verified_factory_contract():
    with Replay() as reader:
        block, _ = setup(reader)
        for method in ("swapFeeModule()", "unstakedFeeModule()"):
            reader.change_call(reader.source.factory, method, ("address",), (ZERO_ADDRESS,))
        assert verify_fee_modules(reader, reader.source, block) == ()
        require_no_discount(reader, reader.source, block)


@pytest.mark.parametrize(
    "field,value",
    [("swap_fee", 100001), ("unstaked_fee", 1000001), ("staked_liquidity", 2**128 - 1)],
)
def test_observation_rejects_impossible_fee_or_liquidity_state(field, value):
    wire = {**CAPTURE["observed"], field: value}
    with pytest.raises(ValueError):
        PoolObservation.model_validate_json(json.dumps(wire))
