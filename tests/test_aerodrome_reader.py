"""Captured canonical reads and adversarial configured-role substitutions."""

import gzip
import json

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.caip import AssetType
from almanak.connectors.aerodrome.deployments import (
    require_direct_fund,
    revisions,
    verify_pool,
    verify_router,
)
from almanak.connectors.aerodrome.domain import AerodromePool, Contracts, PoolObservation
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.reader import fund_position, quote, read_pool
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import ZERO_ADDRESS, Account, AssetAmount
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/aerodrome-base-discovery")
CAPTURE = json.loads(gzip.decompress((EVIDENCE / "reader.json.gz").read_bytes()))


class Replay(EthereumReader):
    def __init__(self, index=0):
        self.row = CAPTURE["rows"][index]
        self.source = AerodromePool.model_validate_json(json.dumps(self.row["source"]))
        self.contracts = Contracts.model_validate_json(json.dumps(self.row["contracts"]))
        super().__init__(self.source.chain, "http://unused.invalid")
        self.records = {
            json.dumps([r["method"], r["params"]], sort_keys=True): r["result"]
            for r in CAPTURE["requests"]
        }

    def _rpc(self, method, params, *, allow_none=False):
        key = json.dumps([method, params], sort_keys=True)
        assert key in self.records, "test cannot acquire unrecorded provider data"
        return self.records[key]

    def change_call(self, address, signature, types, values, argument=""):
        selector = "0x" + keccak(text=signature)[:4].hex()
        matches = [
            key
            for key in self.records
            if '"eth_call"' in key and address in key and selector in key and argument in key
        ]
        assert len(matches) == 1
        self.records[matches[0]] = "0x" + encode(types, values).hex()

    def code_key(self, address):
        return next(k for k in self.records if '"eth_getCode"' in k and address in k)


@pytest.mark.parametrize("index", [0, 1])
def test_verified_pool_reader_reproduces_canonical_observation_and_quotes(index):
    with Replay(index) as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        source, contracts, row = reader.source, reader.contracts, reader.row
        identities = verify_pool(reader, source, block) + verify_router(
            reader, source, contracts, block
        )
        assert [i.model_dump(mode="json") for i in identities] == row["identities"]
        observed = read_pool(reader, source, block)
        assert observed == PoolObservation.model_validate_json(json.dumps(row["observed"]))
        assert observed.shares.identifier == source.shares_asset
        assert (observed.balance0, observed.balance1) == (observed.reserve0, observed.reserve1)
        assert observed.deposited_shares == 0
        fund = Account.model_validate_json(json.dumps(row["fund"]))
        shares, fees = fund_position(reader, observed, fund)
        assert [shares, list(fees)] == row["position"]
        for item in row["quotes"]:
            amount = AssetAmount.model_validate_json(json.dumps(item["amount"]))
            output = AssetType(item["output_asset"])
            assert quote(reader, source, contracts, block, amount, output) == item["result"]


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("role_index", range(6))
def test_every_runtime_identity_is_checked(index, role_index):
    with Replay(index) as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        key = reader.code_key(reader.row["identities"][role_index]["address"])
        raw = bytearray.fromhex(reader.records[key][2:])
        raw[0] ^= 1
        reader.records[key] = "0x" + raw.hex()
        with pytest.raises(DataError, match="code differs"):
            verify_pool(reader, reader.source, block)
            verify_router(reader, reader.source, reader.contracts, block)


@pytest.mark.parametrize("signature", ["factory()", "token0()", "token1()", "stable()"])
def test_configured_pool_storage_is_correlated(signature):
    with Replay() as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        types, values = (
            (("bool",), (True,))
            if signature == "stable()"
            else (
                ("address",),
                (ZERO_ADDRESS,),
            )
        )
        reader.change_call(reader.source.pool, signature, types, values)
        with pytest.raises(DataError, match="storage differs|invariant differs"):
            verify_pool(reader, reader.source, block)


@pytest.mark.parametrize(
    "role,role_index,field",
    [
        ("implementation", 1, "_cachedChainId"),
        ("implementation", 1, "_hashedName"),
        ("pool_fees", 3, "pool"),
        ("pool_fees", 3, "token0"),
        ("router", 4, "defaultFactory"),
        ("router", 4, "weth"),
        ("factory_registry", 5, "fallbackPoolFactory"),
    ],
)
def test_compiled_immutables_cannot_substitute_configured_dependencies(role, role_index, field):
    with Replay() as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        key = reader.code_key(reader.row["identities"][role_index]["address"])
        raw = bytearray.fromhex(reader.records[key][2:])
        for slot in revisions()[role]["immutables"][field]:
            raw[slot["start"] : slot["start"] + slot["length"]] = bytes(slot["length"])
        reader.records[key] = "0x" + raw.hex()
        with pytest.raises(DataError, match="immutables differ"):
            verify_pool(reader, reader.source, block)
            verify_router(reader, reader.source, reader.contracts, block)


def test_factory_registration_is_required():
    with Replay() as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        argument = encode(
            ("address", "address", "bool"),
            (reader.source.currency0.reference, reader.source.currency1.reference, False),
        ).hex()
        reader.change_call(
            reader.source.factory,
            "getPool(address,address,bool)",
            ("address",),
            (ZERO_ADDRESS,),
            argument,
        )
        with pytest.raises(DataError, match="factory registration"):
            verify_pool(reader, reader.source, block)


def test_router_registry_approval_is_required():
    with Replay() as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        reader.change_call(
            reader.row["identities"][5]["address"],
            "isPoolFactoryApproved(address)",
            ("bool",),
            (False,),
        )
        with pytest.raises(DataError, match="does not approve"):
            verify_router(reader, reader.source, reader.contracts, block)


def test_router_trusted_forwarder_cannot_be_a_direct_fund():
    with Replay() as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        slot = revisions()["router"]["immutables"]["_trustedForwarder"][0]
        raw = bytes.fromhex(reader.records[reader.code_key(reader.contracts.router)][2:])
        address = "0x" + raw[slot["start"] + 12 : slot["start"] + 32].hex()
        fund = Account.model_validate_json(
            json.dumps({"chain": reader.source.chain.model_dump(mode="json"), "address": address})
        )
        target = Target(
            allocation="pool",
            account="fund",
            fund=fund,
            pool=reader.source,
            contracts=reader.contracts,
            funding_asset=reader.source.currency0,
            liquidity_enabled=True,
        )
        with pytest.raises(DataError, match="trusted forwarder"):
            require_direct_fund(reader, target, block)
        normal = Account.model_validate_json(json.dumps(reader.row["fund"]))
        require_direct_fund(reader, target.model_copy(update={"fund": normal}), block)
