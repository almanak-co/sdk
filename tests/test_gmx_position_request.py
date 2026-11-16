"""GMX ax position requests validate strictly and read through the pinned reader."""

import json

import pytest
from pydantic import ValidationError

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.gmx_v2.domain import Contracts, Market
from almanak.connectors.gmx_v2.position_request import GmxPositionRequest, read_gmx_position
from almanak.models import ZERO_ADDRESS, Account, Block, Chain

CHAIN = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
OTHER_CHAIN = Chain(chain_id=ChainId("eip155:43114"), environment="mainnet")
FUND = Account(chain=CHAIN, address=AccountReference("0x" + "55" * 20))
BLOCK = Block(chain=CHAIN, number=123, hash="0x" + "ab" * 32, timestamp=1_700_000_000)


def market(chain):
    reference = str(chain.chain_id)
    assets = tuple(AssetType(f"{reference}/erc20:0x{i:040x}") for i in range(1, 5))
    contracts = Contracts(
        **{field: f"0x{i:040x}" for i, field in enumerate(Contracts.model_fields, 10)}
    )
    return Market(
        chain=chain,
        share_asset=assets[0],
        index_asset=assets[1],
        long_asset=assets[1],
        short_asset=assets[2],
        wrapped_native=assets[1],
        contracts=contracts,
    )


def request(chain=CHAIN):
    value = market(chain)
    return GmxPositionRequest(market=value, collateral_asset=value.long_asset, side="long")


class MockReader:
    """Replays the pinned Reader getMarket/getPosition sequence without RPC."""

    def __init__(self, chain, request_market, *, addresses, numbers, flags):
        self.chain = chain
        self.market = request_market
        self.addresses = addresses
        self.numbers = numbers
        self.flags = flags
        self.calls = []

    def call(self, to, signature, block, *, inputs=(), args=(), outputs=("uint256",)):
        self.calls.append((to, signature, args))
        if signature == "getMarket(address,address)":
            assets = (
                self.market.share_asset,
                self.market.index_asset,
                self.market.long_asset,
                self.market.short_asset,
            )
            return (tuple(asset.reference for asset in assets),)
        if signature == "getPosition(address,bytes32)":
            return ((self.addresses, self.numbers, self.flags),)
        raise AssertionError(f"unexpected call {signature}")


def present_reader(value):
    return MockReader(
        CHAIN,
        value.market,
        addresses=(
            FUND.address,
            value.market.share_asset.reference,
            value.collateral_asset.reference,
        ),
        numbers=(10**30, 5, 7, -2, 1, 2, 3, 4, 100, 200),
        flags=(True,),
    )


def test_read_gmx_position_echoes_request_and_pinned_block():
    value = request()
    reader = present_reader(value)
    result = read_gmx_position(reader, value, FUND, BLOCK)
    assert result["product"] == "gmx_perp_position"
    assert result["request"] == value.model_dump(mode="json")
    observation = result["observation"]
    assert observation["block"] == BLOCK.model_dump(mode="json")
    assert observation["target"]["fund"] == FUND.model_dump(mode="json")
    position = observation["position"]
    assert position["size_usd_e30"] == 10**30
    assert position["index_amount"] == {"asset": str(value.market.index_asset), "amount": 5}
    assert position["collateral"] == {"asset": str(value.collateral_asset), "amount": 7}
    assert position["pending_impact_amount"] == -2
    json.dumps(result)
    store = value.market.contracts.data_store
    assert reader.calls[0] == (
        value.market.contracts.reader,
        "getMarket(address,address)",
        (store, value.market.share_asset.reference),
    )
    assert reader.calls[1][0] == value.market.contracts.reader
    assert reader.calls[1][1] == "getPosition(address,bytes32)"
    assert reader.calls[1][2][0] == store
    assert len(reader.calls[1][2][1]) == 32


def test_read_gmx_position_reports_absent_position_as_null():
    value = request()
    reader = MockReader(
        CHAIN, value.market, addresses=(ZERO_ADDRESS,) * 3, numbers=(0,) * 10, flags=(False,)
    )
    result = read_gmx_position(reader, value, FUND, BLOCK)
    assert result["observation"]["position"] is None


def test_malformed_request_json_is_refused():
    with pytest.raises(ValidationError):
        GmxPositionRequest.model_validate_json("{not json")
    with pytest.raises(ValidationError):
        extra = request().model_dump(mode="json") | {"extra": 1}
        GmxPositionRequest.model_validate_json(json.dumps(extra))
    incomplete = request().model_dump(mode="json")
    del incomplete["side"]
    with pytest.raises(ValidationError):
        GmxPositionRequest.model_validate_json(json.dumps(incomplete))
    bare_chain = request().model_dump(mode="json")
    bare_chain["market"]["chain"] = {"chain_id": "42161", "environment": "mainnet"}
    with pytest.raises(ValidationError):
        GmxPositionRequest.model_validate_json(json.dumps(bare_chain))
    wrong_collateral = request().model_dump(mode="json")
    wrong_collateral["collateral_asset"] = str(request().market.share_asset)
    with pytest.raises(ValidationError, match="collateral"):
        GmxPositionRequest.model_validate_json(json.dumps(wrong_collateral))


def test_unsupported_chain_is_refused():
    unsupported = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    with pytest.raises(ValidationError, match="unsupported on eip155:1"):
        request(unsupported)


def test_request_chain_must_match_reader_chain():
    value = request()
    reader = present_reader(value)
    reader.chain = OTHER_CHAIN
    with pytest.raises(ValueError, match="request chain differs"):
        read_gmx_position(reader, value, FUND, BLOCK)


def test_account_chain_must_match_reader_chain():
    value = request()
    reader = present_reader(value)
    account = Account(chain=OTHER_CHAIN, address=AccountReference("0x" + "55" * 20))
    with pytest.raises(ValueError, match="account chain differs"):
        read_gmx_position(reader, value, account, BLOCK)
