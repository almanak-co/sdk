"""Retained canonical reads reproduce Comet revisions, market state and funded accounts."""

import copy
import gzip
import json
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.caip import AssetType
from almanak.connectors.compound_v3.accounting import indices, rates, validate_account
from almanak.connectors.compound_v3.domain import AccountObservation, Market, Revision
from almanak.connectors.compound_v3.reader import read_account, read_market
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Block
from tests.retained_fixtures import retained

ROOT = Path(__file__).parents[1]
EVIDENCE = retained("2026-09-07-rollout/compound-v3-reader")


def recorded(network, funded):
    suffix = "-reader-operations" if funded else "-reader"
    raw = json.loads(gzip.decompress((EVIDENCE / f"{network}{suffix}.json.gz").read_bytes()))
    return raw, raw["reader_snapshot"] if funded else raw


def key(method, params):
    return json.dumps((method, params), sort_keys=True)


class Replay(EthereumReader):
    def __init__(self, raw, data):
        self.source = Market.model_validate_json(json.dumps(data["source"]))
        self.block = Block.model_validate_json(json.dumps(data["market"]["block"]))
        super().__init__(self.source.chain, "http://retained.invalid")
        self.responses = {}
        self.deny = None
        for request in raw["requests"]:
            if request["method"] == "eth_getBlockByNumber" and request["params"][0] == "latest":
                continue
            identity = key(request["method"], request["params"])
            assert self.responses.setdefault(identity, request["result"]) == request["result"]

    def _rpc(self, method, params, *, allow_none=False):
        if (
            self.deny is not None
            and method == "eth_call"
            and params[0]["data"].startswith(self.deny)
        ):
            raise DataError("unavailable", "retained provider input is unavailable")
        try:
            return copy.deepcopy(self.responses[key(method, params)])
        except KeyError:
            raise DataError(
                "unavailable", "no captured RPC input; online fallback is forbidden"
            ) from None


@pytest.mark.parametrize("network", ["mainnet", "base"])
@pytest.mark.parametrize("funded", [False, True])
def test_compiled_market_and_fund_observations_reproduce_pinned_rpc(network, funded):
    raw, data = recorded(network, funded)
    with Replay(raw, data) as reader:
        assert reader.pin_block(reader.block.number) == reader.block
        market = read_market(reader, reader.source, reader.block)
        assert market.model_dump(mode="json") == data["market"]
        accounts = data["accounts"] if funded else [data["account"]]
        for value in accounts:
            expected = AccountObservation.model_validate_json(json.dumps(value))
            observed = read_account(reader, market, expected.fund)
            assert observed == expected
        if funded:
            assert accounts[0]["principal"] > 0 and accounts[1]["principal"] < 0


@pytest.mark.parametrize("mutation", ["proxy", "base_asset", "feed", "block"])
def test_market_scope_and_revision_refuse_unverified_inputs(mutation):
    raw, data = recorded("mainnet", False)
    with Replay(raw, data) as reader:
        block = reader.pin_block(reader.block.number)
        source = reader.source
        if mutation == "proxy":
            identity = key(
                "eth_getCode", [source.comet, {"blockHash": block.hash, "requireCanonical": True}]
            )
            reader.responses[identity] = "0x00" + reader.responses[identity][4:]
        elif mutation == "base_asset":
            source = source.model_copy(
                update={
                    "base_asset": AssetType(
                        data["market"]["collaterals"][0]["asset"]["identifier"]
                    ),
                }
            )
        elif mutation == "feed":
            source = source.model_copy(update={"base_price_feed": "0x" + "ab" * 20})
        else:
            block = block.model_copy(update={"timestamp": block.timestamp + 1})
        with pytest.raises(DataError):
            read_market(reader, source, block)


def test_missing_prices_remain_visible_and_missing_extended_controls_refuse():
    raw, data = recorded("base", False)
    with Replay(raw, data) as reader:
        block = reader.pin_block(reader.block.number)
        reader.deny = "0x" + keccak(text="getPrice(address)")[:4].hex()
        market = read_market(reader, reader.source, block)
        assert market.base_price is None and market.base_price_error
        assert all(c.price is None and c.price_error for c in market.collaterals)
        reader.deny = "0x" + keccak(text="extendedPauseFlags()")[:4].hex()
        with pytest.raises(DataError, match="unavailable"):
            read_market(reader, reader.source, block)


def test_funded_balance_views_cannot_disagree_with_signed_principal():
    _, data = recorded("base", True)
    account = AccountObservation.model_validate_json(json.dumps(data["accounts"][1]))
    with pytest.raises(ValueError, match="balance views disagree"):
        validate_account(account.model_copy(update={"borrowed_base": account.borrowed_base + 1}))
    with pytest.raises(ValueError, match="precede"):
        indices(account.market, account.market.accounting.last_accrual_time - 1)
    assert indices(account.market, account.market.accounting.last_accrual_time) == (
        account.market.accounting.supply_index,
        account.market.accounting.borrow_index,
    )


def test_extended_empty_market_rates_preserve_revision_specific_reserve_checks():
    _, data = recorded("base", True)
    account = AccountObservation.model_validate_json(json.dumps(data["accounts"][1]))
    market = account.market
    market = market.model_copy(
        update={
            "accounting": market.accounting.model_copy(
                update={"total_supply_principal": 0, "total_borrow_principal": 0}
            ),
            "supply_curve": market.supply_curve.model_copy(update={"base_per_second": 1}),
            "borrow_curve": market.borrow_curve.model_copy(update={"base_per_second": 2}),
        }
    )
    assert rates(market) == (0, 0)
    assert rates(market.model_copy(update={"revision": Revision.STANDARD, "controls": None})) == (
        1,
        2,
    )
    supplied = market.model_copy(
        update={
            "accounting": market.accounting.model_copy(update={"total_supply_principal": 10}),
            "base_available": 0,
        }
    )
    assert rates(supplied) == (0, 0)
    assert rates(supplied.model_copy(update={"base_available": 1000})) == (1, 0)
