"""Pinned market and funded account observations replay without provider fallback."""

import copy
import gzip
import json
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.connectors.morpho_blue.domain import AccountObservation, Market
from almanak.connectors.morpho_blue.reader import read_account, read_market
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Block

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/morpho-blue-reader"


def recorded(chain, funded):
    kind = "funded-reader" if funded else "draft-reader"
    return json.loads(gzip.decompress((EVIDENCE / f"{chain}-{kind}.json.gz").read_bytes()))


def key(method, params):
    return json.dumps((method, params), sort_keys=True)


class Replay(EthereumReader):
    def __init__(self, raw):
        self.source = Market.model_validate_json(json.dumps(raw["source"]))
        self.block = Block.model_validate_json(json.dumps(raw["market"]["block"]))
        super().__init__(
            self.source.chain,
            "http://retained.invalid",
            local_transport=self.source.chain.environment != "mainnet",
        )
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
            and params[0]["to"]
            not in (self.source.loan_asset.reference, self.source.collateral_asset.reference)
        ):
            raise DataError("unavailable", "retained provider input unavailable")
        try:
            return copy.deepcopy(self.responses[key(method, params)])
        except KeyError:
            raise DataError(
                "unavailable", "no captured RPC input; online fallback forbidden"
            ) from None


@pytest.mark.parametrize("chain", [1, 8453])
@pytest.mark.parametrize("funded", [False, True])
def test_integrated_market_and_fund_reader_reproduces_retained_inputs(chain, funded):
    raw = recorded(chain, funded)
    with Replay(raw) as reader:
        assert reader.pin_block(reader.block.number) == reader.block
        observed = read_market(reader, reader.source, reader.block)
        assert observed.model_dump(mode="json") == raw["market"]
        for account in raw.get("accounts", []):
            expected = AccountObservation.model_validate_json(json.dumps(account))
            assert read_account(reader, observed, expected.fund) == expected
        if funded:
            assert raw["accounts"][0]["position"]["supply_shares"] > 0
            assert raw["accounts"][1]["position"]["borrow_shares"] > 0


@pytest.mark.parametrize("signature", ["price()", "decimals()"])
def test_missing_oracle_inputs_remain_missing_while_accounting_survives(signature):
    raw = recorded(8453, True)
    with Replay(raw) as reader:
        reader.pin_block(reader.block.number)
        reader.deny = "0x" + keccak(text=signature)[:4].hex()
        observed = read_market(reader, reader.source, reader.block)
        assert observed.price is None and observed.price_error
        assert observed.accounting.model_dump() == raw["market"]["accounting"]


@pytest.mark.parametrize("role", ["core", "irm", "oracle"])
def test_runtime_revision_mutation_fails_closed(role):
    raw = recorded(1, False)
    with Replay(raw) as reader:
        reader.pin_block(reader.block.number)
        contract = getattr(reader.source, role)
        identity = key(
            "eth_getCode", [contract, {"blockHash": reader.block.hash, "requireCanonical": True}]
        )
        reader.responses[identity] = "0x00" + reader.responses[identity][4:]
        with pytest.raises(DataError, match="revision"):
            read_market(reader, reader.source, reader.block)


def test_reader_refuses_mixed_block_scope():
    raw = recorded(1, False)
    with Replay(raw) as reader:
        reader.pin_block(reader.block.number)
        other = reader.block.model_copy(update={"timestamp": reader.block.timestamp + 1})
        with pytest.raises(DataError, match="pinned"):
            read_market(reader, reader.source, other)
