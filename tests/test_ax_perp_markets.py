"""GMX V2 market resolution requests: validation, enumeration and label matching."""

import json
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak import ax
from almanak.caip import ChainId
from almanak.connectors.gmx_v2 import market_request
from almanak.connectors.gmx_v2.market_request import (
    GmxMarketRequest,
    parse_label,
    read_gmx_market,
    synthetic_index,
)
from almanak.connectors.gmx_v2.networks import roles_for
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Block, Chain
from tests.read_at_support import read_at

CHAIN = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
BLOCK = Block(chain=CHAIN, number=25908472, hash="0x" + "ab" * 32, timestamp=1_700_000_000)
CONTRACTS = roles_for(CHAIN.chain_id)
WETH = "0x" + "82" * 20
USDC = "0x" + "af" * 20
WBTC = "0x" + "2f" * 20
TBTC = "0x" + "7b" * 20
ETH_SHARE = "0x" + "43" * 20
WBTC_SHARE = "0x" + "42" * 20
BTC_USDC_SHARE = "0x" + "47" * 20
BTC_TBTC_SHARE = "0x" + "44" * 20
DOGE_SHARE = "0x" + "68" * 20
SWAP_SHARE = "0x" + "9c" * 20
BTC_INDEX = synthetic_index(CHAIN.chain_id, "BTC")
DOGE_INDEX = synthetic_index(CHAIN.chain_id, "DOGE")
SYMBOLS = {WETH: "WETH", USDC: "USDC", WBTC: "WBTC", TBTC: "tBTC"}
# (share, index, long, short): a deployed index, two synthetic BTC markets with
# different collateral, a synthetic DOGE market on WETH and a swap-only market.
MARKETS = [
    (ETH_SHARE, WETH, WETH, USDC),
    (BTC_USDC_SHARE, BTC_INDEX, WBTC, USDC),
    (BTC_TBTC_SHARE, BTC_INDEX, TBTC, TBTC),
    (DOGE_SHARE, DOGE_INDEX, WETH, USDC),
    (SWAP_SHARE, ZERO_ADDRESS, USDC, WETH),
]
MARKET_LIST = market_request.key("MARKET_LIST")


class FakeReader:
    """Serves the DataStore address set, the Reader market pages, code and symbols."""

    def __init__(self, markets):
        self.chain = CHAIN
        self._markets = markets
        self.calls: list[str] = []

    def pin_block(self, selector):
        return BLOCK

    def _rpc(self, method, params):
        assert method == "eth_getCode" and params[1] == {
            "blockHash": BLOCK.hash,
            "requireCanonical": True,
        }
        self.calls.append(f"eth_getCode {params[0]}")
        return "0x6001" if params[0] in SYMBOLS else "0x"

    def call(self, target, signature, block, inputs=(), args=(), outputs=()):
        self.calls.append(signature)
        if signature == "getAddressCount(bytes32)":
            assert (target, args) == (CONTRACTS.data_store, (MARKET_LIST,))
            return (len(self._markets),)
        if signature == "getMarkets(address,uint256,uint256)":
            assert (target, args) == (
                CONTRACTS.reader,
                (CONTRACTS.data_store, 0, len(self._markets)),
            )
            return (tuple(self._markets),)
        if signature == "symbol()":
            return (SYMBOLS[target],)
        raise AssertionError(f"unexpected call {signature} on {target}")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None


def fake_resolved(share, index, long):
    return SimpleNamespace(
        summary=lambda: {"block": BLOCK.number, "share": share},
        selection=SimpleNamespace(chain=CHAIN.chain_id),
        market=SimpleNamespace(
            index_asset=SimpleNamespace(reference=index),
            long_asset=SimpleNamespace(reference=long),
        ),
    )


def request(query, chain="eip155:42161"):
    return GmxMarketRequest(chain=ChainId(chain), query=query)


@pytest.fixture
def resolved(monkeypatch):
    """Resolution is replayed elsewhere; here it echoes the selected market's tuple."""
    seen = {}

    def capture(reader, selection, block):
        seen["selection"] = selection
        share, index, long, _ = next(
            entry for entry in reader._markets if entry[0] == selection.market.reference
        )
        return fake_resolved(share, index, long)

    monkeypatch.setattr(market_request, "resolve_market", capture)
    return seen


def test_request_refuses_unsupported_chains_and_empty_queries():
    with pytest.raises(ValueError, match="unsupported"):
        request("BTC/USD", chain="eip155:1")
    with pytest.raises(ValueError, match="must not be empty"):
        request("  ")


def test_share_address_parses_only_as_exact_address():
    assert request("0x" + "47" * 20).share_address == "0x" + "47" * 20
    assert request("BTC/USD").share_address is None
    assert request("0x123").share_address is None


def test_synthetic_index_follows_the_venue_derivation():
    digest = keccak(encode(("uint256", "string"), (42161, "BTC")))
    assert BTC_INDEX == "0x" + digest.hex()[-40:]
    # The Arbitrum BTC/USD index token GMX registers, with no contract deployed there.
    assert BTC_INDEX == "0x47904963fc8b2340414262125af798b9655e58cd"


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("BTC/USD", ("BTC", None)),
        ("btc", ("btc", None)),
        ("BTC/USD [WBTC-USDC]", ("BTC", ("WBTC", "USDC"))),
        ("BTC/USD [ WBTC ]", ("BTC", ("WBTC", "WBTC"))),
        ("BTC/EUR", None),
        ("/USD", None),
        ("BTC/USD [WBTC-", None),
        ("BTC/USD [WBTC-]", None),
        ("BTC/USD [WBTC-USDC] extra", None),
    ],
)
def test_label_parsing_accepts_the_venue_notation_only(query, expected):
    assert parse_label(query) == expected


def test_enumeration_reads_the_address_set_and_pages_the_reader(resolved):
    reader = FakeReader(MARKETS)
    read_gmx_market(reader, request("DOGE/USD"), BLOCK)
    assert reader.calls[:2] == ["getAddressCount(bytes32)", "getMarkets(address,uint256,uint256)"]
    # One code read per distinct index token; only the deployed index answers symbol().
    assert reader.calls.count("symbol()") == 1
    assert sum(call.startswith("eth_getCode") for call in reader.calls) == 4
    assert resolved["selection"].market.reference == DOGE_SHARE


def test_address_query_labels_a_deployed_index_by_its_symbol(resolved):
    output = read_gmx_market(FakeReader(MARKETS), request(ETH_SHARE), BLOCK)
    assert resolved["selection"].market.reference == ETH_SHARE
    assert output["product"] == "gmx_perp_market"
    assert output["observation"]["index_symbol"] == "WETH"
    assert output["observation"]["index_kind"] == "erc20"


def test_address_query_verifies_a_synthetic_label_through_its_collateral(resolved):
    output = read_gmx_market(FakeReader(MARKETS), request(BTC_USDC_SHARE), BLOCK)
    assert output["observation"]["index_symbol"] == "BTC"
    assert output["observation"]["index_kind"] == "synthetic"
    # tBTC unwraps to no alias, so the venue derivation cannot confirm a label.
    output = read_gmx_market(FakeReader(MARKETS), request(BTC_TBTC_SHARE), BLOCK)
    assert output["observation"]["index_symbol"] is None
    assert output["observation"]["index_kind"] == "synthetic"


def test_label_query_matches_the_wrapped_index_alias(resolved):
    for query in ("ETH/USD", "WETH", "weth/usd"):
        output = read_gmx_market(FakeReader(MARKETS), request(query), BLOCK)
        assert resolved["selection"].market.reference == ETH_SHARE
        assert output["observation"]["index_symbol"] == "WETH"


def test_synthetic_label_query_matches_the_derived_index(resolved):
    output = read_gmx_market(FakeReader(MARKETS), request("doge/usd"), BLOCK)
    assert resolved["selection"].market.reference == DOGE_SHARE
    assert output["observation"] == {
        "block": BLOCK.number,
        "share": DOGE_SHARE,
        "index_symbol": "DOGE",
        "index_kind": "synthetic",
    }


def test_collateral_notation_selects_among_markets_sharing_an_index(resolved):
    with pytest.raises(DataError, match="ambiguous") as failure:
        read_gmx_market(FakeReader(MARKETS), request("BTC/USD"), BLOCK)
    assert f"{BTC_USDC_SHARE} [WBTC-USDC]" in str(failure.value)
    assert f"{BTC_TBTC_SHARE} [tBTC-tBTC]" in str(failure.value)
    read_gmx_market(FakeReader(MARKETS), request("BTC/USD [wbtc-usdc]"), BLOCK)
    assert resolved["selection"].market.reference == BTC_USDC_SHARE
    read_gmx_market(FakeReader(MARKETS), request("BTC [tBTC]"), BLOCK)
    assert resolved["selection"].market.reference == BTC_TBTC_SHARE


def test_missing_labels_and_quotes_fail_loudly(resolved):
    for query in ("SOL/USD", "BTC/EUR", "BTC/USD [WETH-USDC]", "USDC"):
        with pytest.raises(DataError, match="no GMX V2 market matches"):
            read_gmx_market(FakeReader(MARKETS), request(query), BLOCK)
    with pytest.raises(DataError, match="no GMX V2 market matches"):
        read_gmx_market(FakeReader([]), request("BTC/USD"), BLOCK)


def test_perp_markets_passes_request_and_block_through(monkeypatch, tmp_path):
    config = tmp_path / "gmx.json"
    config.write_text(json.dumps({"chain": "eip155:42161", "query": "BTC/USD"}))
    seen = {}

    def read(reader, req, pinned):
        seen.update(request=req, pinned=pinned)
        return {"product": "gmx_perp_market"}

    monkeypatch.setattr(ax, "read_gmx_market", read)
    monkeypatch.setattr(ax, "verified_reader", lambda chain, env_file: FakeReader(MARKETS))
    invocation = SimpleNamespace(
        product="ax-perp-markets",
        ax_command="perp-markets",
        connector="gmx_v2",
        config=config,
        block=25908472,
        env_file="operator.env",
        gateway_url=None,
        gateway_auth_file=None,
    )
    stated = read_at(monkeypatch, 25908472, BLOCK)
    result = ax.perp_markets(invocation)
    assert result == {"product": "gmx_perp_market", "read_at": stated}
    assert seen["pinned"] == BLOCK
    assert seen["request"].chain == ChainId("eip155:42161")
    assert seen["request"].query == "BTC/USD"
