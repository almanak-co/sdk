"""Lending market discovery: protocol records name candidates, the resolver verifies them."""

import contextvars
import json
import threading
import time
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import encode

from almanak import ax_lending_markets
from almanak.ax_lending_markets import (
    COMET_DEPLOYED,
    CREATE_MARKET,
    MarketQuery,
    created,
    lending_markets,
    market_params,
)
from almanak.caip import AssetType, ChainId
from almanak.connectors import lending_lookup
from almanak.connectors.aave_v3.networks import pool_for
from almanak.connectors.aave_v4.hubs import HUBS, hubs_for
from almanak.connectors.compound_v3.configurators import CONFIGURATORS, configurator_for
from almanak.connectors.compound_v3.domain import Revision
from almanak.connectors.compound_v3.support import candidate_support
from almanak.connectors.morpho_blue.networks import CORES, core_for
from almanak.connectors.support import aave_support
from almanak.gateway import ethereum, morpho_lookup
from almanak.gateway.ethereum import DataError, EthereumReader, ProviderBusy
from almanak.gateway.onchain_lookup import LookupResult
from almanak.models import ZERO_ADDRESS, Block, Chain

BASE = ChainId("eip155:8453")
BLOCK = Block(
    chain=Chain(chain_id=BASE, environment="mainnet"),
    number=36_000_000,
    hash="0x" + "ab" * 32,
    timestamp=1_790_000_000,
)
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH = "0x4200000000000000000000000000000000000006"
CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
OLD = "0x" + "0d" * 20
POOL = pool_for(BASE)
CONFIGURATOR = configurator_for(BASE)
CORE = core_for(BASE)


def asset(token):
    return f"{BASE}/erc20:{token}"


def query(connector, asset_=None, collateral=None, limit=10, offset=0, unlisted=False):
    return MarketQuery(
        connector=connector,
        asset=None if asset_ is None else AssetType(asset(asset_)),
        collateral=None if collateral is None else AssetType(asset(collateral)),
        limit=limit,
        offset=offset,
        include_unlisted=unlisted,
    )


class Reader:
    """Answers fixed calls and event lists; any other read is a test failure."""

    def __init__(self, calls=None, logs=None, block=BLOCK):
        self.block = block
        self.answers = {
            OLD: {"symbol()": DataError("abi", "symbol(): invalid ABI response")},
            **{token: {"symbol()": (name,)} for token, name in SYMBOLS.items()},
        }
        for (target, signature), value in (calls or {}).items():
            self.answers.setdefault(target, {})[signature] = value
        self.logs = logs or {}
        self.queries = []
        self.sent = []

    def call(self, target, signature, block, *, inputs=(), args=(), outputs=()):
        assert block == self.block
        self.sent.append((target, signature, *args))
        value = self.answers[target][signature]
        if callable(value):
            value = value(*args)
        if isinstance(value, Exception):
            raise value
        return value

    def pin_block(self, number):
        assert number == self.block.number
        return self.block

    def _rpc(self, method, params):
        assert method == "eth_getLogs"
        (span,) = params
        self.queries.append(span)
        return self.logs[(span["address"], span["topics"][0])]


SYMBOLS = {USDC: "USDC", WETH: "WETH", CBBTC: "cbBTC"}


def label(token):
    return {"asset": asset(token), "symbol": SYMBOLS.get(token)}


def reserve_config(
    ltv, threshold, frozen=False, borrowing=True, active=True, paused=False, bonus=0
):
    """An Aave V3 reserve configuration word with the flags the listing reports."""
    flags = active << 56 | frozen << 57 | borrowing << 58 | paused << 60
    return ltv | threshold << 16 | bonus << 32 | flags


CONFIGS = {WETH: reserve_config(8000, 8300, bonus=10500), USDC: reserve_config(7500, 7800)}


@pytest.fixture(autouse=True)
def core_events(monkeypatch):
    """Morpho listings read the core's events unless a test opts into the API."""
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", frozenset())


@pytest.fixture(autouse=True)
def unobserved(monkeypatch):
    """Verified markets report no rates unless a test observes them."""
    monkeypatch.setattr(ax_lending_markets, "observed_rates", lambda *_: {})


@pytest.fixture
def resolver(monkeypatch):
    """The connector's resolver, faked: it refuses the markets named in `refused`."""
    refused = set()

    def market_config(reader, selected, request, block):
        named = request.model_dump(mode="json", exclude_none=True)
        if {str(value) for value in named.values()} & refused:
            raise DataError("deployment_changed", "unverified revision")
        return SimpleNamespace(summary=dict), {}, {"market": named}

    monkeypatch.setattr(ax_lending_markets, "market_config", market_config)
    return refused


def test_aave_reserves_are_listed_in_pool_order_and_each_resolved(resolver):
    resolver.add(asset(OLD))
    configs = {**CONFIGS, OLD: reserve_config(0, 6700, frozen=True, borrowing=False)}
    reader = Reader(
        {
            (POOL, "getReservesList()"): ([WETH, OLD, USDC],),
            (POOL, "getConfiguration(address)"): lambda token: (configs[token],),
        }
    )
    report = lending_markets(reader, None, BLOCK, query("aave_v3", limit=2))
    assert report["source"] == {"record": "pool_reserves", "contract": POOL, "listed": 3}
    assert report["page"] == {"offset": 0, "limit": 2, "matched": 3, "has_more": True}
    first, second = report["markets"]
    assert first == {
        "asset": label(WETH),
        "reserve": {
            "active": True,
            "frozen": False,
            "paused": False,
            "borrowing_enabled": True,
            "ltv_bps": 8000,
            "liquidation_threshold_bps": 8300,
            # Stored as 10000 + bonus, reported as the bonus alone.
            "liquidation_bonus_bps": 500,
        },
        "status": "verified",
        "config": {"market": {"connector": "aave_v3", "chain": str(BASE), "asset": asset(WETH)}},
    }
    # A token that does not answer symbol() as a string has no symbol, not a guess.
    assert second["asset"] == {"asset": asset(OLD), "symbol": None}
    # The resolver checks revisions only; the reserve's own flags show it takes nothing.
    assert second["reserve"] == {
        "active": True,
        "frozen": True,
        "paused": False,
        "borrowing_enabled": False,
        "ltv_bps": 0,
        "liquidation_threshold_bps": 6700,
        "liquidation_bonus_bps": 0,
    }
    assert (second["status"], second["reason"]) == (
        "refused",
        "deployment_changed: unverified revision",
    )


def test_a_reserve_whose_flags_cannot_be_read_reports_them_missing(resolver):
    unreadable = DataError("abi", "getConfiguration(address): invalid ABI response")
    reader = Reader(
        {
            (POOL, "getReservesList()"): ([WETH],),
            (POOL, "getConfiguration(address)"): unreadable,
        }
    )
    (market,) = lending_markets(reader, None, BLOCK, query("aave_v3"))["markets"]
    assert market["reserve"] is None and market["status"] == "verified"


def test_only_morpho_listings_take_unlisted_markets():
    with pytest.raises(ValueError, match="--include-unlisted applies to morpho_blue"):
        lending_markets(Reader(), None, BLOCK, query("compound_v3", unlisted=True))


def test_an_aave_asset_names_its_one_reserve_and_collateral_is_refused(resolver):
    reader = Reader(
        {
            (POOL, "getReservesList()"): ([WETH, USDC],),
            (POOL, "getConfiguration(address)"): lambda token: (CONFIGS[token],),
        }
    )
    report = lending_markets(reader, None, BLOCK, query("aave_v3", USDC))
    assert [market["asset"] for market in report["markets"]] == [label(USDC)]
    with pytest.raises(
        ValueError, match="each Aave V3, Aave V4 or SparkLend reserve is its own market"
    ):
        lending_markets(reader, None, BLOCK, query("sparklend", collateral=WETH))


def comet_log(comet, implementation="0x" + "99" * 20):
    return {
        "address": CONFIGURATOR,
        "topics": [
            COMET_DEPLOYED,
            "0x" + "00" * 12 + comet[2:],
            "0x" + "00" * 12 + implementation[2:],
        ],
        "removed": False,
    }


USDC_COMET, WETH_COMET = "0x" + "c1" * 20, "0x" + "c2" * 20


def comets(reader=None):
    info = {USDC_COMET: (WETH, CBBTC), WETH_COMET: (CBBTC,)}
    calls = {}
    for comet, base in ((USDC_COMET, USDC), (WETH_COMET, WETH)):
        calls[(comet, "baseToken()")] = (base,)
        calls[(comet, "numAssets()")] = (len(info[comet]),)
        calls[(comet, "getAssetInfo(uint8)")] = lambda i, held=info[comet]: (
            (i, held[i], ZERO_ADDRESS, 1, 825 * 10**15, 895 * 10**15, 95 * 10**16, 1),
        )
    # An upgrade records the same Comet again with its new implementation.
    logs = [comet_log(USDC_COMET), comet_log(WETH_COMET), comet_log(USDC_COMET, "0x" + "98" * 20)]
    return (reader or Reader)(calls, {(CONFIGURATOR, COMET_DEPLOYED): logs})


def test_compound_lists_each_configured_comet_once_with_its_assets(resolver):
    reader = comets()
    report = lending_markets(reader, None, BLOCK, query("compound_v3"))
    assert report["source"] == {
        "record": "comet_deployments",
        "contract": CONFIGURATOR,
        "listed": 2,
        "unreadable": [],
    }
    usdc, weth = report["markets"]
    assert (usdc["comet"], usdc["base_asset"]) == (USDC_COMET, label(USDC))
    assert usdc["collateral_assets"] == [label(WETH), label(CBBTC)]
    # Each collateral's factors are kept, labelled as `lending-reserve` labels them.
    factors = {"ltv_bps": 8250, "liquidation_threshold_bps": 8950, "liquidation_bonus_bps": 500}
    assert usdc["collateral_factors"] == [factors, factors]
    # Compound keeps no per-reserve flags here; the Comet's resolver is the check.
    assert "reserve" not in usdc
    assert usdc["config"]["market"]["asset"] == asset(USDC)
    assert weth["config"]["market"]["asset"] == asset(WETH)
    assert reader.queries == [
        {
            "address": CONFIGURATOR,
            "topics": [COMET_DEPLOYED],
            "fromBlock": "0x0",
            "toBlock": hex(BLOCK.number),
        }
    ]


def test_compound_filters_by_asset_or_collateral_and_selects_the_asked_asset(resolver):
    collateral = lending_markets(comets(), None, BLOCK, query("compound_v3", WETH))
    # WETH is one Comet's base and the other's collateral; each selection names WETH.
    assert [m["comet"] for m in collateral["markets"]] == [USDC_COMET, WETH_COMET]
    assert {m["config"]["market"]["asset"] for m in collateral["markets"]} == {asset(WETH)}
    accepting = lending_markets(comets(), None, BLOCK, query("compound_v3", collateral=WETH))
    assert [m["comet"] for m in accepting["markets"]] == [USDC_COMET]
    both = lending_markets(comets(), None, BLOCK, query("compound_v3", USDC, collateral=CBBTC))
    assert [m["comet"] for m in both["markets"]] == [USDC_COMET]


def test_every_supported_compound_chain_has_a_configurator():
    assert set(CONFIGURATORS) == {support.chain for support in candidate_support()}
    with pytest.raises(DataError, match="no Configurator listed on eip155:137"):
        configurator_for(ChainId("eip155:137"))


def market_log(market_id, loan, collateral, lltv=860_000_000_000_000_000):
    data = encode(
        ["address", "address", "address", "address", "uint256"],
        [loan, collateral, "0x" + "0a" * 20, "0x" + "0b" * 20, lltv],
    )
    return {"address": CORE, "topics": [CREATE_MARKET, market_id], "data": "0x" + data.hex()}


IDS = ["0x" + digit * 64 for digit in "12345"]


def morpho(supplies, reader_type=Reader):
    logs = [
        market_log(IDS[0], USDC, WETH),
        market_log(IDS[1], USDC, CBBTC, 915_000_000_000_000_000),
        market_log(IDS[2], WETH, USDC),
        market_log(IDS[3], USDC, ZERO_ADDRESS),
        market_log(IDS[4], USDC, WETH),
    ]
    state = {
        bytes.fromhex(market_id[2:]): (supplied, 0, supplied // 2, 0, 1, 0)
        for market_id, supplied in zip(IDS, supplies, strict=True)
    }
    calls = {
        (CORE, "market(bytes32)"): lambda market_id: state[market_id],
        (USDC, "decimals()"): (6,),
    }
    return reader_type(calls, {(CORE, CREATE_MARKET): logs})


def test_morpho_lists_supplied_markets_of_one_loan_asset_most_supplied_first(resolver):
    resolver.add(IDS[1])
    reader = morpho([5_000_000, 90_000_000, 7, 0, 1_500_000])
    report = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC, limit=2))
    assert report["source"] == {
        "record": "core_markets",
        "listed_only": False,
        "curation": None,
        "contract": CORE,
        "listed": 5,
        "matching": 4,
        "without_supply": 1,
    }
    assert report["page"] == {"offset": 0, "limit": 2, "matched": 3, "has_more": True}
    first, second = report["markets"]
    assert first == {
        "market_id": IDS[1],
        "loan_asset": label(USDC),
        "collateral_asset": label(CBBTC),
        "lltv": "0.915",
        "liquidation_incentive_factor": "1.02616726526423807",
        "supplied": "90.000000",
        "borrowed": "45.000000",
        "status": "refused",
        "reason": "deployment_changed: unverified revision",
    }
    assert (second["market_id"], second["supplied"], second["lltv"]) == (IDS[0], "5.000000", "0.86")
    assert second["config"]["market"] == {
        "connector": "morpho_blue",
        "chain": str(BASE),
        "market_id": IDS[0],
        "leg": "loan",
    }
    last = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC, offset=2))["markets"]
    assert [market["market_id"] for market in last] == [IDS[4]]


def test_morpho_collateral_narrows_and_an_idle_market_names_no_collateral(resolver):
    reader = morpho([1, 2, 3, 4, 5])
    narrowed = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC, collateral=WETH))
    assert [market["market_id"] for market in narrowed["markets"]] == [IDS[4], IDS[0]]
    listed = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC))["markets"]
    assert next(m for m in listed if m["market_id"] == IDS[3])["collateral_asset"] is None


def test_morpho_needs_a_loan_asset_and_refuses_listings_past_the_read_cap(monkeypatch):
    with pytest.raises(ValueError, match="--asset is required for morpho_blue"):
        lending_markets(Reader(), None, BLOCK, query("morpho_blue"))
    monkeypatch.setattr(ax_lending_markets, "MAX_STATE_READS", 3)
    with pytest.raises(
        ValueError, match="4 Morpho markets match, over 3; narrow them with --collateral"
    ):
        lending_markets(morpho([1, 1, 1, 1, 1]), None, BLOCK, query("morpho_blue", USDC))


def test_event_lists_from_another_contract_or_event_are_refused():
    good = comet_log(USDC_COMET)
    cases = [
        "not a list",
        [{**good, "removed": True}],
        [{**good, "address": "0x" + "ee" * 20}],
        [{**good, "topics": [CREATE_MARKET, *good["topics"][1:]]}],
        [{**good, "topics": [COMET_DEPLOYED]}],
        ["not a log"],
    ]
    for logs in cases:
        reader = Reader(logs={(CONFIGURATOR, COMET_DEPLOYED): logs})
        with pytest.raises(DataError, match="invalid event list"):
            created(reader, BLOCK, CONFIGURATOR, COMET_DEPLOYED, 3)


def test_market_parameters_that_do_not_decode_are_refused():
    for log in (
        {"topics": [CREATE_MARKET, IDS[0]]},
        {"topics": [CREATE_MARKET, IDS[0]], "data": "0x12"},
    ):
        with pytest.raises(DataError, match="invalid market parameters"):
            market_params(log)


def test_sparklend_lists_its_pool_reserves(resolver):
    pool = ax_lending_markets.pool_for("sparklend")(ChainId("eip155:1"))
    mainnet = Block(
        chain=Chain(chain_id=ChainId("eip155:1"), environment="mainnet"),
        number=BLOCK.number,
        hash=BLOCK.hash,
        timestamp=BLOCK.timestamp,
    )
    dai = "0x6b175474e89094c44da98b954eedeac495271d0f"

    class Mainnet(Reader):
        def call(self, target, signature, block, **kwargs):
            assert block == mainnet
            return super().call(target, signature, BLOCK, **kwargs)

    calls = {
        (pool, "getReservesList()"): ([dai],),
        (pool, "getConfiguration(address)"): (reserve_config(0, 0, frozen=True),),
        (dai, "symbol()"): ("DAI",),
    }
    report = lending_markets(Mainnet(calls), None, mainnet, query("sparklend"))
    assert report["source"]["contract"] == pool
    (market,) = report["markets"]
    assert market["asset"] == {"asset": f"eip155:1/erc20:{dai}", "symbol": "DAI"}
    assert market["reserve"]["frozen"] is True


def test_a_refusal_naming_an_asset_is_not_read_as_a_reported_asset(monkeypatch):
    def market_config(reader, selected, request, block):
        raise ValueError(f"Comet lists neither {asset(WETH)} as collateral nor as its base asset.")

    monkeypatch.setattr(ax_lending_markets, "market_config", market_config)
    report = lending_markets(comets(), None, BLOCK, query("compound_v3", collateral=CBBTC))
    assert {m["status"] for m in report["markets"]} == {"refused"}
    assert report["markets"][0]["base_asset"] == label(USDC)
    assert asset(WETH) in report["markets"][0]["reason"]


def test_an_unreadable_comet_is_named_and_the_others_still_list(resolver):
    reader = comets()
    reader.answers[WETH_COMET]["numAssets()"] = DataError(
        "abi", "numAssets(): invalid ABI response"
    )
    report = lending_markets(reader, None, BLOCK, query("compound_v3"))
    assert [m["comet"] for m in report["markets"]] == [USDC_COMET]
    assert report["source"]["unreadable"] == [
        {
            "comet": WETH_COMET,
            "status": "refused",
            "reason": "abi: numAssets(): invalid ABI response",
        }
    ]
    shifted = comets()
    shifted.answers[USDC_COMET]["getAssetInfo(uint8)"] = lambda i: (
        (i + 1, WETH, ZERO_ADDRESS, 1, 1, 1, 1, 1),
    )
    unread = lending_markets(shifted, None, BLOCK, query("compound_v3"))["source"]["unreadable"]
    assert unread[0]["reason"] == "deployment_changed: Comet asset list returned another offset"


def test_morpho_state_reads_see_the_pinned_block_and_stop_after_a_failure(resolver):
    pinned = contextvars.ContextVar("pinned", default=None)
    reader = morpho([3, 2, 1, 0, 4])
    answer = reader.answers[CORE]["market(bytes32)"]
    sent = []

    def market(market_id):
        # The real reader refuses a read outside the context where its block is pinned.
        assert pinned.get() == BLOCK
        sent.append(threading.get_ident())
        return answer(market_id)

    reader.answers[CORE]["market(bytes32)"] = market
    token = pinned.set(BLOCK)
    try:
        report = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC))
    finally:
        pinned.reset(token)
    assert report["page"]["matched"] == 3 and len(sent) == 4
    failing = morpho([1] * 5)
    failing.answers[CORE]["market(bytes32)"] = DataError(
        "rate_limit", "rpc: gateway rate cap exhausted"
    )
    with pytest.raises(DataError, match="rate cap"):
        lending_markets(failing, None, BLOCK, query("morpho_blue", USDC))


def test_market_ids_and_comet_topics_must_be_canonical_words():
    log = market_log(IDS[0], USDC, WETH)
    for market_id in ("0x12", "0x" + "zz" * 32):
        with pytest.raises(DataError, match="invalid market parameters"):
            market_params({**log, "topics": [CREATE_MARKET, market_id]})
    padded = comet_log(USDC_COMET)
    padded["topics"][1] = "0x" + "01" * 32
    reader = Reader(logs={(CONFIGURATOR, COMET_DEPLOYED): [padded]})
    with pytest.raises(DataError, match="CometDeployed: invalid Comet address"):
        lending_markets(reader, None, BLOCK, query("compound_v3"))


AVALANCHE = ChainId("eip155:43114")
(V4_HUB,) = hubs_for(AVALANCHE)
V4_BLOCK = Block(
    chain=Chain(chain_id=AVALANCHE, environment="mainnet"),
    number=BLOCK.number,
    hash=BLOCK.hash,
    timestamp=BLOCK.timestamp,
)
MAIN, TREASURY, OTHER = "0x" + "51" * 20, "0x" + "52" * 20, "0x" + "53" * 20
V4_USDC, V4_WAVAX = "0x" + "a1" * 20, "0x" + "a2" * 20


class Avalanche(Reader):
    def call(self, target, signature, block, **kwargs):
        assert block == V4_BLOCK
        return super().call(target, signature, BLOCK, **kwargs)


def aave_v4_calls(reserves=None):
    hub, main, treasury, other = V4_HUB, MAIN, TREASURY, OTHER
    # Asset 0 is served by both Spokes and the treasury; asset 1 by the main Spoke only.
    served = {0: [main, treasury, other], 1: [main]}
    reserves = reserves or {main: [V4_USDC, V4_WAVAX], other: [V4_USDC]}
    calls = {
        (hub, "getAssetCount()"): (2,),
        (hub, "getSpokeCount(uint256)"): lambda asset_id: (len(served[asset_id]),),
        (hub, "getSpokeAddress(uint256,uint256)"): lambda a, i: (served[a][i],),
        (treasury, "getReserveCount()"): DataError(
            "rpc", "eth_call: RPC rejected the requested read (JSON-RPC 3)", code=3
        ),
    }
    for spoke, held in reserves.items():
        calls[(spoke, "getReserveCount()")] = (len(held),)
        # The other Spoke's reserve is frozen and lends nothing.
        frozen = spoke == other
        calls[(spoke, "getReserveConfig(uint256)")] = lambda i, f=frozen: (
            0,
            False,
            f,
            not f,
            False,
        )
        calls[(spoke, "getReserve(uint256)")] = lambda i, held=held: (held[i], hub, i, 6, 0, 0, 0)
    for token, name in ((V4_USDC, "USDC"), (V4_WAVAX, "WAVAX")):
        calls[(token, "symbol()")] = (name,)
    return calls


def test_aave_v4_lists_the_reserves_of_each_spoke_its_hubs_serve(resolver):
    chain, hub, avalanche = AVALANCHE, V4_HUB, V4_BLOCK
    main, other, usdc, wavax = MAIN, OTHER, V4_USDC, V4_WAVAX
    calls = aave_v4_calls()

    report = lending_markets(Avalanche(calls), None, avalanche, query("aave_v4"))
    assert report["source"] == {
        "record": "hub_spokes",
        "contracts": [hub],
        "listed": 2,
        "without_reserves": 1,
        "unreadable": [],
    }
    rows = [(m["spoke"], m["reserve_id"], m["asset"]["asset"]) for m in report["markets"]]
    assert rows == [
        (main, 0, f"{chain}/erc20:{usdc}"),
        (main, 1, f"{chain}/erc20:{wavax}"),
        (other, 0, f"{chain}/erc20:{usdc}"),
    ]
    assert report["markets"][0]["reserve"] == {"frozen": False, "paused": False, "borrowable": True}
    assert report["markets"][2]["reserve"] == {"frozen": True, "paused": False, "borrowable": False}
    assert report["markets"][0]["hub"] == hub
    assert report["markets"][0]["config"]["market"] == {
        "connector": "aave_v4",
        "chain": str(chain),
        "spoke": main,
        "reserve_id": 0,
    }
    usdc_only = MarketQuery("aave_v4", AssetType(f"{chain}/erc20:{usdc}"), None, 10)
    picked = lending_markets(Avalanche(calls), None, avalanche, usdc_only)["markets"]
    assert [(m["spoke"], m["reserve_id"]) for m in picked] == [(main, 0), (other, 0)]


def test_every_supported_aave_v4_chain_has_a_hub():
    assert set(HUBS) == {support.chain for support in aave_support()}
    with pytest.raises(DataError, match="no Hub listed on eip155:8453"):
        hubs_for(BASE)


class MorphoPages:
    """Answers Morpho market lookups with a fixed page; records each lookup."""

    def __init__(self, items, total):
        self.page = {"pageInfo": {"countTotal": total}, "items": items}
        self.lookups = []

    def fetch(self, request):
        self.lookups.append(request.lookup)
        return LookupResult(request=request, result=self.page)


def api_row(market_id, collateral, listed=True, warnings=(), loan=USDC):
    return {
        "marketId": market_id,
        "listed": listed,
        "lltv": "860000000000000000",
        "loanAsset": {"address": "0x" + loan[2:].upper(), "decimals": 6},
        "collateralAsset": None if collateral is None else {"address": collateral},
        "state": {
            # Past 2**53 the API sends integers as digit strings.
            "supplyAssets": "12410391162851449",
            "borrowAssets": None,
            "supplyAssetsUsd": 12.509,
            "borrowAssetsUsd": 6.2,
            "supplyApy": 0.0441,
        },
        "warnings": [{"type": w} for w in warnings],
    }


def test_morpho_on_an_indexed_chain_pages_through_the_api(monkeypatch, resolver):
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", frozenset({BASE}))
    resolver.add(IDS[1])
    rows = [
        api_row(IDS[0], WETH),
        {
            **api_row(IDS[1], WETH, listed=False, warnings=("not_whitelisted", "bad_debt")),
            "state": {"supplyAssets": 5, "supplyApy": "4%"},
        },
        {**api_row(IDS[2], WETH), "state": {"supplyAssets": 1, "supplyApy": float("nan")}},
        api_row(IDS[3], WETH, loan=WETH),
        api_row(IDS[4], CBBTC),
        {"marketId": "0x12"},
        "not a row",
    ]
    lookups = MorphoPages(rows, 40)
    report = lending_markets(Reader(), lookups, BLOCK, query("morpho_blue", USDC, WETH, 5, 10))
    (lookup,) = lookups.lookups
    assert (lookup.kind, lookup.chain, lookup.loan, lookup.collateral) == (
        "morpho_markets",
        BASE,
        USDC,
        WETH,
    )
    assert (lookup.first, lookup.skip, lookup.listed_only) == (5, 10, True)
    # Another loan asset, other collateral than asked and malformed rows are counted.
    assert report["source"] == {
        "record": "morpho_api",
        "listed_only": True,
        "curation": "morpho_api",
        "skipped": 4,
    }
    everything = MorphoPages([], 0)
    lending_markets(Reader(), everything, BLOCK, query("morpho_blue", USDC, unlisted=True))
    assert everything.lookups[0].listed_only is False
    assert report["page"] == {"offset": 10, "limit": 5, "matched": 40, "has_more": True}
    first, second, third = report["markets"]
    # Only the market ID and the asked-for loan asset are the market's own facts.
    assert {key: first[key] for key in ("market_id", "loan_asset")} == {
        "market_id": IDS[0],
        "loan_asset": label(USDC),
    }
    assert "collateral_asset" not in first and "lltv" not in first
    assert first["vendor"] == {
        "collateral_asset": label(WETH),
        "lltv": "0.86",
        "liquidation_incentive_factor": "1.043841336116910229",
        "supplied": "12410391162.851449",
        "borrowed": None,
        "supplied_usd": "12.50",
        "borrowed_usd": "6.20",
        "supply_apy": "0.0441",
        "listed": True,
        "warnings": [],
    }
    assert first["config"]["market"] == {
        "connector": "morpho_blue",
        "chain": str(BASE),
        "market_id": IDS[0],
        "leg": "loan",
    }
    assert second["status"] == "refused"
    assert second["vendor"]["warnings"] == ["bad_debt", "not_whitelisted"]
    assert second["vendor"]["supply_apy"] is None
    # A figure the API leaves out, or sends as no finite number, stays absent, never zero.
    assert third["vendor"]["supply_apy"] is None and third["vendor"]["supplied_usd"] is None
    assert "symbols" not in report


def test_an_api_row_missing_morphos_figures_still_lists_with_them_null(monkeypatch, resolver):
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", frozenset({BASE}))
    full = api_row(IDS[0], WETH, warnings=("bad_debt",))
    without_decimals = {**full, "loanAsset": {"address": USDC}}
    rows = [
        {**full, "state": None},
        {key: value for key, value in full.items() if key != "state"},
        without_decimals,
        {**full, "warnings": [{}, {"type": None}, "bad_debt", {"type": "bad_debt"}]},
        {**full, "warnings": None},
        # Without its market ID or loan asset a row cannot be resolved.
        {key: value for key, value in full.items() if key != "marketId"},
        {**full, "loanAsset": None},
        {**full, "loanAsset": {"decimals": 6}},
    ]
    report = lending_markets(Reader(), MorphoPages(rows, 8), BLOCK, query("morpho_blue", USDC))
    assert report["source"]["skipped"] == 3
    no_state, no_key, no_decimals, odd_warnings, no_warnings = (
        market["vendor"] for market in report["markets"]
    )
    for vendor in (no_state, no_key):
        figures = ("supplied", "borrowed", "supplied_usd", "supply_apy")
        assert all(vendor[figure] is None for figure in figures)
        assert (vendor["lltv"], vendor["warnings"]) == ("0.86", ["bad_debt"])
    assert (no_decimals["supplied"], no_decimals["borrowed"]) == (None, None)
    assert (no_decimals["supplied_usd"], no_decimals["supply_apy"]) == ("12.50", "0.0441")
    assert odd_warnings["warnings"] == ["bad_debt"] and no_warnings["warnings"] == []


def test_an_api_page_in_another_shape_is_refused(monkeypatch):
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", frozenset({BASE}))
    pages = (
        {"pageInfo": {}, "items": []},
        {"pageInfo": {"countTotal": -1}, "items": []},
        {"pageInfo": None, "items": []},
        {"pageInfo": {"countTotal": 1}},
        [],
    )
    for page in pages:
        lookups = MorphoPages([], 0)
        lookups.page = page
        with pytest.raises(DataError, match="morpho_api: invalid market page"):
            lending_markets(Reader(), lookups, BLOCK, query("morpho_blue", USDC))


def test_each_morpho_chain_has_one_fixed_source(monkeypatch, resolver):
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", morpho_lookup.API_CHAINS)
    cores = set(CORES)
    assert morpho_lookup.API_CHAINS < cores
    assert cores - morpho_lookup.API_CHAINS == {ChainId("eip155:56"), ChainId("eip155:43114")}
    lookups = MorphoPages([api_row(IDS[0], WETH)], 1)
    indexed = lending_markets(Reader(), lookups, BLOCK, query("morpho_blue", USDC))
    assert indexed["source"]["record"] == "morpho_api"
    bsc = ChainId("eip155:56")
    block = Block(
        chain=Chain(chain_id=bsc, environment="mainnet"),
        number=BLOCK.number,
        hash=BLOCK.hash,
        timestamp=BLOCK.timestamp,
    )
    reader = Reader(logs={(core_for(bsc), CREATE_MARKET): []}, block=block)
    reader.answers["0x" + "55" * 20] = {"decimals()": (18,)}
    events = lending_markets(
        reader,
        None,
        block,
        MarketQuery("morpho_blue", AssetType(f"{bsc}/erc20:0x{'55' * 20}"), None, 10),
    )
    assert events["source"]["record"] == "core_markets" and events["page"]["matched"] == 0


def test_a_provider_that_refuses_the_event_range_is_named():
    reader = Reader()

    def refuse(method, params):
        raise DataError("http", "eth_getLogs: HTTP 400")

    reader._rpc = refuse
    with pytest.raises(DataError, match="refused .* event history in one request") as raised:
        created(reader, BLOCK, CORE, CREATE_MARKET, 2)
    assert raised.value.kind == "http"

    def too_many(method, params):
        raise DataError(
            "rpc", "eth_getLogs: RPC rejected the requested read (JSON-RPC -32000)", code=-32000
        )

    reader._rpc = too_many
    with pytest.raises(DataError, match="refused .* event history in one request") as raised:
        created(reader, BLOCK, CORE, CREATE_MARKET, 2)
    assert (raised.value.kind, raised.value.code) == ("rpc", -32000)

    # A throttle is not a refusal of the range; it keeps its own kind, code and wait.
    for throttle in (
        ProviderBusy("eth_getLogs", 2.0),
        DataError("http", "eth_getLogs: HTTP 429", code=429),
        DataError("rpc", "eth_getLogs: RPC rejected the requested read (JSON-RPC 429)", code=429),
        DataError("rpc", "eth_getLogs: limit exceeded", code=-32005),
    ):

        def throttling(method, params, throttle=throttle):
            raise throttle

        reader._rpc = throttling
        with pytest.raises(DataError) as raised:
            created(reader, BLOCK, CORE, CREATE_MARKET, 2)
        assert raised.value is throttle

    def capped(method, params):
        raise DataError("rate_limit", "rpc: gateway rate cap exhausted")

    reader._rpc = capped
    # Any other failure keeps its own message.
    with pytest.raises(DataError, match="^rpc: gateway rate cap exhausted$"):
        created(reader, BLOCK, CORE, CREATE_MARKET, 2)


def test_event_history_from_a_block_no_longer_canonical_fails_the_listing(resolver):
    replaced = BLOCK.model_copy(update={"hash": "0x" + "cd" * 32})
    for reader, connector in ((comets(), "compound_v3"), (morpho([1] * 5), "morpho_blue")):
        reader.pin_block = lambda number: replaced
        with pytest.raises(DataError, match="no longer canonical") as raised:
            lending_markets(reader, None, BLOCK, query(connector, USDC))
        assert raised.value.kind == "reorganization"
        # Nothing is read from the block once its history is in doubt.
        assert reader.sent == []


@pytest.mark.parametrize("own", [{"loan_asset": WETH}, {"collateral_asset": USDC}])
def test_a_morpho_market_whose_own_assets_differ_from_the_api_is_refused(monkeypatch, own):
    # The API row claims a USDC loan against WETH; the market's parameters say otherwise.
    truth = {"loan_asset": asset(USDC), "collateral_asset": asset(WETH)}
    truth |= {name: asset(token) for name, token in own.items()}

    def market_config(reader, selected, request, block):
        named = request.model_dump(mode="json", exclude_none=True)
        return SimpleNamespace(summary=lambda: truth), {}, {"market": named}

    monkeypatch.setattr(ax_lending_markets, "market_config", market_config)
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", frozenset({BASE}))
    report = lending_markets(
        Reader(), MorphoPages([api_row(IDS[0], WETH)], 1), BLOCK, query("morpho_blue", USDC)
    )
    (market,) = report["markets"]
    (name,) = own
    assert market["status"] == "refused"
    assert market["reason"].startswith(f"vendor_mismatch: the listing names {name}")
    assert "config" not in market


def test_an_aave_v4_spoke_that_cannot_be_read_is_named_not_counted_empty(monkeypatch):
    # A Spoke that reverts holds no reserves; one the transport failed to read is unknown.
    chain = ChainId("eip155:43114")
    avalanche = Block(
        chain=Chain(chain_id=chain, environment="mainnet"),
        number=BLOCK.number,
        hash=BLOCK.hash,
        timestamp=BLOCK.timestamp,
    )
    lost, throttled, garbled = "0x" + "51" * 20, "0x" + "53" * 20, "0x" + "54" * 20
    treasury = "0x" + "52" * 20
    errors = {
        lost: DataError("transport", "no answer"),
        throttled: DataError("rpc", "limit exceeded", code=-32005),
        garbled: DataError("abi", "getReserveCount(): invalid ABI response"),
        treasury: DataError("rpc", "execution reverted", code=3),
    }

    class Reader:
        def call(self, target, signature, block, **kwargs):
            raise errors[target]

    monkeypatch.setattr(ax_lending_markets, "served_spokes", lambda *_: (list(errors), []))
    source = ax_lending_markets.spoke_reserves(Reader(), None, avalanche, query("aave_v4")).source
    assert (source["listed"], source["without_reserves"]) == (0, 1)
    assert [
        (unread["spoke"], unread["reason"].split(":")[0]) for unread in source["unreadable"]
    ] == [
        (lost, "transport"),
        (throttled, "rpc"),
        (garbled, "abi"),
    ]


@pytest.mark.parametrize(
    ("own", "status"), [(asset(WETH), "refused"), (asset(ZERO_ADDRESS), "verified")]
)
def test_an_api_row_naming_no_collateral_claims_an_idle_market(monkeypatch, own, status):
    # Morpho's API answers `collateralAsset: null`; only an idle market, whose
    # collateral is the zero address, has none.
    truth = {"loan_asset": asset(USDC), "collateral_asset": own}

    def market_config(reader, selected, request, block):
        named = request.model_dump(mode="json", exclude_none=True)
        return SimpleNamespace(summary=lambda: truth), {}, {"market": named}

    monkeypatch.setattr(ax_lending_markets, "market_config", market_config)
    monkeypatch.setattr(ax_lending_markets, "API_CHAINS", frozenset({BASE}))
    report = lending_markets(
        Reader(), MorphoPages([api_row(IDS[0], None)], 1), BLOCK, query("morpho_blue", USDC)
    )
    (market,) = report["markets"]
    assert market["status"] == status
    if status == "refused":
        assert market["reason"] == (
            f"vendor_mismatch: the listing names collateral_asset none,"
            f" the market's own is {asset(WETH)}"
        )


def test_core_events_carry_no_curation_so_unlisted_markets_change_nothing(resolver):
    reader = morpho([5_000_000, 90_000_000, 7, 0, 1_500_000])
    listed = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC))
    everything = lending_markets(reader, None, BLOCK, query("morpho_blue", USDC, unlisted=True))
    assert (listed["source"]["listed_only"], listed["source"]["curation"]) == (False, None)
    assert everything == listed


def test_an_aave_v4_reserve_or_hub_that_cannot_be_read_is_named_and_the_rest_list(resolver):
    calls = aave_v4_calls()
    throttled = DataError("rate_limit", "rpc: gateway rate cap exhausted")
    calls[(MAIN, "getReserve(uint256)")] = lambda i: (
        throttled if i == 0 else (V4_WAVAX, V4_HUB, i, 6, 0, 0, 0)
    )
    report = lending_markets(Avalanche(calls), None, V4_BLOCK, query("aave_v4"))
    rows = [(m["spoke"], m["reserve_id"]) for m in report["markets"]]
    assert rows == [(MAIN, 1), (OTHER, 0)]
    source = report["source"]
    assert (source["listed"], source["without_reserves"]) == (2, 1)
    assert source["unreadable"] == [
        {
            "spoke": MAIN,
            "reserve_id": 0,
            "status": "refused",
            "reason": "rate_limit: rpc: gateway rate cap exhausted",
        }
    ]
    # A Hub that cannot be enumerated is named; none of its Spokes are listed from it.
    calls[(V4_HUB, "getSpokeCount(uint256)")] = DataError("transport", "no answer")
    lost = lending_markets(Avalanche(calls), None, V4_BLOCK, query("aave_v4"))
    assert lost["markets"] == [] and lost["page"]["matched"] == 0
    assert lost["source"]["unreadable"] == [
        {"hub": V4_HUB, "status": "refused", "reason": "transport: no answer"}
    ]
    assert (lost["source"]["listed"], lost["source"]["without_reserves"]) == (0, 0)


def test_an_aave_v4_listing_does_not_swallow_programming_errors(resolver):
    calls = aave_v4_calls()
    calls[(MAIN, "getReserve(uint256)")] = lambda i: {}[i]
    with pytest.raises(KeyError):
        lending_markets(Avalanche(calls), None, V4_BLOCK, query("aave_v4"))


def page_of(report):
    return report["page"]["matched"], report["page"]["has_more"], report["markets"]


def read(reader, signature):
    # Reads run concurrently; which were sent matters here, not when.
    return sorted(sent[0::2] for sent in reader.sent if sent[1] == signature)


def test_aave_v4_pages_read_only_the_reserves_on_the_page(resolver):
    calls = aave_v4_calls({MAIN: [V4_USDC, V4_WAVAX, V4_USDC], OTHER: [V4_USDC, V4_WAVAX]})
    everything = lending_markets(Avalanche(calls), None, V4_BLOCK, query("aave_v4", limit=20))
    order = [(MAIN, 0), (MAIN, 1), (MAIN, 2), (OTHER, 0), (OTHER, 1)]
    assert [(m["spoke"], m["reserve_id"]) for m in everything["markets"]] == order
    for offset in range(6):
        for limit in (1, 2, 3):
            reader = Avalanche(calls)
            report = lending_markets(
                reader, None, V4_BLOCK, query("aave_v4", None, None, limit, offset)
            )
            window = everything["markets"][offset : offset + limit]
            assert page_of(report) == (5, offset + limit < 5, window)
            assert report["source"] == everything["source"]
            # Without a filter every reserve matches; only the page's are read.
            on_page = order[offset : offset + limit]
            assert read(reader, "getReserve(uint256)") == on_page
            assert read(reader, "getReserveConfig(uint256)") == on_page
    usdc = AssetType(f"{AVALANCHE}/erc20:{V4_USDC}")
    reader = Avalanche(calls)
    filtered = lending_markets(reader, None, V4_BLOCK, MarketQuery("aave_v4", usdc, None, 1, 1))
    assert [(m["spoke"], m["reserve_id"]) for m in filtered["markets"]] == [(MAIN, 2)]
    assert filtered["page"] == {"offset": 1, "limit": 1, "matched": 3, "has_more": True}
    # A filter reads every reserve's asset to count the matches, but flags only the page's.
    assert read(reader, "getReserve(uint256)") == order
    assert read(reader, "getReserveConfig(uint256)") == [(MAIN, 2)]


def test_compound_pages_read_only_the_comets_on_the_page(resolver):
    everything = lending_markets(comets(), None, BLOCK, query("compound_v3"))
    reader = comets()
    second = lending_markets(reader, None, BLOCK, query("compound_v3", limit=1, offset=1))
    assert page_of(second) == (2, False, everything["markets"][1:])
    assert second["source"] == everything["source"]
    assert {sent[0] for sent in reader.sent if sent[1] == "baseToken()"} == {WETH_COMET}
    reader = comets()
    filtered = lending_markets(reader, None, BLOCK, query("compound_v3", WETH, limit=1, offset=1))
    assert page_of(filtered) == (2, False, everything["markets"][1:])
    assert {sent[0] for sent in reader.sent if sent[1] == "baseToken()"} == {USDC_COMET, WETH_COMET}


def test_record_ordered_pages_resolve_only_their_own_candidates(monkeypatch, resolver):
    resolved = []
    fake = ax_lending_markets.market_config

    def market_config(reader, selected, request, block):
        resolved.append(request.model_dump(mode="json", exclude_none=True))
        return fake(reader, selected, request, block)

    monkeypatch.setattr(ax_lending_markets, "market_config", market_config)
    reader = morpho([5_000_000, 90_000_000, 7, 0, 1_500_000])
    lending_markets(reader, None, BLOCK, query("morpho_blue", USDC, limit=1, offset=1))
    assert [request["market_id"] for request in resolved] == [IDS[0]]
    resolved.clear()
    reader = Reader(
        {
            (POOL, "getReservesList()"): ([WETH, USDC],),
            (POOL, "getConfiguration(address)"): lambda token: (CONFIGS[token],),
        }
    )
    lending_markets(reader, None, BLOCK, query("aave_v3", limit=1, offset=1))
    assert [request["asset"] for request in resolved] == [asset(USDC)]
    assert read(reader, "getConfiguration(address)") == [(POOL, USDC)]


def together(reader_type, rounds, pinned, block):
    """A reader whose reads of each named signature wait for one another.

    Reads sent one at a time never meet, and the barrier's timeout fails the
    listing. Once met, each read answers later the earlier it arrived, so the
    answers come back out of order.
    """
    barriers = {signature: threading.Barrier(n, timeout=5) for signature, n in rounds.items()}

    class Together(reader_type):
        def call(self, target, signature, block_, **kwargs):
            # The real reader refuses a read outside the context where its block is pinned.
            assert pinned.get() == block
            barrier = barriers.get(signature)
            if barrier is not None:
                time.sleep(0.01 * (barrier.parties - barrier.wait()))
            return super().call(target, signature, block_, **kwargs)

    return Together


def test_aave_v4_reads_each_enumeration_round_concurrently_and_keeps_order(resolver):
    pinned = contextvars.ContextVar("pinned", default=None)
    rounds = {
        "getSpokeCount(uint256)": 2,
        "getSpokeAddress(uint256,uint256)": 4,
        "getReserveCount()": 3,
        "getReserve(uint256)": 3,
    }
    calls = aave_v4_calls()
    expected = lending_markets(Avalanche(calls), None, V4_BLOCK, query("aave_v4"))
    reader = together(Avalanche, rounds, pinned, V4_BLOCK)(calls)
    token = pinned.set(V4_BLOCK)
    try:
        report = lending_markets(reader, None, V4_BLOCK, query("aave_v4"))
    finally:
        pinned.reset(token)
    assert report == expected


def test_compound_reads_its_comets_concurrently_and_keeps_order(resolver):
    pinned = contextvars.ContextVar("pinned", default=None)
    expected = lending_markets(comets(), None, BLOCK, query("compound_v3"))
    reader = comets(together(Reader, {"baseToken()": 2}, pinned, BLOCK))
    token = pinned.set(BLOCK)
    try:
        report = lending_markets(reader, None, BLOCK, query("compound_v3"))
    finally:
        pinned.reset(token)
    assert report == expected


def throttling(reader_type, signature, times, monkeypatch):
    """A reader whose calls each pass through an observation reader's single-read path first.

    The provider answers HTTP 429 `times` times to the first read of `signature` it
    receives; the answers themselves come from the fake.
    """
    monkeypatch.setattr(ethereum, "sleep", lambda _: None)
    lock, throttled, sent = threading.Lock(), [], []

    def provider(request):
        body = json.loads(request.content)
        with lock:
            sent.append(body["params"])
            if body["params"][1] == signature and not throttled:
                throttled.append(body["params"])
            busy = body["params"] in throttled and sent.count(throttled[0]) <= times
        if busy:
            return httpx.Response(429)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": "0x"})

    class Throttled(reader_type):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            client = httpx.Client(transport=httpx.MockTransport(provider))
            self.provider = EthereumReader(
                self.block.chain,
                "https://rpc.invalid",
                client=client,
                retry_budget=ethereum.BUSY_WAIT_LIMIT,
            )
            self.provider_sent, self.throttled = sent, throttled

        def call(self, target, signature_, block, *, args=(), **kwargs):
            self.provider._rpc("eth_call", [target, signature_, *map(str, args)])
            return super().call(target, signature_, block, args=args, **kwargs)

    return Throttled


def test_a_throttled_hub_read_is_retried_and_its_spokes_still_list(resolver, monkeypatch):
    expected = lending_markets(Avalanche(aave_v4_calls()), None, V4_BLOCK, query("aave_v4"))
    for signature in ("getSpokeCount(uint256)", "getSpokeAddress(uint256,uint256)"):
        reader = throttling(Avalanche, signature, 1, monkeypatch)(aave_v4_calls())
        assert lending_markets(reader, None, V4_BLOCK, query("aave_v4")) == expected
    signature = "getSpokeAddress(uint256,uint256)"
    reader = throttling(Avalanche, signature, ethereum.BUSY_RETRIES + 1, monkeypatch)(
        aave_v4_calls()
    )
    report = lending_markets(reader, None, V4_BLOCK, query("aave_v4"))
    # A throttle that outlasts the bounds names the Hub, as any other refusal does.
    assert report["markets"] == []
    assert report["source"]["unreadable"] == [
        {"hub": V4_HUB, "status": "refused", "reason": "http: eth_call: HTTP 429"}
    ]
    (throttled,) = reader.throttled
    assert reader.provider_sent.count(throttled) == ethereum.BUSY_RETRIES + 1


def test_a_throttled_morpho_state_read_is_retried_within_bounds(resolver, monkeypatch):
    supplies = [5_000_000, 90_000_000, 7, 0, 1_500_000]
    expected = lending_markets(morpho(supplies), None, BLOCK, query("morpho_blue", USDC))
    reader = morpho(supplies, throttling(Reader, "market(bytes32)", 1, monkeypatch))
    assert lending_markets(reader, None, BLOCK, query("morpho_blue", USDC)) == expected
    times = ethereum.BUSY_RETRIES + 1
    reader = morpho(supplies, throttling(Reader, "market(bytes32)", times, monkeypatch))
    with pytest.raises(ethereum.ProviderBusy, match="HTTP 429"):
        lending_markets(reader, None, BLOCK, query("morpho_blue", USDC))


def test_verified_markets_carry_their_observed_rates_and_refused_ones_none(monkeypatch, resolver):
    resolver.add(asset(OLD))
    observed = []

    def observed_rates(reader, connector, resolved, block):
        observed.append((connector, block))
        return {"rates": {"supply_apr": "0.023047691115"}}

    monkeypatch.setattr(ax_lending_markets, "observed_rates", observed_rates)
    reader = Reader(
        {
            (POOL, "getReservesList()"): ([WETH, OLD],),
            (POOL, "getConfiguration(address)"): lambda token: (reserve_config(0, 0),),
        }
    )
    verified, refused = lending_markets(reader, None, BLOCK, query("aave_v3"))["markets"]
    assert verified["rates"] == {"supply_apr": "0.023047691115"}
    assert refused["status"] == "refused" and "rates" not in refused
    assert observed == [("aave_v3", BLOCK)]


def comet_observation(base):
    curve = SimpleNamespace(kink=8 * 10**17, base_per_second=0)
    return SimpleNamespace(
        accounting=SimpleNamespace(
            total_supply_principal=10_000_000,
            total_borrow_principal=5_000_000,
            supply_index=10**15,
            borrow_index=10**15,
        ),
        supply_curve=SimpleNamespace(
            **vars(curve), slope_low_per_second=10**9, slope_high_per_second=0
        ),
        borrow_curve=SimpleNamespace(
            **vars(curve), slope_low_per_second=2 * 10**9, slope_high_per_second=0
        ),
        revision=Revision.STANDARD,
        base_available=5_000_000,
        base_asset=SimpleNamespace(identifier=AssetType(asset(base)), decimals=6),
    )


def test_listed_rates_convert_the_comet_read_and_scope_it_to_the_selected_asset(monkeypatch):
    # Only the resolver and the Comet's state read are faked; the listing's rates
    # path from `observed_rates` through the conversion runs as shipped.
    monkeypatch.setattr(ax_lending_markets, "observed_rates", lending_lookup.observed_rates)
    bases = {USDC_COMET: USDC, WETH_COMET: WETH}

    def market_config(reader, selected, request, block):
        reserve = SimpleNamespace(asset=request.asset, comet=request.comet)
        named = request.model_dump(mode="json", exclude_none=True)
        return SimpleNamespace(summary=dict, reserve=reserve), {}, {"market": named}

    def observe(reader, selected, resolved, block):
        assert block == BLOCK
        return comet_observation(bases[resolved.reserve.comet])

    monkeypatch.setattr(ax_lending_markets, "market_config", market_config)
    monkeypatch.setitem(lending_lookup.FAMILIES, "compound_v3", (None, observe, None, None))
    as_collateral, as_base = lending_markets(comets(), None, BLOCK, query("compound_v3", WETH))[
        "markets"
    ]
    # WETH is collateral in the USDC Comet: the market rates are USDC's, WETH earns nothing.
    collateral = as_collateral["rates"]
    assert (collateral["asset"], collateral["supply_apr"]) == (asset(USDC), "0.015768000000")
    assert collateral["selected_asset"] == {
        "asset": asset(WETH),
        "role": "collateral",
        "supply_apr": "0.000000000000",
        "supply_apy": "0.000000000000",
    }
    base = as_base["rates"]
    assert base["asset"] == asset(WETH)
    assert base["selected_asset"]["role"] == "lent"
    assert base["selected_asset"]["supply_apr"] == "0.015768000000"
