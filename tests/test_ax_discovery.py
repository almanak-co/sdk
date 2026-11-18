"""Token and pool discovery: vendor candidates, verified only through on-chain reads."""

from decimal import Decimal

import pytest

from almanak import ax_discovery
from almanak.ax_discovery import (
    PoolCandidate,
    PoolQuery,
    connector_for,
    currency,
    dex_pools,
    pool_candidate,
    recommended,
    resolve,
    token_address,
    token_candidate,
    usd,
    vendor_fee,
)
from almanak.caip import AssetType, ChainId
from almanak.connectors.uniswap_v4.networks import roles_for
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.resolution import authored_pool
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import currency_address
from almanak.gateway.onchain_lookup import LookupResult
from almanak.models import ZERO_ADDRESS, Block, Chain

BASE = ChainId("eip155:8453")
BLOCK = Block(
    chain=Chain(chain_id=BASE, environment="mainnet"),
    number=36_000_000,
    hash="0x" + "ab" * 32,
    timestamp=1_790_000_000,
)
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
FAKE = "0x" + "fa" * 20
POOL = "0x" + "11" * 20
OTHER_POOL = "0x" + "22" * 20


def pool_row(pool, dex, base, quote, reserve="1000", volume="10", name="WETH / USDC 0.05%"):
    return {
        "id": f"base_{pool}",
        "attributes": {
            "address": pool,
            "name": name,
            "reserve_in_usd": reserve,
            "volume_usd": {"h24": volume},
        },
        "relationships": {
            "base_token": {"data": {"id": f"base_{base}"}},
            "quote_token": {"data": {"id": f"base_{quote}"}},
            "dex": {"data": {"id": dex}},
        },
    }


def token_row(token, symbol, reserve):
    return {
        "attributes": {
            "address": token,
            "symbol": symbol,
            "name": symbol + " token",
            "decimals": 8,
            "coingecko_coin_id": symbol.lower(),
            "total_reserve_in_usd": reserve,
        }
    }


class Lookups:
    """Answers each lookup kind from a list of pages; records every request."""

    def __init__(self, pages):
        self.pages = pages
        self.requests = []

    def fetch(self, request):
        self.requests.append(request.lookup)
        kind = request.lookup.kind
        if kind == "token_pools" and request.lookup.token == ZERO_ADDRESS:
            # The vendor files native-coin pools under the zero address, a list of its own.
            kind = "native_pools"
        page = getattr(request.lookup, "page", 1)
        if kind not in self.pages:
            return LookupResult(request=request, result={"data": []})
        rows = self.pages[kind][page - 1] if page <= len(self.pages[kind]) else []
        if isinstance(rows, dict):
            # A whole vendor answer, such as the not-indexed marker of a 404.
            return LookupResult(request=request, result=rows)
        if kind == "tokens":
            # The vendor answers only the tokens a batch names.
            rows = [row for row in rows if row["attributes"]["address"] in request.lookup.tokens]
        return LookupResult(request=request, result={"data": rows})


class Reader:
    """Answers decimals() and symbol() per token; any other call is a test failure."""

    def __init__(self, tokens):
        self.tokens = tokens
        self.calls = []

    def call(self, target, signature, block, **kwargs):
        assert block == BLOCK
        self.calls.append((target, signature))
        value = self.tokens[target][signature]
        if isinstance(value, Exception):
            raise value
        return (value,)


def test_vendor_figures_stay_absent_when_missing():
    assert usd("12.509") == "12.50" and usd(7) == "7.00"
    assert usd("-0") == usd(-0.0) == usd("0.004") == "0.00"
    for value in (None, "", "abc", "-1", "NaN", True, {"x": 1}, "1e26"):
        assert usd(value) is None
    assert token_address(f"base_{WETH}", BASE) == WETH
    for identifier in (f"eth_{WETH}", "base_0x12", None):
        assert token_address(identifier, BASE) is None


def test_vendor_rows_parse_into_candidates_or_are_skipped():
    candidate = pool_candidate(pool_row(POOL, "uniswap-v3-base", WETH, USDC), BASE)
    assert candidate == PoolCandidate(
        pool=POOL,
        vendor_dex="uniswap-v3-base",
        name="WETH / USDC 0.05%",
        tokens=(WETH, USDC),
        liquidity_usd="1000.00",
        volume_24h_usd="10.00",
    )
    broken = pool_row("0x1234", "uniswap-v3-base", WETH, USDC)
    assert pool_candidate(broken, BASE) is None
    assert pool_candidate({"attributes": [], "relationships": {}}, BASE) is None
    assert pool_candidate({"attributes": {"address": POOL}, "relationships": []}, BASE) is None
    assert token_candidate(token_row(CBBTC, "cbBTC", None)).liquidity_usd is None
    assert token_candidate({"attributes": {"address": "0x12", "symbol": "X"}}) is None
    assert token_candidate({"attributes": {"address": WETH, "symbol": 5}}) is None
    assert token_candidate({"attributes": None}) is None


def test_dex_labels_only_choose_a_resolver():
    assert connector_for("aerodrome-slipstream-3") == "aerodrome_slipstream"
    assert connector_for("uniswap_v3_arbitrum") == "uniswap_v3"
    assert connector_for("uniswap-v3-base") == "uniswap_v3"
    assert connector_for("uniswap-v4-base") == "uniswap_v4"
    # Pons pools on the V4 PoolManager use a reviewed hook; Bankr's Doppler hooks are not.
    assert connector_for("pons-v2-dex") == "uniswap_v4"
    assert connector_for("bankr-robinhood") is None
    assert connector_for("pancakeswap-v3-bsc") == "pancakeswap_v3"
    assert connector_for("curve_arbitrum") == "curve"
    # Unknown labels, Uniswap V3 on BNB Chain among them, are matched by factory instead.
    for label in ("uniswap-bsc", "uniswap-v2-base", "quickswap-v4-base", "velodrome"):
        assert connector_for(label) is None


def test_vendor_fee_reads_whole_pips_from_the_pool_name():
    assert vendor_fee("WETH / USDC 0.3%") == 3000
    assert vendor_fee("WETH / USDC 0.05% ") == 500
    assert vendor_fee("WETH / USDC 0.00005%") is None
    assert vendor_fee("WETH / USDC") is None


def test_native_placeholders_name_the_native_asset():
    assert currency(BASE, "0x" + "00" * 20) == AssetType("eip155:8453/slip44:60")
    assert currency(BASE, "0x" + "ee" * 20) == AssetType("eip155:8453/slip44:60")
    assert currency(BASE, WETH) == AssetType(f"eip155:8453/erc20:{WETH}")


SEARCH = {
    "pool_search": [
        [pool_row(POOL, "uniswap-v3-base", CBBTC, WETH)] * 20,
        [pool_row(OTHER_POOL, "uniswap-v3-base", FAKE, USDC)],
    ],
    "tokens": [
        [
            token_row(CBBTC, "cbBTC", "5000000"),
            token_row(FAKE, "CBBTC", "100"),
            token_row(WETH, "WETH", "9000000"),
        ]
    ],
}
ONCHAIN = {
    CBBTC: {"decimals()": 8, "symbol()": "cbBTC"},
    FAKE: {"decimals()": 8, "symbol()": DataError("abi", "symbol(): invalid ABI response")},
    WETH: {"decimals()": 18, "symbol()": "WETH"},
}


def test_a_symbol_names_verified_candidates_and_recommends_only_a_clear_leader():
    lookups, reader = Lookups(SEARCH), Reader(ONCHAIN)
    report = resolve(lookups, reader, BLOCK, "CBBTC", limit=10)
    assert [c["asset"] for c in report["candidates"]] == [
        f"eip155:8453/erc20:{CBBTC}",
        f"eip155:8453/erc20:{FAKE}",
    ]
    assert report["candidates"][0]["status"] == "verified"
    assert report["candidates"][1]["onchain_symbol"] is None
    assert report["recommended"] == f"eip155:8453/erc20:{CBBTC}"
    assert report["source"] == {"provider": "coingecko_onchain", "complete": True}
    # Both search pages were read; the second was short, so the search ended there.
    assert [lookup.kind for lookup in lookups.requests] == ["pool_search"] * 2 + ["tokens"]
    assert set(lookups.requests[-1].tokens) == {CBBTC, WETH, FAKE, USDC}


def test_close_candidates_are_ambiguous():
    close = {
        **SEARCH,
        "tokens": [[token_row(CBBTC, "cbBTC", "500"), token_row(FAKE, "CBBTC", "100")]],
    }
    report = resolve(Lookups(close), Reader(ONCHAIN), BLOCK, "cbBTC", limit=10)
    assert report["recommended"] is None
    assert [c["status"] for c in report["candidates"]] == ["verified", "verified"]


def test_a_limit_of_one_still_compares_the_leader_with_the_next_match():
    close = {
        **SEARCH,
        "tokens": [[token_row(CBBTC, "cbBTC", "500"), token_row(FAKE, "CBBTC", "100")]],
    }
    report = resolve(Lookups(close), Reader(ONCHAIN), BLOCK, "cbBTC", limit=1)
    assert [c["asset"] for c in report["candidates"]] == [f"eip155:8453/erc20:{CBBTC}"]
    assert report["recommended"] is None


def test_an_address_query_reads_that_token_only():
    pages = {"tokens": [[token_row(CBBTC, "cbBTC", None)]]}
    lookups = Lookups(pages)
    report = resolve(lookups, Reader(ONCHAIN), BLOCK, CBBTC.upper().replace("X", "x"), 10)
    assert report["recommended"] == f"eip155:8453/erc20:{CBBTC}"
    assert [lookup.kind for lookup in lookups.requests] == ["tokens"]


def test_a_token_that_does_not_answer_decimals_is_refused_and_not_recommended():
    reader = Reader({CBBTC: {"decimals()": DataError("abi", "decimals(): invalid ABI response")}})
    report = resolve(
        Lookups({"tokens": [[token_row(CBBTC, "cbBTC", "1")]]}), reader, BLOCK, CBBTC, 10
    )
    assert report["candidates"][0]["status"] == "refused"
    assert report["recommended"] is None


def test_no_match_reports_no_candidates():
    pages = {"pool_search": [[]], "tokens": [[]]}
    lookups = Lookups(pages)
    report = resolve(lookups, Reader({}), BLOCK, "NOPE", 10)
    assert report["candidates"] == [] and report["recommended"] is None
    assert [lookup.kind for lookup in lookups.requests] == ["pool_search"]


def candidates(**overrides):
    rows = [
        {
            "asset": "a",
            "status": "verified",
            "symbol": "X",
            "onchain_symbol": "x",
            "liquidity_usd": "1000",
        },
        {
            "asset": "b",
            "status": "verified",
            "symbol": "X",
            "onchain_symbol": "X",
            "liquidity_usd": "10",
        },
    ]
    return [{**row, **overrides} for row in rows]


def test_recommendation_needs_a_confirmed_leader_with_a_measured_lead():
    first, second = candidates()
    assert recommended([first, second]) == "a"
    assert recommended([first]) == "a"
    assert recommended([]) is None
    assert recommended([first | {"status": "refused"}, second]) is None
    # The chain must confirm the symbol the vendor lists.
    assert recommended([first | {"onchain_symbol": None}]) is None
    assert recommended([first | {"onchain_symbol": "Y"}]) is None
    # A lead is only shown when both figures are measured.
    assert recommended([first | {"liquidity_usd": None}, second]) is None
    assert recommended([first, second | {"liquidity_usd": None}]) is None
    assert recommended([first, second | {"liquidity_usd": "101"}]) is None
    # Two figures below a cent show no lead, whatever the vendor's order.
    zero = {"liquidity_usd": "0.00"}
    assert recommended([first | zero, second | zero]) is None
    assert recommended([first, second | zero]) == "a"


def test_every_searched_token_is_looked_up_in_vendor_batches():
    many = [f"0x{index:040x}" for index in range(1, 41)]
    search = [pool_row("0x" + "77" * 20, "uniswap-v3-base", token, WETH) for token in many[:20]]
    search_two = [pool_row("0x" + "78" * 20, "uniswap-v3-base", token, WETH) for token in many[20:]]
    match = token_row(many[-1], "LATE", "50")
    lookups = Lookups({"pool_search": [search, search_two], "tokens": [[match]]})
    report = resolve(
        lookups, Reader({many[-1]: {"decimals()": 18, "symbol()": "LATE"}}), BLOCK, "LATE", 10
    )
    batches = [lookup.tokens for lookup in lookups.requests if lookup.kind == "tokens"]
    assert [len(batch) for batch in batches] == [30, 11]
    assert report["candidates"][0]["asset"] == f"eip155:8453/erc20:{many[-1]}"
    # Both search pages were full, so a later page may hold another, quieter match:
    # the cut is reported beside the recommendation rather than withholding it.
    assert report["source"]["complete"] is False
    assert "first 40 pools only" in report["source"]["limitation"]
    assert report["recommended"] == f"eip155:8453/erc20:{many[-1]}"


def test_a_search_the_vendor_has_not_indexed_recommends_nothing():
    for pages in (
        {"pool_search": [{"data": [], "indexed": False}], "tokens": [[]]},
        {"pool_search": SEARCH["pool_search"], "tokens": [{"data": [], "indexed": False}]},
    ):
        report = resolve(Lookups(pages), Reader(ONCHAIN), BLOCK, "cbBTC", 10)
        assert report["candidates"] == [] and report["recommended"] is None
        assert report["source"]["complete"] is False
        assert report["source"]["limitation"].startswith("the vendor has not indexed")


def test_an_unindexed_token_batch_in_a_cut_search_still_recommends_nothing():
    many = [f"0x{index:040x}" for index in range(1, 41)]
    search = [pool_row("0x" + "77" * 20, "uniswap-v3-base", token, WETH) for token in many[:20]]
    search_two = [pool_row("0x" + "78" * 20, "uniswap-v3-base", token, WETH) for token in many[20:]]
    match = token_row(many[0], "LATE", "50")

    class SecondBatchUnindexed(Lookups):
        def fetch(self, request):
            batches = [lookup for lookup in self.requests if lookup.kind == "tokens"]
            if request.lookup.kind == "tokens" and batches:
                self.requests.append(request.lookup)
                return LookupResult(request=request, result={"data": [], "indexed": False})
            return super().fetch(request)

    lookups = SecondBatchUnindexed({"pool_search": [search, search_two], "tokens": [[match]]})
    report = resolve(
        lookups, Reader({many[0]: {"decimals()": 18, "symbol()": "LATE"}}), BLOCK, "LATE", 10
    )
    assert report["candidates"][0]["asset"] == f"eip155:8453/erc20:{many[0]}"
    # The second batch may hold a larger match, so the cut search names no leader.
    assert report["source"]["complete"] is False
    assert report["source"]["limitation"].startswith("the vendor has not indexed")
    assert report["recommended"] is None


def test_an_address_the_vendor_does_not_list_is_still_read_on_chain():
    report = resolve(Lookups({"tokens": [[]]}), Reader(ONCHAIN), BLOCK, CBBTC, 10)
    (candidate,) = report["candidates"]
    assert candidate["asset"] == f"eip155:8453/erc20:{CBBTC}"
    assert (candidate["status"], candidate["decimals"], candidate["onchain_symbol"]) == (
        "verified",
        8,
        "cbBTC",
    )
    assert candidate["symbol"] is None and candidate["liquidity_usd"] is None
    # Without a vendor symbol the chain has nothing to confirm.
    assert report["recommended"] is None
    assert report["source"] == {"provider": "coingecko_onchain", "complete": True}


WETH_ASSET = AssetType(f"eip155:8453/erc20:{WETH}")
USDC_ASSET = AssetType(f"eip155:8453/erc20:{USDC}")
# What a fake resolver reads from every pool, whatever the vendor claims.
PAIR = {"currencies": [str(WETH_ASSET), str(USDC_ASSET)]}


def v4_id(fee, spacing):
    from almanak.connectors.uniswap_v4.resolution import MarketSelection

    selection = MarketSelection(
        connector="uniswap_v4",
        chain=BASE,
        currency0=WETH_ASSET,
        currency1=USDC_ASSET,
        fee=fee,
        tick_spacing=spacing,
    )
    return pool_id(authored_pool(selection, roles_for(BASE), BLOCK))


REFUSED_RESOLVED = "0x" + "55" * 20
UNISWAP_FACTORY = "0x" + "fc" * 20


class Factories:
    """Answers `factory()` per pool; a pool absent here reverts the call."""

    def __init__(self, factories):
        self.factories = factories
        self.calls = []

    def call(self, target, signature, block, **kwargs):
        assert (signature, block) == ("factory()", BLOCK)
        self.calls.append(target)
        if target not in self.factories:
            raise DataError("abi", "factory(): invalid ABI response")
        return (self.factories[target],)


@pytest.fixture
def resolvers(monkeypatch):
    """Fake resolvers for every connector; each call is recorded."""
    calls = []

    def read_pool_state(reader, connector, request, block, depth_bps):
        calls.append((connector, request.pool, depth_bps))
        if request.pool == OTHER_POOL:
            raise DataError("deployment_changed", "pool names another factory")
        resolution = {"selection": {"pool": request.pool}, "pool": request.pool} | PAIR
        state = {"support": "verified", "resolution": resolution, "price": None}
        return state | ({"depth": {"bps": depth_bps}} if depth_bps else {})

    class Resolved:
        hook = None

        def summary(self):
            return {"selection": {"connector": "uniswap_v4"}, "pool_id": "resolved"}

    def read_resolved_pool(reader, connector, request, block):
        calls.append((connector, request.pool))
        if request.pool == REFUSED_RESOLVED:
            raise ValueError(f"{connector} declares no support on {request.chain}")
        resolution = {"selection": {"pool": request.pool}, "pool": request.pool}
        key = "coins" if connector == "curve" else "currencies"
        return {"support": "verified", "resolution": resolution | {key: PAIR["currencies"]}}

    monkeypatch.setattr(ax_discovery, "read_pool_state", read_pool_state)
    monkeypatch.setattr(ax_discovery, "read_resolved_pool", read_resolved_pool)
    monkeypatch.setattr(
        ax_discovery,
        "resolve_market",
        lambda reader, selection, block: calls.append(("uniswap_v4", selection.fee)) or Resolved(),
    )
    return calls


def query(**changes):
    values = {
        "token": WETH_ASSET,
        "quote": None,
        "connector": None,
        "limit": 20,
        "depth_bps": None,
    }
    return PoolQuery(**{**values, **changes})


def test_pools_keep_the_vendor_order_and_are_verified_by_their_resolver(resolvers):
    hooked = "0x" + "99" * 32
    pages = {
        "token_pools": [
            [
                pool_row(POOL, "uniswap-v3-base", WETH, USDC, reserve="500"),
                pool_row(OTHER_POOL, "aerodrome-slipstream-3", WETH, USDC, reserve="900"),
                pool_row(
                    v4_id(3000, 60), "uniswap-v4-base", WETH, USDC, "800", name="WETH / USDC 0.3%"
                ),
                pool_row(hooked, "uniswap-v4-base", WETH, USDC, "700", name="WETH / USDC 0.3%"),
                pool_row("0x" + "33" * 20, "aerodrome-base", WETH, USDC, reserve=None),
                pool_row("0x" + "44" * 20, "quickswap-v4-base", WETH, USDC, reserve="1"),
            ]
        ]
    }
    report = dex_pools(Lookups(pages), Factories({}), BLOCK, query(depth_bps=200))
    statuses = [(pool["connector"], pool["status"]) for pool in report["pools"]]
    assert statuses == [
        ("uniswap_v3", "verified"),
        ("aerodrome_slipstream", "refused"),
        ("uniswap_v4", "verified"),
        ("uniswap_v4", "unverified"),
        ("aerodrome", "verified"),
        (None, "unsupported"),
    ]
    assert report["pools"][4]["market"]["connector"] == "aerodrome"
    assert report["pools"][4]["resolution"] == {"pool": "0x" + "33" * 20} | PAIR
    assert report["source"] == {"provider": "coingecko_onchain", "complete": True}
    assert report["page"] == {"offset": 0, "limit": 20, "matched": 6, "has_more": False}
    assert report["pools"][1]["reason"].startswith("deployment_changed")
    assert report["pools"][2]["market"]["fee"] == 3000
    assert report["pools"][0]["market"] == {
        "connector": "uniswap_v3",
        "chain": str(BASE),
        "pool": POOL,
    }
    assert report["pools"][0]["depth"] == {"bps": 200}
    # The resolver's request echo is dropped; `market` states it once.
    assert report["pools"][0]["resolution"] == {"pool": POOL} | PAIR
    assert report["pools"][2]["resolution"] == {"pool_id": "resolved"}
    assert report["pools"][4]["liquidity_usd"] is None
    assert ("uniswap_v4", 3000) in resolvers


def test_quote_connector_and_budget_narrow_what_is_read(resolvers):
    pages = {
        "token_pools": [
            pool_page(1),
            [pool_row(OTHER_POOL, "uniswap-v3-base", WETH, CBBTC)],
        ]
    }
    lookups = Lookups(pages)
    usdc_only = query(quote=USDC_ASSET, connector="uniswap_v3", limit=3)
    report = dex_pools(lookups, None, BLOCK, usdc_only)
    assert [pool["status"] for pool in report["pools"]] == ["verified", "verified", "verified"]
    # One page already holds more matches than the window, so no further page is read.
    assert [lookup.page for lookup in lookups.requests] == [1]
    assert report["page"]["has_more"] is True
    assert resolvers == [("uniswap_v3", "0x" + f"{index:040x}", None) for index in (1, 2, 3)]
    other = dex_pools(Lookups(pages), None, BLOCK, query(connector="curve"))
    assert other["pools"] == []


def test_a_listing_longer_than_the_pages_read_is_not_complete(resolvers):
    pages = {"token_pools": [pool_page(1), pool_page(21), pool_page(41)]}
    report = dex_pools(Lookups(pages), None, BLOCK, query())
    assert report["source"]["complete"] is False
    # An incomplete list says so in words: a missing pool may still exist.
    assert "a pool missing here may still exist" in report["source"]["limitation"]
    assert len(report["pools"]) == 20
    assert report["page"] == {"offset": 0, "limit": 20, "matched": 40, "has_more": True}


def test_a_sparse_filter_stops_at_the_page_cap_without_promising_more(resolvers):
    def rare(start):
        page = pool_page(start)
        return page[:1] + [row | {"relationships": cbbtc_pair} for row in page[1:]]

    cbbtc_pair = pool_row(OTHER_POOL, "uniswap-v3-base", WETH, CBBTC)["relationships"]
    lookups = Lookups({"token_pools": [rare(start) for start in range(1, 141, 20)]})
    report = dex_pools(lookups, None, BLOCK, query(quote=USDC_ASSET))
    assert len(lookups.requests) == ax_discovery.MAX_POOL_PAGES
    # The native coin's list, read for the wrapped token, shares the page cap.
    assert [lookup.token for lookup in lookups.requests].count(ZERO_ADDRESS) == 1
    assert report["source"]["complete"] is False
    assert report["page"] == {"offset": 0, "limit": 20, "matched": 4, "has_more": False}


def pool_page(start):
    return [
        pool_row("0x" + f"{index:040x}", "uniswap-v3-base", WETH, USDC)
        for index in range(start, start + 20)
    ]


def test_an_offset_pages_through_the_vendor_order(resolvers):
    pages = {"token_pools": [pool_page(1), pool_page(21), pool_page(41)[:5]]}
    lookups = Lookups(pages)
    report = dex_pools(lookups, None, BLOCK, query(offset=25, limit=10))
    assert [pool["pool"] for pool in report["pools"]] == [
        "0x" + f"{index:040x}" for index in range(26, 36)
    ]
    assert report["page"] == {"offset": 25, "limit": 10, "matched": 40, "has_more": True}
    # The wrapped token's list and the native coin's (empty here) are read a page in turn.
    assert [(lookup.token, lookup.page) for lookup in lookups.requests] == [
        (WETH, 1),
        (ZERO_ADDRESS, 1),
        (WETH, 2),
    ]
    last = dex_pools(Lookups(pages), None, BLOCK, query(offset=40, limit=10))
    assert len(last["pools"]) == 5
    assert last["page"] == {"offset": 40, "limit": 10, "matched": 45, "has_more": False}
    assert last["source"]["complete"] is True


def test_a_pool_the_vendor_moves_onto_the_next_page_is_reported_once(resolvers):
    first = pool_page(1)
    pages = {"token_pools": [first, first[-1:] + pool_page(21)[:5]]}
    report = dex_pools(Lookups(pages), None, BLOCK, query(offset=15, limit=10))
    assert [pool["pool"] for pool in report["pools"]] == [
        "0x" + f"{index:040x}" for index in range(16, 26)
    ]
    assert report["page"] == {"offset": 15, "limit": 10, "matched": 25, "has_more": False}


def test_a_curve_pool_is_resolved_or_refused_by_its_connector(resolvers):
    pages = {
        "token_pools": [
            [
                pool_row(POOL, "curve-base", WETH, USDC),
                pool_row(REFUSED_RESOLVED, "curve-base", WETH, USDC),
            ]
        ]
    }
    report = dex_pools(Lookups(pages), Factories({}), BLOCK, query())
    assert [pool["status"] for pool in report["pools"]] == ["verified", "refused"]
    assert report["pools"][1]["reason"] == "invalid: curve declares no support on eip155:8453"


def test_an_unknown_label_is_matched_by_the_reviewed_factory(resolvers, monkeypatch):
    monkeypatch.setattr(
        ax_discovery, "reviewed_factories", lambda chain: {UNISWAP_FACTORY: "uniswap_v3"}
    )
    reviewed, other, silent = POOL, "0x" + "66" * 20, "0x" + "67" * 20
    pages = {
        "token_pools": [
            [
                pool_row(reviewed, "uniswap-bsc", WETH, USDC),
                pool_row(other, "topaz", WETH, USDC),
                pool_row(silent, "topaz", WETH, USDC),
                pool_row("0x" + "68" * 32, "unknown-v4", WETH, USDC),
                pool_row("0x" + "69" * 20, "topaz", WETH, USDC),
                pool_row(OTHER_POOL, "topaz", WETH, USDC),
            ]
        ]
    }
    reader = Factories(
        {reviewed: UNISWAP_FACTORY, other: "0x" + "01" * 20, OTHER_POOL: UNISWAP_FACTORY}
    )
    report = dex_pools(Lookups(pages), reader, BLOCK, query())
    assert [(pool["connector"], pool["status"]) for pool in report["pools"]] == [
        ("uniswap_v3", "verified"),
        (None, "unsupported"),
        (None, "unsupported"),
        (None, "unsupported"),
        (None, "unsupported"),
        # The factory chose the connector, so its refusal names that connector.
        ("uniswap_v3", "refused"),
    ]
    # A V4 pool ID names no contract, so no factory read is made for it.
    assert reader.calls == [reviewed, other, silent, "0x" + "69" * 20, OTHER_POOL]


def test_reviewed_factories_name_each_declared_deployment():
    factories = ax_discovery.reviewed_factories(BASE)
    assert set(factories.values()) == {
        "uniswap_v3",
        "pancakeswap_v3",
        "aerodrome_slipstream",
        "aerodrome",
    }
    # A chain without an Aerodrome deployment simply has no Aerodrome factory.
    assert "aerodrome" not in ax_discovery.reviewed_factories(ChainId("eip155:56")).values()


def test_a_v4_pool_without_a_derivable_key_is_not_guessed(resolvers):
    odd_fee = pool_row("0x" + "77" * 32, "uniswap-v4-base", WETH, USDC, name="WETH / USDC 0.07%")
    missing_token = pool_row("0x" + "78" * 32, "uniswap-v4-base", WETH, "0x12", name="X 0.3%")
    report = dex_pools(Lookups({"token_pools": [[odd_fee, missing_token]]}), None, BLOCK, query())
    assert [pool["status"] for pool in report["pools"]] == ["unverified", "unverified"]
    assert resolvers == []


def test_ranking_never_reads_missing_liquidity_as_zero():
    assert ax_discovery.rank(None) < ax_discovery.rank("0") == Decimal(0)


class InitializeLogs:
    """Answers the PoolManager `Initialize` query with the given events, or refuses it."""

    def __init__(self, events, chain=None):
        self.events = events
        self.queries = []
        self.chain = chain or BLOCK.chain

    def _rpc(self, method, params):
        assert method == "eth_getLogs"
        self.queries.append(params[0])
        if isinstance(self.events, Exception):
            raise self.events
        return self.events


def initialize_event(
    identifier,
    *,
    fee=4900,
    spacing=98,
    hooks=ZERO_ADDRESS,
    manager=None,
    currencies=(WETH, USDC),
    chain=BASE,
):
    from eth_abi import encode

    word = "0x" + "00" * 12
    return {
        "address": manager or roles_for(chain).pool_manager,
        "topics": [
            ax_discovery.V4_INITIALIZE,
            identifier,
            word + currencies[0][2:],
            word + currencies[1][2:],
        ],
        "data": "0x"
        + encode(
            ["uint24", "int24", "address", "uint160", "int24"], [fee, spacing, hooks, 2**96, 0]
        ).hex(),
    }


def custom_key_id(spacing=98, fee=4900, hooks=ZERO_ADDRESS):
    from almanak.connectors.uniswap_v4.resolution import MarketSelection

    selection = MarketSelection(
        connector="uniswap_v4",
        chain=BASE,
        currency0=WETH_ASSET,
        currency1=USDC_ASSET,
        fee=fee,
        tick_spacing=spacing,
        hooks=hooks,
    )
    return ax_discovery.key_id(selection.chain, selection.key)


def test_a_custom_spacing_v4_pool_is_keyed_by_its_initialize_event(resolvers):
    identifier = custom_key_id()
    row = pool_row(identifier, "uniswap-v4-base", WETH, USDC, name="WETH / USDC 0.49%")
    reader = InitializeLogs([initialize_event(identifier)])
    report = dex_pools(Lookups({"token_pools": [[row]]}), reader, BLOCK, query())
    pool = report["pools"][0]
    assert pool["status"] == "verified"
    assert pool["support"] == "verified"
    assert (pool["market"]["fee"], pool["market"]["tick_spacing"]) == (4900, 98)
    assert reader.queries == [
        {
            "address": roles_for(BASE).pool_manager,
            "topics": [ax_discovery.V4_INITIALIZE, identifier],
            "fromBlock": "0x0",
            "toBlock": hex(BLOCK.number),
        }
    ]


def test_a_v4_pool_whose_vendor_repeats_a_token_is_keyed_by_its_initialize_event(resolvers):
    identifier = custom_key_id()
    row = pool_row(identifier, "uniswap-v4-base", WETH, WETH, name="WETH / USDC 0.3%")
    reader = InitializeLogs([initialize_event(identifier)])
    report = dex_pools(Lookups({"token_pools": [[row]]}), reader, BLOCK, query())
    pool = report["pools"][0]
    assert pool["status"] == "verified"
    assert (pool["market"]["fee"], pool["market"]["tick_spacing"]) == (4900, 98)


def test_a_native_currency_v4_pool_is_verified_as_the_wrapped_token_it_holds(resolvers):
    from almanak.connectors.uniswap_v4.resolution import MarketSelection

    native = MarketSelection(
        connector="uniswap_v4",
        chain=BASE,
        currency0=AssetType("eip155:8453/slip44:60"),
        currency1=USDC_ASSET,
        fee=3000,
        tick_spacing=60,
    )
    identifier = pool_id(authored_pool(native, roles_for(BASE), BLOCK))
    row = pool_row(identifier, "uniswap-v4-base", ZERO_ADDRESS, USDC, name="ETH / USDC 0.3%")
    # The fund holds WETH for the native leg, so a WETH request matches the pool.
    report = dex_pools(Lookups({"token_pools": [[row]]}), None, BLOCK, query(token=WETH_ASSET))
    (pool,) = report["pools"]
    assert (pool["connector"], pool["status"]) == ("uniswap_v4", "verified")
    assert pool["market"]["currency0"] == "eip155:8453/slip44:60"
    assert ("uniswap_v4", 3000) in resolvers


CBBTC_ASSET = AssetType(f"eip155:8453/erc20:{CBBTC}")
NATIVE = "eip155:8453/slip44:60"


def native_row(other, name, fee, spacing):
    from almanak.connectors.uniswap_v4.resolution import MarketSelection

    # The native coin's zero address always sorts first in a PoolKey.
    native = MarketSelection(
        connector="uniswap_v4",
        chain=BASE,
        currency0=AssetType(NATIVE),
        currency1=AssetType(f"eip155:8453/erc20:{other}"),
        fee=fee,
        tick_spacing=spacing,
    )
    identifier = pool_id(authored_pool(native, roles_for(BASE), BLOCK))
    return pool_row(identifier, "uniswap-v4-base", ZERO_ADDRESS, other, name=name)


def test_a_wrapped_native_token_also_reads_the_native_coin_pools(resolvers):
    wrapped = pool_row(POOL, "uniswap-v3-base", WETH, USDC)
    native = native_row(USDC, "ETH / USDC 0.3%", 3000, 60)
    pages = {
        "token_pools": [[wrapped]],
        "native_pools": [
            [
                native,
                # Only a connector holding a native leg as its wrapped token matches WETH.
                pool_row(OTHER_POOL, "fluid-base", "0x" + "ee" * 20, USDC, name="ETH / USDC"),
                pool_row("0x" + "44" * 32, "unknown-v4", ZERO_ADDRESS, USDC, name="ETH / USDC"),
            ]
        ],
    }
    lookups = Lookups(pages)
    report = dex_pools(lookups, None, BLOCK, query(quote=USDC_ASSET))
    assert [lookup.token for lookup in lookups.requests] == [WETH, ZERO_ADDRESS]
    assert [pool["pool"] for pool in report["pools"]] == [POOL, native["attributes"]["address"]]
    assert report["source"]["complete"] is True
    # The market says which currency the pool takes: native here, WETH in the other.
    (_, pool) = report["pools"]
    assert (pool["connector"], pool["status"]) == ("uniswap_v4", "verified")
    assert pool["market"]["currency0"] == NATIVE
    assert ("uniswap_v4", 3000) in resolvers


def test_a_wrapped_native_quote_keeps_native_coin_pools(resolvers):
    native = native_row(CBBTC, "ETH / cbBTC 0.05%", 500, 10)
    pages = {"token_pools": [[native, pool_row(POOL, "uniswap-v3-base", CBBTC, USDC)]]}
    lookups = Lookups(pages)
    report = dex_pools(lookups, None, BLOCK, query(token=CBBTC_ASSET, quote=WETH_ASSET))
    # A token other than the wrapped native reads only its own list.
    assert [lookup.token for lookup in lookups.requests] == [CBBTC]
    (pool,) = report["pools"]
    assert pool["pool"] == native["attributes"]["address"]
    assert pool["market"]["currency0"] == NATIVE
    # A native pool of a connector that holds no wrapped leg does not match WETH.
    fluid = pool_row(OTHER_POOL, "fluid-base", CBBTC, "0x" + "ee" * 20, name="cbBTC / ETH")
    unheld = dex_pools(
        Lookups({"token_pools": [[fluid]]}), None, BLOCK, query(token=CBBTC_ASSET, quote=WETH_ASSET)
    )
    assert unheld["pools"] == []


def test_native_coin_pools_the_vendor_has_not_indexed_leave_the_list_incomplete(resolvers):
    pages = {
        "token_pools": [[pool_row(POOL, "uniswap-v3-base", WETH, USDC)]],
        "native_pools": [{"data": [], "indexed": False}],
    }
    report = dex_pools(Lookups(pages), None, BLOCK, query())
    assert [pool["pool"] for pool in report["pools"]] == [POOL]
    assert report["source"]["complete"] is False
    assert "a pool missing here may still exist" in report["source"]["limitation"]


def test_an_unindexed_wrapped_token_list_still_reads_the_native_coin_pools(resolvers):
    native = native_row(USDC, "ETH / USDC 0.3%", 3000, 60)
    pages = {"token_pools": [{"data": [], "indexed": False}], "native_pools": [[native]]}
    lookups = Lookups(pages)
    report = dex_pools(lookups, None, BLOCK, query())
    assert [lookup.token for lookup in lookups.requests] == [WETH, ZERO_ADDRESS]
    assert [pool["pool"] for pool in report["pools"]] == [native["attributes"]["address"]]
    assert report["pools"][0]["market"]["currency0"] == NATIVE
    # The wrapped token's own pools are still unknown, so the source says so first.
    assert report["source"]["complete"] is False
    assert report["source"]["limitation"].startswith("the vendor has not indexed this token")
    # It outranks a native list cut at the page cap.
    many = {
        "token_pools": [{"data": [], "indexed": False}],
        "native_pools": [native_page(start) for start in range(1, 141, 20)],
    }
    cut = dex_pools(Lookups(many), None, BLOCK, query(limit=1, offset=60))
    assert cut["source"]["limitation"].startswith("the vendor has not indexed this token")


def native_page(start, matching=20):
    # Native-coin V4 pools (held as WETH), then native pools of a venue holding no wrapped leg.
    v4 = (
        pool_row("0x" + f"{i:064x}", "uniswap-v4-base", ZERO_ADDRESS, USDC, name="ETH / USDC")
        for i in range(start, start + matching)
    )
    other = (
        pool_row("0x" + f"{i:040x}", "fluid-base", ZERO_ADDRESS, USDC, name="ETH / USDC")
        for i in range(start + matching, start + 20)
    )
    return [*v4, *other]


def test_a_pool_in_both_lists_is_reported_once(resolvers):
    native = native_row(USDC, "ETH / USDC 0.3%", 3000, 60)
    wrapped = pool_row(POOL, "uniswap-v3-base", WETH, USDC)
    pages = {"token_pools": [[native, wrapped]], "native_pools": [[wrapped, native]]}
    report = dex_pools(Lookups(pages), None, BLOCK, query())
    assert [pool["pool"] for pool in report["pools"]] == [native["attributes"]["address"], POOL]
    assert report["page"]["matched"] == 2
    assert report["source"]["complete"] is True


def test_native_coin_pages_are_read_in_turn_at_the_default_limit(resolvers):
    pages = {
        "token_pools": [pool_page(1)[:3]],
        "native_pools": [native_page(start, matching=5) for start in range(101, 201, 20)],
    }
    lookups = Lookups(pages)
    first = dex_pools(lookups, None, BLOCK, query(limit=10))
    # The wrapped list ends on its first page; native pages continue until one match past
    # the window is known.
    assert [(lookup.token, lookup.page) for lookup in lookups.requests] == [
        (WETH, 1),
        (ZERO_ADDRESS, 1),
        (ZERO_ADDRESS, 2),
    ]
    assert len(first["pools"]) == 10
    assert first["page"] == {"offset": 0, "limit": 10, "matched": 13, "has_more": True}
    assert first["source"]["complete"] is False
    lookups = Lookups(pages)
    second = dex_pools(lookups, None, BLOCK, query(limit=10, offset=10))
    assert len(lookups.requests) == ax_discovery.MAX_POOL_PAGES
    assert [lookup.page for lookup in lookups.requests if lookup.token == ZERO_ADDRESS] == [
        1,
        2,
        3,
        4,
    ]
    assert second["page"] == {"offset": 10, "limit": 10, "matched": 23, "has_more": True}
    # Rows past the first window continue the same order, native pools after the three wrapped.
    assert second["pools"][0]["pool"] == "0x" + f"{123:064x}"
    assert second["source"]["complete"] is False


def test_unverified_rows_label_the_vendor_currencies_native_or_wrapped(resolvers):
    # Twins the vendor names alike; neither key is derivable without a fee in the name.
    native = pool_row("0x" + "77" * 32, "uniswap-v4-base", ZERO_ADDRESS, USDC, name="ETH / USDC")
    wrapped = pool_row("0x" + "78" * 32, "uniswap-v4-base", WETH, USDC, name="ETH / USDC")
    unsupported = pool_row("0x" + "33" * 20, "quickswap-base", "0x" + "ee" * 20, None)
    verified = pool_row(POOL, "uniswap-v3-base", WETH, USDC)
    pages = {"token_pools": [[verified, wrapped, unsupported]], "native_pools": [[native]]}
    report = dex_pools(Lookups(pages), Factories({}), BLOCK, query())
    rows = {pool["pool"]: pool for pool in report["pools"]}
    assert "unverified_vendor_currencies" not in rows[POOL]
    assert rows["0x" + "77" * 32]["status"] == "unverified"
    assert rows["0x" + "77" * 32]["unverified_vendor_currencies"] == [NATIVE, str(USDC_ASSET)]
    assert rows["0x" + "78" * 32]["unverified_vendor_currencies"] == [
        str(WETH_ASSET),
        str(USDC_ASSET),
    ]
    # A refused or unsupported row is labelled the same, and an omitted token stays absent.
    assert rows["0x" + "33" * 20]["status"] == "unsupported"
    assert rows["0x" + "33" * 20]["unverified_vendor_currencies"] == [NATIVE, None]


WRAPPED_NATIVE = {
    "eip155:1": ("eip155:1/slip44:60", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "eth"),
    "eip155:56": ("eip155:56/slip44:714", "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", "bsc"),
    "eip155:137": (
        "eip155:137/slip44:966",
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
        "polygon_pos",
    ),
    "eip155:43114": (
        "eip155:43114/slip44:9005",
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        "avax",
    ),
    "eip155:8453": ("eip155:8453/slip44:60", WETH, "base"),
}


@pytest.mark.parametrize("chain", sorted(WRAPPED_NATIVE))
def test_each_chain_maps_its_native_coin_to_its_own_wrapped_token(chain):
    native, wrapped, prefix = WRAPPED_NATIVE[chain]
    chain_id = ChainId(chain)
    assert ax_discovery.wrapped_native(chain_id) == wrapped
    stable = "0x" + "5a" * 20
    row = pool_row("0x" + "77" * 32, "uniswap-v4-x", ZERO_ADDRESS, stable, name="N / S")
    for link in ("base_token", "quote_token"):
        vendor_id = row["relationships"][link]["data"]["id"]
        row["relationships"][link]["data"]["id"] = vendor_id.replace("base_", prefix + "_", 1)
    native_pool = pool_candidate(row, chain_id)
    asked = PoolQuery(
        token=AssetType(f"{chain}/erc20:{wrapped}"),
        quote=None,
        connector=None,
        limit=10,
        depth_bps=None,
    )
    lookups = Lookups({"native_pools": [[row]]})
    matches, _ = ax_discovery.token_pools(lookups, chain_id, asked, lambda pool: None)
    assert [lookup.token for lookup in lookups.requests] == [wrapped, ZERO_ADDRESS]
    assert matches == [native_pool]
    assert ax_discovery.held(native_pool, chain_id) == {ZERO_ADDRESS, stable, wrapped}
    claimed = ax_discovery.pool_report(native_pool, chain_id, {"status": "unverified"})
    assert claimed["unverified_vendor_currencies"] == [native, f"{chain}/erc20:{stable}"]


def test_a_connector_without_native_legs_reads_only_the_token_list(resolvers):
    lookups = Lookups({"token_pools": [[pool_row(POOL, "uniswap-v3-base", WETH, USDC)]]})
    report = dex_pools(lookups, None, BLOCK, query(connector="uniswap_v3"))
    assert [lookup.token for lookup in lookups.requests] == [WETH]
    assert report["source"]["complete"] is True


def test_an_unreviewed_hook_is_refused_by_the_connector_before_any_pool_read():
    hooks = "0x" + "12" * 20
    identifier = custom_key_id(fee=10000, spacing=200, hooks=hooks)
    row = pool_row(identifier, "uniswap-v4-base", WETH, USDC, name="WETH / USDC 1%")
    reader = InitializeLogs([initialize_event(identifier, fee=10000, spacing=200, hooks=hooks)])
    report = dex_pools(Lookups({"token_pools": [[row]]}), reader, BLOCK, query())
    (pool,) = report["pools"]
    assert (pool["connector"], pool["status"]) == ("uniswap_v4", "refused")
    assert pool["reason"] == (
        f"unsupported_pool: Uniswap V4 hook {hooks} has no reviewed profile on eip155:8453;"
        " hooked pool behavior is supported only for reviewed hooks"
    )
    assert "market" not in pool and "hook" not in pool
    # Only the Initialize query was read: admission refused the key before any pool read.
    assert len(reader.queries) == 1


ROBINHOOD_BLOCK = BLOCK.model_copy(
    update={"chain": Chain(chain_id=ChainId("eip155:4663"), environment="mainnet")}
)


def robinhood_key(name, **changes):
    from almanak.connectors.uniswap_v4.resolution import MarketSelection
    from tests.uniswap_hook_support import ROBINHOOD, key

    chain, currency0, currency1, fee, spacing, hooks = key(name)
    values = {
        "connector": "uniswap_v4",
        "chain": ROBINHOOD.chain_id,
        "currency0": currency(ROBINHOOD.chain_id, currency0),
        "currency1": currency(ROBINHOOD.chain_id, currency1),
        "fee": fee,
        "tick_spacing": spacing,
        "hooks": hooks,
    }
    return MarketSelection(**(values | changes))


def verify_initialized(selection):
    identifier = ax_discovery.key_id(selection.chain, selection.key)
    event = initialize_event(
        identifier,
        fee=selection.fee,
        spacing=selection.tick_spacing,
        hooks=selection.hooks,
        currencies=tuple(
            currency_address(c, selection.chain) for c in (selection.currency0, selection.currency1)
        ),
        chain=selection.chain,
    )
    candidate = PoolCandidate(identifier, "uniswap-v4-robinhood", "X / Y", (None, None), None, None)
    reader = InitializeLogs([event], chain=ROBINHOOD_BLOCK.chain)
    return ax_discovery.verify_v4(reader, ROBINHOOD_BLOCK, candidate)


def test_a_reviewed_hook_is_verified_with_its_profile_pin_and_terms(monkeypatch):
    from almanak.connectors.uniswap_v4.hook_terms import OutputHaircut
    from almanak.connectors.uniswap_v4.resolution import HookAdmission
    from tests.uniswap_hook_support import PONS_HOOK, pinned

    selection = robinhood_key("pons")
    admission = HookAdmission(
        profile="pons-v2-meme",
        identity=pinned(PONS_HOOK),
        terms=OutputHaircut(hook_fee_bps=100, creator_tax_bps=50),
    )

    class Resolved:
        hook = admission

        def summary(self):
            return {"selection": {}, "hooks": PONS_HOOK, "hook_profile": "pons-v2-meme"}

    seen = []
    monkeypatch.setattr(
        ax_discovery,
        "resolve_market",
        lambda reader, chosen, block: seen.append(chosen) or Resolved(),
    )
    report = verify_initialized(selection)
    assert seen == [selection]
    assert report["status"] == "verified"
    assert report["market"]["hooks"] == PONS_HOOK
    assert report["hook"] == {
        "profile": "pons-v2-meme",
        "address": PONS_HOOK,
        "pin": "match",
        "terms": {"kind": "output_haircut", "hook_fee_bps": 100, "creator_tax_bps": 50},
    }


def test_a_reviewed_hook_with_the_wrong_fee_mode_is_refused_with_the_connector_reason():
    from almanak.connectors.uniswap_v4.hooks import DYNAMIC_FEE

    with pytest.raises(DataError) as refused:
        verify_initialized(robinhood_key("fables", fee=3000))
    assert refused.value.kind == "unsupported_pool"
    assert "requires the dynamic-fee flag" in str(refused.value)
    with pytest.raises(DataError, match="requires a static fee"):
        verify_initialized(robinhood_key("pons", fee=DYNAMIC_FEE))


def test_a_native_pool_with_a_reviewed_hook_is_resolved_by_the_connector(monkeypatch):
    from almanak.connectors.uniswap_v4.hooks import DYNAMIC_FEE, registry

    eth_hook = next(
        address
        for (chain, address), entry in registry().items()
        if chain == "eip155:4663" and entry.profile == "fables-ramp-eth"
    )
    seen = []

    class Resolved:
        hook = None

        def summary(self):
            return {"selection": {}}

    monkeypatch.setattr(
        ax_discovery,
        "resolve_market",
        lambda reader, chosen, block: seen.append(chosen) or Resolved(),
    )
    selection = robinhood_key(
        "fables",
        currency0=currency(ChainId("eip155:4663"), ZERO_ADDRESS),
        fee=DYNAMIC_FEE,
        hooks=eth_hook,
    )
    report = verify_initialized(selection)
    assert seen == [selection] and report["status"] == "verified"


@pytest.mark.parametrize(
    "events",
    [
        [],
        "two",
        "other manager",
        "removed",
        "short topics",
        "short data",
        "bad padding",
        DataError("http", "block range too large"),
        [{"address": "0x12"}],
    ],
)
def test_an_initialize_answer_that_is_not_one_canonical_event_leaves_the_key_unknown(events):
    identifier = custom_key_id()
    event = initialize_event(identifier)
    answers = {
        "two": [event, event],
        "other manager": [initialize_event(identifier, manager="0x" + "99" * 20)],
        "removed": [event | {"removed": True}],
        "short topics": [event | {"topics": event["topics"][:2]}],
        "short data": [event | {"data": "0x12"}],
        "bad padding": [event | {"data": "0x" + "ff" * 160}],
    }
    reader = InitializeLogs(answers.get(events, events) if isinstance(events, str) else events)
    assert ax_discovery.initialized_key(reader, BLOCK, identifier) is None


@pytest.mark.parametrize(
    ("changes", "missing"),
    [
        ({"token": AssetType(f"eip155:8453/erc20:{CBBTC}")}, CBBTC),
        ({"quote": AssetType(f"eip155:8453/erc20:{CBBTC}")}, CBBTC),
    ],
)
def test_a_pool_that_does_not_trade_the_requested_pair_is_refused(resolvers, changes, missing):
    # The vendor lists the pool under the requested pair; the connector reads WETH/USDC.
    row = pool_row(
        POOL, "uniswap-v3-base", *((CBBTC, USDC) if "token" in changes else (WETH, CBBTC))
    )
    report = dex_pools(Lookups({"token_pools": [[row]]}), None, BLOCK, query(**changes))
    (pool,) = report["pools"]
    assert pool["status"] == "refused"
    assert pool["reason"].startswith("vendor_mismatch: the pool trades")
    assert pool["reason"].endswith(f"not eip155:8453/erc20:{missing}")
    assert "market" not in pool


def test_a_connector_filter_keeps_a_pool_its_factory_names(resolvers, monkeypatch):
    monkeypatch.setattr(
        ax_discovery, "reviewed_factories", lambda chain: {UNISWAP_FACTORY: "uniswap_v3"}
    )
    other = "0x" + "66" * 20
    rows = [pool_row(POOL, "uniswap-bsc", WETH, USDC), pool_row(other, "topaz", WETH, USDC)]
    reader = Factories({POOL: UNISWAP_FACTORY, other: "0x" + "01" * 20})
    report = dex_pools(
        Lookups({"token_pools": [rows]}), reader, BLOCK, query(connector="uniswap_v3")
    )
    assert [(pool["pool"], pool["status"]) for pool in report["pools"]] == [(POOL, "verified")]
    assert report["page"]["matched"] == 1


def test_a_token_the_vendor_lists_under_another_symbol_is_found_by_its_own():
    # The vendor lists USD₮0 as "USDT"; only its own symbol() names it USD₮0.
    usdt0 = "0x" + "d0" * 20
    listed = {
        "pool_search": [[pool_row(POOL, "uniswap-v3-base", usdt0, WETH)]],
        "tokens": [[token_row(usdt0, "USDT", "900"), token_row(WETH, "WETH", "9000000")]],
    }
    reader = Reader({usdt0: {"decimals()": 6, "symbol()": "USD₮0"}, WETH: {"symbol()": "WETH"}})
    report = resolve(Lookups(listed), reader, BLOCK, "usd₮0", limit=10)
    assert [c["asset"] for c in report["candidates"]] == [f"eip155:8453/erc20:{usdt0}"]
    assert report["candidates"][0]["onchain_symbol"] == "USD₮0"


def test_a_token_matching_by_vendor_symbol_does_not_hide_one_matching_by_its_own():
    usdt0, copycat = "0x" + "d0" * 20, "0x" + "c0" * 20
    listed = {
        "pool_search": [
            [
                pool_row(POOL, "uniswap-v3-base", usdt0, WETH),
                pool_row(OTHER_POOL, "uniswap-v3-base", copycat, WETH),
            ]
        ],
        "tokens": [
            [
                token_row(usdt0, "USDT", "50000000"),
                token_row(copycat, "USD₮0", "50"),
                token_row(WETH, "WETH", "9000000"),
            ]
        ],
    }
    reader = Reader(
        {
            usdt0: {"decimals()": 6, "symbol()": "USD₮0"},
            copycat: {"decimals()": 6, "symbol()": "USD₮0"},
            WETH: {"symbol()": "WETH"},
        }
    )
    report = resolve(Lookups(listed), reader, BLOCK, "USD₮0", limit=10)
    assert [c["asset"] for c in report["candidates"]] == [
        f"eip155:8453/erc20:{usdt0}",
        f"eip155:8453/erc20:{copycat}",
    ]
    # The leader's vendor symbol differs from its own, so neither is recommended.
    assert report["recommended"] is None
    # Each token's symbol() is read once, though both candidates report it.
    reads = [target for target, signature in reader.calls if signature == "symbol()"]
    assert sorted(reads) == sorted({usdt0, copycat, WETH})


def test_a_unicode_symbol_is_a_valid_query():
    from almanak.gateway.onchain_lookup import PoolSearch

    assert PoolSearch(chain=BASE, query="USD₮0").query == "USD₮0"
    for unsafe in ("a/b", "a?b=1", "<x>", "$ABC"):
        with pytest.raises(ValueError):
            PoolSearch(chain=BASE, query=unsafe)


def test_a_token_the_vendor_has_not_indexed_is_unknown_not_empty():
    # A 404 is the vendor's missing data; it must not read as a complete, empty list.
    report = dex_pools(
        Lookups({"token_pools": [{"data": [], "indexed": False}]}), None, BLOCK, query()
    )
    assert report["pools"] == []
    assert report["source"]["complete"] is False
    assert report["source"]["limitation"].startswith("the vendor has not indexed this token")


def test_uniswap_v4_reports_support_like_the_other_pool_connectors():
    from almanak.connectors.v3_pool_request import support_level

    # Every chain V4 declares has production execution, which the others call verified.
    assert support_level("uniswap_v4", BASE) == "verified"
    with pytest.raises(ValueError, match="uniswap_v4 declares no support on eip155:324"):
        support_level("uniswap_v4", ChainId("eip155:324"))


def test_a_uniswap_v4_candidate_chain_the_resolver_accepts_is_a_candidate(monkeypatch):
    from datetime import UTC, datetime

    from almanak.connectors import support
    from almanak.connectors.v3_pool_request import support_level

    start = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(support, "UNISWAP_CANDIDATE_STARTS", (("eip155:324", start),))
    assert support_level("uniswap_v4", ChainId("eip155:324")) == "candidate"


def test_the_reviewed_factory_map_is_read_once_per_chain(monkeypatch):
    imported = []
    real = ax_discovery.import_module

    def counting(name):
        imported.append(name)
        return real(name)

    ax_discovery.reviewed_factories.cache_clear()
    monkeypatch.setattr(ax_discovery, "import_module", counting)
    try:
        first = ax_discovery.reviewed_factories(BASE)
        assert ax_discovery.reviewed_factories(BASE) is first
        assert len(imported) == len(ax_discovery.FACTORY_CONNECTORS)
        with pytest.raises(TypeError):
            first["0x" + "00" * 20] = "uniswap_v3"  # type: ignore[index]
    finally:
        ax_discovery.reviewed_factories.cache_clear()
