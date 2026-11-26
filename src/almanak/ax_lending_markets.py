"""Lending market discovery for `almanak ax lending-markets`.

Each protocol's own records at the pinned block name the candidate markets: an
Aave V3 or SparkLend Pool's reserve list, the reserves of the Spokes an Aave V4
Hub serves, the Comets a Compound V3 Configurator deployed and the Morpho Blue
markets, from Morpho's API on the chains it indexes and from the markets the core
created elsewhere. A record only names a candidate. Each reported market
is resolved by its connector as `ax lending-reserve` resolves it, and is
verified with its config-ready entries and the annual rates of the reserve read
`lending-reserve` makes, or refused with the resolver's reason.
"""

import math
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from importlib import import_module
from typing import NamedTuple

from eth_utils import keccak

from almanak.ax_discovery import block_report, concurrently, refusal, usd
from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.hubs import hubs_for
from almanak.connectors.aave_v4.reader import read_reserve_config, read_reserve_data
from almanak.connectors.compound_v3.configurators import configurator_for
from almanak.connectors.compound_v3.resolution import ASSET_INFO
from almanak.connectors.discovery import lending_discovery
from almanak.connectors.lending_lookup import family, market_config, observed_rates, selection
from almanak.connectors.lending_risk import comet_factors, morpho_incentive, pool_bonus_bps
from almanak.connectors.morpho_blue.networks import core_for
from almanak.connectors.pool_lending_domain import ReserveConfiguration
from almanak.gateway.ethereum import DataError, throttled
from almanak.gateway.evm import erc20_address
from almanak.gateway.evm_logs import data, topic_address, topic_bytes
from almanak.gateway.morpho_lookup import API_CHAINS, MorphoMarkets
from almanak.gateway.onchain_lookup import LookupRequest
from almanak.models import ZERO_ADDRESS, Block, address
from almanak.report_values import exact_decimal

COMET_DEPLOYED = "0x" + keccak(text="CometDeployed(address,address)").hex()
CREATE_MARKET = (
    "0x" + keccak(text="CreateMarket(bytes32,(address,address,address,address,uint256))").hex()
)
# totalSupplyAssets, totalSupplyShares, totalBorrowAssets, totalBorrowShares, lastUpdate, fee
MARKET_STATE = ("uint128",) * 6
# Independent record reads (Hub, Spoke, reserve and Comet enumeration, Morpho market
# states) run concurrently. A Morpho core holds thousands of markets, most of them
# empty; a listing that would read more than the cap is refused.
MAX_STATE_READS = 5000
# The fact fields that name assets; each is reported beside its on-chain symbol.
ASSET_FACTS = ("asset", "base_asset", "collateral_assets", "loan_asset", "collateral_asset")


@dataclass(frozen=True)
class MarketQuery:
    connector: str
    asset: AssetType | None
    collateral: AssetType | None
    limit: int
    offset: int = 0
    include_unlisted: bool = False


@dataclass(frozen=True)
class Candidate:
    # The connector's market selection, in the fields an author writes under `markets:`.
    selection: dict
    # What the protocol's record says about the market, reported beside the verdict.
    facts: dict


class Listing(NamedTuple):
    # The requested page of candidates, how many match in all, and where they came from.
    page: list[Candidate]
    matched: int
    source: dict


class MarketParams(NamedTuple):
    market_id: str
    loan: str
    collateral: str
    lltv: int


def lending_markets(reader, lookups, block: Block, query: MarketQuery) -> dict:
    if query.include_unlisted and query.connector != "morpho_blue":
        raise ValueError(
            "--include-unlisted applies to morpho_blue, whose API marks listed markets"
        )
    listing = LISTINGS[query.connector](reader, lookups, block, query)
    # Each page market is resolved and observed independently, so they are read concurrently.
    reports = concurrently(lambda c: market_status(reader, block, query.connector, c), listing.page)
    names = {asset: token_symbol(reader, block, asset) for asset in sorted(set(assets(reports)))}
    return {
        "product": "lending_markets",
        "connector": query.connector,
        "chain": str(block.chain.chain_id),
        "asset": None if query.asset is None else str(query.asset),
        "collateral": None if query.collateral is None else str(query.collateral),
        "block": block_report(block),
        "source": listing.source,
        "page": {
            "offset": query.offset,
            "limit": query.limit,
            "matched": listing.matched,
            "has_more": listing.matched > query.offset + query.limit,
        },
        "markets": [labelled(report, names) for report in reports],
    }


def market_status(reader, block: Block, connector: str, candidate: Candidate) -> dict:
    facts = {**candidate.facts, **reserve_state(reader, block, connector, candidate)}
    try:
        request = selection(connector, {"chain": str(block.chain.chain_id), **candidate.selection})
        resolved, _, config = market_config(reader, family(connector), request, block)
    except (DataError, ValueError) as error:
        return {**facts, **refusal(error)}
    claim = unconfirmed(candidate.facts, resolved.summary())
    if claim is not None:
        return {**facts, "status": "refused", "reason": f"vendor_mismatch: {claim}"}
    rates = observed_rates(reader, connector, resolved, block)
    return {**facts, "status": "verified", "config": config, **rates}


def unconfirmed(facts: dict, resolution: dict) -> str | None:
    """An asset the listing claims for the market that its resolved record contradicts.

    Morpho's API names a market's assets beside its ID; only the market's own
    parameters, read at the pinned block, verify them. An API row that names no
    collateral claims an idle market, whose collateral is the zero address.
    """
    claims = {} if facts.get("loan_asset") is None else {"loan_asset": facts["loan_asset"]}
    if "vendor" in facts:
        claims["collateral_asset"] = facts["vendor"].get("collateral_asset")
    for name, claimed in claims.items():
        own = resolution.get(name)
        if own is not None and held(claimed) != held(own):
            return f"the listing names {name} {claimed or 'none'}, the market's own is {own}"
    return None


def held(asset: object) -> str | None:
    # An idle market's collateral is the zero address, which Morpho's API names as none.
    text = None if asset is None else str(asset).lower()
    return None if text is None or text.endswith(":" + ZERO_ADDRESS) else text


def reserve_state(reader, block: Block, connector: str, candidate: Candidate) -> dict:
    """The reserve's own flags, where the protocol keeps them per reserve.

    A resolver verifies the deployment's revisions, not whether the reserve takes
    deposits or loans now: a frozen reserve or one with no loan-to-value still
    resolves. A flag read that fails is reported as missing, never as a default.
    """
    flags = RESERVE_FLAGS.get(connector)
    if flags is None:
        return {}
    try:
        return {"reserve": flags(reader, block, candidate)}
    except DataError:
        return {"reserve": None}


def pool_flags(pool_for):
    def flags(reader, block: Block, candidate: Candidate) -> dict:
        token = candidate.selection["asset"].split("/erc20:")[1]
        (raw,) = reader.call(
            pool_for(block.chain.chain_id),
            "getConfiguration(address)",
            block,
            inputs=("address",),
            args=(token,),
            outputs=("uint256",),
        )
        config = ReserveConfiguration(raw=raw)
        return {
            "active": config.active,
            "frozen": config.frozen,
            "paused": config.paused,
            "borrowing_enabled": config.borrowing,
            "ltv_bps": config.ltv_bps,
            "liquidation_threshold_bps": config.liquidation_threshold_bps,
            "liquidation_bonus_bps": pool_bonus_bps(config.liquidation_bonus_bps),
        }

    return flags


def spoke_flags(reader, block: Block, candidate: Candidate) -> dict:
    spoke, reserve_id = candidate.selection["spoke"], candidate.selection["reserve_id"]
    config = read_reserve_config(reader, spoke, reserve_id, block)
    return {"frozen": config.frozen, "paused": config.paused, "borrowable": config.borrowable}


def whole(listing):
    """A listing that names every candidate; the requested page is cut from them here.

    Only the page's candidates are resolved and have their flags and symbols read.
    """

    def page(reader, lookups, block: Block, query: MarketQuery) -> Listing:
        candidates, source = listing(reader, block, query)
        end = query.offset + query.limit
        return Listing(candidates[query.offset : end], len(candidates), source)

    return page


def windowed(entries: list, read, query: MarketQuery, filtered: bool):
    """The requested page of a record kept in its own fixed order, read no further than needed.

    `read` makes an entry's candidate, or None where the filter drops it. Unfiltered,
    every entry matches, so only the page's entries are read and `matched` counts all
    of them; filtered, every entry is read to count the matches. An entry whose read
    fails is returned beside its error, and is not on the page.
    """
    end = query.offset + query.limit
    chosen = entries if filtered else entries[query.offset : end]
    results = settled(read, chosen)
    failed = [(e, r) for e, r in zip(chosen, results, strict=True) if isinstance(r, DataError)]
    found = [result for result in results if isinstance(result, Candidate)]
    if filtered:
        return found[query.offset : end], len(found), failed
    return found, len(entries), failed


def settled(read, items: list) -> list:
    """Each item's read, concurrently, in item order; a refused read answers its DataError.

    Any other error is a bug, and ends the listing as `concurrently` does.
    """

    def attempt(item):
        try:
            return read(item)
        except DataError as error:
            return error

    return concurrently(attempt, items)


def created(reader, block: Block, contract: str, topic: str, topics: int) -> list[dict]:
    """Every event of one kind the contract emitted up to the pinned block.

    The whole history is asked for at once. A provider that refuses the range
    fails the listing rather than shortening it, and a log from any other
    contract or event, or with another topic count, refuses the answer. The range
    ends at the pinned block's number, so its hash is confirmed at that height
    after the read.
    """
    span = {"fromBlock": "0x0", "toBlock": hex(block.number)}
    try:
        logs = reader._rpc("eth_getLogs", [{"address": contract, "topics": [topic], **span}])
    except DataError as error:
        if error.kind not in ("rpc", "http") or throttled(error):
            raise
        # A provider refusing the request itself; some cap the block range per chain.
        raise DataError(
            error.kind,
            f"eth_getLogs: the provider refused {contract}'s event history in one request"
            f" ({error})",
            code=error.code,
        ) from None
    if not isinstance(logs, list) or not all(emitted(log, contract, topic, topics) for log in logs):
        raise DataError("malformed", "eth_getLogs: invalid event list")
    if reader.pin_block(block.number) != block:
        raise DataError("reorganization", "eth_getLogs: the pinned block is no longer canonical")
    return logs


def emitted(log: object, contract: str, topic: str, count: int) -> bool:
    if not isinstance(log, dict) or log.get("removed"):
        return False
    topics = log.get("topics")
    return (
        isinstance(topics, list)
        and len(topics) == count
        and str(topics[0]).lower() == topic
        and str(log.get("address", "")).lower() == contract
    )


def asset_type(chain: ChainId, token: str) -> str:
    return f"{chain}/erc20:{address(token)}"


# --- Aave V3 and SparkLend --------------------------------------------------------


def pool_reserves(pool_for):
    def listing(reader, block: Block, query: MarketQuery):
        reserve_markets(query)
        chain = block.chain.chain_id
        pool = pool_for(chain)
        (reserves,) = reader.call(pool, "getReservesList()", block, outputs=("address[]",))
        wanted = None if query.asset is None else erc20_address(query.asset, chain)
        candidates = [
            Candidate({"asset": asset_type(chain, token)}, {"asset": asset_type(chain, token)})
            for token in reserves
            if wanted in (None, address(token))
        ]
        return candidates, {"record": "pool_reserves", "contract": pool, "listed": len(reserves)}

    return listing


def reserve_markets(query: MarketQuery) -> None:
    if query.collateral is not None:
        raise ValueError(
            "--collateral filters Compound V3 and Morpho Blue markets;"
            " each Aave V3, Aave V4 or SparkLend reserve is its own market"
        )


# --- Aave V4 ----------------------------------------------------------------------


def spoke_reserves(reader, lookups, block: Block, query: MarketQuery) -> Listing:
    reserve_markets(query)
    chain = block.chain.chain_id
    hubs = hubs_for(chain)
    spokes, unreadable = served_spokes(reader, block, hubs)
    counts, without_reserves, unread = spoke_counts(reader, block, spokes)
    wanted = None if query.asset is None else erc20_address(query.asset, chain)

    def read(entry: tuple[str, int]) -> Candidate | None:
        spoke, reserve_id = entry
        reserve = read_reserve_data(reader, spoke, reserve_id, block)
        if wanted not in (None, address(reserve.underlying)):
            return None
        return reserve_candidate(chain, spoke, reserve_id, reserve)

    reserves = [(spoke, index) for spoke, count in counts.items() for index in range(count)]
    page, matched, failed = windowed(reserves, read, query, wanted is not None)
    # One reserve that cannot be read is named; the others still list.
    unread += [{"spoke": s, "reserve_id": i, **refusal(error)} for (s, i), error in failed]
    source = {
        "record": "hub_spokes",
        "contracts": list(hubs),
        "listed": len(counts),
        "without_reserves": without_reserves,
        "unreadable": unreadable + unread,
    }
    return Listing(page, matched, source)


def spoke_counts(reader, block: Block, spokes: list[str]) -> tuple[dict[str, int], int, list]:
    """Each Spoke's reserve count, how many Spokes hold none, and the Spokes not read."""
    counts, without_reserves, unreadable = {}, 0, []
    answers = settled(lambda spoke: reader.call(spoke, "getReserveCount()", block)[0], spokes)
    for spoke, answer in zip(spokes, answers, strict=True):
        if not isinstance(answer, DataError):
            counts[spoke] = answer
        elif answer.kind == "rpc" and answer.reverted:
            # Hubs also serve treasury and tokenization Spokes, which revert the call.
            # The reader marks a revert whichever code the node answers it with (3, or
            # -32000 "execution reverted" without data). A throttle, an undecodable
            # answer, a transport failure or another -32000 refusal is no such answer.
            without_reserves += 1
        else:
            unreadable.append({"spoke": spoke, **refusal(answer)})
    return counts, without_reserves, unreadable


def served_spokes(reader, block: Block, hubs) -> tuple[list[str], list[dict]]:
    """Each Spoke the Hubs serve, once, in Hub, asset and Spoke order, and each Hub not read.

    A Spoke can draw on several Hubs, and on several assets of one Hub. A Hub that
    fails any read is named whole; none of its Spokes are taken from it.
    """
    failed: dict[str, DataError] = {}
    assets = hub_reads(reader, block, [(hub,) for hub in hubs], "getAssetCount()", failed)
    pairs = [(hub, asset_id) for (hub,), count in assets for asset_id in range(count)]
    counts = hub_reads(reader, block, pairs, "getSpokeCount(uint256)", failed)
    slots = [(*pair, index) for pair, count in counts for index in range(count)]
    found = hub_reads(reader, block, slots, "getSpokeAddress(uint256,uint256)", failed, "address")
    spokes = [address(spoke) for (hub, *_), spoke in found if hub not in failed]
    unreadable = [{"hub": hub, **refusal(failed[hub])} for hub in hubs if hub in failed]
    return list(dict.fromkeys(spokes)), unreadable


def hub_reads(reader, block: Block, keys: list[tuple], signature: str, failed, output="uint256"):
    """One read per (Hub, *arguments) key, concurrently, answered in key order.

    Keys of a Hub already in `failed` are not read; a refusal adds the key's Hub.
    """
    live = [key for key in keys if key[0] not in failed]

    def read(key: tuple):
        hub, *args = key
        inputs = ("uint256",) * len(args)
        return reader.call(hub, signature, block, inputs=inputs, args=args, outputs=(output,))[0]

    answered = []
    for key, answer in zip(live, settled(read, live), strict=True):
        if isinstance(answer, DataError):
            failed.setdefault(key[0], answer)
        else:
            answered.append((key, answer))
    return answered


def reserve_candidate(chain: ChainId, spoke: str, reserve_id: int, reserve) -> Candidate:
    facts = {
        "spoke": spoke,
        "reserve_id": reserve_id,
        "hub": address(reserve.hub),
        "asset": asset_type(chain, reserve.underlying),
    }
    return Candidate({"spoke": spoke, "reserve_id": reserve_id}, facts)


# --- Compound V3 ------------------------------------------------------------------


def comet_markets(reader, lookups, block: Block, query: MarketQuery) -> Listing:
    chain = block.chain.chain_id
    configurator = configurator_for(chain)
    # An upgrade deploys a new implementation for the same Comet and records it again.
    comets = list(dict.fromkeys(comet_proxies(reader, block, configurator)))

    def read(comet: str) -> Candidate | None:
        base, collateral, factors = comet_assets(reader, block, comet)
        if not comet_matches(query, chain, base, collateral):
            return None
        return comet_candidate(query, chain, comet, base, collateral, factors)

    filtered = query.asset is not None or query.collateral is not None
    page, matched, failed = windowed(comets, read, query, filtered)
    source = {
        "record": "comet_deployments",
        "contract": configurator,
        "listed": len(comets),
        # A Comet whose assets cannot be read cannot be matched or resolved; it is
        # named here rather than failing the whole listing.
        "unreadable": [{"comet": comet, **refusal(error)} for comet, error in failed],
    }
    return Listing(page, matched, source)


def comet_proxies(reader, block: Block, configurator: str) -> Iterator[str]:
    for log in created(reader, block, configurator, COMET_DEPLOYED, 3):
        try:
            yield topic_address(log["topics"][1])
        except DataError:
            raise DataError("malformed", "CometDeployed: invalid Comet address") from None


def comet_candidate(
    query: MarketQuery, chain: ChainId, comet: str, base: str, collateral, factors: list[dict]
):
    # The selection names the asked-for asset, base or collateral, or else the base.
    chosen = base if query.asset is None else erc20_address(query.asset, chain)
    facts = {
        "comet": comet,
        "base_asset": asset_type(chain, base),
        "collateral_assets": [asset_type(chain, token) for token in collateral],
        # In the order of collateral_assets, as `lending-reserve` labels them.
        "collateral_factors": factors,
    }
    return Candidate({"comet": comet, "asset": asset_type(chain, chosen)}, facts)


def comet_assets(reader, block: Block, comet: str) -> tuple[str, list[str], list[dict]]:
    (base,) = reader.call(comet, "baseToken()", block, outputs=("address",))
    (count,) = reader.call(comet, "numAssets()", block, outputs=("uint8",))
    collateral, factors = [], []
    for index in range(count):
        (info,) = reader.call(
            comet,
            "getAssetInfo(uint8)",
            block,
            inputs=("uint8",),
            args=(index,),
            outputs=(ASSET_INFO,),
        )
        if info[0] != index:
            raise DataError("deployment_changed", "Comet asset list returned another offset")
        collateral.append(address(info[1]))
        factors.append(comet_factors(*info[4:7]))
    return address(base), collateral, factors


def comet_matches(query: MarketQuery, chain: ChainId, base: str, collateral: list[str]) -> bool:
    if query.asset is not None and erc20_address(query.asset, chain) not in (base, *collateral):
        return False
    return query.collateral is None or erc20_address(query.collateral, chain) in collateral


# --- Morpho Blue ------------------------------------------------------------------


def morpho_markets(reader, lookups, block: Block, query: MarketQuery) -> Listing:
    # A fixed source per chain: the API where it indexes the chain, the core's
    # events elsewhere. One never stands in for the other when it fails.
    if block.chain.chain_id in API_CHAINS:
        return indexed_markets(lookups, block.chain.chain_id, query)
    return whole(core_markets)(reader, lookups, block, query)


def loan_address(query: MarketQuery, chain: ChainId) -> str:
    if query.asset is None:
        raise ValueError(
            "--asset is required for morpho_blue: name the loan asset;"
            " a Morpho core holds thousands of markets"
        )
    return erc20_address(query.asset, chain)


def indexed_markets(lookups, chain: ChainId, query: MarketQuery) -> Listing:
    loan = loan_address(query, chain)
    collateral = None if query.collateral is None else erc20_address(query.collateral, chain)
    lookup = MorphoMarkets(
        chain=chain,
        loan=loan,
        collateral=collateral,
        listed_only=not query.include_unlisted,
        first=query.limit,
        skip=query.offset,
    )
    page = lookups.fetch(LookupRequest(lookup=lookup)).result
    info = page.get("pageInfo") if isinstance(page, dict) else None
    total = info.get("countTotal") if isinstance(info, dict) else None
    items = page.get("items") if isinstance(page, dict) else None
    if type(total) is not int or total < 0 or not isinstance(items, list):
        raise DataError("malformed", "morpho_api: invalid market page")
    parsed = [indexed_candidate(chain, item, loan, collateral) for item in items]
    candidates = [candidate for candidate in parsed if candidate is not None]
    # Rows in another shape, or for another loan or collateral asset than asked,
    # are counted, not reported.
    source = {
        "record": "morpho_api",
        "listed_only": lookup.listed_only,
        # Each row carries Morpho's own `listed` flag and warnings, under `vendor`.
        "curation": "morpho_api",
        "skipped": len(parsed) - len(candidates),
    }
    return Listing(candidates, total, source)


def indexed_candidate(
    chain: ChainId, item: object, loan: str, collateral: str | None
) -> Candidate | None:
    """One API market row as a candidate.

    Only the market ID and the asked-for loan asset are reported as the market's
    own; everything else is Morpho's, under `vendor`, until the resolver reads the
    market at the pinned block.
    """
    if not isinstance(item, dict):
        return None
    try:
        market_id = "0x" + topic_bytes(item["marketId"]).hex()
        loan_asset = item["loanAsset"]
        named = (item.get("collateralAsset") or {}).get("address")
        backing = None if named is None else address(named)
        if address(loan_asset["address"]) != loan or collateral not in (None, backing):
            return None
    except (DataError, KeyError, TypeError, ValueError, AttributeError):
        return None
    # Each figure Morpho omits is null; the market still lists.
    state = item.get("state")
    state = state if isinstance(state, dict) else {}
    decimals = loan_asset.get("decimals")
    vendor = {
        "collateral_asset": None if backing is None else asset_type(chain, backing),
        "lltv": lltv(vendor_integer(item.get("lltv"))),
        "liquidation_incentive_factor": incentive(vendor_integer(item.get("lltv"))),
        "supplied": vendor_amount(state.get("supplyAssets"), decimals),
        "borrowed": vendor_amount(state.get("borrowAssets"), decimals),
        "supplied_usd": usd(state.get("supplyAssetsUsd")),
        "borrowed_usd": usd(state.get("borrowAssetsUsd")),
        "supply_apy": ratio(state.get("supplyApy")),
        "listed": item["listed"] if isinstance(item.get("listed"), bool) else None,
        "warnings": vendor_warnings(item.get("warnings")),
    }
    facts = {"market_id": market_id, "loan_asset": asset_type(chain, loan), "vendor": vendor}
    return Candidate({"market_id": market_id, "leg": "loan"}, facts)


def vendor_warnings(raw: object) -> list[str]:
    rows = raw if isinstance(raw, list) else ()
    return sorted({str(w["type"]) for w in rows if isinstance(w, dict) and w.get("type")})


def vendor_integer(raw: object) -> int | None:
    """A vendor integer, sent as a JSON number or, past 2**53, as a digit string."""
    if isinstance(raw, str) and raw.isascii() and raw.isdecimal():
        return int(raw)
    return raw if type(raw) is int and raw >= 0 else None


def vendor_amount(raw: object, decimals: object) -> str | None:
    value = vendor_integer(raw)
    if value is None or type(decimals) is not int:
        return None
    return amount(value, decimals)


def lltv(wad: int | None) -> str | None:
    return None if wad is None else str(Decimal(wad).scaleb(-18).normalize())


def incentive(wad: int | None) -> str | None:
    """Morpho's liquidation incentive factor for an LLTV, as `lending-reserve` reports it."""
    if wad is None or not 0 < wad < 10**18:
        return None
    return str(Decimal(morpho_incentive(wad)).scaleb(-18).normalize())


def ratio(value: object) -> str | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return str(Decimal(str(value))) if math.isfinite(value) else None


def core_markets(reader, block: Block, query: MarketQuery):
    chain = block.chain.chain_id
    loan = loan_address(query, chain)
    core = core_for(chain)
    listed = [market_params(log) for log in created(reader, block, core, CREATE_MARKET, 2)]
    collateral = None if query.collateral is None else erc20_address(query.collateral, chain)
    matching = [p for p in listed if p.loan == loan and collateral in (None, p.collateral)]
    if len(matching) > MAX_STATE_READS:
        narrow = "" if collateral else "; narrow them with --collateral"
        raise ValueError(f"{len(matching)} Morpho markets match, over {MAX_STATE_READS}{narrow}")
    states = market_states(reader, block, core, [p.market_id for p in matching])
    (decimals,) = reader.call(loan, "decimals()", block, outputs=("uint8",))
    # Markets nobody supplies are left out; the source block counts them. Supply
    # and borrow are the totals the core last recorded, before interest since then.
    supplied = sorted(
        ((state, params) for state, params in zip(states, matching, strict=True) if state[0]),
        key=lambda pair: pair[0][0],
        reverse=True,
    )
    candidates = [morpho_candidate(chain, params, state, decimals) for state, params in supplied]
    source = {
        "record": "core_markets",
        # The core's events carry no curation: every supplied market is listed,
        # so --include-unlisted changes nothing here.
        "listed_only": False,
        "curation": None,
        "contract": core,
        "listed": len(listed),
        "matching": len(matching),
        "without_supply": len(matching) - len(supplied),
    }
    return candidates, source


def market_params(log: dict) -> MarketParams:
    try:
        market_id = "0x" + topic_bytes(log["topics"][1]).hex()
        loan, collateral, _, _, lltv = data(log, ["address"] * 4 + ["uint256"])
    except (DataError, KeyError, TypeError):
        raise DataError("malformed", "CreateMarket: invalid market parameters") from None
    return MarketParams(market_id, address(loan), address(collateral), lltv)


def market_states(reader, block: Block, core: str, market_ids: list[str]) -> list[tuple]:
    def read(market_id: str) -> tuple:
        return reader.call(
            core,
            "market(bytes32)",
            block,
            inputs=("bytes32",),
            args=(bytes.fromhex(market_id[2:]),),
            outputs=MARKET_STATE,
        )

    # A failed read ends the listing.
    return concurrently(read, market_ids)


def morpho_candidate(chain: ChainId, params: MarketParams, state: tuple, decimals: int):
    facts = {
        "market_id": params.market_id,
        "loan_asset": asset_type(chain, params.loan),
        "collateral_asset": None
        if params.collateral == ZERO_ADDRESS
        else asset_type(chain, params.collateral),
        "lltv": lltv(params.lltv),
        "liquidation_incentive_factor": incentive(params.lltv),
        "supplied": amount(state[0], decimals),
        "borrowed": amount(state[2], decimals),
    }
    return Candidate({"market_id": params.market_id, "leg": "loan"}, facts)


def amount(raw: int, decimals: int) -> str:
    return exact_decimal(raw, decimals, trim=False)


# --- symbols ----------------------------------------------------------------------


def assets(reports: list[dict]) -> Iterator[str]:
    for report in reports:
        for fields in (report, report.get("vendor") or {}):
            for name in ASSET_FACTS:
                value = fields.get(name)
                yield from (a for a in (value if isinstance(value, list) else (value,)) if a)


def labelled(fields: dict, names: dict) -> dict:
    """Each reported asset beside its on-chain `symbol()`, null where it answers none."""
    out = {}
    for key, value in fields.items():
        if key in ASSET_FACTS:
            out[key] = (
                [named(asset, names) for asset in value]
                if isinstance(value, list)
                else named(value, names)
            )
        else:
            out[key] = labelled(value, names) if key == "vendor" else value
    return out


def named(asset: str | None, names: dict) -> dict | None:
    return None if asset is None else {"asset": asset, "symbol": names[asset]}


def token_symbol(reader, block: Block, asset: str) -> str | None:
    try:
        (symbol,) = reader.call(asset.split("/erc20:")[1], "symbol()", block, outputs=("string",))
    except DataError:
        # Some tokens answer symbol() as bytes32; the symbol is a label, never identity.
        return None
    return symbol


def pool_for(connector: str):
    return import_module(f"almanak.connectors.{connector}.networks").pool_for


# Each listing record's reader, and the reserve flags it reports, for a connector.
LISTERS = {
    "pool_reserves": lambda connector: whole(pool_reserves(pool_for(connector))),
    "hub_spokes": lambda connector: spoke_reserves,
    "comet_deployments": lambda connector: comet_markets,
    "morpho_markets": lambda connector: morpho_markets,
}
FLAG_READERS = {
    "pool_reserves": lambda connector: pool_flags(pool_for(connector)),
    "hub_spokes": lambda connector: spoke_flags,
}
LISTINGS = {c: LISTERS[found.listing](c) for c, found in lending_discovery().items()}
RESERVE_FLAGS = {
    c: FLAG_READERS[found.listing](c)
    for c, found in lending_discovery().items()
    if found.listing in FLAG_READERS
}
