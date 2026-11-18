"""Token and pool discovery for `almanak ax resolve` and `ax dex-pools`.

CoinGecko on-chain names the candidates; the chain decides. A token candidate
is verified by reading its `decimals()` and `symbol()` at the pinned block, and
a pool candidate by the connector's own resolver, which refuses a pool from an
unreviewed deployment. Vendor liquidity and volume are reported as the vendor's
figures, absent when the vendor omits them, and never become strategy inputs.
A symbol names candidates only: the author still states the CAIP-19 identity.
"""

import contextvars
import re
import unicodedata
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from functools import cache
from importlib import import_module
from types import MappingProxyType

from eth_abi import decode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak

from almanak.caip import AssetType, ChainId
from almanak.connectors.discovery import pool_discovery
from almanak.connectors.uniswap_v4.networks import roles_for
from almanak.connectors.uniswap_v4.pool import key_id
from almanak.connectors.uniswap_v4.resolution import MarketSelection, resolve_market
from almanak.connectors.v3_pool_request import (
    RESOLVED_POOL_CONNECTORS,
    PoolRequest,
    read_pool_state,
    read_resolved_pool,
    support_level,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import currency_address, native_asset
from almanak.gateway.evm_logs import topic_address
from almanak.gateway.onchain_lookup import (
    MAX_TOKENS,
    LookupRequest,
    PoolSearch,
    TokenPools,
    Tokens,
    network,
)
from almanak.models import ZERO_ADDRESS, Block, address

# Vendor pages hold 20 rows; a shorter page is the end of the vendor's list.
PAGE_ROWS = 20
# One call reads at most five vendor pages (100 pools, the busiest by the vendor's
# order); a sparse `--quote` filter would otherwise spend ten requests per call.
MAX_POOL_PAGES = 5
SEARCH_PAGES = 2
# One candidate is recommended only when its vendor liquidity is at least this
# multiple of the next one's; otherwise the symbol is reported as ambiguous.
DOMINANCE = 10
# Independent reads, such as each searched token's symbol(), run this many at a time.
READERS = 8

# Each connector declares its vendor labels and whether its pools answer `factory()`
# (`connectors/<id>/discovery.py`); a label only selects which resolver to try.
DEX_CONNECTORS = tuple(
    (label, connector)
    for connector, found in pool_discovery().items()
    for label in found.vendor_labels
)
FACTORY_CONNECTORS = tuple(c for c, found in pool_discovery().items() if found.answers_factory)
# Uniswap V4 tick spacing the factory-era fee tiers use; any other key is not derivable.
V4_SPACING = {100: 1, 500: 10, 3000: 60, 10000: 200}
NATIVE_PLACEHOLDERS = (ZERO_ADDRESS, "0x" + "ee" * 20)
CENT = Decimal("0.01")


@dataclass(frozen=True)
class PoolCandidate:
    pool: str
    vendor_dex: str
    name: str
    tokens: tuple[str | None, str | None]
    liquidity_usd: str | None
    volume_24h_usd: str | None

    def report(self) -> dict:
        return {
            "pool": self.pool,
            "name": self.name,
            "vendor_dex": self.vendor_dex,
            "liquidity_usd": self.liquidity_usd,
            "volume_24h_usd": self.volume_24h_usd,
        }


@dataclass(frozen=True)
class TokenCandidate:
    address: str
    symbol: str | None
    name: str | None
    coin_id: str | None
    liquidity_usd: str | None


def connector_for(dex: str) -> str | None:
    for prefix, connector in DEX_CONNECTORS:
        if dex.startswith(prefix):
            return connector
    return None


def usd(value: object) -> str | None:
    """A vendor USD figure as a canonical decimal string; absent stays absent, never zero."""
    if not isinstance(value, str | int | float) or isinstance(value, bool):
        return None
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            return None
        # Vendor reserves carry dozens of digits of float noise; cents are what it measures.
        # A value too large to state in cents is no measurement either; -0 is zero.
        return str(amount.copy_abs().quantize(CENT, rounding=ROUND_DOWN))
    except InvalidOperation:
        return None


def rank(value: str | None) -> Decimal:
    # Unmeasured liquidity sorts last without being read as zero liquidity.
    return Decimal(value) if value is not None else Decimal(-1)


def related(row: dict, name: str) -> str | None:
    links = row.get("relationships")
    entry = links.get(name) if isinstance(links, dict) else None
    link = entry.get("data") if isinstance(entry, dict) else None
    return link.get("id") if isinstance(link, dict) else None


def token_address(identifier: str | None, chain: ChainId) -> str | None:
    """The address in a vendor token ID (`<network>_<address>`) on the requested chain."""
    prefix = network(chain) + "_"
    if not isinstance(identifier, str) or not identifier.startswith(prefix):
        return None
    try:
        return address(identifier.removeprefix(prefix))
    except ValueError:
        return None


def rows(result: object) -> list[dict]:
    data = result.get("data") if isinstance(result, dict) else None
    return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []


def pool_candidate(row: dict, chain: ChainId) -> PoolCandidate | None:
    attributes = row.get("attributes")
    dex = related(row, "dex")
    if not isinstance(attributes, dict) or not isinstance(dex, str):
        return None
    pool = attributes.get("address")
    if (
        not isinstance(pool, str)
        or re.fullmatch(r"0x([0-9a-fA-F]{40}|[0-9a-fA-F]{64})", pool) is None
    ):
        return None
    volume = attributes.get("volume_usd")
    return PoolCandidate(
        pool=pool.lower(),
        vendor_dex=dex,
        name=str(attributes.get("name", "")),
        tokens=(
            token_address(related(row, "base_token"), chain),
            token_address(related(row, "quote_token"), chain),
        ),
        liquidity_usd=usd(attributes.get("reserve_in_usd")),
        volume_24h_usd=usd(volume.get("h24")) if isinstance(volume, dict) else None,
    )


def token_candidate(row: dict) -> TokenCandidate | None:
    attributes = row.get("attributes")
    if not isinstance(attributes, dict):
        return None
    symbol = attributes.get("symbol")
    try:
        token = address(attributes.get("address"))
    except ValueError:
        return None
    if not isinstance(symbol, str):
        return None
    coin = attributes.get("coingecko_coin_id")
    return TokenCandidate(
        address=token,
        symbol=symbol,
        name=str(attributes.get("name", "")),
        coin_id=coin if isinstance(coin, str) else None,
        liquidity_usd=usd(attributes.get("total_reserve_in_usd")),
    )


def not_indexed(result: object) -> bool:
    return isinstance(result, dict) and result.get("indexed") is False


def source_report(limitation: str | None) -> dict:
    return {"provider": "coingecko_onchain", "complete": limitation is None} | (
        {} if limitation is None else {"limitation": limitation}
    )


def block_report(block: Block) -> dict:
    return {"number": block.number, "hash": block.hash}


def refusal(error: Exception) -> dict:
    kind = error.kind if isinstance(error, DataError) else "invalid"
    return {"status": "refused", "reason": f"{kind}: {error}"}


# --- ax resolve ---------------------------------------------------------------

UNLISTED = (
    "the vendor has not indexed every token looked up, so a candidate or its vendor"
    " figures may be missing: check a known token by its address"
)
TRUNCATED = (
    f"the vendor's search was read for its first {SEARCH_PAGES * PAGE_ROWS} pools only;"
    " a candidate missing here may still exist: check a known token by its address"
)


def resolve(lookups, reader, block: Block, query: str, limit: int) -> dict:
    chain = block.chain.chain_id

    @cache
    def own_symbol(token: TokenCandidate) -> str | None:
        return onchain_symbol(reader, block, token)

    tokens, limitation = token_rows(lookups, chain, query, own_symbol)
    # The recommendation compares the leader with the next match, even past the limit.
    candidates = concurrently(
        lambda token: token_report(reader, block, token, own_symbol), tokens[: max(limit, 2)]
    )
    return {
        "product": "token_candidates",
        "chain": str(chain),
        "query": query,
        "block": block_report(block),
        "source": source_report(limitation),
        "candidates": candidates[:limit],
        # Liquidity is each token's total over all its pools, and a token large enough
        # to rival the leader has pools in the first pages read; a cut search can still
        # miss a smaller one, which `source.limitation` reports. An unindexed lookup
        # names no leader.
        "recommended": None if limitation == UNLISTED else recommended(candidates),
    }


def token_rows(
    lookups, chain: ChainId, query: str, read_symbol: Callable
) -> tuple[list[TokenCandidate], str | None]:
    if re.fullmatch(r"0x[0-9a-fA-F]{40}", query):
        token = address(query)
        found, indexed = token_details(lookups, chain, (token,))
        # A token the vendor does not list is still read on chain, without vendor figures.
        unlisted = TokenCandidate(token, symbol=None, name=None, coin_id=None, liquidity_usd=None)
        return found or [unlisted], None if indexed else UNLISTED
    searched, limitation = searched_tokens(lookups, chain, query)
    found, indexed = token_details(lookups, chain, searched)
    wanted = symbol_key(query)
    # The vendor can list a token under another symbol (USD₮0 as USDT), so a token
    # matches by its vendor symbol or by its own symbol().
    listed = [token for token in found if symbol_key(token.symbol or "") == wanted]
    others = [token for token in found if token not in listed]
    own = concurrently(read_symbol, others)
    matching = listed + [
        token
        for token, symbol in zip(others, own, strict=True)
        if symbol_key(symbol or "") == wanted
    ]
    ranked = sorted(matching, key=lambda token: rank(token.liquidity_usd), reverse=True)
    # An unindexed batch outranks a cut search: only it withholds the recommendation.
    return ranked, limitation if indexed else UNLISTED


def symbol_key(symbol: str) -> str:
    return unicodedata.normalize("NFKC", symbol).casefold()


def onchain_symbol(reader, block: Block, token: TokenCandidate) -> str | None:
    try:
        (symbol,) = reader.call(token.address, "symbol()", block, outputs=("string",))
    except DataError:
        return None
    return symbol


def concurrently(read: Callable, items: list) -> list:
    """Each item's read, in item order, with at most READERS in flight.

    Each read runs in its own copy of the caller's context, where the reader's block
    is pinned; the reader's own rate cap admits each call as it is sent. A read that
    raises ends the call, and the reads still queued are not sent.
    """
    pool = ThreadPoolExecutor(READERS)
    try:
        futures = [pool.submit(contextvars.copy_context().run, read, item) for item in items]
        return [future.result() for future in futures]
    finally:
        pool.shutdown(cancel_futures=True)


def searched_tokens(lookups, chain: ChainId, query: str) -> tuple[tuple[str, ...], str | None]:
    """Every token in the pools the vendor's search returns, in its order.

    The limitation is None only when the search ended within the pages read.
    """
    seen: dict[str, None] = {}
    for page in range(1, SEARCH_PAGES + 1):
        result = lookups.fetch(
            LookupRequest(lookup=PoolSearch(chain=chain, query=query, page=page))
        ).result
        if not_indexed(result):
            return tuple(seen), UNLISTED
        found = rows(result)
        for row in found:
            for name in ("base_token", "quote_token"):
                token = token_address(related(row, name), chain)
                if token is not None:
                    seen.setdefault(token, None)
        if len(found) < PAGE_ROWS:
            return tuple(seen), None
    return tuple(seen), TRUNCATED


def token_details(
    lookups, chain: ChainId, tokens: tuple[str, ...]
) -> tuple[list[TokenCandidate], bool]:
    """Details for every token, fetched in the vendor's batches so none is dropped.

    The flag is False when the vendor has not indexed a batch.
    """
    found: list[TokenCandidate] = []
    indexed = True
    for start in range(0, len(tokens), MAX_TOKENS):
        batch = Tokens(chain=chain, tokens=tokens[start : start + MAX_TOKENS])
        result = lookups.fetch(LookupRequest(lookup=batch)).result
        indexed = indexed and not not_indexed(result)
        parsed = (token_candidate(row) for row in rows(result))
        found.extend(token for token in parsed if token is not None)
    return found, indexed


def token_report(reader, block: Block, token: TokenCandidate, own_symbol: Callable) -> dict:
    asset = AssetType(f"{block.chain.chain_id}/erc20:{token.address}")
    report = {
        "asset": str(asset),
        "symbol": token.symbol,
        "name": token.name,
        "coingecko_coin_id": token.coin_id,
        "liquidity_usd": token.liquidity_usd,
    }
    return {**report, **onchain_token(reader, block, token, own_symbol)}


def onchain_token(reader, block: Block, token: TokenCandidate, own_symbol: Callable) -> dict:
    try:
        (decimals,) = reader.call(token.address, "decimals()", block, outputs=("uint8",))
    except DataError as error:
        return refusal(error)
    # Some tokens return symbol() as bytes32; the decimals read alone verifies the contract.
    return {
        "status": "verified",
        "decimals": decimals,
        "onchain_symbol": own_symbol(token),
    }


def recommended(candidates: list[dict]) -> str | None:
    """The leading candidate, only when the chain confirms it and it clearly leads.

    It must be verified and answer `symbol()` with the symbol the vendor lists. With
    several candidates both leading liquidity figures must be measured, the first
    above zero and at least `DOMINANCE` times the second; an unmeasured runner-up
    cannot be shown to trail, and two figures below a cent show no lead.
    """
    if not candidates or not confirmed(candidates[0]):
        return None
    if len(candidates) == 1:
        return candidates[0]["asset"]
    first, second = candidates[0]["liquidity_usd"], candidates[1]["liquidity_usd"]
    if first is None or second is None or Decimal(first) <= 0:
        return None
    if Decimal(first) < DOMINANCE * Decimal(second):
        return None
    return candidates[0]["asset"]


def confirmed(candidate: dict) -> bool:
    symbol, listed = candidate.get("onchain_symbol"), candidate["symbol"]
    return (
        candidate["status"] == "verified"
        and isinstance(symbol, str)
        and isinstance(listed, str)
        and symbol_key(symbol) == symbol_key(listed)
    )


# --- ax dex-pools ----------------------------------------------------------------

INCOMPLETE = (
    "the vendor lists a token's busiest pools only; a pool missing here may still exist:"
    " check a known pool with ax pool or ax pool-price"
)
NOT_INDEXED = (
    "the vendor has not indexed this token, so its pools are unknown, not absent:"
    " check a known pool with ax pool or ax pool-price"
)


@dataclass(frozen=True)
class PoolQuery:
    token: AssetType
    quote: AssetType | None
    connector: str | None
    limit: int
    depth_bps: int | None
    offset: int = 0


def dex_pools(lookups, reader, block: Block, query: PoolQuery) -> dict:
    chain = block.chain.chain_id

    @cache
    def connector_of(pool: PoolCandidate) -> str | None:
        return connector_for(pool.vendor_dex) or factory_connector(reader, block, pool)

    matches, limitation = token_pools(lookups, chain, query, connector_of)
    end = query.offset + query.limit
    reports = [
        pool_report(pool, chain, pool_status(reader, block, pool, query, connector_of(pool)))
        for pool in matches[query.offset : end]
    ]
    return {
        "product": "dex_pools",
        "chain": str(chain),
        "token": str(query.token),
        "quote": None if query.quote is None else str(query.quote),
        "block": block_report(block),
        "source": source_report(limitation),
        # `matched` counts the matches in the pages read; `has_more` is whether a later
        # offset returns more of them. An incomplete vendor list may hold others past them.
        "page": {
            "offset": query.offset,
            "limit": query.limit,
            "matched": len(matches),
            "has_more": len(matches) > end,
        },
        "pools": reports,
    }


def pool_report(pool: PoolCandidate, chain: ChainId, status: dict) -> dict:
    """The pool's row; one the chain did not verify also states the vendor's currencies.

    A verified row's `market` names the currencies read on chain. Otherwise only the
    vendor's claim tells a native-coin pool from its wrapped-token twin, so it is
    reported as unverified, the native coin as `slip44` and absent when the vendor omits it.
    """
    if status["status"] == "verified":
        return {**pool.report(), **status}
    claimed = [None if token is None else str(currency(chain, token)) for token in pool.tokens]
    return {**pool.report(), "unverified_vendor_currencies": claimed, **status}


def token_pools(
    lookups, chain: ChainId, query: PoolQuery, connector_of: Callable
) -> tuple[list[PoolCandidate], str | None]:
    """The token's matching pools in the vendor's order, read page by page.

    The vendor's order (24h volume with liquidity) is kept rather than re-ranked,
    so an offset continues the vendor's own list. The vendor offers no snapshot or
    cursor, so its order can shift between calls, and between the pages of one call,
    as volumes move; a pool already read is not repeated. Pages are read until one
    match past the requested window is known, every list read has ended, or the
    page limit is reached. The vendor files native-coin pools under the native
    coin, so a wrapped-native token also reads that list, a page of each in turn
    within the same page limit. A list the vendor has not indexed closes alone; the
    other is still read. The limitation is None only when every list ended within
    the pages read, and names an unindexed token list before a cut one.
    """
    token = currency_address(query.token, chain)
    needed = query.offset + query.limit + 1
    matches: list[PoolCandidate] = []
    seen: set[str] = set()
    # Pages read of each list still open; the least-read list is read next.
    pages = dict.fromkeys(listed_tokens(chain, query), 0)
    limitation = None
    for _ in range(MAX_POOL_PAGES):
        if not pages or len(matches) >= needed:
            break
        listed = min(pages, key=pages.__getitem__)
        pages[listed] += 1
        lookup = TokenPools(chain=chain, token=listed, page=pages[listed])
        result = lookups.fetch(LookupRequest(lookup=lookup)).result
        if not_indexed(result):
            # An unindexed list's pools are unknown, not absent; the other list is still read.
            del pages[listed]
            limitation = NOT_INDEXED if listed == token else limitation or INCOMPLETE
            continue
        page_rows = rows(result)
        for pool in (pool_candidate(row, chain) for row in page_rows):
            if pool is None or pool.pool in seen:
                continue
            seen.add(pool.pool)
            if wanted(pool, chain, query, connector_of, aliased=listed != token):
                matches.append(pool)
        if len(page_rows) < PAGE_ROWS:
            del pages[listed]
    if pages and limitation != NOT_INDEXED:
        return matches, INCOMPLETE
    return matches, limitation


def listed_tokens(chain: ChainId, query: PoolQuery) -> tuple[str, ...]:
    """The vendor lists to read: the token's, and the native coin's for its wrapped token."""
    token = currency_address(query.token, chain)
    if query.connector is not None and not wraps_native(query.connector):
        return (token,)
    if token != wrapped_native(chain):
        return (token,)
    return (token, ZERO_ADDRESS)


def wanted(
    pool: PoolCandidate,
    chain: ChainId,
    query: PoolQuery,
    connector_of: Callable,
    aliased: bool = False,
) -> bool:
    # The vendor's pair is only a filter; the connector's read decides the pair. A pool
    # from the token's own list is kept as before; one from the native coin's list must
    # hold the token as a wrapped native leg.
    assets = held(pool, chain)
    asked = (query.token, query.quote) if aliased else (query.quote,)
    if any(a is not None and currency_address(a, chain) not in assets for a in asked):
        return False
    return query.connector is None or connector_of(pool) == query.connector


def held(pool: PoolCandidate, chain: ChainId) -> set[str]:
    """The vendor's token addresses, with the wrapped token a fund holds for a native leg.

    Only a connector that holds a native leg as its wrapped token (Uniswap V4) adds it;
    the label alone decides, since a V4 pool ID names no contract to ask.
    """
    tokens = {token for token in pool.tokens if token is not None}
    connector = connector_for(pool.vendor_dex)
    if tokens.intersection(NATIVE_PLACEHOLDERS) and wraps_native(connector):
        tokens.add(wrapped_native(chain))
    return tokens


def wraps_native(connector: str | None) -> bool:
    return connector is not None and pool_discovery()[connector].family == "keyed"


def wrapped_native(chain: ChainId) -> str:
    from almanak.connectors.uniswap_v4.custody import custody_asset

    return currency_address(custody_asset(native_asset(chain)), chain)


def pool_status(
    reader, block: Block, pool: PoolCandidate, query: PoolQuery, connector: str | None
) -> dict:
    if connector is None:
        return {
            "connector": None,
            "status": "unsupported",
            "reason": "no SDK connector for this venue: no reviewed factory created it",
        }
    try:
        result = verify(reader, block, pool, connector, query.depth_bps)
    except (DataError, ValueError) as error:
        return {"connector": connector, **refusal(error)}
    missing = result["status"] == "verified" and unmatched(connector, result, query)
    if missing:
        return {
            "connector": connector,
            "status": "refused",
            "reason": f"vendor_mismatch: the pool trades"
            f" {', '.join(sorted(traded(connector, result)))},"
            f" not {missing}",
        }
    return {"connector": connector, **result}


def traded(connector: str, result: dict) -> set[str]:
    """The assets the connector read from the pool, never the vendor's claim."""
    found = pool_discovery()[connector]
    if found.family == "keyed":
        from almanak.connectors.uniswap_v4.custody import custody_asset

        # A native currency trades as the wrapped token the fund holds for it.
        market = (AssetType(result["market"][f"currency{i}"]) for i in (0, 1))
        return {str(a).lower() for c in market for a in {c, custody_asset(c)}}
    return {str(asset).lower() for asset in result["resolution"][found.currencies]}


def unmatched(connector: str, result: dict, query: PoolQuery) -> str | None:
    assets = traded(connector, result)
    for asked in (query.token, query.quote):
        if asked is not None and str(asked).lower() not in assets:
            return str(asked)
    return None


def verify(reader, block: Block, pool: PoolCandidate, connector: str, depth_bps) -> dict:
    if connector == "uniswap_v4":
        return verify_v4(reader, block, pool)
    if connector in RESOLVED_POOL_CONNECTORS:
        return verify_resolved(reader, block, pool, connector)
    return verify_v3(reader, block, pool, connector, depth_bps)


def factory_connector(reader, block: Block, pool: PoolCandidate) -> str | None:
    """The connector whose reviewed factory created the pool, asked of the pool itself.

    A V4 pool ID names no contract, and a pool that does not answer `factory()` is
    left to its label; neither is guessed at.
    """
    if len(pool.pool) != 42:
        return None
    try:
        (factory,) = reader.call(pool.pool, "factory()", block, outputs=("address",))
        return reviewed_factories(block.chain.chain_id).get(address(factory))
    except (DataError, ValueError):
        return None


@cache
def reviewed_factories(chain: ChainId) -> Mapping[str, str]:
    """Each reviewed factory on the chain and its connector; fixed per chain, so read once."""
    found: dict[str, str] = {}
    for connector in FACTORY_CONNECTORS:
        networks = import_module(f"almanak.connectors.{connector}.networks")
        try:
            found[networks.deployment_for(chain).factory] = connector
        except DataError:
            continue
    return MappingProxyType(found)


def verify_resolved(reader, block: Block, pool: PoolCandidate, connector: str) -> dict:
    chain = block.chain.chain_id
    state = read_resolved_pool(reader, connector, PoolRequest(chain=chain, pool=pool.pool), block)
    result = {
        "status": "verified",
        "support": state["support"],
        "market": {"connector": connector, "chain": str(chain), "pool": pool.pool},
        "resolution": without_selection(state["resolution"]),
        **({"pool_state": state["pool_state"]} if "pool_state" in state else {}),
    }
    if "reason" in state:
        # A verified pool the connector cannot trade: an unfunded Curve pool.
        result.update(status="refused", reason=f"{state['status']}: {state['reason']}")
    return result


def verify_v3(reader, block: Block, pool: PoolCandidate, connector: str, depth_bps) -> dict:
    chain = block.chain.chain_id
    request = PoolRequest(chain=chain, pool=pool.pool)
    state = read_pool_state(reader, connector, request, block, depth_bps)
    return {
        "status": "verified",
        "support": state["support"],
        "market": {"connector": connector, "chain": str(chain), "pool": pool.pool},
        "resolution": without_selection(state["resolution"]),
        "price": state["price"],
        **({"depth": state["depth"]} if "depth" in state else {}),
    }


def verify_v4(reader, block: Block, pool: PoolCandidate) -> dict:
    """Find the pool's exact key, then let the connector admit and resolve it.

    The hookless key with the standard tick spacing for the vendor's fee is tried
    first, without a read; otherwise the PoolManager's `Initialize` event for the
    pool ID states the key. Either way the key's hash must equal the vendor's pool
    ID. The connector's own resolution then admits or refuses the key, as it does
    for a strategy: a hook needs a reviewed profile, its permission bits, its fee
    mode and code equal to its pin.
    """
    selection = v4_selection(block.chain.chain_id, pool)
    if selection is None or key_id(selection.chain, selection.key) != pool.pool:
        selection = initialized_key(reader, block, pool.pool)
    if selection is None or key_id(selection.chain, selection.key) != pool.pool:
        return {
            "status": "unverified",
            "reason": "the pool's key is not derivable from the vendor or its Initialize event",
        }
    support = support_level("uniswap_v4", block.chain.chain_id)
    resolved = resolve_market(reader, selection, block)
    return {
        "status": "verified",
        "support": support,
        "market": selection.model_dump(mode="json", exclude_none=True),
        "resolution": without_selection(resolved.summary()),
        **hook_report(resolved.hook),
    }


def hook_report(hook) -> dict:
    """An admitted hook: resolution already refused a key whose code differs from its pin."""
    if hook is None:
        return {}
    return {
        "hook": {
            "profile": hook.profile,
            "address": hook.identity.address,
            "pin": "match",
            "terms": hook.terms.model_dump(mode="json"),
        }
    }


V4_INITIALIZE = (
    "0x"
    + keccak(text="Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)").hex()
)


def initialized_key(reader, block: Block, identifier: str):
    """The key the reviewed PoolManager recorded when it created the pool.

    The pool ID is an indexed topic, so at most one event matches and the whole
    history up to the pinned block is asked for at once. A provider that refuses
    that range, or any answer that is not exactly one canonical event from the
    PoolManager, leaves the key unknown rather than guessed.
    """
    chain = block.chain.chain_id
    manager = roles_for(chain).pool_manager
    query = {
        "address": manager,
        "topics": [V4_INITIALIZE, identifier],
        "fromBlock": "0x0",
        "toBlock": hex(block.number),
    }
    try:
        logs = reader._rpc("eth_getLogs", [query])
        if not isinstance(logs, list) or len(logs) != 1:
            return None
        event = logs[0]
        if event.get("removed") or address(event["address"]) != manager:
            return None
        topics, data = event["topics"], event["data"]
        # Two indexed currencies after the signature and ID, and five ABI words of data.
        if len(topics) != 4 or len(data) != 2 + 5 * 64:
            return None
        if [topic.lower() for topic in topics[:2]] != [V4_INITIALIZE, identifier]:
            return None
        currencies = [topic_address(topic) for topic in topics[2:4]]
        fee, spacing, hooks, _, _ = decode(
            ["uint24", "int24", "address", "uint160", "int24"], bytes.fromhex(data[2:])
        )
        return MarketSelection(
            connector="uniswap_v4",
            chain=chain,
            currency0=currency(chain, currencies[0]),
            currency1=currency(chain, currencies[1]),
            fee=fee,
            tick_spacing=spacing,
            hooks=address(hooks),
        )
    except (DataError, DecodingError, ValueError, KeyError, TypeError, AttributeError):
        return None


def without_selection(summary: dict) -> dict:
    # The resolver echoes the request as `selection`; `market` already states it.
    return {key: value for key, value in summary.items() if key != "selection"}


def v4_selection(chain: ChainId, pool: PoolCandidate):
    fee = vendor_fee(pool.name)
    if fee not in V4_SPACING or None in pool.tokens:
        return None
    currencies = sorted(
        (currency(chain, token) for token in pool.tokens if token is not None),
        key=lambda asset: currency_address(asset, chain),
    )
    if currency_address(currencies[0], chain) == currency_address(currencies[1], chain):
        # A vendor that maps both tokens to one currency states no key; the event can.
        return None
    return MarketSelection(
        connector="uniswap_v4",
        chain=chain,
        currency0=currencies[0],
        currency1=currencies[1],
        fee=fee,
        tick_spacing=V4_SPACING[fee],
    )


def vendor_fee(name: str) -> int | None:
    """The fee in hundredths of a basis point from a vendor pool name such as `WETH / USDC 0.3%`."""
    match = re.search(r"(\d+(?:\.\d+)?)%$", name.strip())
    if match is None:
        return None
    pips = Decimal(match.group(1)) * 10_000
    return int(pips) if pips == pips.to_integral_value() else None


def currency(chain: ChainId, token: str) -> AssetType:
    if token in NATIVE_PLACEHOLDERS:
        return native_asset(chain)
    return AssetType(f"{chain}/erc20:{token}")
