"""Read-only GMX V2 market resolution requests for the ax CLI.

The DataStore market list is the venue's own catalogue: enumerating it at the
pinned block needs no hosted API and preserves the read-only evidence model.
"""

from eth_abi import encode
from eth_utils import keccak
from pydantic import model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.gmx_v2.networks import roles_for
from almanak.connectors.gmx_v2.oracle_reader import is_deployed, key
from almanak.connectors.gmx_v2.position_request import supported_chains
from almanak.connectors.gmx_v2.resolution import (
    MARKET_ABI,
    MarketSelection,
    ResolvedMarket,
    resolve_market,
)
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import chain_number
from almanak.models import Address, Block, Model, address

# GMX labels a market by its index asset, and each chain's majors trade under a
# wrapped or bridged token: the venue publishes BTC/USD, ETH/USD and AVAX/USD, while
# the tokens' own symbols are WBTC and WETH on Arbitrum and BTC.b, WETH.e and WAVAX
# on Avalanche. Each chain's alias maps the venue's public label to the symbol read
# on-chain rather than inventing a name the venue never published. A label selects
# among the venue's registered markets only, and one that matches several is refused
# as ambiguous, so an alias cannot bind a market silently.
TOKEN_ALIASES = {
    ChainId("eip155:42161"): {"BTC": "WBTC", "ETH": "WETH"},
    ChainId("eip155:43114"): {"BTC": "BTC.b", "ETH": "WETH.e", "AVAX": "WAVAX"},
}


def names_token(chain: ChainId, label: str, symbol: str) -> bool:
    """Whether a label is the token's on-chain symbol or the venue's alias of it."""
    alias = TOKEN_ALIASES.get(chain, {}).get(label.upper())
    return symbol.upper() in (label.upper(), None if alias is None else alias.upper())


def venue_label(chain: ChainId, symbol: str) -> str | None:
    """The venue's public label of a wrapped or bridged token symbol, if it has one."""
    for label, wrapped in TOKEN_ALIASES.get(chain, {}).items():
        if wrapped.upper() == symbol.upper():
            return label
    return None


class GmxMarketRequest(Model):
    """A market query: an exact GM share address or a label like "BTC/USD"."""

    chain: ChainId
    query: str

    @model_validator(mode="after")
    def scope(self):
        if self.chain not in supported_chains():
            raise ValueError(f"GMX V2 market resolution is unsupported on {self.chain}")
        if not self.query.strip():
            raise ValueError("GMX V2 market query must not be empty")
        return self

    @property
    def share_address(self) -> str | None:
        query = self.query.strip()
        if query.startswith("0x") and len(query) == 42:
            return address(query)
        return None


class Listing(Model):
    """One registered market. A synthetic index has no contract, so no symbol."""

    share: Address
    index: Address
    long: Address
    short: Address
    synthetic: bool
    index_symbol: str | None


def read_gmx_market(reader: EthereumReader, request: GmxMarketRequest, block: Block) -> dict:
    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain/environment")
    contracts = roles_for(request.chain)
    if request.share_address is not None:
        resolved = resolve_market(reader, selection(request.chain, request.share_address), block)
        return payload(request, resolved, *verified_label(reader, resolved, block))
    candidates = matching_markets(reader, contracts, request, block)
    if not candidates:
        raise DataError(
            "not_found", f"no GMX V2 market matches {request.query!r} at the pinned block"
        )
    if len(candidates) > 1:
        described = ", ".join(describe(reader, listing, block) for listing, _ in candidates)
        raise DataError(
            "ambiguous",
            f"GMX V2 market query {request.query!r} is ambiguous:"
            f" matches {len(candidates)} markets: {described}",
        )
    listing, symbol = candidates[0]
    resolved = resolve_market(reader, selection(request.chain, listing.share), block)
    return payload(request, resolved, symbol, listing.synthetic)


def selection(chain: ChainId, share: str) -> MarketSelection:
    return MarketSelection(
        connector="gmx_v2", chain=chain, market=AssetType(f"{chain}/erc20:{address(share)}")
    )


def payload(
    request: GmxMarketRequest, resolved: ResolvedMarket, symbol: str | None, synthetic: bool
) -> dict:
    return {
        "product": "gmx_perp_market",
        "request": request.model_dump(mode="json"),
        "observation": {
            **resolved.summary(),
            "index_symbol": symbol,
            "index_kind": "synthetic" if synthetic else "erc20",
        },
    }


def synthetic_index(chain: ChainId, symbol: str) -> str:
    """GMX deploys no contract for a synthetic index; its address is derived from the symbol."""
    digest = keccak(encode(("uint256", "string"), (chain_number(chain), symbol)))
    return address("0x" + digest.hex()[-40:])


def matching_markets(reader, contracts, request: GmxMarketRequest, block: Block):
    label = parse_label(request.query)
    if label is None:
        return []
    base, collateral = label
    matches = []
    for listing in enumerate_markets(reader, contracts, block):
        symbol = matched_label(request.chain, base, listing)
        if symbol is not None and collateral_matches(
            reader, request.chain, collateral, listing, block
        ):
            matches.append((listing, symbol))
    return matches


def parse_label(query: str) -> tuple[str, tuple[str, str] | None] | None:
    """The index label and optional collateral pair of a query.

    `BTC/USD` and `BTC` name the same index; `BTC/USD [WBTC-USDC]` and
    `BTC/USD [WBTC]` add the venue's own collateral notation, where a single
    symbol names a market whose long and short collateral are the same token.
    A collateral is named by its on-chain symbol or by the venue's alias of it.
    A quote other than USD names no GMX market.
    """
    label, bracket, rest = query.strip().partition("[")
    base, separator, quote = label.strip().partition("/")
    if (separator and quote.strip().upper() != "USD") or not base.strip():
        return None
    if not bracket:
        return base.strip(), None
    collateral, closing, trailing = rest.partition("]")
    long, dash, short = collateral.strip().partition("-")
    if not closing or trailing.strip() or not long.strip() or (dash and not short.strip()):
        return None
    return base.strip(), (long.strip(), short.strip() if dash else long.strip())


def collateral_matches(reader, chain: ChainId, collateral, listing: Listing, block: Block) -> bool:
    if collateral is None:
        return True
    observed = (read_symbol(reader, listing.long, block), read_symbol(reader, listing.short, block))
    return all(
        names_token(chain, label, symbol)
        for label, symbol in zip(collateral, observed, strict=True)
    )


def matched_label(chain: ChainId, base: str, listing: Listing) -> str | None:
    """The verified label the listing answers to, or None when it does not match."""
    if listing.index_symbol is not None:
        return listing.index_symbol if names_token(chain, base, listing.index_symbol) else None
    for candidate in dict.fromkeys((base, base.upper())):
        if synthetic_index(chain, candidate) == listing.index:
            return candidate
    return None


def enumerate_markets(reader, contracts, block: Block) -> list[Listing]:
    """Every registered market at the pinned block, with the symbol of a deployed index token."""
    labels: dict[str, tuple[bool, str | None]] = {}
    listings = []
    for share, index, long, short in registered_markets(reader, contracts, block):
        if index not in labels:
            deployed = is_deployed(reader, block, index)
            labels[index] = (not deployed, read_symbol(reader, index, block) if deployed else None)
        synthetic, symbol = labels[index]
        listings.append(
            Listing(
                share=share,
                index=index,
                long=long,
                short=short,
                synthetic=synthetic,
                index_symbol=symbol,
            )
        )
    return listings


def registered_markets(reader, contracts, block: Block) -> list[tuple[str, str, str, str]]:
    """The DataStore MARKET_LIST is an address set; the Reader pages it as market tuples."""
    count = reader.call(
        contracts.data_store,
        "getAddressCount(bytes32)",
        block,
        inputs=("bytes32",),
        args=(key("MARKET_LIST"),),
        outputs=("uint256",),
    )[0]
    if count == 0:
        return []
    markets = reader.call(
        contracts.reader,
        "getMarkets(address,uint256,uint256)",
        block,
        inputs=("address", "uint256", "uint256"),
        args=(contracts.data_store, 0, count),
        outputs=(MARKET_ABI + "[]",),
    )[0]
    return [
        (address(share), address(index), address(long), address(short))
        for share, index, long, short in markets
    ]


def verified_label(reader, resolved: ResolvedMarket, block: Block) -> tuple[str | None, bool]:
    """The index symbol of a deployed index token, or the synthetic label its collateral verifies.

    A synthetic index has no symbol on-chain. When the long collateral is the
    wrapped form of the index (WBTC for BTC), the venue's address derivation
    confirms that label; otherwise the label stays unknown rather than guessed.
    """
    index = resolved.market.index_asset.reference
    if is_deployed(reader, block, index):
        return read_symbol(reader, index, block), False
    long = read_symbol(reader, resolved.market.long_asset.reference, block)
    chain = resolved.selection.chain
    for candidate in dict.fromkeys((long, venue_label(chain, long))):
        if candidate is not None and synthetic_index(chain, candidate) == index:
            return candidate, True
    return None, True


def describe(reader, listing: Listing, block: Block) -> str:
    long = collateral_label(reader, listing.long, block)
    short = collateral_label(reader, listing.short, block)
    return f"{listing.share} [{long}-{short}]"


def collateral_label(reader, token: str, block: Block) -> str:
    try:
        return read_symbol(reader, token, block)
    except DataError:
        return token


def read_symbol(reader, token: str, block: Block) -> str:
    return reader.call(token, "symbol()", block, outputs=("string",))[0]
