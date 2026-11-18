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

# GMX labels a market by its index token, and the two Arbitrum majors trade
# under wrapped symbols; the alias maps the venue's public label to the token
# symbol read on-chain rather than inventing a name the venue never published.
WRAPPED_INDEX_ALIASES = {"BTC": "WBTC", "ETH": "WETH"}
UNWRAPPED_INDEX_ALIASES = {wrapped: label for label, wrapped in WRAPPED_INDEX_ALIASES.items()}


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
        if symbol is not None and collateral_matches(reader, collateral, listing, block):
            matches.append((listing, symbol))
    return matches


def parse_label(query: str) -> tuple[str, tuple[str, str] | None] | None:
    """The index label and optional collateral pair of a query.

    `BTC/USD` and `BTC` name the same index; `BTC/USD [WBTC-USDC]` and
    `BTC/USD [WBTC]` add the venue's own collateral notation, where a single
    symbol names a market whose long and short collateral are the same token.
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


def collateral_matches(reader, collateral, listing: Listing, block: Block) -> bool:
    if collateral is None:
        return True
    observed = (read_symbol(reader, listing.long, block), read_symbol(reader, listing.short, block))
    return tuple(symbol.upper() for symbol in observed) == tuple(
        symbol.upper() for symbol in collateral
    )


def matched_label(chain: ChainId, base: str, listing: Listing) -> str | None:
    """The verified label the listing answers to, or None when it does not match."""
    if listing.index_symbol is not None:
        symbol = listing.index_symbol.upper()
        if base.upper() == symbol or WRAPPED_INDEX_ALIASES.get(base.upper()) == symbol:
            return listing.index_symbol
        return None
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
    for candidate in dict.fromkeys((long, UNWRAPPED_INDEX_ALIASES.get(long.upper()))):
        if candidate is not None and synthetic_index(resolved.selection.chain, candidate) == index:
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
