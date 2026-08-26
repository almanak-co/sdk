"""Canonical Chainlink feed catalogue.

Chain descriptors own chain physics and identity.  Chainlink owns which feeds
it deploys, how symbols alias to pairs, and how those feeds are decoded.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from types import MappingProxyType

from almanak.core.chains import ChainRegistry

from .models import FeedKind, FeedSpec

DEFAULT_CHAINLINK_CHAIN = "ethereum"

# Frozen compatibility surface for the framework's legacy per-chain archive
# RPC environment variables. Provider feed support is broader and derives
# independently from ``CATALOG.chains``.
LEGACY_ARCHIVE_RPC_CHAINS = ("ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche")

_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {
        "ETH/USD": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
        "BTC/USD": "0x6CE185860A4963106506C203335A2910F5E5E8CC",
        "LINK/USD": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
        "USDC/USD": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
        "USDT/USD": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
        "DAI/USD": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",
        "ARB/USD": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
        "GMX/USD": "0xDB98056FecFff59D032aB628337A4887110df3dB",
        "UNI/USD": "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",
        "AAVE/USD": "0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034",
        "PENDLE/USD": "0x66853E19D73C0F9301fE099c324A1e9726953C89",
        "RDNT/USD": "0x20d0Fcab0ECFD078B036b6CAf1FaC69A6453b352",
        "MAGIC/USD": "0x47E55cCec6582838E173f252D08Afd8116c2202d",
        "WOO/USD": "0x5d5Ab15fb857De6FA209B6B41C7375F1C4BD9B90",
        "SOL/USD": "0x24ceA4b8ce57cdA5058b924B9B9987992450590c",
    },
    "avalanche": {
        "AVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
        "ETH/USD": "0x976B3D034E162d8bD72D6b9C989d545b839003b0",
        "BTC/USD": "0x2779D32d5166BAaa2B2b658333bA7e6Ec0C65743",
        "LINK/USD": "0x49cCd9Ca821efeAb2B98C60Dc60f518e765EdADc",
        "USDC/USD": "0xF096872672F44d6EBA71458D74fe67F9a77a23B9",
        "USDT/USD": "0xEBE676ee90Fe1112671f19b6B7459bC678B67e8a",
        "DAI/USD": "0x51D7180edA2260cc4F6e4EebB82FEF5c3c2B8300",
        "AAVE/USD": "0x3CA13391E9fb38a75330fb28f8cc2eB3D9ceceED",
        "JOE/USD": "0x02D35d3a8aC3e1626d3eE09A78Dd87286F5E8e3a",
    },
    "base": {
        "ETH/USD": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
        "BTC/USD": "0x64c911996d3C6Ac71e9B8934F4e4f21B9C3bD7d1",
        "LINK/USD": "0x17CAb8FE31E32f08326e5E27412894e49B0f9D65",
        "USDC/USD": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B",
        "DAI/USD": "0x591e79239a7d679378eC8c847e5038150364C78F",
        "CBETH/USD": "0xd7818272B9e248357d13057AAb0B417aF31E817d",
    },
    "bsc": {
        "BNB/USD": "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE",
        "BTC/USD": "0x264990fbd0A4796A3E3d8E37C4d5F87a3aCa5Ebf",
        "ETH/USD": "0x9ef1B8c0E4F7dc8bF5719Ea496883DC6401d5b2e",
        "USDC/USD": "0x51597f405303c4377E36123CbF172bc359765377",
        "USDT/USD": "0xB97Ad0E74fa7d920791E90258A6E2085088b4320",
        "DAI/USD": "0x132d3C0B1D2cEa0BC552588063bdBb210FDeecfA",
        "LINK/USD": "0xca236E327F629f9Fc2c30A4E95775EbF0B89fac8",
        "CAKE/USD": "0xb6064eD41d4F67e353768AA239CA98F9c422E159",
        "AAVE/USD": "0xA8357BF572460fC40f4B0aCacbB2a6A61c89f475",
    },
    "ethereum": {
        "ETH/USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
        "BTC/USD": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
        "LINK/USD": "0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
        "USDC/USD": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",
        "USDT/USD": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",
        "DAI/USD": "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9",
        "AAVE/USD": "0x547a514d5e3769680Ce22B2361c10Ea13619e8a9",
        "UNI/USD": "0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
        "CRV/USD": "0xcD627aa160A6fA45Eb793D19286F3879d5cdCe0a",
        "COMP/USD": "0xdBD020CAef83eFd542f4de03864E8c5D2d9bc6CA",
        "MKR/USD": "0xEC1D1b3b0443256Cc3860E24a46f108E699cF2b4",
        "SNX/USD": "0xDC3EA94CD0AC27d9A86C180091e7f78C683d3699",
        "MATIC/USD": "0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676",
        "ARB/USD": "0x31697852a68433DBcC2FF612A4c1C919a0254678",
        "LDO/USD": "0x4e844125952d32acdF339be976C98FE6D1F5F8bE",
        "WSTETH/USD": "0x164b276057258D81941072Eb5f9D7F71C3Dd94b8",
        "CBETH/USD": "0xF017fcB346A1885194689bA23Eff2fE6fA5C483b",
        "SOL/USD": "0x4ffC43a60e009B551865A93d232E33Fce9f01507",
    },
    "linea": {
        "ETH/USD": "0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA",
        "BTC/USD": "0x7A99092816C8BD5ec8ba229e3a6E6Da1E628E1F9",
        "USDC/USD": "0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5",
        "USDT/USD": "0xefCA2bbe0EdD0E22b2e0d2F8248E99F4bEf4A7dB",
        "DAI/USD": "0x5133D67c38AFbdd02997c14Abd8d83676B4e309A",
    },
    "optimism": {
        "ETH/USD": "0x13e3Ee699D1909E989722E753853AE30b17e08c5",
        "BTC/USD": "0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593",
        "LINK/USD": "0xCC232DcFAaE6354cE191bd574108c1Ad03F86CeA",
        "USDC/USD": "0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3",
        "USDT/USD": "0xECef79e109E997BCa29c1c0897EC9D7678e00BB1",
        "DAI/USD": "0x8dBa75e83DA73cc766A7e5a0ee71F656BAb470d6",
        "OP/USD": "0x0D276FC14719f9292D5C1eA2198673d1f4269246",
        "SNX/USD": "0x2FCF37343e916eAEd1f1DdaaF84458a359b53877",
        "AAVE/USD": "0x338ed6787f463394D24813b297401B9F05a8C9d1",
        "WSTETH/USD": "0x698B585CbC4407e2D54aa898B2600B53C68958f7",
    },
    "polygon": {
        "ETH/USD": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "BTC/USD": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "MATIC/USD": "0xAB594600376Ec9fD91F8e885dADF0CE036862dE0",
        "LINK/USD": "0xd9FFdb71EbE7496cC440152d43986Aae0AB76665",
        "USDC/USD": "0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7",
        "USDT/USD": "0x0A6513e40db6EB1b165753AD52E80663aeA50545",
        "DAI/USD": "0x4746DeC9e833A82EC7C2C1356372CcF2cfcD2F3D",
        "AAVE/USD": "0x72484B12719E23115761D5DA1646945632979bB6",
        "UNI/USD": "0xdf0Fb4e4F928d2dCB76f438575fDD8682386e13C",
        "CRV/USD": "0x336584C8E6Dc19637A5b36206B1c79923111b405",
        "WSTETH/USD": "0x10f964234cae09cB6a9854B56FF7D4F38Cda5E6a",
    },
    "sonic": {
        "ETH/USD": "0x824364077993847f71293B24ccA8567c00c2de11",
        "USDC/USD": "0x7A8443a2a5D772db7f1E40DeFe32db485108F128",
        "S/USD": "0xc76dFb89fF298145b417d221B2c747d84952e01d",
    },
}

# Provider-exact reference feeds are intentionally separate from the legacy
# token-price map. They are reachable only through GetReferencePrice and can
# never enter generic symbol aggregation or its public compatibility views.
_REFERENCE_ADDRESSES: dict[str, dict[str, str]] = {
    "bsc": {
        # Chainlink Data Feeds directory: XAU/USD-RefPrice-DF-Binance-001,
        # 10-minute heartbeat. ALM-3223's 300s entry limit is intentionally
        # stricter than provider heartbeat freshness.
        # https://data.chain.link/feeds/bsc/mainnet/xau-usd
        # This is the proxy (consumer) address, not the underlying aggregator.
        "XAU/USD": "0x86896fEB19D8A607c3b11f2aF50A0f239Bd71CD0",
    }
}

_ETH_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {"WSTETH/ETH": "0xb523AE262D20A936BC152e6023996e46FDC2A95D"},
    "base": {"WSTETH/ETH": "0x43a5C292A453A3bF3606fa856197f09D7B74251a"},
    "ethereum": {
        "WSTETH/ETH": "0x86392dC19c0b719886221c78AB11eb8Cf5c52812",
        # Chainlink Data Feeds directory: RETH/ETH, 18 decimals. This is not
        # a USD feed; public RETH/USD prices are derived with ETH/USD.
        # https://data.chain.link/feeds/ethereum/mainnet/reth-eth
        "RETH/ETH": "0x536218f9E9Eb48863970252233c8F271f554C2d0",
    },
}

TOKEN_TO_PAIR: Mapping[str, str] = MappingProxyType(
    {
        "ETH": "ETH/USD",
        "WETH": "ETH/USD",
        "BTC": "BTC/USD",
        "WBTC": "BTC/USD",
        "LINK": "LINK/USD",
        "USDC": "USDC/USD",
        "USDT": "USDT/USD",
        "DAI": "DAI/USD",
        "AAVE": "AAVE/USD",
        "UNI": "UNI/USD",
        "CRV": "CRV/USD",
        "COMP": "COMP/USD",
        "MKR": "MKR/USD",
        "SNX": "SNX/USD",
        "MATIC": "MATIC/USD",
        "ARB": "ARB/USD",
        "OP": "OP/USD",
        "LDO": "LDO/USD",
        "WSTETH": "WSTETH/USD",
        "STETH": "WSTETH/USD",
        "CBETH": "CBETH/USD",
        "SOL": "SOL/USD",
        "AVAX": "AVAX/USD",
        "WAVAX": "AVAX/USD",
        "GMX": "GMX/USD",
        "PENDLE": "PENDLE/USD",
        "RDNT": "RDNT/USD",
        "MAGIC": "MAGIC/USD",
        "WOO": "WOO/USD",
        "JOE": "JOE/USD",
        "CAKE": "CAKE/USD",
        "BNB": "BNB/USD",
        "WBNB": "BNB/USD",
        "S": "S/USD",
        "WS": "S/USD",
    }
)
TOKEN_TO_ETH_PAIR: Mapping[str, str] = MappingProxyType(
    {"WSTETH": "WSTETH/ETH", "STETH": "WSTETH/ETH", "RETH": "RETH/ETH"}
)

_HEARTBEATS = {
    "ETH/USD": 3600,
    "BTC/USD": 3600,
    "LINK/USD": 3600,
    "USDC/USD": 86400,
    "USDT/USD": 86400,
    "DAI/USD": 3600,
    "XAU/USD": 600,
    "RETH/ETH": 86400,
}
_DEVIATIONS = {
    "ETH/USD": Decimal("0.5"),
    "BTC/USD": Decimal("0.5"),
    "LINK/USD": Decimal("1.0"),
    "USDC/USD": Decimal("0.25"),
    "USDT/USD": Decimal("0.25"),
    "DAI/USD": Decimal("0.25"),
    "RETH/ETH": Decimal("2.0"),
}


class ChainlinkCatalog:
    """Immutable lookup surface for Chainlink feed metadata."""

    def __init__(self) -> None:
        specs: dict[str, dict[str, FeedSpec]] = {}
        for chain, feeds in _ADDRESSES.items():
            chain_id = ChainRegistry.resolve(chain).chain_id
            chain_specs = {
                pair: FeedSpec(
                    chain=chain,
                    chain_id=chain_id,
                    pair=pair,
                    address=address,
                    heartbeat_seconds=_HEARTBEATS.get(pair, 3600),
                    deviation_threshold_pct=_DEVIATIONS.get(pair, Decimal("1.0")),
                )
                for pair, address in feeds.items()
            }
            for pair, address in _ETH_ADDRESSES.get(chain, {}).items():
                chain_specs[pair] = FeedSpec(
                    chain=chain,
                    chain_id=chain_id,
                    pair=pair,
                    address=address,
                    kind=FeedKind.ETH,
                    decimals=18,
                    heartbeat_seconds=_HEARTBEATS.get(pair, 3600),
                    deviation_threshold_pct=_DEVIATIONS.get(pair, Decimal("1.0")),
                )
            for pair, address in _REFERENCE_ADDRESSES.get(chain, {}).items():
                chain_specs[pair] = FeedSpec(
                    chain=chain,
                    chain_id=chain_id,
                    pair=pair,
                    address=address,
                    kind=FeedKind.REFERENCE,
                    heartbeat_seconds=_HEARTBEATS[pair],
                    deviation_threshold_pct=_DEVIATIONS.get(pair, Decimal("1.0")),
                )
            specs[chain] = chain_specs
        self._specs: Mapping[str, Mapping[str, FeedSpec]] = MappingProxyType(
            {chain: MappingProxyType(feeds) for chain, feeds in specs.items()}
        )

    @property
    def chains(self) -> tuple[str, ...]:
        return tuple(self._specs)

    def supports_chain(self, chain: str) -> bool:
        return self._canonical_chain(chain) in self._specs

    def feeds(self, chain: str, *, kind: FeedKind | None = None) -> Mapping[str, FeedSpec]:
        feeds: Mapping[str, FeedSpec] = self._specs.get(self._canonical_chain(chain), {})
        if kind is None:
            return feeds
        return MappingProxyType({pair: spec for pair, spec in feeds.items() if spec.kind is kind})

    def feed(self, chain: str, pair: str) -> FeedSpec | None:
        feeds = self._specs.get(self._canonical_chain(chain))
        return feeds.get(pair.strip().upper()) if feeds is not None else None

    @staticmethod
    def _canonical_chain(chain: str) -> str:
        descriptor = ChainRegistry.try_resolve(chain.strip().lower())
        return descriptor.name if descriptor is not None else ""

    def feed_for_token(self, chain: str, token: str) -> FeedSpec | None:
        pair = TOKEN_TO_PAIR.get(token.strip().upper())
        return self.feed(chain, pair) if pair else None

    def derived_feed_for_token(self, chain: str, token: str) -> FeedSpec | None:
        pair = TOKEN_TO_ETH_PAIR.get(token.strip().upper())
        return self.feed(chain, pair) if pair else None


CATALOG = ChainlinkCatalog()

# Read-only compatibility views. Ownership remains in this integration package.
CHAINLINK_PRICE_FEEDS: Mapping[str, Mapping[str, str]] = MappingProxyType(
    {
        chain: MappingProxyType({pair: spec.address for pair, spec in CATALOG.feeds(chain, kind=FeedKind.USD).items()})
        for chain in CATALOG.chains
    }
)
ETH_DENOMINATED_FEEDS: Mapping[str, Mapping[str, str]] = MappingProxyType(
    {
        chain: MappingProxyType({pair: spec.address for pair, spec in CATALOG.feeds(chain, kind=FeedKind.ETH).items()})
        for chain in CATALOG.chains
        if CATALOG.feeds(chain, kind=FeedKind.ETH)
    }
)
CHAINLINK_CHAIN_IDS: Mapping[str, int] = MappingProxyType(
    {chain: ChainRegistry.resolve(chain).chain_id for chain in CATALOG.chains}
)
CHAINLINK_HEARTBEATS: Mapping[str, int] = MappingProxyType({**_HEARTBEATS, "default": 3600})
CHAINLINK_DEVIATION_THRESHOLDS: Mapping[str, Decimal] = MappingProxyType({**_DEVIATIONS, "default": Decimal("1.0")})
