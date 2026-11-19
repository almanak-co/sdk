"""The live read cases: known markets and accounts on the chains public endpoints serve.

Every address here is a public contract. Accounts are contracts too (a pool, a
wrapped-native token, the deposit contract), chosen because they hold a balance;
no case names a person's wallet. A case states the correct behavior. Where the
SDK is known not to meet it yet, the case carries a `KnownIssue` and keeps failing
until the defect is fixed.

Not covered, and why:

- `ax lending-markets --connector compound_v3`: the listing reads the
  Configurator's whole event history in one `eth_getLogs`, which both public
  endpoints refuse. Public endpoints promise no wide log ranges.
- `ax resolve` and `ax dex-pools` succeed only with a vendor key; the cases below
  hold their refusal on a public endpoint instead.
- `ax perp-position` takes a request file holding a fully resolved GMX market with
  its contract set, which no `ax` command prints, so a user without the SDK's
  source cannot write one.
- `ax hypercore-account` reads a named account; the venue lookup is covered by
  `ax hypercore-markets`.
"""

from scripts.live_qa import checks
from scripts.live_qa.reads import KnownIssue, ReadCase, Refusal

ETHEREUM = "eip155:1"
AVALANCHE = "eip155:43114"

WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
AVALANCHE_USDC = "eip155:43114/erc20:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
AVALANCHE_WAVAX = "eip155:43114/erc20:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"

# Balance holders: the beacon deposit contract, the Uniswap V3 USDC/WETH 0.05% pools
# and the WAVAX token, which holds the AVAX it wraps.
DEPOSIT_CONTRACT = "0x00000000219ab540356cbb839cbe05303d7705fa"
ETHEREUM_V3_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
AVALANCHE_V3_POOL = "0xfae3f424a0a47706811521e3ee268f00cfb5c45e"
WAVAX_TOKEN = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"

AAVE_V4_ETHEREUM_SPOKE = "0x94e7a5dcbe816e498b89ab752661904e2f56c485"
AAVE_V4_AVALANCHE_SPOKE = "0x435272ceff93a1e657e8abfdf0a13e95900a3a56"
COMET_USDC = "0xc3d688b66703497daa19211eedff47f25384cdc3"
MORPHO_CBBTC_USDC = "0x64d65c9a2d91c36d56fbc42d69e979335320169b3df63bf92789e2c8883fcc64"

GMX_BTC_USDC = "0xfb02132333a79c8b5bd0b64e3abcca5f7faf2937"
GMX_WAVAX_USDC = "0x913c1f46b48b3ed35e7dc3cf754d4ae8499f31cf"

AAVE_V3_REVISION = KnownIssue(
    "Aave V3 reads refuse with deployment_changed after the on-chain Pool upgrade;"
    " the SDK pins the earlier revision's code hashes",
    "deployment_changed",
)
GMX_AVALANCHE_LABELS = KnownIssue(
    "GMX Avalanche markets are indexed by BTC.b and WAVAX, so the BTC/USD and"
    " AVAX/USD labels the help text suggests match nothing",
    "not_found",
)


def balance(asset: str, account: str) -> tuple[str, ...]:
    return ("balance", "--asset", asset, "--account", account)


def aave_v4(command: str, chain: str, spoke: str, reserve: int) -> tuple[str, ...]:
    selector = ("--chain", chain, "--spoke", spoke, "--reserve-id", str(reserve))
    return (command, "--connector", "aave_v4", *selector)


def reserve(connector: str, chain: str, *selector: str) -> tuple[str, ...]:
    return ("lending-reserve", "--connector", connector, "--chain", chain, *selector)


def markets(connector: str, chain: str, *options: str) -> tuple[str, ...]:
    listing = ("lending-markets", "--connector", connector, "--chain", chain)
    return (*listing, "--limit", "3", *options)


def gmx(query: str) -> tuple[str, ...]:
    return ("perp-markets", "--connector", "gmx_v2", "--chain", AVALANCHE, "--query", query)


def v3_pool(chain: str, pool: str) -> tuple[str, ...]:
    return ("pool", "--connector", "uniswap_v3", "--chain", chain, "--pool", pool)


ETHEREUM_CASES = (
    ReadCase(
        "balance/native/ethereum",
        ETHEREUM,
        (*balance("eip155:1/slip44:60", DEPOSIT_CONTRACT), "--block", "latest"),
        checks.native_balance,
    ),
    ReadCase(
        "balance/erc20/ethereum-usdc",
        ETHEREUM,
        balance(USDC, ETHEREUM_V3_POOL),
        checks.erc20_balance,
    ),
    ReadCase(
        "lending-reserve/aave_v4/ethereum-weth",
        ETHEREUM,
        aave_v4("lending-reserve", ETHEREUM, AAVE_V4_ETHEREUM_SPOKE, 0),
        checks.reserve_decimals,
    ),
    ReadCase(
        "lending-position/aave_v4/ethereum-weth",
        ETHEREUM,
        (
            *aave_v4("lending-position", ETHEREUM, AAVE_V4_ETHEREUM_SPOKE, 0),
            "--account",
            ETHEREUM_V3_POOL,
        ),
        checks.aave_v4_position,
    ),
    ReadCase(
        "lending-markets/aave_v4/ethereum",
        ETHEREUM,
        markets("aave_v4", ETHEREUM),
        checks.listed_markets,
    ),
    ReadCase(
        "lending-reserve/aave_v3/ethereum-weth",
        ETHEREUM,
        reserve("aave_v3", ETHEREUM, "--asset", WETH),
        checks.pooled_reserve,
        known_issue=AAVE_V3_REVISION,
    ),
    ReadCase(
        "lending-markets/aave_v3/ethereum",
        ETHEREUM,
        markets("aave_v3", ETHEREUM),
        checks.pool_reserve_listing,
        known_issue=AAVE_V3_REVISION,
    ),
    ReadCase(
        "lending-reserve/sparklend/ethereum-weth",
        ETHEREUM,
        reserve("sparklend", ETHEREUM, "--asset", WETH),
        checks.pooled_reserve,
    ),
    ReadCase(
        "lending-markets/sparklend/ethereum",
        ETHEREUM,
        markets("sparklend", ETHEREUM),
        checks.pool_reserve_listing,
    ),
    ReadCase(
        "lending-reserve/compound_v3/ethereum-usdc",
        ETHEREUM,
        reserve("compound_v3", ETHEREUM, "--comet", COMET_USDC, "--asset", USDC),
        checks.comet_reserve,
    ),
    ReadCase(
        "lending-reserve/morpho_blue/ethereum-cbbtc-usdc",
        ETHEREUM,
        reserve("morpho_blue", ETHEREUM, "--market-id", MORPHO_CBBTC_USDC, "--leg", "loan"),
        checks.morpho_reserve,
    ),
    ReadCase(
        "lending-markets/morpho_blue/ethereum-usdc",
        ETHEREUM,
        markets("morpho_blue", ETHEREUM, "--asset", USDC),
        checks.some_verified_markets,
    ),
    ReadCase(
        "pool/uniswap_v3/ethereum-usdc-weth",
        ETHEREUM,
        v3_pool(ETHEREUM, ETHEREUM_V3_POOL),
        checks.v3_pool,
    ),
    ReadCase(
        "pool-price/uniswap_v4/ethereum-usdc-weth",
        ETHEREUM,
        (
            "pool-price",
            "--connector",
            "uniswap_v4",
            "--chain",
            ETHEREUM,
            "--currency0",
            USDC,
            "--currency1",
            WETH,
            "--fee",
            "500",
            "--tick-spacing",
            "10",
        ),
        checks.v4_pool_price,
    ),
)

AVALANCHE_CASES = (
    ReadCase(
        "balance/native/avalanche",
        AVALANCHE,
        (*balance("eip155:43114/slip44:9005", WAVAX_TOKEN), "--block", "latest"),
        checks.native_balance,
    ),
    ReadCase(
        "balance/erc20/avalanche-usdc",
        AVALANCHE,
        balance(AVALANCHE_USDC, AVALANCHE_V3_POOL),
        checks.erc20_balance,
    ),
    ReadCase(
        "lending-reserve/aave_v4/avalanche-usdc",
        AVALANCHE,
        aave_v4("lending-reserve", AVALANCHE, AAVE_V4_AVALANCHE_SPOKE, 2),
        checks.reserve_decimals,
    ),
    ReadCase(
        "lending-position/aave_v4/avalanche-usdc",
        AVALANCHE,
        (
            *aave_v4("lending-position", AVALANCHE, AAVE_V4_AVALANCHE_SPOKE, 2),
            "--account",
            AVALANCHE_V3_POOL,
        ),
        checks.aave_v4_position,
    ),
    ReadCase(
        "lending-markets/aave_v4/avalanche",
        AVALANCHE,
        markets("aave_v4", AVALANCHE),
        checks.listed_markets,
    ),
    ReadCase(
        "lending-reserve/aave_v3/avalanche-usdc",
        AVALANCHE,
        reserve("aave_v3", AVALANCHE, "--asset", AVALANCHE_USDC),
        checks.pooled_reserve,
        known_issue=AAVE_V3_REVISION,
    ),
    ReadCase(
        "pool/uniswap_v3/avalanche-wavax-usdc",
        AVALANCHE,
        v3_pool(AVALANCHE, AVALANCHE_V3_POOL),
        checks.v3_pool,
    ),
    ReadCase(
        "perp-markets/gmx_v2/avalanche-btc-by-address",
        AVALANCHE,
        gmx(GMX_BTC_USDC),
        checks.gmx_market(GMX_BTC_USDC, "BTC.b"),
    ),
    ReadCase(
        "perp-markets/gmx_v2/avalanche-btc.b-label",
        AVALANCHE,
        gmx("BTC.b/USD [BTC.b-USDC]"),
        checks.gmx_market(GMX_BTC_USDC, "BTC.b"),
    ),
    ReadCase(
        "perp-markets/gmx_v2/avalanche-wavax-label",
        AVALANCHE,
        gmx("WAVAX/USD [WAVAX-USDC]"),
        checks.gmx_market(GMX_WAVAX_USDC, "WAVAX"),
    ),
    # The venue's own labels. Finding the market or naming the candidates is correct;
    # reporting that nothing matches is the defect.
    ReadCase(
        "perp-markets/gmx_v2/avalanche-btc-usd-label",
        AVALANCHE,
        gmx("BTC/USD [BTC.b-USDC]"),
        checks.gmx_market(GMX_BTC_USDC, "BTC.b"),
        known_issue=GMX_AVALANCHE_LABELS,
    ),
    ReadCase(
        "perp-markets/gmx_v2/avalanche-btc-usd-bare-label",
        AVALANCHE,
        gmx("BTC/USD"),
        refusals=(Refusal("ambiguous", "BTC.b-USDC"),),
        known_issue=GMX_AVALANCHE_LABELS,
    ),
    ReadCase(
        "perp-markets/gmx_v2/avalanche-avax-usd-bare-label",
        AVALANCHE,
        gmx("AVAX/USD"),
        refusals=(Refusal("ambiguous", "WAVAX-USDC"),),
        known_issue=GMX_AVALANCHE_LABELS,
    ),
)

# Results that do not depend on the endpoint's chain reads, run on one endpoint only.
VENUE_AND_REFUSAL_CASES = (
    ReadCase(
        "hypercore-markets/btc",
        "eip155:999",
        ("hypercore-markets", "--query", "BTC"),
        checks.hypercore_market("BTC"),
        endpoints=("publicnode",),
    ),
    ReadCase(
        "refusal/resolve-needs-a-vendor-key",
        ETHEREUM,
        ("resolve", "--chain", ETHEREUM, "--query", "USDC"),
        succeeds=False,
        refusals=(Refusal("unavailable", "this lookup uses a vendor key"),),
        endpoints=("publicnode",),
    ),
    ReadCase(
        "refusal/dex-pools-needs-a-vendor-key",
        ETHEREUM,
        ("dex-pools", "--chain", ETHEREUM, "--token", WETH),
        succeeds=False,
        refusals=(Refusal("unavailable", "this lookup uses a vendor key"),),
        endpoints=("publicnode",),
    ),
    ReadCase(
        "refusal/base-has-no-public-endpoint",
        "eip155:8453",
        balance("eip155:8453/slip44:60", DEPOSIT_CONTRACT),
        succeeds=False,
        refusals=(Refusal("invalid_input", "use a provider key instead"),),
        endpoints=("publicnode", "drpc"),
    ),
)

CASES: tuple[ReadCase, ...] = (*ETHEREUM_CASES, *AVALANCHE_CASES, *VENUE_AND_REFUSAL_CASES)
