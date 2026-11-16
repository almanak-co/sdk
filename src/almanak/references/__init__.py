"""Read-only reference strategy files, listed by ``almanak support --references``.

The repository's ``strategies/`` directory is the single source. The wheel build
copies each published reference's author files into ``files/`` so an installed SDK
without a source checkout can read them as examples. Listing a
reference never loads or runs it, and a reference directory does not establish
support: ``almanak support --connector ... --chain ...`` states the declaration.

The build hook loads this module by path, before the SDK's dependencies are
installed, so the file-selection rule below uses only the standard library.
"""

from pathlib import Path

# The references an agent sees: one per family of per-chain copies, plus each distinct
# pattern. The rest stay in strategies/ as test fixtures and QA proofs but never ship
# and are never listed, so a new reference is hidden until it is added here.
PUBLISHED = frozenset(
    {
        "aave_v3_arbitrum_lending",
        "aave_v3_arbitrum_managed_borrow",
        "aave_v3_base_sma_borrow",
        "aave_v4_avalanche_borrowing",
        "aave_v4_weth_supply",
        "aerodrome_base_stable_lp",
        "aerodrome_base_stable_trading",
        "aerodrome_base_volatile_lp",
        "aerodrome_base_volatile_trading",
        "aerodrome_slipstream_base_lp",
        "aerodrome_slipstream_base_lp_band",
        "aerodrome_slipstream_base_trading",
        "arbitrum_aave_borrow_and_trade",
        "arbitrum_aave_borrow_lp",
        "arbitrum_aave_lending_short",
        "arbitrum_aave_leverage_loop",
        "arbitrum_morpho_borrow_and_trade",
        "avalanche_aave_v4_borrow_and_trade",
        "base_morpho_leverage_loop",
        "compound_v3_base_lending",
        "curve_arbitrum_stable_lp",
        "curve_arbitrum_tricrypto_lp",
        "curve_ethereum_stable_trading",
        "curve_ethereum_twocrypto_lp",
        "gmx_v2_arbitrum_collateral_path",
        "gmx_v2_arbitrum_liquidity",
        "gmx_v2_arbitrum_position",
        "gmx_v2_arbitrum_position_path",
        "gmx_v2_arbitrum_price_stop",
        "gmx_v2_arbitrum_swap",
        "morpho_blue_base_lending",
        "pancakeswap_v3_bnb_lp",
        "pancakeswap_v3_bnb_lp_band",
        "pancakeswap_v3_bnb_trading",
        "sparklend_ethereum_lending",
        "uniswap_v3_arbitrum_lp",
        "uniswap_v3_arbitrum_trading",
        "uniswap_v3_base_atr_breakout",
        "uniswap_v3_base_rsi",
        "uniswap_v3_base_rsi_lp",
        "uniswap_v3_base_trading",
        "uniswap_v3_robinhood_lp_band",
        "uniswap_v4_arbitrum_lp",
        "uniswap_v4_arbitrum_trading",
        "uniswap_v4_ethereum_native_trading",
        "uniswap_v4_robinhood_lp_band",
    }
)

# Author inputs and their explanation. Evidence captures, production variants and
# anything an operator supplies stay out of the wheel.
AUTHOR_FILES = frozenset(
    {"strategy.py", "parameters.py", "dashboard.py", "config.yaml", "README.md", "backtest.yaml"}
)


def is_published(name: str) -> bool:
    """Whether a reference directory is listed and shipped in the wheel."""
    return name in PUBLISHED


def is_author_file(name: str) -> bool:
    """Whether a file directly inside one reference directory is distributed."""
    if name in AUTHOR_FILES:
        return True
    # Named backtest variants only; a production or operator variant never ships.
    return name.startswith("backtest-") and name.endswith(".yaml") and ".production." not in name


def directory() -> Path:
    bundled = Path(__file__).parent / "files"
    if bundled.is_dir():
        return bundled
    # A source checkout reads the single source directly; anything else, such as
    # an install whose bundled files are missing, carries no references.
    package = Path(__file__).parents[1]
    checkout = package.parents[1]
    if package.parent.name == "src" and (checkout / "pyproject.toml").is_file():
        return checkout / "strategies"
    return bundled


def catalog(connector: str | None = None, chain: str | None = None) -> dict:
    """References whose markets include one matching the connector and chain filters."""
    root = directory()
    if not root.is_dir():
        raise ValueError("this almanak install carries no reference strategies")
    references = []
    for path in sorted(root.iterdir()):
        entry = reference(path)
        if entry is not None and selected(entry, connector, chain):
            references.append(entry)
    return {
        "directory": str(root.resolve()),
        "note": (
            "Read-only examples. A reference does not establish support; confirm each"
            " connector and chain with almanak support --connector <id> --chain <CAIP-2>."
        ),
        "references": references,
    }


def selected(entry: dict, connector: str | None, chain: str | None) -> bool:
    """Unfiltered listings include every reference, even one without markets."""
    if connector is None and chain is None:
        return True
    return any(
        connector in (None, market["connector"]) and chain in (None, market["chain"])
        for market in entry["markets"]
    )


def market_pairs(config: Path) -> list[tuple[str, str]]:
    """Validated (connector, CAIP-2 chain) pairs through the SDK's one author loader."""
    from almanak.caip import ChainId
    from almanak.configuration.selection import require_connector
    from almanak.configuration.yaml import ConfigurationError, load

    document = load(config)
    markets = document.value.get("markets") or {}
    if not isinstance(markets, dict):
        raise document.error("markets must be a mapping of named markets", "markets")
    pairs = set()
    for name, market in markets.items():
        field = f"markets.{name}"
        if not isinstance(market, dict):
            raise document.error("each market must be a mapping", field)
        connector = require_connector(
            market.get("connector"), field=f"{field}.connector", file=str(config)
        )
        try:
            chain = ChainId(str(market.get("chain", "")))
        except ValueError as error:
            raise ConfigurationError(
                str(error), file=str(config), field=f"{field}.chain"
            ) from error
        pairs.add((str(connector), str(chain)))
    return sorted(pairs)


def reference(path: Path) -> dict | None:
    config = path / "config.yaml"
    if not is_published(path.name):
        return None
    if not (path / "strategy.py").is_file() or not config.is_file():
        return None
    pairs = market_pairs(config)
    readme = path / "README.md"
    return {
        "name": path.name,
        "title": title(readme),
        "summary": summary(readme),
        "markets": [{"connector": name, "chain": chain} for name, chain in pairs],
        "path": str(path.resolve()),
        "files": sorted(item.name for item in path.iterdir() if is_author_file(item.name)),
    }


def title(readme: Path) -> str | None:
    if not readme.is_file():
        return None
    first = readme.read_text(encoding="utf-8").splitlines()[:1]
    return first[0].lstrip("# ").strip() if first and first[0].startswith("#") else None


def summary(readme: Path) -> str | None:
    """What the reference teaches: a one-line `> ` quote directly under the title."""
    if not readme.is_file():
        return None
    lines = [line for line in readme.read_text(encoding="utf-8").splitlines()[1:4] if line.strip()]
    if not lines or not lines[0].startswith("> "):
        return None
    return lines[0][2:].strip() or None
