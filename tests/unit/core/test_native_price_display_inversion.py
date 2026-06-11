"""Equivalence tests for the CS-3b / CS-4 inversions (VIB-4851 Phase E).

CS-3b: native / wrapped-native CoinGecko coin ids move onto
``NativeToken`` and project into the price maps via
``native_coingecko_ids()`` — a DELIBERATE widening (every registered
chain's gas asset becomes priceable; previously plasma's XPL and sonic's
S had no entry anywhere — the VIB-3805 drift class).
CS-4: explorer browse URLs and Tenderly dashboard slugs move onto the
descriptor; the three duplicate explorer maps derive from one helper.

Frozen legacy literals are verbatim from the pre-CS-3b modules; widenings
are pinned explicitly, never implied.
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import (
    explorer_tx_prefix_map,
    native_coingecko_ids,
    vendor_chain_map,
)

# ── Frozen legacy literals ──────────────────────────────────────────────────

# Native/wrapped rows previously hardcoded across the gateway per-chain
# *_TOKEN_IDS maps and the framework TOKEN_IDS map.
FROZEN_NATIVE_ROWS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "SOL": "solana",
    "WSOL": "solana",
    "AVAX": "avalanche-2",
    "WAVAX": "avalanche-2",
    "BNB": "binancecoin",
    "WBNB": "binancecoin",
    "MNT": "mantle",
    "WMNT": "mantle",
    "OKB": "okb",
    "WOKB": "okb",
    "MON": "monad",
    "WMON": "monad",
    "MATIC": "polygon-ecosystem-token",
    "POL": "polygon-ecosystem-token",
    "WMATIC": "polygon-ecosystem-token",
}

# Newly covered natives — the deliberate CS-3b widening, ids verified
# against the live CoinGecko /search API on 2026-06-11.
PINNED_WIDENING_ROWS: dict[str, str] = {
    "S": "sonic-3",
    "wS": "sonic-3",
    "BERA": "berachain-bera",
    "WBERA": "berachain-bera",
    "XPL": "plasma",
    "WXPL": "plasma",
    "A0GI": "zero-gravity",
    "W0G": "wrapped-0g",
}

FROZEN_TENDERLY_SLUGS: dict[str, str] = {
    "ethereum": "mainnet",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "polygon": "polygon",
    "base": "base",
    "avalanche": "avalanche",
    "bsc": "bsc",
}

# Legacy dashboard/config.py map. NOTE the avalanche value: the legacy
# literal said snowscan.xyz while api/timeline.py and pages/detail.py both
# said snowtrace.io — a real three-way drift this rung resolves in favour
# of snowtrace.io (matching Explorer.api_url).
FROZEN_DASHBOARD_EXPLORERS: dict[str, str] = {
    "ethereum": "https://etherscan.io/tx/",
    "arbitrum": "https://arbiscan.io/tx/",
    "optimism": "https://optimistic.etherscan.io/tx/",
    "polygon": "https://polygonscan.com/tx/",
    "base": "https://basescan.org/tx/",
    "avalanche": "https://snowscan.xyz/tx/",
    "bsc": "https://bscscan.com/tx/",
    "sonic": "https://sonicscan.org/tx/",
    "blast": "https://blastscan.io/tx/",
    "mantle": "https://mantlescan.xyz/tx/",
    "berachain": "https://berascan.com/tx/",
    "solana": "https://solscan.io/tx/",
    "monad": "https://explorer.monad.xyz/tx/",
    "plasma": "https://plasmascan.io/tx/",
}

FROZEN_TIMELINE_CHAINS = frozenset(
    {"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bsc"}
)

FROZEN_GATEWAY_SUPPORTED_TOKENS = frozenset(
    {
        "ETH",
        "WETH",
        "USDC",
        "USDC.E",
        "ARB",
        "WBTC",
        "USDT",
        "DAI",
        "LINK",
        "RDNT",
        "SOL",
        "BTC",
        "CBETH",
    }
)

FROZEN_FRAMEWORK_SUPPORTED_CHAINS = frozenset(
    {"arbitrum", "ethereum", "base", "optimism", "avalanche", "bnb", "bsc"}
)


class TestNativeCoinGeckoProjection:
    def test_legacy_rows_byte_equivalent(self) -> None:
        projection = {s.upper(): v for s, v in native_coingecko_ids().items()}
        for symbol, cg_id in FROZEN_NATIVE_ROWS.items():
            assert projection.get(symbol.upper()) == cg_id, symbol

    def test_pinned_widening_rows(self) -> None:
        projection = dict(native_coingecko_ids())
        for symbol, cg_id in PINNED_WIDENING_ROWS.items():
            assert projection.get(symbol) == cg_id, symbol

    def test_every_registered_chain_native_priceable(self) -> None:
        # The functional point of CS-3b: no registered chain's gas asset
        # is silently unpriceable (the VIB-3805 drift class).
        projection = native_coingecko_ids()
        for d in ChainRegistry.all():
            assert d.native.symbol in projection, d.name
            assert d.native.wrapped_symbol in projection, d.name

    def test_no_invented_symbols(self) -> None:
        # Anti-widening: the projection contains ONLY declared native /
        # accepted / wrapped symbols — nothing else.
        allowed: set[str] = set()
        for d in ChainRegistry.all():
            allowed.update((d.native.symbol, *d.native.accepted_symbols))
            if d.native.wrapped_symbol:
                allowed.add(d.native.wrapped_symbol)
        assert set(native_coingecko_ids()) <= allowed

    def test_gateway_flat_map_values(self) -> None:
        from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS

        for symbol, cg_id in {**FROZEN_NATIVE_ROWS}.items():
            assert GLOBAL_TOKEN_IDS.get(symbol) == cg_id, symbol
        # Legacy ethereum-map-wins precedence for chain-variant rows must
        # survive the projection merge (BSC's WBTC->bitcoin row is
        # per-chain only; the flat map keeps ethereum's wrapped-bitcoin).
        assert GLOBAL_TOKEN_IDS.get("WBTC") == "wrapped-bitcoin"

    def test_gateway_supported_tokens_set_preserved(self) -> None:
        from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource

        assert set(CoinGeckoPriceSource._SUPPORTED_TOKENS) == FROZEN_GATEWAY_SUPPORTED_TOKENS

    def test_framework_token_ids_legacy_values(self) -> None:
        from almanak.framework.backtesting.pnl.providers.coingecko import TOKEN_IDS

        legacy = {
            **FROZEN_NATIVE_ROWS,
            "USDC": "usd-coin",
            "USDC.E": "usd-coin",
            "ARB": "arbitrum",
            "WBTC": "wrapped-bitcoin",
            "USDT": "tether",
            "DAI": "dai",
            "LINK": "chainlink",
            "UNI": "uniswap",
            "GMX": "gmx",
            "PENDLE": "pendle",
            "RDNT": "radiant-capital",
            "JOE": "trader-joe",
            "LDO": "lido-dao",
            "BTC": "bitcoin",
            "STETH": "lido-dao-wrapped-staked-eth",
            "CBETH": "coinbase-wrapped-staked-eth",
            "OP": "optimism",
            "WPOL": "polygon-ecosystem-token",
            "AAVE": "aave",
            "CRV": "curve-dao-token",
        }
        for symbol, cg_id in legacy.items():
            assert TOKEN_IDS.get(symbol) == cg_id, symbol

    def test_framework_supported_chains_superset(self) -> None:
        from almanak.framework.backtesting.pnl.providers.coingecko import (
            CoinGeckoDataProvider,
        )

        got = set(CoinGeckoDataProvider._SUPPORTED_CHAINS)
        # Deliberate widening: superset of the legacy 7-entry literal,
        # bounded by vendor presence + registered aliases.
        assert FROZEN_FRAMEWORK_SUPPORTED_CHAINS <= got


class TestExplorerDisplayLane:
    def test_dashboard_map_equivalent_except_pinned_avalanche_fix(self) -> None:
        derived = dict(explorer_tx_prefix_map())
        expected = {**FROZEN_DASHBOARD_EXPLORERS, "avalanche": "https://snowtrace.io/tx/"}
        assert derived == expected

    def test_timeline_widening_pinned(self) -> None:
        from almanak.framework.api.timeline import BLOCK_EXPLORER_URLS

        # Values identical on the legacy 7 …
        for chain in FROZEN_TIMELINE_CHAINS:
            prefix = explorer_tx_prefix_map()[chain]
            assert BLOCK_EXPLORER_URLS[chain] == prefix + "{tx_hash}"
        # … membership widens to every chain with a declared browse_url.
        assert set(BLOCK_EXPLORER_URLS) == set(explorer_tx_prefix_map())

    def test_tenderly_slugs_byte_equivalent(self) -> None:
        assert dict(vendor_chain_map("tenderly")) == FROZEN_TENDERLY_SLUGS

        from almanak.framework.models.reproduction_bundle import (
            _generate_tenderly_trace_url,
        )

        assert (
            _generate_tenderly_trace_url("ethereum", "0xabc")
            == "https://dashboard.tenderly.co/tx/mainnet/0xabc"
        )
        assert _generate_tenderly_trace_url("linea", "0xabc") is None
