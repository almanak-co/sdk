"""Engine-plane fixtures: portfolios and market states shaped like a real run.

Hand-built symbol-keyed fixtures drift from the engine's address-native key
plane (how ALM-2960 escaped the unit suite). These factories build through
the same code paths the engine loop uses, in the same order: portfolio
construction, identity registration, token-funding seed. The parity test in
``test_engine_plane_parity.py`` pins them to the real loop.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.pnl.data_provider import MarketState, normalize_token_key
from almanak.framework.backtesting.pnl.initial_portfolio import seed_portfolio_from_token_funding
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

__all__ = [
    "DEFAULT_CHAIN",
    "DEFAULT_TOKEN_ADDRESSES",
    "START",
    "USDC_ARBITRUM",
    "WETH_ARBITRUM",
    "make_run_market_state",
    "make_run_portfolio",
]

DEFAULT_CHAIN = "arbitrum"
WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
START = datetime(2024, 1, 1, tzinfo=UTC)

DEFAULT_TOKEN_ADDRESSES: dict[str, tuple[str, str]] = {
    "WETH": (DEFAULT_CHAIN, WETH_ARBITRUM),
    "USDC": (DEFAULT_CHAIN, USDC_ARBITRUM),
}

_DEFAULT_PRICES = {"WETH": "2000", "USDC": "1"}


def make_run_market_state(
    *,
    chain: str = DEFAULT_CHAIN,
    hour: int = 0,
    prices: dict[str, str | Decimal] | None = None,
    token_addresses: dict[str, tuple[str, str]] | None = None,
    gas_price_gwei: str | Decimal = "30",
) -> MarketState:
    """A tick's MarketState with the real run's key plane.

    ``prices`` is symbol-keyed for caller convenience; tokens present in
    ``token_addresses`` ALSO get their address-native ``(chain, address)`` key
    (that is the plane providers emit for funded tokens), and the map is
    registered as symbol aliases exactly as the engine loop does each tick.
    """
    if chain != DEFAULT_CHAIN and token_addresses is None:
        raise ValueError("token_addresses is required for non-default chains (the defaults are arbitrum-keyed)")
    token_addresses = DEFAULT_TOKEN_ADDRESSES if token_addresses is None else token_addresses
    symbol_prices = {sym: Decimal(str(px)) for sym, px in (prices or _DEFAULT_PRICES).items()}

    plane: dict[Any, Decimal] = dict(symbol_prices)
    for symbol, (token_chain, address) in token_addresses.items():
        if symbol in symbol_prices:
            plane[normalize_token_key(token_chain, address)] = symbol_prices[symbol]

    state = MarketState(
        timestamp=START + timedelta(hours=hour),
        prices=plane,
        chain=chain,
        block_number=1_000_000 + hour,
        gas_price_gwei=Decimal(str(gas_price_gwei)),
    )
    if token_addresses:
        state.register_symbol_aliases(token_addresses)
    return state


def make_run_portfolio(
    *,
    chain: str = DEFAULT_CHAIN,
    funding: list[dict[str, Any]] | None = None,
    token_addresses: dict[str, tuple[str, str]] | None = None,
    market_state: MarketState | None = None,
) -> SimulatedPortfolio:
    """A portfolio seeded and registered exactly as the engine loop does it.

    Defaults fund 10,000 USDC on arbitrum. The returned portfolio's balance
    keys are ADDRESS-NATIVE (never bare symbols) because that is what
    ``initial_portfolio`` produces in every real run — a test that wants a
    symbol-keyed portfolio is testing a world the engine no longer inhabits
    and should construct it explicitly (and say why).
    """
    if chain != DEFAULT_CHAIN and token_addresses is None:
        raise ValueError("token_addresses is required for non-default chains (the defaults are arbitrum-keyed)")
    token_addresses = DEFAULT_TOKEN_ADDRESSES if token_addresses is None else token_addresses
    if market_state is None:
        market_state = make_run_market_state(chain=chain, token_addresses=token_addresses)
    if funding is None:
        funding = [
            {
                "symbol": "USDC",
                "address": USDC_ARBITRUM,
                "chain": chain,
                "amount": "10000",
                "amount_type": "token",
            }
        ]

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain=chain)
    portfolio.register_token_identities(token_addresses)
    seed_portfolio_from_token_funding(portfolio, raw_funding=funding, chain=chain, market_state=market_state)
    return portfolio
