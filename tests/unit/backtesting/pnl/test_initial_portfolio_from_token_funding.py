from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState, normalize_token_key
from almanak.framework.backtesting.pnl.initial_portfolio import (
    TokenFundingInitializationError,
    active_token_funding_entries,
    build_initial_portfolio_from_token_funding,
    resolve_funding_seeds,
    seed_portfolio_from_token_funding,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


def _funding_entry(
    *,
    symbol: str = "cbBTC",
    address: str = BASE_CBBTC,
    chain: str | None = "base",
    amount: str = "1",
    amount_type: str = "token",
) -> dict[str, str]:
    entry = {
        "symbol": symbol,
        "address": address,
        "amount": amount,
        "amount_type": amount_type,
    }
    if chain is not None:
        entry["chain"] = chain
    return entry


def _market_state(prices: dict[object, Decimal]) -> MarketState:
    return MarketState(
        timestamp=datetime(2026, 6, 1, tzinfo=UTC),
        chain="base",
        prices=prices,
    )


def test_filters_token_funding_to_active_chain_and_defaults_missing_chain() -> None:
    funding = [
        _funding_entry(symbol="USDC", address=ARB_USDC, chain="arbitrum"),
        _funding_entry(symbol="cbBTC", address=BASE_CBBTC, chain=None),
    ]

    active = active_token_funding_entries(funding, chain="base")

    assert len(active) == 1
    assert active[0].symbol == "CBBTC"
    assert active[0].chain == "base"


def test_token_amount_seeds_exact_address_native_units_and_cost_basis() -> None:
    token = normalize_token_key("base", BASE_CBBTC)
    state = _market_state({token: Decimal("100000")})
    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain="base")

    initial_value = seed_portfolio_from_token_funding(
        portfolio,
        raw_funding=[_funding_entry(amount="0.25", amount_type="token")],
        chain="base",
        market_state=state,
    )

    assert portfolio.cash_usd == Decimal("0")
    assert portfolio.tokens == {token: Decimal("0.25")}
    assert portfolio._cost_basis[token] == Decimal("100000")
    assert initial_value == Decimal("25000.00")
    assert portfolio.initial_capital_usd == Decimal("25000.00")


def test_usd_amount_converts_to_explicit_token_units_at_first_tick_price() -> None:
    token = normalize_token_key("base", BASE_CBBTC)
    state = _market_state({token: Decimal("100000")})

    seeds = resolve_funding_seeds(
        raw_funding=[_funding_entry(amount="200", amount_type="usd")],
        chain="base",
        market_state=state,
    )

    assert seeds[0].amount_tokens == Decimal("0.002")
    assert seeds[0].value_usd == Decimal("200")
    assert seeds[0].price_usd == Decimal("100000")


def test_usdc_funding_remains_explicit_token_balance_not_cash() -> None:
    token = normalize_token_key("base", BASE_USDC)
    state = _market_state({token: Decimal("1")})
    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain="base")

    seed_portfolio_from_token_funding(
        portfolio,
        raw_funding=[
            _funding_entry(symbol="USDC", address=BASE_USDC, amount="200", amount_type="usd"),
        ],
        chain="base",
        market_state=state,
    )

    assert portfolio.cash_usd == Decimal("0")
    assert portfolio.tokens[token] == Decimal("200")


def test_build_initial_portfolio_from_token_funding_wires_empty_cash_and_initial_value() -> None:
    token = normalize_token_key("base", BASE_USDC)
    state = _market_state({token: Decimal("1")})

    portfolio = build_initial_portfolio_from_token_funding(
        raw_funding=[_funding_entry(symbol="USDC", address=BASE_USDC, amount="200", amount_type="usd")],
        chain="base",
        market_state=state,
    )

    assert portfolio.cash_usd == Decimal("0")
    assert portfolio.tokens[token] == Decimal("200")
    assert portfolio.initial_capital_usd == Decimal("200")


def test_percentage_funding_is_rejected_for_pnl_startup() -> None:
    token = normalize_token_key("base", BASE_CBBTC)
    state = _market_state({token: Decimal("100000")})

    with pytest.raises(TokenFundingInitializationError, match="percentage"):
        resolve_funding_seeds(
            raw_funding=[_funding_entry(amount="50", amount_type="percentage")],
            chain="base",
            market_state=state,
        )


def test_missing_first_tick_price_fails_loud() -> None:
    with pytest.raises(TokenFundingInitializationError, match="Missing first-tick price"):
        resolve_funding_seeds(
            raw_funding=[_funding_entry(amount="200", amount_type="usd")],
            chain="base",
            market_state=_market_state({}),
        )


def test_none_first_tick_price_fails_loud() -> None:
    token = normalize_token_key("base", BASE_CBBTC)

    with pytest.raises(TokenFundingInitializationError, match="must be positive"):
        resolve_funding_seeds(
            raw_funding=[_funding_entry(amount="200", amount_type="usd")],
            chain="base",
            market_state=_market_state({token: None}),  # type: ignore[dict-item]
        )


def test_negative_amount_fails_loud() -> None:
    token = normalize_token_key("base", BASE_CBBTC)
    state = _market_state({token: Decimal("100000")})

    with pytest.raises(TokenFundingInitializationError, match="cannot be negative"):
        resolve_funding_seeds(
            raw_funding=[_funding_entry(amount="-1", amount_type="token")],
            chain="base",
            market_state=state,
        )


def test_zero_amount_is_allowed_without_seeding_balance() -> None:
    token = normalize_token_key("base", BASE_CBBTC)
    state = _market_state({token: Decimal("100000")})
    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"), cash_usd=Decimal("0"), chain="base")

    initial_value = seed_portfolio_from_token_funding(
        portfolio,
        raw_funding=[_funding_entry(amount="0", amount_type="token")],
        chain="base",
        market_state=state,
    )

    assert initial_value == Decimal("0")
    assert portfolio.tokens == {}
    assert portfolio._cost_basis == {}


def test_zero_amount_does_not_require_first_tick_price() -> None:
    seeds = resolve_funding_seeds(
        raw_funding=[_funding_entry(amount="0", amount_type="usd")],
        chain="base",
        market_state=_market_state({}),
    )

    assert seeds[0].amount_tokens == Decimal("0")
    assert seeds[0].value_usd == Decimal("0")
    assert seeds[0].price_usd == Decimal("0")


def test_missing_active_chain_funding_fails_loud() -> None:
    with pytest.raises(TokenFundingInitializationError, match="active chain"):
        active_token_funding_entries([_funding_entry(chain="arbitrum", address=ARB_USDC)], chain="base")


def test_cross_chain_drop_is_loud(caplog) -> None:
    # Declared capital filtered off the run (chain mismatch) must WARN per
    # entry - the CLI echoes the pre-filter count, so a silent drop just
    # looks like a poorer backtest (re-cut phase 1; harness audit item).
    import logging

    with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.initial_portfolio"):
        active = active_token_funding_entries(
            [
                _funding_entry(),
                _funding_entry(chain="arbitrum", address=ARB_USDC),
            ],
            chain="base",
        )

    assert len(active) == 1
    dropped_warnings = [r for r in caplog.records if "DROPPED" in r.message]
    assert len(dropped_warnings) == 1
    assert "arbitrum" in dropped_warnings[0].message
    assert "NOT funded" in dropped_warnings[0].message
