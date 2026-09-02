from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState, normalize_token_key
from almanak.framework.backtesting.pnl.initial_portfolio import (
    TokenFundingInitializationError,
    active_token_funding_entries,
    build_initial_portfolio_from_token_funding,
    funded_token_refs,
    resolve_funding_seeds,
    seed_portfolio_from_token_funding,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
PLATFORM_NATIVE_ALIAS = "0x0000000000000000000000000000000000000000"


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


def _market_state(prices: dict[object, Decimal], *, chain: str = "base") -> MarketState:
    return MarketState(
        timestamp=datetime(2026, 6, 1, tzinfo=UTC),
        chain=chain,
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


@pytest.mark.parametrize("chain", ["base", "arbitrum"])
def test_platform_zero_address_native_funding_uses_canonical_sentinel_everywhere(chain: str) -> None:
    """The shipped platform alias reaches prefetch and seeding as one key."""
    native = normalize_token_key(chain, NATIVE_SENTINEL)
    funding = [
        _funding_entry(
            symbol="ETH",
            address=PLATFORM_NATIVE_ALIAS,
            chain=chain,
            amount="300",
            amount_type="usd",
        )
    ]

    assert funded_token_refs(funding, chain=chain) == [native]

    seeds = resolve_funding_seeds(
        raw_funding=funding,
        chain=chain,
        market_state=_market_state({native: Decimal("3000")}, chain=chain),
    )

    assert seeds[0].entry.address == NATIVE_SENTINEL
    assert seeds[0].entry.symbol == "ETH"
    assert seeds[0].token == native
    assert seeds[0].amount_tokens == Decimal("0.1")
    assert seeds[0].value_usd == Decimal("300")


def test_native_alias_and_sentinel_duplicate_fails_closed() -> None:
    funding = [
        _funding_entry(symbol="ETH", address=PLATFORM_NATIVE_ALIAS, amount="300", amount_type="usd"),
        _funding_entry(symbol="ETH", address=NATIVE_SENTINEL, amount="300", amount_type="usd"),
    ]

    with pytest.raises(TokenFundingInitializationError, match="duplicate canonical token identity"):
        resolve_funding_seeds(
            raw_funding=funding,
            chain="base",
            market_state=_market_state({normalize_token_key("base", NATIVE_SENTINEL): Decimal("3000")}),
        )


@pytest.mark.parametrize("chain", ["nosuchchain", "solana"])
def test_platform_zero_address_native_alias_fails_closed_off_registered_evm(chain: str) -> None:
    with pytest.raises(TokenFundingInitializationError, match="zero-address native alias"):
        funded_token_refs(
            [_funding_entry(symbol="NATIVE", address=PLATFORM_NATIVE_ALIAS, chain=chain)],
            chain=chain,
        )


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


def test_absent_funding_points_to_missing_strategy_config() -> None:
    with pytest.raises(TokenFundingInitializationError, match="require strategy config token_funding"):
        active_token_funding_entries(None, chain="base")


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


POLYGON_NATIVE_PRECOMPILE = "0x0000000000000000000000000000000000001010"


def test_polygon_native_precompile_alias_funding_uses_canonical_sentinel_everywhere() -> None:
    """ALM-3058: ``0x...1010`` funding lands under the identity ``balance("POL")`` reads."""
    from almanak.core.chains import ChainRegistry

    native = normalize_token_key("polygon", NATIVE_SENTINEL)
    funding = [
        _funding_entry(
            symbol="POL",
            address=POLYGON_NATIVE_PRECOMPILE,
            chain="polygon",
            amount="100",
            amount_type="token",
        )
    ]

    assert funded_token_refs(funding, chain="polygon") == [native]

    seeds = resolve_funding_seeds(
        raw_funding=funding,
        chain="polygon",
        market_state=_market_state({native: Decimal("0.5")}, chain="polygon"),
    )

    assert seeds[0].entry.address == NATIVE_SENTINEL
    # The registry keeps Polygon's gas/price symbol pinned to MATIC; POL is an
    # accepted symbol for the same native coin and resolves to the same key.
    assert seeds[0].entry.symbol == ChainRegistry.resolve("polygon").native.symbol.upper()
    assert seeds[0].token == native
    assert seeds[0].amount_tokens == Decimal("100")
    assert seeds[0].value_usd == Decimal("50")

    portfolio = build_initial_portfolio_from_token_funding(
        raw_funding=funding,
        chain="polygon",
        market_state=_market_state({native: Decimal("0.5")}, chain="polygon"),
    )

    assert portfolio.tokens == {native: Decimal("100")}
    assert portfolio.get_token_balance("POL") == Decimal("100")
    assert portfolio.get_token_balance("MATIC") == Decimal("100")
    assert portfolio.get_token_balance(NATIVE_SENTINEL) == Decimal("100")
    assert portfolio.initial_capital_usd == Decimal("50")


def test_polygon_native_precompile_alias_is_not_folded_off_polygon() -> None:
    """The fold is chain-scoped: elsewhere ``0x...1010`` stays an ordinary address."""
    funding = [_funding_entry(symbol="X1010", address=POLYGON_NATIVE_PRECOMPILE, chain="arbitrum", amount="1")]

    assert funded_token_refs(funding, chain="arbitrum") == [normalize_token_key("arbitrum", POLYGON_NATIVE_PRECOMPILE)]


def test_polygon_precompile_and_zero_address_aliases_duplicate_fails_closed() -> None:
    funding = [
        _funding_entry(symbol="POL", address=POLYGON_NATIVE_PRECOMPILE, chain="polygon", amount="1"),
        _funding_entry(symbol="MATIC", address=PLATFORM_NATIVE_ALIAS, chain="polygon", amount="1"),
    ]

    with pytest.raises(TokenFundingInitializationError, match="duplicate canonical token identity"):
        funded_token_refs(funding, chain="polygon")
