from datetime import date

from almanak.services.backtest.models import StrategySpec, TimeframeSpec
from almanak.services.backtest.services.backtest_runner import build_backtest_config

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"

TOKEN_FUNDING = [
    {
        "symbol": "cbBTC",
        "address": BASE_CBBTC,
        "chain": "base",
        "amount": "200",
        "amount_type": "usd",
    }
]


def _timeframe() -> TimeframeSpec:
    return TimeframeSpec(start=date(2026, 6, 1), end=date(2026, 6, 2))


def test_request_level_token_funding_is_included_in_strategy_spec_token_coverage() -> None:
    config = build_backtest_config(
        StrategySpec(protocol="uniswap_v3", chain="base", action="swap", parameters={}),
        _timeframe(),
        token_funding=TOKEN_FUNDING,
    )

    assert "CBBTC" in {token.upper() for token in config.tokens}


def test_request_level_token_funding_is_included_in_named_strategy_token_coverage() -> None:
    config = build_backtest_config(
        None,
        _timeframe(),
        chain="base",
        tokens=["WETH"],
        token_funding=TOKEN_FUNDING,
    )

    assert "CBBTC" in {token.upper() for token in config.tokens}
