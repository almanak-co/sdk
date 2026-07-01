from datetime import UTC, datetime

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def _funding(symbol: str, address: str, amount: str) -> dict[str, str]:
    return {
        "symbol": symbol,
        "address": address,
        "chain": "arbitrum",
        "amount": amount,
        "amount_type": "token",
    }


def _config(**overrides) -> PnLBacktestConfig:
    params = {
        "start_time": datetime(2026, 6, 1, tzinfo=UTC),
        "end_time": datetime(2026, 6, 2, tzinfo=UTC),
        "token_funding": [_funding("USDC", USDC_ARBITRUM, "10000")],
    }
    params.update(overrides)
    return PnLBacktestConfig(**params)


def test_token_funding_must_be_a_list() -> None:
    with pytest.raises(ValueError, match="token_funding must be a list"):
        _config(token_funding={"symbol": "USDC"})


def test_from_dict_rejects_legacy_initial_capital_usd() -> None:
    data = _config().to_dict()
    data["initial_capital_usd"] = "10000"

    with pytest.raises(ValueError, match="initial_capital_usd"):
        PnLBacktestConfig.from_dict(data)


def test_config_hash_order_normalizes_token_funding() -> None:
    usdc = _funding("USDC", USDC_ARBITRUM, "10000")
    weth = _funding("WETH", WETH_ARBITRUM, "1")

    first = _config(token_funding=[usdc, weth])
    second = _config(token_funding=[weth, usdc])

    assert first.calculate_config_hash() == second.calculate_config_hash()
