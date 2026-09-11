"""Declared token universes reach historical coverage before strategy decisions."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.services.backtest.services.backtest_runner import collect_backtest_token_refs
from scripts import platform_backtest_runner as runner

NVDAB = "0x02fca66c1d1afb4e2a7884261eb00f63598a7436"
AAPLB = "0x431a3bee82e2ca41e49895cbece5bb0f76a89b7a"
USDT = "0x55d398326f99059ff775485246999027b3197955"
POOL = "0x8fb4243b553ac29ba088acf00b9b7da24bd6690c"


class _Strategy:
    deployment_id = "declared_universe_preflight"

    def decide(self, market: Any) -> None:
        pytest.fail("unavailable declared token must be rejected before decide")


def _strategy_config() -> dict[str, Any]:
    return {
        "chain": "bsc",
        "reserve_token": {"symbol": "USDT", "address": USDT},
        "universe": [
            {"symbol": "NVDAB", "address": NVDAB, "pool": POOL},
            {"symbol": "AAPLB", "address": AAPLB},
        ],
        "token_funding": [{"symbol": "USDT", "address": USDT, "amount": "50", "amount_type": "usd"}],
    }


def _config(**overrides: Any):
    payload = {"start_time": "2026-06-10", "end_time": "2026-09-10", **overrides}
    return runner.build_platform_backtest_config(json.dumps(payload), _strategy_config(), _Strategy)


def test_declared_universe_and_reserve_are_historical_tokens():
    config = _config()
    addresses = runner.build_backtest_token_address_map(config, strategy_config=_strategy_config())

    identities = {token if isinstance(token, tuple) else addresses[token] for token in config.tokens}
    assert identities == {("bsc", NVDAB), ("bsc", AAPLB), ("bsc", USDT)}
    assert addresses["USDT"] == ("bsc", USDT)
    assert POOL not in {address for _, address in addresses.values()}
    assert config.preflight_validation is True
    assert config.fail_on_preflight_error is True


def test_explicit_tokens_do_not_erase_declared_universe():
    config = _config(tokens=["USDT"])
    addresses = runner.build_backtest_token_address_map(config, strategy_config=_strategy_config())
    identities = {token if isinstance(token, tuple) else addresses[token] for token in config.tokens}
    assert identities == {("bsc", NVDAB), ("bsc", AAPLB), ("bsc", USDT)}


def test_explicit_preflight_opt_out_is_preserved():
    config = _config(preflight_validation=False, fail_on_preflight_error=False)
    assert config.preflight_validation is False
    assert config.fail_on_preflight_error is False


def test_only_token_positions_accept_address_first_objects():
    refs = collect_backtest_token_refs(
        chain="bsc",
        strategy_config={
            "universe": [{"symbol": "WRONG_SYMBOL", "address": NVDAB, "pool": POOL}],
            "reserve_token": {"symbol": "USDT", "address": USDT},
            "base_token": {"symbol": "AAPLB", "address": AAPLB},
            "wallet": {"address": "0x1111111111111111111111111111111111111111"},
        },
    )
    assert set(refs) == {NVDAB, AAPLB, USDT}


@pytest.mark.asyncio
async def test_unavailable_universe_fails_before_any_decision():
    class Provider:
        provider_name = "resolution_based_preflight"
        historical_capability = HistoricalDataCapability.FULL
        resolution_based_availability = True
        supported_tokens: list[str] = []

        async def get_price(self, token: Any, timestamp: datetime):
            if str(token).upper() == "NVDAB" or token == ("bsc", NVDAB) or NVDAB in str(token).lower():
                raise ValueError("Unknown token: NVDAB")
            return Decimal("1")

        async def iterate(self, config: Any):
            pytest.fail("unavailable declared token must be rejected before iteration")
            yield

    backtester = PnLBacktester(data_provider=Provider(), fee_models={}, slippage_models={})
    with pytest.raises(PreflightValidationError, match="preflight|Preflight|historical"):
        await backtester.backtest(_Strategy(), _config())


@pytest.mark.asyncio
async def test_priced_unheld_universe_has_measured_zero_simulated_balances():
    from almanak.framework.backtesting.pnl.data_provider import MarketState

    observations: list[tuple[Decimal, Decimal]] = []

    class Strategy:
        deployment_id = "priced_universe_balances"

        def decide(self, market: Any) -> None:
            observations.append((market.balance(NVDAB).balance, market.balance(USDT).balance))

    class Provider:
        provider_name = "historical_address_fixture"
        historical_capability = HistoricalDataCapability.FULL
        resolution_based_availability = True
        supported_tokens: list[str] = []

        async def get_price(self, token: Any, timestamp: datetime):
            return Decimal("100") if token in {("bsc", NVDAB), ("bsc", AAPLB)} else Decimal("1")

        async def iterate(self, config: Any):
            yield (
                config.start_time,
                MarketState(
                    timestamp=config.start_time,
                    prices={
                        ("bsc", NVDAB): Decimal("100"),
                        ("bsc", AAPLB): Decimal("100"),
                        ("bsc", USDT): Decimal("1"),
                    },
                    chain="bsc",
                    block_number=1,
                ),
            )

    config = _config(end_time="2026-06-11", include_gas_costs=False)
    backtester = PnLBacktester(data_provider=Provider(), fee_models={}, slippage_models={})
    result = await backtester.backtest(Strategy(), config)

    assert result.success is True
    assert observations == [(Decimal("0"), Decimal("50"))]
    assert not result.decision_input_failures
