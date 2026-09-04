from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


def _make_runner() -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=RunnerConfig(enable_state_persistence=False, enable_alerting=False),
    )


def _configure_merge(
    runner: StrategyRunner,
    *,
    refreshed: dict | None,
) -> tuple[MagicMock, MagicMock]:
    runner._refresh_price_oracle_for_ledger = MagicMock(return_value=refreshed)  # type: ignore[method-assign]
    address_backfill = MagicMock(side_effect=lambda _market, oracle, *_args, **_kwargs: oracle)
    symbol_backfill = MagicMock(side_effect=lambda _market, oracle, *_args, **_kwargs: oracle)
    runner._backfill_address_priced_legs = address_backfill  # type: ignore[method-assign]
    runner._backfill_coin_symbol_legs = symbol_backfill  # type: ignore[method-assign]
    return address_backfill, symbol_backfill


def test_nested_oracle_overrides_flat_inputs_and_preserves_sources() -> None:
    runner = _make_runner()
    nested = {
        "WETH": {"price_usd": "3", "oracle_source": "chainlink"},
        "ARB": {"price_usd": "4", "oracle_source": "coingecko"},
    }
    market = MagicMock()
    market.get_price_oracle_dict.return_value = nested
    address_backfill, symbol_backfill = _configure_merge(
        runner,
        refreshed={"WETH": Decimal("1"), "USDC": Decimal("1")},
    )
    state = SimpleNamespace(market=market, price_oracle={"WETH": Decimal("2")})
    intent = SimpleNamespace(chain="arbitrum")
    result = SimpleNamespace()

    merged = runner._merge_oracle_for_ledger(state, intent, result)

    assert merged == {"WETH": nested["WETH"], "USDC": Decimal("1"), "ARB": nested["ARB"]}
    market.get_price_oracle_dict.assert_called_once_with(with_sources=True)
    assert address_backfill.call_args.kwargs["with_sources"] is True
    assert symbol_backfill.call_args.kwargs["with_sources"] is True


@pytest.mark.parametrize("source_error", [TypeError("legacy market"), RuntimeError("provider failed")])
def test_source_aware_read_failure_uses_flat_cached_precedence(source_error: Exception) -> None:
    runner = _make_runner()
    market = MagicMock()
    market.get_price_oracle_dict.side_effect = source_error
    address_backfill, symbol_backfill = _configure_merge(
        runner,
        refreshed={"WETH": Decimal("1"), "USDC": Decimal("1")},
    )
    state = SimpleNamespace(market=market, price_oracle={"WETH": Decimal("2")})

    merged = runner._merge_oracle_for_ledger(state, SimpleNamespace(chain="arbitrum"))

    assert merged == {"WETH": Decimal("2"), "USDC": Decimal("1")}
    assert address_backfill.call_args.kwargs["with_sources"] is False
    assert symbol_backfill.call_args.kwargs["with_sources"] is False


def test_source_aware_provider_error_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    runner = _make_runner()
    market = MagicMock()
    market.get_price_oracle_dict.side_effect = RuntimeError("provider failed")
    _configure_merge(runner, refreshed={"USDC": Decimal("1")})

    with caplog.at_level(logging.DEBUG, logger="almanak.framework.runner.strategy_runner"):
        runner._merge_oracle_for_ledger(SimpleNamespace(market=market), SimpleNamespace(chain="arbitrum"))

    assert "get_price_oracle_dict(with_sources=True) raised" in caplog.text


@pytest.mark.parametrize("market", [None, SimpleNamespace()])
def test_market_without_source_aware_oracle_uses_flat_merge(market: object | None) -> None:
    runner = _make_runner()
    address_backfill, symbol_backfill = _configure_merge(runner, refreshed={"USDC": Decimal("1")})

    merged = runner._merge_oracle_for_ledger(
        SimpleNamespace(market=market, price_oracle={"WETH": Decimal("2")}),
        SimpleNamespace(chain="arbitrum"),
    )

    assert merged == {"USDC": Decimal("1"), "WETH": Decimal("2")}
    assert address_backfill.call_args.kwargs["with_sources"] is False
    assert symbol_backfill.call_args.kwargs["with_sources"] is False


def test_empty_inputs_still_run_both_receipt_backfills_in_order() -> None:
    runner = _make_runner()
    runner._refresh_price_oracle_for_ledger = MagicMock(return_value=None)  # type: ignore[method-assign]
    runner._backfill_address_priced_legs = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda _market, oracle, *_args, **_kwargs: {**oracle, "SUSDAI": Decimal("1.04")}
    )
    runner._backfill_coin_symbol_legs = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda _market, oracle, *_args, **_kwargs: {**oracle, "WBTC": Decimal("58000")}
    )

    merged = runner._merge_oracle_for_ledger(
        SimpleNamespace(market=None, price_oracle=None),
        SimpleNamespace(chain="arbitrum"),
        SimpleNamespace(),
    )

    assert merged == {"SUSDAI": Decimal("1.04"), "WBTC": Decimal("58000")}
    assert runner._backfill_coin_symbol_legs.call_args.args[1] == {"SUSDAI": Decimal("1.04")}


def test_empty_oracle_and_receipt_backfills_return_none() -> None:
    runner = _make_runner()
    _configure_merge(runner, refreshed=None)

    merged = runner._merge_oracle_for_ledger(
        SimpleNamespace(market=None, price_oracle=None),
        SimpleNamespace(chain="arbitrum"),
    )

    assert merged is None
