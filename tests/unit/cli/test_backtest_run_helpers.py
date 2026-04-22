"""Unit tests for `almanak.framework.cli.backtest.run_helpers`.

Phase 5B.1 introduces this shared scaffold consumed by both `pnl_backtest`
and `sweep_backtest`. Tests focus on behavioural contracts that downstream
commands rely on:

- `validate_strategy_is_registered`: error strings, exit type.
- `parse_token_list`: whitespace + case handling.
- `ensure_strategy_id`: `_strategy_id`-before-`strategy_id` precedence and
  no-op when the id is already populated.
- `resolve_strategy_class_or_mock`: allow-mock fallback warning vs strict
  abort path.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import click
import pytest

from almanak.framework.cli.backtest.run_helpers import (
    ensure_strategy_id,
    parse_token_list,
    resolve_strategy_class_or_mock,
    validate_strategy_is_registered,
)


# =============================================================================
# parse_token_list
# =============================================================================


class TestParseTokenList:
    def test_splits_comma_separated(self) -> None:
        assert parse_token_list("WETH,USDC") == ["WETH", "USDC"]

    def test_strips_whitespace(self) -> None:
        assert parse_token_list(" weth , usdc ") == ["WETH", "USDC"]

    def test_upper_cases_values(self) -> None:
        assert parse_token_list("weth,usdc,btc") == ["WETH", "USDC", "BTC"]

    def test_single_token(self) -> None:
        assert parse_token_list("eth") == ["ETH"]

    def test_empty_segments_produce_empty_strings(self) -> None:
        # Matches the inline behaviour we are preserving: split then strip.
        # Downstream code relies on this shape; do not silently drop empties.
        assert parse_token_list("WETH,,USDC") == ["WETH", "", "USDC"]


# =============================================================================
# validate_strategy_is_registered
# =============================================================================


class TestValidateStrategyIsRegistered:
    def test_noop_when_registered(self) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=["demo_lp", "mean_reversion"],
        ):
            # Must not raise
            validate_strategy_is_registered("demo_lp")

    def test_aborts_when_missing(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=["demo_lp"],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("does_not_exist")

        captured = capsys.readouterr()
        assert "Error: Strategy 'does_not_exist' is not registered." in captured.err

    def test_lists_available_strategies_when_any(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=["beta", "alpha"],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("missing")

        captured = capsys.readouterr()
        # Sorted output
        assert "Available strategies: alpha, beta" in captured.err

    def test_omits_available_line_when_registry_empty(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=[],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("missing")

        captured = capsys.readouterr()
        assert "Available strategies:" not in captured.err

    def test_includes_discovery_guidance(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=[],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("missing")

        captured = capsys.readouterr()
        assert "The backtest command discovers strategies by:" in captured.err
        assert "Importing ./strategy.py in the current working directory" in captured.err
        assert "$ALMANAK_STRATEGIES_DIR" in captured.err
        assert "almanak strat new --name <name>" in captured.err


# =============================================================================
# ensure_strategy_id
# =============================================================================


class _StrategyWithPrivateId:
    """Simulates an IntentStrategy that backs strategy_id with _strategy_id."""

    def __init__(self, initial: str = "") -> None:
        self._strategy_id = initial

    @property
    def strategy_id(self) -> str:
        return self._strategy_id


class _PlainStrategy:
    """Simulates a minimal strategy with only a public attribute."""

    strategy_id: str = ""


class TestEnsureStrategyId:
    def test_noop_when_strategy_id_already_set(self) -> None:
        strat = _StrategyWithPrivateId(initial="existing-id")
        ensure_strategy_id(strat, fallback="should-not-apply")
        assert strat.strategy_id == "existing-id"

    def test_sets_private_attribute_when_present(self) -> None:
        strat = _StrategyWithPrivateId(initial="")
        ensure_strategy_id(strat, fallback="fallback-id")
        assert strat._strategy_id == "fallback-id"
        assert strat.strategy_id == "fallback-id"

    def test_sets_public_attribute_when_no_private(self) -> None:
        strat = _PlainStrategy()
        ensure_strategy_id(strat, fallback="plain-id")
        assert strat.strategy_id == "plain-id"

    def test_noop_on_plain_strategy_with_existing_id(self) -> None:
        strat = _PlainStrategy()
        strat.strategy_id = "already-set"
        ensure_strategy_id(strat, fallback="ignored")
        assert strat.strategy_id == "already-set"

    def test_missing_attribute_falls_back_to_public_assignment(self) -> None:
        """Instances without strategy_id at all still receive the fallback."""

        class _Bare:
            pass

        strat = _Bare()
        ensure_strategy_id(strat, fallback="bare-id")
        assert strat.strategy_id == "bare-id"  # type: ignore[attr-defined]


# =============================================================================
# resolve_strategy_class_or_mock
# =============================================================================


class TestResolveStrategyClassOrMock:
    def test_returns_class_when_registered(self) -> None:
        class Dummy:
            strategy_id = "dummy"

        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            return_value=Dummy,
        ):
            result = resolve_strategy_class_or_mock("dummy", allow_mock=False)
        assert result is Dummy

    def test_allow_mock_returns_mock_on_value_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError("not registered"),
        ):
            result = resolve_strategy_class_or_mock("missing", allow_mock=True)

        # Mock class must be instantiable and satisfy the contract used in sweep.
        instance = result({"foo": "bar"})
        assert instance.strategy_id == "mock-sweep"
        # decide() returns None for the mock
        assert instance.decide(market=None) is None  # type: ignore[arg-type]

        captured = capsys.readouterr()
        assert "No strategies registered in factory." in captured.err
        assert "Running with mock strategy for demonstration." in captured.err

    def test_no_mock_path_aborts_on_value_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError("not registered"),
        ):
            with pytest.raises(click.Abort):
                resolve_strategy_class_or_mock("missing", allow_mock=False)

        captured = capsys.readouterr()
        assert "Error: Strategy 'missing' is not registered." in captured.err

    def test_mock_config_is_retained(self) -> None:
        """The mock class preserves the config dict it was constructed with."""
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError(),
        ):
            mock_cls: Any = resolve_strategy_class_or_mock("x", allow_mock=True)

        cfg = {"a": 1, "b": 2}
        instance = mock_cls(cfg)
        assert instance.config == cfg
