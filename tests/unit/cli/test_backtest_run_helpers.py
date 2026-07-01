"""Unit tests for `almanak.framework.cli.backtest.run_helpers`.

Phase 5B.1 introduces this shared scaffold consumed by both `pnl_backtest`
and `sweep_backtest`. Tests focus on behavioural contracts that downstream
commands rely on:

- `validate_strategy_is_registered`: error strings, exit type.
- `parse_token_list`: whitespace + case handling.
- `ensure_deployment_id`: `_deployment_id`-before-`deployment_id` precedence and
  no-op when the id is already populated.
- `resolve_strategy_class_or_mock`: allow-mock fallback warning vs strict
  abort path.

Phase 5B.4 extends coverage across these helpers to >= 85% with focus on
error paths, `build_pnl_config` kwarg propagation edge cases, and
`ensure_deployment_id` precedence corner cases.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import click
import pytest

from almanak.framework.cli.backtest.run_helpers import (
    build_pnl_config,
    ensure_deployment_id,
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

    def test_lists_available_strategies_when_any(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=["beta", "alpha"],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("missing")

        captured = capsys.readouterr()
        # Sorted output
        assert "Available strategies: alpha, beta" in captured.err

    def test_omits_available_line_when_registry_empty(self, capsys: pytest.CaptureFixture[str]) -> None:
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
# ensure_deployment_id
# =============================================================================


class _StrategyWithPrivateId:
    """Simulates an IntentStrategy that backs deployment_id with _deployment_id."""

    def __init__(self, initial: str = "") -> None:
        self._deployment_id = initial

    @property
    def deployment_id(self) -> str:
        return self._deployment_id


class _PlainStrategy:
    """Simulates a minimal strategy with only a public attribute."""

    deployment_id: str = ""


class TestEnsureStrategyId:
    def test_noop_when_deployment_id_already_set(self) -> None:
        strat = _StrategyWithPrivateId(initial="existing-id")
        ensure_deployment_id(strat, fallback="should-not-apply")
        assert strat.deployment_id == "existing-id"

    def test_sets_private_attribute_when_present(self) -> None:
        strat = _StrategyWithPrivateId(initial="")
        ensure_deployment_id(strat, fallback="fallback-id")
        assert strat._deployment_id == "fallback-id"
        assert strat.deployment_id == "fallback-id"

    def test_sets_public_attribute_when_no_private(self) -> None:
        strat = _PlainStrategy()
        ensure_deployment_id(strat, fallback="plain-id")
        assert strat.deployment_id == "plain-id"

    def test_noop_on_plain_strategy_with_existing_id(self) -> None:
        strat = _PlainStrategy()
        strat.deployment_id = "already-set"
        ensure_deployment_id(strat, fallback="ignored")
        assert strat.deployment_id == "already-set"

    def test_missing_attribute_falls_back_to_public_assignment(self) -> None:
        """Instances without deployment_id at all still receive the fallback."""

        class _Bare:
            pass

        strat = _Bare()
        ensure_deployment_id(strat, fallback="bare-id")
        assert strat.deployment_id == "bare-id"  # type: ignore[attr-defined]


# =============================================================================
# resolve_strategy_class_or_mock
# =============================================================================


class TestResolveStrategyClassOrMock:
    def test_returns_class_when_registered(self) -> None:
        class Dummy:
            deployment_id = "dummy"

        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            return_value=Dummy,
        ):
            result = resolve_strategy_class_or_mock("dummy", allow_mock=False)
        assert result is Dummy

    def test_allow_mock_returns_mock_on_value_error(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError("not registered"),
        ):
            result = resolve_strategy_class_or_mock("missing", allow_mock=True)

        # Mock class must be instantiable and satisfy the contract used in sweep.
        instance = result({"foo": "bar"})
        assert instance.deployment_id == "mock-sweep"
        # decide() returns None for the mock
        assert instance.decide(market=None) is None  # type: ignore[arg-type]

        captured = capsys.readouterr()
        assert "No strategies registered in factory." in captured.err
        assert "Running with mock strategy for demonstration." in captured.err

    def test_no_mock_path_aborts_on_value_error(self, capsys: pytest.CaptureFixture[str]) -> None:
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


# =============================================================================
# Phase 5B.4 extended coverage
# =============================================================================


class TestEnsureStrategyIdPrecedence:
    """Extended edge cases for `_deployment_id` vs `deployment_id` precedence."""

    def test_both_private_and_public_prefers_private(self) -> None:
        """When both attrs exist, assignment goes to `_deployment_id`."""

        class _Both:
            def __init__(self) -> None:
                self._deployment_id: str = ""
                self.deployment_id: str = ""

        strat = _Both()
        ensure_deployment_id(strat, fallback="priv-id")
        assert strat._deployment_id == "priv-id"
        # `deployment_id` is untouched when private channel exists
        assert strat.deployment_id == ""

    def test_truthy_existing_id_beats_private_channel(self) -> None:
        """Non-empty public `deployment_id` is respected even when `_deployment_id` exists."""

        class _BothExisting:
            def __init__(self) -> None:
                self._deployment_id: str = ""
                self.deployment_id: str = "already-set"

        strat = _BothExisting()
        ensure_deployment_id(strat, fallback="ignored")
        assert strat.deployment_id == "already-set"
        assert strat._deployment_id == ""  # no write happened

    def test_noop_when_private_channel_already_populated(self) -> None:
        """IntentStrategy-style: `deployment_id` reads `_deployment_id`; when latter set, no-op."""

        class _Backed:
            def __init__(self, initial: str) -> None:
                self._deployment_id = initial

            @property
            def deployment_id(self) -> str:
                return self._deployment_id

        strat = _Backed(initial="pre-seeded")
        ensure_deployment_id(strat, fallback="ignored")
        assert strat._deployment_id == "pre-seeded"

    def test_falsy_empty_string_triggers_fallback(self) -> None:
        """Empty string is falsy — triggers the fallback path."""

        class _EmptyPlain:
            deployment_id: str = ""

        strat = _EmptyPlain()
        ensure_deployment_id(strat, fallback="from-fallback")
        assert strat.deployment_id == "from-fallback"

    def test_none_existing_id_triggers_fallback(self) -> None:
        """None is falsy — triggers the fallback path via public attr."""

        class _NoneId:
            deployment_id: str | None = None

        strat = _NoneId()
        ensure_deployment_id(strat, fallback="from-none")
        assert strat.deployment_id == "from-none"


class TestBuildPnlConfigKwargs:
    """Coverage gaps for `build_pnl_config` kwarg propagation."""

    _start = datetime(2024, 1, 1, tzinfo=UTC)
    _end = datetime(2024, 2, 1, tzinfo=UTC)

    def test_allow_degraded_data_false_propagates(self) -> None:
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
            allow_degraded_data=False,
        )
        assert cfg.allow_degraded_data is False
        # other sweep kwargs still fall through to dataclass defaults
        assert cfg.preflight_validation is True
        assert cfg.fail_on_preflight_error is True

    def test_preflight_validation_false_propagates(self) -> None:
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
            preflight_validation=False,
        )
        assert cfg.preflight_validation is False
        assert cfg.allow_degraded_data is True
        assert cfg.fail_on_preflight_error is True

    def test_fail_on_preflight_error_false_propagates(self) -> None:
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
            fail_on_preflight_error=False,
        )
        assert cfg.fail_on_preflight_error is False
        assert cfg.allow_degraded_data is True
        assert cfg.preflight_validation is True

    def test_include_gas_costs_false_propagates(self) -> None:
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
            include_gas_costs=False,
        )
        assert cfg.include_gas_costs is False

    def test_explicit_none_kwargs_fall_through_to_defaults(self) -> None:
        """Passing None explicitly should still retain dataclass defaults."""
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
            allow_degraded_data=None,
            preflight_validation=None,
            fail_on_preflight_error=None,
        )
        assert cfg.allow_degraded_data is True
        assert cfg.preflight_validation is True
        assert cfg.fail_on_preflight_error is True

    def test_token_funding_not_list_raises_underlying_validation(self) -> None:
        """Non-list token_funding raises in `PnLBacktestConfig.__post_init__`."""
        with pytest.raises(ValueError, match="token_funding must be a list"):
            build_pnl_config(
                start_time=self._start,
                end_time=self._end,
                interval_seconds=3600,
                chain="arbitrum",
                tokens=["WETH"],
                token_funding="not-a-list",  # type: ignore[arg-type]
            )

    def test_tokens_empty_list_raises_underlying_validation(self) -> None:
        """Empty token list raises in `PnLBacktestConfig.__post_init__`."""
        with pytest.raises(ValueError, match="tokens list cannot be empty"):
            build_pnl_config(
                start_time=self._start,
                end_time=self._end,
                interval_seconds=3600,
                chain="arbitrum",
                tokens=[],
            )

    def test_gas_price_default_is_chain_aware(self) -> None:
        """VIB-5088: the flat 30 gwei default is gone -- an unset gas price
        resolves from the chain registry (arbitrum: 0.1 gwei) and is marked
        default-sourced for the audit trail."""
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
        )
        assert cfg.gas_price_gwei == Decimal("0.1")
        assert cfg.gas_price_gwei_is_default is True

    def test_gas_price_explicit_pass_through(self) -> None:
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=3600,
            chain="arbitrum",
            tokens=["WETH"],
            gas_price_gwei=30.0,
        )
        assert cfg.gas_price_gwei == Decimal("30.0")
        assert cfg.gas_price_gwei_is_default is False

    def test_interval_seconds_pass_through(self) -> None:
        cfg = build_pnl_config(
            start_time=self._start,
            end_time=self._end,
            interval_seconds=7200,
            chain="base",
            tokens=["WETH"],
        )
        assert cfg.interval_seconds == 7200
        assert cfg.chain == "base"


class TestParseTokenListExtended:
    """Extended parsing edge cases."""

    def test_mixed_case_upper_applied(self) -> None:
        assert parse_token_list("WeTh,UsDc,BtC") == ["WETH", "USDC", "BTC"]

    def test_only_whitespace_token_becomes_empty_string(self) -> None:
        """Whitespace-only segment strips to empty (behaviour we preserve)."""
        assert parse_token_list("WETH,   ,USDC") == ["WETH", "", "USDC"]

    def test_trailing_comma_produces_trailing_empty(self) -> None:
        assert parse_token_list("WETH,") == ["WETH", ""]

    def test_leading_comma_produces_leading_empty(self) -> None:
        assert parse_token_list(",WETH") == ["", "WETH"]

    def test_hyphen_in_token_preserved(self) -> None:
        assert parse_token_list("usdc-e,usdt") == ["USDC-E", "USDT"]


class TestValidateStrategyIsRegisteredExtended:
    """Additional error-path tests."""

    def test_error_prefix_exact_string(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Grep-asserted string — the exact prefix must be preserved."""
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=[],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("nope")

        captured = capsys.readouterr()
        # Byte-for-byte match on the load-bearing line
        assert "Error: Strategy 'nope' is not registered." in captured.err

    def test_discovery_guidance_sequence(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify the full guidance block lines are emitted in order."""
        with patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=[],
        ):
            with pytest.raises(click.Abort):
                validate_strategy_is_registered("missing")

        err = capsys.readouterr().err
        # Ordered assertions
        i_header = err.find("The backtest command discovers strategies by:")
        i_import = err.find("Importing ./strategy.py")
        i_scan = err.find("Scanning ./strategies/")
        i_hint = err.find("See registered strategies with:")
        i_new = err.find("almanak strat new --name <name>")
        assert 0 <= i_header < i_import < i_scan < i_hint < i_new


class TestResolveStrategyClassOrMockExtended:
    """Additional edges for the resolver."""

    def test_mock_decide_accepts_any_market(self) -> None:
        """Mock.decide returns None regardless of input — covers stub branch."""
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError(),
        ):
            mock_cls: Any = resolve_strategy_class_or_mock("x", allow_mock=True)

        inst = mock_cls({})
        assert inst.decide(market="anything") is None  # type: ignore[arg-type]
        assert inst.decide(market=None) is None  # type: ignore[arg-type]

    def test_mock_class_attribute_accessible(self) -> None:
        """Class-level deployment_id is accessible before instantiation."""
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError(),
        ):
            mock_cls: Any = resolve_strategy_class_or_mock("x", allow_mock=True)

        assert mock_cls.deployment_id == "mock-sweep"

    def test_get_strategy_succeeds_ignores_allow_mock_false(self) -> None:
        """When get_strategy succeeds, allow_mock is irrelevant."""

        class _Real:
            pass

        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            return_value=_Real,
        ):
            assert resolve_strategy_class_or_mock("real", allow_mock=False) is _Real
            assert resolve_strategy_class_or_mock("real", allow_mock=True) is _Real

    def test_allow_mock_emits_blank_line_around_warnings(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Output structure: blank, warning1, warning2, blank."""
        with patch(
            "almanak.framework.cli.backtest.run_helpers.get_strategy",
            side_effect=ValueError(),
        ):
            resolve_strategy_class_or_mock("m", allow_mock=True)
        err = capsys.readouterr().err
        assert "Warning: No strategies registered in factory." in err
        assert "Running with mock strategy for demonstration." in err
