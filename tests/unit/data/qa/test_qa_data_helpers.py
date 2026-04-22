"""Unit tests for ``almanak.framework.data.qa.cli_helpers``.

Phase 6 extraction: ``qa_data`` grew to CC 47. The first PR pulls the
environment bootstrap, config loading, CLI override, and banner-print
phases onto small, side-effect-compatible helpers. These tests pin the
behavioural contract so subsequent PRs can keep carving at the
``qa_data`` body without regressing observable behaviour.

Focus areas:

- ``configure_logging``: level selection is driven by ``verbose``.
- ``load_qa_config_or_exit``: both echo paths, both exit-1 paths.
- ``apply_cli_overrides``: chain/days precedence, no-op case.
- ``print_startup_banner``: byte-for-byte output + single-vs-all branch.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from almanak.framework.data.qa.cli_helpers import (
    apply_cli_overrides,
    configure_logging,
    load_qa_config_or_exit,
    print_startup_banner,
)
from almanak.framework.data.qa.config import QAConfig, QAThresholds


def _make_config(**overrides: object) -> QAConfig:
    """Construct a fully-populated ``QAConfig`` for override tests."""
    defaults: dict[str, object] = {
        "chain": "arbitrum",
        "historical_days": 30,
        "timeframe": "4h",
        "rsi_period": 14,
        "thresholds": QAThresholds(),
        "popular_tokens": ["ETH", "WBTC"],
        "additional_tokens": ["LINK"],
        "dex_tokens": ["USDC"],
    }
    defaults.update(overrides)
    return QAConfig(**defaults)  # type: ignore[arg-type]


# =============================================================================
# configure_logging
# =============================================================================


class TestConfigureLogging:
    def test_verbose_sets_debug_level(self) -> None:
        with patch("almanak.framework.data.qa.cli_helpers.logging.basicConfig") as bc:
            configure_logging(verbose=True)
        bc.assert_called_once()
        kwargs = bc.call_args.kwargs
        assert kwargs["level"] == logging.DEBUG

    def test_non_verbose_sets_info_level(self) -> None:
        with patch("almanak.framework.data.qa.cli_helpers.logging.basicConfig") as bc:
            configure_logging(verbose=False)
        bc.assert_called_once()
        kwargs = bc.call_args.kwargs
        assert kwargs["level"] == logging.INFO

    def test_preserves_format_and_datefmt(self) -> None:
        """Format strings are load-bearing for operator log scraping."""
        with patch("almanak.framework.data.qa.cli_helpers.logging.basicConfig") as bc:
            configure_logging(verbose=False)
        kwargs = bc.call_args.kwargs
        assert kwargs["format"] == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        assert kwargs["datefmt"] == "%Y-%m-%d %H:%M:%S"


# =============================================================================
# load_qa_config_or_exit
# =============================================================================


class TestLoadQAConfigOrExit:
    def test_no_path_uses_default_loader_and_echoes_default(self) -> None:
        sentinel = _make_config()
        runner = CliRunner()

        @click.command()
        def _cmd() -> None:
            load_qa_config_or_exit(None)

        with patch(
            "almanak.framework.data.qa.cli_helpers.load_config",
            return_value=sentinel,
        ) as lc:
            result = runner.invoke(_cmd)

        lc.assert_called_once_with()
        assert result.exit_code == 0
        assert "Loaded default config" in result.output

    def test_explicit_path_forwards_and_echoes_path(self) -> None:
        sentinel = _make_config()
        runner = CliRunner()

        @click.command()
        def _cmd() -> None:
            load_qa_config_or_exit("my.yaml")

        with patch(
            "almanak.framework.data.qa.cli_helpers.load_config",
            return_value=sentinel,
        ) as lc:
            result = runner.invoke(_cmd)

        lc.assert_called_once_with("my.yaml")
        assert result.exit_code == 0
        assert "Loaded config from: my.yaml" in result.output

    def test_file_not_found_exits_1_with_error_prefix(self) -> None:
        # mix_stderr=False keeps stderr separate so we can assert on it.
        runner = CliRunner(mix_stderr=False)

        @click.command()
        def _cmd() -> None:
            load_qa_config_or_exit("missing.yaml")

        with patch(
            "almanak.framework.data.qa.cli_helpers.load_config",
            side_effect=FileNotFoundError("no such file: missing.yaml"),
        ):
            result = runner.invoke(_cmd)

        assert result.exit_code == 1
        assert "Error: no such file: missing.yaml" in result.stderr

    def test_value_error_exits_1_with_invalid_prefix(self) -> None:
        runner = CliRunner(mix_stderr=False)

        @click.command()
        def _cmd() -> None:
            load_qa_config_or_exit("bad.yaml")

        with patch(
            "almanak.framework.data.qa.cli_helpers.load_config",
            side_effect=ValueError("bad shape"),
        ):
            result = runner.invoke(_cmd)

        assert result.exit_code == 1
        assert "Invalid config: bad shape" in result.stderr


# =============================================================================
# apply_cli_overrides
# =============================================================================


class TestApplyCliOverrides:
    def test_no_overrides_returns_same_object(self) -> None:
        base = _make_config()
        # When neither override is set, the original instance must pass
        # through untouched: downstream code may rely on identity.
        assert apply_cli_overrides(base, chain=None, days=None) is base

    def test_chain_override_rebuilds_with_new_chain_and_keeps_days(self) -> None:
        base = _make_config(chain="arbitrum", historical_days=30)
        out = apply_cli_overrides(base, chain="base", days=None)
        assert out is not base
        assert out.chain == "base"
        assert out.historical_days == 30  # kept from base
        # Other fields preserved
        assert out.timeframe == base.timeframe
        assert out.popular_tokens == base.popular_tokens
        assert out.dex_tokens == base.dex_tokens
        assert out.thresholds is base.thresholds

    def test_chain_and_days_override_applies_both(self) -> None:
        base = _make_config(chain="arbitrum", historical_days=30)
        out = apply_cli_overrides(base, chain="ethereum", days=14)
        assert out.chain == "ethereum"
        assert out.historical_days == 14

    def test_days_override_without_chain_keeps_chain(self) -> None:
        base = _make_config(chain="arbitrum", historical_days=30)
        out = apply_cli_overrides(base, chain=None, days=7)
        assert out is not base
        assert out.chain == "arbitrum"
        assert out.historical_days == 7

    def test_days_zero_is_treated_as_unset(self) -> None:
        """Mirrors the original ``elif days:`` truthiness: 0 means no override."""
        base = _make_config(historical_days=30)
        out = apply_cli_overrides(base, chain=None, days=0)
        assert out is base


# =============================================================================
# print_startup_banner
# =============================================================================


class TestPrintStartupBanner:
    def _invoke_banner(
        self,
        *,
        test_name: str | None,
        skip_plots: bool = False,
    ) -> str:
        """Render the banner via a throwaway Click command for output capture."""
        config = _make_config()
        runner = CliRunner()

        @click.command()
        def _cmd() -> None:
            print_startup_banner(
                config,
                Path("reports/qa-data"),
                skip_plots=skip_plots,
                test_name=test_name,
            )

        result = runner.invoke(_cmd)
        assert result.exit_code == 0
        return result.output

    def test_banner_includes_framework_header_and_separator(self) -> None:
        out = self._invoke_banner(test_name=None)
        assert "ALMANAK DATA QA FRAMEWORK" in out
        assert "=" * 60 in out

    def test_banner_includes_all_config_lines(self) -> None:
        out = self._invoke_banner(test_name=None)
        # Spot-check every line that the banner prints for config fields.
        assert "Chain: arbitrum" in out
        assert "Historical days: 30" in out
        assert "Timeframe: 4h" in out
        assert "RSI period: 14" in out
        assert "Popular tokens: ETH, WBTC" in out
        assert "Additional tokens: LINK" in out
        assert "DEX tokens: USDC" in out
        assert "Output: reports/qa-data" in out
        assert "Skip plots: False" in out

    def test_banner_all_tests_branch(self) -> None:
        out = self._invoke_banner(test_name=None)
        assert "Running: All tests" in out
        assert "Running test:" not in out

    def test_banner_single_test_branch(self) -> None:
        out = self._invoke_banner(test_name="rsi")
        assert "Running test: rsi" in out
        assert "Running: All tests" not in out

    def test_banner_reports_skip_plots_true(self) -> None:
        out = self._invoke_banner(test_name=None, skip_plots=True)
        assert "Skip plots: True" in out


# =============================================================================
# Regression: helpers cover the docstring-quoted echoes verbatim
# =============================================================================


@pytest.mark.parametrize(
    ("config_file", "expected"),
    [
        (None, "Loaded default config"),
        ("x.yaml", "Loaded config from: x.yaml"),
    ],
)
def test_load_qa_config_or_exit_echo_strings_verbatim(
    config_file: str | None,
    expected: str,
) -> None:
    """Pin the echo strings that downstream operator tooling greps on."""
    runner = CliRunner()

    @click.command()
    def _cmd() -> None:
        load_qa_config_or_exit(config_file)

    with patch(
        "almanak.framework.data.qa.cli_helpers.load_config",
        return_value=_make_config(),
    ):
        result = runner.invoke(_cmd)

    assert expected in result.output
