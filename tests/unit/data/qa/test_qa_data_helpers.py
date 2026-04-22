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
    echo_category_failures,
    load_qa_config_or_exit,
    print_startup_banner,
    summarize_category,
)
from almanak.framework.data.qa.config import QAConfig, QAThresholds


class _FakeResult:
    """Lightweight stand-in for a category result used by the summary/failure helpers.

    Matches the ``.passed`` / ``.token`` / ``.error`` surface that the inline
    ``qa_data`` blocks read. We deliberately avoid constructing real
    ``CEXSpotResult`` / ``DEXSpotResult`` etc. instances so the helper tests
    stay decoupled from the runner's result dataclasses.
    """

    def __init__(self, passed: bool, token: str = "T", error: str | None = None) -> None:
        self.passed = passed
        self.token = token
        self.error = error


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


# =============================================================================
# summarize_category (Phase 6.2)
# =============================================================================


def _invoke_summary(results: list[_FakeResult], label: str) -> str:
    """Invoke ``summarize_category`` via a throwaway Click command for capture."""
    runner = CliRunner()

    @click.command()
    def _cmd() -> None:
        summarize_category(results, label)

    result = runner.invoke(_cmd)
    assert result.exit_code == 0
    return result.output


class TestSummarizeCategory:
    def test_empty_results_is_noop(self) -> None:
        """Guarded ``if report.<category>_results:`` original => no output."""
        out = _invoke_summary([], "CEX Spot Prices:    ")
        assert out == ""

    def test_all_pass_emits_pass_status(self) -> None:
        results = [_FakeResult(passed=True), _FakeResult(passed=True)]
        out = _invoke_summary(results, "CEX Spot Prices:    ")
        assert out == "CEX Spot Prices:    2/2 [PASS]\n"

    def test_all_fail_emits_fail_status(self) -> None:
        results = [_FakeResult(passed=False), _FakeResult(passed=False)]
        out = _invoke_summary(results, "DEX Historical:     ")
        assert out == "DEX Historical:     0/2 [FAIL]\n"

    def test_mixed_pass_fail_emits_fail_status(self) -> None:
        """Any failing result flips the status to FAIL, not PARTIAL."""
        results = [
            _FakeResult(passed=True),
            _FakeResult(passed=False),
            _FakeResult(passed=True),
        ]
        out = _invoke_summary(results, "RSI Indicators:     ")
        assert out == "RSI Indicators:     2/3 [FAIL]\n"

    def test_single_passing_result(self) -> None:
        out = _invoke_summary([_FakeResult(passed=True)], "CEX Historical:     ")
        assert out == "CEX Historical:     1/1 [PASS]\n"

    def test_label_is_written_verbatim(self) -> None:
        """Alignment / padding is caller-owned -- we don't trim or re-pad."""
        out = _invoke_summary([_FakeResult(passed=True)], "X")
        assert out == "X1/1 [PASS]\n"

    @pytest.mark.parametrize(
        "label",
        [
            "CEX Spot Prices:    ",
            "DEX Spot Prices:    ",
            "CEX Historical:     ",
            "DEX Historical:     ",
            "RSI Indicators:     ",
        ],
    )
    def test_canonical_labels_are_20_chars_wide(self, label: str) -> None:
        """All five caller-supplied labels must share the 20-char column width
        so the summary block in ``qa_data`` renders as a clean aligned table."""
        assert len(label) == 20


# =============================================================================
# echo_category_failures (Phase 6.3)
# =============================================================================


def _invoke_failures(results: list[_FakeResult], label: str) -> str:
    """Invoke ``echo_category_failures`` via a throwaway Click command."""
    runner = CliRunner()

    @click.command()
    def _cmd() -> None:
        echo_category_failures(results, label)

    result = runner.invoke(_cmd)
    assert result.exit_code == 0
    return result.output


class TestEchoCategoryFailures:
    def test_empty_results_is_noop(self) -> None:
        assert _invoke_failures([], "CEX Spot") == ""

    def test_all_pass_is_noop(self) -> None:
        """If every result passed, the Phase G loop has nothing to echo."""
        results = [
            _FakeResult(passed=True, token="ETH"),
            _FakeResult(passed=True, token="WBTC"),
        ]
        assert _invoke_failures(results, "CEX Spot") == ""

    def test_single_failure_uses_error_message(self) -> None:
        results = [_FakeResult(passed=False, token="ETH", error="price mismatch")]
        out = _invoke_failures(results, "CEX Spot")
        assert out == "  - CEX Spot ETH: price mismatch\n"

    def test_failure_without_error_falls_back_to_validation_failed(self) -> None:
        """``None`` / empty error => ``'validation failed'`` sentinel."""
        results = [_FakeResult(passed=False, token="WBTC", error=None)]
        out = _invoke_failures(results, "DEX Spot")
        assert out == "  - DEX Spot WBTC: validation failed\n"

    def test_empty_string_error_falls_back_to_validation_failed(self) -> None:
        """``""`` is falsy in the original inline expression; mirror that."""
        results = [_FakeResult(passed=False, token="LINK", error="")]
        out = _invoke_failures(results, "RSI")
        assert out == "  - RSI LINK: validation failed\n"

    def test_mixed_results_only_failures_are_echoed(self) -> None:
        """Passing rows are silently skipped; failures preserve input order."""
        results = [
            _FakeResult(passed=True, token="ETH"),
            _FakeResult(passed=False, token="WBTC", error="boom"),
            _FakeResult(passed=True, token="LINK"),
            _FakeResult(passed=False, token="UNI", error=None),
        ]
        out = _invoke_failures(results, "CEX Historical")
        assert out == (
            "  - CEX Historical WBTC: boom\n"
            "  - CEX Historical UNI: validation failed\n"
        )

    def test_multiple_failures_preserve_iteration_order(self) -> None:
        """Operator log readability depends on stable category iteration order."""
        results = [
            _FakeResult(passed=False, token="A", error="e1"),
            _FakeResult(passed=False, token="B", error="e2"),
            _FakeResult(passed=False, token="C", error="e3"),
        ]
        out = _invoke_failures(results, "DEX Historical")
        assert out == (
            "  - DEX Historical A: e1\n"
            "  - DEX Historical B: e2\n"
            "  - DEX Historical C: e3\n"
        )

    @pytest.mark.parametrize(
        "label",
        ["CEX Spot", "DEX Spot", "CEX Historical", "DEX Historical", "RSI"],
    )
    def test_all_canonical_labels_round_trip(self, label: str) -> None:
        """Each of the 5 category labels must render in the fixed line format."""
        results = [_FakeResult(passed=False, token="X", error="why")]
        out = _invoke_failures(results, label)
        assert out == f"  - {label} X: why\n"
