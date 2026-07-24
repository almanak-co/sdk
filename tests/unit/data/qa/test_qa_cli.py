"""Tests for the ``qa-data`` CLI command module (``almanak.framework.data.qa.cli``).

Covers ``_run_single_test`` (per-category dispatch, plot/report generation,
pass/fail determination) and the ``qa_data`` click command end-to-end with the
runner faked — no CoinGecko, no chain reads, no plots.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from almanak.framework.data.qa.cli import (
    _create_test_only_runner,
    _run_single_test,
    qa_data,
)
from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.runner import QAReport


class _FakeResult:
    """Duck-typed category result (matches .passed/.token/.error reads)."""

    def __init__(self, passed: bool, token: str = "ETH", error: str | None = None) -> None:
        self.passed = passed
        self.token = token
        self.error = error


def _make_config(**overrides: object) -> QAConfig:
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


class _FakeRunner:
    """Duck-typed QARunner covering the surface _run_single_test touches."""

    def __init__(
        self,
        config: QAConfig | None = None,
        output_dir: Path | None = None,
        skip_plots: bool = True,
        results: list[Any] | None = None,
    ) -> None:
        self.config = config
        self.output_dir = output_dir
        self.skip_plots = skip_plots
        self.results = results if results is not None else [_FakeResult(True)]
        self.calls: list[str] = []

    async def _run_cex_spot(self):
        self.calls.append("cex_spot")
        return self.results, 1.25

    async def _run_dex_spot(self):
        self.calls.append("dex_spot")
        return self.results, 2.25

    async def _run_cex_historical(self):
        self.calls.append("cex_historical")
        return self.results, 3.25

    async def _run_dex_historical(self):
        self.calls.append("dex_historical")
        return self.results, 4.25

    async def _run_rsi(self):
        self.calls.append("rsi")
        return self.results, 5.25

    async def _generate_plots(self, report):
        self.calls.append("plots")

    def _generate_report(self, report):
        self.calls.append("report")
        return Path("/tmp/qa/report.md")

    async def run_all(self):
        self.calls.append("run_all")
        report = QAReport(config=self.config or _make_config())
        report.cex_spot_results = self.results
        report.report_path = Path("/tmp/qa/report.md")
        report.total_duration_seconds = 1.0
        report.passed = report.failed_tests == 0
        return report


class TestRunSingleTest:
    @pytest.mark.parametrize(
        ("test_name", "expected_call", "report_attr", "duration_key", "duration"),
        [
            ("cex_spot", "cex_spot", "cex_spot_results", "cex_spot", 1.25),
            ("dex_spot", "dex_spot", "dex_spot_results", "dex_spot", 2.25),
            ("cex_history", "cex_historical", "cex_historical_results", "cex_historical", 3.25),
            ("dex_history", "dex_historical", "dex_historical_results", "dex_historical", 4.25),
            ("rsi", "rsi", "rsi_results", "rsi", 5.25),
        ],
    )
    def test_each_category_dispatches_to_its_runner_method(
        self, test_name, expected_call, report_attr, duration_key, duration
    ) -> None:
        config = _make_config()
        runner = _FakeRunner(results=[_FakeResult(True), _FakeResult(True)])

        report = asyncio.run(_run_single_test(runner, test_name, config))

        assert expected_call in runner.calls
        assert getattr(report, report_attr) == runner.results
        assert report.durations[duration_key] == duration
        # Only the selected category ran.
        category_calls = [c for c in runner.calls if c not in ("plots", "report")]
        assert category_calls == [expected_call]
        assert report.report_path == Path("/tmp/qa/report.md")
        assert report.total_duration_seconds >= 0.0
        assert report.passed is True

    def test_failing_result_marks_report_failed(self) -> None:
        runner = _FakeRunner(results=[_FakeResult(True), _FakeResult(False, error="boom")])

        report = asyncio.run(_run_single_test(runner, "cex_spot", _make_config()))

        assert report.failed_tests == 1
        assert report.passed is False

    def test_skip_plots_false_generates_plots(self) -> None:
        runner = _FakeRunner(skip_plots=False)

        asyncio.run(_run_single_test(runner, "rsi", _make_config()))

        assert "plots" in runner.calls
        # Report generation always happens after (optional) plot generation.
        assert runner.calls.index("plots") < runner.calls.index("report")

    def test_skip_plots_true_skips_plot_generation(self) -> None:
        runner = _FakeRunner(skip_plots=True)

        asyncio.run(_run_single_test(runner, "rsi", _make_config()))

        assert "plots" not in runner.calls
        assert "report" in runner.calls

    def test_unknown_test_name_runs_no_category_and_passes(self) -> None:
        runner = _FakeRunner()

        report = asyncio.run(_run_single_test(runner, "nonsense", _make_config()))

        assert [c for c in runner.calls if c not in ("plots", "report")] == []
        assert report.total_tests == 0
        assert report.passed is True


class TestCreateTestOnlyRunner:
    @pytest.mark.parametrize(
        ("test_name", "display"),
        [
            ("cex_spot", "CEX Spot Prices"),
            ("dex_spot", "DEX Spot Prices"),
            ("cex_history", "CEX Historical"),
            ("dex_history", "DEX Historical"),
            ("rsi", "RSI Indicators"),
        ],
    )
    def test_maps_test_names_to_display_names(self, test_name, display, tmp_path) -> None:
        runner, name = _create_test_only_runner(_make_config(), tmp_path, test_name, True)
        assert name == display
        assert runner.skip_plots is True

    def test_unknown_test_name_falls_back_to_raw_name(self, tmp_path) -> None:
        _, name = _create_test_only_runner(_make_config(), tmp_path, "mystery", False)
        assert name == "mystery"


@pytest.fixture
def cli_env(monkeypatch):
    """Patch qa_data's collaborators: dotenv, logging, config load, runner."""
    monkeypatch.setattr("almanak.framework.data.qa.cli._load_dotenv_once", lambda: None)
    monkeypatch.setattr("almanak.framework.data.qa.cli.configure_logging", lambda verbose: None)

    state: dict[str, Any] = {"results": [_FakeResult(True)], "runners": []}

    def _load(config_file):
        state["config_file"] = config_file
        return _make_config()

    monkeypatch.setattr("almanak.framework.data.qa.cli.load_qa_config_or_exit", _load)

    class _CliRunner(_FakeRunner):
        def __init__(self, config=None, output_dir=None, skip_plots=False):
            super().__init__(
                config=config,
                output_dir=output_dir,
                skip_plots=skip_plots,
                results=state["results"],
            )
            state["runners"].append(self)

    monkeypatch.setattr("almanak.framework.data.qa.cli.QARunner", _CliRunner)
    return state


class TestQaDataCommand:
    def test_full_suite_pass_exits_0(self, cli_env, tmp_path) -> None:
        out = tmp_path / "qa-out"
        runner = CliRunner()
        result = runner.invoke(qa_data, ["--output", str(out), "--skip-plots"])

        assert result.exit_code == 0, result.output
        assert "Running all QA tests..." in result.output
        assert "QA TEST SUMMARY" in result.output
        assert "CEX Spot Prices:    1/1 [PASS]" in result.output
        assert "Total:              1/1" in result.output
        assert "OVERALL: PASSED" in result.output
        assert out.is_dir()
        (fake_runner,) = cli_env["runners"]
        assert fake_runner.calls == ["run_all"]
        assert fake_runner.skip_plots is True
        assert fake_runner.output_dir == out

    def test_full_suite_failure_exits_1_with_failure_details(self, cli_env, tmp_path) -> None:
        cli_env["results"] = [
            _FakeResult(True),
            _FakeResult(False, token="WBTC", error="price divergence"),
            _FakeResult(False, token="LINK", error=None),
        ]
        runner = CliRunner()
        result = runner.invoke(qa_data, ["--output", str(tmp_path / "o")])

        assert result.exit_code == 1
        assert "OVERALL: FAILED" in result.output
        assert "Failed tests:" in result.output
        assert "  - CEX Spot WBTC: price divergence" in result.output
        assert "  - CEX Spot LINK: validation failed" in result.output
        assert "CEX Spot Prices:    1/3 [FAIL]" in result.output

    def test_single_test_runs_selected_category_only(self, cli_env, tmp_path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            qa_data,
            ["--test", "dex_spot", "--output", str(tmp_path / "o"), "--skip-plots"],
        )

        assert result.exit_code == 0, result.output
        assert "Running DEX Spot Prices tests..." in result.output
        assert "Running test: dex_spot" in result.output
        (fake_runner,) = cli_env["runners"]
        assert "dex_spot" in fake_runner.calls
        assert "run_all" not in fake_runner.calls
        assert "DEX Spot Prices:    1/1 [PASS]" in result.output

    def test_chain_and_days_overrides_reach_banner(self, cli_env, tmp_path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            qa_data,
            ["--chain", "base", "--days", "14", "--output", str(tmp_path / "o")],
        )

        assert result.exit_code == 0, result.output
        assert "Chain: base" in result.output
        assert "Historical days: 14" in result.output

    def test_config_file_option_is_forwarded(self, cli_env, tmp_path) -> None:
        cfg = tmp_path / "custom.yaml"
        cfg.write_text("chain: arbitrum\n")
        runner = CliRunner()
        result = runner.invoke(
            qa_data,
            ["--config", str(cfg), "--output", str(tmp_path / "o")],
        )

        assert result.exit_code == 0, result.output
        assert cli_env["config_file"] == str(cfg)

    def test_invalid_test_name_rejected_by_click(self, cli_env) -> None:
        runner = CliRunner()
        result = runner.invoke(qa_data, ["--test", "unknown_test"])

        assert result.exit_code != 0
        assert "Invalid value" in result.output
