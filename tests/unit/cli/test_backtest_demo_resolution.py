"""Tests for the backtest CLI demo resolution lane.

``resolve_backtest_strategy_name`` closes the lane disagreement where
``load_strategy_config`` resolved ``almanak/demo_strategies/<name>/config.json``
from the repo root while strategy auto-discovery (cwd + ``./strategies/``,
VIB-2917) never registered the demo's class — so ``backtest pnl -s <demo>``
failed with "not registered" unless run from inside the demo directory.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.cli.backtest.helpers import resolve_backtest_strategy_name
from almanak.framework.strategies import STRATEGY_REGISTRY, unregister_strategy


class TestResolveBacktestStrategyName:
    def test_registered_name_returned_unchanged_without_demo_lookup(self) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.helpers.list_strategies_fn",
                return_value=["my_strategy"],
            ),
            patch("almanak.framework.demos.register_demo_strategy") as register,
        ):
            assert resolve_backtest_strategy_name("my_strategy") == "my_strategy"
        register.assert_not_called()

    def test_miss_with_no_demo_match_passes_through(self) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.helpers.list_strategies_fn",
                return_value=["other"],
            ),
            patch("almanak.framework.demos.register_demo_strategy", return_value=None),
        ):
            assert resolve_backtest_strategy_name("unknown") == "unknown"

    def test_demo_match_returns_canonical_name_and_echoes(self, capsys: pytest.CaptureFixture[str]) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.helpers.list_strategies_fn",
                return_value=["demo_spark_lender"],
            ),
            patch(
                "almanak.framework.demos.register_demo_strategy",
                return_value="demo_spark_lender",
            ),
        ):
            resolved = resolve_backtest_strategy_name("spark_lender")

        assert resolved == "demo_spark_lender"
        out = capsys.readouterr().out
        assert "Resolved demo strategy 'spark_lender' as 'demo_spark_lender'" in out

    def test_demo_match_with_same_name_does_not_echo(self, capsys: pytest.CaptureFixture[str]) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.helpers.list_strategies_fn",
                return_value=["gmx_perp_lifecycle"],
            ),
            patch(
                "almanak.framework.demos.register_demo_strategy",
                return_value="gmx_perp_lifecycle",
            ),
        ):
            resolved = resolve_backtest_strategy_name("gmx_perp_lifecycle")

        assert resolved == "gmx_perp_lifecycle"
        assert "Resolved demo strategy" not in capsys.readouterr().out

    def test_unregistered_canonical_name_passes_through(self) -> None:
        # Defensive: the demo claimed to register a name the registry does not
        # hold (e.g. shadowed registration) — fall back to the caller's name so
        # its validation path reports the miss.
        with (
            patch(
                "almanak.framework.cli.backtest.helpers.list_strategies_fn",
                return_value=[],
            ),
            patch(
                "almanak.framework.demos.register_demo_strategy",
                return_value="phantom",
            ),
        ):
            assert resolve_backtest_strategy_name("spark_lender") == "spark_lender"


class TestResolveBacktestStrategyNameIntegration:
    """End-to-end through a real (tmp) demos root, no mocks on the lane itself."""

    STRATEGY_SOURCE = textwrap.dedent(
        '''
        """Fixture demo used by test_backtest_demo_resolution.py."""
        from almanak.framework.intents import Intent
        from almanak.framework.strategies import IntentStrategy, almanak_strategy


        @almanak_strategy(name="demo_cli_lane_fixture", supported_chains=["arbitrum"], default_chain="arbitrum")
        class CliLaneFixtureStrategy(IntentStrategy):
            def decide(self, market):
                return Intent.hold(reason="fixture")
        '''
    ).strip()

    def test_resolves_registers_and_canonicalizes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        demo_dir = tmp_path / "cli_lane_fixture"
        demo_dir.mkdir()
        (demo_dir / "strategy.py").write_text(self.STRATEGY_SOURCE)
        monkeypatch.setattr(
            "almanak.framework.demos.spec.default_demos_root",
            lambda: tmp_path,
        )

        try:
            assert "demo_cli_lane_fixture" not in STRATEGY_REGISTRY
            resolved = resolve_backtest_strategy_name("cli_lane_fixture")
            assert resolved == "demo_cli_lane_fixture"
            assert resolved in STRATEGY_REGISTRY
        finally:
            if "demo_cli_lane_fixture" in STRATEGY_REGISTRY:
                unregister_strategy("demo_cli_lane_fixture")
            for mod in [m for m in sys.modules if m.startswith("_almanak_demo_strategy_")]:
                sys.modules.pop(mod, None)
