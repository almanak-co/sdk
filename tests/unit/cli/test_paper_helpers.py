"""Unit tests for `almanak.framework.cli.backtest.paper_helpers`.

Targets the helpers extracted from paper_start / paper_resume / paper_status
(VIB-4080 W3 Sub-D). Each test covers a distinct branch in the previously
un-tested inner logic of the three Click commands.
"""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli.backtest import helpers as cli_helpers
from almanak.framework.cli.backtest import paper_helpers as ph

paper_cli = importlib.import_module("almanak.framework.cli.backtest.paper")

# ---------------------------------------------------------------------------
# paper_start helpers (4 tests)
# ---------------------------------------------------------------------------


class TestPaperStartHelpers:
    def test_resolve_max_ticks_aborts_when_both_duration_and_max_ticks(self):
        with pytest.raises(click.Abort):
            ph.resolve_max_ticks_from_duration("5m", 100, 60)

    def test_resolve_max_ticks_converts_duration(self):
        # 5m / 60s + 1 = 6 ticks (first tick is immediate).
        assert ph.resolve_max_ticks_from_duration("5m", None, 60) == 6
        # No duration, no max_ticks -> None passes through.
        assert ph.resolve_max_ticks_from_duration(None, None, 60) is None

    def test_resolve_rpc_url_picks_chain_specific_env_var_then_aborts(self, monkeypatch):
        for var in ("ALMANAK_ARBITRUM_RPC_URL", "ARBITRUM_RPC_URL", "ALMANAK_RPC_URL", "RPC_URL"):
            monkeypatch.delenv(var, raising=False)
        # Aborts cleanly when nothing is set anywhere.
        with pytest.raises(click.Abort):
            ph.resolve_rpc_url(None, "arbitrum")
        # Chain-specific env var wins.
        monkeypatch.setenv("ALMANAK_ARBITRUM_RPC_URL", "https://env.example")
        assert ph.resolve_rpc_url(None, "arbitrum") == "https://env.example"
        # Explicit arg always wins.
        assert ph.resolve_rpc_url("https://cli.example", "arbitrum") == "https://cli.example"

    def test_parse_initial_tokens_arg_handles_empty_and_invalid(self):
        assert ph.parse_initial_tokens_arg("") == {}
        assert ph.parse_initial_tokens_arg(" USDC: 1000 , WETH:5 ") == {
            "USDC": Decimal("1000"),
            "WETH": Decimal("5"),
        }
        with pytest.raises(click.Abort):
            ph.parse_initial_tokens_arg("USDC1000")  # missing colon

    def test_apply_preset_yield_validation_is_refused_before_boot(self):
        from almanak.framework.backtesting.paper.config import PersistentForkOracleUnavailableError

        cfg = MagicMock()
        with pytest.raises(PersistentForkOracleUnavailableError, match="execution-validation"):
            ph.apply_preset(cfg, "yield-validation")

    @pytest.mark.parametrize(
        "unsupported_args",
        [
            ["--preset", "yield-validation"],
            ["--no-reset-fork"],
        ],
        ids=["yield-validation-preset", "legacy-no-reset-flag"],
    )
    @pytest.mark.parametrize(
        "execution_args",
        [[], ["--foreground"]],
        ids=["background", "foreground"],
    )
    def test_paper_start_refuses_unsupported_lifecycle_before_session_boot(
        self,
        unsupported_args: list[str],
        execution_args: list[str],
        tmp_path,
        monkeypatch,
    ):
        """The Click boundary must refuse both public persistent-mode spellings."""
        state_dir = tmp_path / "paper_sessions"
        monkeypatch.setattr(cli_helpers, "PAPER_STATE_DIR", state_dir)

        with (
            patch.object(paper_cli, "validate_strategy_registered"),
            patch.object(paper_cli, "abort_if_session_running"),
            patch.object(paper_cli, "load_funding_from_config", return_value=(None, {}, {}, {})),
            patch.object(
                paper_cli,
                "cli_runtime_config_from_env",
                return_value=SimpleNamespace(allow_hardcoded_prices=False),
            ),
            patch.object(paper_cli, "BackgroundPaperTrader") as background_trader,
            patch.object(paper_cli, "_run_paper_trading_foreground") as foreground_runner,
            patch.object(paper_cli, "save_paper_session_state") as save_session_state,
        ):
            result = CliRunner().invoke(
                paper_cli.paper_start,
                [
                    "--strategy",
                    "review-test",
                    "--rpc-url",
                    "https://rpc.example",
                    *execution_args,
                    *unsupported_args,
                ],
            )

        assert result.exit_code != 0
        assert "Configuration error:" in result.output
        assert "fork-bound gateway oracle" in result.output
        background_trader.assert_not_called()
        foreground_runner.assert_not_called()
        save_session_state.assert_not_called()
        assert not state_dir.exists()

    def test_parse_funding_dict_native_erc20_address_and_invalid(self, capsys):
        """`parse_funding_dict` collapses native tokens, checksums addresses, skips bad ones."""
        native = frozenset({"ETH", "AVAX"})

        # Native token is split out into eth_val.
        # ERC-20 by symbol passes through; ERC-20 by address is checksummed.
        eth_val, tokens = ph.parse_funding_dict(
            {
                "ETH": "1.5",
                "USDC": "1000",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": "500",  # USDC mainnet, lower-case last char
            },
            native,
            source="test",
        )
        assert eth_val == Decimal("1.5")
        assert tokens["USDC"] == Decimal("1000")
        # checksummed: address-by-len-42 path adds the canonical-cased entry.
        assert any(addr.lower() == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" for addr in tokens)
        for addr, amount in tokens.items():
            if addr.startswith("0x"):
                assert amount == Decimal("500")

        # Native lookup is case-insensitive.
        eth_val2, _ = ph.parse_funding_dict({"avax": "2"}, native, source="test")
        assert eth_val2 == Decimal("2")

        # Empty input: both slots empty.
        eth_empty, tokens_empty = ph.parse_funding_dict({}, native, source="test")
        assert eth_empty is None
        assert tokens_empty == {}

        # An invalid 0x… string (length 42 but bad chars) hits the except branch and prints a warning.
        bad_addr = "0x" + "Z" * 40
        _, tokens_bad = ph.parse_funding_dict({bad_addr: "1"}, native, source="cfg")
        # The bad address must NOT show up in the parsed tokens.
        assert bad_addr not in tokens_bad
        assert all(not k.startswith("0x") for k in tokens_bad) or tokens_bad == {}
        err = capsys.readouterr().err
        assert "Warning: ignoring invalid token address in cfg" in err

        # Non-string keys are coerced via str() and routed to the bare-symbol slot.
        _, tokens_int = ph.parse_funding_dict({123: "7"}, native, source="test")
        assert tokens_int == {"123": Decimal("7")}

    def test_load_funding_selects_nested_section_by_chain_alias(self):
        base_usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        strategy_config = {"anvil_funding": {"eip155:8453": {base_usdc: 500}}}

        with patch.object(ph, "load_strategy_config", return_value=strategy_config):
            config_eth, config_tokens, config_bootstrap, loaded = ph.load_funding_from_config("demo", "base")

        assert config_eth is None
        assert config_bootstrap == {}
        assert loaded is strategy_config
        assert {address.lower(): amount for address, amount in config_tokens.items()} == {base_usdc: Decimal("500")}

    def test_anvil_funding_requires_address_shaped_native_identity(self):
        from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

        eth_val, tokens = ph.parse_funding_dict(
            {NATIVE_SENTINEL: "250"},
            frozenset({NATIVE_SENTINEL}),
            source="anvil_funding",
            addresses_only=True,
        )
        assert eth_val == Decimal("250")
        assert tokens == {}

        with pytest.raises(ValueError, match="is not an address"):
            ph.parse_funding_dict(
                {"ETH": "250"},
                frozenset({NATIVE_SENTINEL}),
                source="anvil_funding",
                addresses_only=True,
            )

    def test_parse_funding_dict_aggregates_case_normalized_addresses(self):
        from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

        token_address = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        eth_val, tokens = ph.parse_funding_dict(
            {
                NATIVE_SENTINEL: "1.25",
                NATIVE_SENTINEL.lower(): "0.75",
                token_address: "100",
                token_address.upper(): "50",
            },
            frozenset({NATIVE_SENTINEL}),
            source="anvil_funding",
            addresses_only=True,
        )

        assert eth_val == Decimal("2")
        assert len(tokens) == 1
        assert next(iter(tokens.values())) == Decimal("150")


# ---------------------------------------------------------------------------
# paper_resume helpers (3 tests)
# ---------------------------------------------------------------------------


class TestPaperResumeHelpers:
    def test_compute_resume_max_ticks_branches(self):
        # Saved value passes through.
        assert ph.compute_resume_max_ticks(None, None, 1000, 50, 60) == 1000
        # Duration extends from current tick count.
        assert ph.compute_resume_max_ticks("60s", None, 1000, 10, 60) == 12
        # Explicit max_ticks replaces saved.
        assert ph.compute_resume_max_ticks(None, 5000, 1000, 100, 60) == 5000
        # Aborts if requested max-ticks is not above current count.
        with pytest.raises(click.Abort):
            ph.compute_resume_max_ticks(None, 100, 1000, 100, 60)

    def test_resolve_resume_rpc_url_falls_back_to_env_when_masked(self, monkeypatch):
        # Unmasked saved URL passes straight through.
        assert ph.resolve_resume_rpc_url("https://saved.example", "arbitrum") == "https://saved.example"
        # Masked URL ("***" sentinel) falls back to env var.
        for var in ("ALMANAK_ARBITRUM_RPC_URL", "ARBITRUM_RPC_URL", "ALMANAK_RPC_URL", "RPC_URL"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("ALMANAK_ARBITRUM_RPC_URL", "https://env.example")
        assert ph.resolve_resume_rpc_url("https://****", "arbitrum") == "https://env.example"
        # Aborts when masked and no env override exists.
        monkeypatch.delenv("ALMANAK_ARBITRUM_RPC_URL")
        with pytest.raises(click.Abort):
            ph.resolve_resume_rpc_url("https://****", "arbitrum")

    def test_build_resume_config_round_trips_decimals(self):
        saved = {
            "anvil_port": 8546,
            "reset_fork_every_tick": True,
            "initial_eth": "12.5",
            "initial_tokens": {"USDC": "1000"},
        }
        cfg = ph.build_resume_config(
            saved_config=saved,
            strategy="my_strat",
            chain="arbitrum",
            rpc_url="https://x",
            new_max_ticks=42,
            tick_interval=30,
        )
        assert cfg.deployment_id == "my_strat"
        assert cfg.tick_interval_seconds == 30
        assert cfg.max_ticks == 42
        assert cfg.initial_eth == Decimal("12.5")
        assert cfg.initial_tokens == {"USDC": Decimal("1000")}

    def test_build_resume_config_preserves_supported_saved_settings(self):
        from almanak.framework.backtesting.paper.config import ForkLifecycle

        saved = {
            "anvil_port": 8546,
            "reset_fork_every_tick": True,
            "initial_eth": "5.5",
            "initial_tokens": {"WETH": "1.25"},
            "bootstrap": {"arbitrum": {"USDC": "100", "WETH": 2}},
            "strict_bootstrap": True,
            "strict_price_mode": False,
            "fork_lifecycle": ForkLifecycle.ROLLING_RESET.value,
            "yield_poker_enabled": True,
            "use_rich_valuation": True,
            "position_reconciler_enabled": False,
            "position_reconciler_tolerance_pct": "0.025",
            "max_ticks": 10,
        }

        cfg = ph.build_resume_config(
            saved_config=saved,
            strategy="resumed",
            chain="arbitrum",
            rpc_url="https://x",
            new_max_ticks=999,
            tick_interval=15,
        )

        assert cfg.fork_lifecycle == ForkLifecycle.ROLLING_RESET
        assert cfg.strict_bootstrap is True
        assert cfg.strict_price_mode is False
        assert cfg.yield_poker_enabled is True
        assert cfg.use_rich_valuation is True
        assert cfg.position_reconciler_enabled is False
        assert cfg.bootstrap == {
            "arbitrum": {"USDC": Decimal("100"), "WETH": Decimal("2")},
        }
        assert cfg.position_reconciler_tolerance_pct == Decimal("0.025")
        assert cfg.max_ticks == 999

    def test_build_resume_config_refuses_saved_persistent_session(self):
        """Resume must refuse a saved mode that cannot satisfy the oracle boundary."""
        from almanak.framework.backtesting.paper.config import (
            ForkLifecycle,
            PersistentForkOracleUnavailableError,
        )

        saved = {
            "chain": "ethereum",  # overridden below
            "rpc_url": "***masked***",  # overridden below
            "deployment_id": "old",  # overridden below
            "anvil_port": 8546,
            "reset_fork_every_tick": False,
            "initial_eth": "5",
            "initial_tokens": {"WETH": "1"},
            "bootstrap": {"arbitrum": {"USDC": "100"}},
            "strict_bootstrap": True,
            "strict_price_mode": False,
            "fork_lifecycle": ForkLifecycle.PERSISTENT.value,
            "yield_poker_enabled": True,
            "use_rich_valuation": True,
            "position_reconciler_enabled": True,
            "log_level": "DEBUG",
            # An unknown / future field should be ignored, not crash __init__.
            "future_field": "ignored",
        }
        with pytest.raises(PersistentForkOracleUnavailableError, match="Cannot resume saved paper session"):
            ph.build_resume_config(
                saved_config=saved,
                strategy="resumed",
                chain="arbitrum",
                rpc_url="https://x",
                new_max_ticks=999,
                tick_interval=15,
            )


# ---------------------------------------------------------------------------
# paper_status helpers (4 tests)
# ---------------------------------------------------------------------------


class TestPaperStatusHelpers:
    def test_format_pid_status_dispatches_on_liveness(self):
        with patch.object(ph, "is_process_running", return_value=True):
            assert ph._format_pid_status(123, "stopped") == "running (PID: 123)"
        with patch.object(ph, "is_process_running", return_value=False):
            assert ph._format_pid_status(123, "stopped") == "stopped (process not found)"
        # No PID -> use fallback string.
        assert ph._format_pid_status(None, "completed") == "completed"

    def test_render_all_sessions_empty_verbose_and_non_verbose(self, capsys):
        # Empty listing.
        with patch.object(ph, "list_paper_sessions", return_value=[]):
            ph.render_all_sessions(verbose=False)
        assert "No paper trading sessions found" in capsys.readouterr().out

        sessions = [
            {
                "deployment_id": "s1",
                "status": "stopped",
                "pid": "N/A",
                "start_time": "2026-01-01T00:00:00",
                "config": {"chain": "arbitrum", "tick_interval_seconds": 60, "max_ticks": 100},
                "summary": {"successful_trades": 5, "failed_trades": 1},
            }
        ]
        # Verbose includes the per-session config block.
        with (
            patch.object(ph, "list_paper_sessions", return_value=sessions),
            patch.object(ph, "is_process_running", return_value=False),
        ):
            ph.render_all_sessions(verbose=True)
        out = capsys.readouterr().out
        assert "Strategy: s1" in out
        assert "Chain: arbitrum" in out
        assert "Trades: 5" in out
        assert "Errors: 1" in out

        # Non-verbose omits the config block.
        with (
            patch.object(ph, "list_paper_sessions", return_value=sessions),
            patch.object(ph, "is_process_running", return_value=False),
        ):
            ph.render_all_sessions(verbose=False)
        out = capsys.readouterr().out
        assert "Strategy: s1" in out
        assert "Chain: arbitrum" not in out
        assert "Trades: 5" not in out

    def test_render_bg_status_prints_running_block(self, capsys):
        from almanak.framework.backtesting.paper.background import BackgroundStatus

        bg = BackgroundStatus(
            is_running=True,
            pid=4242,
            deployment_id="s1",
            session_start=datetime(2026, 5, 6, 12, 0, 0, tzinfo=UTC),
            tick_count=10,
            trade_count=3,
            error_count=1,
            status="running",
            can_resume=True,
            resume_count=2,
        )
        ph.render_bg_status("s1", bg)
        out = capsys.readouterr().out
        assert "PAPER TRADING STATUS: s1" in out
        assert "Status: running (PID: 4242)" in out
        assert "Ticks: 10" in out
        assert "Can Resume: yes (resume_count: 2)" in out

    def test_render_single_session_status_falls_back_to_help_text(self, capsys):
        from almanak.framework.backtesting.paper.background import BackgroundStatus

        empty_bg = BackgroundStatus(is_running=False, tick_count=0)
        bg_trader = MagicMock()
        bg_trader.get_status.return_value = empty_bg
        with (
            patch.object(ph, "BackgroundPaperTrader", return_value=bg_trader),
            patch.object(ph, "load_paper_session_state", return_value=None),
        ):
            ph.render_single_session_status("missing")
        out = capsys.readouterr().out
        assert "No paper trading session found for 'missing'" in out
        assert "almanak strat backtest paper start -s missing" in out


class TestListPaperSessions:
    def test_missing_state_dir_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli_helpers, "PAPER_STATE_DIR", tmp_path / "missing")

        assert cli_helpers.list_paper_sessions() == []

    def test_lists_valid_sessions_marks_stale_and_skips_corrupt_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli_helpers, "PAPER_STATE_DIR", tmp_path)
        (tmp_path / "running.json").write_text(
            json.dumps({"deployment_id": "running", "pid": 111, "status": "running"})
        )
        (tmp_path / "stale.json").write_text(json.dumps({"deployment_id": "stale", "pid": 222, "status": "running"}))
        (tmp_path / "broken.json").write_text("{not-json")

        def fake_is_running(pid):
            return pid == 111

        monkeypatch.setattr(cli_helpers, "is_process_running", fake_is_running)

        sessions = sorted(cli_helpers.list_paper_sessions(), key=lambda item: item["deployment_id"])

        assert sessions == [
            {"deployment_id": "running", "pid": 111, "status": "running"},
            {"deployment_id": "stale", "pid": 222, "status": "stopped (process not found)"},
        ]
