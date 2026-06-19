"""Unit tests for `almanak.framework.cli.backtest.paper_helpers`.

Targets the helpers extracted from paper_start / paper_resume / paper_status
(VIB-4080 W3 Sub-D). Each test covers a distinct branch in the previously
un-tested inner logic of the three Click commands.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import click
import pytest

from almanak.framework.cli.backtest import helpers as cli_helpers
from almanak.framework.cli.backtest import paper_helpers as ph

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

    def test_apply_preset_yield_validation_mutates_config(self):
        from almanak.framework.backtesting.paper.config import ForkLifecycle

        cfg = MagicMock()
        ph.apply_preset(cfg, "yield-validation")
        assert cfg.fork_lifecycle == ForkLifecycle.PERSISTENT
        assert cfg.reset_fork_every_tick is False
        assert cfg.yield_poker_enabled is True
        assert cfg.use_rich_valuation is True
        assert cfg.position_reconciler_enabled is True

    def test_parse_funding_dict_native_erc20_address_and_invalid(self, capsys):
        """`parse_funding_dict` collapses native tokens, checksums addresses, skips bad ones."""
        native = frozenset({"ETH", "AVAX"})

        # Native token is split out into eth_val (overwritten by last hit per current shape).
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

    def test_build_resume_config_preserves_preset_flags(self):
        """Resume must restore preset-driven flags rather than silently dropping them."""
        from almanak.framework.backtesting.paper.config import ForkLifecycle

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
        cfg = ph.build_resume_config(
            saved_config=saved,
            strategy="resumed",
            chain="arbitrum",
            rpc_url="https://x",
            new_max_ticks=999,
            tick_interval=15,
        )

        # Resume overrides applied
        assert cfg.deployment_id == "resumed"
        assert cfg.chain == "arbitrum"
        assert cfg.rpc_url == "https://x"
        assert cfg.tick_interval_seconds == 15
        assert cfg.max_ticks == 999

        # Saved preset flags preserved
        assert cfg.strict_bootstrap is True
        assert cfg.strict_price_mode is False
        assert cfg.fork_lifecycle == ForkLifecycle.PERSISTENT
        assert cfg.yield_poker_enabled is True
        assert cfg.use_rich_valuation is True
        assert cfg.position_reconciler_enabled is True
        assert cfg.log_level == "DEBUG"
        assert cfg.bootstrap == {"arbitrum": {"USDC": Decimal("100")}}


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
        (tmp_path / "running.json").write_text(json.dumps({"deployment_id": "running", "pid": 111, "status": "running"}))
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
