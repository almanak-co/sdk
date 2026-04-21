"""Extended unit tests for `almanak/framework/cli/run_helpers.py` Phase 4a helpers.

Closes coverage gaps in:
    - _discover_and_load_config (load error path, copy override error branches)
    - _handle_list_all (get_strategy exception fallback)
    - _load_strategy_class (load error path)

Pattern mirrors `test_run_helpers_setup.py`: CliRunner + monkeypatch, no
gateway / Anvil startup.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers

# ---------------------------------------------------------------------------
# _discover_and_load_config — error + edge paths (lines 279-281 and copy overrides)
# ---------------------------------------------------------------------------


class _DummyStrategyClass:
    """Minimal strategy class stand-in; only __name__ is used."""


class TestDiscoverAndLoadConfigErrors:
    def test_load_strategy_config_raising_exits_with_code_1(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Covers the `except Exception as e: sys.exit(1)` block (279-281)."""
        from almanak.framework.cli import run as run_mod

        def _raises(_name: str, _path: str | None) -> dict[str, Any]:
            raise RuntimeError("bad config file")

        monkeypatch.setattr(run_mod, "load_strategy_config", _raises)
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=_DummyStrategyClass,
                copy_mode=None,
                copy_shadow=False,
                copy_replay_file=None,
                copy_strict=False,
                dry_run=False,
            )
        assert exc_info.value.code == 1
        err_text = err.getvalue().decode()
        assert "Error loading strategy config" in err_text
        assert "bad config file" in err_text

    def test_copy_trading_non_dict_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Non-dict copy_trading config with override flags raises ClickException."""
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(
            run_mod, "load_strategy_config", lambda _n, _p: {"copy_trading": "not-a-dict"}
        )
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)

        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="copy_trading config must be an object"):
            run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=_DummyStrategyClass,
                copy_mode="live",
                copy_shadow=False,
                copy_replay_file=None,
                copy_strict=False,
                dry_run=False,
            )

    def test_copy_trading_execution_policy_non_dict_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Non-dict copy_trading.execution_policy raises ClickException."""
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(
            run_mod,
            "load_strategy_config",
            lambda _n, _p: {"copy_trading": {"execution_policy": ["list-not-dict"]}},
        )
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)

        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="execution_policy must be an object"):
            run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=_DummyStrategyClass,
                copy_mode=None,
                copy_shadow=False,
                copy_replay_file=None,
                copy_strict=True,
                dry_run=False,
            )

    def test_all_copy_flags_merged_into_execution_policy(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Every CLI override flag writes into execution_policy."""
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(run_mod, "load_strategy_config", lambda _n, _p: {})
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)

        cli = CliRunner()
        with cli.isolation():
            config, multi_chain, dry_run, _resolved, norm = run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=_DummyStrategyClass,
                copy_mode="Live",
                copy_shadow=True,
                copy_replay_file="/tmp/replay.jsonl",
                copy_strict=True,
                dry_run=False,
            )
        ep = config["copy_trading"]["execution_policy"]
        # copy_replay_file wins last -> copy_mode="replay"
        assert ep["copy_mode"] == "replay"
        assert ep["shadow"] is True
        assert ep["replay_file"] == "/tmp/replay.jsonl"
        assert ep["strict"] is True
        assert norm == "live"
        # copy_shadow OR copy_replay_file -> effective_dry_run True
        assert dry_run is True
        assert multi_chain is False

    def test_effective_dry_run_respects_copy_mode_shadow(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """copy_mode=shadow (without explicit --copy-shadow) triggers dry-run."""
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(run_mod, "load_strategy_config", lambda _n, _p: {})
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)

        cli = CliRunner()
        with cli.isolation():
            _cfg, _mc, dry_run, _rc, _norm = run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=_DummyStrategyClass,
                copy_mode="shadow",
                copy_shadow=False,
                copy_replay_file=None,
                copy_strict=False,
                dry_run=False,
            )
        assert dry_run is True

    def test_auto_discover_prefers_json_over_yaml(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When both config.json and config.yaml exist, json wins (discovery order)."""
        (tmp_path / "config.json").write_text("{}")
        (tmp_path / "config.yaml").write_text("chain: base\n")
        from almanak.framework.cli import run as run_mod

        captured: dict[str, Any] = {}

        def _capture(_name: str, path: str | None) -> dict[str, Any]:
            captured["path"] = path
            return {}

        monkeypatch.setattr(run_mod, "load_strategy_config", _capture)
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=_DummyStrategyClass,
                copy_mode=None,
                copy_shadow=False,
                copy_replay_file=None,
                copy_strict=False,
                dry_run=False,
            )
        assert captured["path"].endswith("config.json")


# ---------------------------------------------------------------------------
# _handle_list_all — get_strategy exception fallback
# ---------------------------------------------------------------------------


class TestHandleListAllExceptions:
    def test_get_strategy_raises_falls_back_to_plain_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If get_strategy() raises, helper still prints the name (uncovered fallback)."""
        from almanak.framework import strategies as strategies_pkg
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(strategies_pkg, "list_strategies", lambda: ["busted_strategy"])

        def _boom(_name: str) -> Any:
            raise RuntimeError("loader failure")

        monkeypatch.setattr(strategies_pkg, "get_strategy", _boom)
        # The other helpers can return defaults; they are unused when get_strategy raises.
        monkeypatch.setattr(run_mod, "is_multi_chain_strategy", lambda *_a, **_kw: False)
        monkeypatch.setattr(run_mod, "get_strategy_chains", lambda _c: [])

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            handled = run_helpers._handle_list_all(True, gateway_client=None)
        assert handled is True
        out_text = out.getvalue().decode()
        # Plain name fallback (no [multi-chain: ...] bracket)
        assert "  - busted_strategy" in out_text
        assert "multi-chain" not in out_text

    def test_no_strategies_prints_empty_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covers the `else` branch when list_strategies() returns empty."""
        from almanak.framework import strategies as strategies_pkg

        monkeypatch.setattr(strategies_pkg, "list_strategies", lambda: [])
        cli = CliRunner()
        with cli.isolation() as (out, _err):
            handled = run_helpers._handle_list_all(True, gateway_client=None)
        assert handled is True
        assert "No strategies registered" in out.getvalue().decode()


# ---------------------------------------------------------------------------
# _load_strategy_class — error paths
# ---------------------------------------------------------------------------


class TestLoadStrategyClassErrors:
    def test_strategy_file_missing_exits_1(self, tmp_path: Path) -> None:
        """Covers the `if not strategy_file.exists(): sys.exit(1)` branch."""
        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._load_strategy_class(str(tmp_path), preloaded=None)
        assert exc_info.value.code == 1
        assert "No strategy.py found" in err.getvalue().decode()

    def test_load_failure_exits_1(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Covers the `if not loaded: sys.exit(1)` branch after load_strategy_from_file."""
        (tmp_path / "strategy.py").write_text("# placeholder")

        from almanak.framework.cli import intent_debug

        def _fake_load(_path: Path) -> tuple[Any, str | None]:
            return None, "syntax error at line 5"

        monkeypatch.setattr(intent_debug, "load_strategy_from_file", _fake_load)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._load_strategy_class(str(tmp_path), preloaded=None)
        assert exc_info.value.code == 1
        assert "Error loading strategy" in err.getvalue().decode()

    def test_preloaded_short_circuits(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When `preloaded` is not None, helper returns it without re-loading."""
        (tmp_path / "strategy.py").write_text("# placeholder")

        calls: list[Any] = []
        from almanak.framework.cli import intent_debug

        def _fake_load(path: Path) -> tuple[Any, str | None]:
            calls.append(path)
            return object(), None

        monkeypatch.setattr(intent_debug, "load_strategy_from_file", _fake_load)

        sentinel = type("PreloadedCls", (), {})
        cli = CliRunner()
        with cli.isolation():
            result = run_helpers._load_strategy_class(str(tmp_path), preloaded=sentinel)
        assert result is sentinel
        assert calls == []


# ---------------------------------------------------------------------------
# _print_startup_banner — uncovered branch coverage
# ---------------------------------------------------------------------------


class _RuntimeStub:
    def __init__(self, chain: str = "arbitrum", wallet: str = "0xaaa", safe: bool = False) -> None:
        self.chain = chain
        self.execution_address = wallet
        self.is_safe_mode = safe


class TestPrintStartupBannerBranches:
    def test_resume_once_not_fresh_prints_warning(self) -> None:
        """is_resume + once + not fresh triggers the RED warning about prior state."""
        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._print_startup_banner(
                strategy_name="Foo",
                strategy_id="dep-1",
                run_id="run-1",
                is_resume=True,
                existing_state_info={"version": 2, "keys": ["x"]},
                once=True,
                fresh=False,
                multi_chain=False,
                strategy_chains=[],
                strategy_protocols=[],
                runtime_config=_RuntimeStub(),
                interval=10,
                max_iterations=None,
                effective_dry_run=False,
                strategy_config={},
                gateway_host="127.0.0.1",
                gateway_port=50051,
                dashboard=False,
            )
        text = out.getvalue().decode()
        assert "RESUME" in text
        assert "State version: 2" in text
        assert "WARNING" in text

    def test_multi_chain_prints_chains_and_protocols(self) -> None:
        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._print_startup_banner(
                strategy_name="Multi",
                strategy_id="dep-m",
                run_id="run-m",
                is_resume=False,
                existing_state_info=None,
                once=False,
                fresh=False,
                multi_chain=True,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=["uniswap_v3"],
                runtime_config=_RuntimeStub(chain="arbitrum"),
                interval=60,
                max_iterations=10,
                effective_dry_run=False,
                strategy_config={},
                gateway_host="127.0.0.1",
                gateway_port=50051,
                dashboard=True,
            )
        text = out.getvalue().decode()
        assert "Chains: arbitrum, base" in text
        assert "Protocols:" in text
        # max_iterations + not once -> appended
        assert "max 10 iterations" in text
        # dashboard flag
        assert "Dashboard:" in text

    def test_safe_mode_label_and_copy_trading_replay(self) -> None:
        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._print_startup_banner(
                strategy_name="Safe",
                strategy_id="dep-s",
                run_id="run-s",
                is_resume=False,
                existing_state_info=None,
                once=True,
                fresh=False,
                multi_chain=False,
                strategy_chains=[],
                strategy_protocols=[],
                runtime_config=_RuntimeStub(safe=True),
                interval=30,
                max_iterations=None,
                effective_dry_run=True,
                strategy_config={
                    "copy_trading": {
                        "execution_policy": {
                            "copy_mode": "replay",
                            "replay_file": "/tmp/r.jsonl",
                        }
                    }
                },
                gateway_host="127.0.0.1",
                gateway_port=50051,
                dashboard=False,
            )
        text = out.getvalue().decode()
        assert "(Safe)" in text
        assert "Copy mode: replay" in text
        assert "Copy replay file: /tmp/r.jsonl" in text


# ---------------------------------------------------------------------------
# _wire_token_resolver — simple happy path (already exercised indirectly)
# ---------------------------------------------------------------------------


class TestWireTokenResolver:
    def test_sets_gateway_channel_on_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.data import tokens as tokens_pkg

        recorded: dict[str, Any] = {}

        class _FakeResolver:
            def set_gateway_channel(self, channel: Any) -> None:
                recorded["channel"] = channel

        fake_resolver = _FakeResolver()
        monkeypatch.setattr(tokens_pkg, "get_token_resolver", lambda: fake_resolver)

        fake_channel = object()
        fake_client = type("C", (), {"channel": fake_channel})()
        run_helpers._wire_token_resolver(fake_client)
        assert recorded["channel"] is fake_channel


# ---------------------------------------------------------------------------
# _normalize_quick_chains — tiny utility not covered elsewhere
# ---------------------------------------------------------------------------


class TestNormalizeQuickChains:
    def test_string_wraps_to_single_list(self) -> None:
        assert run_helpers._normalize_quick_chains("arbitrum") == ["arbitrum"]

    def test_list_coerced_to_strings(self) -> None:
        assert run_helpers._normalize_quick_chains(["arbitrum", 123]) == ["arbitrum", "123"]

    def test_dict_returns_empty_list(self) -> None:
        """Dict would otherwise silently become list of keys; helper rejects this."""
        assert run_helpers._normalize_quick_chains({"base": 1}) == []

    def test_none_returns_empty_list(self) -> None:
        assert run_helpers._normalize_quick_chains(None) == []

    def test_int_returns_empty_list(self) -> None:
        assert run_helpers._normalize_quick_chains(42) == []
