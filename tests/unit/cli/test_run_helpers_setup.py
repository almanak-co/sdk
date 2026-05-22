"""Unit tests for `almanak/framework/cli/run_helpers.py` (Phase 4a).

Covers the five low-risk helpers extracted from `framework/cli/run.py:run`:

    - _configure_logging_and_validate
    - _handle_list_all
    - _load_strategy_class
    - _discover_and_load_config
    - _print_startup_banner

Tests follow the pattern used by `test_strategy_run_teardown_after.py`:
CliRunner for helpers that call click.echo, monkeypatch for the few helpers
that touch external module state. No gateway / Anvil startup.
"""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers


# ---------------------------------------------------------------------------
# _configure_logging_and_validate
# ---------------------------------------------------------------------------


class TestConfigureLoggingAndValidate:
    def test_verbose_sets_debug_src_level_and_keeps_third_party_quiet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, object] = {}

        def fake_configure(level, format) -> None:  # noqa: A002 - matches real API
            captured["level"] = level
            captured["format"] = format

        from almanak.framework.utils import logging as lib_logging

        monkeypatch.setattr(lib_logging, "configure_logging", fake_configure)

        run_helpers._configure_logging_and_validate(
            verbose=True,
            debug=False,
            log_file=None,
            once=True,
            teardown_after=False,
        )

        assert captured["level"] == lib_logging.LogLevel.DEBUG
        # Third-party loggers stay at WARNING because --debug is False.
        assert logging.getLogger("web3").level == logging.WARNING
        assert logging.getLogger("urllib3").level == logging.WARNING

    def test_debug_sets_debug_on_third_party_loggers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework.utils import logging as lib_logging

        monkeypatch.setattr(lib_logging, "configure_logging", lambda level, format: None)

        run_helpers._configure_logging_and_validate(
            verbose=False,
            debug=True,
            log_file=None,
            once=True,
            teardown_after=False,
        )

        assert logging.getLogger("web3").level == logging.DEBUG
        assert logging.getLogger("urllib3").level == logging.DEBUG

    def test_default_uses_info_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, object] = {}
        from almanak.framework.utils import logging as lib_logging

        def fake_configure(level, format) -> None:  # noqa: A002
            captured["level"] = level

        monkeypatch.setattr(lib_logging, "configure_logging", fake_configure)

        run_helpers._configure_logging_and_validate(
            verbose=False,
            debug=False,
            log_file=None,
            once=True,
            teardown_after=False,
        )

        assert captured["level"] == lib_logging.LogLevel.INFO

    def test_log_file_registers_file_handler_at_given_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from almanak.framework.utils import logging as lib_logging

        monkeypatch.setattr(lib_logging, "configure_logging", lambda level, format: None)

        captured: dict[str, object] = {}

        def fake_add_file_handler(path: str, level) -> None:
            captured["path"] = path
            captured["level"] = level

        monkeypatch.setattr(lib_logging, "add_file_handler", fake_add_file_handler)

        log_path = tmp_path / "nested" / "dir" / "run.log"

        runner = CliRunner()
        with runner.isolation() as (out_stream, _err):
            run_helpers._configure_logging_and_validate(
                verbose=False,
                debug=False,
                log_file=str(log_path),
                once=True,
                teardown_after=False,
            )
            out_stream.seek(0)
            output = out_stream.read().decode()

        assert captured["path"] == str(log_path)
        assert captured["level"] == lib_logging.LogLevel.DEBUG
        # Parent directory was created.
        assert log_path.parent.exists()
        # Echo confirms the file path.
        assert f"Logging to file: {log_path} (JSON format)" in output

    def test_teardown_after_without_once_exits_nonzero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework.utils import logging as lib_logging

        monkeypatch.setattr(lib_logging, "configure_logging", lambda level, format: None)

        with pytest.raises(SystemExit) as exc_info:
            run_helpers._configure_logging_and_validate(
                verbose=False,
                debug=False,
                log_file=None,
                once=False,
                teardown_after=True,
            )
        assert exc_info.value.code == 1

    def test_teardown_after_with_once_does_not_exit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework.utils import logging as lib_logging

        monkeypatch.setattr(lib_logging, "configure_logging", lambda level, format: None)

        # Should complete without raising SystemExit.
        run_helpers._configure_logging_and_validate(
            verbose=False,
            debug=False,
            log_file=None,
            once=True,
            teardown_after=True,
        )


# ---------------------------------------------------------------------------
# _handle_list_all
# ---------------------------------------------------------------------------


def _make_fake_strategy_class(name: str) -> type:
    return type(name, (), {})


class TestHandleListAll:
    def test_returns_false_when_list_all_is_false(self) -> None:
        assert run_helpers._handle_list_all(list_all=False, gateway_client=None) is False

    def test_lists_registered_strategies_and_marks_multi_chain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework import strategies as strategies_pkg
        from almanak.framework.cli import run as run_module

        single = _make_fake_strategy_class("Single")
        multi = _make_fake_strategy_class("Multi")

        def fake_list_strategies() -> list[str]:
            return ["single_strat", "multi_strat"]

        def fake_get_strategy(name: str) -> type:
            return {"single_strat": single, "multi_strat": multi}[name]

        def fake_is_multi_chain(cls, config=None) -> bool:
            return cls is multi

        def fake_get_chains(cls) -> list[str]:
            return ["arbitrum", "base"] if cls is multi else ["arbitrum"]

        monkeypatch.setattr(strategies_pkg, "list_strategies", fake_list_strategies)
        monkeypatch.setattr(strategies_pkg, "get_strategy", fake_get_strategy)
        monkeypatch.setattr(run_module, "is_multi_chain_strategy", fake_is_multi_chain)
        monkeypatch.setattr(run_module, "get_strategy_chains", fake_get_chains)

        runner = CliRunner()
        with runner.isolation() as (out_stream, _err):
            handled = run_helpers._handle_list_all(list_all=True, gateway_client=None)
            out_stream.seek(0)
            output = out_stream.read().decode()

        assert handled is True
        assert "Registered strategies:" in output
        assert "  - single_strat" in output
        assert "  - multi_strat [multi-chain: arbitrum, base]" in output
        assert "almanak strat run --once" in output

    def test_empty_registry_shows_friendly_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework import strategies as strategies_pkg

        monkeypatch.setattr(strategies_pkg, "list_strategies", lambda: [])

        runner = CliRunner()
        with runner.isolation() as (out_stream, _err):
            handled = run_helpers._handle_list_all(list_all=True, gateway_client=None)
            out_stream.seek(0)
            output = out_stream.read().decode()

        assert handled is True
        assert "No strategies registered in the factory." in output

    def test_swallows_errors_per_strategy_in_introspection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.framework import strategies as strategies_pkg
        from almanak.framework.cli import run as run_module

        def bad_get_strategy(name: str) -> type:
            raise RuntimeError("boom")

        monkeypatch.setattr(strategies_pkg, "list_strategies", lambda: ["broken"])
        monkeypatch.setattr(strategies_pkg, "get_strategy", bad_get_strategy)
        monkeypatch.setattr(run_module, "is_multi_chain_strategy", lambda cls, config=None: False)
        monkeypatch.setattr(run_module, "get_strategy_chains", lambda cls: [])

        runner = CliRunner()
        with runner.isolation() as (out_stream, _err):
            handled = run_helpers._handle_list_all(list_all=True, gateway_client=None)
            out_stream.seek(0)
            output = out_stream.read().decode()

        assert handled is True
        # Falls through to the bare `- name` line even when introspection fails.
        assert "  - broken" in output


# ---------------------------------------------------------------------------
# _load_strategy_class
# ---------------------------------------------------------------------------


class TestLoadStrategyClass:
    def test_missing_strategy_py_exits_nonzero(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit) as exc_info:
            run_helpers._load_strategy_class(str(tmp_path), preloaded=None)
        assert exc_info.value.code == 1

    def test_preloaded_short_circuits(self, tmp_path: Path) -> None:
        # strategy.py must still exist (the helper validates that first).
        (tmp_path / "strategy.py").write_text("# placeholder\n")
        fake_class = _make_fake_strategy_class("Preloaded")

        result = run_helpers._load_strategy_class(str(tmp_path), preloaded=fake_class)
        assert result is fake_class

    def test_loads_from_file_when_no_preloaded(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        (tmp_path / "strategy.py").write_text("# placeholder\n")
        fake_class = _make_fake_strategy_class("Loaded")

        from almanak.framework.cli import intent_debug

        def fake_load(path: Path):
            assert path == tmp_path / "strategy.py"
            return fake_class, None

        monkeypatch.setattr(intent_debug, "load_strategy_from_file", fake_load)

        result = run_helpers._load_strategy_class(str(tmp_path), preloaded=None)
        assert result is fake_class

    def test_load_error_exits_nonzero(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        (tmp_path / "strategy.py").write_text("# placeholder\n")

        from almanak.framework.cli import intent_debug

        def fake_load(path: Path):
            return None, "ImportError: missing dep"

        monkeypatch.setattr(intent_debug, "load_strategy_from_file", fake_load)

        with pytest.raises(SystemExit) as exc_info:
            run_helpers._load_strategy_class(str(tmp_path), preloaded=None)
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# _discover_and_load_config
# ---------------------------------------------------------------------------


def _stub_config_loader(monkeypatch: pytest.MonkeyPatch, config: dict | None = None) -> list[str | None]:
    """Replace run.load_strategy_config with a stub and capture the path it was called with.

    Returns a mutable list; index 0 holds the resolved config_file path. Uses
    the caller-supplied `config` dict (or a minimal default) as the returned
    config.
    """
    from almanak.framework.cli import run as run_module

    captured: list[str | None] = [None]

    def fake_load(strategy_name: str, config_file: str | None) -> dict:
        captured[0] = config_file
        return dict(config) if config is not None else {"deployment_id": strategy_name}

    monkeypatch.setattr(run_module, "load_strategy_config", fake_load)
    monkeypatch.setattr(run_module, "is_multi_chain_strategy", lambda cls, config=None: False)
    return captured


class TestDiscoverAndLoadConfig:
    def test_explicit_config_file_is_passed_through(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        captured = _stub_config_loader(monkeypatch)

        explicit = tmp_path / "elsewhere.json"
        explicit.write_text("{}")

        fake_class = _make_fake_strategy_class("Strat")
        cfg, multi_chain, dry, resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=str(explicit),
            strategy_class=fake_class,
            copy_mode=None,
            copy_shadow=False,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=False,
        )
        assert captured[0] == str(explicit)
        assert resolved == str(explicit)
        assert multi_chain is False
        assert dry is False
        assert isinstance(cfg, dict)

    @pytest.mark.parametrize("name", ["config.json", "config.yaml", "config.yml"])
    def test_auto_discovers_config_filename(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
    ) -> None:
        captured = _stub_config_loader(monkeypatch)
        (tmp_path / name).write_text("{}" if name.endswith(".json") else "")

        fake_class = _make_fake_strategy_class("Strat")
        _cfg, _multi, _dry, resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode=None,
            copy_shadow=False,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=False,
        )
        assert captured[0] == str(tmp_path / name)
        assert resolved == str(tmp_path / name)

    def test_json_discovered_before_yaml(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # When both exist, config.json wins (matches the candidate order).
        _stub_config_loader(monkeypatch)
        (tmp_path / "config.json").write_text("{}")
        (tmp_path / "config.yaml").write_text("")
        fake_class = _make_fake_strategy_class("Strat")

        _cfg, _multi, _dry, resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode=None,
            copy_shadow=False,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=False,
        )
        assert resolved == str(tmp_path / "config.json")

    def test_dry_run_propagates_to_effective(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _stub_config_loader(monkeypatch)
        fake_class = _make_fake_strategy_class("Strat")
        _cfg, _multi, effective, _resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode=None,
            copy_shadow=False,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=True,
        )
        assert effective is True

    def test_copy_shadow_forces_dry_run_and_mode(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _stub_config_loader(monkeypatch)
        fake_class = _make_fake_strategy_class("Strat")
        cfg, _multi, effective, _resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode=None,
            copy_shadow=True,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=False,
        )
        assert effective is True
        assert cfg["copy_trading"]["execution_policy"]["copy_mode"] == "shadow"
        assert cfg["copy_trading"]["execution_policy"]["shadow"] is True

    def test_copy_replay_file_sets_replay_mode(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _stub_config_loader(monkeypatch)
        fake_class = _make_fake_strategy_class("Strat")
        cfg, _multi, effective, _resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode=None,
            copy_shadow=False,
            copy_replay_file="/tmp/fills.jsonl",
            copy_strict=True,
            dry_run=False,
        )
        policy = cfg["copy_trading"]["execution_policy"]
        assert policy["copy_mode"] == "replay"
        assert policy["replay_file"] == "/tmp/fills.jsonl"
        assert policy["strict"] is True
        # Safety fix (closes #1678): passing --copy-replay-file alone now forces
        # effective_dry_run to True even when the caller did not also pass
        # --copy-mode replay or --dry-run explicitly. Prevents a replay session
        # from silently submitting live transactions.
        assert effective is True

    def test_explicit_copy_mode_replay_triggers_effective_dry_run(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _stub_config_loader(monkeypatch)
        fake_class = _make_fake_strategy_class("Strat")
        _cfg, _multi, effective, _resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode="replay",
            copy_shadow=False,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=False,
        )
        assert effective is True

    def test_copy_mode_string_normalized_to_lowercase(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _stub_config_loader(monkeypatch)
        fake_class = _make_fake_strategy_class("Strat")
        cfg, _multi, effective, _resolved, _norm = run_helpers._discover_and_load_config(
            working_dir=str(tmp_path),
            config_file=None,
            strategy_class=fake_class,
            copy_mode="LIVE",
            copy_shadow=False,
            copy_replay_file=None,
            copy_strict=False,
            dry_run=False,
        )
        assert cfg["copy_trading"]["execution_policy"]["copy_mode"] == "live"
        # copy_mode="live" does NOT force dry_run.
        assert effective is False

    def test_copy_trading_wrong_type_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Seed an explicit bad-typed copy_trading value.
        _stub_config_loader(monkeypatch, config={"copy_trading": ["not", "a", "dict"]})
        fake_class = _make_fake_strategy_class("Strat")

        with pytest.raises(click.ClickException):
            run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=fake_class,
                copy_mode=None,
                copy_shadow=True,
                copy_replay_file=None,
                copy_strict=False,
                dry_run=False,
            )

    def test_copy_trading_execution_policy_wrong_type_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # execution_policy present but not a dict - must fail with a friendly message
        # rather than a raw TypeError/ValueError from `dict(...)`.
        _stub_config_loader(
            monkeypatch,
            config={"copy_trading": {"execution_policy": "live"}},
        )
        fake_class = _make_fake_strategy_class("Strat")

        with pytest.raises(click.ClickException) as excinfo:
            run_helpers._discover_and_load_config(
                working_dir=str(tmp_path),
                config_file=None,
                strategy_class=fake_class,
                copy_mode="live",
                copy_shadow=False,
                copy_replay_file=None,
                copy_strict=False,
                dry_run=False,
            )
        assert "execution_policy" in str(excinfo.value.message)


# ---------------------------------------------------------------------------
# _print_startup_banner
# ---------------------------------------------------------------------------


def _make_runtime_config(
    *,
    chain: str = "arbitrum",
    execution_address: str = "0xabc",
    is_safe_mode: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        chain=chain,
        execution_address=execution_address,
        is_safe_mode=is_safe_mode,
    )


def _capture_banner(**overrides) -> str:
    """Invoke the banner with sane defaults + overrides, capturing stdout."""
    defaults = dict(
        strategy_name="DemoStrat",
        deployment_id="demo:abcd",
        run_id="run-1",
        is_resume=False,
        existing_state_info=None,
        once=True,
        fresh=False,
        multi_chain=False,
        strategy_chains=["arbitrum"],
        strategy_protocols={"arbitrum": ["uniswap_v3"]},
        runtime_config=_make_runtime_config(),
        interval=10,
        max_iterations=None,
        effective_dry_run=False,
        strategy_config={},
        gateway_host="127.0.0.1",
        gateway_port=50051,
        dashboard=False,
    )
    defaults.update(overrides)

    runner = CliRunner()
    with runner.isolation() as (out_stream, _err):
        run_helpers._print_startup_banner(**defaults)
        out_stream.seek(0)
        return out_stream.read().decode()


class TestPrintStartupBanner:
    def test_fresh_start_headers_present(self) -> None:
        output = _capture_banner()
        assert "ALMANAK STRATEGY RUNNER" in output
        assert "Strategy: DemoStrat" in output
        assert "Deployment ID: demo:abcd" in output
        assert "Run ID: run-1" in output
        assert "FRESH START" in output
        assert "Chain: arbitrum" in output
        assert "Wallet: 0xabc" in output
        assert "Gateway: 127.0.0.1:50051" in output

    def test_resume_mode_includes_state_info_and_warning(self) -> None:
        output = _capture_banner(
            is_resume=True,
            existing_state_info={"version": 3, "keys": ["foo", "bar"]},
            once=True,
            fresh=False,
        )
        assert "RESUME" in output
        assert "State version: 3, keys: ['foo', 'bar']" in output
        assert "WARNING: Loading state from a previous run." in output

    def test_resume_with_fresh_suppresses_warning(self) -> None:
        output = _capture_banner(
            is_resume=True,
            existing_state_info={"version": 3, "keys": []},
            once=True,
            fresh=True,
        )
        assert "RESUME" in output
        assert "WARNING: Loading state" not in output

    def test_hosted_mode_shows_gateway_managed_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Hosted mode (ALMANAK_IS_HOSTED set) keeps state in Postgres via the
        gateway — the runner CLI does no SQLite resume detection so is_resume
        is always False. Without a hosted-aware branch the banner would always
        print the misleading "FRESH START (no existing state)" even when the
        agent is actually resuming from prior Postgres state across pod
        restarts. Regression guard for the gemini-code-assist review on PR #2004."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "test-agent-id")
        output = _capture_banner(is_resume=False)
        assert "HOSTED" in output
        assert "Postgres" in output
        # Local-mode markers must not leak through in hosted output.
        assert "FRESH START" not in output
        assert "RESUME" not in output

    def test_multi_chain_prints_chains_and_protocols(self) -> None:
        output = _capture_banner(
            multi_chain=True,
            strategy_chains=["arbitrum", "base"],
            strategy_protocols={"arbitrum": ["uniswap_v3"], "base": ["aerodrome"]},
        )
        assert "Chains: arbitrum, base" in output
        # Protocols are echoed via its __repr__; accept either order.
        assert "Protocols:" in output

    def test_continuous_mode_with_max_iterations(self) -> None:
        output = _capture_banner(once=False, interval=30, max_iterations=5)
        assert "Execution: Continuous (every 30s), max 5 iterations" in output

    def test_once_ignores_max_iterations(self) -> None:
        output = _capture_banner(once=True, interval=10, max_iterations=5)
        assert "Execution: Single run" in output
        assert "max 5" not in output

    def test_safe_mode_wallet_suffix(self) -> None:
        output = _capture_banner(
            runtime_config=_make_runtime_config(is_safe_mode=True),
        )
        assert "Wallet: 0xabc (Safe)" in output

    def test_copy_trading_section_printed_when_config_present(self) -> None:
        output = _capture_banner(
            strategy_config={
                "copy_trading": {
                    "execution_policy": {"copy_mode": "replay", "replay_file": "/tmp/x.jsonl"}
                }
            }
        )
        assert "Copy mode: replay" in output
        assert "Copy replay file: /tmp/x.jsonl" in output

    def test_dashboard_flag_adds_dashboard_line(self) -> None:
        output = _capture_banner(dashboard=True)
        assert "Dashboard: Will launch alongside strategy" in output


# ---------------------------------------------------------------------------
# _anchor_strategy_folder_env (Phase 4c)
# ---------------------------------------------------------------------------


class TestAnchorStrategyFolderEnv:
    """The Phase 4c hoist of the VIB-3761 strategy-folder pin out of `run()`.

    Three branches:
        1. folder exists + env unset → set to resolved absolute path
        2. folder exists + env already set → leave operator override alone
        3. folder does not exist → no-op
    """

    def test_dir_with_unset_env_pins_resolved_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        run_helpers._anchor_strategy_folder_env(str(tmp_path))
        import os as _os

        assert _os.environ["ALMANAK_STRATEGY_FOLDER"] == str(tmp_path.resolve())

    def test_dir_with_existing_env_does_not_overwrite(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        operator_override = "/explicitly/set/by/operator"
        monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", operator_override)
        run_helpers._anchor_strategy_folder_env(str(tmp_path))
        import os as _os

        assert _os.environ["ALMANAK_STRATEGY_FOLDER"] == operator_override

    def test_nonexistent_path_is_noop(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        missing = tmp_path / "does-not-exist"
        run_helpers._anchor_strategy_folder_env(str(missing))
        import os as _os

        assert "ALMANAK_STRATEGY_FOLDER" not in _os.environ
