"""Tests for backtest CLI config resolution and deployment_id fixes (VIB-171, VIB-200)."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.cli.backtest import load_strategy_config


# ---------------------------------------------------------------------------
# VIB-171: Config resolution for strategies/demo/ and strategies/incubating/
# ---------------------------------------------------------------------------


class TestLoadStrategyConfigPaths:
    """Verify load_strategy_config searches demo and incubating directories."""

    def test_finds_demo_strategy_with_prefix_stripping(self, tmp_path, monkeypatch):
        """demo_uniswap_rsi should find strategies/demo/uniswap_rsi/config.json."""
        monkeypatch.chdir(tmp_path)

        # Create the demo strategy config
        config_dir = tmp_path / "almanak" / "demo_strategies" / "uniswap_rsi"
        config_dir.mkdir(parents=True)
        config_file = config_dir / "config.json"
        config_file.write_text(json.dumps({"deployment_id": "uniswap_rsi", "rsi_period": 14}))

        result = load_strategy_config("demo_uniswap_rsi", "arbitrum")

        assert result["deployment_id"] == "uniswap_rsi"
        assert result["rsi_period"] == 14

    def test_finds_incubating_strategy_with_prefix_stripping(self, tmp_path, monkeypatch):
        """incubating_my_strat should find strategies/incubating/my_strat/config.json."""
        monkeypatch.chdir(tmp_path)

        config_dir = tmp_path / "strategies" / "incubating" / "my_strat"
        config_dir.mkdir(parents=True)
        config_file = config_dir / "config.json"
        config_file.write_text(json.dumps({"deployment_id": "my_strat"}))

        result = load_strategy_config("incubating_my_strat", "ethereum")

        assert result["deployment_id"] == "my_strat"

    def test_finds_demo_strategy_without_prefix(self, tmp_path, monkeypatch):
        """If the full name matches a demo dir, it should still be found."""
        monkeypatch.chdir(tmp_path)

        config_dir = tmp_path / "almanak" / "demo_strategies" / "demo_uniswap_rsi"
        config_dir.mkdir(parents=True)
        config_file = config_dir / "config.json"
        config_file.write_text(json.dumps({"deployment_id": "full_name_match"}))

        result = load_strategy_config("demo_uniswap_rsi", "arbitrum")

        assert result["deployment_id"] == "full_name_match"

    def test_falls_back_to_default_when_not_found(self, tmp_path, monkeypatch):
        """When no config file exists, should return default config."""
        monkeypatch.chdir(tmp_path)

        result = load_strategy_config("nonexistent_strategy", "arbitrum")

        assert "deployment_id" in result
        # Chain is no longer in default config - it comes from decorator metadata
        assert "chain" not in result

    def test_configs_dir_takes_precedence(self, tmp_path, monkeypatch):
        """configs/ directory should be searched before strategies/."""
        monkeypatch.chdir(tmp_path)

        # Create both configs/name.json and strategies/demo/name/config.json
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "my_strat.json").write_text(json.dumps({"source": "configs_dir"}))

        demo_dir = tmp_path / "almanak" / "demo_strategies" / "my_strat"
        demo_dir.mkdir(parents=True)
        (demo_dir / "config.json").write_text(json.dumps({"source": "demo_dir"}))

        result = load_strategy_config("my_strat", "arbitrum")

        assert result["source"] == "configs_dir"


# ---------------------------------------------------------------------------
# VIB-200: deployment_id should be non-blank
# ---------------------------------------------------------------------------


class TestStrategyIdFallback:
    """Verify deployment_id is set to a non-empty value for backtest strategies."""

    def test_default_config_has_deployment_id(self, tmp_path, monkeypatch):
        """Default config should include deployment_id derived from strategy name.

        Uses a name matching no real strategy: demo names now resolve their
        shipped config.json from any cwd (via the absolute demos-root
        fallback), so a demo name would no longer reach the default path.
        """
        monkeypatch.chdir(tmp_path)

        result = load_strategy_config("no_such_strategy_xyz", "arbitrum")

        assert result["deployment_id"]  # non-empty
        assert "no_such_strategy_xyz" in result["deployment_id"]

    def test_demo_config_resolves_from_any_cwd(self, tmp_path, monkeypatch):
        """Demo configs ship inside the package and resolve regardless of cwd.

        Mirrors the real command flow: resolve_backtest_strategy_name registers
        the shipped demo class first; load_strategy_config then finds its
        config.json next to that class's source, wherever cwd is.
        """
        from almanak.framework.demos import register_demo_strategy
        from almanak.framework.strategies import STRATEGY_REGISTRY, unregister_strategy

        monkeypatch.chdir(tmp_path)

        pre_registered = "demo_uniswap_rsi" in STRATEGY_REGISTRY
        assert register_demo_strategy("demo_uniswap_rsi") == "demo_uniswap_rsi"
        try:
            result = load_strategy_config("demo_uniswap_rsi", "arbitrum")

            # The real shipped demo config (chain + token_funding), not the
            # minimal placeholder default.
            assert result.get("chain") == "ethereum"
            assert result.get("token_funding"), "shipped demo config must carry token_funding"
        finally:
            if not pre_registered and "demo_uniswap_rsi" in STRATEGY_REGISTRY:
                unregister_strategy("demo_uniswap_rsi")

    def test_local_copy_of_demo_keeps_its_own_config(self, tmp_path, monkeypatch):
        """A local strategy shadowing a demo name loads ./config.json, not the shipped config.

        Users copy demos out and edit them; their class registers via the cwd
        lane and wins the name. The shipped-demo config fallback must apply
        only when the registered class actually IS the shipped demo —
        otherwise every backtest of a customized copy silently ran on the
        shipped parameters and token funding.
        """
        from almanak.framework.strategies import STRATEGY_REGISTRY, unregister_strategy

        monkeypatch.chdir(tmp_path)
        (tmp_path / "strategy.py").write_text("# fixture strategy module")
        (tmp_path / "config.json").write_text(json.dumps({"source": "local_copy"}))

        class LocalCopyOfDemo:  # source file = this test file, not the demos root
            pass

        pre_existing = STRATEGY_REGISTRY.get("demo_uniswap_rsi")
        STRATEGY_REGISTRY["demo_uniswap_rsi"] = LocalCopyOfDemo
        try:
            result = load_strategy_config("demo_uniswap_rsi", "arbitrum")
            assert result.get("source") == "local_copy", (
                "local ./config.json must win over the shipped demo config when "
                "the registered class is not the shipped demo"
            )
        finally:
            if pre_existing is not None:
                STRATEGY_REGISTRY["demo_uniswap_rsi"] = pre_existing
            else:
                unregister_strategy("demo_uniswap_rsi")


# ---------------------------------------------------------------------------
# VIB-2917: cwd ./config.json is picked up only when ./strategy.py exists
# ---------------------------------------------------------------------------


class TestCwdConfigJsonDiscovery:
    """Verify load_strategy_config honors ./config.json when cwd is a strategy dir."""

    def test_cwd_config_loaded_when_strategy_py_present(self, tmp_path, monkeypatch):
        """./config.json should load when ./strategy.py marks the cwd as a strategy dir."""
        monkeypatch.chdir(tmp_path)

        (tmp_path / "strategy.py").write_text("# fixture strategy module")
        (tmp_path / "config.json").write_text(json.dumps({"source": "cwd_config"}))

        result = load_strategy_config("my_strat", "arbitrum")

        assert result["source"] == "cwd_config"

    def test_cwd_config_ignored_without_strategy_py(self, tmp_path, monkeypatch):
        """./config.json must NOT be loaded when ./strategy.py is absent (random cwd)."""
        monkeypatch.chdir(tmp_path)

        (tmp_path / "config.json").write_text(json.dumps({"source": "stray_config"}))

        result = load_strategy_config("my_strat", "arbitrum")

        # Falls through to the default dict (no "source" field).
        assert "source" not in result
        assert result["deployment_id"].startswith("backtest-my_strat-")

    def test_strategy_specific_config_beats_cwd_config(self, tmp_path, monkeypatch):
        """configs/<name>.json must win over ./config.json when running for a different strategy."""
        monkeypatch.chdir(tmp_path)

        # cwd looks like strategy A's dir (has strategy.py + config.json).
        (tmp_path / "strategy.py").write_text("# strategy A fixture")
        (tmp_path / "config.json").write_text(json.dumps({"source": "strategy_a_config"}))

        # configs/strategy_b.json also exists (user is backtesting strategy B from A's dir).
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        (configs_dir / "strategy_b.json").write_text(json.dumps({"source": "strategy_b_config"}))

        result = load_strategy_config("strategy_b", "arbitrum")

        assert result["source"] == "strategy_b_config"
