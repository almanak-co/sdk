"""Tests for backtest CLI config resolution and strategy_id fixes (VIB-171, VIB-200)."""

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
        config_file.write_text(json.dumps({"strategy_id": "uniswap_rsi", "rsi_period": 14}))

        result = load_strategy_config("demo_uniswap_rsi", "arbitrum")

        assert result["strategy_id"] == "uniswap_rsi"
        assert result["rsi_period"] == 14

    def test_finds_incubating_strategy_with_prefix_stripping(self, tmp_path, monkeypatch):
        """incubating_my_strat should find strategies/incubating/my_strat/config.json."""
        monkeypatch.chdir(tmp_path)

        config_dir = tmp_path / "strategies" / "incubating" / "my_strat"
        config_dir.mkdir(parents=True)
        config_file = config_dir / "config.json"
        config_file.write_text(json.dumps({"strategy_id": "my_strat"}))

        result = load_strategy_config("incubating_my_strat", "ethereum")

        assert result["strategy_id"] == "my_strat"

    def test_finds_demo_strategy_without_prefix(self, tmp_path, monkeypatch):
        """If the full name matches a demo dir, it should still be found."""
        monkeypatch.chdir(tmp_path)

        config_dir = tmp_path / "almanak" / "demo_strategies" / "demo_uniswap_rsi"
        config_dir.mkdir(parents=True)
        config_file = config_dir / "config.json"
        config_file.write_text(json.dumps({"strategy_id": "full_name_match"}))

        result = load_strategy_config("demo_uniswap_rsi", "arbitrum")

        assert result["strategy_id"] == "full_name_match"

    def test_falls_back_to_default_when_not_found(self, tmp_path, monkeypatch):
        """When no config file exists, should return default config."""
        monkeypatch.chdir(tmp_path)

        result = load_strategy_config("nonexistent_strategy", "arbitrum")

        assert "strategy_id" in result
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
# VIB-200: strategy_id should be non-blank
# ---------------------------------------------------------------------------


class TestStrategyIdFallback:
    """Verify strategy_id is set to a non-empty value for backtest strategies."""

    def test_default_config_has_strategy_id(self, tmp_path, monkeypatch):
        """Default config should include strategy_id derived from strategy name."""
        monkeypatch.chdir(tmp_path)

        result = load_strategy_config("demo_uniswap_rsi", "arbitrum")

        assert result["strategy_id"]  # non-empty
        assert "demo_uniswap_rsi" in result["strategy_id"]
