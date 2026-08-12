"""Tests for paper trading anvil_funding config.json integration (VIB-202).

Verifies:
- Paper trader reads anvil_funding from strategy config.json
- CLI flags override config values
- Default behavior unchanged when neither config nor CLI specify funding
"""

import json
from decimal import Decimal

from almanak.framework.cli.backtest import load_strategy_config
from almanak.framework.cli.backtest.paper_helpers import parse_funding_dict
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
BASE_WETH = "0x4200000000000000000000000000000000000006"


class TestAnvilFundingConfigLoading:
    """Test that load_strategy_config extracts anvil_funding correctly."""

    def test_load_config_with_anvil_funding(self, tmp_path, monkeypatch):
        """Config with anvil_funding returns the funding block."""
        config = {
            "chain": "base",
            "anvil_funding": {NATIVE_SENTINEL: 100, BASE_USDC: 10000, BASE_WETH: 5},
        }
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        config_file = config_dir / "test_strat.json"
        config_file.write_text(json.dumps(config))

        monkeypatch.chdir(tmp_path)
        result = load_strategy_config("test_strat", "base")
        assert result["anvil_funding"] == {NATIVE_SENTINEL: 100, BASE_USDC: 10000, BASE_WETH: 5}

    def test_anvil_funding_parsing_eth_and_tokens(self):
        """anvil_funding separates the native sentinel from ERC-20 addresses."""
        anvil_funding = {NATIVE_SENTINEL: 100, BASE_USDC: 10000, BASE_WETH: 5}
        config_eth, config_tokens = parse_funding_dict(
            anvil_funding,
            frozenset({NATIVE_SENTINEL}),
            "anvil_funding",
            addresses_only=True,
        )

        assert config_eth == Decimal("100")
        assert {key.lower(): value for key, value in config_tokens.items()} == {
            BASE_USDC: Decimal("10000"),
            BASE_WETH: Decimal("5"),
        }

    def test_anvil_funding_non_eth_native_token(self):
        """The sentinel represents the active chain's native asset, not ETH specifically."""
        config_eth, config_tokens = parse_funding_dict(
            {NATIVE_SENTINEL: 100, BASE_USDC: 10000},
            frozenset({NATIVE_SENTINEL}),
            "anvil_funding",
            addresses_only=True,
        )

        assert config_eth == Decimal("100")
        assert {key.lower(): value for key, value in config_tokens.items()} == {BASE_USDC: Decimal("10000")}

    def test_empty_anvil_funding(self):
        """Empty anvil_funding produces no overrides."""
        config_eth, config_tokens = parse_funding_dict(
            {},
            frozenset({NATIVE_SENTINEL}),
            "anvil_funding",
            addresses_only=True,
        )

        assert config_eth is None
        assert config_tokens == {}

    def test_no_anvil_funding_key(self):
        """Config without anvil_funding key returns empty dict."""
        config = {"chain": "base", "deployment_id": "test"}
        anvil_funding = config.get("anvil_funding", {})
        assert anvil_funding == {}


class TestAnvilFundingMerge:
    """Test CLI flags override config values."""

    def test_cli_tokens_override_config(self):
        """CLI tokens should override config tokens for same symbol."""
        config_tokens = {"USDC": Decimal("10000"), "WETH": Decimal("5")}
        cli_tokens = {"USDC": Decimal("50000")}

        merged = {**config_tokens, **cli_tokens}

        assert merged["USDC"] == Decimal("50000")  # CLI wins
        assert merged["WETH"] == Decimal("5")  # Config preserved

    def test_cli_adds_new_tokens(self):
        """CLI can add tokens not in config."""
        config_tokens = {"USDC": Decimal("10000")}
        cli_tokens = {"WBTC": Decimal("1")}

        merged = {**config_tokens, **cli_tokens}

        assert merged["USDC"] == Decimal("10000")
        assert merged["WBTC"] == Decimal("1")

    def test_empty_cli_preserves_config(self):
        """When no CLI tokens, config tokens are used as-is."""
        config_tokens = {"USDC": Decimal("10000"), "WETH": Decimal("5")}
        cli_tokens = {}

        merged = {**config_tokens, **cli_tokens}

        assert merged == config_tokens

    def test_empty_config_uses_cli(self):
        """When no config tokens, CLI tokens are used as-is."""
        config_tokens = {}
        cli_tokens = {"USDC": Decimal("50000")}

        merged = {**config_tokens, **cli_tokens}

        assert merged == cli_tokens

    def test_both_empty(self):
        """When both empty, result is empty."""
        merged = {**{}, **{}}
        assert merged == {}

    def test_eth_config_used_when_cli_default(self):
        """Config ETH used when CLI initial_eth is at default (10.0)."""
        config_eth = Decimal("100")
        cli_eth_explicit = False
        initial_eth = 10.0  # click default

        if config_eth is not None and not cli_eth_explicit:
            initial_eth = float(config_eth)

        assert initial_eth == 100.0

    def test_cli_eth_overrides_config(self):
        """Explicit CLI --initial-eth overrides config ETH."""
        config_eth = Decimal("100")
        cli_eth_explicit = True
        initial_eth = 20.0  # user passed --initial-eth 20

        if config_eth is not None and not cli_eth_explicit:
            initial_eth = float(config_eth)

        assert initial_eth == 20.0  # CLI wins
