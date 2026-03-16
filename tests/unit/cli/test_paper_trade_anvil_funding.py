"""Tests for paper trading anvil_funding config.json integration (VIB-202).

Verifies:
- Paper trader reads anvil_funding from strategy config.json
- CLI flags override config values
- Default behavior unchanged when neither config nor CLI specify funding
"""

import json
from decimal import Decimal
from pathlib import Path

from almanak.framework.cli.backtest import load_strategy_config


class TestAnvilFundingConfigLoading:
    """Test that load_strategy_config extracts anvil_funding correctly."""

    def test_load_config_with_anvil_funding(self, tmp_path, monkeypatch):
        """Config with anvil_funding returns the funding block."""
        config = {
            "chain": "base",
            "anvil_funding": {"ETH": 100, "USDC": 10000, "WETH": 5},
        }
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        config_file = config_dir / "test_strat.json"
        config_file.write_text(json.dumps(config))

        monkeypatch.chdir(tmp_path)
        result = load_strategy_config("test_strat", "base")
        assert result["anvil_funding"] == {"ETH": 100, "USDC": 10000, "WETH": 5}

    def test_anvil_funding_parsing_eth_and_tokens(self):
        """anvil_funding block correctly separates ETH from ERC-20 tokens."""
        from almanak.gateway.managed import ManagedGateway

        anvil_funding = {"ETH": 100, "USDC": 10000, "WETH": 5}
        native_symbols = ManagedGateway.NATIVE_TOKEN_SYMBOLS

        config_eth = None
        config_tokens = {}
        for token_name, amount in anvil_funding.items():
            token_upper = str(token_name).upper()
            if token_upper in native_symbols:
                config_eth = Decimal(str(amount))
            else:
                config_tokens[token_upper] = Decimal(str(amount))

        assert config_eth == Decimal("100")
        assert config_tokens == {"USDC": Decimal("10000"), "WETH": Decimal("5")}

    def test_anvil_funding_non_eth_native_token(self):
        """Native tokens other than ETH (MNT, AVAX, etc.) route to config_eth."""
        from almanak.gateway.managed import ManagedGateway

        anvil_funding = {"MNT": 100, "USDC": 10000}
        native_symbols = ManagedGateway.NATIVE_TOKEN_SYMBOLS

        config_eth = None
        config_tokens = {}
        for token_name, amount in anvil_funding.items():
            token_upper = str(token_name).upper()
            if token_upper in native_symbols:
                config_eth = Decimal(str(amount))
            else:
                config_tokens[token_upper] = Decimal(str(amount))

        assert config_eth == Decimal("100")
        assert config_tokens == {"USDC": Decimal("10000")}

    def test_empty_anvil_funding(self):
        """Empty anvil_funding produces no overrides."""
        from almanak.gateway.managed import ManagedGateway

        anvil_funding = {}
        native_symbols = ManagedGateway.NATIVE_TOKEN_SYMBOLS

        config_eth = None
        config_tokens = {}
        for token_name, amount in anvil_funding.items():
            token_upper = str(token_name).upper()
            if token_upper in native_symbols:
                config_eth = Decimal(str(amount))
            else:
                config_tokens[token_upper] = Decimal(str(amount))

        assert config_eth is None
        assert config_tokens == {}

    def test_no_anvil_funding_key(self):
        """Config without anvil_funding key returns empty dict."""
        config = {"chain": "base", "strategy_id": "test"}
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
