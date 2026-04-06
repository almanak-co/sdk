"""Tests for paper-local token override registry (VIB-2378).

Verifies that:
- Token overrides load correctly from JSON file
- Missing file returns empty dict (no-op)
- Malformed JSON file logs warning and returns empty dict
- Address-only and address+decimals formats both work
- Chain-specific filtering works correctly
"""

import json
from pathlib import Path

import pytest

from almanak.framework.backtesting.paper.token_overrides import TokenOverride, load_token_overrides


@pytest.fixture
def config_dir(tmp_path):
    """Create a temp directory for config files."""
    return tmp_path


def _write_config(config_dir: Path, data: dict) -> Path:
    """Write a token override config file."""
    config_file = config_dir / "paper_trading_tokens.json"
    config_file.write_text(json.dumps(data), encoding="utf-8")
    return config_file


class TestLoadTokenOverrides:
    """Tests for load_token_overrides()."""

    def test_missing_file_returns_empty(self, config_dir):
        """Missing config file is a no-op (returns empty dict)."""
        missing_path = config_dir / "nonexistent.json"
        result = load_token_overrides("ethereum", config_path=missing_path)
        assert result == {}

    def test_plain_address_format(self, config_dir):
        """Plain address string is loaded correctly."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "swETH": "0xf951E335afb289353dc249e82926178EaC7DEd78",
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)

        assert "swETH" in result
        assert result["swETH"].address == "0xf951e335afb289353dc249e82926178eac7ded78"
        assert result["swETH"].decimals is None

    def test_dict_format_with_decimals(self, config_dir):
        """Dict format with address and decimals is loaded correctly."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "ankrETH": {
                    "address": "0xe95a203b1a91a908f9b9ce46459d101078c2c3cb",
                    "decimals": 18,
                },
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)

        assert "ankrETH" in result
        assert result["ankrETH"].address == "0xe95a203b1a91a908f9b9ce46459d101078c2c3cb"
        assert result["ankrETH"].decimals == 18

    def test_dict_format_without_decimals(self, config_dir):
        """Dict format without decimals still loads (decimals=None)."""
        config_file = _write_config(config_dir, {
            "arbitrum": {
                "CUSTOM": {"address": "0x1234567890abcdef1234567890abcdef12345678"},
            }
        })
        result = load_token_overrides("arbitrum", config_path=config_file)

        assert "CUSTOM" in result
        assert result["CUSTOM"].decimals is None

    def test_chain_filtering(self, config_dir):
        """Only returns tokens for the requested chain."""
        config_file = _write_config(config_dir, {
            "ethereum": {"swETH": "0xaaaa"},
            "arbitrum": {"CUSTOM": "0xbbbb"},
        })

        eth_result = load_token_overrides("ethereum", config_path=config_file)
        arb_result = load_token_overrides("arbitrum", config_path=config_file)

        assert "swETH" in eth_result
        assert "CUSTOM" not in eth_result
        assert "CUSTOM" in arb_result
        assert "swETH" not in arb_result

    def test_unknown_chain_returns_empty(self, config_dir):
        """Unknown chain returns empty dict."""
        config_file = _write_config(config_dir, {
            "ethereum": {"swETH": "0xaaaa"},
        })
        result = load_token_overrides("solana", config_path=config_file)
        assert result == {}

    def test_malformed_json_returns_empty(self, config_dir, caplog):
        """Malformed JSON logs warning and returns empty."""
        config_file = config_dir / "paper_trading_tokens.json"
        config_file.write_text("not valid json {{{", encoding="utf-8")

        result = load_token_overrides("ethereum", config_path=config_file)
        assert result == {}
        assert "Malformed" in caplog.text

    def test_non_object_root_returns_empty(self, config_dir, caplog):
        """Non-object root (e.g., array) logs warning and returns empty."""
        config_file = config_dir / "paper_trading_tokens.json"
        config_file.write_text('["not", "an", "object"]', encoding="utf-8")

        result = load_token_overrides("ethereum", config_path=config_file)
        assert result == {}
        assert "must be a JSON object" in caplog.text

    def test_missing_address_in_dict_skipped(self, config_dir, caplog):
        """Dict entry without valid address is skipped with warning."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "BAD": {"decimals": 18},  # No address
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)
        assert result == {}
        assert "missing valid address" in caplog.text

    def test_non_int_decimals_ignored(self, config_dir, caplog):
        """Non-integer decimals are ignored (set to None) with warning."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "WEIRD": {"address": "0xaaaa", "decimals": "eighteen"},
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)
        assert "WEIRD" in result
        assert result["WEIRD"].decimals is None
        assert "non-int decimals" in caplog.text

    def test_boolean_decimals_rejected(self, config_dir, caplog):
        """Boolean decimals (True/False) are rejected, not treated as 1/0."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "BOOL": {"address": "0xaaaa", "decimals": True},
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)
        assert "BOOL" in result
        assert result["BOOL"].decimals is None
        assert "non-int decimals" in caplog.text

    def test_unexpected_value_type_skipped(self, config_dir, caplog):
        """Unexpected value types (e.g., int) are skipped with warning."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "BAD": 12345,
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)
        assert result == {}
        assert "unexpected type" in caplog.text

    def test_multiple_tokens_loaded(self, config_dir):
        """Multiple tokens for one chain are all loaded."""
        config_file = _write_config(config_dir, {
            "ethereum": {
                "swETH": "0xaaaa",
                "ankrETH": "0xbbbb",
                "rETH": {"address": "0xcccc", "decimals": 18},
            }
        })
        result = load_token_overrides("ethereum", config_path=config_file)
        assert len(result) == 3
        assert all(isinstance(v, TokenOverride) for v in result.values())


class TestTokenOverrideDataclass:
    """Tests for the TokenOverride dataclass."""

    def test_frozen(self):
        """TokenOverride is immutable."""
        override = TokenOverride(address="0xaaaa", decimals=18)
        with pytest.raises(AttributeError):
            override.address = "0xbbbb"  # type: ignore

    def test_defaults(self):
        """TokenOverride defaults decimals to None."""
        override = TokenOverride(address="0xaaaa")
        assert override.decimals is None
