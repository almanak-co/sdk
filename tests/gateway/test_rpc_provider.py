"""Tests for RPC provider env var precedence and custom URL resolution."""

import os
from unittest.mock import patch

import pytest

from almanak.gateway.utils.rpc_provider import (
    NodeProvider,
    _get_custom_url,
    _has_custom_url,
    get_rpc_url,
    has_api_key_configured,
)


@pytest.fixture(autouse=True)
def clean_env():
    """Clear all env vars to guarantee isolation from ambient RPC config."""
    with patch.dict(os.environ, {}, clear=True):
        yield


class TestCustomUrlPrecedence:
    """Test _get_custom_url env var precedence order."""

    def test_almanak_prefixed_chain_url_highest_priority(self):
        """ALMANAK_ARBITRUM_RPC_URL beats everything else."""
        with patch.dict(os.environ, {
            "ALMANAK_ARBITRUM_RPC_URL": "https://almanak-chain",
            "ARBITRUM_RPC_URL": "https://bare-chain",
            "ALMANAK_RPC_URL": "https://almanak-generic",
            "RPC_URL": "https://bare-generic",
        }):
            assert _get_custom_url("arbitrum") == "https://almanak-chain"

    def test_bare_chain_rpc_url_beats_generic(self):
        """ARBITRUM_RPC_URL beats ALMANAK_RPC_URL and RPC_URL."""
        with patch.dict(os.environ, {
            "ARBITRUM_RPC_URL": "https://bare-chain",
            "ALMANAK_RPC_URL": "https://almanak-generic",
            "RPC_URL": "https://bare-generic",
        }):
            assert _get_custom_url("arbitrum") == "https://bare-chain"

    def test_almanak_rpc_url_beats_bare_rpc_url(self):
        """ALMANAK_RPC_URL beats RPC_URL."""
        with patch.dict(os.environ, {
            "ALMANAK_RPC_URL": "https://almanak-generic",
            "RPC_URL": "https://bare-generic",
        }):
            assert _get_custom_url("arbitrum") == "https://almanak-generic"

    def test_bare_rpc_url_is_lowest_custom_priority(self):
        """RPC_URL is used when no other custom env var is set."""
        with patch.dict(os.environ, {"RPC_URL": "https://bare-generic"}):
            assert _get_custom_url("arbitrum") == "https://bare-generic"

    def test_no_custom_url_raises(self):
        """ValueError raised when no custom URL env var is set."""
        with pytest.raises(ValueError, match="No custom RPC URL found"):
            _get_custom_url("arbitrum")


class TestBscBnbAlias:
    """Test that bsc/bnb aliases work interchangeably."""

    def test_bsc_finds_bnb_env_var(self):
        """BSC chain checks BNB_RPC_URL too."""
        with patch.dict(os.environ, {"BNB_RPC_URL": "https://bnb-rpc"}):
            assert _get_custom_url("bsc") == "https://bnb-rpc"

    def test_bnb_finds_bsc_env_var(self):
        """BNB chain checks BSC_RPC_URL too."""
        with patch.dict(os.environ, {"BSC_RPC_URL": "https://bsc-rpc"}):
            assert _get_custom_url("bnb") == "https://bsc-rpc"

    def test_bsc_prefixed_env_var(self):
        """ALMANAK_BSC_RPC_URL works for bsc chain."""
        with patch.dict(os.environ, {"ALMANAK_BSC_RPC_URL": "https://almanak-bsc"}):
            assert _get_custom_url("bsc") == "https://almanak-bsc"

    def test_bnb_prefixed_env_var(self):
        """ALMANAK_BNB_RPC_URL works for bnb chain."""
        with patch.dict(os.environ, {"ALMANAK_BNB_RPC_URL": "https://almanak-bnb"}):
            assert _get_custom_url("bnb") == "https://almanak-bnb"

    def test_bsc_variant_has_priority_for_bsc_chain(self):
        """When both BSC and BNB are set, BSC comes first for 'bsc' chain."""
        with patch.dict(os.environ, {
            "ALMANAK_BSC_RPC_URL": "https://bsc-first",
            "ALMANAK_BNB_RPC_URL": "https://bnb-second",
        }):
            assert _get_custom_url("bsc") == "https://bsc-first"


class TestHasCustomUrl:
    """Test _has_custom_url helper."""

    def test_returns_true_for_chain_url(self):
        with patch.dict(os.environ, {"ARBITRUM_RPC_URL": "https://arb"}):
            assert _has_custom_url("arbitrum") is True

    def test_returns_true_for_generic_url(self):
        with patch.dict(os.environ, {"RPC_URL": "https://generic"}):
            assert _has_custom_url("arbitrum") is True

    def test_returns_false_when_empty(self):
        assert _has_custom_url("arbitrum") is False


class TestGetRpcUrl:
    """Test get_rpc_url end-to-end behavior."""

    def test_custom_url_beats_alchemy(self):
        """Custom URL env var is preferred over ALCHEMY_API_KEY."""
        with patch.dict(os.environ, {
            "RPC_URL": "https://custom-rpc",
            "ALCHEMY_API_KEY": "test-alchemy-key",
        }):
            url = get_rpc_url("arbitrum")
            assert url == "https://custom-rpc"

    def test_alchemy_fallback_when_no_custom_url(self):
        """ALCHEMY_API_KEY is used when no custom URL is set (backward compat)."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-alchemy-key"}):
            url = get_rpc_url("arbitrum")
            assert "alchemy.com" in url
            assert "test-alchemy-key" in url

    def test_anvil_mode_ignores_custom_url(self):
        """Anvil network always returns localhost regardless of custom URL."""
        with patch.dict(os.environ, {"RPC_URL": "https://custom-rpc"}):
            url = get_rpc_url("arbitrum", network="anvil")
            assert "127.0.0.1" in url

    def test_explicit_custom_url_param_unchanged(self):
        """get_rpc_url(provider=CUSTOM, custom_url=...) still works."""
        url = get_rpc_url(
            "arbitrum",
            provider=NodeProvider.CUSTOM,
            custom_url="https://explicit-url",
        )
        assert url == "https://explicit-url"

    def test_custom_provider_without_param_uses_env(self):
        """NodeProvider.CUSTOM without custom_url falls back to env var lookup."""
        with patch.dict(os.environ, {"RPC_URL": "https://from-env"}):
            url = get_rpc_url("arbitrum", provider=NodeProvider.CUSTOM)
            assert url == "https://from-env"

    def test_per_chain_url_used_for_correct_chain(self):
        """Per-chain URL is used for the matching chain."""
        with patch.dict(os.environ, {
            "ARBITRUM_RPC_URL": "https://arb-specific",
            "BASE_RPC_URL": "https://base-specific",
        }):
            assert get_rpc_url("arbitrum") == "https://arb-specific"
            assert get_rpc_url("base") == "https://base-specific"

    def test_public_rpc_fallback_when_nothing_configured(self):
        """Falls back to free public RPC when no env vars or API keys are set."""
        url = get_rpc_url("arbitrum")
        assert url == "https://arbitrum-one-rpc.publicnode.com"

    def test_error_for_unsupported_chain_no_config(self):
        """Unsupported chain with no public RPC raises ValueError."""
        with pytest.raises(ValueError, match="No RPC provider available"):
            get_rpc_url("unsupported_chain_xyz")


class TestAutoSelectProvider:
    """Test _auto_select_provider ordering."""

    def test_custom_url_selected_over_alchemy(self):
        """Custom URL env var takes priority over Alchemy."""
        with patch.dict(os.environ, {
            "RPC_URL": "https://custom",
            "ALCHEMY_API_KEY": "test-key",
        }):
            url = get_rpc_url("arbitrum")
            assert url == "https://custom"

    def test_alchemy_selected_over_tenderly(self):
        """Alchemy takes priority over Tenderly."""
        with patch.dict(os.environ, {
            "ALCHEMY_API_KEY": "test-key",
            "TENDERLY_API_KEY_ARBITRUM": "tenderly-key",
        }):
            url = get_rpc_url("arbitrum")
            assert "alchemy.com" in url

    def test_alchemy_selected_over_public(self):
        """Alchemy takes priority over free public RPCs."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-key"}):
            url = get_rpc_url("arbitrum")
            assert "alchemy.com" in url
            assert "publicnode" not in url

    def test_public_rpc_used_when_no_keys(self):
        """Public RPC used as last resort when no env vars or keys are set."""
        url = get_rpc_url("ethereum")
        assert url == "https://ethereum-rpc.publicnode.com"


class TestHasApiKeyConfigured:
    """Test has_api_key_configured with custom URL env vars."""

    def test_with_rpc_url(self):
        """RPC_URL is recognized as a configured source."""
        with patch.dict(os.environ, {"RPC_URL": "https://custom"}):
            assert has_api_key_configured() is True

    def test_with_almanak_rpc_url(self):
        """ALMANAK_RPC_URL is recognized."""
        with patch.dict(os.environ, {"ALMANAK_RPC_URL": "https://custom"}):
            assert has_api_key_configured() is True

    def test_with_per_chain_url(self):
        """Per-chain URL is recognized."""
        with patch.dict(os.environ, {"ARBITRUM_RPC_URL": "https://arb"}):
            assert has_api_key_configured() is True

    def test_with_alchemy(self):
        """ALCHEMY_API_KEY still works (backward compat)."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "key"}):
            assert has_api_key_configured() is True

    def test_with_bsc_alias(self):
        """BSC_RPC_URL is recognized (alias for BNB)."""
        with patch.dict(os.environ, {"BSC_RPC_URL": "https://bsc"}):
            assert has_api_key_configured() is True

    def test_with_almanak_bsc_alias(self):
        """ALMANAK_BSC_RPC_URL is recognized (alias for BNB)."""
        with patch.dict(os.environ, {"ALMANAK_BSC_RPC_URL": "https://bsc"}):
            assert has_api_key_configured() is True

    def test_with_nothing(self):
        """Returns False when nothing is configured."""
        assert has_api_key_configured() is False
