"""Tests for _get_gateway_api_key helper in rpc_provider."""

import pytest

from almanak.gateway.utils.rpc_provider import _get_gateway_api_key


class TestGetGatewayApiKey:
    """Test env-var precedence for _get_gateway_api_key."""

    def test_prefixed_key_wins(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_ALCHEMY_API_KEY", "prefixed-val")
        monkeypatch.setenv("ALCHEMY_API_KEY", "bare-val")
        assert _get_gateway_api_key("ALCHEMY_API_KEY") == "prefixed-val"

    def test_bare_key_fallback(self, monkeypatch):
        monkeypatch.delenv("ALMANAK_GATEWAY_ALCHEMY_API_KEY", raising=False)
        monkeypatch.setenv("ALCHEMY_API_KEY", "bare-val")
        assert _get_gateway_api_key("ALCHEMY_API_KEY") == "bare-val"

    def test_neither_key_returns_none(self, monkeypatch):
        monkeypatch.delenv("ALMANAK_GATEWAY_ALCHEMY_API_KEY", raising=False)
        monkeypatch.delenv("ALCHEMY_API_KEY", raising=False)
        assert _get_gateway_api_key("ALCHEMY_API_KEY") is None

    def test_empty_prefixed_key_falls_through(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_COINGECKO_API_KEY", "")
        monkeypatch.setenv("COINGECKO_API_KEY", "bare-cg")
        assert _get_gateway_api_key("COINGECKO_API_KEY") == "bare-cg"
