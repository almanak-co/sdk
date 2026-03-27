"""Tests for GatewaySettings env var fallback behavior.

Verifies precedence: ALMANAK_GATEWAY_* takes priority over ALMANAK_* fallbacks.
"""

import os
from unittest.mock import patch

from almanak.gateway.core.settings import GatewaySettings


class TestSettingsFallback:
    """Test _fallback_env_vars hydrates from ALMANAK_* when ALMANAK_GATEWAY_* is missing."""

    def test_eoa_address_fallback(self):
        """ALMANAK_EOA_ADDRESS populates eoa_address when gateway var is unset."""
        env = {"ALMANAK_EOA_ADDRESS": "0xABC123", "ALMANAK_GATEWAY_EOA_ADDRESS": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.eoa_address == "0xABC123"

    def test_safe_address_fallback(self):
        """ALMANAK_SAFE_ADDRESS populates safe_address when gateway var is unset."""
        env = {"ALMANAK_SAFE_ADDRESS": "0xSAFE", "ALMANAK_GATEWAY_SAFE_ADDRESS": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.safe_address == "0xSAFE"

    def test_zodiac_address_fallback(self):
        """ALMANAK_ZODIAC_ADDRESS populates zodiac_roles_address."""
        env = {"ALMANAK_ZODIAC_ADDRESS": "0xZODIAC", "ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.zodiac_roles_address == "0xZODIAC"

    def test_signer_service_url_fallback(self):
        """ALMANAK_SIGNER_SERVICE_URL populates signer_service_url."""
        env = {"ALMANAK_SIGNER_SERVICE_URL": "https://signer.test", "ALMANAK_GATEWAY_SIGNER_SERVICE_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.signer_service_url == "https://signer.test"

    def test_signer_service_jwt_fallback(self):
        """ALMANAK_SIGNER_SERVICE_JWT populates signer_service_jwt."""
        env = {"ALMANAK_SIGNER_SERVICE_JWT": "jwt-tok", "ALMANAK_GATEWAY_SIGNER_SERVICE_JWT": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.signer_service_jwt == "jwt-tok"

    def test_gateway_var_takes_precedence_over_fallback(self):
        """ALMANAK_GATEWAY_EOA_ADDRESS takes priority over ALMANAK_EOA_ADDRESS."""
        env = {
            "ALMANAK_GATEWAY_EOA_ADDRESS": "0xGATEWAY",
            "ALMANAK_EOA_ADDRESS": "0xFALLBACK",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.eoa_address == "0xGATEWAY"

    def test_private_key_fallback(self):
        """ALMANAK_PRIVATE_KEY populates private_key when gateway var is unset."""
        env = {"ALMANAK_PRIVATE_KEY": "0xKEY", "ALMANAK_GATEWAY_PRIVATE_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.private_key == "0xKEY"

    # --- Third-party API key fallbacks (bare name -> Pydantic field) ---

    def test_alchemy_api_key_bare_fallback(self):
        """ALCHEMY_API_KEY populates alchemy_api_key when prefixed var is unset."""
        env = {"ALCHEMY_API_KEY": "alchemy-test-key", "ALMANAK_GATEWAY_ALCHEMY_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.alchemy_api_key == "alchemy-test-key"

    def test_coingecko_api_key_bare_fallback(self):
        """COINGECKO_API_KEY populates coingecko_api_key when prefixed var is unset."""
        env = {"COINGECKO_API_KEY": "cg-test-key", "ALMANAK_GATEWAY_COINGECKO_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.coingecko_api_key == "cg-test-key"

    def test_prefixed_alchemy_key_takes_precedence(self):
        """ALMANAK_GATEWAY_ALCHEMY_API_KEY takes priority over bare ALCHEMY_API_KEY."""
        env = {
            "ALMANAK_GATEWAY_ALCHEMY_API_KEY": "prefixed-key",
            "ALCHEMY_API_KEY": "bare-key",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.alchemy_api_key == "prefixed-key"

    def test_enso_api_key_bare_fallback(self):
        """ENSO_API_KEY populates enso_api_key when prefixed var is unset."""
        env = {"ENSO_API_KEY": "enso-test-key", "ALMANAK_GATEWAY_ENSO_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.enso_api_key == "enso-test-key"

    def test_prefixed_enso_key_takes_precedence(self):
        """ALMANAK_GATEWAY_ENSO_API_KEY takes priority over bare ENSO_API_KEY."""
        env = {
            "ALMANAK_GATEWAY_ENSO_API_KEY": "prefixed-enso-key",
            "ENSO_API_KEY": "bare-enso-key",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert settings.enso_api_key == "prefixed-enso-key"

    def test_api_keys_empty_when_neither_set(self):
        """alchemy_api_key, coingecko_api_key, and enso_api_key remain empty when no env vars are set."""
        env = {
            "ALMANAK_GATEWAY_ALCHEMY_API_KEY": "",
            "ALCHEMY_API_KEY": "",
            "ALMANAK_GATEWAY_COINGECKO_API_KEY": "",
            "COINGECKO_API_KEY": "",
            "ALMANAK_GATEWAY_ENSO_API_KEY": "",
            "ENSO_API_KEY": "",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = GatewaySettings()
            assert not settings.alchemy_api_key
            assert not settings.coingecko_api_key
            assert not settings.enso_api_key
