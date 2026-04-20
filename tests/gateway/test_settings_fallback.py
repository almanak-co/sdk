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
