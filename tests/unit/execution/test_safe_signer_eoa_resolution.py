"""Tests for EOA address resolution in Safe signer creation.

Covers both branches:
- SAFE_ZODIAC: requires explicit ALMANAK_EOA_ADDRESS
- SAFE_DIRECT: derives EOA from private key via Account.from_key()
"""

import os
from unittest.mock import patch

import pytest

from almanak.framework.execution.config import (
    ConfigurationError,
    ExecutionMode,
    MissingEnvironmentVariableError,
    MultiChainRuntimeConfig,
    _create_safe_signer_from_env,
)

# Deterministic test key (Anvil default #0) - derives to TEST_DERIVED_EOA
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_DERIVED_EOA = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
# Distinct EOA that does NOT match TEST_PRIVATE_KEY - proves zodiac uses env var, not derivation
TEST_ZODIAC_EOA = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
# Dummy key for zodiac tests - proves zodiac doesn't need a real key for EOA
TEST_DUMMY_PRIVATE_KEY = "0x0000000000000000000000000000000000000000000000000000000000000001"
TEST_SAFE_ADDRESS = "0x1234567890123456789012345678901234567890"
TEST_ZODIAC_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TEST_SIGNER_URL = "https://signer.example.com"
TEST_SIGNER_JWT = "test-jwt-token"


class TestCreateSafeSignerFromEnv:
    """Test _create_safe_signer_from_env EOA resolution branches."""

    def test_zodiac_mode_uses_eoa_address_env_var(self):
        """SAFE_ZODIAC branch reads EOA from ALMANAK_EOA_ADDRESS, not from private key."""
        env = {
            "ALMANAK_SAFE_ADDRESS": TEST_SAFE_ADDRESS,
            "ALMANAK_EOA_ADDRESS": TEST_ZODIAC_EOA,
            "ALMANAK_ZODIAC_ADDRESS": TEST_ZODIAC_ADDRESS,
            "ALMANAK_SIGNER_SERVICE_URL": TEST_SIGNER_URL,
            "ALMANAK_SIGNER_SERVICE_JWT": TEST_SIGNER_JWT,
        }
        with patch.dict(os.environ, env, clear=False):
            signer = _create_safe_signer_from_env(
                execution_mode=ExecutionMode.SAFE_ZODIAC,
                private_key=TEST_DUMMY_PRIVATE_KEY,
            )
            # Verify EOA came from env var, not derived from key
            assert signer._config.wallet_config.eoa_address == TEST_ZODIAC_EOA

    def test_zodiac_mode_fails_without_eoa_address(self):
        """SAFE_ZODIAC raises when ALMANAK_EOA_ADDRESS is missing."""
        env = {
            "ALMANAK_SAFE_ADDRESS": TEST_SAFE_ADDRESS,
            "ALMANAK_EOA_ADDRESS": "",
            "ALMANAK_ZODIAC_ADDRESS": TEST_ZODIAC_ADDRESS,
            "ALMANAK_SIGNER_SERVICE_URL": TEST_SIGNER_URL,
            "ALMANAK_SIGNER_SERVICE_JWT": TEST_SIGNER_JWT,
        }
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(MissingEnvironmentVariableError, match="EOA_ADDRESS"):
                _create_safe_signer_from_env(
                    execution_mode=ExecutionMode.SAFE_ZODIAC,
                    private_key=TEST_PRIVATE_KEY,
                )

    def test_direct_mode_derives_eoa_from_private_key(self):
        """SAFE_DIRECT branch derives EOA from Account.from_key(private_key)."""
        env = {
            "ALMANAK_SAFE_ADDRESS": TEST_SAFE_ADDRESS,
        }
        with patch.dict(os.environ, env, clear=False):
            signer = _create_safe_signer_from_env(
                execution_mode=ExecutionMode.SAFE_DIRECT,
                private_key=TEST_PRIVATE_KEY,
            )
            # Should derive the known address from the Anvil test key
            assert signer._config.wallet_config.eoa_address == TEST_DERIVED_EOA


class TestMultiChainRuntimeConfigCreate:
    """Test MultiChainRuntimeConfig.create() EOA resolution."""

    def test_zodiac_mode_uses_provided_eoa_address(self):
        """create() with safe_zodiac uses the explicit eoa_address param, not private key derivation."""
        config = MultiChainRuntimeConfig.create(
            chains=["arbitrum"],
            protocols={"arbitrum": ["enso"]},
            private_key=TEST_DUMMY_PRIVATE_KEY,
            execution_mode="safe_zodiac",
            safe_address=TEST_SAFE_ADDRESS,
            eoa_address=TEST_ZODIAC_EOA,
            zodiac_address=TEST_ZODIAC_ADDRESS,
            signer_service_url=TEST_SIGNER_URL,
            signer_service_jwt=TEST_SIGNER_JWT,
        )
        assert config.safe_signer is not None
        assert config.safe_signer._config.wallet_config.eoa_address == TEST_ZODIAC_EOA

    def test_zodiac_mode_raises_without_eoa_address(self):
        """create() with safe_zodiac raises ConfigurationError if eoa_address is missing."""
        with pytest.raises(ConfigurationError, match="eoa_address"):
            MultiChainRuntimeConfig.create(
                chains=["arbitrum"],
                protocols={"arbitrum": ["enso"]},
                private_key=TEST_PRIVATE_KEY,
                execution_mode="safe_zodiac",
                safe_address=TEST_SAFE_ADDRESS,
                zodiac_address=TEST_ZODIAC_ADDRESS,
                signer_service_url=TEST_SIGNER_URL,
                signer_service_jwt=TEST_SIGNER_JWT,
            )

    def test_direct_mode_derives_eoa_from_private_key(self):
        """create() with safe_direct derives EOA from private_key."""
        config = MultiChainRuntimeConfig.create(
            chains=["arbitrum"],
            protocols={"arbitrum": ["enso"]},
            private_key=TEST_PRIVATE_KEY,
            execution_mode="safe_direct",
            safe_address=TEST_SAFE_ADDRESS,
        )
        assert config.safe_signer is not None
        assert config.safe_signer._config.wallet_config.eoa_address == TEST_DERIVED_EOA
