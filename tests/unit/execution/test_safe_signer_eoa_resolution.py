"""Tests for EOA address resolution in Safe signer creation.

Covers both branches:
- SAFE_ZODIAC: derives EOA from private key when available, falls back to explicit EOA_ADDRESS
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
# Distinct EOA that does NOT match TEST_PRIVATE_KEY - for explicit EOA_ADDRESS tests
TEST_ZODIAC_EOA = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
# Dummy key for zodiac tests
TEST_DUMMY_PRIVATE_KEY = "0x0000000000000000000000000000000000000000000000000000000000000001"
TEST_DUMMY_DERIVED_EOA = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
TEST_SAFE_ADDRESS = "0x1234567890123456789012345678901234567890"
TEST_ZODIAC_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TEST_SIGNER_URL = "https://signer.example.com"
TEST_SIGNER_JWT = "test-jwt-token"


class TestCreateSafeSignerFromEnv:
    """Test _create_safe_signer_from_env EOA resolution branches."""

    def test_zodiac_mode_derives_eoa_from_private_key(self):
        """SAFE_ZODIAC with private_key derives EOA from key, ignoring EOA_ADDRESS env var."""
        env = {
            "ALMANAK_SAFE_ADDRESS": TEST_SAFE_ADDRESS,
            "ALMANAK_ZODIAC_ADDRESS": TEST_ZODIAC_ADDRESS,
        }
        with patch.dict(os.environ, env, clear=False):
            signer = _create_safe_signer_from_env(
                execution_mode=ExecutionMode.SAFE_ZODIAC,
                private_key=TEST_DUMMY_PRIVATE_KEY,
            )
            # EOA derived from private key
            assert signer._config.wallet_config.eoa_address == TEST_DUMMY_DERIVED_EOA

    def test_zodiac_mode_falls_back_to_eoa_address_env_var(self):
        """SAFE_ZODIAC without private_key reads EOA from ALMANAK_EOA_ADDRESS."""
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
                private_key=None,
            )
            # EOA came from env var since no private key
            assert signer._config.wallet_config.eoa_address == TEST_ZODIAC_EOA

    def test_zodiac_mode_fails_without_key_or_eoa_address(self):
        """SAFE_ZODIAC raises when neither private_key nor ALMANAK_EOA_ADDRESS is available."""
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
                    private_key=None,
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
        """create() with safe_zodiac and explicit eoa_address uses that address."""
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

    def test_zodiac_mode_derives_eoa_from_private_key(self):
        """create() with safe_zodiac and no eoa_address derives EOA from private_key."""
        config = MultiChainRuntimeConfig.create(
            chains=["arbitrum"],
            protocols={"arbitrum": ["enso"]},
            private_key=TEST_DUMMY_PRIVATE_KEY,
            execution_mode="safe_zodiac",
            safe_address=TEST_SAFE_ADDRESS,
            zodiac_address=TEST_ZODIAC_ADDRESS,
        )
        assert config.safe_signer is not None
        assert config.safe_signer._config.wallet_config.eoa_address == TEST_DUMMY_DERIVED_EOA

    def test_zodiac_mode_raises_without_key_or_eoa(self):
        """create() with safe_zodiac raises when neither eoa_address nor private_key is provided."""
        with pytest.raises(ConfigurationError, match="eoa_address"):
            MultiChainRuntimeConfig.create(
                chains=["arbitrum"],
                protocols={"arbitrum": ["enso"]},
                private_key=None,
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
