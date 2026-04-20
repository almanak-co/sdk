"""Tests for zodiac EOA address resolution in ExecutionService._create_signer().

Validates that zodiac mode prefers explicit eoa_address over private key derivation.
This is critical for platform deployments where ALMANAK_PRIVATE_KEY is a dummy value
and ALMANAK_EOA_ADDRESS holds the real authorized EOA.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer


# Anvil default #0 private key and its derived address
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_DERIVED_EOA = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
# A different explicit EOA (platform signer service)
EXPLICIT_EOA = "0x5201565562a45db04419f4c3d582d3ad38ad8bca"
TEST_SAFE_ADDRESS = "0x88c0fede55dfca0512c1a013c2ba118706cd4ae2"
TEST_ZODIAC_ADDRESS = "0xa7cfda03e0ccc7d5c119de9390269a1804f73b68"


class TestZodiacEoaResolution:
    """Tests that zodiac mode prefers eoa_address over private key derivation."""

    def test_eoa_address_preferred_over_private_key(self):
        """When both eoa_address and private_key are set, eoa_address wins."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="zodiac",
            eoa_address=EXPLICIT_EOA,
            zodiac_roles_address=TEST_ZODIAC_ADDRESS,
            metrics_enabled=False,
            audit_enabled=False,
        )
        service = ExecutionServiceServicer(settings)

        with patch("almanak.framework.execution.signer.safe.create_safe_signer") as mock_create:
            mock_create.return_value = MagicMock()
            service._create_signer(TEST_SAFE_ADDRESS)

            # Check the wallet config passed to create_safe_signer
            call_args = mock_create.call_args[0][0]
            assert call_args.wallet_config.eoa_address.lower() == EXPLICIT_EOA.lower()

    def test_private_key_derivation_when_no_eoa_address(self):
        """When only private_key is set (no eoa_address), derive EOA from key."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="zodiac",
            eoa_address=None,
            zodiac_roles_address=TEST_ZODIAC_ADDRESS,
            metrics_enabled=False,
            audit_enabled=False,
        )
        service = ExecutionServiceServicer(settings)

        with patch("almanak.framework.execution.signer.safe.create_safe_signer") as mock_create:
            mock_create.return_value = MagicMock()
            service._create_signer(TEST_SAFE_ADDRESS)

            call_args = mock_create.call_args[0][0]
            assert call_args.wallet_config.eoa_address.lower() == TEST_DERIVED_EOA.lower()

    def test_no_eoa_no_key_raises(self):
        """When neither eoa_address nor private_key is set, raise ValueError."""
        settings = GatewaySettings(
            private_key=None,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="zodiac",
            eoa_address=None,
            zodiac_roles_address=TEST_ZODIAC_ADDRESS,
            metrics_enabled=False,
            audit_enabled=False,
        )
        # Force private_key to None in case .env provides a fallback
        settings.private_key = None
        service = ExecutionServiceServicer(settings)

        with pytest.raises(ValueError, match="EOA_ADDRESS"):
            service._create_signer(TEST_SAFE_ADDRESS)

    def test_direct_mode_always_derives_from_key(self):
        """In direct mode, EOA is always derived from private_key (eoa_address ignored)."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="direct",
            eoa_address=EXPLICIT_EOA,  # Should be ignored for direct mode
            metrics_enabled=False,
            audit_enabled=False,
        )
        service = ExecutionServiceServicer(settings)

        with patch("almanak.framework.execution.signer.safe.create_safe_signer") as mock_create:
            mock_create.return_value = MagicMock()
            service._create_signer(TEST_SAFE_ADDRESS)

            call_args = mock_create.call_args[0][0]
            assert call_args.wallet_config.eoa_address.lower() == TEST_DERIVED_EOA.lower()
