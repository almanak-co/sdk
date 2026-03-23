"""Tests for Safe-vs-EOA wallet resolution in gateway server.

Covers _RegisterChainsServicer.RegisterChains and GatewayServer._prewarm_chains
wallet address resolution: Safe precedence, EOA fallback, missing-wallet error.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.gateway.core.settings import GatewaySettings

# A well-known test private key (anvil default #0) and its derived address.
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_EOA_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_SAFE_ADDRESS = "0xSafe0000000000000000000000000000000000AA"


# ---------------------------------------------------------------------------
# Lightweight request stub so we don't need the full proto import
# ---------------------------------------------------------------------------
@dataclass
class _FakeRegisterChainsRequest:
    chains: list[str] = field(default_factory=list)
    wallet_address: str = ""


# ---------------------------------------------------------------------------
# _RegisterChainsServicer wallet resolution
# ---------------------------------------------------------------------------
class TestRegisterChainsWalletResolution:
    """Wallet resolution inside _RegisterChainsServicer.RegisterChains."""

    def _make_servicer(self, settings: GatewaySettings):
        from almanak.gateway.server import _RegisterChainsServicer

        health = MagicMock()
        execution = MagicMock()
        return _RegisterChainsServicer(health, execution, settings)

    @pytest.mark.asyncio
    async def test_safe_address_takes_precedence(self) -> None:
        """When safe_mode + safe_address are set, Safe address is used."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="direct",
            metrics_enabled=False,
            audit_enabled=False,
        )
        servicer = self._make_servicer(settings)
        servicer._execution._get_orchestrator = AsyncMock()
        servicer._execution._get_compiler = MagicMock()

        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")
        context = MagicMock()

        response = await servicer.RegisterChains(request, context)

        assert response.wallet_address == TEST_SAFE_ADDRESS

    @pytest.mark.asyncio
    async def test_eoa_fallback_when_no_safe(self) -> None:
        """When Safe mode is off, derive wallet from private key."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        servicer = self._make_servicer(settings)
        servicer._execution._get_orchestrator = AsyncMock()
        servicer._execution._get_compiler = MagicMock()

        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")
        context = MagicMock()

        response = await servicer.RegisterChains(request, context)

        assert response.wallet_address.lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_missing_wallet_returns_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When no safe_address, no safe_mode, and no private_key, error is returned."""
        # Clear env vars that GatewaySettings reads via pydantic-settings + fallback validator
        monkeypatch.delenv("ALMANAK_GATEWAY_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        settings = GatewaySettings(
            private_key=None,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        # Force private_key to None in case .env file provides a fallback
        settings.private_key = None
        servicer = self._make_servicer(settings)

        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")
        context = MagicMock()

        response = await servicer.RegisterChains(request, context)

        assert response.success is False
        assert "No wallet_address" in response.error

    @pytest.mark.asyncio
    async def test_explicit_wallet_address_used_as_is(self) -> None:
        """When request provides wallet_address, it is used regardless of settings."""
        explicit = "0xExplicit0000000000000000000000000000AABB"
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="direct",
            metrics_enabled=False,
            audit_enabled=False,
        )
        servicer = self._make_servicer(settings)
        servicer._execution._get_orchestrator = AsyncMock()
        servicer._execution._get_compiler = MagicMock()

        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address=explicit)
        context = MagicMock()

        response = await servicer.RegisterChains(request, context)

        assert response.wallet_address == explicit


# ---------------------------------------------------------------------------
# GatewayServer._prewarm_chains wallet resolution
# ---------------------------------------------------------------------------
class TestPrewarmChainsWalletResolution:
    """Wallet resolution inside GatewayServer._prewarm_chains."""

    @pytest.mark.asyncio
    async def test_prewarm_uses_safe_address(self) -> None:
        """_prewarm_chains passes Safe address to orchestrator/compiler when configured."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode="direct",
            chains=["arbitrum"],
            metrics_enabled=False,
            audit_enabled=False,
        )

        from almanak.gateway.server import GatewayServer

        server = GatewayServer(settings)
        server._wallet_registry = None  # No multi-wallet registry, use legacy path
        mock_exec = MagicMock()
        mock_exec._get_orchestrator = AsyncMock()
        mock_exec._get_compiler = MagicMock()
        server._execution_servicer = mock_exec

        await server._prewarm_chains()

        mock_exec._get_orchestrator.assert_called_once_with("arbitrum", TEST_SAFE_ADDRESS)
        mock_exec._get_compiler.assert_called_once_with("arbitrum", TEST_SAFE_ADDRESS)

    @pytest.mark.asyncio
    async def test_prewarm_uses_eoa_when_no_safe(self) -> None:
        """_prewarm_chains derives EOA from private key when Safe is not configured."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=None,
            safe_mode=None,
            chains=["arbitrum"],
            metrics_enabled=False,
            audit_enabled=False,
        )

        from almanak.gateway.server import GatewayServer

        server = GatewayServer(settings)
        server._wallet_registry = None  # No multi-wallet registry, use legacy path
        mock_exec = MagicMock()
        mock_exec._get_orchestrator = AsyncMock()
        mock_exec._get_compiler = MagicMock()
        server._execution_servicer = mock_exec

        await server._prewarm_chains()

        called_addr = mock_exec._get_orchestrator.call_args[0][1]
        assert called_addr.lower() == TEST_EOA_ADDRESS.lower()
