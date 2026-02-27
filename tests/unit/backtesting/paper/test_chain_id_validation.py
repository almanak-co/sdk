"""Unit tests for chain ID validation on Anvil fork startup.

Tests cover:
- Successful validation when source RPC chain ID matches expected
- RuntimeError raised on chain ID mismatch with descriptive message
- Graceful handling of network errors (warn, don't block)
- Graceful handling of RPC error responses
- Validation runs before Anvil process starts

Addresses VIB-226: Chain ID validation on Anvil fork startup.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.fork_manager import (
    CHAIN_IDS,
    RollingForkManager,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_chain_id_response(chain_id: int) -> dict[str, Any]:
    """Build a JSON-RPC response for eth_chainId."""
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": hex(chain_id),
    }


def _make_error_response(message: str = "internal error") -> dict[str, Any]:
    """Build a JSON-RPC error response."""
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": -32000, "message": message},
    }


def _make_manager(chain: str = "arbitrum", rpc_url: str = "http://fake-rpc:8545") -> RollingForkManager:
    """Create a RollingForkManager for testing."""
    return RollingForkManager(
        rpc_url=rpc_url,
        chain=chain,
        anvil_port=8546,
    )


class _FakeResponse:
    """Fake aiohttp response for testing."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    async def json(self) -> dict[str, Any]:
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeSession:
    """Fake aiohttp.ClientSession for testing."""

    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


# =============================================================================
# Tests: _validate_source_chain_id
# =============================================================================


class TestValidateSourceChainId:
    """Tests for chain ID validation on fork startup."""

    @pytest.mark.asyncio
    async def test_passes_when_chain_id_matches(self) -> None:
        """Should pass silently when source RPC chain ID matches expected."""
        manager = _make_manager(chain="arbitrum")
        response = _FakeResponse(_make_chain_id_response(42161))  # Arbitrum
        session = _FakeSession(response)

        with patch("aiohttp.ClientSession", return_value=session):
            # Should not raise
            await manager._validate_source_chain_id()

    @pytest.mark.asyncio
    async def test_raises_on_chain_id_mismatch(self) -> None:
        """Should raise RuntimeError when source chain ID doesn't match expected."""
        manager = _make_manager(chain="base")  # Expects 8453
        response = _FakeResponse(_make_chain_id_response(42161))  # Got Arbitrum
        session = _FakeSession(response)

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(RuntimeError, match="Fork source RPC chain_id mismatch"):
                await manager._validate_source_chain_id()

    @pytest.mark.asyncio
    async def test_mismatch_error_includes_both_chain_ids(self) -> None:
        """Error message should include both expected and actual chain IDs."""
        manager = _make_manager(chain="base")  # Expects 8453
        response = _FakeResponse(_make_chain_id_response(42161))  # Got Arbitrum
        session = _FakeSession(response)

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(RuntimeError) as exc_info:
                await manager._validate_source_chain_id()

            msg = str(exc_info.value)
            assert "8453" in msg  # Expected (base)
            assert "42161" in msg  # Actual (arbitrum)
            assert "base" in msg
            assert "arbitrum" in msg

    @pytest.mark.asyncio
    async def test_mismatch_with_unknown_chain_id(self) -> None:
        """Should handle unknown chain IDs in the error message."""
        manager = _make_manager(chain="ethereum")  # Expects 1
        response = _FakeResponse(_make_chain_id_response(999999))  # Unknown chain
        session = _FakeSession(response)

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(RuntimeError) as exc_info:
                await manager._validate_source_chain_id()

            msg = str(exc_info.value)
            assert "999999" in msg
            assert "unknown" in msg

    @pytest.mark.asyncio
    async def test_warns_on_rpc_error_response(self) -> None:
        """Should warn but not raise when RPC returns an error."""
        manager = _make_manager(chain="arbitrum")
        response = _FakeResponse(_make_error_response("method not found"))
        session = _FakeSession(response)

        with patch("aiohttp.ClientSession", return_value=session):
            # Should not raise -- just logs a warning
            await manager._validate_source_chain_id()

    @pytest.mark.asyncio
    async def test_warns_on_network_error(self) -> None:
        """Should warn but not raise on network errors."""
        manager = _make_manager(chain="arbitrum")

        # Simulate network error
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=ConnectionError("refused"))

        with patch("aiohttp.ClientSession", return_value=mock_session):
            # Should not raise -- just logs a warning
            await manager._validate_source_chain_id()

    @pytest.mark.asyncio
    async def test_all_supported_chains(self) -> None:
        """Validation should work for all supported chains."""
        for chain_name, chain_id in CHAIN_IDS.items():
            manager = _make_manager(chain=chain_name)
            response = _FakeResponse(_make_chain_id_response(chain_id))
            session = _FakeSession(response)

            with patch("aiohttp.ClientSession", return_value=session):
                await manager._validate_source_chain_id()


# =============================================================================
# Tests: Integration with start()
# =============================================================================


class TestStartValidation:
    """Tests that validation is called during start()."""

    @pytest.mark.asyncio
    async def test_start_calls_validate_before_anvil(self) -> None:
        """start() should call _validate_source_chain_id before starting Anvil."""
        manager = _make_manager(chain="base")

        call_order: list[str] = []

        async def mock_validate():
            call_order.append("validate")
            raise RuntimeError("Fork source RPC chain_id mismatch: expected 8453 (base) but got 42161 (arbitrum)")

        manager._validate_source_chain_id = mock_validate  # type: ignore[assignment]

        # start() should propagate the RuntimeError from validation
        result = await manager.start()
        assert result is False
        assert "validate" in call_order

    @pytest.mark.asyncio
    async def test_start_returns_false_on_chain_id_mismatch(self) -> None:
        """start() should return False when source chain ID mismatches."""
        manager = _make_manager(chain="base")
        response = _FakeResponse(_make_chain_id_response(42161))  # Wrong chain
        session = _FakeSession(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = await manager.start()
            assert result is False
