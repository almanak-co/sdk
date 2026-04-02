"""Tests for RpcService SSL context initialization in _get_session()."""

import ssl
from unittest.mock import MagicMock, patch

import aiohttp
import pytest


@pytest.fixture
def settings():
    mock = MagicMock()
    mock.network = "mainnet"
    return mock


@pytest.mark.asyncio
async def test_get_session_uses_ssl_context(settings):
    """_get_session() initializes aiohttp.ClientSession with build_ssl_context TCPConnector."""
    from almanak.gateway.services.rpc_service import RpcServiceServicer

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    service = RpcServiceServicer(settings=settings)

    # build_ssl_context is lazily imported inside _get_session, so patch at source
    with patch(
        "almanak.gateway.utils.ssl_context.build_ssl_context",
        return_value=fake_ctx,
    ) as mock_build:
        session = await service._get_session()

    mock_build.assert_called_once()
    assert service._session is not None
    assert not service._session.closed
    assert session is service._session

    # Verify timeout is configured
    assert isinstance(session.timeout, aiohttp.ClientTimeout)
    assert session.timeout.total == 30.0

    # Verify the TCPConnector received the SSL context returned by build_ssl_context
    assert session.connector is not None
    assert session.connector._ssl is fake_ctx

    await service.close()


@pytest.mark.asyncio
async def test_get_session_reuses_existing_open_session(settings):
    """_get_session() returns the same session when called twice (lazy singleton)."""
    from almanak.gateway.services.rpc_service import RpcServiceServicer

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    service = RpcServiceServicer(settings=settings)

    with patch(
        "almanak.gateway.utils.ssl_context.build_ssl_context",
        return_value=fake_ctx,
    ) as mock_build:
        session1 = await service._get_session()
        session2 = await service._get_session()

    # build_ssl_context must only be called once — session is reused
    mock_build.assert_called_once()
    assert session1 is session2

    await service.close()
