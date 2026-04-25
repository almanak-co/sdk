"""Tests for BaseIntegration SSL context initialization in _get_session()."""

import ssl
from unittest.mock import MagicMock, patch

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_get_session_uses_ssl_context():
    """_get_session() initializes aiohttp.ClientSession with build_ssl_context TCPConnector."""
    from almanak.gateway.integrations.binance import BinanceIntegration

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    integration = BinanceIntegration()

    try:
        with patch(
            "almanak.gateway.integrations.base.build_ssl_context",
            return_value=fake_ctx,
        ) as mock_build:
            session = await integration._get_session()

        mock_build.assert_called_once()
        assert integration._session is not None
        assert not integration._session.closed
        assert session is integration._session

        assert isinstance(session.timeout, aiohttp.ClientTimeout)
        assert session.connector is not None
        assert session.connector._ssl is fake_ctx
    finally:
        await integration.close()


@pytest.mark.asyncio
async def test_get_session_reuses_existing_open_session():
    """_get_session() returns the same session when called twice (lazy singleton)."""
    from almanak.gateway.integrations.binance import BinanceIntegration

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    integration = BinanceIntegration()

    try:
        with patch(
            "almanak.gateway.integrations.base.build_ssl_context",
            return_value=fake_ctx,
        ) as mock_build:
            session1 = await integration._get_session()
            session2 = await integration._get_session()

        mock_build.assert_called_once()
        assert session1 is session2
    finally:
        await integration.close()


@pytest.mark.asyncio
async def test_get_session_creates_new_session_after_close():
    """After close(), _get_session() creates a fresh SSL-armed session."""
    from almanak.gateway.integrations.binance import BinanceIntegration

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    integration = BinanceIntegration()

    session2 = None
    try:
        with patch(
            "almanak.gateway.integrations.base.build_ssl_context",
            return_value=fake_ctx,
        ):
            session1 = await integration._get_session()
            await integration.close()
            assert integration._session is None

            session2 = await integration._get_session()

        assert session2 is not session1
        assert not session2.closed
        assert session2.connector is not None
        assert session2.connector._ssl is fake_ctx
    finally:
        await integration.close()
