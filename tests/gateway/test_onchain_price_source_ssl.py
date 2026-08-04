"""Tests for ChainlinkPriceSource SSL context initialization in _get_session()."""

import ssl
from unittest.mock import MagicMock, patch

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_get_session_uses_ssl_context():
    """_get_session() initializes aiohttp.ClientSession with build_ssl_context TCPConnector."""
    from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    source = ChainlinkPriceSource(chain="arbitrum", network="mainnet")

    with patch(
        "almanak.integrations.chainlink.gateway.live.build_ssl_context",
        return_value=fake_ctx,
    ) as mock_build:
        session = await source._get_session()

    mock_build.assert_called_once()
    assert source._session is not None
    assert not source._session.closed
    assert session is source._session

    assert session.connector is not None
    assert session.connector._ssl is fake_ctx

    await source.close()


@pytest.mark.asyncio
async def test_get_session_reuses_existing_open_session():
    """_get_session() returns the same session when called twice (lazy singleton)."""
    from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource

    fake_ctx = MagicMock(spec=ssl.SSLContext)
    source = ChainlinkPriceSource(chain="arbitrum", network="mainnet")

    with patch(
        "almanak.integrations.chainlink.gateway.live.build_ssl_context",
        return_value=fake_ctx,
    ) as mock_build:
        session1 = await source._get_session()
        session2 = await source._get_session()

    mock_build.assert_called_once()
    assert session1 is session2

    await source.close()
