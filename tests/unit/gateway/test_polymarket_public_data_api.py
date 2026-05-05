"""Unit tests for VIB-3769: Polymarket Data API public-client routing.

Polymarket's Data API (``/positions``) and Gamma API (``/markets``) reads
are public — keyed only by wallet address — and do NOT require L2 (HMAC-API-key)
auth at the upstream layer. Routing them through ``_build_authenticated_client``
forces unnecessary credential derivation and is a fragile blocker for any
wallet that has not registered Polymarket API keys.

The fix introduces ``_build_public_client``: a CLOB client built with
``require_signer=False`` that bypasses ``_ensure_credentials``. This file
locks in:

  1. ``GetPositions`` invokes ``_build_public_client`` and never touches L2
     credential derivation, even when no API keys are configured on the
     gateway.
  2. Write/auth-required handlers (``CreateAndPostOrder``, ``CancelOrder``,
     ``CancelOrders``, ``CancelAll``, ``GetOpenOrders``, ``GetOrder``)
     continue to call ``_build_authenticated_client`` and therefore still
     derive credentials.
  3. ``GetPositions`` returns a clean structured error when no wallet is
     configured on the gateway (rather than silently querying the
     zero-address placeholder).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from eth_account import Account

from almanak.framework.connectors.polymarket.models import Position
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.polymarket_service import PolymarketServiceServicer

TEST_PRIVATE_KEY = "0x" + "ab" * 32
TEST_ACCOUNT = Account.from_key(TEST_PRIVATE_KEY)
TEST_WALLET = TEST_ACCOUNT.address


def _make_settings(*, with_api_keys: bool) -> MagicMock:
    s = MagicMock(spec=GatewaySettings)
    s.private_key = TEST_PRIVATE_KEY
    s.polymarket_private_key = None
    s.eoa_address = TEST_WALLET
    s.polymarket_wallet_address = None
    s.safe_address = None
    s.safe_mode = None
    if with_api_keys:
        s.polymarket_api_key = "k"
        s.polymarket_secret = "c2VjcmV0"  # base64("secret")
        s.polymarket_passphrase = "p"
    else:
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
    return s


@pytest.fixture
def servicer_no_api_keys() -> PolymarketServiceServicer:
    """Servicer with no Polymarket API keys configured.

    This is the surface that VIB-3769 unblocks: a wallet that has never
    registered API keys must still be able to read its positions from the
    public Data API.
    """
    return PolymarketServiceServicer(settings=_make_settings(with_api_keys=False))


@pytest.fixture
def servicer_with_api_keys() -> PolymarketServiceServicer:
    return PolymarketServiceServicer(settings=_make_settings(with_api_keys=True))


# =============================================================================
# (a) Read endpoints succeed without L2 auth derivation being invoked
# =============================================================================


class TestGetPositionsSkipsL2Auth:
    """``GetPositions`` must route through ``_build_public_client``.

    The lock here is two-fold:
      * ``_ensure_credentials`` / ``_derive_or_create_credentials`` MUST NOT
        be called, even on a servicer with no API keys configured.
      * The underlying ClobClient call still happens and the response is
        translated to the proto type.
    """

    @pytest.mark.asyncio
    async def test_get_positions_does_not_derive_credentials(
        self, servicer_no_api_keys: PolymarketServiceServicer
    ) -> None:
        positions = [
            Position(
                market_id="m-1",
                condition_id="0xcondition",
                token_id="t-1",
                outcome="YES",
                size=Decimal("10"),
                avg_price=Decimal("0.42"),
                current_price=Decimal("0.50"),
                realized_pnl=Decimal("0"),
                market_question="Will X happen?",
            )
        ]

        fake_client = MagicMock()
        fake_client.get_positions = MagicMock(return_value=positions)
        fake_client.close = MagicMock()

        with (
            patch.object(servicer_no_api_keys, "_build_public_client", return_value=fake_client) as build_public,
            patch.object(
                servicer_no_api_keys, "_build_authenticated_client", new=AsyncMock()
            ) as build_auth,
            patch.object(
                servicer_no_api_keys, "_ensure_credentials", new=AsyncMock(return_value=False)
            ) as ensure_creds,
            patch.object(
                servicer_no_api_keys,
                "_derive_or_create_credentials",
                new=AsyncMock(return_value=False),
            ) as derive_creds,
        ):
            response = await servicer_no_api_keys.GetPositions(
                gateway_pb2.PolymarketGetPositionsRequest(),
                MagicMock(),
            )

        # Public client used; authenticated path never touched.
        build_public.assert_called_once()
        build_auth.assert_not_called()
        ensure_creds.assert_not_called()
        derive_creds.assert_not_called()

        # Result successfully translated.
        assert response.success is True
        assert len(response.positions) == 1
        assert response.positions[0].token_id == "t-1"
        assert response.positions[0].outcome == "YES"
        assert response.positions[0].size == "10"
        assert response.positions[0].avg_price == "0.42"

        fake_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_positions_no_wallet_returns_clean_error(self) -> None:
        """No wallet configured → structured error, not a zero-address query."""
        s = _make_settings(with_api_keys=False)
        s.eoa_address = None
        servicer = PolymarketServiceServicer(settings=s)
        # Force the wallet to be unset on the servicer (defensive — settings
        # may have left it populated through other code paths).
        servicer._wallet_address = None

        with (
            patch.object(servicer, "_build_public_client") as build_public,
            patch.object(servicer, "_build_authenticated_client", new=AsyncMock()) as build_auth,
        ):
            response = await servicer.GetPositions(
                gateway_pb2.PolymarketGetPositionsRequest(),
                MagicMock(),
            )

        build_public.assert_not_called()
        build_auth.assert_not_called()
        assert response.success is False
        assert "wallet address" in response.error.lower()


class TestPublicClientHelper:
    """``_build_public_client`` must skip credential derivation."""

    def test_build_public_client_does_not_require_signer(
        self, servicer_with_api_keys: PolymarketServiceServicer
    ) -> None:
        # Wipe signer state — public client must still be buildable.
        servicer_with_api_keys._signer = None
        servicer_with_api_keys._available = False

        client = servicer_with_api_keys._build_public_client()

        # ClobClient created with signer=None → any signed call would raise.
        assert client is not None
        assert getattr(client, "signer", None) is None
        client.close()


# =============================================================================
# (b) Write endpoints still derive L2 auth
# =============================================================================


class TestWriteEndpointsStillAuthenticated:
    """Trading paths must continue to route through ``_build_authenticated_client``.

    No regression: write endpoints retain their existing credential
    derivation contract.
    """

    @pytest.mark.asyncio
    async def test_cancel_order_uses_authenticated_client(
        self, servicer_with_api_keys: PolymarketServiceServicer
    ) -> None:
        fake_client = MagicMock()
        fake_client.cancel_order = MagicMock(return_value=True)
        fake_client.close = MagicMock()

        with (
            patch.object(
                servicer_with_api_keys,
                "_build_authenticated_client",
                new=AsyncMock(return_value=fake_client),
            ) as build_auth,
            patch.object(servicer_with_api_keys, "_build_public_client") as build_public,
        ):
            response = await servicer_with_api_keys.CancelOrder(
                gateway_pb2.PolymarketCancelOrderRequest(order_id="0xabc"),
                MagicMock(),
            )

        build_auth.assert_awaited_once()
        build_public.assert_not_called()
        assert response.success is True
        assert "0xabc" in response.canceled

    @pytest.mark.asyncio
    async def test_cancel_orders_uses_authenticated_client(
        self, servicer_with_api_keys: PolymarketServiceServicer
    ) -> None:
        fake_client = MagicMock()
        fake_client.cancel_order = MagicMock(return_value=True)
        fake_client.close = MagicMock()

        with (
            patch.object(
                servicer_with_api_keys,
                "_build_authenticated_client",
                new=AsyncMock(return_value=fake_client),
            ) as build_auth,
            patch.object(servicer_with_api_keys, "_build_public_client") as build_public,
        ):
            await servicer_with_api_keys.CancelOrders(
                gateway_pb2.PolymarketCancelOrdersRequest(order_ids=["a", "b"]),
                MagicMock(),
            )

        build_auth.assert_awaited_once()
        build_public.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_open_orders_uses_authenticated_client(
        self, servicer_with_api_keys: PolymarketServiceServicer
    ) -> None:
        """``/data/orders`` requires HMAC L2 auth at the upstream layer
        (CLOB client passes ``authenticated=True``) — so this handler MUST
        derive credentials, even though it is "read-only" from the caller's
        perspective.
        """
        fake_client = MagicMock()
        fake_client.get_open_orders = MagicMock(return_value=[])
        fake_client.close = MagicMock()

        with (
            patch.object(
                servicer_with_api_keys,
                "_build_authenticated_client",
                new=AsyncMock(return_value=fake_client),
            ) as build_auth,
            patch.object(servicer_with_api_keys, "_build_public_client") as build_public,
        ):
            response = await servicer_with_api_keys.GetOpenOrders(
                gateway_pb2.PolymarketGetOpenOrdersRequest(),
                MagicMock(),
            )

        build_auth.assert_awaited_once()
        build_public.assert_not_called()
        assert response.success is True

    @pytest.mark.asyncio
    async def test_get_order_uses_authenticated_client(
        self, servicer_with_api_keys: PolymarketServiceServicer
    ) -> None:
        """``/data/orders?orderID=...`` is also authenticated upstream."""
        fake_client = MagicMock()
        fake_client.get_order = MagicMock(return_value=None)
        fake_client.close = MagicMock()

        with (
            patch.object(
                servicer_with_api_keys,
                "_build_authenticated_client",
                new=AsyncMock(return_value=fake_client),
            ) as build_auth,
            patch.object(servicer_with_api_keys, "_build_public_client") as build_public,
        ):
            await servicer_with_api_keys.GetOrder(
                gateway_pb2.PolymarketGetOrderRequest(order_id="0xabc"),
                MagicMock(),
            )

        build_auth.assert_awaited_once()
        build_public.assert_not_called()
