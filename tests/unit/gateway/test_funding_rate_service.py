"""Unit tests for ``_fetch_hyperliquid_rate`` and its module-private helpers.

VIB-4079 W2 Sub-C: lifts ``funding_rate_service.py`` Hyperliquid branch
coverage by exercising the post + parse pipeline against a mocked HTTP
client. Per the W2 audit, the function had zero direct test references
prior to this file.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from almanak.gateway.services.funding_rate_service import (
    FundingRateServiceServicer,
    _compute_hyperliquid_next_funding_time,
    _find_hyperliquid_coin_index,
)


def _make_settings() -> SimpleNamespace:
    return SimpleNamespace(network="mainnet")


def _patch_session_post(
    svc: FundingRateServiceServicer,
    *,
    status: int = 200,
    json_payload: object | None = None,
    json_side_effect: BaseException | None = None,
    post_side_effect: BaseException | None = None,
) -> MagicMock:
    """Patch ``svc._get_http_session`` so a single ``session.post(...)`` is mocked.

    Either supply a JSON payload (returned by ``response.json()``) or a side
    effect to raise from ``response.json()`` / from the ``post()`` call itself.
    """
    mock_response = MagicMock()
    mock_response.status = status
    if json_side_effect is not None:
        mock_response.json = AsyncMock(side_effect=json_side_effect)
    else:
        mock_response.json = AsyncMock(return_value=json_payload)

    post_cm = MagicMock()
    post_cm.__aenter__ = AsyncMock(return_value=mock_response)
    post_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    if post_side_effect is not None:
        mock_session.post = MagicMock(side_effect=post_side_effect)
    else:
        mock_session.post = MagicMock(return_value=post_cm)

    svc._get_http_session = AsyncMock(return_value=mock_session)  # type: ignore[method-assign]
    return mock_session


def _hyperliquid_response(
    *,
    coin: str = "ETH",
    funding: str | None = "0.00008",
    open_interest: str | None = "1000",
    mark_px: str | None = "3000",
    extra_universe: list[dict] | None = None,
) -> list:
    """Build a realistic 2-element Hyperliquid metaAndAssetCtxs response."""
    universe = list(extra_universe or [])
    universe.append({"name": coin})
    asset_ctxs = [{} for _ in range(len(universe) - 1)]
    asset_ctxs.append(
        {
            "funding": funding,
            "openInterest": open_interest,
            "markPx": mark_px,
        }
    )
    return [{"universe": universe}, asset_ctxs]


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────────────


class TestFindHyperliquidCoinIndex:
    def test_case_insensitive_match_skip_invalid_and_absent(self):
        """One pure-helper test covers case-folding, invalid-entry skip, and absence."""
        universe = [
            {"name": "BTC"},
            {"not_a_name_field": "junk"},  # invalid → skipped, no crash
            {"name": "eth"},  # match (case-insensitive)
            {"name": "SOL"},
        ]
        assert _find_hyperliquid_coin_index(universe, "ETH") == 2
        assert _find_hyperliquid_coin_index(universe, "MATIC") is None
        assert _find_hyperliquid_coin_index([], "ETH") is None


class TestComputeHyperliquidNextFundingTime:
    def test_advances_within_day_and_wraps_to_next_day(self):
        """Covers both branches: same-day next-window and end-of-day wrap."""
        # 03:30 UTC → next settlement at 08:00 same day.
        same_day = _compute_hyperliquid_next_funding_time(datetime(2026, 5, 6, 3, 30, tzinfo=UTC))
        assert same_day == datetime(2026, 5, 6, 8, 0, tzinfo=UTC)

        # 23:00 UTC → wraps to 00:00 next day.
        wrap = _compute_hyperliquid_next_funding_time(datetime(2026, 5, 6, 23, 0, tzinfo=UTC))
        assert wrap == datetime(2026, 5, 7, 0, 0, tzinfo=UTC)


# ──────────────────────────────────────────────────────────────────────────────
# _fetch_hyperliquid_rate — end-to-end with mocked HTTP
# ──────────────────────────────────────────────────────────────────────────────


class TestFetchHyperliquidRate:
    @pytest.mark.asyncio
    async def test_happy_path_returns_live_data_with_correct_url_and_payload(self):
        svc = FundingRateServiceServicer(_make_settings())
        body = _hyperliquid_response(coin="ETH", funding="0.00008", open_interest="1000", mark_px="3000")
        session = _patch_session_post(svc, status=200, json_payload=body)

        data = await svc._fetch_hyperliquid_rate("ETH-USD")

        # Live funding rate: 0.00008 / 8 = 0.00001
        assert data.is_live_data is True
        assert data.venue == "hyperliquid"
        assert data.market == "ETH-USD"
        assert data.rate_hourly == Decimal("0.00008") / Decimal("8")
        assert data.mark_price == Decimal("3000")
        assert data.index_price == Decimal("3000")
        # OI USD = 1000 * 3000 = 3_000_000; long = 52%, short = 48%
        assert data.open_interest_long == Decimal("3000000") * Decimal("0.52")
        assert data.open_interest_short == Decimal("3000000") * Decimal("0.48")

        # Caller invoked the documented Hyperliquid endpoint with the
        # metaAndAssetCtxs payload.
        url, _ = session.post.call_args[0], session.post.call_args[1]
        assert "api.hyperliquid.xyz/info" in url[0]
        assert session.post.call_args.kwargs["json"] == {"type": "metaAndAssetCtxs"}

    @pytest.mark.asyncio
    async def test_http_non_200_falls_back_to_default_rate(self):
        svc = FundingRateServiceServicer(_make_settings())
        _patch_session_post(svc, status=503, json_payload=None)

        data = await svc._fetch_hyperliquid_rate("ETH-USD")

        assert data.is_live_data is False
        # Default ETH-USD rate from DEFAULT_RATES["hyperliquid"]
        assert data.rate_hourly == Decimal("0.000015")
        # Default mark price for ETH-USD
        assert data.mark_price == Decimal("3000")

    @pytest.mark.asyncio
    async def test_transport_error_falls_back_to_default_rate(self):
        svc = FundingRateServiceServicer(_make_settings())
        _patch_session_post(svc, post_side_effect=aiohttp.ClientError("connect failed"))

        data = await svc._fetch_hyperliquid_rate("BTC-USD")

        assert data.is_live_data is False
        assert data.rate_hourly == Decimal("0.000011")  # default BTC-USD
        assert data.mark_price == Decimal("60000")

    @pytest.mark.asyncio
    async def test_malformed_json_falls_back_to_default_rate(self):
        svc = FundingRateServiceServicer(_make_settings())
        # Non-list payload → fails the `isinstance(data, list)` guard cleanly.
        _patch_session_post(svc, status=200, json_payload={"unexpected": "shape"})

        data = await svc._fetch_hyperliquid_rate("ETH-USD")

        assert data.is_live_data is False
        assert data.rate_hourly == Decimal("0.000015")  # default

    @pytest.mark.asyncio
    async def test_market_not_in_universe_falls_back_to_default_rate(self):
        svc = FundingRateServiceServicer(_make_settings())
        # Universe lists BTC but caller asks for ETH-USD.
        body = [
            {"universe": [{"name": "BTC"}]},
            [{"funding": "0.00008", "openInterest": "1", "markPx": "60000"}],
        ]
        _patch_session_post(svc, status=200, json_payload=body)

        data = await svc._fetch_hyperliquid_rate("ETH-USD")

        assert data.is_live_data is False
        assert data.rate_hourly == Decimal("0.000015")  # default ETH-USD

    @pytest.mark.asyncio
    async def test_invalid_asset_ctx_falls_back_to_default_rate(self):
        svc = FundingRateServiceServicer(_make_settings())
        # Universe matches ETH, but the asset context is not validatable
        # (Pydantic accepts unknown shapes by default, so force a hard failure
        # by giving a non-dict that will raise during ``model_validate``).
        body = [
            {"universe": [{"name": "ETH"}]},
            ["this-is-not-a-dict"],
        ]
        _patch_session_post(svc, status=200, json_payload=body)

        data = await svc._fetch_hyperliquid_rate("ETH-USD")

        # Validation failure → ctx is empty → defaults preserved → not live.
        assert data.is_live_data is False
        assert data.rate_hourly == Decimal("0.000015")
        assert data.mark_price == Decimal("3000")
