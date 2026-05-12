"""Tests for ``CoinGeckoPriceSource``'s bounded retry on 429.

PriceAggregator fans sources out concurrently via ``asyncio.gather`` and
waits for the slowest. Before this fix, CoinGecko's internal exponential
backoff slept inside ``get_price`` for up to ~10s, blocking every other
source's already-cached answer and pushing ``decide()`` past the
framework's 30s ``decide_timeout_seconds`` ceiling.

Current contract: on a 429, retry **once** after a bounded 1s pause. If
the retry also fails, raise ``DataSourceRateLimited`` so the aggregator
falls over to the other sources. Behaviour mirrors Binance / DexScreener
/ OnChain — all fail-fast peers in the aggregator.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.core.enums import Chain
from almanak.framework.data.interfaces import DataSourceRateLimited, DataSourceUnavailable
from almanak.framework.data.tokens import ResolvedToken
from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource

# cbBTC on Base — intentionally not in the static registry, so a get_price
# call with a ResolvedToken forces routing through _try_fetch_by_address.
_CBBTC_ADDRESS = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"


def _resolved_cbbtc() -> ResolvedToken:
    return ResolvedToken(
        symbol="cbBTC",
        address=_CBBTC_ADDRESS,
        decimals=8,
        chain=Chain.BASE,
        chain_id=8453,
        source="on_chain",
        is_verified=False,
    )


def _mock_session_with_responses(
    source: CoinGeckoPriceSource, responses: Sequence[tuple[int, dict | str]]
) -> tuple[Any, MagicMock]:
    """Patch ``_get_session`` so successive ``session.get(...)`` calls return
    the supplied ``(status, payload)`` pairs in order. Returns
    ``(patch_context_manager, session_mock)`` — callers ``with patch_cm:``
    to activate, and inspect ``session_mock.get.call_count`` to assert on
    the number of HTTP attempts."""

    def _make_cm(status: int, payload):
        resp = MagicMock()
        resp.status = status
        resp.json = AsyncMock(return_value=payload if isinstance(payload, dict) else {})
        resp.text = AsyncMock(return_value=payload if isinstance(payload, str) else "")
        cm = AsyncMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    context_managers = [_make_cm(status, payload) for status, payload in responses]
    session = MagicMock()
    session.get = MagicMock(side_effect=context_managers)
    patch_cm = patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)
    return patch_cm, session


@pytest.mark.asyncio
async def test_429_then_200_retries_once_and_succeeds():
    """First call returns 429, second call returns 200 — source should retry
    after a 1s pause and return the successful price. This is the path that
    proves the fix actually recovers when CoinGecko's per-minute window
    rolls over within the retry budget."""
    source = CoinGeckoPriceSource(api_key="")  # free tier, no key

    success_payload = {"ethereum": {"usd": 2350.0}}
    responses = [(429, "Too Many Requests"), (200, success_payload)]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        result = await source.get_price("ETH", "USD")

    assert result.price == Decimal("2350.0")
    assert result.source == "coingecko"
    # Critical: exactly one 1s sleep between the two attempts.
    mock_sleep.assert_awaited_once_with(1.0)
    # And exactly two HTTP attempts (initial + one retry).
    assert session.get.call_count == 2


@pytest.mark.asyncio
async def test_429_twice_raises_rate_limited():
    """Both attempts return 429 — second 429 must surface as
    ``DataSourceRateLimited`` so the aggregator records it as a failed
    source and proceeds with the other 3 (Chainlink + Binance + DexScreener)."""
    source = CoinGeckoPriceSource(api_key="")

    responses = [(429, "Too Many Requests"), (429, "Too Many Requests")]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        with pytest.raises(DataSourceRateLimited) as exc_info:
            await source.get_price("ETH", "USD")

    # One bounded retry, then give up — no compounding backoff.
    mock_sleep.assert_awaited_once_with(1.0)
    # Exactly two HTTP attempts before giving up — not three, not one.
    assert session.get.call_count == 2
    assert exc_info.value.source == "coingecko"


@pytest.mark.asyncio
async def test_200_on_first_attempt_does_not_sleep():
    """Happy path: 200 on the first attempt. No retry, no sleep — the
    aggregator's gather call should resolve in a single HTTP roundtrip."""
    source = CoinGeckoPriceSource(api_key="")

    success_payload = {"ethereum": {"usd": 2350.0}}
    responses = [(200, success_payload)]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        result = await source.get_price("ETH", "USD")

    assert result.price == Decimal("2350.0")
    mock_sleep.assert_not_awaited()
    # Exactly one HTTP attempt — no retry on success.
    assert session.get.call_count == 1


@pytest.mark.asyncio
async def test_429_then_200_does_not_proactively_sleep_on_first_attempt():
    """Regression guard: the source used to sleep *before* the first request
    if a prior call had been rate-limited. That proactive sleep blocked the
    aggregator's concurrent gather on every subsequent call. Now the only
    sleep happens *between* attempt 1 and attempt 2 of a single call —
    nothing carries over to a fresh ``get_price`` invocation."""
    source = CoinGeckoPriceSource(api_key="")

    # Simulate a previously rate-limited state (would have caused a proactive
    # pre-request sleep in the old behaviour).
    source._rate_limit_state.record_rate_limit()
    source._rate_limit_state.record_rate_limit()
    # backoff_seconds is now ~2s and get_wait_time() would have returned >0.

    success_payload = {"ethereum": {"usd": 2350.0}}
    responses = [(200, success_payload)]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        result = await source.get_price("ETH", "USD")

    assert result.price == Decimal("2350.0")
    # No proactive sleep — first attempt fires immediately even with
    # stale rate-limit state. Aggregator's gather is no longer stalled.
    mock_sleep.assert_not_awaited()
    # Single HTTP attempt — the prior 429 state from earlier calls must
    # not cause a redundant retry on a fresh successful first response.
    assert session.get.call_count == 1


# =============================================================================
# Address-endpoint orchestration (_try_fetch_by_address)
# =============================================================================
#
# The address endpoint follows the same bounded-retry shape as the main /simple/price
# path. These tests cover the orchestration in _try_fetch_by_address — the per-attempt
# logic lives in _attempt_address_fetch and inherits coverage via these calls.


@pytest.mark.asyncio
async def test_address_endpoint_429_then_200_retries_once():
    """Address endpoint: first attempt 429, second attempt 200 — must retry
    once with a 1s pause and return the successful price. Mirrors the
    ID-keyed path's 429→200 behaviour so the aggregator stays unblocked."""
    source = CoinGeckoPriceSource(api_key="")

    success_payload = {_CBBTC_ADDRESS.lower(): {"usd": 65000.12}}
    responses = [(429, "Too Many Requests"), (200, success_payload)]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        result = await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())

    assert result.price == Decimal("65000.12")
    assert result.source == "coingecko"
    mock_sleep.assert_awaited_once_with(1.0)
    assert session.get.call_count == 2


@pytest.mark.asyncio
async def test_address_endpoint_429_twice_raises_rate_limited():
    """Address endpoint: both attempts 429 — must surface as
    ``DataSourceRateLimited`` after one bounded retry so the aggregator
    records the failure and falls over to other sources rather than the
    raise getting swallowed as "unknown token"."""
    source = CoinGeckoPriceSource(api_key="")

    responses = [(429, "Too Many Requests"), (429, "Too Many Requests")]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        with pytest.raises(DataSourceRateLimited):
            await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())

    mock_sleep.assert_awaited_once_with(1.0)
    assert session.get.call_count == 2


@pytest.mark.asyncio
async def test_address_endpoint_timeout_raises_unavailable_with_no_cache():
    """Address endpoint TimeoutError with no prior cache: must raise
    ``DataSourceUnavailable`` so the aggregator can fall over cleanly,
    not silently become "Unknown token" which bypasses health tracking."""
    source = CoinGeckoPriceSource(api_key="")

    session = MagicMock()
    session.get = MagicMock(side_effect=TimeoutError())
    patch_session = patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)

    with patch_session, pytest.raises(DataSourceUnavailable, match="timeout"):
        await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())


@pytest.mark.asyncio
async def test_address_endpoint_client_error_raises_unavailable_with_no_cache():
    """Address endpoint aiohttp.ClientError with no prior cache: must raise
    ``DataSourceUnavailable``. Same fall-over contract as TimeoutError but
    classified separately in metrics (errors vs timeouts)."""
    source = CoinGeckoPriceSource(api_key="")

    session = MagicMock()
    session.get = MagicMock(side_effect=aiohttp.ClientError("connection reset"))
    patch_session = patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)

    with patch_session, pytest.raises(DataSourceUnavailable, match="connection reset"):
        await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())
