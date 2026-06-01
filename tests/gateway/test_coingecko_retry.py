"""Tests for ``CoinGeckoPriceSource``'s 429 cooldown circuit breaker (VIB-4841).

PriceAggregator fans sources out concurrently via ``asyncio.gather`` and
waits for the slowest (``_fetch_all_sources`` has no timeout, no early
return). An earlier design retried a 429 once after a 1s ``asyncio.sleep``;
on a CoinGecko free-tier key (~10 calls/min ≈ 1 call / 6s) that 1s landed
back inside the same rate-limit window and re-throttled, *and* the sleep
stalled the whole aggregate behind this one source on every fetch.

Current contract (T1 — source-level cooldown, fail-fast, no sleep):

* On a 429, open an exponential-backoff cooldown window and raise
  ``DataSourceRateLimited`` immediately — no in-call sleep / retry — so the
  aggregator falls over to Binance / DexScreener / Chainlink instantly.
* While the cooldown window is open, subsequent calls fast-fail WITHOUT a
  network request.
* A successful 200 resets the breaker.

``asyncio.sleep`` must never be awaited on this path.
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
async def test_429_fails_fast_without_sleep():
    """A single 429 must raise ``DataSourceRateLimited`` immediately — one HTTP
    attempt, no ``asyncio.sleep``, no retry. The aggregator records the failed
    source and proceeds with the other 3 (Chainlink + Binance + DexScreener)
    without waiting on this rate-limited source."""
    source = CoinGeckoPriceSource(api_key="")  # free tier, no key

    responses = [(429, "Too Many Requests")]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        with pytest.raises(DataSourceRateLimited) as exc_info:
            await source.get_price("ETH", "USD")

    # Fail-fast: never sleeps, never retries.
    mock_sleep.assert_not_awaited()
    assert session.get.call_count == 1
    assert exc_info.value.source == "coingecko"
    # retry_after surfaces the computed backoff as advisory metadata.
    assert exc_info.value.retry_after == pytest.approx(1.0)
    # The cooldown window is now open.
    assert source._rate_limit_state.cooldown_remaining() > 0


@pytest.mark.asyncio
async def test_429_opens_cooldown_then_next_call_skips_network():
    """After a 429 opens the cooldown window, the immediate next call must
    fast-fail WITHOUT issuing a network request — and a call after the window
    expires hits the network again. This is the core circuit-breaker contract:
    no compounding 429s while cooling down, automatic recovery after expiry."""
    source = CoinGeckoPriceSource(api_key="")

    # First call: 429 opens the cooldown.
    patch_session_1, session_1 = _mock_session_with_responses(source, [(429, "Too Many Requests")])
    with (
        patch_session_1,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(DataSourceRateLimited):
            await source.get_price("ETH", "USD")
    assert session_1.get.call_count == 1

    # Second call while still in cooldown: must NOT hit the network at all.
    success_payload = {"ethereum": {"usd": 2350.0}}
    patch_session_2, session_2 = _mock_session_with_responses(source, [(200, success_payload)])
    with patch_session_2:
        with pytest.raises(DataSourceRateLimited):
            await source.get_price("ETH", "USD")
    # Zero network requests — fast-failed on the open cooldown window.
    assert session_2.get.call_count == 0
    assert source._metrics.cooldown_skips == 1

    # Force the window to expire, then the next call hits the network again.
    source._rate_limit_state.next_allowed_at = None
    patch_session_3, session_3 = _mock_session_with_responses(source, [(200, success_payload)])
    with patch_session_3:
        result = await source.get_price("ETH", "USD")
    assert result.price == Decimal("2350.0")
    assert session_3.get.call_count == 1
    # Success closed the breaker.
    assert source._rate_limit_state.cooldown_remaining() == 0
    assert source._rate_limit_state.consecutive_429s == 0


@pytest.mark.asyncio
async def test_429_with_stale_cache_returns_stale_not_raise():
    """A 429 with a stale cache entry returns the downgraded stale price rather
    than raising — the source still surfaces a usable (if stale) signal to the
    aggregator. The cooldown still opens so the *next* miss fast-fails."""
    from datetime import UTC, datetime, timedelta

    from almanak.framework.data.interfaces import PriceResult
    from almanak.gateway.data.price.coingecko import CacheEntry

    source = CoinGeckoPriceSource(api_key="")
    source._cache["ETH/USD"] = CacheEntry(
        result=PriceResult(
            price=Decimal("2400"),
            source="coingecko",
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
            confidence=1.0,
            stale=False,
        ),
        cached_at=datetime.now(UTC) - timedelta(seconds=60),  # expired TTL
    )

    patch_session, _ = _mock_session_with_responses(source, [(429, "Too Many Requests")])
    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        result = await source.get_price("ETH", "USD")

    assert result.price == Decimal("2400")
    assert result.stale is True
    mock_sleep.assert_not_awaited()
    # Cooldown still opened by the 429.
    assert source._rate_limit_state.cooldown_remaining() > 0


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
async def test_expired_cooldown_does_not_block_fresh_call():
    """Regression guard: stale rate-limit state (consecutive_429s > 0) with an
    already-elapsed cooldown window must NOT block or sleep on a fresh call.
    The first attempt fires immediately and a 200 closes the breaker."""
    source = CoinGeckoPriceSource(api_key="")

    # Simulate prior 429s, but force the cooldown window to be already expired.
    source._rate_limit_state.record_rate_limit()
    source._rate_limit_state.record_rate_limit()
    source._rate_limit_state.next_allowed_at = None  # window elapsed

    success_payload = {"ethereum": {"usd": 2350.0}}
    responses = [(200, success_payload)]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        result = await source.get_price("ETH", "USD")

    assert result.price == Decimal("2350.0")
    # No sleep — fail-fast design never sleeps on this path.
    mock_sleep.assert_not_awaited()
    # Single HTTP attempt; success resets the breaker.
    assert session.get.call_count == 1
    assert source._rate_limit_state.consecutive_429s == 0


# =============================================================================
# Address-endpoint orchestration (_try_fetch_by_address)
# =============================================================================
#
# The address endpoint follows the same bounded-retry shape as the main /simple/price
# path. These tests cover the orchestration in _try_fetch_by_address — the per-attempt
# logic lives in _attempt_address_fetch and inherits coverage via these calls.


@pytest.mark.asyncio
async def test_address_endpoint_429_fails_fast_without_sleep():
    """Address endpoint: a 429 must surface as ``DataSourceRateLimited``
    immediately — one HTTP attempt, no sleep — so the aggregator records the
    failure and falls over to other sources rather than the raise getting
    swallowed as "unknown token". Mirrors the ID-keyed path."""
    source = CoinGeckoPriceSource(api_key="")

    responses = [(429, "Too Many Requests")]
    patch_session, session = _mock_session_with_responses(source, responses)

    with (
        patch_session,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        with pytest.raises(DataSourceRateLimited):
            await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())

    mock_sleep.assert_not_awaited()
    assert session.get.call_count == 1
    assert source._rate_limit_state.cooldown_remaining() > 0


@pytest.mark.asyncio
async def test_address_endpoint_cooldown_skips_network():
    """Address endpoint: after a 429 opens the cooldown, the next address-keyed
    call must fast-fail without a network request."""
    source = CoinGeckoPriceSource(api_key="")

    patch_session_1, session_1 = _mock_session_with_responses(source, [(429, "Too Many Requests")])
    with (
        patch_session_1,
        patch("almanak.gateway.data.price.coingecko.asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(DataSourceRateLimited):
            await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())
    assert session_1.get.call_count == 1

    success_payload = {_CBBTC_ADDRESS.lower(): {"usd": 65000.12}}
    patch_session_2, session_2 = _mock_session_with_responses(source, [(200, success_payload)])
    with patch_session_2:
        with pytest.raises(DataSourceRateLimited):
            await source.get_price(_CBBTC_ADDRESS, "USD", resolved_token=_resolved_cbbtc())
    # No network request while cooling down.
    assert session_2.get.call_count == 0


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
