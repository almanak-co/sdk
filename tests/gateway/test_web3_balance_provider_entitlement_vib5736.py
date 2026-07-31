"""VIB-5736: Web3BalanceProvider fails FAST on a 401/403 auth/entitlement error.

Regression gate for the Robinhood (4663) crash-loop. Before the fix, a keyed
provider returning HTTP 403 ("chain not enabled on the Alchemy app") was treated
as a transient error: retried ``max_retries`` times every iteration, then raised
an opaque "after N attempts" message with no cause and no fix. Now:

* a 403/401 raises immediately (exactly ONE attempt — no wasted retries),
* the error names the chain, the method, and the ``ALMANAK_{CHAIN}_RPC_URL`` fix,
* a stale cache does NOT mask it (config error must surface, not serve stale),
* a genuinely transient error (5xx) still retries the full budget.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

from almanak.framework.data.interfaces import BalanceResult, DataSourceUnavailable
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.gateway.data.balance.web3_provider import (
    BalanceCacheEntry,
    RPCError,
    Web3BalanceProvider,
)

WALLET = "0x0000000000000000000000000000000000000001"
USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


class _HttpStatusError(Exception):
    """Mimics an aiohttp ClientResponseError from web3's async HTTPProvider."""

    def __init__(self, status: int):
        self.status = status
        super().__init__(f"{status}, message='error'")


def _resolver() -> MagicMock:
    resolver = MagicMock()

    def _resolve(token: str, chain: str) -> ResolvedToken:
        if token.upper() == "ETH":
            return ResolvedToken(
                symbol="ETH",
                address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
                decimals=18,
                chain="arbitrum",
                chain_id=42161,
                is_native=True,
                is_wrapped_native=False,
                source="static",
            )
        return ResolvedToken(
            symbol="USDC",
            address=USDC_ADDR,
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            is_native=False,
            is_wrapped_native=False,
            source="static",
        )

    resolver.resolve.side_effect = _resolve
    return resolver


def _provider() -> Web3BalanceProvider:
    return Web3BalanceProvider(
        rpc_url="https://arb-mainnet.g.alchemy.com/v2/key",
        wallet_address=WALLET,
        chain="arbitrum",
        token_resolver=_resolver(),
        retry_delay=0.0,
    )


def _patch_native(provider: Web3BalanceProvider, side_effect) -> AsyncMock:
    mock = AsyncMock(side_effect=side_effect)
    provider._w3 = MagicMock()
    provider._w3.eth.get_balance = mock
    return mock


def _patch_erc20(provider: Web3BalanceProvider, side_effect) -> AsyncMock:
    call_mock = AsyncMock(side_effect=side_effect)
    provider._w3 = MagicMock()
    contract = MagicMock()
    provider._w3.eth.contract.return_value = contract
    balance_of = MagicMock()
    contract.functions.balanceOf.return_value = balance_of
    balance_of.call = call_mock
    return call_mock


# --------------------------------------------------------------------------- #
# Fail-fast: exactly one attempt, actionable message
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_native_fails_fast_on_entitlement(status):
    provider = _provider()
    mock = _patch_native(provider, _HttpStatusError(status))

    with pytest.raises(RPCError) as ei:
        await provider._get_native_balance_with_retry()

    assert mock.call_count == 1, "entitlement error must NOT be retried"
    msg = str(ei.value)
    assert "auth/entitlement" in msg
    assert "eth_getBalance" in msg
    assert "ALMANAK_ARBITRUM_RPC_URL" in msg


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_erc20_fails_fast_on_entitlement(status):
    provider = _provider()
    mock = _patch_erc20(provider, _HttpStatusError(status))

    with pytest.raises(RPCError) as ei:
        await provider._get_erc20_balance_with_retry(USDC_ADDR)

    assert mock.call_count == 1, "entitlement error must NOT be retried"
    assert "balanceOf" in str(ei.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "expected_attempts"), [(403, 1), (503, 3)])
async def test_native_real_http_transport_uses_owned_retry_budget(monkeypatch, status, expected_attempts):
    """Prove AsyncWeb3 has no hidden retry layer ahead of our classifier."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    attempts = 0

    async def reject(_request):
        nonlocal attempts
        attempts += 1
        return web.Response(status=status, text="Forbidden" if status == 403 else "Unavailable")

    app = web.Application()
    app.router.add_post("/", reject)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        sockets = site._server.sockets  # type: ignore[union-attr]  # test-owned listener
        port = sockets[0].getsockname()[1]
        provider = Web3BalanceProvider(
            rpc_url=f"http://127.0.0.1:{port}/",
            wallet_address=WALLET,
            chain="arbitrum",
            token_resolver=_resolver(),
            retry_delay=0.0,
        )

        with pytest.raises(RPCError) as ei:
            await provider._get_native_balance_with_retry()

        assert attempts == expected_attempts
        if status == 403:
            assert "auth/entitlement" in str(ei.value)
        else:
            assert "after 3 attempts" in str(ei.value)
    finally:
        await runner.cleanup()


# --------------------------------------------------------------------------- #
# Contrast: a transient 5xx still exhausts the retry budget
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_transient_5xx_still_retries_full_budget():
    provider = _provider()
    mock = _patch_native(provider, _HttpStatusError(503))

    with pytest.raises(RPCError) as ei:
        await provider._get_native_balance_with_retry()

    assert mock.call_count == 3, "a transient 5xx must still use the full retry budget"
    assert "after 3 attempts" in str(ei.value)


# --------------------------------------------------------------------------- #
# get_balance: entitlement error is surfaced, NOT masked by stale cache
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_get_balance_does_not_serve_stale_on_entitlement():
    provider = _provider()
    # Prime an EXPIRED cache entry so the fresh-cache short-circuit misses and
    # the RPC path (which will 403) runs, but a stale entry IS available.
    stale = BalanceResult(
        balance=Decimal("42"),
        token="ETH",
        address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
        decimals=18,
        raw_balance=42 * 10**18,
        timestamp=datetime.now(UTC) - timedelta(seconds=100),
        stale=False,
    )
    provider._cache["ETH"] = BalanceCacheEntry(
        result=stale,
        cached_at=datetime.now(UTC) - timedelta(seconds=100),  # older than 5s TTL
    )
    _patch_native(provider, _HttpStatusError(403))

    with pytest.raises(DataSourceUnavailable) as ei:
        await provider.get_balance("ETH")

    # The stale $42 must NOT have been returned; the actionable message surfaces.
    assert "auth/entitlement" in str(ei.value)
    assert "ALMANAK_ARBITRUM_RPC_URL" in str(ei.value)
