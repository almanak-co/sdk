"""Coin-id resolution tests for the backtesting CoinGecko provider.

The hardcoded ``TOKEN_IDS`` symbol->coin-id allowlist was removed. Coin ids are
now resolved dynamically:

- **Native** gas / wrapped-native symbols -> chain-registry coin id, zero HTTP.
- **ERC20** symbols -> the CoinGecko contract-address endpoint
  ``/coins/{asset_platform}/contract/{address}``, returning the chain-specific
  (BRIDGED) coin id the contract maps to. That bridged id is the asset actually
  traded and is returned deliberately (not a canonical/mainnet id).

These tests mock the provider's single egress point (``_make_request``) so no
live network call is made. They pin: native zero-HTTP resolution, address-backed
resolution to the bridged id, honest misses (unknown symbol / contract 404 /
asset-platform absent) producing NO fabricated price, in-memory caching, and the
transient-vs-miss distinction (429 propagates, never cached as a miss).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.coingecko import (
    CoinGeckoDataProvider,
    CoinGeckoRateLimitError,
    RetryConfig,
    _is_auth_error,
    _is_transient_request_error,
)

# Live-verified: this wstETH contract on Arbitrum resolves to the chain-specific
# BRIDGED coin id (not "wrapped-steth"). Pins the "use the bridged id" decision.
_WSTETH_ARB_ADDRESS = "0x5979D7b546E38E414F7E9822514be443A4800529"
_WSTETH_ARB_BRIDGED_ID = "arbitrum-bridged-wsteth-arbitrum"
_USDC_ARB_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
_TS = datetime(2024, 1, 15, tzinfo=UTC)


def _fast_retry() -> RetryConfig:
    return RetryConfig(max_retries=1, base_delay=0.01, max_delay=0.02)


class TestNativeResolution:
    @pytest.mark.asyncio
    async def test_native_resolves_with_zero_http(self) -> None:
        """A native symbol resolves via the registry projection, no _make_request."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        with patch.object(provider, "_make_request", new_callable=AsyncMock) as req:
            coin_id = await provider._resolve_token_id("WETH")
        assert coin_id == "weth"
        req.assert_not_called()
        await provider.close()

    @pytest.mark.asyncio
    async def test_native_resolution_is_case_insensitive(self) -> None:
        """Native lookup matches verbatim-case registry keys case-insensitively."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        with patch.object(provider, "_make_request", new_callable=AsyncMock) as req:
            assert await provider._resolve_token_id("weth") == "weth"
            assert await provider._resolve_token_id("Eth") == "ethereum"
        req.assert_not_called()
        await provider.close()


class TestAddressBackedResolution:
    @pytest.mark.asyncio
    async def test_address_resolves_to_bridged_id(self) -> None:
        """An address-backed symbol resolves via the contract endpoint to the BRIDGED id."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"wstETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            return_value={"id": _WSTETH_ARB_BRIDGED_ID},
        ) as req:
            coin_id = await provider._resolve_token_id("WSTETH")

        assert coin_id == _WSTETH_ARB_BRIDGED_ID
        req.assert_awaited_once()
        # Hit the chain-scoped contract endpoint (arbitrum-one platform id).
        endpoint = req.await_args.args[0]
        assert endpoint == f"/coins/arbitrum-one/contract/{_WSTETH_ARB_ADDRESS}"

    @pytest.mark.asyncio
    async def test_resolution_is_cached_no_extra_http(self) -> None:
        """The second resolution of the same (chain, address) makes zero extra HTTP."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"wstETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            return_value={"id": _WSTETH_ARB_BRIDGED_ID},
        ) as req:
            first = await provider._resolve_token_id("WSTETH")
            second = await provider._resolve_token_id("WSTETH")

        assert first == second == _WSTETH_ARB_BRIDGED_ID
        assert req.await_count == 1  # cached after first call

    @pytest.mark.asyncio
    async def test_usdc_resolves_by_address(self) -> None:
        """USDC (absent from the native map) resolves by address now, not an allowlist."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"USDC": ("arbitrum", _USDC_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            return_value={"id": "usd-coin"},
        ) as req:
            coin_id = await provider._resolve_token_id("USDC")

        assert coin_id == "usd-coin"
        assert req.await_args.args[0] == f"/coins/arbitrum-one/contract/{_USDC_ARB_ADDRESS}"


class TestHonestMisses:
    @pytest.mark.asyncio
    async def test_unknown_symbol_no_address_is_miss(self) -> None:
        """A symbol that is neither native nor address-backed resolves to None (miss)."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        with patch.object(provider, "_make_request", new_callable=AsyncMock) as req:
            coin_id = await provider._resolve_token_id("FOOBAR")
        assert coin_id is None
        req.assert_not_called()
        await provider.close()

    @pytest.mark.asyncio
    async def test_get_price_unknown_symbol_raises_no_fabricated_price(self) -> None:
        """get_price on an unknown token raises ValueError - never a fabricated $1/$0."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        with patch.object(provider, "_make_request", new_callable=AsyncMock):
            with pytest.raises(ValueError, match="Unknown token"):
                await provider.get_price("FOOBAR", _TS)
        await provider.close()

    @pytest.mark.asyncio
    async def test_contract_404_is_honest_miss(self) -> None:
        """A genuine 'contract not found' (ValueError from _make_request) -> None, no price."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=ValueError("CoinGecko API error 404: coin not found"),
        ):
            coin_id = await provider._resolve_token_id("WSTETH")
        assert coin_id is None

    @pytest.mark.asyncio
    async def test_contract_404_makes_get_price_raise(self) -> None:
        """A contract 404 surfaces at get_price as ValueError, not a fabricated price."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=ValueError("CoinGecko API error 404: coin not found"),
        ):
            with pytest.raises(ValueError, match="Unknown token"):
                await provider.get_price("WSTETH", _TS)

    @pytest.mark.asyncio
    async def test_external_id_none_is_miss_no_price(self) -> None:
        """A chain CoinGecko does not index (external_id_for -> None) is an honest miss."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"TKN": ("nonexistent-chain", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(provider, "_make_request", new_callable=AsyncMock) as req:
            coin_id = await provider._resolve_token_id("TKN")
        assert coin_id is None
        # No contract call attempted when the asset platform is unknown.
        req.assert_not_called()
        await provider.close()


class TestTransientErrorsPropagate:
    @pytest.mark.asyncio
    async def test_rate_limit_propagates_not_a_miss(self) -> None:
        """A 429 during resolution propagates as CoinGeckoRateLimitError, never a miss."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=CoinGeckoRateLimitError("rate limited", retry_count=1),
        ):
            with pytest.raises(CoinGeckoRateLimitError):
                await provider._resolve_token_id("WSTETH")

        # A transient failure must NOT be cached as a resolution.
        assert ("arbitrum", _WSTETH_ARB_ADDRESS.lower()) not in provider._coin_id_cache

    @pytest.mark.asyncio
    async def test_rate_limit_does_not_poison_cache(self) -> None:
        """After a 429, a subsequent success still resolves and caches normally."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=[
                CoinGeckoRateLimitError("rate limited"),
                {"id": _WSTETH_ARB_BRIDGED_ID},
            ],
        ):
            with pytest.raises(CoinGeckoRateLimitError):
                await provider._resolve_token_id("WSTETH")
            coin_id = await provider._resolve_token_id("WSTETH")

        assert coin_id == _WSTETH_ARB_BRIDGED_ID

    @pytest.mark.asyncio
    async def test_timeout_valueerror_propagates_not_a_miss(self) -> None:
        """A timeout surfaced as `ValueError(...) from TimeoutError` propagates (cause-type path)."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        timeout_err = ValueError("Request timed out after 30s")
        timeout_err.__cause__ = TimeoutError()
        with patch.object(provider, "_make_request", new_callable=AsyncMock, side_effect=timeout_err):
            with pytest.raises(ValueError, match="timed out"):
                await provider._resolve_token_id("WSTETH")
        # Transient: must NOT be cached as a resolution / treated as a miss.
        assert ("arbitrum", _WSTETH_ARB_ADDRESS.lower()) not in provider._coin_id_cache

    @pytest.mark.asyncio
    async def test_network_valueerror_propagates_not_a_miss(self) -> None:
        """A network error surfaced as a bare ValueError propagates (message-marker fallback)."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=ValueError("Network error: connection reset"),
        ):
            with pytest.raises(ValueError, match="Network error"):
                await provider._resolve_token_id("WSTETH")
        assert ("arbitrum", _WSTETH_ARB_ADDRESS.lower()) not in provider._coin_id_cache

    @pytest.mark.asyncio
    async def test_http_5xx_propagates_not_a_miss(self) -> None:
        """A CoinGecko 5xx (transient server outage) propagates, not an honest miss."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=ValueError("CoinGecko API error 503: service unavailable"),
        ):
            with pytest.raises(ValueError, match="503"):
                await provider._resolve_token_id("WSTETH")
        assert ("arbitrum", _WSTETH_ARB_ADDRESS.lower()) not in provider._coin_id_cache

    @pytest.mark.asyncio
    async def test_auth_401_propagates_not_a_miss(self) -> None:
        """A 401/403 (bad/expired key, plan access) fails loudly, not a per-token miss."""
        for status in (401, 403):
            provider = CoinGeckoDataProvider(
                retry_config=_fast_retry(),
                token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
            )
            with patch.object(
                provider,
                "_make_request",
                new_callable=AsyncMock,
                side_effect=ValueError(f"CoinGecko API error {status}: unauthorized"),
            ):
                with pytest.raises(ValueError, match=str(status)):
                    await provider._resolve_token_id("WSTETH")
            assert ("arbitrum", _WSTETH_ARB_ADDRESS.lower()) not in provider._coin_id_cache


class TestIsAuthError:
    """A 401/403 is provider misconfiguration, not a token miss."""

    def test_401_and_403_are_auth_errors(self) -> None:
        assert _is_auth_error(ValueError("CoinGecko API error 401: bad key")) is True
        assert _is_auth_error(ValueError("CoinGecko API error 403: forbidden")) is True

    def test_404_400_and_no_data_are_not_auth_errors(self) -> None:
        assert _is_auth_error(ValueError("CoinGecko API error 404: coin not found")) is False
        assert _is_auth_error(ValueError("CoinGecko API error 400: bad address")) is False
        assert _is_auth_error(ValueError("Unknown token: WSTETH")) is False


class TestIsTransientRequestError:
    """Direct coverage of the transient-vs-miss discriminator."""

    def test_timeout_cause_is_transient(self) -> None:
        exc = ValueError("Request timed out after 30s")
        exc.__cause__ = TimeoutError()
        assert _is_transient_request_error(exc) is True

    def test_network_message_is_transient(self) -> None:
        assert _is_transient_request_error(ValueError("Network error: connection reset")) is True

    def test_http_408_and_5xx_are_transient(self) -> None:
        for status in (408, 500, 502, 503, 504):
            assert _is_transient_request_error(ValueError(f"CoinGecko API error {status}: x")) is True

    def test_http_4xx_is_not_transient(self) -> None:
        assert _is_transient_request_error(ValueError("CoinGecko API error 404: coin not found")) is False
        assert _is_transient_request_error(ValueError("CoinGecko API error 400: bad request")) is False

    def test_no_data_miss_is_not_transient(self) -> None:
        assert _is_transient_request_error(ValueError("No price data available for WSTETH in range")) is False
        assert _is_transient_request_error(ValueError("Unknown token: WSTETH")) is False


class TestHistoricalCacheKeyedByCoinId:
    @pytest.mark.asyncio
    async def test_get_price_caches_under_coin_id_not_symbol(self) -> None:
        """Persistent cache is keyed by the resolved coin id, preventing cross-chain reuse."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            side_effect=[
                {"id": _WSTETH_ARB_BRIDGED_ID},  # contract resolution
                {"market_data": {"current_price": {"usd": 2500.0}}},  # /history
            ],
        ):
            price = await provider.get_price("WSTETH", _TS)
        assert price == Decimal("2500.0")
        # Cached under the resolved coin id, NOT the bare symbol.
        assert provider._historical_cache.get(_WSTETH_ARB_BRIDGED_ID, _TS) == Decimal("2500.0")
        assert provider._historical_cache.get("WSTETH", _TS) is None


class TestSupportedTokens:
    def test_supported_tokens_is_membership_union(self) -> None:
        """supported_tokens = natives ∪ address-map keys, synchronous, no I/O."""
        provider = CoinGeckoDataProvider(
            token_addresses={"wstETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        tokens = provider.supported_tokens
        assert "WETH" in tokens  # native
        assert "WSTETH" in tokens  # address-backed (upper-cased)
        # Sorted and de-duplicated.
        assert tokens == sorted(set(tokens))

    def test_supported_tokens_native_only_without_map(self) -> None:
        """With no address map, only natives have a route."""
        provider = CoinGeckoDataProvider()
        tokens = provider.supported_tokens
        assert "WETH" in tokens
        assert "USDC" not in tokens  # no longer a hardcoded allowlist row


class TestPersistentResolutionCache:
    @pytest.mark.asyncio
    async def test_persistent_resolution_round_trips(self, tmp_path) -> None:
        """A persistent provider reads a prior resolution from SQLite with no HTTP."""
        db_path = str(tmp_path / "hist.db")

        first = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            persistent_cache=True,
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        # Point the persistent cache at the temp db explicitly.
        first._historical_cache.close()
        from almanak.framework.backtesting.pnl.providers.coingecko import HistoricalPriceCache

        first._historical_cache = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
        with patch.object(
            first,
            "_make_request",
            new_callable=AsyncMock,
            return_value={"id": _WSTETH_ARB_BRIDGED_ID},
        ) as req1:
            assert await first._resolve_token_id("WSTETH") == _WSTETH_ARB_BRIDGED_ID
        assert req1.await_count == 1
        first._historical_cache.close()

        # A fresh provider over the SAME db resolves with zero HTTP (read-through
        # the persisted resolution; no in-memory warm start).
        second = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"WSTETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        second._historical_cache.close()
        second._historical_cache = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
        with patch.object(second, "_make_request", new_callable=AsyncMock) as req2:
            assert await second._resolve_token_id("WSTETH") == _WSTETH_ARB_BRIDGED_ID
        req2.assert_not_called()
        second._historical_cache.close()

    def test_clear_removes_persisted_resolution(self, tmp_path) -> None:
        """clear() drops coin_id_resolutions rows too, not just historical_prices."""
        from almanak.framework.backtesting.pnl.providers.coingecko import HistoricalPriceCache

        cache = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=str(tmp_path / "hist.db"))
        cache.set_coin_id("arbitrum", _WSTETH_ARB_ADDRESS, _WSTETH_ARB_BRIDGED_ID)
        assert cache.get_coin_id("arbitrum", _WSTETH_ARB_ADDRESS) == _WSTETH_ARB_BRIDGED_ID
        cache.clear()
        assert cache.get_coin_id("arbitrum", _WSTETH_ARB_ADDRESS) is None
        cache.close()


# cbBTC on Base -- a non-native ERC20 absent from the native projection, so it
# is an honest miss until its address is registered (the reported numeraire bug).
_CBBTC_BASE_ADDRESS = "0xBdb9300b7CDE636d9cD4AFF00f6F009fFBBc8EE6"
_CBBTC_BASE_BRIDGED_ID = "coinbase-wrapped-btc"


class TestRegisterTokenAddresses:
    """``register_token_addresses`` augments the resolution map post-construction.

    This is the engine's hook for the declared numeraire, which is auto-added to
    the data-fetch set after the provider is built and would otherwise be an
    unpriceable honest miss.
    """

    @pytest.mark.asyncio
    async def test_unregistered_token_is_an_honest_miss(self) -> None:
        """Baseline: a non-native ERC20 with no address entry resolves to None."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        with patch.object(provider, "_make_request", new_callable=AsyncMock) as req:
            assert await provider._resolve_token_id("CBBTC") is None
        req.assert_not_called()  # honest miss, no network
        await provider.close()

    @pytest.mark.asyncio
    async def test_register_enables_address_resolution(self) -> None:
        """After registration the symbol resolves via the contract endpoint."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        provider.register_token_addresses({"CBBTC": ("base", _CBBTC_BASE_ADDRESS)})
        with patch.object(
            provider,
            "_make_request",
            new_callable=AsyncMock,
            return_value={"id": _CBBTC_BASE_BRIDGED_ID},
        ) as req:
            coin_id = await provider._resolve_token_id("CBBTC")

        assert coin_id == _CBBTC_BASE_BRIDGED_ID
        req.assert_awaited_once()
        # The address is normalised to lowercase before hitting the CoinGecko
        # contract endpoint (which rejects checksummed addresses -- ALM-2664).
        endpoint = req.await_args.args[0]
        assert _CBBTC_BASE_ADDRESS.lower() in endpoint
        assert _CBBTC_BASE_ADDRESS not in endpoint  # the checksummed form must not leak

    @pytest.mark.asyncio
    async def test_register_merges_with_constructor_map(self) -> None:
        """Registration augments, never replaces, the construction-time map."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"wstETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        provider.register_token_addresses({"CBBTC": ("base", _CBBTC_BASE_ADDRESS)})

        def _fake_request(endpoint: str, _params: dict) -> dict:
            if _WSTETH_ARB_ADDRESS in endpoint:
                return {"id": _WSTETH_ARB_BRIDGED_ID}
            return {"id": _CBBTC_BASE_BRIDGED_ID}

        with patch.object(provider, "_make_request", new_callable=AsyncMock, side_effect=_fake_request):
            assert await provider._resolve_token_id("WSTETH") == _WSTETH_ARB_BRIDGED_ID
            assert await provider._resolve_token_id("CBBTC") == _CBBTC_BASE_BRIDGED_ID
        await provider.close()

    @pytest.mark.asyncio
    async def test_register_is_case_insensitive(self) -> None:
        """Keys are upper-cased on store, matching __init__ normalisation."""
        provider = CoinGeckoDataProvider(retry_config=_fast_retry())
        provider.register_token_addresses({"cbBTC": ("base", _CBBTC_BASE_ADDRESS)})
        with patch.object(
            provider, "_make_request", new_callable=AsyncMock, return_value={"id": _CBBTC_BASE_BRIDGED_ID}
        ):
            assert await provider._resolve_token_id("CBBTC") == _CBBTC_BASE_BRIDGED_ID
        await provider.close()

    @pytest.mark.asyncio
    async def test_register_empty_is_noop(self) -> None:
        """An empty registration leaves existing entries intact."""
        provider = CoinGeckoDataProvider(
            retry_config=_fast_retry(),
            token_addresses={"wstETH": ("arbitrum", _WSTETH_ARB_ADDRESS)},
        )
        provider.register_token_addresses({})
        with patch.object(
            provider, "_make_request", new_callable=AsyncMock, return_value={"id": _WSTETH_ARB_BRIDGED_ID}
        ):
            assert await provider._resolve_token_id("WSTETH") == _WSTETH_ARB_BRIDGED_ID
        await provider.close()
