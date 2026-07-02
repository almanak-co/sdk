"""Tests for the HyperEVM data layer (VIB-5576).

Three surfaces:
  (a) ``HypercoreOraclePriceSource`` — decodes the ``0x0807`` oracle precompile
      for a perp symbol, returns the $1.00 peg for HyperEVM stablecoins, and
      misses (raises ``DataSourceUnavailable``) for unknown symbols.
  (b) ``MarketServiceServicer._do_initialize`` — selects the HyperCore stack
      (HyperCore oracle + DexScreener + CoinGecko, NO Chainlink) for
      ``chain == "hyperevm"``.
  (c) Token resolution — USDC / USDT0 / WHYPE resolve to their HyperEVM
      addresses instantly (static registry, no gateway on-chain discovery).
"""

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import eth_abi
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.gateway.data.price.hyperevm import HypercoreOraclePriceSource


def _oracle_blob(wire: int) -> str:
    """Build a hex-encoded ``uint64`` oracle-precompile return."""
    return "0x" + eth_abi.encode(["uint64"], [wire]).hex()


class TestHypercoreOraclePriceSource:
    """Perp-symbol decode, stablecoin peg, and miss behaviour."""

    @pytest.mark.asyncio
    async def test_perp_symbol_decodes_oracle_price(self):
        """BTC (asset 0, szDecimals 5): raw 598970 -> 59897 (exact precompile scale)."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock(return_value=_oracle_blob(598970))) as call:
            result = await source.get_price("BTC", "USD")

        assert isinstance(result, PriceResult)
        assert result.price == Decimal("59897")
        assert result.source == "hypercore_oracle"
        assert result.stale is False
        # eth_call hit the oracle precompile with the raw ABI perp query (no selector).
        call.assert_awaited_once()
        to_addr, data = call.await_args.args
        assert to_addr.lower().endswith("807")
        assert data == "0x" + eth_abi.encode(["uint32"], [0]).hex()
        await source.close()

    @pytest.mark.asyncio
    async def test_eth_symbol_decodes_oracle_price(self):
        """ETH (asset 1, szDecimals 4): scale 10**(6-4)=100, raw 300000 -> 3000.0."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock(return_value=_oracle_blob(300_000))):
            result = await source.get_price("ETH", "USD")
        assert result.price == Decimal("3000")
        await source.close()

    @pytest.mark.asyncio
    async def test_usdc_returns_peg_without_rpc(self):
        """USDC returns the $1.00 peg with NO eth_call."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock()) as call:
            result = await source.get_price("USDC", "USD")
        assert result.price == Decimal("1.00")
        assert result.source == "hypercore_oracle"
        call.assert_not_awaited()
        await source.close()

    @pytest.mark.asyncio
    async def test_usdt0_returns_peg_without_rpc(self):
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock()) as call:
            result = await source.get_price("USDT0", "USD")
        assert result.price == Decimal("1.00")
        call.assert_not_awaited()
        await source.close()

    @pytest.mark.asyncio
    async def test_unknown_symbol_misses(self):
        """A non-perp, non-stablecoin symbol raises DataSourceUnavailable (miss)."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock()) as call:
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("DOODOO", "USD")
        call.assert_not_awaited()
        await source.close()

    @pytest.mark.asyncio
    async def test_empty_read_is_miss_not_zero(self):
        """Empty precompile return -> miss (Empty≠Zero), never a fabricated 0."""
        source = HypercoreOraclePriceSource()
        # decode_uint64("0x") -> None; but _eth_call itself raises on empty. Cover
        # the decoder path directly by returning a zero-length decodable blob.
        with patch.object(source, "_eth_call", new=AsyncMock(return_value="0x")):
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("BTC", "USD")
        await source.close()

    @pytest.mark.asyncio
    async def test_zero_price_is_miss_not_zero(self):
        """A decoded zero oracle price -> miss (never trade against 0)."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock(return_value=_oracle_blob(0))):
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("BTC", "USD")
        await source.close()

    @pytest.mark.asyncio
    async def test_failed_read_is_miss(self):
        """An eth_call error surfaces as a miss, not a crash."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock(side_effect=RuntimeError("rpc down"))):
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("BTC", "USD")
        await source.close()

    @pytest.mark.asyncio
    async def test_malformed_payload_is_miss_not_crash(self):
        """A non-empty, undecodable oracle payload is a MISS, never an unhandled crash.

        ``_eth_call`` only rejects the empty ("0x") result, so a non-empty but
        garbage / odd-length hex return reaches ``decode_uint64``, which raises
        (ValueError / eth_abi padding error). The decode runs inside the
        try/except in ``_read_oracle_price`` so the exception is caught and
        re-raised as ``DataSourceUnavailable`` (a miss) — locking in the
        "never crash the aggregator on a bad payload" contract.
        """
        source = HypercoreOraclePriceSource()
        # "0xabc" is non-empty (passes _eth_call) but odd-length garbage — decode_uint64 raises.
        with patch.object(source, "_eth_call", new=AsyncMock(return_value="0xabc")):
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("BTC", "USD")
        await source.close()

    @pytest.mark.asyncio
    async def test_non_usd_quote_misses(self):
        source = HypercoreOraclePriceSource()
        with pytest.raises(DataSourceUnavailable):
            await source.get_price("BTC", "EUR")
        await source.close()

    @pytest.mark.asyncio
    async def test_price_is_cached(self):
        """Second call within TTL is served from cache (one eth_call)."""
        source = HypercoreOraclePriceSource()
        with patch.object(source, "_eth_call", new=AsyncMock(return_value=_oracle_blob(598970))) as call:
            await source.get_price("BTC", "USD")
            await source.get_price("BTC", "USD")
        call.assert_awaited_once()
        await source.close()


class TestMarketServiceHyperevmStack:
    """MarketService selects the HyperCore stack for chain == 'hyperevm'."""

    @pytest.mark.asyncio
    async def test_hyperevm_selects_hypercore_stack(self):
        from almanak.gateway.core.settings import GatewaySettings
        from almanak.gateway.services.market_service import MarketServiceServicer

        settings = GatewaySettings(chains=["hyperevm"], network="mainnet", coingecko_api_key="")
        servicer = MarketServiceServicer(settings)
        await servicer._ensure_initialized()

        names = [s.source_name for s in servicer._price_aggregator._sources]
        # HyperCore oracle primary + DexScreener + CoinGecko fallback; NO Chainlink.
        assert names[0] == "hypercore_oracle"
        assert "onchain" not in names  # no Chainlink on HyperEVM
        assert "dexscreener" in names
        assert "coingecko" in names
        assert len(names) == 3


class TestHyperevmTokenResolution:
    """USDC / USDT0 / WHYPE resolve to HyperEVM addresses instantly (static)."""

    def test_usdc_resolves_instantly(self):
        from almanak.framework.data.tokens import get_token_resolver

        t = get_token_resolver().resolve("USDC", "hyperevm", skip_gateway=True)
        assert t.address.lower() == "0xb88339cb7199b77e23db6e890353e22632ba630f"
        assert t.decimals == 6

    def test_usdt0_resolves_instantly(self):
        from almanak.framework.data.tokens import get_token_resolver

        t = get_token_resolver().resolve("USDT0", "hyperevm", skip_gateway=True)
        assert t.address.lower() == "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb"
        assert t.decimals == 6

    def test_whype_resolves_instantly(self):
        from almanak.framework.data.tokens import get_token_resolver

        t = get_token_resolver().resolve("WHYPE", "hyperevm", skip_gateway=True)
        assert t.address == "0x5555555555555555555555555555555555555555"
        assert t.decimals == 18
