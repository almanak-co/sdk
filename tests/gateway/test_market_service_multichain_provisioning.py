"""Per-chain price-aggregator provisioning + routing (VIB-5651, test plan §8 items 1,2,3,5,6,7).

Before VIB-5651 the MarketService built ONE flat ``PriceAggregator`` for ``chains[0]``,
so a gateway serving ``["arbitrum", "hyperevm"]`` priced every hyperevm request off the
arbitrum stack (wrong chain) and stamped native hyperevm ETH ``price_missing`` — the
``AccountingPersistenceError`` that halted the smoke. The fix builds one chain-correct
``PriceAggregator`` per configured chain in ``_price_aggregators`` and routes each call
through ``_aggregator_for(chain)``.

These tests pin, in order:
  1. multi-chain build     — one chain-correct sub-aggregator per configured chain;
  2. venue-chain routing   — ``price(ETH, chain=hyperevm)`` hits the HyperCore oracle;
  3. EVM-chain routing      — ``price(ETH, chain=arbitrum)`` hits Chainlink, NOT the venue;
  5. native-gas fold        — ``GetBalance``/``BatchGetBalances`` price native ETH off the
                              request's chain (regression for the accounting halt);
  6. shared-source dedup    — Binance/CoinGecko instances are shared and closed once;
  7. single-chain compat    — one-entry map with the identical pre-change 4-source stack.

(Test 4 — the source-level chain-mismatch guard — lives in
``test_price_source_chain_guard.py``.)
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import eth_abi
import pytest

from almanak.framework.data.interfaces import BalanceResult, PriceResult
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.market_service import _NO_CHAIN_KEY, MarketServiceServicer


def _settings(chains: list[str]) -> GatewaySettings:
    """Real GatewaySettings — faithful to production provisioning (no auto-vivifying stub)."""
    return GatewaySettings(chains=list(chains), network="mainnet", coingecko_api_key="")


def _source_names(aggregator) -> list[str]:
    return [s.source_name for s in aggregator.sources]


def _oracle_blob(wire: int) -> str:
    """Hex-encoded ``uint64`` HyperCore oracle-precompile return (mirrors the source's decode)."""
    return "0x" + eth_abi.encode(["uint64"], [wire]).hex()


def _price_result(price: str, source: str) -> PriceResult:
    from datetime import UTC, datetime

    return PriceResult(
        price=Decimal(price),
        source=source,
        timestamp=datetime.now(UTC),
        confidence=1.0,
        stale=False,
    )


# ---------------------------------------------------------------------------
# Test 1 — multi-chain build: one chain-correct sub-aggregator per chain
# ---------------------------------------------------------------------------


class TestMultiChainBuild:
    @pytest.mark.asyncio
    async def test_builds_one_chain_correct_aggregator_per_chain(self):
        servicer = MarketServiceServicer(_settings(["arbitrum", "hyperevm"]))
        await servicer._ensure_initialized()

        # One aggregator per configured chain, keyed by chain, primary is chains[0].
        assert set(servicer._price_aggregators.keys()) == {"arbitrum", "hyperevm"}
        assert servicer._primary_chain == "arbitrum"

        arb_names = _source_names(servicer._price_aggregators["arbitrum"])
        hyper_names = _source_names(servicer._price_aggregators["hyperevm"])

        # The arbitrum aggregator holds Chainlink (onchain), NOT the venue oracle.
        assert "onchain" in arb_names
        assert "hypercore_oracle" not in arb_names

        # The hyperevm aggregator holds the venue oracle, NOT Chainlink.
        assert "hypercore_oracle" in hyper_names
        assert "onchain" not in hyper_names

        await servicer.close()


# ---------------------------------------------------------------------------
# Test 2 — routing to the venue-oracle aggregator for a hyperevm request
# ---------------------------------------------------------------------------


class TestVenueChainRouting:
    @pytest.mark.asyncio
    async def test_price_eth_on_hyperevm_routes_to_hypercore_oracle(self):
        """``GetPrice(ETH, chain=hyperevm)`` on a multi-chain gateway must be served
        by the hyperevm sub-aggregator's HyperCore oracle — not the arbitrum stack."""
        servicer = MarketServiceServicer(_settings(["arbitrum", "hyperevm"]))
        await servicer._ensure_initialized()

        # Isolate the hyperevm venue oracle: reduce its sub-aggregator to only the
        # HyperCore source (avoids DexScreener/CoinGecko real HTTP) and stub the
        # 0x0807 precompile read (ETH asset 1, szDecimals 4: raw 300000 -> 3000).
        hyper_agg = servicer._price_aggregators["hyperevm"]
        hyper_source = next(s for s in hyper_agg.sources if s.source_name == "hypercore_oracle")
        hyper_agg._sources = [hyper_source]

        # Guard against a mis-route: if the arbitrum aggregator is consulted, fail loudly.
        servicer._price_aggregators["arbitrum"].get_aggregated_price = AsyncMock(
            side_effect=AssertionError("routed to arbitrum aggregator for a hyperevm request")
        )

        with patch.object(hyper_source, "_eth_call", new=AsyncMock(return_value=_oracle_blob(300_000))):
            request = MagicMock()
            request.token = "ETH"
            request.quote = "USD"
            request.chain = "hyperevm"
            context = MagicMock()

            response = await servicer.GetPrice(request, context)

        context.set_code.assert_not_called()
        assert Decimal(response.price) == Decimal("3000")
        assert "hypercore_oracle" in list(response.sources_ok)
        await servicer.close()


# ---------------------------------------------------------------------------
# Test 3 — routing to the EVM (Chainlink) aggregator, NOT the venue oracle
# ---------------------------------------------------------------------------


class TestEvmChainRouting:
    @pytest.mark.asyncio
    async def test_price_eth_on_arbitrum_routes_to_chainlink_not_venue(self):
        """``GetPrice(ETH, chain=arbitrum)`` must be served by the arbitrum Chainlink
        source; the hyperevm venue oracle must NOT be touched."""
        servicer = MarketServiceServicer(_settings(["arbitrum", "hyperevm"]))
        await servicer._ensure_initialized()

        # Isolate the arbitrum Chainlink source and stub its feed read.
        arb_agg = servicer._price_aggregators["arbitrum"]
        onchain_source = next(s for s in arb_agg.sources if s.source_name == "onchain")
        arb_agg._sources = [onchain_source]
        onchain_source._chain_id_validated = True  # skip the eth_chainId RPC
        fetch = AsyncMock(return_value=(Decimal("2500"), 1.0))

        # The venue oracle must never be read for an arbitrum request.
        hyper_source = next(
            s for s in servicer._price_aggregators["hyperevm"].sources if s.source_name == "hypercore_oracle"
        )
        venue_call = AsyncMock(side_effect=AssertionError("venue oracle read for an arbitrum request"))

        with (
            patch.object(onchain_source, "_fetch_chainlink", new=fetch),
            patch.object(hyper_source, "_eth_call", new=venue_call),
        ):
            request = MagicMock()
            request.token = "ETH"
            request.quote = "USD"
            request.chain = "arbitrum"
            context = MagicMock()

            response = await servicer.GetPrice(request, context)

        context.set_code.assert_not_called()
        assert Decimal(response.price) == Decimal("2500")
        # Chainlink credit + venue oracle untouched.
        assert "onchain_chainlink" in list(response.sources_ok)
        assert "hypercore_oracle" not in list(response.sources_ok)
        venue_call.assert_not_awaited()
        await servicer.close()


# ---------------------------------------------------------------------------
# Test 5 — native-gas fold routes to the request's chain aggregator
# ---------------------------------------------------------------------------


def _balance_result(balance: str) -> BalanceResult:
    return BalanceResult(
        balance=Decimal(balance),
        token="ETH",
        address="0x0000000000000000000000000000000000000000",
        decimals=18,
        raw_balance=int(Decimal(balance) * (10**18)),
    )


class TestNativeGasFold:
    @pytest.mark.asyncio
    async def test_getbalance_prices_native_off_request_chain_not_primary(self):
        """``GetBalance(ETH, chain=hyperevm)`` must stamp ``balance_usd`` from the
        HYPEREVM aggregator, not the primary (arbitrum). Regression for the
        native-gas-fold accounting halt: the primary would miss and leave
        ``balance_usd`` empty."""
        servicer = MarketServiceServicer(_settings(["arbitrum", "hyperevm"]))
        await servicer._ensure_initialized()

        # Swap both sub-aggregators for tagged mocks so routing is observable:
        # hyperevm answers $1743, primary (arbitrum) would answer a wrong number.
        servicer._price_aggregators["hyperevm"].get_aggregated_price = AsyncMock(
            return_value=_price_result("1743", "hypercore_oracle")
        )
        servicer._price_aggregators["arbitrum"].get_aggregated_price = AsyncMock(
            side_effect=AssertionError("native-gas fold routed to the primary aggregator, not hyperevm")
        )

        provider = MagicMock()
        provider.get_native_balance = AsyncMock(return_value=_balance_result("2"))
        provider.get_balance = AsyncMock(return_value=_balance_result("2"))

        request = MagicMock()
        request.chain = "hyperevm"
        request.token = "ETH"
        request.wallet_address = "0x" + "a" * 40
        request.block_tag = 0
        request.force_refresh = False
        context = MagicMock()

        with patch.object(servicer, "_get_balance_provider", new=AsyncMock(return_value=provider)):
            response = await servicer.GetBalance(request, context)

        context.set_code.assert_not_called()
        # 2 ETH * $1743 = $3486, priced by the hyperevm aggregator.
        assert Decimal(response.balance_usd) == Decimal("3486")
        servicer._price_aggregators["hyperevm"].get_aggregated_price.assert_awaited()
        await servicer.close()

    @pytest.mark.asyncio
    async def test_batchgetbalances_prices_native_off_request_chain(self):
        """Same native-gas-fold routing contract for the batch path."""
        from almanak.gateway.proto import gateway_pb2

        servicer = MarketServiceServicer(_settings(["arbitrum", "hyperevm"]))
        await servicer._ensure_initialized()

        servicer._price_aggregators["hyperevm"].get_aggregated_price = AsyncMock(
            return_value=_price_result("1743", "hypercore_oracle")
        )
        servicer._price_aggregators["arbitrum"].get_aggregated_price = AsyncMock(
            side_effect=AssertionError("batch native-gas fold routed to the primary aggregator")
        )

        provider = MagicMock()
        provider.get_native_balance = AsyncMock(return_value=_balance_result("2"))
        provider.get_balance = AsyncMock(return_value=_balance_result("2"))

        request = gateway_pb2.BatchBalanceRequest(
            requests=[
                gateway_pb2.BalanceRequest(chain="hyperevm", token="ETH", wallet_address="0x" + "a" * 40),
            ]
        )
        context = MagicMock()

        with patch.object(servicer, "_get_balance_provider", new=AsyncMock(return_value=provider)):
            response = await servicer.BatchGetBalances(request, context)

        assert len(response.responses) == 1
        assert Decimal(response.responses[0].balance_usd) == Decimal("3486")
        servicer._price_aggregators["hyperevm"].get_aggregated_price.assert_awaited()
        await servicer.close()


# ---------------------------------------------------------------------------
# Test 6 — shared-source dedup + close-once lifecycle
# ---------------------------------------------------------------------------


class TestSharedSourceDedupAndClose:
    @pytest.mark.asyncio
    async def test_binance_and_coingecko_shared_across_evm_chains(self):
        """Two EVM chains must SHARE one Binance and one CoinGecko instance (§5 no
        O(N) client duplication); chain-scoped sources (onchain/dexscreener) differ."""
        servicer = MarketServiceServicer(_settings(["arbitrum", "base"]))
        await servicer._ensure_initialized()

        arb = {s.source_name: s for s in servicer._price_aggregators["arbitrum"].sources}
        base = {s.source_name: s for s in servicer._price_aggregators["base"].sources}

        # Chain-agnostic sources: SAME object (identity) across sub-aggregators.
        assert arb["binance"] is base["binance"]
        assert arb["coingecko"] is base["coingecko"]

        # Chain-scoped sources: DISTINCT per chain.
        assert arb["onchain"] is not base["onchain"]
        assert arb["dexscreener"] is not base["dexscreener"]

        await servicer.close()

    @pytest.mark.asyncio
    async def test_close_closes_each_shared_source_exactly_once(self):
        """A shared source referenced by both sub-aggregators must be closed ONCE,
        not once per aggregator (lead decision 2 — dedup-close by ``id()``)."""
        servicer = MarketServiceServicer(_settings(["arbitrum", "base"]))
        await servicer._ensure_initialized()

        arb = {s.source_name: s for s in servicer._price_aggregators["arbitrum"].sources}
        base = {s.source_name: s for s in servicer._price_aggregators["base"].sources}
        assert arb["binance"] is base["binance"]  # precondition: it IS shared

        shared_binance = arb["binance"]
        shared_coingecko = arb["coingecko"]
        arb_onchain = arb["onchain"]
        base_onchain = base["onchain"]
        for src in (shared_binance, shared_coingecko, arb_onchain, base_onchain):
            src.close = AsyncMock()

        await servicer._close_price_sources()

        # Shared instances closed exactly once despite two references each.
        shared_binance.close.assert_awaited_once()
        shared_coingecko.close.assert_awaited_once()
        # Per-chain instances each closed once too.
        arb_onchain.close.assert_awaited_once()
        base_onchain.close.assert_awaited_once()
        # Map cleared after close.
        assert servicer._price_aggregators == {}


# ---------------------------------------------------------------------------
# Test 7 — single-chain compatibility (byte-for-byte source set)
# ---------------------------------------------------------------------------


class TestSingleChainCompat:
    @pytest.mark.asyncio
    async def test_single_chain_builds_identical_four_source_evm_stack(self):
        """A single-chain gateway yields a one-entry map whose aggregator holds the
        exact pre-change 4-source EVM stack (Chainlink + Binance + DexScreener +
        CoinGecko)."""
        servicer = MarketServiceServicer(_settings(["arbitrum"]))
        await servicer._ensure_initialized()

        assert set(servicer._price_aggregators.keys()) == {"arbitrum"}
        assert servicer._primary_chain == "arbitrum"
        assert _NO_CHAIN_KEY not in servicer._price_aggregators

        names = _source_names(servicer._price_aggregators["arbitrum"])
        assert set(names) == {"onchain", "binance", "dexscreener", "coingecko"}
        assert len(names) == 4
        # Back-compat property still returns the primary aggregator.
        assert servicer._price_aggregator is servicer._price_aggregators["arbitrum"]

        await servicer.close()

    @pytest.mark.asyncio
    async def test_no_chain_gateway_uses_sentinel_key(self):
        """A gateway started with no chains keeps a single CoinGecko-only aggregator
        under the sentinel key — ``_aggregator_for`` always has a primary to fall
        back to."""
        servicer = MarketServiceServicer(_settings([]))
        await servicer._ensure_initialized()

        assert set(servicer._price_aggregators.keys()) == {_NO_CHAIN_KEY}
        assert servicer._primary_chain == _NO_CHAIN_KEY
        names = _source_names(servicer._price_aggregators[_NO_CHAIN_KEY])
        assert names == ["coingecko"]
        # A bare/unconfigured chain falls back to the primary (sentinel) aggregator.
        assert servicer._aggregator_for("hyperevm") is servicer._price_aggregators[_NO_CHAIN_KEY]

        await servicer.close()
