"""Venue-native OHLCV routing (ALM-3148 / ALM-3152).

Two hosted agents ran healthy forever with ``txs_planned = 0`` because
``market.ema("XRP")`` could not obtain candles: XRP has no ``CEX_SYMBOL_MAP``
row, so it classified DeFi-native and routed to a pool search that has no XRP
pool — while GMX, the venue the strategy was trading, published XRP index
candles the whole time.

These tests pin the routing decision, the configurability that surrounds it,
and the two failure modes that would be invisible if they regressed: silently
swapping the price basis, and silently serving a different source than the
strategist asked for.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataClassification, Instrument
from almanak.framework.data.ohlcv.ohlcv_router import (
    OHLCVRouter,
    _PROVIDER_CHAINS,
    classify_instrument,
    provider_names_in_chains,
)
from almanak.framework.data.ohlcv.venue_context import (
    SOURCE_AUTO,
    SOURCE_VENUE_NATIVE,
    VENUE_NATIVE_PROVIDER,
    OHLCVSourcePolicy,
    build_source_policy,
    resolve_ohlcv_source,
)
from almanak.framework.data.ohlcv.venue_native_provider import VenueNativeOHLCVProvider
from almanak.framework.data.timeframes import OHLCVTimeframe

_KNOWN = frozenset({"binance", "coingecko", "coingecko_onchain"})


def _gmx_policy(source: str = SOURCE_VENUE_NATIVE) -> OHLCVSourcePolicy:
    return OHLCVSourcePolicy(
        source=source,
        venue="gmx_v2",
        chain="arbitrum",
        markets={"XRP": "XRP/USD", "ETH": "ETH/USD", "WETH": "ETH/USD"},
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


class TestExplicitRequestScopeDisownsTheClaim:
    """An explicitly scoped request must not be answered with the venue index.

    Classification read only `(base, chain)`, so it could not see that the
    caller had named a specific pool or a non-USD quote. The venue index is one
    plane per market quoted in USD; serving it for `ETH/BTC`, or for a named
    Uniswap pool, returns a plausible wrong number at confidence 1.0 to a caller
    who could not have been more explicit. Both arguments are public on
    `MarketSnapshot.ohlcv`.
    """

    @staticmethod
    def _policy():
        return OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE,
            venue="gmx_v2",
            chain="arbitrum",
            markets={"ETH": "ETH/USD", "WETH": "ETH/USD"},
        )

    def test_explicit_pool_address_is_not_claimed(self):
        instrument = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert classify_instrument(instrument, self._policy(), "0xpool") != VENUE_NATIVE_PROVIDER

    def test_non_usd_quote_is_not_claimed(self):
        instrument = Instrument(base="WETH", quote="BTC", chain="arbitrum")
        assert classify_instrument(instrument, self._policy()) != VENUE_NATIVE_PROVIDER

    @pytest.mark.parametrize("quote", ["USD", "USDC", "USDT", "DAI", "USDC.e"])
    def test_liveness_usd_quotes_still_claim(self, quote):
        """Without this, "never claim a quoted request" would pass the two above
        while disabling the feature — every request carries a resolved quote."""
        instrument = Instrument(base="WETH", quote=quote, chain="arbitrum")
        assert classify_instrument(instrument, self._policy()) == VENUE_NATIVE_PROVIDER

    def test_cache_key_and_routing_agree_about_ownership(self):
        """The two must never disagree: a request routed venue-native but keyed
        as default files the venue's candles where every other consumer reads
        them. They are one function precisely so this cannot drift."""
        from almanak.framework.data.ohlcv.ohlcv_router import venue_native_owns

        policy = self._policy()
        for quote, pool in [("USDC", None), ("BTC", None), ("USDC", "0xpool")]:
            instrument = Instrument(base="WETH", quote=quote, chain="arbitrum")
            routed = classify_instrument(instrument, policy, pool) == VENUE_NATIVE_PROVIDER
            owned = venue_native_owns(instrument, policy, pool)
            assert routed == owned, f"routing and cache-key ownership disagree for quote={quote} pool={pool}"


class TestClassification:
    def test_reported_symbol_routes_venue_native_instead_of_pool_search(self):
        """The ALM-3148 regression itself: XRP must stop classifying DeFi-native."""
        instrument = Instrument(base="XRP", quote="USD", chain="arbitrum")
        assert classify_instrument(instrument) == "defi_primary"  # the bug, unchanged without a policy
        assert classify_instrument(instrument, _gmx_policy()) == VENUE_NATIVE_PROVIDER

    def test_venue_native_outranks_cex_primary(self):
        """A CEX-listed base still prefers the plane it is liquidated against."""
        instrument = Instrument(base="WETH", quote="USD", chain="arbitrum")
        assert classify_instrument(instrument) == "cex_primary"
        assert classify_instrument(instrument, _gmx_policy()) == VENUE_NATIVE_PROVIDER

    def test_policy_only_claims_markets_the_strategy_declared(self):
        """Keying on (base, venue) bounds the implicit venue.

        A GMX strategy holding an unrelated spot leg must not give that leg a
        GMX dependency — keying on the symbol alone would.
        """
        unrelated = Instrument(base="USDC", quote="USD", chain="arbitrum")
        assert classify_instrument(unrelated, _gmx_policy()) != VENUE_NATIVE_PROVIDER

    def test_policy_does_not_claim_another_chain(self):
        instrument = Instrument(base="XRP", quote="USD", chain="base")
        assert classify_instrument(instrument, _gmx_policy()) != VENUE_NATIVE_PROVIDER

    def test_no_policy_preserves_todays_classification(self):
        """Callers with no strategy context (dashboard, tooling) are untouched."""
        assert classify_instrument(Instrument(base="WETH", quote="USD", chain="base")) == "cex_primary"
        assert classify_instrument(Instrument(base="XRP", quote="USD", chain="base")) == "defi_primary"


class TestProviderChain:
    def test_venue_native_has_no_cex_fallback(self):
        """Falling GMX -> Binance would silently swap the basis.

        The strategy would decide on a CEX price while its position is marked
        and liquidated against the venue index, and the call would still return
        a plausible number. Failing loudly is the deliberate choice.
        """
        assert _PROVIDER_CHAINS[VENUE_NATIVE_PROVIDER] == [VENUE_NATIVE_PROVIDER]

    def test_venue_native_excluded_from_the_unconditional_registry_invariant(self):
        """VIB-4847's guard must not demand a provider most strategies never build.

        Requiring it unconditionally would fail every non-perp strategy at boot.
        """
        assert VENUE_NATIVE_PROVIDER not in provider_names_in_chains()


# ---------------------------------------------------------------------------
# Source resolution (the configurability contract)
# ---------------------------------------------------------------------------


class TestSourceResolution:
    def test_default_is_venue_native_when_a_venue_exists(self):
        assert resolve_ohlcv_source(configured=None, venue="gmx_v2", known_providers=_KNOWN) == SOURCE_VENUE_NATIVE

    def test_default_is_auto_without_a_venue(self):
        """A strategy on no native-candle venue behaves exactly as it does today."""
        assert resolve_ohlcv_source(configured=None, venue=None, known_providers=_KNOWN) == SOURCE_AUTO

    @pytest.mark.parametrize("configured", ["binance", "BINANCE", " binance "])
    def test_strategist_can_pin_binance_on_a_perp_strategy(self, configured):
        """The owner ruling: venue-native is the default, never a hardcoded answer."""
        assert resolve_ohlcv_source(configured=configured, venue="gmx_v2", known_providers=_KNOWN) == "binance"

    def test_strategist_can_ask_for_todays_classification(self):
        assert resolve_ohlcv_source(configured="auto", venue="gmx_v2", known_providers=_KNOWN) == SOURCE_AUTO

    def test_unknown_source_is_rejected_not_ignored(self):
        """Ignoring it would serve a different market than the strategist chose."""
        with pytest.raises(ValueError, match="Unknown ohlcv_source"):
            resolve_ohlcv_source(configured="bitmex", venue="gmx_v2", known_providers=_KNOWN)

    def test_venue_native_without_a_venue_is_rejected(self):
        with pytest.raises(ValueError, match="requires a strategy protocol"):
            resolve_ohlcv_source(configured="venue_native", venue=None, known_providers=_KNOWN)

    def test_pinned_provider_applies_to_every_instrument(self):
        """ "Use Binance for this strategy" has to mean all of it.

        Honouring the pin only for CEX-classified bases would serve two planes
        inside one strategy.
        """
        policy = _gmx_policy(source="binance")
        assert policy.pinned_provider() == "binance"
        assert policy.claims("XRP", "arbitrum") is False

    def test_policy_modes_that_are_not_providers(self):
        assert _gmx_policy().pinned_provider() is None
        assert _gmx_policy(source=SOURCE_AUTO).pinned_provider() is None


# ---------------------------------------------------------------------------
# Building the policy from a strategy
# ---------------------------------------------------------------------------


class TestPolicyFromStrategy:
    @staticmethod
    def _strategy(protocols: list[str]):
        return SimpleNamespace(STRATEGY_METADATA=SimpleNamespace(supported_protocols=protocols))

    def test_gmx_strategy_gets_a_venue_native_policy_without_touching_its_code(self):
        """An already-deployed agent must recover on release, not on a rewrite.

        ``market.ema("ETH")`` passes a bare string and always will; deriving the
        venue from the strategy's own declaration is what avoids requiring the
        author to change code and redeploy.
        """
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2"]),
            strategy_config={"market": "ETH/USD", "base_token": "ETH"},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.source == SOURCE_VENUE_NATIVE
        assert policy.venue == "gmx_v2"
        # Registered under both spellings: resolve_instrument canonicalises
        # ETH -> WETH before the router ever sees the request.
        assert policy.market_for("ETH", "arbitrum") == "ETH/USD"
        assert policy.market_for("WETH", "arbitrum") == "ETH/USD"

    @staticmethod
    def _with_second_venue(monkeypatch):
        """Give the registry a second eligible venue, as a manifest would."""
        from almanak.connectors._strategy_base.perp_price_history_registry import (
            PerpPriceHistoryRegistry,
        )

        PerpPriceHistoryRegistry._ensure()
        monkeypatch.setitem(PerpPriceHistoryRegistry._venue_map, "acme_perp", "acme_perp")
        monkeypatch.setitem(PerpPriceHistoryRegistry._chains_map, "acme_perp", ("arbitrum",))

    def test_ambiguous_venue_degrades_only_when_venue_native_was_not_requested(self, monkeypatch):
        """Two eligible venues, no explicit ask: keep booting, exactly as before.

        A strategy that declares two perp protocols and never asked for
        venue-native candles must not stop booting because the SDK gained this
        feature. It gets its previous behaviour plus a warning.
        """
        self._with_second_venue(monkeypatch)
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2", "acme_perp"]),
            strategy_config={"market": "ETH/USD", "base_token": "ETH"},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.source == SOURCE_AUTO
        assert policy.venue is None
        assert policy.claims("ETH", "arbitrum") is False

    def test_ambiguous_venue_raises_when_venue_native_was_explicitly_requested(self, monkeypatch):
        """...and names the ambiguity, not a missing declaration.

        Degrading here would serve a plane the strategist did not choose while
        their request sat in the config looking honoured. The message matters as
        much as the raise: reporting "declares no such protocol" for a strategy
        that declares two sends the reader hunting for a declaration that is not
        missing.
        """
        self._with_second_venue(monkeypatch)
        with pytest.raises(ValueError) as excinfo:
            build_source_policy(
                strategy=self._strategy(["gmx_v2", "acme_perp"]),
                strategy_config={
                    "market": "ETH/USD",
                    "base_token": "ETH",
                    "ohlcv_source": "venue_native",
                },
                chain="arbitrum",
                known_providers=_KNOWN,
            )
        message = str(excinfo.value)
        assert "ambiguous" in message
        assert "ohlcv_venue" in message
        assert "gmx_v2" in message and "acme_perp" in message
        # The generic no-such-venue error would be the wrong diagnosis here.
        assert "declares none" not in message

    def test_ambiguity_is_resolved_by_naming_the_venue(self, monkeypatch):
        self._with_second_venue(monkeypatch)
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2", "acme_perp"]),
            strategy_config={
                "market": "ETH/USD",
                "base_token": "ETH",
                "ohlcv_source": "venue_native",
                "ohlcv_venue": "acme_perp",
            },
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.venue == "acme_perp"
        assert policy.source == SOURCE_VENUE_NATIVE

    def test_non_perp_strategy_gets_no_venue(self):
        policy = build_source_policy(
            strategy=self._strategy(["uniswap_v3"]),
            strategy_config={"market": "WETH/USDC"},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.source == SOURCE_AUTO
        assert policy.venue is None
        assert policy.claims_any() is False

    def test_venue_on_an_undeclared_chain_is_not_claimed(self):
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2"]),
            strategy_config={"market": "ETH/USD"},
            chain="base",
            known_providers=_KNOWN,
        )
        assert policy.venue is None

    def test_strategy_config_overrides_the_default(self):
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2"]),
            strategy_config={"market": "ETH/USD", "ohlcv_source": "binance"},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.pinned_provider() == "binance"

    def test_missing_market_declaration_falls_back_rather_than_guessing(self):
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2"]),
            strategy_config={},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.claims_any() is False
        assert policy.source == SOURCE_AUTO

    def test_multi_market_strategy_claims_each_declared_market(self):
        policy = build_source_policy(
            strategy=self._strategy(["gmx_v2"]),
            strategy_config={"markets": ["XRP/USD", "NEAR/USD"]},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.market_for("XRP", "arbitrum") == "XRP/USD"
        assert policy.market_for("NEAR", "arbitrum") == "NEAR/USD"

    def test_alias_protocol_resolves_through_the_manifest_registry(self):
        """Venue eligibility is manifest-derived, never a list in the framework."""
        policy = build_source_policy(
            strategy=self._strategy(["gmx"]),
            strategy_config={"market": "XRP/USD"},
            chain="arbitrum",
            known_providers=_KNOWN,
        )
        assert policy.venue == "gmx_v2"


# ---------------------------------------------------------------------------
# The provider
# ---------------------------------------------------------------------------


def _page(candles, *, timeframe="5m", success=True, error=""):
    return SimpleNamespace(
        venue="gmx_v2",
        chain="arbitrum",
        market="XRP/USD",
        market_token="0x0ccb4faa6f1f1b30911619f1184082ab4e25813c",
        index_token="0xc14e065b0067de91534e032868f5ac6ecf2c6868",
        index_symbol="XRP",
        timeframe=timeframe,
        candles=candles,
        success=success,
        error=error,
    )


def _raw(ts, close):
    return SimpleNamespace(timestamp=ts, open="1.0", high="1.1", low="0.9", close=str(close))


def _client(response):
    return SimpleNamespace(
        is_connected=True,
        config=SimpleNamespace(timeout=5.0),
        rate_history=SimpleNamespace(GetPerpPriceCandles=lambda request, timeout=None: response),
    )


class TestVenueNativeProvider:
    def test_serves_ascending_candles_with_unmeasured_volume(self):
        """The venue pages newest-first; the router reads the youngest from the tail.

        Volume stays ``None`` — a perp index plane is not a trade tape, and a
        substituted ``0`` would be a false measurement (``Empty != Zero``).
        """
        provider = VenueNativeOHLCVProvider(
            gateway_client=_client(_page([_raw(300, "1.03"), _raw(240, "1.02"), _raw(180, "1.01")])),
            policy=_gmx_policy(),
        )
        envelope = provider.fetch(token="XRP", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=3)

        candles = envelope.value
        assert [candle.timestamp for candle in candles] == sorted(candle.timestamp for candle in candles)
        assert candles[-1].close == Decimal("1.03")
        assert candles[-1].timestamp == datetime.fromtimestamp(300, tz=UTC)
        assert all(candle.volume is None for candle in candles)
        assert envelope.meta.source == VENUE_NATIVE_PROVIDER

    def test_unclaimed_instrument_names_the_real_cause(self):
        provider = VenueNativeOHLCVProvider(gateway_client=_client(_page([])), policy=_gmx_policy())
        with pytest.raises(DataSourceUnavailable, match="not a market this strategy trades"):
            provider.fetch(token="DOGE", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=3)

    def test_missing_market_provenance_is_refused(self):
        """market_token/index_token travel with the page so the series can be proved.

        A page that lost them is not evidence about any market, so it cannot be
        served as if it were.
        """
        page = _page([_raw(300, "1.03")])
        page.index_token = ""
        provider = VenueNativeOHLCVProvider(gateway_client=_client(page), policy=_gmx_policy())
        with pytest.raises(DataSourceUnavailable, match="incomplete market provenance"):
            provider.fetch(token="XRP", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=3)

    def test_a_substituted_timeframe_is_refused(self):
        """Serving 1h for a 5m request would silently change what the EMA means."""
        provider = VenueNativeOHLCVProvider(
            gateway_client=_client(_page([_raw(300, "1.03")], timeframe="1h")),
            policy=_gmx_policy(),
        )
        with pytest.raises(DataSourceUnavailable, match="refusing to treat it as the requested plane"):
            provider.fetch(token="XRP", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=3)

    def test_venue_error_surfaces_verbatim(self):
        provider = VenueNativeOHLCVProvider(
            gateway_client=_client(_page([], success=False, error="market is delisted")),
            policy=_gmx_policy(),
        )
        with pytest.raises(DataSourceUnavailable, match="market is delisted"):
            provider.fetch(token="XRP", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=3)

    def test_rpc_failure_is_tagged_transport(self):
        def _boom(request, timeout=None):
            raise RuntimeError("channel closed")

        client = SimpleNamespace(
            is_connected=True,
            config=SimpleNamespace(timeout=5.0),
            rate_history=SimpleNamespace(GetPerpPriceCandles=_boom),
        )
        provider = VenueNativeOHLCVProvider(gateway_client=client, policy=_gmx_policy())
        with pytest.raises(DataSourceUnavailable) as excinfo:
            provider.fetch(token="XRP", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=3)
        assert excinfo.value.transport is True


# ---------------------------------------------------------------------------
# Panel findings (high-risk audit, ALM-3148)
# ---------------------------------------------------------------------------


class TestDiskCacheKeyNamesThePlane:
    """The disk key must identify the price plane, not just the instrument.

    Before venue-native routing, one instrument had exactly one series, so
    ``(base, quote, chain, timeframe, limit, pool)`` identified it completely.
    This change makes two answers to "ETH/USD 4h on arbitrum" simultaneously
    valid, and a key that cannot tell them apart lets one be served for the
    other -- the silent basis swap this lane exists to prevent, re-created in
    the cache and returning a plausible number while doing it.
    """

    @staticmethod
    def _key_for(policy, *, token="ETH", force=None):
        seen: list[str] = []

        router = OHLCVRouter(default_chain="arbitrum", source_policy=policy)
        router._consume_disk_cache = lambda key, *a, **k: seen.append(key) or None  # type: ignore[assignment]
        with contextlib.suppress(Exception):
            router.get_ohlcv(
                token=token, chain="arbitrum", timeframe=OHLCVTimeframe.ONE_DAY, limit=30, force_provider=force
            )
        return seen[0] if seen else None

    def test_venue_native_and_pinned_binance_do_not_share_a_key(self):
        native = self._key_for(_gmx_policy())
        pinned = self._key_for(_gmx_policy(source="binance"))
        assert native is not None and pinned is not None
        assert native != pinned, (
            "a venue-native request and a Binance-pinned request for the same "
            "instrument must not read each other's cached candles"
        )

    def test_force_provider_gets_its_own_key(self):
        default = self._key_for(_gmx_policy())
        forced = self._key_for(_gmx_policy(), force="coingecko")
        assert default != forced

    def test_unclaimed_instrument_keeps_the_pre_existing_key(self):
        """An unclaimed leg must not be re-partitioned, or every existing entry
        for it is silently orphaned on upgrade."""
        with_policy = self._key_for(_gmx_policy(), token="USDC")
        without_policy = self._key_for(None, token="USDC")
        assert with_policy == without_policy

    def test_default_key_is_byte_identical_to_the_legacy_format(self):
        """No suffix on the default plane.

        Appending ":auto" unconditionally would orphan every entry written
        before this change -- including poisoned ones the ALM-2697 staleness
        guard can only evict while it can still find them.
        """
        key = self._key_for(None, token="WETH")
        assert key is not None
        # Six segments: base:quote:chain:timeframe:limit:pool. The last one is
        # the pool-address placeholder, which is itself the literal "auto" when
        # there is no pool -- so an endswith(":auto") check would be testing the
        # wrong segment and pass for the wrong reason.
        assert key.count(":") == 5, f"legacy key must keep six segments, got {key!r}"
        assert key == "WETH:USDC:arbitrum:1d:30:auto"

        # ...and a claimed instrument extends that exact prefix, never rewrites it.
        claimed = self._key_for(_gmx_policy(), token="WETH")
        assert claimed is not None
        assert claimed.startswith("WETH:USDC:arbitrum:1d:30:auto:")
        assert f":{VENUE_NATIVE_PROVIDER}:gmx_v2:" in claimed


class TestVenueResponseIsBoundToTheRequest:
    """A page must be about the asset that was asked for.

    Market label and token addresses can be mutually consistent on a page about
    a different asset entirely. The index symbol is what the candles are OF, and
    it is the only field that binds the answer to the question.
    """

    @staticmethod
    def _provider(policy, response):
        gateway = SimpleNamespace(
            is_connected=True,
            config=SimpleNamespace(timeout=5.0),
            rate_history=SimpleNamespace(GetPerpPriceCandles=lambda _req, timeout=None: response),
        )
        return VenueNativeOHLCVProvider(gateway_client=gateway, policy=policy)

    @staticmethod
    def _page(**kw):
        now = int(datetime.now(UTC).timestamp()) // 300 * 300
        candles = [
            SimpleNamespace(timestamp=now - (29 - i) * 300, open="10", high="10", low="10", close="10")
            for i in range(30)
        ]
        base = {
            "success": True,
            "error": "",
            "market": "ETH/USD",
            "market_token": "0xmarket",
            "index_token": "0xindex",
            "index_symbol": "ETH",
            "timeframe": "5m",
            "candles": candles,
        }
        base.update(kw)
        return SimpleNamespace(**base)

    def test_page_for_another_asset_is_refused(self):
        """The config mis-binding that makes this reachable: base_token says XRP
        while market says ETH/USD, so a request for XRP fetches ETH candles."""
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE, venue="gmx_v2", chain="arbitrum", markets={"XRP": "ETH/USD"}
        )
        provider = self._provider(policy, self._page(index_symbol="ETH"))
        with pytest.raises(DataSourceUnavailable, match="different asset"):
            provider.fetch(token="XRP", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)

    def test_address_request_is_still_bound_by_index_symbol(self):
        """An address label cannot be compared, so it must not wave the page
        through -- that was the widest hole in the first version of this guard."""
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE,
            venue="gmx_v2",
            chain="arbitrum",
            markets={"ETH": "0x70d95587d40a2caf56bd97485ab3eec10bee6336"},
        )
        provider = self._provider(policy, self._page(market="XRP/USD", index_symbol="XRP"))
        with pytest.raises(DataSourceUnavailable, match="different asset"):
            provider.fetch(token="ETH", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)

    def test_empty_market_label_is_refused(self):
        policy = _gmx_policy()
        provider = self._provider(policy, self._page(market=""))
        with pytest.raises(DataSourceUnavailable, match="no market label"):
            provider.fetch(token="ETH", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)

    def test_near_miss_market_label_is_refused(self):
        """BTC/USDT is not BTC/USD. Prefix matching accepted it; exact does not."""
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE, venue="gmx_v2", chain="arbitrum", markets={"BTC": "BTC/USD"}
        )
        provider = self._provider(policy, self._page(market="BTC/USDT", index_symbol="BTC"))
        with pytest.raises(DataSourceUnavailable, match="not about the requested market"):
            provider.fetch(token="BTC", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)

    def test_liveness_the_correct_page_is_still_served(self):
        """Without this, "always refuse" would satisfy every case above."""
        policy = _gmx_policy()
        provider = self._provider(policy, self._page())
        envelope = provider.fetch(token="ETH", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)
        assert len(envelope.value) == 30
        assert envelope.meta.source == VENUE_NATIVE_PROVIDER

    def test_liveness_wrapped_and_native_spellings_are_the_same_asset(self):
        """WETH vs ETH must not be read as two assets, or every ETH strategy breaks."""
        policy = _gmx_policy()
        provider = self._provider(policy, self._page(index_symbol="ETH"))
        envelope = provider.fetch(token="WETH", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)
        assert len(envelope.value) == 30


class TestCacheKeySeparatesVenues:
    def test_two_venues_quoting_the_same_base_do_not_share_a_key(self):
        keys = []
        for venue in ("gmx_v2", "hyperliquid"):
            # Both spellings, so the claim is evaluated on the canonical base the
            # router actually resolves to rather than on the proxy retry.
            policy = OHLCVSourcePolicy(
                source=SOURCE_VENUE_NATIVE,
                venue=venue,
                chain="arbitrum",
                markets={"ETH": "ETH/USD", "WETH": "ETH/USD"},
            )
            router = OHLCVRouter(default_chain="arbitrum", source_policy=policy)
            seen: list[str] = []
            router._consume_disk_cache = lambda key, *a, **k: seen.append(key) or None  # type: ignore[assignment]
            with contextlib.suppress(Exception):
                router.get_ohlcv(token="WETH", chain="arbitrum", timeframe=OHLCVTimeframe.ONE_DAY, limit=30)
            keys.append(seen[0] if seen else None)
        assert keys[0] is not None and keys[1] is not None
        assert VENUE_NATIVE_PROVIDER in keys[0]
        assert keys[0] != keys[1], "one venue's candles must not be served for another's"


class TestVenueNativeLaneHasNoBackDoor:
    """The lane's "no CEX fallback" invariant has to survive the proxy retry.

    `_PROVIDER_CHAINS[venue_native]` is a single entry on purpose. The wrapped
    token proxy retry re-enters classification from scratch with the *unwrapped*
    base, so a policy that claims only the wrapped spelling loses the lane on
    the retry and lands on Binance -- the exact fallback the chain refuses,
    arriving through the back door and without the DeFi confidence haircut.
    """

    @staticmethod
    def _router_claiming_only_wrapped():
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE,
            venue="gmx_v2",
            chain="arbitrum",
            markets={"WETH": "ETH/USD"},  # NOT the unwrapped spelling
        )
        router = OHLCVRouter(default_chain="arbitrum", source_policy=policy)

        class _AlwaysFails:
            name = VENUE_NATIVE_PROVIDER
            data_class = DataClassification.INFORMATIONAL

            def fetch(self, **_kw):
                raise DataSourceUnavailable(source=VENUE_NATIVE_PROVIDER, reason="venue lagging")

            def health(self):
                return {}

        class _Binance:
            name = "binance"
            data_class = DataClassification.INFORMATIONAL
            called = False

            def fetch(self, **_kw):
                type(self).called = True
                raise DataSourceUnavailable(source="binance", reason="should never be consulted")

            def health(self):
                return {}

        router.register_provider(_AlwaysFails())
        binance = _Binance()
        router.register_provider(binance)
        return router, binance

    def test_venue_native_miss_does_not_proxy_into_binance(self):
        router, binance = self._router_claiming_only_wrapped()
        with pytest.raises(DataSourceUnavailable):
            router.get_ohlcv(token="WETH", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)
        assert binance.called is False, (
            "the venue-native lane fell back to a CEX through the proxy retry; "
            "the strategy would decide on a CEX tape while marking against the venue index"
        )

    def test_liveness_an_unclaimed_instrument_still_gets_its_proxy_retry(self):
        """Without this, "never proxy" would pass the test above while breaking
        the wrapped-token fallback every non-perp strategy relies on."""
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE, venue="gmx_v2", chain="arbitrum", markets={"XRP": "XRP/USD"}
        )
        router = OHLCVRouter(default_chain="arbitrum", source_policy=policy)
        seen: list[str] = []

        class _Recorder:
            name = "binance"
            data_class = DataClassification.INFORMATIONAL

            def fetch(self, **kw):
                seen.append(str(kw.get("token")))
                raise DataSourceUnavailable(source="binance", reason="miss")

            def health(self):
                return {}

        router.register_provider(_Recorder())
        with contextlib.suppress(Exception):
            router.get_ohlcv(token="WETH", chain="arbitrum", timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)
        assert "ETH" in seen, "the wrapped-token proxy retry must still happen off the venue-native lane"

    def test_forced_provider_still_gets_its_proxy_retry_on_the_lane(self):
        """`force_provider` is threaded through the retry, so the recursion cannot
        land anywhere the first attempt would not have. Skipping the proxy for the
        whole lane refused a fallback that was never able to leave it."""
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE, venue="gmx_v2", chain="arbitrum", markets={"WETH": "ETH/USD"}
        )
        router = OHLCVRouter(default_chain="arbitrum", source_policy=policy)
        seen: list[str] = []

        class _Recorder:
            name = "binance"
            data_class = DataClassification.INFORMATIONAL

            def fetch(self, **kw):
                seen.append(str(kw.get("token")))
                raise DataSourceUnavailable(source="binance", reason="miss")

            def health(self):
                return {}

        router.register_provider(_Recorder())
        with contextlib.suppress(Exception):
            router.get_ohlcv(
                token="WETH",
                chain="arbitrum",
                timeframe=OHLCVTimeframe.FIVE_MINUTES,
                limit=30,
                force_provider="binance",
            )
        assert "ETH" in seen, (
            "an explicit force_provider override lost its proxy retry: the guard fired outside "
            "the case it reasons about, because a forced retry cannot re-classify onto a CEX"
        )


class TestGuardAgreesWithTheResolverAboutNames:
    """A refusal on this lane is terminal -- there is no fallback to absorb it.

    So the two identity guards must not be *stricter* than the registry the
    request is resolved against: every spelling the connector accepts has to
    survive the guard, or a supported config becomes an unrecoverable error.
    """

    @staticmethod
    def _provider(policy, response):
        gateway = SimpleNamespace(
            is_connected=True,
            config=SimpleNamespace(timeout=5.0),
            rate_history=SimpleNamespace(GetPerpPriceCandles=lambda _req, timeout=None: response),
        )
        return VenueNativeOHLCVProvider(gateway_client=gateway, policy=policy)

    @staticmethod
    def _page(**kw):
        now = int(datetime.now(UTC).timestamp()) // 300 * 300
        candles = [
            SimpleNamespace(timestamp=now - (29 - i) * 300, open="10", high="10", low="10", close="10")
            for i in range(30)
        ]
        base = {
            "success": True,
            "error": "",
            "market": "ETH/USD",
            "market_token": "0xmarket",
            "index_token": "0xindex",
            "index_symbol": "WETH",
            "timeframe": "5m",
            "candles": candles,
        }
        base.update(kw)
        return SimpleNamespace(**base)

    def _fetch(self, *, declared_market, served_market, served_index, base, chain="arbitrum"):
        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE, venue="gmx_v2", chain=chain, markets={base: declared_market}
        )
        provider = self._provider(policy, self._page(market=served_market, index_symbol=served_index))
        return provider.fetch(token=base, chain=chain, timeframe=OHLCVTimeframe.FIVE_MINUTES, limit=30)

    @pytest.mark.parametrize("declared", ["ETH", "ETH/USD", "ETH-USD", "ETH_USD", "eth:usd"])
    def test_every_spelling_canonicalise_market_accepts_survives_the_guard(self, declared):
        """`perp_market_pair_key` (via `canonicalise_market`) collapses all of
        these to one market, and bare connector aliases stay bare on purpose."""
        envelope = self._fetch(
            declared_market=declared, served_market="ETH/USD", served_index="WETH", base="ETH"
        )
        assert len(envelope.value) == 30

    @pytest.mark.parametrize(
        ("served_index", "base"),
        [
            ("WETH", "ETH"),  # arbitrum: GMX answers with the token symbol
            ("WBTC", "BTC"),
            ("WETH.e", "ETH"),  # avalanche: GMX lists ETH only as the bridged form
            ("BTC.b", "BTC"),
            ("WAVAX", "AVAX"),
        ],
    )
    def test_venue_token_spellings_resolve_to_the_asset_requested(self, served_index, base):
        envelope = self._fetch(
            declared_market=f"{base}/USD",
            served_market=f"{base}/USD",
            served_index=served_index,
            base=base,
        )
        assert len(envelope.value) == 30

    def test_bridged_base_token_is_claimed_not_dropped_to_a_cex(self):
        """The claim side and the response side must agree about a symbol.

        They did not. `_declared_markets` registered only `WETH.E` for
        `base_token="WETH.e"` — the spelling GMX itself uses for Avalanche ETH,
        so a strategist copying from `GMX_V2_TOKENS` writes exactly that — while
        a request for `ETH` arrives canonicalised to `WETH`. The claim missed and
        the request fell through to Binance: the silent CEX basis swap this lane
        exists to prevent, entering through the claim rather than the fallback.
        """
        from almanak.framework.data.models import resolve_instrument
        from almanak.framework.data.ohlcv.venue_context import _declared_markets

        for base_token, requested, market in [
            ("WETH.e", "ETH", "ETH/USD"),
            ("BTC.b", "BTC", "BTC/USD"),
            ("WETH", "ETH", "ETH/USD"),
            ("ETH", "ETH", "ETH/USD"),
        ]:
            policy = OHLCVSourcePolicy(
                source=SOURCE_VENUE_NATIVE,
                venue="gmx_v2",
                chain="avalanche",
                markets=_declared_markets({"market": market, "base_token": base_token}),
            )
            instrument = resolve_instrument(requested, "avalanche")
            assert policy.claims(instrument.base, "avalanche"), (
                f"base_token={base_token!r} did not claim a {requested!r} request "
                f"(resolved to {instrument.base!r}); it would be served by a CEX"
            )

    @pytest.mark.parametrize("declared", ["ETH/USD", "ETH-USD", "ETH_USD", "ETH:USD", "ETH/USD [ETH-USDC]"])
    def test_the_claim_producer_knows_every_separator_the_guard_knows(self, declared):
        """The two halves of one feature must agree about what a market name is.

        `eb0aa4323e` taught the *guard* all four separators via
        `perp_market_pair_key`, but the *claim producer* still split on `/` and
        `-` only. `ETH_USD` — which the GMX compiler resolves fine — produced no
        claim at all, so the request never entered this lane and Binance served
        it, while the position marked against the GMX index. Silent, and the
        only diagnostic said the strategy "declares no market", which it did.
        """
        from almanak.framework.data.models import resolve_instrument
        from almanak.framework.data.ohlcv.venue_context import _declared_markets

        policy = OHLCVSourcePolicy(
            source=SOURCE_VENUE_NATIVE,
            venue="gmx_v2",
            chain="arbitrum",
            markets=_declared_markets({"market": declared}),
        )
        instrument = resolve_instrument("ETH", "arbitrum")
        assert policy.claims(instrument.base, "arbitrum"), (
            f"market={declared!r} produced no claim; the request would be served by a CEX"
        )

    def test_base_token_does_not_mispair_a_multi_market_strategy(self):
        """`base_token` names the base of ONE market. Binding it to the first
        mapped BTC -> ETH/USD and dropped ETH from the policy entirely, so the
        BTC leg failed loudly on the index guard and the ETH leg went to a CEX."""
        from almanak.framework.data.ohlcv.venue_context import _declared_markets

        markets = _declared_markets({"markets": ["ETH/USD", "BTC/USD"], "base_token": "BTC"})
        assert markets.get("BTC") == "BTC/USD", "base_token mispaired BTC to another market"
        assert markets.get("ETH") == "ETH/USD", "ETH was dropped from the policy entirely"

    def test_liveness_a_single_market_still_honours_base_token(self):
        """Without this, ignoring `base_token` outright would pass the test above
        while breaking the only way an address-declared market can be claimed."""
        from almanak.framework.data.ohlcv.venue_context import _declared_markets

        assert _declared_markets({"market": "0xabc", "base_token": "ETH"}).get("ETH") == "0xabc"
        assert _declared_markets({"market": "0xabc"}) == {}

    def test_a_wrapper_prefix_is_not_a_licence_to_strip_a_letter(self):
        """wstETH and stETH are different assets at different prices. A generic
        `startswith("W")` strip read them as one, which is the silent basis swap
        this guard exists to prevent -- arriving from inside the guard."""
        with pytest.raises(DataSourceUnavailable, match="different asset"):
            self._fetch(
                declared_market="STETH/USD",
                served_market="STETH/USD",
                served_index="WSTETH",
                base="STETH",
            )
