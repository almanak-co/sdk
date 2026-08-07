"""Venue-native OHLCV source selection (ALM-3148 / ALM-3152).

Why this module exists
----------------------

A strategy trading a perpetual on GMX asked ``market.ema("XRP")`` and never got
an answer. The router classified XRP from a *static* CEX symbol table, found no
entry, called it DeFi-native, and sent the request to a pool-search provider
that has no XRP pool — while the venue the strategy was actually trading
publishes its own XRP index candles. Two hosted agents sat in permanent HOLD
with a green heartbeat.

The missing input was never the data. It was the fact that *the request came
from a strategy that trades this instrument on a specific venue*. This module
carries that fact, and the router consults it before it classifies.

The design decision, and why it is configurable
-----------------------------------------------

**Venue-native is the default, not a hardcoded answer.** Defaulting to the
venue's own index series removes signal-vs-fill basis risk: the number the
strategy decides on is the number its position is marked and liquidated
against. That is the right default for the common case, and it is what a
strategist who has expressed no opinion should get.

It is not universally better, and the cases where it is worse are legitimate
strategy choices rather than edge cases:

===========================================  =====================================
Strategy intent                              Preferred source
===========================================  =====================================
Directional TA aligned with fill/liquidation Venue-native (the default)
Momentum deliberately *leading* the oracle   ``binance`` — CEX price commonly
                                             leads the venue oracle
Volume / VWAP / OBV-style signals            ``binance`` — a perp index plane
                                             publishes **no volume at all**
DeFi-native token with no CEX listing        ``auto`` (today's classification)
===========================================  =====================================

So the strategist decides and the framework only picks the default, resolved
the same way :meth:`MarketSnapshot._resolve_timeframe` resolves timeframes:

    explicit call argument  >  strategy config  >  framework default

If you are reading this because you want to delete the indirection and hardcode
the venue: don't. The volume row above is the concrete reason — a volume
indicator against a volume-less source cannot be served at all, and the config
key is the documented way out of it.

Scaling to other venues
-----------------------

Nothing here knows what GMX is. A venue becomes eligible by declaring
``perp_price_history=PerpPriceHistoryDecl(...)`` on its connector manifest and
implementing ``GatewayPerpPriceHistoryCapability`` gateway-side; Hyperliquid and
Aster light up with no change to this module, the router, or the runner.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

__all__ = [
    "SOURCE_AUTO",
    "SOURCE_VENUE_NATIVE",
    "VENUE_NATIVE_PROVIDER",
    "OHLCVSourcePolicy",
    "build_source_policy",
    "resolve_ohlcv_source",
]

logger = logging.getLogger(__name__)

# Router provider name and classification label for the venue-native lane.
VENUE_NATIVE_PROVIDER = "venue_native"

# Strategy-config source values that are not literal provider names.
SOURCE_VENUE_NATIVE = "venue_native"
SOURCE_AUTO = "auto"

# The strategy-config key a strategist sets to override the default.
OHLCV_SOURCE_CONFIG_KEY = "ohlcv_source"


@dataclass(frozen=True)
class OHLCVSourcePolicy:
    """Which candle series this strategy's OHLCV requests should resolve to.

    Attributes:
        source: ``"venue_native"`` (default), ``"auto"`` (today's CEX/DEX
            classification), or a literal registered provider name such as
            ``"binance"``.
        venue: Connector-declared venue slug (e.g. ``"gmx_v2"``). ``None`` when
            the strategy trades no venue that publishes native candles.
        chain: Canonical chain the venue markets live on.
        markets: Canonical base symbol -> the venue market identifier the
            strategy itself declared. Threading the strategy's *own* market
            here is what keeps the signal series pinned to the market it
            trades, rather than a differently-collateralised namesake.
    """

    source: str = SOURCE_VENUE_NATIVE
    venue: str | None = None
    chain: str | None = None
    markets: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", (self.source or SOURCE_VENUE_NATIVE).strip().lower())
        if self.venue is not None:
            object.__setattr__(self, "venue", self.venue.strip().lower())
        if self.chain is not None:
            object.__setattr__(self, "chain", self.chain.strip().lower())
        object.__setattr__(
            self,
            "markets",
            MappingProxyType({str(base).upper(): str(market) for base, market in self.markets.items()}),
        )

    # -- claim ---------------------------------------------------------------

    def market_for(self, base: str, chain: str | None = None) -> str | None:
        """Return the venue market identifier for ``base``, or ``None``.

        ``None`` means "this policy does not claim this instrument" — the
        caller falls back to ordinary classification. That is the bound on the
        implicit-venue rule: a GMX strategy holding an unrelated spot leg gets
        the spot leg's normal routing, because the policy only knows about the
        markets the strategy declared.

        The lookup falls back to :func:`price_identity` so the claim side and
        the response side agree about what a symbol names. They did not: a
        strategy declaring ``base_token="WETH.e"`` — the spelling GMX itself
        uses for Avalanche ETH — registered only ``WETH.E``, while a request
        for ``ETH`` arrives canonicalised to ``WETH``. The claim missed, the
        request fell through to ordinary classification, and Binance served it.
        That is the silent CEX basis swap this whole lane exists to prevent,
        entering through the claim rather than through the fallback.
        """
        if not self.venue or not self.markets:
            return None
        if chain is not None and self.chain is not None and chain.strip().lower() != self.chain:
            return None
        key = base.strip().upper()
        hit = self.markets.get(key)
        if hit is None:
            hit = self.markets.get(price_identity(key))
        return hit

    def claims(self, base: str, chain: str | None = None) -> bool:
        """True when the venue-native lane owns ``base`` under this policy."""
        return self.source == SOURCE_VENUE_NATIVE and self.market_for(base, chain) is not None

    def claims_any(self) -> bool:
        """True when this policy can route at least one instrument venue-native.

        Used by the provider-chain ↔ registry invariant to decide whether the
        venue-native provider is required to be registered.
        """
        return self.source == SOURCE_VENUE_NATIVE and bool(self.venue) and bool(self.markets)

    def pinned_provider(self) -> str | None:
        """Return the provider the strategist pinned, if any.

        ``venue_native`` and ``auto`` are policies rather than providers and
        return ``None``; anything else is a literal provider name the router
        should use for *every* instrument, which is what "use Binance for this
        strategy" has to mean to be worth having.
        """
        if self.source in (SOURCE_VENUE_NATIVE, SOURCE_AUTO):
            return None
        return self.source


def resolve_ohlcv_source(
    *,
    configured: object,
    venue: str | None,
    known_providers: frozenset[str] | set[str],
) -> str:
    """Resolve the effective OHLCV source for a strategy.

    Priority: strategy config > framework default. (The per-call override lives
    at the call site — ``market.ohlcv(..., data_source=...)`` — and is applied
    by the router, which is why it does not appear here.)

    The framework default is ``venue_native`` when the strategy trades a venue
    that publishes native candles, and ``auto`` otherwise. An unconfigured
    strategy on a non-perp venue therefore behaves exactly as it does today.

    Args:
        configured: Raw ``ohlcv_source`` config value, possibly ``None``.
        venue: Venue slug resolved from the strategy's declared protocols, or
            ``None`` when it trades no venue with a native candle source.
        known_providers: Provider names the router can actually route to; an
            unknown name is rejected loudly rather than silently ignored,
            because silently ignoring it would serve a different market than
            the strategist asked for.

    Raises:
        ValueError: If ``configured`` names neither a policy nor a registered
            provider, or asks for ``venue_native`` on a strategy with no
            native-candle venue.
    """
    if configured is None or (isinstance(configured, str) and not configured.strip()):
        return SOURCE_VENUE_NATIVE if venue else SOURCE_AUTO

    if not isinstance(configured, str):
        raise ValueError(f"{OHLCV_SOURCE_CONFIG_KEY} must be a string, got {type(configured).__name__}")

    candidate = configured.strip().lower()
    if candidate == SOURCE_AUTO:
        return SOURCE_AUTO
    if candidate == SOURCE_VENUE_NATIVE:
        if not venue:
            raise ValueError(
                f"{OHLCV_SOURCE_CONFIG_KEY}='venue_native' requires a strategy protocol that publishes "
                "venue-native candles (a connector declaring perp_price_history); this strategy declares "
                f"none. Use '{SOURCE_AUTO}' or a provider name instead."
            )
        return SOURCE_VENUE_NATIVE
    if candidate in known_providers:
        return candidate

    raise ValueError(
        f"Unknown {OHLCV_SOURCE_CONFIG_KEY}={configured!r}. Valid values: "
        f"'{SOURCE_VENUE_NATIVE}', '{SOURCE_AUTO}', or one of {sorted(known_providers)}."
    )


# ---------------------------------------------------------------------------
# Building a policy from a strategy
# ---------------------------------------------------------------------------


def _declared_venue(protocols: list[str], chain: str, configured_venue: object) -> tuple[str | None, list[str]]:
    """Resolve the venue whose native candles this strategy can use.

    Manifest-driven: a protocol is eligible when its connector declares
    ``perp_price_history`` for this chain. Nothing here enumerates venues, so
    Hyperliquid and Aster become eligible by declaring the capability.

    Returns:
        ``(venue, eligible)``. ``venue`` is ``None`` when the strategy declares
        no eligible protocol *or* declares more than one and did not say which.
        ``eligible`` is returned alongside so the caller can tell those two
        cases apart — they need different errors, and reporting "declares none"
        for a strategy that declares two would send the reader looking for a
        missing declaration that is not missing.
    """
    from almanak.connectors._strategy_base.perp_price_history_registry import (
        PerpPriceHistoryRegistry,
    )

    eligible = [
        protocol
        for protocol in protocols
        if PerpPriceHistoryRegistry.has(protocol) and chain in PerpPriceHistoryRegistry.declared_chains(protocol)
    ]

    if isinstance(configured_venue, str) and configured_venue.strip():
        requested = configured_venue.strip().lower()
        for protocol in eligible:
            if PerpPriceHistoryRegistry.venue_for(protocol).lower() == requested or protocol.lower() == requested:
                return PerpPriceHistoryRegistry.venue_for(protocol), eligible
        raise ValueError(
            f"ohlcv_venue={configured_venue!r} is not a protocol this strategy declares with venue-native "
            f"candle support on {chain}. Declared and eligible: {eligible or 'none'}."
        )

    if not eligible:
        return None, eligible
    if len(eligible) > 1:
        # Two venues quoting the same base would give one symbol two different
        # answers. Refusing beats picking by list order; the strategist names
        # the one they mean.
        #
        # Degrading to ordinary classification here rather than raising is
        # deliberate, and it is the *implicit* case only: a strategy that
        # declares two perp protocols and never asked for venue-native candles
        # must not stop booting because the SDK gained this feature. It gets
        # exactly the behaviour it had before, plus a warning. When the
        # strategist DID ask, `build_source_policy` raises instead — see there.
        logger.warning(
            "ohlcv_venue_ambiguous protocols=%s chain=%s — set ohlcv_venue to choose; "
            "falling back to default classification",
            eligible,
            chain,
        )
        return None, eligible
    return PerpPriceHistoryRegistry.venue_for(eligible[0]), eligible


# Bridged spellings a venue lists an asset under. Stripping these is correct
# *for a price plane* and would be wrong for a balance: `USDC.e` and `USDC` are
# distinct holdings but quote one price, and GMX lists Avalanche ETH and BTC
# only as `WETH.e` / `BTC.b` (`gmx_v2/addresses.py`). Only price-plane identity
# is decided here.
_BRIDGED_SUFFIXES = (".E", ".B")


def price_identity(symbol: str) -> str:
    """Reduce a token symbol to the asset whose price plane it quotes.

    The single source of truth for "are these two spellings the same price?".
    Both the *claim* side (`_declared_markets` / `OHLCVSourcePolicy.market_for`)
    and the *response* side (`_index_symbols_agree`) must answer that question
    identically — when they did not, a config the claim side rejected was
    quietly served by a CEX instead.
    """
    from almanak.framework.data.models import _NATIVE_TO_WRAPPED

    upper = symbol.strip().upper()
    for suffix in _BRIDGED_SUFFIXES:
        if upper.endswith(suffix) and len(upper) > len(suffix):
            upper = upper[: -len(suffix)]
            break
    # Reverse of the framework's native->wrapped table, plus the one wrapper
    # that is not a gas token. Deliberately a table and not `startswith("W")`:
    # the generic strip read `WSTETH` as `STETH`, which are different assets
    # with different prices, and accepting one for the other is exactly the
    # silent basis swap this guard exists to prevent.
    unwrapped = {wrapped: native for native, wrapped in _NATIVE_TO_WRAPPED.items()}
    unwrapped["WBTC"] = "BTC"
    return unwrapped.get(upper, upper)


def _declared_markets(strategy_config: Mapping[str, object]) -> dict[str, str]:
    """Map each traded base symbol to the market identifier the strategy declared.

    Accepts ``market`` (one) or ``markets`` (several). The base is taken from
    ``base_token`` when present and otherwise from the market label, and is
    registered under both its raw and canonical (wrapped) spellings because
    ``resolve_instrument`` canonicalises ``ETH`` to ``WETH`` before the router
    ever sees it.
    """
    from almanak.framework.data.models import _canonicalize_symbol

    raw_markets: list[str] = []
    single = strategy_config.get("market")
    if isinstance(single, str) and single.strip():
        raw_markets.append(single.strip())
    several = strategy_config.get("markets")
    if isinstance(several, list | tuple):
        raw_markets.extend(str(item).strip() for item in several if str(item).strip())

    from almanak.core.perp_markets import perp_market_base

    declared_base = strategy_config.get("base_token")
    single = len(raw_markets) == 1
    markets: dict[str, str] = {}
    for market in raw_markets:
        # `perp_market_base` splits on all four separators the connectors accept
        # and returns None for a raw address (which carries no base symbol and
        # can only be claimed via an explicit `base_token`). Hand-rolling `/` and
        # `-` meant `ETH_USD` — a spelling the GMX compiler resolves fine —
        # produced no claim at all, so the request never entered this lane and a
        # CEX served it. The guard already delegates to the same module; the
        # claim producer has to agree with it or the halves disagree about what
        # a market name is.
        base = perp_market_base(market.split("[", 1)[0])
        # `base_token` names the base of ONE declared market. With several it
        # cannot say which, and binding it to the first mispaired every other:
        # `markets=[ETH/USD, BTC/USD], base_token=BTC` mapped BTC -> ETH/USD and
        # dropped ETH entirely, so ETH silently fell through to a CEX.
        if single and isinstance(declared_base, str) and declared_base.strip():
            base = declared_base.strip()
        if not base:
            continue
        # Register the price identity alongside the raw and wrapped spellings.
        # Purely additive — it can only make a claim match that would otherwise
        # have fallen through to a CEX, never redirect one that already matched.
        for spelling in {base.upper(), _canonicalize_symbol(base), price_identity(base)}:
            markets.setdefault(spelling, market)
    return markets


def build_source_policy(
    *,
    strategy: object,
    strategy_config: Mapping[str, object] | None,
    chain: str,
    known_providers: frozenset[str] | set[str],
) -> OHLCVSourcePolicy:
    """Build the OHLCV source policy for one strategy on one chain.

    The venue is derived from what the strategy *declares* rather than asked
    for at each call site. ``market.ema("XRP")`` passes a bare string and always
    will; if the venue had to be supplied per call, the two hosted agents in
    ALM-3148 / ALM-3152 would stay stuck until their authors changed code and
    redeployed. Deriving it from the strategy's own declared protocol and market
    is what lets an already-deployed agent recover on the next release.
    """
    config: Mapping[str, object] = strategy_config or {}
    chain_key = chain.strip().lower()

    protocols = list(getattr(getattr(strategy, "STRATEGY_METADATA", None), "supported_protocols", None) or [])
    venue, eligible = _declared_venue(protocols, chain_key, config.get("ohlcv_venue"))

    configured_source = config.get(OHLCV_SOURCE_CONFIG_KEY)
    asked_for_venue_native = (
        isinstance(configured_source, str) and configured_source.strip().lower() == SOURCE_VENUE_NATIVE
    )
    if venue is None and len(eligible) > 1 and asked_for_venue_native:
        # The strategist asked for venue-native candles on a strategy that
        # trades two venues publishing them. Degrading to classification here
        # would serve a price plane they did not choose while their request sat
        # in the config looking honoured. Name the real cause: the generic
        # "declares no such protocol" error would be actively misleading, since
        # this strategy declares two.
        raise ValueError(
            f"{OHLCV_SOURCE_CONFIG_KEY}='venue_native' is ambiguous: this strategy declares "
            f"{eligible} on {chain_key}, and both publish venue-native candles. Set ohlcv_venue "
            "to the one whose price plane the signal should use."
        )

    markets = _declared_markets(config) if venue else {}
    if venue and not markets:
        # The venue publishes candles but the strategy never said which market
        # it trades, so there is nothing to pin the series to. Ordinary
        # classification still applies — this is a missing declaration, not a
        # failure.
        logger.info(
            "ohlcv_venue_native_unclaimed venue=%s chain=%s — strategy declares no market/markets config; "
            "using default classification",
            venue,
            chain_key,
        )

    source = resolve_ohlcv_source(
        configured=configured_source,
        venue=venue if markets else None,
        known_providers=known_providers,
    )
    policy = OHLCVSourcePolicy(source=source, venue=venue, chain=chain_key, markets=markets)
    logger.info(
        "ohlcv_source_policy source=%s venue=%s chain=%s markets=%s",
        policy.source,
        policy.venue,
        policy.chain,
        dict(policy.markets) or "none",
    )
    return policy
