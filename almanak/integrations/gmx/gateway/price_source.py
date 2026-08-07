"""Gateway-only GMX venue ticker price source (ALM-3177).

GMX perp markets use **synthetic index tokens** — identifier addresses with no
deployed contract on any chain (DOGE, XMR, ZEC, gold, oil, … — 100 of the 127
tokens GMX prices on Arbitrum). No address-based price source can ever serve
them, and the previous coverage model (hand-curated CoinGecko slugs declared in
the connector's ``coingecko_ids()``, VIB-6219) is a treadmill the venue outruns:
XMR listed 2025-06-02 and was still unpriceable when ALM-3177 was filed —
which makes every XMR ``PERP_OPEN`` fail closed at acceptable-price derivation.

This source serves the venue's own ``/prices/tickers`` feed — the signed oracle
plane the GMX keeper actually settles orders against, and the only feed that by
construction covers every market the venue lists. Venue-native by design, the
same reasoning as the ALM-3148 venue-native OHLCV router for candles.

Scope (deliberately narrow — two independent gates, both required):

* The connector serves only **synthetic index rows** from the venue feed
  (``GatewayVenueTickerPriceCapability`` scope contract). Necessary but NOT
  sufficient: ``synthetic`` describes GMX's identifier *address*, not the
  symbol — CRV / DOT / LDO / BONK / … have synthetic GMX index rows AND real
  deployed Arbitrum contracts in our token registry.
* This source therefore additionally gates out any symbol with a
  **chain-deployed identity in the SDK's static token registry** — those
  symbols are the address-based sources' vote, and answering here would
  change existing spot pricing for them (the exact regression this scope
  exists to prevent).
* **Anything else** → :class:`DataSourceUnavailable` (a miss), so the
  aggregator falls through to CoinGecko / DexScreener / Binance.

The venue-specific bits (API base URLs, catalogue caching, price descaling)
are NOT imported from the connector — the gateway↔connector isolation ratchet
(VIB-4121) forbids importing ``almanak.connectors.gmx_v2.*`` here. The perp
connector publishes them through ``GatewayVenueTickerPriceCapability``
(resolved from ``GATEWAY_REGISTRY.capability_providers`` keyed on
``ticker_price_chains()``); this source owns only the freshness policy and the
Empty≠Zero miss semantics.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from functools import lru_cache
from typing import TYPE_CHECKING

from almanak.connectors._base.gateway_capabilities import (
    GatewayVenueTickerPriceCapability,
)
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.core.chains import ChainRegistry
from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.models import ResolvedToken

logger = logging.getLogger(__name__)

# This integration's manifest name — the typed dispatch identity matched
# against ``GatewayVenueTickerPriceCapability.ticker_price_integration()``.
# Naming OURSELVES is not a protocol coupling; it keeps venue selection
# qualified (identity AND chain) instead of registry-order-dependent.
_INTEGRATION_NAME = "gmx"

# Freshness policy against the VENUE's own observation timestamp, not fetch
# time. GMX tickers update continuously (~1s cadence), so a minute-old
# observation already signals an upstream stall; five minutes means the feed
# is lying about "now" and an unavailable price beats a wrong one (Empty≠Zero).
_FRESH_MAX_AGE_SECONDS = 60.0
_STALE_MAX_AGE_SECONDS = 300.0


def _canonical_chain(chain: object | None) -> str | None:
    """Canonical lowercase chain name for cross-chain comparison, or ``None``.

    Canonicalizes aliases via the ChainRegistry so a ``ResolvedToken.chain``
    can be compared apples-to-apples against this source's chain (VIB-5651).
    """
    if not chain:
        return None
    desc = ChainRegistry.try_resolve(str(chain).lower())
    return desc.name if desc is not None else str(chain).lower()


@lru_cache(maxsize=8)
def _chain_deployed_symbols(chain: str) -> frozenset[str]:
    """Uppercase symbols with a registered contract on ``chain``.

    Built from the SDK's static token registry — the same catalogue the
    resolver's static index serves — so this gate and the rest of the stack
    share one notion of "deployed identity". GMX's ``synthetic`` flag cannot
    stand in for this: it describes the venue's index identifier address, not
    whether the symbol has a real token on the chain (CRV / DOT / LDO / BONK
    are synthetic on GMX Arbitrum AND deployed in the registry).
    """
    from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS

    return frozenset(token.symbol.upper() for token in DEFAULT_TOKENS if token.has_address_on(chain))


class GmxTickerPriceSource(BasePriceSource):
    """Price source serving GMX synthetic-index USD mids from the venue feed.

    Implements the same :class:`BasePriceSource` contract as
    :class:`HypercoreOraclePriceSource` / :class:`PythPriceSource`:
    ``async get_price(token, quote="USD", *, resolved_token=None) -> PriceResult``,
    raising :class:`DataSourceUnavailable` on a miss. Page caching lives in the
    connector capability (one fetch serves a whole compile's symbol lookups).
    """

    def __init__(self, *, chain: str) -> None:
        self._chain = _canonical_chain(chain) or str(chain).lower()

        # Venue ticker provider, resolved once from the gateway registry (NOT a
        # direct connector import — gateway↔connector isolation, VIB-4121).
        # Dispatch matches BOTH the integration identity the provider declares
        # (``ticker_price_integration()`` — typed, connector-owned, no protocol
        # literal here) AND the chain, so a second venue on the same chain can
        # never be routed under this source's name by registry order. Resolved
        # at construction so dispatch stays O(1) per request.
        self._provider: GatewayVenueTickerPriceCapability | None = None
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayVenueTickerPriceCapability):  # type: ignore[type-abstract]
            if (
                provider.ticker_price_integration() == _INTEGRATION_NAME
                and self._chain in provider.ticker_price_chains()
            ):
                self._provider = provider
                break

    @property
    def source_name(self) -> str:
        return "gmx_ticker"

    async def get_price(
        self, token: str, quote: str = "USD", *, resolved_token: ResolvedToken | None = None
    ) -> PriceResult:
        """Fetch the venue ticker mid for a synthetic GMX index symbol.

        Raises:
            DataSourceUnavailable: On a non-USD quote, a cross-chain request, a
                symbol outside the venue's synthetic catalogue, a feed outage,
                or an observation older than the staleness cutoff — never a
                fabricated price (Empty≠Zero).
        """
        # Chain-correctness guard (VIB-5651): a request explicitly tagged with
        # a different chain must never be answered by this chain's venue feed.
        # Synthetic index prices are global so the numeric risk is low, but a
        # mis-route must stay unrepresentable.
        if resolved_token is not None:
            rt_chain = _canonical_chain(getattr(resolved_token, "chain", None))
            if rt_chain and rt_chain != self._chain:
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=f"chain_mismatch:{rt_chain}!={self._chain}",
                )

        if quote.upper() != "USD":
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Only USD quote supported, got {quote}",
            )

        # Deployed-identity gate: GMX's synthetic flag describes the venue's
        # index identifier address, NOT the symbol. A symbol registered with a
        # contract on this chain (CRV, DOT, LDO, …) belongs to the
        # address-based sources; answering here would change existing spot
        # pricing for it. Checked before any fetch — the gate is static.
        token_upper = token.upper()
        if token_upper in _chain_deployed_symbols(self._chain):
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=(
                    f"{token_upper} has a deployed identity on {self._chain} in the token registry; "
                    "the venue ticker feed only serves symbols with no on-chain identity"
                ),
            )

        if self._provider is None:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"No venue ticker provider registered for chain={self._chain}",
            )

        try:
            page = await self._provider.fetch_ticker_prices(chain=self._chain)
        except Exception as exc:
            # Transport / catalogue failure is a source outage, not an error the
            # aggregator should crash on.
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"GMX ticker feed unavailable for chain={self._chain}: {exc}",
            ) from exc

        entry = page.get(token_upper)
        if entry is None:
            # Not a synthetic GMX index symbol → miss, so the aggregator falls
            # through to the address-based sources for spot tokens.
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"{token_upper} is not a synthetic GMX index symbol on {self._chain}",
            )

        # Symmetric bound: a future-dated observation (age < 0 beyond clock-skew
        # tolerance) is as untrustworthy as a stale one — without the lower
        # bound it would be served fresh at full confidence.
        age = time.time() - entry.updated_at
        if age > _STALE_MAX_AGE_SECONDS or age < -_FRESH_MAX_AGE_SECONDS:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=(
                    f"GMX ticker for {entry.symbol} has implausible age {age:.0f}s "
                    f"(stale cutoff {_STALE_MAX_AGE_SECONDS:.0f}s, future tolerance {_FRESH_MAX_AGE_SECONDS:.0f}s)"
                ),
            )
        stale = age > _FRESH_MAX_AGE_SECONDS
        return PriceResult(
            price=entry.price_usd,
            source=self.source_name,
            timestamp=datetime.fromtimestamp(entry.updated_at, UTC),
            confidence=0.7 if stale else 0.95,
            stale=stale,
        )


__all__ = ["GmxTickerPriceSource"]
