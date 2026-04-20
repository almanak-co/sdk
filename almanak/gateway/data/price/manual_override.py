"""Manual price override source (Bug 3 of the 0G DogFooding report, 2026-04-16).

Some tokens have no oracle coverage — no Chainlink feed, not listed on
CoinGecko / Binance / DexScreener. The canonical example is ``W0G`` on
0G Chain: every automated price source fails, the circuit breaker trips
after 5 failures, and every price-dependent operation (teardown, slippage
calculation, swap quoting) is blocked for ~30 minutes.

This module provides a last-resort ``ManualPriceOverrideSource`` that reads
prices from environment variables. Operators can unblock strategies on
unsupported tokens by setting:

    ALMANAK_PRICE_OVERRIDE_W0G=0.12          # W0G priced at $0.12 USD
    ALMANAK_PRICE_OVERRIDE_WBTC=95000        # WBTC/USD
    ALMANAK_PRICE_OVERRIDE_W0G_WBTC=0.0000012  # W0G/WBTC pair

Confidence is deliberately low (0.5) so that when a real oracle source
also produces a price, the aggregator prefers the real one. The override
only wins when nothing else does.

This is explicitly a *safety valve*, not a production pricing mechanism.
The preferred long-term fix is a ``UniswapV3PoolPriceSource`` that reads
``slot0()`` from discovered pools; this source buys time without requiring
that infrastructure to ship first.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.models import ResolvedToken

logger = logging.getLogger(__name__)

_ENV_PREFIX = "ALMANAK_PRICE_OVERRIDE_"
_SOURCE_NAME = "manual_override"
_DEFAULT_CONFIDENCE = 0.5


def _normalise(symbol: str) -> str:
    """Uppercase and strip so env vars are easier to type."""
    return symbol.strip().upper()


def _lookup(token: str, quote: str) -> Decimal | None:
    """Return the override price for ``token/quote`` if one is configured.

    Resolution order:
      1. ALMANAK_PRICE_OVERRIDE_{TOKEN}_{QUOTE}  (explicit pair)
      2. ALMANAK_PRICE_OVERRIDE_{TOKEN}          (implicit USD)

    Invalid (non-decimal) values are logged and ignored so a typo can't
    poison production pricing.
    """
    token_u = _normalise(token)
    quote_u = _normalise(quote)
    candidates = [f"{_ENV_PREFIX}{token_u}_{quote_u}"]
    if quote_u == "USD":
        candidates.append(f"{_ENV_PREFIX}{token_u}")

    for var in candidates:
        raw = os.environ.get(var)
        if raw is None:
            continue
        try:
            value = Decimal(raw)
        except (InvalidOperation, TypeError):
            logger.warning("Invalid manual price override for %s: %r", var, raw)
            continue
        if value <= 0:
            logger.warning("Non-positive manual price override for %s: %s", var, value)
            continue
        return value
    return None


class ManualPriceOverrideSource(BasePriceSource):
    """Environment-variable-backed price source (last-resort fallback).

    The aggregator consults this source like any other. It returns a
    low-confidence ``PriceResult`` when ``ALMANAK_PRICE_OVERRIDE_{TOKEN}``
    (or ``_{TOKEN}_{QUOTE}``) is set, and raises ``DataSourceUnavailable``
    otherwise — so tokens without an override fall through to the next
    source without affecting aggregation.
    """

    def __init__(self, confidence: float = _DEFAULT_CONFIDENCE) -> None:
        self._confidence = confidence

    @property
    def source_name(self) -> str:
        return _SOURCE_NAME

    @property
    def supported_tokens(self) -> list[str]:
        """Return symbols whose override env vars are currently set.

        This is a best-effort snapshot — operators can set or unset env
        vars without restarting the gateway.
        """
        supported: list[str] = []
        for key in os.environ:
            if not key.startswith(_ENV_PREFIX):
                continue
            suffix = key[len(_ENV_PREFIX) :]
            # Strip the optional quote suffix (_USD, _WBTC, ...) to recover
            # the base token symbol.
            base = suffix.split("_")[0] if "_" in suffix else suffix
            if base and base not in supported:
                supported.append(base)
        return supported

    async def get_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        resolved_token: ResolvedToken | None = None,  # noqa: ARG002
    ) -> PriceResult:
        """Return the overridden price, or raise DataSourceUnavailable."""
        price = _lookup(token, quote)
        if price is None:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=(
                    f"No manual override for {token}/{quote}. "
                    f"Set {_ENV_PREFIX}{_normalise(token)}={{price}} "
                    f"or {_ENV_PREFIX}{_normalise(token)}_{_normalise(quote)}={{price}}."
                ),
            )
        return PriceResult(
            price=price,
            source=self.source_name,
            timestamp=datetime.now(UTC),
            confidence=self._confidence,
            stale=False,
        )

    async def close(self) -> None:  # pragma: no cover - no resources to release
        """No-op close; provided for interface parity with network sources."""
        return None


__all__ = ["ManualPriceOverrideSource"]
