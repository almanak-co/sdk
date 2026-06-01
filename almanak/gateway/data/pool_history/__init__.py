"""Pool-history providers + dispatcher (VIB-4728 / POOL-5 / VIB-4753).

Gateway-side egress layer for ``PoolHistoryService``. Lives under
``almanak/gateway/data/`` where outbound HTTP / GraphQL is the *correct*
layer (AGENTS.md "Gateway boundary"). Strategy containers never import this
package — they reach it only through the gateway gRPC channel.

Public surface:

* ``PoolHistoryDispatcher`` — provider fallback orchestration. The servicer
  constructs one in ``__init__`` and calls ``dispatch()`` inside the public
  cache's ``get_or_fetch`` closure.
* The three providers (``TheGraphPoolHistoryProvider`` /
  ``DefiLlamaPoolHistoryProvider`` / ``GeckoTerminalPoolHistoryProvider``).
* The 3-state taxonomy sentinels + ``_ProviderError`` (parallel copies of the
  analytics-service types — the two services are intentionally decoupled,
  decision #3).
"""

from __future__ import annotations

from ._base import (
    _NOT_ATTEMPTED,
    PoolHistoryProvider,
    ProviderResult,
    _MonthlyBudgetTracker,
    _NotAttempted,
    _ProviderError,
    _safe_decimal_str,
    _TokenBucket,
)
from ._graphql import GatewayGraphQLClient
from .defillama import DefiLlamaPoolHistoryProvider
from .dispatcher import (
    PoolHistoryDispatcher,
    _DispatchCounters,
    _DispatchOutcome,
)
from .geckoterminal import GeckoTerminalPoolHistoryProvider
from .thegraph import TheGraphPoolHistoryProvider

__all__ = [
    "PoolHistoryDispatcher",
    "TheGraphPoolHistoryProvider",
    "DefiLlamaPoolHistoryProvider",
    "GeckoTerminalPoolHistoryProvider",
    "GatewayGraphQLClient",
    "PoolHistoryProvider",
    "ProviderResult",
    "_DispatchCounters",
    "_DispatchOutcome",
    "_MonthlyBudgetTracker",
    "_NotAttempted",
    "_NOT_ATTEMPTED",
    "_ProviderError",
    "_TokenBucket",
    "_safe_decimal_str",
]
