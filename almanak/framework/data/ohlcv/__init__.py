"""OHLCV Module - Candlestick data with caching.

This module provides strategy-side OHLCV data providers and modules for
efficient historical candlestick data access.

Providers:
    - GatewayOHLCVProvider: Gateway-backed provider (recommended for production)
    - GatewayGeckoTerminalOHLCVProvider: gRPC client for GeckoTerminal data
    - DedupingOHLCVProvider: Deduplication wrapper

Modules:
    - OHLCVModule: Combines providers with persistent SQLite caching
    - OHLCVRouter: Multi-source provider routing
    - RoutingOHLCVProvider: Routing-aware provider wrapper

Note:
    Raw HTTP providers (Binance, GeckoTerminal direct) live under
    ``almanak.gateway.data.ohlcv`` because they perform outbound network
    egress and are gateway-side only. Strategy-container code must not
    import them directly (VIB-3799).

``OHLCVModule`` and ``GapStrategy`` are resolved lazily via PEP 562
``__getattr__`` because their defining module imports pandas at module
load, which in turn auto-loads pyarrow. The CLI bootstrap
(``almanak/cli/cli.py`` -> ``framework/cli/run.py``) imports sibling
``ohlcv.*`` modules for runner wiring, so eager re-export of
``OHLCVModule`` would pull pandas + pyarrow into every ``almanak strat
run`` startup inside the deployed strategy container even when the
strategy never calls ``MarketSnapshot.ohlcv(...)``.
"""

from typing import TYPE_CHECKING

from almanak.framework.data.ohlcv.dedup_provider import DedupingOHLCVProvider
from almanak.framework.data.ohlcv.factory import (
    OHLCVStack,
    assert_provider_chains_registered,
    create_ohlcv_stack,
    create_routing_ohlcv_provider,
)
from almanak.framework.data.ohlcv.gateway_data_adapter import (
    CoinGeckoGatewayDataProvider,
    GatewayOHLCVDataProvider,
    GeckoTerminalGatewayDataProvider,
)
from almanak.framework.data.ohlcv.gateway_provider import (
    TOKEN_TO_BINANCE_SYMBOL,
    GatewayCoinGeckoOHLCVProvider,
    GatewayGeckoTerminalOHLCVProvider,
    GatewayOHLCVProvider,
)
from almanak.framework.data.ohlcv.ohlcv_router import (
    OHLCVRouter,
    classify_instrument,
    provider_names_in_chains,
)
from almanak.framework.data.ohlcv.routing_provider import (
    RoutingOHLCVProvider,
)

if TYPE_CHECKING:
    from almanak.framework.data.ohlcv.module import GapStrategy, OHLCVModule

__all__ = [
    "CoinGeckoGatewayDataProvider",
    "GatewayCoinGeckoOHLCVProvider",
    "GatewayOHLCVProvider",
    "GatewayGeckoTerminalOHLCVProvider",
    "GatewayOHLCVDataProvider",
    "GeckoTerminalGatewayDataProvider",
    "OHLCVStack",
    "TOKEN_TO_BINANCE_SYMBOL",
    "DedupingOHLCVProvider",
    "GapStrategy",
    "OHLCVModule",
    "OHLCVRouter",
    "RoutingOHLCVProvider",
    "assert_provider_chains_registered",
    "classify_instrument",
    "create_ohlcv_stack",
    "create_routing_ohlcv_provider",
    "provider_names_in_chains",
]


def __getattr__(name: str) -> object:
    if name in ("GapStrategy", "OHLCVModule"):
        # VIB-4901: switched from the legacy dynamic-loader shape to a
        # function-local relative `from . import ... as ...` statement
        # to satisfy the broadened L1 dynamic-import suppression on
        # Scan B / Scan C. Same lazy semantic — the inner import only
        # runs when ``__getattr__`` fires; the inner module is NOT
        # loaded at package-import time. Relative form is idiomatic for
        # in-package lazy loads (Claude pr-auditor post-PR Potential).
        from . import module as _module

        attr = getattr(_module, name)
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
