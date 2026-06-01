"""Gateway service implementations.

This package contains the gRPC service implementations for the gateway:
- MarketService: Price, balance, and indicator data
- StateService: Strategy state persistence
- ExecutionService: Intent compilation and transaction execution
- ObserveService: Logging, alerting, and metrics
- RpcService: JSON-RPC proxy for blockchain access (Phase 3)
- IntegrationService: Third-party data sources (Phase 3)
- DashboardService: Operator dashboard data access
- FundingRateService: Perpetual funding rate data
- SimulationService: Transaction simulation via Alchemy/Tenderly
- PoolAnalyticsService: Off-chain pool analytics — TVL / volume / fee APR (VIB-4727)
- PoolHistoryService: Off-chain pool history time-series (VIB-4728; POOL-2 skeleton)
- RateHistoryService: Lending APY / perp funding / DEX TWAP / DEX volume (VIB-4859)
- TokenService: Unified token resolution and on-chain metadata discovery
- LifecycleService: Agent state and command management (V2 deployment)
- TeardownService: Hosted teardown state access through the gateway boundary
- PositionService: On-chain reconciliation of position_registry (T24 / VIB-4210)

Connector-owned servicers (e.g. ``PolymarketServiceServicer``,
``EnsoServiceServicer``) live in
``almanak.connectors.<protocol>.gateway.service`` and are discovered at
gateway boot via ``GATEWAY_REGISTRY``. They are NOT re-exported from
this package — import them directly from their connector module.
"""

# VIB-4810 / VIB-4812: Enso + Polymarket servicers live in their respective
# connector folders (``almanak.connectors.<protocol>.gateway.service``) and
# are discovered via ``GATEWAY_REGISTRY`` at gateway boot. The re-exports
# previously kept here as a backwards-compat shim were dropped in VIB-4813
# now that ``server.py`` is registry-driven and there are no in-repo
# consumers importing connector servicers from ``almanak.gateway.services``.
from almanak.gateway.services.dashboard_service import DashboardServiceServicer
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from almanak.gateway.services.funding_rate_service import FundingRateServiceServicer
from almanak.gateway.services.integration_service import IntegrationServiceServicer
from almanak.gateway.services.lifecycle_service import LifecycleServiceServicer
from almanak.gateway.services.market_service import MarketServiceServicer
from almanak.gateway.services.observe_service import ObserveServiceServicer
from almanak.gateway.services.pool_analytics_service import PoolAnalyticsServiceServicer
from almanak.gateway.services.pool_history_service import PoolHistoryServiceServicer
from almanak.gateway.services.position_service import PositionServiceServicer
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer
from almanak.gateway.services.rpc_service import RpcServiceServicer
from almanak.gateway.services.simulation_service import SimulationServiceServicer
from almanak.gateway.services.state_service import StateServiceServicer
from almanak.gateway.services.teardown_service import TeardownServiceServicer
from almanak.gateway.services.token_service import TokenServiceServicer

__all__ = [
    "DashboardServiceServicer",
    "MarketServiceServicer",
    "PositionServiceServicer",
    "StateServiceServicer",
    "ExecutionServiceServicer",
    "LifecycleServiceServicer",
    "TeardownServiceServicer",
    "ObserveServiceServicer",
    "RpcServiceServicer",
    "IntegrationServiceServicer",
    "FundingRateServiceServicer",
    "SimulationServiceServicer",
    "PoolAnalyticsServiceServicer",
    "PoolHistoryServiceServicer",
    "RateHistoryServiceServicer",
    "TokenServiceServicer",
]
