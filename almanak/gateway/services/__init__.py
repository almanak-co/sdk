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
- PolymarketService: Polymarket CLOB API proxy
- EnsoService: Enso Finance routing API proxy
- TokenService: Unified token resolution and on-chain metadata discovery
"""

from almanak.gateway.services.dashboard_service import DashboardServiceServicer
from almanak.gateway.services.enso_service import EnsoServiceServicer
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from almanak.gateway.services.funding_rate_service import FundingRateServiceServicer
from almanak.gateway.services.integration_service import IntegrationServiceServicer
from almanak.gateway.services.market_service import MarketServiceServicer
from almanak.gateway.services.observe_service import ObserveServiceServicer
from almanak.gateway.services.polymarket_service import PolymarketServiceServicer
from almanak.gateway.services.rpc_service import RpcServiceServicer
from almanak.gateway.services.simulation_service import SimulationServiceServicer
from almanak.gateway.services.state_service import StateServiceServicer
from almanak.gateway.services.token_service import TokenServiceServicer

__all__ = [
    "DashboardServiceServicer",
    "EnsoServiceServicer",
    "MarketServiceServicer",
    "StateServiceServicer",
    "ExecutionServiceServicer",
    "ObserveServiceServicer",
    "RpcServiceServicer",
    "IntegrationServiceServicer",
    "FundingRateServiceServicer",
    "SimulationServiceServicer",
    "PolymarketServiceServicer",
    "TokenServiceServicer",
]
