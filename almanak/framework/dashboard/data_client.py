"""DashboardDataClient -- clean abstraction over gRPC dashboard access.

Wraps ``GatewayDashboardClient`` and returns plain Python dataclasses/dicts.
No protobuf types leak through this API, making the dashboard frontend-replaceable
(Streamlit, React, mobile, Telegram bot).

Usage::

    from almanak.framework.dashboard import DashboardDataClient

    client = DashboardDataClient()
    client.connect()
    strategies = client.get_strategies()
    detail = client.get_strategy_detail("my-strategy")
    trades = client.get_trades("my-strategy")

For PM integration, use the per-portfolio factory::

    client = DashboardDataClient.for_gateway(host="10.0.1.5", port=50051)
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from almanak.framework.dashboard.gateway_client import (
    GatewayDashboardClient,
    StrategyDetails,
    StrategySummary,
    TimelineEvent,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Trade record dataclass (protobuf-free)
# =============================================================================


@dataclass
class TradeRecord:
    """A structured trade record from the transaction ledger.

    This is the public-facing type returned by ``get_trades()``.
    No protobuf dependency.
    """

    id: str = ""
    cycle_id: str = ""
    deployment_id: str = ""
    timestamp: datetime | None = None
    intent_type: str = ""
    token_in: str = ""
    amount_in: str = ""
    token_out: str = ""
    amount_out: str = ""
    effective_price: str = ""
    slippage_bps: float = 0.0
    gas_used: int = 0
    gas_usd: str = ""
    tx_hash: str = ""
    chain: str = ""
    protocol: str = ""
    success: bool = True
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a flat dictionary for CSV/JSON export."""
        return {
            "id": self.id,
            "cycle_id": self.cycle_id,
            "deployment_id": self.deployment_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else "",
            "intent_type": self.intent_type,
            "token_in": self.token_in,
            "amount_in": self.amount_in,
            "token_out": self.token_out,
            "amount_out": self.amount_out,
            "effective_price": self.effective_price,
            "slippage_bps": self.slippage_bps,
            "gas_used": self.gas_used,
            "gas_usd": self.gas_usd,
            "tx_hash": self.tx_hash,
            "chain": self.chain,
            "protocol": self.protocol,
            "success": self.success,
            "error": self.error,
        }


@dataclass
class PnLDataPoint:
    """A single PnL data point for charting.

    Empty≠Zero (VIB-5942 CodeRabbit): ``pnl_usd`` is ``None`` when UNMEASURED (the
    wire carried no pnl for this sample), never coerced to ``Decimal("0")`` — a
    fabricated measured zero would show as $0 profit on the exported series.
    ``value_usd`` is always present: ``get_pnl_history`` drops unmeasured-NAV
    samples upstream, so a point only exists when its NAV was measured.
    """

    timestamp: datetime
    value_usd: Decimal
    pnl_usd: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "value_usd": str(self.value_usd),
            # Empty≠Zero: unmeasured pnl exports as "" (blank), not "0" or "None".
            "pnl_usd": "" if self.pnl_usd is None else str(self.pnl_usd),
        }


@dataclass
class PortfolioMetricsSummary:
    """Summary of portfolio metrics for a strategy."""

    deployment_id: str = ""
    total_value_usd: Decimal = Decimal("0")
    initial_value_usd: Decimal = Decimal("0")
    pnl_usd: Decimal = Decimal("0")
    gas_spent_usd: Decimal = Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "total_value_usd": str(self.total_value_usd),
            "initial_value_usd": str(self.initial_value_usd),
            "pnl_usd": str(self.pnl_usd),
            "gas_spent_usd": str(self.gas_spent_usd),
        }


# =============================================================================
# Client cache for per-portfolio connections (bounded)
# =============================================================================

from functools import lru_cache


@lru_cache(maxsize=16)
def _get_cached_gateway_client(host: str, port: int) -> "GatewayDashboardClient":
    """Create and cache a GatewayDashboardClient for a (host, port) pair."""
    from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

    config = GatewayClientConfig(host=host, port=port)
    gw_client = GatewayClient(config=config)
    return GatewayDashboardClient(gateway_client=gw_client)


# =============================================================================
# DashboardDataClient
# =============================================================================


class DashboardDataClient:
    """Clean abstraction over gRPC dashboard data access.

    All methods return plain Python types (dataclasses, dicts, lists).
    No protobuf types leak through the public API.
    """

    def __init__(self, gateway_client: GatewayDashboardClient | None = None) -> None:
        self._gw = gateway_client or GatewayDashboardClient()

    # -- Lifecycle ------------------------------------------------------------

    def connect(self) -> None:
        """Connect to the gateway."""
        self._gw.connect()

    def disconnect(self) -> None:
        """Disconnect from the gateway."""
        self._gw.disconnect()

    @property
    def is_connected(self) -> bool:
        return self._gw.is_connected

    # -- Factory --------------------------------------------------------------

    @classmethod
    def for_gateway(cls, host: str = "localhost", port: int = 50051) -> "DashboardDataClient":
        """Get or create a client for a specific gateway endpoint.

        Underlying GatewayDashboardClient is LRU-cached by (host, port),
        bounded to 16 entries to prevent unbounded growth.
        """
        dashboard_gw = _get_cached_gateway_client(host, port)
        return cls(gateway_client=dashboard_gw)

    # -- Queries (protobuf-free returns) --------------------------------------

    def get_strategies(
        self,
        status_filter: str | None = None,
        chain_filter: str | None = None,
    ) -> list[StrategySummary]:
        """List strategies with summary info.

        Returns plain ``StrategySummary`` dataclasses (no protobuf).
        """
        return self._gw.list_strategies(
            status_filter=status_filter,
            chain_filter=chain_filter,
        )

    def get_strategy_detail(
        self,
        deployment_id: str,
        include_timeline: bool = True,
        include_pnl_history: bool = False,
        timeline_limit: int = 20,
    ) -> StrategyDetails:
        """Get detailed strategy information."""
        return self._gw.get_strategy_details(
            deployment_id=deployment_id,
            include_timeline=include_timeline,
            include_pnl_history=include_pnl_history,
            timeline_limit=timeline_limit,
        )

    def get_timeline(
        self,
        deployment_id: str,
        limit: int = 50,
        event_type_filter: str | None = None,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy."""
        return self._gw.get_timeline(
            deployment_id=deployment_id,
            limit=limit,
            event_type_filter=event_type_filter,
        )

    def get_pnl_history(
        self,
        deployment_id: str,
        since: datetime | None = None,
        from_ts: datetime | None = None,
        to_ts: datetime | None = None,
        max_points: int = 0,
    ) -> list[PnLDataPoint]:
        """Get PnL history for charting.

        ``max_points > 0`` (VIB-5059 P2) selects **windowed mode**: the server
        fetches ``[from_ts, to_ts]`` and decimates to the budget (spikes preserved),
        so a back-in-time window is bounded server-side rather than over-fetched and
        filtered client-side. ``max_points <= 0`` keeps the legacy recent-window
        behavior, optionally narrowed client-side by ``since``.

        Returns plain ``PnLDataPoint`` dataclasses.
        """
        details = self._gw.get_strategy_details(
            deployment_id=deployment_id,
            include_timeline=False,
            include_pnl_history=True,
            from_ts=int(from_ts.timestamp()) if from_ts else 0,
            to_ts=int(to_ts.timestamp()) if to_ts else 0,
            max_points=max_points,
        )
        windowed = max_points > 0
        points = []
        for entry in details.pnl_history:
            ts = entry.get("timestamp")
            if ts is None:
                continue  # Skip entries without timestamps to avoid garbage chart points
            if not windowed and since and ts < since:
                # Legacy client-side narrowing; the server already bounds the
                # windowed path, so don't double-filter it.
                continue
            value_usd = entry.get("value_usd")
            if value_usd is None:
                # Empty != Zero (VIB-5942): an UNMEASURED NAV sample — skip it
                # rather than fabricate a $0 point in the exported series. A
                # measured zero (Decimal("0")) is not None and is kept.
                continue
            # Empty≠Zero (VIB-5942 CodeRabbit): keep an unmeasured pnl as None —
            # never coerce to Decimal("0") (a fabricated measured zero).
            points.append(
                PnLDataPoint(
                    timestamp=ts,
                    value_usd=value_usd,
                    pnl_usd=entry.get("pnl_usd"),
                )
            )
        return points

    def get_trades(
        self,
        deployment_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
    ) -> list[TradeRecord]:
        """Get structured trade records from the transaction ledger.

        Returns plain ``TradeRecord`` dataclasses.
        """
        try:
            records = self._gw.get_transaction_ledger(
                deployment_id=deployment_id,
                since=since,
                intent_type=intent_type,
                limit=limit,
            )
            return [
                TradeRecord(
                    id=r.id,
                    cycle_id=r.cycle_id,
                    deployment_id=r.deployment_id,
                    timestamp=r.timestamp,
                    intent_type=r.intent_type,
                    token_in=r.token_in,
                    amount_in=r.amount_in,
                    token_out=r.token_out,
                    amount_out=r.amount_out,
                    effective_price=r.effective_price,
                    slippage_bps=r.slippage_bps,
                    gas_used=r.gas_used,
                    gas_usd=r.gas_usd,
                    tx_hash=r.tx_hash,
                    chain=r.chain,
                    protocol=r.protocol,
                    success=r.success,
                    error=r.error,
                )
                for r in records
            ]
        except Exception:
            logger.debug("Failed to fetch trades for %s", deployment_id, exc_info=True)
            return []

    def get_portfolio_metrics(self, deployment_id: str) -> PortfolioMetricsSummary:
        """Get portfolio metrics summary for a strategy."""
        try:
            details = self._gw.get_strategy_details(
                deployment_id=deployment_id,
                include_timeline=False,
                include_pnl_history=False,
            )
            summary = details.summary
            return PortfolioMetricsSummary(
                deployment_id=deployment_id,
                total_value_usd=summary.total_value_usd,
                # Coalesce an unmeasured 24h (None) to 0 for this legacy
                # wallet-level summary shape (VIB-5787).
                pnl_usd=summary.pnl_24h_usd if summary.pnl_24h_usd is not None else Decimal("0"),
            )
        except Exception:
            logger.debug("Failed to fetch portfolio metrics for %s", deployment_id, exc_info=True)
            return PortfolioMetricsSummary(deployment_id=deployment_id)

    def get_strategy_config(self, deployment_id: str) -> dict[str, Any]:
        """Get strategy configuration as a plain dict."""
        return self._gw.get_strategy_config(deployment_id)

    def get_strategy_state(
        self,
        deployment_id: str,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get current strategy state as a plain dict."""
        return self._gw.get_strategy_state(deployment_id, fields=fields)

    def execute_action(
        self,
        deployment_id: str,
        action: str,
        reason: str,
        params: dict[str, str] | None = None,
    ) -> bool:
        """Execute operator action (pause, resume, etc.)."""
        return self._gw.execute_action(deployment_id, action, reason, params)
