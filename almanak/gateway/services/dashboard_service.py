"""DashboardService implementation - provides data for operator dashboards.

This service exposes strategy data for dashboards via gRPC. All filesystem
and database access happens here in the gateway; dashboard containers only
receive the formatted data.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import grpc

if TYPE_CHECKING:
    from almanak.framework.portfolio.models import PortfolioSnapshot
    from almanak.framework.state.state_manager import StateManager

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.integrations.portfolio_chain import PortfolioProviderChain, build_portfolio_chain
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.registry import get_instance_registry
from almanak.gateway.timeline.store import get_timeline_store
from almanak.gateway.validation import ValidationError, resolve_agent_id, validate_strategy_id

logger = logging.getLogger(__name__)


# Strategy categories in the filesystem
STRATEGY_CATEGORIES = ["demo", "production", "incubating", "poster_child", "tests"]
PORTFOLIO_STALE_THRESHOLD_SECONDS = 300


class DashboardServiceServicer(gateway_pb2_grpc.DashboardServiceServicer):
    """Implements DashboardService gRPC interface.

    Provides dashboard data access for operator dashboards:
    - ListStrategies: Discover and list available strategies
    - GetStrategyDetails: Get strategy status, position, timeline
    - GetTimeline: Get strategy timeline events
    - GetStrategyConfig: Get strategy configuration
    - GetStrategyState: Get current strategy state
    - ExecuteAction: Execute operator actions (pause, resume, etc.)
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize DashboardService.

        Args:
            settings: Gateway settings with configuration.
        """
        self.settings = settings
        self._state_manager: StateManager | None = None
        self._initialized = False
        self._strategies_root: Path | None = None
        # In-memory cache of strategy positions reported via heartbeat
        self._cached_positions: dict[str, list[gateway_pb2.StrategyPosition]] = {}
        self._portfolio_chain: PortfolioProviderChain | None = None

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of dependencies."""
        if self._initialized:
            return

        # Find strategies directory (relative to gateway package)
        # Try multiple possible locations
        possible_roots = [
            Path(__file__).parent.parent.parent.parent / "strategies",  # From gateway/services/
            Path.cwd() / "strategies",
            Path(__file__).parent.parent.parent.parent.parent / "strategies",
        ]

        for root in possible_roots:
            if root.exists():
                self._strategies_root = root
                break

        if self._strategies_root is None:
            logger.warning("Strategies directory not found")
            self._strategies_root = Path.cwd() / "strategies"  # Default even if doesn't exist

        # Initialize state manager for reading strategy state
        try:
            from almanak.framework.state.state_manager import (
                StateManager,
                StateManagerConfig,
                WarmBackendType,
            )

            if self.settings.database_url:
                backend_type = WarmBackendType.POSTGRESQL
                config = StateManagerConfig(
                    warm_backend=backend_type,
                    database_url=self.settings.database_url,
                )
            else:
                backend_type = WarmBackendType.SQLITE
                config = StateManagerConfig(warm_backend=backend_type)

            self._state_manager = StateManager(config)
            await self._state_manager.initialize()
            logger.info(f"DashboardService: StateManager initialized with {backend_type.name}")
        except Exception as e:
            logger.warning(f"DashboardService: Failed to initialize StateManager: {e}")
            self._state_manager = None

        try:
            self._portfolio_chain = build_portfolio_chain(
                portfolio_providers_csv=self.settings.portfolio_providers,
                portfolio_api_key=self.settings.portfolio_api_key,
                portfolio_api_provider=self.settings.portfolio_api_provider,
                portfolio_api_cache_ttl=self.settings.portfolio_api_cache_ttl,
            )
        except Exception as e:
            logger.warning(f"DashboardService: Failed to initialize portfolio providers: {e}")
            self._portfolio_chain = None

        self._initialized = True
        logger.info(f"DashboardService initialized (strategies_root={self._strategies_root})")

    def _discover_strategies_from_filesystem(self) -> list[dict]:
        """Discover strategies from the strategies/ directory.

        Returns:
            List of strategy info dicts from config.json files
        """
        strategies: list[dict] = []

        if self._strategies_root is None or not self._strategies_root.exists():
            return strategies

        for category in STRATEGY_CATEGORIES:
            category_dir = self._strategies_root / category
            if not category_dir.exists():
                continue

            for strategy_dir in category_dir.iterdir():
                if not strategy_dir.is_dir():
                    continue

                config_file = strategy_dir / "config.json"
                if not config_file.exists():
                    continue

                try:
                    config = json.loads(config_file.read_text())
                    strategy_id = config.get("strategy_id", strategy_dir.name)
                    strategy_name = config.get("strategy_name", strategy_dir.name)

                    # Derive display name
                    display_name = strategy_name.replace("_", " ").title()
                    if category != "demo":
                        display_name += f" ({category.title()})"

                    # Determine chain and protocol from config
                    chain = config.get("chain", "arbitrum")
                    protocol = self._derive_protocol_from_config(config, strategy_id)

                    strategies.append(
                        {
                            "strategy_id": strategy_id,
                            "name": display_name,
                            "status": "PAUSED",  # Default - will be updated from state
                            "chain": chain,
                            "protocol": protocol,
                            "total_value_usd": "0",
                            "pnl_24h_usd": "0",
                            "last_action_at": 0,
                            "attention_required": False,
                            "attention_reason": "",
                            "is_multi_chain": "," in str(chain),
                            "chains": [c.strip() for c in str(chain).split(",")],
                            "config_path": str(config_file),
                            "category": category,
                            "consecutive_errors": 0,
                            "last_iteration_at": 0,
                            "pnl_since_deploy_usd": "",
                        }
                    )
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to load strategy config from {config_file}: {e}")
                    continue

        return strategies

    def _discover_paper_sessions(self) -> list[dict]:
        """Discover paper trading sessions from ~/.almanak/paper/.

        Reads state files produced by the BackgroundPaperTrader to surface
        paper sessions alongside live strategies in the dashboard.

        Returns:
            List of strategy info dicts for paper sessions.
        """
        paper_dir = Path.home() / ".almanak" / "paper"
        if not paper_dir.exists():
            return []

        sessions: list[dict] = []

        for state_file in paper_dir.glob("*.state.json"):
            try:
                data = json.loads(state_file.read_text())
            except (json.JSONDecodeError, OSError) as e:
                logger.debug(f"Failed to read paper state file {state_file}: {e}")
                continue

            if not isinstance(data, dict):
                logger.debug(f"Paper state file {state_file} is not a JSON object, skipping")
                continue

            strategy_id = data.get("strategy_id", state_file.stem.replace(".state", ""))
            config = data.get("config", {})
            if not isinstance(config, dict):
                config = {}

            # Determine status: check PID liveness and file freshness
            status = "PAPER_TRADING"
            pid = data.get("pid")
            file_status = data.get("status", "unknown")
            if file_status in ("stopped", "stopped_clean", "error", "completed"):
                status = "INACTIVE"
            elif isinstance(pid, int) and pid > 0:
                try:
                    os.kill(pid, 0)
                except OSError:
                    last_save = data.get("last_save")
                    if last_save:
                        try:
                            last_dt = datetime.fromisoformat(last_save)
                            if last_dt.tzinfo is None:
                                last_dt = last_dt.replace(tzinfo=UTC)
                            age = (datetime.now(UTC) - last_dt).total_seconds()
                            if age > 300:
                                status = "INACTIVE"
                        except (ValueError, TypeError):
                            status = "INACTIVE"

            chain = config.get("chain", "arbitrum")
            protocol = config.get("protocol", "")
            if not protocol:
                protocol = self._derive_protocol_from_config(config, strategy_id)

            trades = data.get("trades", [])
            if not isinstance(trades, list):
                trades = []
            errors = data.get("errors", [])
            if not isinstance(errors, list):
                errors = []
            equity_curve = data.get("equity_curve", [])
            if not isinstance(equity_curve, list):
                equity_curve = []
            tick_count = data.get("tick_count", 0)
            success_count = len(trades)
            error_count = len(errors)
            hold_count = max(0, tick_count - success_count - error_count)

            total_gas_cost = Decimal("0")
            for trade in trades:
                try:
                    total_gas_cost += Decimal(str(trade.get("gas_cost_usd", "0")))
                except (ValueError, TypeError, ArithmeticError) as e:
                    logger.debug("Skipping malformed gas_cost_usd in trade %s: %s", trade, e)

            # PnL from portfolio state, not summed trade deltas (Fix #4).
            # The equity curve tracks mark-to-market portfolio value including
            # open positions. PnL = latest equity value - initial value.
            simulated_pnl = Decimal("0")
            initial_value = Decimal("0")
            current_value = Decimal("0")
            if equity_curve:
                try:
                    initial_value = Decimal(str(equity_curve[0].get("value", "0")))
                    current_value = Decimal(str(equity_curve[-1].get("value", "0")))
                    simulated_pnl = current_value - initial_value
                except (IndexError, AttributeError, ValueError):
                    pass
            # Fallback: use initial/current balances if no equity curve
            if not equity_curve:
                initial_balances = data.get("initial_balances", {})
                current_balances = data.get("current_balances", {})
                if initial_balances and current_balances:
                    # Can't compute PnL without prices — leave at 0
                    pass

            last_trade_at = ""
            if trades:
                last_trade_at = trades[-1].get("timestamp", "")

            trades_per_hour = Decimal("0")
            session_start = data.get("session_start", "")
            if session_start and success_count > 0:
                try:
                    start_dt = datetime.fromisoformat(session_start)
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=UTC)
                    hours = Decimal(str((datetime.now(UTC) - start_dt).total_seconds())) / Decimal("3600")
                    if hours > 0:
                        trades_per_hour = Decimal(success_count) / hours
                except (ValueError, TypeError):
                    pass

            # Prefer persisted error_breakdown; fall back to reconstructing from errors list
            error_breakdown = data.get("error_breakdown")
            if not isinstance(error_breakdown, dict):
                error_breakdown = {}
                for error in errors:
                    if isinstance(error, dict):
                        etype = error.get("error_type", "unknown")
                        error_breakdown[etype] = error_breakdown.get(etype, 0) + 1

            # Downsample equity curve to max 200 points (always include last point)
            eq_points = equity_curve
            if len(eq_points) > 200:
                step = len(eq_points) / 199
                eq_points = [eq_points[int(i * step)] for i in range(199)] + [eq_points[-1]]

            paper_metrics = {
                "tick_count": tick_count,
                "success_count": success_count,
                "hold_count": hold_count,
                "error_count": error_count,
                "simulated_pnl_usd": str(simulated_pnl),
                "total_gas_cost_usd": str(total_gas_cost),
                "last_trade_at": last_trade_at,
                "session_start": session_start,
                "trades_per_hour": str(trades_per_hour),
                "equity_curve": eq_points,
                "error_breakdown": error_breakdown,
                "ticks_with_fork": data.get("ticks_with_fork", 0),
                "ticks_with_indicators": data.get("ticks_with_indicators", 0),
                "ticks_with_action": data.get("ticks_with_action", 0),
                "anvil_result": data.get("anvil_result"),
            }

            total_value = str(current_value) if current_value else "0"

            last_action_ts = 0
            last_save = data.get("last_save")
            if last_save:
                try:
                    last_dt = datetime.fromisoformat(last_save)
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=UTC)
                    last_action_ts = int(last_dt.timestamp())
                except (ValueError, TypeError):
                    pass

            sessions.append(
                {
                    "strategy_id": f"paper:{strategy_id}",
                    "name": strategy_id.replace("_", " ").title() + " (Paper)",
                    "status": status,
                    "chain": chain,
                    "protocol": protocol,
                    "total_value_usd": total_value,
                    "pnl_24h_usd": "0",  # Keep 0 to avoid contaminating portfolio 24h total; simulated PnL is in paper_metrics_json
                    "last_action_at": last_action_ts,
                    "attention_required": status == "INACTIVE",
                    "attention_reason": "Paper session inactive" if status == "INACTIVE" else "",
                    "is_multi_chain": "," in str(chain),
                    "chains": [c.strip() for c in str(chain).split(",")],
                    "execution_mode": "paper",
                    "paper_metrics_json": json.dumps(paper_metrics),
                }
            )

        return sessions

    def _derive_protocol_from_config(self, config: dict, strategy_id: str) -> str:
        """Derive protocol string from config or strategy ID."""
        if "protocol" in config:
            return config["protocol"]

        if "pool" in config:
            return "Uniswap V3"

        strategy_id_lower = strategy_id.lower()
        if "uniswap" in strategy_id_lower:
            return "Uniswap V3"
        if "aave" in strategy_id_lower:
            return "Aave V3"
        if "gmx" in strategy_id_lower:
            return "GMX V2"
        if "enso" in strategy_id_lower:
            return "Enso"
        if "pancake" in strategy_id_lower:
            return "PancakeSwap V3"
        if "aerodrome" in strategy_id_lower:
            return "Aerodrome"
        if "traderjoe" in strategy_id_lower or "tj_" in strategy_id_lower:
            return "TraderJoe V2"
        if "benqi" in strategy_id_lower:
            return "Benqi"
        if "morpho" in strategy_id_lower:
            return "Morpho"
        if "compound" in strategy_id_lower:
            return "Compound V3"
        if "sushi" in strategy_id_lower:
            return "SushiSwap V3"
        if "curve" in strategy_id_lower:
            return "Curve"
        if "balancer" in strategy_id_lower:
            return "Balancer"
        if "velodrome" in strategy_id_lower:
            return "Velodrome"

        return "Unknown"

    async def _get_strategy_state_data(self, strategy_id: str, fallback_strategy_id: str | None = None) -> dict | None:
        """Get strategy state from StateManager.

        Args:
            strategy_id: Primary key to look up.
            fallback_strategy_id: If provided and different from strategy_id,
                tried when the primary lookup returns nothing.  This bridges
                legacy warm state written under the SDK key before AGENT_ID
                normalization was deployed.

        Returns:
            State dict or None if not found
        """
        if self._state_manager is None:
            return None

        try:
            state = await self._state_manager.load_state(strategy_id)
            if state is None and fallback_strategy_id and fallback_strategy_id != strategy_id:
                state = await self._state_manager.load_state(fallback_strategy_id)
            if state is not None:
                return state.state
        except Exception as e:
            logger.debug(f"Failed to load state for {strategy_id}: {e}")

        return None

    async def _get_portfolio_value_and_pnl(
        self,
        strategy_id: str,
        state: dict | None = None,
        chain: str = "",
        wallet_address: str = "",
        chain_wallets: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        """Get portfolio total value and PnL.

        Primary source: PortfolioMetrics (framework-owned, populated by PortfolioValuer).
        Fallback order:
        1. Fresh latest snapshot
        2. External wallet portfolio API (read-only)
        3. Stale snapshot
        4. Strategy state dict (legacy)

        Returns:
            Tuple of (total_value_usd, pnl_usd) as strings. Defaults to "0".
        """
        # Primary: PortfolioMetrics are always authoritative when available.
        # They are framework-owned and updated by PortfolioValuer each iteration.
        if self._state_manager is not None:
            try:
                metrics = await self._state_manager.get_portfolio_metrics(strategy_id)
                if metrics is not None:
                    pnl_24h = await self._compute_pnl_24h(strategy_id, metrics.total_value_usd)
                    return str(metrics.total_value_usd), str(pnl_24h)
            except Exception:
                logger.debug("Failed to get portfolio metrics for %s", strategy_id, exc_info=True)

        latest_snapshot = await self._get_latest_snapshot(strategy_id)
        if latest_snapshot is not None and self._snapshot_is_fresh(latest_snapshot):
            return str(latest_snapshot.total_value_usd), "0"

        external_total = await self._get_external_portfolio_total(
            chain=chain,
            wallet_address=wallet_address,
            chain_wallets=chain_wallets,
        )
        if external_total is not None:
            return str(external_total), "0"

        if latest_snapshot is not None:
            return str(latest_snapshot.total_value_usd), "0"

        # Fallback: extract from state dict (legacy strategies)
        if state:
            for key in (
                "total_value_usd",
                "total_position_value_usd",
                "portfolio_value_usd",
                "total_collateral_value_usd",
                "position_value_usd",
                "net_value_usd",
            ):
                if key in state:
                    try:
                        return str(Decimal(str(state[key]))), "0"
                    except (ValueError, TypeError):
                        continue

        return "0", "0"

    async def _compute_pnl_24h(self, strategy_id: str, current_value: Decimal) -> Decimal:
        """Compute PnL over a 24-hour window using snapshot history.

        Falls back to lifetime PnL if strategy has been running < 24h.

        Note: Both paths report PnL net of gas — in the 24h path, gas is
        implicitly captured because total_value_usd on snapshots already
        reflects the lower wallet balance after gas expenditure. The fallback
        path uses the same implicit approach: current_value already accounts
        for gas spent. Neither path adjusts for capital flows (deposits/
        withdrawals), which are rare for SDK strategies.
        """
        if self._state_manager is None or current_value <= 0:
            return Decimal("0")

        try:
            target_time = datetime.now(UTC) - timedelta(hours=24)
            snapshot_24h = await self._state_manager.get_snapshot_at(strategy_id, target_time)

            if snapshot_24h is not None and snapshot_24h.total_value_usd > 0:
                return current_value - snapshot_24h.total_value_usd

            # Strategy running < 24h: fall back to lifetime PnL.
            # Gas is already reflected in current_value (wallet balance reduced).
            metrics = await self._state_manager.get_portfolio_metrics(strategy_id)
            if metrics is not None and metrics.initial_value_usd > 0:
                return current_value - metrics.initial_value_usd

        except Exception:
            logger.debug("Failed to compute PnL 24h for %s", strategy_id, exc_info=True)

        return Decimal("0")

    async def _build_pnl_history(self, strategy_id: str) -> list:
        """Build PnL time series from portfolio snapshots for chart rendering.

        Returns a list of PnLDataPoint protos from the last 7 days of snapshots.
        """
        from almanak.gateway.proto import gateway_pb2

        pnl_points: list[gateway_pb2.PnLDataPoint] = []
        if self._state_manager is None:
            return pnl_points

        try:
            since = datetime.now(UTC) - timedelta(days=7)
            snapshots = await self._state_manager.get_snapshots_since(strategy_id, since, limit=168)

            if not snapshots:
                return pnl_points

            # Get initial value for PnL calculation
            metrics = await self._state_manager.get_portfolio_metrics(strategy_id)
            initial_value = metrics.initial_value_usd if metrics else Decimal("0")

            for snap in snapshots:
                ts = snap.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                pnl = snap.total_value_usd - initial_value if initial_value > 0 else Decimal("0")
                pnl_points.append(
                    gateway_pb2.PnLDataPoint(
                        timestamp=int(ts.timestamp()),
                        value_usd=str(snap.total_value_usd),
                        pnl_usd=str(pnl),
                    )
                )
        except Exception:
            logger.debug("Failed to build PnL history for %s", strategy_id, exc_info=True)

        return pnl_points

    async def _get_latest_snapshot(self, strategy_id: str) -> PortfolioSnapshot | None:
        """Get the most recent portfolio snapshot for staleness checks."""
        if self._state_manager is None:
            return None
        try:
            return await self._state_manager.get_latest_snapshot(strategy_id)
        except Exception:
            logger.debug("Failed to get latest snapshot for %s", strategy_id, exc_info=True)
            return None

    @staticmethod
    def _snapshot_is_fresh(
        snapshot: PortfolioSnapshot | None,
        stale_threshold_seconds: int = PORTFOLIO_STALE_THRESHOLD_SECONDS,
    ) -> bool:
        """Return True when a snapshot is recent enough to trust directly."""
        if snapshot is None:
            return False
        timestamp = snapshot.timestamp
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        age = (datetime.now(UTC) - timestamp).total_seconds()
        return age <= stale_threshold_seconds

    async def _get_external_portfolio_total(
        self,
        chain: str,
        wallet_address: str,
        chain_wallets: dict[str, str] | None = None,
    ) -> Decimal | None:
        """Fetch external portfolio totals for one or more chain/wallet contexts."""
        if self._portfolio_chain is None:
            return None

        contexts = self._resolve_portfolio_contexts(
            chain=chain, wallet_address=wallet_address, chain_wallets=chain_wallets
        )
        if not contexts:
            return None

        async def _fetch_one(context_chain: str, context_wallet: str) -> Decimal | None:
            try:
                snapshot = await self._portfolio_chain.get_wallet_portfolio(context_wallet, context_chain)  # type: ignore[union-attr]
                if snapshot is None:
                    logger.warning(
                        "All portfolio providers failed for %s on %s",
                        context_wallet,
                        context_chain,
                    )
                    return None
                return Decimal(snapshot.total_value_usd)
            except Exception:
                logger.debug(
                    "External portfolio lookup failed for %s on %s",
                    context_wallet,
                    context_chain,
                    exc_info=True,
                )
                return None

        results = await asyncio.gather(*[_fetch_one(c, w) for c, w in contexts])
        # Return None unless ALL contexts succeeded — partial totals would under-report
        totals = [r for r in results if r is not None]
        if len(totals) != len(results):
            return None
        return sum(totals, Decimal("0")) if totals else None

    @staticmethod
    def _resolve_portfolio_contexts(
        chain: str,
        wallet_address: str,
        chain_wallets: dict[str, str] | None = None,
    ) -> list[tuple[str, str]]:
        """Resolve the chain/wallet contexts to query for dashboard fallback."""
        if chain_wallets:
            return [
                (context_chain.lower(), context_wallet)
                for context_chain, context_wallet in chain_wallets.items()
                if context_chain and context_wallet
            ]

        chains = [value.strip().lower() for value in chain.split(",") if value.strip()]
        if not chains or not wallet_address:
            return []
        return [(context_chain, wallet_address) for context_chain in chains]

    async def _get_portfolio_metrics(self, strategy_id: str) -> Decimal | None:
        """Return pnl_after_gas for a strategy, or None if unavailable."""
        if self._state_manager is None:
            return None
        try:
            metrics = await self._state_manager.get_portfolio_metrics(strategy_id)
            if metrics is None:
                return None
            return metrics.pnl_after_gas
        except Exception:
            return None

    def _compute_effective_status(self, instance: Any, stale_threshold_seconds: int = 300) -> str:
        """Compute effective status for a registered instance.

        If an instance reports RUNNING but hasn't heartbeated within the threshold,
        its effective status is STALE (likely crashed).

        Args:
            instance: A StrategyInstance from the registry.
            stale_threshold_seconds: Seconds without heartbeat before marking STALE.

        Returns:
            Effective status string.
        """
        if instance.status == "RUNNING" and instance.last_heartbeat_at is not None:
            heartbeat = instance.last_heartbeat_at
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=UTC)
            age = (datetime.now(UTC) - heartbeat).total_seconds()
            if age > stale_threshold_seconds:
                return "STALE"
        return instance.status

    # Supported status_filter values for ListStrategies.
    _SOURCE_FILTERS = frozenset({"REGISTRY", "AVAILABLE", "ALL"})
    _STATUS_FILTERS = frozenset(
        {"RUNNING", "PAUSED", "ERROR", "STUCK", "STALE", "INACTIVE", "ARCHIVED", "PAPER_TRADING"}
    )
    _VALID_FILTERS = _SOURCE_FILTERS | _STATUS_FILTERS

    @staticmethod
    def _canonical_template_id(strategy_id: str) -> str:
        """Extract canonical template ID from a strategy instance ID.

        Instance IDs use the format ``"template_name:uuid_suffix"`` for
        continuous runs, or plain ``"template_name"`` for ``--once`` runs.
        This returns the part before the first colon.
        """
        return strategy_id.split(":")[0]

    async def ListStrategies(
        self,
        request: gateway_pb2.ListStrategiesRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ListStrategiesResponse:
        """List strategies with summary info.

        Uses ``status_filter`` to control data source:

        Source modes:
        - ``REGISTRY`` (default): Only instances from the instance registry
          (executed/running strategies). Used by the Command Center page.
        - ``AVAILABLE``: Only templates from filesystem discovery, excluding
          templates that already have a non-archived instance in the registry.
          Used by the Strategy Library page.
        - ``ALL``: Registry instances combined with filesystem templates
          (deduplicated). Useful for API consumers that want both.

        Status modes (applied on top of registry results):
        - ``RUNNING``, ``PAUSED``, ``ERROR``, ``STUCK``, ``STALE``,
          ``INACTIVE``, ``ARCHIVED``: Filter registry instances by status.

        Args:
            request: List request with optional filters
            context: gRPC context

        Returns:
            ListStrategiesResponse with strategy summaries
        """
        await self._ensure_initialized()

        status_filter = request.status_filter.upper() if request.status_filter else "REGISTRY"
        chain_filter = request.chain_filter.lower() if request.chain_filter else ""

        # Validate filter value
        if status_filter not in self._VALID_FILTERS:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Unknown status_filter '{request.status_filter}'. "
                f"Valid values: {', '.join(sorted(self._VALID_FILTERS))}",
            )
            return gateway_pb2.ListStrategiesResponse()  # unreachable; defensive

        strategies: list[dict] = []

        # --- Collect registry instances ---
        include_registry = status_filter != "AVAILABLE"
        registry_template_ids: set[str] = set()

        if include_registry or status_filter in ("AVAILABLE", "ALL"):
            # We always need the registry to build the canonical ID set for dedupe
            try:
                registry = get_instance_registry()
                registered = registry.list_all(
                    include_archived=(status_filter == "ARCHIVED"),
                )

                for inst in registered:
                    # Use strategy_name for dedupe — after AGENT_ID normalization,
                    # strategy_id may be a platform UUID that won't match filesystem
                    # template names.  strategy_name preserves the original template ID.
                    template_key = inst.strategy_name or self._canonical_template_id(inst.strategy_id)
                    registry_template_ids.add(template_key)

                    if include_registry:
                        effective_status = self._compute_effective_status(inst)

                        # Harden last_action_at for missing heartbeats
                        last_action_ts = 0
                        if inst.last_heartbeat_at is not None:
                            try:
                                hb = inst.last_heartbeat_at
                                if hb.tzinfo is None:
                                    hb = hb.replace(tzinfo=UTC)
                                last_action_ts = int(hb.timestamp())
                            except (ValueError, OSError):
                                pass

                        # Parse chain_wallets JSON if present
                        inst_chain_wallets: dict[str, str] = {}
                        if hasattr(inst, "chain_wallets") and inst.chain_wallets:
                            try:
                                parsed = json.loads(inst.chain_wallets)
                                if isinstance(parsed, dict):
                                    inst_chain_wallets = parsed
                            except (json.JSONDecodeError, TypeError):
                                pass

                        strategy_info = {
                            "strategy_id": inst.strategy_id,
                            "name": inst.strategy_name.replace("_", " ").title(),
                            "status": effective_status,
                            "chain": inst.chain,
                            "protocol": inst.protocol,
                            "total_value_usd": "0",
                            "pnl_24h_usd": "0",
                            "last_action_at": last_action_ts,
                            "attention_required": effective_status in ("STALE", "ERROR"),
                            "attention_reason": "Heartbeat stale" if effective_status == "STALE" else "",
                            "is_multi_chain": "," in inst.chain,
                            "chains": [c.strip() for c in inst.chain.split(",")],
                            "consecutive_errors": 0,
                            "last_iteration_at": 0,
                            "pnl_since_deploy_usd": "",
                            "wallet_address": inst.wallet_address,
                            "chain_wallets": inst_chain_wallets,
                        }

                        # Enrich with state data
                        state = await self._get_strategy_state_data(inst.strategy_id)

                        # Portfolio metrics (framework-owned, state fallback for legacy)
                        total_value, pnl = await self._get_portfolio_value_and_pnl(
                            inst.strategy_id,
                            state,
                            chain=inst.chain,
                            wallet_address=inst.wallet_address,
                            chain_wallets=inst_chain_wallets,
                        )
                        strategy_info["total_value_usd"] = total_value
                        strategy_info["pnl_24h_usd"] = pnl

                        if state:
                            try:
                                strategy_info["consecutive_errors"] = int(state.get("consecutive_errors", 0) or 0)
                            except (TypeError, ValueError):
                                strategy_info["consecutive_errors"] = 0

                            last_iteration = state.get("last_iteration", {})
                            last_iteration_ts = last_iteration.get("timestamp")
                            if last_iteration_ts:
                                try:
                                    ts = datetime.fromisoformat(last_iteration_ts)
                                    strategy_info["last_iteration_at"] = int(ts.timestamp())
                                except (ValueError, TypeError):
                                    strategy_info["last_iteration_at"] = 0
                            else:
                                strategy_info["last_iteration_at"] = 0

                        pnl_metrics = await self._get_portfolio_metrics(inst.strategy_id)
                        if pnl_metrics is not None:
                            strategy_info["pnl_since_deploy_usd"] = str(pnl_metrics)

                        strategies.append(strategy_info)

            except Exception as e:
                logger.debug(f"Failed to get instances from registry: {e}")

        # --- Collect filesystem templates (for AVAILABLE and ALL) ---
        if status_filter in ("AVAILABLE", "ALL"):
            for fs_strategy in self._discover_strategies_from_filesystem():
                template_id = self._canonical_template_id(fs_strategy["strategy_id"])
                if template_id in registry_template_ids:
                    continue
                strategies.append(fs_strategy)

        # --- Collect paper trading sessions ---
        # Include paper sessions for REGISTRY (default), ALL, or any status
        # filter that could match paper session statuses (PAPER_TRADING, INACTIVE).
        if status_filter not in ("AVAILABLE",):
            for paper_session in self._discover_paper_sessions():
                strategies.append(paper_session)

        # Apply status filter AFTER all sources are collected (Fix: consistent
        # filtering for paper sessions — INACTIVE filter catches inactive paper
        # sessions, PAPER_TRADING filter catches active ones).
        if status_filter in self._STATUS_FILTERS:
            strategies = [s for s in strategies if s["status"] == status_filter]

        # Apply chain filter
        filtered = []
        for s in strategies:
            if chain_filter and chain_filter not in s["chain"].lower():
                continue
            filtered.append(s)

        # Convert to proto messages
        summaries = []
        for s in filtered:
            summary_kwargs = {
                "strategy_id": s["strategy_id"],
                "name": s["name"],
                "status": s["status"],
                "chain": s["chain"],
                "protocol": s["protocol"],
                "total_value_usd": s["total_value_usd"],
                "pnl_24h_usd": s["pnl_24h_usd"],
                "last_action_at": s["last_action_at"],
                "attention_required": s["attention_required"],
                "attention_reason": s["attention_reason"],
                "is_multi_chain": s["is_multi_chain"],
                "chains": s["chains"],
                "consecutive_errors": s.get("consecutive_errors", 0),
                "last_iteration_at": s.get("last_iteration_at", 0),
                "pnl_since_deploy_usd": s.get("pnl_since_deploy_usd", ""),
                "execution_mode": s.get("execution_mode", ""),
                "paper_metrics_json": s.get("paper_metrics_json", ""),
            }
            if "wallet_address" in s:
                summary_kwargs["wallet_address"] = s["wallet_address"]
            if "chain_wallets" in s:
                summary_kwargs["chain_wallets"] = s["chain_wallets"]
            summaries.append(gateway_pb2.StrategySummary(**summary_kwargs))

        return gateway_pb2.ListStrategiesResponse(
            strategies=summaries,
            total_count=len(summaries),
        )

    async def GetStrategyDetails(
        self,
        request: gateway_pb2.GetStrategyDetailsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StrategyDetails:
        """Get detailed information about a specific strategy.

        Args:
            request: Details request with strategy_id
            context: gRPC context

        Returns:
            StrategyDetails with summary, position, timeline, etc.
        """
        await self._ensure_initialized()

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StrategyDetails()

        # In deployed mode, use platform AGENT_ID for consistent data access
        original_strategy_id = strategy_id
        strategy_id = resolve_agent_id(strategy_id)

        # Check registry first, then fall back to filesystem
        strategy_info = None
        try:
            registry = get_instance_registry()
            inst = registry.get(strategy_id)
            if inst is not None:
                effective_status = self._compute_effective_status(inst)

                # Harden last_action_at for missing heartbeats
                last_action_ts = 0
                if inst.last_heartbeat_at is not None:
                    try:
                        hb = inst.last_heartbeat_at
                        if hb.tzinfo is None:
                            hb = hb.replace(tzinfo=UTC)
                        last_action_ts = int(hb.timestamp())
                    except (ValueError, OSError):
                        pass

                # Parse chain_wallets JSON if present
                inst_chain_wallets: dict[str, str] = {}
                if hasattr(inst, "chain_wallets") and inst.chain_wallets:
                    try:
                        inst_chain_wallets = json.loads(inst.chain_wallets)
                    except (json.JSONDecodeError, TypeError):
                        pass

                strategy_info = {
                    "strategy_id": inst.strategy_id,
                    "name": inst.strategy_name.replace("_", " ").title(),
                    "status": effective_status,
                    "chain": inst.chain,
                    "protocol": inst.protocol,
                    "total_value_usd": "0",
                    "pnl_24h_usd": "0",
                    "last_action_at": last_action_ts,
                    "attention_required": effective_status in ("STALE", "ERROR"),
                    "attention_reason": "Heartbeat stale" if effective_status == "STALE" else "",
                    "is_multi_chain": "," in inst.chain,
                    "chains": [c.strip() for c in inst.chain.split(",")],
                    "consecutive_errors": 0,
                    "last_iteration_at": 0,
                    "pnl_since_deploy_usd": "",
                    "wallet_address": inst.wallet_address,
                    "chain_wallets": inst_chain_wallets,
                }
        except Exception as e:
            logger.debug(f"Failed to check registry for {strategy_id}: {e}")

        # Fallback to filesystem discovery
        if strategy_info is None:
            strategies = self._discover_strategies_from_filesystem()
            for s in strategies:
                if s["strategy_id"] == strategy_id:
                    strategy_info = s
                    break

        # Fallback to paper session discovery (match against original ID
        # because resolve_agent_id may have rewritten paper:xxx IDs)
        if strategy_info is None:
            for s in self._discover_paper_sessions():
                if s["strategy_id"] == original_strategy_id or s["strategy_id"] == strategy_id:
                    strategy_info = s
                    break

        if strategy_info is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Strategy not found: {strategy_id}")
            return gateway_pb2.StrategyDetails()

        # Enrich with state data (fallback bridges legacy pre-normalization state)
        state = await self._get_strategy_state_data(strategy_id, fallback_strategy_id=original_strategy_id)

        # Portfolio metrics (framework-owned, state fallback for legacy)
        total_value, pnl = await self._get_portfolio_value_and_pnl(
            strategy_id,
            state,
            chain=str(strategy_info.get("chain", "")),
            wallet_address=str(strategy_info.get("wallet_address", "")),
            chain_wallets=strategy_info.get("chain_wallets"),  # type: ignore[arg-type]
        )
        strategy_info["total_value_usd"] = total_value
        strategy_info["pnl_24h_usd"] = pnl

        if state:
            # Derive status from state, but never downgrade a registry-set PAUSED to ERROR.
            # The runner explicitly sets PAUSED in the registry via _gateway_update_status();
            # that signal must take precedence over a stale last_iteration error status.
            last_iteration = state.get("last_iteration", {})
            iteration_status = last_iteration.get("status", "")
            registry_status = strategy_info.get("status", "")
            if registry_status == "PAUSED":
                pass  # preserve PAUSED — operator explicitly paused this strategy
            elif iteration_status in ("EXECUTION_FAILED", "STRATEGY_ERROR"):
                strategy_info["status"] = "ERROR"
                strategy_info["attention_required"] = True
                strategy_info["attention_reason"] = f"Last iteration: {iteration_status}"
            elif "is_running" in state and state["is_running"]:
                strategy_info["status"] = "RUNNING"
            elif "is_paused" in state and state["is_paused"]:
                strategy_info["status"] = "PAUSED"

            # Get last action timestamp
            if "updated_at" in state:
                try:
                    ts = datetime.fromisoformat(state["updated_at"])
                    strategy_info["last_action_at"] = int(ts.timestamp())
                except (ValueError, TypeError):
                    pass

            try:
                strategy_info["consecutive_errors"] = int(state.get("consecutive_errors", 0) or 0)
            except (TypeError, ValueError):
                strategy_info["consecutive_errors"] = 0

            last_iteration_ts = last_iteration.get("timestamp")
            if last_iteration_ts:
                try:
                    ts = datetime.fromisoformat(last_iteration_ts)
                    strategy_info["last_iteration_at"] = int(ts.timestamp())
                except (ValueError, TypeError):
                    strategy_info["last_iteration_at"] = 0
            else:
                strategy_info["last_iteration_at"] = 0

        pnl_metrics = await self._get_portfolio_metrics(strategy_id)
        if pnl_metrics is not None:
            strategy_info["pnl_since_deploy_usd"] = str(pnl_metrics)

        # Build summary
        summary_kwargs = {
            "strategy_id": str(strategy_info["strategy_id"]),
            "name": str(strategy_info["name"]),
            "status": str(strategy_info["status"]),
            "chain": str(strategy_info["chain"]),
            "protocol": str(strategy_info["protocol"]),
            "total_value_usd": str(strategy_info["total_value_usd"]),
            "pnl_24h_usd": str(strategy_info["pnl_24h_usd"]),
            "last_action_at": int(str(strategy_info["last_action_at"])),
            "attention_required": bool(strategy_info["attention_required"]),
            "attention_reason": str(strategy_info["attention_reason"]),
            "is_multi_chain": bool(strategy_info["is_multi_chain"]),
            "chains": strategy_info["chains"],
            "consecutive_errors": int(str(strategy_info.get("consecutive_errors", 0))),
            "last_iteration_at": int(str(strategy_info.get("last_iteration_at", 0))),
            "pnl_since_deploy_usd": str(strategy_info.get("pnl_since_deploy_usd", "")),
            "execution_mode": str(strategy_info.get("execution_mode", "")),
            "paper_metrics_json": str(strategy_info.get("paper_metrics_json", "")),
        }
        if "wallet_address" in strategy_info:
            summary_kwargs["wallet_address"] = str(strategy_info["wallet_address"])
        if "chain_wallets" in strategy_info:
            summary_kwargs["chain_wallets"] = strategy_info["chain_wallets"]
        summary = gateway_pb2.StrategySummary(**summary_kwargs)  # type: ignore[arg-type]

        # Build position info — prefer framework snapshot over state dict
        position = gateway_pb2.PositionInfo()

        # Primary: wallet balances from persisted portfolio snapshot (Phase 1c)
        snapshot_balances_populated = False
        try:
            latest_snap = await self._get_latest_snapshot(strategy_id)
            if latest_snap and latest_snap.wallet_balances:
                for wb in latest_snap.wallet_balances:
                    position.token_balances.append(
                        gateway_pb2.TokenBalanceInfo(
                            symbol=wb.symbol,
                            balance=str(wb.balance),
                            value_usd=str(wb.value_usd),
                        )
                    )
                snapshot_balances_populated = True
        except Exception:
            logger.debug("Failed to get snapshot balances for %s", strategy_id, exc_info=True)

        if state:
            # Fallback: extract token balances from state dict if snapshot didn't have them
            if not snapshot_balances_populated:
                balances = state.get("balances", {})
                for symbol, balance_data in balances.items():
                    if isinstance(balance_data, dict):
                        position.token_balances.append(
                            gateway_pb2.TokenBalanceInfo(
                                symbol=symbol,
                                balance=str(balance_data.get("balance", "0")),
                                value_usd=str(balance_data.get("value_usd", "0")),
                            )
                        )

            # Extract health factor and leverage if present
            if "health_factor" in state:
                position.health_factor = str(state["health_factor"])
            if "leverage" in state:
                position.leverage = str(state["leverage"])

        # Include cached strategy positions from heartbeat
        cached = self._cached_positions.get(strategy_id)
        if cached:
            position.strategy_positions.extend(cached)

        # Get timeline events if requested
        timeline = []
        if request.include_timeline:
            limit = request.timeline_limit if request.timeline_limit > 0 else 20
            timeline_response = await self.GetTimeline(
                gateway_pb2.GetTimelineRequest(strategy_id=strategy_id, limit=limit),
                context,
            )
            timeline = list(timeline_response.events)

        # Build PnL history time series from portfolio snapshots
        pnl_history = []
        if request.include_pnl_history:
            pnl_history = await self._build_pnl_history(strategy_id)

        # Derive chain health from strategy chains.
        # Stub: reports UNKNOWN until real health probing (RPC latency, block number, gas price) is wired.
        chain_health = {}
        raw_chains = strategy_info.get("chains")
        chains: list[str] = raw_chains if isinstance(raw_chains, list) else []
        for chain_name in chains:
            chain_health[chain_name] = gateway_pb2.ChainHealthInfo(
                chain=chain_name,
                status="UNKNOWN",
                last_updated=int(datetime.now(UTC).timestamp()),
            )

        return gateway_pb2.StrategyDetails(
            summary=summary,
            position=position,
            timeline=timeline,
            pnl_history=pnl_history,
            chain_health=chain_health,
        )

    async def GetTimeline(
        self,
        request: gateway_pb2.GetTimelineRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTimelineResponse:
        """Get timeline events for a strategy.

        Args:
            request: Timeline request with strategy_id, limit, filters
            context: gRPC context

        Returns:
            GetTimelineResponse with timeline events
        """
        await self._ensure_initialized()

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTimelineResponse()

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        limit = request.limit if request.limit > 0 else 50
        event_type_filter = request.event_type_filter if request.event_type_filter else None
        since = datetime.fromtimestamp(request.since_timestamp, tz=UTC) if request.since_timestamp > 0 else None

        events = []

        # Get events from TimelineStore (primary source)
        # TimelineStore is initialized at server startup with persistent path if configured
        try:
            store = get_timeline_store()
            timeline_events = store.get_events(
                strategy_id=strategy_id,
                limit=limit,
                event_type=event_type_filter,
                since=since,
            )

            for event in timeline_events:
                events.append(
                    gateway_pb2.TimelineEventInfo(
                        timestamp=int(event.timestamp.timestamp()) if event.timestamp else 0,
                        event_type=event.event_type,
                        description=event.description,
                        tx_hash=event.tx_hash or "",
                        details_json=json.dumps(event.details) if event.details else "",
                        chain=event.chain or "",
                    )
                )
        except Exception as e:
            logger.debug(f"Failed to get events from TimelineStore: {e}")

        # Fallback: Try to load events from cache file if TimelineStore is empty
        if not events:
            cache_file = self._strategies_root.parent / ".dashboard_events.json" if self._strategies_root else None
            if cache_file and cache_file.exists():
                try:
                    cached_data = json.loads(cache_file.read_text())
                    strategy_events = cached_data.get(strategy_id, [])

                    for event_data in strategy_events[:limit]:
                        events.append(
                            gateway_pb2.TimelineEventInfo(
                                timestamp=int(datetime.fromisoformat(event_data.get("timestamp", "")).timestamp())
                                if event_data.get("timestamp")
                                else 0,
                                event_type=event_data.get("event_type", "UNKNOWN"),
                                description=event_data.get("description", ""),
                                tx_hash=event_data.get("tx_hash", ""),
                                details_json=json.dumps(event_data.get("details", {})),
                                chain=event_data.get("chain", ""),
                            )
                        )
                except Exception as e:
                    logger.debug(f"Failed to load timeline events from cache: {e}")

        # Also check state for execution history
        state = await self._get_strategy_state_data(strategy_id)
        if state and "execution_history" in state:
            for exec_record in state.get("execution_history", [])[:limit]:
                if isinstance(exec_record, dict):
                    events.append(
                        gateway_pb2.TimelineEventInfo(
                            timestamp=int(datetime.fromisoformat(exec_record.get("timestamp", "")).timestamp())
                            if exec_record.get("timestamp")
                            else 0,
                            event_type=exec_record.get("event_type", "EXECUTION"),
                            description=exec_record.get("description", "Execution completed"),
                            tx_hash=exec_record.get("tx_hash", ""),
                            details_json=json.dumps(exec_record.get("details", {})),
                            chain=exec_record.get("chain", ""),
                        )
                    )

        # Sort by timestamp descending and limit
        events.sort(key=lambda e: e.timestamp, reverse=True)
        events = events[:limit]

        return gateway_pb2.GetTimelineResponse(
            events=events,
            has_more=len(events) >= limit,
        )

    async def GetStrategyConfig(
        self,
        request: gateway_pb2.GetStrategyConfigRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StrategyConfigResponse:
        """Get strategy configuration.

        Args:
            request: Config request with strategy_id
            context: gRPC context

        Returns:
            StrategyConfigResponse with config JSON
        """
        await self._ensure_initialized()

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StrategyConfigResponse()

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        # Try filesystem first (local development)
        if self._strategies_root is not None:
            for category in STRATEGY_CATEGORIES:
                config_file = self._strategies_root / category / strategy_id / "config.json"
                if config_file.exists():
                    try:
                        config = json.loads(config_file.read_text())
                        return gateway_pb2.StrategyConfigResponse(
                            strategy_id=strategy_id,
                            strategy_name=config.get("strategy_name", strategy_id),
                            config_json=json.dumps(config),
                            last_updated=int(config_file.stat().st_mtime),
                        )
                    except Exception as e:
                        logger.error(f"Failed to read config file for {strategy_id}: {e}")
                        context.set_code(grpc.StatusCode.INTERNAL)
                        context.set_details("Failed to read strategy config")
                        return gateway_pb2.StrategyConfigResponse()

        # Fallback to instance registry (deployed mode — config was stored at registration)
        try:
            registry = get_instance_registry()
            inst = registry.get(strategy_id)
        except Exception as e:
            logger.error(f"Failed to get config from registry for {strategy_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to read strategy config")
            return gateway_pb2.StrategyConfigResponse()

        if inst is not None and inst.config_json:
            try:
                config = json.loads(inst.config_json)
            except json.JSONDecodeError:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Stored config is invalid JSON")
                return gateway_pb2.StrategyConfigResponse()
            return gateway_pb2.StrategyConfigResponse(
                strategy_id=strategy_id,
                strategy_name=config.get("strategy_name", inst.strategy_name),
                config_json=inst.config_json,
                last_updated=int(inst.updated_at.timestamp()) if inst.updated_at else 0,
            )

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Config not found for strategy: {strategy_id}")
        return gateway_pb2.StrategyConfigResponse()

    async def GetStrategyState(
        self,
        request: gateway_pb2.GetStrategyStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.StrategyStateResponse:
        """Get current strategy state.

        Args:
            request: State request with strategy_id and optional field filter
            context: gRPC context

        Returns:
            StrategyStateResponse with state JSON
        """
        await self._ensure_initialized()

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.StrategyStateResponse()

        # In deployed mode, use platform AGENT_ID for consistent data access
        original_strategy_id = strategy_id
        strategy_id = resolve_agent_id(strategy_id)

        state = await self._get_strategy_state_data(strategy_id, fallback_strategy_id=original_strategy_id)
        if state is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"State not found for strategy: {strategy_id}")
            return gateway_pb2.StrategyStateResponse()

        # Filter fields if specified
        if request.fields:
            filtered_state = {k: v for k, v in state.items() if k in request.fields}
        else:
            filtered_state = state

        # Get version from state manager
        version = 0
        updated_at = 0
        if self._state_manager:
            try:
                state_obj = await self._state_manager.load_state(strategy_id)
                if state_obj:
                    version = state_obj.version
                    if state_obj.created_at:
                        updated_at = int(state_obj.created_at.timestamp())
            except Exception:
                pass

        return gateway_pb2.StrategyStateResponse(
            strategy_id=strategy_id,
            state_json=json.dumps(filtered_state),
            version=version,
            updated_at=updated_at,
        )

    async def ExecuteAction(
        self,
        request: gateway_pb2.ExecuteActionRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ExecuteActionResponse:
        """Execute operator action (pause, resume, emergency).

        Args:
            request: Action request with strategy_id, action, reason
            context: gRPC context

        Returns:
            ExecuteActionResponse with success status
        """
        await self._ensure_initialized()

        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ExecuteActionResponse(success=False, error=str(e))

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        action = request.action.upper()
        reason = request.reason

        if not reason:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Reason is required for audit")
            return gateway_pb2.ExecuteActionResponse(success=False, error="Reason is required")

        action_id = str(uuid4())

        # Log the action for audit
        logger.info(f"Dashboard action: {action} on {strategy_id}, reason: {reason}, action_id: {action_id}")

        # Map dashboard actions to lifecycle commands.
        # Instead of mutating state flags directly, we write a command to the
        # LifecycleStore. The strategy runner's poll loop picks it up and
        # transitions state atomically.
        _ACTION_TO_COMMAND = {"PAUSE": "PAUSE", "RESUME": "RESUME", "STOP": "STOP"}
        command = _ACTION_TO_COMMAND.get(action)
        if command is None:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(f"Action not implemented: {action}")
            return gateway_pb2.ExecuteActionResponse(
                success=False,
                error=f"Action not implemented: {action}",
                action_id=action_id,
            )

        try:
            from almanak.gateway.lifecycle import get_lifecycle_store

            store = get_lifecycle_store()
            store.write_command(
                agent_id=strategy_id,
                command=command,
                issued_by=f"dashboard:{reason}",
            )
            logger.info(f"Issued {command} command to {strategy_id} via lifecycle store: {reason}")
            return gateway_pb2.ExecuteActionResponse(
                success=True,
                action_id=action_id,
            )
        except Exception as e:
            logger.error(f"Failed to issue {command} command to {strategy_id}: {e}")
            return gateway_pb2.ExecuteActionResponse(
                success=False,
                error=str(e),
                action_id=action_id,
            )

    # =========================================================================
    # Instance Registry RPCs
    # =========================================================================

    async def RegisterStrategyInstance(
        self,
        request: gateway_pb2.RegisterInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.RegisterInstanceResponse:
        """Register a strategy instance in the persistent registry."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            return gateway_pb2.RegisterInstanceResponse(success=False, error=str(e))

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        try:
            from almanak.gateway.registry.store import StrategyInstance

            registry = get_instance_registry()
            now = datetime.now(UTC)

            existing = registry.get(strategy_id)
            # Read chains and chain_wallets from request
            chains_str = ",".join(request.chains) if request.chains else request.chain
            chain_wallets_str = ""
            if request.chain_wallets:
                chain_wallets_str = json.dumps(dict(request.chain_wallets))

            # Derive protocol from strategy name/ID if not provided.
            # Use the original strategy_name or request.strategy_id for derivation,
            # not the resolved strategy_id which may be a platform UUID/AGENT_ID.
            protocol = request.protocol
            if not protocol:
                config = {}
                if request.config_json:
                    try:
                        config = json.loads(request.config_json)
                    except (json.JSONDecodeError, TypeError):
                        pass
                derivation_key = request.strategy_name or request.strategy_id or strategy_id
                protocol = self._derive_protocol_from_config(config, derivation_key)
                if protocol == "Unknown":
                    protocol = ""

            instance = StrategyInstance(
                strategy_id=strategy_id,
                strategy_name=request.strategy_name or strategy_id,
                template_name=request.template_name,
                chain=request.chain,
                protocol=protocol,
                wallet_address=request.wallet_address,
                config_json=request.config_json,
                chains=chains_str,
                chain_wallets=chain_wallets_str,
                status="RUNNING",
                archived=existing.archived if existing else False,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                last_heartbeat_at=now,
                version=request.version,
            )

            registry.register(instance)

            return gateway_pb2.RegisterInstanceResponse(
                success=True,
                already_existed=existing is not None,
            )
        except Exception as e:
            logger.error(f"Failed to register instance {request.strategy_id}: {e}")
            return gateway_pb2.RegisterInstanceResponse(success=False, error=str(e))

    async def UpdateStrategyInstanceStatus(
        self,
        request: gateway_pb2.UpdateInstanceStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UpdateInstanceStatusResponse:
        """Update strategy instance status or send heartbeat."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            return gateway_pb2.UpdateInstanceStatusResponse(success=False, error=str(e))

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        try:
            registry = get_instance_registry()

            if request.heartbeat_only:
                success = registry.heartbeat(strategy_id)
            else:
                success = registry.update_status(strategy_id, request.status, request.reason)

            if not success:
                return gateway_pb2.UpdateInstanceStatusResponse(
                    success=False,
                    error=f"Instance not found: {strategy_id}",
                )

            # Cache strategy positions (clear stale data when none reported)
            if request.positions:
                self._cached_positions[strategy_id] = list(request.positions)
            else:
                self._cached_positions.pop(strategy_id, None)

            return gateway_pb2.UpdateInstanceStatusResponse(success=True)
        except Exception as e:
            logger.error(f"Failed to update instance status {request.strategy_id}: {e}")
            return gateway_pb2.UpdateInstanceStatusResponse(success=False, error=str(e))

    async def ArchiveStrategyInstance(
        self,
        request: gateway_pb2.ArchiveInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ArchiveInstanceResponse:
        """Archive a strategy instance (hidden from dashboard, data retained)."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            return gateway_pb2.ArchiveInstanceResponse(success=False, error=str(e))

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        try:
            registry = get_instance_registry()
            success = registry.archive(strategy_id)
            if not success:
                return gateway_pb2.ArchiveInstanceResponse(
                    success=False,
                    error=f"Instance not found: {strategy_id}",
                )

            self._cached_positions.pop(strategy_id, None)
            logger.info(f"Archived instance {strategy_id}: {request.reason}")
            return gateway_pb2.ArchiveInstanceResponse(success=True)
        except Exception as e:
            logger.error(f"Failed to archive instance {request.strategy_id}: {e}")
            return gateway_pb2.ArchiveInstanceResponse(success=False, error=str(e))

    async def PurgeStrategyInstance(
        self,
        request: gateway_pb2.PurgeInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PurgeInstanceResponse:
        """Purge a strategy instance and all its events (permanent delete)."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            return gateway_pb2.PurgeInstanceResponse(success=False, error=str(e))

        # In deployed mode, use platform AGENT_ID for consistent data access
        strategy_id = resolve_agent_id(strategy_id)

        if not request.reason:
            return gateway_pb2.PurgeInstanceResponse(
                success=False,
                error="Reason is required for audit when purging",
            )

        try:
            registry = get_instance_registry()

            # Atomic delete of instance + events in single transaction
            success = registry.purge_with_events(strategy_id)
            if not success:
                return gateway_pb2.PurgeInstanceResponse(
                    success=False,
                    error=f"Instance not found: {strategy_id}",
                )

            # Also clear from timeline cache
            try:
                store = get_timeline_store()
                store.clear_events(strategy_id)
            except Exception as e:
                logger.debug(f"Failed to clear timeline cache for {strategy_id} (non-fatal): {e}")

            self._cached_positions.pop(strategy_id, None)
            logger.info(f"Purged instance {strategy_id}: {request.reason}")
            return gateway_pb2.PurgeInstanceResponse(success=True)
        except Exception as e:
            logger.error(f"Failed to purge instance {request.strategy_id}: {e}")
            return gateway_pb2.PurgeInstanceResponse(success=False, error=str(e))

    async def GetTransactionLedger(
        self,
        request: gateway_pb2.GetTransactionLedgerRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTransactionLedgerResponse:
        """Get structured trade records from the transaction ledger."""
        try:
            strategy_id = validate_strategy_id(request.strategy_id)
        except ValidationError as e:
            logger.warning(f"Invalid strategy_id in GetTransactionLedger: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTransactionLedgerResponse()

        await self._ensure_initialized()
        strategy_id = resolve_agent_id(strategy_id)

        since = None
        if request.since_timestamp > 0:
            since = datetime.fromtimestamp(request.since_timestamp, tz=UTC)

        intent_type = request.intent_type_filter or None
        limit = request.limit if request.limit > 0 else 100

        entries = []
        if self._state_manager is not None:
            try:
                entries = await self._state_manager.get_ledger_entries(
                    strategy_id, since=since, intent_type=intent_type, limit=limit + 1
                )
            except Exception:
                logger.debug("Failed to query transaction ledger for %s", strategy_id, exc_info=True)

        has_more = len(entries) > limit
        if has_more:
            entries = entries[:limit]

        proto_entries = []
        for entry in entries:
            proto_entries.append(
                gateway_pb2.LedgerEntryInfo(
                    id=entry.id,
                    cycle_id=entry.cycle_id,
                    strategy_id=entry.strategy_id,
                    timestamp=int(entry.timestamp.timestamp()),
                    intent_type=entry.intent_type,
                    token_in=entry.token_in,
                    amount_in=entry.amount_in,
                    token_out=entry.token_out,
                    amount_out=entry.amount_out,
                    effective_price=entry.effective_price,
                    slippage_bps=entry.slippage_bps or 0.0,
                    gas_used=entry.gas_used,
                    gas_usd=entry.gas_usd,
                    tx_hash=entry.tx_hash,
                    chain=entry.chain,
                    protocol=entry.protocol,
                    success=entry.success,
                    error=entry.error,
                )
            )

        return gateway_pb2.GetTransactionLedgerResponse(
            entries=proto_entries,
            has_more=has_more,
        )
