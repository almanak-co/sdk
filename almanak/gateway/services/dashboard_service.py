"""DashboardService implementation - provides data for operator dashboards.

This service exposes strategy data for dashboards via gRPC. All filesystem
and database access happens here in the gateway; dashboard containers only
receive the formatted data.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import grpc

if TYPE_CHECKING:
    from almanak.framework.state.state_manager import StateManager

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.registry import get_instance_registry
from almanak.gateway.timeline.store import get_timeline_store
from almanak.gateway.validation import ValidationError, validate_strategy_id

logger = logging.getLogger(__name__)


# Strategy categories in the filesystem
STRATEGY_CATEGORIES = ["demo", "production", "incubating", "poster_child", "tests"]


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
                        }
                    )
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to load strategy config from {config_file}: {e}")
                    continue

        return strategies

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

        return "Unknown"

    async def _get_strategy_state_data(self, strategy_id: str) -> dict | None:
        """Get strategy state from StateManager.

        Returns:
            State dict or None if not found
        """
        if self._state_manager is None:
            return None

        try:
            state = await self._state_manager.load_state(strategy_id)
            if state is not None:
                return state.state
        except Exception as e:
            logger.debug(f"Failed to load state for {strategy_id}: {e}")

        return None

    def _extract_portfolio_value_from_state(self, state: dict) -> tuple[str, str]:
        """Extract portfolio value and 24h PnL from strategy state.

        Returns:
            Tuple of (total_value_usd, pnl_24h_usd) as strings
        """
        total_value_usd = Decimal("0")
        pnl_24h_usd = Decimal("0")

        value_keys = [
            "total_value_usd",
            "total_position_value_usd",
            "portfolio_value_usd",
            "total_collateral_value_usd",
            "position_value_usd",
            "net_value_usd",
        ]

        for key in value_keys:
            if key in state:
                try:
                    total_value_usd = Decimal(str(state[key]))
                    break
                except (ValueError, TypeError):
                    continue

        # For lending strategies, try collateral - debt
        if total_value_usd == Decimal("0"):
            collateral = state.get("total_collateral_value_usd")
            debt = state.get("total_debt_value_usd")
            if collateral is not None and debt is not None:
                try:
                    total_value_usd = Decimal(str(collateral)) - Decimal(str(debt))
                except (ValueError, TypeError):
                    pass

        # Try to extract PnL
        pnl_keys = ["pnl_24h_usd", "pnl_today_usd", "daily_pnl_usd", "total_profit_usd"]
        for key in pnl_keys:
            if key in state:
                try:
                    pnl_24h_usd = Decimal(str(state[key]))
                    break
                except (ValueError, TypeError):
                    continue

        return str(total_value_usd), str(pnl_24h_usd)

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
    _STATUS_FILTERS = frozenset({"RUNNING", "PAUSED", "ERROR", "STUCK", "STALE", "INACTIVE", "ARCHIVED"})
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
                    registry_template_ids.add(self._canonical_template_id(inst.strategy_id))

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
                        }

                        # Enrich with state data
                        state = await self._get_strategy_state_data(inst.strategy_id)
                        if state:
                            total_value, pnl = self._extract_portfolio_value_from_state(state)
                            strategy_info["total_value_usd"] = total_value
                            strategy_info["pnl_24h_usd"] = pnl

                        strategies.append(strategy_info)

            except Exception as e:
                logger.debug(f"Failed to get instances from registry: {e}")

        # Apply status filter (RUNNING, PAUSED, ERROR, etc.)
        if status_filter in self._STATUS_FILTERS:
            strategies = [s for s in strategies if s["status"] == status_filter]

        # --- Collect filesystem templates (for AVAILABLE and ALL) ---
        if status_filter in ("AVAILABLE", "ALL"):
            for fs_strategy in self._discover_strategies_from_filesystem():
                template_id = self._canonical_template_id(fs_strategy["strategy_id"])
                if template_id in registry_template_ids:
                    continue
                strategies.append(fs_strategy)

        # Apply chain filter
        filtered = []
        for s in strategies:
            if chain_filter and chain_filter not in s["chain"].lower():
                continue
            filtered.append(s)

        # Convert to proto messages
        summaries = []
        for s in filtered:
            summaries.append(
                gateway_pb2.StrategySummary(
                    strategy_id=s["strategy_id"],
                    name=s["name"],
                    status=s["status"],
                    chain=s["chain"],
                    protocol=s["protocol"],
                    total_value_usd=s["total_value_usd"],
                    pnl_24h_usd=s["pnl_24h_usd"],
                    last_action_at=s["last_action_at"],
                    attention_required=s["attention_required"],
                    attention_reason=s["attention_reason"],
                    is_multi_chain=s["is_multi_chain"],
                    chains=s["chains"],
                )
            )

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

        if strategy_info is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Strategy not found: {strategy_id}")
            return gateway_pb2.StrategyDetails()

        # Enrich with state data
        state = await self._get_strategy_state_data(strategy_id)
        if state:
            total_value, pnl = self._extract_portfolio_value_from_state(state)
            strategy_info["total_value_usd"] = total_value
            strategy_info["pnl_24h_usd"] = pnl

            # Derive status from state (same logic as ListStrategies)
            last_iteration = state.get("last_iteration", {})
            iteration_status = last_iteration.get("status", "")
            if iteration_status in ("EXECUTION_FAILED", "STRATEGY_ERROR"):
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

        # Build summary
        summary = gateway_pb2.StrategySummary(
            strategy_id=str(strategy_info["strategy_id"]),
            name=str(strategy_info["name"]),
            status=str(strategy_info["status"]),
            chain=str(strategy_info["chain"]),
            protocol=str(strategy_info["protocol"]),
            total_value_usd=str(strategy_info["total_value_usd"]),
            pnl_24h_usd=str(strategy_info["pnl_24h_usd"]),
            last_action_at=int(str(strategy_info["last_action_at"])),
            attention_required=bool(strategy_info["attention_required"]),
            attention_reason=str(strategy_info["attention_reason"]),
            is_multi_chain=bool(strategy_info["is_multi_chain"]),
            chains=strategy_info["chains"],  # type: ignore[arg-type]
        )

        # Build position info from state
        position = gateway_pb2.PositionInfo()
        if state:
            # Extract token balances
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

        # Derive PnL snapshot from already-extracted portfolio values
        pnl_history = []
        if request.include_pnl_history:
            total_value = str(strategy_info["total_value_usd"])
            pnl = str(strategy_info["pnl_24h_usd"])
            if total_value != "0":
                pnl_history.append(
                    gateway_pb2.PnLDataPoint(
                        timestamp=int(datetime.now(UTC).timestamp()),
                        value_usd=total_value,
                        pnl_usd=pnl,
                    )
                )

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

        # Find config file
        if self._strategies_root is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Strategies directory not found")
            return gateway_pb2.StrategyConfigResponse()

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
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"Failed to read config: {e}")
                    return gateway_pb2.StrategyConfigResponse()

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

        state = await self._get_strategy_state_data(strategy_id)
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

        try:
            from almanak.gateway.registry.store import StrategyInstance

            registry = get_instance_registry()
            now = datetime.now(UTC)

            existing = registry.get(strategy_id)
            instance = StrategyInstance(
                strategy_id=strategy_id,
                strategy_name=request.strategy_name or strategy_id,
                template_name=request.template_name,
                chain=request.chain,
                protocol=request.protocol,
                wallet_address=request.wallet_address,
                config_json=request.config_json,
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
