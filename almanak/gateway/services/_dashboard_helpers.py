"""Internal helpers shared between ``DashboardServiceServicer.ListStrategies``
and ``DashboardServiceServicer.GetStrategyDetails``.

These functions are extracted to collapse the large blocks of duplicated
code between the two RPCs. They are intentionally pure (no ``self``, no
I/O, no await) so that the calling RPCs keep control over awaits and
gateway state access.

Contract surface is captured by the Phase 5a characterization tests in
``tests/gateway/test_dashboard_service.py`` — any change here must keep
those tests passing byte-for-byte.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.portfolio.models import PortfolioSnapshot

logger = logging.getLogger(__name__)


def build_registry_strategy_info(
    inst: Any,
    effective_status: str,
) -> dict:
    """Build the base ``strategy_info`` dict from a registry instance.

    Replaces the near-identical blocks in ``ListStrategies`` (lines
    ~697-738) and ``GetStrategyDetails`` (lines ~870-909). The dict
    shape is what the downstream enrichment + ``StrategySummary``
    construction expects.

    ``chain_wallets`` parsing uses ``isinstance(parsed, dict)`` to guard
    against malformed JSON that happens to parse to a non-dict (e.g. a
    list). This is the stricter of the two original variants — a
    superset: any JSON that parses to a dict yields identical results
    in both sites, and any JSON that parses to a non-dict is discarded
    rather than propagated to the proto layer (which previously would
    have raised on the ``GetStrategyDetails`` path).

    Args:
        inst: Registry ``StrategyInstance`` row.
        effective_status: Result of ``_compute_effective_status(inst)``
            (computed by caller because the method lives on the
            servicer).

    Returns:
        ``strategy_info`` dict matching the pre-refactor shape:
        ``strategy_id, name, status, chain, protocol, total_value_usd,
        pnl_24h_usd, last_action_at, attention_required, attention_reason,
        is_multi_chain, chains, consecutive_errors, last_iteration_at,
        pnl_since_deploy_usd, wallet_address, chain_wallets``.
    """
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

    return {
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


def enrich_strategy_info(
    info: dict,
    *,
    state: dict | None,
    total_value: str,
    pnl: str,
    pnl_metrics: Decimal | None,
    preserve_status_precedence: bool,
) -> None:
    """Merge portfolio values and state-derived fields into ``info``.

    This folds the enrichment tails of the two RPCs into a single
    function with an explicit flag for the behavioural difference.

    Args:
        info: The ``strategy_info`` dict being enriched (mutated in place).
        state: The state dict from ``_get_strategy_state_data`` (may be
            ``None``).
        total_value: ``total_value_usd`` returned by
            ``_get_portfolio_value_and_pnl``.
        pnl: ``pnl_24h_usd`` returned by the same.
        pnl_metrics: The ``pnl_after_gas`` returned by
            ``_get_portfolio_metrics``; ``None`` when unavailable.
        preserve_status_precedence: When ``True`` (``GetStrategyDetails``
            behaviour), derive the effective ``status`` field from
            ``state`` with the documented precedence:

            1. A registry-set ``PAUSED`` wins over any iteration signal
               (so an operator pause is never downgraded to ``ERROR`` by
               a stale iteration record).
            2. ``EXECUTION_FAILED`` / ``STRATEGY_ERROR`` →
               ``status=ERROR`` + attention flag.
            3. ``is_running=True`` → ``RUNNING``.
            4. ``is_paused=True`` → ``PAUSED``.

            Also honours ``state["updated_at"]`` for
            ``last_action_at``. When ``False`` (``ListStrategies``
            behaviour), these status/last-action derivations are skipped
            — ``ListStrategies`` keeps the registry-computed status.

    Preserves byte-for-byte behaviour of the original blocks, including
    the ``issue #1706`` ordering (``is_running`` checked before
    ``is_paused``) and ``issue #1705`` (``chains`` field not touched).
    """
    info["total_value_usd"] = total_value
    info["pnl_24h_usd"] = pnl

    if state:
        if preserve_status_precedence:
            # Derive status from state, but never downgrade a registry-set
            # PAUSED to ERROR. The runner explicitly sets PAUSED in the
            # registry via _gateway_update_status(); that signal must take
            # precedence over a stale last_iteration error status.
            last_iteration = state.get("last_iteration", {})
            iteration_status = last_iteration.get("status", "")
            registry_status = info.get("status", "")
            if registry_status == "PAUSED":
                pass  # preserve PAUSED — operator explicitly paused this strategy
            elif iteration_status in ("EXECUTION_FAILED", "STRATEGY_ERROR"):
                info["status"] = "ERROR"
                info["attention_required"] = True
                info["attention_reason"] = f"Last iteration: {iteration_status}"
            elif "is_running" in state and state["is_running"]:
                info["status"] = "RUNNING"
            elif "is_paused" in state and state["is_paused"]:
                info["status"] = "PAUSED"

            # Get last action timestamp
            if "updated_at" in state:
                try:
                    ts = datetime.fromisoformat(state["updated_at"])
                    info["last_action_at"] = int(ts.timestamp())
                except (ValueError, TypeError):
                    pass

        try:
            info["consecutive_errors"] = int(state.get("consecutive_errors", 0) or 0)
        except (TypeError, ValueError):
            info["consecutive_errors"] = 0

        last_iteration = state.get("last_iteration", {})
        last_iteration_ts = last_iteration.get("timestamp")
        if last_iteration_ts:
            try:
                ts = datetime.fromisoformat(last_iteration_ts)
                info["last_iteration_at"] = int(ts.timestamp())
            except (ValueError, TypeError):
                info["last_iteration_at"] = 0
        else:
            info["last_iteration_at"] = 0

    if pnl_metrics is not None:
        info["pnl_since_deploy_usd"] = str(pnl_metrics)


def build_strategy_summary_kwargs(info: dict) -> dict:
    """Return the kwargs dict for ``gateway_pb2.StrategySummary(**kwargs)``.

    Kwarg set must remain stable — downstream proto serialization
    depends on an exact keyset. Both RPCs feed ``StrategySummary`` from
    dicts built by the discovery helpers
    (``_registry_instance_to_info``, ``_discover_strategies_from_filesystem``,
    ``_discover_paper_sessions``) or the registry builder above, all of
    which emit the same shape.

    ``wallet_address`` and ``chain_wallets`` are included ONLY when
    present in ``info`` — registry instances carry them, filesystem
    templates and paper sessions do not.
    """
    summary_kwargs: dict[str, Any] = {
        "strategy_id": info["strategy_id"],
        "name": info["name"],
        "status": info["status"],
        "chain": info["chain"],
        "protocol": info["protocol"],
        "total_value_usd": info["total_value_usd"],
        "pnl_24h_usd": info["pnl_24h_usd"],
        "last_action_at": info["last_action_at"],
        "attention_required": info["attention_required"],
        "attention_reason": info["attention_reason"],
        "is_multi_chain": info["is_multi_chain"],
        "chains": info["chains"],
        "consecutive_errors": info.get("consecutive_errors", 0),
        "last_iteration_at": info.get("last_iteration_at", 0),
        "pnl_since_deploy_usd": info.get("pnl_since_deploy_usd", ""),
        "execution_mode": info.get("execution_mode", ""),
        "paper_metrics_json": info.get("paper_metrics_json", ""),
    }
    if "wallet_address" in info:
        summary_kwargs["wallet_address"] = info["wallet_address"]
    if "chain_wallets" in info:
        summary_kwargs["chain_wallets"] = info["chain_wallets"]
    return summary_kwargs


def build_chain_health(chains: list[str]) -> dict[str, gateway_pb2.ChainHealthInfo]:
    """Build the chain-health stub map for ``GetStrategyDetails``.

    Reports ``status="UNKNOWN"`` for every chain in ``chains`` with the
    current wall-clock timestamp. This is a stub until real health
    probing (RPC latency, block number, gas price) is wired up.

    Args:
        chains: List of chain names (may be empty).

    Returns:
        A dict mapping chain name -> ``ChainHealthInfo``. Empty dict when
        ``chains`` is empty.
    """
    now_ts = int(datetime.now(UTC).timestamp())
    return {
        chain_name: gateway_pb2.ChainHealthInfo(
            chain=chain_name,
            status="UNKNOWN",
            last_updated=now_ts,
        )
        for chain_name in chains
    }


def build_position_proto(
    state: dict | None,
    cached_positions: Any,
    snapshot: PortfolioSnapshot | None,
) -> gateway_pb2.PositionInfo:
    """Assemble the ``PositionInfo`` proto for a strategy.

    Balance sourcing precedence (preserved from the pre-refactor block):

    1. **Primary** — ``snapshot.wallet_balances`` from the persisted
       portfolio snapshot (Phase 1c). When the snapshot is present and
       carries at least one balance row, those rows populate
       ``position.token_balances`` and the state-dict fallback is
       skipped.
    2. **Fallback** — ``state["balances"]`` dict entries, iterated in
       insertion order. Only used when the snapshot did not populate any
       balances. Non-dict ``balance_data`` values are skipped.

    Regardless of balance source, ``health_factor`` and ``leverage`` are
    pulled from ``state`` (when present) and ``cached_positions`` from
    the heartbeat cache are extended onto ``position.strategy_positions``.

    Args:
        state: Strategy state dict (may be ``None``).
        cached_positions: Iterable of cached ``StrategyPosition`` protos
            from the heartbeat cache; may be ``None`` or empty.
        snapshot: Latest ``PortfolioSnapshot`` (may be ``None``).

    Returns:
        A populated ``gateway_pb2.PositionInfo``.
    """
    position = gateway_pb2.PositionInfo()

    # Primary: wallet balances from persisted portfolio snapshot (Phase 1c)
    snapshot_balances_populated = False
    try:
        if snapshot is not None and snapshot.wallet_balances:
            for wb in snapshot.wallet_balances:
                position.token_balances.append(
                    gateway_pb2.TokenBalanceInfo(
                        symbol=wb.symbol,
                        balance=str(wb.balance),
                        value_usd=str(wb.value_usd),
                    )
                )
            snapshot_balances_populated = True
    except Exception:
        logger.debug("Failed to populate snapshot balances", exc_info=True)

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
    if cached_positions:
        position.strategy_positions.extend(cached_positions)

    return position


def lookup_strategy_source(
    *,
    strategy_id: str,
    original_strategy_id: str,
    registry_getter: Any,
    compute_effective_status: Any,
    discover_filesystem: Any,
    discover_paper_sessions: Any,
) -> dict | None:
    """Resolve a strategy to its base ``strategy_info`` dict.

    Performs the three-way fallback cascade preserved byte-for-byte
    from the pre-refactor ``GetStrategyDetails`` block:

    1. **Registry lookup** via ``registry_getter().get(strategy_id)``.
       On hit, builds the dict via :func:`build_registry_strategy_info`
       using the effective status computed by the caller-supplied
       ``compute_effective_status`` callable (which lives on the
       servicer). Registry errors are caught and logged at DEBUG.
    2. **Filesystem discovery** via the caller-supplied
       ``discover_filesystem`` callable. First strategy whose
       ``strategy_id`` matches ``strategy_id`` wins.
    3. **Paper session discovery** via the caller-supplied
       ``discover_paper_sessions`` callable. Matches against either
       ``original_strategy_id`` OR ``strategy_id``, because
       ``resolve_agent_id`` may rewrite ``paper:xxx`` IDs.

    The ``original_strategy_id`` vs resolved ``strategy_id`` distinction
    is load-bearing for paper-session matching and must be preserved —
    do not collapse.

    ``registry_getter`` is passed in (rather than imported here) so
    ``unittest.mock.patch`` on the caller's module-scope
    ``get_instance_registry`` symbol continues to intercept calls —
    existing characterization tests rely on that patch target.

    Args:
        strategy_id: Resolved strategy ID (post ``resolve_agent_id``).
        original_strategy_id: Original client-supplied strategy ID
            (pre-resolution) — used for paper-session matching.
        registry_getter: Zero-arg callable returning an instance
            registry with a ``.get(strategy_id)`` method.
        compute_effective_status: Callable ``(inst) -> str`` that
            returns the effective status for a registry instance.
        discover_filesystem: Callable ``() -> list[dict]`` returning
            filesystem-discovered strategy info dicts.
        discover_paper_sessions: Callable ``() -> list[dict]`` returning
            paper-session info dicts.

    Returns:
        The ``strategy_info`` dict when a source matches, else ``None``.
    """
    # Registry first
    try:
        registry = registry_getter()
        inst = registry.get(strategy_id)
        if inst is not None:
            effective_status = compute_effective_status(inst)
            return build_registry_strategy_info(inst, effective_status)
    except Exception as e:
        logger.debug(f"Failed to check registry for {strategy_id}: {e}")

    # Fallback to filesystem discovery
    for s in discover_filesystem():
        if s["strategy_id"] == strategy_id:
            return s

    # Fallback to paper session discovery (match against original ID
    # because resolve_agent_id may have rewritten paper:xxx IDs)
    for s in discover_paper_sessions():
        if s["strategy_id"] == original_strategy_id or s["strategy_id"] == strategy_id:
            return s

    return None
