"""Protocol-neutral settlement barrier for asynchronous execution results."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from almanak.connectors._base.types import ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    AsyncSettlementStatus,
    AsyncSettlementVerdict,
)

_monotonic = time.monotonic


@dataclass(frozen=True)
class AsyncSettlementBarrierResult:
    """Aggregate result returned to the lifecycle runner."""

    status: AsyncSettlementStatus
    terminal: bool
    attempts: int
    elapsed_seconds: float
    orders: tuple[dict[str, Any], ...] = ()
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "terminal": self.terminal,
            "attempts": self.attempts,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "orders": [dict(order) for order in self.orders],
            "reason": self.reason,
        }


def _submitted_orders(orders: tuple[Any, ...], status: AsyncSettlementStatus) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "protocol": str(getattr(order, "protocol", "") or ""),
            "order_id": str(getattr(order, "order_id", "") or ""),
            "kind": str(getattr(getattr(order, "kind", None), "value", getattr(order, "kind", "")) or ""),
            "status": status.value,
        }
        for order in orders
    )


async def await_async_settlement(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    network: str,
    orders: tuple[Any, ...],
    intent: Any,
    timeout_seconds: int | None = None,
    poll_interval_seconds: int | None = None,
) -> AsyncSettlementBarrierResult:
    """Wait until connector-observed async orders reach a terminal state.

    Managed Anvil fails immediately with ``INFRASTRUCTURE_UNSUPPORTED`` when
    the connector cannot execute orders locally. This avoids spending the full
    live-settlement timeout on a fork where no keeper can ever arrive.
    """
    started = _monotonic()
    if not orders:
        return AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.SETTLED,
            terminal=True,
            attempts=0,
            elapsed_seconds=0,
        )

    protocols = {str(getattr(order, "protocol", "") or "").lower() for order in orders}
    if len(protocols) != 1 or "" in protocols:
        return AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.OBSERVATION_FAILED,
            terminal=False,
            attempts=0,
            elapsed_seconds=_monotonic() - started,
            orders=_submitted_orders(orders, AsyncSettlementStatus.OBSERVATION_FAILED),
            reason="Async settlement barrier requires one measured owning protocol per execution result",
        )
    protocol = ProtocolName(next(iter(protocols)))

    from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY

    policy = STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy(protocol)
    if policy is None:
        return AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            terminal=False,
            attempts=0,
            elapsed_seconds=_monotonic() - started,
            orders=_submitted_orders(orders, AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED),
            reason=f"Connector {protocol} exposes async orders but no settlement observer",
        )
    if str(network or "").lower() == "anvil" and not policy.supports_local_order_execution:
        return AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            terminal=False,
            attempts=0,
            elapsed_seconds=_monotonic() - started,
            orders=_submitted_orders(orders, AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED),
            reason=f"Connector {protocol} cannot execute keeper-settled orders on managed Anvil",
        )

    timeout = timeout_seconds if timeout_seconds is not None else policy.timeout_seconds
    poll_interval = poll_interval_seconds if poll_interval_seconds is not None else policy.poll_interval_seconds
    if timeout <= 0 or poll_interval <= 0:
        return AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.OBSERVATION_FAILED,
            terminal=False,
            attempts=0,
            elapsed_seconds=_monotonic() - started,
            orders=_submitted_orders(orders, AsyncSettlementStatus.OBSERVATION_FAILED),
            reason="Async settlement timeout and poll interval must be positive",
        )

    attempts = 0
    deadline = started + timeout
    last: AsyncSettlementVerdict | None = None
    observation_state: Any = None
    while True:
        attempts += 1
        last = await asyncio.to_thread(
            STRATEGY_RUNNER_HOOK_REGISTRY.observe_async_orders,
            protocol=protocol,
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            intent=intent,
            observation_state=observation_state,
        )
        if last is None:
            last = AsyncSettlementVerdict(
                status=AsyncSettlementStatus.OBSERVATION_FAILED,
                terminal=False,
                reason=f"Connector {protocol} returned no async settlement verdict",
                observation_state=observation_state,
            )
        observation_state = last.observation_state
        if last.terminal:
            return AsyncSettlementBarrierResult(
                status=last.status,
                terminal=True,
                attempts=attempts,
                elapsed_seconds=_monotonic() - started,
                orders=last.orders,
                reason=last.reason,
            )

        remaining = deadline - _monotonic()
        if remaining <= 0:
            status = (
                AsyncSettlementStatus.OBSERVATION_FAILED
                if last.status is AsyncSettlementStatus.OBSERVATION_FAILED
                else AsyncSettlementStatus.PENDING_SETTLEMENT_TIMEOUT
            )
            return AsyncSettlementBarrierResult(
                status=status,
                terminal=False,
                attempts=attempts,
                elapsed_seconds=_monotonic() - started,
                orders=last.orders or _submitted_orders(orders, status),
                reason=last.reason or f"Connector {protocol} did not reach terminal settlement before timeout",
            )
        await asyncio.sleep(min(poll_interval, remaining))


__all__ = ["AsyncSettlementBarrierResult", "await_async_settlement"]
