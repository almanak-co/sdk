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
    receipts: tuple[dict[str, Any], ...] = ()
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


@dataclass
class _SettlementPollState:
    """Mutable evidence accumulated across local execution and observation."""

    started: float
    attempts: int = 0
    observation_state: Any = None
    receipts: tuple[dict[str, Any], ...] = ()

    def absorb(self, verdict: AsyncSettlementVerdict) -> None:
        self.observation_state = verdict.observation_state
        if verdict.receipts:
            self.receipts += tuple(receipt for receipt in verdict.receipts if receipt not in self.receipts)

    def result(
        self,
        *,
        status: AsyncSettlementStatus,
        terminal: bool,
        orders: tuple[dict[str, Any], ...],
        reason: str | None,
    ) -> AsyncSettlementBarrierResult:
        return AsyncSettlementBarrierResult(
            status=status,
            terminal=terminal,
            attempts=self.attempts,
            elapsed_seconds=_monotonic() - self.started,
            orders=orders,
            receipts=self.receipts,
            reason=reason,
        )


def _failure_result(
    *,
    state: _SettlementPollState,
    orders: tuple[Any, ...],
    status: AsyncSettlementStatus,
    reason: str,
) -> AsyncSettlementBarrierResult:
    return state.result(
        status=status,
        terminal=False,
        orders=_submitted_orders(orders, status),
        reason=reason,
    )


def _owning_protocol(orders: tuple[Any, ...]) -> ProtocolName | None:
    protocols = {str(getattr(order, "protocol", "") or "").lower() for order in orders}
    if len(protocols) != 1 or "" in protocols:
        return None
    return ProtocolName(next(iter(protocols)))


async def _execute_pending_orders_on_anvil(
    *,
    registry: Any,
    state: _SettlementPollState,
    protocol: ProtocolName,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    orders: tuple[Any, ...],
    intent: Any,
    network: str,
    deadline: float,
    poll_interval: int,
) -> AsyncSettlementBarrierResult | None:
    while True:
        if state.observation_state is not None:
            # A prior attempt already captured the pre-execution baseline. A
            # retry must observe FIRST: if the transient failure hit after the
            # keeper transaction landed, the order has left the pending set
            # and re-executing can never re-capture a baseline — but the
            # carried baseline can still conclude the fill. Only a
            # still-PENDING order needs another execution attempt.
            state.attempts += 1
            observed = await asyncio.to_thread(
                registry.observe_async_orders,
                protocol=protocol,
                gateway_client=gateway_client,
                chain=chain,
                wallet_address=wallet_address,
                orders=orders,
                intent=intent,
                observation_state=state.observation_state,
            )
            if observed is not None:
                state.absorb(observed)
                if observed.terminal:
                    return state.result(
                        status=observed.status,
                        terminal=True,
                        orders=observed.orders or _submitted_orders(orders, observed.status),
                        reason=observed.reason,
                    )
                if observed.status is AsyncSettlementStatus.OBSERVATION_FAILED:
                    remaining = deadline - _monotonic()
                    if remaining > 0:
                        await asyncio.sleep(min(poll_interval, remaining))
                        continue
                    return state.result(
                        status=AsyncSettlementStatus.OBSERVATION_FAILED,
                        terminal=False,
                        orders=observed.orders or _submitted_orders(orders, AsyncSettlementStatus.OBSERVATION_FAILED),
                        reason=observed.reason,
                    )
                # PENDING: the order is still in the vault — execute below.

        state.attempts += 1
        verdict = await asyncio.to_thread(
            registry.execute_pending_orders_for_test,
            protocol=protocol,
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            intent=intent,
            network=network,
        )
        if verdict is None:
            return _failure_result(
                state=state,
                orders=orders,
                status=AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
                reason=f"Connector {protocol} returned no managed-fork execution verdict",
            )

        state.absorb(verdict)
        if verdict.status is AsyncSettlementStatus.OBSERVATION_FAILED and not verdict.terminal:
            # A transient measurement blip (cold-fork RPC latency, indexer lag
            # on the baseline/pending reads) must not abort keeper execution
            # that the policy's timeout budget can still absorb. Retry within
            # the shared deadline; a failure that persists past it falls
            # through to the fail-closed return below. Structural
            # INFRASTRUCTURE_UNSUPPORTED stays immediate — waiting cannot
            # conjure a keeper.
            remaining = deadline - _monotonic()
            if remaining > 0:
                await asyncio.sleep(min(poll_interval, remaining))
                continue
        if verdict.terminal or verdict.status in {
            AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            AsyncSettlementStatus.OBSERVATION_FAILED,
        }:
            return state.result(
                status=verdict.status,
                terminal=verdict.terminal,
                orders=verdict.orders or _submitted_orders(orders, verdict.status),
                reason=verdict.reason,
            )
        return None


async def _poll_until_settled(
    *,
    registry: Any,
    state: _SettlementPollState,
    protocol: ProtocolName,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    orders: tuple[Any, ...],
    intent: Any,
    deadline: float,
    poll_interval: int,
) -> AsyncSettlementBarrierResult:
    while True:
        state.attempts += 1
        verdict = await asyncio.to_thread(
            registry.observe_async_orders,
            protocol=protocol,
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            intent=intent,
            observation_state=state.observation_state,
        )
        if verdict is None:
            verdict = AsyncSettlementVerdict(
                status=AsyncSettlementStatus.OBSERVATION_FAILED,
                terminal=False,
                reason=f"Connector {protocol} returned no async settlement verdict",
                observation_state=state.observation_state,
            )
        state.absorb(verdict)
        if verdict.terminal:
            return state.result(
                status=verdict.status,
                terminal=True,
                orders=verdict.orders,
                reason=verdict.reason,
            )

        remaining = deadline - _monotonic()
        if remaining <= 0:
            status = (
                AsyncSettlementStatus.OBSERVATION_FAILED
                if verdict.status is AsyncSettlementStatus.OBSERVATION_FAILED
                else AsyncSettlementStatus.PENDING_SETTLEMENT_TIMEOUT
            )
            return state.result(
                status=status,
                terminal=False,
                orders=verdict.orders or _submitted_orders(orders, status),
                reason=verdict.reason or f"Connector {protocol} did not reach terminal settlement before timeout",
            )
        await asyncio.sleep(min(poll_interval, remaining))


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
    state = _SettlementPollState(started=started)
    if not orders:
        return AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.SETTLED,
            terminal=True,
            attempts=0,
            elapsed_seconds=0,
        )

    protocol = _owning_protocol(orders)
    if protocol is None:
        return _failure_result(
            state=state,
            orders=orders,
            status=AsyncSettlementStatus.OBSERVATION_FAILED,
            reason="Async settlement barrier requires one measured owning protocol per execution result",
        )

    from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY

    policy = STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy(protocol)
    if policy is None:
        return _failure_result(
            state=state,
            orders=orders,
            status=AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            reason=f"Connector {protocol} exposes async orders but no settlement observer",
        )
    if str(network or "").lower() == "anvil" and not policy.supports_local_order_execution:
        return _failure_result(
            state=state,
            orders=orders,
            status=AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            reason=f"Connector {protocol} cannot execute keeper-settled orders on managed Anvil",
        )

    timeout = timeout_seconds if timeout_seconds is not None else policy.timeout_seconds
    poll_interval = poll_interval_seconds if poll_interval_seconds is not None else policy.poll_interval_seconds
    if timeout <= 0 or poll_interval <= 0:
        return _failure_result(
            state=state,
            orders=orders,
            status=AsyncSettlementStatus.OBSERVATION_FAILED,
            reason="Async settlement timeout and poll interval must be positive",
        )

    deadline = started + timeout
    if str(network or "").lower() == "anvil":
        immediate_result = await _execute_pending_orders_on_anvil(
            registry=STRATEGY_RUNNER_HOOK_REGISTRY,
            state=state,
            protocol=protocol,
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            orders=orders,
            intent=intent,
            network=network,
            deadline=deadline,
            poll_interval=poll_interval,
        )
        if immediate_result is not None:
            return immediate_result

    return await _poll_until_settled(
        registry=STRATEGY_RUNNER_HOOK_REGISTRY,
        state=state,
        protocol=protocol,
        gateway_client=gateway_client,
        chain=chain,
        wallet_address=wallet_address,
        orders=orders,
        intent=intent,
        deadline=deadline,
        poll_interval=poll_interval,
    )


__all__ = ["AsyncSettlementBarrierResult", "await_async_settlement"]
