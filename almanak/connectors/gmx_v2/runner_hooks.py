"""GMX V2 asynchronous-order lifecycle hooks for the strategy runner."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from web3.types import RPCEndpoint

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    AsyncSettlementPolicy,
    AsyncSettlementStatus,
    AsyncSettlementVerdict,
    RunnerAsyncSettlementCapability,
    RunnerHookConnector,
)
from almanak.connectors.gmx_v2.teardown_reads import read_open_positions, read_pending_orders
from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

logger = logging.getLogger(__name__)

_PROTOCOL = ProtocolName("gmx_v2")
_USD_SCALE = Decimal(10**30)
_PositionKey = tuple[str, str, bool]


@dataclass(frozen=True)
class _GmxSettlementBaseline:
    """Measured position sizes while every submitted order is still pending."""

    position_sizes: tuple[tuple[_PositionKey, int], ...]

    def as_dict(self) -> dict[_PositionKey, int]:
        return dict(self.position_sizes)


def _normalize_address(value: Any) -> str | None:
    address = str(value or "").lower()
    if len(address) != 42 or not address.startswith("0x"):
        return None
    try:
        return address if int(address, 16) != 0 else None
    except ValueError:
        return None


def _requested_position_deltas(orders: tuple[Any, ...]) -> dict[_PositionKey, int] | None:
    """Return exact GMX target -> requested raw USD delta, or None if unmeasured."""
    targets: dict[_PositionKey, int] = {}
    for order in orders:
        market = _normalize_address(getattr(order, "market", None))
        collateral = _normalize_address(getattr(order, "collateral_token", None))
        is_long = getattr(order, "is_long", None)
        try:
            size_delta = Decimal(str(getattr(order, "size_delta_usd", None)))
        except (InvalidOperation, ValueError):
            return None
        if market is None or collateral is None or not isinstance(is_long, bool) or size_delta <= 0:
            return None
        key = (market, collateral, is_long)
        targets[key] = targets.get(key, 0) + int(size_delta * _USD_SCALE)
    return targets or None


def _active_position_sizes(positions: Any) -> dict[_PositionKey, int]:
    sizes: dict[_PositionKey, int] = {}
    for position in positions.positions:
        if not getattr(position, "is_active", False):
            continue
        market = _normalize_address(getattr(position, "market", None))
        collateral = _normalize_address(getattr(position, "collateral_token", None))
        is_long = getattr(position, "is_long", None)
        if market is None or collateral is None or not isinstance(is_long, bool):
            continue
        sizes[(market, collateral, is_long)] = int(getattr(position, "size_in_usd", 0) or 0)
    return sizes


def _position_delta_reached(
    intent_type: str,
    requested: dict[_PositionKey, int],
    baseline: _GmxSettlementBaseline,
    current: dict[_PositionKey, int],
) -> bool:
    before = baseline.as_dict()
    if intent_type == "PERP_OPEN":
        return all(current.get(target, 0) >= before.get(target, 0) + delta for target, delta in requested.items())
    if intent_type == "PERP_CLOSE":
        return all(
            before.get(target, 0) > 0 and current.get(target, 0) <= max(0, before[target] - delta)
            for target, delta in requested.items()
        )
    return False


def _order_verdict_rows(
    requested_keys: set[str],
    status: AsyncSettlementStatus,
) -> tuple[dict[str, str], ...]:
    return tuple(
        {
            "protocol": str(_PROTOCOL),
            "order_id": key,
            "status": status.value,
        }
        for key in sorted(requested_keys)
        if key
    )


def _observation_failed(reason: str, observation_state: Any = None) -> AsyncSettlementVerdict:
    return AsyncSettlementVerdict(
        status=AsyncSettlementStatus.OBSERVATION_FAILED,
        terminal=False,
        reason=reason,
        observation_state=observation_state,
    )


def _pending_order_keys(pending: Any) -> set[str]:
    keys = {str(key).lower() for key in pending.order_keys}
    keys.update(str(order.order_key).lower() for order in pending.orders if order.order_key)
    return keys


def _pending_verdict(
    requested_keys: set[str],
    *,
    reason: str,
    observation_state: _GmxSettlementBaseline,
) -> AsyncSettlementVerdict:
    return AsyncSettlementVerdict(
        status=AsyncSettlementStatus.PENDING,
        terminal=False,
        orders=_order_verdict_rows(requested_keys, AsyncSettlementStatus.PENDING),
        reason=reason,
        observation_state=observation_state,
    )


def _capture_settlement_baseline(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    requested_keys: set[str],
) -> AsyncSettlementVerdict:
    positions = read_open_positions(gateway_client, chain, wallet_address)
    if not getattr(positions, "ok", False):
        return _observation_failed("GMX position baseline was unmeasured while the order was pending")

    pending_after_baseline = read_pending_orders(gateway_client, chain, wallet_address)
    if not pending_after_baseline.ok or not requested_keys.issubset(_pending_order_keys(pending_after_baseline)):
        return _observation_failed("GMX order changed state while its position baseline was being measured")

    baseline = _GmxSettlementBaseline(tuple(sorted(_active_position_sizes(positions).items())))
    return _pending_verdict(
        requested_keys,
        reason="GMX order remains pending; exact target position baseline captured",
        observation_state=baseline,
    )


def _final_position_verdict(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    requested_keys: set[str],
    requested_deltas: dict[_PositionKey, int],
    intent: Any,
    baseline: _GmxSettlementBaseline,
) -> AsyncSettlementVerdict:
    positions = read_open_positions(gateway_client, chain, wallet_address)
    if not getattr(positions, "ok", False):
        return _observation_failed(
            "GMX position read was unmeasured after the order left the pending set",
            baseline,
        )

    intent_type = getattr(getattr(intent, "intent_type", None), "value", "")
    target_reached = _position_delta_reached(
        intent_type,
        requested_deltas,
        baseline,
        _active_position_sizes(positions),
    )
    status = AsyncSettlementStatus.SETTLED if target_reached else AsyncSettlementStatus.TERMINAL_FAILED
    return AsyncSettlementVerdict(
        status=status,
        terminal=True,
        orders=_order_verdict_rows(requested_keys, status),
        reason=None if target_reached else "GMX order left the pending set without its exact target position delta",
        observation_state=baseline,
    )


class GmxV2RunnerHookConnector(RunnerHookConnector, RunnerAsyncSettlementCapability):
    """Observe GMX keeper settlement and advance its cancel gate on Anvil."""

    protocol: ClassVar[ProtocolName] = _PROTOCOL
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def async_settlement_policy(self) -> AsyncSettlementPolicy:
        """Return the lifecycle test policy declared by ALM-2972."""
        return AsyncSettlementPolicy(
            timeout_seconds=360,
            poll_interval_seconds=5,
            supports_local_order_execution=False,
            supports_cancellation=True,
        )

    def observe_async_orders(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        orders: tuple[Any, ...],
        intent: Any,
        observation_state: Any = None,
    ) -> AsyncSettlementVerdict:
        """Measure whether the submitted order reached the intent's target state."""
        requested_keys = {str(getattr(order, "order_id", "") or "").lower() for order in orders}
        requested_deltas = _requested_position_deltas(orders)
        if not requested_keys or "" in requested_keys or requested_deltas is None:
            return _observation_failed(
                "GMX order target identity or size delta was unmeasured",
                observation_state,
            )

        pending = read_pending_orders(gateway_client, chain, wallet_address)
        if not pending.ok:
            return _observation_failed(pending.error or "GMX pending-order read was unmeasured", observation_state)

        still_pending = requested_keys.intersection(_pending_order_keys(pending))
        if pending.truncated and len(still_pending) != len(requested_keys):
            return _observation_failed(
                "GMX pending-order set was truncated; order absence was not measurable",
                observation_state,
            )

        if observation_state is None:
            if len(still_pending) != len(requested_keys):
                return _observation_failed(
                    "GMX order left the pending set before a position baseline could be measured"
                )
            return _capture_settlement_baseline(
                gateway_client=gateway_client,
                chain=chain,
                wallet_address=wallet_address,
                requested_keys=requested_keys,
            )

        if not isinstance(observation_state, _GmxSettlementBaseline):
            return _observation_failed("GMX settlement baseline had an invalid connector-private shape")
        if still_pending:
            return _pending_verdict(
                requested_keys,
                reason="GMX order remains in the account pending-order set",
                observation_state=observation_state,
            )

        return _final_position_verdict(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            requested_keys=requested_keys,
            requested_deltas=requested_deltas,
            intent=intent,
            baseline=observation_state,
        )

    def prepare_pending_orders_for_teardown(
        self,
        *,
        gateway_client: Any,
        chain: str,
        wallet_address: str,
        residuals: tuple[Any, ...],
        network: str,
    ) -> bool:
        """Advance the current managed Anvil session to measured cancel eligibility."""
        del wallet_address
        if str(network or "").lower() != "anvil" or gateway_client is None:
            return False

        waits = [(getattr(residual, "details", None) or {}).get("seconds_until_cancellable") for residual in residuals]
        seconds = max((wait for wait in waits if isinstance(wait, int) and wait > 0), default=None)
        if seconds is None:
            return False

        provider = GatewayWeb3Provider(gateway_client, chain=chain)
        advance = provider.make_request(RPCEndpoint("evm_increaseTime"), [seconds])
        if advance.get("error"):
            logger.warning("GMX teardown could not advance Anvil time: %s", advance["error"])
            return False
        mined = provider.make_request(RPCEndpoint("evm_mine"), [])
        if mined.get("error"):
            logger.warning("GMX teardown could not mine after advancing Anvil time: %s", mined["error"])
            return False
        logger.info("GMX teardown advanced the managed Anvil session by %ds to cancel eligibility", seconds)
        return True


__all__ = ["GmxV2RunnerHookConnector"]
