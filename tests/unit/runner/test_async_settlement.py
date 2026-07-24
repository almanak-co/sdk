"""Protocol-neutral asynchronous settlement barrier tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors._strategy_base.runner_hook_registry import (
    AsyncSettlementPolicy,
    AsyncSettlementStatus,
    AsyncSettlementVerdict,
)
from almanak.framework.runner.async_settlement import await_async_settlement

_KEY = "0x" + "cd" * 32


def _order(protocol: str = "gmx_v2") -> SimpleNamespace:
    return SimpleNamespace(protocol=protocol, order_id=_KEY, kind=SimpleNamespace(value="INCREASE"))


@pytest.mark.asyncio
async def test_no_async_orders_are_already_settled() -> None:
    result = await await_async_settlement(
        gateway_client=object(),
        chain="arbitrum",
        wallet_address="0xabc",
        network="anvil",
        orders=(),
        intent=object(),
    )

    assert result.status == AsyncSettlementStatus.SETTLED
    assert result.terminal is True
    assert result.attempts == 0


@pytest.mark.asyncio
async def test_missing_connector_capability_fails_as_infrastructure_unsupported() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = None
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
        registry,
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order("unknown"),),
            intent=object(),
        )

    assert result.status == AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED
    assert result.terminal is False
    registry.observe_async_orders.assert_not_called()


@pytest.mark.asyncio
async def test_managed_anvil_fails_immediately_when_connector_has_no_keeper_simulator() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, False, True)
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
        registry,
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="anvil",
            orders=(_order(),),
            intent=object(),
        )

    assert result.status == AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED
    assert result.attempts == 0
    registry.observe_async_orders.assert_not_called()


@pytest.mark.asyncio
async def test_managed_anvil_executes_exact_order_before_polling() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, True, True)
    registry.execute_pending_orders_for_test.return_value = AsyncSettlementVerdict(
        status=AsyncSettlementStatus.SETTLED,
        terminal=True,
        orders=({"protocol": "gmx_v2", "order_id": _KEY, "status": "SETTLED"},),
    )
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
        registry,
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="anvil",
            orders=(_order(),),
            intent=object(),
        )

    assert result.status == AsyncSettlementStatus.SETTLED
    assert result.terminal is True
    assert result.attempts == 1
    assert registry.execute_pending_orders_for_test.call_args.kwargs["orders"] == (_order(),)
    registry.observe_async_orders.assert_not_called()


@pytest.mark.asyncio
async def test_live_observer_terminal_verdict_completes_barrier() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, False, True)
    registry.observe_async_orders.return_value = AsyncSettlementVerdict(
        status=AsyncSettlementStatus.SETTLED,
        terminal=True,
        orders=({"protocol": "gmx_v2", "order_id": _KEY, "status": "SETTLED"},),
    )
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
        registry,
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=object(),
        )

    assert result.status == AsyncSettlementStatus.SETTLED
    assert result.terminal is True
    assert result.attempts == 1


@pytest.mark.asyncio
async def test_barrier_carries_connector_private_observation_state_between_polls() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, False, True)
    baseline = object()
    registry.observe_async_orders.side_effect = [
        AsyncSettlementVerdict(
            status=AsyncSettlementStatus.PENDING,
            terminal=False,
            observation_state=baseline,
        ),
        AsyncSettlementVerdict(
            status=AsyncSettlementStatus.SETTLED,
            terminal=True,
            observation_state=baseline,
        ),
    ]
    with (
        patch(
            "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
            registry,
        ),
        patch("almanak.framework.runner.async_settlement._monotonic", side_effect=[0.0, 0.0, 0.5]),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", new=AsyncMock()),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=object(),
            timeout_seconds=1,
            poll_interval_seconds=1,
        )

    assert result.status == AsyncSettlementStatus.SETTLED
    assert result.attempts == 2
    assert registry.observe_async_orders.call_args_list[0].kwargs["observation_state"] is None
    assert registry.observe_async_orders.call_args_list[1].kwargs["observation_state"] is baseline


@pytest.mark.asyncio
async def test_non_terminal_verdict_becomes_bounded_timeout() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, False, True)
    registry.observe_async_orders.return_value = AsyncSettlementVerdict(
        status=AsyncSettlementStatus.PENDING,
        terminal=False,
        reason="order remains pending",
    )
    with (
        patch(
            "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
            registry,
        ),
        patch("almanak.framework.runner.async_settlement._monotonic", side_effect=[0.0, 1.0, 1.0]),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=object(),
            timeout_seconds=1,
            poll_interval_seconds=1,
        )

    assert result.status == AsyncSettlementStatus.PENDING_SETTLEMENT_TIMEOUT
    assert result.terminal is False
    assert result.attempts == 1
    assert result.reason == "order remains pending"


@pytest.mark.asyncio
async def test_repeated_unmeasured_observation_retains_observation_failed_category() -> None:
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, False, True)
    registry.observe_async_orders.return_value = AsyncSettlementVerdict(
        status=AsyncSettlementStatus.OBSERVATION_FAILED,
        terminal=False,
        reason="gateway read was unmeasured",
    )
    with (
        patch(
            "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
            registry,
        ),
        patch("almanak.framework.runner.async_settlement._monotonic", side_effect=[0.0, 1.0, 1.0]),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=object(),
            timeout_seconds=1,
            poll_interval_seconds=1,
        )

    assert result.status == AsyncSettlementStatus.OBSERVATION_FAILED
    assert result.terminal is False
    assert result.reason == "gateway read was unmeasured"
