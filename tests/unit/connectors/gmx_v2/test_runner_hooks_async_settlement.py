"""GMX V2 runner-hook coverage for asynchronous order settlement."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.runner_hook_registry import AsyncSettlementStatus
from almanak.connectors.gmx_v2.runner_hooks import GmxV2RunnerHookConnector

_KEY = "0x" + "ab" * 32
_MARKET = "0x" + "11" * 20
_OTHER_MARKET = "0x" + "22" * 20
_COLLATERAL = "0x" + "33" * 20
_RAW_USD = 10**30


def _order(*, size_delta_usd: str = "100") -> SimpleNamespace:
    return SimpleNamespace(
        order_id=_KEY,
        market=_MARKET,
        collateral_token=_COLLATERAL,
        is_long=False,
        size_delta_usd=Decimal(size_delta_usd),
    )


def _intent(intent_type: str) -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value=intent_type))


def _position(*, market: str = _MARKET, size_usd: int = 100 * _RAW_USD) -> SimpleNamespace:
    return SimpleNamespace(
        is_active=size_usd > 0,
        market=market,
        collateral_token=_COLLATERAL,
        is_long=False,
        size_in_usd=size_usd,
    )


def test_policy_declares_live_keeper_wait_and_no_local_execution() -> None:
    policy = GmxV2RunnerHookConnector().async_settlement_policy()

    assert policy.timeout_seconds == 360
    assert policy.poll_interval_seconds == 5
    assert policy.supports_local_order_execution is False
    assert policy.supports_cancellation is True


def test_observer_keeps_exact_authoritative_key_pending_and_captures_baseline() -> None:
    pending = SimpleNamespace(ok=True, order_keys=[_KEY], orders=[], truncated=False)
    positions = SimpleNamespace(ok=True, positions=(_position(size_usd=200 * _RAW_USD),))
    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=pending),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", return_value=positions),
    ):
        verdict = GmxV2RunnerHookConnector().observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
        )

    assert verdict.status == AsyncSettlementStatus.PENDING
    assert verdict.terminal is False
    assert verdict.orders[0]["order_id"] == _KEY
    assert verdict.observation_state is not None


def test_open_settles_only_after_exact_target_grows_by_requested_delta() -> None:
    pending = SimpleNamespace(ok=True, order_keys=[_KEY], orders=[], truncated=False)
    removed = SimpleNamespace(ok=True, order_keys=[], orders=[], truncated=False)
    before = SimpleNamespace(ok=True, positions=(_position(size_usd=200 * _RAW_USD),))
    after = SimpleNamespace(ok=True, positions=(_position(size_usd=300 * _RAW_USD),))
    connector = GmxV2RunnerHookConnector()
    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", side_effect=[pending, pending, removed]),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", side_effect=[before, after]),
    ):
        baseline = connector.observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
        )
        verdict = connector.observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
            observation_state=baseline.observation_state,
        )

    assert verdict.status == AsyncSettlementStatus.SETTLED
    assert verdict.terminal is True


def test_close_settles_when_exact_target_closes_even_if_unrelated_position_remains() -> None:
    pending = SimpleNamespace(ok=True, order_keys=[_KEY], orders=[], truncated=False)
    removed = SimpleNamespace(ok=True, order_keys=[], orders=[], truncated=False)
    before = SimpleNamespace(
        ok=True,
        positions=(
            _position(size_usd=100 * _RAW_USD),
            _position(market=_OTHER_MARKET, size_usd=50 * _RAW_USD),
        ),
    )
    after = SimpleNamespace(ok=True, positions=(_position(market=_OTHER_MARKET, size_usd=50 * _RAW_USD),))
    connector = GmxV2RunnerHookConnector()
    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", side_effect=[pending, pending, removed]),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", side_effect=[before, after]),
    ):
        baseline = connector.observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
        )
        verdict = connector.observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
            observation_state=baseline.observation_state,
        )

    assert verdict.status == AsyncSettlementStatus.SETTLED
    assert verdict.terminal is True


def test_cancelled_open_does_not_settle_against_preexisting_position() -> None:
    pending = SimpleNamespace(ok=True, order_keys=[_KEY], orders=[], truncated=False)
    removed = SimpleNamespace(ok=True, order_keys=[], orders=[], truncated=False)
    unchanged = SimpleNamespace(ok=True, positions=(_position(size_usd=200 * _RAW_USD),))
    connector = GmxV2RunnerHookConnector()
    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", side_effect=[pending, pending, removed]),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", side_effect=[unchanged, unchanged]),
    ):
        baseline = connector.observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
        )
        verdict = connector.observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
            observation_state=baseline.observation_state,
        )

    assert verdict.status == AsyncSettlementStatus.TERMINAL_FAILED
    assert verdict.terminal is True
    assert "exact target position delta" in (verdict.reason or "")


def test_removed_order_before_baseline_is_observation_failed() -> None:
    removed = SimpleNamespace(ok=True, order_keys=[], orders=[], truncated=False)
    with patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=removed):
        verdict = GmxV2RunnerHookConnector().observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
        )

    assert verdict.status == AsyncSettlementStatus.OBSERVATION_FAILED
    assert verdict.terminal is False
    assert "before a position baseline" in (verdict.reason or "")


def test_order_state_change_during_baseline_capture_fails_closed() -> None:
    pending = SimpleNamespace(ok=True, order_keys=[_KEY], orders=[], truncated=False)
    removed = SimpleNamespace(ok=True, order_keys=[], orders=[], truncated=False)
    positions = SimpleNamespace(ok=True, positions=(_position(size_usd=300 * _RAW_USD),))
    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", side_effect=[pending, removed]),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", return_value=positions),
    ):
        verdict = GmxV2RunnerHookConnector().observe_async_orders(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
        )

    assert verdict.status == AsyncSettlementStatus.OBSERVATION_FAILED
    assert verdict.terminal is False
    assert "changed state" in (verdict.reason or "")


def test_teardown_advances_same_anvil_session_by_measured_wait() -> None:
    provider = MagicMock()
    provider.make_request.side_effect = [{"result": 210}, {"result": "0x0"}]
    residuals = (
        SimpleNamespace(details={"seconds_until_cancellable": 90}),
        SimpleNamespace(details={"seconds_until_cancellable": 210}),
    )

    with patch("almanak.connectors.gmx_v2.runner_hooks.GatewayWeb3Provider", return_value=provider):
        prepared = GmxV2RunnerHookConnector().prepare_pending_orders_for_teardown(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            residuals=residuals,
            network="anvil",
        )

    assert prepared is True
    assert provider.make_request.call_args_list[0].args == ("evm_increaseTime", [210])
    assert provider.make_request.call_args_list[1].args == ("evm_mine", [])


def test_teardown_never_advances_time_outside_anvil() -> None:
    with patch("almanak.connectors.gmx_v2.runner_hooks.GatewayWeb3Provider") as provider:
        prepared = GmxV2RunnerHookConnector().prepare_pending_orders_for_teardown(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            residuals=(SimpleNamespace(details={"seconds_until_cancellable": 210}),),
            network="mainnet",
        )

    assert prepared is False
    provider.assert_not_called()
