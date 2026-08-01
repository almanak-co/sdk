"""VIB-6254 teardown runner-helper settlement wiring."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors._strategy_base.runner_hook_registry import AsyncSettlementStatus
from almanak.framework.runner.async_settlement import AsyncSettlementBarrierResult
from almanak.framework.runner.perp_settlement_reconciler import PerpSettlementReconcileOutcome
from almanak.framework.teardown.runner_helpers import (
    SettlementPreparation,
    _async_settlement_enrichment_failure,
    build_runner_helpers,
)


def test_async_settlement_enrichment_failure_ignores_intents_without_protocol() -> None:
    assert _async_settlement_enrichment_failure(SimpleNamespace(), RuntimeError("boom")) is None


def test_async_settlement_enrichment_failure_ignores_unknown_protocol() -> None:
    assert (
        _async_settlement_enrichment_failure(SimpleNamespace(protocol="not_a_protocol"), RuntimeError("boom")) is None
    )


def test_async_settlement_enrichment_failure_ignores_sync_connector() -> None:
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy",
        return_value=None,
    ):
        assert (
            _async_settlement_enrichment_failure(SimpleNamespace(protocol="uniswap_v3"), RuntimeError("boom")) is None
        )


def test_async_settlement_enrichment_failure_fails_closed_when_policy_lookup_raises() -> None:
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy",
        side_effect=RuntimeError("registry unavailable"),
    ):
        error = _async_settlement_enrichment_failure(
            SimpleNamespace(protocol="gmx_v2"),
            RuntimeError("enrichment failed"),
        )

    assert error is not None
    assert "capability lookup failed" in error
    assert "refusing to retry" in error


def test_async_settlement_enrichment_failure_fails_closed_for_async_connector() -> None:
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy",
        return_value=object(),
    ):
        error = _async_settlement_enrichment_failure(
            SimpleNamespace(protocol="gmx_v2"),
            RuntimeError("enrichment failed"),
        )

    assert error is not None
    assert "gmx_v2" in error
    assert "enrichment failed" in error


@pytest.mark.parametrize("intent", [SimpleNamespace(protocol="gmx_v2"), {"protocol": "gmx_v2"}])
def test_prepare_fails_closed_when_async_connector_emits_no_order_identity(intent: object) -> None:
    runner = SimpleNamespace(
        _is_live_mode=MagicMock(return_value=True),
        _build_pool_key_lookup=MagicMock(return_value=None),
        _build_curve_pool_meta_lookup=MagicMock(return_value=None),
    )
    with (
        patch("almanak.framework.execution.result_enricher.ResultEnricher") as enricher_cls,
        patch(
            "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY.async_settlement_policy",
            return_value=object(),
        ),
    ):
        enricher_cls.return_value.enrich.return_value = SimpleNamespace(async_orders=[])
        prepare = build_runner_helpers(runner).prepare_intent_settlement
        assert prepare is not None
        preparation = prepare(
            SimpleNamespace(deployment_id="dep"),
            intent,
            SimpleNamespace(),
            SimpleNamespace(chain="arbitrum", wallet_address="0xwallet"),
        )

    assert preparation.applicable is True
    assert preparation.orders == ()
    assert preparation.error is not None
    assert "no accepted order identity" in preparation.error


@pytest.mark.asyncio
async def test_helper_enriches_waits_and_attaches_keeper_receipt_before_success() -> None:
    order = SimpleNamespace(protocol="gmx_v2", order_id="0xorder")
    submission = SimpleNamespace(async_orders=[])
    keeper_receipt = {"transactionHash": "0xkeeper", "status": "0x1", "logs": []}
    barrier = AsyncSettlementBarrierResult(
        status=AsyncSettlementStatus.SETTLED,
        terminal=True,
        attempts=1,
        elapsed_seconds=0.1,
        receipts=(keeper_receipt,),
    )
    runner = SimpleNamespace(
        config=SimpleNamespace(
            async_settlement_timeout_seconds=17,
            async_settlement_poll_interval_seconds=2,
        ),
        _is_live_mode=MagicMock(return_value=True),
        _build_pool_key_lookup=MagicMock(return_value=None),
        _build_curve_pool_meta_lookup=MagicMock(return_value=None),
        _get_gateway_client=MagicMock(return_value=object()),
        _append_settlement_receipts=MagicMock(),
        state_manager=None,
    )
    strategy = SimpleNamespace(
        deployment_id="dep",
        chain="arbitrum",
        wallet_address="0xstrategy",
        _gateway_network="anvil",
    )
    intent = SimpleNamespace(protocol="gmx_v2")
    context = SimpleNamespace(chain="avalanche", wallet_address="0xcontext")

    with (
        patch("almanak.framework.execution.result_enricher.ResultEnricher") as enricher_cls,
        patch(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            new=AsyncMock(return_value=barrier),
        ) as wait_mock,
    ):
        enricher_cls.return_value.enrich.return_value = SimpleNamespace(async_orders=[order])
        helpers = build_runner_helpers(runner)
        assert helpers.prepare_intent_settlement is not None
        preparation = helpers.prepare_intent_settlement(
            strategy,
            intent,
            submission,
            context,
            bundle_metadata={"expected": "value"},
        )
        assert preparation.applicable is True
        assert preparation.error is None
        assert helpers.await_intent_settlement is not None
        error = await helpers.await_intent_settlement(
            strategy,
            intent,
            submission,
            context,
            preparation=preparation,
        )

    assert error is None
    wait_mock.assert_awaited_once()
    assert wait_mock.await_args.kwargs["orders"] == (order,)
    assert wait_mock.await_args.kwargs["timeout_seconds"] == 17
    assert wait_mock.await_args.kwargs["chain"] == "avalanche"
    assert wait_mock.await_args.kwargs["wallet_address"] == "0xcontext"
    runner._append_settlement_receipts.assert_called_once_with(submission, (keeper_receipt,))
    assert submission._teardown_async_settlement_order_keys == ("0xorder",)


@pytest.mark.asyncio
async def test_helper_fails_closed_when_prepared_order_identity_is_missing() -> None:
    runner = SimpleNamespace(_get_gateway_client=MagicMock(return_value=object()))
    helpers = build_runner_helpers(runner)
    assert helpers.await_intent_settlement is not None

    with patch("almanak.framework.runner.async_settlement.await_async_settlement", new=AsyncMock()) as wait_mock:
        error = await helpers.await_intent_settlement(
            SimpleNamespace(deployment_id="dep"),
            SimpleNamespace(protocol="gmx_v2"),
            SimpleNamespace(async_orders=[]),
            SimpleNamespace(chain="arbitrum", wallet_address="0xwallet"),
            preparation=SettlementPreparation(applicable=True),
        )

    assert error is not None
    assert "order identity was unavailable" in error
    wait_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_helper_converts_observer_raise_to_fail_closed_no_retry_reason() -> None:
    """An observer exception after submission must not escape into slippage retry."""
    order = SimpleNamespace(protocol="gmx_v2", order_id="0xorder")
    submission = SimpleNamespace(async_orders=[])
    runner = SimpleNamespace(
        config=SimpleNamespace(
            async_settlement_timeout_seconds=17,
            async_settlement_poll_interval_seconds=2,
        ),
        _is_live_mode=MagicMock(return_value=True),
        _build_pool_key_lookup=MagicMock(return_value=None),
        _build_curve_pool_meta_lookup=MagicMock(return_value=None),
        _get_gateway_client=MagicMock(return_value=object()),
        _append_settlement_receipts=MagicMock(),
        state_manager=None,
    )
    strategy = SimpleNamespace(
        deployment_id="dep",
        chain="arbitrum",
        wallet_address="0xwallet",
        _gateway_network="mainnet",
    )

    with (
        patch("almanak.framework.execution.result_enricher.ResultEnricher") as enricher_cls,
        patch(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            new=AsyncMock(side_effect=RuntimeError("observer unavailable")),
        ),
    ):
        enricher_cls.return_value.enrich.side_effect = lambda result, *_args, **_kwargs: (
            setattr(result, "async_orders", [order]) or result
        )
        helpers = build_runner_helpers(runner)
        assert helpers.prepare_intent_settlement is not None
        preparation = helpers.prepare_intent_settlement(
            strategy,
            SimpleNamespace(protocol="gmx_v2"),
            submission,
            SimpleNamespace(chain="arbitrum"),
        )
        assert preparation.applicable is True
        assert preparation.error is None
        assert helpers.await_intent_settlement is not None
        error = await helpers.await_intent_settlement(
            strategy,
            SimpleNamespace(protocol="gmx_v2"),
            submission,
            SimpleNamespace(chain="arbitrum"),
        )

    assert error is not None
    assert "refusing to retry" in error
    assert "observer unavailable" in error
    runner._append_settlement_receipts.assert_not_called()


@pytest.mark.asyncio
async def test_terminal_settlement_is_reconciled_immediately_after_submission_commit() -> None:
    """Teardown must book correlated Phase-2 settlement before runner exit."""
    gateway_client = object()
    runner = SimpleNamespace(
        _get_gateway_client=MagicMock(return_value=gateway_client),
        state_manager=None,
    )
    strategy = SimpleNamespace(deployment_id="dep", chain="arbitrum", wallet_address="0xwallet")
    order_key = "0x" + "12" * 32
    execution_result = SimpleNamespace(
        _teardown_async_settlement_terminal=True,
        _teardown_async_settlement_order_keys=(order_key,),
    )
    context = SimpleNamespace(chain="avalanche", wallet_address="0xavaxwallet")

    with (
        patch(
            "almanak.framework.runner.perp_settlement_reconciler.reconcile_perp_settlements",
            new=AsyncMock(
                return_value=PerpSettlementReconcileOutcome(
                    attempted=1,
                    booked=1,
                    attempted_order_keys=(order_key,),
                    booked_order_keys=(order_key,),
                )
            ),
        ) as reconcile_mock,
    ):
        reconcile = build_runner_helpers(runner).reconcile_intent_settlement
        assert reconcile is not None
        degraded = await reconcile(
            strategy,
            execution_result,
            context,
            "teardown-123",
        )

    assert degraded == ()
    reconcile_mock.assert_awaited_once_with(
        runner,
        strategy,
        deployment_id="dep",
        cycle_id="teardown-123",
        gateway_client=gateway_client,
        chain="avalanche",
        wallet_address="0xavaxwallet",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("gateway_client", [object(), None])
async def test_terminal_settlement_without_target_booking_is_deferred(gateway_client: object | None) -> None:
    """Empty resolver/block lookup or lost gateway must never look accounting-clean."""
    order_key = "0x" + "34" * 32
    runner = SimpleNamespace(_get_gateway_client=MagicMock(return_value=gateway_client), state_manager=None)
    strategy = SimpleNamespace(deployment_id="dep", chain="arbitrum", wallet_address="0xwallet")
    execution_result = SimpleNamespace(
        _teardown_async_settlement_terminal=True,
        _teardown_async_settlement_order_keys=(order_key,),
    )
    deferred: list[object] = []

    with (
        patch(
            "almanak.framework.runner.perp_settlement_reconciler.reconcile_perp_settlements",
            new=AsyncMock(return_value=PerpSettlementReconcileOutcome()),
        ),
        patch("almanak.framework.accounting.deferred_log.append", side_effect=deferred.append),
    ):
        reconcile = build_runner_helpers(runner).reconcile_intent_settlement
        assert reconcile is not None
        records = await reconcile(
            strategy,
            execution_result,
            SimpleNamespace(chain="arbitrum", wallet_address="0xwallet"),
            "teardown-123",
        )

    assert len(records) == 1
    assert tuple(deferred) == records
    assert order_key in records[0].error


@pytest.mark.asyncio
async def test_terminal_settlement_degradation_is_returned_and_deferred() -> None:
    """A failed Phase-2 write must be visible on the teardown result path."""
    runner = SimpleNamespace(_get_gateway_client=MagicMock(return_value=object()), state_manager=None)
    strategy = SimpleNamespace(deployment_id="dep", chain="arbitrum", wallet_address="0xwallet")
    execution_result = SimpleNamespace(_teardown_async_settlement_terminal=True)
    deferred: list[object] = []

    with (
        patch(
            "almanak.framework.runner.perp_settlement_reconciler.reconcile_perp_settlements",
            new=AsyncMock(
                return_value=PerpSettlementReconcileOutcome(
                    attempted=1,
                    booked=0,
                    degraded_reasons=("order 0xorder: settlement commit failed",),
                )
            ),
        ),
        patch("almanak.framework.accounting.deferred_log.append", side_effect=deferred.append),
    ):
        reconcile = build_runner_helpers(runner).reconcile_intent_settlement
        assert reconcile is not None
        records = await reconcile(
            strategy,
            execution_result,
            SimpleNamespace(chain="arbitrum", wallet_address="0xwallet"),
            "teardown-123",
        )

    assert len(records) == 1
    assert tuple(deferred) == records
    assert records[0].kind == "perp_settlement"
    assert "settlement commit failed" in records[0].error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("settlement_state", "measured", "expected"),
    [
        ("EXECUTED", True, "executed"),
        ("CANCELLED", True, "terminal_failed"),
        ("FROZEN", True, "terminal_failed"),
        ("NOT_FOUND_UNCORRELATED", True, "unproven"),
        ("EXECUTED", False, None),
    ],
)
async def test_resume_checker_requires_measured_executed_order(
    settlement_state: str,
    measured: bool,
    expected: str | None,
) -> None:
    order_key = "0x" + "78" * 32
    event = {
        "event_type": "PERP_SETTLEMENT",
        "ledger_entry_id": "ledger-1",
        "payload_json": json.dumps({"order_key": order_key, "settlement_state": settlement_state}),
    }
    state_manager = SimpleNamespace(
        read_accounting_events_measured=lambda _deployment_id: ([event], measured),
        get_ledger_entry_by_id=AsyncMock(
            return_value={"extracted_data_json": json.dumps({"async_orders": [{"order_id": order_key}]})}
        ),
    )
    runner = SimpleNamespace(_get_gateway_client=MagicMock(return_value=object()), state_manager=state_manager)
    strategy = SimpleNamespace(deployment_id="dep")

    with patch(
        "almanak.framework.runner.perp_settlement_reconciler.reconcile_perp_settlements",
        new=AsyncMock(return_value=PerpSettlementReconcileOutcome()),
    ):
        checker = build_runner_helpers(runner).check_intent_settlement
        assert checker is not None
        result = await checker(
            strategy,
            ledger_entry_id="ledger-1",
            order_keys=(order_key,),
            cycle_id="teardown-1",
            chain="arbitrum",
            wallet_address="0xwallet",
        )
        result_without_ledger_id = await checker(
            strategy,
            ledger_entry_id=None,
            order_keys=(order_key,),
            cycle_id="teardown-1",
            chain="arbitrum",
            wallet_address="0xwallet",
        )
        result_recovered_from_ledger = await checker(
            strategy,
            ledger_entry_id="ledger-1",
            order_keys=(),
            cycle_id="teardown-1",
            chain="arbitrum",
            wallet_address="0xwallet",
        )

    assert result == expected
    assert result_without_ledger_id == expected
    assert result_recovered_from_ledger == expected


@pytest.mark.asyncio
async def test_resume_checker_mixed_executed_and_cancelled_orders_is_fulfilled() -> None:
    executed_key = "0x" + "aa" * 32
    cancelled_key = "0x" + "bb" * 32
    events = [
        {
            "event_type": "PERP_SETTLEMENT",
            "ledger_entry_id": "ledger-1",
            "payload_json": json.dumps({"order_key": executed_key, "settlement_state": "EXECUTED"}),
        },
        {
            "event_type": "PERP_SETTLEMENT",
            "ledger_entry_id": "ledger-1",
            "payload_json": json.dumps({"order_key": cancelled_key, "settlement_state": "CANCELLED"}),
        },
    ]
    state_manager = SimpleNamespace(
        # Production GatewayStateManager exposes this as a synchronous measured
        # read; the helper intentionally moves it off-loop with asyncio.to_thread.
        read_accounting_events_measured=lambda _deployment_id: (events, True),
        get_ledger_entry_by_id=AsyncMock(return_value=None),
    )
    runner = SimpleNamespace(_get_gateway_client=MagicMock(return_value=object()), state_manager=state_manager)

    with patch(
        "almanak.framework.runner.perp_settlement_reconciler.reconcile_perp_settlements",
        new=AsyncMock(return_value=PerpSettlementReconcileOutcome()),
    ):
        checker = build_runner_helpers(runner).check_intent_settlement
        assert checker is not None
        result = await checker(
            SimpleNamespace(deployment_id="dep"),
            ledger_entry_id="ledger-1",
            order_keys=(executed_key, cancelled_key),
            cycle_id="teardown-1",
            chain="arbitrum",
            wallet_address="0xwallet",
        )

    assert result == "executed"
