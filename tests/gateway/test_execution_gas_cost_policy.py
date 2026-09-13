"""Cost policy crosses the execution boundary without permitting a legacy fallback."""

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.execution.gateway_orchestrator import (
    GatewayExecutionOrchestrator,
    _resolve_execution_options,
)
from almanak.framework.execution.interfaces import UnsignedTransaction
from almanak.framework.execution.orchestrator import ExecutionOrchestrator, TransactionRiskConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer
from almanak.gateway.services.market_service import MarketServiceServicer

WALLET = "0x" + "12" * 20


def _observations(timestamp, *, stale=False):
    return {
        "contributing_observations": [
            {"source": "chainlink", "timestamp": timestamp.isoformat(), "stale": stale, "peg_tokens": []},
        ]
    }


def _request(**caps):
    request = gateway_pb2.ExecuteRequest(
        chain="bsc",
        wallet_address=WALLET,
        action_bundle=json.dumps({"intent_type": "swap", "transactions": []}).encode(),
    )
    for field, value in caps.items():
        setattr(request.gas_cost_policy, field, value)
    return request


def _service():
    service = ExecutionServiceServicer(GatewaySettings())
    service._ensure_initialized = AsyncMock()
    risk = TransactionRiskConfig(max_gas_cost_native=0.02, max_gas_cost_usd=0)
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = risk
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=True, transaction_results=[], total_gas_used=0, correlation_id="test", error=""
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    service._gas_policy_native_price = AsyncMock(return_value=(600, datetime.now(UTC)))
    return service, orchestrator, risk


@pytest.mark.parametrize("caps", [{"max_gas_cost_native": 0}, {"max_gas_cost_usd": 2}, {}])
def test_client_selects_cost_policy_rpc_by_presence(caps):
    client = MagicMock()
    orchestrator = GatewayExecutionOrchestrator(client, chain="bsc", wallet_address=WALLET, **caps)
    options = _resolve_execution_options(
        context=None,
        deployment_id="deployment:test",
        intent_id="test",
        dry_run=False,
        simulation_enabled=True,
        wallet_address=None,
        default_wallet_address=WALLET,
    )
    request = orchestrator._build_execute_request(b"{}", options)
    orchestrator._dispatch_execute(request)
    if caps:
        client.execution.Execute.assert_not_called()
        client.execution.ExecuteWithGasPolicy.assert_called_once()
        for field, value in caps.items():
            assert request.gas_cost_policy.HasField(field)
            assert getattr(request.gas_cost_policy, field) == value
    else:
        client.execution.ExecuteWithGasPolicy.assert_not_called()
        client.execution.Execute.assert_called_once()


def test_client_never_falls_back_to_legacy_execution():
    client = MagicMock()
    client.execution.ExecuteWithGasPolicy.side_effect = NotImplementedError("old server")
    orchestrator = GatewayExecutionOrchestrator(client)
    with pytest.raises(NotImplementedError):
        orchestrator._dispatch_execute(_request(max_gas_cost_usd=1))
    client.execution.Execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["max_gas_cost_native", "max_gas_cost_usd"])
@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), -float("inf")])
async def test_malformed_policy_refuses_before_initialization(field, value):
    service, orchestrator, _ = _service()
    response = await service.ExecuteWithGasPolicy(_request(**{field: value}), MagicMock())
    assert not response.success
    assert response.submission_provenance == gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED
    service._ensure_initialized.assert_not_called()
    orchestrator.execute.assert_not_called()


@pytest.mark.asyncio
async def test_cost_policy_method_requires_policy():
    service, orchestrator, _ = _service()
    response = await service.ExecuteWithGasPolicy(_request(), MagicMock())
    assert not response.success
    orchestrator.execute.assert_not_called()


@pytest.mark.asyncio
async def test_cost_policy_preserves_absence_zero_and_request_isolation():
    service, orchestrator, original = _service()
    seen = []

    async def execute(*_):
        risk = orchestrator.tx_risk_config
        await asyncio.sleep(0)
        seen.append((risk.max_gas_cost_native, risk.max_gas_cost_usd, risk.native_token_price_usd))
        if risk.max_gas_cost_usd > 0:
            assert risk.native_token_price_timestamp == service._gas_policy_native_price.return_value[1]
            assert risk.native_token_price_max_age_seconds == 60
        return SimpleNamespace(success=True, transaction_results=[], total_gas_used=0, correlation_id="test", error="")

    orchestrator.execute.side_effect = execute
    responses = await asyncio.gather(
        service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock()),
        service.ExecuteWithGasPolicy(_request(max_gas_cost_native=0), MagicMock()),
        service.Execute(_request(), MagicMock()),
    )
    assert all(response.success for response in responses)
    assert seen == [(0.02, 1, 600), (0, 0, 0), (0.02, 0, 0)]
    assert orchestrator.tx_risk_config is original
    service._gas_policy_native_price.assert_awaited_once_with("bsc")


@pytest.mark.asyncio
async def test_price_failure_never_reuses_previous_quote():
    service, orchestrator, original = _service()
    original.native_token_price_usd = 999
    service._gas_policy_native_price.side_effect = ValueError("oracle unavailable")
    response = await service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock())
    assert not response.success
    orchestrator.execute.assert_not_called()
    assert orchestrator.tx_risk_config is original


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("failure"), asyncio.CancelledError()])
async def test_cost_policy_configuration_restored_on_execution_failure(error):
    service, orchestrator, original = _service()
    orchestrator.execute.side_effect = error
    if isinstance(error, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock())
    else:
        response = await service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock())
        assert not response.success
    assert orchestrator.tx_risk_config is original


@pytest.mark.asyncio
async def test_legacy_execute_enforces_inherited_usd_policy():
    service, orchestrator, original = _service()
    original.max_gas_cost_usd = 3
    response = await service.Execute(_request(), MagicMock())
    assert response.success
    service._gas_policy_native_price.assert_awaited_once_with("bsc")
    assert orchestrator.tx_risk_config is original


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["max_gas_cost_native", "max_gas_cost_usd"])
@pytest.mark.parametrize("rpc", ["Execute", "ExecuteWithGasPolicy"])
@pytest.mark.parametrize(
    "configured,override,other_configured,other_override,allowed",
    [
        pytest.param(None, None, None, None, True, id="absent"),
        pytest.param(0, None, 0, None, True, id="configured-zero"),
        pytest.param(None, 0, None, None, True, id="explicit-zero"),
        pytest.param(1, 0, None, None, True, id="zero-disables-configured-cap"),
        pytest.param(1, 0, 1, 0, True, id="zero-disables-both-caps"),
        pytest.param(None, 1, None, None, False, id="explicit-positive"),
        pytest.param(1, None, None, None, False, id="inherited-positive"),
        pytest.param(0, 1, None, None, False, id="positive-enables-configured-zero"),
        pytest.param(1, 0, 1, None, False, id="zero-preserves-other-inherited-cap"),
        pytest.param(None, 0, None, 1, False, id="zero-preserves-other-explicit-cap"),
    ],
)
async def test_solana_routes_only_when_effective_cost_caps_are_disabled(
    field, rpc, configured, override, other_configured, other_override, allowed
):
    other_field = "max_gas_cost_usd" if field == "max_gas_cost_native" else "max_gas_cost_native"
    service, orchestrator, _ = _service()
    setattr(service.settings, field, configured)
    setattr(service.settings, other_field, other_configured)
    caps = {key: value for key, value in ((field, override), (other_field, other_override)) if value is not None}
    request = _request(**caps)
    request.chain = "solana"
    request.wallet_address = "11111111111111111111111111111111"
    planner = SimpleNamespace(
        execute_actions=AsyncMock(
            return_value=SimpleNamespace(
                success=True,
                receipts=[],
                total_fee_native=Decimal(0),
                tx_ids=[],
                error=None,
                submission_provenance="NOT_ATTEMPTED",
            )
        )
    )
    service._get_solana_planner = AsyncMock(return_value=planner)
    if rpc == "ExecuteWithGasPolicy":
        request.gas_cost_policy.SetInParent()
    response = await getattr(service, rpc)(request, MagicMock())
    assert response.success is allowed
    if allowed:
        service._get_solana_planner.assert_awaited_once_with("solana", request.wallet_address)
        planner.execute_actions.assert_awaited_once()
        assert planner.execute_actions.call_args.args[1]["chain"] == "solana"
    else:
        assert response.error_code == "GAS_POLICY_REFUSED"
        assert response.submission_provenance == gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED
        service._ensure_initialized.assert_not_called()
        service._get_solana_planner.assert_not_called()
        planner.execute_actions.assert_not_called()
    orchestrator.execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "price,stale,age,aware",
    [
        (0, False, 0, True),
        (-1, False, 0, True),
        ("NaN", False, 0, True),
        ("Infinity", False, 0, True),
        (600, True, 0, True),
        (600, False, 61, True),
        (600, False, -1, True),
        (600, False, 0, False),
    ],
)
async def test_native_quote_rejects_unusable_observations(price, stale, age, aware):
    service = ExecutionServiceServicer(GatewaySettings())
    timestamp = datetime.now(UTC) - timedelta(seconds=age)
    if not aware:
        timestamp = timestamp.replace(tzinfo=None)
    aggregator = SimpleNamespace(
        get_aggregated_price=AsyncMock(
            return_value=SimpleNamespace(
                price=Decimal(str(price)),
                stale=stale,
                timestamp=timestamp,
                source_details=_observations(timestamp, stale=stale),
            )
        )
    )
    market = MarketServiceServicer(GatewaySettings())
    market._ensure_initialized = AsyncMock()
    market._auto_reinitialize_unconfigured_chains = AsyncMock()
    market._chain_configuration_error = MagicMock(return_value=None)
    market._price_aggregators = {"bsc": aggregator}
    service.market_servicer = market
    with pytest.raises(ValueError):
        await service._gas_policy_native_price("bsc")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chain,symbol,price", [("bsc", "BNB", 600), ("ethereum", "ETH", 3000), ("polygon", "MATIC", 0.3)]
)
async def test_native_quote_uses_chain_descriptor_and_fresh_gateway_oracle(chain, symbol, price):
    service = ExecutionServiceServicer(GatewaySettings())
    aggregator = SimpleNamespace(
        get_aggregated_price=AsyncMock(
            return_value=SimpleNamespace(
                price=Decimal(str(price)),
                stale=False,
                timestamp=datetime.now(UTC),
                source_details=_observations(datetime.now(UTC)),
            )
        )
    )
    market = MarketServiceServicer(GatewaySettings())
    market._ensure_initialized = AsyncMock()
    market._auto_reinitialize_unconfigured_chains = AsyncMock()
    market._chain_configuration_error = MagicMock(return_value=None)
    market._price_aggregators = {chain: aggregator}
    service.market_servicer = market
    observed_price, timestamp = await service._gas_policy_native_price(chain)
    assert observed_price == price
    assert timestamp == datetime.fromisoformat(
        aggregator.get_aggregated_price.return_value.source_details["contributing_observations"][0]["timestamp"]
    )
    aggregator.get_aggregated_price.assert_awaited_once_with(symbol, "USD", max_observation_age_seconds=60)


def test_unimplemented_rpc_is_a_proven_unsubmitted_refusal():
    class Unimplemented(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNIMPLEMENTED

    client = MagicMock()
    client.execution.ExecuteWithGasPolicy.side_effect = Unimplemented()
    orchestrator = GatewayExecutionOrchestrator(client)
    response = orchestrator._dispatch_execute(_request(max_gas_cost_usd=1))
    assert not response.success
    assert response.error_code == "GAS_POLICY_UNSUPPORTED"
    assert response.submission_provenance == gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED
    client.execution.Execute.assert_not_called()


@pytest.mark.asyncio
async def test_native_quote_never_falls_back_to_another_chains_aggregator():
    service = ExecutionServiceServicer(GatewaySettings())
    wrong_chain_aggregator = SimpleNamespace(get_aggregated_price=AsyncMock())
    market = MarketServiceServicer(GatewaySettings())
    market._ensure_initialized = AsyncMock()
    market._auto_reinitialize_unconfigured_chains = AsyncMock()
    market._chain_configuration_error = MagicMock(return_value=None)
    market._price_aggregators = {"ethereum": wrong_chain_aggregator}
    service.market_servicer = market
    with pytest.raises(ValueError, match="execution chain"):
        await service._gas_policy_native_price("bsc")
    wrong_chain_aggregator.get_aggregated_price.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [None, RuntimeError("execution failed"), asyncio.CancelledError()])
async def test_gas_overrides_preserve_daily_risk_state_across_requests(error):
    service, orchestrator, risk = _service()
    risk.max_daily_volume_eth = Decimal("2")
    transaction = UnsignedTransaction(
        to=WALLET,
        value=10**18,
        data="0x",
        chain_id=56,
        gas_limit=21000,
        max_fee_per_gas=1000000000,
        max_priority_fee_per_gas=100000000,
    )
    validations = []

    async def execute(_bundle, context):
        validation = await ExecutionOrchestrator._validate_transactions(orchestrator, [transaction], context)
        validations.append(validation.passed)
        active_risk = orchestrator.tx_risk_config
        active_risk.observed_request_count = getattr(active_risk, "observed_request_count", 0) + 1
        if error is not None:
            raise error
        return SimpleNamespace(
            success=validation.passed,
            transaction_results=[],
            total_gas_used=0,
            correlation_id="test",
            error="; ".join(validation.violations),
        )

    orchestrator.execute.side_effect = execute
    for _ in range(3):
        if isinstance(error, asyncio.CancelledError):
            with pytest.raises(asyncio.CancelledError):
                await service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock())
        else:
            await service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock())
        assert risk.max_gas_cost_usd == 0
        assert risk.native_token_price_usd == 0
        assert risk.native_token_price_timestamp is None
    assert validations == [True, True, False]
    assert risk._daily_volume_wei == 2 * 10**18
    assert risk._daily_volume_date == date.today().isoformat()
    assert risk.observed_request_count == 3


@pytest.mark.asyncio
async def test_gas_overrides_restore_originally_absent_policy_fields():
    service, orchestrator, _ = _service()
    risk = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.tx_risk_config = risk
    response = await service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=1), MagicMock())
    assert response.success
    assert vars(risk) == {"max_gas_price_gwei": 42}


@pytest.mark.asyncio
async def test_native_only_execution_does_not_wait_for_another_requests_price_fetch():
    service, orchestrator, risk = _service()
    fetching = asyncio.Event()
    release = asyncio.Event()

    async def fetch(chain):
        fetching.set()
        await release.wait()
        return 600, datetime.now(UTC)

    service._gas_policy_native_price.side_effect = fetch
    usd_request = asyncio.create_task(service.ExecuteWithGasPolicy(_request(max_gas_cost_usd=2), MagicMock()))
    try:
        await asyncio.wait_for(fetching.wait(), 2)
        response = await asyncio.wait_for(
            service.ExecuteWithGasPolicy(_request(max_gas_cost_native=0.01), MagicMock()), 2
        )
        assert response.success
        assert risk.max_gas_cost_usd == 0
    finally:
        release.set()
        usd_response = await asyncio.wait_for(usd_request, 2)
    assert usd_response.success
    assert risk.max_gas_cost_native == 0.02
    assert risk.max_gas_cost_usd == 0
