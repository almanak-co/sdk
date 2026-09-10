"""Synthetic reference controls preserve the production decoder and real balance providers."""

import copy
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.cli._reference_scenario import ReferenceScenarioHook
from almanak.framework.cli._scenario import ScenarioParseError, apply_scenario, parse_scenario
from almanak.framework.market import MarketSnapshotBuilder
from almanak.framework.market.models import ReferencePriceBasis
from almanak.gateway.proto import gateway_pb2 as pb
from almanak.integrations.bstocks.catalog import GOOGLB

AT = datetime(2020, 9, 8, 15, tzinfo=UTC)
EPOCH = int(AT.timestamp())


def observation():
    return {
        "instrument": "GOOGLB",
        "quote": "USD",
        "chain": "bsc",
        "price": "200",
        "confidence": 0.95,
        "source": "composition:bstocks",
        "observed_at": EPOCH,
        "stale": False,
        "availability": "REFERENCE_PRICE_AVAILABILITY_AVAILABLE",
        "market_status": "REFERENCE_MARKET_STATUS_OPEN",
        "market_status_as_of": EPOCH,
        "market_status_source": "regular_session",
        "reason": "",
        "basis": "REFERENCE_PRICE_BASIS_RAW_TOKEN",
        "token_address": GOOGLB.address,
        "composition": {
            "underlying_instrument": "GOOGL",
            "underlying_price": "100",
            "underlying_source": "verified:GOOGL/USD",
            "underlying_observed_at": EPOCH,
            "multiplier": "2",
            "multiplier_block_number": 120603059,
            "multiplier_block_hash": "0x" + "ab" * 32,
            "multiplier_block_timestamp": EPOCH,
            "multiplier_read_at": EPOCH,
            "scheduled_multiplier": "",
            "multiplier_effective_at": 0,
            "beacon_address": GOOGLB.beacon,
            "implementation_address": GOOGLB.implementation,
            "composed_at": EPOCH,
        },
    }


def document(raw=None):
    return {"reference_events": [{"scenario_at": AT.isoformat(), "references": [raw or observation()]}]}


def test_receipt_withholding_requires_explicit_reference_scenario():
    doc = document()
    doc["withhold_execution_receipts_once"] = True
    assert parse_scenario(json.dumps(doc)).withhold_execution_receipts_once is True
    assert parse_scenario(json.dumps(document())).withhold_execution_receipts_once is False


@pytest.mark.parametrize("value", [True, 1, "true", None])
def test_receipt_withholding_rejects_missing_reference_events(value):
    with pytest.raises(ScenarioParseError):
        parse_scenario(json.dumps({"withhold_execution_receipts_once": value}))


@pytest.mark.parametrize("value", [1, "true", None])
def test_receipt_withholding_rejects_non_boolean_value(value):
    doc = document()
    doc["withhold_execution_receipts_once"] = value
    with pytest.raises(ScenarioParseError):
        parse_scenario(json.dumps(doc))


def client_and_market():
    client = SimpleNamespace(is_connected=True, config=SimpleNamespace(timeout=2), market=MagicMock(), rpc=MagicMock())
    client.rpc.Call.return_value = pb.RpcResponse(success=True, result='"anvil/v1"')
    client.market.GetReferencePrice.return_value = pb.ReferencePriceResponse(
        availability=pb.REFERENCE_PRICE_AVAILABILITY_UNMEASURED, reason="real_provider_unavailable"
    )
    strategy = SimpleNamespace(chain="bsc", wallet_address="0x" + "11" * 20)
    market = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy, gateway_client=client, runtime_surface="unit_test"
    )
    return client, market


def hook(doc, client):
    return ReferenceScenarioHook(
        parse_scenario(json.dumps(doc)).reference_events, network="anvil", managed=True, client=client
    )


def test_full_composed_reference_uses_normal_decoder_and_explicit_clock(caplog):
    client, market = client_and_market()
    control = hook(document(), client)
    original_balance_provider = market._balance_provider
    original_prices = market._prices.copy()
    control(market)
    result = market.reference_price("GOOGLB", token_address=GOOGLB.address)
    assert result.price == Decimal("200")
    assert result.basis is ReferencePriceBasis.RAW_TOKEN
    assert result.composition.multiplier == Decimal("2")
    assert result.observed_at == result.composition.underlying_observed_at == AT
    assert market.timestamp == AT
    assert result.trade_block_reason(max_age_seconds=120, now=market.timestamp) is None
    assert result.trade_block_reason(max_age_seconds=120) == "multiplier_observation_stale"
    assert result.source == "synthetic:composition:bstocks"
    assert result.composition.underlying_source == "synthetic:verified:GOOGL/USD"
    assert market._balance_provider is original_balance_provider
    assert market._prices == original_prices
    assert market._synthetic_reference_scenario["synthetic"] is True
    assert control.digest in caplog.text
    client.market.GetReferencePrice.assert_not_called()


def test_chain_identity_is_canonical_before_validation_and_matching():
    raw = observation()
    raw["chain"] = " BSC "
    client, market = client_and_market()
    control = hook(document(raw), client)
    control(market)
    result = market.reference_price("GOOGLB")
    assert result.price == Decimal("200") and result.source.startswith("synthetic:")
    request = pb.ReferencePriceRequest(instrument="GOOGLB", chain="BSC", quote="USD", token_address=GOOGLB.address)
    response = market._gateway_client.market.GetReferencePrice(request)
    assert response.chain == "bsc" and response.price == "200"
    assert client.rpc.Call.call_args.args[0].chain == "bsc"
    client.market.GetReferencePrice.assert_not_called()


def test_duplicate_chain_spellings_are_rejected_after_normalization():
    doc = document()
    duplicate = observation()
    duplicate["chain"] = " BSC "
    doc["reference_events"][0]["references"].append(duplicate)
    with pytest.raises(ScenarioParseError, match="duplicate"):
        parse_scenario(json.dumps(doc))


@pytest.mark.parametrize("last_frame", [False, True])
def test_unconsumed_observation_fails_before_frame_advance_or_exhaustion(caplog, last_frame):
    client, first = client_and_market()
    doc = document()
    if not last_frame:
        doc["reference_events"].append(
            {"scenario_at": datetime.fromtimestamp(EPOCH + 1, UTC).isoformat(), "references": []}
        )
    control = hook(doc, client)
    control(first)
    assert first.reference_price("TSLA").reason == "real_provider_unavailable"
    _, second = client_and_market()
    original_timestamp = second.timestamp
    with pytest.raises(ValueError, match="unconsumed"):
        control(second)
    assert second.timestamp == original_timestamp
    assert control._next == 1
    assert "SYNTHETIC_REFERENCE_UNCONSUMED" in caplog.text
    assert control.digest in caplog.text and GOOGLB.address in caplog.text
    assert '"event": 1' in caplog.text


def test_consuming_one_observation_does_not_satisfy_the_other_declared_identity():
    doc = document()
    other = observation()
    other.update(
        instrument="TSLA",
        basis="REFERENCE_PRICE_BASIS_UNDERLYING_SHARE",
        token_address="",
        price="100",
    )
    del other["composition"]
    doc["reference_events"][0]["references"].append(other)
    client, first = client_and_market()
    control = hook(doc, client)
    control(first)
    assert first.reference_price("GOOGLB").price == Decimal("200")
    _, second = client_and_market()
    with pytest.raises(ValueError, match="unconsumed"):
        control(second)


@pytest.mark.parametrize(
    "section,key",
    [
        (None, "stale"),
        (None, "observed_at"),
        (None, "basis"),
        (None, "market_status_source"),
        (None, "composition"),
        ("composition", "multiplier_read_at"),
        ("composition", "scheduled_multiplier"),
        ("composition", "multiplier_effective_at"),
    ],
)
def test_missing_fields_never_default_to_complete_observations(section, key):
    raw = observation()
    del (raw[section] if section else raw)[key]
    with pytest.raises(ScenarioParseError):
        parse_scenario(json.dumps(document(raw)))


@pytest.mark.parametrize(
    "section,key,value",
    [
        (None, "price", "100"),
        (None, "token_address", "0x" + "22" * 20),
        (None, "stale", "false"),
        (None, "observed_at", True),
        (None, "confidence", "NaN"),
        ("composition", "multiplier_block_hash", "0xab"),
        ("composition", "multiplier_read_at", 0),
        ("composition", "underlying_observed_at", EPOCH - 1),
        ("composition", "implementation_address", "0x" + "22" * 20),
    ],
)
def test_malformed_composition_is_rejected(section, key, value):
    raw = observation()
    (raw[section] if section else raw)[key] = value
    with pytest.raises(ScenarioParseError):
        parse_scenario(json.dumps(document(raw)))


def test_events_advance_only_explicit_clock_preserving_old_source_time():
    client, first = client_and_market()
    raw = observation()
    raw["observed_at"] -= 121
    raw["composition"]["underlying_observed_at"] -= 121
    doc = document(raw)
    second_event = copy.deepcopy(doc["reference_events"][0])
    second_event["scenario_at"] = datetime.fromtimestamp(EPOCH + 31, UTC).isoformat()
    doc["reference_events"].append(second_event)
    control = hook(doc, client)
    control(first)
    assert (
        first.reference_price("GOOGLB").trade_block_reason(max_age_seconds=120, now=first.timestamp)
        == "reference_price_too_old"
    )
    _, second = client_and_market()
    control(second)
    assert second.reference_price("GOOGLB").observed_at == datetime.fromtimestamp(EPOCH - 121, UTC)
    assert (
        second.reference_price("GOOGLB").trade_block_reason(max_age_seconds=120, now=second.timestamp)
        == "multiplier_observation_stale"
    )
    with pytest.raises(ValueError, match="exhausted"):
        control(second)


def test_unlisted_reference_keeps_real_provider_failure():
    client, market = client_and_market()
    hook(document(), client)(market)
    result = market.reference_price("TSLA")
    assert result.price is None and result.reason == "real_provider_unavailable"
    client.market.GetReferencePrice.assert_called_once()


def test_unavailable_and_closed_stale_observations_remain_nontradeable():
    raw = observation()
    raw.update(availability="REFERENCE_PRICE_AVAILABILITY_UNMEASURED", price="", stale=True, reason="read_failed")
    client, market = client_and_market()
    hook(document(raw), client)(market)
    assert market.reference_price("GOOGLB").trade_block_reason(max_age_seconds=120, now=AT) == "read_failed"
    raw = observation()
    raw.update(market_status="REFERENCE_MARKET_STATUS_CLOSED", stale=True)
    client, market = client_and_market()
    hook(document(raw), client)(market)
    assert market.reference_price("GOOGLB").trade_block_reason(max_age_seconds=120, now=AT) == "reference_market_closed"


@pytest.mark.parametrize(
    "extra", [{"balances": {"USDT": "100"}}, {"prices": {"USDT": "1"}}, {"indicators": {"rsi": {"GOOGLB": 25}}}]
)
def test_reference_run_cannot_fabricate_balances_or_token_valuation(extra):
    with pytest.raises(ScenarioParseError, match="cannot override"):
        parse_scenario(json.dumps(document() | extra))


def test_reference_applier_requires_guarded_hook():
    _, market = client_and_market()
    with pytest.raises(ScenarioParseError, match="guarded"):
        apply_scenario(market, parse_scenario(json.dumps(document())))


@pytest.mark.parametrize("network,managed", [("mainnet", True), ("testnet", True), ("anvil", False), (None, True)])
def test_reference_runtime_rejects_mainnet_and_external_gateway(network, managed):
    client, _ = client_and_market()
    with pytest.raises(ValueError, match="local managed Anvil"):
        ReferenceScenarioHook(
            parse_scenario(json.dumps(document())).reference_events, network=network, managed=managed, client=client
        )
    client.rpc.Call.assert_not_called()


def test_actual_rpc_backend_must_be_anvil():
    client, _ = client_and_market()
    client.rpc.Call.return_value = pb.RpcResponse(success=True, result='"Geth/v1"')
    with pytest.raises(ValueError, match="not connected to Anvil"):
        hook(document(), client)


def test_hosted_refusal_rechecked_after_hook_install(monkeypatch):
    client, market = client_and_market()
    control = hook(document(), client)
    control(market)
    monkeypatch.setattr("almanak.framework.cli._reference_scenario.is_hosted", lambda: True)
    with pytest.raises(ValueError, match="local managed Anvil"):
        hook(document(), client)
    with pytest.raises(ValueError, match="local managed Anvil"):
        control(market)
    assert market.reference_price("GOOGLB").price is None


def test_continuous_cli_forwards_reference_scenario(tmp_path, monkeypatch):
    import importlib

    cli = importlib.import_module("almanak.cli.cli")
    captured = {}
    monkeypatch.setattr(cli, "framework_run_cmd", lambda **kwargs: captured.update(kwargs))
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(document()))
    result = CliRunner().invoke(
        cli.almanak, ["strat", "run", "-d", str(tmp_path), "--network", "anvil", "--reference-scenario", str(path)]
    )
    assert result.exit_code == 0, result.output
    assert captured["reference_scenario"] == str(path)
    assert captured["once"] is False and captured["max_iterations"] is None


@pytest.mark.parametrize("args", [["--network", "mainnet"], ["--network", "anvil", "--no-gateway"], []])
def test_framework_run_rejects_unsafe_reference_controls_before_gateway_start(tmp_path, monkeypatch, args):
    from almanak.framework.cli import run_helpers
    from almanak.framework.cli.run import run

    setup = MagicMock(side_effect=AssertionError("gateway must not start"))
    monkeypatch.setattr(run_helpers, "_setup_gateway", setup)
    (tmp_path / "config.json").write_text(json.dumps({"chain": "bsc", "network": "anvil"}))
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(document()))
    result = CliRunner().invoke(run, ["-d", str(tmp_path), "--reference-scenario", str(path), *args])
    assert result.exit_code != 0
    assert "Error: reference scenarios require local managed Anvil" in result.output
    assert "explicit --network anvil" in result.output
    assert "Traceback" not in result.output
    setup.assert_not_called()


def test_duplicate_and_nonincreasing_frames_rejected():
    doc = document()
    doc["reference_events"][0]["references"].append(observation())
    with pytest.raises(ScenarioParseError, match="duplicate"):
        parse_scenario(json.dumps(doc))
    doc = document()
    doc["reference_events"].append(copy.deepcopy(doc["reference_events"][0]))
    with pytest.raises(ScenarioParseError, match="strictly increase"):
        parse_scenario(json.dumps(doc))


def test_missing_frame_observation_does_not_reuse_previous_response():
    client, first = client_and_market()
    doc = document()
    doc["reference_events"].append(
        {"scenario_at": datetime.fromtimestamp(EPOCH + 1, UTC).isoformat(), "references": []}
    )
    control = hook(doc, client)
    control(first)
    assert first.reference_price("GOOGLB").price == Decimal("200")
    _, second = client_and_market()
    control(second)
    assert second.reference_price("GOOGLB").reason == "real_provider_unavailable"


def test_reference_events_cannot_silently_attach_to_wrong_snapshot_chain():
    client, _ = client_and_market()
    control = hook(document(), client)
    strategy = SimpleNamespace(chain="ethereum", wallet_address="0x" + "11" * 20)
    other = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy, gateway_client=client, runtime_surface="unit_test"
    )
    original = other.timestamp
    with pytest.raises(ValueError, match="matching single-chain"):
        control(other)
    assert other.timestamp == original and other._gateway_client is client


@pytest.mark.parametrize("mode", ["continuous", "once", "lifecycle"])
@pytest.mark.parametrize("consumption", ["unused", "consumed", "empty"])
def test_framework_modes_finalize_active_reference_after_cleanup(tmp_path, monkeypatch, capsys, mode, consumption):
    _run_reference_cleanup_case(tmp_path, monkeypatch, capsys, mode, consumption)


@pytest.mark.parametrize("mode", ["continuous", "once", "lifecycle"])
@pytest.mark.parametrize("failure", ["iteration", "cleanup"])
def test_framework_modes_cleanup_on_failure_preserves_cleanup_error(tmp_path, monkeypatch, capsys, mode, failure):
    _run_reference_cleanup_case(tmp_path, monkeypatch, capsys, mode, "unused", failure=failure)


def _run_reference_cleanup_case(tmp_path, monkeypatch, capsys, mode, consumption, failure=None):
    from unittest.mock import AsyncMock

    import click

    from almanak.framework.cli import run_helpers
    from almanak.framework.cli.run import run
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    client, market = client_and_market()
    doc = document()
    if consumption == "empty":
        doc["reference_events"].insert(0, {"scenario_at": AT.replace(second=0).isoformat(), "references": []})
        doc["reference_events"][1]["scenario_at"] = AT.replace(second=1).isoformat()
    path = tmp_path / "final-frame.json"
    path.write_text(json.dumps(doc))
    events = []
    original_assert_consumed = ReferenceScenarioHook.assert_consumed

    def assert_consumed(control):
        events.append("validate")
        original_assert_consumed(control)

    monkeypatch.setattr(ReferenceScenarioHook, "assert_consumed", assert_consumed)
    strategy = SimpleNamespace(
        deployment_id="deployment:reference-final",
        chain="bsc",
        force_action="",
        load_state_async=AsyncMock(return_value=False),
        flush_pending_saves=AsyncMock(),
        _wallet_activity_provider=None,
        get_open_positions=lambda: SimpleNamespace(positions=[]),
    )
    runner = MagicMock()
    runner.config.enable_state_persistence = False
    runner._signal_received = False
    runner._capture_portfolio_snapshot = AsyncMock()
    calls = 0

    async def iteration(_strategy):
        nonlocal calls
        calls += 1
        if calls > 1:
            events.append("teardown")
            runner._teardown_closure_verification = {
                "positions_total": 0,
                "positions_closed": 0,
                "all_closed": True,
                "closure_unknown": False,
                "has_position_breakdown": True,
            }
            return IterationResult(status=IterationStatus.TEARDOWN, deployment_id=strategy.deployment_id)
        runner._snapshot_override_hook(market)
        if consumption == "consumed":
            assert market.reference_price("GOOGLB").price == Decimal("200")
        events.append("iteration")
        if failure == "iteration":
            raise RuntimeError("iteration failed")
        return IterationResult(status=IterationStatus.HOLD, deployment_id=strategy.deployment_id)

    async def continuous(**_kwargs):
        await iteration(strategy)
        events.append("teardown")

    async def cleanup():
        events.append("cleanup")
        if failure == "cleanup":
            raise RuntimeError("cleanup failed")

    runner.run_iteration = AsyncMock(side_effect=iteration)
    runner.run_loop = AsyncMock(side_effect=continuous)
    runtime = MagicMock(resolved_network="anvil", deployment_id=strategy.deployment_id)
    components = SimpleNamespace(runner=runner, state_manager=MagicMock())
    for name in [
        "_configure_logging_and_validate",
        "_wire_token_resolver",
        "_echo_strategy_runtime_summary",
        "_stop_dashboard",
    ]:
        monkeypatch.setattr(run_helpers, name, MagicMock())
    monkeypatch.setattr(run_helpers, "_handle_list_all", lambda *_a, **_k: False)
    monkeypatch.setattr(run_helpers, "_maybe_handle_run_early_exit", lambda **_k: False)
    monkeypatch.setattr(run_helpers, "_maybe_start_dashboard_process", lambda **_k: None)
    monkeypatch.setattr(
        run_helpers, "_setup_gateway", lambda **_k: (client, MagicMock(), "localhost", 1, "anvil", None, None, None)
    )
    monkeypatch.setattr(run_helpers, "_load_strategy_bootstrap", lambda **_k: MagicMock())
    monkeypatch.setattr(run_helpers, "_prepare_runtime_bootstrap", lambda **_k: runtime)
    monkeypatch.setattr(run_helpers, "_instantiate_strategy", lambda **_k: strategy)
    monkeypatch.setattr(run_helpers, "_build_components_or_exit", lambda **_k: components)
    monkeypatch.setattr(run_helpers, "_build_cleanup_fn", lambda **_k: cleanup)
    monkeypatch.setattr("almanak.framework.teardown.get_teardown_state_manager", lambda **_k: MagicMock())

    with pytest.raises(SystemExit) as stopped, click.Context(run) as context:
        context.invoke(
            run,
            network="anvil",
            reference_scenario=str(path),
            once=mode == "once",
            test_actions=[""] if mode == "lifecycle" else None,
            test_json=mode == "lifecycle",
            teardown_after=mode != "continuous",
        )
    output = capsys.readouterr()
    expected_failure = consumption == "unused" or failure is not None
    assert stopped.value.code == (1 if expected_failure else 0)
    tail = ["cleanup"] if failure == "cleanup" else ["cleanup", "validate"]
    assert events[-len(tail) :] == tail
    if failure != "iteration":
        assert events[-len(tail) - 1] == "teardown"
    if mode == "lifecycle":
        start = output.out.rfind("\n{")
        payload = json.loads(output.out[start + 1 :])
        assert payload["summary"]["all_passed"] is (not expected_failure)
        if failure == "cleanup":
            assert payload["summary"]["error"] == "cleanup failed"
        elif consumption == "unused":
            assert "unconsumed" in payload["summary"]["error"]
    elif failure == "cleanup":
        assert "cleanup failed" in output.err
    elif consumption == "unused":
        assert "unconsumed" in output.err


@pytest.mark.parametrize("intent_type", ["SWAP", "LP_OPEN"])
def test_receipt_withholding_selects_validated_intent(intent_type):
    doc = document()
    doc.update(withhold_execution_receipts_once=True, withhold_execution_receipts_intent=intent_type)
    assert parse_scenario(json.dumps(doc)).withhold_execution_receipts_intent == intent_type


@pytest.mark.parametrize("intent_type", ["LP_CLOSE", "swap", None, 1, []])
def test_receipt_withholding_rejects_unsupported_intent(intent_type):
    doc = document()
    doc.update(withhold_execution_receipts_once=True, withhold_execution_receipts_intent=intent_type)
    with pytest.raises(ScenarioParseError):
        parse_scenario(json.dumps(doc))


def test_receipt_target_cannot_be_silently_unused():
    doc = document()
    doc["withhold_execution_receipts_intent"] = "LP_OPEN"
    with pytest.raises(ScenarioParseError, match="requires receipt withholding"):
        parse_scenario(json.dumps(doc))


def test_receipt_only_frames_preserve_real_reference_reads_with_explicit_chain():
    client, market = client_and_market()
    events = parse_scenario(
        json.dumps({"reference_events": [{"scenario_at": AT.isoformat(), "references": []}]})
    ).reference_events
    control = ReferenceScenarioHook(events, network="anvil", managed=True, client=client, chain="bsc")
    control(market)
    result = market._gateway_client.market.GetReferencePrice(pb.ReferencePriceRequest(instrument="GOOGL", chain="bsc"))
    assert result == client.market.GetReferencePrice.return_value
    client.market.GetReferencePrice.assert_called_once()
    control.assert_consumed()
    assert client.rpc.Call.call_args.args[0].chain == "bsc"


def test_explicit_chain_cannot_override_reference_observation_chain():
    client, _ = client_and_market()
    events = parse_scenario(json.dumps(document())).reference_events
    with pytest.raises(ValueError, match="exactly one chain"):
        ReferenceScenarioHook(events, network="anvil", managed=True, client=client, chain="ethereum")
    client.rpc.Call.assert_not_called()


def test_empty_reference_frames_still_require_chain_identity():
    client, _ = client_and_market()
    events = parse_scenario(
        json.dumps({"reference_events": [{"scenario_at": AT.isoformat(), "references": []}]})
    ).reference_events
    with pytest.raises(ValueError, match="exactly one chain"):
        ReferenceScenarioHook(events, network="anvil", managed=True, client=client)
    client.rpc.Call.assert_not_called()
