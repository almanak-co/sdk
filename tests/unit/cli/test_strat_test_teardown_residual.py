"""Post-teardown residual-position gate + asset-policy plumbing for `strat test` (ALM-2900)."""

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from almanak.framework.cli.run_helpers import _run_test_lifecycle
from almanak.framework.runner.runner_gateway import lifecycle_handle_stop
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.teardown import (
    PositionInfo,
    PositionType,
    TeardownAssetPolicy,
    TeardownPositionSummary,
    TeardownProfile,
    resolve_preferred_asset_policy,
)


def _parse_last_json_object(stream: str) -> dict:
    decoder = json.JSONDecoder()
    for m in reversed(list(re.finditer(r"^\{", stream, re.MULTILINE))):
        try:
            payload, _ = decoder.raw_decode(stream[m.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise AssertionError(f"No JSON object found in stream:\n{stream}")


def _make_runner(*results: IterationResult) -> MagicMock:
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner.run_iteration = AsyncMock(side_effect=list(results))
    runner.config = MagicMock(enable_state_persistence=False)
    runner._capture_portfolio_snapshot = AsyncMock()
    return runner


def _make_strategy(
    open_positions: list[PositionInfo] | None = None,
    open_positions_error: Exception | None = None,
    profile: TeardownProfile | None = None,
) -> MagicMock:
    s = MagicMock(
        spec=[
            "deployment_id",
            "STRATEGY_NAME",
            "chain",
            "force_action",
            "load_state_async",
            "_wallet_activity_provider",
            "flush_pending_saves",
            "get_open_positions",
            "get_teardown_profile",
        ]
    )
    s.deployment_id = "TestStrategy:abc"
    s.STRATEGY_NAME = "TestStrategy"
    s.chain = "ethereum"
    s.force_action = ""
    s.load_state_async = AsyncMock(return_value=False)
    s._wallet_activity_provider = None
    s.flush_pending_saves = AsyncMock()
    if open_positions_error is not None:
        s.get_open_positions = MagicMock(side_effect=open_positions_error)
    else:
        s.get_open_positions = MagicMock(
            return_value=TeardownPositionSummary(
                deployment_id="TestStrategy:abc",
                timestamp=datetime.now(UTC),
                positions=open_positions or [],
            )
        )
    s.get_teardown_profile = MagicMock(return_value=profile or TeardownProfile())
    return s


def _supply_position(value_usd: str = "6500") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-supply-wbtc",
        chain="polygon",
        protocol="aave_v3",
        value_usd=Decimal(value_usd),
    )


def _teardown_result() -> IterationResult:
    return IterationResult(status=IterationStatus.TEARDOWN, deployment_id="TestStrategy:abc")


def _patch_state_manager(monkeypatch) -> MagicMock:
    manager = MagicMock(create_request=MagicMock())
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: manager,
    )
    return manager


def _run(strategy, monkeypatch, asset_policy: str | None = None) -> tuple[int, MagicMock]:
    manager = _patch_state_manager(monkeypatch)
    runner = _make_runner(_teardown_result())
    exit_code = _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=AsyncMock(),
        actions=[],
        teardown=True,
        json_output=True,
        asset_policy=asset_policy,
    )
    return exit_code, manager


def test_residual_open_position_fails_teardown(capsys, monkeypatch):
    """A TEARDOWN-status iteration that leaves a supply leg open must fail the ladder."""
    strategy = _make_strategy(open_positions=[_supply_position()])
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["summary"]["all_passed"] is False
    assert payload["summary"]["teardown_passed"] is False
    step = payload["steps"][0]
    assert step["status"] == "TEARDOWN"  # the iteration itself completed
    residuals = step["open_positions_after_teardown"]
    assert len(residuals) == 1
    assert residuals[0]["position_id"] == "aave-supply-wbtc"
    assert residuals[0]["value_usd"] == "6500"
    assert "failure_logs" in step


def test_clean_teardown_passes(capsys, monkeypatch):
    strategy = _make_strategy(open_positions=[])
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["summary"]["teardown_passed"] is True
    assert "open_positions_after_teardown" not in payload["steps"][0]


def test_dust_residual_is_ignored(capsys, monkeypatch):
    strategy = _make_strategy(open_positions=[_supply_position(value_usd="0.005")])
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["summary"]["teardown_passed"] is True


def test_unvalued_residual_still_fails_teardown(capsys, monkeypatch):
    """A reported open position with no value_usd is a residual, not dust."""
    strategy = _make_strategy(open_positions=[_supply_position()])
    # Mutate after construction — TeardownPositionSummary.__post_init__ sums
    # value_usd, so a None can only reach the gate via post-hoc/duck-typed state.
    strategy.get_open_positions.return_value.positions[0].value_usd = None
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["summary"]["teardown_passed"] is False
    residuals = payload["steps"][0]["open_positions_after_teardown"]
    assert residuals[0]["value_usd"] == "unknown"


def test_unmeasured_check_does_not_fail_teardown(capsys, monkeypatch):
    """A read fault is UNMEASURED — surfaced, never fabricated into a residual."""
    strategy = _make_strategy(open_positions_error=RuntimeError("gateway gone"))
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["summary"]["teardown_passed"] is True
    assert "unmeasured" in payload["steps"][0]["open_positions_check"]


def test_explicit_asset_policy_reaches_teardown_request(capsys, monkeypatch):
    strategy = _make_strategy(open_positions=[])
    _, manager = _run(strategy, monkeypatch, asset_policy="keep_outputs")
    request = manager.create_request.call_args[0][0]
    assert request.asset_policy == TeardownAssetPolicy.KEEP_OUTPUTS


def test_strategy_preferred_policy_used_without_flag(capsys, monkeypatch):
    strategy = _make_strategy(
        open_positions=[],
        profile=TeardownProfile(preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS),
    )
    _, manager = _run(strategy, monkeypatch)
    request = manager.create_request.call_args[0][0]
    assert request.asset_policy == TeardownAssetPolicy.KEEP_OUTPUTS


def test_explicit_flag_wins_over_strategy_preference(capsys, monkeypatch):
    strategy = _make_strategy(
        open_positions=[],
        profile=TeardownProfile(preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS),
    )
    _, manager = _run(strategy, monkeypatch, asset_policy="target_token")
    request = manager.create_request.call_args[0][0]
    assert request.asset_policy == TeardownAssetPolicy.TARGET_TOKEN


def test_no_flag_no_preference_keeps_framework_default(capsys, monkeypatch):
    strategy = _make_strategy(open_positions=[])
    _, manager = _run(strategy, monkeypatch)
    request = manager.create_request.call_args[0][0]
    assert request.asset_policy == TeardownAssetPolicy.TARGET_TOKEN  # dataclass default


def test_resolve_preferred_asset_policy_degrades_on_broken_hook():
    class Broken:
        def get_teardown_profile(self):
            raise RuntimeError("boom")

    assert resolve_preferred_asset_policy(Broken()) is None


def test_resolve_preferred_asset_policy_coerces_string_value():
    class Stringy:
        def get_teardown_profile(self):
            return TeardownProfile(preferred_asset_policy="keep_outputs")  # type: ignore[arg-type]

    assert resolve_preferred_asset_policy(Stringy()) == TeardownAssetPolicy.KEEP_OUTPUTS


def test_lifecycle_stop_honors_strategy_preference(monkeypatch):
    """Production STOP must carry the strategy's declared no-swap policy (ALM-2900)."""
    manager = MagicMock(create_request=MagicMock())
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager_for_runtime",
        lambda *a, **k: manager,
    )
    runner = MagicMock()
    strategy = MagicMock()
    strategy.get_teardown_profile = MagicMock(
        return_value=TeardownProfile(preferred_asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS)
    )
    lifecycle_handle_stop(runner, "dep-1", strategy)
    request = manager.create_request.call_args[0][0]
    assert request.asset_policy == TeardownAssetPolicy.KEEP_OUTPUTS
    assert request.mode.value == "SOFT"


def test_lifecycle_stop_default_policy_unchanged(monkeypatch):
    manager = MagicMock(create_request=MagicMock())
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager_for_runtime",
        lambda *a, **k: manager,
    )
    runner = MagicMock()
    strategy = MagicMock()
    strategy.get_teardown_profile = MagicMock(return_value=TeardownProfile())
    lifecycle_handle_stop(runner, "dep-1", strategy)
    request = manager.create_request.call_args[0][0]
    assert request.asset_policy == TeardownAssetPolicy.TARGET_TOKEN


def test_missing_positions_collection_is_unmeasured(capsys, monkeypatch):
    """A summary without a positions collection is a broken hook, not a clean teardown."""
    strategy = _make_strategy(open_positions=[])
    strategy.get_open_positions = MagicMock(return_value=object())  # no .positions
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 0  # unmeasured surfaces, does not fail
    assert "no positions collection" in payload["steps"][0]["open_positions_check"]


def test_nonfinite_value_counts_as_unknown_residual(capsys, monkeypatch):
    strategy = _make_strategy(open_positions=[_supply_position()])
    strategy.get_open_positions.return_value.positions[0].value_usd = Decimal("NaN")
    exit_code, _ = _run(strategy, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["steps"][0]["open_positions_after_teardown"][0]["value_usd"] == "unknown"
