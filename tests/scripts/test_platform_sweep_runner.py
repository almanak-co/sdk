from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from scripts import platform_sweep_runner as sweep_runner

_TOKEN_FUNDING = [
    {
        "symbol": "USDC",
        "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "chain": "base",
        "amount": "10000",
        "amount_type": "token",
    }
]
_SWEEP_CONFIG = {"params": {"threshold": [0.1, 0.2], "lookback": [24, 48]}}


def _env(**overrides: str) -> sweep_runner.PlatformSweepEnv:
    values = {
        "SWEEP_ID": "sweep-123",
        "COMMIT_SHA": "a" * 40,
        "GITHUB_CLONE_URL": "https://x-access-token:token@example/repo.git",
        "STRATEGY_CONFIG": json.dumps({"token_funding": _TOKEN_FUNDING}),
        "BACKTEST_CONFIG": '{"start_time":"2024-01-01","end_time":"2024-03-01"}',
        "SWEEP_CONFIG": json.dumps(_SWEEP_CONFIG),
        "GCS_BUCKET": "bucket",
        "PLATFORM_CALLBACK_URL": "https://api.example",
        "PLATFORM_CALLBACK_SECRET": "secret",
    }
    values.update(overrides)
    return sweep_runner.PlatformSweepEnv.from_env(values)


def test_from_env_parses_and_derives_result_path() -> None:
    env = _env()

    assert env.sweep_id == "sweep-123"
    assert env.gcs_result_path == "sweep-results/sweep-123/result.json"
    assert env.strategy_dir == Path("/workspace/strategy")


def test_from_env_rejects_unsafe_sweep_id() -> None:
    with pytest.raises(sweep_runner.PlatformRunnerError, match="SWEEP_ID"):
        _env(SWEEP_ID="sweep/../123")


def test_from_env_rejects_non_sha_commit() -> None:
    with pytest.raises(sweep_runner.PlatformRunnerError, match="COMMIT_SHA"):
        _env(COMMIT_SHA="main")


def test_from_env_requires_sweep_config() -> None:
    with pytest.raises(sweep_runner.PlatformRunnerError, match="SWEEP_CONFIG"):
        _env(SWEEP_CONFIG="   ")


def test_parse_sweep_parameters_accepts_native_scalars() -> None:
    params = sweep_runner.parse_sweep_parameters(
        json.dumps({"params": {"threshold": [0.1, 0.2], "mode": ["fast", "slow"], "enabled": [True, False]}})
    )

    assert params == {
        "threshold": [0.1, 0.2],
        "mode": ["fast", "slow"],
        "enabled": [True, False],
    }


@pytest.mark.parametrize(
    ("sweep_config", "match"),
    [
        ("[]", "must be a JSON object"),
        ("{}", "non-empty 'params' object"),
        ('{"params": {}}', "non-empty 'params' object"),
        ('{"params": {"threshold": []}}', "non-empty list"),
        ('{"params": {"threshold": 0.1}}', "non-empty list"),
        ('{"params": {"threshold": [null]}}', "non-scalar"),
        ('{"params": {"threshold": [[0.1]]}}', "non-scalar"),
        ('{"params": {"threshold": [0.1, 0.1]}}', "duplicate value"),
        ('{"params": {"chain": ["base", "arbitrum"]}}', "reserved"),
        ('{"params": {"token_funding": ["a", "b"]}}', "reserved"),
        ('{"params": {" ": [1]}}', "non-empty strings"),
    ],
)
def test_parse_sweep_parameters_rejects_invalid_shapes(sweep_config: str, match: str) -> None:
    with pytest.raises(sweep_runner.PlatformRunnerError, match=match):
        sweep_runner.parse_sweep_parameters(sweep_config)


def test_parse_sweep_parameters_keeps_bool_and_int_values_distinct() -> None:
    # `in` membership uses ==, under which True == 1 / False == 0; the
    # duplicate check must be type-sensitive so these grids stay valid.
    params = sweep_runner.parse_sweep_parameters(json.dumps({"params": {"a": [1, True], "b": [0, False]}}))

    assert params == {"a": [1, True], "b": [0, False]}


def test_parse_sweep_parameters_caps_grid_size() -> None:
    oversized = {"params": {f"p{i}": list(range(10)) for i in range(3)}}  # 1000 combos

    with pytest.raises(sweep_runner.PlatformRunnerError, match="caps a single job at"):
        sweep_runner.parse_sweep_parameters(json.dumps(oversized))


def test_generate_sweep_combinations_is_deterministic_cartesian_product() -> None:
    combos = sweep_runner.generate_sweep_combinations({"a": [1, 2], "b": ["x", "y"]})

    assert combos == [
        {"a": 1, "b": "x"},
        {"a": 1, "b": "y"},
        {"a": 2, "b": "x"},
        {"a": 2, "b": "y"},
    ]


def test_sweep_callback_url_uses_sweep_segment_and_encodes_id() -> None:
    url = sweep_runner._sweep_callback_url("https://api.example/", "sweep:1", "start")

    assert url == "https://api.example/internal/sweep/sweep%3A1/start"

    with pytest.raises(ValueError, match="Unsupported callback action"):
        sweep_runner._sweep_callback_url("https://api.example", "sweep-1", "delete")


def test_rank_sweep_entries_sorts_by_sharpe_with_failures_last() -> None:
    entries = [
        {"params": {"t": 1}, "result_summary": {"sharpe_ratio": "0.5"}, "error": None},
        {"params": {"t": 2}, "result_summary": None, "error": "boom"},
        {"params": {"t": 3}, "result_summary": {"sharpe_ratio": "1.8"}, "error": None},
        {"params": {"t": 4}, "result_summary": {"sharpe_ratio": "not-a-number"}, "error": None},
    ]

    ranked = sweep_runner.rank_sweep_entries(entries)

    assert [entry["params"]["t"] for entry in ranked] == [3, 1, 4, 2]
    assert [entry["rank"] for entry in ranked] == [1, 2, 3, 4]


def test_build_sweep_summary_reports_best_combo_and_counts() -> None:
    ranked = [
        {"params": {"t": 3}, "result_summary": {"sharpe_ratio": "1.8"}, "error": None, "rank": 1},
        {"params": {"t": 2}, "result_summary": None, "error": "boom", "rank": 2},
    ]

    summary = sweep_runner.build_sweep_summary(ranked, total_combinations=2, elapsed_seconds=12.5)

    assert summary == {
        "total_combinations": 2,
        "succeeded": 1,
        "failed": 1,
        "duration_seconds": 12.5,
        "best_params": {"t": 3},
        "best_result_summary": {"sharpe_ratio": "1.8"},
    }


def test_build_sweep_summary_omits_best_when_top_entry_failed() -> None:
    ranked = [{"params": {"t": 2}, "result_summary": None, "error": "boom", "rank": 1}]

    summary = sweep_runner.build_sweep_summary(ranked, total_combinations=1, elapsed_seconds=3.0)

    assert "best_params" not in summary
    assert summary["failed"] == 1


class _StrategyStub:
    __name__ = "StrategyStub"


class _BacktestConfigStub:
    chain = "base"
    tokens = ["WETH", "USDC"]

    def to_dict(self) -> dict[str, str]:
        return {"chain": self.chain}


def _patch_sweep_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    sharpe_by_threshold: dict[Any, str],
    failing_thresholds: set[Any] = frozenset(),
) -> dict[str, Any]:
    """Wire run_platform_sweep dependencies; per-combo Sharpe keyed by 'threshold'."""
    observed: dict[str, Any] = {
        "order": [],
        "instantiated": [],
        "uploaded": None,
        "callbacks": [],
        "backtesters_created": 0,
    }

    class Backtester:
        # Stateless: one shared instance serves every combo, reading the
        # combo's threshold off the strategy it is handed.
        async def backtest(self, strategy: object, config: object) -> dict[str, Any]:
            threshold = getattr(strategy, "config", {}).get("threshold")
            if threshold in failing_thresholds:
                raise RuntimeError(f"combo blew up at threshold={threshold}")
            return {"threshold": threshold}

        async def close(self) -> None:
            observed["order"].append("close")

    def fake_instantiate(strategy_class: type, config: dict[str, Any], chain: str) -> object:
        observed["instantiated"].append(dict(config))
        instance = type("Instance", (), {"config": config, "deployment_id": ""})()
        return instance

    def fake_create_backtester(*, token_addresses: Any = None, close_providers_on_finish: bool = True) -> Backtester:
        observed["backtesters_created"] += 1
        observed["close_providers_on_finish"] = close_providers_on_finish
        return Backtester()

    def fake_serialize(result: dict[str, Any]) -> dict[str, Any]:
        return {
            "metrics": {"sharpe_ratio": sharpe_by_threshold[result["threshold"]]},
            "trades": [],
            "duration_seconds": 1.0,
        }

    def fake_post_sweep_callback(env: object, payload: Any = None, *, action: str = "complete") -> None:
        observed["callbacks"].append({"action": action, "payload": payload})
        observed["order"].append(action if payload is None else payload["status"])

    def fake_upload(bucket: str, object_path: str, payload: dict[str, Any]) -> None:
        observed["uploaded"] = payload
        observed["order"].append("upload")

    monkeypatch.setattr(sweep_runner, "post_sweep_callback", fake_post_sweep_callback)
    monkeypatch.setattr(sweep_runner, "clone_strategy_repo", lambda env: observed["order"].append("clone") or tmp_path)
    monkeypatch.setattr(
        sweep_runner,
        "load_effective_strategy_config",
        lambda repo_root, raw: {"token_funding": _TOKEN_FUNDING, "threshold": 0.05},
    )
    monkeypatch.setattr(sweep_runner, "prime_strategy_registry", lambda: None)
    monkeypatch.setattr(sweep_runner.os, "chdir", lambda path: None)
    monkeypatch.setattr(sweep_runner, "discover_strategy_class", lambda repo_root, config: _StrategyStub)
    monkeypatch.setattr(
        sweep_runner,
        "build_platform_backtest_config",
        lambda raw, config, strategy_class: _BacktestConfigStub(),
    )
    monkeypatch.setattr(sweep_runner, "instantiate_strategy", fake_instantiate)
    monkeypatch.setattr(sweep_runner, "build_backtest_token_address_map", lambda config, **kwargs: {})
    monkeypatch.setattr(sweep_runner, "create_backtester", fake_create_backtester)
    monkeypatch.setattr(sweep_runner, "serialize_result", fake_serialize)
    monkeypatch.setattr(sweep_runner, "upload_result_to_gcs", fake_upload)
    return observed


def test_run_platform_sweep_ranks_results_and_completes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(SWEEP_CONFIG=json.dumps({"params": {"threshold": [0.1, 0.2, 0.3]}}))
    observed = _patch_sweep_pipeline(
        monkeypatch,
        tmp_path,
        sharpe_by_threshold={0.1: "0.4", 0.2: "2.1", 0.3: "1.3"},
    )

    payload = asyncio.run(sweep_runner.run_platform_sweep(env))

    # Start fires before the clone; upload lands before the COMPLETED callback.
    assert observed["order"][0] == "start"
    assert observed["order"][1] == "clone"
    assert observed["order"][-3:] == ["close", "upload", "COMPLETED"]

    # One shared backtester for the whole sweep, closed exactly once by the
    # orchestration (close_providers_on_finish=False, VIB-5621).
    assert observed["backtesters_created"] == 1
    assert observed["close_providers_on_finish"] is False
    assert observed["order"].count("close") == 1

    # First instantiation is the token-map probe on the base config; every
    # combo after it overrides the base strategy config value.
    assert [config["threshold"] for config in observed["instantiated"]] == [0.05, 0.1, 0.2, 0.3]

    uploaded = observed["uploaded"]
    assert uploaded["sweep_id"] == "sweep-123"
    assert uploaded["sweep_parameters"] == {"threshold": [0.1, 0.2, 0.3]}
    assert [entry["params"]["threshold"] for entry in uploaded["results"]] == [0.2, 0.3, 0.1]
    assert all("result" not in entry for entry in uploaded["results"])
    assert uploaded["best_result"]["metrics"]["sharpe_ratio"] == "2.1"
    assert uploaded["result_summary"]["best_params"] == {"threshold": 0.2}
    assert uploaded["result_summary"]["succeeded"] == 3

    assert payload["status"] == "COMPLETED"
    assert payload["gcs_result_path"] == "sweep-results/sweep-123/result.json"
    assert payload["result_summary"]["best_params"] == {"threshold": 0.2}


def test_run_platform_sweep_isolates_failing_combo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(SWEEP_CONFIG=json.dumps({"params": {"threshold": [0.1, 0.2]}}))
    observed = _patch_sweep_pipeline(
        monkeypatch,
        tmp_path,
        sharpe_by_threshold={0.1: "0.4", 0.2: "2.1"},
        failing_thresholds={0.2},
    )

    payload = asyncio.run(sweep_runner.run_platform_sweep(env))

    uploaded = observed["uploaded"]
    assert uploaded["result_summary"] == payload["result_summary"]
    assert payload["result_summary"]["succeeded"] == 1
    assert payload["result_summary"]["failed"] == 1
    assert payload["result_summary"]["best_params"] == {"threshold": 0.1}
    failed_entry = uploaded["results"][-1]
    assert failed_entry["params"] == {"threshold": 0.2}
    assert "combo blew up" in failed_entry["error"]


def test_run_platform_sweep_fails_when_every_combo_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(SWEEP_CONFIG=json.dumps({"params": {"threshold": [0.1, 0.2]}}))
    observed = _patch_sweep_pipeline(
        monkeypatch,
        tmp_path,
        sharpe_by_threshold={0.1: "0.4", 0.2: "2.1"},
        failing_thresholds={0.1, 0.2},
    )

    with pytest.raises(sweep_runner.PlatformRunnerError, match="All 2 sweep combinations failed"):
        asyncio.run(sweep_runner.run_platform_sweep(env))

    assert observed["uploaded"] is None
    assert "COMPLETED" not in observed["order"]


def test_run_platform_sweep_raises_distinct_error_when_completed_callback_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env = _env(SWEEP_CONFIG=json.dumps({"params": {"threshold": [0.1]}}))
    observed = _patch_sweep_pipeline(monkeypatch, tmp_path, sharpe_by_threshold={0.1: "0.4"})

    original = ConnectionError("callback unreachable")

    def fail_completed_callback(env: object, payload: Any = None, *, action: str = "complete") -> None:
        if payload is not None:
            observed["order"].append(payload["status"])
            raise original
        observed["order"].append(action)

    monkeypatch.setattr(sweep_runner, "post_sweep_callback", fail_completed_callback)

    with pytest.raises(sweep_runner.CompletedCallbackDeliveryError) as exc_info:
        asyncio.run(sweep_runner.run_platform_sweep(env))

    # The result must have been uploaded before the callback was even attempted.
    assert observed["order"][-2:] == ["upload", "COMPLETED"]
    assert exc_info.value.gcs_result_path == env.gcs_result_path
    assert exc_info.value.__cause__ is original
    assert not isinstance(exc_info.value, sweep_runner.PlatformRunnerError)


def test_apply_param_attributes_skips_dict_config_strategies() -> None:
    class DictConfigStrategy:
        config: dict[str, Any] = {}

    class AttrStrategy:
        config = None

    dict_strategy = DictConfigStrategy()
    attr_strategy = AttrStrategy()

    sweep_runner._apply_param_attributes(dict_strategy, {"threshold": 0.5})
    sweep_runner._apply_param_attributes(attr_strategy, {"threshold": 0.5})

    # Dict-config strategies already received the override merged into the
    # combo config before instantiation, so the helper must be a full no-op:
    # no attribute added AND no mutation of the config dict.
    assert not hasattr(dict_strategy, "threshold")
    assert dict_strategy.config == {}
    assert attr_strategy.threshold == 0.5


def test_ensure_combo_deployment_id_prefers_existing_and_private_attr() -> None:
    class HasId:
        deployment_id = "keep-me"

    class PrivateSlot:
        _deployment_id = ""

        @property
        def deployment_id(self) -> str:
            return self._deployment_id

    class Plain:
        deployment_id = ""

    has_id = HasId()
    private_slot = PrivateSlot()
    plain = Plain()

    sweep_runner._ensure_combo_deployment_id(has_id, "sweep-123", 7)
    sweep_runner._ensure_combo_deployment_id(private_slot, "sweep-123", 7)
    sweep_runner._ensure_combo_deployment_id(plain, "sweep-123", 7)

    assert has_id.deployment_id == "keep-me"
    assert private_slot.deployment_id == "sweep-sweep-123-0007"
    assert plain.deployment_id == "sweep-sweep-123-0007"


def test_main_posts_failed_callback_for_env_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    monkeypatch.setenv("SWEEP_ID", "sweep-123")
    monkeypatch.setenv("COMMIT_SHA", "main")
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("SWEEP_CONFIG", json.dumps(_SWEEP_CONFIG))
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    def fake_post_with_retries(url: str, *, headers: dict[str, str], payload: Any) -> None:
        calls.append({"url": url, "headers": headers, "payload": payload})

    monkeypatch.setattr(sweep_runner, "_post_with_retries", fake_post_with_retries)

    assert sweep_runner.main() == 1
    assert calls == [
        {
            "url": "https://api.example/internal/sweep/sweep-123/complete",
            "headers": {"x-almanak-secret-key": "secret"},
            "payload": {
                "status": "FAILED",
                "error_message": "PlatformRunnerError: COMMIT_SHA must be a 40-character git SHA",
            },
        }
    ]


def test_main_does_not_post_failed_when_only_completed_callback_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[Any] = []

    monkeypatch.setenv("SWEEP_ID", "sweep-123")
    monkeypatch.setenv("COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("SWEEP_CONFIG", json.dumps(_SWEEP_CONFIG))
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    async def fake_run_platform_sweep(env: sweep_runner.PlatformSweepEnv) -> dict[str, Any]:
        raise sweep_runner.CompletedCallbackDeliveryError(env.gcs_result_path)

    monkeypatch.setattr(sweep_runner, "run_platform_sweep", fake_run_platform_sweep)
    monkeypatch.setattr(
        sweep_runner, "post_sweep_callback", lambda env, payload=None, action="complete": posted.append(payload)
    )
    monkeypatch.setattr(sweep_runner, "_post_with_retries", lambda url, **kwargs: posted.append(kwargs))

    # Non-zero exit signals the platform to retry, but no FAILED verdict is posted.
    assert sweep_runner.main() == 1
    assert posted == []
