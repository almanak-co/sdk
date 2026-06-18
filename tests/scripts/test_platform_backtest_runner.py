from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from scripts import platform_backtest_runner as runner


def _env(**overrides: str) -> runner.PlatformRunnerEnv:
    values = {
        "BACKTEST_ID": "test-123",
        "COMMIT_SHA": "a" * 40,
        "GITHUB_CLONE_URL": "https://x-access-token:token@example/repo.git",
        "STRATEGY_CONFIG": "{}",
        "BACKTEST_CONFIG": '{"start_time":"2024-01-01","end_time":"2024-03-01","initial_capital_usd":"10000"}',
        "GCS_BUCKET": "bucket",
        "PLATFORM_CALLBACK_URL": "https://api.example",
        "PLATFORM_CALLBACK_SECRET": "secret",
    }
    values.update(overrides)
    return runner.PlatformRunnerEnv.from_env(values)


def test_build_platform_backtest_config_parses_platform_payload() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-03-01",
                "initial_capital_usd": "10000",
                "include_gas_costs": "false",
            }
        ),
        {"base_token": "WETH", "quote_token": "USDC"},
        Strategy,
    )

    assert config.start_time.tzinfo is not None
    assert config.end_time.tzinfo is not None
    assert config.initial_capital_usd == runner.Decimal("10000")
    assert config.chain == "base"
    assert config.tokens == ["WETH", "USDC"]
    assert config.include_gas_costs is False
    assert config.preflight_validation is False
    assert config.allow_hardcoded_fallback is True


def test_build_platform_backtest_config_rejects_non_increasing_time_range() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    with pytest.raises(runner.PlatformRunnerError, match="start_time must be strictly before end_time"):
        runner.build_platform_backtest_config(
            json.dumps(
                {
                    "start_time": "2024-03-01",
                    "end_time": "2024-03-01",
                    "initial_capital_usd": "10000",
                }
            ),
            {},
            Strategy,
        )


def test_load_effective_strategy_config_merges_env_over_repo(tmp_path: Path) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "chain": "arbitrum",
                "strategy_module": "nested.strategy",
                "risk": {"max_slippage": "0.01", "max_size": "1000"},
            }
        ),
        encoding="utf-8",
    )

    effective = runner.load_effective_strategy_config(
        tmp_path,
        json.dumps({"chain": "base", "risk": {"max_size": "2000"}}),
    )

    assert effective == {
        "chain": "base",
        "strategy_module": "nested.strategy",
        "risk": {"max_slippage": "0.01", "max_size": "2000"},
    }
    assert json.loads((tmp_path / "config.json").read_text(encoding="utf-8")) == effective


def test_discover_strategy_prefers_root_strategy_py(tmp_path: Path) -> None:
    (tmp_path / "strategy.py").write_text(
        """
from almanak.framework.strategies import almanak_strategy

@almanak_strategy(name="platform_runner_root_strategy")
class RootStrategy:
    deployment_id = "root"
    def decide(self, market):
        return None
""",
        encoding="utf-8",
    )
    package_dir = tmp_path / "pkg"
    package_dir.mkdir()
    (package_dir / "other.py").write_text(
        """
from almanak.framework.strategies import almanak_strategy

@almanak_strategy(name="platform_runner_other_strategy")
class OtherStrategy:
    deployment_id = "other"
    def decide(self, market):
        return None
""",
        encoding="utf-8",
    )

    strategy_class = runner.discover_strategy_class(tmp_path, {"strategy_module": "pkg.other"})

    assert strategy_class.__name__ == "RootStrategy"


def test_discover_strategy_uses_config_strategy_module(tmp_path: Path) -> None:
    package_dir = tmp_path / "pkg"
    package_dir.mkdir()
    (package_dir / "strategy_mod.py").write_text(
        """
from almanak.framework.strategies import almanak_strategy

@almanak_strategy(name="platform_runner_config_strategy")
class ConfigStrategy:
    deployment_id = "config"
    def decide(self, market):
        return None
""",
        encoding="utf-8",
    )

    strategy_class = runner.discover_strategy_class(tmp_path, {"strategy_module": "pkg.strategy_mod"})

    assert strategy_class.__name__ == "ConfigStrategy"


def test_discover_strategy_supports_nested_relative_imports(tmp_path: Path) -> None:
    package_dir = tmp_path / "pkg"
    package_dir.mkdir()
    (package_dir / "helper.py").write_text('MARKER = "relative-import-ok"\n', encoding="utf-8")
    (package_dir / "strategy_mod.py").write_text(
        """
from .helper import MARKER
from almanak.framework.strategies import almanak_strategy

@almanak_strategy(name="platform_runner_relative_import_strategy")
class ConfigStrategy:
    deployment_id = "config"
    marker = MARKER
    def decide(self, market):
        return None
""",
        encoding="utf-8",
    )

    strategy_class = runner.discover_strategy_class(tmp_path, {"strategy_module": "pkg.strategy_mod"})

    assert strategy_class.__name__ == "ConfigStrategy"
    assert strategy_class.marker == "relative-import-ok"


def test_discover_strategy_scans_for_decorator(tmp_path: Path) -> None:
    (tmp_path / "plain.py").write_text("class Plain: pass\n", encoding="utf-8")
    (tmp_path / "z_strategy.py").write_text(
        """
from almanak.framework.strategies import almanak_strategy

@almanak_strategy(name="platform_runner_scanned_strategy")
class ScannedStrategy:
    deployment_id = "scanned"
    def decide(self, market):
        return None
""",
        encoding="utf-8",
    )

    strategy_class = runner.discover_strategy_class(tmp_path, {})

    assert strategy_class.__name__ == "ScannedStrategy"


def test_build_result_summary_uses_metrics_and_trade_fallback() -> None:
    summary = runner.build_result_summary(
        {
            "metrics": {
                "total_return_pct": "12.4",
                "sharpe_ratio": "1.8",
                "max_drawdown_pct": "8.2",
                "net_pnl_usd": "1240.00",
            },
            "trades": [{"id": 1}, {"id": 2}],
            "duration_seconds": 87.3,
        },
        elapsed_seconds=100.0,
    )

    assert summary == {
        "total_return_pct": "12.4",
        "sharpe_ratio": "1.8",
        "max_drawdown_pct": "8.2",
        "total_trades": 2,
        "net_pnl_usd": "1240.00",
        "duration_seconds": 87.3,
    }


def test_clone_strategy_repo_separates_options_from_clone_url(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    calls: list[list[str]] = []

    def fake_run_git(
        args: list[str],
        *,
        cwd: Path | None = None,
        env: runner.PlatformRunnerEnv,
    ) -> str:
        calls.append(args)
        if args == ["git", "rev-parse", "HEAD"]:
            return env.commit_sha
        return ""

    monkeypatch.setattr(runner, "_run_git", fake_run_git)

    repo_root = runner.clone_strategy_repo(env)

    assert repo_root == tmp_path / "strategy"
    assert calls[0] == ["git", "clone", "--no-checkout", "--", env.github_clone_url, str(repo_root)]


def test_redact_masks_general_url_credentials() -> None:
    env = _env(
        GITHUB_CLONE_URL="https://oauth2:token@example/repo.git",
        PLATFORM_CALLBACK_SECRET="callback-secret",
    )

    redacted = runner._redact(
        "clone https://oauth2:token@example/repo.git failed; fallback https://user:pass@host/repo.git "
        "callback-secret",
        env,
    )

    assert "token" not in redacted
    assert "pass" not in redacted
    assert "callback-secret" not in redacted
    assert "GITHUB_CLONE_URL" in redacted
    assert "https://***@host/repo.git" in redacted


def test_instantiate_strategy_does_not_swallow_internal_type_error() -> None:
    class BrokenStrategy:
        def __init__(self, config: dict[str, Any]) -> None:
            raise TypeError("internal constructor bug")

    with pytest.raises(TypeError, match="internal constructor bug"):
        runner.instantiate_strategy(BrokenStrategy, {}, "base")


def test_post_callback_values_retries_transient_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    sleeps: list[float] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: Any) -> Response:
        calls.append({"url": url, "kwargs": kwargs})
        if len(calls) < 3:
            raise runner.requests.ConnectionError("temporary network failure")
        return Response()

    monkeypatch.setattr(runner.requests, "post", fake_post)
    monkeypatch.setattr(runner.time, "sleep", lambda delay: sleeps.append(delay))

    runner.post_callback_values(
        platform_callback_url="https://api.example",
        backtest_id="test-123",
        platform_callback_secret="secret",
        payload={"status": "COMPLETED"},
    )

    assert [call["url"] for call in calls] == [
        "https://api.example/internal/backtest/test-123/complete",
        "https://api.example/internal/backtest/test-123/complete",
        "https://api.example/internal/backtest/test-123/complete",
    ]
    assert calls[-1]["kwargs"]["json"] == {"status": "COMPLETED"}
    assert sleeps == [1.0, 2.0]


def test_post_start_callback_uses_start_endpoint_without_json(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: Any) -> Response:
        calls.append({"url": url, "kwargs": kwargs})
        return Response()

    monkeypatch.setattr(runner.requests, "post", fake_post)

    runner.post_start_callback(_env())

    assert calls == [
        {
            "url": "https://api.example/internal/backtest/test-123/start",
            "kwargs": {
                "headers": {"x-almanak-secret-key": "secret"},
                "timeout": 30,
            },
        }
    ]


def test_run_platform_backtest_posts_start_before_clone(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    order: list[str] = []

    class Strategy:
        __name__ = "Strategy"

    class BacktestConfig:
        chain = "base"

        def to_dict(self) -> dict[str, str]:
            return {"chain": self.chain}

    class Backtester:
        async def backtest(self, strategy: object, config: BacktestConfig) -> object:
            order.append("backtest")
            return object()

        async def close(self) -> None:
            order.append("close")

    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: order.append("start"))
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: order.append("clone") or tmp_path)
    monkeypatch.setattr(runner, "load_effective_strategy_config", lambda repo_root, raw_config: {})
    monkeypatch.setattr(runner, "prime_strategy_registry", lambda: None)
    monkeypatch.setattr(runner.os, "chdir", lambda path: order.append("chdir"))
    monkeypatch.setattr(runner, "discover_strategy_class", lambda repo_root, strategy_config: Strategy)
    monkeypatch.setattr(runner, "build_platform_backtest_config", lambda raw_config, strategy_config, strategy_class: BacktestConfig())
    monkeypatch.setattr(runner, "instantiate_strategy", lambda strategy_class, strategy_config, chain: object())
    monkeypatch.setattr(runner, "create_backtester", lambda: Backtester())
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda bucket, object_path, payload: order.append("upload"))
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: order.append(payload["status"]))

    asyncio.run(runner.run_platform_backtest(env))

    assert order[:2] == ["start", "clone"]
    assert order[-2:] == ["upload", "COMPLETED"]


def test_from_env_rejects_non_sha_commit() -> None:
    with pytest.raises(runner.PlatformRunnerError, match="COMMIT_SHA"):
        runner.PlatformRunnerEnv.from_env(
            {
                "BACKTEST_ID": "test-123",
                "COMMIT_SHA": "main",
                "GITHUB_CLONE_URL": "https://x-access-token:token@example/repo.git",
                "STRATEGY_CONFIG": "{}",
                "BACKTEST_CONFIG": "{}",
                "GCS_BUCKET": "bucket",
                "PLATFORM_CALLBACK_URL": "https://api.example",
                "PLATFORM_CALLBACK_SECRET": "secret",
            }
        )


def test_main_posts_failed_callback_for_env_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", "main")
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    def fake_post_callback_values(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(runner, "post_callback_values", fake_post_callback_values)

    assert runner.main() == 1
    assert calls == [
        {
            "platform_callback_url": "https://api.example",
            "backtest_id": "test-123",
            "platform_callback_secret": "secret",
            "payload": {
                "status": "FAILED",
                "error_message": "PlatformRunnerError: COMMIT_SHA must be a 40-character git SHA",
            },
        }
    ]


def test_main_posts_failed_callback_for_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    commit_sha = "a" * 40

    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", commit_sha)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    async def fake_run_platform_backtest(env: runner.PlatformRunnerEnv) -> dict[str, Any]:
        raise runner.PlatformRunnerError("strategy not found")

    def fake_post_callback(env: runner.PlatformRunnerEnv, payload: dict[str, Any]) -> None:
        calls.append({"env": env, "payload": payload})

    monkeypatch.setattr(runner, "run_platform_backtest", fake_run_platform_backtest)
    monkeypatch.setattr(runner, "post_callback", fake_post_callback)

    assert runner.main() == 1
    assert calls[0]["env"].backtest_id == "test-123"
    assert calls[0]["payload"] == {
        "status": "FAILED",
        "error_message": "PlatformRunnerError: strategy not found",
    }
