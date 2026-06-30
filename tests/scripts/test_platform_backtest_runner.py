from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig, MarketState
from almanak.framework.intents.vocabulary import SwapIntent
from scripts import platform_backtest_runner as runner

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
_BASE_CHAIN_ID = 8453


class _AddressKeyedProvider:
    """Network-free provider that emits only address-native price keys."""

    provider_name = "address-keyed-platform-test"

    def __init__(self, token_addresses: dict[str, tuple[str, str]]) -> None:
        self._token_addresses = {
            symbol.upper(): (chain.lower(), address.lower())
            for symbol, (chain, address) in token_addresses.items()
        }
        self.registered: list[dict[str, tuple[str, str]]] = []

    def register_token_addresses(self, token_addresses: dict[str, tuple[str, str]]) -> None:
        normalized = {
            symbol.upper(): (chain.lower(), address.lower())
            for symbol, (chain, address) in token_addresses.items()
        }
        self._token_addresses.update(normalized)
        self.registered.append(normalized)

    async def close(self) -> None:
        return None

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            yield (
                current,
                MarketState(
                    timestamp=current,
                    prices={
                        ("base", BASE_CBBTC): Decimal("60000") - Decimal(index * 100),
                        ("base", BASE_USDC): Decimal("1"),
                    },
                    chain="base",
                    block_number=10_000_000 + index,
                    gas_price_gwei=Decimal("0"),
                ),
            )
            current += timedelta(seconds=config.interval_seconds)
            index += 1


class _CbBtcPlatformStrategy:
    STRATEGY_METADATA = type(
        "Meta",
        (),
        {
            "default_chain": "base",
            "supported_chains": ["base"],
            "supported_protocols": ["uniswap_v3"],
            "intent_types": ["SWAP"],
            "tags": ["swap", "trading"],
            "quote_asset": QuoteAsset.token(_BASE_CHAIN_ID, BASE_CBBTC),
        },
    )()
    quote_asset = QuoteAsset.token(_BASE_CHAIN_ID, BASE_CBBTC)
    deployment_id = "cbbtc-platform-address-keyed-test"

    def __init__(self) -> None:
        self._sent_swap = False

    def decide(self, market: Any) -> Any:
        if self._sent_swap:
            return None
        self._sent_swap = True
        return SwapIntent(
            from_token=BASE_USDC,
            to_token=BASE_CBBTC,
            amount_usd=Decimal("100"),
            protocol="uniswap_v3",
            chain="base",
        )


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


def test_build_platform_backtest_config_resolves_address_token_fields() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-03-01",
                "initial_capital_usd": "10000",
            }
        ),
        {"base_token_address": BASE_CBBTC, "quote_token_address": BASE_USDC},
        Strategy,
    )

    assert config.chain == "base"
    assert config.tokens == ["CBBTC", "USDC"]


def test_build_platform_backtest_config_uses_generic_token_address_field_without_defaults() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-03-01",
                "initial_capital_usd": "10000",
            }
        ),
        {"entry_token_address": BASE_CBBTC},
        Strategy,
    )

    assert config.chain == "base"
    assert config.tokens == ["CBBTC"]


def test_build_platform_backtest_config_adds_decorator_quote_asset() -> None:
    class Strategy:
        STRATEGY_METADATA = type(
            "Meta",
            (),
            {
                "default_chain": "base",
                "supported_chains": ["base"],
                "quote_asset": QuoteAsset.token(8453, BASE_CBBTC),
            },
        )()

    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-03-01",
                "initial_capital_usd": "10000",
            }
        ),
        {"from_token": "USDC", "to_token": "WETH"},
        Strategy,
    )

    assert config.tokens == ["USDC", "WETH", "CBBTC"]


def test_platform_numeraire_backtest_prices_address_keyed_data_and_coverage() -> None:
    strategy_config = {"base_token_address": BASE_CBBTC, "quote_token_address": BASE_USDC}
    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-01-01T03:00:00Z",
                "initial_capital_usd": "10000",
                "include_gas_costs": False,
                "institutional_mode": True,
            }
        ),
        strategy_config,
        _CbBtcPlatformStrategy,
    )
    strategy = _CbBtcPlatformStrategy()
    token_addresses = runner.build_backtest_token_address_map(
        config,
        strategy=strategy,
        strategy_config=strategy_config,
    )
    provider = _AddressKeyedProvider(token_addresses)
    backtester = runner.create_backtester(token_addresses=token_addresses)
    original_provider = backtester.data_provider
    asyncio.run(original_provider.close())
    backtester.data_provider = provider

    result = asyncio.run(backtester.backtest(strategy, config))

    assert config.tokens == ["CBBTC", "USDC"]
    assert token_addresses == {
        "CBBTC": ("base", BASE_CBBTC),
        "USDC": ("base", BASE_USDC),
    }
    assert provider.registered == [
        {
            "CBBTC": ("base", BASE_CBBTC),
            "USDC": ("base", BASE_USDC),
        }
    ]
    assert result.error is None
    assert len(result.trades) > 0
    assert result.metrics.numeraire_metrics is not None
    assert result.data_quality is not None
    assert result.data_quality.coverage_ratio == Decimal("1")
    assert result.institutional_compliance is True


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
        tokens = ["WETH", "USDC"]

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
    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda bucket, object_path, payload: order.append("upload"))
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: order.append(payload["status"]))

    asyncio.run(runner.run_platform_backtest(env))

    assert order[:2] == ["start", "clone"]
    assert order[-2:] == ["upload", "COMPLETED"]


def test_run_platform_backtest_threads_token_addresses(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    captured: list[dict[str, tuple[str, str]] | None] = []

    class Strategy:
        __name__ = "Strategy"

    class BacktestConfig:
        chain = "base"
        tokens = ["CBBTC", "USDC"]

        def to_dict(self) -> dict[str, object]:
            return {"chain": self.chain, "tokens": self.tokens}

    class Backtester:
        async def backtest(self, strategy: object, config: BacktestConfig) -> object:
            return object()

        async def close(self) -> None:
            return None

    def fake_create_backtester(*, token_addresses: dict[str, tuple[str, str]] | None = None) -> Backtester:
        captured.append(token_addresses)
        return Backtester()

    strategy_config = {"base_token_address": BASE_CBBTC, "quote_token_address": BASE_USDC}

    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: None)
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: tmp_path)
    monkeypatch.setattr(runner, "load_effective_strategy_config", lambda repo_root, raw_config: strategy_config)
    monkeypatch.setattr(runner, "prime_strategy_registry", lambda: None)
    monkeypatch.setattr(runner.os, "chdir", lambda path: None)
    monkeypatch.setattr(runner, "discover_strategy_class", lambda repo_root, current_config: Strategy)
    monkeypatch.setattr(runner, "build_platform_backtest_config", lambda raw_config, current_config, strategy_class: BacktestConfig())
    monkeypatch.setattr(runner, "instantiate_strategy", lambda strategy_class, current_config, chain: object())
    monkeypatch.setattr(runner, "create_backtester", fake_create_backtester)
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda bucket, object_path, payload: None)
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: None)

    asyncio.run(runner.run_platform_backtest(env))

    assert captured == [
        {
            "CBBTC": ("base", BASE_CBBTC),
            "USDC": ("base", BASE_USDC),
        }
    ]


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


def _patch_successful_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    """Wire run_platform_backtest dependencies so the run reaches the COMPLETED callback."""
    order: list[str] = []

    class Strategy:
        __name__ = "Strategy"

    class BacktestConfig:
        chain = "base"
        tokens = ["WETH", "USDC"]

        def to_dict(self) -> dict[str, str]:
            return {"chain": self.chain}

    class Backtester:
        async def backtest(self, strategy: object, config: BacktestConfig) -> object:
            return object()

        async def close(self) -> None:
            return None

    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: order.append("start"))
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: tmp_path)
    monkeypatch.setattr(runner, "load_effective_strategy_config", lambda repo_root, raw_config: {})
    monkeypatch.setattr(runner, "prime_strategy_registry", lambda: None)
    monkeypatch.setattr(runner.os, "chdir", lambda path: None)
    monkeypatch.setattr(runner, "discover_strategy_class", lambda repo_root, strategy_config: Strategy)
    monkeypatch.setattr(runner, "build_platform_backtest_config", lambda raw_config, strategy_config, strategy_class: BacktestConfig())
    monkeypatch.setattr(runner, "instantiate_strategy", lambda strategy_class, strategy_config, chain: object())
    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda bucket, object_path, payload: order.append("upload"))
    return order


def test_run_platform_backtest_raises_distinct_error_when_completed_callback_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    order = _patch_successful_run(monkeypatch, tmp_path)

    original = runner.requests.ConnectionError("callback unreachable")

    def fail_completed_callback(current_env: runner.PlatformRunnerEnv, payload: dict[str, Any]) -> None:
        order.append(payload["status"])
        raise original

    monkeypatch.setattr(runner, "post_callback", fail_completed_callback)

    with pytest.raises(runner.CompletedCallbackDeliveryError) as exc_info:
        asyncio.run(runner.run_platform_backtest(env))

    # The result must have been uploaded before the callback was even attempted.
    assert order == ["start", "upload", "COMPLETED"]
    assert exc_info.value.gcs_result_path == env.gcs_result_path
    assert exc_info.value.__cause__ is original
    assert not isinstance(exc_info.value, runner.PlatformRunnerError)


def test_main_does_not_post_failed_when_only_completed_callback_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []
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
        raise runner.CompletedCallbackDeliveryError(env.gcs_result_path)

    monkeypatch.setattr(runner, "run_platform_backtest", fake_run_platform_backtest)
    monkeypatch.setattr(runner, "post_callback", lambda env, payload: posted.append(payload))
    monkeypatch.setattr(runner, "post_callback_values", lambda **kwargs: posted.append(kwargs))

    # Non-zero exit signals the platform to retry, but no FAILED verdict is posted.
    assert runner.main() == 1
    assert posted == []
