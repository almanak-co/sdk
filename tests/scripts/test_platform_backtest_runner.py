from __future__ import annotations

import asyncio
import importlib
import json
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from almanak._version import __version__
from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig, MarketState
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.services.backtest.platform_artifacts import (
    PLATFORM_BACKTEST_TERMINAL_KIND,
    PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION,
)
from scripts import platform_backtest_runner as runner

BASE_CBBTC = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
BASE_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
_BASE_CHAIN_ID = 8453
_TOKEN_FUNDING = [
    {
        "symbol": "USDC",
        "address": BASE_USDC,
        "chain": "base",
        "amount": "10000",
        "amount_type": "token",
    }
]


class _AddressKeyedProvider:
    """Network-free provider that emits only address-native price keys."""

    provider_name = "address-keyed-platform-test"

    def __init__(self, token_addresses: dict[str, tuple[str, str]]) -> None:
        self._token_addresses = {
            symbol.upper(): (chain.lower(), address.lower()) for symbol, (chain, address) in token_addresses.items()
        }
        self.registered: list[dict[str, tuple[str, str]]] = []

    def register_token_addresses(self, token_addresses: dict[str, tuple[str, str]]) -> None:
        normalized = {
            symbol.upper(): (chain.lower(), address.lower()) for symbol, (chain, address) in token_addresses.items()
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
        "STRATEGY_CONFIG": json.dumps({"token_funding": _TOKEN_FUNDING}),
        "BACKTEST_CONFIG": '{"start_time":"2024-01-01","end_time":"2024-03-01"}',
        "GCS_BUCKET": "bucket",
        "PLATFORM_CALLBACK_URL": "https://api.example",
        "PLATFORM_CALLBACK_SECRET": "secret",
    }
    values.update(overrides)
    return runner.PlatformRunnerEnv.from_env(values)


def test_from_env_honors_platform_result_uri_and_derives_siblings() -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")

    assert env.gcs_result_path == "backtests/test-123/result.json"
    assert env.gcs_result_uri == "gs://bucket/backtests/test-123/result.json"
    assert env.gcs_decisions_path == "backtests/test-123/decisions.jsonl"
    assert env.gcs_terminal_path == "backtests/test-123/terminal.json"


@pytest.mark.parametrize(
    "configured_path",
    [
        "gs://other-bucket/backtests/test-123/result.json",
        "gs://bucket/backtests/other-run/result.json",
        "gs://bucket/backtests/test-123/../result.json",
    ],
)
def test_from_env_rejects_unsafe_platform_result_uri(configured_path: str) -> None:
    with pytest.raises(runner.PlatformRunnerError, match="GCS_RESULT_PATH"):
        _env(GCS_RESULT_PATH=configured_path)


def test_build_platform_backtest_config_parses_platform_payload() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-03-01",
                "include_gas_costs": "false",
            }
        ),
        {"base_token": "WETH", "quote_token": "USDC", "token_funding": _TOKEN_FUNDING},
        Strategy,
    )

    assert config.start_time.tzinfo is not None
    assert config.end_time.tzinfo is not None
    assert config.token_funding == _TOKEN_FUNDING
    assert config.chain == "base"
    assert config.tokens == ["WETH", "USDC"]
    assert config.include_gas_costs is False
    assert config.preflight_validation is False
    assert config.allow_hardcoded_fallback is True


def test_build_platform_backtest_config_infers_address_first_token_objects() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "polygon", "supported_chains": ["polygon"]})()

    config = runner.build_platform_backtest_config(
        json.dumps({"start_time": "2026-02-11", "end_time": "2026-02-12"}),
        {
            "base_token": {"symbol": "WBTC", "address": "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6"},
            "quote_token": {"symbol": "WETH", "address": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"},
            "token_funding": _TOKEN_FUNDING,
        },
        Strategy,
    )

    # Known addresses canonicalize back to their symbols; importantly the
    # object-valued config no longer falls through to the WETH/USDC default.
    assert config.tokens == ["WBTC", "WETH"]


def test_platform_native_perp_price_timeframe_defaults_to_auto() -> None:
    config = SimpleNamespace(timeframe=None)
    strategy = SimpleNamespace(config={"protocol": "gmx_v2", "market": "ETH/USD"})

    runner.apply_platform_price_timeframe_default("{}", config, strategy=strategy)

    assert config.timeframe == "auto"


@pytest.mark.parametrize("explicit", ["1h", None])
def test_platform_explicit_price_timeframe_is_never_replaced(
    monkeypatch: pytest.MonkeyPatch,
    explicit: str | None,
) -> None:
    config = SimpleNamespace(timeframe=explicit)
    monkeypatch.setattr(
        runner,
        "coverage_aware_default_timeframe",
        lambda current: pytest.fail("explicit timeframe must bypass default discovery"),
    )

    runner.apply_platform_price_timeframe_default(
        json.dumps({"timeframe": explicit}),
        config,
        strategy=object(),
    )

    assert config.timeframe == explicit


def test_platform_non_native_strategy_keeps_legacy_price_timeframe(monkeypatch: pytest.MonkeyPatch) -> None:
    config = SimpleNamespace(timeframe=None)
    monkeypatch.setattr(runner, "coverage_aware_default_timeframe", lambda current: None)

    runner.apply_platform_price_timeframe_default("{}", config, strategy=object())

    assert config.timeframe is None


def test_platform_funding_only_non_native_token_is_registered_in_address_map() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    strategy_config = {
        "token": "USDC",
        "token_funding": [
            {
                "symbol": "cbBTC",
                "address": BASE_CBBTC,
                "chain": "base",
                "amount": "100",
                "amount_type": "usd",
            }
        ],
    }
    config = runner.build_platform_backtest_config(
        json.dumps({"start_time": "2024-01-01", "end_time": "2024-03-01"}),
        strategy_config,
        Strategy,
    )

    token_addresses = runner.build_backtest_token_address_map(config, strategy_config=strategy_config)

    assert config.tokens == ["USDC"]
    assert token_addresses["CBBTC"] == ("base", BASE_CBBTC)


def test_build_platform_backtest_config_resolves_address_token_fields() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-03-01",
            }
        ),
        {"base_token_address": BASE_CBBTC, "quote_token_address": BASE_USDC, "token_funding": _TOKEN_FUNDING},
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
            }
        ),
        {"entry_token_address": BASE_CBBTC, "token_funding": _TOKEN_FUNDING},
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
            }
        ),
        {"from_token": "USDC", "to_token": "WETH", "token_funding": _TOKEN_FUNDING},
        Strategy,
    )

    assert config.tokens == ["USDC", "WETH", "CBBTC"]


def test_platform_numeraire_backtest_prices_address_keyed_data_and_coverage() -> None:
    strategy_config = {
        "base_token_address": BASE_CBBTC,
        "quote_token_address": BASE_USDC,
        "token_funding": _TOKEN_FUNDING,
    }
    config = runner.build_platform_backtest_config(
        json.dumps(
            {
                "start_time": "2024-01-01",
                "end_time": "2024-01-01T03:00:00Z",
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
    # The run chain's native rides along unconditionally (ALM-3067).
    assert token_addresses == {
        "CBBTC": ("base", BASE_CBBTC),
        "USDC": ("base", BASE_USDC),
        "ETH": ("base", NATIVE_SENTINEL.lower()),
    }
    assert provider.registered == [
        {
            "CBBTC": ("base", BASE_CBBTC),
            "USDC": ("base", BASE_USDC),
            "ETH": ("base", NATIVE_SENTINEL.lower()),
        }
    ]
    assert result.error is None
    assert len(result.trades) > 0
    # Numeraire-canonical merge (blueprint 31 §7): the CBBTC quote asset is
    # priced through the address-keyed provider and becomes the canonical
    # performance denomination; the legacy sub-block is no longer attached.
    assert result.metrics.performance_denomination == "CBBTC"
    assert result.metrics.total_pnl_numeraire is not None
    assert result.metrics.numeraire_price_usd_end is not None
    assert result.metrics.total_pnl_usd == result.metrics.total_pnl_numeraire * result.metrics.numeraire_price_usd_end
    assert result.metrics.numeraire_metrics is None
    assert result.data_quality is not None
    assert result.data_quality.coverage_ratio == Decimal("1")
    assert result.institutional_compliance is True


def test_build_platform_backtest_config_requires_token_funding() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    with pytest.raises(runner.PlatformRunnerError, match="token_funding"):
        runner.build_platform_backtest_config(
            json.dumps(
                {
                    "start_time": "2024-01-01",
                    "end_time": "2024-03-01",
                }
            ),
            {},
            Strategy,
        )


def test_build_platform_backtest_config_wraps_invalid_token_funding_shape() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    with pytest.raises(runner.PlatformRunnerError, match="BACKTEST_CONFIG is invalid: token_funding must be a list"):
        runner.build_platform_backtest_config(
            json.dumps(
                {
                    "start_time": "2024-01-01",
                    "end_time": "2024-03-01",
                    "token_funding": {"symbol": "USDC"},
                }
            ),
            {},
            Strategy,
        )


def test_build_platform_backtest_config_rejects_non_increasing_time_range() -> None:
    class Strategy:
        STRATEGY_METADATA = type("Meta", (), {"default_chain": "base", "supported_chains": ["base"]})()

    with pytest.raises(runner.PlatformRunnerError, match="start_time must be strictly before end_time"):
        runner.build_platform_backtest_config(
            json.dumps(
                {
                    "start_time": "2024-03-01",
                    "end_time": "2024-03-01",
                }
            ),
            {"token_funding": _TOKEN_FUNDING},
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
        "metrics_schema_version": 3,
        "performance_denomination": "USD",
        "total_return_pct": "12.4",
        "sharpe_ratio": "1.8",
        "max_drawdown_pct": "8.2",
        "total_trades": 2,
        "net_pnl_usd": "1240.00",
        "duration_seconds": 87.3,
    }


def test_build_result_summary_carries_numeraire_denomination() -> None:
    """Token-quoted runs headline their denomination and native PnL."""
    summary = runner.build_result_summary(
        {
            "metrics": {
                "performance_denomination": "CBBTC",
                "total_return_pct": "-4.35",
                "sharpe_ratio": "-3.47",
                "max_drawdown_pct": "0.0699",
                "net_pnl_usd": "-14.96",
                "net_pnl_numeraire": "-0.000255625736414282553855967989",
                "total_trades": 46,
            },
            "trades": [],
            "duration_seconds": 17.9,
        },
        elapsed_seconds=20.0,
    )

    assert summary["performance_denomination"] == "CBBTC"
    assert summary["net_pnl_usd"] == "-14.96"
    assert summary["net_pnl_numeraire"] == "-0.000255625736414282553855967989"
    assert summary["total_trades"] == 46


def test_build_result_summary_carries_metrics_schema_version() -> None:
    summary = runner.build_result_summary(
        {"metrics": {"schema_version": 4}, "trades": []},
        elapsed_seconds=1.0,
    )

    assert summary["metrics_schema_version"] == 4


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
        "clone https://oauth2:token@example/repo.git failed; fallback https://user:pass@host/repo.git callback-secret",
        env,
    )

    assert "token" not in redacted
    assert "pass" not in redacted
    assert "callback-secret" not in redacted
    assert str(env.strategy_dir) not in runner._redact(f"failed in {env.strategy_dir}", env)
    assert "GITHUB_CLONE_URL" in redacted
    assert "https://***@host/repo.git" in redacted


def test_redact_json_value_preserves_tuple_shape_and_redacts_nested_values() -> None:
    env = _env()
    value = (
        "https://user:password@example.test/top",
        {"nested": (env.github_clone_url, env.platform_callback_secret)},
    )

    redacted = runner._redact_json_value(value, env)

    assert isinstance(redacted, tuple)
    assert isinstance(redacted[1]["nested"], tuple)
    assert redacted == (
        "https://***@example.test/top",
        {"nested": ("GITHUB_CLONE_URL", "PLATFORM_CALLBACK_SECRET")},
    )


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
    monkeypatch.setattr(
        runner, "build_platform_backtest_config", lambda raw_config, strategy_config, strategy_class: BacktestConfig()
    )
    monkeypatch.setattr(runner, "instantiate_strategy", lambda strategy_class, strategy_config, chain: object())
    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: None)
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(
        runner, "upload_result_to_gcs", lambda bucket, object_path, payload: order.append("upload") or "42"
    )
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: order.append(payload["status"]))

    asyncio.run(runner.run_platform_backtest(env))

    assert order[:2] == ["start", "clone"]
    assert order[-3:] == ["upload", "upload", "COMPLETED"]


def test_run_platform_backtest_threads_token_addresses(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(
        STRATEGY_WORKDIR=str(tmp_path / "strategy"),
        # data_config rides BACKTEST_CONFIG; the fake below asserts the exact
        # mapping reaches create_backtester (CodeRabbit, #3271: a discarded
        # kwarg would let a propagation regression pass silently).
        BACKTEST_CONFIG='{"start_time":"2024-01-01","end_time":"2024-03-01",'
        '"data_config":{"allow_volume_fallback":true,"subgraph_rate_limit_per_minute":42}}',
    )
    captured: list[dict[str, tuple[str, str]] | None] = []
    captured_overrides: list[dict | None] = []

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

    def fake_create_backtester(
        *,
        token_addresses: dict[str, tuple[str, str]] | None = None,
        data_config_overrides: dict | None = None,
    ) -> Backtester:
        captured.append(token_addresses)
        captured_overrides.append(data_config_overrides)
        return Backtester()

    strategy_config = {"base_token_address": BASE_CBBTC, "quote_token_address": BASE_USDC}

    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: None)
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: tmp_path)
    monkeypatch.setattr(runner, "load_effective_strategy_config", lambda repo_root, raw_config: strategy_config)
    monkeypatch.setattr(runner, "prime_strategy_registry", lambda: None)
    monkeypatch.setattr(runner.os, "chdir", lambda path: None)
    monkeypatch.setattr(runner, "discover_strategy_class", lambda repo_root, current_config: Strategy)
    monkeypatch.setattr(
        runner, "build_platform_backtest_config", lambda raw_config, current_config, strategy_class: BacktestConfig()
    )
    monkeypatch.setattr(runner, "instantiate_strategy", lambda strategy_class, current_config, chain: object())
    monkeypatch.setattr(runner, "create_backtester", fake_create_backtester)
    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: None)
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda bucket, object_path, payload: "42")
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: None)

    asyncio.run(runner.run_platform_backtest(env))

    assert captured == [
        {
            "CBBTC": ("base", BASE_CBBTC),
            "USDC": ("base", BASE_USDC),
            # The run chain's native is always registered (ALM-3067) — the
            # balance plane exists whether or not the run mentions it.
            "ETH": ("base", NATIVE_SENTINEL.lower()),
        }
    ]
    assert captured_overrides == [{"allow_volume_fallback": True, "subgraph_rate_limit_per_minute": 42}]


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
    error_message = "PlatformRunnerError: COMMIT_SHA must be a 40-character git SHA"
    assert calls == [
        {
            "platform_callback_url": "https://api.example",
            "backtest_id": "test-123",
            "platform_callback_secret": "secret",
            "payload": {
                "status": "FAILED",
                "error_message": error_message,
                "result_summary": {
                    "failure_stage": "infra",
                    "code": "INFRA_ERROR",
                    "blockers": [
                        {
                            "code": "INFRA_ERROR",
                            "message": error_message,
                            "failed_checks": [],
                            "recommendations": [],
                        }
                    ],
                },
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
        "result_summary": {
            "failure_stage": "infra",
            "code": "INFRA_ERROR",
            "blockers": [
                {
                    "code": "INFRA_ERROR",
                    "message": "PlatformRunnerError: strategy not found",
                    "failed_checks": [],
                    "recommendations": [],
                }
            ],
        },
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
    monkeypatch.setattr(
        runner, "build_platform_backtest_config", lambda raw_config, strategy_config, strategy_class: BacktestConfig()
    )
    monkeypatch.setattr(runner, "instantiate_strategy", lambda strategy_class, strategy_config, chain: object())
    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: None)
    monkeypatch.setattr(runner, "serialize_result", lambda result: {"metrics": {}, "trades": []})
    monkeypatch.setattr(
        runner, "upload_result_to_gcs", lambda bucket, object_path, payload: order.append("upload") or "42"
    )
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
    assert order == ["start", "upload", "upload", "COMPLETED"]
    assert exc_info.value.gcs_result_path == env.gcs_result_uri
    assert exc_info.value.__cause__ is original
    assert not isinstance(exc_info.value, runner.PlatformRunnerError)


def test_run_platform_backtest_does_not_post_callback_when_terminal_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    order = _patch_successful_run(monkeypatch, tmp_path)
    original = ConnectionError("terminal upload unavailable")

    def fail_terminal_upload(bucket: str, object_path: str, payload: dict[str, Any]) -> str:
        order.append("upload")
        if object_path == env.gcs_terminal_path:
            raise original
        return "42"

    monkeypatch.setattr(runner, "upload_result_to_gcs", fail_terminal_upload)
    monkeypatch.setattr(runner, "post_callback", lambda *args: pytest.fail("must not post a terminal callback"))

    with pytest.raises(runner.TerminalArtifactPublicationError) as exc_info:
        asyncio.run(runner.run_platform_backtest(env))

    assert order == ["start", "upload", "upload"]
    assert exc_info.value.gcs_result_path == env.gcs_result_uri
    assert exc_info.value.outcome is runner.PlatformBacktestOutcome.COMPLETED
    assert exc_info.value.__cause__ is original
    assert not isinstance(exc_info.value, runner.PlatformRunnerError)


def test_run_platform_backtest_certifies_failed_engine_result(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = _env(
        STRATEGY_WORKDIR=str(tmp_path / "strategy"),
        GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json",
    )
    _patch_successful_run(monkeypatch, tmp_path)
    result = SimpleNamespace(
        success=False,
        error="engine failed at https://user:password@example.test and secret",
        decision_events=None,
    )

    class Backtester:
        async def backtest(self, strategy: object, config: object) -> object:
            return result

        async def close(self) -> None:
            return None

    uploads: list[tuple[str, dict[str, Any]]] = []
    callbacks: list[dict[str, Any]] = []

    def fake_upload(bucket: str, object_path: str, payload: dict[str, Any]) -> str:
        uploads.append((object_path, payload))
        return "42" if object_path.endswith("result.json") else "43"

    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: None)
    monkeypatch.setattr(
        runner,
        "serialize_result",
        lambda current_result: {
            "success": False,
            "error": current_result.error,
            "errors": [{"error_message": current_result.error}],
            "metrics": {},
            "trades": [],
        },
    )
    monkeypatch.setattr(runner, "upload_result_to_gcs", fake_upload)
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: callbacks.append(payload))

    payload = asyncio.run(runner.run_platform_backtest(env))

    assert [path for path, _ in uploads] == [
        "backtests/test-123/result.json",
        "backtests/test-123/terminal.json",
    ]
    result_artifact = uploads[0][1]
    assert result_artifact["result"]["success"] is False
    assert "password" not in result_artifact["result"]["error"]
    assert "secret" not in result_artifact["result"]["error"]

    terminal = uploads[1][1]
    assert terminal["schema_version"] == 1
    assert terminal["backtest_id"] == "test-123"
    assert terminal["backtest_outcome"] == "FAILED"
    assert terminal["result_uri"] == "gs://bucket/backtests/test-123/result.json"
    assert terminal["result_generation"] == "42"
    assert terminal["error_message"] == result_artifact["result"]["error"]

    assert payload == callbacks[0]
    assert payload == {
        "status": "FAILED",
        "artifact_contract_version": 1,
        "gcs_result_path": "gs://bucket/backtests/test-123/result.json",
        "error_message": result_artifact["result"]["error"],
        "result_summary": {
            "failure_stage": "run",
            "code": "STRATEGY_ERROR",
            "blockers": [
                {
                    "code": "STRATEGY_ERROR",
                    "message": result_artifact["result"]["error"],
                    "failed_checks": [],
                    "recommendations": [],
                }
            ],
        },
    }
    # The engine still produced metrics for the failed run; result.json keeps
    # them while the callback carries the structured failure taxonomy.
    assert result_artifact["result_summary"]["total_trades"] == 0


def test_run_platform_backtest_uses_fallback_for_whitespace_engine_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(
        STRATEGY_WORKDIR=str(tmp_path / "strategy"),
        GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json",
    )
    _patch_successful_run(monkeypatch, tmp_path)
    result = SimpleNamespace(success=False, error="   ", decision_events=None)

    class Backtester:
        async def backtest(self, strategy: object, config: object) -> object:
            return result

        async def close(self) -> None:
            return None

    uploads: list[tuple[str, dict[str, Any]]] = []

    def record_upload(bucket: str, object_path: str, payload: dict[str, Any]) -> str:
        uploads.append((object_path, payload))
        return "42"

    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(
        runner,
        "serialize_result",
        lambda current_result: {"success": False, "error": current_result.error, "metrics": {}, "trades": []},
    )
    monkeypatch.setattr(runner, "upload_result_to_gcs", record_upload)
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: None)

    payload = asyncio.run(runner.run_platform_backtest(env))

    assert uploads[1][1]["error_message"] == "Backtest engine returned a failed result."
    assert payload["error_message"] == "Backtest engine returned a failed result."


# ---------------------------------------------------------------------------
# Structured preflight failures (ALM-3384 / ALM-3386)
# ---------------------------------------------------------------------------


class _FeasibilityGateError(RuntimeError):
    """Stand-in for a gate that raises before data materialization.

    The runner must recognise it through the duck-typed attribute contract
    only — never by importing the raiser's class.
    """

    def __init__(self, message: str, *, code: str, blockers: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.preflight_code = code
        self.preflight_blockers = blockers


def _patch_preflight_failure_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exc: BaseException,
) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]], list[str]]:
    """Wire a run whose engine raises ``exc`` out of ``backtester.backtest``."""
    _patch_successful_run(monkeypatch, tmp_path)
    closed: list[str] = []

    class Backtester:
        async def backtest(self, strategy: object, config: object) -> object:
            raise exc

        async def close(self) -> None:
            closed.append("close")

    uploads: list[tuple[str, dict[str, Any]]] = []
    callbacks: list[dict[str, Any]] = []

    def record_upload(bucket: str, object_path: str, payload: dict[str, Any]) -> str:
        uploads.append((object_path, payload))
        return "42" if object_path.endswith("result.json") else "43"

    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "serialize_result", lambda result: pytest.fail("no result exists for preflight"))
    monkeypatch.setattr(
        runner, "upload_decisions_to_gcs", lambda *args: pytest.fail("preflight has no decision telemetry")
    )
    monkeypatch.setattr(runner, "upload_result_to_gcs", record_upload)
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: callbacks.append(payload))
    return uploads, callbacks, closed


def test_run_platform_backtest_certifies_structured_preflight_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(
        STRATEGY_WORKDIR=str(tmp_path / "strategy"),
        GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json",
    )
    exc = _FeasibilityGateError(
        "Requested window needs 8h of engine time; the job budget is 15m",
        code="WINDOW_TOO_LONG",
        blockers=[
            {
                "code": "WINDOW_TOO_LONG",
                "message": "Requested 6-month window cannot complete inside the job budget",
                "failed_checks": ["window_feasibility"],
                "recommendations": ["Shorten the window to 60 days", "Increase interval_seconds"],
            }
        ],
    )
    uploads, callbacks, closed = _patch_preflight_failure_run(monkeypatch, tmp_path, exc)

    payload = asyncio.run(runner.run_platform_backtest(env))

    assert closed == ["close"]
    assert [path for path, _ in uploads] == [
        "backtests/test-123/result.json",
        "backtests/test-123/terminal.json",
    ]

    result_artifact = uploads[0][1]
    assert result_artifact["backtest_id"] == "test-123"
    assert result_artifact["commit_sha"] == "a" * 40
    assert result_artifact["strategy"]["class_name"] == "Strategy"
    assert result_artifact["backtest_config"] == {"chain": "base"}
    assert "result" not in result_artifact
    assert result_artifact["failure"] == {
        "failure_stage": "preflight",
        "code": "WINDOW_TOO_LONG",
        "blockers": [
            {
                "code": "WINDOW_TOO_LONG",
                "message": "Requested 6-month window cannot complete inside the job budget",
                "failed_checks": ["window_feasibility"],
                "recommendations": ["Shorten the window to 60 days", "Increase interval_seconds"],
            }
        ],
        "error_message": "Requested window needs 8h of engine time; the job budget is 15m",
    }

    terminal = uploads[1][1]
    assert terminal["backtest_outcome"] == "FAILED"
    assert terminal["result_generation"] == "42"
    assert terminal["error_message"] == result_artifact["failure"]["error_message"]

    assert callbacks == [payload]
    assert payload == {
        "status": "FAILED",
        "artifact_contract_version": 1,
        "gcs_result_path": "gs://bucket/backtests/test-123/result.json",
        "error_message": "Requested window needs 8h of engine time; the job budget is 15m",
        "result_summary": {
            "failure_stage": "preflight",
            "code": "WINDOW_TOO_LONG",
            "blockers": result_artifact["failure"]["blockers"],
        },
    }


def test_run_platform_backtest_maps_bare_preflight_error_to_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(
        STRATEGY_WORKDIR=str(tmp_path / "strategy"),
        GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json",
    )
    exc = PreflightValidationError(
        "Preflight validation failed with 1 error(s)",
        failed_checks=["price_coverage: no USD price for WETH on 2024-01-05"],
        recommendations=["Shorten the backtest window", "Pick a token with historical coverage"],
        error_count=1,
    )
    uploads, callbacks, _ = _patch_preflight_failure_run(monkeypatch, tmp_path, exc)

    payload = asyncio.run(runner.run_platform_backtest(env))

    summary = payload["result_summary"]
    assert summary["failure_stage"] == "preflight"
    assert summary["code"] == "BACKTEST_NOT_READY"
    assert summary["blockers"] == [
        {
            "code": "BACKTEST_NOT_READY",
            "message": str(exc),
            "failed_checks": ["price_coverage: no USD price for WETH on 2024-01-05"],
            "recommendations": ["Shorten the backtest window", "Pick a token with historical coverage"],
        }
    ]
    assert payload["error_message"] == str(exc)
    assert uploads[0][1]["failure"]["code"] == "BACKTEST_NOT_READY"
    assert uploads[1][1]["error_message"] == str(exc)
    assert callbacks == [payload]


def test_run_platform_backtest_keeps_engine_code_on_bare_preflight_blocker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    exc = PreflightValidationError("Protocol is unsupported on base", code="UNSUPPORTED_PROTOCOL")
    _, callbacks, _ = _patch_preflight_failure_run(monkeypatch, tmp_path, exc)

    payload = asyncio.run(runner.run_platform_backtest(env))

    # The stage headline stays the contract default; the engine's own stable
    # code rides along on the blocker so the platform can still branch on it.
    assert payload["result_summary"]["code"] == "BACKTEST_NOT_READY"
    assert payload["result_summary"]["blockers"][0]["code"] == "UNSUPPORTED_PROTOCOL"
    assert callbacks == [payload]


def test_run_platform_backtest_redacts_preflight_artifacts_and_callback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    exc = _FeasibilityGateError(
        "clone https://x-access-token:token@example/repo.git failed with secret",
        code="BACKTEST_NOT_READY",
        blockers=[
            {
                "code": "BACKTEST_NOT_READY",
                "message": "provider https://user:password@data.example rejected the request",
                "recommendations": ["retry against https://x-access-token:token@example/repo.git"],
            }
        ],
    )
    uploads, callbacks, _ = _patch_preflight_failure_run(monkeypatch, tmp_path, exc)

    payload = asyncio.run(runner.run_platform_backtest(env))

    rendered = json.dumps([uploads[0][1], uploads[1][1], payload])
    assert "x-access-token:token@example" not in rendered
    assert "user:password" not in rendered
    assert "GITHUB_CLONE_URL" in payload["error_message"]
    assert "PLATFORM_CALLBACK_SECRET" in payload["error_message"]
    blocker = payload["result_summary"]["blockers"][0]
    assert blocker["message"] == "provider https://***@data.example rejected the request"
    assert blocker["recommendations"] == ["retry against GITHUB_CLONE_URL"]
    assert callbacks == [payload]


def test_run_platform_backtest_does_not_post_callback_when_preflight_terminal_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    exc = PreflightValidationError("Preflight validation failed with 1 error(s)")
    _patch_preflight_failure_run(monkeypatch, tmp_path, exc)
    original = ConnectionError("terminal upload unavailable")

    def fail_terminal_upload(bucket: str, object_path: str, payload: dict[str, Any]) -> str:
        if object_path == env.gcs_terminal_path:
            raise original
        return "42"

    monkeypatch.setattr(runner, "upload_result_to_gcs", fail_terminal_upload)
    monkeypatch.setattr(runner, "post_callback", lambda *args: pytest.fail("must not post a terminal callback"))

    with pytest.raises(runner.TerminalArtifactPublicationError) as exc_info:
        asyncio.run(runner.run_platform_backtest(env))

    assert exc_info.value.outcome is runner.PlatformBacktestOutcome.FAILED
    assert exc_info.value.__cause__ is original
    assert not isinstance(exc_info.value, runner.PlatformRunnerError)


def test_run_platform_backtest_raises_delivery_error_when_preflight_callback_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    exc = PreflightValidationError("Preflight validation failed with 1 error(s)")
    _patch_preflight_failure_run(monkeypatch, tmp_path, exc)
    original = runner.requests.ConnectionError("callback unreachable")

    def fail_callback(current_env: runner.PlatformRunnerEnv, payload: dict[str, Any]) -> None:
        raise original

    monkeypatch.setattr(runner, "post_callback", fail_callback)

    with pytest.raises(runner.TerminalCallbackDeliveryError) as exc_info:
        asyncio.run(runner.run_platform_backtest(env))

    assert exc_info.value.outcome is runner.PlatformBacktestOutcome.FAILED
    assert exc_info.value.gcs_result_path == env.gcs_result_uri
    assert exc_info.value.__cause__ is original


def test_main_certifies_preflight_failure_without_a_second_verdict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", json.dumps({"token_funding": _TOKEN_FUNDING}))
    monkeypatch.setenv("BACKTEST_CONFIG", '{"start_time":"2024-01-01","end_time":"2024-03-01"}')
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("GCS_RESULT_PATH", "gs://bucket/backtests/test-123/result.json")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")
    monkeypatch.setenv("STRATEGY_WORKDIR", str(tmp_path / "strategy"))

    exc = _FeasibilityGateError("window is infeasible", code="WINDOW_TOO_LONG", blockers=[])
    _, callbacks, _ = _patch_preflight_failure_run(monkeypatch, tmp_path, exc)
    monkeypatch.setattr(runner, "post_callback_values", lambda **kwargs: pytest.fail("must not post a second verdict"))

    # A certified terminal outcome is a successful runner task: Cloud Run must
    # not retry a run whose verdict is already durable.
    assert runner.main() == 0
    assert [payload["status"] for payload in callbacks] == ["FAILED"]
    assert callbacks[0]["result_summary"]["code"] == "WINDOW_TOO_LONG"
    assert callbacks[0]["result_summary"]["blockers"] == [
        {
            "code": "WINDOW_TOO_LONG",
            "message": "window is infeasible",
            "failed_checks": [],
            "recommendations": [],
        }
    ]


def test_main_maps_escaped_preflight_failure_without_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []

    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    async def fake_run_platform_backtest(env: runner.PlatformRunnerEnv) -> dict[str, Any]:
        raise PreflightValidationError(
            "Preflight validation failed with 1 error(s)",
            failed_checks=["support_matrix: unsupported venue"],
        )

    monkeypatch.setattr(runner, "run_platform_backtest", fake_run_platform_backtest)
    monkeypatch.setattr(runner, "post_callback", lambda env, payload: posted.append(payload))

    assert runner.main() == 1
    assert posted[0]["result_summary"] == {
        "failure_stage": "preflight",
        "code": "BACKTEST_NOT_READY",
        "blockers": [
            {
                "code": "BACKTEST_NOT_READY",
                "message": posted[0]["error_message"],
                "failed_checks": ["support_matrix: unsupported venue"],
                "recommendations": [],
            }
        ],
    }


def test_failure_summaries_truncate_oversized_messages() -> None:
    exc = PreflightValidationError("x" * 6000)

    summary = runner.build_preflight_failure_summary(exc)

    assert len(summary["blockers"][0]["message"]) == 5000
    assert len(runner.build_run_failure_summary("y" * 6000)["blockers"][0]["message"]) == 5000


def _patch_terminal_blob(monkeypatch: pytest.MonkeyPatch, download: bytes | Exception) -> None:
    class Blob:
        def exists(self) -> bool:
            return True

        def download_as_bytes(self) -> bytes:
            if isinstance(download, Exception):
                raise download
            return download

    class Bucket:
        def blob(self, object_path: str) -> Blob:
            return Blob()

    class Client:
        def bucket(self, bucket_name: str) -> Bucket:
            return Bucket()

    storage_module = ModuleType("google.cloud.storage")
    storage_module.Client = Client
    cloud_module = importlib.import_module("google.cloud")
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)
    monkeypatch.setattr(cloud_module, "storage", storage_module, raising=False)


@pytest.mark.parametrize(
    "download",
    [TimeoutError("GCS read timed out"), b"{truncated"],
)
def test_load_terminal_manifest_classifies_read_and_decode_failures_as_transient(
    monkeypatch: pytest.MonkeyPatch,
    download: bytes | Exception,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    _patch_terminal_blob(monkeypatch, download)

    with pytest.raises(runner.TerminalArtifactInspectionError):
        runner.load_terminal_manifest_from_gcs(env)


def test_load_terminal_manifest_classifies_contract_failure_as_permanent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    invalid_terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.COMPLETED)
    invalid_terminal["result_uri"] = "gs://bucket/backtests/other-run/result.json"
    _patch_terminal_blob(monkeypatch, json.dumps(invalid_terminal).encode())

    with pytest.raises(runner.TerminalArtifactContractError, match="result URI does not match"):
        runner.load_terminal_manifest_from_gcs(env)


def _existing_terminal(
    env: runner.PlatformRunnerEnv,
    outcome: runner.PlatformBacktestOutcome,
    *,
    error_message: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION,
        "kind": PLATFORM_BACKTEST_TERMINAL_KIND,
        "backtest_id": env.backtest_id,
        "backtest_outcome": outcome.value,
        "result_uri": env.gcs_result_uri,
        "result_generation": "42",
        "sdk_version": __version__,
        "commit_sha": env.commit_sha,
        "error_message": error_message,
        "created_at": "2026-08-06T00:00:00+00:00",
    }


def test_cloud_run_retry_redelivers_existing_certificate_without_rerunning(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    order: list[str] = []
    existing_terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.COMPLETED)

    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: order.append("start"))
    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: existing_terminal)
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: pytest.fail("strategy must not rerun"))
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda *args: pytest.fail("evidence must not be overwritten"))
    monkeypatch.setattr(
        runner,
        "load_result_artifact_from_gcs",
        lambda *args: pytest.fail("a COMPLETED redelivery must not read result.json"),
    )
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: order.append(payload["status"]))

    payload = asyncio.run(runner.run_platform_backtest(env))

    assert order == ["COMPLETED"]
    assert payload == {
        "status": "COMPLETED",
        "artifact_contract_version": 1,
        "gcs_result_path": env.gcs_result_uri,
    }


def test_cloud_run_retry_reads_legacy_prefix_certificate_without_rerunning(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _env()
    inspected_paths: list[str] = []
    callbacks: list[dict[str, Any]] = []
    existing_terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.COMPLETED)

    def fake_load(current_env: runner.PlatformRunnerEnv) -> dict[str, Any]:
        inspected_paths.append(current_env.gcs_terminal_path)
        return existing_terminal

    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", fake_load)
    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: pytest.fail("must not post STARTED"))
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: pytest.fail("strategy must not rerun"))
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda *args: pytest.fail("evidence must not be overwritten"))
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: callbacks.append(payload))

    payload = asyncio.run(runner.run_platform_backtest(env))

    assert inspected_paths == ["backtest-results/test-123/terminal.json"]
    assert payload["gcs_result_path"] == "gs://bucket/backtest-results/test-123/result.json"
    assert callbacks == [payload]


def test_cloud_run_retry_redelivers_failed_certificate_with_its_error_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    existing_terminal = _existing_terminal(
        env,
        runner.PlatformBacktestOutcome.FAILED,
        error_message="historical price provider failed",
    )
    callbacks: list[dict[str, Any]] = []

    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: existing_terminal)
    # A certificate whose result.json cannot be read carries no recoverable
    # summary: the redelivered payload must stay exactly what it was pre-ALM-3384.
    monkeypatch.setattr(runner, "load_result_artifact_from_gcs", lambda *args: None)
    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: pytest.fail("must not post STARTED"))
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: pytest.fail("strategy must not rerun"))
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda *args: pytest.fail("evidence must not be overwritten"))
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: callbacks.append(payload))

    payload = asyncio.run(runner.run_platform_backtest(env))

    assert payload == {
        "status": "FAILED",
        "artifact_contract_version": 1,
        "gcs_result_path": env.gcs_result_uri,
        "error_message": "historical price provider failed",
    }
    assert callbacks == [payload]


def _patch_result_blob(
    monkeypatch: pytest.MonkeyPatch,
    download: bytes | Exception,
    requested: list[tuple[str, Any]] | None = None,
) -> None:
    class Blob:
        def download_as_bytes(self) -> bytes:
            if isinstance(download, Exception):
                raise download
            return download

    class Bucket:
        def blob(self, object_path: str, generation: Any = None) -> Blob:
            if requested is not None:
                requested.append((object_path, generation))
            return Blob()

    class Client:
        def bucket(self, bucket_name: str) -> Bucket:
            return Bucket()

    storage_module = ModuleType("google.cloud.storage")
    storage_module.Client = Client
    cloud_module = importlib.import_module("google.cloud")
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)
    monkeypatch.setattr(cloud_module, "storage", storage_module, raising=False)


def _preflight_result_artifact(env: runner.PlatformRunnerEnv, error_message: str) -> dict[str, Any]:
    """The result.json shape the preflight terminal path uploads."""
    return {
        "backtest_id": env.backtest_id,
        "commit_sha": env.commit_sha,
        "failure": {
            "failure_stage": "preflight",
            "code": "WINDOW_TOO_LONG",
            "blockers": [
                {
                    "code": "WINDOW_TOO_LONG",
                    "message": error_message,
                    "failed_checks": ["window: 6 months exceeds the tick budget"],
                    "recommendations": ["Shorten the backtest window"],
                }
            ],
            "error_message": error_message,
        },
    }


def _redelivered_failed_payload(monkeypatch: pytest.MonkeyPatch, env: runner.PlatformRunnerEnv, **kwargs: Any) -> Any:
    existing_terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.FAILED, **kwargs)
    monkeypatch.setattr(runner, "load_terminal_manifest_from_gcs", lambda current_env: existing_terminal)
    monkeypatch.setattr(runner, "post_start_callback", lambda current_env: pytest.fail("must not post STARTED"))
    monkeypatch.setattr(runner, "clone_strategy_repo", lambda current_env: pytest.fail("strategy must not rerun"))
    monkeypatch.setattr(runner, "upload_result_to_gcs", lambda *args: pytest.fail("evidence must not be overwritten"))
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: None)
    return asyncio.run(runner.run_platform_backtest(env))


def test_cloud_run_retry_redelivers_structured_summary_from_certified_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    error_message = "Backtest window is longer than the runner budget"
    requested: list[tuple[str, Any]] = []
    _patch_result_blob(monkeypatch, json.dumps(_preflight_result_artifact(env, error_message)).encode(), requested)

    payload = _redelivered_failed_payload(monkeypatch, env, error_message=error_message)

    assert payload["error_message"] == error_message
    # Same structured summary the first attempt sent — blockers, not a flat string.
    assert payload["result_summary"] == {
        "failure_stage": "preflight",
        "code": "WINDOW_TOO_LONG",
        "blockers": [
            {
                "code": "WINDOW_TOO_LONG",
                "message": error_message,
                "failed_checks": ["window: 6 months exceeds the tick budget"],
                "recommendations": ["Shorten the backtest window"],
            }
        ],
    }
    # Read is pinned to the certified generation, never to "current" bytes.
    assert requested == [("backtests/test-123/result.json", 42)]


def test_cloud_run_retry_rebuilds_run_stage_summary_for_engine_failure_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    # An engine-produced failed result (and every certificate written by an
    # already-deployed image) has no "failure" section; its summary is a pure
    # function of the certified error message.
    legacy_artifact = {
        "backtest_id": env.backtest_id,
        "commit_sha": env.commit_sha,
        "result_summary": {"total_trades": 0},
        "result": {"success": False},
    }
    _patch_result_blob(monkeypatch, json.dumps(legacy_artifact).encode())

    payload = _redelivered_failed_payload(monkeypatch, env, error_message="historical price provider failed")

    assert payload["result_summary"] == runner.build_run_failure_summary("historical price provider failed")
    assert payload["result_summary"]["failure_stage"] == "run"
    assert payload["result_summary"]["code"] == "STRATEGY_ERROR"


def test_cloud_run_retry_keeps_run_validity_on_redelivered_engine_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    verdict = {
        "schema_version": 1,
        "validity": "INVALID",
        "reason_codes": ["ZERO_INITIAL_CAPITAL"],
        "executed_fills": 0,
        "passive_only": False,
    }
    artifact = {
        "backtest_id": env.backtest_id,
        "commit_sha": env.commit_sha,
        "result_summary": {"total_trades": 0, "run_validity": verdict},
        "result": {"success": False},
    }
    _patch_result_blob(monkeypatch, json.dumps(artifact).encode())

    payload = _redelivered_failed_payload(monkeypatch, env, error_message="BACKTEST_INVALID: no capital")

    # The redelivered callback must not be poorer than the one that was lost.
    assert payload["result_summary"]["code"] == "STRATEGY_ERROR"
    assert payload["result_summary"]["run_validity"] == verdict


def test_cloud_run_retry_keeps_the_cadence_echo_on_redelivered_engine_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    simulation = {
        "decision_interval_seconds": 900,
        "price_timeframe_requested": "15m",
        "price_timeframe_resolved": None,
    }
    artifact = {
        "backtest_id": env.backtest_id,
        "commit_sha": env.commit_sha,
        "result_summary": {"total_trades": 0, "simulation": simulation},
        "result": {"success": False},
    }
    _patch_result_blob(monkeypatch, json.dumps(artifact).encode())

    payload = _redelivered_failed_payload(monkeypatch, env, error_message="historical price provider failed")

    assert payload["result_summary"]["simulation"] == simulation


def test_cloud_run_retry_derives_run_validity_from_the_artifact_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    artifact = {
        "backtest_id": env.backtest_id,
        "commit_sha": env.commit_sha,
        "result_summary": {"total_trades": 0},
        "result": {
            "success": False,
            "run_validity": {"validity": "NOT_EVALUABLE", "reasons": [{"code": "INPUT_STARVED"}], "warnings": []},
        },
    }
    _patch_result_blob(monkeypatch, json.dumps(artifact).encode())

    payload = _redelivered_failed_payload(monkeypatch, env, error_message="BACKTEST_UNSUPPORTED_DATA: starved")

    assert payload["result_summary"]["run_validity"]["validity"] == "NOT_EVALUABLE"
    assert payload["result_summary"]["run_validity"]["reason_codes"] == ["INPUT_STARVED"]


@pytest.mark.parametrize(
    "download",
    [TimeoutError("GCS read timed out"), b"{truncated", b'"not an object"'],
)
def test_recover_failure_summary_degrades_when_certified_artifact_is_unreadable(
    monkeypatch: pytest.MonkeyPatch,
    download: bytes | Exception,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    _patch_result_blob(monkeypatch, download)
    terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.FAILED, error_message="engine exploded")

    # Redelivery must never become more fragile than the payload it replaces.
    assert runner.recover_failure_summary(env, terminal) is None


def test_recover_failure_summary_redacts_recovered_blockers(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    artifact = _preflight_result_artifact(env, "boom")
    artifact["failure"]["blockers"][0]["recommendations"] = ["retry with secret"]
    artifact["failure"]["blockers"][0]["message"] = "clone https://user:pw@example/repo.git failed"
    _patch_result_blob(monkeypatch, json.dumps(artifact).encode())
    terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.FAILED, error_message="boom")

    summary = runner.recover_failure_summary(env, terminal)

    assert summary is not None
    assert summary["blockers"][0]["message"] == "clone https://***@example/repo.git failed"
    assert summary["blockers"][0]["recommendations"] == ["retry with PLATFORM_CALLBACK_SECRET"]


@pytest.mark.parametrize(
    "failure",
    [
        "not-a-mapping",
        {"failure_stage": "preflight", "code": "X"},
        {"failure_stage": "", "code": "X", "blockers": [{}]},
    ],
)
def test_recover_failure_summary_ignores_malformed_failure_sections(
    monkeypatch: pytest.MonkeyPatch,
    failure: Any,
) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    _patch_result_blob(monkeypatch, json.dumps({"failure": failure}).encode())
    terminal = _existing_terminal(env, runner.PlatformBacktestOutcome.FAILED, error_message="engine exploded")

    # A half-written section is not evidence: fall through to the run-stage
    # rebuild rather than shipping a summary with missing blockers.
    assert runner.recover_failure_summary(env, terminal) == runner.build_run_failure_summary("engine exploded")


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


def test_main_does_not_post_failed_when_terminal_certificate_inspection_is_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("GCS_RESULT_PATH", "gs://bucket/backtests/test-123/result.json")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    async def fake_run_platform_backtest(env: runner.PlatformRunnerEnv) -> dict[str, Any]:
        raise runner.TerminalArtifactInspectionError("GCS read timed out")

    monkeypatch.setattr(runner, "run_platform_backtest", fake_run_platform_backtest)
    monkeypatch.setattr(runner, "post_callback", lambda env, payload: posted.append(payload))
    monkeypatch.setattr(runner, "post_callback_values", lambda **kwargs: posted.append(kwargs))

    assert runner.main() == 1
    assert posted == []


def test_main_does_not_post_failed_when_terminal_certificate_publication_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("GCS_RESULT_PATH", "gs://bucket/backtests/test-123/result.json")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    async def fake_run_platform_backtest(env: runner.PlatformRunnerEnv) -> dict[str, Any]:
        raise runner.TerminalArtifactPublicationError(env.gcs_result_uri, runner.PlatformBacktestOutcome.COMPLETED)

    monkeypatch.setattr(runner, "run_platform_backtest", fake_run_platform_backtest)
    monkeypatch.setattr(runner, "post_callback", lambda env, payload: posted.append(payload))
    monkeypatch.setattr(runner, "post_callback_values", lambda **kwargs: posted.append(kwargs))

    assert runner.main() == 1
    assert posted == []


def test_main_posts_failed_for_permanent_terminal_certificate_contract_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    monkeypatch.setenv("BACKTEST_ID", "test-123")
    monkeypatch.setenv("COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_CLONE_URL", "https://x-access-token:token@example/repo.git")
    monkeypatch.setenv("STRATEGY_CONFIG", "{}")
    monkeypatch.setenv("BACKTEST_CONFIG", "{}")
    monkeypatch.setenv("GCS_BUCKET", "bucket")
    monkeypatch.setenv("GCS_RESULT_PATH", "gs://bucket/backtests/test-123/result.json")
    monkeypatch.setenv("PLATFORM_CALLBACK_URL", "https://api.example")
    monkeypatch.setenv("PLATFORM_CALLBACK_SECRET", "secret")

    async def fake_run_platform_backtest(env: runner.PlatformRunnerEnv) -> dict[str, Any]:
        raise runner.TerminalArtifactContractError("Existing terminal certificate is invalid: wrong result URI")

    monkeypatch.setattr(runner, "run_platform_backtest", fake_run_platform_backtest)
    monkeypatch.setattr(runner, "post_callback", lambda env, payload: posted.append(payload))

    assert runner.main() == 1
    error_message = "TerminalArtifactContractError: Existing terminal certificate is invalid: wrong result URI"
    assert posted == [
        {
            "status": "FAILED",
            "error_message": error_message,
            "result_summary": {
                "failure_stage": "infra",
                "code": "INFRA_ERROR",
                "blockers": [
                    {
                        "code": "INFRA_ERROR",
                        "message": error_message,
                        "failed_checks": [],
                        "recommendations": [],
                    }
                ],
            },
        }
    ]


# ---------------------------------------------------------------------------
# decisions.jsonl sidecar (decision telemetry)
# ---------------------------------------------------------------------------


def test_build_decisions_jsonl_interleaves_decisions_and_executions() -> None:
    result = SimpleNamespace(
        decision_events=[
            {
                "event": "decision",
                "tick": 1,
                "timestamp": "2026-06-01T00:00:00+00:00",
                "source": "strategy",
                "decision": "SWAP",
                "intents": [{"intent_type": "SWAP"}],
            },
            {
                "event": "decision",
                "tick": 2,
                "timestamp": "2026-06-01T01:00:00+00:00",
                "source": "strategy",
                "decision": "HOLD",
                "hold_reason": "cooldown",
                "hold_reason_code": None,
            },
        ]
    )
    serialized = {
        "trades": [
            {
                "timestamp": "2026-06-01 00:00:00+00:00",
                "intent_type": "SWAP",
                "status": "filled",
                "amount_usd": "10",
                "fee_usd": "0",
                "rejection_reason": None,
            }
        ]
    }

    ndjson = runner.build_decisions_jsonl(result, serialized)

    assert ndjson.endswith("\n")
    lines = [json.loads(line) for line in ndjson.strip().splitlines()]
    # Chronological interleave despite the "T"-vs-space timestamp forms, with
    # the decision preceding its same-instant execution event.
    assert [(line["event"], line.get("tick")) for line in lines] == [
        ("decision", 1),
        ("execution", None),
        ("decision", 2),
    ]
    assert lines[1]["status"] == "filled"


def test_build_decisions_jsonl_empty_without_telemetry() -> None:
    assert runner.build_decisions_jsonl(object(), {"trades": []}) == ""
    # A pre-telemetry result WITH trades must not produce an execution-only
    # file that misrepresents the run as "decided nothing, filled things".
    trades = {"trades": [{"timestamp": "2026-06-01 00:00:00+00:00", "intent_type": "SWAP", "status": "filled"}]}
    assert runner.build_decisions_jsonl(SimpleNamespace(decision_events=None), trades) == ""
    assert runner.build_decisions_jsonl(SimpleNamespace(decision_events=[]), trades) == ""


def test_run_platform_backtest_uploads_decisions_sidecar_before_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env = _env(STRATEGY_WORKDIR=str(tmp_path / "strategy"))
    order = _patch_successful_run(monkeypatch, tmp_path)

    result = SimpleNamespace(
        decision_events=[
            {
                "event": "decision",
                "tick": 1,
                "timestamp": "2026-06-01T00:00:00+00:00",
                "source": "strategy",
                "decision": "HOLD",
                "hold_reason": "r",
                "hold_reason_code": None,
            }
        ]
    )

    class Backtester:
        async def backtest(self, strategy: object, config: object) -> object:
            return result

        async def close(self) -> None:
            return None

    uploaded: dict[str, str] = {}

    def record_sidecar(bucket: str, object_path: str, ndjson: str) -> None:
        order.append("decisions")
        uploaded.update(path=object_path, body=ndjson)

    monkeypatch.setattr(runner, "create_backtester", lambda **kwargs: Backtester())
    monkeypatch.setattr(runner, "upload_decisions_to_gcs", record_sidecar)
    monkeypatch.setattr(runner, "post_callback", lambda current_env, payload: order.append(payload["status"]))

    asyncio.run(runner.run_platform_backtest(env))

    # Sidecar lands before result.json, then terminal.json certifies completion.
    assert order == ["start", "decisions", "upload", "upload", "COMPLETED"]
    assert uploaded["path"] == env.gcs_decisions_path
    assert json.loads(uploaded["body"].strip())["hold_reason"] == "r"


def test_build_result_summary_echoes_compact_decision_summary() -> None:
    serialized = {
        "metrics": {"total_trades": 0},
        "trades": [],
        "duration_seconds": 12.5,
        "decision_summary": {
            "schema_version": 1,
            "ticks": 2160,
            "intent_ticks": 0,
            "hold_ticks": 2160,
            "intent_types": {},
            "hold_reasons": [
                {
                    "source": "strategy",
                    "reason_code": None,
                    "reason_template": "Allocation data unavailable: X",
                    "example": "Allocation data unavailable: X",
                    "ticks": 2160,
                    "first_tick": 1,
                    "last_tick": 2160,
                }
            ],
            "executions": {"fills": 0, "rejected": 0},
        },
    }

    summary = runner.build_result_summary(serialized, elapsed_seconds=1.0)

    assert summary["decision_summary"] == {
        "ticks": 2160,
        "hold_ticks": 2160,
        "intent_ticks": 0,
        "top_hold_reason": {
            "source": "strategy",
            "reason_code": None,
            "example": "Allocation data unavailable: X",
            "ticks": 2160,
        },
    }


def test_build_result_summary_omits_decision_block_when_absent() -> None:
    summary = runner.build_result_summary({"metrics": {}, "trades": []}, elapsed_seconds=1.0)
    assert "run_validity" not in summary


def test_build_result_summary_echoes_compact_run_validity() -> None:
    serialized = {
        "metrics": {},
        "trades": [],
        "run_validity": {
            "schema_version": 1,
            "validity": "INVALID",
            "reasons": [{"code": "ZERO_INITIAL_CAPITAL", "message": "no capital", "details": {"tick_count": 4}}],
            "warnings": [{"code": "INPUT_STARVED_LANE", "message": "lane", "details": {}}],
            "executed_fills": 0,
            "passive_only": False,
        },
    }

    summary = runner.build_result_summary(serialized, elapsed_seconds=1.0)

    # Codes only: the reason table stays in the artifact.
    assert summary["run_validity"] == {
        "schema_version": 1,
        "validity": "INVALID",
        "reason_codes": ["ZERO_INITIAL_CAPITAL"],
        "warning_codes": ["INPUT_STARVED_LANE"],
        "executed_fills": 0,
        "passive_only": False,
    }


def test_build_result_summary_never_forwards_an_unknown_verdict() -> None:
    serialized = {
        "metrics": {},
        "trades": [],
        "run_validity": {
            "validity": "SOMETHING_NEW",
            "reasons": [],
            "warnings": [],
            "executed_fills": 0,
            "passive_only": True,
        },
    }

    summary = runner.build_result_summary(serialized, elapsed_seconds=1.0)

    assert summary["run_validity"]["validity"] == "INVALID"
    assert summary["run_validity"]["passive_only"] is False


def test_cloud_run_retry_normalizes_a_persisted_unknown_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    env = _env(GCS_RESULT_PATH="gs://bucket/backtests/test-123/result.json")
    artifact = {
        "backtest_id": env.backtest_id,
        "commit_sha": env.commit_sha,
        "result_summary": {
            "total_trades": 0,
            "run_validity": {"validity": "SOMETHING_NEW", "reason_codes": ["NEW_CODE"], "passive_only": True},
        },
        "result": {"success": False},
    }
    _patch_result_blob(monkeypatch, json.dumps(artifact).encode())

    payload = _redelivered_failed_payload(monkeypatch, env, error_message="BACKTEST_INVALID: something new")

    assert payload["result_summary"]["run_validity"]["validity"] == "INVALID"
    assert payload["result_summary"]["run_validity"]["passive_only"] is False
    assert payload["result_summary"]["run_validity"]["reason_codes"] == ["NEW_CODE"]


def test_build_run_failure_summary_carries_run_validity() -> None:
    verdict = {"schema_version": 1, "validity": "NOT_EVALUABLE", "reason_codes": ["INPUT_STARVED"]}

    summary = runner.build_run_failure_summary("BACKTEST_UNSUPPORTED_DATA: starved", run_validity=verdict)

    assert summary["code"] == "STRATEGY_ERROR"
    assert summary["run_validity"] == verdict
    assert "run_validity" not in runner.build_run_failure_summary("plain failure")
    assert "decision_summary" not in summary


def test_result_summary_carries_both_cadence_axes() -> None:
    """The platform row sees the decision grid and the price plane it actually got."""
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from tests.backtesting_funding import pnl_token_funding

    config = PnLBacktestConfig(
        start_time=datetime(2026, 6, 25, tzinfo=UTC),
        end_time=datetime(2026, 9, 1, tzinfo=UTC),
        interval_seconds=900,
        timeframe="15m",
        token_funding=pnl_token_funding(Decimal("1000")),
        tokens=["WETH", "USDC"],
    )

    summary = runner.build_result_summary(
        {"metrics": {}, "trades": [], "resolved_timeframe": "15m"},
        elapsed_seconds=1.0,
        backtest_config=config,
    )
    failure = runner.build_run_failure_summary("boom", simulation=summary["simulation"])

    assert summary["simulation"] == {
        "decision_interval_seconds": 900,
        "price_timeframe_requested": "15m",
        "price_timeframe_resolved": "15m",
    }
    assert failure["simulation"] == summary["simulation"]


def test_result_summary_shows_an_unverified_price_request() -> None:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from tests.backtesting_funding import pnl_token_funding

    config = PnLBacktestConfig(
        start_time=datetime(2026, 6, 25, tzinfo=UTC),
        end_time=datetime(2026, 9, 1, tzinfo=UTC),
        token_funding=pnl_token_funding(Decimal("1000")),
        tokens=["WETH", "USDC"],
    )

    summary = runner.build_result_summary({"metrics": {}, "trades": []}, elapsed_seconds=1.0, backtest_config=config)

    assert summary["simulation"] == {
        "decision_interval_seconds": 3600,
        "price_timeframe_requested": None,
        "price_timeframe_resolved": None,
    }
    assert "simulation" not in runner.build_result_summary({"metrics": {}, "trades": []}, elapsed_seconds=1.0)
