"""Targeted tests for VIB-4423 config-service helpers."""

from __future__ import annotations

import json

import pytest

from almanak.config import load_config
from almanak.config.backtest import (
    DEFAULT_BACKTEST_LOG_LEVEL,
    DEFAULT_BACKTEST_MAX_JOBS,
    DEFAULT_BACKTEST_MAX_PAPER_SESSIONS,
    DEFAULT_BACKTEST_SERVICE_HOST,
    DEFAULT_BACKTEST_SERVICE_PORT,
    DEFAULT_BACKTEST_SERVICE_WORKERS,
    backtest_service_config_from_env,
)
from almanak.config.framework import framework_config_from_env
from almanak.config.safe_signer import safe_signer_service_config_from_env
from almanak.config.simulation import (
    DEFAULT_PREFER_ALCHEMY,
    DEFAULT_SIMULATION_ENABLED,
    DEFAULT_SIMULATION_TIMEOUT_SECONDS,
    simulation_config_from_env,
)
from almanak.framework.data.token_safety.client import TokenSafetyClient
from almanak.framework.execution.signer.safe.config import (
    create_signer_config_from_env,
    get_wallet_mapping,
)
from almanak.framework.execution.simulator.config import SimulationConfig as LegacySimulationConfig
from almanak.services.backtest.config import BacktestServiceConfig as LegacyBacktestServiceConfig

_ENV_VARS: tuple[str, ...] = (
    "ALMANAK_DEMO_MODE",
    "ALMANAK_FORCE_PRODUCTION",
    "RUGCHECK_API_KEY",
    "ALMANAK_REDACT_SECRETS",
    "ALMANAK_PLATFORM_WALLETS",
    "ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT",
    "ALMANAK_SIGNER_SERVICE_JWT",
    "BACKTEST_SERVICE_HOST",
    "BACKTEST_SERVICE_PORT",
    "BACKTEST_SERVICE_WORKERS",
    "BACKTEST_MAX_JOBS",
    "BACKTEST_MAX_PAPER_SESSIONS",
    "BACKTEST_LOG_LEVEL",
    "ALMANAK_SIMULATION_ENABLED",
    "ALMANAK_SIMULATION_TIMEOUT",
    "ALMANAK_SIMULATION_PREFER_ALCHEMY",
    "TENDERLY_ACCOUNT_SLUG",
    "TENDERLY_PROJECT_SLUG",
    "TENDERLY_ACCESS_KEY",
    "ALCHEMY_API_KEY",
)

_TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
_TEST_SAFE_ADDRESS = "0x1234567890123456789012345678901234567890"
_TEST_EOA_ADDRESS = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
_TEST_ZODIAC_ADDRESS = "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"


@pytest.fixture(autouse=True)
def _scrub_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    get_wallet_mapping.cache_clear()
    yield
    get_wallet_mapping.cache_clear()


def test_framework_runtime_helpers_read_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_DEMO_MODE", "true")
    monkeypatch.setenv("ALMANAK_FORCE_PRODUCTION", "yes")
    monkeypatch.setenv("RUGCHECK_API_KEY", "rugcheck-secret")
    monkeypatch.setenv("ALMANAK_REDACT_SECRETS", "false")

    cfg = framework_config_from_env()
    assert cfg.demo_mode_enabled is True
    assert cfg.force_production_enabled is True
    assert cfg.rugcheck_api_key == "rugcheck-secret"
    assert cfg.redact_secrets_enabled is False


def test_safe_signer_helpers_read_env(monkeypatch: pytest.MonkeyPatch) -> None:
    wallets_json = '[{"SAFE_ACCOUNT_ADDRESS":"0x1","EOA_ADDRESS":"0x2"}]'
    monkeypatch.setenv("ALMANAK_PLATFORM_WALLETS", wallets_json)
    monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT", "https://signer.example.com")
    monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_JWT", "jwt-token")

    cfg = safe_signer_service_config_from_env()
    typed_cfg = load_config().safe_signer
    assert typed_cfg.platform_wallets_json == wallets_json
    assert typed_cfg.endpoint_root == "https://signer.example.com"
    assert typed_cfg.jwt == "jwt-token"
    assert cfg.endpoint_root == "https://signer.example.com"
    assert cfg.jwt == "jwt-token"


def test_backtest_service_config_defaults() -> None:
    cfg = backtest_service_config_from_env()
    assert cfg.host == DEFAULT_BACKTEST_SERVICE_HOST
    assert cfg.port == DEFAULT_BACKTEST_SERVICE_PORT
    assert cfg.workers == DEFAULT_BACKTEST_SERVICE_WORKERS
    assert cfg.max_concurrent_backtest_jobs == DEFAULT_BACKTEST_MAX_JOBS
    assert cfg.max_concurrent_paper_sessions == DEFAULT_BACKTEST_MAX_PAPER_SESSIONS
    assert cfg.log_level == DEFAULT_BACKTEST_LOG_LEVEL


def test_backtest_service_config_reads_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_SERVICE_HOST", "127.0.0.1")
    monkeypatch.setenv("BACKTEST_SERVICE_PORT", "9001")
    monkeypatch.setenv("BACKTEST_SERVICE_WORKERS", "3")
    monkeypatch.setenv("BACKTEST_MAX_JOBS", "8")
    monkeypatch.setenv("BACKTEST_MAX_PAPER_SESSIONS", "5")
    monkeypatch.setenv("BACKTEST_LOG_LEVEL", "debug")

    cfg = backtest_service_config_from_env()
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 9001
    assert cfg.workers == 3
    assert cfg.max_concurrent_backtest_jobs == 8
    assert cfg.max_concurrent_paper_sessions == 5
    assert cfg.log_level == "debug"


@pytest.mark.parametrize(
    "env_var",
    [
        "BACKTEST_SERVICE_PORT",
        "BACKTEST_SERVICE_WORKERS",
        "BACKTEST_MAX_JOBS",
        "BACKTEST_MAX_PAPER_SESSIONS",
    ],
)
def test_backtest_service_config_rejects_malformed_integer_env(
    monkeypatch: pytest.MonkeyPatch,
    env_var: str,
) -> None:
    monkeypatch.setenv(env_var, "not-an-int")

    with pytest.raises(ValueError):
        backtest_service_config_from_env()


def test_simulation_config_defaults() -> None:
    cfg = simulation_config_from_env()
    assert cfg.enabled is DEFAULT_SIMULATION_ENABLED
    assert cfg.timeout_seconds == DEFAULT_SIMULATION_TIMEOUT_SECONDS
    assert cfg.prefer_alchemy is DEFAULT_PREFER_ALCHEMY
    assert cfg.tenderly_account is None
    assert cfg.alchemy_api_key is None


def test_simulation_config_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_SIMULATION_ENABLED", "false")
    monkeypatch.setenv("ALMANAK_SIMULATION_TIMEOUT", "22.5")
    monkeypatch.setenv("ALMANAK_SIMULATION_PREFER_ALCHEMY", "yes")
    monkeypatch.setenv("TENDERLY_ACCOUNT_SLUG", "acct")
    monkeypatch.setenv("TENDERLY_PROJECT_SLUG", "proj")
    monkeypatch.setenv("TENDERLY_ACCESS_KEY", "tenderly-key")
    monkeypatch.setenv("ALCHEMY_API_KEY", "alchemy-key")

    cfg = simulation_config_from_env()
    assert cfg.enabled is False
    assert cfg.timeout_seconds == 22.5
    assert cfg.prefer_alchemy is True
    assert cfg.tenderly_account == "acct"
    assert cfg.tenderly_project == "proj"
    assert cfg.tenderly_access_key == "tenderly-key"
    assert cfg.alchemy_api_key == "alchemy-key"


def test_simulation_config_invalid_timeout_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_SIMULATION_TIMEOUT", "garbage")
    cfg = simulation_config_from_env()
    assert cfg.timeout_seconds == DEFAULT_SIMULATION_TIMEOUT_SECONDS


def test_safe_signer_adapter_reads_wallet_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ALMANAK_PLATFORM_WALLETS",
        json.dumps(
            [
                {
                    "SAFE_ACCOUNT_ADDRESS": _TEST_SAFE_ADDRESS,
                    "EOA_ADDRESS": _TEST_EOA_ADDRESS,
                }
            ]
        ),
    )

    cfg = create_signer_config_from_env(_TEST_SAFE_ADDRESS, _TEST_PRIVATE_KEY, mode="direct")
    assert cfg.wallet_config.safe_address == _TEST_SAFE_ADDRESS
    assert cfg.wallet_config.eoa_address == _TEST_EOA_ADDRESS
    assert cfg.signer_service_url is None
    assert cfg.signer_service_jwt is None


def test_safe_signer_adapter_reads_remote_signer_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ALMANAK_PLATFORM_WALLETS",
        json.dumps(
            [
                {
                    "SAFE_ACCOUNT_ADDRESS": _TEST_SAFE_ADDRESS,
                    "EOA_ADDRESS": _TEST_EOA_ADDRESS,
                    "ZODIAC_ROLES_ADDRESS": _TEST_ZODIAC_ADDRESS,
                }
            ]
        ),
    )
    monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT", "https://signer.example.com")
    monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_JWT", "jwt-token")

    cfg = create_signer_config_from_env(_TEST_SAFE_ADDRESS, _TEST_PRIVATE_KEY, mode="zodiac")
    assert cfg.mode == "zodiac"
    assert cfg.signer_service_url == "https://signer.example.com"
    assert cfg.signer_service_jwt == "jwt-token"
    assert cfg.wallet_config.zodiac_roles_address == _TEST_ZODIAC_ADDRESS


def test_legacy_simulation_config_from_env_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_SIMULATION_ENABLED", "false")
    monkeypatch.setenv("ALMANAK_SIMULATION_TIMEOUT", "22.5")
    monkeypatch.setenv("ALMANAK_SIMULATION_PREFER_ALCHEMY", "yes")
    monkeypatch.setenv("TENDERLY_ACCOUNT_SLUG", "acct")
    monkeypatch.setenv("TENDERLY_PROJECT_SLUG", "proj")
    monkeypatch.setenv("TENDERLY_ACCESS_KEY", "tenderly-key")
    monkeypatch.setenv("ALCHEMY_API_KEY", "alchemy-key")

    cfg = LegacySimulationConfig.from_env()
    assert cfg.enabled is False
    assert cfg.timeout_seconds == 22.5
    assert cfg.prefer_alchemy is True
    assert cfg.tenderly_account == "acct"
    assert cfg.tenderly_project == "proj"
    assert cfg.tenderly_access_key == "tenderly-key"
    assert cfg.alchemy_api_key == "alchemy-key"


def test_legacy_backtest_service_from_env_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_SERVICE_HOST", "127.0.0.1")
    monkeypatch.setenv("BACKTEST_SERVICE_PORT", "9001")
    monkeypatch.setenv("BACKTEST_SERVICE_WORKERS", "3")
    monkeypatch.setenv("BACKTEST_MAX_JOBS", "8")
    monkeypatch.setenv("BACKTEST_MAX_PAPER_SESSIONS", "5")
    monkeypatch.setenv("BACKTEST_LOG_LEVEL", "debug")

    cfg = LegacyBacktestServiceConfig.from_env()
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 9001
    assert cfg.workers == 3
    assert cfg.max_concurrent_backtest_jobs == 8
    assert cfg.max_concurrent_paper_sessions == 5
    assert cfg.log_level == "debug"


def test_token_safety_client_reads_rugcheck_key_from_typed_framework_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUGCHECK_API_KEY", "rugcheck-secret")
    client = TokenSafetyClient()
    assert client._rugcheck_api_key == "rugcheck-secret"
