"""Targeted tests for the VIB-4425 demo runtime helper slice."""

from __future__ import annotations

import os

import pytest

from almanak.config.demo_runtime import (
    demo_alchemy_api_key,
    demo_anvil_port,
    demo_anvil_url,
    demo_chain_rpc_url,
    demo_fork_block,
    demo_subprocess_env,
    load_demo_dotenv,
)

_ENV_VARS: tuple[str, ...] = (
    "ALMANAK_ETHEREUM_RPC_URL",
    "ALMANAK_ARBITRUM_RPC_URL",
    "ALMANAK_BASE_RPC_URL",
    "ETHEREUM_RPC_URL",
    "ARBITRUM_RPC_URL",
    "BASE_RPC_URL",
    "ALMANAK_RPC_URL",
    "RPC_URL",
    "ANVIL_URL",
    "ANVIL_ETHEREUM_PORT",
    "ANVIL_ARBITRUM_PORT",
    "ANVIL_FORK_BLOCK",
    "ANVIL_FORK_BLOCK_ARBITRUM",
    "ALCHEMY_API_KEY",
)


@pytest.fixture(autouse=True)
def _scrub_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_demo_chain_rpc_url_uses_full_ladder_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_RPC_URL", "https://generic")
    monkeypatch.setenv("ALMANAK_ARBITRUM_RPC_URL", "https://chain-specific")

    assert demo_chain_rpc_url("arbitrum") == "https://chain-specific"

    monkeypatch.delenv("ALMANAK_ARBITRUM_RPC_URL", raising=False)
    assert demo_chain_rpc_url("arbitrum") == "https://generic"


def test_demo_chain_rpc_url_can_ignore_generic_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_RPC_URL", "https://generic")

    assert demo_chain_rpc_url("base", allow_generic_fallback=False) is None
    assert demo_chain_rpc_url("base", allow_generic_fallback=False, fallback="https://mainnet.base.org") == (
        "https://mainnet.base.org"
    )


def test_demo_anvil_url_prefers_generic_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANVIL_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("ANVIL_ARBITRUM_PORT", "8546")

    assert demo_anvil_url("arbitrum", default_port=8545) == "http://127.0.0.1:9999"


def test_demo_anvil_url_rebuilds_from_chain_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANVIL_ETHEREUM_PORT", "8551")

    assert demo_anvil_port("ethereum", default=8549) == 8551
    assert demo_anvil_url("ethereum", default_port=8549) == "http://127.0.0.1:8551"


def test_demo_fork_block_prefers_chain_specific_then_generic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANVIL_FORK_BLOCK", "123456")
    assert demo_fork_block("arbitrum") == "123456"

    monkeypatch.setenv("ANVIL_FORK_BLOCK_ARBITRUM", "654321")
    assert demo_fork_block("arbitrum") == "654321"


def test_demo_fork_block_whitespace_chain_specific_falls_back_to_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whitespace in the chain-specific key must not block the generic fallback.

    Without this, ``or`` short-circuits on the truthy whitespace string and the
    generic value never gets checked — both keys yield None.
    """
    monkeypatch.setenv("ANVIL_FORK_BLOCK_ARBITRUM", "   ")
    monkeypatch.setenv("ANVIL_FORK_BLOCK", "123456")

    assert demo_fork_block("arbitrum") == "123456"


def test_load_demo_dotenv_loads_env_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """``load_demo_dotenv`` reads ``<project_root>/.env`` into ``os.environ``."""
    env_file = tmp_path / ".env"
    env_file.write_text("ALMANAK_DEMO_RUNTIME_TEST_FLAG=loaded\n", encoding="utf-8")
    monkeypatch.delenv("ALMANAK_DEMO_RUNTIME_TEST_FLAG", raising=False)

    load_demo_dotenv(tmp_path)

    assert os.environ.get("ALMANAK_DEMO_RUNTIME_TEST_FLAG") == "loaded"


def test_demo_subprocess_env_sets_chain_and_rpc_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO", "bar")

    env = demo_subprocess_env(
        chain="arbitrum",
        rpc_url="http://127.0.0.1:8545",
        private_key="0xabc",
        extra_overrides={"EXTRA_FLAG": "1"},
    )

    assert env["FOO"] == "bar"
    assert env["ALMANAK_CHAIN"] == "arbitrum"
    assert env["ALMANAK_RPC_URL"] == "http://127.0.0.1:8545"
    assert env["ALMANAK_ARBITRUM_RPC_URL"] == "http://127.0.0.1:8545"
    assert env["ALMANAK_PRIVATE_KEY"] == "0xabc"
    assert env["EXTRA_FLAG"] == "1"


def test_demo_alchemy_api_key_uses_backtest_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALCHEMY_API_KEY", "alchemy-secret")
    assert demo_alchemy_api_key() == "alchemy-secret"
