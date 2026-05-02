"""VIB-3819 — `almanak strat teardown execute --network anvil` must boot a fork.

The April 30 QA batch reported `uniswap_v4_swap_base` teardown exit-1 (PARTIAL)
while `uniswap_v4_swap_arbitrum` passed. Reproduction on Anvil Base fork showed
the real cause: the teardown CLI was constructing ``ManagedGateway(settings)``
without passing ``anvil_chains``, so the gateway started but no fork was
spawned. The balance provider then hit a dead RPC port (8548), the strategy's
``get_open_positions()`` swallowed the error and returned empty, and VIB-3705's
no-op branch silently exited 0 — leaving the WETH position stranded on-chain.

This regression guard pins the contract that the managed-gateway constructor
receives ``anvil_chains=[<config.chain>]`` AND ``wallet_address`` AND
``anvil_funding`` whenever ``--network anvil`` is used. The ``strat run`` path
in ``run_helpers.py`` already does this; teardown was the divergent path.
"""

from __future__ import annotations

import importlib
import json
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

teardown_cli_module = importlib.import_module("almanak.framework.cli.teardown")


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


class _FakeGatewayClient:
    """Minimal gateway client that satisfies the CLI's connect/health probe."""

    def __init__(self, _config) -> None:
        self.connected = False
        self.channel = None

    def connect(self) -> None:
        self.connected = True

    def health_check(self) -> bool:
        return True

    def disconnect(self) -> None:
        self.connected = False


class _SwapOnlyStrategy:
    """Strategy with no positions — short-circuits early so we never reach the
    orchestrator. Test focuses purely on ManagedGateway construction args."""

    STRATEGY_NAME = "vib_3819_probe"

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.strategy_id = "vib_3819_probe"

    def get_open_positions(self):
        from types import SimpleNamespace

        return SimpleNamespace(positions=[])

    def create_market_snapshot(self):
        from types import SimpleNamespace

        return SimpleNamespace(get_price_oracle_dict=lambda: {})

    def generate_teardown_intents(self, _mode, market=None):
        return []


def _write_strategy_files(tmp_path) -> tuple[str, str]:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text("# placeholder — load_strategy_from_file is monkeypatched\n")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "chain": "base",
                "wallet_address": "0x0000000000000000000000000000000000000001",
                "strategy_id": "vib_3819_probe",
                "anvil_funding": {"WETH": 1, "USDC": 1000, "ETH": 1},
            }
        )
    )
    return str(strategy_file), str(config_file)


def test_managed_gateway_receives_anvil_chains_for_anvil_network(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """VIB-3819: ``--network anvil`` MUST forward anvil_chains/wallet/funding.

    Without these args, ``ManagedGateway._start_anvil_forks()`` short-circuits
    (see ``managed.py:577``: ``if self._anvil_chains and self.settings.network
    == "anvil"``), the fork never starts, and every downstream RPC call hits a
    dead port — masked by VIB-3705 as a silent no-op exit 0.
    """
    _, config_file = _write_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_SwapOnlyStrategy, None),
    )
    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClient",
        _FakeGatewayClient,
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    fake_managed_gateway_instance = MagicMock()
    fake_managed_gateway_instance.start = MagicMock(return_value=None)
    fake_managed_gateway_instance.stop = MagicMock(return_value=None)
    managed_gateway_ctor = MagicMock(return_value=fake_managed_gateway_instance)

    monkeypatch.setattr("almanak.gateway.managed.ManagedGateway", managed_gateway_ctor)
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--network",
            "anvil",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output

    assert managed_gateway_ctor.called, "ManagedGateway was never instantiated"
    _, kwargs = managed_gateway_ctor.call_args
    assert kwargs.get("anvil_chains") == ["base"], (
        f"VIB-3819 regression: ManagedGateway must get anvil_chains=['base'] for "
        f"--network anvil + chain=base, got anvil_chains={kwargs.get('anvil_chains')!r}"
    )
    assert kwargs.get("wallet_address") == "0x0000000000000000000000000000000000000001", (
        "VIB-3819 regression: wallet_address must be passed so the Anvil fork "
        f"can pre-fund it, got wallet_address={kwargs.get('wallet_address')!r}"
    )
    assert kwargs.get("anvil_funding") == {"WETH": 1, "USDC": 1000, "ETH": 1}, (
        "VIB-3819 regression: anvil_funding from config.json must be forwarded, "
        f"got anvil_funding={kwargs.get('anvil_funding')!r}"
    )

    fake_managed_gateway_instance.start.assert_called_once()
    _, start_kwargs = fake_managed_gateway_instance.start.call_args
    timeout = start_kwargs.get("timeout") if start_kwargs else None
    if timeout is None and fake_managed_gateway_instance.start.call_args.args:
        timeout = fake_managed_gateway_instance.start.call_args.args[0]
    assert timeout is not None and timeout >= 30.0, (
        f"VIB-3819 regression: timeout must scale with per-chain Anvil budget "
        f"(>= 30s for L2s), got timeout={timeout!r} (10s default starves the fork)"
    )


def _write_multi_chain_strategy_files(tmp_path) -> tuple[str, str]:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text("# placeholder — load_strategy_from_file is monkeypatched\n")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "chain": "base",
                "chains": ["base", "arbitrum"],
                "wallet_address": "0x0000000000000000000000000000000000000001",
                "strategy_id": "vib_3819_multichain_probe",
                "anvil_funding": {"WETH": 1, "USDC": 1000},
            }
        )
    )
    return str(strategy_file), str(config_file)


def test_managed_gateway_forks_every_chain_for_multi_chain_config(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """VIB-3819 + Codex follow-up: multi-chain configs must boot every fork.

    A strategy with ``chains: ["base", "arbitrum"]`` holds positions on both
    chains. Forking only the scalar ``chain`` would leave the second chain's
    teardown intents routed at a missing ``ANVIL_<CHAIN>_PORT`` endpoint.
    Mirrors run_helpers.py:882-897.
    """
    _, config_file = _write_multi_chain_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_SwapOnlyStrategy, None),
    )
    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClient",
        _FakeGatewayClient,
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    fake_managed_gateway_instance = MagicMock()
    fake_managed_gateway_instance.start = MagicMock(return_value=None)
    fake_managed_gateway_instance.stop = MagicMock(return_value=None)
    managed_gateway_ctor = MagicMock(return_value=fake_managed_gateway_instance)

    monkeypatch.setattr("almanak.gateway.managed.ManagedGateway", managed_gateway_ctor)
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--network",
            "anvil",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    _, kwargs = managed_gateway_ctor.call_args
    assert kwargs.get("anvil_chains") == ["base", "arbitrum"], (
        f"Multi-chain teardown regression: ManagedGateway must get every chain "
        f"from config['chains'], got anvil_chains={kwargs.get('anvil_chains')!r}"
    )


def _write_solana_strategy_files(tmp_path) -> tuple[str, str]:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text("# placeholder — load_strategy_from_file is monkeypatched\n")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "chain": "solana",
                "wallet_address": "0x0000000000000000000000000000000000000001",
                "strategy_id": "vib_3819_solana_probe",
            }
        )
    )
    return str(strategy_file), str(config_file)


def test_solana_chain_filtered_from_anvil_chains(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Solana uses solana-test-validator, NOT Anvil — must be filtered out.

    Without the filter, ``ManagedGateway`` would attempt to start a
    ``RollingForkManager`` for "solana" and fail. Mirrors the
    ``NON_EVM_CHAINS`` filter in run_helpers.py:907-910 (claude pr-auditor
    finding #1).
    """
    _, config_file = _write_solana_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_SwapOnlyStrategy, None),
    )
    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClient",
        _FakeGatewayClient,
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    fake_managed_gateway_instance = MagicMock()
    fake_managed_gateway_instance.start = MagicMock(return_value=None)
    fake_managed_gateway_instance.stop = MagicMock(return_value=None)
    managed_gateway_ctor = MagicMock(return_value=fake_managed_gateway_instance)

    monkeypatch.setattr("almanak.gateway.managed.ManagedGateway", managed_gateway_ctor)
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--network",
            "anvil",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    _, kwargs = managed_gateway_ctor.call_args
    assert kwargs.get("anvil_chains") == [], (
        f"Solana must be filtered from anvil_chains, got anvil_chains={kwargs.get('anvil_chains')!r}"
    )


def test_managed_gateway_no_anvil_chains_when_network_is_mainnet(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Mainnet path must NOT pass anvil_chains — only the anvil branch boots a fork."""
    _, config_file = _write_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_SwapOnlyStrategy, None),
    )
    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClient",
        _FakeGatewayClient,
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    fake_managed_gateway_instance = MagicMock()
    fake_managed_gateway_instance.start = MagicMock(return_value=None)
    fake_managed_gateway_instance.stop = MagicMock(return_value=None)
    managed_gateway_ctor = MagicMock(return_value=fake_managed_gateway_instance)

    monkeypatch.setattr("almanak.gateway.managed.ManagedGateway", managed_gateway_ctor)
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--network",
            "mainnet",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    _, kwargs = managed_gateway_ctor.call_args
    assert kwargs.get("anvil_chains") in ([], None), (
        f"Mainnet path must not boot an Anvil fork, got anvil_chains={kwargs.get('anvil_chains')!r}"
    )
    assert kwargs.get("anvil_funding") in ({}, None), (
        f"Mainnet path must not forward anvil_funding, got anvil_funding={kwargs.get('anvil_funding')!r}"
    )
