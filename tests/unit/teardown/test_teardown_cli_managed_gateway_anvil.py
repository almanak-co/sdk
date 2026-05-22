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
        self.deployment_id = "vib_3819_probe"

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
                "deployment_id": "vib_3819_probe",
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
                "deployment_id": "vib_3819_multichain_probe",
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
                "deployment_id": "vib_3819_solana_probe",
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

    # VIB-3878 wired SolanaForkManager into the teardown CLI for solana strategies
    # on --network anvil. Mock it so the test does not require a real
    # solana-test-validator binary in CI (which doesn't ship one).
    async def _async_true(*_args, **_kwargs):
        return True

    async def _async_none(*_args, **_kwargs):
        return None

    fake_solana_fork_mgr = MagicMock()
    fake_solana_fork_mgr.start = MagicMock(side_effect=_async_true)
    fake_solana_fork_mgr.stop = MagicMock(side_effect=_async_none)
    fake_solana_fork_mgr.fund_wallet = MagicMock(side_effect=_async_true)
    fake_solana_fork_mgr.fund_tokens = MagicMock(side_effect=_async_true)
    fake_solana_fork_mgr.get_rpc_url = MagicMock(return_value="http://127.0.0.1:8899")
    monkeypatch.setattr(
        "almanak.framework.anvil.solana_fork_manager.SolanaForkManager",
        MagicMock(return_value=fake_solana_fork_mgr),
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
    # VIB-3878: fork manager should have been started and the wallet funded.
    fake_solana_fork_mgr.start.assert_called_once()
    fake_solana_fork_mgr.fund_wallet.assert_called_once()


def test_solana_fork_startup_failure_aborts_with_clear_message(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """VIB-3878: a Solana fork that fails to start must abort the CLI with a
    diagnostic mentioning the install command — never silently fall through to
    teardown logic against a dead validator (CodeRabbit P_major lifecycle ask)."""
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
    monkeypatch.setattr(
        "almanak.gateway.managed.ManagedGateway",
        MagicMock(return_value=fake_managed_gateway_instance),
    )
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    async def _async_false(*_args, **_kwargs):
        return False

    async def _async_none(*_args, **_kwargs):
        return None

    fake_solana_fork_mgr = MagicMock()
    fake_solana_fork_mgr.start = MagicMock(side_effect=_async_false)  # startup FAILS
    fake_solana_fork_mgr.stop = MagicMock(side_effect=_async_none)
    monkeypatch.setattr(
        "almanak.framework.anvil.solana_fork_manager.SolanaForkManager",
        MagicMock(return_value=fake_solana_fork_mgr),
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        ["execute", "-d", str(tmp_path), "-c", config_file, "--network", "anvil", "--mode", "graceful", "--force"],
    )

    assert result.exit_code != 0, "CLI must abort when solana-test-validator fails to start"
    assert "Failed to start solana-test-validator" in result.output
    # ManagedGateway must be torn down to avoid leaking the gRPC daemon when the
    # Solana side fails after the EVM gateway already started.
    fake_managed_gateway_instance.stop.assert_called()


def test_solana_fork_stop_idempotent_no_double_call(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """VIB-3878: success-path finally cleanup must mark ``solana_stopped=True``
    so the atexit safety-net doesn't re-stop after the loop closed (CodeRabbit
    P_major lifecycle ask). Counts ``stop()`` invocations to pin the contract.

    Captures the registered atexit callback and invokes it manually after the
    CLI returns — atexit handlers don't fire inside ``CliRunner.invoke`` (the
    pytest process keeps running), so without this capture the test would only
    prove the ``finally``-path single stop, not that the atexit safety-net
    short-circuits via ``solana_stopped``.
    """
    _, config_file = _write_solana_strategy_files(tmp_path)

    registered_atexit_callbacks: list = []

    def _capture_atexit(fn, *args, **kwargs):
        registered_atexit_callbacks.append(fn)

    # ``atexit`` is imported lazily inside the teardown function (line 637),
    # so we can't patch ``teardown_cli_module.atexit.register`` (the attribute
    # doesn't exist at module level). Patch ``atexit.register`` itself; the
    # other tests in this file don't rely on real atexit firing.
    import atexit as _atexit_mod

    monkeypatch.setattr(_atexit_mod, "register", _capture_atexit)

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
    monkeypatch.setattr(
        "almanak.gateway.managed.ManagedGateway",
        MagicMock(return_value=MagicMock(start=MagicMock(return_value=None), stop=MagicMock(return_value=None))),
    )
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    async def _async_true(*_args, **_kwargs):
        return True

    async def _async_none(*_args, **_kwargs):
        return None

    fake_solana_fork_mgr = MagicMock()
    fake_solana_fork_mgr.start = MagicMock(side_effect=_async_true)
    fake_solana_fork_mgr.stop = MagicMock(side_effect=_async_none)
    fake_solana_fork_mgr.fund_wallet = MagicMock(side_effect=_async_true)
    fake_solana_fork_mgr.fund_tokens = MagicMock(side_effect=_async_true)
    fake_solana_fork_mgr.get_rpc_url = MagicMock(return_value="http://127.0.0.1:8899")
    monkeypatch.setattr(
        "almanak.framework.anvil.solana_fork_manager.SolanaForkManager",
        MagicMock(return_value=fake_solana_fork_mgr),
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        ["execute", "-d", str(tmp_path), "-c", config_file, "--network", "anvil", "--mode", "graceful", "--force"],
    )

    assert result.exit_code == 0, result.output
    # Sanity: the teardown CLI registers an atexit safety-net for the Solana
    # validator. Without this, the captured-callback list would be empty and
    # the test below would silently degrade to "did nothing".
    assert registered_atexit_callbacks, "Expected teardown to register a Solana atexit cleanup"
    # The success-path finally cleanup already ran ``stop()`` once. Now invoke
    # the atexit callback by hand (CliRunner doesn't fire atexit) — the
    # ``solana_stopped`` flag set by the finally must short-circuit it so
    # ``stop()`` stays at exactly 1 call.
    for fn in registered_atexit_callbacks:
        fn()
    assert fake_solana_fork_mgr.stop.call_count == 1, (
        f"Expected exactly 1 stop() call (idempotent atexit), got {fake_solana_fork_mgr.stop.call_count}"
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
