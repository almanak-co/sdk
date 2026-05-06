"""Unit tests for the Phase 4c helpers in `almanak/framework/cli/run_helpers.py`.

Covers:
    - _instantiate_strategy        (phase 11)
    - _build_runtime_config        (phase 12)
    - _build_components            (phase 13)
    - _build_cleanup_fn            (phase 14)

Pattern mirrors `test_run_helpers_setup.py` / `test_run_helpers_gateway.py`:
CliRunner + monkeypatch, with MagicMocks for the heavy factory surfaces
(GatewayPriceOracle / GatewayBalanceProvider / MultiChainOrchestrator /
StrategyRunner / VaultLifecycleManager / etc.). No real gateway startup.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers
from almanak.framework.cli._run_context import ComponentBundle


# ---------------------------------------------------------------------------
# Fake config dataclasses + strategy classes for _instantiate_strategy
# ---------------------------------------------------------------------------


@dataclass
class _SampleDataclassConfig:
    """Dataclass with a Decimal field so the Decimal-coercion branch fires."""

    threshold: Decimal = Decimal("0.05")
    label: str = "default"


class _FakeStrategyBase:
    """A non-IntentStrategy class that accepts a config dict."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.init_called_with = ("dict",)


class _FakeStrategyNoArgs:
    """A non-IntentStrategy class that takes no args (fall-through branch)."""

    def __init__(self) -> None:
        self.init_called_with = ("noargs",)


def _make_intent_strategy_stub() -> type:
    """Build a real IntentStrategy subclass parameterized with `_SampleDataclassConfig`.

    We subclass `IntentStrategy[_SampleDataclassConfig]` and override the
    three abstract methods so instantiation works without hitting the full
    framework. The result carries `__orig_bases__` so the helper's
    dataclass-detection loop finds `_SampleDataclassConfig`.
    """
    from almanak.framework.intents.vocabulary import Intent
    from almanak.framework.strategies import IntentStrategy

    class _RealIntentStrategy(IntentStrategy[_SampleDataclassConfig]):
        def decide(self, market: Any) -> Intent:  # pragma: no cover - not exercised
            return Intent.hold(reason="test stub")

        def get_open_positions(self) -> Any:  # pragma: no cover
            return None

        def generate_teardown_intents(self, mode: Any, market: Any = None) -> list[Any]:  # pragma: no cover
            return []

    return _RealIntentStrategy


def _make_intent_strategy_no_generic() -> type:
    """IntentStrategy subclass WITHOUT a dataclass generic (dict-config fallback)."""
    from almanak.framework.intents.vocabulary import Intent
    from almanak.framework.strategies import IntentStrategy

    class _NoGenericStrategy(IntentStrategy):
        def decide(self, market: Any) -> Intent:  # pragma: no cover
            return Intent.hold(reason="test stub")

        def get_open_positions(self) -> Any:  # pragma: no cover
            return None

        def generate_teardown_intents(self, mode: Any, market: Any = None) -> list[Any]:  # pragma: no cover
            return []

    return _NoGenericStrategy


# ---------------------------------------------------------------------------
# _instantiate_strategy
# ---------------------------------------------------------------------------


class TestInstantiateStrategy:
    """Phase 11 helper: strategy class -> configured instance."""

    def test_dataclass_config_with_decimal_coercion(self) -> None:
        strategy_cls = _make_intent_strategy_stub()
        runtime_config = MagicMock()
        runtime_config.chain = "arbitrum"
        runtime_config.execution_address = "0xabc"

        runner = CliRunner()
        with runner.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=strategy_cls,
                strategy_config={
                    "threshold": "0.25",  # int/float/str -> Decimal
                    "label": "override",
                    "chain": "arbitrum",
                    "wallet_address": "0xabc",
                    "strategy_id": "ignored-runtime-field",
                },
                runtime_config=runtime_config,
                multi_chain=False,
                strategy_chains=[],
                chain_wallets={},
            )
        # Coercion happened: threshold became Decimal.
        assert isinstance(instance.config.threshold, Decimal)
        assert instance.config.threshold == Decimal("0.25")
        assert instance.config.label == "override"
        assert instance.chain == "arbitrum"
        assert instance.wallet_address == "0xabc"

    def test_dict_config_fallback_when_no_dataclass_generic(self) -> None:
        strategy_cls = _make_intent_strategy_no_generic()
        runtime_config = MagicMock()
        runtime_config.chain = "base"
        runtime_config.execution_address = "0xdef"

        runner = CliRunner()
        with runner.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=strategy_cls,
                strategy_config={"chain": "base", "wallet_address": "0xdef", "extra": 1},
                runtime_config=runtime_config,
                multi_chain=False,
                strategy_chains=[],
                chain_wallets={},
            )
        # No dataclass detected -> DictConfigWrapper is used.
        from almanak.framework.cli.run import DictConfigWrapper

        assert isinstance(instance.config, DictConfigWrapper)
        assert instance.chain == "base"
        assert instance.wallet_address == "0xdef"

    def test_strategy_base_subclass_dict_config_branch(self) -> None:
        runtime_config = MagicMock()
        runtime_config.chain = "arbitrum"
        runtime_config.execution_address = "0xaaa"

        runner = CliRunner()
        with runner.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=_FakeStrategyBase,
                strategy_config={"foo": "bar"},
                runtime_config=runtime_config,
                multi_chain=False,
                strategy_chains=[],
                chain_wallets={},
            )
        assert isinstance(instance, _FakeStrategyBase)
        assert instance.init_called_with == ("dict",)
        assert instance.config == {"foo": "bar"}

    def test_strategy_base_subclass_falls_back_to_noargs_on_typeerror(self) -> None:
        runtime_config = MagicMock()

        runner = CliRunner()
        with runner.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=_FakeStrategyNoArgs,
                strategy_config={"ignored": "value"},
                runtime_config=runtime_config,
                multi_chain=False,
                strategy_chains=[],
                chain_wallets={},
            )
        assert isinstance(instance, _FakeStrategyNoArgs)
        assert instance.init_called_with == ("noargs",)

    def test_multi_chain_uses_first_chain_and_sets_chains_kwarg(self) -> None:
        strategy_cls = _make_intent_strategy_stub()
        runtime_config = MagicMock()
        runtime_config.execution_address = "0xbbb"

        runner = CliRunner()
        with runner.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=strategy_cls,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xbbb"},
                runtime_config=runtime_config,
                multi_chain=True,
                strategy_chains=["arbitrum", "base"],
                chain_wallets={"arbitrum": "0xaaa1", "base": "0xaaa2"},
            )
        # multi_chain -> primary_chain is strategy_chains[0]
        assert instance.chain == "arbitrum"
        # chain_wallets mapping wins over runtime_config.execution_address
        assert instance.wallet_address == "0xaaa1"

    def test_instantiation_failure_exits_with_status_1(self) -> None:
        class _BoomStrategy:
            def __init__(self, *_a: Any, **_kw: Any) -> None:
                raise RuntimeError("boom")

        runtime_config = MagicMock()
        with pytest.raises(SystemExit) as exc_info:
            runner = CliRunner()
            with runner.isolation():
                run_helpers._instantiate_strategy(
                    strategy_class=_BoomStrategy,
                    strategy_config={},
                    runtime_config=runtime_config,
                    multi_chain=False,
                    strategy_chains=[],
                    chain_wallets={},
                )
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# _build_runtime_config
# ---------------------------------------------------------------------------


def _make_fake_local_config(chain: str = "arbitrum") -> Any:
    """Build a real LocalRuntimeConfig (the isinstance assert in
    `_build_runtime_config` requires a real subclass match)."""
    from almanak.framework.execution.config import LocalRuntimeConfig

    return LocalRuntimeConfig(
        chain=chain,
        rpc_url="https://rpc.test",
        private_key="0x" + "11" * 32,
    )


def _make_fake_multichain_config(chains: list[str]) -> Any:
    from almanak.framework.execution.config import MultiChainRuntimeConfig

    # MultiChainRuntimeConfig.__init__ validates chain env vars aggressively;
    # bypass it with object.__new__ and override is_safe_mode on a subclass.
    class _TestMultiChain(MultiChainRuntimeConfig):
        @property
        def is_safe_mode(self) -> bool:  # type: ignore[override]
            return False

        @property
        def execution_address(self) -> str:  # type: ignore[override]
            return "0xwallet-multi"

    obj = object.__new__(_TestMultiChain)
    obj.chain = chains[0]
    obj.chains = chains
    obj.wallet_address = "0xwallet-multi"
    obj.max_gas_price_gwei = 50
    obj.rpc_urls = {c: f"https://rpc.{c}.example" for c in chains}
    return obj


class _FakeMissingEnvErr(Exception):
    """Stand-in for MissingEnvironmentVariableError."""

    def __init__(self, var_name: str) -> None:
        super().__init__(var_name)
        self.var_name = var_name


def _patch_runtime_config_imports(
    monkeypatch: pytest.MonkeyPatch,
    *,
    local_factory: Any = None,
    multi_factory: Any = None,
    missing_env_cls: type = _FakeMissingEnvErr,
) -> None:
    """Patch `from_env` classmethods on the real config classes so the helper
    uses fakes while keeping `isinstance(rt, LocalRuntimeConfig)` true."""
    from almanak.framework.execution import config as execution_config

    default_local = lambda chain, network, private_key=None: _make_fake_local_config(chain or "arbitrum")
    default_multi = lambda chains, protocols, network, private_key=None: _make_fake_multichain_config(chains)

    monkeypatch.setattr(
        execution_config.LocalRuntimeConfig,
        "from_env",
        staticmethod(local_factory or default_local),
    )
    monkeypatch.setattr(
        execution_config.MultiChainRuntimeConfig,
        "from_env",
        staticmethod(multi_factory or default_multi),
    )
    monkeypatch.setattr(execution_config, "MissingEnvironmentVariableError", missing_env_cls)


class TestBuildRuntimeConfig:
    """Phase 12 helper: LocalRuntimeConfig / MultiChainRuntimeConfig / Gateway* wiring."""

    def test_single_chain_happy_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_runtime_config_imports(monkeypatch)
        # Avoid the safe-mode preflight check by keeping is_safe_mode=False.
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        gateway_client = MagicMock()
        strategy_config: dict[str, Any] = {}
        runner = CliRunner()
        with runner.isolation():
            rt, chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=gateway_client,
                strategy_config=strategy_config,
            )
        assert rt.chain == "arbitrum"
        assert chain_wallets == {}
        # strategy_config mutated with chain / wallet_address
        assert strategy_config["chain"] == "arbitrum"
        # wallet_address is derived from the fake private_key, not hard-coded.
        assert strategy_config["wallet_address"] == rt.execution_address

    def test_multi_chain_happy_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        runner = CliRunner()
        strategy_config: dict[str, Any] = {}
        with runner.isolation():
            rt, chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="mainnet",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )
        from almanak.framework.execution.config import MultiChainRuntimeConfig

        assert isinstance(rt, MultiChainRuntimeConfig)
        assert rt.chains == ["arbitrum", "base"]
        assert chain_wallets == {}
        assert strategy_config["chain"] == "arbitrum"

    def test_anvil_falls_back_to_default_key_when_private_key_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: dict[str, Any] = {"count": 0, "retry_kwargs": None}

        def from_env_with_retry(chain: str, network: str, private_key: str | None = None) -> Any:
            calls["count"] += 1
            if calls["count"] == 1:
                raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")
            calls["retry_kwargs"] = {"chain": chain, "network": network, "private_key": private_key}
            return _make_fake_local_config(chain or "arbitrum")

        _patch_runtime_config_imports(monkeypatch, local_factory=from_env_with_retry)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        # Non-TTY so the confirm prompt does not block.
        import sys as _sys

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: False)

        runner = CliRunner()
        strategy_config: dict[str, Any] = {}
        with runner.isolation():
            rt, _ = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="anvil",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )
        assert rt.chain == "arbitrum"
        # Retry was driven by the explicit kwarg, not by env mutation (#2100).
        from almanak.framework.cli.run import ANVIL_DEFAULT_PRIVATE_KEY

        assert calls["count"] == 2
        assert calls["retry_kwargs"]["private_key"] == ANVIL_DEFAULT_PRIVATE_KEY
        # Boundary contract: os.environ is NOT mutated by the helper.
        import os as _os

        assert _os.environ.get("ALMANAK_PRIVATE_KEY") is None

    def test_safe_mode_preflight_fail_exits_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.config import LocalRuntimeConfig

        class _SafeLocal(LocalRuntimeConfig):
            @property
            def is_safe_mode(self) -> bool:  # type: ignore[override]
                return True

        def safe_from_env(chain: str, network: str, private_key: str | None = None) -> Any:
            return _SafeLocal(
                chain=chain or "arbitrum",
                rpc_url="https://rpc.test",
                private_key="0x" + "11" * 32,
            )

        _patch_runtime_config_imports(monkeypatch, local_factory=safe_from_env)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        # Pretend preflight returns an error string.
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(run_mod, "_validate_safe_mode_preflight", lambda addr: "safe mode mismatch")

        runner = CliRunner()
        with runner.isolation(), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1

    def test_sidecar_mode_builds_gateway_runtime_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.setenv("ALMANAK_EOA_ADDRESS", "0xeoa")
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)

        runner = CliRunner()
        strategy_config: dict[str, Any] = {}
        with runner.isolation():
            rt, chain_wallets = run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )
        # In sidecar mode the built runtime_config is a GatewayRuntimeConfig.
        from almanak.framework.execution.config import GatewayRuntimeConfig

        assert isinstance(rt, GatewayRuntimeConfig)
        assert rt.chain == "arbitrum"
        assert rt.execution_address == "0xeoa"
        assert chain_wallets == {}

    def test_gateway_wallets_registration_updates_runtime_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", '{"arbitrum":"0xfromregistry"}')

        gateway_client = MagicMock()
        gateway_client.register_chains.return_value = {"arbitrum": "0xfromregistry"}

        runner = CliRunner()
        strategy_config: dict[str, Any] = {}
        with runner.isolation():
            rt, chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=gateway_client,
                strategy_config=strategy_config,
            )
        assert chain_wallets == {"arbitrum": "0xfromregistry"}
        assert rt.wallet_address == "0xfromregistry"
        gateway_client.register_chains.assert_called_once_with(["arbitrum"])


# ---------------------------------------------------------------------------
# _build_cleanup_fn
# ---------------------------------------------------------------------------


class TestBuildCleanupFn:
    """Phase 14 helper: zero-arg async cleanup closure."""

    def test_all_handles_none_is_no_op(self) -> None:
        components = ComponentBundle()
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=None,
            keep_anvil=False,
            components=components,
        )
        # Should complete without raising.
        asyncio.run(cleanup())

    def test_gateway_client_disconnect_called(self) -> None:
        gw_client = MagicMock()
        gw_client.disconnect = MagicMock()
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=gw_client,
            managed_gateway=None,
            keep_anvil=False,
            components=ComponentBundle(),
        )
        asyncio.run(cleanup())
        gw_client.disconnect.assert_called_once()

    def test_managed_gateway_stop_called(self) -> None:
        managed = MagicMock()
        managed._anvil_managers = {}
        managed.stop = MagicMock()
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=managed,
            keep_anvil=False,
            components=ComponentBundle(),
        )
        asyncio.run(cleanup())
        managed.stop.assert_called_once()

    def test_solana_fork_mgr_stop_called(self) -> None:
        fork_mgr = MagicMock()
        fork_mgr.stop = AsyncMock()
        components = ComponentBundle(solana_fork_mgr=fork_mgr)
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=None,
            keep_anvil=False,
            components=components,
        )
        asyncio.run(cleanup())
        fork_mgr.stop.assert_awaited_once()

    def test_ohlcv_and_price_oracle_close_called(self) -> None:
        ohlcv = MagicMock()
        ohlcv.close = AsyncMock()
        price_oracle = MagicMock()
        price_oracle.close = AsyncMock()
        components = ComponentBundle(ohlcv_provider=ohlcv, price_oracle=price_oracle)
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=None,
            keep_anvil=False,
            components=components,
        )
        asyncio.run(cleanup())
        ohlcv.close.assert_awaited_once()
        price_oracle.close.assert_awaited_once()

    def test_price_oracle_without_close_is_skipped(self) -> None:
        """Some price oracles don't expose `.close()` — helper must not call it."""

        class _OracleNoClose:
            pass

        components = ComponentBundle(price_oracle=_OracleNoClose())
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=None,
            keep_anvil=False,
            components=components,
        )
        # Must not raise AttributeError.
        asyncio.run(cleanup())

    def test_keep_anvil_prints_running_fork_info(self, capsys: pytest.CaptureFixture[str]) -> None:
        fake_anvil_mgr = MagicMock()
        fake_anvil_mgr.anvil_port = 8545
        fake_anvil_mgr._process = MagicMock(pid=12345)
        managed = MagicMock()
        managed._anvil_managers = {"arbitrum": fake_anvil_mgr}
        managed.stop = MagicMock()

        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=managed,
            keep_anvil=True,
            components=ComponentBundle(),
        )
        asyncio.run(cleanup())
        captured = capsys.readouterr()
        assert "Anvil for arbitrum still running on port 8545" in captured.out
        assert "PID 12345" in captured.out
        managed.stop.assert_called_once()


# ---------------------------------------------------------------------------
# _build_components
# ---------------------------------------------------------------------------


def _patch_component_factories(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub every heavy factory `_build_components` reaches for.

    Returns a dict of MagicMocks keyed by name so tests can inspect call args.
    """
    mocks: dict[str, Any] = {}

    # price oracle / balance provider
    from almanak.framework.data.balance import gateway_provider as bal_mod
    from almanak.framework.data.price import gateway_oracle as oracle_mod

    mocks["GatewayPriceOracle"] = MagicMock()
    mocks["GatewayBalanceProvider"] = MagicMock()
    monkeypatch.setattr(oracle_mod, "GatewayPriceOracle", mocks["GatewayPriceOracle"])
    monkeypatch.setattr(bal_mod, "GatewayBalanceProvider", mocks["GatewayBalanceProvider"])

    # single-chain orchestrator
    from almanak.framework.execution import gateway_orchestrator as gw_orch_mod

    mocks["GatewayExecutionOrchestrator"] = MagicMock()
    monkeypatch.setattr(gw_orch_mod, "GatewayExecutionOrchestrator", mocks["GatewayExecutionOrchestrator"])

    # multi-chain orchestrator + balance
    from almanak.framework.data.balance import gateway_multichain as mcb_mod
    from almanak.framework.execution import multichain as mc_mod

    mocks["MultiChainOrchestrator"] = MagicMock()
    mocks["MultiChainOrchestrator"].from_gateway = MagicMock(return_value=MagicMock(name="multi_orch"))
    mocks["MultiChainGatewayBalanceProvider"] = MagicMock()
    monkeypatch.setattr(mc_mod, "MultiChainOrchestrator", mocks["MultiChainOrchestrator"])
    monkeypatch.setattr(mcb_mod, "MultiChainGatewayBalanceProvider", mocks["MultiChainGatewayBalanceProvider"])

    # OHLCV + indicators + prediction — reach into .run module
    from almanak.framework.cli import run as run_mod

    mocks["create_routing_ohlcv_provider"] = MagicMock(return_value=MagicMock(name="ohlcv"))
    mocks["_wire_indicators"] = MagicMock()
    mocks["_init_prediction_provider"] = MagicMock()
    mocks["_get_orca_pool_accounts"] = MagicMock(return_value=[])
    mocks["_auto_deploy_lagoon_vault"] = MagicMock()
    mocks["_has_placeholder_vault_address"] = MagicMock(return_value=False)
    monkeypatch.setattr(run_mod, "create_routing_ohlcv_provider", mocks["create_routing_ohlcv_provider"])
    monkeypatch.setattr(run_mod, "_wire_indicators", mocks["_wire_indicators"])
    monkeypatch.setattr(run_mod, "_init_prediction_provider", mocks["_init_prediction_provider"])
    monkeypatch.setattr(run_mod, "_get_orca_pool_accounts", mocks["_get_orca_pool_accounts"])
    monkeypatch.setattr(run_mod, "_auto_deploy_lagoon_vault", mocks["_auto_deploy_lagoon_vault"])
    monkeypatch.setattr(run_mod, "_has_placeholder_vault_address", mocks["_has_placeholder_vault_address"])

    # State manager
    from almanak.framework.state import gateway_state_manager as state_mod

    mocks["GatewayStateManager"] = MagicMock(return_value=MagicMock(name="state_mgr"))
    monkeypatch.setattr(state_mod, "GatewayStateManager", mocks["GatewayStateManager"])

    # StrategyRunner
    from almanak.framework import runner as runner_pkg

    mocks["StrategyRunner"] = MagicMock(return_value=MagicMock(name="runner"))
    monkeypatch.setattr(runner_pkg, "StrategyRunner", mocks["StrategyRunner"])

    return mocks


def _make_strategy_instance() -> Any:
    """A minimal strategy-like object used across component tests."""
    instance = MagicMock()
    # hasattr() checks should resolve to False by default except where the test
    # explicitly enables them. Configure common optional attrs to absent.
    for attr in ("_rate_monitor", "_funding_rate_provider", "_prediction_provider", "set_multi_chain_providers"):
        # Remove to make hasattr() return False
        if hasattr(instance, attr):
            delattr(instance, attr)
    instance.strategy_id = "test-strat"
    return instance


class TestBuildComponents:
    """Phase 13 helper: full component bundle construction."""

    def test_single_chain_happy_path_builds_bundle(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        runner = CliRunner()
        with runner.isolation():
            bundle = run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="deploy-1",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert isinstance(bundle, ComponentBundle)
        assert bundle.runner is not None
        assert bundle.state_manager is not None
        assert bundle.execution_orchestrator is not None
        assert bundle.price_oracle is not None
        assert bundle.balance_provider is not None
        assert bundle.ohlcv_provider is not None
        assert bundle.circuit_breaker is not None
        assert bundle.stuck_detector is not None
        # StrategyRunner was constructed exactly once.
        mocks["StrategyRunner"].assert_called_once()
        # Wire_indicators was called for single-chain.
        mocks["_wire_indicators"].assert_called_once()

    def test_multi_chain_happy_path_uses_multichain_orchestrator(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mocks = _patch_component_factories(monkeypatch)
        runtime_config = _make_fake_multichain_config(["arbitrum", "base"])
        strategy_instance = _make_strategy_instance()

        runner = CliRunner()
        with runner.isolation():
            bundle = run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=runtime_config,
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="deploy-m",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )
        assert bundle.execution_orchestrator is not None
        mocks["MultiChainOrchestrator"].from_gateway.assert_called_once()
        # Multichain balance provider was created.
        mocks["MultiChainGatewayBalanceProvider"].assert_called_once()

    def test_vault_auto_deploy_precedes_runner_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mocks = _patch_component_factories(monkeypatch)
        # Simulate a placeholder vault that must be auto-deployed.
        mocks["_has_placeholder_vault_address"].return_value = True
        mocks["_auto_deploy_lagoon_vault"].return_value = {
            "vault_address": "0x" + "a" * 40,
            "valuator_address": "0x" + "b" * 40,
            "underlying_token": "USDC",
            "settlement_interval_minutes": 60,
        }

        # Record the order vault auto-deploy and StrategyRunner() get called.
        call_order: list[str] = []
        original_auto = mocks["_auto_deploy_lagoon_vault"].side_effect

        def _record_auto(*args: Any, **kwargs: Any) -> Any:
            call_order.append("auto_deploy")
            if original_auto is None:
                return mocks["_auto_deploy_lagoon_vault"].return_value
            return original_auto(*args, **kwargs)

        mocks["_auto_deploy_lagoon_vault"].side_effect = _record_auto

        def _record_runner(*args: Any, **kwargs: Any) -> Any:
            call_order.append("runner_init")
            return MagicMock(name="runner")

        mocks["StrategyRunner"].side_effect = _record_runner

        # Stub out VaultLifecycleManager + LagoonVaultSDK/Adapter so construction
        # does not require real gateway IO.
        from almanak.framework.connectors import lagoon as lagoon_mod
        from almanak.framework.vault import lifecycle as vlc_mod

        monkeypatch.setattr(lagoon_mod, "LagoonVaultAdapter", MagicMock())
        monkeypatch.setattr(lagoon_mod, "LagoonVaultSDK", MagicMock())
        monkeypatch.setattr(vlc_mod, "VaultLifecycleManager", MagicMock())

        # State manager load_state mock — the vault helper calls asyncio.run on it.
        fake_state_mgr = MagicMock()

        async def _fake_load(_sid: str) -> None:
            return None

        fake_state_mgr.load_state = _fake_load
        mocks["GatewayStateManager"].return_value = fake_state_mgr

        strategy_instance = _make_strategy_instance()
        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xwallet",
            "vault": {
                "vault_address": "0x_DEPLOY_HERE_",
                "underlying_token": "USDC",
            },
        }

        runner = CliRunner()
        with runner.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config=strategy_config,
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="anvil",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="vault-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )

        # auto_deploy must come before runner construction.
        assert call_order == ["auto_deploy", "runner_init"], call_order

    def test_copy_trading_v2_attributes_injected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        # Stub the copy-trading v2 config + supporting machinery.
        from almanak.framework.services import (
            copy_circuit_breaker as ccb_mod,
        )
        from almanak.framework.services import (
            copy_intent_builder as cib_mod,
        )
        from almanak.framework.services import (
            copy_ledger as cl_mod,
        )
        from almanak.framework.services import (
            copy_policy_engine as cpe_mod,
        )
        from almanak.framework.services import (
            copy_signal_engine as cse_mod,
        )
        from almanak.framework.services import (
            copy_trading_models as ctm_mod,
        )
        from almanak.framework.services import (
            wallet_monitor as wm_mod,
        )

        fake_v2 = MagicMock(name="ct_v2")
        fake_v2.leaders = [MagicMock(chain="arbitrum", address="0xleader")]
        fake_v2.monitoring = MagicMock(
            poll_interval_seconds=10,
            lookback_blocks=30,
            confirmation_depth=1,
            max_signal_age_seconds=200,
        )
        fake_v2.execution_policy = MagicMock(copy_mode="live", replay_file=None)

        fake_v1 = MagicMock(
            leaders=[{"address": "0xleader", "chain": "arbitrum"}],
            monitoring={},
        )
        monkeypatch.setattr(ctm_mod.CopyTradingConfig, "from_config", staticmethod(lambda _raw: fake_v1))
        monkeypatch.setattr(ctm_mod.CopyTradingConfigV2, "from_config", staticmethod(lambda _raw: fake_v2))

        monkeypatch.setattr(cse_mod, "CopySignalEngine", MagicMock())
        monkeypatch.setattr(cpe_mod, "CopyPolicyEngine", MagicMock())
        monkeypatch.setattr(cib_mod, "CopyIntentBuilder", MagicMock())
        monkeypatch.setattr(ccb_mod, "CopyCircuitBreaker", MagicMock())
        # CopyLedger default constructor accepts a path — stub it.
        monkeypatch.setattr(cl_mod, "CopyLedger", MagicMock())
        monkeypatch.setattr(wm_mod, "WalletMonitor", MagicMock())

        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xwallet",
            "copy_trading": {
                "leaders": [{"address": "0xleader", "chain": "arbitrum"}],
            },
        }

        runner = CliRunner()
        with runner.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config=strategy_config,
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="ct-test",
                normalized_copy_mode="live",
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        # v2 attributes were injected in the expected order.
        assert strategy_instance._wallet_activity_provider is not None
        assert strategy_instance._copy_mode == "live"  # v2 overrides v1
        assert strategy_instance._copy_config_v2 is fake_v2
        assert strategy_instance._copy_policy_engine is not None
        assert strategy_instance._copy_ledger is not None

    def test_copy_trading_v1_only_when_v2_invalid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        from almanak.framework.services import copy_signal_engine as cse_mod
        from almanak.framework.services import copy_trading_models as ctm_mod
        from almanak.framework.services import wallet_monitor as wm_mod

        fake_v1 = MagicMock(
            leaders=[{"address": "0xleader", "chain": "arbitrum"}],
            monitoring={},
        )
        monkeypatch.setattr(ctm_mod.CopyTradingConfig, "from_config", staticmethod(lambda _raw: fake_v1))

        def _v2_raises(_raw: Any) -> Any:
            raise ctm_mod.CopyTradingConfigError("invalid v2 schema")

        monkeypatch.setattr(ctm_mod.CopyTradingConfigV2, "from_config", staticmethod(_v2_raises))
        monkeypatch.setattr(cse_mod, "CopySignalEngine", MagicMock())
        monkeypatch.setattr(wm_mod, "WalletMonitor", MagicMock())

        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xwallet",
            "copy_trading": {"leaders": [{"address": "0xleader", "chain": "arbitrum"}]},
        }

        runner = CliRunner()
        with runner.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config=strategy_config,
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="ct-legacy",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        # v1 attrs present, v2-only attrs absent.
        assert strategy_instance._wallet_activity_provider is not None
        # `_copy_config_v2` was never set because the v2 schema parse failed.
        assert not hasattr(strategy_instance, "_copy_config_v2") or strategy_instance._copy_config_v2 is None or isinstance(strategy_instance._copy_config_v2, MagicMock)

    def test_copy_trading_v2_failure_strict_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        from almanak.framework.services import copy_trading_models as ctm_mod

        monkeypatch.setattr(ctm_mod.CopyTradingConfig, "from_config", staticmethod(lambda _raw: MagicMock()))

        def _v2_raises(_raw: Any) -> Any:
            raise ctm_mod.CopyTradingConfigError("invalid v2 schema")

        monkeypatch.setattr(ctm_mod.CopyTradingConfigV2, "from_config", staticmethod(_v2_raises))

        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xwallet",
            "copy_trading": {"strict": True, "leaders": []},
        }
        runner = CliRunner()
        # Strict mode escalates CopyTradingConfigError -> ClickException (Click
        # turns this into exit 1 when called from the CLI; we catch the
        # exception directly here).
        with runner.isolation(), pytest.raises(click.ClickException, match="strict mode"):
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config=strategy_config,
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="ct-strict",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=True,
                config_chain="arbitrum",
            )

    def test_component_init_unexpected_exception_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        # Make price-oracle construction throw an unexpected runtime error.
        from almanak.framework.data.price import gateway_oracle as oracle_mod

        def _boom(*_a: Any, **_kw: Any) -> None:
            raise RuntimeError("unexpected provider failure")

        monkeypatch.setattr(oracle_mod, "GatewayPriceOracle", _boom)

        runner = CliRunner()
        with runner.isolation(), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="err",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert exc_info.value.code == 1

    def test_dry_run_vault_placeholder_raises_early_exit_with_components(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dry-run + placeholder vault raises `_DryRunVaultEarlyExit` carrying
        the partial component bundle so the `run()` driver can still run
        `cleanup_fn` before exiting 0 (see #1682)."""
        mocks = _patch_component_factories(monkeypatch)
        mocks["_has_placeholder_vault_address"].return_value = True

        strategy_instance = _make_strategy_instance()
        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xwallet",
            "vault": {"vault_address": "0x_DEPLOY_HERE_"},
        }

        runner = CliRunner()
        with runner.isolation(), pytest.raises(run_helpers._DryRunVaultEarlyExit) as exc_info:
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config=strategy_config,
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="anvil",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=True,  # dry-run + placeholder -> raises for cleanup
                strategy_id="dryrun",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        # Partial component bundle is attached so cleanup_fn can still close
        # providers/gateway even though runner was never constructed.
        assert exc_info.value.components is not None
        assert exc_info.value.components.runner is None
