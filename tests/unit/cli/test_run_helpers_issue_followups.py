"""Tests for Phase 4 follow-up fixes: #1682, #1683, #1684, #1686.

Each class targets one issue. The fixes live in
``almanak/framework/cli/run_helpers.py`` (and a handful of lines in
``run.py`` for #1682's cleanup-unwind path).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers
from tests.unit.cli.test_run_helpers_components import (
    _make_fake_local_config,
    _make_strategy_instance,
    _patch_component_factories,
    _patch_runtime_config_imports,
)

# ---------------------------------------------------------------------------
# #1686 — dead elif teardown_after branch removed
# ---------------------------------------------------------------------------


class TestIssue1686TeardownDeadBranchRemoved:
    def test_run_once_no_unreachable_teardown_error_message(self) -> None:
        """The dead ``elif teardown_after`` branch is gone: searching the
        function source for its marker string returns zero hits."""
        import inspect

        source = inspect.getsource(run_helpers._run_once)
        assert "does not support teardown" not in source


# ---------------------------------------------------------------------------
# #1683 — multi-chain + copy_trading fails fast with ClickException
# ---------------------------------------------------------------------------


class TestIssue1683MultiChainCopyTradingFailsFast:
    def test_multi_chain_with_copy_trading_raises_click_exception(self) -> None:
        """Direct call into `_init_copy_trading` with multi_chain=True and a
        populated copy_trading dict must raise ClickException, not silently
        no-op (see #1683)."""
        with pytest.raises(click.ClickException) as exc_info:
            run_helpers._init_copy_trading(
                strategy_instance=MagicMock(),
                strategy_config={
                    "copy_trading": {
                        "leaders": [{"address": "0xleader", "chain": "arbitrum"}]
                    }
                },
                runtime_config=MagicMock(),
                gateway_client=MagicMock(),
                price_oracle=MagicMock(),
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                multi_chain=True,
            )
        assert "multi-chain" in exc_info.value.message.lower()
        assert "copy_trading" in exc_info.value.message

    def test_multi_chain_without_copy_trading_returns_cleanly(self) -> None:
        """Multi-chain strategy WITHOUT copy_trading still no-ops cleanly."""
        # No exception expected.
        run_helpers._init_copy_trading(
            strategy_instance=MagicMock(),
            strategy_config={},  # no copy_trading
            runtime_config=MagicMock(),
            gateway_client=MagicMock(),
            price_oracle=MagicMock(),
            normalized_copy_mode=None,
            copy_replay_file=None,
            copy_shadow=False,
            copy_strict=False,
            multi_chain=True,
        )

    def test_single_chain_empty_copy_trading_returns_cleanly(self) -> None:
        """Single-chain strategy without copy_trading still no-ops cleanly
        (regression guard against reversing the two checks)."""
        run_helpers._init_copy_trading(
            strategy_instance=MagicMock(),
            strategy_config={},
            runtime_config=MagicMock(),
            gateway_client=MagicMock(),
            price_oracle=MagicMock(),
            normalized_copy_mode=None,
            copy_replay_file=None,
            copy_shadow=False,
            copy_strict=False,
            multi_chain=False,
        )

    def test_pre_validation_runs_before_providers_built(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`_build_components` must raise ClickException BEFORE
        `_build_orchestrator_and_providers` creates any gateway-backed
        resources, so a rejected config doesn't leak sockets / orchestrators
        (addresses CR comment on PR #1689)."""
        mocks = _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "copy_trading": {
                        "leaders": [{"address": "0xleader", "chain": "arbitrum"}]
                    },
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                strategy_id="mc-ct",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert "multi-chain" in exc_info.value.message.lower()
        # No orchestrator was built -- validation fired first.
        mocks["GatewayBalanceProvider"].assert_not_called()
        mocks["GatewayPriceOracle"].assert_not_called()


# ---------------------------------------------------------------------------
# #1684 — deployment_id uses runtime-resolved wallet, not stale config value
# ---------------------------------------------------------------------------


class TestIssue1684RuntimeWalletOverridesConfig:
    def test_stale_config_wallet_overwritten_by_runtime(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `wallet_address` left in strategy_config must be replaced with
        the runtime-resolved wallet so deployment_id keys on the signer."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xSTALE",  # left over from an old config
        }

        cli = CliRunner()
        with cli.isolation():
            runtime_config, _chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )

        # The stale wallet must have been overwritten with the runtime-resolved
        # one. Deployment_id reads `strategy_config["wallet_address"]`, so
        # this guarantees state attaches to the actual signer.
        assert strategy_config["wallet_address"] != "0xSTALE"
        assert strategy_config["wallet_address"] == runtime_config.execution_address

    def test_missing_wallet_in_config_still_populated_from_runtime(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression guard: when wallet_address was absent, it still gets
        populated from runtime (unchanged from the original behavior)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        strategy_config: dict[str, Any] = {"chain": "arbitrum"}  # no wallet_address

        cli = CliRunner()
        with cli.isolation():
            runtime_config, _chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )
        assert strategy_config["wallet_address"] == runtime_config.execution_address

    def test_chain_wallets_registry_overrides_both_stale_and_runtime(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the gateway WalletRegistry returns a per-chain wallet via
        ``register_chains()``, that value wins over both the stale config
        wallet and runtime_config.execution_address (addresses CR comment
        on PR #1689)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", '{"arbitrum":"0xREGISTRY"}')

        strategy_config: dict[str, Any] = {
            "chain": "arbitrum",
            "wallet_address": "0xSTALE",  # previous config value
        }

        gateway_client = MagicMock()
        gateway_client.register_chains.return_value = {"arbitrum": "0xREGISTRY"}

        cli = CliRunner()
        with cli.isolation():
            _runtime_config, chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=gateway_client,
                strategy_config=strategy_config,
            )
        gateway_client.register_chains.assert_called_once_with(["arbitrum"])
        assert chain_wallets == {"arbitrum": "0xREGISTRY"}
        # Registry value wins over stale config and over execution_address.
        assert strategy_config["wallet_address"] == "0xREGISTRY"


# ---------------------------------------------------------------------------
# #1682 — vault dry-run on Anvil raises for cleanup instead of sys.exit(0)
# ---------------------------------------------------------------------------


class TestIssue1682VaultDryRunPreservesCleanup:
    def test_maybe_auto_deploy_vault_raises_dry_run_exception_on_anvil(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With --dry-run + placeholder vault on Anvil, the helper raises
        `_DryRunVaultEarlyExit` (so callers can unwind cleanup) instead of
        calling `sys.exit(0)` inline."""
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(run_mod, "_has_placeholder_vault_address", lambda _v: True)

        cli = CliRunner()
        with cli.isolation(), pytest.raises(run_helpers._DryRunVaultEarlyExit):
            run_helpers._maybe_auto_deploy_vault(
                strategy_config={"vault": {"vault_address": "0x_PLACEHOLDER_"}},
                resolved_network="anvil",
                effective_dry_run=True,
                config_chain="arbitrum",
                runtime_config=MagicMock(),
                gateway_client=MagicMock(),
                execution_orchestrator=MagicMock(),
                state_manager=MagicMock(load_state=AsyncMock(return_value=None)),
                strategy_instance=MagicMock(),
                strategy_id="dryrun-vault",
            )

    def test_build_components_attaches_partial_components_on_early_exit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When `_maybe_auto_deploy_vault` raises the early-exit exception,
        `_build_components` re-raises it with the partially-built
        ComponentBundle attached so the caller can run cleanup_fn."""
        mocks = _patch_component_factories(monkeypatch)
        mocks["_has_placeholder_vault_address"].return_value = True

        strategy_instance = _make_strategy_instance()

        cli = CliRunner()
        with cli.isolation(), pytest.raises(run_helpers._DryRunVaultEarlyExit) as exc_info:
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "vault": {"vault_address": "0x_DEPLOY_HERE_"},
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="anvil",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=True,
                strategy_id="dryrun",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )

        # The partial bundle must exist so `cleanup_fn` can close providers
        # (ohlcv, price oracle, gateway, solana fork) even though the runner
        # was never constructed.
        bundle = exc_info.value.components
        assert bundle is not None
        assert bundle.runner is None
        # Providers were built before the vault helper ran, so at least one of
        # them should be populated.
        assert bundle.execution_orchestrator is not None

    def test_dry_run_exception_carries_original_message(self) -> None:
        """Sanity: the exception has an informative str() for logs."""
        exc = run_helpers._DryRunVaultEarlyExit()
        assert "dry-run" in str(exc).lower()
