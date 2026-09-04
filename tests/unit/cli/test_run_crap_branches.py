"""Branch contracts for runtime gas-risk overrides and Lagoon vault auto-deploy."""

from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from almanak.framework.cli import run as run_mod

_WALLET = "0x1111111111111111111111111111111111111111"
_UNDERLYING = "0x2222222222222222222222222222222222222222"
_VAULT = "0x3333333333333333333333333333333333333333"


@pytest.fixture
def vault_autodeploy(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    from almanak.framework.data import tokens as tokens_mod
    from almanak.framework.vault import capability as capability_mod

    resolver = MagicMock()
    resolver.get_address.return_value = _UNDERLYING
    monkeypatch.setattr(tokens_mod, "get_token_resolver", MagicMock(return_value=resolver))

    deployer = MagicMock()
    deploy_bundle = MagicMock(name="deploy_bundle")
    approve_bundle = MagicMock(name="approve_bundle")
    deployer.build_deploy_vault_bundle.return_value = deploy_bundle
    deployer.build_post_deploy_bundle.return_value = approve_bundle

    params_type = MagicMock(side_effect=lambda **kwargs: SimpleNamespace(**kwargs))
    receipt = MagicMock(name="deploy_receipt")
    parsed = SimpleNamespace(success=True, vault_address=_VAULT, error=None)
    capability = MagicMock()
    capability.build_deployer.return_value = deployer
    capability.deploy_params_type.return_value = params_type
    capability.parse_deploy_receipt.return_value = parsed
    get_capability = MagicMock(return_value=capability)
    monkeypatch.setattr(capability_mod, "get_vault_tool_capability", get_capability)

    deploy_result = SimpleNamespace(
        success=True,
        error=None,
        transaction_results=[SimpleNamespace(receipt=receipt)],
    )
    approve_result = SimpleNamespace(success=True)
    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(side_effect=[deploy_result, approve_result])

    return SimpleNamespace(
        resolver=resolver,
        get_capability=get_capability,
        capability=capability,
        deployer=deployer,
        params_type=params_type,
        receipt=receipt,
        parsed=parsed,
        deploy_bundle=deploy_bundle,
        approve_bundle=approve_bundle,
        deploy_result=deploy_result,
        approve_result=approve_result,
        orchestrator=orchestrator,
        gateway_client=MagicMock(name="gateway_client"),
        runtime_config=SimpleNamespace(wallet_address=_WALLET),
    )


def _auto_deploy(context: SimpleNamespace, vault_raw: dict | None = None) -> dict:
    return run_mod._auto_deploy_lagoon_vault(
        vault_raw if vault_raw is not None else {},
        "base",
        context.runtime_config,
        context.gateway_client,
        context.orchestrator,
    )


class TestAutoDeployLagoonVault:
    def test_deploys_approves_and_returns_a_patched_copy(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        original = {"settlement_interval_minutes": 30}

        result = _auto_deploy(vault_autodeploy, original)

        assert original == {"settlement_interval_minutes": 30}
        assert result == {
            "settlement_interval_minutes": 30,
            "vault_address": _VAULT,
            "valuator_address": _WALLET,
        }
        vault_autodeploy.resolver.get_address.assert_called_once_with("base", "USDC")
        vault_autodeploy.get_capability.assert_called_once_with()
        vault_autodeploy.capability.build_deployer.assert_called_once_with(vault_autodeploy.gateway_client)
        vault_autodeploy.capability.parse_deploy_receipt.assert_called_once_with(vault_autodeploy.receipt)
        params = vault_autodeploy.params_type.call_args.kwargs
        assert params == {
            "chain": "base",
            "underlying_token_address": _UNDERLYING,
            "name": "Almanak Anvil Vault",
            "symbol": "aVLT",
            "safe_address": _WALLET,
            "admin_address": _WALLET,
            "fee_receiver_address": _WALLET,
            "deployer_address": _WALLET,
        }
        assert vault_autodeploy.orchestrator.execute.await_args_list == [
            call(vault_autodeploy.deploy_bundle),
            call(vault_autodeploy.approve_bundle),
        ]
        assert capsys.readouterr().out == (
            "  Building vault deploy transaction...\n"
            "  Executing vault deploy transaction...\n"
            f"  Vault deployed at: {_VAULT}\n"
            "  Approving underlying token for vault...\n"
            "  Vault config patched with deployed addresses\n"
        )

    def test_uses_configured_underlying_symbol(self, vault_autodeploy: SimpleNamespace) -> None:
        _auto_deploy(vault_autodeploy, {"underlying_token": "USDT"})

        vault_autodeploy.resolver.get_address.assert_called_once_with("base", "USDT")

    def test_token_resolution_failure_exits_with_existing_error(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.resolver.get_address.side_effect = RuntimeError("token unavailable")

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy, {"underlying_token": "USDT"})

        assert exc_info.value.code == 1
        assert capsys.readouterr().out == "  ERROR: Cannot resolve token 'USDT' on base: token unavailable\n"
        vault_autodeploy.get_capability.assert_not_called()

    def test_capability_resolution_failure_exits_with_existing_error(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.get_capability.side_effect = RuntimeError("capability unavailable")

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy)

        assert exc_info.value.code == 1
        assert capsys.readouterr().out == (
            "  ERROR: Failed to resolve vault capability/deployer: capability unavailable\n"
        )

    def test_deploy_bundle_build_failure_exits_with_existing_error(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.deployer.build_deploy_vault_bundle.side_effect = RuntimeError("invalid deploy params")

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy)

        assert exc_info.value.code == 1
        assert capsys.readouterr().out == (
            "  Building vault deploy transaction...\n"
            "  ERROR: Failed to build deploy transaction: invalid deploy params\n"
        )
        vault_autodeploy.orchestrator.execute.assert_not_awaited()

    def test_deploy_execution_exception_exits_with_existing_error(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.orchestrator.execute.side_effect = RuntimeError("gateway unavailable")

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy)

        assert exc_info.value.code == 1
        assert capsys.readouterr().out == (
            "  Building vault deploy transaction...\n"
            "  Executing vault deploy transaction...\n"
            "  ERROR: Vault deploy transaction failed: gateway unavailable\n"
        )

    def test_unsuccessful_deploy_result_exits_with_reported_error(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.deploy_result.success = False
        vault_autodeploy.deploy_result.error = "execution reverted"

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy)

        assert exc_info.value.code == 1
        assert capsys.readouterr().out.endswith("  ERROR: Vault deploy transaction reverted: execution reverted\n")
        vault_autodeploy.capability.parse_deploy_receipt.assert_not_called()

    def test_missing_receipt_exits_before_parsing(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.deploy_result.transaction_results = [SimpleNamespace(receipt=None)]

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy)

        assert exc_info.value.code == 1
        assert capsys.readouterr().out.endswith("  ERROR: No receipt found for vault deploy transaction\n")
        vault_autodeploy.capability.parse_deploy_receipt.assert_not_called()

    def test_uses_first_available_receipt(self, vault_autodeploy: SimpleNamespace) -> None:
        vault_autodeploy.deploy_result.transaction_results.insert(0, SimpleNamespace(receipt=None))

        _auto_deploy(vault_autodeploy)

        vault_autodeploy.capability.parse_deploy_receipt.assert_called_once_with(vault_autodeploy.receipt)

    @pytest.mark.parametrize(
        "parsed",
        [
            SimpleNamespace(success=False, vault_address=None, error="event missing"),
            SimpleNamespace(success=True, vault_address=None, error="address missing"),
        ],
    )
    def test_unusable_parsed_receipt_exits_with_parser_error(
        self,
        vault_autodeploy: SimpleNamespace,
        parsed: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.capability.parse_deploy_receipt.return_value = parsed

        with pytest.raises(SystemExit) as exc_info:
            _auto_deploy(vault_autodeploy)

        assert exc_info.value.code == 1
        assert capsys.readouterr().out.endswith(f"  ERROR: Could not extract vault address: {parsed.error}\n")
        assert vault_autodeploy.orchestrator.execute.await_count == 1

    def test_unsuccessful_approval_warns_and_keeps_deployed_vault(
        self,
        vault_autodeploy: SimpleNamespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        vault_autodeploy.approve_result.success = False

        result = _auto_deploy(vault_autodeploy)

        assert result["vault_address"] == _VAULT
        assert "  WARNING: Underlying approval failed (vault may still work)\n" in capsys.readouterr().out

    @pytest.mark.parametrize("failure_stage", ["build", "execute"])
    def test_approval_exception_warns_and_keeps_deployed_vault(
        self,
        vault_autodeploy: SimpleNamespace,
        failure_stage: str,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        if failure_stage == "build":
            vault_autodeploy.deployer.build_post_deploy_bundle.side_effect = RuntimeError("approval build failed")
            expected = "approval build failed"
        else:
            vault_autodeploy.orchestrator.execute.side_effect = [
                vault_autodeploy.deploy_result,
                RuntimeError("approval submit failed"),
            ]
            expected = "approval submit failed"

        result = _auto_deploy(vault_autodeploy)

        assert result["vault_address"] == _VAULT
        assert f"  WARNING: Underlying approval failed: {expected}\n" in capsys.readouterr().out


_GAS_RISK_ENV_VARS = (
    "ALMANAK_MAX_GAS_PRICE_GWEI",
    "MAX_GAS_PRICE_GWEI",
    "ALMANAK_MAX_GAS_COST_NATIVE",
    "MAX_GAS_COST_NATIVE",
    "ALMANAK_MAX_GAS_COST_USD",
    "MAX_GAS_COST_USD",
    "ALMANAK_MAX_SLIPPAGE_BPS",
    "MAX_SLIPPAGE_BPS",
    "ALMANAK_MAX_GAS_PRICE_GWEI_BASE",
    "MAX_GAS_PRICE_GWEI_BASE",
    "ALMANAK_MAX_VALUE_USD",
    "MAX_VALUE_USD",
)


@pytest.fixture
def gas_risk_context(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    from almanak.framework.execution.gas import constants as gas_constants

    for name in _GAS_RISK_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    warn = MagicMock()
    monkeypatch.setattr(gas_constants, "warn_if_effective_cap_below_typical_gas", warn)
    return SimpleNamespace(
        config=SimpleNamespace(
            chain="base",
            max_gas_price_gwei=999,
            max_gas_cost_native=Decimal("0.25"),
            max_gas_cost_usd=Decimal("12.50"),
            max_slippage_bps=75,
        ),
        risk=SimpleNamespace(
            max_gas_price_gwei=10,
            max_gas_cost_native=Decimal("1"),
            max_gas_cost_usd=Decimal("2"),
            max_slippage_bps=3,
            max_value_usd=Decimal("4"),
        ),
        warn=warn,
    )


class TestApplyRuntimeGasRiskOverrides:
    @pytest.mark.parametrize(
        ("env_var", "field", "expected"),
        [
            ("ALMANAK_MAX_GAS_COST_NATIVE", "max_gas_cost_native", Decimal("0.25")),
            ("MAX_GAS_COST_NATIVE", "max_gas_cost_native", Decimal("0.25")),
            ("ALMANAK_MAX_GAS_COST_USD", "max_gas_cost_usd", Decimal("12.50")),
            ("MAX_GAS_COST_USD", "max_gas_cost_usd", Decimal("12.50")),
            ("ALMANAK_MAX_SLIPPAGE_BPS", "max_slippage_bps", 75),
            ("MAX_SLIPPAGE_BPS", "max_slippage_bps", 75),
        ],
    )
    def test_explicit_prefixed_and_legacy_values_override_chain_defaults(
        self,
        gas_risk_context: SimpleNamespace,
        monkeypatch: pytest.MonkeyPatch,
        env_var: str,
        field: str,
        expected: Decimal | int,
    ) -> None:
        monkeypatch.setenv(env_var, "configured")

        run_mod._apply_runtime_gas_risk_overrides(gas_risk_context.risk, gas_risk_context.config)

        assert getattr(gas_risk_context.risk, field) == expected

    def test_unset_overrides_preserve_chain_defaults_and_set_value_default(
        self,
        gas_risk_context: SimpleNamespace,
    ) -> None:
        run_mod._apply_runtime_gas_risk_overrides(gas_risk_context.risk, gas_risk_context.config)

        assert gas_risk_context.risk.max_gas_price_gwei == 10
        assert gas_risk_context.risk.max_gas_cost_native == Decimal("1")
        assert gas_risk_context.risk.max_gas_cost_usd == Decimal("2")
        assert gas_risk_context.risk.max_slippage_bps == 3
        assert gas_risk_context.risk.max_value_usd == Decimal("50000")

    def test_deprecated_global_gwei_value_is_not_applied(
        self,
        gas_risk_context: SimpleNamespace,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "800")

        run_mod._apply_runtime_gas_risk_overrides(gas_risk_context.risk, gas_risk_context.config)

        assert gas_risk_context.risk.max_gas_price_gwei == 10

    @pytest.mark.parametrize("env_var", ["ALMANAK_MAX_GAS_PRICE_GWEI_BASE", "MAX_GAS_PRICE_GWEI_BASE"])
    def test_chain_scoped_gwei_value_overrides_chain_default(
        self,
        gas_risk_context: SimpleNamespace,
        monkeypatch: pytest.MonkeyPatch,
        env_var: str,
    ) -> None:
        monkeypatch.setenv(env_var, "42")

        run_mod._apply_runtime_gas_risk_overrides(gas_risk_context.risk, gas_risk_context.config)

        assert gas_risk_context.risk.max_gas_price_gwei == 42
        gas_risk_context.warn.assert_called_once_with(
            chain="base",
            effective_cap_gwei=42,
            logger=run_mod.logger,
        )

    @pytest.mark.parametrize(
        ("prefixed", "legacy", "expected"),
        [
            ("123.45", None, Decimal("123.45")),
            (None, "234.56", Decimal("234.56")),
            ("123.45", "234.56", Decimal("123.45")),
            ("0", None, Decimal("0")),
        ],
    )
    def test_max_value_usd_preserves_prefixed_legacy_and_zero_semantics(
        self,
        gas_risk_context: SimpleNamespace,
        monkeypatch: pytest.MonkeyPatch,
        prefixed: str | None,
        legacy: str | None,
        expected: Decimal,
    ) -> None:
        if prefixed is not None:
            monkeypatch.setenv("ALMANAK_MAX_VALUE_USD", prefixed)
        if legacy is not None:
            monkeypatch.setenv("MAX_VALUE_USD", legacy)

        run_mod._apply_runtime_gas_risk_overrides(gas_risk_context.risk, gas_risk_context.config)

        assert gas_risk_context.risk.max_value_usd == expected

    def test_malformed_max_value_usd_raises_existing_error_with_decimal_cause(
        self,
        gas_risk_context: SimpleNamespace,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ALMANAK_MAX_VALUE_USD", "1,000 USD")

        with pytest.raises(ValueError, match="must be a plain decimal number") as exc_info:
            run_mod._apply_runtime_gas_risk_overrides(gas_risk_context.risk, gas_risk_context.config)

        assert "Got: '1,000 USD'" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, InvalidOperation)
