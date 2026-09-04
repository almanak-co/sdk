import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.agent_tools.bundle_cache import BundleCache
from almanak.framework.agent_tools.errors import AgentErrorCode
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus

_WALLET = "0x1111111111111111111111111111111111111111"
_DEPOSITOR = "0x2222222222222222222222222222222222222222"
_VAULT = "0x3333333333333333333333333333333333333333"
_UNDERLYING = "0x4444444444444444444444444444444444444444"


def _balance(*, balance: str = "0", balance_usd: str = "", error: str = "") -> SimpleNamespace:
    return SimpleNamespace(balance=balance, balance_usd=balance_usd, error=error)


def _execution_result(
    *,
    success: bool,
    tx_hashes: list[str] | None = None,
    error: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(success=success, tx_hashes=[] if tx_hashes is None else tx_hashes, error=error)


@pytest.fixture
def gateway() -> MagicMock:
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def executor(gateway: MagicMock, tmp_path) -> ToolExecutor:
    return ToolExecutor(
        gateway,
        policy=AgentPolicy(
            allowed_chains={"base", "bsc"},
            max_tool_calls_per_minute=100,
            max_single_trade_usd=Decimal("1e30"),
            max_daily_spend_usd=Decimal("1e30"),
            max_position_size_usd=Decimal("1e30"),
            require_human_approval_above_usd=Decimal("1e30"),
            cooldown_seconds=0,
            require_rebalance_check=False,
        ),
        wallet_address=_WALLET,
        deployment_id="deployment:test",
        default_chain="base",
        bundle_cache=BundleCache(cache_dir=tmp_path),
    )


def _vault_sdk(executor: ToolExecutor) -> tuple[MagicMock, MagicMock]:
    sdk = MagicMock()
    sdk.build_approve_deposit_tx.return_value = {
        "to": _UNDERLYING,
        "from": _DEPOSITOR,
        "data": "0xapprove",
        "value": "0",
    }
    sdk.build_request_deposit_tx.return_value = {
        "to": _VAULT,
        "from": _DEPOSITOR,
        "data": "0xdeposit",
        "value": "0",
    }
    capability = MagicMock()
    capability.build_sdk.return_value = sdk
    return sdk, capability


class TestGetWalletOverviewBranches:
    def test_zero_threshold_does_not_fabricate_unmeasured_usd_balances(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        gateway.market.BatchGetBalances.return_value.responses = [
            _balance(balance="1", balance_usd=""),
            _balance(balance="1", balance_usd="not-a-decimal"),
            _balance(balance="1", balance_usd="Infinity"),
            _balance(balance="0", balance_usd="0"),
        ]

        with patch.object(
            executor,
            "_default_tokens_for_chain",
            return_value=["UNPRICED", "MALFORMED", "NONFINITE", "MEASURED_ZERO"],
        ):
            result = executor._execute_get_wallet_overview(
                {"chain": "base", "wallet_address": _WALLET, "min_balance_usd": 0}
            )

        assert result.status is ToolResponseStatus.SUCCESS
        assert result.data["tokens"] == [
            {"symbol": "MEASURED_ZERO", "balance": "0", "balance_usd": "0"},
        ]
        assert result.data["total_usd"] == "0"

    def test_filters_unmeasured_errors_dust_and_preserves_decimal_totals(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        default_tokens = ["USDC", "WETH", "DAI", "WBTC", "USDT", "ETH"]
        gateway.market.BatchGetBalances.return_value.responses = [
            _balance(balance="10.25", balance_usd="10.250"),
            _balance(error="rpc failed"),
            _balance(balance="1", balance_usd=""),
            _balance(balance="1", balance_usd="not-a-decimal"),
            _balance(balance="1", balance_usd="NaN"),
            _balance(balance="0.5", balance_usd="0.50"),
            _balance(balance="20.75", balance_usd="20.750"),
        ]

        with patch.object(executor, "_default_tokens_for_chain", return_value=default_tokens):
            result = executor._execute_get_wallet_overview(
                {
                    "chain": "base",
                    "wallet_address": _DEPOSITOR,
                    "min_balance_usd": 1.0,
                    "extra_tokens": ["USDC", "DEGEN"],
                }
            )

        assert result.status is ToolResponseStatus.SUCCESS
        assert result.error is None
        assert result.data == {
            "wallet_address": _DEPOSITOR,
            "chain": "base",
            "tokens": [
                {"symbol": "USDC", "balance": "10.25", "balance_usd": "10.250"},
                {"symbol": "DEGEN", "balance": "20.75", "balance_usd": "20.750"},
            ],
            "total_usd": "31.000",
        }
        request = gateway.market.BatchGetBalances.call_args.args[0]
        assert [item.token for item in request.requests] == [*default_tokens, "DEGEN"]
        assert {item.chain for item in request.requests} == {"base"}
        assert {item.wallet_address for item in request.requests} == {_DEPOSITOR}

    @pytest.mark.asyncio
    async def test_response_count_mismatch_is_typed_and_uses_canonical_identity(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        gateway.market.BatchGetBalances.return_value.responses = [_balance(balance_usd="1")]

        with patch.object(executor, "_default_tokens_for_chain", return_value=["WBNB", "USDT"]):
            result = await executor.execute(
                "get_wallet_overview",
                {"chain": "bnb", "apiKey": "must-not-leak"},
            )

        assert result.status is ToolResponseStatus.ERROR
        assert result.data == {"error": "BatchGetBalances returned 1 responses for 2 requests"}
        assert result.error is not None
        assert result.error.error_code is AgentErrorCode.GATEWAY_ERROR
        assert result.error.recoverable is True
        request = gateway.market.BatchGetBalances.call_args.args[0]
        assert {item.chain for item in request.requests} == {"bsc"}
        assert {item.wallet_address for item in request.requests} == {_WALLET}
        trace = executor.tracer.get_entries()[-1]
        assert trace.args["apiKey"] == "***REDACTED***"
        assert "must-not-leak" not in result.model_dump_json()

    @pytest.mark.asyncio
    async def test_policy_denial_precedes_gateway_dispatch_and_redacts_trace(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        result = await executor.execute(
            "get_wallet_overview",
            {"chain": "arbitrum", "privateKey": "must-not-leak"},
        )

        assert result.status is ToolResponseStatus.ERROR
        assert result.error is not None
        assert result.error.error_code is AgentErrorCode.RISK_BLOCKED
        gateway.market.BatchGetBalances.assert_not_called()
        trace = executor.tracer.get_entries()[-1]
        assert trace.policy_result is not None
        assert trace.policy_result["allowed"] is False
        assert trace.args["privateKey"] == "***REDACTED***"
        assert "must-not-leak" not in result.model_dump_json()


class TestDepositVaultBranches:
    @pytest.mark.asyncio
    async def test_live_success_preserves_bundle_chain_wallet_and_spend_accounting(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        sdk, capability = _vault_sdk(executor)
        gateway.execution.Execute.side_effect = [
            _execution_result(success=True, tx_hashes=["0xapprove"]),
            _execution_result(success=True, tx_hashes=["0xdeposit"]),
        ]

        with (
            patch.object(executor, "_vault_capability", return_value=capability),
            patch.object(executor, "_estimate_usd_spend", AsyncMock(return_value=Decimal("12.34"))) as estimate,
            patch.object(executor._policy_engine, "record_trade") as record_trade,
        ):
            result = await executor.execute(
                "deposit_vault",
                {
                    "chain": "bnb",
                    "vault_address": _VAULT,
                    "underlying_token": _UNDERLYING,
                    "amount": "123",
                    "depositor_address": _DEPOSITOR,
                },
            )

        assert result.status is ToolResponseStatus.SUCCESS
        assert result.error is None
        assert result.data == {
            "tx_hash": "0xdeposit",
            "approve_tx_hash": "0xapprove",
            "amount_deposited": "123",
            "message": f"Deposited 123 into vault {_VAULT[:10]}...",
            "status": ToolResponseStatus.SUCCESS,
        }
        capability.build_sdk.assert_called_once_with(gateway, "bsc")
        sdk.build_approve_deposit_tx.assert_called_once_with(_UNDERLYING, _VAULT, _DEPOSITOR, 123)
        sdk.build_request_deposit_tx.assert_called_once_with(_VAULT, _DEPOSITOR, 123)
        approve_request, deposit_request = [call.args[0] for call in gateway.execution.Execute.call_args_list]
        assert (approve_request.chain, approve_request.wallet_address, approve_request.deployment_id) == (
            "bsc",
            _DEPOSITOR,
            "deployment:test",
        )
        assert (deposit_request.chain, deposit_request.wallet_address, deposit_request.deployment_id) == (
            "bsc",
            _DEPOSITOR,
            "deployment:test",
        )
        assert approve_request.simulation_enabled is True
        assert deposit_request.simulation_enabled is False
        assert json.loads(approve_request.action_bundle)["metadata"]["amount"] == "123"
        assert json.loads(deposit_request.action_bundle)["metadata"]["depositor"] == _DEPOSITOR
        estimate.assert_awaited_once()
        record_trade.assert_called_once_with(Decimal("12.34"), success=True, tool_name="deposit_vault")

    @pytest.mark.asyncio
    async def test_dry_run_success_uses_default_identity_and_keeps_missing_hashes_none(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        sdk, capability = _vault_sdk(executor)
        gateway.execution.Execute.side_effect = [
            _execution_result(success=True),
            _execution_result(success=True),
        ]

        with (
            patch.object(executor, "_vault_capability", return_value=capability),
            patch.object(executor, "_estimate_usd_spend", AsyncMock()) as estimate,
            patch.object(executor._policy_engine, "record_trade") as record_trade,
        ):
            result = await executor.execute(
                "deposit_vault",
                {
                    "vault_address": _VAULT,
                    "underlying_token": _UNDERLYING,
                    "amount": "42",
                    "dry_run": True,
                },
            )

        assert result.status is ToolResponseStatus.SIMULATED
        assert result.error is None
        assert result.data["status"] is ToolResponseStatus.SIMULATED
        assert result.data["tx_hash"] is None
        assert result.data["approve_tx_hash"] is None
        assert result.data["amount_deposited"] == "42"
        capability.build_sdk.assert_called_once_with(gateway, "base")
        sdk.build_approve_deposit_tx.assert_called_once_with(_UNDERLYING, _VAULT, _WALLET, 42)
        sdk.build_request_deposit_tx.assert_called_once_with(_VAULT, _WALLET, 42)
        requests = [call.args[0] for call in gateway.execution.Execute.call_args_list]
        assert all(request.dry_run for request in requests)
        assert all(request.wallet_address == _WALLET for request in requests)
        estimate.assert_not_awaited()
        record_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_live_approval_failure_stops_before_deposit(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        sdk, capability = _vault_sdk(executor)
        gateway.execution.Execute.return_value = _execution_result(success=False, error="approval reverted")

        with (
            patch.object(executor, "_vault_capability", return_value=capability),
            patch.object(executor, "_estimate_usd_spend", AsyncMock()) as estimate,
            patch.object(executor._policy_engine, "record_trade") as record_trade,
        ):
            result = await executor.execute(
                "deposit_vault",
                {
                    "vault_address": _VAULT,
                    "underlying_token": _UNDERLYING,
                    "amount": "1",
                },
            )

        assert result.status is ToolResponseStatus.ERROR
        assert result.error is not None
        assert result.error.error_code is AgentErrorCode.EXECUTION_FAILED
        assert result.error.message == "Vault deposit approve failed: approval reverted"
        gateway.execution.Execute.assert_called_once()
        sdk.build_request_deposit_tx.assert_not_called()
        estimate.assert_not_awaited()
        record_trade.assert_not_called()

    @pytest.mark.asyncio
    async def test_dry_run_approval_failure_returns_simulation_error_and_stops(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        sdk, capability = _vault_sdk(executor)
        gateway.execution.Execute.return_value = _execution_result(
            success=False,
            tx_hashes=["0xfailed-approval"],
            error="approval simulation reverted",
        )

        with (
            patch.object(executor, "_vault_capability", return_value=capability),
            patch.object(executor._policy_engine, "record_trade") as record_trade,
        ):
            result = await executor.execute(
                "deposit_vault",
                {
                    "vault_address": _VAULT,
                    "underlying_token": _UNDERLYING,
                    "amount": "1",
                    "dry_run": True,
                },
            )

        assert result.status is ToolResponseStatus.ERROR
        assert result.error is not None
        assert result.error.error_code is AgentErrorCode.SIMULATION_FAILED
        assert result.error.recoverable is True
        assert result.error.message == "approval simulation reverted"
        assert result.data["tx_hash"] is None
        assert result.data["approve_tx_hash"] == "0xfailed-approval"
        assert result.data["amount_deposited"] == "0"
        gateway.execution.Execute.assert_called_once()
        sdk.build_request_deposit_tx.assert_not_called()
        record_trade.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("dry_run", [False, True])
    async def test_deposit_failure_has_typed_error_and_never_claims_amount_deposited(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
        dry_run: bool,
    ) -> None:
        _, capability = _vault_sdk(executor)
        gateway.execution.Execute.side_effect = [
            _execution_result(success=True, tx_hashes=["0xapprove"]),
            _execution_result(success=False, error="deposit reverted"),
        ]

        with (
            patch.object(executor, "_vault_capability", return_value=capability),
            patch.object(executor, "_estimate_usd_spend", AsyncMock(return_value=Decimal("5"))) as estimate,
            patch.object(executor._policy_engine, "record_trade") as record_trade,
        ):
            result = await executor.execute(
                "deposit_vault",
                {
                    "vault_address": _VAULT,
                    "underlying_token": _UNDERLYING,
                    "amount": "5",
                    "dry_run": dry_run,
                    "privateKey": "must-not-leak",
                },
            )

        assert result.status is ToolResponseStatus.ERROR
        assert result.error is not None
        expected_code = AgentErrorCode.SIMULATION_FAILED if dry_run else AgentErrorCode.EXECUTION_FAILED
        assert result.error.error_code is expected_code
        if dry_run:
            assert result.data["amount_deposited"] == "0"
            assert result.data["tx_hash"] is None
            estimate.assert_not_awaited()
            record_trade.assert_not_called()
        else:
            assert result.data is None
            estimate.assert_awaited_once()
            record_trade.assert_called_once_with(Decimal("5"), success=False, tool_name="deposit_vault")
        trace = executor.tracer.get_entries()[-1]
        assert trace.args["privateKey"] == "***REDACTED***"
        assert "must-not-leak" not in result.model_dump_json()

    @pytest.mark.asyncio
    async def test_disallowed_depositor_is_blocked_before_vault_sdk_and_gateway_execution(
        self,
        executor: ToolExecutor,
        gateway: MagicMock,
    ) -> None:
        capability = MagicMock()
        executor._policy_engine.policy.allowed_execution_wallets = {_WALLET}
        tool = executor._catalog.get("deposit_vault")
        assert tool is not None
        decision = executor._policy_engine.check(
            tool,
            {
                "chain": "base",
                "underlying_token": _UNDERLYING,
                "amount": "1",
                "execution_wallet": _WALLET,
                "depositor_address": _DEPOSITOR,
            },
        )
        assert decision.allowed is False
        assert any(_DEPOSITOR in violation for violation in decision.violations)

        with patch.object(executor, "_vault_capability", return_value=capability) as vault_capability:
            result = await executor.execute(
                "deposit_vault",
                {
                    "vault_address": _VAULT,
                    "underlying_token": _UNDERLYING,
                    "amount": "1",
                    "depositor_address": _DEPOSITOR,
                },
            )

        assert result.status is ToolResponseStatus.ERROR
        assert result.error is not None
        assert result.error.error_code is AgentErrorCode.RISK_BLOCKED
        assert _DEPOSITOR in result.error.message
        vault_capability.assert_not_called()
        gateway.execution.Execute.assert_not_called()
