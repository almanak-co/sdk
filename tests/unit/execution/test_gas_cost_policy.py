"""Per-transaction caps apply to final fees and physical Safe wrappers."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.gas.cost import gas_cap_violations
from almanak.framework.execution.interfaces import SigningError, TransactionType
from almanak.framework.execution.orchestrator import ExecutionOrchestrator, TransactionRiskConfig
from tests.unit.execution.signer.test_direct_safe_signer_web3 import (
    make_mock_account,
    make_signer,
    make_tx,
    make_web3,
)


def _violations(tx=None, **overrides):
    options = {
        "max_gas_price_gwei": 0,
        "max_gas_cost_native": 0.001,
        "max_gas_cost_usd": 0,
        "native_token_price_usd": 0,
        "native_token_price_timestamp": datetime.now(UTC),
    }
    options.update(overrides)
    return gas_cap_violations([tx or make_tx()], **options)


@pytest.mark.parametrize("field", ["max_gas_price_gwei", "max_gas_cost_native", "max_gas_cost_usd"])
@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), -float("inf")])
def test_invalid_caps_refuse(field, value):
    assert _violations(**{field: value})


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf"), None])
def test_required_usd_price_refuses_unusable_values(price):
    assert _violations(max_gas_cost_usd=1, native_token_price_usd=price)


@pytest.mark.parametrize("gas", [0, -1, None, float("nan")])
def test_cost_cap_refuses_unmeasured_gas(gas):
    tx = make_tx()
    tx.gas_limit = gas
    assert _violations(tx)


@pytest.mark.parametrize("fee", [-1, None, True, "0", float("nan")])
def test_cost_cap_refuses_unmeasured_fee(fee):
    tx = make_tx()
    tx.max_fee_per_gas = fee
    tx.gas_price = None
    assert _violations(tx)


@pytest.mark.parametrize("chain_id", [1, 56, 137, 42161])
@pytest.mark.parametrize("usd", [False, True])
def test_exact_limit_and_one_wei_excess_across_native_assets(chain_id, usd):
    tx = replace(make_tx(), chain_id=chain_id, gas_limit=1, max_fee_per_gas=10**15)
    limits = {
        "max_gas_cost_native": 0 if usd else 0.001,
        "max_gas_cost_usd": 0.6 if usd else 0,
        "native_token_price_usd": 600,
    }
    assert not _violations(tx, **limits)
    assert _violations(replace(tx, max_fee_per_gas=10**15 + 1), **limits)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["direct", "zodiac"])
@pytest.mark.parametrize("bundle", [False, True])
@pytest.mark.parametrize("usd", [False, True])
@pytest.mark.parametrize("wrapper_gas,allowed", [(200_000, True), (400_000, False)])
async def test_safe_physical_wrapper_checked_before_outer_sign(kind, bundle, usd, wrapper_gas, allowed):
    signer = make_signer()
    web3 = make_web3()
    from web3 import Web3

    web3.keccak = Web3.keccak
    web3.eth.get_transaction_count = AsyncMock(return_value=0)
    if kind == "zodiac":
        from tests.unit.execution.signer.test_zodiac_sign_bundle import make_signer as make_zodiac_signer

        signer = make_zodiac_signer()
        contract = MagicMock()

        async def build(params):
            return {**params, "to": signer.address, "data": "0x1234", "chainId": 42161}

        contract.functions.execTransactionWithRole.return_value.build_transaction = AsyncMock(side_effect=build)
        web3.eth.contract = MagicMock(return_value=contract)
        signer._sign_wrapper_tx = AsyncMock(return_value="0x1234")
        signing = signer._sign_wrapper_tx
    else:
        signer._account = make_mock_account("12" * 20, "34" * 32)
        signing = signer._account.sign_transaction
    signer._estimate_wrapper_gas = AsyncMock(return_value=wrapper_gas)
    orchestrator = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
    orchestrator.signer = signer
    orchestrator._get_web3 = AsyncMock(return_value=web3)
    orchestrator._local_nonce = {}
    orchestrator.tx_risk_config = TransactionRiskConfig(
        max_gas_cost_native=0 if usd else 0.0005,
        max_gas_cost_usd=0.3 if usd else 0,
        native_token_price_usd=600,
        native_token_price_timestamp=datetime.now(UTC),
    )
    tx = replace(make_tx(), gas_limit=100_000)
    transactions = [tx, tx] if bundle else [tx]
    assert orchestrator._validate_gas_prices(transactions).passed
    if allowed:
        signed = await orchestrator._sign_safe_batch(transactions, SimpleNamespace(chain="arbitrum"))
        assert len(signed) == 1
        signing.assert_called_once()
    else:
        with pytest.raises(SigningError, match="gas cap exceeded"):
            await orchestrator._sign_safe_batch(transactions, SimpleNamespace(chain="arbitrum"))
        signing.assert_not_called()


@pytest.mark.parametrize("age", [-10, 61, 3600])
def test_quote_freshness_at_final_validation(age):
    assert _violations(
        max_gas_cost_usd=10,
        native_token_price_usd=600,
        native_token_price_timestamp=datetime.now(UTC) - timedelta(seconds=age),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("quote_lost", [False, True])
async def test_nonce_delay_cannot_outlive_or_lose_required_usd_quote(quote_lost):
    from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionPhase, ExecutionResult
    from tests.unit.execution.test_gas_price_cap import _make_orchestrator

    orchestrator = _make_orchestrator(max_gas_price_gwei=0)
    risk = orchestrator.tx_risk_config
    risk.max_gas_cost_usd = 10
    risk.native_token_price_usd = 600
    risk.native_token_price_timestamp = datetime.now(UTC)
    tx = make_tx()
    assert orchestrator._validate_gas_prices([tx]).passed

    async def delayed_nonce(transactions, context):
        risk.native_token_price_timestamp = None if quote_lost else datetime.now(UTC) - timedelta(seconds=61)
        return transactions

    orchestrator._assign_nonces = AsyncMock(side_effect=delayed_nonce)
    orchestrator._complete_session = MagicMock()
    orchestrator._emit_event = MagicMock()
    state = SimpleNamespace(
        context=ExecutionContext(
            deployment_id="test", intent_id="test", chain="bsc", wallet_address=orchestrator.signer.address
        ),
        result=ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION),
        session=None,
        unsigned_txs=[tx],
    )
    result = await orchestrator._phase_sign(state)
    assert result is not None and not result.success
    expected = "timezone-aware native price timestamp" if quote_lost else "fresh native token price"
    assert expected in result.error
    orchestrator.signer.sign_batch.assert_not_called()
    orchestrator.submitter.submit.assert_not_called()


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("fee,allowed", [(0, True), (500 * 10**9, True), (500 * 10**9 + 1, False)])
def test_price_ceiling_accepts_measured_zero_without_disabling_limit(legacy, fee, allowed):
    tx = replace(
        make_tx(),
        tx_type=TransactionType.LEGACY if legacy else TransactionType.EIP_1559,
        max_fee_per_gas=None if legacy else fee,
        gas_price=fee if legacy else None,
    )
    violations = _violations(tx, max_gas_price_gwei=500, max_gas_cost_native=0)
    assert bool(violations) is not allowed


@pytest.mark.parametrize("fee", [None, -1, True, float("nan")])
def test_price_ceiling_refuses_missing_or_invalid_fee(fee):
    tx = make_tx()
    tx.max_fee_per_gas, tx.gas_price = fee, None
    assert _violations(tx, max_gas_price_gwei=500, max_gas_cost_native=0)


@pytest.mark.parametrize("chain", ["arbitrum", "base", "optimism", "bsc"])
@pytest.mark.parametrize("cost_field", ["max_gas_cost_native", "max_gas_cost_usd"])
def test_zero_fee_fork_satisfies_price_and_cost_caps(chain, cost_field):
    from almanak.framework.execution.gas.fees import build_eip1559_fees

    fees = build_eip1559_fees(base_fee_wei=0, rpc_priority_fee_wei=0, chain=chain)
    assert fees["max_fee_per_gas"] == 0
    tx = replace(make_tx(), max_fee_per_gas=fees["max_fee_per_gas"], gas_price=None)
    orchestrator = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
    orchestrator.tx_risk_config = TransactionRiskConfig(max_gas_price_gwei=500)
    assert orchestrator._validate_gas_prices([tx]).passed
    setattr(orchestrator.tx_risk_config, cost_field, 0.01)
    orchestrator.tx_risk_config.native_token_price_usd = 600
    orchestrator.tx_risk_config.native_token_price_timestamp = datetime.now(UTC)
    assert orchestrator._validate_gas_prices([tx]).passed


@pytest.mark.parametrize("timestamp", [None, datetime(2026, 1, 1), "2026-01-01T00:00:00Z"])
def test_usd_cap_requires_measured_aware_price_timestamp(timestamp):
    violations = _violations(
        max_gas_cost_usd=10,
        native_token_price_usd=600,
        native_token_price_timestamp=timestamp,
    )
    assert violations == ["USD gas cap requires a timezone-aware native price timestamp"]


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("usd", [False, True])
def test_measured_zero_fee_satisfies_cost_cap(legacy, usd):
    tx = replace(
        make_tx(),
        tx_type=TransactionType.LEGACY if legacy else TransactionType.EIP_1559,
        max_fee_per_gas=None if legacy else 0,
        gas_price=0 if legacy else None,
    )
    assert not _violations(tx, max_gas_cost_usd=1 if usd else 0, native_token_price_usd=600)


@pytest.mark.parametrize("gas", [None, 0, -1, True, "21000"])
@pytest.mark.parametrize("key", ["gas", "gasLimit"])
def test_safe_wrapper_malformed_gas_refuses_with_signing_error(key, gas):
    signer = make_signer()
    validator = MagicMock()
    wrapper = {} if gas is None else {key: gas}
    with pytest.raises(SigningError, match="positive integer gas limit"):
        signer._validate_wrapper_gas(make_tx(), wrapper, validator)
    validator.assert_not_called()
