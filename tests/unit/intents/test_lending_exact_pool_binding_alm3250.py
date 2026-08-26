"""Permanent exact-binding counterexamples for ALM-3250.

These tests distinguish an Aave execution assertion from routing.  The
connector registry remains the only source of the canonical singleton Pool;
``expected_pool`` can only confirm that selection or stop compilation.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending import aave_helpers
from almanak.framework.intents import Intent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler_models import CompilationStatus, TransactionData

WALLET = "0x1234567890123456789012345678901234567890"
CANONICAL_POOL = "0x794a61358d6845594f94dc1db02a252b5b4814ad"
CANONICAL_POOL_CHECKSUM = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
WRONG_POOL = "0x000000000000000000000000000000000000dead"
TOKEN_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
AAVE_ADAPTER = "almanak.framework.intents.compiler_adapters.AaveV3Adapter"


def _compiler() -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "polygon"
    compiler.wallet_address = WALLET
    compiler._gateway_client = None
    compiler._build_approve_tx.return_value = [
        TransactionData(
            to=TOKEN_ADDRESS,
            value=0,
            data="0x095ea7b3",
            gas_estimate=50_000,
            description="approve",
            tx_type="approve",
        )
    ]
    compiler._format_amount.return_value = "100"
    return compiler


def _token() -> MagicMock:
    token = MagicMock()
    token.symbol = "USDC"
    token.address = TOKEN_ADDRESS
    token.decimals = 6
    token.is_native = False
    token.to_dict.return_value = {
        "symbol": "USDC",
        "address": TOKEN_ADDRESS,
        "decimals": 6,
        "is_native": False,
    }
    return token


def _adapter(pool: str = CANONICAL_POOL) -> MagicMock:
    adapter = MagicMock()
    adapter.get_pool_address.return_value = pool
    adapter.get_supply_calldata.return_value = b"\x01\x02"
    adapter.estimate_supply_gas.return_value = 150_000
    adapter.get_set_collateral_calldata.return_value = b"\x03\x04"
    adapter.estimate_set_collateral_gas.return_value = 70_000
    adapter.get_withdraw_calldata.return_value = b"\x05\x06"
    adapter.estimate_withdraw_gas.return_value = 180_000
    return adapter


def test_alm_3250_expected_pool_round_trips_through_public_intent_factories() -> None:
    supply = Intent.supply(
        "aave_v3",
        TOKEN_ADDRESS,
        Decimal("100"),
        expected_pool=CANONICAL_POOL,
    )
    withdraw = Intent.withdraw(
        "aave_v3",
        TOKEN_ADDRESS,
        Decimal("100"),
        expected_pool=CANONICAL_POOL,
    )

    assert SupplyIntent.deserialize(supply.serialize()).expected_pool == CANONICAL_POOL_CHECKSUM
    assert WithdrawIntent.deserialize(withdraw.serialize()).expected_pool == CANONICAL_POOL_CHECKSUM


@pytest.mark.parametrize("intent_type", [SupplyIntent, WithdrawIntent])
def test_alm_3250_expected_pool_rejects_malformed_addresses(intent_type: type) -> None:
    with pytest.raises(ValueError, match="20-byte EVM address"):
        intent_type(
            protocol="aave_v3",
            token=TOKEN_ADDRESS,
            amount=Decimal("100"),
            expected_pool="0x1234",
        )


@pytest.mark.parametrize("intent_type", [SupplyIntent, WithdrawIntent])
def test_alm_3250_expected_pool_is_rejected_by_non_owning_connectors(intent_type: type) -> None:
    with pytest.raises(ValueError, match="does not support exact Pool-address binding"):
        intent_type(
            protocol="morpho_blue",
            token=TOKEN_ADDRESS,
            amount=Decimal("100"),
            market_id="0x" + "11" * 32,
            expected_pool=CANONICAL_POOL,
        )


@patch(AAVE_ADAPTER)
def test_alm_3250_supply_expected_pool_survives_to_compiled_transaction_target(mock_adapter_cls: MagicMock) -> None:
    mock_adapter_cls.return_value = _adapter()
    intent = SupplyIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("100"),
        use_as_collateral=False,
        market_id=WRONG_POOL,
        expected_pool=CANONICAL_POOL_CHECKSUM,
    )

    result = aave_helpers._compile_supply_aave_compatible(_compiler(), intent, _token(), Decimal("100"))

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["expected_pool"] == CANONICAL_POOL_CHECKSUM
    assert result.action_bundle.metadata["pool_address"] == CANONICAL_POOL
    assert [tx.to.lower() for tx in result.transactions if tx.tx_type == "lending_supply"] == [CANONICAL_POOL]


@patch(AAVE_ADAPTER)
def test_alm_3250_supply_pool_mismatch_fails_before_transaction_construction(mock_adapter_cls: MagicMock) -> None:
    adapter = _adapter()
    mock_adapter_cls.return_value = adapter
    compiler = _compiler()
    intent = SupplyIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("100"),
        expected_pool=WRONG_POOL,
    )

    result = aave_helpers._compile_supply_aave_compatible(compiler, intent, _token(), Decimal("100"))

    assert result.status is CompilationStatus.FAILED
    assert "Pool binding mismatch" in (result.error or "")
    assert result.action_bundle is None
    assert result.transactions == []
    compiler._build_approve_tx.assert_not_called()
    adapter.get_supply_calldata.assert_not_called()


@patch(AAVE_ADAPTER)
def test_alm_3250_withdraw_expected_pool_survives_to_compiled_transaction_target(
    mock_adapter_cls: MagicMock,
) -> None:
    mock_adapter_cls.return_value = _adapter()
    intent = WithdrawIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("100"),
        market_id=WRONG_POOL,
        expected_pool=CANONICAL_POOL,
    )

    result = aave_helpers._compile_withdraw_aave_compatible(_compiler(), intent, _token(), Decimal("100"), [])

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["expected_pool"] == CANONICAL_POOL_CHECKSUM
    assert result.action_bundle.metadata["pool_address"] == CANONICAL_POOL
    assert [tx.to.lower() for tx in result.transactions if tx.tx_type == "lending_withdraw"] == [CANONICAL_POOL]


@patch(AAVE_ADAPTER)
def test_alm_3250_withdraw_pool_mismatch_fails_before_transaction_construction(
    mock_adapter_cls: MagicMock,
) -> None:
    adapter = _adapter()
    mock_adapter_cls.return_value = adapter
    intent = WithdrawIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("100"),
        expected_pool=WRONG_POOL,
    )

    result = aave_helpers._compile_withdraw_aave_compatible(_compiler(), intent, _token(), Decimal("100"), [])

    assert result.status is CompilationStatus.FAILED
    assert "Pool binding mismatch" in (result.error or "")
    assert result.action_bundle is None
    assert result.transactions == []
    adapter.get_withdraw_calldata.assert_not_called()
