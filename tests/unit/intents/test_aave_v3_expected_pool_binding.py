"""ALM-3250: caller-asserted Aave V3 Pool identity."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from almanak.connectors._strategy_base.base.lending import aave_helpers
from almanak.framework.intents import BorrowIntent, Intent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler_models import CompilationStatus

CANONICAL_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
OTHER_POOL = "0x1111111111111111111111111111111111111111"
WALLET = "0x2222222222222222222222222222222222222222"
TOKEN_ADDRESS = "0x3333333333333333333333333333333333333333"
ADAPTER_PATH = "almanak.framework.intents.compiler_adapters.AaveV3Adapter"


def _compiler() -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "polygon"
    compiler.wallet_address = WALLET
    compiler._gateway_client = None
    compiler._format_amount.side_effect = lambda amount, decimals: str(amount)
    compiler._build_approve_tx.return_value = []
    return compiler


def _token() -> MagicMock:
    token = MagicMock()
    token.symbol = "USDT"
    token.address = TOKEN_ADDRESS
    token.decimals = 6
    token.is_native = False
    token.to_dict.return_value = {
        "symbol": "USDT",
        "address": TOKEN_ADDRESS,
        "decimals": 6,
        "is_native": False,
    }
    return token


def _adapter(mock_adapter_cls: MagicMock) -> MagicMock:
    adapter = MagicMock()
    adapter.get_pool_address.return_value = CANONICAL_POOL
    adapter.get_supply_calldata.return_value = b"\x01"
    adapter.get_withdraw_calldata.return_value = b"\x02"
    adapter.estimate_supply_gas.return_value = 150_000
    adapter.estimate_withdraw_gas.return_value = 150_000
    mock_adapter_cls.return_value = adapter
    return adapter


def test_expected_pool_is_checksummed_and_round_trips() -> None:
    intent = Intent.supply(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("1"),
        use_as_collateral=False,
        expected_pool=CANONICAL_POOL.lower(),
    )

    assert intent.expected_pool == CANONICAL_POOL
    serialized = intent.serialize()
    assert serialized["expected_pool"] == CANONICAL_POOL
    assert SupplyIntent.deserialize(serialized) == intent


@pytest.mark.parametrize("intent_cls", [SupplyIntent, WithdrawIntent])
def test_legacy_serialization_omits_unset_expected_pool(intent_cls: type[SupplyIntent] | type[WithdrawIntent]) -> None:
    intent = intent_cls(protocol="aave_v3", token=TOKEN_ADDRESS, amount=Decimal("1"))

    assert "expected_pool" not in intent.serialize()
    assert intent_cls.deserialize(intent.serialize()).expected_pool is None


@pytest.mark.parametrize("bad_address", ["polygon-pool", "0x1234", "0x" + "gg" * 20])
def test_expected_pool_rejects_non_evm_address(bad_address: str) -> None:
    with pytest.raises(ValidationError, match="20-byte EVM address"):
        SupplyIntent(
            protocol="aave_v3",
            token=TOKEN_ADDRESS,
            amount=Decimal("1"),
            expected_pool=bad_address,
        )


@pytest.mark.parametrize("intent_cls", [SupplyIntent, WithdrawIntent])
def test_expected_pool_is_rejected_by_connectors_that_cannot_enforce_it(
    intent_cls: type[SupplyIntent] | type[WithdrawIntent],
) -> None:
    with pytest.raises(ValidationError, match="does not support exact Pool-address binding"):
        intent_cls(
            protocol="spark",
            token=TOKEN_ADDRESS,
            amount=Decimal("1"),
            expected_pool=CANONICAL_POOL,
        )


@pytest.mark.parametrize(
    ("intent_cls", "kwargs"),
    [
        (
            BorrowIntent,
            {
                "protocol": "aave_v3",
                "collateral_token": TOKEN_ADDRESS,
                "collateral_amount": Decimal("0"),
                "borrow_token": TOKEN_ADDRESS,
                "borrow_amount": Decimal("1"),
            },
        ),
        (RepayIntent, {"protocol": "aave_v3", "token": TOKEN_ADDRESS, "amount": Decimal("1")}),
    ],
)
def test_expected_pool_is_rejected_by_unsupported_aave_intents(
    intent_cls: type[BorrowIntent] | type[RepayIntent], kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        intent_cls(**kwargs, expected_pool=CANONICAL_POOL)


@patch(ADAPTER_PATH)
def test_supply_matching_pool_compiles_to_registry_selected_pool(mock_adapter_cls: MagicMock) -> None:
    _adapter(mock_adapter_cls)
    intent = SupplyIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("1"),
        use_as_collateral=False,
        expected_pool=CANONICAL_POOL.lower(),
    )

    result = aave_helpers._compile_supply_aave_compatible(_compiler(), intent, _token(), Decimal("1"))

    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["pool_address"] == CANONICAL_POOL
    assert result.action_bundle.metadata["expected_pool"] == CANONICAL_POOL
    assert result.transactions[-1].to == CANONICAL_POOL


@patch(ADAPTER_PATH)
def test_supply_mismatched_pool_fails_before_calldata_or_approval(mock_adapter_cls: MagicMock) -> None:
    adapter = _adapter(mock_adapter_cls)
    compiler = _compiler()
    intent = SupplyIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("1"),
        use_as_collateral=False,
        expected_pool=OTHER_POOL,
    )

    result = aave_helpers._compile_supply_aave_compatible(compiler, intent, _token(), Decimal("1"))

    assert result.status == CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert "Pool binding mismatch" in (result.error or "")
    assert CANONICAL_POOL in (result.error or "")
    assert OTHER_POOL in (result.error or "")
    compiler._build_approve_tx.assert_not_called()
    adapter.get_supply_calldata.assert_not_called()


@patch(ADAPTER_PATH)
def test_withdraw_matching_pool_compiles_to_registry_selected_pool(mock_adapter_cls: MagicMock) -> None:
    _adapter(mock_adapter_cls)
    intent = WithdrawIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("1"),
        expected_pool=CANONICAL_POOL.lower(),
    )

    result = aave_helpers._compile_withdraw_aave_compatible(_compiler(), intent, _token(), Decimal("1"), [])

    assert result.status == CompilationStatus.SUCCESS
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["pool_address"] == CANONICAL_POOL
    assert result.action_bundle.metadata["expected_pool"] == CANONICAL_POOL
    assert result.transactions[-1].to == CANONICAL_POOL


@patch(ADAPTER_PATH)
def test_withdraw_mismatched_pool_fails_before_calldata(mock_adapter_cls: MagicMock) -> None:
    adapter = _adapter(mock_adapter_cls)
    intent = WithdrawIntent(
        protocol="aave_v3",
        token=TOKEN_ADDRESS,
        amount=Decimal("1"),
        expected_pool=OTHER_POOL,
    )

    result = aave_helpers._compile_withdraw_aave_compatible(_compiler(), intent, _token(), Decimal("1"), [])

    assert result.status == CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert "Pool binding mismatch" in (result.error or "")
    assert CANONICAL_POOL in (result.error or "")
    assert OTHER_POOL in (result.error or "")
    adapter.get_withdraw_calldata.assert_not_called()
