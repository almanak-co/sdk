"""Branch coverage for KrakenAdapter.compile_intent and the compile helpers.

compile_intent dispatches on intent_type (enum-like value, plain string, or
class-name inference when intent_type is absent). The swap/withdraw/deposit
compile helpers are exercised through it with a stub SDK — amount coercion,
"all"-balance withdrawal, chain validation and error propagation. No network.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors.kraken.adapter import (
    ActionType,
    ExecutionContext,
    KrakenAdapter,
    VenueType,
)
from almanak.connectors.kraken.exceptions import (
    KrakenChainNotSupportedError,
    KrakenMinimumOrderError,
)


class _StubSDK:
    """Stands in for KrakenSDK — validation passes amounts through."""

    def __init__(self) -> None:
        self.validate_swap_amount = Mock(side_effect=lambda **kwargs: kwargs["amount_in"])
        self.get_balance = Mock(return_value=SimpleNamespace(available=Decimal("2.5")))


class FakeSwapIntent:
    """Class name carries 'swap' for class-name inference."""

    def __init__(self, **fields) -> None:
        defaults = {"from_token": "USDC", "to_token": "ETH", "amount": Decimal("100")}
        defaults.update(fields)
        for name, value in defaults.items():
            setattr(self, name, value)


class FakeWithdrawIntent:
    def __init__(self, **fields) -> None:
        defaults = {
            "token": "USDC",
            "amount": Decimal("100"),
            "to_address": "0xrecipient",
            "chain": "arbitrum",
        }
        defaults.update(fields)
        for name, value in defaults.items():
            setattr(self, name, value)


class FakeDepositIntent:
    def __init__(self, **fields) -> None:
        defaults = {
            "token": "USDC",
            "amount": Decimal("100"),
            "tx_hash": "0xdeadbeef",
            "chain": "optimism",
        }
        defaults.update(fields)
        for name, value in defaults.items():
            setattr(self, name, value)


class FakeMysteryIntent:
    pass


@pytest.fixture
def sdk():
    return _StubSDK()


@pytest.fixture
def adapter(sdk):
    return KrakenAdapter(sdk=sdk)


@pytest.fixture
def context():
    return ExecutionContext(chain="ethereum", token_decimals={"USDC": 6, "ETH": 18})


class TestDispatchByClassName:
    def test_swap_class_name_inferred(self, adapter, context):
        bundle = adapter.compile_intent(FakeSwapIntent(), context)
        assert bundle.actions[0].type == ActionType.CEX_SWAP

    def test_withdraw_class_name_inferred(self, adapter, context):
        bundle = adapter.compile_intent(FakeWithdrawIntent(), context)
        assert bundle.actions[0].type == ActionType.CEX_WITHDRAW

    def test_deposit_class_name_inferred(self, adapter, context):
        bundle = adapter.compile_intent(FakeDepositIntent(), context)
        assert bundle.actions[0].type == ActionType.CEX_DEPOSIT

    def test_unknown_class_name_rejected(self, adapter, context):
        with pytest.raises(ValueError, match="Unknown intent type: FakeMysteryIntent"):
            adapter.compile_intent(FakeMysteryIntent(), context)


class TestDispatchByIntentType:
    def test_enum_like_intent_type_uses_value(self, adapter, context):
        intent = FakeSwapIntent()
        intent.intent_type = SimpleNamespace(value="SWAP")
        bundle = adapter.compile_intent(intent, context)
        assert bundle.actions[0].type == ActionType.CEX_SWAP

    def test_plain_string_intent_type_withdraw(self, adapter, context):
        intent = FakeWithdrawIntent()
        intent.intent_type = "withdraw"
        bundle = adapter.compile_intent(intent, context)
        assert bundle.actions[0].type == ActionType.CEX_WITHDRAW

    def test_plain_string_intent_type_deposit_case_insensitive(self, adapter, context):
        intent = FakeDepositIntent()
        intent.intent_type = "Deposit"
        bundle = adapter.compile_intent(intent, context)
        assert bundle.actions[0].type == ActionType.CEX_DEPOSIT

    def test_unsupported_intent_type_rejected(self, adapter, context):
        intent = FakeSwapIntent()
        intent.intent_type = SimpleNamespace(value="lp_open")
        with pytest.raises(ValueError, match="Unsupported intent type for CEX: lp_open"):
            adapter.compile_intent(intent, context)


class TestCompileSwap:
    def test_happy_path_bundle_shape(self, adapter, sdk, context):
        bundle = adapter.compile_intent(FakeSwapIntent(chain="arbitrum"), context)

        assert bundle.venue_type == VenueType.CEX
        assert bundle.exchange == "kraken"
        assert bundle.description == "Swap USDC -> ETH on Kraken"
        action = bundle.actions[0]
        assert action.id.startswith("swap_")
        assert action.asset_in == "USDC"
        assert action.asset_out == "ETH"
        # 100 USDC at 6 decimals, passed through validation unchanged.
        assert action.amount_in == 100_000_000
        assert action.decimals_in == 6
        assert action.decimals_out == 18
        assert isinstance(action.userref, int)
        assert action.metadata == {"chain": "arbitrum", "original_amount": 100_000_000}
        sdk.validate_swap_amount.assert_called_once_with(
            asset_in="USDC",
            asset_out="ETH",
            amount_in=100_000_000,
            decimals_in=6,
            chain="arbitrum",
        )

    def test_chain_falls_back_to_context(self, adapter, sdk, context):
        adapter.compile_intent(FakeSwapIntent(), context)
        assert sdk.validate_swap_amount.call_args.kwargs["chain"] == "ethereum"

    def test_float_amount_coerced(self, adapter, context):
        bundle = adapter.compile_intent(FakeSwapIntent(amount=0.5), context)
        assert bundle.actions[0].amount_in == 500_000

    def test_unsupported_amount_type_rejected(self, adapter, context):
        with pytest.raises(ValueError, match="Unsupported amount type"):
            adapter.compile_intent(FakeSwapIntent(amount="lots"), context)

    def test_missing_tokens_rejected(self, adapter, context):
        with pytest.raises(ValueError, match="SwapIntent requires from_token and to_token"):
            adapter.compile_intent(FakeSwapIntent(to_token=None), context)

    def test_validation_error_propagates(self, adapter, sdk, context):
        sdk.validate_swap_amount.side_effect = KrakenMinimumOrderError(
            "too small", pair="USDC/ETH", amount="1", minimum="10"
        )
        with pytest.raises(KrakenMinimumOrderError):
            adapter.compile_intent(FakeSwapIntent(), context)


class TestCompileWithdraw:
    def test_happy_path_bundle_shape(self, adapter, context):
        bundle = adapter.compile_intent(FakeWithdrawIntent(), context)

        assert bundle.venue_type == VenueType.CEX
        assert bundle.description == "Withdraw USDC to arbitrum on Kraken"
        action = bundle.actions[0]
        assert action.id.startswith("withdraw_")
        assert action.asset == "USDC"
        assert action.amount == 100_000_000
        assert action.decimals == 6
        assert action.chain == "arbitrum"
        assert action.to_address == "0xrecipient"

    @pytest.mark.parametrize(
        ("field", "message"),
        [
            ("token", "WithdrawIntent requires token"),
            ("to_address", "WithdrawIntent requires to_address"),
            ("chain", "WithdrawIntent requires chain"),
        ],
    )
    def test_missing_required_field_rejected(self, adapter, context, field, message):
        with pytest.raises(ValueError, match=message):
            adapter.compile_intent(FakeWithdrawIntent(**{field: None}), context)

    def test_unsupported_chain_rejected(self, adapter, context):
        with pytest.raises(KrakenChainNotSupportedError):
            adapter.compile_intent(FakeWithdrawIntent(chain="base"), context)

    def test_amount_all_uses_available_balance(self, adapter, sdk, context):
        bundle = adapter.compile_intent(FakeWithdrawIntent(amount="all"), context)
        # 2.5 available USDC at 6 decimals.
        assert bundle.actions[0].amount == 2_500_000
        sdk.get_balance.assert_called_once_with("USDC", "arbitrum")

    def test_float_amount_coerced(self, adapter, context):
        bundle = adapter.compile_intent(FakeWithdrawIntent(amount=0.75), context)
        assert bundle.actions[0].amount == 750_000

    def test_unsupported_amount_type_rejected(self, adapter, context):
        with pytest.raises(ValueError, match="Unsupported amount type"):
            adapter.compile_intent(FakeWithdrawIntent(amount=None), context)

    def test_unknown_token_defaults_to_18_decimals(self, adapter, context):
        bundle = adapter.compile_intent(
            FakeWithdrawIntent(token="WETH", amount=Decimal("1")), context
        )
        assert bundle.actions[0].decimals == 18
        assert bundle.actions[0].amount == 10**18


class TestCompileDeposit:
    def test_happy_path_bundle_shape(self, adapter, context):
        bundle = adapter.compile_intent(FakeDepositIntent(), context)

        assert bundle.description == "Track USDC deposit from optimism on Kraken"
        action = bundle.actions[0]
        assert action.id.startswith("deposit_")
        assert action.asset == "USDC"
        assert action.amount == 100_000_000
        assert action.tx_hash == "0xdeadbeef"
        assert action.from_chain == "optimism"

    @pytest.mark.parametrize(
        ("field", "message"),
        [
            ("token", "DepositIntent requires token"),
            ("tx_hash", "DepositIntent requires tx_hash"),
            ("chain", "DepositIntent requires chain"),
        ],
    )
    def test_missing_required_field_rejected(self, adapter, context, field, message):
        with pytest.raises(ValueError, match=message):
            adapter.compile_intent(FakeDepositIntent(**{field: None}), context)

    def test_float_amount_coerced(self, adapter, context):
        bundle = adapter.compile_intent(FakeDepositIntent(amount=1.5), context)
        assert bundle.actions[0].amount == 1_500_000

    def test_unknown_amount_defaults_to_zero(self, adapter, context):
        # Amount resolved later from the deposit receipt.
        bundle = adapter.compile_intent(FakeDepositIntent(amount=None), context)
        assert bundle.actions[0].amount == 0
