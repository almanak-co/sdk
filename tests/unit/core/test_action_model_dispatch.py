"""Dispatch and validation coverage for the core action models.

Covers every branch of ``Params.from_dict`` and ``Receipt.from_dict`` (one
round-trip per ActionType), the unknown-type fall-through, the
``model_dump`` serialization contracts both dispatchers rely on, and the
multi-branch ``validate_params`` guards on the LP position params.
"""

import uuid

import pytest

from almanak.core.enums import ActionType, SwapSide
from almanak.core.models.params import (
    ApproveParams,
    BorrowParams,
    ClosePositionParams,
    CustomParams,
    DepositParams,
    OpenPositionParams,
    Params,
    RepayParams,
    SettleDepositParams,
    SettleRedeemParams,
    SupplyParams,
    SwapParams,
    TransferParams,
    UnwrapParams,
    UpdateTotalAssetsParams,
    WithdrawParams,
    WrapParams,
)
from almanak.core.models.receipt import (
    ApproveReceipt,
    BorrowReceipt,
    ClosePositionReceipt,
    CustomReceipt,
    DepositReceipt,
    OpenPositionReceipt,
    Receipt,
    RepayReceipt,
    SettleDepositReceipt,
    SettleRedeemReceipt,
    SupplyReceipt,
    SwapReceipt,
    UnwrapReceipt,
    UpdateTotalAssetsReceipt,
    WithdrawReceipt,
    WrapReceipt,
)

ADDR = "0x0000000000000000000000000000000000000001"
ADDR2 = "0x0000000000000000000000000000000000000002"

PARAMS_PAYLOADS = {
    ActionType.TRANSFER: (
        TransferParams,
        {"from_address": ADDR, "to_address": ADDR2, "amount": 100},
    ),
    ActionType.WRAP: (WrapParams, {"from_address": ADDR, "amount": 100}),
    ActionType.UNWRAP: (
        UnwrapParams,
        {"from_address": ADDR, "token_address": ADDR2, "amount": 100},
    ),
    ActionType.APPROVE: (
        ApproveParams,
        {"token_address": ADDR, "spender_address": ADDR2, "from_address": ADDR, "amount": 100},
    ),
    ActionType.SWAP: (
        SwapParams,
        {"tokenIn": ADDR, "tokenOut": ADDR2, "recipient": ADDR, "amount": 100},
    ),
    ActionType.OPEN_LP_POSITION: (
        OpenPositionParams,
        {
            "token0": ADDR,
            "token1": ADDR2,
            "fee": 500,
            "price_lower": 0.9,
            "price_upper": 1.1,
            "amount0_desired": 10,
            "amount1_desired": 20,
            "recipient": ADDR,
            "slippage": 0.01,
        },
    ),
    ActionType.CLOSE_LP_POSITION: (
        ClosePositionParams,
        {
            "position_id": 7,
            "recipient": ADDR,
            "token0": ADDR,
            "token1": ADDR2,
            "slippage": 0.01,
        },
    ),
    ActionType.PROPOSE_VAULT_VALUATION: (
        UpdateTotalAssetsParams,
        {
            "vault_address": ADDR,
            "valuator_address": ADDR2,
            "new_total_assets": 1000,
            "pending_deposits": 0,
        },
    ),
    ActionType.SETTLE_VAULT_DEPOSIT: (
        SettleDepositParams,
        {"vault_address": ADDR, "safe_address": ADDR2, "total_assets": 1000},
    ),
    ActionType.SETTLE_VAULT_REDEEM: (
        SettleRedeemParams,
        {"vault_address": ADDR, "safe_address": ADDR2, "total_assets": 1000},
    ),
    ActionType.DEPOSIT: (
        DepositParams,
        {"token_address": ADDR, "amount": 100, "from_address": ADDR2},
    ),
    ActionType.WITHDRAW: (
        WithdrawParams,
        {"token_address": ADDR, "amount": 100, "to": ADDR2, "from_address": ADDR},
    ),
    ActionType.SUPPLY: (
        SupplyParams,
        {"token_address": ADDR, "amount": 100, "from_address": ADDR2},
    ),
    ActionType.BORROW: (
        BorrowParams,
        {"token_address": ADDR, "amount": 100, "interest_rate_mode": 2, "from_address": ADDR2},
    ),
    ActionType.REPAY: (
        RepayParams,
        {"token_address": ADDR, "amount": 100, "interest_rate_mode": 2, "from_address": ADDR2},
    ),
    ActionType.CUSTOM: (
        CustomParams,
        {
            "protocol": "ENSO",
            "target_protocol": "morpho-markets-v1",
            "function": "setAuthorization",
            "params": {"authorized": True},
            "contract_address": ADDR,
            "from_address": ADDR2,
        },
    ),
}


RECEIPT_BASE = {
    "action_id": str(uuid.uuid4()),
    "bundle_id": None,
    "tx_hash": "0xabc",
    "tx_cost": 21,
    "gas_used": 21000,
    "block_number": 123,
}

_SETTLE_COMMON = {
    "vault_address": ADDR,
    "total_assets": 1000,
    "redeem_assets_withdrawn": 5,
    "redeem_shares_burned": 5,
    "redeem_total_supply": 100,
    "redeem_total_assets": 100,
    "protocol_fee_shares_minted": 1,
    "strategist_fee_shares_minted": 1,
    "old_high_water_mark": 10,
    "new_high_water_mark": 11,
}

RECEIPT_PAYLOADS = {
    ActionType.WRAP: (WrapReceipt, {"amount": 100}),
    ActionType.UNWRAP: (UnwrapReceipt, {"amount": 100}),
    ActionType.APPROVE: (ApproveReceipt, {}),
    ActionType.SWAP: (
        SwapReceipt,
        {"tokenIn_symbol": "USDC", "tokenOut_symbol": "WETH", "amountIn": 100, "amountOut": 99},
    ),
    ActionType.OPEN_LP_POSITION: (
        OpenPositionReceipt,
        {
            "token0_symbol": "USDC",
            "token1_symbol": "WETH",
            "amount0": 10,
            "amount1": 20,
            "position_id": 7,
            "bound_tick_lower": -100,
            "bound_tick_upper": 100,
            "bound_price_lower": 0.9,
            "bound_price_upper": 1.1,
            "pool_tick": 5,
            "pool_spot_rate": 1.0,
        },
    ),
    ActionType.CLOSE_LP_POSITION: (
        ClosePositionReceipt,
        {
            "position_id": 7,
            "token0_symbol": "USDC",
            "token1_symbol": "WETH",
            "amount0": 10,
            "amount1": 20,
            "liquidity0": 9,
            "liquidity1": 19,
            "fees0": 1,
            "fees1": 1,
            "pool_tick": None,
            "pool_spot_rate": None,
        },
    ),
    ActionType.SETTLE_VAULT_DEPOSIT: (
        SettleDepositReceipt,
        {
            **_SETTLE_COMMON,
            "deposit_assets": 50,
            "deposit_shares_minted": 50,
            "deposit_total_supply": 100,
            "deposit_total_assets": 100,
        },
    ),
    ActionType.SETTLE_VAULT_REDEEM: (SettleRedeemReceipt, dict(_SETTLE_COMMON)),
    ActionType.PROPOSE_VAULT_VALUATION: (
        UpdateTotalAssetsReceipt,
        {"vault_address": ADDR, "valuator_address": ADDR2, "total_assets": 1000},
    ),
    ActionType.DEPOSIT: (DepositReceipt, {"amount": 100}),
    ActionType.WITHDRAW: (WithdrawReceipt, {"amount": 100}),
    ActionType.SUPPLY: (SupplyReceipt, {"amount": 100}),
    ActionType.BORROW: (BorrowReceipt, {"amount": 100}),
    ActionType.REPAY: (RepayReceipt, {"amount": 100}),
    ActionType.CUSTOM: (
        CustomReceipt,
        {"function": "setAuthorization", "target_protocol": "morpho-markets-v1"},
    ),
}


class TestParamsFromDict:
    @pytest.mark.parametrize(
        "action_type", sorted(PARAMS_PAYLOADS, key=lambda t: t.value), ids=lambda t: t.value
    )
    def test_dispatches_every_action_type(self, action_type):
        expected_cls, payload = PARAMS_PAYLOADS[action_type]
        params = Params.from_dict({"type": action_type.value, **payload})
        assert type(params) is expected_cls
        assert params.type == action_type
        params.validate_params()
        assert str(params)

    @pytest.mark.parametrize(
        "action_type", sorted(PARAMS_PAYLOADS, key=lambda t: t.value), ids=lambda t: t.value
    )
    def test_model_dump_round_trips(self, action_type):
        _, payload = PARAMS_PAYLOADS[action_type]
        params = Params.from_dict({"type": action_type.value, **payload})
        dumped = params.model_dump()
        assert dumped["type"] == action_type.value
        again = Params.from_dict(dict(dumped))
        assert again == params

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown Params type"):
            Params.from_dict({"type": ActionType.VAULT_REALLOCATE.value})

    def test_invalid_type_string_raises(self):
        with pytest.raises(ValueError):
            Params.from_dict({"type": "NOT_A_REAL_ACTION"})


class TestReceiptFromDict:
    @pytest.mark.parametrize(
        "action_type", sorted(RECEIPT_PAYLOADS, key=lambda t: t.value), ids=lambda t: t.value
    )
    def test_dispatches_every_action_type(self, action_type):
        expected_cls, extra = RECEIPT_PAYLOADS[action_type]
        receipt = Receipt.from_dict({"type": action_type.value, **RECEIPT_BASE, **extra})
        assert type(receipt) is expected_cls
        assert receipt.type == action_type
        assert str(receipt)

    @pytest.mark.parametrize(
        "action_type", sorted(RECEIPT_PAYLOADS, key=lambda t: t.value), ids=lambda t: t.value
    )
    def test_model_dump_round_trips(self, action_type):
        _, extra = RECEIPT_PAYLOADS[action_type]
        receipt = Receipt.from_dict({"type": action_type.value, **RECEIPT_BASE, **extra})
        dumped = receipt.model_dump()
        assert dumped["type"] == action_type.value
        assert dumped["action_id"] == RECEIPT_BASE["action_id"]
        again = Receipt.from_dict(dict(dumped))
        assert again == receipt

    def test_unhandled_type_raises(self):
        # TRANSFER is a valid ActionType with no receipt class — falls through.
        with pytest.raises(ValueError, match="Unknown Receipt type"):
            Receipt.from_dict({"type": ActionType.TRANSFER.value, **RECEIPT_BASE})

    def test_model_dump_stringifies_bundle_id(self):
        bundle_id = uuid.uuid4()
        receipt = Receipt.from_dict(
            {
                "type": ActionType.WRAP.value,
                **{**RECEIPT_BASE, "bundle_id": str(bundle_id)},
                "amount": 100,
            }
        )
        dumped = receipt.model_dump()
        assert dumped["bundle_id"] == str(bundle_id)

    def test_model_dump_honors_exclude(self):
        receipt = Receipt.from_dict({"type": ActionType.WRAP.value, **RECEIPT_BASE, "amount": 100})
        dumped = receipt.model_dump(exclude={"type", "action_id"})
        assert "type" not in dumped
        assert "action_id" not in dumped

    def test_swap_receipt_dumps_side_value(self):
        _, extra = RECEIPT_PAYLOADS[ActionType.SWAP]
        receipt = Receipt.from_dict(
            {"type": ActionType.SWAP.value, **RECEIPT_BASE, **extra, "side": SwapSide.SELL.value}
        )
        assert receipt.model_dump()["side"] == SwapSide.SELL.value
        assert "Side: SELL" in str(receipt)


def _open_params(**overrides) -> OpenPositionParams:
    _, payload = PARAMS_PAYLOADS[ActionType.OPEN_LP_POSITION]
    return OpenPositionParams(**{**payload, **overrides})


def _close_params(**overrides) -> ClosePositionParams:
    _, payload = PARAMS_PAYLOADS[ActionType.CLOSE_LP_POSITION]
    return ClosePositionParams(**{**payload, **overrides})


class TestOpenPositionValidateParams:
    def test_slippage_only_is_valid(self):
        _open_params().validate_params()

    def test_both_mins_is_valid(self):
        _open_params(slippage=None, amount0_min=1, amount1_min=1).validate_params()

    @pytest.mark.parametrize("field", ["token0", "token1"])
    def test_empty_token_rejected(self, field):
        params = _open_params()
        setattr(params, field, "")
        with pytest.raises(ValueError, match="Invalid parameters"):
            params.validate_params()

    @pytest.mark.parametrize("field", ["amount0_desired", "amount1_desired"])
    def test_negative_desired_amount_rejected(self, field):
        params = _open_params()
        setattr(params, field, -1)
        with pytest.raises(ValueError, match="Invalid parameters"):
            params.validate_params()

    @pytest.mark.parametrize(
        "overrides",
        [{"amount0_min": 1, "amount1_min": None}, {"amount0_min": None, "amount1_min": 1}],
    )
    def test_single_min_rejected(self, overrides):
        with pytest.raises(ValueError, match="Both amount_min"):
            _open_params(slippage=None, **overrides).validate_params()

    def test_mins_and_slippage_conflict(self):
        with pytest.raises(ValueError, match="not both"):
            _open_params(amount0_min=1, amount1_min=1).validate_params()

    def test_no_protection_rejected(self):
        with pytest.raises(ValueError, match="Either amount_min or slippage"):
            _open_params(slippage=None).validate_params()


class TestClosePositionValidateParams:
    def test_slippage_only_is_valid(self):
        _close_params().validate_params()

    def test_missing_position_id_rejected(self):
        params = _close_params()
        params.position_id = 0
        with pytest.raises(ValueError, match="Invalid parameters"):
            params.validate_params()

    @pytest.mark.parametrize(
        "overrides",
        [{"amount0_min": 1, "amount1_min": None}, {"amount0_min": None, "amount1_min": 1}],
    )
    def test_single_min_rejected(self, overrides):
        with pytest.raises(ValueError, match="Both amount_min"):
            _close_params(slippage=None, **overrides).validate_params()

    def test_mins_and_slippage_conflict(self):
        with pytest.raises(ValueError, match="not both"):
            _close_params(amount0_min=1, amount1_min=1).validate_params()

    def test_no_protection_rejected(self):
        with pytest.raises(ValueError, match="Either amount_min or slippage"):
            _close_params(slippage=None).validate_params()

    def test_str_includes_optional_fields(self):
        rendered = str(
            _close_params(slippage=None, amount0_min=1, amount1_min=2, pool_address=ADDR)
        )
        assert "amount0_min=1" in rendered
        assert "amount1_min=2" in rendered
        assert f"pool_address={ADDR}" in rendered
