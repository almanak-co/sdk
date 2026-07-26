"""VIB-3873 / VIB-3872 WI-1 — keyed EventUtils decode for GMX fill economics.

These tests encode the keeper events with the REAL EventUtils ABI encoder (the
same dynamic keyed struct production emits) rather than hand-crafted flat words.
The flat-word shape is exactly the fixture blindspot that masked the VIB-3873
misread class (ALM-2993): a positional decode reads dynamic-struct ABI offsets
as field values. Building the keyed struct and decoding it BY NAME is what these
tests lock in.
"""

from decimal import Decimal

import pytest
from eth_abi import encode as abi_encode

from almanak.connectors.gmx_v2.receipt_parser import (
    _EVENT_LOG_DATA_ABI_TYPE,
    EVENT_TOPICS,
    GMX_MAX_ORDER_TYPE,
    GMXOrderTypeError,
    GMXv2ReceiptParser,
    PerpFillData,
)

_EVENT_LOG1_TOPIC = "0x" + "11" * 32
_TX_HASH = "0x" + "22" * 32
_MARKET = "0x" + "33" * 20
_COLLATERAL = "0x" + "44" * 20  # treated as USDC-scaled below
_ACCOUNT = "0x" + "55" * 20
_ORDER_KEY = "0x" + "ab" * 32
_POSITION_KEY = "0x" + "cd" * 32

# USDC-style collateral: 6 decimals => collateralTokenPrice raw = 1e(30-6) = 1e24.
_USDC_PRICE_RAW = 10**24


def _group(scalars: list[tuple], arrays: list[tuple] | None = None) -> tuple:
    return (scalars, arrays or [])


def _encode_event(event_name: str, *, addresses, uints, ints, bools, bytes32) -> str:
    payload = abi_encode(
        ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
        [
            "0x" + "77" * 20,
            event_name,
            (
                _group(addresses),
                _group(uints),
                _group(ints),
                _group(bools),
                _group(bytes32),
                _group([]),  # bytes items (unused)
                _group([]),  # string items (unused)
            ),
        ],
    )
    return "0x" + payload.hex()


def _position_increase_data(
    *,
    order_type: int = 2,
    execution_price_raw: int = 3000 * 10**12,  # ETH ~ $3000, native GMX price
    size_delta_usd_raw: int = 6000 * 10**30,
    size_delta_in_tokens_raw: int = 2 * 10**18,
    collateral_delta_raw: int = 1500 * 10**6,
    price_impact_raw: int = -2 * 10**28,  # -0.02 USD, SIGNED
    is_long: bool = True,
    scramble: bool = False,
) -> str:
    uints = [
        ("sizeInUsd", size_delta_usd_raw),
        ("collateralAmount", collateral_delta_raw),
        ("executionPrice", execution_price_raw),
        ("sizeDeltaUsd", size_delta_usd_raw),
        ("sizeDeltaInTokens", size_delta_in_tokens_raw),
        ("orderType", order_type),
    ]
    if scramble:
        # Keyed decode must be order-independent — reverse the item order and the
        # values must still map to the right keys. A positional decode would break.
        uints = list(reversed(uints))
    return _encode_event(
        "PositionIncrease",
        addresses=[("account", _ACCOUNT), ("market", _MARKET), ("collateralToken", _COLLATERAL)],
        uints=uints,
        ints=[("collateralDeltaAmount", collateral_delta_raw), ("pendingPriceImpactUsd", price_impact_raw)],
        bools=[("isLong", is_long)],
        bytes32=[("orderKey", bytes.fromhex(_ORDER_KEY[2:])), ("positionKey", bytes.fromhex(_POSITION_KEY[2:]))],
    )


def _position_decrease_data(
    *,
    order_type: int = 4,
    execution_price_raw: int = 3100 * 10**12,
    size_delta_usd_raw: int = 6000 * 10**30,
    collateral_delta_raw: int = 1500 * 10**6,
    base_pnl_raw: int = -25 * 10**30,  # SIGNED loss
    price_impact_raw: int = -3 * 10**28,
    is_long: bool = True,
) -> str:
    return _encode_event(
        "PositionDecrease",
        addresses=[("account", _ACCOUNT), ("market", _MARKET), ("collateralToken", _COLLATERAL)],
        uints=[
            ("executionPrice", execution_price_raw),
            ("sizeDeltaUsd", size_delta_usd_raw),
            ("sizeDeltaInTokens", 2 * 10**18),
            ("collateralAmount", 0),
            ("collateralDeltaAmount", collateral_delta_raw),
            ("orderType", order_type),
        ],
        ints=[("priceImpactUsd", price_impact_raw), ("basePnlUsd", base_pnl_raw)],
        bools=[("isLong", is_long)],
        bytes32=[("orderKey", bytes.fromhex(_ORDER_KEY[2:])), ("positionKey", bytes.fromhex(_POSITION_KEY[2:]))],
    )


def _position_fees_data(
    *,
    funding_fee_amount: int = 500_000,  # 0.5 USDC
    position_fee_amount: int = 1_200_000,  # 1.2 USDC
    borrowing_fee_amount: int = 300_000,  # 0.3 USDC
    price_raw: int = _USDC_PRICE_RAW,
) -> str:
    return _encode_event(
        "PositionFeesCollected",
        addresses=[("market", _MARKET), ("collateralToken", _COLLATERAL)],
        uints=[
            ("collateralTokenPrice.min", price_raw),
            ("collateralTokenPrice.max", price_raw),
            ("fundingFeeAmount", funding_fee_amount),
            ("positionFeeAmount", position_fee_amount),
            ("borrowingFeeAmount", borrowing_fee_amount),
        ],
        ints=[],
        bools=[("isIncrease", False)],
        bytes32=[("orderKey", bytes.fromhex(_ORDER_KEY[2:])), ("positionKey", bytes.fromhex(_POSITION_KEY[2:]))],
    )


def _log(event_name: str, data: str, *, log_index: int = 1) -> dict:
    return {
        "address": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
        "topics": [_EVENT_LOG1_TOPIC, EVENT_TOPICS[event_name], "0x" + _POSITION_KEY[2:]],
        "data": data,
        "logIndex": log_index,
    }


def _receipt(logs: list[dict]) -> dict:
    return {
        "transactionHash": _TX_HASH,
        "blockNumber": 987654,
        "status": 1,
        "logs": logs,
        "gasUsed": 300_000,
    }


class TestPerpFillOpen:
    def test_open_fill_measures_entry_and_identity(self) -> None:
        receipt = _receipt([_log("PositionIncrease", _position_increase_data())])
        fill = GMXv2ReceiptParser().extract_perp_fill(receipt)

        assert isinstance(fill, PerpFillData)
        assert fill.is_open is True
        assert fill.is_long is True
        assert fill.market == _MARKET
        assert fill.collateral_token == _COLLATERAL
        assert fill.position_key == _POSITION_KEY
        assert fill.order_key == _ORDER_KEY
        assert fill.entry_price == Decimal(3000 * 10**12) / Decimal(10**30)
        assert fill.exit_price is None  # opens have no exit
        assert fill.size_delta_usd == Decimal(6000)
        assert fill.collateral_delta_amount == Decimal(1500 * 10**6)
        assert fill.price_impact_usd == Decimal(-2 * 10**28) / Decimal(10**30)  # signed
        assert fill.realized_pnl_usd is None  # opens have no realized pnl
        assert fill.keeper_tx_hash == _TX_HASH
        assert fill.block_number == 987654

    def test_keyed_decode_is_order_independent(self) -> None:
        """Proves the decode is BY KEY, not positional: shuffling item order must
        not change any decoded value (a positional decode would corrupt them)."""
        ordered = GMXv2ReceiptParser().extract_perp_fill(
            _receipt([_log("PositionIncrease", _position_increase_data())])
        )
        shuffled = GMXv2ReceiptParser().extract_perp_fill(
            _receipt([_log("PositionIncrease", _position_increase_data(scramble=True))])
        )
        assert ordered == shuffled
        assert shuffled.entry_price == Decimal(3000 * 10**12) / Decimal(10**30)
        assert shuffled.size_delta_usd == Decimal(6000)


class TestPerpFillClose:
    def test_close_fill_measures_exit_pnl_and_fees(self) -> None:
        receipt = _receipt(
            [
                _log("PositionDecrease", _position_decrease_data(), log_index=1),
                _log("PositionFeesCollected", _position_fees_data(), log_index=2),
            ]
        )
        fill = GMXv2ReceiptParser().extract_perp_fill(receipt)

        assert fill.is_open is False
        assert fill.exit_price == Decimal(3100 * 10**12) / Decimal(10**30)
        assert fill.entry_price is None
        assert fill.realized_pnl_usd == Decimal(-25)  # signed loss, measured
        assert fill.price_impact_usd == Decimal(-3 * 10**28) / Decimal(10**30)
        assert fill.collateral_delta_amount == Decimal(1500 * 10**6)
        # Fees: amount * collateralTokenPrice / 1e30 — decimals-free USD.
        assert fill.funding_fee_usd == Decimal("0.5")
        assert fill.position_fee_usd == Decimal("1.2")
        assert fill.borrowing_fee_usd == Decimal("0.3")

    def test_close_without_fees_event_leaves_fees_none(self) -> None:
        """Empty != Zero: no PositionFeesCollected => fee fields unmeasured (None)."""
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        fill = GMXv2ReceiptParser().extract_perp_fill(receipt)

        assert fill.is_open is False
        assert fill.realized_pnl_usd == Decimal(-25)
        assert fill.funding_fee_usd is None
        assert fill.position_fee_usd is None
        assert fill.borrowing_fee_usd is None


class TestExtractFundingFeeUsd:
    def test_measured_funding_fee(self) -> None:
        receipt = _receipt([_log("PositionFeesCollected", _position_fees_data())])
        assert GMXv2ReceiptParser().extract_funding_fee_usd(receipt) == Decimal("0.5")

    def test_measured_zero_funding_is_zero_not_none(self) -> None:
        receipt = _receipt([_log("PositionFeesCollected", _position_fees_data(funding_fee_amount=0))])
        result = GMXv2ReceiptParser().extract_funding_fee_usd(receipt)
        assert result == Decimal("0")
        assert result is not None

    def test_no_fees_event_is_none(self) -> None:
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        assert GMXv2ReceiptParser().extract_funding_fee_usd(receipt) is None


class TestOrderTypeBoundCheck:
    def test_bound_constant_is_seven(self) -> None:
        assert GMX_MAX_ORDER_TYPE == 7

    def test_increase_order_type_above_bound_raises(self) -> None:
        parser = GMXv2ReceiptParser()
        # A flat-word misread yields garbage like 32 (an ABI offset). Direct
        # decoder call proves the tripwire in isolation (mutation-resistant).
        with pytest.raises(GMXOrderTypeError):
            parser._decode_event_utils_position_increase(
                _position_increase_data(order_type=32).removeprefix("0x")
            )

    def test_decrease_order_type_above_bound_raises(self) -> None:
        parser = GMXv2ReceiptParser()
        with pytest.raises(GMXOrderTypeError):
            parser._decode_event_utils_position_decrease(
                _position_decrease_data(order_type=160).removeprefix("0x")
            )

    def test_parse_receipt_fails_closed_on_bad_order_type(self) -> None:
        receipt = _receipt([_log("PositionIncrease", _position_increase_data(order_type=32))])
        result = GMXv2ReceiptParser().parse_receipt(receipt)
        assert result.success is False
        assert "order_type" in (result.error or "")

    @pytest.mark.parametrize("order_type", [0, 2, 4, 7])
    def test_in_range_order_type_ok(self, order_type: int) -> None:
        decoded = GMXv2ReceiptParser()._decode_event_utils_position_increase(
            _position_increase_data(order_type=order_type).removeprefix("0x")
        )
        assert decoded is not None
        assert decoded["order_type"] == order_type


class TestEmptyNotZero:
    def test_no_position_event_returns_none(self) -> None:
        receipt = _receipt([_log("PositionFeesCollected", _position_fees_data())])
        assert GMXv2ReceiptParser().extract_perp_fill(receipt) is None

    def test_empty_receipt_returns_none(self) -> None:
        assert GMXv2ReceiptParser().extract_perp_fill(_receipt([])) is None

    def test_keyed_increase_does_not_fabricate_typed_position_increase(self) -> None:
        """A production keyed PositionIncrease must NOT populate positionally
        decoded typed data (the VIB-3873 garbage). It flows via PerpFillData."""
        receipt = _receipt([_log("PositionIncrease", _position_increase_data())])
        parsed = GMXv2ReceiptParser().parse_receipt(receipt)
        assert parsed.success is True
        assert parsed.position_increases == []  # no garbage typed row


class TestPerpFillToDict:
    def test_to_dict_preserves_empty_not_zero(self) -> None:
        receipt = _receipt([_log("PositionDecrease", _position_decrease_data())])
        payload = GMXv2ReceiptParser().extract_perp_fill(receipt).to_dict()
        assert payload["funding_fee_usd"] is None  # unmeasured stays None
        assert payload["realized_pnl_usd"] == "-25"
        assert payload["is_open"] is False
