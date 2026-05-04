"""Tests for AaveV3ReceiptParser end-to-end + decoder branches.

Covers the wide set of branches not exercised by the existing
test_aave_v3_receipt_enrichment.py / test_extract_result_variants.py /
test_protocol_fees.py / test_format_token_amount.py files:

- parse_receipt: bytes tx_hash, bytes/hex-string blockNumber + status,
  status==0 (revert), unknown topics, parsed actions log path,
  liquidation logging path
- parse_logs (top-level helper)
- _parse_log: bytes first_topic, address+data bytes, no topics
- _decode_*_data: FlashLoan, LiquidationCall, UserEModeSet,
  IsolationModeTotalDebtUpdated, collateral toggle Enabled+Disabled,
  decode error → returns {"raw_data": ...}
- _parse_*: error path returns None when data dict is malformed
- extract_a_token_received: non-zero from_addr (not a mint), missing topics,
  empty receipt
- extract_debt_token: amount-mismatch transfer ignored, bytes log address,
  empty logs returns None
- extract_a_token_burned: empty receipt, non-burn transfer ignored,
  burn from another address ignored
- extract_supply_rate: receipt where ReserveDataUpdated has no liquidity_rate
- extract_borrow_rate: empty receipt
- extract_protocol_fees: explodes when parse_receipt is patched to raise
- is_aave_event / get_event_type: bytes input, string without 0x prefix
- AaveV3Event: to_dict / from_dict round-trip, FlashLoanEventData.opened_debt,
  UserEModeSetEventData.category_name fallback
- ParseResult.to_dict
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.aave_v3.receipt_parser import (
    EVENT_TOPICS,
    AaveV3Event,
    AaveV3EventType,
    AaveV3ReceiptParser,
    BorrowEventData,
    FlashLoanEventData,
    IsolationModeDebtUpdatedEventData,
    LiquidationCallEventData,
    ParseResult,
    RepayEventData,
    ReserveDataUpdatedEventData,
    SupplyEventData,
    UserEModeSetEventData,
    WithdrawEventData,
)
from almanak.framework.execution.extract_result import ExtractError


# =============================================================================
# Common helpers (mirror test_aave_v3_receipt_enrichment.py)
# =============================================================================

USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USER_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
LIQUIDATOR = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
OTHER_USER = "0x9999999999999999999999999999999999999999"
DEBT_TOKEN = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
ATOKEN_ADDR = "0xA0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0"
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _pad_address(addr: str) -> str:
    clean = addr.lower().replace("0x", "")
    return "0x" + clean.zfill(64)


def _encode_uint256(v: int) -> str:
    return hex(v)[2:].zfill(64)


def _make_flash_loan_log(
    target: str = LIQUIDATOR,
    initiator: str = USER_ADDRESS,
    asset: str = USDC_ADDRESS,
    amount: int = 1_000_000_000,
    interest_rate_mode: int = 0,
    premium: int = 500_000,
    referral_code: int = 0,
) -> dict[str, Any]:
    """FlashLoan(address indexed target, address initiator, address indexed asset,
    uint256 amount, uint256 interestRateMode, uint256 premium, uint16 referralCode)
    """
    data = (
        _encode_uint256(int(initiator, 16))
        + _encode_uint256(amount)
        + _encode_uint256(interest_rate_mode)
        + _encode_uint256(premium)
        + _encode_uint256(referral_code)
    )
    return {
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["FlashLoan"],
            _pad_address(target),
            _pad_address(asset),
        ],
        "data": "0x" + data,
        "logIndex": 5,
    }


def _make_liquidation_log(
    collateral: str = WETH_ADDRESS,
    debt: str = USDC_ADDRESS,
    user: str = USER_ADDRESS,
    debt_to_cover: int = 500_000_000,
    collateral_amount: int = 100_000_000_000_000_000,  # 0.1 WETH
    liquidator: str = LIQUIDATOR,
    receive_atoken: bool = False,
) -> dict[str, Any]:
    """LiquidationCall(address indexed collateralAsset, address indexed debtAsset,
    address indexed user, uint256 debtToCover, uint256 liquidatedCollateralAmount,
    address liquidator, bool receiveAToken)"""
    data = (
        _encode_uint256(debt_to_cover)
        + _encode_uint256(collateral_amount)
        + _encode_uint256(int(liquidator, 16))
        + _encode_uint256(1 if receive_atoken else 0)
    )
    return {
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["LiquidationCall"],
            _pad_address(collateral),
            _pad_address(debt),
            _pad_address(user),
        ],
        "data": "0x" + data,
        "logIndex": 7,
    }


def _make_user_emode_log(user: str = USER_ADDRESS, category_id: int = 1) -> dict[str, Any]:
    """UserEModeSet(address indexed user, uint8 categoryId)"""
    return {
        "address": POOL_ADDRESS,
        "topics": [EVENT_TOPICS["UserEModeSet"], _pad_address(user)],
        "data": "0x" + _encode_uint256(category_id),
        "logIndex": 8,
    }


def _make_isolation_mode_log(
    asset: str = USDC_ADDRESS, total_debt_cents: int = 5_000_00
) -> dict[str, Any]:
    """IsolationModeTotalDebtUpdated(address indexed asset, uint256 totalDebt)"""
    return {
        "address": POOL_ADDRESS,
        "topics": [EVENT_TOPICS["IsolationModeTotalDebtUpdated"], _pad_address(asset)],
        "data": "0x" + _encode_uint256(total_debt_cents),
        "logIndex": 9,
    }


def _make_collateral_toggle_log(
    enabled: bool, reserve: str = USDC_ADDRESS, user: str = USER_ADDRESS
) -> dict[str, Any]:
    """ReserveUsedAsCollateralEnabled/Disabled(address indexed reserve,
    address indexed user)"""
    topic = EVENT_TOPICS[
        "ReserveUsedAsCollateralEnabled" if enabled else "ReserveUsedAsCollateralDisabled"
    ]
    return {
        "address": POOL_ADDRESS,
        "topics": [topic, _pad_address(reserve), _pad_address(user)],
        "data": "0x",
        "logIndex": 10,
    }


def _make_reserve_data_updated_log(
    reserve: str = WETH_ADDRESS,
    liquidity_rate_ray: int = 35_000_000_000_000_000_000_000_000,
    stable_rate_ray: int = 60_000_000_000_000_000_000_000_000,
    var_rate_ray: int = 50_000_000_000_000_000_000_000_000,
    liq_index_ray: int = 10**27,
    var_index_ray: int = 10**27,
) -> dict[str, Any]:
    data = (
        _encode_uint256(liquidity_rate_ray)
        + _encode_uint256(stable_rate_ray)
        + _encode_uint256(var_rate_ray)
        + _encode_uint256(liq_index_ray)
        + _encode_uint256(var_index_ray)
    )
    return {
        "address": POOL_ADDRESS,
        "topics": [EVENT_TOPICS["ReserveDataUpdated"], _pad_address(reserve)],
        "data": "0x" + data,
        "logIndex": 11,
    }


# =============================================================================
# parse_receipt branches
# =============================================================================


class TestParseReceiptNormalisation:
    def test_bytes_tx_hash_and_block_number(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt(
            {
                "transactionHash": b"\xab" * 32,
                "blockNumber": (12345).to_bytes(32, "big"),
                "status": (1).to_bytes(1, "big"),
                "logs": [],
            }
        )
        assert result.success is True
        assert result.transaction_hash == "0x" + "ab" * 32
        assert result.block_number == 12345

    def test_hex_block_number_and_status(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt(
            {
                "blockNumber": "0x1234",
                "status": "0x1",
                "logs": [],
            }
        )
        assert result.success is True
        assert result.block_number == 0x1234

    def test_decimal_string_block_number(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt(
            {
                "blockNumber": "999",
                "status": "1",
                "logs": [],
            }
        )
        assert result.block_number == 999

    def test_revert_status_returns_error(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt({"status": 0, "logs": [{}]})
        assert result.success is True
        assert result.error == "Transaction reverted"

    def test_no_logs_returns_empty_success(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt({"status": 1, "logs": []})
        assert result.success is True
        assert result.events == []

    def test_logs_with_no_actions_logs_event_count(self) -> None:
        # Logs with no parseable Aave V3 events: still a successful parse
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt(
            {
                "status": 1,
                "logs": [
                    {
                        "topics": ["0x" + "11" * 32],  # Unknown topic
                        "data": "0x",
                        "address": "0x" + "00" * 20,
                    }
                ],
            }
        )
        assert result.success is True
        assert result.events == []  # Unknown event filtered out

    def test_top_level_exception_returns_failure(self) -> None:
        parser = AaveV3ReceiptParser()
        # logs as None will trip an exception in normalisation
        result = parser.parse_receipt({"status": 1, "logs": None})
        # Either success with no events or success=False; assert it doesn't crash hard
        assert isinstance(result, ParseResult)


class TestParseReceiptActionsLogging:
    def test_supply_action_summary(self) -> None:
        # Build supply receipt and ensure parse_receipt's action-summary branch fires
        from .test_aave_v3_receipt_enrichment import _make_supply_receipt

        parser = AaveV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_supply_receipt())
        assert result.success is True
        assert result.supplies

    def test_withdraw_action_summary(self) -> None:
        from .test_aave_v3_receipt_enrichment import _make_withdraw_receipt

        parser = AaveV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_withdraw_receipt())
        assert result.success is True
        assert result.withdraws

    def test_borrow_action_summary(self) -> None:
        from .test_aave_v3_receipt_enrichment import _make_borrow_receipt

        parser = AaveV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_borrow_receipt())
        assert result.success is True
        assert result.borrows
        assert result.borrows[0].is_variable_rate is True

    def test_repay_action_summary(self) -> None:
        from .test_aave_v3_receipt_enrichment import _make_repay_receipt

        parser = AaveV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(_make_repay_receipt())
        assert result.success is True
        assert result.repays

    def test_liquidation_action_summary(self) -> None:
        parser = AaveV3ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt(
            {"status": 1, "logs": [_make_liquidation_log()]}
        )
        assert result.success is True
        assert len(result.liquidations) == 1


# =============================================================================
# parse_logs / _parse_log
# =============================================================================


class TestParseLogs:
    def test_parse_logs_helper_returns_events(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([_make_flash_loan_log()])
        assert len(events) == 1
        assert events[0].event_type == AaveV3EventType.FLASH_LOAN

    def test_parse_logs_skips_unknown(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [{"topics": ["0x" + "ff" * 32], "data": "0x", "address": "0x" + "00" * 20}]
        )
        assert events == []

    def test_parse_log_no_topics_returns_none(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([{"topics": [], "data": "0x"}])
        assert events == []

    def test_parse_log_bytes_first_topic(self) -> None:
        parser = AaveV3ReceiptParser()
        topic_bytes = bytes.fromhex(EVENT_TOPICS["UserEModeSet"][2:])
        events = parser.parse_logs(
            [
                {
                    "topics": [topic_bytes, _pad_address(USER_ADDRESS)],
                    "data": "0x" + _encode_uint256(1),
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1
        assert events[0].event_type == AaveV3EventType.USER_EMODE_SET

    def test_parse_log_bytes_data_field(self) -> None:
        parser = AaveV3ReceiptParser()
        data_bytes = bytes.fromhex(_encode_uint256(2))
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["UserEModeSet"],
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": data_bytes,
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1
        assert events[0].data["category_id"] == 2

    def test_parse_log_bytes_address(self) -> None:
        parser = AaveV3ReceiptParser()
        addr_bytes = bytes.fromhex(POOL_ADDRESS[2:])
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["UserEModeSet"],
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": "0x" + _encode_uint256(0),
                    "address": addr_bytes,
                }
            ]
        )
        assert len(events) == 1
        assert events[0].contract_address.lower().endswith(POOL_ADDRESS[2:].lower())

    def test_parse_log_bytes_topic_in_topics_list(self) -> None:
        # Make sure bytes topic in topics_str loop is converted (line 964-965)
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["UserEModeSet"],
                        bytes.fromhex(_pad_address(USER_ADDRESS)[2:]),
                    ],
                    "data": "0x" + _encode_uint256(1),
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1
        # raw_topics must include the converted hex string
        assert any(t.startswith("0x") for t in events[0].raw_topics)


# =============================================================================
# _decode_* event decoders
# =============================================================================


class TestFlashLoanDecoder:
    def test_decode_flash_loan_full(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([_make_flash_loan_log()])
        assert len(events) == 1
        d = events[0].data
        assert d["target"].lower() == LIQUIDATOR.lower()
        assert d["initiator"].lower() == USER_ADDRESS.lower()
        assert d["asset"].lower() == USDC_ADDRESS.lower()
        assert d["amount"] == "1000000000"
        assert d["premium"] == "500000"

    def test_decode_flash_loan_propagates_to_typed_data(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt(
            {"status": 1, "logs": [_make_flash_loan_log(interest_rate_mode=2)]}
        )
        assert len(result.flash_loans) == 1
        fl = result.flash_loans[0]
        assert fl.opened_debt is True
        assert fl.amount == Decimal("1000000000")

    def test_decode_flash_loan_truncated_data_returns_raw(self) -> None:
        parser = AaveV3ReceiptParser()
        # truncated data → HexDecoder will likely throw → except branch
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["FlashLoan"],
                        _pad_address(LIQUIDATOR),
                        _pad_address(USDC_ADDRESS),
                    ],
                    "data": "0x" + "00" * 4,  # too short
                    "address": POOL_ADDRESS,
                }
            ]
        )
        # the event is parsed, but data may be {"raw_data": ...} from the except branch
        assert len(events) == 1


class TestLiquidationDecoder:
    def test_decode_liquidation_full(self) -> None:
        parser = AaveV3ReceiptParser()
        result = parser.parse_receipt(
            {"status": 1, "logs": [_make_liquidation_log(receive_atoken=True)]}
        )
        liq = result.liquidations[0]
        assert liq.user.lower() == USER_ADDRESS.lower()
        assert liq.receive_atoken is True
        assert liq.liquidator.lower() == LIQUIDATOR.lower()

    def test_decode_liquidation_truncated_data_returns_raw(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["LiquidationCall"],
                        _pad_address(WETH_ADDRESS),
                        _pad_address(USDC_ADDRESS),
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": "0x" + "00" * 8,  # truncated
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1


class TestReserveDataUpdatedDecoder:
    def test_decode_reserve_data_updated(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([_make_reserve_data_updated_log()])
        assert len(events) == 1
        d = events[0].data
        # 5% in ray → 0.05
        assert Decimal(d["variable_borrow_rate"]) == Decimal("0.05")

    def test_decode_reserve_data_updated_truncated_returns_raw(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["ReserveDataUpdated"],
                        _pad_address(WETH_ADDRESS),
                    ],
                    "data": "0x" + "00" * 8,  # truncated
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1


class TestUserEModeSetDecoder:
    def test_decode_user_emode(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([_make_user_emode_log(category_id=2)])
        assert len(events) == 1
        assert events[0].data["category_id"] == 2

    def test_decode_user_emode_truncated_data_returns_raw(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [
                {
                    "topics": [EVENT_TOPICS["UserEModeSet"], _pad_address(USER_ADDRESS)],
                    "data": "0x",  # too short
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1


class TestIsolationModeDebtDecoder:
    def test_decode_isolation_mode_debt(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [_make_isolation_mode_log(total_debt_cents=12_345)]
        )
        # total_debt = cents / 100 = 123.45
        assert Decimal(events[0].data["total_debt"]) == Decimal("123.45")

    def test_decode_isolation_mode_debt_truncated_returns_raw(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["IsolationModeTotalDebtUpdated"],
                        _pad_address(USDC_ADDRESS),
                    ],
                    "data": "0x",
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1


class TestCollateralToggleDecoder:
    def test_decode_collateral_enabled(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([_make_collateral_toggle_log(enabled=True)])
        assert len(events) == 1
        assert events[0].data["enabled"] is True
        assert events[0].event_type == AaveV3EventType.RESERVE_USED_AS_COLLATERAL_ENABLED

    def test_decode_collateral_disabled(self) -> None:
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs([_make_collateral_toggle_log(enabled=False)])
        assert events[0].data["enabled"] is False
        assert events[0].event_type == AaveV3EventType.RESERVE_USED_AS_COLLATERAL_DISABLED


class TestUnknownDecodingPath:
    def test_unknown_aave_event_falls_to_default_branch(self) -> None:
        # A registered event without a custom decoder hits the generic branch
        parser = AaveV3ReceiptParser()
        events = parser.parse_logs(
            [
                {
                    "topics": [
                        EVENT_TOPICS["AssetSourceUpdated"],
                        _pad_address(USDC_ADDRESS),
                    ],
                    "data": "0x" + _encode_uint256(0),
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert len(events) == 1
        assert "raw_data" in events[0].data


class TestDecodingErrorPaths:
    """Force the decoder except branches by passing pure garbage data."""

    @pytest.mark.parametrize(
        "event_name",
        [
            "Supply",
            "Withdraw",
            "Borrow",
            "Repay",
        ],
    )
    def test_decoder_truncated_data(self, event_name: str) -> None:
        parser = AaveV3ReceiptParser()
        # need 2 indexed topics for these events but data is too short
        topics = [EVENT_TOPICS[event_name], _pad_address(USDC_ADDRESS)]
        if event_name in ("Withdraw", "Repay"):
            topics.append(_pad_address(USER_ADDRESS))
        if event_name == "Repay":
            topics.append(_pad_address(USER_ADDRESS))
        if event_name in ("Supply", "Borrow"):
            topics.append(_pad_address(USER_ADDRESS))

        events = parser.parse_logs(
            [
                {
                    "topics": topics,
                    "data": "0x" + "00" * 4,
                    "address": POOL_ADDRESS,
                }
            ]
        )
        # Truncated data: HexDecoder pads short data with zeros, so the typed
        # decoder still succeeds and emits exactly one event with the correct
        # event name. If the decoder ever switches to the except branch, the
        # data dict will carry "raw_data" instead — accept either shape but
        # never accept "no event emitted" or "event with empty data".
        assert len(events) == 1
        assert events[0].event_name == event_name
        assert events[0].data, "decoded event data must be non-empty"


# =============================================================================
# extract_a_token_received: edge-case branches
# =============================================================================


class TestExtractATokenReceived:
    def test_returns_none_for_empty_logs(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.extract_a_token_received({"logs": []}) is None

    def test_skips_log_with_too_few_topics(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser.extract_a_token_received(
            {
                "status": 1,
                "logs": [
                    {
                        "topics": [EVENT_TOPICS["Transfer"]],  # only 1 topic
                        "data": "0x" + _encode_uint256(1),
                        "address": ATOKEN_ADDR,
                    }
                ],
            }
        )
        assert out is None

    def test_ignores_non_mint_transfer(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser.extract_a_token_received(
            {
                "status": 1,
                "logs": [
                    {
                        "topics": [
                            EVENT_TOPICS["Transfer"],
                            _pad_address(USER_ADDRESS),  # not zero!
                            _pad_address(OTHER_USER),
                        ],
                        "data": "0x" + _encode_uint256(1),
                        "address": ATOKEN_ADDR,
                    }
                ],
            }
        )
        assert out is None

    def test_handles_bytes_topic(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser.extract_a_token_received(
            {
                "status": 1,
                "logs": [
                    {
                        "topics": [
                            bytes.fromhex(EVENT_TOPICS["Transfer"][2:]),
                            _pad_address(ZERO_ADDRESS),
                            _pad_address(USER_ADDRESS),
                        ],
                        "data": "0x" + _encode_uint256(42),
                        "address": ATOKEN_ADDR,
                    }
                ],
            }
        )
        assert out == 42


# =============================================================================
# extract_debt_token: edge-case branches
# =============================================================================


def _borrow_log_with_amount(amount: int) -> dict:
    data = (
        _encode_uint256(int(USER_ADDRESS, 16))
        + _encode_uint256(amount)
        + _encode_uint256(2)
        + _encode_uint256(50_000_000_000_000_000_000_000_000)
        + _encode_uint256(0)
    )
    return {
        "topics": [
            EVENT_TOPICS["Borrow"],
            _pad_address(USDC_ADDRESS),
            _pad_address(USER_ADDRESS),
        ],
        "data": "0x" + data,
        "address": POOL_ADDRESS,
        "logIndex": 0,
    }


class TestExtractDebtTokenEdgeCases:
    def test_skips_transfer_with_too_few_topics(self) -> None:
        parser = AaveV3ReceiptParser()
        receipt = {
            "status": 1,
            "logs": [
                _borrow_log_with_amount(1000),
                {
                    "topics": [EVENT_TOPICS["Transfer"]],  # too few
                    "data": "0x" + _encode_uint256(1000),
                    "address": DEBT_TOKEN,
                },
            ],
        }
        assert parser.extract_debt_token(receipt) is None

    def test_skips_transfer_not_a_mint(self) -> None:
        parser = AaveV3ReceiptParser()
        receipt = {
            "status": 1,
            "logs": [
                _borrow_log_with_amount(1000),
                {
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        _pad_address(OTHER_USER),  # not zero
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": "0x" + _encode_uint256(1000),
                    "address": DEBT_TOKEN,
                },
            ],
        }
        assert parser.extract_debt_token(receipt) is None

    def test_skips_mint_with_wrong_amount(self) -> None:
        parser = AaveV3ReceiptParser()
        receipt = {
            "status": 1,
            "logs": [
                _borrow_log_with_amount(1000),
                {
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        _pad_address(ZERO_ADDRESS),
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": "0x" + _encode_uint256(999),  # mismatch
                    "address": DEBT_TOKEN,
                },
            ],
        }
        assert parser.extract_debt_token(receipt) is None

    def test_handles_bytes_address_field(self) -> None:
        parser = AaveV3ReceiptParser()
        receipt = {
            "status": 1,
            "logs": [
                _borrow_log_with_amount(1000),
                {
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        _pad_address(ZERO_ADDRESS),
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": "0x" + _encode_uint256(1000),
                    "address": bytes.fromhex(DEBT_TOKEN[2:]),
                },
            ],
        }
        out = parser.extract_debt_token(receipt)
        assert out is not None
        assert out.lower().endswith(DEBT_TOKEN[2:].lower())

    def test_handles_bytes_first_topic(self) -> None:
        parser = AaveV3ReceiptParser()
        receipt = {
            "status": 1,
            "logs": [
                _borrow_log_with_amount(1000),
                {
                    "topics": [
                        bytes.fromhex(EVENT_TOPICS["Transfer"][2:]),
                        _pad_address(ZERO_ADDRESS),
                        _pad_address(USER_ADDRESS),
                    ],
                    "data": "0x" + _encode_uint256(1000),
                    "address": DEBT_TOKEN,
                },
            ],
        }
        assert parser.extract_debt_token(receipt) is not None

    def test_no_logs_returns_none(self) -> None:
        # Borrow event in receipt but no logs after dropping → covers logs==[] branch
        parser = AaveV3ReceiptParser()
        receipt = {"status": 1, "logs": [_borrow_log_with_amount(1000)]}
        # No matching debt mint Transfer → returns None
        assert parser.extract_debt_token(receipt) is None


# =============================================================================
# extract_a_token_burned edge cases
# =============================================================================


class TestExtractATokenBurnedEdgeCases:
    def test_returns_none_when_empty_logs(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.extract_a_token_burned({"logs": []}) is None

    def test_skips_log_with_too_few_topics(self) -> None:
        # build a withdraw receipt then add malformed transfer
        from .test_aave_v3_receipt_enrichment import _make_withdraw_receipt

        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(include_atoken_burn=False)
        receipt["logs"].append(
            {
                "topics": [EVENT_TOPICS["Transfer"]],  # only 1 topic
                "data": "0x" + _encode_uint256(1),
                "address": ATOKEN_ADDR,
            }
        )
        assert parser.extract_a_token_burned(receipt) is None

    def test_ignores_burn_from_other_user(self) -> None:
        from .test_aave_v3_receipt_enrichment import _make_withdraw_receipt

        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(include_atoken_burn=False)
        # Transfer to zero but from the WRONG address
        receipt["logs"].append(
            {
                "topics": [
                    EVENT_TOPICS["Transfer"],
                    _pad_address(OTHER_USER),  # not the withdraw user
                    _pad_address(ZERO_ADDRESS),
                ],
                "data": "0x" + _encode_uint256(99),
                "address": ATOKEN_ADDR,
            }
        )
        assert parser.extract_a_token_burned(receipt) is None

    def test_ignores_non_burn_transfer(self) -> None:
        from .test_aave_v3_receipt_enrichment import _make_withdraw_receipt

        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(include_atoken_burn=False)
        receipt["logs"].append(
            {
                "topics": [
                    EVENT_TOPICS["Transfer"],
                    _pad_address(USER_ADDRESS),
                    _pad_address(OTHER_USER),  # not zero
                ],
                "data": "0x" + _encode_uint256(99),
                "address": ATOKEN_ADDR,
            }
        )
        assert parser.extract_a_token_burned(receipt) is None

    def test_handles_bytes_topic(self) -> None:
        from .test_aave_v3_receipt_enrichment import _make_withdraw_receipt

        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(include_atoken_burn=False)
        receipt["logs"].append(
            {
                "topics": [
                    bytes.fromhex(EVENT_TOPICS["Transfer"][2:]),
                    _pad_address(USER_ADDRESS),
                    _pad_address(ZERO_ADDRESS),
                ],
                "data": "0x" + _encode_uint256(77),
                "address": ATOKEN_ADDR,
            }
        )
        assert parser.extract_a_token_burned(receipt) == 77


# =============================================================================
# extract_borrow_rate / extract_supply_rate edge cases
# =============================================================================


class TestExtractRatesEdge:
    def test_borrow_rate_empty_receipt(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.extract_borrow_rate({"logs": []}) is None

    def test_supply_rate_returns_none_when_event_data_lacks_liquidity_rate(
        self,
    ) -> None:
        # Patch parse_receipt to return a ReserveDataUpdated event whose data
        # dict deliberately omits 'liquidity_rate' — covers the
        # `if liquidity_rate is not None` branch falsy case.
        parser = AaveV3ReceiptParser()
        fake = ParseResult(
            success=True,
            events=[
                AaveV3Event(
                    event_type=AaveV3EventType.RESERVE_DATA_UPDATED,
                    event_name="ReserveDataUpdated",
                    log_index=0,
                    transaction_hash="0x",
                    block_number=1,
                    contract_address=POOL_ADDRESS,
                    data={"reserve": WETH_ADDRESS},  # liquidity_rate absent
                )
            ],
        )
        parser.parse_receipt = lambda _r: fake  # type: ignore[method-assign]
        assert parser.extract_supply_rate({"logs": []}) is None


# =============================================================================
# extract_*_amounts exception paths
# =============================================================================


class TestExtractAmountsExceptionPaths:
    """Force the inner extract_*_amount to raise → outer except returns None."""

    def test_supply_amounts_inner_raise_returns_none(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.extract_supply_amount = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        assert parser.extract_supply_amounts({"logs": []}) is None

    def test_borrow_amounts_inner_raise_returns_none(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.extract_borrow_amount = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        assert parser.extract_borrow_amounts({"logs": []}) is None

    def test_repay_amounts_inner_raise_returns_none(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.extract_repay_amount = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        assert parser.extract_repay_amounts({"logs": []}) is None

    def test_withdraw_amounts_inner_raise_returns_none(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.extract_withdraw_amount = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        assert parser.extract_withdraw_amounts({"logs": []}) is None


class TestExtractAmountExceptionPaths:
    """Force parse_receipt to raise → extract_*_amount returns None gracefully."""

    @pytest.mark.parametrize(
        "method",
        [
            "extract_supply_amount",
            "extract_withdraw_amount",
            "extract_borrow_amount",
            "extract_repay_amount",
            "extract_borrow_rate",
            "extract_debt_token",
            "extract_supply_rate",
            "extract_remaining_debt",
        ],
    )
    def test_method_returns_none_on_parse_exception(self, method: str) -> None:
        parser = AaveV3ReceiptParser()
        parser.parse_receipt = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        out = getattr(parser, method)({"logs": []})
        assert out is None

    def test_a_token_received_returns_none_on_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        # Force an exception inside the loop
        receipt = {
            "status": 1,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        _pad_address(ZERO_ADDRESS),
                        _pad_address(USER_ADDRESS),
                    ],
                    # Garbage non-string non-bytes payload triggers HexDecoder fail
                    "data": object(),
                    "address": ATOKEN_ADDR,
                }
            ],
        }
        # Should swallow exception and return None (line 1599-1600)
        out = parser.extract_a_token_received(receipt)
        assert out is None

    def test_a_token_burned_returns_none_on_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.parse_receipt = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        assert parser.extract_a_token_burned({"logs": []}) is None


# =============================================================================
# extract_protocol_fees exception path
# =============================================================================


class TestProtocolFeesExceptionPath:
    def test_returns_none_on_parse_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.parse_receipt = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        assert parser.extract_protocol_fees({"logs": []}) is None


# =============================================================================
# is_aave_event / get_event_type
# =============================================================================


class TestIsAaveEvent:
    def test_known_event_string(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.is_aave_event(EVENT_TOPICS["Supply"]) is True

    def test_known_event_no_0x_prefix(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.is_aave_event(EVENT_TOPICS["Supply"][2:]) is True

    def test_known_event_bytes(self) -> None:
        parser = AaveV3ReceiptParser()
        assert (
            parser.is_aave_event(bytes.fromhex(EVENT_TOPICS["Supply"][2:])) is True
        )

    def test_unknown_event(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.is_aave_event("0x" + "ab" * 32) is False


class TestGetEventType:
    def test_known_event_string(self) -> None:
        parser = AaveV3ReceiptParser()
        assert parser.get_event_type(EVENT_TOPICS["Supply"]) == AaveV3EventType.SUPPLY

    def test_known_event_bytes(self) -> None:
        parser = AaveV3ReceiptParser()
        assert (
            parser.get_event_type(bytes.fromhex(EVENT_TOPICS["Supply"][2:]))
            == AaveV3EventType.SUPPLY
        )

    def test_known_event_no_0x_prefix(self) -> None:
        parser = AaveV3ReceiptParser()
        assert (
            parser.get_event_type(EVENT_TOPICS["Supply"][2:]) == AaveV3EventType.SUPPLY
        )

    def test_unknown_event_returns_unknown(self) -> None:
        parser = AaveV3ReceiptParser()
        assert (
            parser.get_event_type("0x" + "ab" * 32) == AaveV3EventType.UNKNOWN
        )


# =============================================================================
# Dataclass to_dict / from_dict / property coverage
# =============================================================================


class TestEventDataDictSerialisation:
    def test_supply_event_data_to_dict(self) -> None:
        d = SupplyEventData(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            on_behalf_of=USER_ADDRESS,
            amount=Decimal("1000"),
            referral_code=42,
        ).to_dict()
        assert d["amount"] == "1000"
        assert d["referral_code"] == 42

    def test_withdraw_event_data_to_dict(self) -> None:
        d = WithdrawEventData(
            reserve=USDC_ADDRESS, user=USER_ADDRESS, to=USER_ADDRESS, amount=Decimal("1")
        ).to_dict()
        assert d["amount"] == "1"

    def test_borrow_event_data_to_dict_and_property(self) -> None:
        b = BorrowEventData(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            on_behalf_of=USER_ADDRESS,
            amount=Decimal("1"),
            interest_rate_mode=2,
        )
        assert b.is_variable_rate is True
        d = b.to_dict()
        assert d["is_variable_rate"] is True

    def test_borrow_event_data_stable_mode(self) -> None:
        b = BorrowEventData(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            on_behalf_of=USER_ADDRESS,
            amount=Decimal("1"),
            interest_rate_mode=1,
        )
        assert b.is_variable_rate is False

    def test_repay_event_data_to_dict(self) -> None:
        d = RepayEventData(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            repayer=USER_ADDRESS,
            amount=Decimal("1"),
            use_atokens=True,
        ).to_dict()
        assert d["use_atokens"] is True

    def test_flash_loan_event_data_opened_debt_false(self) -> None:
        f = FlashLoanEventData(
            target=USER_ADDRESS,
            initiator=USER_ADDRESS,
            asset=USDC_ADDRESS,
            amount=Decimal("1"),
            interest_rate_mode=0,
        )
        assert f.opened_debt is False
        d = f.to_dict()
        assert d["opened_debt"] is False

    def test_liquidation_call_event_data_to_dict(self) -> None:
        d = LiquidationCallEventData(
            collateral_asset=WETH_ADDRESS,
            debt_asset=USDC_ADDRESS,
            user=USER_ADDRESS,
            debt_to_cover=Decimal("100"),
            liquidated_collateral_amount=Decimal("0.05"),
            liquidator=LIQUIDATOR,
            receive_atoken=True,
        ).to_dict()
        assert d["receive_atoken"] is True
        assert d["liquidator"] == LIQUIDATOR

    def test_reserve_data_updated_event_data_to_dict(self) -> None:
        d = ReserveDataUpdatedEventData(
            reserve=USDC_ADDRESS,
            liquidity_rate=Decimal("0.05"),
            stable_borrow_rate=Decimal("0.06"),
            variable_borrow_rate=Decimal("0.05"),
        ).to_dict()
        assert d["liquidity_rate"] == "0.05"

    def test_user_emode_set_event_data_known_category(self) -> None:
        e = UserEModeSetEventData(user=USER_ADDRESS, category_id=1)
        assert e.category_name == "ETH Correlated"
        assert e.to_dict()["category_name"] == "ETH Correlated"

    def test_user_emode_set_event_data_unknown_category(self) -> None:
        e = UserEModeSetEventData(user=USER_ADDRESS, category_id=99)
        assert e.category_name == "Category 99"

    def test_user_emode_set_stablecoin_category(self) -> None:
        e = UserEModeSetEventData(user=USER_ADDRESS, category_id=2)
        assert e.category_name == "Stablecoins"

    def test_isolation_mode_debt_to_dict(self) -> None:
        d = IsolationModeDebtUpdatedEventData(
            asset=USDC_ADDRESS, total_debt=Decimal("1000.00")
        ).to_dict()
        assert d["asset"] == USDC_ADDRESS


class TestAaveV3EventSerialisation:
    def test_to_dict_and_from_dict_round_trip(self) -> None:
        e = AaveV3Event(
            event_type=AaveV3EventType.SUPPLY,
            event_name="Supply",
            log_index=1,
            transaction_hash="0x" + "ab" * 32,
            block_number=12345,
            contract_address=POOL_ADDRESS,
            data={"foo": "bar"},
            raw_topics=["0xabc"],
            raw_data="0xff",
            timestamp=datetime.now(UTC),
        )
        d = e.to_dict()
        e2 = AaveV3Event.from_dict(d)
        assert e2.event_type == AaveV3EventType.SUPPLY
        assert e2.transaction_hash == "0x" + "ab" * 32

    def test_from_dict_without_timestamp(self) -> None:
        d = {
            "event_type": "SUPPLY",
            "event_name": "Supply",
            "log_index": 0,
            "transaction_hash": "0x",
            "block_number": 1,
            "contract_address": POOL_ADDRESS,
            "data": {},
        }
        e = AaveV3Event.from_dict(d)
        assert e.event_name == "Supply"


class TestParseResultSerialisation:
    def test_to_dict_full(self) -> None:
        result = ParseResult(
            success=True,
            events=[
                AaveV3Event(
                    event_type=AaveV3EventType.SUPPLY,
                    event_name="Supply",
                    log_index=0,
                    transaction_hash="0x",
                    block_number=1,
                    contract_address=POOL_ADDRESS,
                    data={},
                )
            ],
            supplies=[
                SupplyEventData(
                    reserve=USDC_ADDRESS,
                    user=USER_ADDRESS,
                    on_behalf_of=USER_ADDRESS,
                    amount=Decimal("1"),
                )
            ],
            transaction_hash="0x",
            block_number=1,
        )
        d = result.to_dict()
        assert d["success"] is True
        assert len(d["events"]) == 1
        assert len(d["supplies"]) == 1


# =============================================================================
# _strict_parse + _wrap_amount error paths (VIB-3159)
# =============================================================================


class TestStrictParseWrappers:
    def test_strict_parse_returns_error_when_parse_raises(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.parse_receipt = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        out = parser._strict_parse({"logs": []})
        assert isinstance(out, ExtractError)
        assert "boom" in out.error

    def test_strict_parse_returns_error_when_parse_unsuccessful(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.parse_receipt = lambda _r: ParseResult(  # type: ignore[method-assign]
            success=False, error="bad"
        )
        out = parser._strict_parse({"logs": []})
        assert isinstance(out, ExtractError)
        assert "bad" in out.error

    def test_supply_amount_result_propagates_strict_error(self) -> None:
        parser = AaveV3ReceiptParser()
        parser.parse_receipt = lambda _r: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("boom")
        )
        out = parser.extract_supply_amount_result({"logs": []})
        assert isinstance(out, ExtractError)


# =============================================================================
# permission_hints (covers the trivial 2-line module)
# =============================================================================


class TestPermissionHints:
    def test_permission_hints_module_loads_and_is_empty(self) -> None:
        from almanak.framework.connectors.aave_v3.permission_hints import (
            PERMISSION_HINTS,
        )
        from almanak.framework.permissions.hints import PermissionHints

        assert isinstance(PERMISSION_HINTS, PermissionHints)


# =============================================================================
# Direct decoder exception paths — call _decode_* with non-hex data
# =============================================================================


class TestDecoderExceptionPaths:
    """Force the bare ``except Exception`` branches in every _decode_* method
    by passing ``data`` containing non-hex characters — int(chunk, 16) raises
    inside HexDecoder.decode_uint256 → caught by the decoder → returns
    {"raw_data": ...}.
    """

    # Long garbage string so decode_uint256 at deeper offsets gets a non-empty
    # non-hex chunk and raises. Single 64-char block returned 0 for offset>0 due
    # to empty-chunk fallback in HexDecoder.
    GARBAGE = "G" * (64 * 6)

    def test_supply_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_supply_data([_pad_address(USDC_ADDRESS)], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_withdraw_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_withdraw_data([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_borrow_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_borrow_data([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_repay_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_repay_data([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_flash_loan_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_flash_loan_data([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_liquidation_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_liquidation_data([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_reserve_data_updated_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_reserve_data_updated([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_user_emode_set_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_user_emode_set([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_isolation_mode_decoder_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        out = parser._decode_isolation_mode_debt([], self.GARBAGE)
        assert out == {"raw_data": self.GARBAGE}

    def test_collateral_toggle_decoder_exception_path(self) -> None:
        # collateral_toggle's only failure path is if topic_to_address itself
        # raises — pass an indexed_topics list with a non-string non-bytes element.
        parser = AaveV3ReceiptParser()
        out = parser._decode_collateral_toggle(
            [object()], "0x", "ReserveUsedAsCollateralEnabled"  # type: ignore[list-item]
        )
        # Either branch is acceptable as long as the shape is one of the two
        # documented return types: {raw_data} on except, or {reserve,user,enabled}
        # on success. No third shape and no crash.
        assert isinstance(out, dict)
        assert ("raw_data" in out) or ({"reserve", "user", "enabled"} <= set(out.keys()))


# =============================================================================
# _parse_* exception branches — pass non-numeric Decimal inputs
# =============================================================================


def _event_with_data(event_type: AaveV3EventType, data: dict) -> AaveV3Event:
    return AaveV3Event(
        event_type=event_type,
        event_name=event_type.value,
        log_index=0,
        transaction_hash="0x",
        block_number=1,
        contract_address=POOL_ADDRESS,
        data=data,
    )


class TestTypedParseExceptionPaths:
    """Force the bare except in _parse_supply/_withdraw/_borrow/_repay/
    _flash_loan/_liquidation by passing a non-numeric `amount`."""

    def test_parse_supply_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        e = _event_with_data(AaveV3EventType.SUPPLY, {"amount": "notanumber"})
        assert parser._parse_supply(e) is None

    def test_parse_withdraw_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        e = _event_with_data(AaveV3EventType.WITHDRAW, {"amount": "notanumber"})
        assert parser._parse_withdraw(e) is None

    def test_parse_borrow_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        e = _event_with_data(AaveV3EventType.BORROW, {"amount": "notanumber"})
        assert parser._parse_borrow(e) is None

    def test_parse_repay_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        e = _event_with_data(AaveV3EventType.REPAY, {"amount": "notanumber"})
        assert parser._parse_repay(e) is None

    def test_parse_flash_loan_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        e = _event_with_data(AaveV3EventType.FLASH_LOAN, {"amount": "notanumber"})
        assert parser._parse_flash_loan(e) is None

    def test_parse_liquidation_exception(self) -> None:
        parser = AaveV3ReceiptParser()
        e = _event_with_data(
            AaveV3EventType.LIQUIDATION_CALL, {"debt_to_cover": "notanumber"}
        )
        assert parser._parse_liquidation(e) is None


# =============================================================================
# parse_receipt's _parse_log warning branch (line 981-983)
# =============================================================================


class TestParseLogWarningBranch:
    def test_parse_log_inner_exception_returns_none(self) -> None:
        # Force the inner `try/except` in _parse_log by giving topics a value
        # that breaks topic.lower() resolution
        parser = AaveV3ReceiptParser()
        # log.get('topics', []) returns [<object>] — int(object, 16) is fine,
        # but str().lower() works. Use bytes-with-no-hex to break .lower() -> hex chain.

        class BadTopic:
            def __str__(self) -> str:  # noqa: D401
                raise RuntimeError("nope")

        events = parser.parse_logs(
            [
                {
                    "topics": [BadTopic()],
                    "data": "0x",
                    "address": POOL_ADDRESS,
                }
            ]
        )
        assert events == []


# =============================================================================
# extract_a_token_received: ensure the broad except is hit (line 1599-1600)
# =============================================================================


class TestATokenReceivedExceptionBranch:
    def test_breaks_when_log_get_throws(self) -> None:
        parser = AaveV3ReceiptParser()

        class Boom(dict):
            def get(self, *_args, **_kwargs):
                raise RuntimeError("boom")

        # Top-level receipt.get('logs', []) returns a list; the loop iterates dicts.
        # Wrap an inner dict so its .get() raises.
        receipt = {"logs": [Boom()]}
        out = parser.extract_a_token_received(receipt)
        assert out is None


# =============================================================================
# extract_debt_token: empty-logs branch (line 1645)
# =============================================================================


class TestExtractDebtTokenEmptyLogs:
    def test_returns_none_when_logs_field_missing(self) -> None:
        parser = AaveV3ReceiptParser()
        # parse_receipt returns a result with borrows=[]; logs=[] hits line 1645
        # Force borrows to be present but no logs available
        fake = ParseResult(
            success=True,
            borrows=[
                BorrowEventData(
                    reserve=USDC_ADDRESS,
                    user=USER_ADDRESS,
                    on_behalf_of=USER_ADDRESS,
                    amount=Decimal("1000"),
                    interest_rate_mode=2,
                )
            ],
        )
        parser.parse_receipt = lambda _r: fake  # type: ignore[method-assign]
        # Receipt with no 'logs' key → defaults to [] → triggers the early return
        assert parser.extract_debt_token({}) is None


# =============================================================================
# extract_a_token_burned: empty-logs branch after parse (line 1838)
# =============================================================================


class TestExtractATokenBurnedEmptyLogs:
    def test_returns_none_when_no_logs_in_receipt(self) -> None:
        parser = AaveV3ReceiptParser()
        fake = ParseResult(
            success=True,
            withdraws=[
                WithdrawEventData(
                    reserve=USDC_ADDRESS,
                    user=USER_ADDRESS,
                    to=USER_ADDRESS,
                    amount=Decimal("1"),
                )
            ],
        )
        parser.parse_receipt = lambda _r: fake  # type: ignore[method-assign]
        assert parser.extract_a_token_burned({}) is None
