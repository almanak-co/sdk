"""Characterization coverage for PnL intent extraction helpers."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.intent_extraction import (
    get_intent_amount_usd,
    get_intent_protocol,
    get_intent_tokens,
    get_intent_type,
)


class _IntentWithType:
    def __init__(self, intent_type: Any) -> None:
        self.intent_type = intent_type


class _ExternalIntentType:
    def __init__(self, value: str) -> None:
        self.value = value


class _StringFallbackIntentType:
    value = "NOT_AN_INTENT_TYPE"

    def __str__(self) -> str:
        return "REPAY"


def _market_state() -> MarketState:
    return MarketState(
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
        chain="arbitrum",
    )


@pytest.mark.parametrize(
    ("intent_type_value", "expected"),
    [
        (IntentType.SWAP, IntentType.SWAP),
        ("LP_OPEN", IntentType.LP_OPEN),
        (_ExternalIntentType("VAULT_REDEEM"), IntentType.VAULT_REDEEM),
        (_StringFallbackIntentType(), IntentType.REPAY),
    ],
)
def test_get_intent_type_prefers_intent_type_attribute(intent_type_value: Any, expected: IntentType) -> None:
    assert get_intent_type(_IntentWithType(intent_type_value)) == expected


@pytest.mark.parametrize(
    ("class_name", "expected"),
    [
        ("SwapIntent", IntentType.SWAP),
        ("LpOpenIntent", IntentType.LP_OPEN),
        ("LpCloseIntent", IntentType.LP_CLOSE),
        ("PerpOpenIntent", IntentType.PERP_OPEN),
        ("PerpCloseIntent", IntentType.PERP_CLOSE),
        ("SupplyIntent", IntentType.SUPPLY),
        ("WithdrawIntent", IntentType.WITHDRAW),
        ("BorrowIntent", IntentType.BORROW),
        ("RepayIntent", IntentType.REPAY),
        ("BridgeIntent", IntentType.BRIDGE),
        ("VaultDepositIntent", IntentType.VAULT_DEPOSIT),
        ("VaultRedeemIntent", IntentType.VAULT_REDEEM),
        ("HoldIntent", IntentType.HOLD),
        ("NoopIntent", IntentType.UNKNOWN),
    ],
)
def test_get_intent_type_falls_back_to_class_name(class_name: str, expected: IntentType) -> None:
    intent = type(class_name, (), {})()

    assert get_intent_type(intent) == expected


def test_get_intent_type_uses_class_name_after_invalid_attribute() -> None:
    intent = type("BorrowIntent", (), {"intent_type": "NOT_AN_INTENT_TYPE"})()

    assert get_intent_type(intent) == IntentType.BORROW


def test_get_intent_type_none_is_unknown() -> None:
    assert get_intent_type(None) == IntentType.UNKNOWN


@pytest.mark.parametrize("attr", ["protocol", "protocol_name", "connector", "adapter"])
def test_get_intent_protocol_uses_declared_protocol_attributes(attr: str) -> None:
    intent = type("CustomIntent", (), {attr: "UniSwap_V3"})()

    assert get_intent_protocol(intent) == "uniswap_v3"


@pytest.mark.parametrize(
    ("class_name", "expected"),
    [
        ("UniswapSwapIntent", "uniswap_v3"),
        ("GmxPerpOpenIntent", "gmx"),
        ("AaveSupplyIntent", "aave_v3"),
        ("HyperliquidPerpOpenIntent", "hyperliquid"),
        ("AcrossBridgeIntent", "bridge"),
        ("StargateBridgeIntent", "bridge"),
        ("PlainIntent", "default"),
    ],
)
def test_get_intent_protocol_falls_back_to_class_name(class_name: str, expected: str) -> None:
    intent = type(class_name, (), {})()

    assert get_intent_protocol(intent) == expected


def test_get_intent_amount_usd_direct_amount_wins_and_tracks_no_fallback() -> None:
    tracked: list[str] = []
    intent = SimpleNamespace(
        amount_usd=Decimal("0"),
        size_usd=Decimal("5000"),
        collateral_usd=Decimal("1000"),
    )

    assert get_intent_amount_usd(intent, _market_state(), track_fallback=tracked.append) == Decimal("0")
    assert tracked == []


def test_get_intent_amount_usd_size_wins_over_collateral() -> None:
    intent = SimpleNamespace(size_usd=Decimal("5000"), collateral_usd=Decimal("1000"))

    assert get_intent_amount_usd(intent, _market_state()) == Decimal("5000")


def test_get_intent_amount_usd_generic_skips_all_and_malformed_amounts() -> None:
    intent = SimpleNamespace(amount="all", amount_in="not-a-number", amount_out=Decimal("2"), from_token="WETH")

    assert get_intent_amount_usd(intent, _market_state()) == Decimal("4000")


def test_get_intent_amount_usd_missing_generic_price_tracks_once() -> None:
    tracked: list[str] = []
    intent = SimpleNamespace(amount=Decimal("2"), from_token="ARB")

    assert get_intent_amount_usd(intent, _market_state(), track_fallback=tracked.append) == Decimal("0")
    assert tracked == ["default_usd_amount"]


def test_get_intent_amount_usd_zero_amount_without_token_tracks_fallback() -> None:
    tracked: list[str] = []
    intent = SimpleNamespace(amount=Decimal("0"))

    assert get_intent_amount_usd(intent, _market_state(), track_fallback=tracked.append) == Decimal("0")
    assert tracked == ["default_usd_amount"]


def test_get_intent_tokens_deduplicates_case_insensitive_market_collateral() -> None:
    intent = type("PerpIntent", (), {"market": "ETH/USD", "collateral_token": "usdc"})()

    assert get_intent_tokens(intent) == ["ETH", "USDC"]


def test_get_intent_tokens_deduplicates_case_insensitive_lp_aliases() -> None:
    intent = type(
        "LpIntent",
        (),
        {
            "token0": "WETH",
            "token1": "USDC",
            "token_a": "weth",
            "token_b": "usdc",
        },
    )()

    assert get_intent_tokens(intent) == ["WETH", "USDC"]
