"""Tests for CopyIntentBuilder."""

from decimal import Decimal

from almanak.framework.services.copy_intent_builder import CopyIntentBuilder
from almanak.framework.services.copy_trading_models import (
    CopySignal,
    CopyTradingConfigV2,
    LendingPayload,
    LPPayload,
    SwapPayload,
)


def _make_config() -> CopyTradingConfigV2:
    return CopyTradingConfigV2.from_config(
        {
            "leaders": [{"address": "0x489ee077994B6658eFaCA1507F1FBB620B9308aa", "chain": "arbitrum"}],
            "sizing": {"mode": "fixed_usd", "fixed_usd": "100"},
            "risk": {
                "max_trade_usd": "1000",
                "min_trade_usd": "10",
                "max_daily_notional_usd": "10000",
                "max_open_positions": 10,
                "max_slippage": "0.01",
            },
        }
    )


def test_build_swap_intent() -> None:
    builder = CopyIntentBuilder(_make_config())
    signal = CopySignal(
        event_id="arbitrum:0xabc:0",
        signal_id="sig-swap",
        action_type="SWAP",
        protocol="uniswap_v3",
        chain="arbitrum",
        tokens=["USDC", "WETH"],
        amounts={"USDC": Decimal("100")},
        amounts_usd={"USDC": Decimal("100")},
        metadata={"notional_usd": "100"},
        leader_address="0x489ee077994B6658eFaCA1507F1FBB620B9308aa",
        block_number=1,
        timestamp=1,
        action_payload=SwapPayload(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            amount_out=Decimal("0.05"),
        ),
    )

    result = builder.build(signal)
    assert result.intent is not None
    assert result.intent.intent_type.value == "SWAP"


def test_lp_close_missing_position_returns_reason() -> None:
    builder = CopyIntentBuilder(_make_config())
    signal = CopySignal(
        event_id="arbitrum:0xabc:1",
        signal_id="sig-lp-close",
        action_type="LP_CLOSE",
        protocol="uniswap_v3",
        chain="arbitrum",
        tokens=[],
        amounts={},
        amounts_usd={},
        metadata={},
        leader_address="0x489ee077994B6658eFaCA1507F1FBB620B9308aa",
        block_number=1,
        timestamp=1,
        action_payload=LPPayload(pool="0xpool"),
    )

    result = builder.build(signal)
    assert result.intent is None
    assert result.reason_code == "lp_position_missing"


def test_supply_payload_maps_to_supply_intent() -> None:
    builder = CopyIntentBuilder(_make_config())
    signal = CopySignal(
        event_id="arbitrum:0xabc:2",
        signal_id="sig-supply",
        action_type="SUPPLY",
        protocol="aave_v3",
        chain="arbitrum",
        tokens=["USDC"],
        amounts={"USDC": Decimal("100")},
        amounts_usd={"USDC": Decimal("100")},
        metadata={"notional_usd": "100"},
        leader_address="0x489ee077994B6658eFaCA1507F1FBB620B9308aa",
        block_number=1,
        timestamp=1,
        action_payload=LendingPayload(token="USDC", amount=Decimal("100"), market_id="0xpool"),
    )

    result = builder.build(signal)
    assert result.intent is not None
    assert result.intent.intent_type.value == "SUPPLY"
