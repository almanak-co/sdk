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


class TestAmountAllSentinel:
    """amount="all" has no backtest sizing lane: the extraction returns a
    deterministic $0 placeholder (no warning, no price lookup) and the engine
    rejects the intent with UNSUPPORTED_ALL_SIZING_REASON (ALM-2943 owns the
    typed sizing that would make these executable)."""

    def _sell_all_intent(self) -> Any:
        return SimpleNamespace(intent_type=IntentType.SWAP, from_token="WETH", to_token="USDC", amount="all")

    def test_swap_all_is_deterministic_zero_placeholder(self) -> None:
        assert get_intent_amount_usd(self._sell_all_intent(), _market_state()) == Decimal("0")

    def test_swap_all_is_zero_in_strict_mode_too(self) -> None:
        # The rejection is deterministic; strict mode must not raise over
        # price data a rejected intent does not need.
        amount = get_intent_amount_usd(self._sell_all_intent(), _market_state(), strict_reproducibility=True)
        assert amount == Decimal("0")

    def test_amountless_close_placeholder_applies_in_strict_mode(self) -> None:
        # Position-close resolution sizes these deterministically from the
        # matched position, so strict mode must not raise before it runs.
        for intent_type, field in (
            (IntentType.WITHDRAW, "token"),
            (IntentType.REPAY, "token"),
            (IntentType.PERP_CLOSE, "market"),
        ):
            intent = SimpleNamespace(intent_type=intent_type, **{field: "WETH"}, amount=None)
            assert get_intent_amount_usd(intent, _market_state(), strict_reproducibility=True) == Decimal("0")

    def test_fail_closed_category_only_covers_generic_lane_types(self) -> None:
        # Every type in the fail-closed category must be one the generic
        # engine lane actually simulates — otherwise the run dies with
        # UnsupportedIntentError upstream and the promised rejected-trade
        # blotter record can never be built (BRIDGE is the excluded case:
        # refused wholesale for ANY amount, not just "all").
        from almanak.framework.backtesting.pnl._engine_helpers import GENERIC_SIMULATED_INTENT_TYPES
        from almanak.framework.backtesting.pnl.intent_extraction import WALLET_BALANCE_ALL_INTENT_TYPES

        assert WALLET_BALANCE_ALL_INTENT_TYPES <= GENERIC_SIMULATED_INTENT_TYPES
        assert IntentType.BRIDGE not in WALLET_BALANCE_ALL_INTENT_TYPES

    def test_withdraw_all_is_not_sized_from_wallet_balance(self) -> None:
        # WITHDRAW "all" is a PROTOCOL_SUPPLY resolution in live execution;
        # wallet holdings are the wrong category, so the resolver must not bite.
        withdraw = SimpleNamespace(intent_type=IntentType.WITHDRAW, token="WETH", amount="all")
        amount_usd = get_intent_amount_usd(withdraw, _market_state())
        assert amount_usd == Decimal("0")

    def test_explicit_amount_still_wins_over_resolver(self) -> None:
        intent = SimpleNamespace(intent_type=IntentType.SWAP, from_token="WETH", to_token="USDC", amount="1.5")
        amount_usd = get_intent_amount_usd(intent, _market_state())
        assert amount_usd == Decimal("3000")


class TestLpClosePositionIdAddressCarrier:
    """An address-shaped position_id is a valid pool carrier for fungible LP."""

    def test_address_in_position_id_matches_position_metadata(self) -> None:
        from datetime import UTC, datetime

        from almanak.framework.backtesting.pnl.intent_extraction import find_lp_close_position_id

        pool_address = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"
        position = SimpleNamespace(
            is_lp=True,
            position_id="LP_curve_USDC_USDT_1700000000",
            protocol="curve",
            tokens=["USDC", "USDT"],
            entry_time=datetime(2024, 1, 1, tzinfo=UTC),
            metadata={"pool_address": pool_address},
        )
        close = SimpleNamespace(position_id=pool_address, pool=None, protocol="curve")

        assert find_lp_close_position_id(close, [position]) == "LP_curve_USDC_USDT_1700000000"

    def test_mixed_case_address_carrier_still_matches(self) -> None:
        # Checksummed close id vs lowercase open-stamped metadata: both sides
        # normalize, the match must not be case-sensitive.
        from datetime import UTC, datetime

        from almanak.framework.backtesting.pnl.intent_extraction import find_lp_close_position_id

        position = SimpleNamespace(
            is_lp=True,
            position_id="LP_curve_USDC_USDT_1700000000",
            protocol="curve",
            tokens=["USDC", "USDT"],
            entry_time=datetime(2024, 1, 1, tzinfo=UTC),
            metadata={"pool_address": "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"},
        )
        close = SimpleNamespace(position_id="0xB2cC224c1c9feE385f8ad6a55b4d94E92359DC59", pool=None, protocol="curve")

        assert find_lp_close_position_id(close, [position]) == "LP_curve_USDC_USDT_1700000000"

    def test_multiple_same_pool_positions_close_fifo_oldest(self) -> None:
        # Several open positions on the identical pool address: the OLDEST
        # entry_time wins (FIFO), matching the documented contract.
        from datetime import UTC, datetime

        from almanak.framework.backtesting.pnl.intent_extraction import find_lp_close_position_id

        pool_address = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"

        def _position(position_id: str, day: int) -> SimpleNamespace:
            return SimpleNamespace(
                is_lp=True,
                position_id=position_id,
                protocol="curve",
                tokens=["USDC", "USDT"],
                entry_time=datetime(2024, 1, day, tzinfo=UTC),
                metadata={"pool_address": pool_address},
            )

        newer = _position("LP_curve_USDC_USDT_newer", 5)
        oldest = _position("LP_curve_USDC_USDT_oldest", 1)
        close = SimpleNamespace(position_id=pool_address, pool=None, protocol="curve")

        # Listed newest-first to prove the sort, not the input order, decides.
        assert find_lp_close_position_id(close, [newer, oldest]) == "LP_curve_USDC_USDT_oldest"


class TestWalletBalanceCategoryParity:
    """Cross-layer guard: the backtest's wallet-balance-sized set tracks the
    live resolver's WALLET_BALANCE category, with every divergence named.

    The two constants live in different layers with no shared owner (ALM-2943
    phase 1 replaces both with one resolver; this test is deleted with them).
    Until then, a new WALLET_BALANCE intent type added live must fail here and
    force an explicit backtest decision - not silently $0-placeholder or fall
    through to an unhandled lane.
    """

    # Live sizes these from wallet balance but the backtest deliberately does
    # not: BRIDGE is refused wholesale by the generic lane (any amount, not
    # just "all"); STAKE has no backtest IntentType or engine lane at all
    # (yield family is an ALM-2940 decision). WRAP/UNWRAP_NATIVE left this
    # set when they gained a generic simulation lane with wallet-"all" sizing.
    DOCUMENTED_BACKTEST_EXCLUSIONS = frozenset({"BRIDGE", "STAKE"})

    def test_backtest_set_is_live_category_minus_documented_exclusions(self) -> None:
        from almanak.framework.backtesting.pnl.intent_extraction import WALLET_BALANCE_ALL_INTENT_TYPES
        from almanak.framework.intents.amount_resolver import (
            _INTENT_TYPE_TO_CATEGORY,
            AmountResolutionCategory,
        )

        live = {
            name for name, cat in _INTENT_TYPE_TO_CATEGORY.items() if cat is AmountResolutionCategory.WALLET_BALANCE
        }
        backtest = {t.name for t in WALLET_BALANCE_ALL_INTENT_TYPES}
        assert backtest | self.DOCUMENTED_BACKTEST_EXCLUSIONS == live
        assert backtest & self.DOCUMENTED_BACKTEST_EXCLUSIONS == set()

    def test_exclusions_are_actually_outside_the_generic_lane(self) -> None:
        # Each documented exclusion must remain un-simulated; if an engine lane
        # ever appears for one, this forces the exclusion list (and the fate of
        # its "all" sizing) to be revisited rather than staying stale.
        from almanak.framework.backtesting.pnl._engine_helpers import GENERIC_SIMULATED_INTENT_TYPES

        simulated = {t.name for t in GENERIC_SIMULATED_INTENT_TYPES}
        assert self.DOCUMENTED_BACKTEST_EXCLUSIONS & simulated == set()
