"""Branch coverage for CopyReplayRunner._parse_payload.

_parse_payload is a pure shape-dispatch parser (no self state), so the runner is
constructed via object.__new__ without a CopyTradingConfigV2 — precedent:
tests/unit/agent_tools/test_pm_tools.py exercising logic without heavy wiring.

Covered branches:
- non-dict payload passthrough (None, str, list, int)
- SWAP: all fields present / all absent, effective_price truthiness gate
  (0 -> None), slippage_bps is-not-None gate (0 -> 0)
- LP_OPEN / LP_CLOSE: every optional ternary on both sides
- SUPPLY / WITHDRAW / BORROW / REPAY: amount ternary on both sides
- PERP_OPEN / PERP_CLOSE: every optional ternary on both sides
- unknown / empty action_type dict passthrough
"""

from decimal import Decimal

import pytest

from almanak.framework.services.copy_trading import (
    LendingPayload,
    LPPayload,
    PerpPayload,
    SwapPayload,
)
from almanak.framework.testing.copy_replay import CopyReplayRunner


@pytest.fixture(scope="module")
def runner() -> CopyReplayRunner:
    # _parse_payload never touches instance attributes; skip __init__ wiring.
    return object.__new__(CopyReplayRunner)


class TestNonDictPassthrough:
    @pytest.mark.parametrize(
        "payload",
        [None, "raw-string", ["a", "b"], 42, Decimal("1.5")],
        ids=["none", "str", "list", "int", "decimal"],
    )
    def test_non_dict_returned_unchanged(self, runner, payload):
        assert runner._parse_payload("SWAP", payload) is payload

    def test_non_dict_ignores_action_type(self, runner):
        assert runner._parse_payload("UNKNOWN", None) is None


class TestSwapPayload:
    def test_full_fields(self, runner):
        result = runner._parse_payload(
            "SWAP",
            {
                "token_in": "WETH",
                "token_out": "USDC",
                "amount_in": "1.5",
                "amount_out": 3000,
                "effective_price": "2000.5",
                "slippage_bps": "25",
            },
        )
        assert result == SwapPayload(
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("1.5"),
            amount_out=Decimal("3000"),
            effective_price=Decimal("2000.5"),
            slippage_bps=25,
        )

    def test_empty_dict_defaults(self, runner):
        result = runner._parse_payload("SWAP", {})
        assert result == SwapPayload(
            token_in="",
            token_out="",
            amount_in=Decimal("0"),
            amount_out=Decimal("0"),
            effective_price=None,
            slippage_bps=None,
        )

    def test_effective_price_zero_is_truthiness_dropped(self, runner):
        # effective_price uses a truthiness gate, so 0 collapses to None
        # (unlike slippage_bps, which uses an `is not None` gate).
        result = runner._parse_payload("SWAP", {"effective_price": 0})
        assert result.effective_price is None

    def test_slippage_bps_zero_preserved(self, runner):
        result = runner._parse_payload("SWAP", {"slippage_bps": 0})
        assert result.slippage_bps == 0

    def test_slippage_bps_none_explicit(self, runner):
        result = runner._parse_payload("SWAP", {"slippage_bps": None})
        assert result.slippage_bps is None


class TestLPPayload:
    @pytest.mark.parametrize("action_type", ["LP_OPEN", "LP_CLOSE"])
    def test_full_fields(self, runner, action_type):
        result = runner._parse_payload(
            action_type,
            {
                "pool": "0xpool",
                "position_id": 12345,
                "amount0": "1.0",
                "amount1": "2000",
                "range_lower": "1500.5",
                "range_upper": "2500",
                "close_fraction": "0.5",
            },
        )
        assert result == LPPayload(
            pool="0xpool",
            position_id="12345",
            amount0=Decimal("1.0"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1500.5"),
            range_upper=Decimal("2500"),
            close_fraction=Decimal("0.5"),
        )

    def test_empty_dict_all_none(self, runner):
        result = runner._parse_payload("LP_OPEN", {})
        assert result == LPPayload(
            pool=None,
            position_id=None,
            amount0=None,
            amount1=None,
            range_lower=None,
            range_upper=None,
            close_fraction=None,
        )

    def test_position_id_zero_stringified(self, runner):
        # `is not None` gate: numeric 0 survives and is coerced to "0".
        result = runner._parse_payload("LP_CLOSE", {"position_id": 0})
        assert result.position_id == "0"

    def test_amount_zero_preserved(self, runner):
        result = runner._parse_payload("LP_OPEN", {"amount0": 0, "amount1": "0"})
        assert result.amount0 == Decimal("0")
        assert result.amount1 == Decimal("0")


class TestLendingPayload:
    @pytest.mark.parametrize("action_type", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
    def test_full_fields(self, runner, action_type):
        result = runner._parse_payload(
            action_type,
            {
                "token": "USDC",
                "amount": "100.25",
                "collateral_token": "WETH",
                "borrow_token": "USDC",
                "market_id": "aave-v3-arbitrum",
                "use_as_collateral": True,
            },
        )
        assert result == LendingPayload(
            token="USDC",
            amount=Decimal("100.25"),
            collateral_token="WETH",
            borrow_token="USDC",
            market_id="aave-v3-arbitrum",
            use_as_collateral=True,
        )

    def test_empty_dict_all_none(self, runner):
        result = runner._parse_payload("SUPPLY", {})
        assert result == LendingPayload(
            token=None,
            amount=None,
            collateral_token=None,
            borrow_token=None,
            market_id=None,
            use_as_collateral=None,
        )

    def test_amount_zero_preserved(self, runner):
        result = runner._parse_payload("REPAY", {"amount": 0})
        assert result.amount == Decimal("0")


class TestPerpPayload:
    @pytest.mark.parametrize("action_type", ["PERP_OPEN", "PERP_CLOSE"])
    def test_full_fields(self, runner, action_type):
        result = runner._parse_payload(
            action_type,
            {
                "market": "ETH-USD",
                "collateral_token": "USDC",
                "collateral_amount": "500",
                "size_usd": "2500.75",
                "is_long": False,
                "leverage": "5",
                "position_id": 987,
            },
        )
        assert result == PerpPayload(
            market="ETH-USD",
            collateral_token="USDC",
            collateral_amount=Decimal("500"),
            size_usd=Decimal("2500.75"),
            is_long=False,
            leverage=Decimal("5"),
            position_id="987",
        )

    def test_empty_dict_all_none(self, runner):
        result = runner._parse_payload("PERP_OPEN", {})
        assert result == PerpPayload(
            market=None,
            collateral_token=None,
            collateral_amount=None,
            size_usd=None,
            is_long=None,
            leverage=None,
            position_id=None,
        )

    def test_zero_values_preserved(self, runner):
        result = runner._parse_payload(
            "PERP_CLOSE",
            {"collateral_amount": 0, "size_usd": 0, "leverage": 0, "position_id": 0},
        )
        assert result.collateral_amount == Decimal("0")
        assert result.size_usd == Decimal("0")
        assert result.leverage == Decimal("0")
        assert result.position_id == "0"


class TestUnknownActionType:
    @pytest.mark.parametrize("action_type", ["", "TRANSFER", "swap"])
    def test_dict_returned_unchanged(self, runner, action_type):
        # Falls through every dispatch arm (matching is exact-case: "swap"
        # lowercase does not match the "SWAP" arm) and returns the same object.
        payload = {"anything": 1}
        assert runner._parse_payload(action_type, payload) is payload
