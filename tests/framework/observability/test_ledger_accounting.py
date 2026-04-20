"""Tests for extracted_data_json serialization in the transaction ledger.

Validates Phase 1b of the Dashboard Accounting PRD:
- Type-tagged serialization preserves types across round-trips
- All extracted data dataclasses (SwapAmounts, LPOpenData, PerpData, etc.)
  serialize and deserialize correctly
- Multi-tx bundle tx_hashes are captured in extracted_data_json
"""

from decimal import Decimal

import pytest

from almanak.framework.execution.extracted_data import (
    BorrowData,
    LPCloseData,
    LPOpenData,
    PerpData,
    StakeData,
    SupplyData,
    SwapAmounts,
)
from almanak.framework.observability.ledger import (
    deserialize_extracted_data,
    serialize_extracted_data,
)


class TestSerializeExtractedData:
    """Test type-tagged serialization of extracted data."""

    def test_empty_dict(self):
        result = serialize_extracted_data({})
        assert result == "{}"

    def test_swap_amounts_round_trip(self):
        original = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1000.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("2000.0"),
            slippage_bps=15,
            token_in="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            token_out="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        )

        json_str = serialize_extracted_data({"swap_amounts": original})
        assert json_str  # non-empty

        restored = deserialize_extracted_data(json_str)
        assert "swap_amounts" in restored
        sa = restored["swap_amounts"]
        assert isinstance(sa, SwapAmounts)
        assert sa.amount_in == 1000000
        assert sa.amount_out == 500000000000000000
        assert sa.effective_price == Decimal("2000.0")
        assert sa.slippage_bps == 15

    def test_lp_open_data_round_trip(self):
        original = LPOpenData(
            position_id=12345,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=1000000000,
            amount0=500000,
            amount1=250000000000000000,
        )

        json_str = serialize_extracted_data({"lp_open": original})
        restored = deserialize_extracted_data(json_str)
        lp = restored["lp_open"]
        assert isinstance(lp, LPOpenData)
        assert lp.position_id == 12345
        assert lp.tick_lower == -887220
        assert lp.tick_upper == 887220
        assert lp.liquidity == 1000000000

    def test_lp_close_data_round_trip(self):
        original = LPCloseData(
            amount0_collected=480000,
            amount1_collected=170000000000000000,
            fees0=5000,
            fees1=2000000000000000,
            liquidity_removed=1000000000,
        )

        json_str = serialize_extracted_data({"lp_close": original})
        restored = deserialize_extracted_data(json_str)
        lp = restored["lp_close"]
        assert isinstance(lp, LPCloseData)
        assert lp.amount0_collected == 480000
        assert lp.fees0 == 5000

    def test_perp_data_round_trip(self):
        original = PerpData(
            position_id="0xabc123",
            size_delta=1000000000000000000,
            collateral=500000000,
            entry_price=Decimal("3450.00"),
            leverage=Decimal("2.0"),
            realized_pnl=Decimal("150.25"),
            exit_price=Decimal("3500.00"),
            fees_paid=12000000,
        )

        json_str = serialize_extracted_data({"perp": original})
        restored = deserialize_extracted_data(json_str)
        perp = restored["perp"]
        assert isinstance(perp, PerpData)
        assert perp.position_id == "0xabc123"
        assert perp.entry_price == Decimal("3450.00")
        assert perp.realized_pnl == Decimal("150.25")

    def test_borrow_data_round_trip(self):
        original = BorrowData(
            borrow_amount=1000000,
            borrow_rate=Decimal("0.035"),
            debt_token="0xdebt",
            health_factor=Decimal("1.85"),
        )

        json_str = serialize_extracted_data({"borrow": original})
        restored = deserialize_extracted_data(json_str)
        b = restored["borrow"]
        assert isinstance(b, BorrowData)
        assert b.borrow_amount == 1000000
        assert b.health_factor == Decimal("1.85")

    def test_supply_data_round_trip(self):
        original = SupplyData(
            supply_amount=5000000,
            a_token_received=4999000,
            supply_rate=Decimal("0.025"),
        )

        json_str = serialize_extracted_data({"supply": original})
        restored = deserialize_extracted_data(json_str)
        s = restored["supply"]
        assert isinstance(s, SupplyData)
        assert s.supply_amount == 5000000

    def test_stake_data_round_trip(self):
        original = StakeData(
            stake_amount=1000000000000000000,
            shares_received=900000000000000000,
            stake_token="0xstake",
        )

        json_str = serialize_extracted_data({"stake": original})
        restored = deserialize_extracted_data(json_str)
        s = restored["stake"]
        assert isinstance(s, StakeData)
        assert s.stake_amount == 1000000000000000000

    def test_mixed_types(self):
        """Extracted data often has a mix of typed and raw values."""
        data = {
            "swap_amounts": SwapAmounts(
                amount_in=100,
                amount_out=200,
                amount_in_decimal=Decimal("0.1"),
                amount_out_decimal=Decimal("0.2"),
                effective_price=Decimal("2.0"),
            ),
            "position_id": 12345,
            "custom_field": "some_value",
        }

        json_str = serialize_extracted_data(data)
        restored = deserialize_extracted_data(json_str)

        assert isinstance(restored["swap_amounts"], SwapAmounts)
        assert restored["position_id"] == 12345
        assert restored["custom_field"] == "some_value"

    def test_decimal_value_round_trip(self):
        data = {"price": Decimal("3450.123456789")}
        json_str = serialize_extracted_data(data)
        restored = deserialize_extracted_data(json_str)
        assert restored["price"] == Decimal("3450.123456789")

    def test_empty_string_returns_empty_dict(self):
        assert deserialize_extracted_data("") == {}
        assert deserialize_extracted_data(None) == {}

    def test_invalid_json_returns_empty_dict(self):
        assert deserialize_extracted_data("not json") == {}


class TestBuildLedgerEntryExtractedData:
    """Test that build_ledger_entry captures extracted_data_json."""

    def test_build_with_extracted_data(self):
        from unittest.mock import MagicMock

        from almanak.framework.observability.ledger import build_ledger_entry

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.protocol = "uniswap_v3"
        intent.from_token = "USDC"
        intent.to_token = "ETH"

        result = MagicMock()
        result.swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1000.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("2000.0"),
            slippage_bps=15,
            token_in="USDC",
            token_out="ETH",
        )
        result.extracted_data = {"swap_amounts": result.swap_amounts}
        result.transaction_results = [MagicMock(tx_hash="0xabc", gas_used=100000, success=True)]
        result.total_gas_used = 100000
        result.gas_cost_usd = Decimal("1.50")

        entry = build_ledger_entry(
            strategy_id="test",
            cycle_id="cycle-1",
            intent=intent,
            result=result,
            chain="ethereum",
        )

        assert entry.extracted_data_json  # non-empty
        restored = deserialize_extracted_data(entry.extracted_data_json)
        assert isinstance(restored["swap_amounts"], SwapAmounts)
        assert restored["swap_amounts"].effective_price == Decimal("2000.0")

    def test_build_with_multi_tx_bundle(self):
        from unittest.mock import MagicMock

        from almanak.framework.observability.ledger import build_ledger_entry

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.protocol = "aave_v3"
        intent.supply_token = "USDC"
        intent.from_token = None
        intent.to_token = None

        result = MagicMock()
        result.swap_amounts = None
        result.extracted_data = {
            "supply": SupplyData(supply_amount=5000000, a_token_received=4999000)
        }
        # Multi-tx: approve + supply
        tx1 = MagicMock(tx_hash="0xapprove", gas_used=50000, success=True)
        tx2 = MagicMock(tx_hash="0xsupply", gas_used=200000, success=True)
        result.transaction_results = [tx1, tx2]
        result.total_gas_used = 250000
        result.gas_cost_usd = Decimal("3.75")

        entry = build_ledger_entry(
            strategy_id="test",
            cycle_id="cycle-2",
            intent=intent,
            result=result,
            chain="ethereum",
        )

        import json

        parsed = json.loads(entry.extracted_data_json)
        assert "all_tx_results" in parsed
        assert len(parsed["all_tx_results"]) == 2
        assert parsed["all_tx_results"][0]["tx_hash"] == "0xapprove"
        assert parsed["all_tx_results"][1]["tx_hash"] == "0xsupply"

    def test_build_without_result_has_empty_extracted_data(self):
        from unittest.mock import MagicMock

        from almanak.framework.observability.ledger import build_ledger_entry

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "HOLD"
        intent.protocol = ""
        intent.from_token = None
        intent.to_token = None

        entry = build_ledger_entry(
            strategy_id="test",
            cycle_id="cycle-3",
            intent=intent,
            result=None,
            chain="ethereum",
        )

        assert entry.extracted_data_json == ""
