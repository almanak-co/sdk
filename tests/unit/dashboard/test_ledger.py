"""Tests for transaction ledger (VIB-2402)."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.observability.ledger import LedgerEntry, build_ledger_entry


class TestLedgerEntry:
    """Tests for the LedgerEntry dataclass."""

    def test_default_fields(self):
        entry = LedgerEntry()
        assert entry.id  # UUID auto-generated
        assert entry.cycle_id == ""
        assert entry.success is True

    def test_to_dict_round_trip(self):
        entry = LedgerEntry(
            cycle_id="cycle-1",
            strategy_id="strat-1",
            intent_type="SWAP",
            token_in="USDC",
            amount_in="1000",
            token_out="ETH",
            amount_out="0.5",
            effective_price="2000",
            slippage_bps=5.0,
            gas_used=150000,
            tx_hash="0xabc",
            chain="arbitrum",
            protocol="uniswap_v3",
            success=True,
        )
        d = entry.to_dict()
        restored = LedgerEntry.from_dict(d)
        assert restored.cycle_id == "cycle-1"
        assert restored.strategy_id == "strat-1"
        assert restored.intent_type == "SWAP"
        assert restored.token_in == "USDC"
        assert restored.token_out == "ETH"
        assert restored.amount_in == "1000"
        assert restored.amount_out == "0.5"
        assert restored.effective_price == "2000"
        assert restored.slippage_bps == 5.0
        assert restored.gas_used == 150000
        assert restored.tx_hash == "0xabc"
        assert restored.chain == "arbitrum"
        assert restored.protocol == "uniswap_v3"
        assert restored.success is True

    def test_from_dict_missing_fields_uses_defaults(self):
        entry = LedgerEntry.from_dict({"strategy_id": "s1"})
        assert entry.strategy_id == "s1"
        assert entry.intent_type == ""
        assert entry.success is True
        assert entry.error == ""


class TestBuildLedgerEntry:
    """Tests for the build_ledger_entry helper."""

    def test_extracts_swap_amounts(self):
        swap_amounts = SimpleNamespace(
            token_in="USDC",
            token_out="ETH",
            amount_in_decimal=Decimal("1000"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("2000"),
            slippage_bps=5,
        )
        result = SimpleNamespace(
            swap_amounts=swap_amounts,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
        )
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="SWAP"),
            from_token="USDC",
            to_token="ETH",
            protocol="uniswap_v3",
        )

        entry = build_ledger_entry(
            strategy_id="strat-1",
            cycle_id="cycle-1",
            intent=intent,
            result=result,
            chain="arbitrum",
            success=True,
        )

        assert entry.intent_type == "SWAP"
        assert entry.token_in == "USDC"
        assert entry.token_out == "ETH"
        assert entry.amount_in == "1000"
        assert entry.amount_out == "0.5"
        assert entry.effective_price == "2000"
        assert entry.slippage_bps == 5
        assert entry.gas_used == 150000
        assert entry.gas_usd == "0.50"
        assert entry.tx_hash == "0xabc"
        assert entry.chain == "arbitrum"
        assert entry.protocol == "uniswap_v3"
        assert entry.success is True

    def test_fallback_when_no_swap_amounts(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
        )
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="SUPPLY"),
            token="USDC",
            amount=Decimal("500"),
            protocol="aave_v3",
        )

        entry = build_ledger_entry(
            strategy_id="strat-1",
            cycle_id="cycle-2",
            intent=intent,
            result=result,
            chain="base",
            success=True,
        )

        assert entry.intent_type == "SUPPLY"
        assert entry.token_in == "USDC"
        assert entry.amount_in == "500"
        assert entry.protocol == "aave_v3"

    def test_failure_captures_error(self):
        result = SimpleNamespace(
            swap_amounts=None,
            transaction_results=[],
            total_gas_used=0,
            error="reverted",
        )
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="SWAP"),
            from_token="ETH",
            to_token="USDC",
            protocol="",
        )

        entry = build_ledger_entry(
            strategy_id="strat-1",
            cycle_id="cycle-3",
            intent=intent,
            result=result,
            success=False,
            error="tx reverted",
        )

        assert entry.success is False
        assert entry.error == "tx reverted"

    def test_none_result_does_not_crash(self):
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="HOLD"),
            protocol="",
        )
        entry = build_ledger_entry(
            strategy_id="strat-1",
            cycle_id="",
            intent=intent,
            result=None,
            success=False,
            error="no execution",
        )
        assert entry.intent_type == "HOLD"
        assert entry.tx_hash == ""
        assert entry.gas_used == 0
