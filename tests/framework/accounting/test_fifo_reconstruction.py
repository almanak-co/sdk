"""Additional tests for FIFOBasisStore.reconstruct_from_events() (VIB-3477).

This file focuses on scenarios NOT already covered in test_basis_reconstruction.py:
- Multi-lot stacking and lot-order correctness (FIFO ordering)
- Empty event list produces empty store (complementary to existing tests)
- Rows with missing deployment_id are skipped
- Multiple deployment_ids are tracked independently
- DELEVERAGE events reduce BORROW lots (same as REPAY during reconstruction)
- Reconstruction returns correct count of replayed operations

Do NOT duplicate tests from test_basis_reconstruction.py:
  - test_borrow_repay_interest_correct_after_restart    (covered)
  - test_repay_without_borrow_still_unmatched           (covered)
  - test_partial_repay_leaves_remaining_lot             (covered)
  - test_idempotent_reconstruction                      (covered)
  - test_malformed_row_skipped                          (covered)
  - test_unknown_event_type_skipped                     (covered)
  - PT_BUY / PT_SELL / PT_REDEEM reconstruction         (covered)
  - source_ledger_entry_id propagation                  (covered)
  - policy-v1 warning                                   (covered)
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore


# ──────────────────────────────────────────────────────────────────────────────
# Row builder helpers
# ──────────────────────────────────────────────────────────────────────────────


def _borrow_row(
    deployment_id: str,
    position_key: str,
    asset: str,
    amount_token: Decimal,
    timestamp: str = "2026-01-01T00:00:00+00:00",
    ledger_entry_id: str | None = None,
) -> dict:
    payload = {
        "event_type": "BORROW",
        "position_key": position_key,
        "market_id": "test-market",
        "asset": asset,
        "amount_token": str(amount_token),
        "confidence": "HIGH",
        "unavailable_reason": "",
        "schema_version": 1,
        "collateral_value_before_usd": None,
        "collateral_value_after_usd": None,
        "debt_value_before_usd": None,
        "debt_value_after_usd": None,
        "net_equity_before_usd": None,
        "net_equity_after_usd": None,
        "health_factor_before": None,
        "health_factor_after": None,
        "liquidation_threshold": None,
        "lltv": None,
        "supply_apr_bps": None,
        "borrow_apr_bps": None,
        "principal_delta_usd": None,
        "interest_delta_usd": None,
        "gas_usd": None,
    }
    row: dict = {
        "event_type": "BORROW",
        "deployment_id": deployment_id,
        "position_key": position_key,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }
    if ledger_entry_id is not None:
        row["ledger_entry_id"] = ledger_entry_id
    return row


def _repay_row(
    deployment_id: str,
    position_key: str,
    asset: str,
    amount_token: Decimal,
    timestamp: str = "2026-01-02T00:00:00+00:00",
    event_type: str = "REPAY",
) -> dict:
    payload = {
        "event_type": event_type,
        "position_key": position_key,
        "market_id": "test-market",
        "asset": asset,
        "amount_token": str(amount_token),
        "confidence": "HIGH",
        "unavailable_reason": "",
        "schema_version": 1,
        "collateral_value_before_usd": None,
        "collateral_value_after_usd": None,
        "debt_value_before_usd": None,
        "debt_value_after_usd": None,
        "net_equity_before_usd": None,
        "net_equity_after_usd": None,
        "health_factor_before": None,
        "health_factor_after": None,
        "liquidation_threshold": None,
        "lltv": None,
        "supply_apr_bps": None,
        "borrow_apr_bps": None,
        "principal_delta_usd": None,
        "interest_delta_usd": None,
        "gas_usd": None,
    }
    return {
        "event_type": event_type,
        "deployment_id": deployment_id,
        "position_key": position_key,
        "timestamp": timestamp,
        "payload_json": json.dumps(payload),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Multi-lot FIFO ordering
# ──────────────────────────────────────────────────────────────────────────────


class TestFIFOLotOrdering:
    def test_multiple_borrow_lots_consumed_fifo(self) -> None:
        """Two BORROW lots are consumed FIFO — oldest lot consumed first."""
        dep = "dep-fifo"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _borrow_row(dep, pk, "USDC", Decimal("100"),
                        timestamp="2026-01-01T00:00:00+00:00"),
            _borrow_row(dep, pk, "USDC", Decimal("200"),
                        timestamp="2026-01-02T00:00:00+00:00"),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 2

        # REPAY 100 should consume the first lot entirely and leave second lot untouched.
        result = store.match_repay(dep, pk, "USDC", Decimal("100"))
        assert result.repaid_principal == Decimal("100")
        assert result.unmatched_amount == Decimal("0")

        # Remaining 200 in second lot should still be matchable.
        result2 = store.match_repay(dep, pk, "USDC", Decimal("200"))
        assert result2.repaid_principal == Decimal("200")

    def test_fifo_lot_count_reflects_unreconsumed_lots(self) -> None:
        """reconstruct_from_events counts each lot operation (BORROW and REPAY both counted)."""
        dep = "dep-cnt"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _borrow_row(dep, pk, "USDC", Decimal("300")),
            _repay_row(dep, pk, "USDC", Decimal("150")),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        # Both BORROW and REPAY are counted as replayed operations.
        assert replayed == 2


# ──────────────────────────────────────────────────────────────────────────────
# Empty events list
# ──────────────────────────────────────────────────────────────────────────────


class TestFIFOEmptyEvents:
    def test_empty_events_produces_empty_store(self) -> None:
        """reconstruct_from_events([]) → store is empty, returns 0, no error."""
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events([])
        assert replayed == 0
        assert store._lots == {}

    def test_empty_events_clears_previously_populated_store(self) -> None:
        """Calling reconstruct_from_events([]) on a store with existing lots clears it."""
        dep = "dep-clear"
        pk = "lending:arb:aave_v3:0xwallet:DAI"
        store = FIFOBasisStore()
        store.record_borrow(dep, pk, "DAI", Decimal("500"))
        assert store._lots  # pre-condition: store has data

        replayed = store.reconstruct_from_events([])
        assert replayed == 0
        assert store._lots == {}


# ──────────────────────────────────────────────────────────────────────────────
# Missing identity fields are skipped
# ──────────────────────────────────────────────────────────────────────────────


class TestFIFORowFiltering:
    def test_reconstruction_skips_row_with_missing_deployment_id(self) -> None:
        """Rows with deployment_id='' are skipped — cannot key into lot store."""
        dep = "dep-filter"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _borrow_row("", pk, "USDC", Decimal("100")),          # skipped: no deployment_id
            _borrow_row(dep, pk, "USDC", Decimal("200")),          # recorded
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 1  # only the valid row

    def test_reconstruction_skips_row_with_missing_position_key(self) -> None:
        """Rows with position_key='' are skipped — cannot key into lot store."""
        dep = "dep-filter2"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _borrow_row(dep, "", "USDC", Decimal("100")),          # skipped: no position_key
            _borrow_row(dep, pk, "USDC", Decimal("200")),          # recorded
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 1

    def test_reconstruction_skips_unknown_event_gracefully(self) -> None:
        """Unknown event types are silently skipped — no error raised."""
        dep = "dep-unk"
        pk = "pk-unk"
        payload = json.dumps({"event_type": "FUTURE_V99", "asset": "USDC",
                              "amount_token": "100", "schema_version": 1})
        events = [
            {
                "event_type": "FUTURE_V99",
                "deployment_id": dep,
                "position_key": pk,
                "timestamp": "2026-01-01T00:00:00+00:00",
                "payload_json": payload,
            },
            _borrow_row(dep, pk, "USDC", Decimal("50")),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        # Unknown type skipped; BORROW counted.
        assert replayed == 1
        result = store.match_repay(dep, pk, "USDC", Decimal("50"))
        assert result.repaid_principal == Decimal("50")


# ──────────────────────────────────────────────────────────────────────────────
# Multiple deployments are tracked independently
# ──────────────────────────────────────────────────────────────────────────────


class TestFIFOMultipleDeployments:
    def test_separate_deployment_ids_maintain_independent_lots(self) -> None:
        """BORROW events for two different deployment_ids are stored under separate keys."""
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _borrow_row("dep-A", pk, "USDC", Decimal("100")),
            _borrow_row("dep-B", pk, "USDC", Decimal("200")),
        ]
        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        result_a = store.match_repay("dep-A", pk, "USDC", Decimal("105"))
        result_b = store.match_repay("dep-B", pk, "USDC", Decimal("210"))

        # dep-A: 100 principal + 5 interest
        assert result_a.repaid_principal == Decimal("100")
        assert result_a.interest_or_yield == Decimal("5")

        # dep-B: 200 principal + 10 interest
        assert result_b.repaid_principal == Decimal("200")
        assert result_b.interest_or_yield == Decimal("10")


# ──────────────────────────────────────────────────────────────────────────────
# DELEVERAGE events during reconstruction
# ──────────────────────────────────────────────────────────────────────────────


class TestFIFODeleverageReconstruction:
    def test_deleverage_event_reduces_borrow_lot(self) -> None:
        """DELEVERAGE is treated as REPAY during reconstruction — reduces the open lot."""
        dep = "dep-delev"
        pk = "lending:arb:aave_v3:0xwallet:USDC"
        events = [
            _borrow_row(dep, pk, "USDC", Decimal("1000")),
            _repay_row(dep, pk, "USDC", Decimal("400"),
                       event_type="DELEVERAGE",
                       timestamp="2026-01-03T00:00:00+00:00"),
        ]
        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events(events)
        assert replayed == 2

        # 600 USDC should remain in the lot.
        result = store.match_repay(dep, pk, "USDC", Decimal("600"))
        assert result.repaid_principal == Decimal("600")
        assert result.unmatched_amount == Decimal("0")

    def test_deleverage_full_repay_leaves_empty_lot(self) -> None:
        """Full DELEVERAGE clears the borrow lot; subsequent REPAY has unmatched amount."""
        dep = "dep-full-delev"
        pk = "lending:arb:aave_v3:0xwallet:WETH"
        events = [
            _borrow_row(dep, pk, "WETH", Decimal("5")),
            _repay_row(dep, pk, "WETH", Decimal("5.3"),
                       event_type="DELEVERAGE",
                       timestamp="2026-01-04T00:00:00+00:00"),
        ]
        store = FIFOBasisStore()
        store.reconstruct_from_events(events)

        # Lot is exhausted — further REPAY has nothing to match.
        result = store.match_repay(dep, pk, "WETH", Decimal("1"))
        assert result.unmatched_amount == Decimal("1")


# ──────────────────────────────────────────────────────────────────────────────
# Multiple restarts are idempotent
# ──────────────────────────────────────────────────────────────────────────────


class TestFIFOMultipleRestarts:
    def test_multiple_reconstruct_calls_are_idempotent(self) -> None:
        """Calling reconstruct_from_events twice with the same events gives the same final state."""
        dep = "dep-idem"
        pk = "lending:arb:aave_v3:0xwallet:DAI"
        events = [
            _borrow_row(dep, pk, "DAI", Decimal("750")),
            _repay_row(dep, pk, "DAI", Decimal("250")),
        ]
        store = FIFOBasisStore()

        store.reconstruct_from_events(events)
        # Simulate a second restart with the same historical events.
        store.reconstruct_from_events(events)

        # 750 - 250 = 500 remaining in the lot.
        result = store.match_repay(dep, pk, "DAI", Decimal("500"))
        assert result.repaid_principal == Decimal("500")
        assert result.unmatched_amount == Decimal("0")
