"""G16 native-lane reconciliation — VIB-6061.

The ticket's own words: "A fix that moves a number but adds no invariant is not
done." G16 is that invariant. It asserts that every native token which left the
wallet is named by a Cost Stack line, which is precisely what the GMX keeper
execution fee was not.

Two properties matter more than the rest and are tested first:

* it FAILS on the defect it exists for (a green ratchet that cannot fail is not
  a ratchet); and
* it CAN pass on a correctly-booked run (a cell that is red forever gets muted,
  and then it protects nothing).
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.accounting.accountant_test import _cell_g16_native_lane

_PRICE = "2000"
_WEI = Decimal(10**18)


def _snapshot(ts: str, native: str, symbol: str = "ETH") -> dict:
    return {
        "timestamp": ts,
        "wallet_balances_json": json.dumps(
            [{"symbol": symbol, "balance": native, "price_usd": _PRICE, "address": ""}]
        ),
    }


def _ledger(ts: str, gas_native: str, *, ledger_id: str = "led-1", symbol: str = "ETH") -> dict:
    gas_usd = Decimal(gas_native) * Decimal(_PRICE)
    return {
        "id": ledger_id,
        "timestamp": ts,
        "chain": "arbitrum" if symbol == "ETH" else "avalanche",
        "success": 1,
        "tx_hash": "0x" + "ab" * 32,
        "gas_usd": str(gas_usd),
        "price_inputs_json": json.dumps({symbol: {"price_usd": _PRICE}}),
    }


def _settlement(
    *,
    event_id: str,
    settled_at: str,
    submission_ledger_entry_id: str,
    fee_native: str | None,
    refund_native: str = "0",
) -> dict:
    payload: dict = {
        "event_type": "PERP_SETTLEMENT",
        "settlement_state": "EXECUTED",
        "submission_ledger_entry_id": submission_ledger_entry_id,
    }
    if fee_native is not None:
        payload["keeper_execution_fee_wei"] = int(Decimal(fee_native) * _WEI)
        payload["execution_fee_refund_wei"] = int(Decimal(refund_native) * _WEI)
    return {
        "id": event_id,
        "timestamp": settled_at,
        "event_type": "PERP_SETTLEMENT",
        "payload_json": json.dumps(payload),
    }


def _submission_event(event_id: str, ts: str) -> dict:
    """A PERP_OPEN accounting row.

    Present so the fixtures look like a real run. G16 deliberately does NOT take
    the submission time from here: the join is onto ``transaction_ledger.id``.
    """
    return {
        "id": event_id,
        "timestamp": ts,
        "event_type": "PERP_OPEN",
        "payload_json": json.dumps({"event_type": "PERP_OPEN"}),
    }


class TestTheCellFailsOnItsMotivatingDefect:
    """Without the keeper fee booked, the native lane must not reconcile."""

    def test_an_unbooked_keeper_fee_fails_as_unattributed(self):
        # Wallet drops 0.000130: 0.000012 gas + 0.000118 keeper fee. Only gas is booked.
        snapshots = [_snapshot("2026-07-26T03:00:00", "0.003000"), _snapshot("2026-07-26T05:00:00", "0.002870")]
        ledger = [_ledger("2026-07-26T04:00:00", "0.000012")]
        acct = [
            _submission_event("sub-1", "2026-07-26T04:00:00"),
            _settlement(
                event_id="set-1",
                settled_at="2026-07-26T04:00:30",
                submission_ledger_entry_id="led-1",
                fee_native=None,  # the pre-fix world: no fee measured
            ),
        ]

        result = _cell_g16_native_lane(snapshots, ledger, acct)

        assert result.status == "FAIL"
        assert "UNATTRIBUTED" in result.diagnostic
        assert Decimal(result.decomposition["residual_native"]) > 0

    def test_booking_the_escrow_instead_of_the_keepers_cut_fails_as_over_attributed(self):
        """The mirror defect the ticket's suggested interim would have shipped.

        The escrow is ~8.5x the consumed fee on our Arbitrum sizing, so booking it
        would overstate cost — and G16 catches that direction too.
        """
        snapshots = [_snapshot("2026-07-26T03:00:00", "0.003000"), _snapshot("2026-07-26T05:00:00", "0.002870")]
        ledger = [_ledger("2026-07-26T04:00:00", "0.000012")]
        acct = [
            _submission_event("sub-1", "2026-07-26T04:00:00"),
            _settlement(
                event_id="set-1",
                settled_at="2026-07-26T04:00:30",
                submission_ledger_entry_id="led-1",
                fee_native="0.001",  # the whole escrow, not the keeper's cut
                refund_native="0",
            ),
        ]

        result = _cell_g16_native_lane(snapshots, ledger, acct)

        assert result.status == "FAIL"
        assert "OVER-ATTRIBUTED" in result.diagnostic


class TestTheCellCanPass:
    """A correctly-booked run must green, or the ratchet is floored and useless."""

    def test_gas_plus_keeper_fee_reconciles_to_the_native_delta(self):
        snapshots = [_snapshot("2026-07-26T03:00:00", "0.003000"), _snapshot("2026-07-26T05:00:00", "0.002870")]
        ledger = [_ledger("2026-07-26T04:00:00", "0.000012")]
        acct = [
            _submission_event("sub-1", "2026-07-26T04:00:00"),
            _settlement(
                event_id="set-1",
                settled_at="2026-07-26T04:00:30",
                submission_ledger_entry_id="led-1",
                fee_native="0.000118",
                refund_native="0.000882",
            ),
        ]

        result = _cell_g16_native_lane(snapshots, ledger, acct)

        assert result.status == "PASS", result.diagnostic
        assert result.decomposition["venue_execution_fee_native"] == "0.000118"

    def test_an_escrow_at_an_endpoint_is_unscoreable_not_a_failure(self):
        """The booking-lag ambiguity must SKIP, never FAIL (found by external review).

        ``settled_at`` is when the settlement row was BOOKED, not when the keeper
        settled on-chain. A snapshot landing in that gap sees the refund already
        returned while the cell still counts the whole escrow as outstanding, so a
        confident FAIL there is a residual no missing money caused -- measured on
        this PR's own fork proof as exactly one leg's ``execution_fee_refund_wei``.

        An invariant that cannot separate correct accounting from missing money is
        worse than one that says it does not know: the first gets muted.
        """
        snapshots = [
            _snapshot("2026-07-26T03:00:00", "0.003000"),
            _snapshot("2026-07-26T04:00:10", "0.001988"),
        ]
        ledger = [_ledger("2026-07-26T04:00:00", "0.000012")]
        acct = [
            _submission_event("sub-1", "2026-07-26T04:00:00"),
            _settlement(
                event_id="set-1",
                settled_at="2026-07-26T04:00:30",  # booked AFTER the final snapshot
                submission_ledger_entry_id="led-1",
                fee_native="0.000118",
                refund_native="0.000882",
            ),
        ]

        result = _cell_g16_native_lane(snapshots, ledger, acct)

        assert result.status == "SKIP"
        assert "escrow is outstanding" in result.diagnostic

    def test_a_landed_but_accounting_degraded_row_still_counts_its_gas(self):
        """``success`` is a BOOKS verdict; this cell asks what LANDED on-chain.

        The slippage breaker and the Empty!=Zero guard write success=False on
        transactions that landed and burned real gas. Filtering on ``success``
        drops that gas from ``attributed`` while the balance delta still contains
        it -- manufacturing an UNATTRIBUTED residual exactly when accounting is
        already degraded, and SKIPping outright when every row is.
        """
        snapshots = [_snapshot("2026-07-26T03:00:00", "0.003000"), _snapshot("2026-07-26T05:00:00", "0.002988")]
        degraded = _ledger("2026-07-26T04:00:00", "0.000012")
        # The real shape: success=0 (books verdict) + the degradation marker + a
        # tx_hash. Both conjuncts matter -- the marker alone is not on-chain
        # evidence, and success=0 without it is a plain failure that must NOT count.
        degraded["success"] = 0
        degraded["error"] = "accounting_degraded: receipt yielded no amounts"

        result = _cell_g16_native_lane(snapshots, [degraded], [])

        assert result.status == "PASS", result.diagnostic
        assert result.decomposition["gas_native"] == "0.000012"

    def test_endpoints_are_chosen_by_time_not_by_row_order(self):
        """``_table_rows`` issues no ORDER BY; row order must not pick the window.

        Reversing two otherwise-valid snapshots inverted the window and turned a
        correct PASS into an OVER-ATTRIBUTED FAIL.
        """
        forward = [_snapshot("2026-07-26T03:00:00", "0.003000"), _snapshot("2026-07-26T05:00:00", "0.002988")]
        ledger = [_ledger("2026-07-26T04:00:00", "0.000012")]

        assert _cell_g16_native_lane(forward, ledger, []).status == "PASS"
        assert _cell_g16_native_lane(list(reversed(forward)), ledger, []).status == "PASS"


class TestTheCellRefusesToScoreWhatItCannotMeasure:
    """SKIP, never PASS — a cell that greens on missing data proves nothing.

    SKIP rather than a bespoke "NA": it is in ``CellStatus``, and the ratchet ranks
    it (PASS 3 / XFAIL 2 / SKIP 1 / FAIL 0). An unranked status falls to
    ``STATUS_RANK.get(status, -1)`` = -1, BELOW ``FAIL`` — which would put the floor
    at the bottom of the partial order, make a SKIP->FAIL regression read as an
    improvement, and leave the cell unable to fail its own ratchet.
    """

    def test_a_single_snapshot_is_not_a_window(self):
        result = _cell_g16_native_lane(
            [_snapshot("2026-07-26T03:00:00", "0.003")], [_ledger("2026-07-26T04:00:00", "0.000012")], []
        )

        assert result.status == "SKIP"

    def test_snapshots_without_a_native_balance_are_not_a_measurement(self):
        blank = [
            {"timestamp": "2026-07-26T03:00:00", "wallet_balances_json": json.dumps([{"symbol": "USDC"}])},
            {"timestamp": "2026-07-26T05:00:00", "wallet_balances_json": json.dumps([{"symbol": "USDC"}])},
        ]

        result = _cell_g16_native_lane(blank, [_ledger("2026-07-26T04:00:00", "0.000012")], [])

        assert result.status == "SKIP"

    def test_a_row_whose_native_gas_cannot_be_recovered_blocks_scoring(self):
        """Missing gas_usd or a missing native price is unmeasured, not reconciled."""
        snapshots = [_snapshot("2026-07-26T03:00:00", "0.003"), _snapshot("2026-07-26T05:00:00", "0.00287")]
        row = _ledger("2026-07-26T04:00:00", "0.000012")
        row["price_inputs_json"] = json.dumps({"USDC": {"price_usd": "1.0"}})  # no ETH price

        result = _cell_g16_native_lane(snapshots, [row], [])

        assert result.status == "SKIP"
        assert "unmeasured" in result.diagnostic

    def test_a_run_with_no_settled_ledger_rows_is_skipped(self):
        snapshots = [_snapshot("2026-07-26T03:00:00", "0.003"), _snapshot("2026-07-26T05:00:00", "0.00287")]

        result = _cell_g16_native_lane(snapshots, [], [])

        assert result.status == "SKIP"

def test_the_cell_uses_the_chains_own_native_symbol():
    """An AVAX run must reconcile AVAX, not ETH."""
    snapshots = [
        _snapshot("2026-07-26T03:00:00", "0.280000", symbol="AVAX"),
        _snapshot("2026-07-26T05:00:00", "0.279988", symbol="AVAX"),
    ]
    ledger = [_ledger("2026-07-26T04:00:00", "0.000012", symbol="AVAX")]

    result = _cell_g16_native_lane(snapshots, ledger, [])

    assert result.decomposition["native_symbol"] == "AVAX"
    assert result.status == "PASS", result.diagnostic
