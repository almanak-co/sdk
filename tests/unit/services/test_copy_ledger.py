"""Tests for CopyLedger persistence operations."""

from decimal import Decimal

import pytest

from almanak.framework.services.copy_ledger import CopyLedger
from almanak.framework.services.copy_trading_models import (
    CopyDecision,
    CopyExecutionRecord,
    CopySignal,
    SwapPayload,
)


def _make_signal(event_id: str = "arbitrum:0xabc:0", signal_id: str = "sig-1") -> CopySignal:
    return CopySignal(
        event_id=event_id,
        signal_id=signal_id,
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
        detected_at=2,
        age_seconds=1,
        action_payload=SwapPayload(
            token_in="USDC", token_out="WETH",
            amount_in=Decimal("100"), amount_out=Decimal("0.05"),
        ),
        capability_flags={"chain_supported": True, "protocol_supported": True, "action_supported": True, "token_metadata_resolved": True},
    )


class TestSchemaCreation:
    def test_creates_tables_on_init(self, tmp_path) -> None:
        db_path = tmp_path / "test.db"
        ledger = CopyLedger(db_path)
        # Verify tables exist by querying them
        summary = ledger.get_summary()
        assert summary["signals"] == 0
        assert summary["decisions"] == 0
        ledger.close()

    def test_idempotent_schema_migration(self, tmp_path) -> None:
        db_path = tmp_path / "test.db"
        ledger1 = CopyLedger(db_path)
        ledger1.close()

        # Second init should not fail
        ledger2 = CopyLedger(db_path)
        summary = ledger2.get_summary()
        assert summary["signals"] == 0
        ledger2.close()


class TestHasSeenSignal:
    def test_unseen_signal_returns_false(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        assert ledger.has_seen_signal("new-signal") is False
        ledger.close()

    def test_seen_signal_returns_true(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        ledger.mark_seen_signal("sig-1", detected_at=100)
        assert ledger.has_seen_signal("sig-1") is True
        ledger.close()

    def test_mark_seen_idempotent(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        ledger.mark_seen_signal("sig-1", detected_at=100)
        ledger.mark_seen_signal("sig-1", detected_at=200)  # Should not raise
        assert ledger.has_seen_signal("sig-1") is True
        ledger.close()


class TestRecordSignal:
    def test_record_and_retrieve(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()
        ledger.record_signal(signal)

        summary = ledger.get_summary()
        assert summary["signals"] == 1
        assert ledger.has_seen_signal(signal.signal_id) is True
        ledger.close()

    def test_record_signal_upserts(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()
        ledger.record_signal(signal)
        ledger.record_signal(signal)  # Same signal_id -- upsert

        summary = ledger.get_summary()
        assert summary["signals"] == 1
        ledger.close()


class TestRecordDecision:
    def test_record_decision(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()
        ledger.record_signal(signal)

        decision = CopyDecision(signal=signal, action="execute", decision_id="dec-1")
        ledger.record_decision(decision)

        summary = ledger.get_summary()
        assert summary["decisions"] == 1
        ledger.close()

    def test_record_skip_decision(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()
        decision = CopyDecision(
            signal=signal,
            action="skip",
            skip_reason="stale",
            skip_reason_code="stale_signal",
            decision_id="dec-skip",
        )
        ledger.record_decision(decision)

        decisions = ledger.get_recent_decisions(limit=10)
        assert len(decisions) == 1
        assert decisions[0]["action"] == "skip"
        assert decisions[0]["skip_reason_code"] == "stale_signal"
        ledger.close()


class TestRecordExecution:
    def test_record_execution_success(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        record = CopyExecutionRecord(
            event_id="evt-1",
            signal_id="sig-1",
            intent_id="intent-1",
            status="executed",
            status_code="ok",
            timestamp=100,
            leader_follower_lag_ms=200,
            price_deviation_bps=15,
        )
        ledger.record_execution(record)

        summary = ledger.get_summary()
        assert summary["executions"]["executed"] == 1
        ledger.close()

    def test_record_execution_failed(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        record = CopyExecutionRecord(
            event_id="evt-1",
            signal_id="sig-1",
            status="failed",
            status_code="revert",
            timestamp=100,
        )
        ledger.record_execution(record)

        summary = ledger.get_summary()
        assert summary["executions"]["failed"] == 1
        ledger.close()


class TestGetExecutionRows:
    def test_all_rows_returned(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        for i in range(3):
            ledger.record_execution(CopyExecutionRecord(
                event_id=f"evt-{i}",
                signal_id=f"sig-{i}",
                status="executed",
                timestamp=100 + i,
            ))

        rows = ledger.get_execution_rows()
        assert len(rows) == 3
        ledger.close()

    def test_filtered_by_timestamp(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        ledger.record_execution(CopyExecutionRecord(event_id="old", signal_id="s1", status="executed", timestamp=50))
        ledger.record_execution(CopyExecutionRecord(event_id="new", signal_id="s2", status="executed", timestamp=200))

        rows = ledger.get_execution_rows(since_ts=100)
        assert len(rows) == 1
        assert rows[0]["signal_id"] == "s2"
        ledger.close()


class TestGetRecentDecisions:
    def test_returns_ordered_by_recency(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()

        for i in range(5):
            decision = CopyDecision(signal=signal, action="execute", decision_id=f"dec-{i}")
            ledger.record_decision(decision)

        decisions = ledger.get_recent_decisions(limit=3)
        assert len(decisions) == 3
        ledger.close()

    def test_limit_respected(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()

        for i in range(10):
            ledger.record_decision(CopyDecision(signal=signal, action="execute", decision_id=f"dec-{i}"))

        decisions = ledger.get_recent_decisions(limit=5)
        assert len(decisions) == 5
        ledger.close()


class TestGetSummary:
    def test_summary_with_time_filter(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        signal = _make_signal()
        signal2 = _make_signal(event_id="arbitrum:0xdef:0", signal_id="sig-2")

        ledger.record_signal(signal)
        ledger.record_signal(signal2)

        summary = ledger.get_summary()
        assert summary["signals"] == 2
        assert summary["db_path"] == str(tmp_path / "test.db")
        ledger.close()

    def test_avg_lag_and_deviation(self, tmp_path) -> None:
        ledger = CopyLedger(tmp_path / "test.db")
        ledger.record_execution(CopyExecutionRecord(
            event_id="e1", signal_id="s1", status="executed", timestamp=1,
            leader_follower_lag_ms=100, price_deviation_bps=10,
        ))
        ledger.record_execution(CopyExecutionRecord(
            event_id="e2", signal_id="s2", status="executed", timestamp=2,
            leader_follower_lag_ms=200, price_deviation_bps=30,
        ))

        summary = ledger.get_summary()
        assert summary["avg_leader_follower_lag_ms"] == 150.0
        assert summary["max_price_deviation_bps"] == 30
        ledger.close()
