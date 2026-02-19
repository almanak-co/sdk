"""Tests for CopyLedger and CopyReportGenerator."""

from decimal import Decimal

from almanak.framework.services.copy_ledger import CopyLedger
from almanak.framework.services.copy_reporting import CopyReportGenerator
from almanak.framework.services.copy_trading_models import CopyDecision, CopyExecutionRecord, CopySignal, SwapPayload


def test_copy_ledger_and_reporting_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "copy_ledger.db"
    ledger = CopyLedger(db_path)

    signal = CopySignal(
        event_id="arbitrum:0xabc:0",
        signal_id="sig-1",
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
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            amount_out=Decimal("0.05"),
        ),
        capability_flags={
            "chain_supported": True,
            "protocol_supported": True,
            "action_supported": True,
            "token_metadata_resolved": True,
        },
    )
    ledger.record_signal(signal)

    decision = CopyDecision(signal=signal, action="execute", decision_id="dec-1")
    ledger.record_decision(decision)

    execution = CopyExecutionRecord(
        event_id=signal.event_id,
        signal_id=signal.signal_id,
        intent_id="intent-1",
        status="executed",
        status_code="ok",
        timestamp=3,
        leader_follower_lag_ms=120,
        price_deviation_bps=10,
    )
    ledger.record_execution(execution)

    summary = ledger.get_summary()
    assert summary["signals"] == 1
    assert summary["decisions"] == 1
    assert summary["executions"]["executed"] == 1

    report = CopyReportGenerator(ledger).generate()
    assert report["metrics"]["decision_count"] == 1
    assert report["summary"]["signals"] == 1

    ledger.close()
