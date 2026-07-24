"""Branch coverage for StuckDetector classification.

Exercises every priority tier of ``_classify_stuck_reason`` through the
public ``detect_stuck`` entry point, plus the pending-transaction, balance,
and event-emission helpers.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.models import StuckReason
from almanak.framework.services.stuck_detector import (
    AllowanceInfo,
    BalanceInfo,
    PendingTransaction,
    StrategySnapshot,
    StuckDetectionResult,
    StuckDetector,
)


def _snapshot(**overrides) -> StrategySnapshot:
    defaults = {
        "deployment_id": "deployment:abc123",
        "chain": "ethereum",
        "current_state": "PREPARING_SWAP",
        "state_entered_at": datetime.now(UTC) - timedelta(seconds=700),
        "pending_transactions": [],
    }
    defaults.update(overrides)
    return StrategySnapshot(**defaults)


def _pending_tx(**overrides) -> PendingTransaction:
    defaults = {
        "tx_hash": "0xdead",
        "nonce": 1,
        "gas_price": 100,
        "submitted_at": datetime.now(UTC) - timedelta(seconds=10),
    }
    defaults.update(overrides)
    return PendingTransaction(**defaults)


@pytest.fixture
def detector() -> StuckDetector:
    return StuckDetector(emit_events=False)


def classify(detector: StuckDetector, snapshot: StrategySnapshot) -> StuckDetectionResult:
    result = detector.detect_stuck(snapshot)
    assert result.is_stuck
    return result


class TestDetectStuck:
    def test_not_stuck_below_threshold(self, detector):
        result = detector.detect_stuck(
            _snapshot(state_entered_at=datetime.now(UTC) - timedelta(seconds=5))
        )
        assert not result.is_stuck
        assert result.reason is None
        assert result.time_in_state_seconds < detector.stuck_threshold_seconds

    def test_custom_threshold_respected(self):
        detector = StuckDetector(stuck_threshold_seconds=60, emit_events=False)
        result = detector.detect_stuck(
            _snapshot(state_entered_at=datetime.now(UTC) - timedelta(seconds=90))
        )
        assert result.is_stuck

    def test_emits_timeline_event_when_enabled(self, monkeypatch):
        events = []
        monkeypatch.setattr(
            "almanak.framework.services.stuck_detector.add_event", events.append
        )
        detector = StuckDetector(emit_events=True)
        result = detector.detect_stuck(_snapshot(circuit_breaker_triggered=True))
        assert result.is_stuck
        assert len(events) == 1
        event = events[0]
        assert event.deployment_id == "deployment:abc123"
        assert StuckReason.CIRCUIT_BREAKER.value in event.description
        assert event.details["reason"] == StuckReason.CIRCUIT_BREAKER.value

    def test_result_to_dict_serializes_reason(self, detector):
        result = classify(detector, _snapshot(circuit_breaker_triggered=True))
        as_dict = result.to_dict()
        assert as_dict["is_stuck"] is True
        assert as_dict["reason"] == StuckReason.CIRCUIT_BREAKER.value

    def test_not_stuck_to_dict_has_no_reason(self, detector):
        result = detector.detect_stuck(_snapshot(state_entered_at=datetime.now(UTC)))
        assert result.to_dict()["reason"] is None


class TestClassifyStuckReason:
    def test_circuit_breaker_has_top_priority(self, detector):
        snapshot = _snapshot(
            circuit_breaker_triggered=True,
            risk_guard_blocked=True,
            rpc_healthy=False,
            protocol_paused=True,
        )
        assert classify(detector, snapshot).reason == StuckReason.CIRCUIT_BREAKER

    def test_risk_guard_blocked(self, detector):
        result = classify(
            detector, _snapshot(risk_guard_blocked=True, risk_guard_reason="drawdown cap")
        )
        assert result.reason == StuckReason.RISK_GUARD_BLOCKED
        assert result.details["risk_guard_reason"] == "drawdown cap"

    def test_rpc_failure(self, detector):
        result = classify(
            detector, _snapshot(rpc_healthy=False, last_rpc_error="connection refused")
        )
        assert result.reason == StuckReason.RPC_FAILURE
        assert result.details["last_error"] == "connection refused"

    def test_protocol_paused(self, detector):
        assert classify(detector, _snapshot(protocol_paused=True)).reason == StuckReason.PROTOCOL_PAUSED

    def test_oracle_stale(self, detector):
        result = classify(
            detector,
            _snapshot(oracle_last_updated=datetime.now(UTC) - timedelta(seconds=4000)),
        )
        assert result.reason == StuckReason.ORACLE_STALE
        assert result.details["oracle_age_seconds"] > detector.ORACLE_STALE_THRESHOLD_SECONDS

    def test_fresh_oracle_falls_through_to_unknown(self, detector):
        result = classify(
            detector, _snapshot(oracle_last_updated=datetime.now(UTC) - timedelta(seconds=10))
        )
        assert result.reason == StuckReason.UNKNOWN

    def test_allowance_missing(self, detector):
        result = classify(
            detector,
            _snapshot(
                allowance_issues=[
                    AllowanceInfo(
                        token="USDC",
                        spender="0xrouter",
                        current_allowance=Decimal("1"),
                        required_allowance=Decimal("100"),
                    )
                ]
            ),
        )
        assert result.reason == StuckReason.ALLOWANCE_MISSING
        assert result.details["token"] == "USDC"

    def test_sufficient_allowance_falls_through(self, detector):
        result = classify(
            detector,
            _snapshot(
                allowance_issues=[
                    AllowanceInfo(
                        token="USDC",
                        spender="0xrouter",
                        current_allowance=Decimal("100"),
                        required_allowance=Decimal("100"),
                    )
                ]
            ),
        )
        assert result.reason == StuckReason.UNKNOWN

    def test_slippage_exceeded(self, detector):
        result = classify(
            detector,
            _snapshot(
                current_slippage=Decimal("0.05"), max_allowed_slippage=Decimal("0.01")
            ),
        )
        assert result.reason == StuckReason.SLIPPAGE_EXCEEDED

    def test_slippage_within_limit_falls_through(self, detector):
        result = classify(
            detector,
            _snapshot(
                current_slippage=Decimal("0.005"), max_allowed_slippage=Decimal("0.01")
            ),
        )
        assert result.reason == StuckReason.UNKNOWN

    def test_pool_liquidity_low(self, detector):
        result = classify(detector, _snapshot(pool_liquidity_usd=Decimal("500")))
        assert result.reason == StuckReason.POOL_LIQUIDITY_LOW

    def test_healthy_pool_liquidity_falls_through(self, detector):
        result = classify(detector, _snapshot(pool_liquidity_usd=Decimal("1000000")))
        assert result.reason == StuckReason.UNKNOWN

    def test_unknown_when_nothing_matches(self, detector):
        result = classify(detector, _snapshot())
        assert result.reason == StuckReason.UNKNOWN
        assert result.details["state"] == "PREPARING_SWAP"


class TestPendingTransactionChecks:
    def test_gas_price_blocked(self, detector):
        snapshot = _snapshot(
            pending_transactions=[_pending_tx(gas_price=10)],
            current_gas_price=1000,
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.GAS_PRICE_BLOCKED
        assert result.details["tx_hash"] == "0xdead"

    def test_duplicate_nonces_conflict(self, detector):
        snapshot = _snapshot(
            pending_transactions=[_pending_tx(nonce=5), _pending_tx(nonce=5)]
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.NONCE_CONFLICT
        assert result.details["message"] == "Multiple transactions with same nonce"

    def test_nonce_gap_conflict(self, detector):
        snapshot = _snapshot(
            pending_transactions=[_pending_tx(nonce=1), _pending_tx(nonce=3)]
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.NONCE_CONFLICT
        assert result.details["message"] == "Gap in nonce sequence"

    def test_pending_timeout(self, detector):
        snapshot = _snapshot(
            pending_transactions=[
                _pending_tx(submitted_at=datetime.now(UTC) - timedelta(seconds=400))
            ]
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.NOT_INCLUDED_TIMEOUT

    def test_healthy_pending_txs_fall_through(self, detector):
        snapshot = _snapshot(
            pending_transactions=[_pending_tx(nonce=1, gas_price=100), _pending_tx(nonce=2, gas_price=100)],
            current_gas_price=100,
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.UNKNOWN


class TestBalanceChecks:
    def test_insufficient_gas(self, detector):
        snapshot = _snapshot(
            balance_info=BalanceInfo(
                native_balance=Decimal("0.001"),
                token_balances={},
                required_native=Decimal("0.01"),
            )
        )
        assert classify(detector, snapshot).reason == StuckReason.INSUFFICIENT_GAS

    def test_insufficient_token_balance(self, detector):
        snapshot = _snapshot(
            balance_info=BalanceInfo(
                native_balance=Decimal("1"),
                token_balances={"USDC": Decimal("10")},
                required_native=Decimal("0.01"),
                required_tokens={"USDC": Decimal("100")},
            )
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.INSUFFICIENT_BALANCE
        assert result.details["token"] == "USDC"

    def test_missing_token_treated_as_zero_balance(self, detector):
        snapshot = _snapshot(
            balance_info=BalanceInfo(
                native_balance=Decimal("1"),
                token_balances={},
                required_tokens={"WETH": Decimal("1")},
            )
        )
        result = classify(detector, snapshot)
        assert result.reason == StuckReason.INSUFFICIENT_BALANCE
        assert result.details["current_balance"] == "0"

    def test_sufficient_balances_fall_through(self, detector):
        snapshot = _snapshot(
            balance_info=BalanceInfo(
                native_balance=Decimal("1"),
                token_balances={"USDC": Decimal("1000")},
                required_native=Decimal("0.01"),
                required_tokens={"USDC": Decimal("100")},
            )
        )
        assert classify(detector, snapshot).reason == StuckReason.UNKNOWN
