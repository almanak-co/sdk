"""Tests for CopyPolicyEngine deterministic policy checks."""

from decimal import Decimal

import pytest

from almanak.framework.services.copy_policy_engine import CopyPolicyEngine
from almanak.framework.services.copy_trading_models import CopySignal, CopyTradingConfigV2, SwapPayload


def _make_config() -> CopyTradingConfigV2:
    return CopyTradingConfigV2.from_config(
        {
            "leaders": [
                {
                    "address": "0x489ee077994B6658eFaCA1507F1FBB620B9308aa",
                    "chain": "arbitrum",
                    "weight": "1",
                    "max_notional_usd": "5000",
                }
            ],
            "global_policy": {
                "enabled": True,
                "action_types": ["SWAP"],
                "protocols": ["uniswap_v3"],
                "tokens": ["USDC", "WETH"],
                "min_usd_value": "50",
                "max_usd_value": "5000",
            },
            "risk": {
                "max_trade_usd": "2000",
                "min_trade_usd": "10",
                "max_daily_notional_usd": "10000",
                "max_open_positions": 10,
                "max_slippage": "0.01",
                "max_price_deviation_bps": 200,
            },
        }
    )


@pytest.fixture()
def signal() -> CopySignal:
    return CopySignal(
        event_id="arbitrum:0xabc:0",
        signal_id="sig-abc",
        action_type="SWAP",
        protocol="uniswap_v3",
        chain="arbitrum",
        tokens=["USDC", "WETH"],
        amounts={"USDC": Decimal("1000"), "WETH": Decimal("0.5")},
        amounts_usd={"USDC": Decimal("1000"), "WETH": Decimal("1000")},
        metadata={"notional_usd": "1000"},
        leader_address="0x489ee077994B6658eFaCA1507F1FBB620B9308aa",
        block_number=100,
        timestamp=1700000000,
        detected_at=1700000001,
        age_seconds=1,
        action_payload=SwapPayload(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            amount_out=Decimal("0.5"),
            effective_price=Decimal("0.0005"),
        ),
        capability_flags={
            "chain_supported": True,
            "protocol_supported": True,
            "action_supported": True,
            "token_metadata_resolved": True,
        },
    )


def test_execute_decision_for_valid_signal(signal: CopySignal) -> None:
    engine = CopyPolicyEngine(_make_config())
    decision = engine.evaluate(signal, current_time=1700000002)

    assert decision.action == "execute"
    assert decision.skip_reason_code is None
    assert decision.decision_id


def test_stale_signal_blocked(signal: CopySignal) -> None:
    engine = CopyPolicyEngine(_make_config())
    decision = engine.evaluate(signal, current_time=1700000500)

    assert decision.action == "skip"
    assert decision.skip_reason_code == "stale_signal"


def test_token_allowlist_blocked(signal: CopySignal) -> None:
    cfg = _make_config()
    bad_signal = CopySignal(
        **{
            **signal.__dict__,
            "tokens": ["USDC", "BADTOKEN"],
            "signal_id": "sig-bad-token",
        }
    )

    engine = CopyPolicyEngine(cfg)
    decision = engine.evaluate(bad_signal, current_time=1700000002)

    assert decision.action == "skip"
    assert decision.skip_reason_code == "token_not_allowlisted"


def test_daily_cap_blocked(signal: CopySignal) -> None:
    engine = CopyPolicyEngine(_make_config())
    state = {
        "date": "2023-11-14",
        "daily_notional_usd": "9500",
        "leader_notional_usd": {},
    }

    decision = engine.evaluate(signal, state=state, current_time=1700000002)
    assert decision.action == "skip"
    assert decision.skip_reason_code == "daily_notional_cap_reached"


def test_leader_lag_check_rejects_excessive_lag(signal: CopySignal) -> None:
    """When leader_lag_blocks exceeds max, the signal is blocked."""
    cfg = _make_config()
    # Default max_leader_lag_blocks is 2 in CopyTradingConfigV2; lag=100 far exceeds it
    lagged_signal = CopySignal(
        **{
            **signal.__dict__,
            "metadata": {**signal.metadata, "leader_lag_blocks": 100},
            "signal_id": "sig-lagged",
        }
    )

    engine = CopyPolicyEngine(cfg)
    decision = engine.evaluate(lagged_signal, current_time=1700000002)
    assert decision.action == "skip"
    assert decision.skip_reason_code == "leader_lag_exceeded"


def test_leader_lag_check_passes_within_limit(signal: CopySignal) -> None:
    """When leader_lag_blocks is within max (default=2), the signal is allowed."""
    cfg = _make_config()
    ok_signal = CopySignal(
        **{
            **signal.__dict__,
            "metadata": {**signal.metadata, "leader_lag_blocks": 1},
            "signal_id": "sig-ok-lag",
        }
    )

    engine = CopyPolicyEngine(cfg)
    decision = engine.evaluate(ok_signal, current_time=1700000002)
    assert decision.action == "execute"
