"""Extended tests for CopyPolicyEngine -- covers policy checks not in the original test file."""

from decimal import Decimal

import pytest

from almanak.framework.services.copy_policy_engine import CopyPolicyEngine
from almanak.framework.services.copy_trading_models import (
    CopySignal,
    CopyTradingConfigV2,
    PerpPayload,
    SwapPayload,
)


def _make_config(**overrides) -> CopyTradingConfigV2:
    base = {
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
            "action_types": ["SWAP", "LP_OPEN", "LP_CLOSE", "SUPPLY", "WITHDRAW", "BORROW", "REPAY", "PERP_OPEN", "PERP_CLOSE"],
            "protocols": ["uniswap_v3", "aave_v3", "gmx_v2"],
            "tokens": ["USDC", "WETH"],
            "min_usd_value": "10",
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
    base.update(overrides)
    return CopyTradingConfigV2.from_config(base)


def _make_signal(**overrides) -> CopySignal:
    defaults = {
        "event_id": "arbitrum:0xabc:0",
        "signal_id": "sig-test",
        "action_type": "SWAP",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "tokens": ["USDC", "WETH"],
        "amounts": {"USDC": Decimal("1000"), "WETH": Decimal("0.5")},
        "amounts_usd": {"USDC": Decimal("1000"), "WETH": Decimal("1000")},
        "metadata": {"notional_usd": "1000"},
        "leader_address": "0x489ee077994B6658eFaCA1507F1FBB620B9308aa",
        "block_number": 100,
        "timestamp": 1700000000,
        "detected_at": 1700000001,
        "age_seconds": 1,
        "action_payload": SwapPayload(
            token_in="USDC", token_out="WETH",
            amount_in=Decimal("1000"), amount_out=Decimal("0.5"),
            effective_price=Decimal("0.0005"),
        ),
        "capability_flags": {
            "chain_supported": True,
            "protocol_supported": True,
            "action_supported": True,
            "token_metadata_resolved": True,
        },
    }
    defaults.update(overrides)
    return CopySignal(**defaults)


class TestCapabilityChecks:
    def test_unsupported_chain_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(capability_flags={"chain_supported": False, "protocol_supported": True, "action_supported": True, "token_metadata_resolved": True})
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "unsupported_chain"

    def test_unsupported_protocol_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(capability_flags={"chain_supported": True, "protocol_supported": False, "action_supported": True, "token_metadata_resolved": True})
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "unsupported_protocol"

    def test_unsupported_action_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(capability_flags={"chain_supported": True, "protocol_supported": True, "action_supported": False, "token_metadata_resolved": True})
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "unsupported_action"

    def test_missing_token_metadata_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(capability_flags={"chain_supported": True, "protocol_supported": True, "action_supported": True, "token_metadata_resolved": False})
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "missing_token_metadata"


class TestProtocolAllowlist:
    def test_protocol_not_in_allowlist_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(protocol="curve")
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "protocol_not_allowlisted"

    def test_protocol_in_allowlist_passes(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(protocol="uniswap_v3")
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "execute"


class TestLeaderCap:
    def test_leader_cap_blocks_when_exceeded(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        state = {
            "date": "2023-11-14",
            "daily_notional_usd": "0",
            "leader_notional_usd": {
                "0x489ee077994b6658efaca1507f1fbb620b9308aa": "4500",
            },
        }
        signal = _make_signal()
        decision = engine.evaluate(signal, state=state, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "leader_notional_cap_reached"

    def test_leader_cap_passes_when_within_limit(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        state = {
            "date": "2023-11-14",
            "daily_notional_usd": "0",
            "leader_notional_usd": {
                "0x489ee077994b6658efaca1507f1fbb620b9308aa": "1000",
            },
        }
        signal = _make_signal()
        decision = engine.evaluate(signal, state=state, current_time=1700000002)
        assert decision.action == "execute"


class TestPriceDeviation:
    def test_deviation_exceeded_blocked(self) -> None:
        def mock_price(token: str, chain: str) -> Decimal:
            return {"USDC": Decimal("1"), "WETH": Decimal("2000")}.get(token, Decimal("1"))

        engine = CopyPolicyEngine(_make_config(), reference_price_fn=mock_price)
        # effective_price 0.0005 = WETH/USDC, reference = 2000/1 = 2000
        # These differ enormously -> should block
        signal = _make_signal()
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "price_deviation_exceeded"

    def test_no_deviation_check_without_reference_fn(self) -> None:
        engine = CopyPolicyEngine(_make_config(), reference_price_fn=None)
        signal = _make_signal()
        decision = engine.evaluate(signal, current_time=1700000002)
        # Without reference price fn, deviation check is skipped -- passes
        assert decision.action == "execute"

    def test_deviation_skipped_for_non_swap(self) -> None:
        def mock_price(token: str, chain: str) -> Decimal:
            return Decimal("100")

        engine = CopyPolicyEngine(_make_config(), reference_price_fn=mock_price)
        signal = _make_signal(
            action_type="SUPPLY",
            action_payload=None,
            amounts_usd={"USDC": Decimal("100")},
        )
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "execute"


class TestNotionalBounds:
    def test_below_min_trade_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(
            amounts_usd={"USDC": Decimal("5"), "WETH": Decimal("5")},
            metadata={"notional_usd": "5"},
        )
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "below_min_trade"

    def test_above_max_trade_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(
            amounts_usd={"USDC": Decimal("3000"), "WETH": Decimal("3000")},
            metadata={"notional_usd": "3000"},
        )
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "max_trade_exceeded"


class TestActionEnabled:
    def test_disabled_action_blocked(self) -> None:
        config = _make_config(
            action_policies={"SWAP": {"enabled": False}},
        )
        engine = CopyPolicyEngine(config)
        signal = _make_signal()
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "action_disabled"


class TestRecordExecution:
    def test_record_execution_updates_state(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal()
        state = engine.record_execution(signal, Decimal("500"), current_time=1700000002)
        assert state["daily_notional_usd"] == Decimal("500")
        leader_key = signal.leader_address.lower()
        assert state["leader_notional_usd"][leader_key] == Decimal("500")

    def test_record_execution_accumulates(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal()
        state = engine.record_execution(signal, Decimal("500"), current_time=1700000002)
        state = engine.record_execution(signal, Decimal("300"), state=state, current_time=1700000002)
        assert state["daily_notional_usd"] == Decimal("800")


class TestLeaderLag:
    def test_lag_exceeded_blocked(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(metadata={"notional_usd": "1000", "leader_lag_blocks": 10})
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "skip"
        assert decision.skip_reason_code == "leader_lag_exceeded"

    def test_lag_unknown_passes(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal()  # no leader_lag_blocks in metadata
        decision = engine.evaluate(signal, current_time=1700000002)
        assert decision.action == "execute"


class TestDeriveSignalNotional:
    def test_uses_max_abs_from_amounts_usd(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(amounts_usd={"A": Decimal("500"), "B": Decimal("-800")})
        notional = engine._derive_signal_notional_usd(signal)
        assert notional == Decimal("800")

    def test_falls_back_to_perp_size_usd(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(
            amounts_usd={},
            action_payload=PerpPayload(market="0x1", size_usd=Decimal("2000")),
        )
        notional = engine._derive_signal_notional_usd(signal)
        assert notional == Decimal("2000")

    def test_falls_back_to_metadata_notional(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(amounts_usd={}, action_payload=None, metadata={"notional_usd": "750"})
        notional = engine._derive_signal_notional_usd(signal)
        assert notional == Decimal("750")

    def test_returns_zero_with_no_data(self) -> None:
        engine = CopyPolicyEngine(_make_config())
        signal = _make_signal(amounts_usd={}, action_payload=None, metadata={})
        notional = engine._derive_signal_notional_usd(signal)
        assert notional == Decimal("0")
