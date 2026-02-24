"""Tests for copy trading data models and enums."""

from decimal import Decimal

import pytest

from almanak.framework.services.copy_trading_models import (
    CopyDecision,
    CopyExecutionRecord,
    CopySignal,
    CopyTradingConfig,
    CopyTradingConfigV2,
    LeaderEvent,
    SizingMode,
)


@pytest.fixture
def leader_event():
    return LeaderEvent(
        chain="arbitrum",
        block_number=100,
        tx_hash="0xabc123",
        log_index=0,
        timestamp=1700000000,
        from_address="0xleader",
        to_address="0xrouter",
        receipt={"status": 1},
    )


@pytest.fixture
def copy_signal():
    return CopySignal(
        event_id="arbitrum:0xabc123:0",
        action_type="SWAP",
        protocol="uniswap_v3",
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        amounts={"WETH": Decimal("1.0")},
        amounts_usd={"WETH": Decimal("2000")},
        metadata={},
        leader_address="0xleader",
        block_number=100,
        timestamp=1700000000,
    )


class TestLeaderEvent:
    def test_event_id_format(self, leader_event):
        assert leader_event.event_id == "arbitrum:0xabc123:0"

    def test_event_id_with_different_log_index(self):
        event = LeaderEvent(
            chain="ethereum",
            block_number=200,
            tx_hash="0xdef456",
            log_index=3,
            timestamp=1700000000,
            from_address="0xleader",
            to_address="0xrouter",
            receipt={},
        )
        assert event.event_id == "ethereum:0xdef456:3"

    def test_frozen_immutability(self, leader_event):
        with pytest.raises(AttributeError):
            leader_event.chain = "ethereum"

        with pytest.raises(AttributeError):
            leader_event.block_number = 999


class TestCopySignal:
    def test_frozen_immutability(self, copy_signal):
        with pytest.raises(AttributeError):
            copy_signal.action_type = "LP_OPEN"

    def test_fields(self, copy_signal):
        assert copy_signal.action_type == "SWAP"
        assert copy_signal.protocol == "uniswap_v3"
        assert copy_signal.chain == "arbitrum"
        assert copy_signal.tokens == ["WETH", "USDC"]
        assert copy_signal.amounts["WETH"] == Decimal("1.0")
        assert copy_signal.amounts_usd["WETH"] == Decimal("2000")
        assert copy_signal.leader_address == "0xleader"


class TestCopyDecision:
    def test_execute_action(self, copy_signal):
        decision = CopyDecision(signal=copy_signal, action="execute")
        assert decision.action == "execute"
        assert decision.skip_reason is None

    def test_skip_action_with_reason(self, copy_signal):
        decision = CopyDecision(signal=copy_signal, action="skip", skip_reason="below_min_usd")
        assert decision.action == "skip"
        assert decision.skip_reason == "below_min_usd"


class TestCopyExecutionRecord:
    def test_defaults(self):
        record = CopyExecutionRecord(event_id="arbitrum:0xabc:0")
        assert record.intent_id is None
        assert record.status == "skipped"
        assert record.skip_reason is None
        assert record.tx_hashes is None
        assert record.timestamp == 0

    def test_executed_record(self):
        record = CopyExecutionRecord(
            event_id="arbitrum:0xabc:0",
            intent_id="intent-123",
            status="executed",
            tx_hashes=["0xtx1"],
            timestamp=1700000000,
        )
        assert record.status == "executed"
        assert record.tx_hashes == ["0xtx1"]


class TestSizingMode:
    def test_fixed_usd_value(self):
        assert SizingMode.FIXED_USD == "fixed_usd"
        assert SizingMode.FIXED_USD.value == "fixed_usd"

    def test_proportion_of_leader_value(self):
        assert SizingMode.PROPORTION_OF_LEADER == "proportion_of_leader"
        assert SizingMode.PROPORTION_OF_LEADER.value == "proportion_of_leader"

    def test_is_str_enum(self):
        assert isinstance(SizingMode.FIXED_USD, str)


class TestCopyTradingConfig:
    def test_from_config_full(self):
        config = {
            "leaders": [{"address": "0xleader", "label": "whale"}],
            "monitoring": {
                "confirmation_depth": 2,
                "poll_interval_seconds": 6,
                "lookback_blocks": 100,
                "max_signal_age_seconds": 600,
            },
            "filters": {"action_types": ["SWAP"], "protocols": ["uniswap_v3"]},
            "sizing": {"mode": "fixed_usd", "fixed_usd": 200, "percentage_of_leader": 0.2},
            "risk": {"max_trade_usd": 500, "max_daily_notional_usd": 5000, "max_open_positions": 3, "max_slippage": 0.005},
        }
        ct = CopyTradingConfig.from_config(config)
        assert ct.leaders == [{"address": "0xleader", "label": "whale"}]
        assert ct.monitoring["confirmation_depth"] == 2
        assert ct.monitoring["poll_interval_seconds"] == 6
        assert ct.filters["action_types"] == ["SWAP"]
        assert ct.sizing["fixed_usd"] == 200
        assert ct.risk["max_trade_usd"] == 500

    def test_from_config_partial_uses_defaults(self):
        config = {
            "leaders": [{"address": "0xleader", "label": "test"}],
        }
        ct = CopyTradingConfig.from_config(config)
        assert ct.leaders == [{"address": "0xleader", "label": "test"}]
        # Monitoring defaults
        assert ct.monitoring["confirmation_depth"] == 1
        assert ct.monitoring["poll_interval_seconds"] == 12
        assert ct.monitoring["lookback_blocks"] == 50
        assert ct.monitoring["max_signal_age_seconds"] == 300
        # Sizing defaults
        assert ct.sizing["mode"] == "fixed_usd"
        assert ct.sizing["fixed_usd"] == 100
        assert ct.sizing["percentage_of_leader"] == 0.1
        # Risk defaults
        assert ct.risk["max_trade_usd"] == 1000
        assert ct.risk["max_daily_notional_usd"] == 10000
        assert ct.risk["max_open_positions"] == 10
        assert ct.risk["max_slippage"] == 0.01

    def test_from_config_empty(self):
        ct = CopyTradingConfig.from_config({})
        assert ct.leaders == []
        assert ct.filters == {}
        assert ct.monitoring["confirmation_depth"] == 1

    def test_from_config_partial_monitoring_merges(self):
        config = {
            "monitoring": {"confirmation_depth": 3},
        }
        ct = CopyTradingConfig.from_config(config)
        assert ct.monitoring["confirmation_depth"] == 3
        # Other monitoring defaults still present
        assert ct.monitoring["poll_interval_seconds"] == 12
        assert ct.monitoring["lookback_blocks"] == 50


class TestCopyTradingConfigV2Normalization:
    """Tests for VIB-130: V2 config normalization fixes."""

    def test_v2_parses_with_string_decimals_and_global_policy(self):
        """V2 config with string Decimals and global_policy parses correctly."""
        config = {
            "leaders": [{"address": "0x" + "ab" * 20, "label": "whale"}],
            "global_policy": {"action_types": ["SWAP"], "protocols": ["uniswap_v3"]},
            "sizing": {"mode": "fixed_usd", "fixed_usd": "200", "percentage_of_leader": "0.2"},
            "risk": {"max_trade_usd": "500", "max_daily_notional_usd": "5000", "max_slippage": "0.005"},
        }
        v2 = CopyTradingConfigV2.from_config(config)
        assert v2.sizing.fixed_usd == Decimal("200")
        assert v2.sizing.percentage_of_leader == Decimal("0.2")
        assert v2.risk.max_trade_usd == Decimal("500")
        assert v2.risk.max_slippage == Decimal("0.005")

    def test_filters_key_aliased_to_global_policy(self):
        """Legacy 'filters' key is aliased to 'global_policy' and removed."""
        config = {
            "leaders": [{"address": "0x" + "ab" * 20}],
            "filters": {"action_types": ["SWAP"], "protocols": ["uniswap_v3"]},
            "sizing": {"mode": "fixed_usd", "fixed_usd": "100"},
            "risk": {"max_trade_usd": "500"},
        }
        v2 = CopyTradingConfigV2.from_config(config)
        assert v2.global_policy.action_types == ["SWAP"]
        assert v2.global_policy.protocols == ["uniswap_v3"]

    def test_filters_removed_when_global_policy_present(self):
        """When both 'filters' and 'global_policy' exist, filters is dropped."""
        config = {
            "leaders": [{"address": "0x" + "ab" * 20}],
            "global_policy": {"action_types": ["SWAP"]},
            "filters": {"action_types": ["LP_OPEN"]},  # stale, should be ignored
            "sizing": {"mode": "fixed_usd", "fixed_usd": "100"},
            "risk": {"max_trade_usd": "500"},
        }
        v2 = CopyTradingConfigV2.from_config(config)
        assert v2.global_policy.action_types == ["SWAP"]  # from global_policy, not filters

    def test_leader_weight_as_string_decimal(self):
        """Leader weight as string decimal parses correctly in V2."""
        config = {
            "leaders": [{"address": "0x" + "ab" * 20, "label": "whale", "weight": "0.7"}],
        }
        v2 = CopyTradingConfigV2.from_config(config)
        assert v2.leaders[0].weight == Decimal("0.7")

    def test_legacy_fallback_logs_warning(self, caplog):
        """When V2 parse fails on floats, fallback logs a warning."""
        import logging

        config = {
            "leaders": [{"address": "0xleader"}],
            "filters": {"action_types": ["SWAP"]},
            "sizing": {"mode": "fixed_usd", "fixed_usd": 200, "percentage_of_leader": 0.2},  # floats
            "risk": {"max_trade_usd": 500, "max_slippage": 0.005},  # floats
        }
        with caplog.at_level(logging.WARNING, logger="almanak.framework.services.copy_trading_models"):
            ct = CopyTradingConfig.from_config(config)

        assert "V2 copy trading config validation failed" in caplog.text
        assert "falling back to legacy mode" in caplog.text
        # Legacy fallback still works
        assert ct.sizing["fixed_usd"] == 200

    def test_v2_parse_succeeds_no_warning(self, caplog):
        """V2 with correct string decimals doesn't log a warning."""
        import logging

        config = {
            "leaders": [{"address": "0x" + "ab" * 20}],
            "global_policy": {"action_types": ["SWAP"]},
            "sizing": {"mode": "fixed_usd", "fixed_usd": "200"},
            "risk": {"max_trade_usd": "500"},
        }
        with caplog.at_level(logging.WARNING, logger="almanak.framework.services.copy_trading_models"):
            CopyTradingConfig.from_config(config)

        assert "V2 copy trading config validation failed" not in caplog.text
