"""Tests for WalletActivityProvider."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.wallet_activity import WalletActivityProvider
from almanak.framework.services.copy_trading_models import CopySignal, LeaderEvent


def _make_signal(
    event_id: str = "arb:0xabc:0",
    action_type: str = "SWAP",
    protocol: str = "uniswap_v3",
    chain: str = "arbitrum",
    leader_address: str = "0xLeader1",
    amounts_usd: dict | None = None,
) -> CopySignal:
    return CopySignal(
        event_id=event_id,
        action_type=action_type,
        protocol=protocol,
        chain=chain,
        tokens=["WETH", "USDC"],
        amounts={"WETH": Decimal("1"), "USDC": Decimal("2000")},
        amounts_usd=amounts_usd if amounts_usd is not None else {"WETH": Decimal("2000")},
        metadata={},
        leader_address=leader_address,
        block_number=100,
        timestamp=1000,
    )


def _make_event(
    tx_hash: str = "0xabc",
    log_index: int = 0,
    chain: str = "arbitrum",
) -> LeaderEvent:
    return LeaderEvent(
        chain=chain,
        block_number=100,
        tx_hash=tx_hash,
        log_index=log_index,
        timestamp=1000,
        from_address="0xLeader1",
        to_address="0xRouter",
        receipt={},
    )


@pytest.fixture
def mock_monitor():
    monitor = MagicMock()
    monitor.config.chain = "arbitrum"
    monitor.poll.return_value = ([], {"last_processed_block": 100})
    return monitor


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.process_events.return_value = []
    return engine


@pytest.fixture
def provider(mock_monitor, mock_engine):
    return WalletActivityProvider(
        wallet_monitor=mock_monitor,
        signal_engine=mock_engine,
    )


class TestPollAndProcess:
    def test_poll_and_process_adds_signals(self, provider, mock_monitor, mock_engine):
        events = [_make_event()]
        signals = [_make_signal()]
        mock_monitor.poll.return_value = (events, {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals

        provider.poll_and_process()

        mock_monitor.poll.assert_called_once_with({})
        mock_engine.process_events.assert_called_once_with(events, current_block=101)
        assert provider.get_signals() == signals

    def test_poll_and_process_accumulates_across_polls(self, provider, mock_monitor, mock_engine):
        sig1 = _make_signal(event_id="arb:0xabc:0")
        sig2 = _make_signal(event_id="arb:0xdef:0")

        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = [sig1]
        provider.poll_and_process()

        mock_monitor.poll.return_value = ([_make_event(tx_hash="0xdef")], {"last_processed_block": 102})
        mock_engine.process_events.return_value = [sig2]
        provider.poll_and_process()

        assert len(provider.get_signals()) == 2

    def test_poll_with_no_events_updates_state(self, provider, mock_monitor, mock_engine):
        mock_monitor.poll.return_value = ([], {"last_processed_block": 105})

        provider.poll_and_process()

        state = provider.get_state()
        # Single-monitor backward compat: legacy keys merged in
        assert state["last_processed_block"] == 105
        # Also stored under cursor key
        assert state["cursor:arbitrum"]["last_processed_block"] == 105
        mock_engine.process_events.assert_not_called()

    def test_poll_passes_current_state_to_monitor(self, provider, mock_monitor, mock_engine):
        # Set state with cursor key that the provider will use
        provider.set_state({"cursor:arbitrum": {"last_processed_block": 50}})
        mock_monitor.poll.return_value = ([], {"last_processed_block": 55})

        provider.poll_and_process()

        mock_monitor.poll.assert_called_once_with({"last_processed_block": 50})


class TestGetSignals:
    def test_no_filters_returns_all(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1"),
            _make_signal(event_id="e2"),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals()
        assert len(result) == 2

    def test_filter_by_action_type(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1", action_type="SWAP"),
            _make_signal(event_id="e2", action_type="LP_OPEN"),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals(action_types=["SWAP"])
        assert len(result) == 1
        assert result[0].event_id == "e1"

    def test_filter_by_protocol(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1", protocol="uniswap_v3"),
            _make_signal(event_id="e2", protocol="aerodrome"),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals(protocols=["aerodrome"])
        assert len(result) == 1
        assert result[0].event_id == "e2"

    def test_filter_by_min_usd_value(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1", amounts_usd={"WETH": Decimal("500")}),
            _make_signal(event_id="e2", amounts_usd={"WETH": Decimal("50")}),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals(min_usd_value=Decimal("100"))
        assert len(result) == 1
        assert result[0].event_id == "e1"

    def test_filter_by_leader_address(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1", leader_address="0xLeader1"),
            _make_signal(event_id="e2", leader_address="0xLeader2"),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals(leader_address="0xleader1")  # case-insensitive
        assert len(result) == 1
        assert result[0].event_id == "e1"

    def test_filters_are_and_combined(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1", action_type="SWAP", protocol="uniswap_v3"),
            _make_signal(event_id="e2", action_type="SWAP", protocol="aerodrome"),
            _make_signal(event_id="e3", action_type="LP_OPEN", protocol="uniswap_v3"),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals(action_types=["SWAP"], protocols=["uniswap_v3"])
        assert len(result) == 1
        assert result[0].event_id == "e1"

    def test_get_signals_does_not_consume(self, provider, mock_monitor, mock_engine):
        signals = [_make_signal()]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        # Call twice -- both should return same results
        result1 = provider.get_signals()
        result2 = provider.get_signals()
        assert result1 == result2

    def test_empty_amounts_usd_below_min(self, provider, mock_monitor, mock_engine):
        signals = [_make_signal(event_id="e1", amounts_usd={})]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        result = provider.get_signals(min_usd_value=Decimal("1"))
        assert len(result) == 0


class TestConsumeSignals:
    def test_consume_removes_by_event_id(self, provider, mock_monitor, mock_engine):
        signals = [
            _make_signal(event_id="e1"),
            _make_signal(event_id="e2"),
            _make_signal(event_id="e3"),
        ]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        provider.consume_signals(["e1", "e3"])

        remaining = provider.get_signals()
        assert len(remaining) == 1
        assert remaining[0].event_id == "e2"

    def test_consume_nonexistent_ids_is_noop(self, provider, mock_monitor, mock_engine):
        signals = [_make_signal(event_id="e1")]
        mock_monitor.poll.return_value = ([_make_event()], {"last_processed_block": 101})
        mock_engine.process_events.return_value = signals
        provider.poll_and_process()

        provider.consume_signals(["nonexistent"])

        assert len(provider.get_signals()) == 1


class TestStateManagement:
    def test_state_roundtrip(self, provider):
        state = {"last_processed_block": 42, "extra": "data"}
        provider.set_state(state)
        retrieved = provider.get_state()

        # Legacy flat state is preserved and migrated to per-chain cursor
        assert retrieved["last_processed_block"] == 42
        assert retrieved["extra"] == "data"
        assert retrieved["cursor:arbitrum"]["last_processed_block"] == 42
        # Verify it's a copy, not the same reference
        assert retrieved is not state

    def test_initial_state_empty(self, provider):
        assert provider.get_state() == {}

    def test_state_from_constructor(self, mock_monitor, mock_engine):
        initial_state = {"last_processed_block": 99}
        prov = WalletActivityProvider(
            wallet_monitor=mock_monitor,
            signal_engine=mock_engine,
            state_manager_state=initial_state,
        )
        state = prov.get_state()
        # Legacy flat state is preserved and also migrated to per-chain cursor
        assert state["last_processed_block"] == 99
        assert state["cursor:arbitrum"] == {"last_processed_block": 99}

    def test_set_state_is_defensive_copy(self, provider):
        state = {"last_processed_block": 42}
        provider.set_state(state)
        state["last_processed_block"] = 999
        assert provider.get_state()["last_processed_block"] == 42


class TestMultiChainMonitoring:
    """Tests for multi-chain wallet monitoring."""

    def test_multi_chain_polls_all_monitors(self):
        arb_monitor = MagicMock()
        arb_monitor.config.chain = "arbitrum"
        arb_monitor.poll.return_value = (
            [_make_event(chain="arbitrum")],
            {"last_processed_block": 100},
        )

        base_monitor = MagicMock()
        base_monitor.config.chain = "base"
        base_monitor.poll.return_value = (
            [_make_event(chain="base", tx_hash="0xbase")],
            {"last_processed_block": 200},
        )

        engine = MagicMock()
        engine.process_events.side_effect = [
            [_make_signal(event_id="arb:0xabc:0", chain="arbitrum")],
            [_make_signal(event_id="base:0xbase:0", chain="base")],
        ]

        provider = WalletActivityProvider(
            signal_engine=engine,
            wallet_monitors={"arbitrum": arb_monitor, "base": base_monitor},
        )
        provider.poll_and_process()

        signals = provider.get_signals()
        assert len(signals) == 2
        chains = {s.chain for s in signals}
        assert chains == {"arbitrum", "base"}

    def test_multi_chain_per_chain_cursor(self):
        arb_monitor = MagicMock()
        arb_monitor.config.chain = "arbitrum"
        arb_monitor.poll.return_value = ([], {"last_processed_block": 100})

        base_monitor = MagicMock()
        base_monitor.config.chain = "base"
        base_monitor.poll.return_value = ([], {"last_processed_block": 200})

        engine = MagicMock()
        engine.process_events.return_value = []

        provider = WalletActivityProvider(
            signal_engine=engine,
            wallet_monitors={"arbitrum": arb_monitor, "base": base_monitor},
        )
        provider.poll_and_process()

        state = provider.get_state()
        assert state["cursor:arbitrum"]["last_processed_block"] == 100
        assert state["cursor:base"]["last_processed_block"] == 200

    def test_multi_chain_state_persistence_roundtrip(self):
        arb_monitor = MagicMock()
        arb_monitor.config.chain = "arbitrum"
        arb_monitor.poll.return_value = ([], {"last_processed_block": 100})

        engine = MagicMock()
        engine.process_events.return_value = []

        provider1 = WalletActivityProvider(
            signal_engine=engine,
            wallet_monitors={"arbitrum": arb_monitor},
        )
        provider1.poll_and_process()
        saved_state = provider1.get_state()

        provider2 = WalletActivityProvider(
            signal_engine=engine,
            wallet_monitors={"arbitrum": arb_monitor},
        )
        provider2.set_state(saved_state)

        assert provider2.get_state() == saved_state

    def test_set_state_migrates_legacy_flat_state(self):
        """Legacy flat state (last_processed_block) is migrated to per-chain cursor keys."""
        arb_monitor = MagicMock()
        arb_monitor.config.chain = "arbitrum"
        arb_monitor.poll.return_value = ([], {"last_processed_block": 150})

        engine = MagicMock()
        engine.process_events.return_value = []

        provider = WalletActivityProvider(
            signal_engine=engine,
            wallet_monitors={"arbitrum": arb_monitor},
        )

        # Simulate restoring legacy state (pre-multi-chain format)
        legacy_state = {"last_processed_block": 42, "last_block_hash": "0xdeadbeef"}
        provider.set_state(legacy_state)

        state = provider.get_state()
        # Legacy keys should remain
        assert state["last_processed_block"] == 42
        # Per-chain cursor should be created from legacy keys
        assert state["cursor:arbitrum"]["last_processed_block"] == 42
        assert state["cursor:arbitrum"]["last_block_hash"] == "0xdeadbeef"

        # Now poll should use the migrated cursor
        provider.poll_and_process()
        arb_monitor.poll.assert_called_once_with({"last_processed_block": 42, "last_block_hash": "0xdeadbeef"})

    def test_set_state_does_not_double_migrate(self):
        """State with existing cursor keys should not be re-migrated."""
        arb_monitor = MagicMock()
        arb_monitor.config.chain = "arbitrum"
        arb_monitor.poll.return_value = ([], {"last_processed_block": 200})

        engine = MagicMock()
        engine.process_events.return_value = []

        provider = WalletActivityProvider(
            signal_engine=engine,
            wallet_monitors={"arbitrum": arb_monitor},
        )

        # State already has cursor keys -- should not be touched
        new_state = {
            "cursor:arbitrum": {"last_processed_block": 100},
            "last_processed_block": 42,  # stale legacy key
        }
        provider.set_state(new_state)

        state = provider.get_state()
        # Cursor key should remain as-is (not overwritten by legacy)
        assert state["cursor:arbitrum"]["last_processed_block"] == 100
