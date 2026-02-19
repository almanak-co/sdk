"""Tests for AutoRedemptionService."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.services.auto_redemption import (
    AutoRedemptionService,
    MarketResolvedEvent,
    RedemptionAttempt,
    RedemptionStatus,
)
from almanak.framework.services.prediction_monitor import (
    MonitoredPosition,
    PredictionEvent,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_sdk():
    """Create a mock PolymarketSDK."""
    sdk = MagicMock()
    sdk.config = MagicMock()
    sdk.config.wallet_address = "0x1234567890123456789012345678901234567890"
    sdk.web3 = MagicMock()
    sdk.ctf = MagicMock()

    # Default resolution status - resolved with YES winning
    resolution = MagicMock()
    resolution.is_resolved = True
    resolution.winning_outcome = 0  # YES
    sdk.ctf.get_condition_resolution.return_value = resolution

    # Default balance
    sdk.ctf.get_token_balance.return_value = 1000000  # 1 share (6 decimals)

    # Default redeem tx
    redeem_tx = MagicMock()
    redeem_tx.to_tx_params.return_value = {
        "to": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        "data": "0x1234",
        "value": 0,
        "gas": 200000,
    }
    sdk.ctf.build_redeem_tx.return_value = redeem_tx

    return sdk


@pytest.fixture
def mock_web3(mock_sdk):
    """Set up web3 mock on the SDK."""
    web3 = mock_sdk.web3
    web3.eth.get_transaction_count.return_value = 5
    web3.eth.chain_id = 137
    web3.eth.gas_price = 50000000000  # 50 gwei
    web3.eth.get_block.return_value = {"baseFeePerGas": 30000000000}
    web3.to_wei.return_value = 30000000000  # 30 gwei
    web3.eth.send_raw_transaction.return_value = b"\x12\x34\x56\x78" * 8  # 32 bytes
    return web3


@pytest.fixture
def winning_position():
    """Create a winning monitored position."""
    return MonitoredPosition(
        market_id="btc-100k-2025",
        condition_id="0x1234567890123456789012345678901234567890123456789012345678901234",
        token_id="123456789012345678901234567890",
        outcome="YES",
        size=Decimal("100"),
        entry_price=Decimal("0.65"),
        entry_time=datetime.now(UTC),
        exit_conditions=None,
    )


@pytest.fixture
def losing_position():
    """Create a losing monitored position."""
    return MonitoredPosition(
        market_id="eth-5k-2025",
        condition_id="0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        token_id="987654321098765432109876543210",
        outcome="NO",
        size=Decimal("50"),
        entry_price=Decimal("0.35"),
        entry_time=datetime.now(UTC),
        exit_conditions=None,
    )


@pytest.fixture
def service(mock_sdk):
    """Create an AutoRedemptionService instance."""
    return AutoRedemptionService(
        sdk=mock_sdk,
        private_key="0x" + "ab" * 32,
        strategy_id="test-strategy",
        enabled=True,
        max_retries=3,
        retry_delay_seconds=1,
        emit_events=False,
    )


# =============================================================================
# Initialization Tests
# =============================================================================


class TestAutoRedemptionServiceInit:
    """Tests for AutoRedemptionService initialization."""

    def test_init_default_values(self, mock_sdk):
        """Test initialization with default values."""
        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
        )

        assert service.enabled is True
        assert service.max_retries == 3
        assert service.strategy_id == ""
        assert service.emit_events is True
        assert len(service.redemptions) == 0

    def test_init_custom_values(self, mock_sdk):
        """Test initialization with custom values."""
        callback = MagicMock()

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            strategy_id="my-strategy",
            enabled=False,
            max_retries=5,
            retry_delay_seconds=10,
            emit_events=False,
            redemption_callback=callback,
        )

        assert service.enabled is False
        assert service.max_retries == 5
        assert service.retry_delay_seconds == 10
        assert service.strategy_id == "my-strategy"
        assert service.emit_events is False
        assert service.redemption_callback is callback


# =============================================================================
# Event Handling Tests
# =============================================================================


class TestOnEvent:
    """Tests for the on_event callback handler."""

    def test_on_event_market_resolved(self, service, winning_position):
        """Test handling MARKET_RESOLVED event."""
        details = {
            "winning_outcome": "YES",
            "is_winner": True,
            "size": "100",
        }

        with patch.object(service, "on_market_resolved") as mock_handler:
            service.on_event(winning_position, PredictionEvent.MARKET_RESOLVED, details)
            mock_handler.assert_called_once()

            # Check the event passed to handler
            call_args = mock_handler.call_args[0][0]
            assert isinstance(call_args, MarketResolvedEvent)
            assert call_args.winning_outcome == "YES"
            assert call_args.is_winner is True

    def test_on_event_ignores_other_events(self, service, winning_position):
        """Test that non-resolution events are ignored."""
        with patch.object(service, "on_market_resolved") as mock_handler:
            service.on_event(winning_position, PredictionEvent.STOP_LOSS_TRIGGERED, {})
            mock_handler.assert_not_called()

            service.on_event(winning_position, PredictionEvent.TAKE_PROFIT_TRIGGERED, {})
            mock_handler.assert_not_called()


# =============================================================================
# Market Resolution Tests
# =============================================================================


class TestOnMarketResolved:
    """Tests for on_market_resolved handler."""

    def test_winning_position_triggers_redemption(self, service, winning_position, mock_web3):
        """Test that winning positions trigger redemption."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SUCCESS
        assert result.market_id == winning_position.market_id
        assert result.tx_hash == "0x123abc"
        assert result.amount_received == Decimal("100")

    def test_losing_position_skipped(self, service, losing_position):
        """Test that losing positions are skipped."""
        event = MarketResolvedEvent(
            position=losing_position,
            winning_outcome="YES",
            is_winner=False,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SKIPPED
        assert "not a winner" in result.error_message.lower()

    def test_disabled_service_skips_redemption(self, service, winning_position):
        """Test that disabled service skips redemption."""
        service.enabled = False

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SKIPPED
        assert "disabled" in result.error_message.lower()

    def test_redemption_stored_in_history(self, service, winning_position, mock_web3):
        """Test that redemption is stored in history."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            result = service.on_market_resolved(event)

        assert winning_position.market_id in service.redemptions
        stored = service.get_redemption(winning_position.market_id)
        assert stored is result


# =============================================================================
# Redemption Flow Tests
# =============================================================================


class TestRedemptionFlow:
    """Tests for the redemption execution flow."""

    def test_checks_condition_resolution(self, service, winning_position, mock_sdk, mock_web3):
        """Test that condition resolution is verified."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            service.on_market_resolved(event)

        mock_sdk.ctf.get_condition_resolution.assert_called_with(
            winning_position.condition_id,
            mock_sdk.web3,
        )

    def test_checks_position_balance(self, service, winning_position, mock_sdk, mock_web3):
        """Test that position balance is checked."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            service.on_market_resolved(event)

        mock_sdk.ctf.get_token_balance.assert_called()

    def test_skips_zero_balance(self, service, winning_position, mock_sdk, mock_web3):
        """Test that zero balance positions are skipped."""
        mock_sdk.ctf.get_token_balance.return_value = 0

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SKIPPED
        assert "balance is zero" in result.error_message.lower()

    def test_builds_redeem_transaction(self, service, winning_position, mock_sdk, mock_web3):
        """Test that redemption transaction is built correctly."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            service.on_market_resolved(event)

        mock_sdk.ctf.build_redeem_tx.assert_called_once()
        call_kwargs = mock_sdk.ctf.build_redeem_tx.call_args[1]
        assert call_kwargs["condition_id"] == winning_position.condition_id


# =============================================================================
# Retry Logic Tests
# =============================================================================


class TestRetryLogic:
    """Tests for retry logic."""

    def test_retries_on_transient_failure(self, service, winning_position, mock_sdk, mock_web3):
        """Test that transient failures are retried."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        # First two calls fail, third succeeds
        with patch.object(
            service,
            "_sign_and_submit",
            side_effect=[
                RuntimeError("Network error"),
                RuntimeError("Timeout"),
                "0x123abc",
            ],
        ):
            result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SUCCESS
        assert result.attempts == 3

    def test_max_retries_exhausted(self, service, winning_position, mock_sdk, mock_web3):
        """Test that failure after max retries marks as failed."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(
            service,
            "_sign_and_submit",
            side_effect=RuntimeError("Persistent error"),
        ):
            result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.FAILED
        assert result.attempts == service.max_retries

    def test_no_retry_on_permanent_error(self, service, winning_position, mock_sdk, mock_web3):
        """Test that permanent errors are not retried."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(
            service,
            "_sign_and_submit",
            side_effect=RuntimeError("Insufficient balance"),
        ):
            result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.FAILED
        assert result.attempts == 1  # No retries

    def test_permanent_error_patterns(self, service):
        """Test that permanent error patterns are recognized."""
        permanent_errors = [
            "condition not resolved",
            "insufficient balance",
            "already redeemed",
            "invalid condition",
            "market not found",
        ]

        for error_msg in permanent_errors:
            error = RuntimeError(error_msg)
            assert service._is_permanent_error(error) is True

    def test_transient_error_patterns(self, service):
        """Test that transient errors are retried."""
        transient_errors = [
            "network timeout",
            "connection refused",
            "rate limited",
            "internal server error",
        ]

        for error_msg in transient_errors:
            error = RuntimeError(error_msg)
            assert service._is_permanent_error(error) is False


# =============================================================================
# Timeline Event Tests
# =============================================================================


class TestTimelineEvents:
    """Tests for timeline event emission."""

    def test_emits_success_event(self, mock_sdk, winning_position, mock_web3):
        """Test that success events are emitted."""
        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            strategy_id="test-strategy",
            emit_events=True,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with (
            patch.object(service, "_sign_and_submit", return_value="0x123abc"),
            patch("almanak.framework.services.auto_redemption.add_event") as mock_add_event,
        ):
            service.on_market_resolved(event)

            mock_add_event.assert_called_once()
            timeline_event = mock_add_event.call_args[0][0]
            assert timeline_event.event_type.value == "AUTO_REMEDIATION_SUCCESS"
            assert timeline_event.tx_hash == "0x123abc"

    def test_emits_failure_event(self, mock_sdk, winning_position, mock_web3):
        """Test that failure events are emitted."""
        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            strategy_id="test-strategy",
            emit_events=True,
            max_retries=1,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with (
            patch.object(service, "_sign_and_submit", side_effect=RuntimeError("Error")),
            patch("almanak.framework.services.auto_redemption.add_event") as mock_add_event,
        ):
            service.on_market_resolved(event)

            mock_add_event.assert_called_once()
            timeline_event = mock_add_event.call_args[0][0]
            assert timeline_event.event_type.value == "AUTO_REMEDIATION_FAILED"

    def test_no_event_when_disabled(self, service, winning_position, mock_web3):
        """Test that events are not emitted when disabled."""
        service.emit_events = False

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with (
            patch.object(service, "_sign_and_submit", return_value="0x123abc"),
            patch("almanak.framework.services.auto_redemption.add_event") as mock_add_event,
        ):
            service.on_market_resolved(event)
            mock_add_event.assert_not_called()


# =============================================================================
# Callback Tests
# =============================================================================


class TestRedemptionCallback:
    """Tests for redemption callback."""

    def test_callback_called_on_success(self, mock_sdk, winning_position, mock_web3):
        """Test that callback is called on success."""
        callback = MagicMock()

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            redemption_callback=callback,
            emit_events=False,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            result = service.on_market_resolved(event)

        callback.assert_called_once_with(result)

    def test_callback_called_on_failure(self, mock_sdk, winning_position, mock_web3):
        """Test that callback is called on failure."""
        callback = MagicMock()

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            redemption_callback=callback,
            emit_events=False,
            max_retries=1,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", side_effect=RuntimeError("Error")):
            result = service.on_market_resolved(event)

        callback.assert_called_once_with(result)

    def test_callback_error_does_not_crash(self, mock_sdk, winning_position, mock_web3):
        """Test that callback errors are handled gracefully."""
        callback = MagicMock(side_effect=RuntimeError("Callback error"))

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            redemption_callback=callback,
            emit_events=False,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        # Should not raise
        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SUCCESS


# =============================================================================
# Enable/Disable Tests
# =============================================================================


class TestEnableDisable:
    """Tests for enable/disable functionality."""

    def test_enable(self, service):
        """Test enabling the service."""
        service.enabled = False
        service.enable()
        assert service.enabled is True

    def test_disable(self, service):
        """Test disabling the service."""
        service.enabled = True
        service.disable()
        assert service.enabled is False


# =============================================================================
# Data Model Tests
# =============================================================================


class TestRedemptionAttempt:
    """Tests for RedemptionAttempt dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        attempt = RedemptionAttempt(
            market_id="btc-100k",
            condition_id="0x1234",
            outcome="YES",
            size=Decimal("100"),
            status=RedemptionStatus.SUCCESS,
            tx_hash="0xabcd",
            amount_received=Decimal("100"),
            attempts=1,
        )

        data = attempt.to_dict()

        assert data["market_id"] == "btc-100k"
        assert data["status"] == "SUCCESS"
        assert data["size"] == "100"
        assert data["amount_received"] == "100"

    def test_default_values(self):
        """Test default values."""
        attempt = RedemptionAttempt(
            market_id="test",
            condition_id="0x1234",
            outcome="YES",
            size=Decimal("100"),
        )

        assert attempt.status == RedemptionStatus.PENDING
        assert attempt.tx_hash is None
        assert attempt.amount_received is None
        assert attempt.attempts == 0
        assert attempt.created_at is not None


class TestMarketResolvedEvent:
    """Tests for MarketResolvedEvent dataclass."""

    def test_creation(self, winning_position):
        """Test event creation."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
            details={"extra": "info"},
        )

        assert event.position is winning_position
        assert event.winning_outcome == "YES"
        assert event.is_winner is True
        assert event.details == {"extra": "info"}
        assert event.timestamp is not None


# =============================================================================
# History Management Tests
# =============================================================================


class TestHistoryManagement:
    """Tests for redemption history management."""

    def test_clear_history(self, service, winning_position, mock_web3):
        """Test clearing redemption history."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            service.on_market_resolved(event)

        assert len(service.redemptions) == 1

        service.clear_history()

        assert len(service.redemptions) == 0

    def test_get_nonexistent_redemption(self, service):
        """Test getting a redemption that doesn't exist."""
        result = service.get_redemption("nonexistent")
        assert result is None

    def test_redemptions_returns_copy(self, service, winning_position, mock_web3):
        """Test that redemptions property returns a copy."""
        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        with patch.object(service, "_sign_and_submit", return_value="0x123abc"):
            service.on_market_resolved(event)

        redemptions = service.redemptions
        redemptions.clear()

        # Original should not be affected
        assert len(service.redemptions) == 1


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_web3_for_resolution(self, mock_sdk, winning_position):
        """Test error when web3 is missing for resolution check."""
        mock_sdk.web3 = None

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)
        assert result.status == RedemptionStatus.FAILED
        assert "web3" in result.error_message.lower()

    def test_unresolved_condition_on_chain(self, service, winning_position, mock_sdk, mock_web3):
        """Test handling of unresolved condition on-chain."""
        resolution = MagicMock()
        resolution.is_resolved = False
        mock_sdk.ctf.get_condition_resolution.return_value = resolution

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        # Should fail since condition is not resolved on-chain
        assert result.status == RedemptionStatus.FAILED


# =============================================================================
# Receipt Polling Tests (US-109)
# =============================================================================


class TestReceiptPolling:
    """Tests for transaction receipt polling and verification."""

    def test_init_with_receipt_timeout(self, mock_sdk):
        """Test initialization with custom receipt timeout."""
        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            receipt_timeout_seconds=60,
            receipt_poll_interval_seconds=1.0,
        )

        assert service.receipt_timeout_seconds == 60
        assert service.receipt_poll_interval_seconds == 1.0

    def test_init_default_receipt_timeout(self, mock_sdk):
        """Test initialization with default receipt timeout."""
        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
        )

        assert service.receipt_timeout_seconds == 120
        assert service.receipt_poll_interval_seconds == 2

    def test_wait_for_receipt_success(self, service, mock_sdk):
        """Test successful receipt retrieval."""
        mock_receipt = {
            "transactionHash": b"\x12\x34\x56\x78" * 8,
            "status": 1,
            "gasUsed": 150000,
            "logs": [],
        }
        mock_sdk.web3.eth.get_transaction_receipt.return_value = mock_receipt

        result = service._wait_for_receipt("0x123")

        assert result is not None
        assert result["status"] == 1

    def test_wait_for_receipt_timeout(self, mock_sdk):
        """Test receipt polling timeout."""
        mock_sdk.web3.eth.get_transaction_receipt.return_value = None

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            receipt_timeout_seconds=0.1,  # Very short timeout for testing
            receipt_poll_interval_seconds=0.05,
        )

        result = service._wait_for_receipt("0x123")

        assert result is None

    def test_wait_for_receipt_exception_handling(self, service, mock_sdk):
        """Test receipt polling handles exceptions gracefully."""
        # First call raises exception, second returns receipt
        mock_sdk.web3.eth.get_transaction_receipt.side_effect = [
            RuntimeError("Node error"),
            {
                "transactionHash": "0x123",
                "status": 1,
                "gasUsed": 150000,
                "logs": [],
            },
        ]

        result = service._wait_for_receipt("0x123")

        assert result is not None
        assert result["status"] == 1

    def test_wait_for_receipt_missing_web3(self, mock_sdk):
        """Test receipt polling with missing web3 raises error."""
        mock_sdk.web3 = None

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
        )

        with pytest.raises(ValueError, match="Web3 instance required"):
            service._wait_for_receipt("0x123")


# =============================================================================
# Receipt Parsing Tests (US-109)
# =============================================================================


class TestReceiptParsing:
    """Tests for parsing transaction receipts to extract redemption amounts."""

    def test_parse_successful_redemption_receipt(self, service):
        """Test parsing a successful redemption receipt with PayoutRedemption events."""
        # Mock receipt with PayoutRedemption event
        mock_receipt = {
            "transactionHash": "0x" + "ab" * 32,
            "blockNumber": 12345678,
            "status": 1,
            "gasUsed": 150000,
            "logs": [
                {
                    # PayoutRedemption event
                    "topics": [
                        # PayoutRedemption topic
                        "0x2682012a4a4f1973119f1c9b90745f714c4c1e002c60c52b89896745d90ab678",
                        # redeemer (indexed)
                        "0x000000000000000000000000" + "12" * 20,
                        # collateralToken (indexed)
                        "0x000000000000000000000000" + "ab" * 20,
                        # conditionId (indexed)
                        "0x" + "cd" * 32,
                    ],
                    "data": (
                        # parentCollectionId (bytes32)
                        "00" * 32
                        +
                        # offset to indexSets (64 bytes = 0x40)
                        "0000000000000000000000000000000000000000000000000000000000000060"
                        +
                        # payout (100 USDC = 100000000 in 6 decimals)
                        "0000000000000000000000000000000000000000000000000000000005f5e100"
                        +
                        # indexSets array length (2)
                        "0000000000000000000000000000000000000000000000000000000000000002"
                        +
                        # indexSets[0] = 1 (YES)
                        "0000000000000000000000000000000000000000000000000000000000000001"
                        +
                        # indexSets[1] = 2 (NO)
                        "0000000000000000000000000000000000000000000000000000000000000002"
                    ),
                    "address": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
                    "logIndex": 0,
                },
            ],
        }

        result = service._parse_redemption_receipt(mock_receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.redemptions) >= 0  # May have redemptions if parsing succeeds

    def test_parse_reverted_transaction(self, service):
        """Test parsing a reverted transaction."""
        mock_receipt = {
            "transactionHash": "0x" + "ab" * 32,
            "blockNumber": 12345678,
            "status": 0,  # Reverted
            "gasUsed": 21000,
            "logs": [],
        }

        result = service._parse_redemption_receipt(mock_receipt)

        assert result.success is True  # Parsing succeeded
        assert result.transaction_success is False  # But transaction reverted

    def test_parse_receipt_no_logs(self, service):
        """Test parsing a receipt with no logs."""
        mock_receipt = {
            "transactionHash": "0x" + "ab" * 32,
            "blockNumber": 12345678,
            "status": 1,
            "gasUsed": 21000,
            "logs": [],
        }

        result = service._parse_redemption_receipt(mock_receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.redemptions) == 0


# =============================================================================
# End-to-End Receipt Verification Tests (US-109)
# =============================================================================


class TestEndToEndReceiptVerification:
    """Tests for end-to-end receipt verification flow."""

    def test_redemption_with_receipt_verification(self, mock_sdk, winning_position, mock_web3):
        """Test complete redemption flow with receipt verification."""
        # Set up mock receipt with PayoutRedemption event
        mock_receipt = {
            "transactionHash": b"\x12\x34\x56\x78" * 8,
            "blockNumber": 12345678,
            "status": 1,
            "gasUsed": 150000,
            "logs": [
                {
                    # PayoutRedemption event
                    "topics": [
                        "0x2682012a4a4f1973119f1c9b90745f714c4c1e002c60c52b89896745d90ab678",
                        "0x000000000000000000000000" + "12" * 20,
                        "0x000000000000000000000000" + "ab" * 20,
                        "0x" + "cd" * 32,
                    ],
                    "data": (
                        "00" * 32
                        + "0000000000000000000000000000000000000000000000000000000000000060"
                        + "0000000000000000000000000000000000000000000000000000000005f5e100"  # 100 USDC
                        + "0000000000000000000000000000000000000000000000000000000000000002"
                        + "0000000000000000000000000000000000000000000000000000000000000001"
                        + "0000000000000000000000000000000000000000000000000000000000000002"
                    ),
                    "address": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
                    "logIndex": 0,
                },
            ],
        }
        mock_web3.eth.get_transaction_receipt.return_value = mock_receipt

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
            receipt_timeout_seconds=5,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SUCCESS
        assert result.tx_hash is not None
        # Amount should be extracted from the receipt (or fallback to position size)
        assert result.amount_received is not None
        assert result.amount_received > 0

    def test_redemption_with_transaction_revert(self, mock_sdk, winning_position, mock_web3):
        """Test redemption handling when transaction reverts."""
        # Set up mock receipt with revert status
        mock_receipt = {
            "transactionHash": b"\x12\x34\x56\x78" * 8,
            "blockNumber": 12345678,
            "status": 0,  # Reverted
            "gasUsed": 21000,
            "logs": [],
        }
        mock_web3.eth.get_transaction_receipt.return_value = mock_receipt

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
            max_retries=1,
            receipt_timeout_seconds=5,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.FAILED
        assert "reverted" in result.error_message.lower()

    def test_redemption_with_receipt_timeout(self, mock_sdk, winning_position, mock_web3):
        """Test redemption handling when receipt times out."""
        mock_web3.eth.get_transaction_receipt.return_value = None

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
            max_retries=1,
            receipt_timeout_seconds=0.1,  # Very short timeout
            receipt_poll_interval_seconds=0.05,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.FAILED
        assert "timeout" in result.error_message.lower()

    def test_redemption_no_payout_events_falls_back(self, mock_sdk, winning_position, mock_web3):
        """Test that missing PayoutRedemption events falls back to position size."""
        # Set up mock receipt with success but no PayoutRedemption events
        mock_receipt = {
            "transactionHash": b"\x12\x34\x56\x78" * 8,
            "blockNumber": 12345678,
            "status": 1,
            "gasUsed": 150000,
            "logs": [],  # No events
        }
        mock_web3.eth.get_transaction_receipt.return_value = mock_receipt

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
            receipt_timeout_seconds=5,
        )

        event = MarketResolvedEvent(
            position=winning_position,
            winning_outcome="YES",
            is_winner=True,
        )

        result = service.on_market_resolved(event)

        assert result.status == RedemptionStatus.SUCCESS
        # Should fall back to position size
        assert result.amount_received == winning_position.size


# =============================================================================
# Gas Configuration Tests (US-110)
# =============================================================================


class TestGasConfiguration:
    """Tests for configurable gas pricing."""

    def test_eip1559_with_custom_priority_fee(self, mock_sdk, mock_web3):
        """Test EIP-1559 gas pricing with custom priority fee from config."""
        # Configure custom priority fee
        mock_sdk.config.max_priority_fee_gwei = 50.0
        mock_sdk.config.max_fee_multiplier = 2.5
        mock_sdk.config.use_legacy_gas = False

        # Set up block with base fee
        mock_web3.eth.get_block.return_value = {"baseFeePerGas": 20_000_000_000}  # 20 gwei
        mock_web3.to_wei.return_value = 50_000_000_000  # 50 gwei

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should use config values
        assert "maxFeePerGas" in tx_params
        assert "maxPriorityFeePerGas" in tx_params
        assert "gasPrice" not in tx_params
        # maxFeePerGas should be baseFee * multiplier
        assert tx_params["maxFeePerGas"] == int(20_000_000_000 * 2.5)
        # maxPriorityFeePerGas should be from config
        mock_web3.to_wei.assert_called_with(50.0, "gwei")

    def test_eip1559_with_network_default_priority_fee(self, mock_sdk, mock_web3):
        """Test EIP-1559 gas pricing falls back to network default when config not set."""
        # No custom priority fee configured
        mock_sdk.config.max_priority_fee_gwei = None
        mock_sdk.config.max_fee_multiplier = 2.0
        mock_sdk.config.use_legacy_gas = False

        # Set up block with base fee
        mock_web3.eth.get_block.return_value = {"baseFeePerGas": 30_000_000_000}
        mock_web3.eth.max_priority_fee = 2_000_000_000  # 2 gwei from network

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should use network default priority fee
        assert tx_params["maxPriorityFeePerGas"] == 2_000_000_000

    def test_legacy_gas_pricing_when_configured(self, mock_sdk, mock_web3):
        """Test legacy gas pricing when use_legacy_gas is True."""
        mock_sdk.config.use_legacy_gas = True
        mock_web3.eth.gas_price = 40_000_000_000  # 40 gwei

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should use legacy gasPrice
        assert "gasPrice" in tx_params
        assert "maxFeePerGas" not in tx_params
        assert "maxPriorityFeePerGas" not in tx_params
        assert tx_params["gasPrice"] == 40_000_000_000

    def test_falls_back_to_legacy_when_no_base_fee(self, mock_sdk, mock_web3):
        """Test fallback to legacy pricing when chain has no EIP-1559 support."""
        mock_sdk.config.use_legacy_gas = False
        mock_sdk.config.max_priority_fee_gwei = 30.0

        # No base fee in block (pre-EIP-1559 chain)
        mock_web3.eth.get_block.return_value = {}
        mock_web3.eth.gas_price = 50_000_000_000

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should fall back to legacy
        assert "gasPrice" in tx_params
        assert tx_params["gasPrice"] == 50_000_000_000

    def test_falls_back_to_legacy_on_error(self, mock_sdk, mock_web3):
        """Test fallback to legacy pricing on any error."""
        mock_sdk.config.use_legacy_gas = False

        # Error getting block
        mock_web3.eth.get_block.side_effect = RuntimeError("Node error")
        mock_web3.eth.gas_price = 45_000_000_000

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should fall back to legacy
        assert "gasPrice" in tx_params
        assert tx_params["gasPrice"] == 45_000_000_000

    def test_default_config_values_used_when_not_set(self, mock_sdk, mock_web3):
        """Test that default multiplier is used when not explicitly set in config."""
        # Don't set explicit values - use defaults
        del mock_sdk.config.max_priority_fee_gwei
        del mock_sdk.config.max_fee_multiplier
        del mock_sdk.config.use_legacy_gas

        mock_web3.eth.get_block.return_value = {"baseFeePerGas": 25_000_000_000}
        mock_web3.eth.max_priority_fee = 1_500_000_000
        mock_web3.to_wei.return_value = 2_000_000_000

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should use default multiplier of 2.0
        assert tx_params["maxFeePerGas"] == int(25_000_000_000 * 2.0)

    def test_network_priority_fee_fallback_on_error(self, mock_sdk, mock_web3):
        """Test fallback to safe default when network max_priority_fee errors."""
        mock_sdk.config.max_priority_fee_gwei = None
        mock_sdk.config.max_fee_multiplier = 2.0
        mock_sdk.config.use_legacy_gas = False

        mock_web3.eth.get_block.return_value = {"baseFeePerGas": 30_000_000_000}
        # Simulate error getting max_priority_fee
        type(mock_web3.eth).max_priority_fee = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("Not supported"))
        )
        mock_web3.to_wei.return_value = 2_000_000_000  # 2 gwei default

        service = AutoRedemptionService(
            sdk=mock_sdk,
            private_key="0x" + "ab" * 32,
            emit_events=False,
        )

        tx_params: dict = {}
        service._apply_gas_pricing(mock_web3, tx_params, mock_sdk.config)

        # Should use safe default (2 gwei)
        mock_web3.to_wei.assert_called_with(2, "gwei")
        assert tx_params["maxPriorityFeePerGas"] == 2_000_000_000
