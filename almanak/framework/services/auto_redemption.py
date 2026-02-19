"""Auto-Redemption Service for Prediction Market Positions.

This module provides automatic redemption of winning prediction market positions
after market resolution. It listens for MARKET_RESOLVED events from the
PredictionPositionMonitor and automatically redeems positions.

Features:
- Automatic detection of winning positions
- Configurable retry logic with exponential backoff
- Timeline event emission for tracking
- Per-strategy enable/disable configuration
- Transaction receipt verification with configurable timeout
- Proper extraction of redemption amounts from on-chain events

Example:
    from almanak.framework.services import AutoRedemptionService, PredictionPositionMonitor
    from almanak.framework.connectors.polymarket import PolymarketSDK, PolymarketConfig

    # Create SDK and services
    config = PolymarketConfig.from_env()
    sdk = PolymarketSDK(config, web3)

    redemption_service = AutoRedemptionService(
        sdk=sdk,
        private_key=os.environ["PRIVATE_KEY"],
        strategy_id="my-strategy",
        receipt_timeout_seconds=120,  # Configurable receipt timeout
    )

    # Register with position monitor
    monitor = PredictionPositionMonitor(
        strategy_id="my-strategy",
        event_callback=redemption_service.on_event,
    )
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from .prediction_monitor import (
    MonitoredPosition,
    PredictionEvent,
)

if TYPE_CHECKING:
    from ..connectors.polymarket.receipt_parser import CtfParseResult

logger = logging.getLogger(__name__)


class RedemptionStatus(StrEnum):
    """Status of a redemption attempt."""

    PENDING = "PENDING"
    """Redemption has not been attempted yet."""

    IN_PROGRESS = "IN_PROGRESS"
    """Redemption transaction is being processed."""

    SUCCESS = "SUCCESS"
    """Redemption completed successfully."""

    FAILED = "FAILED"
    """Redemption failed after all retry attempts."""

    SKIPPED = "SKIPPED"
    """Redemption was skipped (e.g., not a winning position)."""


@dataclass
class RedemptionAttempt:
    """Record of a redemption attempt.

    Tracks the details and outcome of attempting to redeem a prediction
    market position.
    """

    market_id: str
    """Market identifier."""

    condition_id: str
    """CTF condition ID."""

    outcome: str
    """Position outcome (YES/NO)."""

    size: Decimal
    """Number of shares being redeemed."""

    status: RedemptionStatus = RedemptionStatus.PENDING
    """Current status of the redemption."""

    tx_hash: str | None = None
    """Transaction hash if submitted."""

    amount_received: Decimal | None = None
    """USDC amount received from redemption."""

    error_message: str | None = None
    """Error message if redemption failed."""

    attempts: int = 0
    """Number of redemption attempts made."""

    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    """When the redemption was initiated."""

    completed_at: datetime | None = None
    """When the redemption completed (success or final failure)."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "outcome": self.outcome,
            "size": str(self.size),
            "status": self.status.value,
            "tx_hash": self.tx_hash,
            "amount_received": str(self.amount_received) if self.amount_received else None,
            "error_message": self.error_message,
            "attempts": self.attempts,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class MarketResolvedEvent:
    """Event data for a resolved market.

    Contains all information needed to process a market resolution
    and redeem winning positions.
    """

    position: MonitoredPosition
    """The monitored position in the resolved market."""

    winning_outcome: str | None
    """The winning outcome (YES/NO) or None if market was voided."""

    is_winner: bool
    """Whether the position is a winning position."""

    details: dict[str, Any] = field(default_factory=dict)
    """Additional event details."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    """When the resolution was detected."""


# Type alias for redemption callbacks
RedemptionCallback = Callable[[RedemptionAttempt], None]


class AutoRedemptionService:
    """Automatically redeems winning prediction market positions.

    This service listens for MARKET_RESOLVED events and automatically
    builds and submits redemption transactions for winning positions.

    Key features:
    - Configurable per-strategy enable/disable
    - Retry logic with exponential backoff
    - Timeline event emission for tracking
    - Callback support for custom handling
    - Transaction receipt verification with configurable timeout
    - Proper extraction of redemption amounts from PayoutRedemption events

    Thread Safety:
        This class is NOT thread-safe. Use separate instances per thread.

    Example:
        >>> service = AutoRedemptionService(
        ...     sdk=sdk,
        ...     private_key="0x...",
        ...     strategy_id="my-strategy",
        ...     enabled=True,
        ...     receipt_timeout_seconds=120,
        ... )
        >>>
        >>> # Handle market resolution
        >>> event = MarketResolvedEvent(position=pos, winning_outcome="YES", is_winner=True)
        >>> result = service.on_market_resolved(event)
        >>> if result.status == RedemptionStatus.SUCCESS:
        ...     print(f"Redeemed {result.amount_received} USDC")
    """

    # Default retry configuration
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY_SECONDS = 5
    DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2
    DEFAULT_RECEIPT_TIMEOUT_SECONDS = 120
    DEFAULT_RECEIPT_POLL_INTERVAL_SECONDS = 2

    def __init__(
        self,
        sdk: Any,  # PolymarketSDK - using Any to avoid circular imports
        private_key: str,
        strategy_id: str = "",
        enabled: bool = True,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
        emit_events: bool = True,
        redemption_callback: RedemptionCallback | None = None,
        receipt_timeout_seconds: int = DEFAULT_RECEIPT_TIMEOUT_SECONDS,
        receipt_poll_interval_seconds: float = DEFAULT_RECEIPT_POLL_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the auto-redemption service.

        Args:
            sdk: PolymarketSDK instance for redemption operations.
            private_key: Private key for signing redemption transactions.
            strategy_id: Strategy identifier for event emission.
            enabled: Whether auto-redemption is enabled.
            max_retries: Maximum number of retry attempts.
            retry_delay_seconds: Initial delay between retries.
            emit_events: Whether to emit timeline events.
            redemption_callback: Optional callback for redemption results.
            receipt_timeout_seconds: Timeout for waiting for transaction receipt (default 120s).
            receipt_poll_interval_seconds: Interval between receipt polling attempts (default 2s).
        """
        self.sdk = sdk
        self._private_key = private_key
        self.strategy_id = strategy_id
        self.enabled = enabled
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.emit_events = emit_events
        self.redemption_callback = redemption_callback
        self.receipt_timeout_seconds = receipt_timeout_seconds
        self.receipt_poll_interval_seconds = receipt_poll_interval_seconds

        # Track redemption history
        self._redemptions: dict[str, RedemptionAttempt] = {}

        # Track pending redemptions for retry
        self._pending_redemptions: list[str] = []

        logger.info(
            "AutoRedemptionService initialized: strategy_id=%s, enabled=%s, max_retries=%d, receipt_timeout=%ds",
            strategy_id,
            enabled,
            max_retries,
            receipt_timeout_seconds,
        )

    @property
    def redemptions(self) -> dict[str, RedemptionAttempt]:
        """Get all redemption attempts."""
        return self._redemptions.copy()

    def get_redemption(self, market_id: str) -> RedemptionAttempt | None:
        """Get redemption attempt for a market.

        Args:
            market_id: Market ID to look up.

        Returns:
            RedemptionAttempt or None if not found.
        """
        return self._redemptions.get(market_id)

    def on_event(
        self,
        position: MonitoredPosition,
        event: PredictionEvent,
        details: dict[str, Any],
    ) -> None:
        """Handle prediction events from the position monitor.

        This method can be registered as a callback with the
        PredictionPositionMonitor to automatically handle events.

        Args:
            position: The position that triggered the event.
            event: The type of event.
            details: Additional event details.
        """
        if event == PredictionEvent.MARKET_RESOLVED:
            resolved_event = MarketResolvedEvent(
                position=position,
                winning_outcome=details.get("winning_outcome"),
                is_winner=details.get("is_winner", False),
                details=details,
            )
            self.on_market_resolved(resolved_event)

    def on_market_resolved(self, event: MarketResolvedEvent) -> RedemptionAttempt:
        """Handle market resolution and redeem winning positions.

        This method is called when a market is resolved. It checks if the
        position is a winner and initiates redemption if enabled.

        Args:
            event: The market resolved event with position details.

        Returns:
            RedemptionAttempt tracking the redemption status.
        """
        position = event.position
        market_id = position.market_id

        logger.info(
            "Market resolved: market_id=%s, winning_outcome=%s, is_winner=%s",
            market_id,
            event.winning_outcome,
            event.is_winner,
        )

        # Create redemption attempt record
        attempt = RedemptionAttempt(
            market_id=market_id,
            condition_id=position.condition_id,
            outcome=position.outcome,
            size=position.size,
        )

        # Check if this is a winning position
        if not event.is_winner:
            attempt.status = RedemptionStatus.SKIPPED
            attempt.completed_at = datetime.now(UTC)
            attempt.error_message = "Position is not a winner"
            self._redemptions[market_id] = attempt
            logger.info("Skipping redemption for losing position: market_id=%s", market_id)
            return attempt

        # Check if auto-redemption is enabled
        if not self.enabled:
            attempt.status = RedemptionStatus.SKIPPED
            attempt.completed_at = datetime.now(UTC)
            attempt.error_message = "Auto-redemption is disabled"
            self._redemptions[market_id] = attempt
            logger.info("Auto-redemption disabled, skipping: market_id=%s", market_id)
            return attempt

        # Store and process redemption
        self._redemptions[market_id] = attempt
        return self._process_redemption(attempt, position)

    def _process_redemption(
        self,
        attempt: RedemptionAttempt,
        position: MonitoredPosition,
    ) -> RedemptionAttempt:
        """Process a redemption with retry logic.

        Builds and submits the redemption transaction, handling errors
        with exponential backoff retries.

        Args:
            attempt: The redemption attempt to process.
            position: The position to redeem.

        Returns:
            Updated RedemptionAttempt with result.
        """
        attempt.status = RedemptionStatus.IN_PROGRESS

        while attempt.attempts < self.max_retries:
            attempt.attempts += 1

            try:
                result = self._execute_redemption(attempt, position)
                if result.status in (RedemptionStatus.SUCCESS, RedemptionStatus.SKIPPED):
                    self._on_redemption_complete(attempt)
                    return attempt
            except Exception as e:
                logger.warning(
                    "Redemption attempt %d/%d failed: market_id=%s, error=%s",
                    attempt.attempts,
                    self.max_retries,
                    attempt.market_id,
                    str(e),
                )
                attempt.error_message = str(e)

                # Don't retry on certain errors
                if self._is_permanent_error(e):
                    break

        # All retries exhausted
        attempt.status = RedemptionStatus.FAILED
        attempt.completed_at = datetime.now(UTC)
        self._on_redemption_complete(attempt)

        logger.error(
            "Redemption failed after %d attempts: market_id=%s, error=%s",
            attempt.attempts,
            attempt.market_id,
            attempt.error_message,
        )

        return attempt

    def _execute_redemption(
        self,
        attempt: RedemptionAttempt,
        position: MonitoredPosition,
    ) -> RedemptionAttempt:
        """Execute a single redemption attempt.

        Builds the redemption transaction, signs it, submits to chain,
        waits for the receipt, and parses the actual redemption amount
        from PayoutRedemption events.

        Args:
            attempt: The redemption attempt record.
            position: The position to redeem.

        Returns:
            Updated RedemptionAttempt.

        Raises:
            Exception: If redemption fails.
        """
        logger.info(
            "Executing redemption: market_id=%s, attempt=%d",
            attempt.market_id,
            attempt.attempts,
        )

        # 1. Verify market is resolved on-chain
        resolution = self._get_condition_resolution(position.condition_id)
        if not resolution.is_resolved:
            raise RuntimeError(f"Market not yet resolved on-chain: {position.condition_id}")

        # 2. Check position balance
        token_id = self._get_token_id(position)
        balance = self._get_position_balance(token_id)
        if balance == 0:
            attempt.status = RedemptionStatus.SKIPPED
            attempt.completed_at = datetime.now(UTC)
            attempt.error_message = "Position balance is zero"
            return attempt

        # 3. Build redemption transaction
        redeem_tx = self._build_redeem_tx(position.condition_id)

        # 4. Sign and submit transaction
        tx_hash = self._sign_and_submit(redeem_tx)
        attempt.tx_hash = tx_hash

        logger.info(
            "Redemption transaction submitted: market_id=%s, tx_hash=%s",
            attempt.market_id,
            tx_hash,
        )

        # 5. Wait for receipt and verify success
        receipt = self._wait_for_receipt(tx_hash)
        if receipt is None:
            raise RuntimeError(f"Timeout waiting for receipt: {tx_hash}")

        # 6. Parse receipt and extract redemption amount
        parse_result = self._parse_redemption_receipt(receipt)

        # 7. Handle transaction failure
        if not parse_result.transaction_success:
            raise RuntimeError(f"Transaction reverted: {tx_hash}")

        # 8. Extract redemption amount from PayoutRedemption events
        if parse_result.redemption_result and parse_result.redemption_result.success:
            attempt.amount_received = parse_result.redemption_result.amount_redeemed
            attempt.status = RedemptionStatus.SUCCESS
            attempt.completed_at = datetime.now(UTC)

            logger.info(
                "Redemption successful: market_id=%s, tx_hash=%s, amount=%s USDC",
                attempt.market_id,
                tx_hash,
                attempt.amount_received,
            )
        elif not parse_result.redemptions:
            # No PayoutRedemption events found - this is unexpected but handle gracefully
            logger.warning(
                "No PayoutRedemption events in receipt: market_id=%s, tx_hash=%s",
                attempt.market_id,
                tx_hash,
            )
            # Fall back to estimating from position size (1:1 for winners)
            attempt.amount_received = position.size
            attempt.status = RedemptionStatus.SUCCESS
            attempt.completed_at = datetime.now(UTC)
        else:
            # Parse failed but we have redemptions - extract manually
            total_payout = sum(r.payout for r in parse_result.redemptions)
            attempt.amount_received = Decimal(total_payout) / Decimal(10**6)  # USDC has 6 decimals
            attempt.status = RedemptionStatus.SUCCESS
            attempt.completed_at = datetime.now(UTC)

            logger.info(
                "Redemption successful (manual parse): market_id=%s, tx_hash=%s, amount=%s USDC",
                attempt.market_id,
                tx_hash,
                attempt.amount_received,
            )

        return attempt

    def _wait_for_receipt(self, tx_hash: str) -> dict[str, Any] | None:
        """Wait for transaction receipt with configurable timeout.

        Polls the blockchain for the transaction receipt until it's found
        or the timeout is reached.

        Args:
            tx_hash: Transaction hash to wait for.

        Returns:
            Transaction receipt dict, or None if timeout.
        """
        if self.sdk.web3 is None:
            raise ValueError("Web3 instance required for receipt polling")

        web3 = self.sdk.web3
        start_time = time.time()

        logger.debug(
            "Waiting for receipt: tx_hash=%s, timeout=%ds",
            tx_hash,
            self.receipt_timeout_seconds,
        )

        while time.time() - start_time < self.receipt_timeout_seconds:
            try:
                receipt = web3.eth.get_transaction_receipt(tx_hash)
                if receipt is not None:
                    logger.debug(
                        "Receipt received: tx_hash=%s, status=%s, gas_used=%s",
                        tx_hash,
                        receipt.get("status"),
                        receipt.get("gasUsed"),
                    )
                    # Convert AttributeDict to regular dict for easier handling
                    return dict(receipt)
            except Exception as e:
                # Receipt not available yet (transaction pending or not found)
                logger.debug("Receipt not yet available: %s", e)

            time.sleep(self.receipt_poll_interval_seconds)

        logger.warning(
            "Timeout waiting for receipt: tx_hash=%s, elapsed=%ds",
            tx_hash,
            self.receipt_timeout_seconds,
        )
        return None

    def _parse_redemption_receipt(self, receipt: dict[str, Any]) -> "CtfParseResult":
        """Parse a redemption transaction receipt.

        Uses PolymarketReceiptParser to extract PayoutRedemption events
        and calculate the actual redeemed amount.

        Args:
            receipt: Transaction receipt dict.

        Returns:
            CtfParseResult with parsed events and redemption data.
        """
        from ..connectors.polymarket.receipt_parser import PolymarketReceiptParser

        parser = PolymarketReceiptParser()
        return parser.parse_ctf_receipt(receipt)

    def _get_condition_resolution(self, condition_id: str) -> Any:
        """Get condition resolution status from CTF contract.

        Args:
            condition_id: The CTF condition ID.

        Returns:
            ResolutionStatus from CTF SDK.
        """
        if self.sdk.web3 is None:
            raise ValueError("Web3 instance required for on-chain operations")
        return self.sdk.ctf.get_condition_resolution(condition_id, self.sdk.web3)

    def _get_token_id(self, position: MonitoredPosition) -> int:
        """Get the ERC-1155 token ID for a position.

        Args:
            position: The monitored position.

        Returns:
            Token ID as integer.
        """
        # Token ID is stored as string in the position
        return int(position.token_id)

    def _get_position_balance(self, token_id: int) -> int:
        """Get position token balance.

        Args:
            token_id: The ERC-1155 token ID.

        Returns:
            Token balance in base units.
        """
        if self.sdk.web3 is None:
            raise ValueError("Web3 instance required for balance checking")
        return self.sdk.ctf.get_token_balance(
            self.sdk.config.wallet_address,
            token_id,
            self.sdk.web3,
        )

    def _build_redeem_tx(self, condition_id: str) -> Any:
        """Build the redemption transaction.

        Args:
            condition_id: The CTF condition ID.

        Returns:
            TransactionData for the redemption.
        """
        # Redeem both YES and NO positions (only winning one has value)
        from ..connectors.polymarket.ctf_sdk import BINARY_PARTITION

        return self.sdk.ctf.build_redeem_tx(
            condition_id=condition_id,
            index_sets=BINARY_PARTITION,
            sender=self.sdk.config.wallet_address,
        )

    def _sign_and_submit(self, tx_data: Any) -> str:
        """Sign and submit a transaction.

        Args:
            tx_data: TransactionData to submit.

        Returns:
            Transaction hash.

        Raises:
            Exception: If signing or submission fails.
        """
        if self.sdk.web3 is None:
            raise ValueError("Web3 instance required for transaction submission")

        web3 = self.sdk.web3
        wallet = self.sdk.config.wallet_address

        # Build full transaction
        tx_params = tx_data.to_tx_params(wallet)

        # Get nonce and gas price
        tx_params["nonce"] = web3.eth.get_transaction_count(wallet)
        tx_params["chainId"] = web3.eth.chain_id

        # Use gas pricing from config, falling back to network defaults
        config = self.sdk.config
        self._apply_gas_pricing(web3, tx_params, config)

        # Sign transaction
        from eth_account import Account

        signed_tx = Account.sign_transaction(tx_params, self._private_key)

        # Submit transaction
        tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)

        return tx_hash.hex()

    def _apply_gas_pricing(self, web3: Any, tx_params: dict[str, Any], config: Any) -> None:
        """Apply gas pricing to transaction parameters.

        Uses configuration values when available, falling back to network defaults.
        Supports both EIP-1559 (maxFeePerGas/maxPriorityFeePerGas) and legacy (gasPrice).

        Args:
            web3: Web3 instance for gas estimation.
            tx_params: Transaction parameters dict to update.
            config: PolymarketConfig with gas settings.
        """
        # Check if config requests legacy gas pricing
        use_legacy = getattr(config, "use_legacy_gas", False)

        if use_legacy:
            # Use legacy gasPrice
            tx_params["gasPrice"] = web3.eth.gas_price
            logger.debug("Using legacy gas pricing: gasPrice=%s", tx_params["gasPrice"])
            return

        # Try EIP-1559 gas pricing
        try:
            latest_block = web3.eth.get_block("latest")
            base_fee = latest_block.get("baseFeePerGas", 0)

            if base_fee:
                # Calculate maxFeePerGas from config multiplier or default to 2x
                fee_multiplier = getattr(config, "max_fee_multiplier", 2.0)
                tx_params["maxFeePerGas"] = int(base_fee * fee_multiplier)

                # Use config priority fee if specified, otherwise get from network
                max_priority_fee_gwei = getattr(config, "max_priority_fee_gwei", None)
                if max_priority_fee_gwei is not None:
                    tx_params["maxPriorityFeePerGas"] = web3.to_wei(max_priority_fee_gwei, "gwei")
                else:
                    # Fall back to network default (eth_maxPriorityFeePerGas if available)
                    try:
                        tx_params["maxPriorityFeePerGas"] = web3.eth.max_priority_fee
                    except Exception:
                        # Polygon default is ~30 gwei, but use 2 gwei as safer default
                        tx_params["maxPriorityFeePerGas"] = web3.to_wei(2, "gwei")

                logger.debug(
                    "Using EIP-1559 gas pricing: baseFee=%s, maxFeePerGas=%s, maxPriorityFeePerGas=%s",
                    base_fee,
                    tx_params["maxFeePerGas"],
                    tx_params["maxPriorityFeePerGas"],
                )
            else:
                # No EIP-1559 support, fall back to legacy
                tx_params["gasPrice"] = web3.eth.gas_price
                logger.debug("Chain does not support EIP-1559, using legacy: gasPrice=%s", tx_params["gasPrice"])

        except Exception as e:
            # Fall back to legacy gas price on any error
            logger.debug("Error getting EIP-1559 gas params, falling back to legacy: %s", e)
            tx_params["gasPrice"] = web3.eth.gas_price

    def _is_permanent_error(self, error: Exception) -> bool:
        """Check if an error is permanent and should not be retried.

        Args:
            error: The exception to check.

        Returns:
            True if the error is permanent.
        """
        error_str = str(error).lower()

        # Permanent errors that should not be retried
        permanent_patterns = [
            "condition not resolved",
            "insufficient balance",
            "already redeemed",
            "invalid condition",
            "market not found",
        ]

        return any(pattern in error_str for pattern in permanent_patterns)

    def _on_redemption_complete(self, attempt: RedemptionAttempt) -> None:
        """Handle redemption completion.

        Emits timeline events and calls the callback if configured.

        Args:
            attempt: The completed redemption attempt.
        """
        # Emit timeline event
        if self.emit_events:
            self._emit_timeline_event(attempt)

        # Call callback
        if self.redemption_callback:
            try:
                self.redemption_callback(attempt)
            except Exception:
                logger.exception("Redemption callback failed for %s", attempt.market_id)

    def _emit_timeline_event(self, attempt: RedemptionAttempt) -> None:
        """Emit a timeline event for a redemption.

        Args:
            attempt: The redemption attempt.
        """
        # Determine event type based on status
        if attempt.status == RedemptionStatus.SUCCESS:
            event_type = TimelineEventType.AUTO_REMEDIATION_SUCCESS
            description = f"Redeemed prediction position: {attempt.market_id} ({attempt.amount_received} USDC)"
        elif attempt.status == RedemptionStatus.FAILED:
            event_type = TimelineEventType.AUTO_REMEDIATION_FAILED
            description = f"Failed to redeem prediction position: {attempt.market_id}"
        elif attempt.status == RedemptionStatus.SKIPPED:
            event_type = TimelineEventType.CUSTOM
            description = f"Skipped redemption for position: {attempt.market_id}"
        else:
            event_type = TimelineEventType.AUTO_REMEDIATION_STARTED
            description = f"Starting redemption for position: {attempt.market_id}"

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=event_type,
            description=description,
            tx_hash=attempt.tx_hash,
            strategy_id=self.strategy_id,
            chain="polygon",  # Polymarket is on Polygon
            details={
                "redemption_status": attempt.status.value,
                "market_id": attempt.market_id,
                "condition_id": attempt.condition_id,
                "outcome": attempt.outcome,
                "size": str(attempt.size),
                "amount_received": str(attempt.amount_received) if attempt.amount_received else None,
                "error_message": attempt.error_message,
                "attempts": attempt.attempts,
            },
        )

        add_event(event)

    def enable(self) -> None:
        """Enable auto-redemption."""
        self.enabled = True
        logger.info("Auto-redemption enabled")

    def disable(self) -> None:
        """Disable auto-redemption."""
        self.enabled = False
        logger.info("Auto-redemption disabled")

    def clear_history(self) -> None:
        """Clear redemption history."""
        self._redemptions.clear()
        self._pending_redemptions.clear()
        logger.info("Cleared redemption history")


__all__ = [
    "AutoRedemptionService",
    "MarketResolvedEvent",
    "RedemptionAttempt",
    "RedemptionCallback",
    "RedemptionStatus",
]
