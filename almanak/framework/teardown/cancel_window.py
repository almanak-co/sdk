"""Cancel Window Manager for the Strategy Teardown System.

Provides a 10-second cancel window for all teardown operations.
This is a critical safety feature that prevents fat-finger mistakes
and gives users a moment to reconsider.

The cancel window:
- Applies to ALL modes (both graceful and emergency)
- Default duration: 10 seconds (configurable)
- Can be skipped for auto-protect exits (configurable)
- Shows a countdown UI with prominent cancel button
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from almanak.framework.teardown.config import TeardownConfig

logger = logging.getLogger(__name__)


@dataclass
class CancelWindowStatus:
    """Status of a cancel window."""

    teardown_id: str
    is_active: bool
    started_at: datetime
    expires_at: datetime
    seconds_remaining: float
    cancelled: bool = False

    @property
    def is_expired(self) -> bool:
        """Check if the cancel window has expired."""
        return datetime.now(UTC) >= self.expires_at


@dataclass
class CancelWindowResult:
    """Result of running a cancel window."""

    was_cancelled: bool
    cancel_time: datetime | None = None
    waited_full_duration: bool = False
    skip_reason: str | None = None


# Type alias for cancel check callback
CancelCheckCallback = Callable[[], Awaitable[bool]]

# Type alias for progress callback (receives seconds remaining)
ProgressCallback = Callable[[float], Awaitable[None]]


class CancelWindowManager:
    """Manages the 10-second cancel window for teardown operations.

    The cancel window is a critical UX safety feature:
    - Gives users time to reconsider
    - Prevents accidental teardowns
    - Shows countdown with prominent cancel button
    - Can be configured per-strategy

    Usage:
        manager = CancelWindowManager(config)
        result = await manager.run_cancel_window(
            teardown_id="td_123",
            on_check_cancelled=check_db_for_cancel,
            on_progress=update_ui_countdown,
        )
        if result.was_cancelled:
            # User cancelled during window
        else:
            # Proceed with execution
    """

    def __init__(self, config: TeardownConfig | None = None):
        """Initialize the cancel window manager.

        Args:
            config: Teardown configuration with window duration settings
        """
        self.config = config or TeardownConfig.default()
        self._active_windows: dict[str, CancelWindowStatus] = {}

    async def run_cancel_window(
        self,
        teardown_id: str,
        on_check_cancelled: CancelCheckCallback | None = None,
        on_progress: ProgressCallback | None = None,
        duration_seconds: int | None = None,
        is_auto_mode: bool = False,
    ) -> CancelWindowResult:
        """Run the cancel window, waiting for duration or cancellation.

        Args:
            teardown_id: ID of the teardown operation
            on_check_cancelled: Callback to check if user requested cancel
            on_progress: Callback for progress updates (receives seconds remaining)
            duration_seconds: Override window duration (uses config default if None)
            is_auto_mode: Whether this is an auto-protect triggered exit

        Returns:
            CancelWindowResult indicating whether cancelled or completed
        """
        # Check if we should skip the window for auto mode
        if is_auto_mode and self.config.skip_cancel_window_for_auto:
            logger.info(f"Skipping cancel window for auto-protect exit: {teardown_id}")
            return CancelWindowResult(
                was_cancelled=False,
                skip_reason="Auto-protect mode - cancel window skipped",
            )

        duration = duration_seconds or self.config.cancel_window_seconds
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=duration)

        # Create and track the window
        status = CancelWindowStatus(
            teardown_id=teardown_id,
            is_active=True,
            started_at=now,
            expires_at=expires_at,
            seconds_remaining=duration,
        )
        self._active_windows[teardown_id] = status

        logger.info(f"Cancel window started for {teardown_id}: {duration} seconds")

        try:
            # Poll for cancellation while counting down
            check_interval = 0.5  # Check every 500ms
            elapsed = 0.0

            while elapsed < duration:
                # Check for cancellation
                if on_check_cancelled:
                    cancelled = await on_check_cancelled()
                    if cancelled:
                        logger.info(f"Cancel window: user cancelled {teardown_id}")
                        status.cancelled = True
                        return CancelWindowResult(
                            was_cancelled=True,
                            cancel_time=datetime.now(UTC),
                        )

                # Update progress
                remaining = duration - elapsed
                status.seconds_remaining = remaining

                if on_progress:
                    await on_progress(remaining)

                # Wait for next check
                await asyncio.sleep(check_interval)
                elapsed += check_interval

            # Window completed without cancellation
            logger.info(f"Cancel window completed for {teardown_id}")
            return CancelWindowResult(
                was_cancelled=False,
                waited_full_duration=True,
            )

        finally:
            # Clean up
            status.is_active = False
            if teardown_id in self._active_windows:
                del self._active_windows[teardown_id]

    def get_window_status(self, teardown_id: str) -> CancelWindowStatus | None:
        """Get the current status of a cancel window.

        Args:
            teardown_id: ID of the teardown operation

        Returns:
            CancelWindowStatus if window is active, None otherwise
        """
        return self._active_windows.get(teardown_id)

    def is_in_cancel_window(self, teardown_id: str) -> bool:
        """Check if a teardown is currently in its cancel window.

        Args:
            teardown_id: ID of the teardown operation

        Returns:
            True if in cancel window, False otherwise
        """
        status = self._active_windows.get(teardown_id)
        if status is None:
            return False
        return status.is_active and not status.is_expired

    def get_seconds_remaining(self, teardown_id: str) -> float | None:
        """Get seconds remaining in a cancel window.

        Args:
            teardown_id: ID of the teardown operation

        Returns:
            Seconds remaining if in window, None otherwise
        """
        status = self._active_windows.get(teardown_id)
        if status is None or not status.is_active:
            return None

        remaining = (status.expires_at - datetime.now(UTC)).total_seconds()
        return max(0, remaining)

    def cancel_window(self, teardown_id: str) -> bool:
        """Request cancellation of a teardown during its cancel window.

        This is typically called by the API when user clicks cancel.

        Args:
            teardown_id: ID of the teardown operation

        Returns:
            True if cancellation was successful, False if window expired/not found
        """
        status = self._active_windows.get(teardown_id)

        if status is None:
            logger.warning(f"Cannot cancel {teardown_id}: no active window")
            return False

        if not status.is_active:
            logger.warning(f"Cannot cancel {teardown_id}: window not active")
            return False

        if status.is_expired:
            logger.warning(f"Cannot cancel {teardown_id}: window expired")
            return False

        # Mark as cancelled
        status.cancelled = True
        logger.info(f"Cancel requested for {teardown_id}")
        return True

    def get_active_windows(self) -> list[CancelWindowStatus]:
        """Get all currently active cancel windows.

        Returns:
            List of active cancel window statuses
        """
        return [status for status in self._active_windows.values() if status.is_active and not status.is_expired]


class CancelWindowUI:
    """Helper class for building cancel window UI data.

    Provides formatted data for the frontend to display the
    cancel window countdown and button.
    """

    @staticmethod
    def format_countdown(seconds_remaining: float) -> dict:
        """Format countdown data for UI display.

        Args:
            seconds_remaining: Seconds remaining in window

        Returns:
            Dictionary with formatted countdown data
        """
        whole_seconds = int(seconds_remaining)
        return {
            "seconds": whole_seconds,
            "display": str(whole_seconds),
            "is_urgent": whole_seconds <= 3,
            "message": f"Starting in {whole_seconds} seconds" if whole_seconds > 0 else "Starting now...",
        }

    @staticmethod
    def get_cancel_button_config() -> dict:
        """Get configuration for the cancel button.

        Returns:
            Dictionary with button configuration
        """
        return {
            "text": "CANCEL - I changed my mind",
            "subtext": "Press ESC or click to cancel",
            "hotkey": "Escape",
            "style": "prominent",  # Large, easy to tap
            "position": "center",
        }

    @staticmethod
    def get_mobile_config() -> dict:
        """Get mobile-specific UI configuration.

        Returns:
            Dictionary with mobile UI settings
        """
        return {
            "countdown_size": "large",  # Large touch target
            "button_height": "80px",  # Fat-finger friendly
            "padding": "20px",
            "font_size": "24px",
        }
