"""Demo Mode Guard for preventing accidental mainnet execution.

This module provides safeguards to prevent accidental execution of transactions
on mainnet when running in demo/development mode.

When ALMANAK_DEMO_MODE=true is set:
- Real transaction submission is blocked
- Simulations are still allowed
- Dashboard shows a prominent "DEMO MODE" indicator
- All execution attempts log a warning

This is a quality-of-life feature to prevent expensive mistakes during
development and testing.

Example:
    from almanak.framework.execution.demo_mode import DemoModeGuard, DemoModeError

    guard = DemoModeGuard()

    # Check before execution
    if guard.is_demo_mode():
        print("Running in demo mode - transactions will not be submitted")

    # Or raise on execution attempt
    guard.validate_not_demo()  # Raises DemoModeError if in demo mode

Environment Variables:
    ALMANAK_DEMO_MODE: Set to "true" to enable demo mode
    ALMANAK_FORCE_PRODUCTION: Set to "true" to explicitly confirm production mode
"""

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class DemoModeError(Exception):
    """Raised when attempting to execute in demo mode.

    This exception is raised when code attempts to submit real transactions
    while ALMANAK_DEMO_MODE is set to true.

    Attributes:
        operation: The operation that was attempted
        message: Human-readable explanation
    """

    def __init__(self, operation: str, message: str | None = None) -> None:
        self.operation = operation
        self.message = message or (
            f"Cannot {operation} in demo mode. Unset ALMANAK_DEMO_MODE environment variable to enable real execution."
        )
        super().__init__(self.message)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class DemoModeConfig:
    """Configuration for demo mode behavior.

    Attributes:
        block_submissions: If True, block transaction submissions
        block_signing: If True, block transaction signing
        allow_simulations: If True, allow simulations even in demo mode
        log_level: Log level for demo mode warnings
    """

    block_submissions: bool = True
    block_signing: bool = False  # Allow signing for testing
    allow_simulations: bool = True
    log_level: str = "WARNING"


# =============================================================================
# Demo Mode Guard
# =============================================================================


class DemoModeGuard:
    """Guard to prevent accidental mainnet execution in demo mode.

    The guard checks the ALMANAK_DEMO_MODE environment variable and blocks
    or allows operations based on the configuration.

    Usage Patterns:

        1. Check before execution:
            guard = DemoModeGuard()
            if guard.is_demo_mode():
                logger.warning("Demo mode - simulation only")
                return simulate_only()
            else:
                return execute_real()

        2. Validate and raise:
            guard = DemoModeGuard()
            guard.validate_not_demo()  # Raises DemoModeError if in demo mode
            # ... proceed with real execution

        3. Wrap operations:
            guard = DemoModeGuard()
            with guard.execution_context():
                # This will raise if in demo mode
                submitter.submit(tx)

    Example:
        guard = DemoModeGuard()

        # Before submitting a transaction
        if guard.is_demo_mode():
            logger.info("Demo mode - skipping submission")
            return MockReceipt()

        # Or with explicit validation
        try:
            guard.validate_not_demo()
            result = await submitter.submit(tx)
        except DemoModeError as e:
            logger.warning(f"Blocked by demo mode: {e}")
    """

    # Environment variable names
    ENV_DEMO_MODE = "ALMANAK_DEMO_MODE"
    ENV_FORCE_PRODUCTION = "ALMANAK_FORCE_PRODUCTION"

    # Cached state
    _logged_demo_warning = False

    def __init__(self, config: DemoModeConfig | None = None) -> None:
        """Initialize the demo mode guard.

        Args:
            config: Optional configuration (uses defaults if not provided)
        """
        self.config = config or DemoModeConfig()
        self._demo_mode: bool | None = None  # Cached value

    def is_demo_mode(self) -> bool:
        """Check if demo mode is enabled.

        Returns:
            True if ALMANAK_DEMO_MODE is set to a truthy value
        """
        if self._demo_mode is None:
            demo_value = os.environ.get(self.ENV_DEMO_MODE, "").lower()
            self._demo_mode = demo_value in ("true", "1", "yes", "on")

            # Log once on first check
            if self._demo_mode and not DemoModeGuard._logged_demo_warning:
                DemoModeGuard._logged_demo_warning = True
                logger.warning(
                    "DEMO MODE ENABLED - Real transaction submission is blocked. "
                    "Unset %s to enable production execution.",
                    self.ENV_DEMO_MODE,
                )

        return self._demo_mode

    def is_production_mode(self) -> bool:
        """Check if explicitly in production mode.

        Production mode is confirmed when:
        1. ALMANAK_DEMO_MODE is not set
        2. AND ALMANAK_FORCE_PRODUCTION is set to "true"

        Returns:
            True if explicitly confirmed for production
        """
        if self.is_demo_mode():
            return False

        force_prod = os.environ.get(self.ENV_FORCE_PRODUCTION, "").lower()
        return force_prod in ("true", "1", "yes", "on")

    def validate_not_demo(self, operation: str = "execute transaction") -> None:
        """Validate that we're not in demo mode.

        Raises DemoModeError if demo mode is enabled.

        Args:
            operation: Description of the operation being attempted

        Raises:
            DemoModeError: If ALMANAK_DEMO_MODE is enabled
        """
        if self.is_demo_mode():
            raise DemoModeError(operation)

    def validate_submission_allowed(self) -> None:
        """Validate that transaction submission is allowed.

        Raises:
            DemoModeError: If demo mode is enabled and submissions are blocked
        """
        if self.is_demo_mode() and self.config.block_submissions:
            raise DemoModeError(
                "submit transaction",
                "Transaction submission is blocked in demo mode. "
                "Simulations are still allowed. "
                f"Unset {self.ENV_DEMO_MODE} to enable real submissions.",
            )

    def validate_signing_allowed(self) -> None:
        """Validate that transaction signing is allowed.

        Raises:
            DemoModeError: If demo mode is enabled and signing is blocked
        """
        if self.is_demo_mode() and self.config.block_signing:
            raise DemoModeError(
                "sign transaction",
                f"Transaction signing is blocked in demo mode. Unset {self.ENV_DEMO_MODE} to enable signing.",
            )

    def get_status(self) -> dict[str, bool]:
        """Get current demo mode status.

        Returns:
            Dictionary with status flags
        """
        return {
            "demo_mode": self.is_demo_mode(),
            "production_mode": self.is_production_mode(),
            "submissions_blocked": self.is_demo_mode() and self.config.block_submissions,
            "signing_blocked": self.is_demo_mode() and self.config.block_signing,
            "simulations_allowed": self.config.allow_simulations,
        }

    def clear_cache(self) -> None:
        """Clear cached demo mode state.

        Useful for testing or when environment variables change at runtime.
        """
        self._demo_mode = None


# =============================================================================
# Module-Level Functions
# =============================================================================

# Singleton instance for convenience
_global_guard: DemoModeGuard | None = None


def get_demo_mode_guard() -> DemoModeGuard:
    """Get the global demo mode guard instance.

    Returns:
        The global DemoModeGuard instance
    """
    global _global_guard
    if _global_guard is None:
        _global_guard = DemoModeGuard()
    return _global_guard


def is_demo_mode() -> bool:
    """Check if demo mode is enabled (convenience function).

    Returns:
        True if ALMANAK_DEMO_MODE is set
    """
    return get_demo_mode_guard().is_demo_mode()


def validate_not_demo(operation: str = "execute transaction") -> None:
    """Validate not in demo mode (convenience function).

    Args:
        operation: Description of operation being attempted

    Raises:
        DemoModeError: If demo mode is enabled
    """
    get_demo_mode_guard().validate_not_demo(operation)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Exception
    "DemoModeError",
    # Configuration
    "DemoModeConfig",
    # Main class
    "DemoModeGuard",
    # Convenience functions
    "get_demo_mode_guard",
    "is_demo_mode",
    "validate_not_demo",
]
