"""Circuit breaker pattern for external API calls.

This module implements the circuit breaker pattern to prevent cascade failures
when external APIs become unavailable. The circuit breaker monitors failures
and automatically stops calling failing services, allowing them to recover.

Circuit Breaker States:
    - CLOSED: Normal operation, requests flow through
    - OPEN: Failures exceeded threshold, requests are blocked
    - HALF_OPEN: Testing if service recovered, limited requests allowed

Key Features:
    - Configurable failure threshold and timeouts
    - Automatic state transitions with configurable reset time
    - Metrics tracking for monitoring and alerting
    - Graceful degradation with fallback support
    - Thread-safe and async-friendly
    - State change logging for observability

Example:
    from almanak.framework.backtesting.pnl.providers.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerConfig,
    )

    # Create circuit breaker for CoinGecko API
    cb = CircuitBreaker(
        name="coingecko",
        config=CircuitBreakerConfig(
            failure_threshold=5,
            reset_timeout_seconds=60,
            half_open_max_calls=3,
        ),
    )

    # Use as async context manager
    async def fetch_price():
        async with cb:
            response = await make_api_request()
            return response

    # Or use decorator
    @cb.protect
    async def fetch_price_decorated():
        return await make_api_request()

    # Check circuit state
    if cb.is_open:
        return use_fallback_data()

    # Get metrics
    metrics = cb.get_metrics()
    print(f"Failures: {metrics.failure_count}, State: {metrics.state}")
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, TypeVar, overload

logger = logging.getLogger(__name__)


# =============================================================================
# Circuit Breaker State
# =============================================================================


class CircuitBreakerState(Enum):
    """Circuit breaker states.

    CLOSED: Normal operation, requests flow through
    OPEN: Circuit is open, requests are blocked to prevent cascade failures
    HALF_OPEN: Testing state, limited requests to check if service recovered
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# =============================================================================
# Exceptions
# =============================================================================


class CircuitBreakerError(Exception):
    """Base exception for circuit breaker errors."""


class CircuitBreakerOpenError(CircuitBreakerError):
    """Raised when circuit breaker is open and request is blocked.

    Attributes:
        circuit_name: Name of the circuit breaker
        open_since: When the circuit was opened
        reset_after_seconds: Seconds until circuit might reset
    """

    def __init__(
        self,
        circuit_name: str,
        open_since: float,
        reset_after_seconds: float,
    ) -> None:
        self.circuit_name = circuit_name
        self.open_since = open_since
        self.reset_after_seconds = reset_after_seconds
        super().__init__(f"Circuit breaker '{circuit_name}' is open. Will reset in {reset_after_seconds:.1f}s")


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior.

    Attributes:
        failure_threshold: Number of failures before opening circuit (default: 5)
        reset_timeout_seconds: Time in seconds before attempting reset (default: 60)
        half_open_max_calls: Max calls allowed in half-open state (default: 3)
        success_threshold_half_open: Successes needed to close from half-open (default: 2)
        failure_rate_threshold: Alternative: open if failure rate exceeds this (0-1)
        min_calls_for_rate: Minimum calls before failure rate is considered
        excluded_exceptions: Exception types that don't count as failures
    """

    failure_threshold: int = 5
    reset_timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    success_threshold_half_open: int = 2
    failure_rate_threshold: float | None = None
    min_calls_for_rate: int = 10
    excluded_exceptions: tuple[type[Exception], ...] = ()

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if self.reset_timeout_seconds <= 0:
            raise ValueError("reset_timeout_seconds must be positive")
        if self.half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be at least 1")
        if self.success_threshold_half_open < 1:
            raise ValueError("success_threshold_half_open must be at least 1")
        if self.failure_rate_threshold is not None:
            if not 0 < self.failure_rate_threshold <= 1:
                raise ValueError("failure_rate_threshold must be between 0 and 1")


# =============================================================================
# Metrics
# =============================================================================


@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker monitoring.

    Tracks call counts, failure rates, and state transitions for
    observability and alerting purposes.

    Attributes:
        name: Circuit breaker name
        state: Current circuit state
        total_calls: Total number of calls attempted
        success_count: Number of successful calls
        failure_count: Number of failed calls
        blocked_count: Number of calls blocked due to open circuit
        last_failure_time: Timestamp of most recent failure
        last_success_time: Timestamp of most recent success
        state_changes: Number of state transitions
        time_in_open_seconds: Cumulative time spent in open state
        created_at: When the circuit breaker was created
    """

    name: str
    state: CircuitBreakerState
    total_calls: int = 0
    success_count: int = 0
    failure_count: int = 0
    blocked_count: int = 0
    last_failure_time: float | None = None
    last_success_time: float | None = None
    state_changes: int = 0
    time_in_open_seconds: float = 0.0
    created_at: float = field(default_factory=time.monotonic)

    @property
    def failure_rate(self) -> float:
        """Calculate current failure rate (0.0 to 1.0)."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.0
        return self.failure_count / total

    @property
    def uptime_rate(self) -> float:
        """Calculate circuit uptime rate (time not in open state)."""
        elapsed = time.monotonic() - self.created_at
        if elapsed <= 0:
            return 1.0
        return max(0.0, 1.0 - (self.time_in_open_seconds / elapsed))

    def to_dict(self) -> dict[str, Any]:
        """Serialize metrics to dictionary for logging/monitoring."""
        return {
            "name": self.name,
            "state": self.state.value,
            "total_calls": self.total_calls,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "blocked_count": self.blocked_count,
            "failure_rate": round(self.failure_rate, 4),
            "uptime_rate": round(self.uptime_rate, 4),
            "state_changes": self.state_changes,
            "time_in_open_seconds": round(self.time_in_open_seconds, 2),
            "last_failure_time": (
                datetime.fromtimestamp(self.last_failure_time).isoformat() if self.last_failure_time else None
            ),
            "last_success_time": (
                datetime.fromtimestamp(self.last_success_time).isoformat() if self.last_success_time else None
            ),
        }


# =============================================================================
# Circuit Breaker Implementation
# =============================================================================


T = TypeVar("T")


class CircuitBreaker:
    """Circuit breaker for protecting external API calls.

    Implements the circuit breaker pattern to prevent cascade failures
    when external services become unavailable. Tracks failures and
    automatically trips the circuit when threshold is exceeded.

    The circuit has three states:
    - CLOSED: Normal operation, calls flow through
    - OPEN: Too many failures, calls are blocked
    - HALF_OPEN: Testing if service recovered

    Attributes:
        name: Unique identifier for this circuit breaker
        config: Circuit breaker configuration

    Example:
        cb = CircuitBreaker("coingecko")

        # Method 1: Context manager
        async with cb:
            result = await fetch_from_api()

        # Method 2: Decorator
        @cb.protect
        async def fetch_data():
            return await api_call()

        # Method 3: Manual execution
        result = await cb.execute(fetch_from_api)
    """

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> None:
        """Initialize circuit breaker.

        Args:
            name: Unique identifier for this circuit breaker
            config: Optional configuration, uses defaults if not provided
        """
        self._name = name
        self._config = config or CircuitBreakerConfig()

        # State tracking
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._half_open_successes = 0

        # Timing
        self._last_failure_time: float | None = None
        self._last_success_time: float | None = None
        self._opened_at: float | None = None
        self._total_open_time = 0.0

        # Metrics
        self._total_calls = 0
        self._blocked_count = 0
        self._state_changes = 0
        self._created_at = time.monotonic()

        # Thread safety
        self._lock = asyncio.Lock()

        logger.debug(
            "Circuit breaker '%s' initialized: threshold=%d, reset=%ds",
            name,
            self._config.failure_threshold,
            self._config.reset_timeout_seconds,
        )

    @property
    def name(self) -> str:
        """Get circuit breaker name."""
        return self._name

    @property
    def state(self) -> CircuitBreakerState:
        """Get current circuit state."""
        return self._state

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking calls)."""
        return self._state == CircuitBreakerState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self._state == CircuitBreakerState.CLOSED

    @property
    def is_half_open(self) -> bool:
        """Check if circuit is half-open (testing recovery)."""
        return self._state == CircuitBreakerState.HALF_OPEN

    async def _transition_to(self, new_state: CircuitBreakerState) -> None:
        """Transition to a new state with logging.

        Args:
            new_state: Target state to transition to
        """
        old_state = self._state
        if old_state == new_state:
            return

        # Track time in open state
        if old_state == CircuitBreakerState.OPEN and self._opened_at:
            self._total_open_time += time.monotonic() - self._opened_at
            self._opened_at = None

        if new_state == CircuitBreakerState.OPEN:
            self._opened_at = time.monotonic()

        self._state = new_state
        self._state_changes += 1

        # Reset counters on state change
        if new_state == CircuitBreakerState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
        elif new_state == CircuitBreakerState.HALF_OPEN:
            self._half_open_calls = 0
            self._half_open_successes = 0

        logger.info(
            "Circuit breaker '%s' state change: %s -> %s (failures=%d, state_changes=%d)",
            self._name,
            old_state.value,
            new_state.value,
            self._failure_count,
            self._state_changes,
        )

    async def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset from OPEN state."""
        if self._state != CircuitBreakerState.OPEN:
            return False
        if self._opened_at is None:
            return True
        elapsed = time.monotonic() - self._opened_at
        return elapsed >= self._config.reset_timeout_seconds

    async def _record_success(self) -> None:
        """Record a successful call."""
        self._success_count += 1
        self._last_success_time = time.time()

        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_successes += 1
            if self._half_open_successes >= self._config.success_threshold_half_open:
                await self._transition_to(CircuitBreakerState.CLOSED)

    async def _record_failure(self, exc: Exception) -> None:
        """Record a failed call.

        Args:
            exc: The exception that caused the failure
        """
        # Check if this exception should be excluded
        if isinstance(exc, self._config.excluded_exceptions):
            logger.debug(
                "Circuit breaker '%s': excluded exception %s, not counting as failure",
                self._name,
                type(exc).__name__,
            )
            return

        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitBreakerState.HALF_OPEN:
            # Any failure in half-open immediately opens circuit
            await self._transition_to(CircuitBreakerState.OPEN)
        elif self._state == CircuitBreakerState.CLOSED:
            # Check if we should open the circuit
            should_open = False

            # Check failure count threshold
            if self._failure_count >= self._config.failure_threshold:
                should_open = True
                logger.warning(
                    "Circuit breaker '%s': failure threshold reached (%d/%d)",
                    self._name,
                    self._failure_count,
                    self._config.failure_threshold,
                )

            # Check failure rate threshold (if configured)
            if self._config.failure_rate_threshold is not None:
                total_calls = self._success_count + self._failure_count
                if total_calls >= self._config.min_calls_for_rate:
                    rate = self._failure_count / total_calls
                    if rate >= self._config.failure_rate_threshold:
                        should_open = True
                        logger.warning(
                            "Circuit breaker '%s': failure rate threshold reached (%.2f >= %.2f)",
                            self._name,
                            rate,
                            self._config.failure_rate_threshold,
                        )

            if should_open:
                await self._transition_to(CircuitBreakerState.OPEN)

    async def _check_state(self) -> None:
        """Check and potentially update state before a call.

        Raises:
            CircuitBreakerOpenError: If circuit is open and should block
        """
        if self._state == CircuitBreakerState.OPEN:
            if await self._should_attempt_reset():
                await self._transition_to(CircuitBreakerState.HALF_OPEN)
            else:
                elapsed = time.monotonic() - (self._opened_at or 0)
                reset_after = max(0, self._config.reset_timeout_seconds - elapsed)
                self._blocked_count += 1
                raise CircuitBreakerOpenError(
                    circuit_name=self._name,
                    open_since=self._opened_at or time.monotonic(),
                    reset_after_seconds=reset_after,
                )

        if self._state == CircuitBreakerState.HALF_OPEN:
            if self._half_open_calls >= self._config.half_open_max_calls:
                # Too many half-open calls, wait for results
                self._blocked_count += 1
                raise CircuitBreakerOpenError(
                    circuit_name=self._name,
                    open_since=self._opened_at or time.monotonic(),
                    reset_after_seconds=1.0,  # Brief wait
                )
            self._half_open_calls += 1

    async def execute(
        self,
        func: Callable[[], Awaitable[T]],
        fallback: Callable[[], Awaitable[T]] | None = None,
    ) -> T:
        """Execute a function with circuit breaker protection.

        Args:
            func: Async function to execute
            fallback: Optional fallback function if circuit is open

        Returns:
            Result from func or fallback

        Raises:
            CircuitBreakerOpenError: If circuit is open and no fallback provided
            Exception: Any exception from func (after recording failure)
        """
        async with self._lock:
            self._total_calls += 1
            try:
                await self._check_state()
            except CircuitBreakerOpenError:
                if fallback is not None:
                    logger.debug(
                        "Circuit breaker '%s': using fallback due to open circuit",
                        self._name,
                    )
                    return await fallback()
                raise

        # Execute outside lock to allow concurrent calls
        try:
            result = await func()
            async with self._lock:
                await self._record_success()
            return result
        except Exception as exc:
            async with self._lock:
                await self._record_failure(exc)
            raise

    @overload
    def protect(
        self,
        func: Callable[..., Awaitable[T]],
        *,
        fallback: Callable[..., Awaitable[T]] | None = ...,
    ) -> Callable[..., Awaitable[T]]: ...

    @overload
    def protect(
        self,
        func: None = ...,
        *,
        fallback: Callable[..., Awaitable[T]] | None = ...,
    ) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]: ...

    def protect(
        self,
        func: Callable[..., Awaitable[T]] | None = None,
        *,
        fallback: Callable[..., Awaitable[T]] | None = None,
    ) -> Callable[..., Awaitable[T]] | Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
        """Decorator to protect an async function with circuit breaker.

        Can be used with or without arguments:
            @cb.protect
            async def my_func(): ...

            @cb.protect(fallback=my_fallback)
            async def my_func(): ...

        Args:
            func: Function to wrap (when used without parentheses)
            fallback: Optional fallback function

        Returns:
            Wrapped function with circuit breaker protection
        """

        def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            @wraps(fn)
            async def wrapper(*args: Any, **kwargs: Any) -> T:
                async def execute_fn() -> T:
                    return await fn(*args, **kwargs)

                async def execute_fallback() -> T:
                    if fallback is None:
                        raise CircuitBreakerOpenError(
                            self._name,
                            self._opened_at or time.monotonic(),
                            self._config.reset_timeout_seconds,
                        )
                    return await fallback(*args, **kwargs)

                return await self.execute(
                    execute_fn,
                    fallback=execute_fallback if fallback else None,
                )

            return wrapper

        if func is not None:
            # Used as @cb.protect without parentheses
            return decorator(func)
        # Used as @cb.protect() or @cb.protect(fallback=...)
        return decorator

    async def __aenter__(self) -> "CircuitBreaker":
        """Async context manager entry: check state."""
        async with self._lock:
            self._total_calls += 1
            await self._check_state()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit: record success or failure."""
        async with self._lock:
            if exc_type is None:
                await self._record_success()
            elif isinstance(exc_val, Exception):
                await self._record_failure(exc_val)

    def get_metrics(self) -> CircuitBreakerMetrics:
        """Get current circuit breaker metrics.

        Returns:
            CircuitBreakerMetrics with current state and counters
        """
        return CircuitBreakerMetrics(
            name=self._name,
            state=self._state,
            total_calls=self._total_calls,
            success_count=self._success_count,
            failure_count=self._failure_count,
            blocked_count=self._blocked_count,
            last_failure_time=self._last_failure_time,
            last_success_time=self._last_success_time,
            state_changes=self._state_changes,
            time_in_open_seconds=self._total_open_time,
            created_at=self._created_at,
        )

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state.

        Use with caution - typically for testing or administrative purposes.
        """
        if self._state == CircuitBreakerState.OPEN and self._opened_at:
            self._total_open_time += time.monotonic() - self._opened_at

        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._half_open_successes = 0
        self._opened_at = None

        logger.info("Circuit breaker '%s' manually reset to CLOSED", self._name)


# =============================================================================
# Circuit Breaker Registry
# =============================================================================


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers.

    Provides centralized management, metrics aggregation, and
    bulk operations for circuit breakers across the application.

    Example:
        registry = CircuitBreakerRegistry()

        # Register circuit breakers
        registry.register("coingecko", CircuitBreakerConfig(failure_threshold=5))
        registry.register("chainlink", CircuitBreakerConfig(failure_threshold=3))

        # Get circuit breaker
        cb = registry.get("coingecko")

        # Get all metrics
        all_metrics = registry.get_all_metrics()
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> CircuitBreaker:
        """Register a new circuit breaker.

        Args:
            name: Unique name for the circuit breaker
            config: Optional configuration

        Returns:
            The registered CircuitBreaker instance

        Raises:
            ValueError: If a circuit breaker with this name already exists
        """
        if name in self._breakers:
            raise ValueError(f"Circuit breaker '{name}' already registered")

        cb = CircuitBreaker(name, config)
        self._breakers[name] = cb
        logger.debug("Registered circuit breaker: %s", name)
        return cb

    def get(self, name: str) -> CircuitBreaker | None:
        """Get a circuit breaker by name.

        Args:
            name: Name of the circuit breaker

        Returns:
            CircuitBreaker instance or None if not found
        """
        return self._breakers.get(name)

    def get_or_create(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> CircuitBreaker:
        """Get existing or create new circuit breaker.

        Args:
            name: Name of the circuit breaker
            config: Configuration for new circuit breaker (ignored if exists)

        Returns:
            CircuitBreaker instance
        """
        if name not in self._breakers:
            return self.register(name, config)
        return self._breakers[name]

    def get_all_metrics(self) -> dict[str, dict[str, Any]]:
        """Get metrics from all registered circuit breakers.

        Returns:
            Dictionary mapping names to serialized metrics
        """
        return {name: cb.get_metrics().to_dict() for name, cb in self._breakers.items()}

    def get_open_circuits(self) -> list[str]:
        """Get names of all open circuit breakers.

        Returns:
            List of circuit breaker names that are currently open
        """
        return [name for name, cb in self._breakers.items() if cb.is_open]

    def reset_all(self) -> None:
        """Reset all circuit breakers to closed state.

        Use with caution - typically for testing purposes.
        """
        for cb in self._breakers.values():
            cb.reset()
        logger.info("Reset all %d circuit breakers", len(self._breakers))

    @property
    def names(self) -> list[str]:
        """Get all registered circuit breaker names."""
        return list(self._breakers.keys())


# =============================================================================
# Default Registry and Factory Functions
# =============================================================================


# Default global registry
_default_registry = CircuitBreakerRegistry()


def get_circuit_breaker(
    name: str,
    config: CircuitBreakerConfig | None = None,
) -> CircuitBreaker:
    """Get or create a circuit breaker from the default registry.

    Convenience function for accessing circuit breakers without
    explicitly managing a registry.

    Args:
        name: Name of the circuit breaker
        config: Configuration for new circuit breaker

    Returns:
        CircuitBreaker instance
    """
    return _default_registry.get_or_create(name, config)


def get_all_circuit_breaker_metrics() -> dict[str, dict[str, Any]]:
    """Get metrics from all circuit breakers in the default registry.

    Returns:
        Dictionary mapping names to serialized metrics
    """
    return _default_registry.get_all_metrics()


def get_open_circuits() -> list[str]:
    """Get names of all open circuit breakers in the default registry.

    Returns:
        List of circuit breaker names that are currently open
    """
    return _default_registry.get_open_circuits()


# =============================================================================
# Pre-configured Circuit Breakers for Known APIs
# =============================================================================


# Recommended configurations for external APIs
COINGECKO_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    reset_timeout_seconds=60.0,
    half_open_max_calls=2,
    success_threshold_half_open=2,
)

CHAINLINK_CONFIG = CircuitBreakerConfig(
    failure_threshold=3,
    reset_timeout_seconds=30.0,
    half_open_max_calls=1,
    success_threshold_half_open=1,
)

SUBGRAPH_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    reset_timeout_seconds=120.0,
    half_open_max_calls=2,
    success_threshold_half_open=2,
)

ETHERSCAN_CONFIG = CircuitBreakerConfig(
    failure_threshold=3,
    reset_timeout_seconds=60.0,
    half_open_max_calls=1,
    success_threshold_half_open=1,
)

RPC_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    reset_timeout_seconds=15.0,
    half_open_max_calls=2,
    success_threshold_half_open=2,
)

GMX_API_CONFIG = CircuitBreakerConfig(
    failure_threshold=3,
    reset_timeout_seconds=60.0,
    half_open_max_calls=2,
    success_threshold_half_open=1,
)

HYPERLIQUID_CONFIG = CircuitBreakerConfig(
    failure_threshold=3,
    reset_timeout_seconds=60.0,
    half_open_max_calls=2,
    success_threshold_half_open=1,
)


def create_coingecko_circuit_breaker() -> CircuitBreaker:
    """Create a circuit breaker configured for CoinGecko API."""
    return get_circuit_breaker("coingecko", COINGECKO_CONFIG)


def create_chainlink_circuit_breaker(chain: str = "default") -> CircuitBreaker:
    """Create a circuit breaker configured for Chainlink RPC calls."""
    return get_circuit_breaker(f"chainlink_{chain}", CHAINLINK_CONFIG)


def create_subgraph_circuit_breaker(subgraph: str = "default") -> CircuitBreaker:
    """Create a circuit breaker configured for The Graph subgraph calls."""
    return get_circuit_breaker(f"subgraph_{subgraph}", SUBGRAPH_CONFIG)


def create_etherscan_circuit_breaker(chain: str = "ethereum") -> CircuitBreaker:
    """Create a circuit breaker configured for Etherscan API calls."""
    return get_circuit_breaker(f"etherscan_{chain}", ETHERSCAN_CONFIG)


def create_rpc_circuit_breaker(chain: str = "default") -> CircuitBreaker:
    """Create a circuit breaker configured for Web3 RPC calls."""
    return get_circuit_breaker(f"rpc_{chain}", RPC_CONFIG)


def create_gmx_circuit_breaker() -> CircuitBreaker:
    """Create a circuit breaker configured for GMX Stats API."""
    return get_circuit_breaker("gmx_stats", GMX_API_CONFIG)


def create_hyperliquid_circuit_breaker() -> CircuitBreaker:
    """Create a circuit breaker configured for Hyperliquid API."""
    return get_circuit_breaker("hyperliquid", HYPERLIQUID_CONFIG)


__all__ = [
    # Core classes
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerState",
    "CircuitBreakerMetrics",
    "CircuitBreakerRegistry",
    # Exceptions
    "CircuitBreakerError",
    "CircuitBreakerOpenError",
    # Registry functions
    "get_circuit_breaker",
    "get_all_circuit_breaker_metrics",
    "get_open_circuits",
    # Pre-configured factories
    "create_coingecko_circuit_breaker",
    "create_chainlink_circuit_breaker",
    "create_subgraph_circuit_breaker",
    "create_etherscan_circuit_breaker",
    "create_rpc_circuit_breaker",
    "create_gmx_circuit_breaker",
    "create_hyperliquid_circuit_breaker",
    # Configurations
    "COINGECKO_CONFIG",
    "CHAINLINK_CONFIG",
    "SUBGRAPH_CONFIG",
    "ETHERSCAN_CONFIG",
    "RPC_CONFIG",
    "GMX_API_CONFIG",
    "HYPERLIQUID_CONFIG",
]
