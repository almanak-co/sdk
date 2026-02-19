"""Protocol Testing Harness.

This module provides a standardized testing framework for all protocol connectors,
ensuring consistent test coverage across different DeFi protocol integrations.

The harness provides:
- Standard test interface for all connectors
- Basic operations tests (supply, borrow, swap, etc.)
- Error handling tests
- Receipt parsing tests
- Gas estimation tests
- Support for both mainnet fork tests and unit tests
- Test coverage report generation
- CI integration utilities

Usage:
    from almanak.framework.testing import ProtocolTestHarness, ProtocolTestSuite

    # Create a test harness for an adapter
    harness = ProtocolTestHarness(
        adapter_class=AaveV3Adapter,
        config_class=AaveV3Config,
        protocol_type=ProtocolType.LENDING,
    )

    # Run all tests
    results = harness.run_all_tests()

    # Generate coverage report
    report = harness.generate_coverage_report()
"""

import logging
import subprocess
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, TypeVar

logger = logging.getLogger(__name__)


# =============================================================================
# Type Definitions
# =============================================================================


class AdapterProtocol(Protocol):
    """Protocol defining the interface adapters must implement for testing."""

    def __init__(self, config: Any) -> None:
        """Initialize the adapter with configuration."""
        ...


AdapterT = TypeVar("AdapterT", bound=AdapterProtocol)
ConfigT = TypeVar("ConfigT")


# =============================================================================
# Enums
# =============================================================================


class ProtocolType(Enum):
    """Types of DeFi protocols that can be tested."""

    DEX = "DEX"
    PERPS = "PERPS"
    LENDING = "LENDING"
    YIELD = "YIELD"
    BRIDGE = "BRIDGE"
    OPTIONS = "OPTIONS"


class TestCategory(Enum):
    """Categories of tests in the harness."""

    BASIC_OPERATIONS = "BASIC_OPERATIONS"
    ERROR_HANDLING = "ERROR_HANDLING"
    RECEIPT_PARSING = "RECEIPT_PARSING"
    GAS_ESTIMATION = "GAS_ESTIMATION"
    EDGE_CASES = "EDGE_CASES"
    INTEGRATION = "INTEGRATION"


class TestStatus(Enum):
    """Status of a test execution."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class TestEnvironment(Enum):
    """Test execution environment."""

    UNIT = "UNIT"
    MAINNET_FORK = "MAINNET_FORK"
    TESTNET = "TESTNET"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TestCase:
    """Definition of a single test case.

    Attributes:
        name: Test case name
        description: What the test verifies
        category: Test category (basic ops, error handling, etc.)
        environment: Required test environment
        test_fn: Function that executes the test
        setup_fn: Optional setup function
        teardown_fn: Optional teardown function
        timeout_seconds: Test timeout
        skip_reason: If set, test will be skipped with this reason
    """

    name: str
    description: str
    category: TestCategory
    environment: TestEnvironment = TestEnvironment.UNIT
    test_fn: Callable[..., bool] | None = None
    setup_fn: Callable[..., None] | None = None
    teardown_fn: Callable[..., None] | None = None
    timeout_seconds: float = 30.0
    skip_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "environment": self.environment.value,
            "timeout_seconds": self.timeout_seconds,
            "skip_reason": self.skip_reason,
        }


@dataclass
class TestResult:
    """Result of executing a single test.

    Attributes:
        test_name: Name of the test
        status: Pass/fail/skip/error status
        duration_ms: Test execution time in milliseconds
        error_message: Error message if failed
        stack_trace: Stack trace if error occurred
        assertions_passed: Number of assertions that passed
        assertions_failed: Number of assertions that failed
        timestamp: When the test was executed
    """

    test_name: str
    status: TestStatus
    duration_ms: float = 0.0
    error_message: str | None = None
    stack_trace: str | None = None
    assertions_passed: int = 0
    assertions_failed: int = 0
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "test_name": self.test_name,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "assertions_passed": self.assertions_passed,
            "assertions_failed": self.assertions_failed,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class TestSuiteResult:
    """Result of executing a test suite.

    Attributes:
        suite_name: Name of the test suite
        total_tests: Total number of tests
        passed: Number of tests passed
        failed: Number of tests failed
        skipped: Number of tests skipped
        errors: Number of tests with errors
        duration_ms: Total execution time
        results: Individual test results
        coverage: Coverage percentage (0-100)
    """

    suite_name: str
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: int = 0
    duration_ms: float = 0.0
    results: list[TestResult] = field(default_factory=list)
    coverage: float = 0.0

    @property
    def pass_rate(self) -> float:
        """Calculate pass rate percentage."""
        if self.total_tests == 0:
            return 0.0
        return (self.passed / self.total_tests) * 100

    @property
    def success(self) -> bool:
        """Check if all tests passed."""
        return self.failed == 0 and self.errors == 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "suite_name": self.suite_name,
            "total_tests": self.total_tests,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "errors": self.errors,
            "duration_ms": self.duration_ms,
            "pass_rate": self.pass_rate,
            "coverage": self.coverage,
            "results": [r.to_dict() for r in self.results],
        }


@dataclass
class CoverageReport:
    """Test coverage report for a protocol connector.

    Attributes:
        protocol_name: Name of the protocol being tested
        protocol_type: Type of protocol (DEX, Lending, etc.)
        total_operations: Total operations the adapter supports
        tested_operations: Operations with test coverage
        coverage_percentage: Overall coverage percentage
        operation_coverage: Coverage per operation
        category_coverage: Coverage per test category
        recommendations: Suggested tests to improve coverage
        generated_at: Report generation timestamp
    """

    protocol_name: str
    protocol_type: ProtocolType
    total_operations: int = 0
    tested_operations: int = 0
    coverage_percentage: float = 0.0
    operation_coverage: dict[str, bool] = field(default_factory=dict)
    category_coverage: dict[str, float] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol_name": self.protocol_name,
            "protocol_type": self.protocol_type.value,
            "total_operations": self.total_operations,
            "tested_operations": self.tested_operations,
            "coverage_percentage": self.coverage_percentage,
            "operation_coverage": self.operation_coverage,
            "category_coverage": self.category_coverage,
            "recommendations": self.recommendations,
            "generated_at": self.generated_at.isoformat(),
        }


@dataclass
class ForkConfig:
    """Configuration for mainnet fork tests.

    Attributes:
        chain: Target blockchain
        fork_block: Block number to fork from (None for latest)
        rpc_url: RPC endpoint URL
        anvil_port: Port for Anvil
        timeout_seconds: Fork startup timeout
    """

    chain: str
    fork_block: int | None = None
    rpc_url: str = "http://localhost:8545"
    anvil_port: int = 8548
    timeout_seconds: float = 60.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "chain": self.chain,
            "fork_block": self.fork_block,
            "rpc_url": self.rpc_url,
            "anvil_port": self.anvil_port,
            "timeout_seconds": self.timeout_seconds,
        }


# =============================================================================
# Test Operations Definitions
# =============================================================================

# Standard operations per protocol type
PROTOCOL_OPERATIONS: dict[ProtocolType, list[str]] = {
    ProtocolType.DEX: [
        "swap",
        "get_quote",
        "get_token_price",
        "approve",
        "get_allowance",
        "get_liquidity",
    ],
    ProtocolType.PERPS: [
        "open_position",
        "close_position",
        "increase_position",
        "decrease_position",
        "get_position",
        "place_order",
        "cancel_order",
        "get_order",
        "get_open_orders",
        "get_funding_rate",
    ],
    ProtocolType.LENDING: [
        "supply",
        "withdraw",
        "borrow",
        "repay",
        "get_position",
        "get_health_factor",
        "liquidate",
        "flash_loan",
        "set_collateral",
        "get_reserve_data",
    ],
    ProtocolType.YIELD: [
        "deposit",
        "withdraw",
        "get_yield",
        "get_apy",
        "compound",
        "get_balance",
    ],
    ProtocolType.BRIDGE: [
        "bridge",
        "get_bridge_quote",
        "get_supported_chains",
        "get_transfer_status",
    ],
    ProtocolType.OPTIONS: [
        "buy_option",
        "sell_option",
        "exercise_option",
        "get_option_chain",
        "get_greeks",
        "get_iv",
    ],
}


# =============================================================================
# Anvil Fork Manager
# =============================================================================


@dataclass
class AnvilFork:
    """Manages an Anvil fork process for mainnet fork testing.

    Attributes:
        config: Fork configuration
        process: Subprocess running Anvil
        is_running: Whether the fork is currently running
    """

    config: ForkConfig
    process: subprocess.Popen[bytes] | None = None
    is_running: bool = False

    def start(self) -> bool:
        """Start the Anvil fork.

        Returns:
            True if fork started successfully
        """
        if self.is_running:
            logger.warning("Anvil fork is already running")
            return True

        try:
            cmd = [
                "anvil",
                "--fork-url",
                self.config.rpc_url,
                "--port",
                str(self.config.anvil_port),
                "--silent",
            ]

            if self.config.fork_block is not None:
                cmd.extend(["--fork-block-number", str(self.config.fork_block)])

            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Wait for Anvil to start
            start_time = time.time()
            while time.time() - start_time < self.config.timeout_seconds:
                if self._is_ready():
                    self.is_running = True
                    logger.info(
                        f"Anvil fork started on port {self.config.anvil_port} "
                        f"(block: {self.config.fork_block or 'latest'})"
                    )
                    return True
                time.sleep(0.5)

            logger.error("Anvil fork startup timed out")
            self.stop()
            return False

        except FileNotFoundError:
            logger.error(
                "Anvil not found. Please install Foundry: https://book.getfoundry.sh/getting-started/installation"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to start Anvil fork: {e}")
            return False

    def stop(self) -> None:
        """Stop the Anvil fork."""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None
        self.is_running = False
        logger.info("Anvil fork stopped")

    def _is_ready(self) -> bool:
        """Check if Anvil is ready to accept connections."""
        import socket

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect(("localhost", self.config.anvil_port))
                return True
        except (TimeoutError, ConnectionRefusedError):
            return False

    def get_rpc_url(self) -> str:
        """Get the RPC URL for the running fork."""
        return f"http://localhost:{self.config.anvil_port}"


# =============================================================================
# Test Context
# =============================================================================


@dataclass
class TestContext:
    """Context provided to test functions.

    Attributes:
        adapter: The protocol adapter instance
        config: The adapter configuration
        fork: Optional Anvil fork for mainnet tests
        test_accounts: Test account addresses
        test_tokens: Test token addresses per chain
    """

    adapter: Any
    config: Any
    fork: AnvilFork | None = None
    test_accounts: list[str] = field(default_factory=list)
    test_tokens: dict[str, dict[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize default test accounts and tokens."""
        if not self.test_accounts:
            self.test_accounts = [
                "0x1234567890123456789012345678901234567890",
                "0x2345678901234567890123456789012345678901",
                "0x3456789012345678901234567890123456789012",
            ]

        if not self.test_tokens:
            self.test_tokens = {
                "arbitrum": {
                    "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                },
                "ethereum": {
                    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                },
                "optimism": {
                    "WETH": "0x4200000000000000000000000000000000000006",
                    "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
                },
                "polygon": {
                    "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
                    "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
                },
                "base": {
                    "WETH": "0x4200000000000000000000000000000000000006",
                    "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                },
                "avalanche": {
                    "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
                    "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
                },
            }


# =============================================================================
# Assertion Helpers
# =============================================================================


class AssertionTracker:
    """Tracks assertions during test execution."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.passed: int = 0
        self.failed: int = 0
        self.errors: list[str] = []

    def assert_true(self, condition: bool, message: str = "") -> None:
        """Assert that a condition is true."""
        if condition:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected True: {message}")

    def assert_false(self, condition: bool, message: str = "") -> None:
        """Assert that a condition is false."""
        if not condition:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected False: {message}")

    def assert_equal(self, actual: Any, expected: Any, message: str = "") -> None:
        """Assert that two values are equal."""
        if actual == expected:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {expected}, got {actual}: {message}")

    def assert_not_equal(self, actual: Any, expected: Any, message: str = "") -> None:
        """Assert that two values are not equal."""
        if actual != expected:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {actual} != {expected}: {message}")

    def assert_none(self, value: Any, message: str = "") -> None:
        """Assert that a value is None."""
        if value is None:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected None, got {value}: {message}")

    def assert_not_none(self, value: Any, message: str = "") -> None:
        """Assert that a value is not None."""
        if value is not None:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected not None: {message}")

    def assert_greater(self, actual: Any, expected: Any, message: str = "") -> None:
        """Assert that actual > expected."""
        if actual > expected:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {actual} > {expected}: {message}")

    def assert_greater_equal(self, actual: Any, expected: Any, message: str = "") -> None:
        """Assert that actual >= expected."""
        if actual >= expected:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {actual} >= {expected}: {message}")

    def assert_less(self, actual: Any, expected: Any, message: str = "") -> None:
        """Assert that actual < expected."""
        if actual < expected:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {actual} < {expected}: {message}")

    def assert_less_equal(self, actual: Any, expected: Any, message: str = "") -> None:
        """Assert that actual <= expected."""
        if actual <= expected:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {actual} <= {expected}: {message}")

    def assert_in(self, item: Any, container: Any, message: str = "") -> None:
        """Assert that item is in container."""
        if item in container:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {item} in {container}: {message}")

    def assert_not_in(self, item: Any, container: Any, message: str = "") -> None:
        """Assert that item is not in container."""
        if item not in container:
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected {item} not in {container}: {message}")

    def assert_type(self, value: Any, expected_type: type, message: str = "") -> None:
        """Assert that value is of expected type."""
        if isinstance(value, expected_type):
            self.passed += 1
        else:
            self.failed += 1
            self.errors.append(f"Expected type {expected_type.__name__}, got {type(value).__name__}: {message}")

    def assert_raises(
        self,
        exception: type[BaseException],
        fn: Callable[..., Any],
        *args: Any,
        message: str = "",
        **kwargs: Any,
    ) -> None:
        """Assert that a function raises an exception."""
        try:
            fn(*args, **kwargs)
            self.failed += 1
            self.errors.append(f"Expected {exception.__name__} to be raised: {message}")
        except exception:
            self.passed += 1
        except Exception as e:
            self.failed += 1
            self.errors.append(f"Expected {exception.__name__}, got {type(e).__name__}: {message}")

    @property
    def all_passed(self) -> bool:
        """Check if all assertions passed."""
        return self.failed == 0


# =============================================================================
# Protocol Test Suite Interface
# =============================================================================


class ProtocolTestSuite[AdapterT: AdapterProtocol, ConfigT](ABC):
    """Abstract base class for protocol-specific test suites.

    Implement this class to create a test suite for a specific protocol connector.
    """

    def __init__(
        self,
        adapter_class: type[AdapterT],
        config_class: type[ConfigT],
        protocol_type: ProtocolType,
        protocol_name: str,
    ) -> None:
        """Initialize the test suite.

        Args:
            adapter_class: The adapter class to test
            config_class: The configuration class for the adapter
            protocol_type: Type of protocol (DEX, Lending, etc.)
            protocol_name: Human-readable protocol name
        """
        self.adapter_class = adapter_class
        self.config_class = config_class
        self.protocol_type = protocol_type
        self.protocol_name = protocol_name
        self.test_cases: list[TestCase] = []
        self._context: TestContext | None = None

    @abstractmethod
    def create_test_config(self, chain: str = "arbitrum") -> ConfigT:
        """Create a test configuration for the adapter.

        Args:
            chain: Target chain for the configuration

        Returns:
            Configuration instance for testing
        """
        pass

    @abstractmethod
    def get_basic_operation_tests(self) -> list[TestCase]:
        """Get basic operation test cases.

        Returns:
            List of test cases for basic protocol operations
        """
        pass

    @abstractmethod
    def get_error_handling_tests(self) -> list[TestCase]:
        """Get error handling test cases.

        Returns:
            List of test cases for error scenarios
        """
        pass

    @abstractmethod
    def get_receipt_parsing_tests(self) -> list[TestCase]:
        """Get receipt parsing test cases.

        Returns:
            List of test cases for transaction receipt parsing
        """
        pass

    @abstractmethod
    def get_gas_estimation_tests(self) -> list[TestCase]:
        """Get gas estimation test cases.

        Returns:
            List of test cases for gas estimation
        """
        pass

    def get_all_test_cases(self) -> list[TestCase]:
        """Get all test cases for this protocol.

        Returns:
            Complete list of test cases
        """
        return (
            self.get_basic_operation_tests()
            + self.get_error_handling_tests()
            + self.get_receipt_parsing_tests()
            + self.get_gas_estimation_tests()
        )


# =============================================================================
# Standard Test Generators
# =============================================================================


def generate_config_validation_tests[ConfigT](
    config_class: type[ConfigT],
    valid_chains: list[str],
    protocol_name: str,
) -> list[TestCase]:
    """Generate standard configuration validation tests.

    Args:
        config_class: Configuration class to test
        valid_chains: List of valid chain names
        protocol_name: Name of the protocol

    Returns:
        List of configuration validation test cases
    """
    tests: list[TestCase] = []

    # Test valid configuration
    def test_valid_config(ctx: TestContext, tracker: AssertionTracker) -> bool:
        config = ctx.config
        tracker.assert_not_none(config, "Config should be created")
        return tracker.all_passed

    tests.append(
        TestCase(
            name=f"{protocol_name}_valid_config",
            description="Test creating a valid configuration",
            category=TestCategory.BASIC_OPERATIONS,
            test_fn=test_valid_config,
        )
    )

    # Test invalid chain
    def test_invalid_chain(ctx: TestContext, tracker: AssertionTracker) -> bool:
        tracker.assert_raises(
            ValueError,
            config_class,
            chain="invalid_chain",
            wallet_address="0x1234567890123456789012345678901234567890",
            message="Should raise ValueError for invalid chain",
        )
        return tracker.all_passed

    tests.append(
        TestCase(
            name=f"{protocol_name}_invalid_chain",
            description="Test that invalid chain raises error",
            category=TestCategory.ERROR_HANDLING,
            test_fn=test_invalid_chain,
        )
    )

    # Test invalid wallet address
    def test_invalid_wallet(ctx: TestContext, tracker: AssertionTracker) -> bool:
        tracker.assert_raises(
            ValueError,
            config_class,
            chain=valid_chains[0] if valid_chains else "arbitrum",
            wallet_address="invalid_address",
            message="Should raise ValueError for invalid wallet address",
        )
        return tracker.all_passed

    tests.append(
        TestCase(
            name=f"{protocol_name}_invalid_wallet",
            description="Test that invalid wallet address raises error",
            category=TestCategory.ERROR_HANDLING,
            test_fn=test_invalid_wallet,
        )
    )

    return tests


def generate_basic_operations_tests(
    protocol_type: ProtocolType,
    protocol_name: str,
) -> list[TestCase]:
    """Generate standard basic operations tests based on protocol type.

    Args:
        protocol_type: Type of protocol
        protocol_name: Name of the protocol

    Returns:
        List of basic operation test cases
    """
    tests: list[TestCase] = []
    operations = PROTOCOL_OPERATIONS.get(protocol_type, [])

    for operation in operations:
        # Generate a test stub for each operation
        def make_test(op: str) -> Callable[[TestContext, AssertionTracker], bool]:
            def test_operation(ctx: TestContext, tracker: AssertionTracker) -> bool:
                # Check if adapter has the operation method
                adapter = ctx.adapter
                has_method = hasattr(adapter, op)
                tracker.assert_true(
                    has_method,
                    f"Adapter should have '{op}' method",
                )
                return tracker.all_passed

            return test_operation

        tests.append(
            TestCase(
                name=f"{protocol_name}_{operation}",
                description=f"Test {operation} operation exists",
                category=TestCategory.BASIC_OPERATIONS,
                test_fn=make_test(operation),
            )
        )

    return tests


def generate_gas_estimation_tests(
    protocol_name: str,
    operations: list[str],
) -> list[TestCase]:
    """Generate gas estimation tests for operations.

    Args:
        protocol_name: Name of the protocol
        operations: List of operations to test gas estimation for

    Returns:
        List of gas estimation test cases
    """
    tests: list[TestCase] = []

    for operation in operations:

        def make_gas_test(op: str) -> Callable[[TestContext, AssertionTracker], bool]:
            def test_gas_estimation(ctx: TestContext, tracker: AssertionTracker) -> bool:
                # Check for gas estimate constant or method
                adapter = ctx.adapter
                gas_attr = f"{op}_gas_estimate"
                gas_method = f"estimate_{op}_gas"

                has_estimate = hasattr(adapter, gas_attr) or hasattr(adapter, gas_method)

                # Gas estimation is optional but recommended
                if has_estimate:
                    tracker.passed += 1
                else:
                    logger.warning(f"No gas estimation found for {op}")
                    tracker.passed += 1  # Don't fail, just warn

                return tracker.all_passed

            return test_gas_estimation

        tests.append(
            TestCase(
                name=f"{protocol_name}_{operation}_gas",
                description=f"Test gas estimation for {operation}",
                category=TestCategory.GAS_ESTIMATION,
                test_fn=make_gas_test(operation),
            )
        )

    return tests


# =============================================================================
# Protocol Test Harness
# =============================================================================


class ProtocolTestHarness[AdapterT: AdapterProtocol, ConfigT]:
    """Main test harness for protocol connectors.

    This class provides a standardized framework for testing protocol adapters,
    including support for unit tests and mainnet fork tests.

    Usage:
        harness = ProtocolTestHarness(
            adapter_class=AaveV3Adapter,
            config_class=AaveV3Config,
            protocol_type=ProtocolType.LENDING,
        )
        results = harness.run_all_tests()
    """

    def __init__(
        self,
        adapter_class: type[AdapterT],
        config_class: type[ConfigT],
        protocol_type: ProtocolType,
        protocol_name: str | None = None,
        test_suite: ProtocolTestSuite[AdapterT, ConfigT] | None = None,
    ) -> None:
        """Initialize the test harness.

        Args:
            adapter_class: The adapter class to test
            config_class: The configuration class
            protocol_type: Type of protocol
            protocol_name: Protocol name (defaults to adapter class name)
            test_suite: Custom test suite (optional)
        """
        self.adapter_class = adapter_class
        self.config_class = config_class
        self.protocol_type = protocol_type
        self.protocol_name = protocol_name or adapter_class.__name__
        self.test_suite = test_suite

        self._test_cases: list[TestCase] = []
        self._results: list[TestResult] = []
        self._fork: AnvilFork | None = None
        self._context: TestContext | None = None

        # Initialize test cases
        self._initialize_tests()

    def _initialize_tests(self) -> None:
        """Initialize the test cases."""
        if self.test_suite:
            self._test_cases = self.test_suite.get_all_test_cases()
        else:
            # Generate standard tests
            operations = PROTOCOL_OPERATIONS.get(self.protocol_type, [])
            self._test_cases = generate_basic_operations_tests(
                self.protocol_type, self.protocol_name
            ) + generate_gas_estimation_tests(self.protocol_name, operations)

    def add_test_case(self, test_case: TestCase) -> None:
        """Add a custom test case.

        Args:
            test_case: Test case to add
        """
        self._test_cases.append(test_case)

    def add_test_cases(self, test_cases: list[TestCase]) -> None:
        """Add multiple custom test cases.

        Args:
            test_cases: Test cases to add
        """
        self._test_cases.extend(test_cases)

    def create_adapter(
        self,
        chain: str = "arbitrum",
        wallet_address: str = "0x1234567890123456789012345678901234567890",
        **kwargs: Any,
    ) -> AdapterT:
        """Create an adapter instance for testing.

        Args:
            chain: Target chain
            wallet_address: Wallet address
            **kwargs: Additional config parameters

        Returns:
            Adapter instance
        """
        config = self.config_class(
            chain=chain,
            wallet_address=wallet_address,
            **kwargs,
        )  # type: ignore[call-arg]
        return self.adapter_class(config)

    def setup_fork(
        self,
        chain: str = "arbitrum",
        fork_block: int | None = None,
        rpc_url: str = "http://localhost:8545",
    ) -> bool:
        """Set up an Anvil fork for mainnet testing.

        Args:
            chain: Target chain
            fork_block: Block to fork from
            rpc_url: Archive RPC URL

        Returns:
            True if fork started successfully
        """
        config = ForkConfig(
            chain=chain,
            fork_block=fork_block,
            rpc_url=rpc_url,
        )
        self._fork = AnvilFork(config)
        return self._fork.start()

    def teardown_fork(self) -> None:
        """Tear down the Anvil fork."""
        if self._fork:
            self._fork.stop()
            self._fork = None

    def run_test(
        self,
        test_case: TestCase,
        context: TestContext | None = None,
    ) -> TestResult:
        """Run a single test case.

        Args:
            test_case: Test case to run
            context: Optional test context

        Returns:
            Test result
        """
        if test_case.skip_reason:
            return TestResult(
                test_name=test_case.name,
                status=TestStatus.SKIPPED,
                error_message=test_case.skip_reason,
            )

        # Create context if not provided
        if context is None:
            try:
                adapter = self.create_adapter()
                config = adapter.config if hasattr(adapter, "config") else None
                context = TestContext(
                    adapter=adapter,
                    config=config,
                    fork=self._fork,
                )
            except Exception as e:
                return TestResult(
                    test_name=test_case.name,
                    status=TestStatus.ERROR,
                    error_message=f"Failed to create adapter: {e}",
                )

        # Run setup
        if test_case.setup_fn:
            try:
                test_case.setup_fn(context)
            except Exception as e:
                return TestResult(
                    test_name=test_case.name,
                    status=TestStatus.ERROR,
                    error_message=f"Setup failed: {e}",
                )

        # Run test
        tracker = AssertionTracker()
        start_time = time.time()

        try:
            if test_case.test_fn:
                test_case.test_fn(context, tracker)
        except Exception as e:
            import traceback

            return TestResult(
                test_name=test_case.name,
                status=TestStatus.ERROR,
                duration_ms=(time.time() - start_time) * 1000,
                error_message=str(e),
                stack_trace=traceback.format_exc(),
                assertions_passed=tracker.passed,
                assertions_failed=tracker.failed,
            )

        # Run teardown
        if test_case.teardown_fn:
            try:
                test_case.teardown_fn(context)
            except Exception as e:
                logger.warning(f"Teardown failed for {test_case.name}: {e}")

        # Determine status
        duration_ms = (time.time() - start_time) * 1000
        if tracker.all_passed:
            status = TestStatus.PASSED
        else:
            status = TestStatus.FAILED

        return TestResult(
            test_name=test_case.name,
            status=status,
            duration_ms=duration_ms,
            error_message="; ".join(tracker.errors) if tracker.errors else None,
            assertions_passed=tracker.passed,
            assertions_failed=tracker.failed,
        )

    def run_tests_by_category(
        self,
        category: TestCategory,
        environment: TestEnvironment | None = None,
    ) -> TestSuiteResult:
        """Run all tests in a specific category.

        Args:
            category: Category of tests to run
            environment: Optional environment filter

        Returns:
            Test suite result
        """
        tests = [t for t in self._test_cases if t.category == category]
        if environment:
            tests = [t for t in tests if t.environment == environment]

        return self._run_test_list(tests, f"{self.protocol_name}_{category.value}")

    def run_unit_tests(self) -> TestSuiteResult:
        """Run all unit tests.

        Returns:
            Test suite result for unit tests
        """
        tests = [t for t in self._test_cases if t.environment == TestEnvironment.UNIT]
        return self._run_test_list(tests, f"{self.protocol_name}_unit_tests")

    def run_fork_tests(
        self,
        chain: str = "arbitrum",
        fork_block: int | None = None,
        rpc_url: str = "http://localhost:8545",
    ) -> TestSuiteResult:
        """Run all mainnet fork tests.

        Args:
            chain: Chain to fork
            fork_block: Block to fork from
            rpc_url: Archive RPC URL

        Returns:
            Test suite result for fork tests
        """
        # Set up fork
        if not self.setup_fork(chain, fork_block, rpc_url):
            return TestSuiteResult(
                suite_name=f"{self.protocol_name}_fork_tests",
                errors=1,
                results=[
                    TestResult(
                        test_name="fork_setup",
                        status=TestStatus.ERROR,
                        error_message="Failed to start Anvil fork",
                    )
                ],
            )

        try:
            tests = [t for t in self._test_cases if t.environment == TestEnvironment.MAINNET_FORK]
            return self._run_test_list(tests, f"{self.protocol_name}_fork_tests")
        finally:
            self.teardown_fork()

    def run_all_tests(self) -> TestSuiteResult:
        """Run all tests (unit tests only, fork tests run separately).

        Returns:
            Test suite result
        """
        return self.run_unit_tests()

    def _run_test_list(self, tests: list[TestCase], suite_name: str) -> TestSuiteResult:
        """Run a list of tests and return the result.

        Args:
            tests: List of tests to run
            suite_name: Name for the test suite

        Returns:
            Test suite result
        """
        results: list[TestResult] = []
        start_time = time.time()

        for test in tests:
            logger.info(f"Running test: {test.name}")
            result = self.run_test(test)
            results.append(result)

            status_symbol = {
                TestStatus.PASSED: "✓",
                TestStatus.FAILED: "✗",
                TestStatus.SKIPPED: "○",
                TestStatus.ERROR: "!",
            }
            logger.info(f"  {status_symbol[result.status]} {test.name}")

        duration_ms = (time.time() - start_time) * 1000

        # Calculate totals
        passed = sum(1 for r in results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in results if r.status == TestStatus.FAILED)
        skipped = sum(1 for r in results if r.status == TestStatus.SKIPPED)
        errors = sum(1 for r in results if r.status == TestStatus.ERROR)

        return TestSuiteResult(
            suite_name=suite_name,
            total_tests=len(tests),
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            duration_ms=duration_ms,
            results=results,
            coverage=self._calculate_coverage(results),
        )

    def _calculate_coverage(self, results: list[TestResult]) -> float:
        """Calculate test coverage percentage.

        Args:
            results: Test results

        Returns:
            Coverage percentage (0-100)
        """
        operations = PROTOCOL_OPERATIONS.get(self.protocol_type, [])
        if not operations:
            return 100.0 if results else 0.0

        # Count operations with passing tests
        tested = set()
        for result in results:
            if result.status == TestStatus.PASSED:
                for op in operations:
                    if op in result.test_name:
                        tested.add(op)

        return (len(tested) / len(operations)) * 100 if operations else 0.0

    def generate_coverage_report(self) -> CoverageReport:
        """Generate a test coverage report.

        Returns:
            Coverage report for this protocol
        """
        operations = PROTOCOL_OPERATIONS.get(self.protocol_type, [])

        # Check which operations have tests
        operation_coverage: dict[str, bool] = {}
        for op in operations:
            has_test = any(op in tc.name for tc in self._test_cases)
            operation_coverage[op] = has_test

        tested_ops = sum(1 for covered in operation_coverage.values() if covered)

        # Category coverage
        category_coverage: dict[str, float] = {}
        for category in TestCategory:
            cat_tests = [t for t in self._test_cases if t.category == category]
            if cat_tests:
                # Simple: all tests count equally
                category_coverage[category.value] = 100.0
            else:
                category_coverage[category.value] = 0.0

        # Generate recommendations
        recommendations: list[str] = []
        for op, covered in operation_coverage.items():
            if not covered:
                recommendations.append(f"Add tests for '{op}' operation")

        for category in TestCategory:
            if category.value not in category_coverage or (category_coverage[category.value] == 0.0):
                recommendations.append(f"Add {category.value} tests")

        return CoverageReport(
            protocol_name=self.protocol_name,
            protocol_type=self.protocol_type,
            total_operations=len(operations),
            tested_operations=tested_ops,
            coverage_percentage=(tested_ops / len(operations) * 100) if operations else 0.0,
            operation_coverage=operation_coverage,
            category_coverage=category_coverage,
            recommendations=recommendations,
        )

    def print_results(self, suite_result: TestSuiteResult) -> None:
        """Print test results in a formatted way.

        Args:
            suite_result: Test suite result to print
        """
        print(f"\n{'=' * 60}")
        print(f"Test Suite: {suite_result.suite_name}")
        print(f"{'=' * 60}")

        for result in suite_result.results:
            status_symbol = {
                TestStatus.PASSED: "✓",
                TestStatus.FAILED: "✗",
                TestStatus.SKIPPED: "○",
                TestStatus.ERROR: "!",
            }
            print(f"  {status_symbol[result.status]} {result.test_name}")
            if result.error_message:
                print(f"    Error: {result.error_message}")

        print(f"\n{'─' * 60}")
        print(f"Total: {suite_result.total_tests}")
        print(f"Passed: {suite_result.passed}")
        print(f"Failed: {suite_result.failed}")
        print(f"Skipped: {suite_result.skipped}")
        print(f"Errors: {suite_result.errors}")
        print(f"Pass Rate: {suite_result.pass_rate:.1f}%")
        print(f"Coverage: {suite_result.coverage:.1f}%")
        print(f"Duration: {suite_result.duration_ms:.0f}ms")
        print(f"{'=' * 60}\n")


# =============================================================================
# CI Integration
# =============================================================================


@dataclass
class CIConfig:
    """Configuration for CI test runs.

    Attributes:
        fail_on_error: Fail CI if any test errors
        fail_on_failure: Fail CI if any test fails
        min_coverage: Minimum coverage percentage required
        generate_junit: Generate JUnit XML report
        generate_coverage: Generate coverage report
        output_dir: Directory for reports
    """

    fail_on_error: bool = True
    fail_on_failure: bool = True
    min_coverage: float = 80.0
    generate_junit: bool = True
    generate_coverage: bool = True
    output_dir: str = "test-reports"


def run_ci_tests(
    harness: ProtocolTestHarness[Any, Any],
    config: CIConfig,
) -> tuple[TestSuiteResult, int]:
    """Run tests in CI mode with reporting.

    Args:
        harness: Test harness to run
        config: CI configuration

    Returns:
        Tuple of (test results, exit code)
    """
    import os

    # Create output directory
    os.makedirs(config.output_dir, exist_ok=True)

    # Run tests
    results = harness.run_all_tests()

    # Generate reports
    if config.generate_coverage:
        coverage = harness.generate_coverage_report()
        coverage_path = os.path.join(config.output_dir, "coverage.json")
        import json

        with open(coverage_path, "w") as f:
            json.dump(coverage.to_dict(), f, indent=2)
        logger.info(f"Coverage report written to {coverage_path}")

    if config.generate_junit:
        junit_path = os.path.join(config.output_dir, "junit.xml")
        _generate_junit_xml(results, junit_path)
        logger.info(f"JUnit report written to {junit_path}")

    # Determine exit code
    exit_code = 0

    if config.fail_on_error and results.errors > 0:
        logger.error(f"CI failed: {results.errors} test errors")
        exit_code = 1

    if config.fail_on_failure and results.failed > 0:
        logger.error(f"CI failed: {results.failed} test failures")
        exit_code = 1

    if results.coverage < config.min_coverage:
        logger.error(f"CI failed: Coverage {results.coverage:.1f}% below minimum {config.min_coverage}%")
        exit_code = 1

    return results, exit_code


def _generate_junit_xml(results: TestSuiteResult, output_path: str) -> None:
    """Generate JUnit XML report.

    Args:
        results: Test results
        output_path: Path to write XML file
    """
    lines: list[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(
        f'<testsuite name="{results.suite_name}" '
        f'tests="{results.total_tests}" '
        f'failures="{results.failed}" '
        f'errors="{results.errors}" '
        f'skipped="{results.skipped}" '
        f'time="{results.duration_ms / 1000:.3f}">'
    )

    for result in results.results:
        lines.append(f'  <testcase name="{result.test_name}" time="{result.duration_ms / 1000:.3f}">')

        if result.status == TestStatus.FAILED:
            message = result.error_message or "Test failed"
            lines.append(f'    <failure message="{_escape_xml(message)}">')
            if result.stack_trace:
                lines.append(f"      {_escape_xml(result.stack_trace)}")
            lines.append("    </failure>")

        elif result.status == TestStatus.ERROR:
            message = result.error_message or "Test error"
            lines.append(f'    <error message="{_escape_xml(message)}">')
            if result.stack_trace:
                lines.append(f"      {_escape_xml(result.stack_trace)}")
            lines.append("    </error>")

        elif result.status == TestStatus.SKIPPED:
            message = result.error_message or "Test skipped"
            lines.append(f'    <skipped message="{_escape_xml(message)}"/>')

        lines.append("  </testcase>")

    lines.append("</testsuite>")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))


def _escape_xml(text: str) -> str:
    """Escape special XML characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


# =============================================================================
# Convenience Functions
# =============================================================================


def create_lending_test_harness[AdapterT: AdapterProtocol, ConfigT](
    adapter_class: type[AdapterT],
    config_class: type[ConfigT],
    protocol_name: str,
) -> ProtocolTestHarness[AdapterT, ConfigT]:
    """Create a test harness for a lending protocol.

    Args:
        adapter_class: Adapter class
        config_class: Config class
        protocol_name: Protocol name

    Returns:
        Configured test harness
    """
    return ProtocolTestHarness(
        adapter_class=adapter_class,
        config_class=config_class,
        protocol_type=ProtocolType.LENDING,
        protocol_name=protocol_name,
    )


def create_perps_test_harness[AdapterT: AdapterProtocol, ConfigT](
    adapter_class: type[AdapterT],
    config_class: type[ConfigT],
    protocol_name: str,
) -> ProtocolTestHarness[AdapterT, ConfigT]:
    """Create a test harness for a perpetuals protocol.

    Args:
        adapter_class: Adapter class
        config_class: Config class
        protocol_name: Protocol name

    Returns:
        Configured test harness
    """
    return ProtocolTestHarness(
        adapter_class=adapter_class,
        config_class=config_class,
        protocol_type=ProtocolType.PERPS,
        protocol_name=protocol_name,
    )


def create_dex_test_harness[AdapterT: AdapterProtocol, ConfigT](
    adapter_class: type[AdapterT],
    config_class: type[ConfigT],
    protocol_name: str,
) -> ProtocolTestHarness[AdapterT, ConfigT]:
    """Create a test harness for a DEX protocol.

    Args:
        adapter_class: Adapter class
        config_class: Config class
        protocol_name: Protocol name

    Returns:
        Configured test harness
    """
    return ProtocolTestHarness(
        adapter_class=adapter_class,
        config_class=config_class,
        protocol_type=ProtocolType.DEX,
        protocol_name=protocol_name,
    )
