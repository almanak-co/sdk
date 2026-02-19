"""Testing Framework for Almanak Strategy Framework.

This module provides testing utilities including:
- A/B Testing Framework for strategy variant comparison
- Protocol Testing Harness for standardized connector testing

A/B Testing Usage:
    from almanak.framework.testing import ABTest, ABTestConfig, ABTestManager

    # Create an A/B test manager
    manager = ABTestManager(strategy_id="my_strategy")

    # Create a new A/B test
    result = manager.create_ab_test(
        variant_a="v_baseline",
        variant_b="v_experimental",
        split_ratio=0.5,  # 50/50 split
        total_capital_usd=Decimal("100000"),
    )

    # Check comparison
    comparison = manager.compare()

    # End test and select winner
    winner = manager.end_test(select_winner="variant_b")

Protocol Testing Harness Usage:
    from almanak.framework.testing import ProtocolTestHarness, ProtocolType

    # Create a test harness for a protocol adapter
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

from .ab_test import (
    ABTest,
    ABTestCallback,
    ABTestConfig,
    ABTestEventType,
    ABTestManager,
    ABTestResult,
    ABTestStatus,
    CreateTestResult,
    EndTestResult,
    StatisticalResult,
    VariantComparison,
    VariantMetrics,
)
from .copy_replay import CopyReplayRunner
from .protocol_harness import (
    # Constants
    PROTOCOL_OPERATIONS,
    # Anvil Fork
    AnvilFork,
    # Assertion Helpers
    AssertionTracker,
    # CI Integration
    CIConfig,
    CoverageReport,
    ForkConfig,
    # Main Classes
    ProtocolTestHarness,
    ProtocolTestSuite,
    # Enums
    ProtocolType,
    # Data Classes
    TestCase,
    TestCategory,
    TestContext,
    TestEnvironment,
    TestResult,
    TestStatus,
    TestSuiteResult,
    create_dex_test_harness,
    # Convenience Functions
    create_lending_test_harness,
    create_perps_test_harness,
    generate_basic_operations_tests,
    # Test Generators
    generate_config_validation_tests,
    generate_gas_estimation_tests,
    run_ci_tests,
)

__all__ = [
    # A/B Testing
    "ABTest",
    "ABTestConfig",
    "ABTestManager",
    "ABTestStatus",
    "ABTestResult",
    "VariantMetrics",
    "VariantComparison",
    "StatisticalResult",
    "ABTestEventType",
    "ABTestCallback",
    "CreateTestResult",
    "EndTestResult",
    # Protocol Testing Harness - Main Classes
    "ProtocolTestHarness",
    "ProtocolTestSuite",
    # Protocol Testing Harness - Enums
    "ProtocolType",
    "TestCategory",
    "TestStatus",
    "TestEnvironment",
    # Protocol Testing Harness - Data Classes
    "TestCase",
    "TestResult",
    "TestSuiteResult",
    "CoverageReport",
    "ForkConfig",
    "TestContext",
    # Protocol Testing Harness - Anvil Fork
    "AnvilFork",
    # Protocol Testing Harness - Assertion Helpers
    "AssertionTracker",
    # Protocol Testing Harness - CI Integration
    "CIConfig",
    "run_ci_tests",
    # Protocol Testing Harness - Test Generators
    "generate_config_validation_tests",
    "generate_basic_operations_tests",
    "generate_gas_estimation_tests",
    # Protocol Testing Harness - Convenience Functions
    "create_lending_test_harness",
    "create_perps_test_harness",
    "create_dex_test_harness",
    # Protocol Testing Harness - Constants
    "PROTOCOL_OPERATIONS",
    # Copy-trading replay
    "CopyReplayRunner",
]
