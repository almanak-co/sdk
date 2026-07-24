"""Unit tests for ProtocolTestHarness.generate_coverage_report and run_test.

The harness is instantiated via ``object.__new__`` (same pattern as
tests/unit/agent_tools/test_pm_tools.py) so each test controls exactly the
attributes the methods under test read, without triggering
``_initialize_tests`` in ``__init__``.
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.testing.protocol_harness import (
    PROTOCOL_OPERATIONS,
    ProtocolTestHarness,
    ProtocolType,
)

# Aliased imports: the production names start with "Test", which pytest would
# otherwise try (and warn) to collect as test classes.
from almanak.framework.testing.protocol_harness import TestCase as HarnessTestCase
from almanak.framework.testing.protocol_harness import TestCategory as HarnessTestCategory
from almanak.framework.testing.protocol_harness import TestContext as HarnessTestContext
from almanak.framework.testing.protocol_harness import TestStatus as HarnessTestStatus

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _DummyConfig:
    """Minimal config accepting the kwargs create_adapter passes."""

    def __init__(self, chain: str, wallet_address: str, **kwargs: Any) -> None:
        self.chain = chain
        self.wallet_address = wallet_address


class _DummyAdapter:
    """Adapter exposing a ``config`` attribute (hasattr branch: True)."""

    def __init__(self, config: Any) -> None:
        self.config = config


class _NoConfigAttrAdapter:
    """Adapter without a ``config`` attribute (hasattr branch: False)."""

    def __init__(self, config: Any) -> None:
        del config


class _ExplodingAdapter:
    """Adapter whose construction always fails."""

    def __init__(self, config: Any) -> None:
        raise RuntimeError("adapter construction failed")


def _make_harness(
    protocol_type: Any = ProtocolType.DEX,
    test_cases: list[HarnessTestCase] | None = None,
    adapter_class: type | None = _DummyAdapter,
    config_class: type | None = _DummyConfig,
) -> ProtocolTestHarness:
    harness = object.__new__(ProtocolTestHarness)
    harness.adapter_class = adapter_class
    harness.config_class = config_class
    harness.protocol_type = protocol_type
    harness.protocol_name = "test_protocol"
    harness.test_suite = None
    harness._test_cases = list(test_cases or [])
    harness._results = []
    harness._fork = None
    harness._context = None
    return harness


def _tc(
    name: str, category: HarnessTestCategory = HarnessTestCategory.BASIC_OPERATIONS, **kwargs: Any
) -> HarnessTestCase:
    return HarnessTestCase(name=name, description="test case", category=category, **kwargs)


def _context() -> HarnessTestContext:
    """Pre-built context so run_test skips adapter creation."""
    return HarnessTestContext(
        adapter=object(),
        config=None,
        test_accounts=["0x0000000000000000000000000000000000000001"],
        test_tokens={"ethereum": {}},
    )


# ---------------------------------------------------------------------------
# generate_coverage_report
# ---------------------------------------------------------------------------


class TestGenerateCoverageReport:
    def test_partial_operation_coverage(self):
        harness = _make_harness(
            protocol_type=ProtocolType.DEX,
            test_cases=[
                _tc("test_swap_basic"),
                _tc("test_get_quote_returns"),
            ],
        )

        report = harness.generate_coverage_report()

        assert report.protocol_name == "test_protocol"
        assert report.protocol_type is ProtocolType.DEX
        assert report.total_operations == 6
        assert report.tested_operations == 2
        assert report.coverage_percentage == pytest.approx(100.0 * 2 / 6)
        assert report.operation_coverage == {
            "swap": True,
            "get_quote": True,
            "get_token_price": False,
            "approve": False,
            "get_allowance": False,
            "get_liquidity": False,
        }
        # Both test cases are BASIC_OPERATIONS; every other category is empty.
        assert report.category_coverage["BASIC_OPERATIONS"] == 100.0
        for category in HarnessTestCategory:
            if category is not HarnessTestCategory.BASIC_OPERATIONS:
                assert report.category_coverage[category.value] == 0.0
        # Untested operations first (in PROTOCOL_OPERATIONS order), then
        # empty categories (in HarnessTestCategory order).
        assert report.recommendations == [
            "Add tests for 'get_token_price' operation",
            "Add tests for 'approve' operation",
            "Add tests for 'get_allowance' operation",
            "Add tests for 'get_liquidity' operation",
            "Add ERROR_HANDLING tests",
            "Add RECEIPT_PARSING tests",
            "Add GAS_ESTIMATION tests",
            "Add EDGE_CASES tests",
            "Add INTEGRATION tests",
        ]

    def test_full_coverage_produces_no_recommendations(self):
        operations = PROTOCOL_OPERATIONS[ProtocolType.DEX]
        assert len(operations) == len(HarnessTestCategory)
        test_cases = [_tc(f"test_{op}", category=cat) for op, cat in zip(operations, HarnessTestCategory, strict=True)]
        harness = _make_harness(protocol_type=ProtocolType.DEX, test_cases=test_cases)

        report = harness.generate_coverage_report()

        assert report.total_operations == len(operations)
        assert report.tested_operations == len(operations)
        assert report.coverage_percentage == 100.0
        assert all(report.operation_coverage.values())
        assert report.category_coverage == {c.value: 100.0 for c in HarnessTestCategory}
        assert report.recommendations == []

    def test_unknown_protocol_type_has_no_operations(self):
        sentinel = object()  # not a PROTOCOL_OPERATIONS key
        harness = _make_harness(protocol_type=sentinel, test_cases=[])

        report = harness.generate_coverage_report()

        assert report.protocol_type is sentinel
        assert report.total_operations == 0
        assert report.tested_operations == 0
        assert report.coverage_percentage == 0.0
        assert report.operation_coverage == {}
        assert report.category_coverage == {c.value: 0.0 for c in HarnessTestCategory}
        assert report.recommendations == [f"Add {c.value} tests" for c in HarnessTestCategory]

    def test_unknown_protocol_type_with_tests_still_scores_categories(self):
        harness = _make_harness(
            protocol_type=object(),
            test_cases=[_tc("test_something", category=HarnessTestCategory.EDGE_CASES)],
        )

        report = harness.generate_coverage_report()

        assert report.total_operations == 0
        assert report.coverage_percentage == 0.0
        assert report.category_coverage["EDGE_CASES"] == 100.0
        assert "Add EDGE_CASES tests" not in report.recommendations
        assert "Add BASIC_OPERATIONS tests" in report.recommendations

    def test_known_type_without_tests_recommends_everything(self):
        harness = _make_harness(protocol_type=ProtocolType.BRIDGE, test_cases=[])

        report = harness.generate_coverage_report()

        operations = PROTOCOL_OPERATIONS[ProtocolType.BRIDGE]
        assert report.total_operations == len(operations)
        assert report.tested_operations == 0
        assert report.coverage_percentage == 0.0
        assert report.operation_coverage == {op: False for op in operations}
        assert report.category_coverage == {c.value: 0.0 for c in HarnessTestCategory}
        assert report.recommendations == [f"Add tests for '{op}' operation" for op in operations] + [
            f"Add {c.value} tests" for c in HarnessTestCategory
        ]


# ---------------------------------------------------------------------------
# run_test
# ---------------------------------------------------------------------------


class TestRunTest:
    def test_skip_reason_short_circuits(self):
        # Even a broken adapter class is never touched for a skipped test.
        harness = _make_harness(adapter_class=_ExplodingAdapter)
        result = harness.run_test(_tc("test_skipped", skip_reason="not supported on chain"))

        assert result.status is HarnessTestStatus.SKIPPED
        assert result.error_message == "not supported on chain"
        assert result.duration_ms == 0.0

    def test_adapter_creation_failure_returns_error(self):
        harness = _make_harness(adapter_class=_ExplodingAdapter)
        result = harness.run_test(_tc("test_boom"))

        assert result.status is HarnessTestStatus.ERROR
        assert result.error_message is not None
        assert result.error_message.startswith("Failed to create adapter:")
        assert "adapter construction failed" in result.error_message

    def test_default_context_uses_adapter_config(self):
        harness = _make_harness(adapter_class=_DummyAdapter)
        seen: dict[str, Any] = {}

        def test_fn(context: HarnessTestContext, tracker: Any) -> None:
            seen["context"] = context
            tracker.assert_true(True, "sanity")

        result = harness.run_test(_tc("test_ctx", test_fn=test_fn))

        assert result.status is HarnessTestStatus.PASSED
        assert result.assertions_passed == 1
        assert result.assertions_failed == 0
        assert result.error_message is None
        context = seen["context"]
        assert isinstance(context.adapter, _DummyAdapter)
        assert context.config is context.adapter.config
        assert context.config.chain
        assert context.fork is None

    def test_default_context_without_config_attribute(self):
        harness = _make_harness(adapter_class=_NoConfigAttrAdapter)
        seen: dict[str, Any] = {}

        def test_fn(context: HarnessTestContext, tracker: Any) -> None:
            seen["context"] = context

        result = harness.run_test(_tc("test_no_config", test_fn=test_fn))

        assert result.status is HarnessTestStatus.PASSED
        assert seen["context"].config is None

    def test_provided_context_skips_adapter_creation(self):
        # Exploding adapter proves the create_adapter path is not taken.
        harness = _make_harness(adapter_class=_ExplodingAdapter)
        result = harness.run_test(_tc("test_with_ctx"), context=_context())

        assert result.status is HarnessTestStatus.PASSED
        assert result.assertions_passed == 0
        assert result.error_message is None

    def test_setup_failure_returns_error(self):
        harness = _make_harness()

        def setup_fn(context: HarnessTestContext) -> None:
            raise ValueError("bad setup")

        result = harness.run_test(_tc("test_setup_fail", setup_fn=setup_fn), context=_context())

        assert result.status is HarnessTestStatus.ERROR
        assert result.error_message == "Setup failed: bad setup"

    def test_setup_and_teardown_run_around_test(self):
        harness = _make_harness()
        calls: list[str] = []

        def setup_fn(context: HarnessTestContext) -> None:
            calls.append("setup")

        def test_fn(context: HarnessTestContext, tracker: Any) -> None:
            calls.append("test")
            tracker.assert_true(True)

        def teardown_fn(context: HarnessTestContext) -> None:
            calls.append("teardown")

        result = harness.run_test(
            _tc("test_lifecycle", setup_fn=setup_fn, test_fn=test_fn, teardown_fn=teardown_fn),
            context=_context(),
        )

        assert result.status is HarnessTestStatus.PASSED
        assert calls == ["setup", "test", "teardown"]

    def test_test_fn_exception_returns_error_with_stack_trace(self):
        harness = _make_harness()

        def test_fn(context: HarnessTestContext, tracker: Any) -> None:
            tracker.assert_true(True, "before the crash")
            raise RuntimeError("mid-test explosion")

        result = harness.run_test(_tc("test_crash", test_fn=test_fn), context=_context())

        assert result.status is HarnessTestStatus.ERROR
        assert result.error_message == "mid-test explosion"
        assert result.stack_trace is not None
        assert "RuntimeError" in result.stack_trace
        assert result.assertions_passed == 1
        assert result.assertions_failed == 0
        assert result.duration_ms >= 0.0

    def test_failed_assertions_produce_failed_status(self):
        harness = _make_harness()

        def test_fn(context: HarnessTestContext, tracker: Any) -> None:
            tracker.assert_true(True, "ok")
            tracker.assert_true(False, "nope")

        result = harness.run_test(_tc("test_failing", test_fn=test_fn), context=_context())

        assert result.status is HarnessTestStatus.FAILED
        assert result.assertions_passed == 1
        assert result.assertions_failed == 1
        assert result.error_message == "Expected True: nope"

    def test_teardown_failure_is_swallowed(self, caplog):
        harness = _make_harness()

        def teardown_fn(context: HarnessTestContext) -> None:
            raise RuntimeError("teardown broke")

        with caplog.at_level("WARNING"):
            result = harness.run_test(
                _tc("test_teardown_fail", teardown_fn=teardown_fn),
                context=_context(),
            )

        assert result.status is HarnessTestStatus.PASSED
        assert result.error_message is None
        assert any("Teardown failed for test_teardown_fail" in r.message for r in caplog.records)

    def test_no_test_fn_passes_with_zero_assertions(self):
        harness = _make_harness()
        result = harness.run_test(_tc("test_empty"), context=_context())

        assert result.status is HarnessTestStatus.PASSED
        assert result.assertions_passed == 0
        assert result.assertions_failed == 0
        assert result.error_message is None
