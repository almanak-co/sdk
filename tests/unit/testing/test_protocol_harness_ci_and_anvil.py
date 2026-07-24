"""Unit tests for protocol_harness CI reporting and Anvil fork lifecycle.

Covers ``_generate_junit_xml``, ``ProtocolTestHarness._run_test_list``,
``run_ci_tests``, and ``AnvilFork.start`` (CRAP-score reduction targets).

Follows the idioms of ``test_protocol_harness_coverage_report.py``: the
harness is instantiated via ``object.__new__`` so each test controls exactly
the attributes the methods under test read, and no real Anvil process is ever
spawned — ``subprocess.Popen`` and the readiness probe are monkeypatched.
"""

from __future__ import annotations

import json
import socket
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.testing import protocol_harness as ph_module
from almanak.framework.testing.protocol_harness import (
    AnvilFork,
    CIConfig,
    CoverageReport,
    ForkConfig,
    ProtocolTestHarness,
    ProtocolType,
    _generate_junit_xml,
    run_ci_tests,
)

# Aliased imports: the production names start with "Test", which pytest would
# otherwise try (and warn) to collect as test classes.
from almanak.framework.testing.protocol_harness import TestCase as HarnessTestCase
from almanak.framework.testing.protocol_harness import TestCategory as HarnessTestCategory
from almanak.framework.testing.protocol_harness import TestEnvironment as HarnessTestEnvironment
from almanak.framework.testing.protocol_harness import TestResult as HarnessTestResult
from almanak.framework.testing.protocol_harness import TestStatus as HarnessTestStatus
from almanak.framework.testing.protocol_harness import TestSuiteResult as HarnessTestSuiteResult

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _DummyConfig:
    """Minimal config accepting the kwargs create_adapter passes."""

    def __init__(self, chain: str, wallet_address: str, **kwargs: Any) -> None:
        self.chain = chain
        self.wallet_address = wallet_address


class _DummyAdapter:
    """Adapter exposing a ``config`` attribute."""

    def __init__(self, config: Any) -> None:
        self.config = config


def _make_harness(
    protocol_type: Any = ProtocolType.DEX,
    test_cases: list[HarnessTestCase] | None = None,
) -> ProtocolTestHarness:
    harness = object.__new__(ProtocolTestHarness)
    harness.adapter_class = _DummyAdapter
    harness.config_class = _DummyConfig
    harness.protocol_type = protocol_type
    harness.protocol_name = "test_protocol"
    harness.test_suite = None
    harness._test_cases = list(test_cases or [])
    harness._results = []
    harness._fork = None
    harness._context = None
    return harness


def _tc(
    name: str,
    category: HarnessTestCategory = HarnessTestCategory.BASIC_OPERATIONS,
    **kwargs: Any,
) -> HarnessTestCase:
    return HarnessTestCase(name=name, description="test case", category=category, **kwargs)


def _result(
    name: str,
    status: HarnessTestStatus,
    duration_ms: float = 250.0,
    error_message: str | None = None,
    stack_trace: str | None = None,
) -> HarnessTestResult:
    return HarnessTestResult(
        test_name=name,
        status=status,
        duration_ms=duration_ms,
        error_message=error_message,
        stack_trace=stack_trace,
    )


# ---------------------------------------------------------------------------
# ProtocolTestHarness._run_test_list
# ---------------------------------------------------------------------------


class TestRunTestList:
    def test_empty_list_returns_zero_totals(self):
        harness = _make_harness()

        suite = harness._run_test_list([], "empty_suite")

        assert suite.suite_name == "empty_suite"
        assert suite.total_tests == 0
        assert suite.passed == 0
        assert suite.failed == 0
        assert suite.skipped == 0
        assert suite.errors == 0
        assert suite.results == []
        assert suite.duration_ms >= 0.0
        # DEX has operations but no passing results touched any of them.
        assert suite.coverage == 0.0

    def test_mixed_statuses_are_aggregated(self, caplog):
        def passing_fn(context: Any, tracker: Any) -> None:
            tracker.assert_true(True, "ok")

        def failing_fn(context: Any, tracker: Any) -> None:
            tracker.assert_true(False, "nope")

        def erroring_fn(context: Any, tracker: Any) -> None:
            raise RuntimeError("kaboom")

        tests = [
            # Name contains "swap" so _calculate_coverage counts the DEX op.
            _tc("test_swap_works", test_fn=passing_fn),
            _tc("test_fail_case", test_fn=failing_fn),
            _tc("test_skip_case", skip_reason="not supported"),
            _tc("test_error_case", test_fn=erroring_fn),
        ]
        harness = _make_harness(test_cases=tests)

        with caplog.at_level("INFO"):
            suite = harness._run_test_list(tests, "mixed_suite")

        assert suite.suite_name == "mixed_suite"
        assert suite.total_tests == 4
        assert suite.passed == 1
        assert suite.failed == 1
        assert suite.skipped == 1
        assert suite.errors == 1
        assert [r.test_name for r in suite.results] == [t.name for t in tests]
        assert [r.status for r in suite.results] == [
            HarnessTestStatus.PASSED,
            HarnessTestStatus.FAILED,
            HarnessTestStatus.SKIPPED,
            HarnessTestStatus.ERROR,
        ]
        assert suite.duration_ms >= 0.0
        # Exactly one of the six DEX operations ("swap") has a passing test.
        assert suite.coverage == pytest.approx(100.0 / 6)
        # Every test is announced and gets a status-symbol line.
        for test in tests:
            assert any(f"Running test: {test.name}" in r.message for r in caplog.records)

    def test_all_passing_suite(self):
        def passing_fn(context: Any, tracker: Any) -> None:
            tracker.assert_true(True)

        tests = [
            _tc("test_swap_a", test_fn=passing_fn),
            _tc("test_get_quote_b", test_fn=passing_fn),
        ]
        harness = _make_harness(test_cases=tests)

        suite = harness._run_test_list(tests, "green_suite")

        assert suite.passed == 2
        assert suite.failed == 0
        assert suite.errors == 0
        assert suite.skipped == 0
        assert suite.success is True
        assert suite.coverage == pytest.approx(100.0 * 2 / 6)


# ---------------------------------------------------------------------------
# _generate_junit_xml
# ---------------------------------------------------------------------------


class TestGenerateJunitXml:
    def test_empty_suite_writes_header_only(self, tmp_path: Path):
        suite = HarnessTestSuiteResult(suite_name="empty", duration_ms=2500.0)
        output = tmp_path / "junit.xml"

        _generate_junit_xml(suite, str(output))

        root = ET.parse(output).getroot()
        assert root.tag == "testsuite"
        assert root.attrib == {
            "name": "empty",
            "tests": "0",
            "failures": "0",
            "errors": "0",
            "skipped": "0",
            "time": "2.500",
        }
        assert list(root) == []

    def test_all_status_branches_and_escaping(self, tmp_path: Path):
        results = [
            _result("test_passed", HarnessTestStatus.PASSED, duration_ms=250.0),
            _result(
                "test_failed_full",
                HarnessTestStatus.FAILED,
                error_message='bad "quote" & <tag> \'apos\'',
                stack_trace="Traceback <frame> & more",
            ),
            _result("test_failed_bare", HarnessTestStatus.FAILED),
            _result(
                "test_error_full",
                HarnessTestStatus.ERROR,
                error_message="boom & bust",
                stack_trace="ErrorTrace<2>",
            ),
            _result("test_error_bare", HarnessTestStatus.ERROR),
            _result("test_skipped_reason", HarnessTestStatus.SKIPPED, error_message="chain <unsupported>"),
            _result("test_skipped_bare", HarnessTestStatus.SKIPPED),
        ]
        suite = HarnessTestSuiteResult(
            suite_name="full_suite",
            total_tests=7,
            passed=1,
            failed=2,
            skipped=2,
            errors=2,
            duration_ms=1234.5,
            results=results,
        )
        output = tmp_path / "junit.xml"

        _generate_junit_xml(suite, str(output))

        raw = output.read_text()
        assert raw.startswith('<?xml version="1.0" encoding="UTF-8"?>')
        # Escaping happened at the string level.
        assert "&amp;" in raw
        assert "&lt;tag&gt;" in raw
        assert "&quot;quote&quot;" in raw
        assert "&apos;apos&apos;" in raw

        root = ET.parse(output).getroot()
        assert root.attrib["name"] == "full_suite"
        assert root.attrib["tests"] == "7"
        assert root.attrib["failures"] == "2"
        assert root.attrib["errors"] == "2"
        assert root.attrib["skipped"] == "2"
        assert root.attrib["time"] == "1.234"

        cases = {tc.attrib["name"]: tc for tc in root}
        assert set(cases) == {r.test_name for r in results}
        for tc in root:
            assert tc.attrib["time"] == "0.250"

        # PASSED: no child element.
        assert list(cases["test_passed"]) == []

        # FAILED with message + stack trace (round-trips unescaped).
        failure = cases["test_failed_full"].find("failure")
        assert failure is not None
        assert failure.attrib["message"] == 'bad "quote" & <tag> \'apos\''
        assert failure.text is not None
        assert "Traceback <frame> & more" in failure.text

        # FAILED without message: fallback text, no stack-trace body.
        bare_failure = cases["test_failed_bare"].find("failure")
        assert bare_failure is not None
        assert bare_failure.attrib["message"] == "Test failed"
        assert bare_failure.text is None or bare_failure.text.strip() == ""

        # ERROR with message + stack trace.
        error = cases["test_error_full"].find("error")
        assert error is not None
        assert error.attrib["message"] == "boom & bust"
        assert error.text is not None
        assert "ErrorTrace<2>" in error.text

        # ERROR without message: fallback.
        bare_error = cases["test_error_bare"].find("error")
        assert bare_error is not None
        assert bare_error.attrib["message"] == "Test error"
        assert bare_error.text is None or bare_error.text.strip() == ""

        # SKIPPED: self-closing element with message.
        skipped = cases["test_skipped_reason"].find("skipped")
        assert skipped is not None
        assert skipped.attrib["message"] == "chain <unsupported>"
        assert cases["test_skipped_bare"].find("skipped").attrib["message"] == "Test skipped"


# ---------------------------------------------------------------------------
# run_ci_tests
# ---------------------------------------------------------------------------


class _StubHarness:
    """Duck-typed stand-in for ProtocolTestHarness in run_ci_tests."""

    def __init__(self, suite: HarnessTestSuiteResult, coverage: CoverageReport | None = None) -> None:
        self._suite = suite
        self._coverage = coverage or CoverageReport(protocol_name="stub", protocol_type=ProtocolType.DEX)
        self.coverage_calls = 0

    def run_all_tests(self) -> HarnessTestSuiteResult:
        return self._suite

    def generate_coverage_report(self) -> CoverageReport:
        self.coverage_calls += 1
        return self._coverage


class TestRunCiTests:
    def test_success_writes_both_reports(self, tmp_path: Path):
        suite = HarnessTestSuiteResult(
            suite_name="ci_suite",
            total_tests=2,
            passed=2,
            duration_ms=100.0,
            coverage=95.0,
        )
        harness = _StubHarness(suite)
        output_dir = tmp_path / "reports"  # does not exist yet: covers makedirs
        config = CIConfig(min_coverage=80.0, output_dir=str(output_dir))

        results, exit_code = run_ci_tests(harness, config)

        assert results is suite
        assert exit_code == 0
        assert harness.coverage_calls == 1

        coverage_data = json.loads((output_dir / "coverage.json").read_text())
        assert coverage_data["protocol_name"] == "stub"
        assert coverage_data["protocol_type"] == ProtocolType.DEX.value

        junit_root = ET.parse(output_dir / "junit.xml").getroot()
        assert junit_root.attrib["name"] == "ci_suite"
        assert junit_root.attrib["tests"] == "2"

    def test_errors_and_failures_set_exit_code(self, tmp_path: Path, caplog):
        suite = HarnessTestSuiteResult(
            suite_name="red_suite",
            total_tests=3,
            passed=1,
            failed=1,
            errors=1,
            coverage=100.0,
        )
        config = CIConfig(output_dir=str(tmp_path / "red"))

        with caplog.at_level("ERROR"):
            results, exit_code = run_ci_tests(_StubHarness(suite), config)

        assert exit_code == 1
        assert results is suite
        assert any("1 test errors" in r.message for r in caplog.records)
        assert any("1 test failures" in r.message for r in caplog.records)

    def test_low_coverage_fails_even_when_green(self, tmp_path: Path, caplog):
        suite = HarnessTestSuiteResult(suite_name="low_cov", total_tests=1, passed=1, coverage=10.0)
        config = CIConfig(min_coverage=80.0, output_dir=str(tmp_path / "lowcov"))

        with caplog.at_level("ERROR"):
            _, exit_code = run_ci_tests(_StubHarness(suite), config)

        assert exit_code == 1
        assert any("Coverage 10.0% below minimum 80.0%" in r.message for r in caplog.records)

    def test_disabled_flags_skip_reports_and_tolerate_red(self, tmp_path: Path):
        suite = HarnessTestSuiteResult(
            suite_name="tolerant",
            total_tests=2,
            failed=1,
            errors=1,
            coverage=0.0,
        )
        harness = _StubHarness(suite)
        output_dir = tmp_path / "tolerant"
        config = CIConfig(
            fail_on_error=False,
            fail_on_failure=False,
            min_coverage=0.0,
            generate_junit=False,
            generate_coverage=False,
            output_dir=str(output_dir),
        )

        _, exit_code = run_ci_tests(harness, config)

        assert exit_code == 0
        assert harness.coverage_calls == 0
        assert output_dir.exists()
        assert list(output_dir.iterdir()) == []

    def test_with_real_harness_end_to_end(self, tmp_path: Path):
        def passing_fn(context: Any, tracker: Any) -> None:
            tracker.assert_true(True)

        tests = [
            _tc("test_swap_ok", test_fn=passing_fn),
            # Fork-environment test must be excluded by run_all_tests.
            _tc("test_approve_fork", test_fn=passing_fn, environment=HarnessTestEnvironment.MAINNET_FORK),
        ]
        harness = _make_harness(test_cases=tests)
        output_dir = tmp_path / "real"
        config = CIConfig(min_coverage=0.0, output_dir=str(output_dir))

        results, exit_code = run_ci_tests(harness, config)

        assert exit_code == 0
        assert results.suite_name == "test_protocol_unit_tests"
        assert results.total_tests == 1  # fork test filtered out
        assert results.passed == 1
        junit_root = ET.parse(output_dir / "junit.xml").getroot()
        assert junit_root.attrib["tests"] == "1"
        assert (output_dir / "coverage.json").exists()


# ---------------------------------------------------------------------------
# AnvilFork.start / stop / _is_ready
# ---------------------------------------------------------------------------


class _FakePopen:
    """Records the anvil command line and terminate/kill calls."""

    def __init__(self, cmd: list[str], stdout: Any = None, stderr: Any = None) -> None:
        self.cmd = cmd
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def kill(self) -> None:
        self.killed = True


class _FakeClock:
    """Deterministic replacement for the module-level ``time`` import."""

    def __init__(self, step: float) -> None:
        self.now = 0.0
        self.step = step
        self.sleeps: list[float] = []

    def time(self) -> float:
        self.now += self.step
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _fork(**overrides: Any) -> AnvilFork:
    config = ForkConfig(chain="ethereum", rpc_url="http://localhost:9999", **overrides)
    return AnvilFork(config=config)


class TestAnvilForkStart:
    def test_already_running_short_circuits(self, monkeypatch, caplog):
        fork = _fork()
        fork.is_running = True

        def _no_spawn(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("Popen must not be called when already running")

        monkeypatch.setattr(ph_module.subprocess, "Popen", _no_spawn)

        with caplog.at_level("WARNING"):
            assert fork.start() is True

        assert any("already running" in r.message for r in caplog.records)

    def test_success_without_fork_block(self, monkeypatch):
        spawned: list[_FakePopen] = []

        def _spawn(cmd: list[str], **kwargs: Any) -> _FakePopen:
            popen = _FakePopen(cmd, **kwargs)
            spawned.append(popen)
            return popen

        monkeypatch.setattr(ph_module.subprocess, "Popen", _spawn)
        monkeypatch.setattr(AnvilFork, "_is_ready", lambda self: True)

        fork = _fork(anvil_port=8551)
        assert fork.start() is True
        assert fork.is_running is True
        assert fork.process is spawned[0]
        assert spawned[0].cmd == [
            "anvil",
            "--fork-url",
            "http://localhost:9999",
            "--port",
            "8551",
            "--silent",
        ]

    def test_fork_block_extends_command(self, monkeypatch):
        spawned: list[_FakePopen] = []

        def _spawn(cmd: list[str], **kwargs: Any) -> _FakePopen:
            popen = _FakePopen(cmd, **kwargs)
            spawned.append(popen)
            return popen

        monkeypatch.setattr(ph_module.subprocess, "Popen", _spawn)
        monkeypatch.setattr(AnvilFork, "_is_ready", lambda self: True)

        fork = _fork(fork_block=17000000)
        assert fork.start() is True
        cmd = spawned[0].cmd
        flag_index = cmd.index("--fork-block-number")
        assert cmd[flag_index + 1] == "17000000"

    def test_timeout_stops_process_and_returns_false(self, monkeypatch, caplog):
        spawned: list[_FakePopen] = []

        def _spawn(cmd: list[str], **kwargs: Any) -> _FakePopen:
            popen = _FakePopen(cmd, **kwargs)
            spawned.append(popen)
            return popen

        clock = _FakeClock(step=0.4)
        monkeypatch.setattr(ph_module.subprocess, "Popen", _spawn)
        monkeypatch.setattr(ph_module, "time", clock)
        monkeypatch.setattr(AnvilFork, "_is_ready", lambda self: False)

        fork = _fork(timeout_seconds=1.0)
        with caplog.at_level("ERROR"):
            assert fork.start() is False

        assert any("startup timed out" in r.message for r in caplog.records)
        # The loop body ran at least once (readiness poll interval).
        assert clock.sleeps == [0.5]
        # stop() terminated and cleared the process.
        assert spawned[0].terminated is True
        assert fork.process is None
        assert fork.is_running is False

    def test_anvil_binary_missing_returns_false(self, monkeypatch, caplog):
        def _spawn(*args: Any, **kwargs: Any) -> None:
            raise FileNotFoundError("anvil")

        monkeypatch.setattr(ph_module.subprocess, "Popen", _spawn)

        fork = _fork()
        with caplog.at_level("ERROR"):
            assert fork.start() is False

        assert fork.is_running is False
        assert fork.process is None
        assert any("Anvil not found" in r.message for r in caplog.records)

    def test_generic_exception_returns_false(self, monkeypatch, caplog):
        def _spawn(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("spawn exploded")

        monkeypatch.setattr(ph_module.subprocess, "Popen", _spawn)

        fork = _fork()
        with caplog.at_level("ERROR"):
            assert fork.start() is False

        assert fork.is_running is False
        assert any("Failed to start Anvil fork: spawn exploded" in r.message for r in caplog.records)


class TestAnvilForkHelpers:
    def test_stop_without_process_is_noop(self):
        fork = _fork()
        fork.is_running = True
        fork.stop()
        assert fork.is_running is False
        assert fork.process is None

    def test_stop_kills_process_when_wait_times_out(self):
        popen = _FakePopen(["anvil"])

        def _hanging_wait(timeout: float | None = None) -> int:
            raise ph_module.subprocess.TimeoutExpired(cmd="anvil", timeout=timeout or 5)

        popen.wait = _hanging_wait  # type: ignore[method-assign]
        fork = _fork()
        fork.process = popen
        fork.is_running = True

        fork.stop()

        assert popen.terminated is True
        assert popen.killed is True
        assert fork.process is None
        assert fork.is_running is False

    def test_get_rpc_url_uses_configured_port(self):
        fork = _fork(anvil_port=8600)
        assert fork.get_rpc_url() == "http://localhost:8600"

    def test_is_ready_true_when_port_listening(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind(("localhost", 0))
            server.listen(1)
            port = server.getsockname()[1]
            fork = _fork(anvil_port=port)
            assert fork._is_ready() is True

    def test_is_ready_false_when_connection_refused(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("localhost", 0))
            free_port = probe.getsockname()[1]
        # Port was released above; nothing is listening on it.
        fork = _fork(anvil_port=free_port)
        assert fork._is_ready() is False
