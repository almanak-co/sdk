"""Unit tests for `almanak.framework.cli.intent_debug`.

Targets the helpers extracted from `trace_strategy`, `print_inspection_result`,
and `print_trace_result` (VIB-4080 W3 Sub-E). Per the W3 Sub-A audit,
`trace_strategy` and `print_trace_result` had **zero** direct test references
in the repo before this file landed — every test below is pure coverage lift.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.cli import intent_debug as ide
from almanak.framework.cli.intent_debug import (
    IntentInspectionResult,
    TraceResult,
    TraceStep,
    _compile_intent_for_trace,
    _normalize_decide_result,
    _print_action_bundle,
    _print_metadata_section,
    _resolve_trace_config,
    _TraceStepRecorder,
    print_inspection_result,
    print_trace_result,
    trace_strategy,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_inspection_result(**overrides) -> IntentInspectionResult:
    """Build an IntentInspectionResult with sensible defaults for tests."""
    base = {
        "strategy_name": "FakeStrategy",
        "strategy_path": "/tmp/fake.py",
        "metadata": {
            "name": "fake",
            "version": "1.0",
            "description": "demo",
            "author": "alice",
            "tags": ["lp", "demo"],
            "supported_chains": ["arbitrum"],
            "supported_protocols": ["uniswap_v3"],
        },
        "intent_types": ["LP_OPEN", "HOLD"],
        "intent_details": [],
        "state_diagrams": {"LP_OPEN": "S1->S2"},
        "action_bundles": {
            "LP_OPEN": {
                "status": "SUCCESS",
                "transactions": [
                    {
                        "to": "0xabc",
                        "data": "0xdead",
                        "gas_estimate": 21000,
                        "description": "open lp",
                    }
                ],
                "total_gas_estimate": 21000,
            },
            "HOLD": {"status": "SUCCESS", "note": "HOLD intents require no transactions"},
        },
        "errors": [],
    }
    base.update(overrides)
    return IntentInspectionResult(**base)


def _make_trace_step(step_number: int = 1, success: bool = True, error: str | None = None) -> TraceStep:
    return TraceStep(
        step_number=step_number,
        description=f"step {step_number}",
        state="STATE",
        intent={"type": "HOLD"},
        action_bundle=None,
        success=success,
        error=error,
        timestamp=datetime.now(UTC),
    )


def _make_trace_result(success: bool = True, **overrides) -> TraceResult:
    base = {
        "strategy_name": "FakeStrategy",
        "scenario_file": "/tmp/scenario.json",
        "scenario": {"prices": {"ETH": 2000}},
        "steps": [_make_trace_step(1), _make_trace_step(2, success=success, error=None if success else "boom")],
        "final_intent": {"type": "HOLD", "intent_id": "abc-123"},
        "final_action_bundle": None,
        "success": success,
        "error": None if success else "boom",
        "execution_time_ms": 12.34,
    }
    base.update(overrides)
    return TraceResult(**base)


# ---------------------------------------------------------------------------
# trace_strategy: load-error path + happy-path through the full pipeline
# ---------------------------------------------------------------------------


class TestTraceStrategy:
    def test_returns_load_failure_when_file_missing(self, tmp_path: Path) -> None:
        # Non-existent file → load_strategy_from_file returns an error → trace_strategy
        # short-circuits to a failed TraceResult without touching strategy initialization.
        missing = tmp_path / "does_not_exist.py"
        result = trace_strategy(missing, scenario={}, scenario_file=str(missing))
        assert result.success is False
        assert result.strategy_name == "unknown"
        assert result.error is not None and "File not found" in result.error
        assert any(s.state == "ERROR" and not s.success for s in result.steps)

    def test_happy_path_with_mocked_loader_returns_hold(self, tmp_path: Path) -> None:
        # Build a minimal strategy class whose decide() returns a HOLD intent.
        # IntentCompiler is patched so the test focuses on trace_strategy's
        # control flow rather than oracle / network configuration.
        from almanak.framework.intents import Intent
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class _StubStrategy(IntentStrategy):
            STRATEGY_NAME = "Stub"

            def __init__(self, config, compiler):
                self.config = config
                self.compiler = compiler

            def decide(self, market):
                return Intent.hold(reason="ok")

            def generate_teardown_intents(self, market):
                return []

            def get_open_positions(self):
                return []

        fake_file = tmp_path / "strategy.py"
        fake_file.write_text("# placeholder, loader is mocked\n")
        # Sibling config.py — exercises _load_config_from_module's happy path
        # (the no-sibling branch raises NameError by design; see _make_mock_config).
        sibling_config = tmp_path / "config.py"
        sibling_config.write_text(
            "from dataclasses import dataclass\n"
            "@dataclass\n"
            "class StubConfig:\n"
            "    deployment_id: str = 'trace-test'\n"
            "    chain: str = 'arbitrum'\n"
            "    wallet_address: str = '0x' + '1' * 40\n"
        )

        fake_compiler = MagicMock()
        with (
            patch.object(ide, "load_strategy_from_file", return_value=(_StubStrategy, None)),
            patch.object(ide, "IntentCompiler", return_value=fake_compiler),
        ):
            result = trace_strategy(fake_file, scenario={}, scenario_file=None)

        assert result.success is True, f"trace failed: {result.error}, steps={result.steps}"
        assert result.strategy_name == "Stub"
        # HOLD path: no compilation, but a "HOLD intent - no compilation needed" step exists.
        assert any(step.state == "HOLD" for step in result.steps)
        assert result.final_action_bundle is None
        assert result.final_intent is not None and result.final_intent.get("type") == "HOLD"
        # IntentCompiler.compile is never invoked for the HOLD branch.
        fake_compiler.compile.assert_not_called()

    def test_compile_failure_surfaces_as_trace_failure(self, tmp_path: Path) -> None:
        """Compilation failure must propagate to `TraceResult.success=False`."""
        from almanak.framework.intents import Intent
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class _StubStrategy(IntentStrategy):
            STRATEGY_NAME = "StubFail"

            def __init__(self, config, compiler):
                self.config = config
                self.compiler = compiler

            def decide(self, market):
                # Non-HOLD intent so the compile path runs.
                from decimal import Decimal

                return Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1"),
                    chain="arbitrum",
                )

            def generate_teardown_intents(self, market):
                return []

            def get_open_positions(self):
                return []

        fake_file = tmp_path / "strategy.py"
        fake_file.write_text("# placeholder, loader is mocked\n")
        sibling = tmp_path / "config.py"
        sibling.write_text(
            "from dataclasses import dataclass\n"
            "@dataclass\n"
            "class StubConfig:\n"
            "    deployment_id: str = 'trace-test'\n"
            "    chain: str = 'arbitrum'\n"
            "    wallet_address: str = '0x' + '1' * 40\n"
        )

        # Compiler raises during compile() -> _compile_intent_for_trace returns
        # error string; trace_strategy must build a failure TraceResult.
        fake_compiler = MagicMock()
        fake_compiler.compile.side_effect = RuntimeError("router unreachable")
        with (
            patch.object(ide, "load_strategy_from_file", return_value=(_StubStrategy, None)),
            patch.object(ide, "IntentCompiler", return_value=fake_compiler),
        ):
            result = trace_strategy(fake_file, scenario={}, scenario_file=None)

        assert result.success is False
        assert result.error is not None
        assert "router unreachable" in result.error
        assert any(s.state == "COMPILE_ERROR" and s.success is False for s in result.steps)


class TestCompileIntentForTrace:
    """Direct coverage of `_compile_intent_for_trace` return contract."""

    def _make_intent(self, intent_type: str = "swap"):
        from decimal import Decimal

        from almanak.framework.intents import Intent

        if intent_type == "hold":
            return Intent.hold(reason="ok")
        return Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1"),
            chain="arbitrum",
        )

    def test_hold_intent_skips_compile_and_returns_no_error(self):
        recorder = _TraceStepRecorder()
        bundle, error = _compile_intent_for_trace(self._make_intent("hold"), MagicMock(), recorder)
        assert bundle is None
        assert error is None
        assert any(step.state == "HOLD" for step in recorder.steps)

    def test_compiler_exception_returns_error_string(self):
        recorder = _TraceStepRecorder()
        compiler = MagicMock()
        compiler.compile.side_effect = ValueError("boom")
        bundle, error = _compile_intent_for_trace(self._make_intent(), compiler, recorder)
        assert bundle is None
        assert error == "boom"
        assert any(s.state == "COMPILE_ERROR" and s.success is False for s in recorder.steps)

    def test_non_success_status_returns_error_string(self):
        from almanak.framework.intents.compiler import CompilationStatus

        recorder = _TraceStepRecorder()
        compiler = MagicMock()
        compilation_result = MagicMock()
        compilation_result.status = CompilationStatus.FAILED
        compilation_result.error = "no liquidity"
        compiler.compile.return_value = compilation_result

        bundle, error = _compile_intent_for_trace(self._make_intent(), compiler, recorder)
        assert bundle is None
        assert error == "no liquidity"
        assert any(s.state == "COMPILE_ERROR" and s.success is False for s in recorder.steps)

    def test_non_success_status_with_no_error_falls_back_to_default_message(self):
        from almanak.framework.intents.compiler import CompilationStatus

        recorder = _TraceStepRecorder()
        compiler = MagicMock()
        compilation_result = MagicMock()
        compilation_result.status = CompilationStatus.FAILED
        compilation_result.error = None
        compiler.compile.return_value = compilation_result

        _, error = _compile_intent_for_trace(self._make_intent(), compiler, recorder)
        assert error == "Compilation failed"


# ---------------------------------------------------------------------------
# trace_strategy helpers: covers all decide()-shape branches in one test
# ---------------------------------------------------------------------------


class TestNormalizeDecideResult:
    def test_covers_none_empty_list_and_passthrough_branches(self) -> None:
        from almanak.framework.intents import Intent

        # Branch 1: None → HOLD, no DECIDE_MULTI step recorded.
        recorder = _TraceStepRecorder()
        intent = _normalize_decide_result(None, recorder)
        assert intent.intent_type.value == "HOLD"
        assert recorder.steps == []

        # Branch 2: empty list → HOLD, DECIDE_MULTI step IS still recorded
        # (matches original behaviour even for zero-length lists).
        recorder = _TraceStepRecorder()
        intent = _normalize_decide_result([], recorder)
        assert intent.intent_type.value == "HOLD"
        assert len(recorder.steps) == 1
        assert recorder.steps[0].state == "DECIDE_MULTI"

        # Branch 3: a bare AnyIntent passes through untouched and adds no steps.
        recorder = _TraceStepRecorder()
        provided = Intent.hold(reason="explicit")
        intent = _normalize_decide_result(provided, recorder)
        assert intent is provided
        assert recorder.steps == []


# ---------------------------------------------------------------------------
# _resolve_trace_config: covers the sibling-loaded path AND the legacy
# NameError-by-design path in one test
# ---------------------------------------------------------------------------


class TestResolveTraceConfig:
    def test_sibling_module_branch_and_no_sibling_mock_branch(self, tmp_path: Path) -> None:
        # Branch 1: no sibling config.py → `_make_mock_config` builds a minimal
        # dataclass populated from the chain/wallet args.
        strat = tmp_path / "strategy.py"
        strat.write_text("# placeholder\n")
        wallet = "0x" + "1" * 40
        config = _resolve_trace_config(strat, {}, "arbitrum", wallet)
        assert config is not None
        assert config.deployment_id == "trace-test"
        assert config.chain == "arbitrum"
        assert config.wallet_address == wallet
        assert config.to_dict() == {
            "deployment_id": "trace-test",
            "chain": "arbitrum",
            "wallet_address": wallet,
        }

        # Branch 2: sibling config.py exists and exposes a *Config dataclass —
        # _load_config_from_module finds it and instantiates via direct kwargs.
        sibling = tmp_path / "config.py"
        sibling.write_text(
            "from dataclasses import dataclass\n"
            "@dataclass\n"
            "class StubConfig:\n"
            "    deployment_id: str = 'x'\n"
            "    chain: str = 'arbitrum'\n"
            "    wallet_address: str = '0x' + '1' * 40\n"
        )
        config = _resolve_trace_config(strat, {}, "arbitrum", wallet)
        assert config is not None
        assert config.chain == "arbitrum"


# ---------------------------------------------------------------------------
# print_inspection_result + helpers
# ---------------------------------------------------------------------------


class TestPrintInspectionResult:
    def test_happy_path_includes_metadata_intent_types_and_action_bundles(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        result = _make_inspection_result()
        print_inspection_result(result, verbose=False)
        out = capsys.readouterr().out
        assert "INTENT INSPECTION RESULT" in out
        assert "FakeStrategy" in out
        assert "METADATA" in out
        assert "Author: alice" in out
        assert "Tags: lp, demo" in out
        assert "Chains: arbitrum" in out
        assert "Protocols: uniswap_v3" in out
        assert "DETECTED INTENT TYPES" in out
        assert "LP_OPEN" in out
        assert "EXAMPLE ACTION BUNDLES" in out
        # Non-verbose stdout omits per-tx To/Data lines.
        assert "To: 0xabc" not in out

    def test_verbose_flag_and_error_section_branches(self, capsys: pytest.CaptureFixture[str]) -> None:
        # verbose=True → adds "To:" and "Data:" rows for each transaction.
        result = _make_inspection_result(errors=["unknown intent"])
        print_inspection_result(result, verbose=True)
        out = capsys.readouterr().out
        assert "To: 0xabc" in out
        assert "Data: 0xdead" in out
        # WARNINGS/ERRORS section IS printed when result.errors is non-empty.
        assert "WARNINGS/ERRORS" in out
        assert "unknown intent" in out

        # And conversely: empty errors list → section is skipped entirely.
        result_clean = _make_inspection_result(errors=[])
        print_inspection_result(result_clean, verbose=False)
        clean_out = capsys.readouterr().out
        assert "WARNINGS/ERRORS" not in clean_out


def test_print_metadata_section_skips_missing_optional_rows(capsys: pytest.CaptureFixture[str]) -> None:
    # When optional rows (author/tags/chains/protocols) are absent, only the
    # required Name/Version/Description rows are emitted.
    minimal = {"name": "x", "version": "0.1", "description": "d"}
    _print_metadata_section(minimal)
    out = capsys.readouterr().out
    assert "Name: x" in out
    assert "Author" not in out
    assert "Tags" not in out
    assert "Chains" not in out
    assert "Protocols" not in out


def test_print_action_bundle_failed_status(capsys: pytest.CaptureFixture[str]) -> None:
    # status != SUCCESS and no note → FAILED branch with error message.
    _print_action_bundle({"status": "ERROR", "error": "compilation blew up"}, verbose=False)
    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "compilation blew up" in out


# ---------------------------------------------------------------------------
# print_trace_result + helpers
# ---------------------------------------------------------------------------


class TestPrintTraceResult:
    def test_happy_path_and_failure_paths(self, capsys: pytest.CaptureFixture[str]) -> None:
        # Happy path: completes successfully, two steps rendered, final status green.
        print_trace_result(_make_trace_result(success=True), verbose=False)
        out = capsys.readouterr().out
        assert "INTENT TRACE RESULT" in out
        assert "FakeStrategy" in out
        assert "Execution Time: 12.34ms" in out
        assert "EXECUTION TRACE" in out
        assert "1. " in out and "2. " in out
        assert "Trace completed successfully." in out

        # Failure path: error message rendered + per-step error line.
        print_trace_result(_make_trace_result(success=False), verbose=False)
        fail_out = capsys.readouterr().out
        assert "Trace failed: boom" in fail_out
        assert "Error: boom" in fail_out

    def test_verbose_dumps_full_intent_json_and_handles_empty_trace(self, capsys: pytest.CaptureFixture[str]) -> None:
        # Verbose mode includes Intent ID + full JSON dump under FINAL RESULT.
        print_trace_result(_make_trace_result(success=True), verbose=True)
        out = capsys.readouterr().out
        assert "Intent ID: abc-123" in out
        assert "Full Intent:" in out

        # Edge case: empty trace (no steps, no final intent) renders cleanly.
        empty = _make_trace_result(steps=[], final_intent=None)
        print_trace_result(empty, verbose=False)
        empty_out = capsys.readouterr().out
        assert "EXECUTION TRACE" in empty_out
        assert "FINAL RESULT" in empty_out
