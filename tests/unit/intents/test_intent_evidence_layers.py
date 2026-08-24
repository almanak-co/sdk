"""The receipt-evidence recorder must derive its layer verdicts, not declare them.

``IntentEvidenceRecorder`` writes an artifact whose ``layers`` block is the
headline claim a sealer later trusts. It used to hardcode ``compile: PASS`` and
``execute: PASS`` for every captured receipt, so the claim described the fact
that the recorder had been called rather than anything that happened on chain:
a test that bound a reverted receipt, or that ran with no compiler observation
wired at all, still published two passing layers.

These are the negative controls for that fix. Each one corrupts exactly one
observation and asserts the artifact stops claiming the layer it can no longer
support. ``Empty != Zero`` governs the third state: an execution the recorder
cannot describe is ``UNMEASURED``, never ``PASS``.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.intents.compiler_models import CompilationStatus
from tests.intents.conftest import _CompileObservations, _finalize_intent_evidence, _record_compilation
from tests.intents.intent_evidence import IntentEvidenceRecorder

SUCCESSFUL_RECEIPT = {
    "transactionHash": "0x" + "ab" * 32,
    "blockNumber": 21_000_000,
    "status": 1,
    "from": "0x" + "11" * 20,
    "to": "0x" + "22" * 20,
    "gasUsed": 120_000,
    "logs": [],
}


def _intent() -> SimpleNamespace:
    return SimpleNamespace(
        protocol="uniswap_v3",
        chain="arbitrum",
        intent_type=SimpleNamespace(value="SWAP"),
        amount="1000",
        from_token="USDC",
        to_token="WETH",
    )


def _recorder(tmp_path: Path, *, observed: list[Any] | None) -> IntentEvidenceRecorder:
    return IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_x.py::TestX::test_y",
        network="anvil",
        exec_path="eoa",
        observed_intents=observed,
    )


def _capture(recorder: IntentEvidenceRecorder, intent: Any, transaction_result: Any) -> None:
    recorder.capture_parse(
        intent=intent,
        transaction_result=transaction_result,
        parser=lambda receipt: SimpleNamespace(success=True, amount=1),
    )


def _layers(tmp_path: Path) -> dict[str, str]:
    artifacts = sorted((tmp_path / "receipts").rglob("*.json"))
    assert len(artifacts) == 1, f"expected exactly one receipt artifact, found {artifacts}"
    return json.loads(artifacts[0].read_text(encoding="utf-8"))["layers"]


def test_a_successful_execution_reports_a_passing_execute_layer(tmp_path: Path) -> None:
    """Positive control: the honest derivation must still be able to say PASS.

    The observation set is built through the real producer rather than handed
    in directly, so this cannot pass on an intent that never compiled.
    """
    intent = _intent()
    observations = _CompileObservations()
    _record_compilation(observations, intent, SimpleNamespace(status=CompilationStatus.SUCCESS))

    recorder = _recorder(tmp_path, observed=observations.succeeded)
    _capture(recorder, intent, SimpleNamespace(receipt=dict(SUCCESSFUL_RECEIPT), success=True))
    recorder.finalize(outcome="PASS", duration_seconds=1.0)

    layers = _layers(tmp_path)
    assert layers["execute"] == "PASS"
    assert layers["compile"] == "PASS"


@pytest.mark.parametrize(
    "success,status",
    [
        (False, 0),  # both signals agree the transaction reverted
        (False, 1),  # the executor reported failure even though the receipt did not
        (True, 0),  # a reverted receipt bound to an optimistic execution result
    ],
)
def test_a_failed_execution_can_never_report_a_passing_execute_layer(
    tmp_path: Path, success: bool, status: int
) -> None:
    intent = _intent()
    recorder = _recorder(tmp_path, observed=[intent])
    receipt = dict(SUCCESSFUL_RECEIPT) | {"status": status}
    _capture(recorder, intent, SimpleNamespace(receipt=receipt, success=success))
    recorder.finalize(outcome="PASS", duration_seconds=1.0)

    assert _layers(tmp_path)["execute"] == "FAIL"


def test_an_unobservable_execution_is_unmeasured_rather_than_passing(tmp_path: Path) -> None:
    """No ``success`` attribute and no receipt status is silence, not success."""
    intent = _intent()
    recorder = _recorder(tmp_path, observed=[intent])
    receipt = {key: value for key, value in SUCCESSFUL_RECEIPT.items() if key != "status"}
    _capture(recorder, intent, SimpleNamespace(receipt=receipt))
    recorder.finalize(outcome="PASS", duration_seconds=1.0)

    assert _layers(tmp_path)["execute"] == "UNMEASURED"


def test_compile_is_unmeasured_when_no_compiler_observation_was_wired(tmp_path: Path) -> None:
    """Without the observer there is nothing that saw the intent compile."""
    intent = _intent()
    recorder = _recorder(tmp_path, observed=None)
    _capture(recorder, intent, SimpleNamespace(receipt=dict(SUCCESSFUL_RECEIPT), success=True))
    recorder.finalize(outcome="PASS", duration_seconds=1.0)

    assert _layers(tmp_path)["compile"] == "UNMEASURED"


def test_finalize_refuses_to_publish_an_intent_that_never_compiled(tmp_path: Path) -> None:
    """This gate is what earns ``compile: PASS``; pin it so the layer stays honest."""
    intent = _intent()
    recorder = _recorder(tmp_path, observed=[])
    _capture(recorder, intent, SimpleNamespace(receipt=dict(SUCCESSFUL_RECEIPT), success=True))

    with pytest.raises(ValueError, match="did not compile"):
        recorder.finalize(outcome="PASS", duration_seconds=1.0)


# --- the producer side: what actually lands in ``observed_intents`` -----------
#
# The recorder can only be as honest as the set it is handed. compile() reports
# failure by RETURNING CompilationResult(status=FAILED) far more often than by
# raising, so a producer that records every call it survived would hand the
# recorder a set that means "compile did not raise" and mint a passing Layer 1
# for a compilation that failed.


@pytest.mark.parametrize(
    "status,expected_success",
    [
        (CompilationStatus.SUCCESS, True),
        (CompilationStatus.FAILED, False),
        # PARTIAL is "some transactions built, some failed" -- not a passing
        # compile layer. SUCCESS is allowlisted rather than FAILED denylisted
        # so a new status defaults to refusing, not to passing.
        (CompilationStatus.PARTIAL, False),
    ],
    ids=lambda value: value.value if isinstance(value, CompilationStatus) else str(value),
)
def test_only_a_successful_compile_reaches_the_evidence_observation_set(
    status: CompilationStatus, expected_success: bool
) -> None:
    observations = _CompileObservations()
    intent = _intent()

    _record_compilation(observations, intent, SimpleNamespace(status=status))

    assert list(observations) == [intent], "Zodiac must still authorize every attempted intent"
    assert (intent in observations.succeeded) is expected_success


def test_an_unreadable_compilation_status_does_not_earn_layer_one() -> None:
    """A result the discriminator cannot read is not a successful compile."""
    observations = _CompileObservations()
    intent = _intent()

    _record_compilation(observations, intent, SimpleNamespace())

    assert observations.succeeded == []


def test_a_failed_compilation_makes_the_whole_node_refuse(tmp_path: Path) -> None:
    """End to end: FAILED status in, refusal out -- never a published PASS."""
    intent = _intent()
    observations = _CompileObservations()
    _record_compilation(observations, intent, SimpleNamespace(status=CompilationStatus.FAILED))

    recorder = _recorder(tmp_path, observed=observations.succeeded)
    _capture(recorder, intent, SimpleNamespace(receipt=dict(SUCCESSFUL_RECEIPT), success=True))

    with pytest.raises(ValueError, match="did not compile"):
        recorder.finalize(outcome="PASS", duration_seconds=1.0)


class _RefusingRecorder:
    """Stands in for a recorder whose finalize() refuses to publish."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def finalize(self, *, outcome: str, duration_seconds: float) -> None:
        del duration_seconds
        self.calls.append(outcome)
        raise ValueError("Intent evidence bound source requests IntentCompiler.compile did not compile")


def _report(*, passed: bool) -> SimpleNamespace:
    return SimpleNamespace(passed=passed, skipped=False, duration=1.0, sections=[])


def test_a_passing_node_that_cannot_publish_its_evidence_fails(tmp_path: Path) -> None:
    """The refusal is the only signal here, so it must reach the test result."""
    del tmp_path
    report = _report(passed=True)

    with pytest.raises(ValueError, match="did not compile"):
        _finalize_intent_evidence(_RefusingRecorder(), report)


def test_a_failing_node_keeps_its_own_failure_when_evidence_cannot_publish() -> None:
    """Raising here would mask the real failure and become a pytest hook ERROR."""
    report = _report(passed=False)

    _finalize_intent_evidence(_RefusingRecorder(), report)

    assert [title for title, _ in report.sections] == ["Intent evidence"]
    assert "did not compile" in report.sections[0][1]


def test_a_healthy_node_finalizes_with_the_pytest_outcome() -> None:
    """Positive control: the guard must not swallow the ordinary path."""
    recorder = _RefusingRecorder()
    recorder.finalize = lambda **kwargs: recorder.calls.append(kwargs["outcome"])  # type: ignore[method-assign]

    _finalize_intent_evidence(recorder, _report(passed=True))

    assert recorder.calls == ["PASS"]


def test_permission_refusal_is_persisted_before_capture_parse_raises(tmp_path: Path) -> None:
    intent = _intent()
    recorder = IntentEvidenceRecorder(
        output_dir=tmp_path,
        nodeid="tests/intents/arbitrum/test_x.py::TestX::test_y",
        network="anvil",
        exec_path="safe",
        observed_intents=[intent],
    )
    transaction = SimpleNamespace(
        receipt=dict(SUCCESSFUL_RECEIPT),
        success=True,
        qa_permission_attestation={"artifact_kind": "almanak.permission_closure_attestation"},
    )

    with pytest.raises(ValueError, match="effective_calls"):
        _capture(recorder, intent, transaction)

    _, payload = recorder._artifact_payloads[-1]
    assert payload["layers"]["permissions"] == "FAIL"
    assert payload["permission_validation_error"]["type"] == "builtins.ValueError"
