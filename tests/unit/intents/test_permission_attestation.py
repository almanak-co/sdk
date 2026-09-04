"""Fail-closed negative controls for opt-in Safe permission evidence."""

from types import SimpleNamespace

import pytest

from almanak.framework.execution.signer.safe.constants import MULTISEND_SELECTOR
from qa_lab.permission_attestation import effective_calls
from tests.intents._permission_onchain_harness import ZodiacOrchestrator


def test_multisend_selector_on_noncanonical_target_refuses_attestation() -> None:
    transaction = {
        "to": "0x" + "11" * 20,
        "data": MULTISEND_SELECTOR + "00" * 32,
        "operation": 1,
        "value": 0,
    }

    with pytest.raises(ValueError, match="not the canonical MultiSend"):
        effective_calls([transaction], chain="arbitrum")


def test_bundle_without_manifest_inputs_clears_the_previous_attestation() -> None:
    orchestrator = SimpleNamespace(
        recorded_intents=None,
        _permission_attestation={"status": "PASS", "digest_sha256": "stale"},
    )

    ZodiacOrchestrator._apply_pending_manifest_targets(orchestrator)

    assert orchestrator._permission_attestation is None
