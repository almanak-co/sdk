"""Regression guards for VIB-3817 (QA-PostFixes April31 NEW-5).

Drift v2 returns Anchor error 101 (``InstructionFallbackNotFound``) when the
on-chain program receives an instruction whose 8-byte discriminator doesn't
match any registered handler — meaning the SDK's vendored discriminators have
drifted from the deployed IDL. This module pins three guards:

1. ``verify_drift_discriminators`` runs at SDK construction and proves that
   every constant in ``DRIFT_INSTRUCTION_NAMES`` equals
   ``sha256("global:<name>")[:8]`` (catches silent edits to constants.py).
2. The state-machine categorizer classifies both the typed
   :class:`DriftInstructionFallbackError` ERROR_PREFIX and the raw Solana
   ``InstructionFallbackNotFound`` literal as ``COMPILATION_PERMANENT`` so the
   runner stops the retry storm immediately when the program rejects an
   instruction at the discriminator layer.
3. ``DriftDiscriminatorMismatchError`` carries the offending instruction name
   and both byte strings for fast diff.
"""

from __future__ import annotations

import hashlib

import pytest

from almanak.framework.connectors.drift.constants import (
    DRIFT_INSTRUCTION_NAMES,
)
from almanak.framework.connectors.drift.exceptions import (
    DriftDiscriminatorMismatchError,
    DriftInstructionFallbackError,
)
from almanak.framework.connectors.drift.sdk import (
    anchor_discriminator,
    verify_drift_discriminators,
)
from almanak.framework.intents.state_machine import IntentStateMachine

WALLET = "BWv2BZTNAQjLkS5K17W3oVZqYxKLT7uNGoiEpxoBRvbm"


class TestAnchorDiscriminatorFormula:
    @pytest.mark.parametrize(
        "name,expected_hex",
        [
            ("place_perp_order", "45a15dca787e4cb9"),
            ("initialize_user", "6f11b9fa3c7a26fe"),
            ("initialize_user_stats", "fef34862fb82a8d5"),
            ("deposit", "f223c68952e1f2b6"),
            ("cancel_order", "5f81edf00831df84"),
        ],
    )
    def test_canonical_discriminators(self, name: str, expected_hex: str) -> None:
        assert anchor_discriminator(name).hex() == expected_hex

    def test_helper_matches_inline_formula(self) -> None:
        """Defence in depth: prove the helper is the formula."""
        for name in DRIFT_INSTRUCTION_NAMES:
            inline = hashlib.sha256(f"global:{name}".encode()).digest()[:8]
            assert anchor_discriminator(name) == inline


class TestVerifyDriftDiscriminators:
    def test_all_vendored_constants_match(self) -> None:
        """Assert on the explicit mismatch list (not just absence of raise) so
        that a future regression is reported with the precise instruction name
        + actual vs expected discriminator hex, not a bare exception trace.
        """
        mismatches: list[tuple[str, str, str]] = []
        for name, vendored in DRIFT_INSTRUCTION_NAMES.items():
            expected = anchor_discriminator(name)
            if vendored != expected:
                mismatches.append((name, expected.hex(), vendored.hex()))
        assert mismatches == [], (
            f"Vendored Drift discriminators have drifted from the canonical "
            f"sha256('global:<name>')[:8] formula. Each tuple is "
            f"(instruction_name, expected_hex, vendored_hex): {mismatches}"
        )
        # Sanity: also confirm the production self-check raises nothing.
        verify_drift_discriminators()

    def test_mismatched_constant_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Flip one constant to a wrong value and ensure the guard fires.
        from almanak.framework.connectors.drift import constants

        monkeypatch.setitem(
            constants.DRIFT_INSTRUCTION_NAMES,
            "place_perp_order",
            bytes.fromhex("0000000000000000"),
        )
        with pytest.raises(DriftDiscriminatorMismatchError) as exc_info:
            verify_drift_discriminators()
        assert exc_info.value.instruction_name == "place_perp_order"
        assert exc_info.value.expected.hex() == "45a15dca787e4cb9"
        assert exc_info.value.actual.hex() == "0000000000000000"


class TestDriftSdkConstructorRunsSelfCheck:
    def test_sdk_init_calls_verify(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.connectors.drift import constants
        from almanak.framework.connectors.drift.sdk import DriftSDK

        monkeypatch.setitem(
            constants.DRIFT_INSTRUCTION_NAMES,
            "deposit",
            bytes.fromhex("FFFFFFFFFFFFFFFF"),
        )
        with pytest.raises(DriftDiscriminatorMismatchError):
            DriftSDK(wallet_address=WALLET)


class TestStateMachineClassifiesAnchorFallbackAsPermanent:
    @pytest.fixture()
    def sm(self) -> IntentStateMachine:
        return IntentStateMachine.__new__(IntentStateMachine)  # method-only access

    def test_typed_drift_error_prefix_is_permanent(self, sm: IntentStateMachine) -> None:
        msg = (
            "Drift instruction not recognized by on-chain program: "
            "instruction='place_perp_order', program=dRiftyHA..., "
            "sdk_layout_version=2026-03-02. ..."
        )
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_raw_anchor_fallback_literal_is_permanent(self, sm: IntentStateMachine) -> None:
        # The Solana program log surfaces the bare Anchor enum name.
        msg = "Program log: AnchorError caused by account: user_pda. Custom: 101: InstructionFallbackNotFound"
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_unrelated_drift_error_is_not_permanent(
        self, sm: IntentStateMachine
    ) -> None:
        # Sanity: don't over-classify generic Drift errors.
        msg = "Drift API timed out after 30s"
        assert sm._categorize_error(msg) != "COMPILATION_PERMANENT"

    def test_discriminator_mismatch_is_permanent(self, sm: IntentStateMachine) -> None:
        """Defence-in-depth: `verify_drift_discriminators` raises at SDK boot
        which normally halts strategy startup before the state machine sees
        anything. But if the SDK is ever lazily constructed inside a compile
        path, the typed error message must short-circuit retries here too —
        otherwise a deterministic discriminator-mismatch failure (program
        upgraded, vendored constant stale) would retry-storm.
        """
        msg = (
            "Drift discriminator mismatch for 'place_perp_order': expected "
            "sha256('global:place_perp_order')[:8] = aabbccddeeff0011, got "
            "1122334455667788. Edit constants.py to match the canonical "
            "Anchor encoding."
        )
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"


class TestDriftInstructionFallbackErrorMessage:
    def test_carries_instruction_name_and_layout_version(self) -> None:
        err = DriftInstructionFallbackError(
            instruction_name="place_perp_order",
            program_id="dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
            layout_version="2026-03-02",
        )
        msg = str(err)
        assert "place_perp_order" in msg
        assert "2026-03-02" in msg
        assert "InstructionFallbackNotFound" in msg
        assert err.ERROR_PREFIX in msg
