"""Regression guards for VIB-3828 (QA-PostFixes April31 BUG-43).

The Enso router reverts on Base for ``leverage_loop_cross_chain`` with the
custom-error selector ``0xef3dcb2f``. Same selector previously seen in BUG-55
was a logging artifact (closed by VIB-3747); the on-chain revert here is real.

Without the upstream router ABI to recover the signature, the four-byte
selector is the only stable handle — so this guard:

1. Pins the typed :class:`EnsoRouterRevertError` exception with the
   stable ``ERROR_PREFIX`` class attribute. The prefix wording
   intentionally avoids the substring ``"revert"`` — the state-machine
   ``REVERT`` pre-classification consumes any error containing
   ``"revert"`` before the ``COMPILATION_PERMANENT`` keyword block runs.
   Pinned by ``test_error_prefix_avoids_revert_substring`` below.
2. Pins the ``KNOWN_REVERT_SELECTORS`` table that documents the diagnosis
   hint for ``0xef3dcb2f``.
3. Pins the state-machine COMPILATION_PERMANENT classification so retries
   on the same route never enter a retry-storm.
"""

from __future__ import annotations

import pytest

from almanak.framework.connectors.enso.exceptions import EnsoRouterRevertError
from almanak.framework.intents.state_machine import IntentStateMachine


class TestEnsoRouterRevertErrorMessage:
    def test_includes_selector_chain_route(self) -> None:
        err = EnsoRouterRevertError(
            selector="0xef3dcb2f",
            chain="base",
            route_summary="USDC -> WETH (Aerodrome)",
        )
        msg = str(err)
        assert err.ERROR_PREFIX in msg
        assert "0xef3dcb2f" in msg
        assert "base" in msg
        assert "USDC -> WETH (Aerodrome)" in msg
        assert "VIB-3828" in msg or "router-side route-validation" in msg

    def test_unknown_selector_no_diagnosis_hint(self) -> None:
        err = EnsoRouterRevertError(
            selector="0xdeadbeef",
            chain="base",
        )
        assert err.diagnosis_hint is None
        msg = str(err)
        assert "0xdeadbeef" in msg
        assert "Diagnosis hint" not in msg

    def test_explicit_diagnosis_hint_overrides_table(self) -> None:
        err = EnsoRouterRevertError(
            selector="0xef3dcb2f",
            chain="base",
            diagnosis_hint="Custom override for this test",
        )
        assert err.diagnosis_hint == "Custom override for this test"

    def test_selector_lowercased(self) -> None:
        err = EnsoRouterRevertError(selector="0xEF3DCB2F", chain="base")
        assert err.selector == "0xef3dcb2f"
        assert err.diagnosis_hint is not None

    @pytest.mark.parametrize(
        "raw_input",
        [
            "0xef3dcb2f",
            "0xEF3DCB2F",
            "ef3dcb2f",
            "EF3DCB2F",
            "0xef3dcb2f00000000000000000000000000000000000000000000000000000000",
            "ef3dcb2f00000000000000000000000000000000000000000000000000000000",
        ],
        ids=["canonical", "uppercase", "no-prefix", "no-prefix-upper", "raw-revert-data", "raw-no-prefix"],
    )
    def test_selector_canonicalized_to_known_form(self, raw_input: str) -> None:
        """All shapes a caller might pass — bare 4-byte, prefixed, uppercase,
        or raw revert data with selector + ABI-encoded args — must collapse to
        ``0x`` + 8 lowercase hex chars so the ``KNOWN_REVERT_SELECTORS`` table
        lookup never silently misses the diagnosis hint."""
        err = EnsoRouterRevertError(selector=raw_input, chain="base")
        assert err.selector == "0xef3dcb2f"
        assert err.diagnosis_hint is not None, (
            f"Canonicalization regressed for input {raw_input!r}: "
            f"got selector={err.selector!r}, diagnosis_hint=None"
        )


class TestKnownRevertSelectorsTable:
    def test_ef3dcb2f_documented(self) -> None:
        hint = EnsoRouterRevertError.KNOWN_REVERT_SELECTORS.get("0xef3dcb2f")
        assert hint is not None
        assert "VIB-3828" in hint or "router-side" in hint


class TestStateMachineClassifiesEnsoRouterRevertAsPermanent:
    @pytest.fixture()
    def sm(self) -> IntentStateMachine:
        return IntentStateMachine.__new__(IntentStateMachine)

    def test_typed_error_prefix_is_permanent(self, sm: IntentStateMachine) -> None:
        msg = (
            "Enso router rejected route with selector 0xef3dcb2f on base "
            "(route: USDC -> WETH). Diagnosis hint: ..."
        )
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_error_prefix_avoids_revert_substring(self) -> None:
        """Guard the well-known regex hazard: any 'revert' in the message is
        captured by the generic REVERT class before COMPILATION_PERMANENT
        keyword matching runs. Pinning the absence here protects against
        accidental wording edits.
        """
        assert "revert" not in EnsoRouterRevertError.ERROR_PREFIX.lower()

    def test_random_enso_error_is_not_permanent(self, sm: IntentStateMachine) -> None:
        msg = "Enso API request timed out"
        assert sm._categorize_error(msg) != "COMPILATION_PERMANENT"
