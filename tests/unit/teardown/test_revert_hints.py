"""Unit tests for the lending / Safe-Roles teardown revert decoder — VIB-5470,
extended by VIB-6111 (zero-LTV market).

Subsumes VIB-5152. Verifies that each operator-facing selector:

* ``0x6679996d`` — ``HealthFactorLowerThanLiquidationThreshold()`` (Aave, the
  dust-debt withdraw-all trap)
* ``0xd27b44a9`` — ``ModuleTransactionFailed()`` (Safe/Zodiac module wrapper)
* ``0xd0a9bf58`` — ``ConditionViolation(uint8,bytes32)`` (Zodiac Roles v2 denial)
* ``0x21e5c4ae`` — ``UserHasAssetWithZeroLtv()`` (Aave zero-LTV market)
* ``0x5b263df7`` — ``LtvValidationFailed()`` (Aave)
* ``0xd0739dae`` — ``UnderlyingCannotBeUsedAsCollateral()`` (Aave)
* ``0x911ceb81`` — ``CollateralCannotCoverNewBorrow()`` (Aave)

decodes to a clear operator message, that the embedded-selector scanner and the
idempotent annotation helper behave, and that the selectors are keccak-correct
and registered in the shared submitter registry.
"""

from __future__ import annotations

import re

import pytest
from eth_utils import keccak

from almanak.framework.execution.submitter.public import KNOWN_CUSTOM_ERRORS
from almanak.framework.teardown.revert_hints import (
    _HINT_MARKER,
    _OPERATOR_HINTS,
    annotate_teardown_error,
    find_revert_selector,
    operator_hint_for_selector,
)

# (selector, canonical signature, a distinctive phrase that MUST appear in the
# decoded operator message). The phrase pins the *meaning* assigned to each
# selector so a careless edit that swaps two explanations fails the test.
_SELECTOR_CASES = [
    (
        "0x6679996d",
        "HealthFactorLowerThanLiquidationThreshold()",
        "dust-debt trap",
    ),
    (
        "0xd27b44a9",
        "ModuleTransactionFailed()",
        "Safe/Zodiac module wrapper",
    ),
    (
        "0xd0a9bf58",
        "ConditionViolation(uint8,bytes32)",
        "Roles v2 permission denial",
    ),
    # VIB-6111 — Aave zero-LTV / collateral-eligibility family.
    (
        "0x21e5c4ae",
        "UserHasAssetWithZeroLtv()",
        "LTV is zero on this market",
    ),
    (
        "0x5b263df7",
        "LtvValidationFailed()",
        "aggregate LTV",
    ),
    (
        "0xd0739dae",
        "UnderlyingCannotBeUsedAsCollateral()",
        "usageAsCollateralEnabled = false",
    ),
    (
        "0x911ceb81",
        "CollateralCannotCoverNewBorrow()",
        "cannot support the requested borrow",
    ),
]


class TestSelectorsAreKeccakCorrect:
    """Each selector must be the keccak4 of its claimed signature (verify, don't invent)."""

    @pytest.mark.parametrize("selector, signature, _phrase", _SELECTOR_CASES)
    def test_selector_matches_signature_keccak(self, selector: str, signature: str, _phrase: str) -> None:
        computed = "0x" + keccak(text=signature)[:4].hex()
        assert computed == selector, f"{signature} hashes to {computed}, not {selector}"

    @pytest.mark.parametrize("selector, signature, _phrase", _SELECTOR_CASES)
    def test_signature_registered_in_shared_registry(self, selector: str, signature: str, _phrase: str) -> None:
        # The submitter / local-simulator decoders rely on this registry to name
        # the bare selector; revert_hints layers the explanation on top of it.
        assert KNOWN_CUSTOM_ERRORS.get(selector) == signature


class TestOperatorHintPerSelector:
    """Each selector decodes to a clear, signature-prefixed operator message."""

    @pytest.mark.parametrize("selector, signature, phrase", _SELECTOR_CASES)
    def test_decodes_to_clear_message(self, selector: str, signature: str, phrase: str) -> None:
        message = operator_hint_for_selector(selector)
        assert message is not None
        # Signature label first (from the shared registry), then the explanation.
        assert message.startswith(f"{signature} — ")
        assert phrase in message

    @pytest.mark.parametrize("selector, signature, _phrase", _SELECTOR_CASES)
    def test_case_and_prefix_insensitive(self, selector: str, signature: str, _phrase: str) -> None:
        upper = operator_hint_for_selector(selector.upper())
        no_prefix = operator_hint_for_selector(selector[2:])  # strip "0x"
        assert upper == operator_hint_for_selector(selector)
        assert no_prefix == operator_hint_for_selector(selector)

    def test_unknown_selector_returns_none(self) -> None:
        assert operator_hint_for_selector("0xdeadbeef") is None

    def test_empty_selector_returns_none(self) -> None:
        assert operator_hint_for_selector("") is None


class TestFindRevertSelector:
    """The scanner locates a hinted selector embedded in a free-form revert string."""

    @pytest.mark.parametrize("selector, _signature, _phrase", _SELECTOR_CASES)
    def test_finds_standalone_selector(self, selector: str, _signature: str, _phrase: str) -> None:
        assert find_revert_selector(f"execution reverted: {selector}") == selector

    def test_finds_selector_as_head_of_revert_payload(self) -> None:
        # ConditionViolation(uint8,bytes32) surfaces as selector + ABI args in one
        # contiguous hex blob; the selector is the head of a longer hex run.
        payload = "0xd0a9bf58" + "00" * 31 + "02" + "ab" * 32
        assert find_revert_selector(f"Reverted {payload}") == "0xd0a9bf58"

    def test_root_cause_wins_over_safe_wrapper(self) -> None:
        # If both the opaque wrapper and the actionable inner cause appear, the
        # actionable selector must win (detection-order contract).
        both = "ModuleTransactionFailed 0xd27b44a9 inner=0x6679996d"
        assert find_revert_selector(both) == "0x6679996d"

    def test_no_known_selector_returns_none(self) -> None:
        assert find_revert_selector("execution reverted: 0xdeadbeef") is None
        assert find_revert_selector("Fork Error: connection reset") is None

    def test_none_and_empty_return_none(self) -> None:
        assert find_revert_selector(None) is None
        assert find_revert_selector("") is None
        # Defensive: a non-string (e.g. a stray exception object) must degrade to
        # "no hint", never raise a TypeError inside the diagnostics path.
        assert find_revert_selector(ValueError("x")) is None  # type: ignore[arg-type]


class TestAnnotateTeardownError:
    """The wiring helper appends the hint losslessly and idempotently."""

    def test_appends_hint_preserving_original(self) -> None:
        raw = "Intent execution failed: reverted 0x6679996d"
        annotated = annotate_teardown_error(raw)
        assert annotated is not None
        assert annotated.startswith(raw)  # original preserved verbatim
        assert _HINT_MARKER in annotated
        assert "dust-debt trap" in annotated

    def test_is_idempotent(self) -> None:
        raw = "reverted 0xd0a9bf58"
        once = annotate_teardown_error(raw)
        twice = annotate_teardown_error(once)
        assert once == twice

    def test_passthrough_when_no_known_selector(self) -> None:
        raw = "Pre-flight balance check failed: Insufficient USDC: have 1, need 5"
        assert annotate_teardown_error(raw) == raw

    def test_passthrough_none_and_empty(self) -> None:
        assert annotate_teardown_error(None) is None
        assert annotate_teardown_error("") == ""
        # Defensive: a non-string is returned untouched (no TypeError) — the
        # teardown surface must never crash on an unexpected error payload.
        exc = ValueError("x")
        assert annotate_teardown_error(exc) is exc  # type: ignore[arg-type]


class TestHintTableIntegrity:
    """Guardrails so the data-driven table stays well-formed as it grows."""

    def test_all_keys_are_lowercase_4byte_selectors(self) -> None:
        for selector in _OPERATOR_HINTS:
            assert selector == selector.lower()
            assert selector.startswith("0x")
            assert len(selector) == 10
            int(selector[2:], 16)  # valid hex

    def test_every_hinted_selector_is_registered_and_keccak_decodable(self) -> None:
        # No hint may reference a selector the shared registry can't name — that
        # would surface a bare explanation with no signature label.
        for selector in _OPERATOR_HINTS:
            assert selector in KNOWN_CUSTOM_ERRORS
            signature = KNOWN_CUSTOM_ERRORS[selector]
            assert "0x" + keccak(text=signature)[:4].hex() == selector

    def test_safe_wrapper_is_last_for_detection_priority(self) -> None:
        # ModuleTransactionFailed is the opaque wrapper; it must be matched only
        # after every more-specific root-cause selector.
        assert list(_OPERATOR_HINTS)[-1] == "0xd27b44a9"

    def test_no_hint_text_embeds_a_hex_selector(self) -> None:
        # annotate_teardown_error appends the hint to the raw error string, so a
        # hex selector inside a hint would be re-detected by find_revert_selector
        # on the annotated string and mis-attribute the root cause.
        for selector, hint in _OPERATOR_HINTS.items():
            assert not re.search(r"0x[0-9a-fA-F]{8}", hint), f"hint for {selector} embeds a hex selector"


class TestZeroLtvOperatorHint:
    """VIB-6111: the zero-LTV hint must name the real cause and the workaround.

    On a zero-LTV Aave market the operator only ever sees Zodiac's outer
    ``ModuleTransactionFailed()``; the actionable content below is the entire
    point of the fix, so it is asserted phrase-by-phrase rather than by shape.
    """

    def test_names_zero_ltv_and_the_non_collateral_workaround(self) -> None:
        message = operator_hint_for_selector("0x21e5c4ae")
        assert message is not None
        assert message.startswith("UserHasAssetWithZeroLtv() — ")
        # (a) plainly states the reserve's LTV is zero / not accepted as collateral
        assert "LTV is zero on this market" in message
        assert "no longer accepts it as collateral" in message
        # (b) supplying WITHOUT use_as_collateral still works
        assert "WITHOUT use_as_collateral still works" in message
        # (c) governance risk-parameter change, not a strategy bug
        assert "governance risk-parameter change" in message
        assert "NOT a bug in the strategy" in message

    def test_root_cause_beats_the_zodiac_wrapper(self) -> None:
        # The production surface shows the wrapper; if the inner selector is ever
        # recovered alongside it, the inner cause must win.
        both = "Custom error: ModuleTransactionFailed() 0xd27b44a9 inner=0x21e5c4ae"
        assert find_revert_selector(both) == "0x21e5c4ae"


class TestZodiacWrapperMaskingMessage:
    """VIB-6111: the wrapper hint must say the inner error is absent AND how to get it."""

    def test_explains_masking_and_prescribes_eth_call_replay(self) -> None:
        message = operator_hint_for_selector("0xd27b44a9")
        assert message is not None
        assert "OUTER wrapper" in message
        assert "NOT present anywhere in this receipt" in message
        assert "eth_call" in message
        assert "PARENT block" in message
        # MultiSend atomicity: replay the leg, not just the outer batch.
        assert "MultiSend" in message


class TestUnverifiedSelectorIsNotCatalogued:
    """``0x0cafc072`` was a stale, keccak-WRONG label for
    ``UnderlyingCannotBeUsedAsCollateral()`` (the correct selector is
    ``0xd0739dae``). It must not be resurrected into any catalogue.
    """

    def test_stale_selector_absent_everywhere(self) -> None:
        assert "0x0cafc072" not in _OPERATOR_HINTS
        assert "0x0cafc072" not in KNOWN_CUSTOM_ERRORS

    def test_correct_selector_is_keccak_of_the_signature(self) -> None:
        signature = "UnderlyingCannotBeUsedAsCollateral()"
        assert "0x" + keccak(text=signature)[:4].hex() == "0xd0739dae"
