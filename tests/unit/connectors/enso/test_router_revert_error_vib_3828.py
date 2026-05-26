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

from almanak.connectors.enso.exceptions import (
    EnsoRouterRevertError,
    check_known_router_revert,
)
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

    @pytest.mark.parametrize(
        "selector",
        sorted(EnsoRouterRevertError.KNOWN_REVERT_SELECTORS),
    )
    @pytest.mark.parametrize(
        "route_summary",
        ["", "USDC -> WETH"],
        ids=["no-summary", "with-summary"],
    )
    def test_every_known_selector_classifies_as_permanent(
        self, selector: str, route_summary: str
    ) -> None:
        """Structural regression guard: every entry in
        ``KNOWN_REVERT_SELECTORS`` must produce a final exception message
        that classifies as ``COMPILATION_PERMANENT`` — across BOTH the
        with-summary and no-summary code paths.

        ``IntentStateMachine._categorize_error`` matches a hard-coded
        keyword set (``insufficient``/``revert``/``slippage``/``timeout``/
        ``nonce``/``gas``/``rate limit``/``connection``/``network``)
        BEFORE the ``COMPILATION_PERMANENT`` keyword block. A diagnosis
        hint containing any of those words short-circuits to a transient
        class, defeating the typed exception's purpose. PR #2013 shipped
        with this exact bug — the ``0xef3dcb2f`` hint contained
        ``"slippage"`` and was classified as ``SLIPPAGE`` (retryable).

        The no-summary path is the one that surfaced the second related
        bug: an earlier draft of the helper smuggled the raw upstream
        error string (typically containing ``"reverted"``) into
        ``route_summary`` when the caller passed none. That re-introduced
        the ``REVERT`` pre-classification. Both paths are now guarded.

        This test prevents future regressions of the same class for any
        new selector entry.
        """
        err = EnsoRouterRevertError(
            selector=selector,
            chain="base",
            route_summary=route_summary,
        )
        sm = IntentStateMachine.__new__(IntentStateMachine)
        actual = sm._categorize_error(str(err))
        assert actual == "COMPILATION_PERMANENT", (
            f"Selector {selector} (route_summary={route_summary!r}) "
            f"produced an exception message that classifies as {actual!r} "
            f"instead of COMPILATION_PERMANENT. The diagnosis hint likely "
            f"contains a state-machine pre-classification keyword "
            f"(insufficient/revert/slippage/timeout/nonce/gas/rate limit/"
            f"connection/network). Rephrase the hint to avoid these words. "
            f"Full message:\n  {err}"
        )

    def test_known_revert_selectors_is_immutable(self) -> None:
        """Pin that ``KNOWN_REVERT_SELECTORS`` is wrapped in a
        ``MappingProxyType`` so accidental writes from downstream callers
        cannot mutate the global classification table at runtime.
        """
        with pytest.raises(TypeError):
            EnsoRouterRevertError.KNOWN_REVERT_SELECTORS["0xfeedface"] = "rogue"  # type: ignore[index]


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


class TestCheckKnownRouterRevertHelper:
    """Pin the VIB-3828 wiring helper that bridges raw Enso error strings
    (as returned by the gateway or direct API call) to the typed
    ``EnsoRouterRevertError`` exception.

    Conservative classifier: only known selectors raise. Unknown selectors
    and selector-free strings return None so the original error path is
    preserved.
    """

    def test_known_selector_in_gateway_error_string_raises(self) -> None:
        # Realistic gateway error string format produced by
        # ``almanak/gateway/services/enso_service.py:_request`` when the
        # upstream Enso API returns a 400 with the revert selector in the
        # response body.
        gateway_error = (
            "HTTP 400: {\"error\":\"execution reverted: 0xef3dcb2f\","
            "\"path\":\"/api/v1/shortcuts/route\"}"
        )
        with pytest.raises(EnsoRouterRevertError) as excinfo:
            check_known_router_revert(
                gateway_error,
                chain="base",
                route_summary="USDC -> WETH",
            )
        err = excinfo.value
        assert err.selector == "0xef3dcb2f"
        assert err.chain == "base"
        # The typed exception's message preserves the route_summary the caller
        # passed (i.e. token symbols), giving the strategy author the leg that
        # failed without having to re-parse the gateway error string.
        assert "USDC -> WETH" in str(err)

    def test_known_selector_with_uppercase_matches(self) -> None:
        with pytest.raises(EnsoRouterRevertError):
            check_known_router_revert(
                "execution reverted: 0xEF3DCB2F",
                chain="base",
            )

    def test_unknown_selector_falls_through(self) -> None:
        # Unknown selectors must NOT raise so callers can preserve the
        # existing error path (operator visibility for novel reverts).
        # Asserting `is None` (not just "does not raise") pins the
        # conservative-classifier contract — a future regression that
        # returns a sentinel value or raises a different exception type
        # would be caught.
        result = check_known_router_revert(
            "execution reverted: 0xdeadbeef",
            chain="base",
        )
        assert result is None

    def test_no_selector_falls_through(self) -> None:
        result = check_known_router_revert(
            "Gateway Enso GetRoute failed: HTTP 503: upstream timeout",
            chain="base",
        )
        assert result is None

    def test_empty_error_string_falls_through(self) -> None:
        result = check_known_router_revert("", chain="base")
        assert result is None

    def test_abi_encoded_revert_data_with_args_matches(self) -> None:
        """Solidity custom errors with arguments surface as
        ``0x<4-byte selector><32-byte ABI-encoded args...>`` — the leading
        4 bytes MUST be matched. This is the canonical custom-error
        format; missing it (which an earlier draft of the regex did)
        means the typed exception never fires for parameterized router
        reverts.
        """
        abi_encoded = (
            "execution reverted: 0xef3dcb2f"
            "0000000000000000000000000000000000000000000000000000000000000001"
        )
        with pytest.raises(EnsoRouterRevertError) as excinfo:
            check_known_router_revert(abi_encoded, chain="base")
        assert excinfo.value.selector == "0xef3dcb2f"

    def test_selector_inside_unanchored_hex_blob_does_not_match(self) -> None:
        """Word-boundary anchoring on the LEADING ``0x`` prevents matches
        inside arbitrary hex substrings that lack a fresh ``0x`` prefix
        (e.g. mid-tx-hash text). Only emissions explicitly prefixed with
        ``0x`` at a word boundary count.
        """
        # ``ef3dcb2f`` appears mid-string with no ``0x`` prefix at a word
        # boundary — must not raise.
        embedded = "txhash 1234ef3dcb2f5678 reverted"
        result = check_known_router_revert(embedded, chain="base")
        assert result is None

    def test_no_route_summary_emits_clean_message_classifies_permanent(self) -> None:
        """Regression guard: when the caller omits ``route_summary``, the
        helper must NOT smuggle the raw upstream error (which typically
        contains the literal ``"reverted"``) into ``str(err)``. Doing so
        would re-introduce the state-machine ``REVERT`` pre-classification
        and defeat the typed exception — exactly the bug Codex P2 #2
        and pr-auditor Important #1 flagged.

        The raw error must instead be available via ``original_error``
        and via the helper's WARNING log line (operator visibility
        preserved).
        """
        gateway_error = "HTTP 400: execution reverted: 0xef3dcb2f"
        with pytest.raises(EnsoRouterRevertError) as excinfo:
            check_known_router_revert(gateway_error, chain="base")
        err = excinfo.value
        # Original error preserved on the attribute
        assert err.original_error == gateway_error
        # ... but explicitly NOT in the rendered exception message
        assert "reverted" not in str(err).lower()
        assert gateway_error not in str(err)
        # End-to-end: state machine classifies as PERMANENT
        sm = IntentStateMachine.__new__(IntentStateMachine)
        assert sm._categorize_error(str(err)) == "COMPILATION_PERMANENT"

    def test_caller_provided_route_summary_appears_in_message(self) -> None:
        """When the caller passes a clean (keyword-free) route_summary,
        it appears in the rendered message for debuggability."""
        with pytest.raises(EnsoRouterRevertError) as excinfo:
            check_known_router_revert(
                "execution reverted: 0xef3dcb2f",
                chain="base",
                route_summary="USDC -> WETH",
            )
        assert "USDC -> WETH" in str(excinfo.value)

    def test_message_emitted_by_helper_state_machine_classifies_permanent(
        self,
    ) -> None:
        """End-to-end pin: error string from the gateway -> helper raises
        EnsoRouterRevertError -> str(err) feeds the state machine ->
        classified as COMPILATION_PERMANENT.

        This is the property the original VIB-3828 PR claimed but did not
        prove — the test that should have caught the missing wiring.
        """
        gateway_error = "HTTP 400: execution reverted: 0xef3dcb2f"
        with pytest.raises(EnsoRouterRevertError) as excinfo:
            check_known_router_revert(
                gateway_error,
                chain="base",
                route_summary="USDC -> WETH",
            )
        sm = IntentStateMachine.__new__(IntentStateMachine)
        assert sm._categorize_error(str(excinfo.value)) == "COMPILATION_PERMANENT"

    def test_helper_logs_raw_error_at_warning(self, caplog) -> None:
        """Operator visibility: the raw upstream error must be logged at
        WARNING when a known selector fires, so an oncall reviewing logs
        sees the verbatim gateway message even though it is excluded
        from ``str(err)``.
        """
        import logging

        gateway_error = "HTTP 400: execution reverted: 0xef3dcb2f"
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.enso.exceptions"):
            with pytest.raises(EnsoRouterRevertError):
                check_known_router_revert(gateway_error, chain="base")
        assert any(gateway_error in rec.getMessage() for rec in caplog.records)
