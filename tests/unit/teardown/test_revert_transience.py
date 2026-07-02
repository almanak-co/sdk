"""Unit tests for the teardown transient-vs-permanent revert classifier — VIB-5573.

The classifier decides whether a teardown revert is expected to clear within
blocks (TRANSIENT → time-retry) or not (UNKNOWN → default handling). The seeded
rule covers the MetaMorpho withdraw-queue transient arithmetic ``Panic(0x11)``.

The Codex safety constraint is the load-bearing property under test: a bare
``Panic(17)`` is NOT enough — the classifier only returns TRANSIENT when the
(intent_type, protocol, signature) tuple matches the vetted allowlist. The
"over-broad" tests below (wrong protocol, wrong intent, wrong panic code) pin
that constraint so a careless widening of a rule fails the suite.
"""

from __future__ import annotations

import pytest

from almanak.framework.teardown.revert_transience import (
    Transience,
    classify_revert_transience,
)

# Raw arithmetic Panic(0x11) payload: selector + 32-byte word ending in 0x11.
_RAW_ARITHMETIC_PANIC = "0x4e487b71" + "00" * 31 + "11"
# Raw div-by-zero Panic(0x12) — the classic near-miss we must NOT treat as transient.
_RAW_DIV_BY_ZERO_PANIC = "0x4e487b71" + "00" * 31 + "12"
# Raw assert Panic(0x01).
_RAW_ASSERT_PANIC = "0x4e487b71" + "00" * 31 + "01"


class TestSeededMetaMorphoRule:
    """VAULT_REDEEM + metamorpho + arithmetic panic → TRANSIENT."""

    def test_decoded_panic_text_metamorpho_is_transient(self) -> None:
        result = classify_revert_transience(
            "execution reverted: Panic(17): Arithmetic overflow/underflow",
            intent_type="VAULT_REDEEM",
            protocol="metamorpho",
        )
        assert result is Transience.TRANSIENT

    def test_raw_panic_hex_metamorpho_is_transient(self) -> None:
        result = classify_revert_transience(
            f"Reverted {_RAW_ARITHMETIC_PANIC}",
            intent_type="VAULT_REDEEM",
            protocol="metamorpho",
        )
        assert result is Transience.TRANSIENT

    def test_case_insensitive_intent_and_protocol(self) -> None:
        # lower-case intent, mixed-case protocol must still match the allowlist.
        result = classify_revert_transience(
            "Panic(17)",
            intent_type="vault_redeem",
            protocol="MetaMorpho",
        )
        assert result is Transience.TRANSIENT


class TestContextScopingSafetyConstraint:
    """The Codex over-broad-retry guard: right panic, WRONG context → UNKNOWN."""

    def test_arithmetic_panic_on_non_vault_protocol_is_unknown(self) -> None:
        # Panic(17) but protocol=uniswap_v3 — the flagged over-broad case.
        result = classify_revert_transience(
            "Panic(17)",
            intent_type="VAULT_REDEEM",
            protocol="uniswap_v3",
        )
        assert result is Transience.UNKNOWN

    def test_arithmetic_panic_on_wrong_intent_is_unknown(self) -> None:
        # Panic(17) on metamorpho but the intent is LP_CLOSE, not a redeem.
        result = classify_revert_transience(
            "Panic(17)",
            intent_type="LP_CLOSE",
            protocol="metamorpho",
        )
        assert result is Transience.UNKNOWN

    def test_div_by_zero_panic_is_not_transient(self) -> None:
        # Panic(18) / 0x12 (div-by-zero) is a DIFFERENT panic code — not the
        # arithmetic-underflow withdraw-queue transient. Both forms must miss.
        assert (
            classify_revert_transience(
                "Panic(18): Division or modulo by zero",
                intent_type="VAULT_REDEEM",
                protocol="metamorpho",
            )
            is Transience.UNKNOWN
        )
        assert (
            classify_revert_transience(
                f"Reverted {_RAW_DIV_BY_ZERO_PANIC}",
                intent_type="VAULT_REDEEM",
                protocol="metamorpho",
            )
            is Transience.UNKNOWN
        )

    def test_assert_panic_is_not_transient(self) -> None:
        # Panic(1) / 0x01 assert must not match the arithmetic signature.
        assert (
            classify_revert_transience(
                f"Reverted {_RAW_ASSERT_PANIC}",
                intent_type="VAULT_REDEEM",
                protocol="metamorpho",
            )
            is Transience.UNKNOWN
        )
        assert (
            classify_revert_transience(
                "Panic(1): Assert failed",
                intent_type="VAULT_REDEEM",
                protocol="metamorpho",
            )
            is Transience.UNKNOWN
        )


class TestDefensiveInputs:
    """The classifier sits on the failure surface: never raise, degrade to UNKNOWN."""

    def test_none_error_text_is_unknown(self) -> None:
        assert (
            classify_revert_transience(
                None, intent_type="VAULT_REDEEM", protocol="metamorpho"
            )
            is Transience.UNKNOWN
        )

    def test_empty_error_text_is_unknown(self) -> None:
        assert (
            classify_revert_transience(
                "", intent_type="VAULT_REDEEM", protocol="metamorpho"
            )
            is Transience.UNKNOWN
        )

    def test_non_string_error_text_does_not_raise(self) -> None:
        # A stray int or exception object must degrade, never TypeError.
        assert (
            classify_revert_transience(
                17, intent_type="VAULT_REDEEM", protocol="metamorpho"  # type: ignore[arg-type]
            )
            is Transience.UNKNOWN
        )
        assert (
            classify_revert_transience(
                ValueError("Panic(17)"),  # type: ignore[arg-type]
                intent_type="VAULT_REDEEM",
                protocol="metamorpho",
            )
            is Transience.UNKNOWN
        )

    def test_missing_intent_or_protocol_is_unknown(self) -> None:
        assert (
            classify_revert_transience("Panic(17)", protocol="metamorpho")
            is Transience.UNKNOWN
        )
        assert (
            classify_revert_transience("Panic(17)", intent_type="VAULT_REDEEM")
            is Transience.UNKNOWN
        )
        assert classify_revert_transience("Panic(17)") is Transience.UNKNOWN

    def test_unrelated_revert_is_unknown(self) -> None:
        assert (
            classify_revert_transience(
                "execution reverted: insufficient balance",
                intent_type="VAULT_REDEEM",
                protocol="metamorpho",
            )
            is Transience.UNKNOWN
        )


@pytest.mark.parametrize(
    "text",
    [
        "Panic(170)",  # decimal 170, not 17 — must not match the whole-token regex
        "Panic(171): something",
    ],
)
def test_panic_code_token_is_not_prefix_matched(text: str) -> None:
    # Guard the \bPanic(17) token boundary: Panic(170)/Panic(171) are NOT the
    # arithmetic-underflow code and must stay UNKNOWN.
    assert (
        classify_revert_transience(
            text, intent_type="VAULT_REDEEM", protocol="metamorpho"
        )
        is Transience.UNKNOWN
    )
