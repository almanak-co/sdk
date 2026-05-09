"""Diagnostic log on materializer_primitive_for(unknown).

VIB-4190 / T05: primitives T2 (VIB-4162) deferred this WARN log to the
position-registry epic. Unknown position-type strings are silently coerced
to None today, then the caller in accounting.position_state._classify_position
treats them as "skip". The WARN gives an operator the unrecognized string
instead of the silent skip.

Tests assert:
- WARN log fires on an unknown string AND includes the input value.
- WARN log does NOT fire for known strings.
- The log level is WARNING (not DEBUG, not INFO) — DEBUG would be swallowed
  by default ops log levels.
"""

from __future__ import annotations

import logging

import pytest

from almanak.framework.primitives.taxonomy import materializer_primitive_for
from almanak.framework.primitives.types import Primitive


_TAXONOMY_LOGGER = "almanak.framework.primitives.taxonomy"


def test_unknown_string_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger=_TAXONOMY_LOGGER)
    result = materializer_primitive_for("UNKNOWN_PROTOCOL_XYZ")
    assert result is None, "unknown strings must still return None for caller compat"

    matching = [
        rec
        for rec in caplog.records
        if rec.name == _TAXONOMY_LOGGER and rec.levelno == logging.WARNING
    ]
    assert matching, "expected a WARNING-level record from taxonomy.py"
    msg = matching[-1].getMessage()
    assert "UNKNOWN_PROTOCOL_XYZ" in msg, (
        f"WARN message must include the input string: {msg!r}"
    )
    assert "materializer_primitive_for" in msg, (
        f"WARN message should name the function for grep-ability: {msg!r}"
    )


def test_unknown_string_logs_at_warning_level_not_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A DEBUG-level log would be invisible at default ops log levels."""
    caplog.set_level(logging.DEBUG, logger=_TAXONOMY_LOGGER)
    materializer_primitive_for("ANOTHER_UNKNOWN_THING")
    debug_records = [
        rec
        for rec in caplog.records
        if rec.name == _TAXONOMY_LOGGER and rec.levelno == logging.DEBUG
    ]
    assert not debug_records, (
        "materializer_primitive_for must not emit DEBUG records on unknown — "
        "WARN is the contract so default ops logs see it"
    )


@pytest.mark.parametrize(
    "known_string, expected_primitive",
    [
        ("LP", Primitive.LP),
        ("UNI_V3", Primitive.LP),
        ("AAVE_V3", Primitive.LENDING),
        ("GMX_V2", Primitive.PERP),
        ("ERC4626", Primitive.VAULT),
        ("STAKING", Primitive.STAKING),
        ("PREDICTION", Primitive.PREDICTION),
        ("CEX", Primitive.UTILITY),
    ],
)
def test_known_strings_do_not_emit_warning(
    caplog: pytest.LogCaptureFixture,
    known_string: str,
    expected_primitive: Primitive,
) -> None:
    caplog.set_level(logging.DEBUG, logger=_TAXONOMY_LOGGER)
    result = materializer_primitive_for(known_string)
    assert result is expected_primitive, (
        f"materializer_primitive_for({known_string!r}) "
        f"returned {result!r}, expected {expected_primitive!r}"
    )

    warnings = [
        rec
        for rec in caplog.records
        if rec.name == _TAXONOMY_LOGGER and rec.levelno >= logging.WARNING
    ]
    assert not warnings, (
        f"unexpected WARN/ERROR for known string {known_string!r}: "
        f"{[r.getMessage() for r in warnings]}"
    )


def test_warn_message_normalized_form_is_visible(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The WARN should expose both the raw input and the normalized (upper) form
    so the operator can tell whether they have a casing bug or a real unknown."""
    caplog.set_level(logging.DEBUG, logger=_TAXONOMY_LOGGER)
    materializer_primitive_for("  morpho_legacy_v0  ")  # raw, with whitespace + lowercase
    warnings = [
        rec
        for rec in caplog.records
        if rec.name == _TAXONOMY_LOGGER and rec.levelno == logging.WARNING
    ]
    assert warnings
    msg = warnings[-1].getMessage()
    # Raw form preserved (whitespace + case) so operator sees what was passed.
    assert "morpho_legacy_v0" in msg
    # Normalized form also visible so operator can tell the upper/strip happened.
    assert "MORPHO_LEGACY_V0" in msg
