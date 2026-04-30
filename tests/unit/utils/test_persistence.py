"""Unit tests for ``almanak.framework.utils.persistence``.

VIB-3757: helper exists so a single malformed persisted entry doesn't
block strategy recovery at startup.
"""

from __future__ import annotations

import logging

import pytest

from almanak.framework.utils.persistence import safe_int_list


class TestSafeIntList:
    """Behaviour matrix for ``safe_int_list``."""

    def test_passes_through_well_formed_int_list(self) -> None:
        assert safe_int_list([1, 2, 3]) == [1, 2, 3]

    def test_coerces_string_digits(self) -> None:
        assert safe_int_list(["1", "2", "3"]) == [1, 2, 3]

    def test_returns_empty_for_none(self) -> None:
        assert safe_int_list(None) == []

    def test_returns_empty_for_missing_key_pattern(self) -> None:
        # Mirrors the canonical caller pattern: state.get("k") with no key.
        state: dict[str, object] = {}
        assert safe_int_list(state.get("position_bin_ids")) == []

    def test_returns_empty_for_explicit_empty_list(self) -> None:
        assert safe_int_list([]) == []

    def test_drops_malformed_entries_keeps_good_ones(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            assert safe_int_list([1, "bad", 2, None, 3]) == [1, 2, 3]
        # Should warn for each of "bad" and None.
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 2

    def test_warning_includes_field_name_when_supplied(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            safe_int_list(["nope"], name="position_bin_ids")
        assert any(
            "'position_bin_ids'" in record.getMessage()
            for record in caplog.records
        )

    def test_string_input_raises_not_iterated_as_chars(self) -> None:
        # Critical (VIB-3757 audit): ``"123"`` must NOT yield ``[1, 2, 3]``
        # (per-char iteration), and must NOT silently return ``[]`` either —
        # silent ``[]`` would mask schema corruption and orphan an open
        # on-chain position.
        with pytest.raises(ValueError, match="expected an iterable of ints"):
            safe_int_list("123")

    def test_bytes_input_raises(self) -> None:
        with pytest.raises(ValueError, match="expected an iterable of ints"):
            safe_int_list(b"\x01\x02")

    def test_dict_input_raises_not_iterated_as_keys(self) -> None:
        with pytest.raises(ValueError, match="expected an iterable of ints"):
            safe_int_list({1: "a", 2: "b"})

    def test_scalar_int_raises(self) -> None:
        with pytest.raises(ValueError, match="non-iterable"):
            safe_int_list(42)

    def test_raise_message_includes_field_name(self) -> None:
        with pytest.raises(ValueError, match="'position_bin_ids'"):
            safe_int_list("oops", name="position_bin_ids")

    def test_tuple_input_works(self) -> None:
        assert safe_int_list((1, 2, 3)) == [1, 2, 3]

    def test_generator_input_works(self) -> None:
        assert safe_int_list(iter([1, 2, 3])) == [1, 2, 3]

    def test_float_coerces_via_int(self) -> None:
        # int(1.7) == 1 — we accept this (matches existing behaviour of
        # the per-strategy pattern this helper replaces).
        assert safe_int_list([1.7, 2.0]) == [1, 2]

    def test_no_warnings_on_well_formed_input(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            safe_int_list([1, 2, 3])
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert warnings == []

    def test_drops_overflow_entry_and_continues(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # ``int(float("inf"))`` raises ``OverflowError``, which sits outside
        # ``(TypeError, ValueError)``. Persisted state from a buggy writer
        # could contain inf / NaN floats; we must drop them like any other
        # malformed entry, not propagate the OverflowError.
        with caplog.at_level(logging.WARNING):
            assert safe_int_list([1, float("inf"), 2, float("nan")]) == [1, 2]
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 2
