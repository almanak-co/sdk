"""Unit tests for Morpho Blue permission hints.

Imports the module to execute the module-level constants and verifies the
exported PermissionHints object has the expected shape.
"""

from __future__ import annotations

from almanak.connectors.morpho_blue.permission_hints import (
    _MORPHO_SELECTOR_LABELS,
    _SYNTHETIC_MARKET_ID,
    PERMISSION_HINTS,
)


class TestPermissionHints:
    def test_synthetic_market_id_is_bytes32(self) -> None:
        assert _SYNTHETIC_MARKET_ID.startswith("0x")
        assert len(_SYNTHETIC_MARKET_ID) == 66

    def test_selector_labels_present(self) -> None:
        assert "0xa99aad89" in _MORPHO_SELECTOR_LABELS  # supply
        assert "0x238d6579" in _MORPHO_SELECTOR_LABELS  # supplyCollateral
        assert "0x5c2bea49" in _MORPHO_SELECTOR_LABELS  # withdraw
        assert "0x8720316d" in _MORPHO_SELECTOR_LABELS  # withdrawCollateral
        assert "0x50d8cd4b" in _MORPHO_SELECTOR_LABELS  # borrow
        assert "0x20b76e81" in _MORPHO_SELECTOR_LABELS  # repay

    def test_permission_hints_object(self) -> None:
        assert PERMISSION_HINTS.synthetic_market_id == _SYNTHETIC_MARKET_ID
        assert PERMISSION_HINTS.selector_labels == _MORPHO_SELECTOR_LABELS

    def test_selector_labels_are_signature_strings(self) -> None:
        for sig in _MORPHO_SELECTOR_LABELS.values():
            assert "(" in sig
            assert sig.endswith(")")
