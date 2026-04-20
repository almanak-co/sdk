"""Tests for _MinimalIntent intent_type -> IntentType mapping in inner_runner.

Regression guard for VIB-3143: `wrap_native` (and its counterpart
`unwrap_native`) was missing from `_TYPE_MAP`, causing the ResultEnricher
to fall back to `IntentType.SWAP` and log a "Unknown intent type
'wrap_native'; defaulting to SWAP" warning.
"""

from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.runner.inner_runner import _MinimalIntent


class TestMinimalIntentTypeMap:
    """Pins the string -> IntentType mapping in _MinimalIntent."""

    def test_wrap_native_maps_to_wrap_native_enum(self):
        """`wrap_native` must map to IntentType.WRAP_NATIVE, not fall back to SWAP."""
        intent = _MinimalIntent("wrap_native", {})
        assert intent.intent_type is IntentType.WRAP_NATIVE

    def test_wrap_native_case_insensitive(self):
        """Upper-case `WRAP_NATIVE` must also resolve correctly (enum value form)."""
        intent = _MinimalIntent("WRAP_NATIVE", {})
        assert intent.intent_type is IntentType.WRAP_NATIVE

    def test_unwrap_native_maps_to_unwrap_native_enum(self):
        """`unwrap_native` must map to IntentType.UNWRAP_NATIVE, not fall back to SWAP."""
        intent = _MinimalIntent("unwrap_native", {})
        assert intent.intent_type is IntentType.UNWRAP_NATIVE

    def test_unwrap_native_case_insensitive(self):
        """Upper-case `UNWRAP_NATIVE` must also resolve correctly (enum value form)."""
        intent = _MinimalIntent("UNWRAP_NATIVE", {})
        assert intent.intent_type is IntentType.UNWRAP_NATIVE

    def test_swap_still_maps_correctly(self):
        """Sanity check that existing mappings are untouched."""
        intent = _MinimalIntent("swap", {})
        assert intent.intent_type is IntentType.SWAP
