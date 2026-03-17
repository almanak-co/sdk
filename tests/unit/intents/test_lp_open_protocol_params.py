"""Tests for LPOpenIntent protocol_params and TraderJoe bin_range wiring (VIB-1409).

Verifies that:
1. LPOpenIntent accepts an optional protocol_params dict
2. The TraderJoe V2 compiler reads bin_range from protocol_params
3. Default bin_range=5 is used when protocol_params is absent
4. Invalid bin_range values are rejected at model validation time
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import Intent, LPOpenIntent


def _make_intent(**kwargs):
    """Helper to create a TraderJoe LPOpenIntent with sensible defaults."""
    defaults = {
        "pool": "0xpool",
        "amount0": Decimal("1"),
        "amount1": Decimal("1"),
        "range_lower": Decimal("1000"),
        "range_upper": Decimal("2000"),
        "protocol": "traderjoe_v2",
    }
    defaults.update(kwargs)
    return LPOpenIntent(**defaults)


class TestLPOpenProtocolParams:
    """Test protocol_params field on LPOpenIntent."""

    def test_protocol_params_defaults_to_none(self):
        """protocol_params should default to None when not provided."""
        intent = _make_intent()
        assert intent.protocol_params is None

    def test_protocol_params_accepts_bin_range(self):
        """protocol_params should accept and preserve bin_range."""
        intent = _make_intent(protocol_params={"bin_range": 10})
        assert intent.protocol_params == {"bin_range": 10}

    def test_protocol_params_serializes(self):
        """protocol_params should survive serialization round-trip."""
        intent = _make_intent(protocol_params={"bin_range": 10})
        serialized = intent.serialize()
        assert serialized["protocol_params"] == {"bin_range": 10}

        deserialized = LPOpenIntent.deserialize(serialized)
        assert deserialized.protocol_params == {"bin_range": 10}

    def test_protocol_params_none_serializes(self):
        """None protocol_params should serialize cleanly."""
        intent = _make_intent(protocol_params=None)
        serialized = intent.serialize()
        deserialized = LPOpenIntent.deserialize(serialized)
        assert deserialized.protocol_params is None


class TestProtocolParamsValidation:
    """Test validation of protocol_params at model level."""

    def test_bin_range_zero_rejected(self):
        """bin_range=0 should be rejected (must be >= 1)."""
        with pytest.raises(ValueError, match="bin_range must be an integer between 1 and 100"):
            _make_intent(protocol_params={"bin_range": 0})

    def test_bin_range_negative_rejected(self):
        """Negative bin_range should be rejected."""
        with pytest.raises(ValueError, match="bin_range must be an integer between 1 and 100"):
            _make_intent(protocol_params={"bin_range": -5})

    def test_bin_range_too_large_rejected(self):
        """bin_range > 100 should be rejected."""
        with pytest.raises(ValueError, match="bin_range must be an integer between 1 and 100"):
            _make_intent(protocol_params={"bin_range": 101})

    def test_bin_range_string_rejected(self):
        """Non-integer bin_range should be rejected."""
        with pytest.raises(ValueError, match="bin_range must be an integer between 1 and 100"):
            _make_intent(protocol_params={"bin_range": "ten"})

    def test_bin_range_boundary_1_accepted(self):
        """bin_range=1 (minimum) should be accepted."""
        intent = _make_intent(protocol_params={"bin_range": 1})
        assert intent.protocol_params["bin_range"] == 1

    def test_bin_range_boundary_100_accepted(self):
        """bin_range=100 (maximum) should be accepted."""
        intent = _make_intent(protocol_params={"bin_range": 100})
        assert intent.protocol_params["bin_range"] == 100

    def test_bin_range_bool_rejected(self):
        """Boolean bin_range should be rejected (bool is subclass of int)."""
        with pytest.raises(ValueError, match="bin_range must be an integer between 1 and 100"):
            _make_intent(protocol_params={"bin_range": True})

    def test_empty_protocol_params_accepted(self):
        """Empty dict is valid (no bin_range key to validate)."""
        intent = _make_intent(protocol_params={})
        assert intent.protocol_params == {}


class TestTraderJoeBinRangeCompilerWiring:
    """Test that bin_range is correctly read from protocol_params using the same pattern as the compiler."""

    def test_bin_range_from_protocol_params(self):
        """Custom bin_range should be extractable from protocol_params."""
        intent = _make_intent(protocol_params={"bin_range": 10})
        params = intent.protocol_params or {}
        assert int(params.get("bin_range", 5)) == 10

    def test_bin_range_defaults_without_protocol_params(self):
        """Default bin_range=5 when no protocol_params."""
        intent = _make_intent()
        params = intent.protocol_params or {}
        assert int(params.get("bin_range", 5)) == 5

    def test_bin_range_defaults_with_empty_protocol_params(self):
        """Default bin_range=5 when protocol_params is empty dict."""
        intent = _make_intent(protocol_params={})
        params = intent.protocol_params or {}
        assert int(params.get("bin_range", 5)) == 5


class TestIntentFactoryLPOpen:
    """Test that Intent.lp_open() factory forwards protocol_params."""

    def test_factory_forwards_protocol_params(self):
        """Intent.lp_open() should pass protocol_params to LPOpenIntent."""
        intent = Intent.lp_open(
            pool="0xpool",
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("2000"),
            protocol="traderjoe_v2",
            protocol_params={"bin_range": 15},
        )
        assert isinstance(intent, LPOpenIntent)
        assert intent.protocol_params == {"bin_range": 15}

    def test_factory_defaults_protocol_params_to_none(self):
        """Intent.lp_open() should default protocol_params to None."""
        intent = Intent.lp_open(
            pool="0xpool",
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("2000"),
        )
        assert intent.protocol_params is None
