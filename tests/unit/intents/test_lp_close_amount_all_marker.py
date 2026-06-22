"""VIB-5346: LPCloseIntent ``amount="all"`` carrier-field marker + resolution.

Layer-1 unit coverage for the WEI-denominated chaining marker:

* ``is_chained_amount`` reflects ``amount="all"`` (and only that).
* ``serialize`` round-trips the ``"all"`` marker.
* The validator rejects a literal Decimal ``amount`` (close-all is the only
  meaningful chained semantic; a numeric amount would be a second silent
  carrier alongside ``position_id``).
* ``Intent.set_resolved_amount`` resolves the marker INTO ``position_id`` as a
  clean integer string and clears the marker, leaving a plain literal-close
  intent the compiler can read via ``int(position_id)``. Literal closes are
  returned unchanged.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import Intent, LPCloseIntent


def test_amount_all_marks_chained() -> None:
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")
    assert intent.is_chained_amount is True


def test_amount_none_is_not_chained() -> None:
    intent = LPCloseIntent(position_id="12345", protocol="uniswap_v3")
    assert intent.is_chained_amount is False
    assert intent.amount is None


def test_serialize_round_trips_all_marker() -> None:
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")
    data = intent.serialize()
    assert data["amount"] == "all"
    restored = LPCloseIntent.deserialize(data)
    assert restored.amount == "all"
    assert restored.is_chained_amount is True


def test_serialize_omits_marker_when_none() -> None:
    intent = LPCloseIntent(position_id="12345", protocol="uniswap_v3")
    data = intent.serialize()
    # model_dump emits the field as None; the marker-preservation branch only
    # forces the string "all". A None round-trips back to a literal close.
    assert data.get("amount") is None
    restored = LPCloseIntent.deserialize(data)
    assert restored.amount is None


def test_validator_rejects_numeric_amount() -> None:
    with pytest.raises(ValueError, match="must be the literal 'all'"):
        LPCloseIntent(position_id="0", protocol="pendle", amount=Decimal("100"))


def test_validator_rejects_empty_position_id_when_amount_none() -> None:
    with pytest.raises(ValueError, match="non-empty string when amount is None"):
        LPCloseIntent(position_id="", protocol="uniswap_v3")


def test_factory_threads_amount_marker() -> None:
    intent = Intent.lp_close(position_id="0", protocol="pendle", amount="all")
    assert isinstance(intent, LPCloseIntent)
    assert intent.amount == "all"
    assert intent.is_chained_amount is True


def test_set_resolved_amount_writes_wei_into_position_id() -> None:
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")
    resolved = Intent.set_resolved_amount(intent, Decimal(1_200_000_000_000_000_000))
    assert isinstance(resolved, LPCloseIntent)
    # WEI lands on position_id as a clean integer string.
    assert resolved.position_id == "1200000000000000000"
    # int(position_id) parses cleanly (no exponent / decimal point).
    assert int(resolved.position_id) == 1_200_000_000_000_000_000
    # Marker cleared -> a plain literal-position_id close.
    assert resolved.amount is None
    assert resolved.is_chained_amount is False


def test_set_resolved_amount_leaves_literal_close_unchanged() -> None:
    intent = LPCloseIntent(position_id="98765", protocol="uniswap_v3")
    resolved = Intent.set_resolved_amount(intent, Decimal("42"))
    # No marker -> position_id and amount are untouched.
    assert resolved.position_id == "98765"
    assert resolved.amount is None
