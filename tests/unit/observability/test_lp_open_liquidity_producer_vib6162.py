"""VIB-6162 — position_events.liquidity must actually be populated.

The column exists, is carried by the gateway read projection, and was EMPTY for
every LP connector in the corpus. Each connector missed it a different way, so a
fixture built from either one alone would have looked fine:

* Aerodrome emits no ``lp_open_data`` at all -> the guard's early return fired
  and the assignment was never reached.
* Curve emits ``lp_open_data`` with ``liquidity=None`` -> the assignment ran and
  wrote "".

Shapes below are taken from real captured rows (the VIB-6162 Anvil reproduction
and 20260719-0036-curve-curve_lp_lifecycle_arbitrum).
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.framework.observability.position_events import PositionEvent, _apply_lp_open

_INTENT = SimpleNamespace(token0=None, token1=None, from_token=None, to_token=None)


def _lp_open_data(liquidity):
    return SimpleNamespace(
        position_id=0,
        liquidity=liquidity,
        tick_lower=None,
        tick_upper=None,
        current_tick=None,
        amount0=None,
        amount1=None,
    )


def _liquidity(extracted) -> str:
    event = PositionEvent()
    _apply_lp_open(event, SimpleNamespace(extracted=extracted, intent=_INTENT))
    return event.liquidity


def test_aerodrome_populates_without_lp_open_data():
    """The real Aerodrome shape: no lp_open_data, bare int at the top level."""
    assert _liquidity({"liquidity": 1126027863160}) == "1126027863160"


def test_curve_populates_despite_a_null_nested_liquidity():
    """The real Curve shape: lp_open_data present, its liquidity None."""
    got = _liquidity(
        {
            "lp_open_data": _lp_open_data(None),
            "liquidity": {"_type": "Decimal", "value": "3.910003556141227987"},
        }
    )
    assert got == "3.910003556141227987"


def test_the_nested_value_stays_authoritative_when_present():
    """The fallback fills a gap; it must never override a real nested value."""
    assert _liquidity({"lp_open_data": _lp_open_data("99"), "liquidity": "11"}) == "99"


def test_an_absent_quantity_stays_empty():
    """Empty != Zero: no measurement must not become a measured zero."""
    assert _liquidity({}) == ""
    assert _liquidity({"lp_open_data": _lp_open_data(None)}) == ""


def test_a_measured_zero_is_preserved():
    """The other half of Empty != Zero: a real zero is a measurement."""
    assert _liquidity({"liquidity": "0"}) == "0"
