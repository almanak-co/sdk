"""Regression: a settlement-reconciler-registered perp must be closable by teardown (VIB-5572).

E2E reproduction (2026-08-07, ``deployment:194e6b4e8771``, position key
``0xca8ae552…e0485``): the settlement reconciler registered a GMX V2 perp whose
registry payload carried ``"direction": "long"`` and NO ``is_long`` key. The
teardown close builder read ``details["is_long"]`` alone and — correctly
refusing to guess — built NO closing intent, so the completeness gate failed
every teardown (two graceful + one emergency, all
"1 tracked-open position(s) have NO closing intent (VIB-5469/ALM-2900)"), and
the failed teardown entry-blocked the strategy: a permanent livelock that would
strand real funds on mainnet.

The fix normalizes the side vocabulary at both seams: the reconciler's registry
payload now writes the measured ``is_long`` boolean, and the close builder
accepts ``direction`` / ``side`` / the typed ``position.direction`` field via
``completeness._position_is_long`` — the SAME normalizer gate G1 uses, so the
builder and the gate can never again disagree about whether a side is known.

Mode coverage: graceful (SOFT) and emergency (HARD) teardown route through the
SAME builder (``generate_teardown_intents`` → ``teardown_full_close_intents`` →
``full_close_intents``) and the SAME coverage gate
(``_teardown_helpers.execute_and_verify`` G1, with the ``teardown_manager``
twin); the mode reaches neither function, so these builder+gate tests cover
both paths.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.intents import IntentType
from almanak.framework.teardown import PositionInfo, PositionType, full_close_intents
from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.registry_enumeration import _position_info_from_perp_registry_row

# The evidence run's shape: GMX V2 on Arbitrum, ETH/USD market, USDC collateral.
_POSITION_KEY = "0x" + "ca8ae552" + "0" * 51 + "e0485"
_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
_COLLATERAL = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _reconciler_row(direction: str | None = "long", **payload_overrides) -> dict:
    """One OPEN ``position_registry`` row exactly as ``_complete_registry`` writes it.

    The pre-fix payload shape: market + collateral + ``direction`` label, NO
    ``is_long`` key, ``source: settlement_reconciler``.
    """
    payload = {
        "protocol": "gmx_v2",
        "position_id": _POSITION_KEY,
        "market": _MARKET,
        "collateral_token": _COLLATERAL,
        "direction": direction,
        "source": "settlement_reconciler",
        "keeper_tx_hash": "0x" + "11" * 32,
    }
    payload.update(payload_overrides)
    return {"chain": "arbitrum", "primitive": "perp", "payload": payload}


def _perp_pos(**details) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=_POSITION_KEY,
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={"market": _MARKET, "collateral_token": _COLLATERAL, **details},
    )


def test_reconciler_long_row_builds_exactly_one_perp_close():
    """The exact e2e payload (direction="long", no is_long) → ONE full PERP_CLOSE."""
    info = _position_info_from_perp_registry_row(_reconciler_row("long"))
    assert info is not None
    out = full_close_intents([info])
    assert len(out) == 1
    close = out[0]
    assert close.intent_type == IntentType.PERP_CLOSE
    assert close.is_long is True
    assert close.market == _MARKET
    assert close.collateral_token == _COLLATERAL
    assert close.size_usd is None  # full close, live-resolved
    assert close.protocol == "gmx_v2"
    assert close.chain == "arbitrum"


def test_reconciler_short_row_builds_short_close():
    info = _position_info_from_perp_registry_row(_reconciler_row("short"))
    assert info is not None
    out = full_close_intents([info])
    assert len(out) == 1
    assert out[0].intent_type == IntentType.PERP_CLOSE
    assert out[0].is_long is False


def test_coverage_gate_passes_for_reconciler_row():
    """The gate that failed the e2e run: the built close now COVERS the position."""
    info = _position_info_from_perp_registry_row(_reconciler_row("long"))
    intents = full_close_intents([info])
    report = check_intent_coverage([info], intents)
    assert report.complete
    assert not report.uncovered


def test_reconciler_row_with_is_long_false_and_no_direction_closes_short():
    """The post-fix payload boolean alone suffices; measured False survives the
    enumeration's ``!= ""`` copy filter as a real short."""
    info = _position_info_from_perp_registry_row(_reconciler_row(None, is_long=False))
    assert info is not None
    assert info.details.get("is_long") is False
    out = full_close_intents([info])
    assert len(out) == 1
    assert out[0].is_long is False


def test_unmeasured_direction_row_is_still_skipped_never_guessed():
    """No side in ANY vocabulary → no intent (honesty over guessing, unchanged)."""
    info = _position_info_from_perp_registry_row(_reconciler_row(None))
    assert info is not None
    assert full_close_intents([info]) == []


def test_side_detail_and_typed_direction_field_accepted():
    """Hand-rolled summaries: ``details["side"]`` and the typed ``direction``
    field resolve through the same normalizer."""
    out_side = full_close_intents([_perp_pos(side="short")])
    assert len(out_side) == 1
    assert out_side[0].is_long is False

    typed = PositionInfo(
        position_type=PositionType.PERP,
        position_id=_POSITION_KEY,
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        direction="LONG",
        details={"market": _MARKET, "collateral_token": _COLLATERAL},
    )
    out_typed = full_close_intents([typed])
    assert len(out_typed) == 1
    assert out_typed[0].is_long is True


def test_measured_is_long_bool_still_wins_unchanged():
    """Pre-existing producers that write the bool keep byte-identical behaviour."""
    out = full_close_intents([_perp_pos(is_long=False)])
    assert len(out) == 1
    assert out[0].is_long is False


def test_empty_string_is_long_is_unmeasured_not_a_short():
    """``""`` is parser-silent (Empty ≠ Zero). The pre-fix builder coerced it to
    ``bool("") == False`` and would have closed the WRONG side; now it is
    unmeasured and the position is skipped loud (unless another vocabulary
    carries the side)."""
    assert full_close_intents([_perp_pos(is_long="")]) == []
    # ...but a direction label alongside it still resolves.
    out = full_close_intents([_perp_pos(is_long="", direction="long")])
    assert len(out) == 1
    assert out[0].is_long is True


def test_unrecognized_direction_vocabulary_is_skipped():
    """A direction outside long/short/buy/sell must never be guessed into a side."""
    info = _position_info_from_perp_registry_row(_reconciler_row("sideways"))
    assert info is not None
    assert full_close_intents([info]) == []


def test_sqlite_integer_is_long_resolves_identically_in_builder_and_gate():
    """SQLite round-trips a persisted boolean as an integer: 0 is a measured
    short, 1 a measured long — and the builder and the coverage gate resolve
    them through the SAME normalizer, so an int-flagged row both gets a close
    AND is credited by it (CodeRabbit on #3650)."""
    for flag, expected in ((0, False), (1, True)):
        info = _position_info_from_perp_registry_row(_reconciler_row(None, is_long=flag))
        assert info is not None
        assert info.details.get("is_long") == flag
        intents = full_close_intents([info])
        assert len(intents) == 1
        assert intents[0].is_long is expected
        report = check_intent_coverage([info], intents)
        assert report.complete
        assert not report.uncovered


def test_string_is_long_is_never_coerced_to_a_side():
    """A string in ``is_long`` is broken vocabulary, not a measured bool. The
    pre-fix builder coerced ANY truthy value with ``bool()`` — ``is_long="short"``
    became a LONG close, the wrong side. Every non-boolean now routes through
    the shared normalizer, which refuses it (skip loud, never guess)."""
    assert full_close_intents([_perp_pos(is_long="short")]) == []
    assert full_close_intents([_perp_pos(is_long="true")]) == []
    # ...unless a real vocabulary carries the side alongside the junk value.
    out = full_close_intents([_perp_pos(is_long="short", side="short")])
    assert len(out) == 1
    assert out[0].is_long is False
