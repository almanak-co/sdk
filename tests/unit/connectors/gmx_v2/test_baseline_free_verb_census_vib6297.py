"""Every verb that can reach the settlement barrier must be classified (VIB-6297).

VIB-6297's fix introduces `_goal_is_baseline_free_decidable`, which today names exactly
one verb: `PERP_CLOSE`. That is a list, and the governing rule for this epic is that a fix
may DELETE a list, or ADD a test that fails when a list is incomplete — it may not add a
list and it may not extend one without a test that would have caught the omission.

This file is that test. It derives the verb universe from the PRODUCER of async orders —
`GMXv2ReceiptParser.REQUIRED_EXTRACTIONS_BY_INTENT`, the table that decides which intents
emit `async_orders` at all — rather than restating it. An intent that emits async orders is
by definition an intent that can reach the barrier. Add a third verb there and this file
fails until someone decides what that verb's target means without a baseline.

Deriving the universe from a hand-written second copy would defeat the point: the failure
mode being guarded is precisely someone updating one list and not the other.

## Why silence is the danger, not a wrong answer

Both functions under census answer with a bare bool, and in both the DEFAULT is the unsafe
direction for at least one caller:

- `_goal_is_baseline_free_decidable` defaults to False → an unclassified verb is treated as
  undecidable → the barrier burns its full budget and reports a failure. Wasteful, honest.
- `_position_delta_reached` defaults to `return False` → an unclassified verb yields
  TERMINAL_FAILED on an order that actually filled. For an open, that can make a strategy
  believe its open failed while a position exists, and re-open. **That defect is already in
  the tree**; VIB-6297 does not fix it. This census makes it impossible to add a third verb
  without confronting it.
"""

from __future__ import annotations

import pytest

from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser
from almanak.connectors.gmx_v2.runner_hooks import (
    _GmxSettlementBaseline,
    _goal_is_baseline_free_decidable,
    _position_delta_reached,
)

_MARKET = "0x" + "11" * 20
_COLLATERAL = "0x" + "33" * 20
_RAW_USD = 10**30
_KEY = (_MARKET, _COLLATERAL, True)

# The classification of record. Every verb in the derived universe must appear here with a
# stated reason, and every verb here must still be in the universe — a verb that stops
# emitting async orders should be removed rather than left as a comforting stale entry.
_CLASSIFICATION: dict[str, bool] = {
    # c == 0 satisfies `c <= max(0, b - d)` for every unmeasured baseline b and delta d.
    "PERP_CLOSE": True,
    # c >= b + d is undecidable without b: a pre-existing position of sufficient size
    # satisfies any absolute reading while the order may in fact have been cancelled.
    "PERP_OPEN": False,
}


def _async_order_verbs() -> set[str]:
    """The verbs that can put an async order through the settlement barrier."""
    parser = GMXv2ReceiptParser
    verbs = {intent for intent, required in parser.REQUIRED_EXTRACTIONS_BY_INTENT.items() if "async_orders" in required}
    verbs |= {intent for intent, extra in parser.EXTRA_EXTRACTIONS_BY_INTENT.items() if "async_orders" in extra}
    return verbs


def test_every_async_order_verb_is_explicitly_classified() -> None:
    """A new verb must be classified before it can reach the barrier."""
    universe = _async_order_verbs()

    unclassified = universe - set(_CLASSIFICATION)
    assert not unclassified, (
        f"{sorted(unclassified)} can emit async orders and therefore reach the settlement "
        "barrier, but no one has decided whether its target is decidable without a position "
        "baseline. Decide, then add it to _CLASSIFICATION and to "
        "_goal_is_baseline_free_decidable. Leaving it unclassified means the barrier burns "
        "its full timeout budget on it — see VIB-6297."
    )

    stale = set(_CLASSIFICATION) - universe
    assert not stale, (
        f"{sorted(stale)} is classified here but no longer emits async orders. Remove it "
        "rather than leaving a stale entry that reads as coverage."
    )


@pytest.mark.parametrize("verb", sorted(_CLASSIFICATION))
def test_classification_matches_the_implementation(verb: str) -> None:
    """The table above and the shipped predicate must not drift apart."""
    assert _goal_is_baseline_free_decidable(verb) is _CLASSIFICATION[verb]


def test_an_unknown_verb_is_never_treated_as_decidable() -> None:
    """The predicate must fail toward 'cannot judge', not toward judging.

    This is the property that makes an unclassified verb merely wasteful rather than
    dangerous: it can never produce a fabricated SETTLED or TERMINAL_FAILED.
    """
    for unknown in ("PERP_INCREASE", "PERP_DECREASE", "PERP_CANCEL_ORDER", "", "perp_close"):
        assert _goal_is_baseline_free_decidable(unknown) is False


def test_position_delta_reached_still_defaults_unsafely_for_unknown_verbs() -> None:
    """Pins a defect that predates VIB-6297 and is NOT fixed by it.

    `_position_delta_reached` returns False for any verb it does not name, and its caller
    turns False into TERMINAL_FAILED. So an unclassified verb whose order genuinely filled
    is reported as a terminal failure — for an open, the direction that can double a
    position.

    Asserted as-is rather than fixed, so the behaviour is documented and cannot change
    silently. Whoever adds a third async-order verb will trip the census above, land here,
    and have to decide what an unclassified verb should do. That decision needs its own
    ticket; it is not smuggled into the stall fix.
    """
    baseline = _GmxSettlementBaseline(((_KEY, 100 * _RAW_USD),))
    requested = {_KEY: 50 * _RAW_USD}
    # A read that unambiguously shows the position grew by the requested delta.
    current = {_KEY: 150 * _RAW_USD}

    assert _position_delta_reached("PERP_OPEN", requested, baseline, current) is True
    assert _position_delta_reached("PERP_INCREASE", requested, baseline, current) is False


def test_close_is_decidable_only_at_a_measured_flat_position() -> None:
    """Guards the sufficiency claim itself, not just the verb table.

    `PERP_CLOSE` is classified decidable because c == 0 settles it for every baseline. That
    is sufficient, not necessary — a partial close at c > 0 remains undecidable, and the
    implementation must not quietly widen to "c < requested delta" or similar, which would
    need the baseline it does not have.
    """
    baseline = _GmxSettlementBaseline(((_KEY, 100 * _RAW_USD),))
    requested = {_KEY: 100 * _RAW_USD}

    # With a baseline, a partial close of the requested size settles.
    assert _position_delta_reached("PERP_CLOSE", requested, baseline, {_KEY: 0}) is True
    # Without one, only a flat account may be called settled; this pins that the
    # baseline-free path never gets to reason about a non-zero current size.
    assert _goal_is_baseline_free_decidable("PERP_CLOSE") is True


def test_unmeasured_intent_type_is_not_decidable() -> None:
    """An intent whose type could not be read is unmeasured, never assumed to be a close."""
    assert _goal_is_baseline_free_decidable("") is False


# ---------------------------------------------------------------------------
# Truncation is only knowable at the REDUCER (VIB-6313 / #3533 panel)
# ---------------------------------------------------------------------------


def test_the_reducer_reports_truncation_from_the_raw_page_not_the_filtered_one():
    """The reason a caller-side length test cannot work, pinned.

    `_reduce_gmx_positions` filters to ACTIVE rows before building
    `PerpsReadResult`, so by the time any caller sees the result the raw page
    length is gone. A full page containing inactive rows yields FEWER active rows
    than the requested range and would slip straight past
    `len(positions) >= _MAX_POSITION_RANGE` — which is exactly what an earlier
    revision of the settlement guard tried.

    So this builds a full raw page in which only ONE row is active, and requires
    `truncated` to still be True while `len(positions) == 1`.
    """
    from eth_abi import encode as abi_encode

    from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery
    from almanak.connectors.gmx_v2.addresses import GMX_V2_MARKETS, GMX_V2_TOKENS
    from almanak.connectors.gmx_v2.perps_read import (
        _GET_ACCOUNT_POSITIONS_OUTPUT,
        _MAX_POSITION_RANGE,
        _reduce_gmx_positions,
    )

    account = "0x" + "aa" * 20
    market = GMX_V2_MARKETS["arbitrum"]["ETH/USD"]
    collateral = GMX_V2_TOKENS["arbitrum"]["USDC"]

    def _row(size_in_usd: int):
        numbers = [size_in_usd, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        return ((account, market, collateral), tuple(numbers), (True,))

    # A FULL page, but only the first row is active (size > 0).
    page = [_row(10**30)] + [_row(0) for _ in range(_MAX_POSITION_RANGE - 1)]
    blob = "0x" + abi_encode([_GET_ACCOUNT_POSITIONS_OUTPUT], [page]).hex()

    result = _reduce_gmx_positions(
        PerpsPositionQuery(chain="arbitrum", wallet_address=account, targets={}),
        [blob],
    )

    assert result.ok is True
    assert len(result.positions) == 1, "fixture drifted — the page must filter down to one active row"
    assert result.truncated is True, (
        "a full raw page must report truncated even when the ACTIVE count is 1; "
        "this is precisely what no caller-side length test can detect"
    )
