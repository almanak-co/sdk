"""A venue-confirmed ladder position must never be publishable as dust.

Both DCA ladders enumerate teardown from a real venue read. The row they publish
carries a ``value_usd``, and ``_measure_open_positions_after_teardown`` DROPS any
row at or below its dust threshold when it measures what teardown left behind.

The trap those two facts set: the ladders fell back to ``_cumulative_size_usd``
whenever the venue notional could not be priced — and that counter is
``Decimal("0")`` in exactly the case the venue probe was added to catch (a keeper
fill the cache never recorded, or a crash before the first state save). A live
position published at ``$0`` is invisible to the very check that should catch it,
so teardown certifies success over it and the migration to a real chain read buys
nothing.

Provenance is pinned alongside value because the two were conflated: a position
the venue positively measured, whose notional merely could not be priced, was
being labelled ``strategy_cache_unverified``.

Covers both twins — the arb and avax ladders carry identical teardown blocks, and
a fix applied to one only is the recurring shape here.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_LADDERS = ("gmx_dca_ladder_arb", "gmx_dca_ladder_avax")


def _load(name: str):
    seed_dir = _REPO_ROOT / "strategies" / "experiments" / name
    spec = importlib.util.spec_from_file_location(f"dca_{name}", seed_dir / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cfg = json.loads((seed_dir / "config.json").read_text(encoding="utf-8"))
    cls = module.GMXDCALadderStrategy
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(strat)
    # ``deployment_id`` is a read-only property over ``_deployment_id``; the
    # teardown summary reads it, and ``IntentStrategy.__init__`` (patched out
    # above) is what normally sets the backing attribute.
    strat._chain = "arbitrum"
    strat._deployment_id = "deployment:test"
    strat.create_market_snapshot = MagicMock(return_value=MagicMock())
    return module, strat


def _probe(*, state: str, notional):
    """A probe result carrying the venue's answer, valued or not."""
    from almanak.framework.strategies import PerpProbePosition, PerpProbeState

    positions = ()
    if state == "OPEN":
        positions = (
            PerpProbePosition(
                is_long=True, market="ETH/USD", collateral_token="USDC", notional_usd=notional
            ),
        )
    probe = MagicMock()
    probe.state = getattr(PerpProbeState, state)
    probe.is_open = state == "OPEN"
    probe.is_measured = state != "UNMEASURED"
    probe.positions = positions
    probe.reason = "test"
    return probe


def _rows(strat, probe):
    with patch("almanak.framework.strategies.probe_perp_position", return_value=probe):
        return strat.get_open_positions().positions


@pytest.mark.parametrize("ladder", _LADDERS)
class TestAVenueConfirmedPositionSurvivesTheDustFilter:
    def test_unpriceable_venue_position_is_not_published_as_zero(self, ladder: str) -> None:
        """The ALM-3109 shape: the venue holds a fill the cache never recorded."""
        module, strat = _load(ladder)
        strat._cumulative_size_usd = Decimal("0")  # the cache missed the fill
        strat._tranches_done = 0

        rows = _rows(strat, _probe(state="OPEN", notional=None))

        assert len(rows) == 1
        assert rows[0].value_usd > module._DUST_NOTIONAL_USD, (
            "a venue-confirmed live position was published at or below the teardown dust "
            "threshold, so the residual check drops it and teardown certifies success over it"
        )
        assert rows[0].details["value_usd_unknown"] is True
        assert rows[0].details["valuation_status"] == "no_path"

    def test_the_venue_measured_it_so_provenance_is_venue(self, ladder: str) -> None:
        _module, strat = _load(ladder)
        strat._cumulative_size_usd = Decimal("0")
        rows = _rows(strat, _probe(state="OPEN", notional=None))
        assert rows[0].details["position_source"] == "venue", (
            "the venue positively measured this position; only its VALUE was unknown"
        )

    def test_unmeasured_read_with_an_empty_counter_is_not_dust_either(self, ladder: str) -> None:
        """The other zero path: cache-side evidence with a zero counter."""
        module, strat = _load(ladder)
        strat._cumulative_size_usd = Decimal("0")
        strat._tranches_done = 1  # local evidence that something was opened
        rows = _rows(strat, _probe(state="UNMEASURED", notional=None))
        assert len(rows) == 1
        assert rows[0].value_usd > module._DUST_NOTIONAL_USD
        assert rows[0].details["position_source"] == "strategy_cache_unverified"
        assert rows[0].details["value_usd_unknown"] is True

    def test_a_priced_venue_position_reports_its_real_value(self, ladder: str) -> None:
        """Liveness control: the fallback must not swallow a genuine measurement."""
        _module, strat = _load(ladder)
        strat._cumulative_size_usd = Decimal("0")
        rows = _rows(strat, _probe(state="OPEN", notional=Decimal("123.45")))
        assert rows[0].value_usd == Decimal("123.45")
        assert rows[0].details["position_source"] == "venue"
        assert "value_usd_unknown" not in rows[0].details

    def test_a_small_but_MEASURED_notional_is_reported_as_measured(self, ladder: str) -> None:
        """The fallback must not overwrite a measurement just because it is small.

        Two different thresholds meet here and must not be conflated. The ladder's
        ``_DUST_NOTIONAL_USD`` ($0.50) is a POLICY threshold — "not worth an
        execution fee to close". The residual harness drops rows at a VISIBILITY
        threshold 50x smaller ($0.01), and is explicit that "only a MEASURED dust
        value excuses it".

        So a venue position priced at $0.25 is a real measurement: it must be
        published as $0.25 with no unmeasured markers. Replacing it with the
        configured ladder notional would substitute a REQUESTED number for a
        measured one — Empty ≠ Zero inverted, and strictly worse than the $0 this
        fallback exists to prevent, because $0 at least advertises itself as empty.
        """
        _module, strat = _load(ladder)
        strat._cumulative_size_usd = Decimal("0")
        rows = _rows(strat, _probe(state="OPEN", notional=Decimal("0.25")))
        assert rows[0].value_usd == Decimal("0.25")
        assert "value_usd_unknown" not in rows[0].details
        assert rows[0].details["position_source"] == "venue"

    def test_a_measured_flat_venue_publishes_nothing(self, ladder: str) -> None:
        """The fallback must not manufacture a phantom residual on a flat account."""
        _module, strat = _load(ladder)
        strat._cumulative_size_usd = Decimal("0")
        assert _rows(strat, _probe(state="FLAT", notional=None)) == []
