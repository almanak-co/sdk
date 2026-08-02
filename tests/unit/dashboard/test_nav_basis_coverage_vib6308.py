"""VIB-6308 — an unbacked NAV leg must not book as profit (reader + tile).

Strategy PnL = ``open_position_nav − cost_basis``. The NAV side counts a leg's
full mark; the cost side (``valuation/net_debt.py``) skips any leg whose
``cost_basis_usd`` did not survive serialization. So an unbacked leg's mark is
differenced against nothing and reads as pure profit — a leveraged carry's
borrowed-and-swapped holding booked **+41.8% on a flat position** for its entire
life, reproduced by a blind dashboard auditor as "+40.7%…+42.2%".

The writer stamps the coverage (it is the only layer that knows the VIB-4909
wallet-overlap partition, so it is the only one that can say which legs reached
NAV). These tests cover the two consumers of that stamp: the reader that lifts it
onto ``PnLSummary`` and the tile that must refuse to render a number over it.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

# The tile receives the CLIENT PnLSummary (gateway_client), not the aggregation
# dataclass of the same name. Constructing the wrong one here is how a tile guard
# passes its tests and AttributeErrors in production (the VIB-5170 class) - a
# pre-existing seam test caught exactly that during this fix.
from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
from almanak.framework.dashboard.pages._detail_header import _strategy_pnl_usd
from almanak.framework.dashboard.quant_aggregations import _nav_basis_partial, compute_pnl_summary


def _zero_cost_stack() -> CostStackInfo:
    """Every realized/earn component measured-zero, inventory MTM unmeasured —
    so ``_strategy_pnl_usd`` reduces to its unrealized leg and isolates the
    cost-basis read under test."""
    z = Decimal("0")
    return CostStackInfo(
        cost_gas_usd=z,
        cost_protocol_fees_usd=z,
        cost_slippage_usd=z,
        fees_earned_usd=z,
        interest_paid_usd=z,
        interest_earned_usd=z,
        funding_paid_usd=z,
        funding_earned_usd=z,
        realized_pnl_usd=z,
        il_usd=z,
        inventory_unrealized_usd=None,
    )


def _pnl(*, deployed_capital_usd: Decimal, cost_basis_partial: bool) -> PnLSummary:
    """A client PnLSummary with only the two fields under test meaningful."""
    z = Decimal("0")
    return PnLSummary(
        deployed_usd=z,
        nav_usd=z,
        lifetime_pnl_usd=z,
        lifetime_pnl_pct=z,
        net_apr_pct=z,
        max_drawdown_pct=z,
        current_drawdown_pct=z,
        value_confidence="HIGH",
        age_days=0,
        deployed_capital_usd=deployed_capital_usd,
        available_cash_usd=z,
        open_position_count=1,
        primary_risk_kind="none",
        primary_risk_label="",
        primary_risk_value="",
        primary_risk_color="neutral",
        cost_basis_partial=cost_basis_partial,
    )


def _snapshot(coverage):
    """A snapshot carrying (or omitting) the writer's coverage stamp."""
    meta = {"gas_native_status": "ok"}
    if coverage is not None:
        meta["nav_basis_coverage"] = coverage
    return SimpleNamespace(snapshot_metadata=meta)


class TestReaderLiftsTheCoverageStamp:
    """Three states — and the third is why this is not a plain ``.get()``."""

    def test_missing_legs_are_partial(self):
        assert _nav_basis_partial(_snapshot({"legs_in_nav": 2, "legs_missing_basis": 1})) is True

    def test_fully_backed_is_not_partial(self):
        assert _nav_basis_partial(_snapshot({"legs_in_nav": 2, "legs_missing_basis": 0})) is False

    def test_absent_stamp_is_unknown_not_partial(self):
        """A pre-VIB-6308 snapshot must not be retro-labelled partial.

        Coverage is genuinely UNKNOWN there. Returning True would flip every
        historical row's tile to a warning on no evidence; returning False keeps
        today's behaviour for rows nobody measured.
        """
        assert _nav_basis_partial(_snapshot(None)) is False
        assert _nav_basis_partial(SimpleNamespace()) is False
        assert _nav_basis_partial(None) is False

    def test_reads_the_dict_and_envelope_shapes_too(self):
        """Both shapes reach this function in practice.

        A real ``PortfolioSnapshot`` has no ``positions_json`` attribute — keying
        on one shape alone is the VIB-5170 inert-feature class, where the read
        silently returns nothing in production while tests pass.
        """
        assert _nav_basis_partial({"snapshot_metadata": {"nav_basis_coverage": {"legs_missing_basis": 3}}}) is True
        envelope = '{"positions": [], "metadata": {"nav_basis_coverage": {"legs_missing_basis": 2}}}'
        assert _nav_basis_partial(SimpleNamespace(positions_json=envelope)) is True

    def test_malformed_coverage_does_not_raise(self):
        assert _nav_basis_partial(_snapshot({"legs_missing_basis": "not-a-number"})) is False
        assert _nav_basis_partial(_snapshot("not-a-dict")) is False


class TestTileRefusesToRenderOverPartialCoverage:
    """The carry numbers, to scale: collateral measured, holding not."""

    def test_partial_coverage_suppresses_strategy_pnl(self):
        pnl = _pnl(deployed_capital_usd=Decimal("2.064408"), cost_basis_partial=True)

        assert _strategy_pnl_usd(pnl, _zero_cost_stack(), Decimal("2.927712")) is None

    def test_full_coverage_still_renders(self):
        """The guard must not suppress a strategy whose basis IS fully backed —
        narrowing to 'never render' would be a regression dressed as a fix."""
        pnl = _pnl(deployed_capital_usd=Decimal("2.064408"), cost_basis_partial=False)

        assert _strategy_pnl_usd(pnl, _zero_cost_stack(), Decimal("2.927712")) == Decimal("0.863304")

    def test_the_phantom_it_suppresses_is_the_observed_one(self):
        """Pin the magnitude: the suppressed number is the +41.8% the blind
        auditor saw, so a future change that re-renders it is caught by value,
        not just by shape."""
        pnl = _pnl(deployed_capital_usd=Decimal("2.064408"), cost_basis_partial=False)
        phantom = _strategy_pnl_usd(pnl, _zero_cost_stack(), Decimal("2.927712"))

        assert phantom is not None
        assert Decimal("40") < (phantom / pnl.deployed_capital_usd * 100) < Decimal("43")

    def test_a_flat_closed_strategy_is_unaffected(self):
        """``open_position_nav`` ~0 means nothing unbacked is in NAV, so a
        partial flag must not suppress a realized-only PnL."""
        pnl = _pnl(deployed_capital_usd=Decimal("0"), cost_basis_partial=True)

        assert _strategy_pnl_usd(pnl, _zero_cost_stack(), Decimal("0")) == Decimal("0")


class TestTheReaderWireIsLive:
    """``compute_pnl_summary`` must actually LIFT the stamp onto PnLSummary.

    Mutation-driven: replacing the assignment with ``pnl.cost_basis_partial =
    False`` was caught by ZERO tests before this class existed. The writer
    stamped, the tile guarded, and the wire between them could be cut with the
    suite still green -- the VIB-5170 inert-feature class, one layer over.
    """

    @staticmethod
    def _snap(coverage, *, total="100", cash="0"):
        meta = {"gas_native_status": "ok"}
        if coverage is not None:
            meta["nav_basis_coverage"] = coverage
        return SimpleNamespace(
            total_value_usd=total,
            available_cash_usd=cash,
            value_confidence="HIGH",
            deployed_capital_usd="60",
            positions_json="[]",
            snapshot_metadata=meta,
            timestamp=datetime.now(tz=UTC),
        )

    def _summary(self, coverage):
        return compute_pnl_summary(
            portfolio_metrics=SimpleNamespace(
                deposits_usd="0", withdrawals_usd="0", initial_value_usd="100", timestamp=datetime.now(tz=UTC)
            ),
            snapshots=[self._snap(coverage)],
            ledger_entries=[],
            accounting_events=[],
        )

    def test_an_unbacked_leg_reaches_the_summary(self):
        assert self._summary({"legs_in_nav": 2, "legs_missing_basis": 1}).cost_basis_partial is True

    def test_a_fully_backed_snapshot_does_not_set_the_flag(self):
        assert self._summary({"legs_in_nav": 2, "legs_missing_basis": 0}).cost_basis_partial is False

    def test_an_unstamped_snapshot_does_not_set_the_flag(self):
        assert self._summary(None).cost_basis_partial is False


class TestReconstructedBasisIsNotPartialCoverage:
    """A basis rebuilt from accounting events is still a basis (VIB-6308).

    The writer's stamp measures the POSITION LEGS. When ``compute_pnl_summary``
    falls back to ``_open_position_cost_basis`` it supplies a cost side from a
    DIFFERENT source, and once it has, the reader is no longer differencing
    against nothing -- so suppressing here hides a number it can actually
    compute.

    This is the traderjoe-lp-avax shape from the 20260801 real-money batch: leg
    basis absent, accounting basis $2.48, tile had rendered -$0.07. The carry
    (pcs-aave-carry-bsc) keeps its leg basis, so this branch never fires for it
    and its +42% phantom stays suppressed. Both directions are pinned below,
    because a test that only proved the clearing would equally pass on code
    that cleared the flag unconditionally.
    """

    @staticmethod
    def _snap(*, deployed_capital_usd):
        return SimpleNamespace(
            total_value_usd="100",
            available_cash_usd="0",
            value_confidence="HIGH",
            deployed_capital_usd=deployed_capital_usd,
            positions_json="[]",
            snapshot_metadata={
                "gas_native_status": "ok",
                # The leg-level stamp says coverage IS partial in both cases.
                # Only the reconstruction distinguishes them.
                "nav_basis_coverage": {"legs_in_nav": 1, "legs_missing_basis": 1},
            },
            timestamp=datetime.now(tz=UTC),
        )

    @staticmethod
    def _supply_event(cost_basis_usd):
        return {
            "payload_json": json.dumps(
                {
                    "event_type": "SUPPLY",
                    "position_key": "lending:avalanche:traderjoe:0xabc:usdc",
                    "cost_basis_usd": cost_basis_usd,
                }
            )
        }

    def _summary(self, *, deployed_capital_usd, events):
        return compute_pnl_summary(
            portfolio_metrics=SimpleNamespace(
                deposits_usd="0", withdrawals_usd="0", initial_value_usd="100", timestamp=datetime.now(tz=UTC)
            ),
            snapshots=[self._snap(deployed_capital_usd=deployed_capital_usd)],
            ledger_entries=[],
            accounting_events=events,
        )

    def test_a_reconstructed_basis_clears_the_partial_flag(self):
        """traderjoe: snapshot basis 0, accounting basis 2.48 -> render."""
        pnl = self._summary(deployed_capital_usd="0", events=[self._supply_event("2.48")])

        assert pnl.deployed_capital_usd == Decimal("2.48"), "the fallback must have fired"
        assert pnl.cost_basis_partial is False

    def test_a_leg_derived_basis_keeps_the_partial_flag(self):
        """carry: snapshot basis already 3.82, so the fallback never fires and
        the phantom stays suppressed. This is the assertion that makes the test
        above discriminating rather than decorative."""
        pnl = self._summary(deployed_capital_usd="3.82", events=[self._supply_event("2.48")])

        assert pnl.deployed_capital_usd == Decimal("3.82"), "the fallback must NOT have fired"
        assert pnl.cost_basis_partial is True

    def test_a_failed_reconstruction_keeps_the_partial_flag(self):
        """No usable accounting basis either -> nothing was recovered, so the
        cost side really is missing and the tile must still suppress."""
        pnl = self._summary(deployed_capital_usd="0", events=[])

        assert pnl.cost_basis_partial is True
