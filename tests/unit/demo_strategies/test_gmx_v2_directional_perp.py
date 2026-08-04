"""Safety-gate regression tests for the gmx_v2_directional_perp golden seed.

Locks in the two money-path properties a directional-perp seed must get right
(both were review findings on the seed's introduction):

1. The open is funded — the wallet balance must cover the ACTUAL required margin
   (notional / leverage), not just a static minimum.
2. Collateral is sized in COLLATERAL-TOKEN units (USD margin / price), so a
   non-stablecoin collateral does not deposit `price`x too many tokens.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_SEED_DIR = (
    Path(__file__).resolve().parents[3]
    / "almanak"
    / "demo_strategies"
    / "gmx_v2_directional_perp"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("gmx_seed", _SEED_DIR / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def gmx():
    module = _load_module()
    cls = module.GmxV2DirectionalPerp
    cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(strat)
    strat._position_side = None
    # ``IntentStrategy.__init__`` is patched out above, so the attributes it
    # normally sets have to be supplied. ``chain`` (a read-only property over
    # ``_chain``) is one the teardown path reads.
    strat._chain = "arbitrum"
    return module, strat


def _market(balance_usd: str, collateral_price: str = "2500"):
    market = MagicMock()
    bal = MagicMock()
    bal.balance_usd = Decimal(balance_usd)
    market.balance.return_value = bal
    market.price.return_value = Decimal(collateral_price)
    return market


class TestBalanceGate:
    """Defaults: position_size_usd=100, leverage=2 -> required margin = $50."""

    def test_holds_when_wallet_below_required_margin(self, gmx):
        module, strat = gmx
        # $25 clears the old $20 min_collateral_usd floor but cannot fund the $50 margin.
        intent = strat._enter(_market("25"), module.LONG, Decimal("0"))
        assert intent.intent_type.value == "HOLD"
        # Pin the exact failure mode: the margin gate, not some unrelated HOLD.
        assert "required margin" in intent.reason

    def test_opens_when_wallet_covers_required_margin(self, gmx):
        module, strat = gmx
        intent = strat._enter(_market("60"), module.LONG, Decimal("0"))
        assert intent.intent_type.value == "PERP_OPEN"


class TestCollateralUnits:
    def test_collateral_is_token_units_not_usd(self, gmx):
        module, strat = gmx
        # $50 margin / $2500 collateral price = 0.02 tokens (NOT 50).
        intent = strat._enter(_market("100", collateral_price="2500"), module.LONG, Decimal("0"))
        assert intent.intent_type.value == "PERP_OPEN"
        assert intent.collateral_amount == Decimal("0.02")


class TestMarketIdentity:
    """ALM-3094: one public market identifier drives funding and execution."""

    def test_demo_has_no_dual_funding_market_workaround(self, gmx):
        _, strat = gmx
        assert strat.market == "ETH/USD"
        assert "funding_market" not in strat._config
        assert not hasattr(strat, "funding_market")

    def test_funding_read_uses_the_execution_market(self, gmx):
        _, strat = gmx
        snapshot = MagicMock()
        snapshot.funding_rate.return_value.rate_hourly = "0.000012"

        assert strat._funding_hourly(snapshot) == Decimal("0.000012")
        snapshot.funding_rate.assert_called_once_with("gmx_v2", "ETH/USD")


class TestFullCloseSemantics:
    """VIB-5950 / ALM-2976 regression pin.

    Exit and teardown closes must emit ``size_usd=None`` so the compiler
    live-reads the on-chain position size. A cached config notional
    (``position_size_usd``) strands residual dust when the position drifts.
    """

    def test_exit_close_emits_size_none(self, gmx):
        module, strat = gmx
        intent = strat._close(module.LONG, reason="reverse")
        assert intent.intent_type.value == "PERP_CLOSE"
        # Must NOT carry the cached config notional.
        assert intent.size_usd is None

    def test_teardown_close_emits_size_none(self, gmx):
        from almanak.framework.teardown import TeardownMode

        module, strat = gmx
        strat._position_side = module.LONG
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "PERP_CLOSE"
        assert intents[0].size_usd is None


class TestTeardownReadsTheVenueNotTheCache:
    """ALM-3109 / VIB-6159 / VIB-6497.

    ``_position_side`` is written on submission success; the GMX keeper fill is
    asynchronous and can revert or be cancelled afterwards. Teardown enumerates
    from ``get_open_positions()``, so these three transitions are the difference
    between closing the user's position and losing access to it.
    """

    ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
    USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def _venue(self, strat, *, positions, ok=True, price="3000"):
        from almanak.connectors._strategy_base.perps_read_base import PerpsReadResult

        snapshot = MagicMock()
        snapshot.perp_positions.return_value = PerpsReadResult(positions=tuple(positions), ok=ok)
        snapshot.price.return_value = Decimal(price)
        strat.create_market_snapshot = lambda: snapshot
        return snapshot

    def _position(self, *, is_long=True):
        from almanak.connectors._strategy_base.perps_read_base import PerpsPositionOnChain

        return PerpsPositionOnChain(
            account="0x" + "1" * 40,
            market=self.ETH_USD_MARKET,
            collateral_token=self.USDC_ARBITRUM,
            size_in_usd=100 * 10**30,
            size_in_tokens=10**17,  # 0.1 ETH
            collateral_amount=50 * 10**6,
            is_long=is_long,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )

    def test_venue_position_the_cache_missed_is_reported_and_closed(self, gmx):
        """The ALM-3109 divergence: cache flat, venue short. Both halves must act."""
        from almanak.framework.teardown import TeardownMode

        _, strat = gmx
        strat._position_side = None  # the cache never learned about the fill
        self._venue(strat, positions=[self._position(is_long=False)])

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        assert row.details["side"] == "short"
        assert row.details["position_source"] == "venue"

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].is_long is False
        assert intents[0].size_usd is None

    def test_reported_value_is_a_real_notional_not_zero(self, gmx):
        """``value_usd=0`` is dropped as dust (<= $0.01) by the teardown harness."""
        _, strat = gmx
        strat._position_side = None
        self._venue(strat, positions=[self._position()], price="3000")

        row = strat.get_open_positions().positions[0]
        assert row.value_usd == Decimal("300.0")  # 0.1 ETH @ $3000
        assert "value_usd_unknown" not in row.details

    def test_measured_flat_venue_overrides_a_stale_open_cache(self, gmx):
        """The phantom residual in ALM-3109: cache open, venue measured flat."""
        from almanak.framework.teardown import TeardownMode

        module, strat = gmx
        strat._position_side = module.LONG
        self._venue(strat, positions=[])

        assert strat.get_open_positions().positions == []
        assert strat.generate_teardown_intents(TeardownMode.SOFT) == []

    def test_unavailable_read_is_not_reported_as_flat(self, gmx):
        """VIB-6497: an unmeasured read must keep the cached row, marked unverified."""
        from almanak.framework.teardown import TeardownMode

        module, strat = gmx
        strat._position_side = module.LONG
        self._venue(strat, positions=[], ok=False)

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        assert row.details["position_source"] == "strategy_cache_unverified"
        # Unmeasured, but NOT a fabricated $0 — both markers, and a size the dust
        # filter can see.
        assert row.details["value_usd_unknown"] is True
        assert row.details["valuation_status"] == "no_path"
        assert row.value_usd > Decimal("0.01")

        assert len(strat.generate_teardown_intents(TeardownMode.SOFT)) == 1
