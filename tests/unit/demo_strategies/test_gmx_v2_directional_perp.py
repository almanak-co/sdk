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
from unittest.mock import MagicMock, call, patch

import pytest

from almanak.connectors.gmx_v2 import market_catalog
from almanak.framework.data import MarketSnapshotError
from tests.unit.connectors.gmx_v2.market_fixtures import prime_catalog

_SEED_DIR = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies" / "gmx_v2_directional_perp"
_WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


@pytest.fixture(autouse=True)
def _verified_markets():
    """Prime the venue-verified market catalog (address-first migration).

    The seed probes the venue by its configured market ADDRESS and the probe
    prices each matched position's notional through the connector's process
    catalog (index symbol + decimals). tests/unit/demo_strategies has no
    catalog-clear conftest — clear on teardown so no verified row leaks.
    """
    prime_catalog()
    yield
    market_catalog.clear()


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
    from types import SimpleNamespace

    market = MagicMock()
    market.perp_market.return_value = SimpleNamespace(index_symbol="ETH")
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


class TestAddressFirstDataReads:
    def test_config_declares_compatible_indicator_cadence(self, gmx):
        _, strat = gmx
        assert strat._config["data_granularity"] == "4h"

    def test_tracked_tokens_include_wallet_collateral_not_perp_index(self, gmx):
        _, strat = gmx
        assert strat._get_tracked_tokens() == [_USDC_ARBITRUM]

    def test_entry_uses_collateral_address_and_index_symbol_for_market_data(self, gmx):
        module, strat = gmx
        market = _market("100", collateral_price="2500")

        assert strat._enter(market, module.LONG, Decimal("0")).intent_type.value == "PERP_OPEN"
        market.balance.assert_called_once_with(_USDC_ARBITRUM)
        assert market.price.call_args_list == [
            call("ETH"),
            call(_USDC_ARBITRUM),
        ]

    def test_synthetic_market_data_uses_verified_symbol_not_config_label(self, gmx):
        """Every signal/risk read follows verified XMR, even with stale ETH config."""
        from types import SimpleNamespace

        module, strat = gmx
        strat.base_token = "ETH"
        market = _market("100")
        market.perp_market.return_value = SimpleNamespace(index_symbol="XMR")
        market.ema.return_value.value = Decimal("1")

        strat.decide(market)
        assert [item.args[0] for item in market.ema.call_args_list] == ["XMR", "XMR"]

        strat._verified_index_symbol = None
        strat._position_side = module.LONG
        strat._entry_price = Decimal("400")
        market.price.reset_mock()
        strat._manage(market, side=module.LONG, signal=module.LONG, funding=None)
        market.price.assert_called_once_with("XMR")

        strat._verified_index_symbol = None
        strat._position_side = None
        strat.force_action = "open_long"
        market.price.reset_mock()
        strat._forced_intent(market)
        assert market.price.call_args_list[0] == call("XMR")

    def test_verified_symbol_is_cached_but_failures_are_not(self, gmx):
        from types import SimpleNamespace

        _, strat = gmx
        market = MagicMock()
        market.perp_market.side_effect = [
            MarketSnapshotError("unavailable"),
            SimpleNamespace(index_symbol="XMR"),
        ]

        assert strat._index_symbol(market) == strat.base_token_address
        assert strat._index_symbol(market) == "XMR"
        assert strat._index_symbol(market) == "XMR"
        assert market.perp_market.call_count == 2

    @pytest.mark.parametrize("key", ["market_address", "base_token_address", "collateral_token_address"])
    @pytest.mark.parametrize("value", ["0x1", "0x" + "z" * 40])
    def test_constructor_rejects_malformed_addresses(self, key, value):
        module = _load_module()
        cls = module.GmxV2DirectionalPerp
        cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
        cfg[key] = value
        with (
            patch(
                "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
                return_value=None,
            ),
            pytest.raises(ValueError, match=key),
        ):
            strategy = cls.__new__(cls)
            strategy.get_config = lambda config_key, default=None: cfg.get(config_key, default)
            cls.__init__(strategy)


class TestMarketIdentity:
    """ALM-3094: one public market identifier drives funding and execution."""

    def test_demo_has_no_dual_funding_market_workaround(self, gmx):
        _, strat = gmx
        assert strat.market == "ETH/USD"
        assert "funding_market" not in strat._config
        assert not hasattr(strat, "funding_market")

    def test_funding_read_uses_the_execution_market(self, gmx):
        """Funding is read by the market ADDRESS — the execution market's
        unambiguous spelling. GMX funding factors are per-market, so a pair
        label with several collateral variants falls back to the default rate
        (PR #3648); the declared address never does."""
        _, strat = gmx
        snapshot = MagicMock()
        snapshot.funding_rate.return_value.rate_hourly = "0.000012"

        assert strat._funding_hourly(snapshot) == Decimal("0.000012")
        snapshot.funding_rate.assert_called_once_with("gmx_v2", "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336")


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


class TestForcedOpenLatch:
    """VIB-5513: ``force_action="open_long"/"open_short"`` opens ONCE, then holds.

    Pre-fix, the forced branch never consulted state: a continuous runner
    re-attempted the open every iteration, and once collateral was spent every
    retry reverted ``ERC20: transfer amount exceeds balance``.

    Both directions are pinned:

    * LIVENESS — the latch must still OPEN on the first iteration, including
      when the venue probe is UNMEASURED (a flaky read is a production shape;
      an open gate that refuses on UNMEASURED refuses forever).
    * LATCH — after one confirmed open, every subsequent iteration HOLDs.
    """

    ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
    USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def _snapshot(self, *, positions=(), ok=True, price="3000"):
        from almanak.connectors._strategy_base.perps_read_base import PerpsReadResult

        snapshot = MagicMock()
        snapshot.perp_positions.return_value = PerpsReadResult(positions=tuple(positions), ok=ok)
        snapshot.perp_market.return_value.index_symbol = "ETH"
        snapshot.price.return_value = Decimal(price)
        return snapshot

    def _venue_position(self, *, is_long=True):
        from almanak.connectors._strategy_base.perps_read_base import PerpsPositionOnChain

        return PerpsPositionOnChain(
            account="0x" + "1" * 40,
            market=self.ETH_USD_MARKET,
            collateral_token=self.USDC_ARBITRUM,
            size_in_usd=100 * 10**30,
            size_in_tokens=10**17,
            collateral_amount=50 * 10**6,
            is_long=is_long,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )

    @staticmethod
    def _confirm_open(strat, *, is_long=True):
        """Drive the production commit path: create-order tx confirmed."""
        intent = MagicMock()
        intent.intent_type.value = "PERP_OPEN"
        intent.is_long = is_long
        strat.on_intent_executed(intent, True, None)

    # ------------------------------------------------------------- liveness

    def test_first_iteration_opens_on_measured_flat_venue(self, gmx):
        _, strat = gmx
        strat.force_action = "open_long"
        intent = strat.decide(self._snapshot())
        assert intent.intent_type.value == "PERP_OPEN"
        assert intent.is_long is True

    def test_first_iteration_opens_short(self, gmx):
        _, strat = gmx
        strat.force_action = "open_short"
        intent = strat.decide(self._snapshot())
        assert intent.intent_type.value == "PERP_OPEN"
        assert intent.is_long is False

    def test_first_iteration_opens_when_probe_is_unmeasured(self, gmx):
        """An unreadable venue (ok=False -> UNMEASURED) must NOT brick the open."""
        _, strat = gmx
        strat.force_action = "open_long"
        intent = strat.decide(self._snapshot(ok=False))
        assert intent.intent_type.value == "PERP_OPEN"

    def test_failed_submission_does_not_latch(self, gmx):
        """A failed create-order tx leaves the latch clear: the open is retried."""
        _, strat = gmx
        strat.force_action = "open_long"
        assert strat.decide(self._snapshot()).intent_type.value == "PERP_OPEN"

        intent = MagicMock()
        intent.intent_type.value = "PERP_OPEN"
        intent.is_long = True
        strat.on_intent_executed(intent, False, None)

        assert strat.decide(self._snapshot()).intent_type.value == "PERP_OPEN"

    # ---------------------------------------------------------------- latch

    def test_latch_holds_after_confirmed_open(self, gmx):
        """NEGATIVE CONTROL for the fix: pre-fix this re-emits PERP_OPEN forever."""
        _, strat = gmx
        strat.force_action = "open_long"

        first = strat.decide(self._snapshot())
        assert first.intent_type.value == "PERP_OPEN"
        self._confirm_open(strat)

        for _ in range(5):
            held = strat.decide(self._snapshot())
            assert held.intent_type.value == "HOLD"
            assert "already executed" in held.reason

    def test_exactly_one_open_across_continuous_iterations(self, gmx):
        """The revert-loop shape in miniature: N iterations, exactly 1 open."""
        _, strat = gmx
        strat.force_action = "open_long"
        opens = 0
        for _ in range(10):
            intent = strat.decide(self._snapshot())
            if intent.intent_type.value == "PERP_OPEN":
                opens += 1
                self._confirm_open(strat)
        assert opens == 1

    def test_venue_open_position_holds_even_when_cache_is_wiped(self, gmx):
        """Belt: positive venue evidence latches when the state DB was lost."""
        _, strat = gmx
        strat.force_action = "open_long"
        strat._position_side = None
        intent = strat.decide(self._snapshot(positions=[self._venue_position()]))
        assert intent.intent_type.value == "HOLD"
        assert "venue already holds" in intent.reason

    def test_latch_survives_restart_via_persisted_state(self, gmx):
        _, strat = gmx
        strat.force_action = "open_long"
        assert strat.decide(self._snapshot()).intent_type.value == "PERP_OPEN"
        self._confirm_open(strat)
        persisted = strat.get_persistent_state()

        strat._position_side = None  # simulate a fresh process pre-restore
        strat._entry_price = None
        strat.load_persistent_state(persisted)

        held = strat.decide(self._snapshot())
        assert held.intent_type.value == "HOLD"
        assert "already executed" in held.reason


class TestTeardownReadsTheVenueNotTheCache:
    """ALM-3109 / VIB-6159 / VIB-6497.

    ``_position_side`` is written on submission success; the GMX keeper fill is
    asynchronous and can revert or be cancelled afterwards. Teardown enumerates
    from ``get_open_positions()``, so these three transitions are the difference
    between closing the user's position and losing access to it.
    """

    ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
    USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def _venue(self, strat, *, positions, ok=True, price="3000", index_symbol="ETH"):
        from almanak.connectors._strategy_base.perps_read_base import PerpsReadResult

        snapshot = MagicMock()
        snapshot.perp_positions.return_value = PerpsReadResult(positions=tuple(positions), ok=ok)
        snapshot.perp_market.return_value.index_symbol = index_symbol
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
        snapshot = self._venue(strat, positions=[self._position()], price="3000")

        row = strat.get_open_positions().positions[0]
        assert strat._verified_index_symbol == "ETH"
        snapshot.perp_market.assert_called_once_with("gmx_v2", self.ETH_USD_MARKET)
        snapshot.price.assert_called_once_with("ETH", chain="arbitrum")
        assert row.value_usd == Decimal("300.0")  # 0.1 ETH @ $3000
        assert "value_usd_unknown" not in row.details

    def test_synthetic_index_is_priced_by_verified_symbol(self, gmx):
        _, strat = gmx
        strat.base_token_address = "0x" + "13" * 20
        snapshot = self._venue(strat, positions=[self._position()], index_symbol="XMR")

        row = strat.get_open_positions().positions[0]

        assert row.value_usd == Decimal("300.0")
        snapshot.price.assert_called_once_with("XMR", chain="arbitrum")

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


class TestAwaitingFillLatch:
    """A submitted open/close latches decide() until its verdict arrives.

    The 92-day demo backtest (2-tick execution delay) decided OPEN twice
    before the first fill landed: doubled $200 exposure, and the second
    fill's commit wiped the entry-price reference, freezing the strategy at
    "awaiting entry price" for 2,186 ticks. Live, a keeper window spanning an
    iteration reproduces the same stack.
    """

    @staticmethod
    def _signal_market(strat, *, signal="long", balance_usd="100"):
        market = _market(balance_usd)
        fast, slow = (Decimal("110"), Decimal("100")) if signal == "long" else (Decimal("100"), Decimal("110"))

        def _ema(token, period=12, **_kw):
            data = MagicMock()
            data.value = fast if period == strat.ema_fast_period else slow
            return data

        market.ema.side_effect = _ema
        market.funding_rate.return_value.rate_hourly = "0"
        return market

    def test_open_is_latched_until_verdict(self, gmx):
        _, strat = gmx
        market = self._signal_market(strat)

        first = strat.decide(market)
        assert first.intent_type.value == "PERP_OPEN"

        second = strat.decide(market)
        assert second.intent_type.value == "HOLD"
        assert "awaiting confirmation" in second.reason

    def test_failed_verdict_unlatches_and_reopens(self, gmx):
        _, strat = gmx
        market = self._signal_market(strat)

        first = strat.decide(market)
        strat.on_intent_executed(first, False, None)

        retry = strat.decide(market)
        assert retry.intent_type.value == "PERP_OPEN"

    def test_close_is_latched_until_verdict(self, gmx):
        module, strat = gmx
        strat._position_side = module.LONG
        strat._entry_price = Decimal("2500")
        market = self._signal_market(strat, signal="short")

        close = strat.decide(market)
        assert close.intent_type.value == "PERP_CLOSE"

        held = strat.decide(market)
        assert held.intent_type.value == "HOLD"
        assert "awaiting confirmation" in held.reason

    def test_duplicate_fill_does_not_wipe_entry_price(self, gmx):
        _, strat = gmx
        market = self._signal_market(strat)

        first = strat.decide(market)
        strat.on_intent_executed(first, True, None)  # entry from _pending_entry_price
        assert strat._entry_price is not None
        entry = strat._entry_price

        # A duplicate fill whose result carries no price (and no pending
        # fallback remains) must not blind the stop-loss reference.
        strat.on_intent_executed(first, True, None)
        assert strat._entry_price == entry

    def test_forced_open_is_latched_under_delayed_execution(self, gmx):
        # Review P1: the forced branch returned before the gate and never
        # armed the latch, so two consecutive forced decides with no verdict
        # both emitted PERP_OPEN — the same doubled exposure the signal path
        # fix removed. The gate now sits ahead of EVERY submitting branch.
        from almanak.connectors._strategy_base.perps_read_base import PerpsReadResult

        _, strat = gmx
        strat.force_action = "open_long"
        snapshot = MagicMock()
        snapshot.perp_positions.return_value = PerpsReadResult(positions=(), ok=True)
        snapshot.price.return_value = Decimal("3000")

        assert strat.decide(snapshot).intent_type.value == "PERP_OPEN"

        held = strat.decide(snapshot)
        assert held.intent_type.value == "HOLD"
        assert "awaiting confirmation" in held.reason

    def test_backtest_trade_record_price_is_the_entry_reference(self, gmx):
        _, strat = gmx
        market = self._signal_market(strat)

        intent = strat.decide(market)
        result = MagicMock(spec=[])  # no entry_price / extracted_data attributes
        result.trade_record = MagicMock()
        result.trade_record.executed_price = Decimal("2294.78")
        strat.on_intent_executed(intent, True, result)

        # The slippage-adjusted fill outranks the decide-time fallback price.
        assert strat._entry_price == Decimal("2294.78")
