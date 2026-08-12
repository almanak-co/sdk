"""Full-close regression tests for the gmx_perp_lifecycle demo.

VIB-5950 / ALM-2976 regression pin: both close paths (the iteration-lane
``_create_close_intent`` and the teardown-lane ``generate_teardown_intents``)
must emit ``size_usd=None`` so the GMX compiler live-reads the on-chain
position size at compile time. Passing a cached notional (``_position_size_usd``)
strands residual dust whenever the position has drifted from the remembered
size — exactly the customer shape reported in ALM-2976.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.perps_read_base import (
    PerpsPositionOnChain,
    PerpsReadResult,
)
from almanak.connectors.gmx_v2 import market_catalog
from tests.unit.connectors.gmx_v2.market_fixtures import prime_catalog

_SEED_DIR = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies" / "gmx_perp_lifecycle"
_ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
_WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


@pytest.fixture(autouse=True)
def _verified_markets():
    prime_catalog()
    yield
    market_catalog.clear()


def _load_module():
    spec = importlib.util.spec_from_file_location("gmx_lifecycle_seed", _SEED_DIR / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def strat():
    module = _load_module()
    cls = module.GMXPerpLifecycleStrategy
    cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = cls.__new__(cls)
        s._config = cfg
        s.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(s)
    s._chain = "arbitrum"
    s._deployment_id = "deployment:gmx_perp_lifecycle_test"
    return s


def _venue(strat, *, positions, ok=True, truncated=False, price="3000"):
    snapshot = MagicMock()
    snapshot.perp_positions.return_value = PerpsReadResult(
        positions=tuple(positions),
        ok=ok,
        truncated=truncated,
    )
    snapshot.price.return_value = Decimal(price)
    strat.create_market_snapshot = lambda: snapshot
    return snapshot


def _raw_position(*, is_long=True):
    return PerpsPositionOnChain(
        account="0x" + "1" * 40,
        market=_ETH_USD_MARKET,
        collateral_token=_USDC_ARBITRUM,
        size_in_usd=100 * 10**30,
        size_in_tokens=10**17,
        collateral_amount=50 * 10**6,
        is_long=is_long,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )


class TestFullCloseSemantics:
    def test_iteration_close_emits_size_none_even_with_cached_size(self, strat):
        # Simulate a tracked (cached) notional that has drifted from on-chain state.
        strat._position_size_usd = Decimal("100")
        intent = strat._create_close_intent()
        assert intent.intent_type.value == "PERP_CLOSE"
        # The cached size must NEVER leak into the close intent.
        assert intent.size_usd is None

    def test_teardown_close_emits_size_none_even_with_cached_size(self, strat):
        from almanak.framework.teardown import TeardownMode

        strat._loop_state = "open"
        strat._position_size_usd = Decimal("100")
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "PERP_CLOSE"
        assert intents[0].size_usd is None


class TestAddressFirstDataReads:
    def test_tracked_tokens_are_chain_specific_addresses(self, strat):
        assert strat._get_tracked_tokens() == [_WETH_ARBITRUM, _USDC_ARBITRUM]

    def test_open_decision_prices_the_index_by_address(self, strat):
        snapshot = MagicMock()
        snapshot.price.return_value = Decimal("3000")
        snapshot.collateral_value_usd.return_value = Decimal("5")

        assert strat.decide(snapshot).intent_type.value == "PERP_OPEN"
        snapshot.price.assert_called_once_with(_WETH_ARBITRUM)
        snapshot.collateral_value_usd.assert_called_once_with(_USDC_ARBITRUM, Decimal("5"))

    @pytest.mark.parametrize("key", ["market_address", "index_token_address", "collateral_token_address"])
    @pytest.mark.parametrize("value", ["0x1", "0x" + "z" * 40])
    def test_constructor_rejects_malformed_addresses(self, key, value):
        module = _load_module()
        cls = module.GMXPerpLifecycleStrategy
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


class TestVenueDerivedTeardown:
    """ALM-3218: raw venue rows are normalized before strategy valuation."""

    def test_raw_position_the_cache_missed_is_reported_and_closed(self, strat):
        from almanak.framework.teardown import TeardownMode

        strat._loop_state = "idle"
        _venue(strat, positions=[_raw_position(is_long=False)])

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        assert row.value_usd == Decimal("300.0")
        assert row.details["position_source"] == "venue"
        assert row.details["is_long"] is False

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].is_long is False
        assert intents[0].collateral_token == _USDC_ARBITRUM
        assert intents[0].size_usd is None

    def test_measured_flat_venue_overrides_stale_cache(self, strat):
        from almanak.framework.teardown import TeardownMode

        strat._loop_state = "open"
        strat._position_size_usd = Decimal("100")
        _venue(strat, positions=[])

        assert strat.get_open_positions().positions == []
        assert strat.generate_teardown_intents(TeardownMode.SOFT) == []

    def test_open_venue_position_with_unavailable_price_keeps_venue_identity(self, strat):
        strat._loop_state = "open"
        strat._position_size_usd = Decimal("100")
        snapshot = _venue(strat, positions=[_raw_position()])
        snapshot.price.side_effect = RuntimeError("oracle unavailable")

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        assert row.value_usd == Decimal("100")
        assert row.details["position_source"] == "venue"
        assert row.details["value_usd_unknown"] is True
        assert row.details["valuation_status"] == "no_path"

    def test_unmeasured_venue_retains_marked_cache_fallback(self, strat):
        from almanak.framework.teardown import TeardownMode

        strat._loop_state = "open"
        strat._position_size_usd = Decimal("100")
        _venue(strat, positions=[], ok=False)

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        assert row.value_usd == Decimal("100")
        assert row.details["position_source"] == "strategy_cache_unverified"
        assert row.details["value_usd_unknown"] is True
        assert row.details["valuation_status"] == "no_path"
        assert len(strat.generate_teardown_intents(TeardownMode.SOFT)) == 1
