"""Tests for PnL attribution (Phase 2, VIB-2776).

Validates:
- LP v1 attribution formula (principal, fees, IL proxy, gas)
- Perp v1 attribution formula (price PnL, gas)
- Failure tolerance (missing data, unknown position types)
- run_attribution_on_close integration with SQLite
- recompute_attribution batch utility
"""

import asyncio
import json

import pytest

from almanak.framework.observability.pnl_attributor import (
    CURRENT_VERSION,
    attribute_lp,
    attribute_perp,
    compute_attribution,
    recompute_attribution,
    run_attribution_on_close,
)
from almanak.framework.observability.position_events import PositionEvent
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


# --- LP attribution tests ---


class TestAttributeLP:
    def test_basic_lp_attribution(self):
        open_evt = {
            "value_usd": "10000",
            "gas_usd": "5.00",
        }
        close_evt = {
            "value_usd": "10500",
            "amount0": "5.0",
            "amount1": "5000",
            "fees_token0": "0.01",
            "fees_token1": "10",
            "gas_usd": "3.00",
        }
        result = attribute_lp(open_evt, close_evt)

        assert result["version"] == CURRENT_VERSION
        assert result["position_type"] == "LP"
        assert result["principal_deposited_usd"] == "10000"
        assert result["principal_recovered_usd"] == "10500"
        assert result["fees_token0"] == "0.01"
        assert result["fees_token1"] == "10"
        assert result["gas_usd"] == "8.00"  # 5 + 3
        # net = 10500 + 0 - 10000 - 8 = 492
        assert result["net_pnl_usd"] == "492.00"

    def test_lp_loss_scenario(self):
        open_evt = {"value_usd": "10000", "gas_usd": "5.00"}
        close_evt = {"value_usd": "8000", "gas_usd": "3.00"}
        result = attribute_lp(open_evt, close_evt)

        # net = 8000 + 0 - 10000 - 8 = -2008
        assert result["net_pnl_usd"] == "-2008.00"

    def test_lp_zero_deposit(self):
        open_evt = {"value_usd": "0", "gas_usd": "0"}
        close_evt = {"value_usd": "100", "gas_usd": "0"}
        result = attribute_lp(open_evt, close_evt)
        # price_pnl should be 0 when principal_deposited is 0
        assert result["price_pnl_usd"] == "0"
        assert result["net_pnl_usd"] == "100"

    def test_lp_missing_values(self):
        result = attribute_lp({}, {})
        assert result["version"] == CURRENT_VERSION
        assert result["principal_deposited_usd"] == "0"
        assert result["net_pnl_usd"] == "0"


# --- Perp attribution tests ---


class TestAttributePerp:
    def test_basic_perp_long_profit(self):
        open_evt = {
            "entry_price": "2000",
            "leverage": "5",
            "is_long": True,
            "gas_usd": "2.00",
        }
        close_evt = {
            "mark_price": "2200",
            "unrealized_pnl": "500",
            "gas_usd": "1.50",
        }
        result = attribute_perp(open_evt, close_evt)

        assert result["version"] == CURRENT_VERSION
        assert result["position_type"] == "PERP"
        assert result["entry_price"] == "2000"
        assert result["exit_price"] == "2200"
        assert result["leverage"] == "5"
        assert result["is_long"] is True
        assert result["price_pnl_usd"] == "500"
        assert result["gas_usd"] == "3.50"
        assert result["fee_pnl_usd"] == "-3.50"
        # net = 500 + (-3.50) = 496.50
        assert result["net_pnl_usd"] == "496.50"

    def test_perp_short_loss(self):
        open_evt = {"entry_price": "2000", "is_long": False, "gas_usd": "2.00"}
        close_evt = {"mark_price": "2200", "unrealized_pnl": "-300", "gas_usd": "1.00"}
        result = attribute_perp(open_evt, close_evt)

        assert result["is_long"] is False
        assert result["price_pnl_usd"] == "-300"
        # net = -300 + (-3) = -303
        assert result["net_pnl_usd"] == "-303.00"

    def test_perp_missing_values(self):
        result = attribute_perp({}, {})
        assert result["version"] == CURRENT_VERSION
        assert result["net_pnl_usd"] == "0"


# --- compute_attribution dispatch tests ---


class TestComputeAttribution:
    def test_dispatches_to_lp(self):
        result = json.loads(
            compute_attribution(
                {"position_type": "LP", "value_usd": "1000", "gas_usd": "1"},
                {"position_type": "LP", "value_usd": "1100", "gas_usd": "1"},
            )
        )
        assert result["position_type"] == "LP"
        assert result["net_pnl_usd"] == "98"

    def test_dispatches_to_perp(self):
        result = json.loads(
            compute_attribution(
                {"position_type": "PERP", "entry_price": "100", "gas_usd": "0"},
                {"position_type": "PERP", "unrealized_pnl": "50", "gas_usd": "0"},
            )
        )
        assert result["position_type"] == "PERP"
        assert result["net_pnl_usd"] == "50"

    def test_unknown_type_returns_empty(self):
        assert compute_attribution({"position_type": "STAKE"}, {"position_type": "STAKE"}) == "{}"

    def test_empty_events_returns_valid_json(self):
        result = compute_attribution({}, {})
        assert result == "{}"


# --- SQLite integration tests ---


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test.db")
    config = SQLiteConfig(db_path=db_path)
    s = SQLiteStore(config)
    asyncio.get_event_loop().run_until_complete(s.initialize())
    yield s
    asyncio.get_event_loop().run_until_complete(s.close())


class TestRunAttributionOnClose:
    def test_attribution_on_close(self, store):
        # Save an OPEN event
        open_event = PositionEvent(
            id="open-1",
            deployment_id="strat:abc",
            position_id="12345",
            position_type="LP",
            event_type="OPEN",
            value_usd="10000",
            gas_usd="5.00",
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(open_event))

        # Save a CLOSE event
        close_event = PositionEvent(
            id="close-1",
            deployment_id="strat:abc",
            position_id="12345",
            position_type="LP",
            event_type="CLOSE",
            value_usd="10500",
            fees_token0="0.01",
            fees_token1="10",
            gas_usd="3.00",
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(close_event))

        # Run attribution
        attribution = asyncio.get_event_loop().run_until_complete(
            run_attribution_on_close(store, close_event)
        )

        assert attribution != "{}"
        data = json.loads(attribution)
        assert data["version"] == CURRENT_VERSION
        assert data["principal_deposited_usd"] == "10000"
        assert data["principal_recovered_usd"] == "10500"

        # Verify persisted in DB
        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:abc", position_id="12345", event_type="CLOSE")
        )
        assert len(events) == 1
        assert events[0]["attribution_version"] == CURRENT_VERSION
        saved_attr = json.loads(events[0]["attribution_json"])
        assert saved_attr["net_pnl_usd"] == "492.00"

    def test_attribution_no_open_event(self, store):
        close_event = PositionEvent(
            id="close-orphan",
            deployment_id="strat:abc",
            position_id="99999",
            position_type="LP",
            event_type="CLOSE",
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(close_event))

        attribution = asyncio.get_event_loop().run_until_complete(
            run_attribution_on_close(store, close_event)
        )
        assert attribution == "{}"


class TestRecomputeAttribution:
    def test_recompute_updates_version(self, store):
        # Save OPEN + CLOSE events
        for eid, etype, vusd in [("o1", "OPEN", "5000"), ("c1", "CLOSE", "5500")]:
            evt = PositionEvent(
                id=eid,
                deployment_id="strat:xyz",
                position_id="777",
                position_type="LP",
                event_type=etype,
                value_usd=vusd,
                gas_usd="1.00",
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(evt))

        # Recompute
        count = asyncio.get_event_loop().run_until_complete(
            recompute_attribution(store, "strat:xyz", version=CURRENT_VERSION)
        )
        assert count == 1

        # Verify updated
        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:xyz", position_id="777", event_type="CLOSE")
        )
        assert events[0]["attribution_version"] == CURRENT_VERSION
        attr = json.loads(events[0]["attribution_json"])
        assert attr["net_pnl_usd"] == "498.00"

    def test_recompute_skips_already_current(self, store):
        # Save with current version already set
        evt = PositionEvent(
            id="c-already",
            deployment_id="strat:skip",
            position_id="888",
            position_type="LP",
            event_type="CLOSE",
            attribution_version=CURRENT_VERSION,
            attribution_json='{"version": 1}',
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(evt))

        count = asyncio.get_event_loop().run_until_complete(
            recompute_attribution(store, "strat:skip", version=CURRENT_VERSION)
        )
        assert count == 0

    def test_recompute_empty_deployment(self, store):
        count = asyncio.get_event_loop().run_until_complete(
            recompute_attribution(store, "nonexistent", version=CURRENT_VERSION)
        )
        assert count == 0
