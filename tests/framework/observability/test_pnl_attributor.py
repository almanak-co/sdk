"""Tests for PnL attribution (Phase 2, VIB-2776; v2 VIB-3205).

Validates:
- LP v2 attribution formula (principal, fee_pnl from protocol_fees, real IL,
  gas)
- Perp v2 attribution formula (price PnL, fee_pnl from protocol_fees,
  funding_pnl placeholder, gas)
- Failure tolerance (missing data, unknown position types, legacy rows
  without protocol_fees_usd / entry_state)
- run_attribution_on_close integration with SQLite
- stamp_entry_state_on_open integration with SQLite
- recompute_attribution batch utility
"""

import asyncio
from decimal import Decimal
import json

import pytest

from almanak.framework.observability.pnl_attributor import (
    CURRENT_VERSION,
    attribute_lp,
    attribute_perp,
    build_entry_state,
    compute_attribution,
    compute_impermanent_loss,
    recompute_attribution,
    run_attribution_on_close,
    stamp_entry_state_on_open,
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
        # VIB-3205: fee_pnl_usd is None when neither event carries
        # protocol_fees_usd (legacy row). net_pnl excludes unknown fees and
        # subtracts gas directly; funding_pnl is a declared follow-up.
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
        assert result["fee_pnl_usd"] is None  # unknown — neither event carried protocol_fees_usd
        assert result["funding_pnl_usd"] is None  # VIB-3205 follow-up
        # net = price_pnl + (fee_pnl or 0) - gas = 500 + 0 - 3.50 = 496.50
        assert result["net_pnl_usd"] == "496.50"

    def test_perp_short_loss(self):
        open_evt = {"entry_price": "2000", "is_long": False, "gas_usd": "2.00"}
        close_evt = {"mark_price": "2200", "unrealized_pnl": "-300", "gas_usd": "1.00"}
        result = attribute_perp(open_evt, close_evt)

        assert result["is_long"] is False
        assert result["price_pnl_usd"] == "-300"
        # net = -300 + 0 - 3 = -303
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

    def test_attribution_on_reopen_pairs_with_latest_open(self, store):
        """VIB-3205 audit fix (pr-auditor Blocker #2): when a position is
        reopened under the same ``position_id`` (OPEN-CLOSE-OPEN-CLOSE),
        the second CLOSE MUST pair with the second OPEN, not the first.

        Before the fix, ``run_attribution_on_close`` found the first OPEN
        in the (ASC-ordered) history and attributed every subsequent
        CLOSE against it — producing wrong ``principal_deposited`` /
        ``entry_state`` for reopened positions (GMX V2 perps, lending
        markets, any protocol reusing ``position_id``).
        """
        # Sequence: OPEN@5000 -> CLOSE@5500 -> OPEN@7000 -> CLOSE@6500.
        # First lifecycle made 500; second lifecycle lost 500.
        events = [
            ("o1", "OPEN", "5000"),
            ("c1", "CLOSE", "5500"),
            ("o2", "OPEN", "7000"),
            ("c2", "CLOSE", "6500"),
        ]
        saved: list[PositionEvent] = []
        for eid, etype, vusd in events:
            evt = PositionEvent(
                id=eid,
                deployment_id="strat:reopen",
                position_id="pos-reopen",
                position_type="LP",
                event_type=etype,
                value_usd=vusd,
                gas_usd="0.00",
            )
            asyncio.get_event_loop().run_until_complete(store.save_position_event(evt))
            saved.append(evt)

        # Attribute the SECOND close (c2). With the fix, it must pair with
        # the second open (o2 @ 7000), so net = 6500 - 7000 = -500.
        close2 = saved[3]
        attribution = asyncio.get_event_loop().run_until_complete(
            run_attribution_on_close(store, close2)
        )

        data = json.loads(attribution)
        assert data["principal_deposited_usd"] == "7000", (
            "Second CLOSE must pair with SECOND OPEN (7000), not the first (5000). "
            "Before the audit fix this returned '5000' — the classic reopen bug."
        )
        assert data["principal_recovered_usd"] == "6500"
        # net = 6500 - 7000 - 0 = -500 (stored as "-500.00" with gas-precision scale)
        assert Decimal(data["net_pnl_usd"]) == Decimal("-500")


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


# --- VIB-3205: real fee_pnl + IL tests ----------------------------------


def _entry_state_json(*, token0, token1, amount0, amount1, price0=None, price1=None):
    """Helper: serialise an attribution_json containing entry_state."""
    return json.dumps({"entry_state": build_entry_state(
        token0=token0,
        token1=token1,
        amount0=amount0,
        amount1=amount1,
        price0=price0,
        price1=price1,
    )})


def _close_attr_with_prices(prices):
    """Helper: serialise an attribution_json carrying current_prices."""
    return json.dumps({"current_prices": prices})


class TestFeePnlFromProtocolFees:
    """VIB-3205 Phase A: fee_pnl reads real protocol_fees_usd."""

    def test_fee_pnl_reads_from_protocol_fees_json(self):
        """LP fee_pnl_usd reflects -(open + close) protocol_fees_usd."""
        open_evt = {
            "value_usd": "10000",
            "gas_usd": "5.00",
            "protocol_fees_usd": "3.00",  # real DEX fee on the OPEN tx
        }
        close_evt = {
            "value_usd": "10500",
            "gas_usd": "4.00",
            "protocol_fees_usd": "2.50",  # fee on the CLOSE tx
        }
        result = attribute_lp(open_evt, close_evt)
        # fee_pnl_usd = -(3.00 + 2.50) = -5.50 (cost)
        assert result["fee_pnl_usd"] == "-5.50"
        # net = 10500 + (-5.50) - 10000 - 9 = 485.50
        assert result["net_pnl_usd"] == "485.50"

    def test_fee_pnl_none_when_protocol_fees_missing(self):
        """Legacy rows (no protocol_fees_usd) yield fee_pnl_usd=None."""
        open_evt = {"value_usd": "10000", "gas_usd": "1.00"}
        close_evt = {"value_usd": "10000", "gas_usd": "1.00"}
        result = attribute_lp(open_evt, close_evt)
        # Unknown must propagate as None — never 0, which would mask absence
        assert result["fee_pnl_usd"] is None
        # net still computable (without the unknown fee term)
        assert result["net_pnl_usd"] == "-2.00"

    def test_fee_pnl_zero_is_distinct_from_unknown(self):
        """Measured zero (e.g. Aave pool op) yields '0', not None."""
        open_evt = {"value_usd": "1000", "gas_usd": "0", "protocol_fees_usd": "0"}
        close_evt = {"value_usd": "1000", "gas_usd": "0", "protocol_fees_usd": "0"}
        result = attribute_lp(open_evt, close_evt)
        # Measured — must serialise as a string, not None
        assert result["fee_pnl_usd"] is not None
        assert result["fee_pnl_usd"] == "0"

    def test_fee_pnl_partial_known_returns_none(self):
        """VIB-3205 audit fix (pr-auditor Important #4): when ONE side is
        measured and the OTHER is unknown, we refuse to attribute — the
        non-missing side's signal is real but the unknown side's absence
        would silently under-attribute fees.

        Prior behaviour (``fee_pnl_usd == "-5"``) conflated unknown-on-close
        with measured-zero-on-close, contradicting the module docstring's
        "Unknown and measured zero must not be conflated" discipline.
        """
        open_evt = {"value_usd": "1000", "gas_usd": "0", "protocol_fees_usd": "5"}
        close_evt = {"value_usd": "1000", "gas_usd": "0"}  # no protocol_fees_usd
        result = attribute_lp(open_evt, close_evt)
        assert result["fee_pnl_usd"] is None

    def test_perps_fee_pnl_sums_perp_fee_usd(self):
        """Phase C: perp attribution uses real protocol_fees_usd."""
        open_evt = {
            "entry_price": "2000",
            "is_long": True,
            "gas_usd": "1.00",
            "protocol_fees_usd": "4.00",  # GMX-style open fee
        }
        close_evt = {
            "mark_price": "2200",
            "unrealized_pnl": "500",
            "gas_usd": "1.00",
            "protocol_fees_usd": "4.50",
        }
        result = attribute_perp(open_evt, close_evt)
        assert result["fee_pnl_usd"] == "-8.50"
        # funding_pnl_usd remains a declared follow-up placeholder
        assert result["funding_pnl_usd"] is None
        # net = 500 + (-8.50) - 2.00 = 489.50
        assert result["net_pnl_usd"] == "489.50"


class TestImpermanentLoss:
    """VIB-3205 Phase B: compute_impermanent_loss against entry vs current prices."""

    def test_compute_impermanent_loss_in_range(self):
        """Hand-crafted OPEN + close snapshot with price drift."""
        # Entry: 1 WETH @ 2000, 2000 USDC @ 1  => entry total = $4000
        # Close price: WETH=2400, USDC=1  => hodl = 1*2400 + 2000*1 = 4400
        # Suppose V_lp at close = 4350 (LP rebalanced as price rose)
        # IL = 4350 - 4400 = -50 (LP lost $50 vs HODL)
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
            "token0": "WETH",
            "token1": "USDC",
        }
        close_evt = {
            "value_usd": "4350",
            "attribution_json": _close_attr_with_prices({"WETH": "2400", "USDC": "1"}),
        }
        il = compute_impermanent_loss(open_evt, close_evt)
        assert il == __import__("decimal").Decimal("-50")

    def test_compute_impermanent_loss_no_drift_is_zero(self):
        """Same prices at open and close -> IL ≈ 0 (when V_lp matches hodl)."""
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="2", amount1="4000",
                price0="2000", price1="1",
            ),
            "token0": "WETH",
            "token1": "USDC",
        }
        # No drift: hodl = 2*2000 + 4000*1 = 8000. V_lp = 8000 => IL = 0
        close_evt = {
            "value_usd": "8000",
            "attribution_json": _close_attr_with_prices({"WETH": "2000", "USDC": "1"}),
        }
        il = compute_impermanent_loss(open_evt, close_evt)
        assert il == __import__("decimal").Decimal("0")

    def test_compute_impermanent_loss_missing_entry_state_returns_none(self):
        """Legacy OPEN without entry_state sidecar -> None, not a wrong 0."""
        open_evt = {
            "attribution_json": "{}",  # legacy row
            "token0": "WETH",
            "token1": "USDC",
        }
        close_evt = {
            "value_usd": "1000",
            "attribution_json": _close_attr_with_prices({"WETH": "2000", "USDC": "1"}),
        }
        assert compute_impermanent_loss(open_evt, close_evt) is None

    def test_compute_impermanent_loss_missing_current_prices_returns_none(self):
        """Close event lacks current_prices -> None."""
        open_evt = {
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
            "token0": "WETH",
            "token1": "USDC",
        }
        close_evt = {"value_usd": "1000"}  # no attribution_json
        assert compute_impermanent_loss(open_evt, close_evt) is None

    def test_attribute_lp_includes_il_when_entry_state_present(self):
        """Full attribute_lp flow emits impermanent_loss_usd from real math."""
        open_evt = {
            "value_usd": "4000",
            "gas_usd": "1.00",
            "protocol_fees_usd": "0",
            "attribution_json": _entry_state_json(
                token0="WETH", token1="USDC",
                amount0="1", amount1="2000",
                price0="2000", price1="1",
            ),
            "token0": "WETH",
            "token1": "USDC",
        }
        close_evt = {
            "value_usd": "4350",
            "gas_usd": "1.00",
            "protocol_fees_usd": "0",
            "attribution_json": _close_attr_with_prices({"WETH": "2400", "USDC": "1"}),
        }
        result = attribute_lp(open_evt, close_evt)
        # IL = 4350 - 4400 = -50
        assert result["impermanent_loss_usd"] == "-50"
        # price_pnl = hodl - principal = 4400 - 4000 = 400
        assert result["price_pnl_usd"] == "400"
        # net = 4350 + 0 - 4000 - 2 = 348  (IL is *reporting*, not subtracted again)
        assert result["net_pnl_usd"] == "348.00"

    def test_attribute_lp_il_none_for_legacy_open(self):
        """v1-era OPEN without entry_state yields impermanent_loss_usd=None."""
        open_evt = {"value_usd": "5000", "gas_usd": "1.00"}  # no attribution_json
        close_evt = {
            "value_usd": "5100",
            "gas_usd": "1.00",
            "attribution_json": _close_attr_with_prices({"WETH": "2000"}),
        }
        result = attribute_lp(open_evt, close_evt)
        # Real IL cannot be computed -> must be None, not 0
        assert result["impermanent_loss_usd"] is None


class TestStampEntryStateOnOpen:
    """End-to-end integration: stamp_entry_state_on_open writes to SQLite."""

    def test_stamp_entry_state_persists_amounts(self, store):
        open_event = PositionEvent(
            id="open-vib3205-1",
            deployment_id="strat:vib3205",
            position_id="42",
            position_type="LP",
            event_type="OPEN",
            value_usd="4000",
            token0="WETH",
            token1="USDC",
            amount0="1",
            amount1="2000",
            gas_usd="1.00",
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(open_event))

        # No portfolio snapshot yet -> prices will be None, but amounts still stamp
        asyncio.get_event_loop().run_until_complete(stamp_entry_state_on_open(store, open_event))

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:vib3205", position_id="42", event_type="OPEN")
        )
        assert len(events) == 1
        attr = json.loads(events[0]["attribution_json"])
        assert "entry_state" in attr
        es = attr["entry_state"]
        assert es["token0"] == "WETH"
        assert es["token1"] == "USDC"
        assert es["amount0"] == "1"
        assert es["amount1"] == "2000"
        # No snapshot available -> prices are None but the row is stamped
        assert es["price0"] is None
        assert es["price1"] is None


class TestProtocolFeesRoundTrip:
    """Verify protocol_fees_usd round-trips through SQLite persistence."""

    def test_protocol_fees_usd_persists(self, store):
        evt = PositionEvent(
            id="roundtrip-1",
            deployment_id="strat:roundtrip",
            position_id="7",
            position_type="LP",
            event_type="OPEN",
            value_usd="100",
            protocol_fees_usd="1.25",
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(evt))

        events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:roundtrip", position_id="7", event_type="OPEN")
        )
        assert len(events) == 1
        assert events[0]["protocol_fees_usd"] == "1.25"
