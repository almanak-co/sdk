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
    attribute_lp_strategy,
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
        # funding_pnl_usd is None when no funding_fee_usd in attribution_json
        assert result["funding_pnl_usd"] is None
        # net = 500 + (-8.50) + 0 - 2.00 = 489.50
        assert result["net_pnl_usd"] == "489.50"


# --- VIB-3497: funding PnL attribution tests ---


class TestPerpFundingAttribution:
    """VIB-3497: funding_pnl_usd wired from close_event attribution_json."""

    def test_attribute_perp_with_funding_from_close_receipt(self):
        """CLOSE event with funding_fee_usd in attribution_json — funding_pnl_usd
        is populated and deducted from net_pnl_usd.

        Simulates the case where the receipt parser has extracted a funding cost
        and _apply_perp has stamped it into attribution_json. The attributor must:
        - Set funding_pnl_usd = -funding_fee_usd (cost to position holder)
        - Include it in net_pnl_usd
        """
        open_evt = {
            "entry_price": "2000",
            "is_long": True,
            "gas_usd": "1.00",
        }
        # attribution_json carries funding_fee_usd = 12.50 (12.50 USD paid in funding)
        close_evt = {
            "mark_price": "2200",
            "unrealized_pnl": "500",
            "gas_usd": "1.00",
            "attribution_json": json.dumps({"funding_fee_usd": "12.50"}),
        }
        result = attribute_perp(open_evt, close_evt)

        assert result["funding_pnl_usd"] == "-12.50", (
            "funding_pnl_usd must be -funding_fee_usd (cost is negative)"
        )
        # net = 500 + 0 (fee unknown) + (-12.50) - 2.00 = 485.50
        assert result["net_pnl_usd"] == "485.50", (
            "net_pnl_usd must include funding cost"
        )
        assert result["price_pnl_usd"] == "500"

    def test_attribute_perp_funding_none_when_unavailable(self):
        """No funding_fee_usd in attribution_json → funding_pnl_usd = None (not 0).

        None means «parser did not extract funding» — distinct from Decimal('0')
        which would mean «verified zero funding». Dashboards must surface this
        as unknown rather than mis-reporting a zero cost.
        """
        open_evt = {
            "entry_price": "2000",
            "is_long": True,
            "gas_usd": "1.00",
        }
        close_evt = {
            "mark_price": "2200",
            "unrealized_pnl": "500",
            "gas_usd": "1.00",
            # No attribution_json at all — parser did not extract funding
        }
        result = attribute_perp(open_evt, close_evt)

        assert result["funding_pnl_usd"] is None, (
            "Missing funding data must propagate as None, not 0. "
            "None = unavailable; Decimal('0') = measured zero."
        )
        # net excludes the unknown funding term
        assert result["net_pnl_usd"] == "498.00"  # 500 - 2.00

    def test_attribute_perp_funding_zero_distinct_from_unknown(self):
        """Measured zero funding (e.g. short hold) yields funding_pnl_usd = '0'.

        When the parser emits Decimal('0') for funding_fee_usd (position held
        for less than one funding period), funding_pnl_usd must be '0' (str),
        not None. None vs '0' must not be conflated.
        """
        open_evt = {"entry_price": "2000", "is_long": True, "gas_usd": "0"}
        close_evt = {
            "mark_price": "2200",
            "unrealized_pnl": "100",
            "gas_usd": "0",
            "attribution_json": json.dumps({"funding_fee_usd": "0"}),
        }
        result = attribute_perp(open_evt, close_evt)

        assert result["funding_pnl_usd"] is not None, "Measured zero must not become None"
        assert result["funding_pnl_usd"] == "0", "Measured zero funding_pnl_usd = '0'"
        assert result["net_pnl_usd"] == "100"  # 100 + 0 + 0 - 0


class TestVIB3519FundingFeePreservation:
    """VIB-3519: funding_fee_usd raw value is persisted in the attribution dict
    so that recompute_attribution() cycles do not silently drop funding_pnl_usd.
    """

    def test_attribute_perp_includes_funding_fee_usd_in_output(self):
        """attribute_perp() must include funding_fee_usd in the returned dict."""
        open_evt = {"entry_price": "2000", "is_long": True, "gas_usd": "1.00"}
        close_evt = {
            "mark_price": "2200",
            "unrealized_pnl": "500",
            "gas_usd": "1.00",
            "attribution_json": json.dumps({"funding_fee_usd": "12.50"}),
        }
        result = attribute_perp(open_evt, close_evt)

        assert "funding_fee_usd" in result, (
            "VIB-3519: funding_fee_usd must be stored in the attribution dict "
            "so that _funding_fee_from_close() can read it back on recompute"
        )
        assert result["funding_fee_usd"] == "12.50", (
            "funding_fee_usd must be the raw cost value (not negated)"
        )
        # funding_pnl_usd is still the derived/signed field
        assert result["funding_pnl_usd"] == "-12.50"

    def test_attribute_perp_funding_fee_usd_none_when_unavailable(self):
        """When no funding_fee_usd in close attribution_json, both fields are None."""
        open_evt = {"entry_price": "2000", "is_long": True, "gas_usd": "0"}
        close_evt = {"mark_price": "2200", "unrealized_pnl": "100", "gas_usd": "0"}
        result = attribute_perp(open_evt, close_evt)

        assert result["funding_fee_usd"] is None
        assert result["funding_pnl_usd"] is None

    def test_recompute_preserves_funding_pnl_after_first_write(self, store):
        """VIB-3519 regression test: after the first attribution write, a
        subsequent recompute_attribution() must NOT lose funding_pnl_usd.

        Before the fix: the first write stored the computed attribution dict
        (which contained funding_pnl_usd but not funding_fee_usd). The second
        recompute called _funding_fee_from_close on the stored dict, found no
        funding_fee_usd key, and silently set funding_pnl_usd = None.

        After the fix: attribute_perp() persists funding_fee_usd in the dict,
        so _funding_fee_from_close can always recover it.
        """
        # Save OPEN event
        open_evt = PositionEvent(
            id="open-vib3519",
            deployment_id="strat:vib3519",
            position_id="pos-3519",
            position_type="PERP",
            event_type="OPEN",
            value_usd="10000",
            gas_usd="1.00",
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(open_evt))

        # Save CLOSE event with funding_fee_usd stamped in attribution_json
        close_evt = PositionEvent(
            id="close-vib3519",
            deployment_id="strat:vib3519",
            position_id="pos-3519",
            position_type="PERP",
            event_type="CLOSE",
            value_usd="10500",
            gas_usd="1.00",
            unrealized_pnl="500",
            attribution_json=json.dumps({"funding_fee_usd": "25.00"}),
        )
        asyncio.get_event_loop().run_until_complete(store.save_position_event(close_evt))

        # First attribution run
        first_attribution = asyncio.get_event_loop().run_until_complete(
            run_attribution_on_close(store, close_evt)
        )
        first_data = json.loads(first_attribution)
        assert first_data["funding_pnl_usd"] == "-25.00", (
            "First attribution must capture funding_pnl_usd"
        )
        assert first_data["funding_fee_usd"] == "25.00", (
            "VIB-3519: first attribution must persist funding_fee_usd raw value"
        )

        # Now simulate a recompute (version bump: set stored version to 0 so
        # recompute_attribution picks it up).
        stored_events = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:vib3519", position_id="pos-3519", event_type="CLOSE")
        )
        assert len(stored_events) == 1
        # Force attribution_version to 0 so recompute treats it as stale
        asyncio.get_event_loop().run_until_complete(
            store.update_position_attribution(stored_events[0]["id"], first_attribution, 0)
        )

        # Run batch recompute targeting CURRENT_VERSION
        count = asyncio.get_event_loop().run_until_complete(
            recompute_attribution(store, "strat:vib3519", version=CURRENT_VERSION)
        )
        assert count == 1, "recompute should have processed one CLOSE event"

        # Verify the recomputed attribution still carries funding_pnl_usd
        events_after = asyncio.get_event_loop().run_until_complete(
            store.get_position_events("strat:vib3519", position_id="pos-3519", event_type="CLOSE")
        )
        recomputed = json.loads(events_after[0]["attribution_json"])
        assert recomputed.get("funding_pnl_usd") == "-25.00", (
            "VIB-3519 regression: recompute_attribution() must not drop "
            "funding_pnl_usd. Before the fix this returned None because "
            "funding_fee_usd was not persisted in the attribution dict."
        )
        assert recomputed.get("funding_fee_usd") == "25.00", (
            "VIB-3519: recomputed dict must still carry the raw funding_fee_usd"
        )


class TestGMXv2FundingFeeExtraction:
    """VIB-3497: GMX V2 receipt parser funding fee extraction."""

    def test_gmx_v2_extract_funding_fee_usd_returns_none_pending_decoder(self):
        """GMX V2 extract_funding_fee_usd returns None until EventUtils decoder.

        The GMX V2 PositionFeesCollected event encodes fundingFeeAmount via the
        GMX EventUtils ABI library (dynamic key-value arrays). Implementing the
        full EventUtils decoder is prerequisite work. Until it lands, the method
        correctly returns None (unavailable) rather than 0 (measured zero).
        """
        from almanak.framework.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

        parser = GMXv2ReceiptParser()

        # Any receipt — including one with PositionDecrease logs — should
        # return None from the current stub implementation.
        receipt_with_decrease = {
            "transactionHash": "0xabc",
            "blockNumber": 12345678,
            "logs": [
                {
                    "topics": [
                        "0x07d51b51b408d7c62dcc47cc558da5ce6a6e0fd129a427ebce150f52b0e5171a",
                    ],
                    "data": "0x" + "00" * 512,
                    "address": "0x" + "ab" * 20,
                    "logIndex": 0,
                }
            ],
        }
        result = parser.extract_funding_fee_usd(receipt_with_decrease)

        assert result is None, (
            "extract_funding_fee_usd must return None (not 0) when EventUtils "
            "decoder is not yet implemented. None = unavailable."
        )

    def test_gmx_v2_extract_funding_fee_usd_result_returns_extract_missing(self):
        """extract_funding_fee_usd_result returns ExtractMissing (not ExtractError).

        The fail-closed variant must propagate the stub's None as ExtractMissing
        (benign «data not in receipt») rather than ExtractError (parse failure).
        ExtractMissing allows the enricher to continue without raising
        CriticalAccountingError in live mode.
        """
        from almanak.framework.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser
        from almanak.framework.execution.extract_result import ExtractMissing

        parser = GMXv2ReceiptParser()
        receipt = {"transactionHash": "0xdef", "blockNumber": 1, "logs": []}

        result = parser.extract_funding_fee_usd_result(receipt)

        assert isinstance(result, ExtractMissing), (
            "A None return from the stub must surface as ExtractMissing so the "
            "enricher treats it as benign missing data, not an accounting error."
        )


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


# ---------------------------------------------------------------------------
# VIB-3493 — strategy-level LP attribution (continuous-strategy-level model)
# ---------------------------------------------------------------------------


def _lp_event(
    *,
    event_id: int,
    timestamp: str,
    event_type: str,
    gas_usd: str,
    position_id: str = "1",
    position_type: str = "LP",
) -> dict:
    """Compact factory for strategy-level LP attribution tests."""
    return {
        "id": event_id,
        "timestamp": timestamp,
        "event_type": event_type,
        "position_type": position_type,
        "position_id": position_id,
        "gas_usd": gas_usd,
    }


class TestAttributeLPStrategy:
    """``attribute_lp_strategy`` aggregates LP gas across every rebalance.

    Per-lifecycle ``attribute_lp`` answers "what did THIS position cost".
    The strategy-level helper is the missing piece that makes
    multi-rebalance strategies stop looking artificially cheap.
    """

    def test_empty_input_returns_zero_totals(self):
        result = attribute_lp_strategy([])
        assert result["model"] == "continuous_strategy_level"
        assert result["total_gas_usd"] == "0"
        assert result["open_count"] == 0
        assert result["close_count"] == 0
        assert result["close_open_pairs"] == 0
        assert result["unique_position_ids"] == 0

    def test_non_lp_events_filtered_out(self):
        events = [
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="OPEN", gas_usd="5", position_type="PERP"),
            _lp_event(event_id=2, timestamp="2026-05-01T00:00:01", event_type="CLOSE", gas_usd="3", position_type="LENDING"),
        ]
        result = attribute_lp_strategy(events)
        assert result["total_gas_usd"] == "0"
        assert result["open_count"] == 0
        assert result["close_count"] == 0

    def test_single_lifecycle_attributes_open_and_close(self):
        events = [
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="OPEN", gas_usd="5"),
            _lp_event(event_id=2, timestamp="2026-05-01T00:01:00", event_type="CLOSE", gas_usd="3"),
        ]
        result = attribute_lp_strategy(events)
        assert result["total_gas_usd"] == "8"
        assert result["open_gas_usd"] == "5"
        assert result["close_gas_usd"] == "3"
        assert result["open_count"] == 1
        assert result["close_count"] == 1
        assert result["close_open_pairs"] == 0
        assert result["unique_position_ids"] == 1

    def test_two_rebalances_counts_two_pairs_and_full_gas(self):
        """Strategy: OPEN_A, CLOSE_A, OPEN_B, CLOSE_B, OPEN_C — counts 2 rebalances."""
        events = [
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="OPEN", gas_usd="5", position_id="A"),
            _lp_event(event_id=2, timestamp="2026-05-01T00:01:00", event_type="CLOSE", gas_usd="3", position_id="A"),
            _lp_event(event_id=3, timestamp="2026-05-01T00:01:01", event_type="OPEN", gas_usd="5", position_id="B"),
            _lp_event(event_id=4, timestamp="2026-05-01T00:02:00", event_type="CLOSE", gas_usd="3", position_id="B"),
            _lp_event(event_id=5, timestamp="2026-05-01T00:02:01", event_type="OPEN", gas_usd="5", position_id="C"),
        ]
        result = attribute_lp_strategy(events)
        # CLOSE_A → OPEN_B is one rebalance; CLOSE_B → OPEN_C is another. Two cycles.
        assert result["close_open_pairs"] == 2
        # 3 opens + 2 closes = 5 events @ (5,3,5,3,5) = 21
        assert result["total_gas_usd"] == "21"
        assert result["open_gas_usd"] == "15"
        assert result["close_gas_usd"] == "6"
        assert result["open_count"] == 3
        assert result["close_count"] == 2
        assert result["unique_position_ids"] == 3

    def test_non_lifecycle_events_count_in_total_but_dont_shift_rebalance_state(self):
        """COLLECT_FEES / SNAPSHOT contribute gas but don't break/start rebalance pairs."""
        events = [
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="OPEN", gas_usd="5"),
            # Mid-position fee collection: gas counted but doesn't affect lifecycle pairing
            _lp_event(event_id=2, timestamp="2026-05-01T00:00:30", event_type="COLLECT_FEES", gas_usd="2"),
            _lp_event(event_id=3, timestamp="2026-05-01T00:01:00", event_type="CLOSE", gas_usd="3"),
            _lp_event(event_id=4, timestamp="2026-05-01T00:01:01", event_type="OPEN", gas_usd="5"),
        ]
        result = attribute_lp_strategy(events)
        assert result["total_gas_usd"] == "15"  # 5 + 2 + 3 + 5
        assert result["close_open_pairs"] == 1  # CLOSE -> OPEN counts despite COLLECT_FEES in the middle of an earlier cycle
        assert result["open_count"] == 2
        assert result["close_count"] == 1

    def test_unsorted_input_sorted_internally(self):
        """The helper sorts by (timestamp, id) so ordering is deterministic."""
        events = [
            _lp_event(event_id=4, timestamp="2026-05-01T00:02:00", event_type="CLOSE", gas_usd="3", position_id="B"),
            _lp_event(event_id=2, timestamp="2026-05-01T00:01:00", event_type="CLOSE", gas_usd="3", position_id="A"),
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="OPEN", gas_usd="5", position_id="A"),
            _lp_event(event_id=3, timestamp="2026-05-01T00:01:01", event_type="OPEN", gas_usd="5", position_id="B"),
        ]
        result = attribute_lp_strategy(events)
        assert result["close_open_pairs"] == 1  # CLOSE_A -> OPEN_B

    def test_missing_gas_usd_treated_as_zero(self):
        """Legacy rows with empty/null gas_usd don't crash and don't inflate totals."""
        events = [
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="OPEN", gas_usd=""),
            _lp_event(event_id=2, timestamp="2026-05-01T00:01:00", event_type="CLOSE", gas_usd="3"),
        ]
        result = attribute_lp_strategy(events)
        assert result["total_gas_usd"] == "3"
        assert result["open_gas_usd"] == "0"
        assert result["close_gas_usd"] == "3"

    def test_event_type_case_insensitive(self):
        """Some legacy rows used lowercase event types — normalise."""
        events = [
            _lp_event(event_id=1, timestamp="2026-05-01T00:00:00", event_type="open", gas_usd="5"),
            _lp_event(event_id=2, timestamp="2026-05-01T00:01:00", event_type="close", gas_usd="3"),
        ]
        result = attribute_lp_strategy(events)
        assert result["open_count"] == 1
        assert result["close_count"] == 1
        assert result["total_gas_usd"] == "8"
