"""VIB-5057 — open swap-inventory lots are deployed capital, not idle cash.

Symptom: on any spot TA strategy the dashboard money trail permanently shows
"Available wallet cash ≈ 100% of wallet NAV" and "Open position NAV $0.00 /
Open cost basis $0.00" even mid-position, because the snapshot writer set
``available_cash_usd = wallet_value`` (the ENTIRE wallet token value) while
only protocol positions contributed to ``total_value_usd`` /
``deployed_capital_usd``. A swap strategy's deployed capital IS wallet-held
tokens, so it was classified as idle cash forever.

Post-fix: the open FIFO swap lots (``FIFOBasisStore.iter_open_swap_lots``,
``source == "SWAP"``) are valued as deployed inventory —

* ``available_cash_usd`` = wallet value − open-swap-lot inventory value,
* the inventory surfaces as visible ``PositionType.TOKEN`` rows that count
  into ``total_value_usd`` (open-position NAV),
* ``deployed_capital_usd`` gains the FIFO cost basis of the open lots,
* wallet NAV (``total_value_usd + available_cash_usd``) and
  ``wallet_total_value_usd`` are INVARIANT — only the split moves.

This file is the unit-test embodiment of the frozen UAT card
``docs/internal/uat-cards/VIB-5057.md`` (D1, D2, D3 scenarios).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.portfolio.models import PortfolioSnapshot
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

DEP = "dep-uat"
WALLET = "0x00000000000000000000000000000000000000aa"

PRICES = {"WBTC": Decimal("110000"), "USDC": Decimal("1"), "ETH": Decimal("2500")}
BALANCES = {"WBTC": Decimal("0.05"), "USDC": Decimal("200"), "ETH": Decimal("0.01")}
WALLET_VALUE = Decimal("0.05") * Decimal("110000") + Decimal("200") + Decimal("0.01") * Decimal("2500")  # 5725
INV_VALUE = Decimal("0.05") * Decimal("110000")  # 5500


# ---------------------------------------------------------------------------
# Harness (mirrors the UAT card's shared harness)
# ---------------------------------------------------------------------------


def make_strategy(tracked=("WBTC", "USDC"), positions=None) -> MagicMock:
    s = MagicMock()
    s.deployment_id = DEP
    s.chain = "arbitrum"
    s.wallet_address = WALLET
    s._get_tracked_tokens.return_value = list(tracked)
    s.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=DEP, timestamp=datetime.now(UTC), positions=positions or []
    )
    return s


def make_market(prices, balances) -> MagicMock:
    m = MagicMock()

    def _price(t: str, quote: str = "USD"):
        if t in prices:
            return prices[t]
        raise ValueError(f"no price for {t}")

    def _bal(t: str):
        if t in balances:
            r = MagicMock()
            r.balance = balances[t]
            return r
        raise ValueError(f"no balance for {t}")

    m.price = _price
    m.balance = _bal
    return m


def swap_event(
    token_out: str,
    amount_out: str,
    *,
    token_in: str = "USDC",
    amount_in: str = "0",
    amount_out_usd: str | None = "__SET__",
    ts: str = "2026-06-01T00:00:00+00:00",
) -> dict:
    payload = {
        "event_type": "SWAP",
        "swap_position_key": f"swap:arbitrum:{WALLET}",
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": token_out,
        "amount_out": amount_out,
    }
    if amount_out_usd is not None:
        payload["amount_out_usd"] = amount_in if amount_out_usd == "__SET__" else amount_out_usd
    return {
        "event_type": "SWAP",
        "deployment_id": DEP,
        "position_key": "",
        "chain": "arbitrum",
        "wallet_address": WALLET,
        "timestamp": ts,
        "payload_json": json.dumps(payload),
    }


def make_store(events) -> MagicMock:
    st = MagicMock()
    st.get_accounting_events_sync = lambda dep, position_key=None: list(events)
    return st


def snap_with(events, prices, balances, tracked=("WBTC", "USDC"), positions=None) -> PortfolioSnapshot:
    """Run the production snapshot writer. ``events=None`` = no accounting context."""
    v = PortfolioValuer()
    if events is not None:
        v.set_accounting_context(make_store(events), DEP)
    return v.value(make_strategy(tracked, positions), make_market(prices, balances))


def money(s: PortfolioSnapshot):
    """All four money fields + full serialized position rows (byte-level guard)."""
    return (
        s.total_value_usd,
        s.available_cash_usd,
        s.deployed_capital_usd,
        s.wallet_total_value_usd,
        [json.dumps(p.__dict__, sort_keys=True, default=str) for p in s.positions],
    )


def inventory_rows(s: PortfolioSnapshot):
    return [p for p in s.positions if p.position_type == PositionType.TOKEN]


BUY_WBTC = swap_event("WBTC", "0.05", amount_in="5000")


# ---------------------------------------------------------------------------
# D1 — the bug repro and its fix
# ---------------------------------------------------------------------------


class TestMidPositionClassification:
    """D1.S1 — mid-position TA strategy books inventory as deployed."""

    def test_inventory_is_deployed_not_cash(self):
        snap = snap_with([BUY_WBTC], PRICES, BALANCES)

        # The bug: pre-fix these read available_cash=5725 / total_value=0 /
        # deployed_capital=0 — 100% of NAV classified as idle cash mid-position.
        assert snap.available_cash_usd == WALLET_VALUE - INV_VALUE  # $225 idle
        assert snap.total_value_usd == INV_VALUE  # $5,500 open-position NAV
        assert snap.deployed_capital_usd == Decimal("5000")  # FIFO cost basis

    def test_nav_and_wallet_total_invariant(self):
        snap = snap_with([BUY_WBTC], PRICES, BALANCES)
        assert snap.total_value_usd + snap.available_cash_usd == WALLET_VALUE
        assert snap.wallet_total_value_usd == WALLET_VALUE

    def test_inventory_surfaces_as_visible_position_row(self):
        snap = snap_with([BUY_WBTC], PRICES, BALANCES)
        rows = [p for p in inventory_rows(snap) if p.value_usd == INV_VALUE]
        assert rows, [str(p) for p in snap.positions]
        row = rows[0]
        assert row.cost_basis_usd == Decimal("5000")
        assert row.unrealized_pnl_usd == INV_VALUE - Decimal("5000")
        assert row.details.get("asset", "").upper() == "WBTC"
        # Metadata stamp marks the classification as applied (drives the
        # gateway-side suppression of the legacy additive inventory term).
        meta = (snap.snapshot_metadata or {}).get("swap_inventory")
        assert isinstance(meta, dict) and meta.get("status") == "applied", meta

    def test_d1_s2_nav_invariance_is_structural(self):
        """D1.S2 — post-fix vs pre-fix-equivalent run: NAV equal, split moves."""
        snap_post = snap_with([BUY_WBTC], PRICES, BALANCES)
        snap_pre = snap_with(None, PRICES, BALANCES)

        nav_pre = snap_pre.total_value_usd + snap_pre.available_cash_usd
        nav_post = snap_post.total_value_usd + snap_post.available_cash_usd
        assert nav_pre == nav_post
        assert snap_pre.wallet_total_value_usd == snap_post.wallet_total_value_usd
        assert snap_pre.available_cash_usd - snap_post.available_cash_usd == INV_VALUE


# ---------------------------------------------------------------------------
# D2 — variance matrix
# ---------------------------------------------------------------------------


class TestNoLotsByteIdentical:
    """D2.M1 — zero swap lots ⇒ byte-identical to today (regression guard)."""

    def test_empty_event_store_identical_to_no_context(self):
        baseline = snap_with(None, PRICES, BALANCES)
        s = snap_with([], PRICES, BALANCES)
        assert money(s) == money(baseline)
        assert s.available_cash_usd == WALLET_VALUE
        assert "swap_inventory" not in (s.snapshot_metadata or {})

    def test_supply_only_event_stream_identical(self):
        supply_event = {
            "event_type": "SUPPLY",
            "deployment_id": DEP,
            "position_key": "supply:aave_v3:arbitrum:usdc",
            "chain": "arbitrum",
            "wallet_address": WALLET,
            "timestamp": "2026-06-01T00:00:00+00:00",
            "payload_json": json.dumps(
                {"event_type": "SUPPLY", "asset": "USDC", "amount_token": "100", "principal_usd": "100"}
            ),
        }
        baseline = snap_with(None, PRICES, BALANCES)
        s = snap_with([supply_event], PRICES, BALANCES)
        assert money(s) == money(baseline)
        assert "swap_inventory" not in (s.snapshot_metadata or {})


class TestMultiTokenInventory:
    """D2.M2 — held WBTC lot + residual USDC proceeds lot."""

    def test_both_lots_classified(self):
        # BUY 0.05 WBTC for $5,000, then SELL 0.003 WBTC for 300 USDC. FIFO:
        # the sell consumes 0.003 of the WBTC lot (remaining 0.047, basis
        # pro-rated to $4,700) and mints a 300-USDC proceeds lot at $300.
        events = [
            BUY_WBTC,
            swap_event(
                "USDC", "300", token_in="WBTC", amount_in="0.003", amount_out_usd="300", ts="2026-06-02T00:00:00+00:00"
            ),
        ]
        balances = dict(BALANCES)
        balances["WBTC"] = Decimal("0.047")
        balances["USDC"] = Decimal("500")
        wallet_value = Decimal("0.047") * Decimal("110000") + Decimal("500") + Decimal("0.01") * Decimal("2500")
        s = snap_with(events, PRICES, balances)
        inv_value = Decimal("0.047") * Decimal("110000") + Decimal("300")
        assert s.available_cash_usd == wallet_value - inv_value
        assert s.total_value_usd == inv_value
        assert s.deployed_capital_usd == Decimal("4700") + Decimal("300")
        baseline = snap_with(None, PRICES, balances)
        assert s.total_value_usd + s.available_cash_usd == baseline.total_value_usd + baseline.available_cash_usd
        assert len(inventory_rows(s)) == 2


class TestInventoryWithProtocolPosition:
    """D2.M3 — inventory coexists with a real protocol position."""

    def test_supply_position_and_inventory(self):
        supply = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-usdc-sup",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("1000"),
            details={},
        )
        s = snap_with([BUY_WBTC], PRICES, BALANCES, positions=[supply])
        assert s.total_value_usd == Decimal("1000") + INV_VALUE
        # Inventory is wallet-held — counted ONCE (in wallet value), so the
        # operator-facing wallet_total gains only the protocol position.
        assert s.wallet_total_value_usd == WALLET_VALUE + Decimal("1000")
        assert s.available_cash_usd == WALLET_VALUE - INV_VALUE
        baseline = snap_with(None, PRICES, BALANCES, positions=[supply])
        assert s.total_value_usd + s.available_cash_usd == baseline.total_value_usd + baseline.available_cash_usd

    def test_strategy_reported_token_pseudo_position_not_double_counted(self):
        """VIB-4909 interplay: a strategy-reported TOKEN pseudo-position for the
        SAME wallet token stays excluded from the sums (its value lives in the
        wallet) while the lot-derived inventory row IS counted — exactly once."""
        pseudo = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="rsi-wbtc",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=INV_VALUE,
            details={"asset": "WBTC"},
        )
        s = snap_with([BUY_WBTC], PRICES, BALANCES, positions=[pseudo])
        assert s.total_value_usd == INV_VALUE  # once, not twice
        assert s.available_cash_usd == WALLET_VALUE - INV_VALUE
        assert s.wallet_total_value_usd == WALLET_VALUE


# ---------------------------------------------------------------------------
# D3 — robustness (no silent failure)
# ---------------------------------------------------------------------------


class TestMissingMarkPrice:
    """D3.F1 — lot token with a wallet balance but no mark price."""

    def test_unpriced_token_skipped_with_sentinel(self):
        events = [swap_event("ARB", "100", amount_in="80", amount_out_usd="80")]
        balances = dict(BALANCES)
        balances["ARB"] = Decimal("100")
        tracked = ("WBTC", "USDC", "ARB")
        s = snap_with(events, PRICES, balances, tracked=tracked)
        baseline = snap_with(None, PRICES, balances, tracked=tracked)

        # ARB has no mark — absent from wallet value too (symmetric); cash unchanged.
        assert s.available_cash_usd == baseline.available_cash_usd
        assert s.available_cash_usd >= 0
        assert s.total_value_usd == baseline.total_value_usd
        assert s.deployed_capital_usd == baseline.deployed_capital_usd

        meta = (s.snapshot_metadata or {}).get("swap_inventory")
        assert meta, s.snapshot_metadata
        assert meta.get("skipped", {}).get("arb") == "price_missing", meta
        # Positive balance with no price already downgrades confidence.
        assert s.value_confidence.value != "high"


class TestUnmeasuredLotCost:
    """D3.F2 — Empty ≠ Zero: unmeasured lot cost must not collapse to $0 basis."""

    def test_cost_unmeasured_lot_not_reclassified(self):
        events = [swap_event("WBTC", "0.05", amount_in="5000", amount_out_usd=None)]
        s = snap_with(events, PRICES, BALANCES)
        baseline = snap_with(None, PRICES, BALANCES)

        assert s.available_cash_usd == baseline.available_cash_usd
        assert s.total_value_usd == baseline.total_value_usd
        assert s.deployed_capital_usd == baseline.deployed_capital_usd
        assert s.total_value_usd + s.available_cash_usd == baseline.total_value_usd + baseline.available_cash_usd

        meta = (s.snapshot_metadata or {}).get("swap_inventory")
        assert meta and meta.get("skipped", {}).get("wbtc") == "cost_unmeasured", meta


class TestLotExceedsWalletHolding:
    """D3.F3 — VIB-5010-shaped drift: lots exceed actual wallet holdings."""

    def test_capped_at_wallet_holding_cost_prorated(self):
        balances = dict(BALANCES)
        balances["WBTC"] = Decimal("0.02")
        wallet_value = Decimal("0.02") * Decimal("110000") + Decimal("200") + Decimal("0.01") * Decimal("2500")
        s = snap_with([BUY_WBTC], PRICES, balances)

        capped_value = Decimal("0.02") * Decimal("110000")
        capped_cost = Decimal("5000") * (Decimal("0.02") / Decimal("0.05"))
        assert s.total_value_usd == capped_value
        assert s.available_cash_usd == wallet_value - capped_value
        assert s.available_cash_usd >= 0
        assert s.deployed_capital_usd == capped_cost
        meta = (s.snapshot_metadata or {}).get("swap_inventory")
        assert meta and "capped" in json.dumps(meta), meta

    def test_zero_holding_skipped_never_negative(self):
        balances0 = dict(BALANCES)
        del balances0["WBTC"]
        s = snap_with([BUY_WBTC], PRICES, balances0)
        baseline = snap_with(None, PRICES, balances0)

        assert s.available_cash_usd == baseline.available_cash_usd
        assert s.available_cash_usd >= 0
        assert s.total_value_usd == baseline.total_value_usd
        assert s.deployed_capital_usd == baseline.deployed_capital_usd
        meta = (s.snapshot_metadata or {}).get("swap_inventory")
        assert meta and meta.get("skipped", {}).get("wbtc") == "capped_to_zero", meta


class TestAccountingStoreFailure:
    """D3.F4 — store failure degrades loudly-but-safely."""

    def test_store_raise_no_reclassification_and_stamped(self, caplog):
        broken = MagicMock()

        def _boom(dep, position_key=None):
            raise RuntimeError("store down")

        broken.get_accounting_events_sync = _boom
        v = PortfolioValuer()
        v.set_accounting_context(broken, DEP)
        with caplog.at_level(logging.DEBUG, logger="almanak"):
            s = v.value(make_strategy(), make_market(PRICES, BALANCES))
        baseline = snap_with(None, PRICES, BALANCES)

        assert s.available_cash_usd == baseline.available_cash_usd
        assert s.total_value_usd == baseline.total_value_usd
        meta = (s.snapshot_metadata or {}).get("swap_inventory")
        assert meta and "unavailable" in json.dumps(meta).lower(), meta
        assert meta.get("status") != "applied"
        assert any(
            "swap inventory" in r.getMessage().lower() or "accounting" in r.getMessage().lower()
            for r in caplog.records
        )


class TestDefensiveBranches:
    """Direct unit tests for the never-raise / never-negative last lines."""

    def test_classification_internal_error_degrades_with_stamp(self):
        """An unexpected failure INSIDE classification (not the store fetch)
        degrades to cash-as-today with an explicit ``unavailable`` stamp."""
        v = PortfolioValuer()
        v.set_accounting_context(make_store([BUY_WBTC]), DEP)
        v._snapshot_events_flat = 42  # type: ignore[assignment] — poisoned state
        result = v._swap_inventory_for_snapshot("arbitrum", dict(BALANCES), dict(PRICES))
        assert result.rows == []
        assert result.inventory_value_usd == Decimal("0")
        assert result.metadata == {"status": "unavailable", "reason": "classification_error"}

    def test_case_variant_balances_sum_before_capping(self):
        """The same token tracked under two case variants must SUM for the
        cap, not last-write-wins — otherwise a partial cap understates
        inventory and overstates available cash (the symptom being fixed).
        Lot = 0.05 WBTC; wallet holds it split 0.03 ``WBTC`` + 0.03 ``wbtc``
        (0.06 total ≥ lot), so the full lot must value uncapped."""
        v = PortfolioValuer()
        v.set_accounting_context(make_store([BUY_WBTC]), DEP)
        v._prefetch_accounting_events(DEP)  # populate _snapshot_events_flat
        balances = {"WBTC": Decimal("0.03"), "wbtc": Decimal("0.03")}
        prices = {"WBTC": Decimal("110000")}
        result = v._swap_inventory_for_snapshot("arbitrum", balances, prices)
        assert len(result.rows) == 1
        token_meta = result.metadata["tokens"]["wbtc"]
        assert token_meta["capped"] is False
        assert token_meta["quantity"] == "0.05"  # full lot, not 0.03
        assert result.inventory_value_usd == Decimal("0.05") * Decimal("110000")

    def test_negative_cash_clamped_to_zero_with_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger="almanak"):
            clamped = PortfolioValuer._idle_cash_after_inventory(Decimal("100"), Decimal("150"))
        assert clamped == Decimal("0")
        assert any("clamping" in r.getMessage() for r in caplog.records)
        # Non-negative input passes through exactly.
        assert PortfolioValuer._idle_cash_after_inventory(Decimal("100"), Decimal("40")) == Decimal("60")


class TestPersistenceRoundTrip:
    """The applied stamp + inventory rows must survive snapshot serialization —
    the gateway-side double-count gate reads them off the PERSISTED snapshot."""

    def test_metadata_and_rows_survive_to_dict(self):
        s = snap_with([BUY_WBTC], PRICES, BALANCES)
        d = s.to_dict()
        # Round-trip through JSON (the persistence envelope is JSON).
        d = json.loads(json.dumps(d, default=str))
        meta = (d.get("snapshot_metadata") or {}).get("swap_inventory")
        assert isinstance(meta, dict) and meta.get("status") == "applied", d.get("snapshot_metadata")
        row_dicts = [p for p in d.get("positions", []) if p.get("details", {}).get("source") == "swap_inventory_lots"]
        assert row_dicts and row_dicts[0].get("cost_basis_usd") == "5000"

    def test_applied_stamp_survives_positions_payload_envelope(self):
        """Pin the gateway's ACTUAL read path: the suppression gate consumes
        snapshot_metadata reconstructed via to_positions_payload() ->
        unpack_positions_payload() (the VIB-3923 envelope), not to_dict().
        A future envelope change that drops the swap_inventory stamp would
        silently resurrect inventory-MTM double-counting (pr-auditor #2740
        Important 1)."""
        s = snap_with([BUY_WBTC], PRICES, BALANCES)
        payload = json.loads(json.dumps(s.to_positions_payload(), default=str))
        positions, metadata = PortfolioSnapshot.unpack_positions_payload(payload)
        stamp = metadata.get("swap_inventory")
        assert isinstance(stamp, dict) and stamp.get("status") == "applied", metadata
        row_dicts = [p for p in positions if p.get("details", {}).get("source") == "swap_inventory_lots"]
        assert row_dicts and row_dicts[0].get("cost_basis_usd") == "5000"

    def test_inventory_rows_count_as_open_positions(self):
        """VIB-5057 deliberately surfaces inventory as open positions, so the
        open_position_count tile follows (a pure swap strategy mid-position
        renders count >= 1 where it previously rendered 0) and the inventory
        row never registers as debt."""
        from almanak.framework.dashboard.quant_aggregations import (
            _open_positions_and_net_debt,
        )

        s = snap_with([BUY_WBTC], PRICES, BALANCES)
        payload = json.loads(json.dumps(s.to_positions_payload(), default=str))
        count, debt_to_net = _open_positions_and_net_debt(payload)
        assert count == 1
        assert debt_to_net == Decimal("0")
