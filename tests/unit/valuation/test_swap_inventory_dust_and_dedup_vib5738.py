"""VIB-5738 / VIB-5787 item 1 — canonical open/deployed vs wallet-inventory classification.

Two production defects observed on the mainnet Robinhood run (4663), both rooted in
the swap-inventory classifier booking wallet dust / a duplicated holding as a
deployed open position:

1. **Sub-floor dust from a closed position (VIB-5738, lp demo).** After an LP is
   closed and burned, the residual WETH left over from balancing the pair (worth
   $1.44, below the $5 token-consolidation floor teardown deliberately strands) was
   still classified ``deployed_inventory`` — the dashboard PnL header read
   "Open position NAV $1.44 / 1 open position(s)" on an otherwise-flat wallet.
   Fix: a sub-floor swap-inventory lot that is NOT the strategy's declared
   directional ``base_token`` is reclassified as wallet cash (``dust_residual``).

2. **Duplicate leg for one holding (VIB-5787 item 1, rsi demo).** A single live
   WETH swap-inventory holding surfaced TWICE in ``positions_json`` — once as a
   discovered ``uniswap_v3 TOKEN`` wallet pseudo-position and once as the synthetic
   ``swap_inventory_lots`` row — so the ``len(positions)`` open-position count read
   "2 open position(s)" for one holding. Fix: drop the redundant discovered
   pseudo-position for any token a swap-inventory row already covers.

Both fixes are at the write side (the PortfolioValuer), so every read path
(snapshot rows, ``positions_json``, open-position count, NAV tiles) inherits the
truth. Both are **NAV-invariant** — classification only moves value between the
cash and deployed buckets; ``total_value_usd + available_cash_usd`` and
``wallet_total_value_usd`` are unchanged.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.portfolio.models import PortfolioSnapshot, PositionValue, TokenBalance
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.valuation.net_debt import net_debt_from_snapshot
from almanak.framework.valuation.portfolio_valuer import (
    PortfolioValuer,
    _build_wallet_match_index,
    _classify_swap_inventory,
    _dedup_wallet_pseudo_positions_covered_by_swap_inventory,
    _resolve_swap_dust_floor,
)

DEP = "dep-5738"
WALLET = "0x00000000000000000000000000000000000000bb"


# ---------------------------------------------------------------------------
# End-to-end harness (production snapshot writer)
# ---------------------------------------------------------------------------


def make_strategy(
    *,
    tracked=("WETH", "USDG"),
    positions=None,
    base_token=None,
    quote_token=None,
    consolidation_floor=None,
) -> MagicMock:
    s = MagicMock()
    s.deployment_id = DEP
    s.chain = "arbitrum"
    s.wallet_address = WALLET
    s._get_tracked_tokens.return_value = list(tracked)

    def _get_config(key, default=None):
        if key == "base_token":
            return base_token if base_token is not None else default
        if key == "quote_token":
            return quote_token if quote_token is not None else default
        if key == "token_consolidation":
            if consolidation_floor is None:
                return default
            return {"min_swap_value_usd": consolidation_floor}
        return default

    s.get_config = _get_config
    s.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=DEP, timestamp=datetime.now(UTC), positions=positions or []
    )
    return s


def make_market(prices, balances) -> MagicMock:
    m = MagicMock()

    def _price(t: str, quote: str = "USD", *, chain: str | None = None):
        if t in prices:
            return prices[t]
        raise ValueError(f"no price for {t}")

    def _bal(t: str, protocol: str | None = None, *, chain: str | None = None, price=None):
        if t in balances:
            r = MagicMock()
            r.balance = balances[t]
            return r
        raise ValueError(f"no balance for {t}")

    m.price = _price
    m.balance = _bal
    return m


def swap_event(token_out: str, amount_out: str, *, amount_out_usd: str, token_in="USDG") -> dict:
    payload = {
        "event_type": "SWAP",
        "swap_position_key": f"swap:arbitrum:{WALLET}",
        "token_in": token_in,
        "amount_in": amount_out_usd,
        "token_out": token_out,
        "amount_out": amount_out,
        "amount_out_usd": amount_out_usd,
    }
    return {
        "event_type": "SWAP",
        "deployment_id": DEP,
        "position_key": "",
        "chain": "arbitrum",
        "wallet_address": WALLET,
        "timestamp": "2026-06-01T00:00:00+00:00",
        "payload_json": json.dumps(payload),
    }


def make_store(events) -> MagicMock:
    st = MagicMock()
    st.get_accounting_events_sync = lambda dep, position_key=None: list(events)
    return st


def run_snapshot(events, prices, balances, **strat_kw) -> PortfolioSnapshot:
    v = PortfolioValuer()
    v.set_accounting_context(make_store(events), DEP)
    return v.value(make_strategy(**strat_kw), make_market(prices, balances))


def _swap_rows(snap: PortfolioSnapshot) -> list[PositionValue]:
    return [
        p
        for p in snap.positions
        if p.position_type == PositionType.TOKEN and (p.details or {}).get("source") == "swap_inventory_lots"
    ]


# A residual WETH lot worth $1.44 (below the $5 floor) — the lp-demo dust.
DUST_PRICES = {"WETH": Decimal("1770"), "USDG": Decimal("1")}
DUST_BALANCES = {"WETH": Decimal("0.000814151402022487"), "USDG": Decimal("1.46")}
DUST_LOT = swap_event("WETH", "0.000814151402022487", amount_out_usd="1.44")
DUST_WALLET_VALUE = Decimal("0.000814151402022487") * Decimal("1770") + Decimal("1.46")


# ===========================================================================
# Bug 1 — sub-floor dust from a closed position → wallet inventory (cash)
# ===========================================================================


class TestDustResidualToCash:
    def test_subfloor_nondirectional_residual_is_cash(self):
        # No base_token declared (an LP strategy): the $1.44 WETH residual is
        # negligible dust, so it must NOT be a deployed open position.
        snap = run_snapshot(DUST_LOT and [DUST_LOT], DUST_PRICES, DUST_BALANCES)
        assert _swap_rows(snap) == []  # no deployed swap-inventory row
        assert snap.total_value_usd == Decimal("0")  # no open-position NAV
        assert snap.deployed_capital_usd == Decimal("0")
        # the WETH value stays in wallet cash
        assert snap.available_cash_usd == DUST_WALLET_VALUE
        meta = snap.snapshot_metadata["swap_inventory"]
        assert meta["skipped"] == {"weth": "dust_residual"}
        # authoritative cash classification keeps the snapshot stamped "applied"
        assert meta["status"] == "applied"

    def test_zero_open_positions_on_every_read_path(self):
        snap = run_snapshot([DUST_LOT], DUST_PRICES, DUST_BALANCES)
        # positions_json carries no leg; the len()-based open-position count is 0.
        count, _debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snap)
        assert count == 0
        assert snap.positions == []

    def test_declared_base_token_residual_stays_deployed(self):
        # A spot/TA strategy declaring base_token=WETH: even a small WETH holding
        # is intended directional exposure — it stays a deployed position.
        snap = run_snapshot([DUST_LOT], DUST_PRICES, DUST_BALANCES, base_token="WETH")
        rows = _swap_rows(snap)
        assert len(rows) == 1
        assert rows[0].value_usd == Decimal("0.000814151402022487") * Decimal("1770")
        assert snap.total_value_usd == rows[0].value_usd
        # sub-floor but PROTECTED — no dust_residual skip
        assert "dust_residual" not in snap.snapshot_metadata["swap_inventory"].get("skipped", {}).values()

    def test_above_floor_residual_stays_deployed(self):
        # VIB-5057 large-inventory contract preserved: a $17.70 WETH lot (above
        # the $5 floor) with no base_token stays deployed.
        prices = {"WETH": Decimal("1770"), "USDG": Decimal("1")}
        balances = {"WETH": Decimal("0.01"), "USDG": Decimal("1.46")}
        lot = swap_event("WETH", "0.01", amount_out_usd="17.70")
        snap = run_snapshot([lot], prices, balances)
        rows = _swap_rows(snap)
        assert len(rows) == 1
        assert snap.total_value_usd == Decimal("0.01") * Decimal("1770")

    def test_dust_reclassification_is_nav_invariant(self):
        # NAV = total + cash and wallet_total_value_usd equal the full wallet
        # value both when the residual is reclassified (no base_token) and when
        # it stays deployed (base_token=WETH).
        as_cash = run_snapshot([DUST_LOT], DUST_PRICES, DUST_BALANCES)
        as_deployed = run_snapshot([DUST_LOT], DUST_PRICES, DUST_BALANCES, base_token="WETH")
        for snap in (as_cash, as_deployed):
            assert snap.total_value_usd + snap.available_cash_usd == DUST_WALLET_VALUE
            assert snap.wallet_total_value_usd == DUST_WALLET_VALUE

    def test_custom_consolidation_floor_shifts_the_boundary(self):
        # A $10 WETH lot: deployed at the default $5 floor, dust at a custom $20
        # floor. The valuation floor tracks the strategy's teardown floor.
        prices = {"WETH": Decimal("1000"), "USDG": Decimal("1")}
        balances = {"WETH": Decimal("0.01"), "USDG": Decimal("1")}
        lot = swap_event("WETH", "0.01", amount_out_usd="10")
        default_floor = run_snapshot([lot], prices, balances)
        assert len(_swap_rows(default_floor)) == 1  # $10 > $5 → deployed
        custom_floor = run_snapshot([lot], prices, balances, consolidation_floor="20")
        assert _swap_rows(custom_floor) == []  # $10 <= $20 → dust cash
        assert custom_floor.snapshot_metadata["swap_inventory"]["skipped"] == {"weth": "dust_residual"}


# ===========================================================================
# Bug 2 — a single holding appears exactly once in positions_json
# ===========================================================================


class TestSingleHoldingDedup:
    def _discovered_weth_token(self, value_usd: str) -> PositionInfo:
        return PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="rsi-weth",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=Decimal(value_usd),
            details={"asset": "WETH", "base_token": "WETH", "quote_token": "USDG"},
        )

    def test_holding_appears_once_end_to_end(self):
        # The rsi repro: WETH held as swap inventory (base_token=WETH so it stays
        # deployed) AND surfaced as a discovered uniswap_v3 TOKEN pseudo-position.
        prices = {"WETH": Decimal("1771")}
        balances = {"WETH": Decimal("0.001412466428818898")}
        lot = swap_event("WETH", "0.001412466428818898", amount_out_usd="2.50")
        snap = run_snapshot(
            [lot],
            prices,
            balances,
            tracked=("WETH",),
            base_token="WETH",
            positions=[self._discovered_weth_token("2.502191340984821901490")],
        )
        weth_legs = [p for p in snap.positions if "WETH" in ((p.details or {}).get("asset", ""), *(p.tokens or []))]
        assert len(weth_legs) == 1, [json.dumps(p.__dict__, default=str) for p in snap.positions]
        # The surviving leg is the authoritative swap-inventory row (cost basis).
        survivor = weth_legs[0]
        assert (survivor.details or {}).get("source") == "swap_inventory_lots"
        assert survivor.cost_basis_usd == Decimal("2.50")
        # Open-position count reads 1, not 2.
        count, *_ = net_debt_from_snapshot(snap)
        assert count == 1

    def test_dedup_is_nav_invariant(self):
        # Dropping the redundant pseudo-position must not move any money — the
        # value sums already excluded it (VIB-4909).
        prices = {"WETH": Decimal("1771")}
        balances = {"WETH": Decimal("0.001412466428818898")}
        lot = swap_event("WETH", "0.001412466428818898", amount_out_usd="2.50")
        wallet_value = Decimal("0.001412466428818898") * Decimal("1771")
        snap = run_snapshot(
            [lot],
            prices,
            balances,
            tracked=("WETH",),
            base_token="WETH",
            positions=[self._discovered_weth_token("2.502191340984821901490")],
        )
        assert snap.total_value_usd + snap.available_cash_usd == wallet_value
        assert snap.wallet_total_value_usd == wallet_value


# ===========================================================================
# Unit-level: the dedup helper in isolation
# ===========================================================================


class TestDedupHelper:
    # WETH wallet balance carries a concrete EVM address so the address-keyed
    # dedup path (VIB-5738 CodeRabbit follow-up) can be exercised.
    _WETH_ADDR = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def _wallet_balances(self):
        return [TokenBalance(symbol="WETH", balance=Decimal("0.5"), value_usd=Decimal("900"), address=self._WETH_ADDR)]

    def _wallet_index(self):
        return _build_wallet_match_index(self._wallet_balances())

    def _dedup(self, positions, swap_rows):
        return _dedup_wallet_pseudo_positions_covered_by_swap_inventory(
            positions, swap_rows, self._wallet_index(), self._wallet_balances()
        )

    def _swap_row(self):
        return PositionValue(
            position_type=PositionType.TOKEN,
            protocol="wallet",
            chain="arbitrum",
            value_usd=Decimal("900"),
            label="swap inventory WETH",
            tokens=["WETH"],
            details={"asset": "WETH", "source": "swap_inventory_lots"},
        )

    def _discovered(self):
        return PositionValue(
            position_type=PositionType.TOKEN,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("900"),
            label="uniswap_v3 TOKEN",
            tokens=[],
            details={"asset": "WETH"},
        )

    def _discovered_by_address(self):
        # A discovered pseudo-position keyed ONLY by contract address (no "asset"
        # symbol) — the case a symbol-only dedup would miss (CodeRabbit VIB-5738).
        return PositionValue(
            position_type=PositionType.TOKEN,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("900"),
            label="uniswap_v3 TOKEN",
            tokens=[],
            details={"address": self._WETH_ADDR},
        )

    def test_drops_overlapping_pseudo_position(self):
        assert self._dedup([self._discovered()], [self._swap_row()]) == []

    def test_drops_address_keyed_pseudo_position(self):
        # The swap row names WETH by symbol; the duplicate names it by address.
        # Both refer to the same wallet holding → the address-keyed one is dropped.
        assert self._dedup([self._discovered_by_address()], [self._swap_row()]) == []

    def test_keeps_nonoverlapping_deployed_token(self):
        # A vault-share pseudo-position whose asset is NOT in the wallet must be
        # kept — it is a distinct real position (VIB-4909), even sharing a symbol.
        vault = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="metamorpho",
            chain="arbitrum",
            value_usd=Decimal("100"),
            label="vault shares",
            tokens=[],
            details={"asset": "USDC"},  # not in the WETH-only wallet index
        )
        assert self._dedup([vault], [self._swap_row()]) == [vault]

    def test_noop_without_swap_rows(self):
        disc = self._discovered()
        assert self._dedup([disc], []) == [disc]

    def test_does_not_drop_a_swap_inventory_row(self):
        row = self._swap_row()
        assert self._dedup([row], [row]) == [row]


class TestResolveSwapDustFloor:
    def test_float_config_floor_is_honored(self):
        # A JSON/YAML-loaded config carries min_swap_value_usd as a float; it must
        # resolve to that value, not silently fall back to the $5 default (VIB-5738).
        strat = make_strategy(consolidation_floor=3.0)
        assert _resolve_swap_dust_floor(strat) == Decimal("3")

    def test_int_and_str_config_floors(self):
        assert _resolve_swap_dust_floor(make_strategy(consolidation_floor=7)) == Decimal("7")
        assert _resolve_swap_dust_floor(make_strategy(consolidation_floor="2.5")) == Decimal("2.5")

    def test_absent_config_uses_default(self):
        assert _resolve_swap_dust_floor(make_strategy()) == Decimal("5")

    def test_malformed_config_degrades_to_default(self):
        assert _resolve_swap_dust_floor(make_strategy(consolidation_floor="not-a-number")) == Decimal("5")


class TestTokenConsolidationFromDict:
    """The teardown config the valuer's dust floor tracks (VIB-5738 CodeRabbit)."""

    def test_explicit_null_floor_falls_back_to_default(self):
        from almanak.framework.teardown.config import DEFAULT_MIN_SWAP_VALUE_USD, TokenConsolidationConfig

        cfg = TokenConsolidationConfig.from_dict({"min_swap_value_usd": None})
        assert cfg.min_swap_value_usd == DEFAULT_MIN_SWAP_VALUE_USD

    def test_float_floor_is_parsed(self):
        from almanak.framework.teardown.config import TokenConsolidationConfig

        assert TokenConsolidationConfig.from_dict({"min_swap_value_usd": 3.5}).min_swap_value_usd == Decimal("3.5")


# ===========================================================================
# Unit-level: the classifier dust_residual skip in isolation
# ===========================================================================


class TestClassifierDustSkip:
    def test_dust_residual_skip_reason(self):
        out = _classify_swap_inventory(
            {"weth": (Decimal("0.0008"), Decimal("1.44"))},
            {"WETH": Decimal("0.0008")},
            {"WETH": Decimal("1770")},
            "arbitrum",
            dust_floor=Decimal("5"),
        )
        assert out.rows == []
        assert out.inventory_value_usd == Decimal("0")
        assert out.metadata["skipped"] == {"weth": "dust_residual"}
        assert out.metadata["status"] == "applied"

    def test_base_token_protects_subfloor_lot(self):
        out = _classify_swap_inventory(
            {"weth": (Decimal("0.0008"), Decimal("1.44"))},
            {"WETH": Decimal("0.0008")},
            {"WETH": Decimal("1770")},
            "arbitrum",
            base_token="WETH",
            dust_floor=Decimal("5"),
        )
        assert len(out.rows) == 1
        assert out.rows[0].value_usd == Decimal("0.0008") * Decimal("1770")
