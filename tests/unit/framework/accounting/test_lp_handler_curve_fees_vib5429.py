"""VIB-5429: Curve LP/protocol USD fee computation (unblocks Accountant Test G6).

A Curve fungible LP_CLOSE returns ALL N pool coins, so the ledger row carries no
swap-style ``token_in`` / ``token_out`` and the position_key has no token
descriptor. The generic 2-leg (``token0`` / ``token1``) fee path therefore can't
resolve decimals or prices and leaves ``fees_total_usd`` NULL — the Accountant
Test G6 ``Σ_lp_fees_null_count`` gap.

The fix: the Curve receipt parser stamps the pool-coin-ordered symbols on
``LPCloseData.coin_symbols`` (from the static ``CURVE_POOLS`` registry), and the
LP accounting handler prices every fee leg from them (``_curve_close_fees_usd``),
honouring Empty ≠ Zero:

  * any UNMEASURED (``None``) fee leg ⇒ ``None`` (never folded in as zero);
  * a NON-ZERO fee leg with no oracle price ⇒ ``None`` (fail closed);
  * a measured-zero fee on every leg (balanced proportional removal) ⇒
    ``Decimal(0)`` even with no coin priced (``0 × anything == 0`` is measured).
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.connectors.curve.receipt_parser import _pool_coin_symbols
from almanak.framework.accounting.category_handlers.lp_handler import (
    _coin_decimals,
    _curve_close_fees_usd,
    _curve_legs,
    _curve_lp_principal_usd,
    _is_usd_stable_pool,
    _value_curve_legs_usd,
    handle_lp,
)
from almanak.framework.accounting.models import AccountingConfidence
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.ledger import (
    deserialize_extracted_data,
    serialize_extracted_data,
)

CURVE_3POOL = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
POSITION_KEY = f"lp:curve:ethereum:{WALLET}:3pool"


def _outbox_row() -> dict:
    return {
        "outbox_id": "ob-1",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "position_key": POSITION_KEY,
        "wallet_address": WALLET,
        "market_id": "3pool",
    }


def _ledger_row(*, extracted_data_json: str, price_inputs_json: str = "{}") -> dict:
    return {
        "id": "le-1",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "intent_type": "LP_CLOSE",
        "protocol": "curve",
        "chain": "ethereum",
        "execution_mode": "paper",
        "tx_hash": "0xcurveclose",
        # Curve LP_CLOSE rows carry no token symbols — the bug's precondition.
        "token_in": "",
        "token_out": "",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-06-04T00:00:00+00:00",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
    }


def _close_data(**overrides) -> LPCloseData:
    """3pool balanced close: DAI + USDC + USDT proceeds, all-zero imbalance fees."""
    base = {
        "amount0_collected": 1_431_924_530_156_463_304,  # DAI (18 dec)
        "amount1_collected": 1_432_946,  # USDC (6 dec)
        "additional_amounts": {2: 7_142_564},  # USDT (6 dec)
        "fees0": 0,
        "fees1": 0,
        "additional_fees": {2: 0},
        "coin_symbols": ["DAI", "USDC", "USDT"],
        "pool_address": CURVE_3POOL,
    }
    base.update(overrides)
    return LPCloseData(**base)


def _roundtrip(lp_close: LPCloseData) -> LPCloseData:
    """Mimic the DB path: serialize → deserialize → reconstructed LPCloseData."""
    return deserialize_extracted_data(serialize_extracted_data({"lp_close_data": lp_close}))["lp_close_data"]


# ──────────────────────────────────────────────────────────────────────────────
# _pool_coin_symbols (connector registry lookup)
# ──────────────────────────────────────────────────────────────────────────────
class TestPoolCoinSymbols:
    def test_3pool_returns_ordered_coins(self):
        assert _pool_coin_symbols(CURVE_3POOL, "ethereum") == ["DAI", "USDC", "USDT"]

    def test_checksummed_address_matches(self):
        # Lookup is case-insensitive on the pool address.
        assert _pool_coin_symbols(CURVE_3POOL.upper(), "ethereum") == ["DAI", "USDC", "USDT"]

    def test_unknown_pool_returns_empty(self):
        assert _pool_coin_symbols("0x" + "ab" * 20, "ethereum") == []

    def test_empty_address_returns_empty(self):
        assert _pool_coin_symbols("", "ethereum") == []


# ──────────────────────────────────────────────────────────────────────────────
# _curve_close_fees_usd (Empty ≠ Zero matrix)
# ──────────────────────────────────────────────────────────────────────────────
class TestCurveCloseFeesUsd:
    def test_balanced_all_zero_fees_no_prices_is_measured_zero(self):
        # The dominant case: a balanced proportional remove_liquidity charges no
        # imbalance fee. Measured zero even when DAI/USDT are unpriced.
        assert _curve_close_fees_usd(_close_data(), "ethereum", {}) == Decimal(0)

    def test_balanced_all_zero_fees_usdc_priced_is_measured_zero(self):
        oracle = {"USDC": Decimal("1.00")}
        assert _curve_close_fees_usd(_close_data(), "ethereum", oracle) == Decimal(0)

    def test_nonzero_fee_leg_priced_sums_usd(self):
        # 1.0 USDT (6 dec) of imbalance fee, USDT @ $1.
        lc = _close_data(additional_fees={2: 1_000_000})
        oracle = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        assert _curve_close_fees_usd(lc, "ethereum", oracle) == Decimal("1.0")

    def test_nonzero_fee_leg_unpriced_pegs_on_stable_pool(self):
        # A non-zero fee on a recognized all-USD-stable pool: the unpriced USDT
        # fee is valued at the $1 peg (consistent with the principal peg).
        lc = _close_data(additional_fees={2: 1_000_000})  # 1.0 USDT fee, USDT unpriced
        assert _curve_close_fees_usd(lc, "ethereum", {"USDC": Decimal("1")}) == Decimal("1")

    def test_nonzero_fee_leg_unpriced_fails_closed_on_nonstable_pool(self):
        # A non-stable pool (WETH present) NEVER pegs: an unpriced non-zero fee
        # leg fails closed (UNAVAILABLE), never fabricated at $1.
        lc = _close_data(
            coin_symbols=["DAI", "USDC", "WETH"],
            additional_fees={2: 1_000_000_000_000_000_000},  # 1.0 WETH fee, unpriced
        )
        assert _curve_close_fees_usd(lc, "ethereum", {"USDC": Decimal("1")}) is None

    def test_unmeasured_fee_leg_returns_none(self):
        # fees0=None ⇒ a leg the parser could not measure ⇒ whole-hook None.
        lc = _close_data(fees0=None)
        oracle = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        assert _curve_close_fees_usd(lc, "ethereum", oracle) is None

    def test_missing_additional_fee_leg_returns_none(self):
        # 3 coin_symbols but additional_fees absent ⇒ coin index 2 unmeasured.
        lc = _close_data(additional_fees=None)
        oracle = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        assert _curve_close_fees_usd(lc, "ethereum", oracle) is None

    def test_no_coin_symbols_is_noop_none(self):
        # Non-Curve / unknown pool: no symbols stamped ⇒ handler keeps legacy path.
        lc = _close_data(coin_symbols=None)
        assert _curve_close_fees_usd(lc, "ethereum", {"USDC": Decimal("1")}) is None

    def test_measured_zero_survives_db_roundtrip(self):
        # additional_fees deserializes with string keys/values; coercion must keep
        # the measured-zero result intact through the real DB path.
        lc = _roundtrip(_close_data())
        assert lc.coin_symbols == ["DAI", "USDC", "USDT"]
        assert _curve_close_fees_usd(lc, "ethereum", {"USDC": Decimal("1.00")}) == Decimal(0)


# ──────────────────────────────────────────────────────────────────────────────
# handle_lp integration — the G6 Σ_lp_fees_null_count mover
# ──────────────────────────────────────────────────────────────────────────────
class TestHandleLpCurveCloseFees:
    def test_balanced_close_books_measured_zero_fee(self):
        ledger = _ledger_row(extracted_data_json=serialize_extracted_data({"lp_close_data": _close_data()}))
        event = handle_lp(_outbox_row(), ledger)

        assert event is not None
        # The fix: fees_total_usd is a MEASURED Decimal(0), not NULL — this is the
        # value the Accountant Test G6 Σ_lp_fees bucket reads.
        assert event.fees_total_usd == Decimal(0)
        assert event.fees_total_usd is not None
        payload = json.loads(event.to_payload_json())
        assert payload["fees_total_usd"] == "0"
        # VIB-5429 — the measured Curve close is NOT symbol-less: it carries its
        # N-coin identity in coin_symbols even though token0/token1 are empty (a
        # proportional remove_liquidity has no 2-token direction). Locks the
        # payload round-trip of the new field.
        assert payload["coin_symbols"] == ["DAI", "USDC", "USDT"]
        assert payload["token0"] == "" and payload["token1"] == ""

    def test_close_without_coin_symbols_keeps_legacy_null(self):
        # Regression guard: a pre-fix close (no coin_symbols) is unchanged — the
        # legacy 2-leg path still leaves fees_total_usd NULL (no fabricated zero).
        lc = _close_data(coin_symbols=None, fees0=None, fees1=None, additional_fees=None)
        ledger = _ledger_row(extracted_data_json=serialize_extracted_data({"lp_close_data": lc}))
        event = handle_lp(_outbox_row(), ledger)
        assert event is not None
        assert event.fees_total_usd is None


# ──────────────────────────────────────────────────────────────────────────────
# _is_usd_stable_pool — the peg gate (single source: core's shared frozenset)
# ──────────────────────────────────────────────────────────────────────────────
class TestIsUsdStablePool:
    def test_all_stable_coins(self):
        assert _is_usd_stable_pool(["DAI", "USDC", "USDT"]) is True

    def test_any_nonstable_coin_disqualifies(self):
        assert _is_usd_stable_pool(["USDT", "WBTC", "WETH"]) is False
        assert _is_usd_stable_pool(["DAI", "USDC", "WETH"]) is False

    def test_empty_is_not_stable(self):
        assert _is_usd_stable_pool([]) is False

    def test_gate_reads_shared_core_constant(self):
        # VIB-5536: the peg gate reads ``CURVE_USD_STABLE_SYMBOLS`` from
        # ``almanak.core.constants`` (a lower layer than both accounting and
        # valuation) — no backward import from valuation, no lazy re-import, so
        # there is no import-failure degradation path to exercise: core is a hard
        # dependency of this module. The valuation NAV repricer aliases the SAME
        # object, so basis-peg and NAV-mark can never drift.
        from almanak.core.constants import CURVE_USD_STABLE_SYMBOLS
        from almanak.framework.accounting.category_handlers import lp_handler
        from almanak.framework.valuation.curve_lp_position_reader import _USD_STABLE_SYMBOLS

        assert lp_handler.CURVE_USD_STABLE_SYMBOLS is CURVE_USD_STABLE_SYMBOLS
        assert _USD_STABLE_SYMBOLS is CURVE_USD_STABLE_SYMBOLS


# ──────────────────────────────────────────────────────────────────────────────
# _curve_lp_principal_usd — the G6 Σ_lp_usd_null_count (realized_pnl) mover
# ──────────────────────────────────────────────────────────────────────────────
class TestCurveLpPrincipalUsd:
    def test_close_all_priced_sums_no_peg(self):
        # ~1.4319 DAI + ~1.4329 USDC + ~7.1426 USDT, all priced @ $1.
        oracle = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        usd, used_peg = _curve_lp_principal_usd(_close_data(), "LP_CLOSE", "ethereum", oracle)
        assert used_peg is False
        assert usd == Decimal("1.431924530156463304") + Decimal("1.432946") + Decimal("7.142564")

    def test_close_stable_pool_pegs_unpriced_legs(self):
        # Real-fork shape: only USDC priced; DAI + USDT pegged at $1 (stable pool).
        usd, used_peg = _curve_lp_principal_usd(_close_data(), "LP_CLOSE", "ethereum", {"USDC": Decimal("1.00")})
        assert used_peg is True
        assert usd == Decimal("1.431924530156463304") + Decimal("1.432946") + Decimal("7.142564")

    def test_close_nonstable_pool_unpriced_fails_closed(self):
        # tricrypto-style pool: WETH unpriced and non-stable ⇒ UNAVAILABLE (None),
        # so G6 correctly stays FAIL for crypto pools (NAV repricer's scope).
        lc = _close_data(coin_symbols=["DAI", "USDC", "WETH"])
        usd, used_peg = _curve_lp_principal_usd(lc, "LP_CLOSE", "ethereum", {"USDC": Decimal("1")})
        assert usd is None
        assert used_peg is False

    def test_open_single_sided_values_funded_leg_no_peg(self):
        # Single-sided USDC deposit: all_amounts = [0 DAI, 10 USDC, 0 USDT].
        # DAI/USDT are MEASURED zeros (need no price); USDC priced ⇒ $10, no peg.
        lo = LPOpenData(
            position_id=0,
            amount0=0,  # DAI
            amount1=10_000_000,  # 10 USDC (6 dec)
            additional_amounts={2: 0},  # USDT measured-zero
            coin_symbols=["DAI", "USDC", "USDT"],
            pool_address=CURVE_3POOL,
        )
        usd, used_peg = _curve_lp_principal_usd(lo, "LP_OPEN", "ethereum", {"USDC": Decimal("1.00")})
        assert used_peg is False
        assert usd == Decimal("10.00")

    def test_unmeasured_leg_returns_none(self):
        lc = _close_data(additional_amounts=None)  # coin 2 (USDT) proceeds unmeasured
        oracle = {"DAI": Decimal("1"), "USDC": Decimal("1"), "USDT": Decimal("1")}
        usd, used_peg = _curve_lp_principal_usd(lc, "LP_CLOSE", "ethereum", oracle)
        assert usd is None


# ──────────────────────────────────────────────────────────────────────────────
# handle_lp — full open→close realized_pnl (the G6 Σ_lp_usd_null_count mover)
# ──────────────────────────────────────────────────────────────────────────────
def _open_data(**overrides) -> LPOpenData:
    """3pool single-sided USDC open: all_amounts = [0 DAI, 10 USDC, 0 USDT]."""
    base = {
        "position_id": 0,
        "amount0": 0,  # DAI
        "amount1": 10_000_000,  # 10 USDC
        "additional_amounts": {2: 0},  # USDT
        "coin_symbols": ["DAI", "USDC", "USDT"],
        "pool_address": CURVE_3POOL,
    }
    base.update(overrides)
    return LPOpenData(**base)


_USDC_PRICED = '{"USDC": {"price_usd": "1.00", "confidence": "HIGH"}}'


class TestHandleLpCurveOpenCloseRealizedPnl:
    def test_open_books_principal_basis(self):
        ledger = {
            **_ledger_row(extracted_data_json=serialize_extracted_data({"lp_open_data": _open_data()})),
            "intent_type": "LP_OPEN",
            "price_inputs_json": _USDC_PRICED,
        }
        event = handle_lp(_outbox_row(), ledger)
        assert event is not None
        # Single-sided $10 deposit, USDC priced ⇒ no peg ⇒ HIGH (not estimated).
        assert event.cost_basis_usd == Decimal("10.00")

    def test_close_realized_pnl_with_peg_provenance(self):
        # Prior OPEN payload carries the $10 basis (as the open handler would book).
        prior_open = json.loads(
            handle_lp(
                _outbox_row(),
                {
                    **_ledger_row(extracted_data_json=serialize_extracted_data({"lp_open_data": _open_data()})),
                    "intent_type": "LP_OPEN",
                    "price_inputs_json": _USDC_PRICED,
                },
            ).to_payload_json()
        )

        close_ledger = {
            **_ledger_row(extracted_data_json=serialize_extracted_data({"lp_close_data": _close_data()})),
            "price_inputs_json": _USDC_PRICED,
        }
        event = handle_lp(_outbox_row(), close_ledger, prior_open_payload=prior_open)
        assert event is not None
        # Both G6 nulls are now resolved on this close event:
        assert event.fees_total_usd == Decimal(0)  # Σ_lp_fees_null_count → 0
        assert event.realized_pnl_usd is not None  # Σ_lp_usd_null_count → 0
        # proceeds ($10.007434) − open basis ($10.00) ≈ +$0.0074 (LP round-trip gain).
        close_basis = Decimal("1.431924530156463304") + Decimal("1.432946") + Decimal("7.142564")
        assert event.realized_pnl_usd == close_basis - Decimal("10.00")
        # Peg used for DAI/USDT ⇒ basis self-describes as an estimate (§7.10).
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert "usd_stable_peg" in (event.unavailable_reason or "")


# ──────────────────────────────────────────────────────────────────────────────
# Fail-closed / boundary branches (CodeRabbit #1 zero-leg short-circuit, #2
# static-only decimals) + the Empty≠Zero edge paths.
# ──────────────────────────────────────────────────────────────────────────────
_GARBAGE = "NOTAREALTOKEN_VIB5429"  # never in the static registry → no decimals


class TestCoinDecimals:
    def test_known_curve_coins_resolve_statically(self):
        # skip_gateway path resolves from the static registry — no network.
        assert _coin_decimals("DAI", "ethereum") == 18
        assert _coin_decimals("USDC", "ethereum") == 6
        assert _coin_decimals("USDT", "ethereum") == 6

    def test_unknown_symbol_returns_none(self):
        # Resolver raises TokenNotFoundError → caught → None (fail-closed,
        # never a network fallback). Covers the except branch.
        assert _coin_decimals(_GARBAGE, "ethereum") is None

    def test_empty_symbol_returns_none(self):
        assert _coin_decimals("", "ethereum") is None


class TestCurveLegsZeroShortCircuit:
    def test_zero_leg_with_unresolvable_decimals_is_measured_zero(self):
        # CodeRabbit #1: a measured-ZERO leg whose symbol can't resolve decimals
        # must still scale to Decimal(0) (short-circuit BEFORE _coin_decimals) —
        # NOT fail-close the whole valuation. The unknown symbol's leg is 0.
        legs = _curve_legs([1_000_000, 0], ["USDC", _GARBAGE], "ethereum")
        assert legs == [("USDC", Decimal("1")), (_GARBAGE, Decimal(0))]

    def test_nonzero_leg_with_unresolvable_decimals_fails_closed(self):
        # A NON-ZERO leg we cannot scale ⇒ whole-hook None (cannot value it).
        assert _curve_legs([1_000_000_000, 5], ["USDC", _GARBAGE], "ethereum") is None

    def test_unmeasured_leg_propagates_none(self):
        legs = _curve_legs([None, 0], ["USDC", "USDT"], "ethereum")
        assert legs == [("USDC", None), ("USDT", Decimal(0))]


class TestValueCurveLegsUsdEdges:
    def test_no_legs_returns_none(self):
        assert _value_curve_legs_usd([], {}, True) == (None, False)

    def test_all_measured_zero_no_prices_is_zero(self):
        usd, used_peg = _value_curve_legs_usd([("DAI", Decimal(0)), ("USDC", Decimal(0))], {}, is_usd_stable=False)
        assert usd == Decimal(0) and used_peg is False

    def test_nonzero_unpriced_nonstable_fails_closed(self):
        assert _value_curve_legs_usd([("WETH", Decimal("1"))], {}, is_usd_stable=False) == (None, False)


class TestCurveCloseFeesUsdFailClosed:
    def test_nonzero_fee_unscalable_symbol_returns_none(self):
        # legs is None (nonzero fee on an unresolvable coin) ⇒ fees None.
        lc = LPCloseData(
            amount0_collected=1,
            amount1_collected=1,
            fees0=0,
            fees1=1_000,  # nonzero fee on the garbage coin (index 1)
            coin_symbols=["USDC", _GARBAGE],
            pool_address=CURVE_3POOL,
        )
        assert _curve_close_fees_usd(lc, "ethereum", {"USDC": Decimal("1")}) is None


class TestCurveLpPrincipalUsdEarlyReturns:
    def test_no_coin_symbols_returns_none_false(self):
        lc = _close_data(coin_symbols=None)
        assert _curve_lp_principal_usd(lc, "LP_CLOSE", "ethereum", {}) == (None, False)

    def test_all_amounts_none_returns_none_false(self):
        from types import SimpleNamespace

        fake = SimpleNamespace(coin_symbols=["DAI", "USDC"], all_amounts=None)
        assert _curve_lp_principal_usd(fake, "LP_CLOSE", "ethereum", {}) == (None, False)

    def test_unscalable_nonzero_leg_returns_none_false(self):
        lc = LPCloseData(
            amount0_collected=1,
            amount1_collected=5_000,  # nonzero on the garbage coin
            coin_symbols=["USDC", _GARBAGE],
            pool_address=CURVE_3POOL,
        )
        assert _curve_lp_principal_usd(lc, "LP_CLOSE", "ethereum", {"USDC": Decimal("1")}) == (None, False)
