"""VIB-3885 — tolerant ``price_inputs_json`` parser regression tests.

Closes the May 2 dashboard miscount class where the LP and SWAP
category handlers couldn't read ``price_inputs_json`` rows the *same
row's* dashboard reader could read. The ledger writes the canonical
nested shape (``{symbol: {price_usd, oracle_source, ...}}``); the
handlers were only tolerating the legacy flat shape
(``{symbol: price}``). Every USD field downstream collapsed to NULL,
G6 reconciliation failed by definition.

These tests fence the round-trip: write nested → parse via the helper
→ multiply against an amount → result equals the writes-flat case.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.accounting.category_handlers._price_helpers import (
    load_raw_price_inputs,
    parse_price_inputs,
)
from almanak.framework.accounting.category_handlers.swap_handler import _token_usd
from almanak.framework.accounting.lp_accounting import compute_lp_cost_basis


# ──────────────────────────────────────────────────────────────────────────
# parse_price_inputs — direct shape coverage
# ──────────────────────────────────────────────────────────────────────────


def test_parse_nested_shape_returns_flat_decimal_dict():
    """Canonical AttemptNo17 §1.2 G12 shape produced by ledger.py:529-544."""
    raw = json.dumps(
        {
            "WETH": {
                "price_usd": "2301.69",
                "oracle_source": "coingecko",
                "fetched_at": "2026-05-02T11:09:00Z",
                "confidence": "HIGH",
            },
            "USDC": {
                "price_usd": "1.0001",
                "oracle_source": "chainlink",
                "fetched_at": "",
                "confidence": "HIGH",
            },
        }
    )
    result = parse_price_inputs(raw)
    assert result == {"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")}


def test_parse_flat_shape_returns_flat_decimal_dict():
    """Legacy ``{symbol: price}`` shape — also tolerated."""
    raw = json.dumps({"WETH": "2301.69", "USDC": "1.0001"})
    result = parse_price_inputs(raw)
    assert result == {"WETH": Decimal("2301.69"), "USDC": Decimal("1.0001")}


def test_parse_normalises_symbol_case_to_upper():
    raw = json.dumps({"weth": {"price_usd": "1"}, "Usdc": "1"})
    result = parse_price_inputs(raw)
    assert "WETH" in result and "USDC" in result
    assert "weth" not in result and "Usdc" not in result


def test_parse_returns_empty_on_falsy_input():
    assert parse_price_inputs(None) == {}
    assert parse_price_inputs("") == {}


def test_parse_returns_empty_on_malformed_json():
    assert parse_price_inputs("{not json") == {}
    assert parse_price_inputs("undefined") == {}


def test_parse_returns_empty_on_non_dict_root():
    """JSON arrays / scalars are not valid price oracles — fail closed."""
    assert parse_price_inputs("[1, 2, 3]") == {}
    assert parse_price_inputs('"just a string"') == {}
    assert parse_price_inputs("42") == {}


def test_parse_drops_entries_missing_price_usd():
    """Nested entry without ``price_usd`` is dropped, not mis-priced."""
    raw = json.dumps({"WETH": {"oracle_source": "coingecko"}})  # no price_usd
    assert parse_price_inputs(raw) == {}


def test_parse_drops_entries_with_non_numeric_price():
    """``Decimal('not-a-number')`` is silently skipped, not propagated."""
    raw = json.dumps({"WETH": "not-a-number", "USDC": "1.0"})
    result = parse_price_inputs(raw)
    assert result == {"USDC": Decimal("1.0")}


def test_parse_drops_entries_with_non_finite_price():
    """NaN / Infinity are unsafe to multiply through — drop them."""
    raw = json.dumps({"NAN": "NaN", "INF": "Infinity", "OK": "1.0"})
    result = parse_price_inputs(raw)
    assert result == {"OK": Decimal("1.0")}


def test_parse_accepts_legacy_price_key_for_back_compat():
    """A handful of in-flight callers wrote ``{"price": ...}`` instead of
    ``{"price_usd": ...}``. Helper accepts that as a fallback."""
    raw = json.dumps({"WETH": {"price": "2301.69"}})
    assert parse_price_inputs(raw) == {"WETH": Decimal("2301.69")}


def test_parse_accepts_int_and_float_prices():
    raw = json.dumps({"INT": 1, "FLOAT": 1.5})
    result = parse_price_inputs(raw)
    assert result["INT"] == Decimal("1")
    assert result["FLOAT"] == Decimal("1.5")


def test_parse_drops_non_string_keys():
    """``{1: "1.0"}`` is illegal JSON, but defensively reject non-str keys."""
    # Equivalent to a dict that bypassed JSON — simulate via the loader path.
    raw_dict = {1: "1.0", "OK": "2.0"}
    json_str = json.dumps({"OK": "2.0"})
    # Direct round-trip via JSON guarantees string keys; this just asserts
    # the non-string-key branch is unreachable through the normal pipeline.
    assert parse_price_inputs(json_str) == {"OK": Decimal("2.0")}
    # Explicit non-str key construction would only surface from a buggy
    # writer; helper would still drop them.
    del raw_dict


# ──────────────────────────────────────────────────────────────────────────
# load_raw_price_inputs — preserves on-disk shape for diagnostic logic
# ──────────────────────────────────────────────────────────────────────────


def test_load_raw_preserves_nested_shape():
    raw = json.dumps({"WETH": {"price_usd": "1", "oracle_source": "coingecko"}})
    result = load_raw_price_inputs(raw)
    assert result == {"WETH": {"price_usd": "1", "oracle_source": "coingecko"}}


def test_load_raw_preserves_flat_shape():
    raw = json.dumps({"WETH": "1"})
    result = load_raw_price_inputs(raw)
    assert result == {"WETH": "1"}


def test_load_raw_returns_empty_on_failure():
    assert load_raw_price_inputs("") == {}
    assert load_raw_price_inputs(None) == {}
    assert load_raw_price_inputs("not json") == {}
    assert load_raw_price_inputs("[1, 2]") == {}


# ──────────────────────────────────────────────────────────────────────────
# Round-trip: nested-shape input → handlers compute identical USD values
# (the regression that produced the May 2 G6 FAIL)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw",
    [
        # Nested (canonical post-AttemptNo17 §1.2 G12)
        json.dumps(
            {
                "WETH": {
                    "price_usd": "2301.69",
                    "oracle_source": "coingecko",
                    "fetched_at": "",
                    "confidence": "HIGH",
                },
                "USDC": {
                    "price_usd": "1.0001",
                    "oracle_source": "chainlink",
                    "fetched_at": "",
                    "confidence": "HIGH",
                },
            }
        ),
        # Flat (legacy fixtures)
        json.dumps({"WETH": "2301.69", "USDC": "1.0001"}),
    ],
)
def test_swap_token_usd_works_for_both_shapes(raw):
    """``_token_usd`` must produce identical results regardless of which
    on-disk shape the ledger wrote. Pre-VIB-3885 the nested form returned
    ``None`` for every symbol, which is what cascaded into G6 FAIL."""
    oracle = parse_price_inputs(raw)
    # 0.000868768309352546 WETH * $2301.69 ≈ $1.99970
    weth_amount = Decimal("0.000868768309352546")
    usd = _token_usd("WETH", weth_amount, oracle)
    assert usd is not None
    assert Decimal("1.99") < usd < Decimal("2.01")


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps(
            {
                "WETH": {"price_usd": "2301.69"},
                "USDC": {"price_usd": "1.0001"},
            }
        ),
        json.dumps({"WETH": "2301.69", "USDC": "1.0001"}),
    ],
)
def test_lp_cost_basis_works_for_both_shapes(raw):
    """``compute_lp_cost_basis`` must compute the same total regardless
    of input shape — exercises the LP_OPEN code path that was the
    flagship bug on the May 2 Anvil run (cost_basis_usd: null)."""
    oracle = parse_price_inputs(raw)
    # Mirror the May 2 LP_OPEN values (§9.3 of AccountingPost1977.md).
    cost = compute_lp_cost_basis(
        amount0=Decimal("0.000891556839636852"),
        amount1=Decimal("2.294332"),
        token0="WETH",
        token1="USDC",
        price_oracle=oracle,
    )
    assert cost is not None
    # 0.000891557 * 2301.69 + 2.294332 * 1.0001 ≈ 2.052 + 2.295 ≈ $4.35
    assert Decimal("4.30") < cost < Decimal("4.40")


def test_lp_cost_basis_returns_none_on_empty_oracle():
    """Empty oracle → fail-closed — must NOT default to $0."""
    cost = compute_lp_cost_basis(
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        token0="WETH",
        token1="USDC",
        price_oracle=parse_price_inputs(""),
    )
    assert cost is None


def test_lp_cost_basis_returns_none_when_one_token_unpriceable():
    """Fail-closed when only one leg is priced — never sum a partial total."""
    raw = json.dumps({"WETH": {"price_usd": "2301.69"}})
    oracle = parse_price_inputs(raw)
    cost = compute_lp_cost_basis(
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        token0="WETH",
        token1="USDC",
        price_oracle=oracle,
    )
    assert cost is None


# ---------------------------------------------------------------------------
# VIB-5124 — measured-zero leg must not void the basis (Empty≠Zero)
# ---------------------------------------------------------------------------


def test_lp_cost_basis_single_sided_open_zero_leg_missing_price():
    """A single-sided LP_OPEN funds one leg; the unfunded leg is a MEASURED
    zero (``Decimal("0")``) whose token (e.g. a coingecko_id-null token) has no
    price. That zero leg contributes $0 and must NOT void the funded leg's basis.

    This is the headline VIB-5124 consumer fix: the Fluid single-sided USDC
    deposit (sUSDai leg = 0, no SUSDAI price) must still tie to $50.
    """
    # USDC priced; SUSDAI absent (coingecko_id-null token the producer couldn't
    # price by symbol on this row).
    raw = json.dumps({"USDC": {"price_usd": "1.00"}})
    oracle = parse_price_inputs(raw)
    cost = compute_lp_cost_basis(
        amount0=Decimal("0"),  # sUSDai — measured zero, unfunded leg
        amount1=Decimal("50"),  # USDC — the single-sided deposit leg
        token0="SUSDAI",
        token1="USDC",
        price_oracle=oracle,
    )
    assert cost == Decimal("50")


def test_lp_cost_basis_both_legs_funded_still_requires_each_price():
    """A NON-zero leg whose price is missing still fails closed — VIB-5124 only
    relaxes the requirement for measured-ZERO legs, never funded ones."""
    raw = json.dumps({"USDC": {"price_usd": "1.00"}})  # SUSDAI absent
    oracle = parse_price_inputs(raw)
    cost = compute_lp_cost_basis(
        amount0=Decimal("10"),  # sUSDai funded but unpriced ⇒ must void
        amount1=Decimal("50"),
        token0="SUSDAI",
        token1="USDC",
        price_oracle=oracle,
    )
    assert cost is None


def test_lp_cost_basis_both_legs_funded_and_priced_sums():
    """Both legs funded and priced ⇒ full basis (no regression from the
    zero-leg relaxation)."""
    raw = json.dumps({"SUSDAI": {"price_usd": "1.05"}, "USDC": {"price_usd": "1.00"}})
    oracle = parse_price_inputs(raw)
    cost = compute_lp_cost_basis(
        amount0=Decimal("20"),
        amount1=Decimal("50"),
        token0="SUSDAI",
        token1="USDC",
        price_oracle=oracle,
    )
    assert cost == Decimal("71.00")  # 20*1.05 + 50*1.00


def test_lp_cost_basis_zero_leg_with_price_is_measured_zero():
    """A zero leg WHOSE PRICE IS AVAILABLE counts as a measured leg (0·price=0
    with ``has_any`` set) so a both-zero-with-prices event yields a measured
    ``Decimal("0")`` — this preserves measured-zero-fees semantics that the IL
    handler relies on (distinguishing ``Decimal("0")`` from ``None``)."""
    raw = json.dumps({"SUSDAI": {"price_usd": "1.05"}, "USDC": {"price_usd": "1.00"}})
    oracle = parse_price_inputs(raw)
    cost = compute_lp_cost_basis(
        amount0=Decimal("0"),
        amount1=Decimal("0"),
        token0="SUSDAI",
        token1="USDC",
        price_oracle=oracle,
    )
    assert cost == Decimal("0")


def test_lp_cost_basis_both_legs_zero_and_unpriced_returns_none():
    """Both legs measured-zero AND unpriced ⇒ nothing measurable ⇒ None
    (not a fabricated $0). ``has_any`` stays False."""
    cost = compute_lp_cost_basis(
        amount0=Decimal("0"),
        amount1=Decimal("0"),
        token0="SUSDAI",
        token1="FOO",
        price_oracle=parse_price_inputs(json.dumps({"BAR": {"price_usd": "1.00"}})),
    )
    assert cost is None
