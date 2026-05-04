"""Unit tests for ``lending_accounting._resolve_oracle_price`` (Codex X1, 2026-05-04 PR #2017 audit).

Codex flagged that the teardown lane builds the price oracle in the
nested AttemptNo17 G12 shape ``{symbol: {"price_usd": ..., ...}}`` (via
``_portfolio_snapshot_to_price_oracle``), but the Morpho Blue and
Compound V3 lending readers were doing ``Decimal(str(price))`` directly
on the lookup result — which on the nested shape passes a dict to
``Decimal(str(...))`` and silently returns None. Result: ``post_state_json``
loses collateral/debt/HF on every non-Aave teardown row.

The fix introduces ``_resolve_oracle_price`` as a shape-tolerant helper and
threads it through ``_amount_to_usd`` plus the inline Morpho/Compound
readers. These tests pin the contract:

- Flat shape ``{symbol: 1500.0}`` → Decimal("1500.0")
- Nested shape ``{symbol: {"price_usd": 1500.0, ...}}`` → Decimal("1500.0")
- Case-insensitive symbol lookup
- Returns None on missing symbol, malformed entries, and missing price_usd
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.lending_accounting import (
    _amount_to_usd,
    _resolve_oracle_price,
)


class TestResolveOraclePriceShapeTolerance:
    def test_flat_shape_returns_decimal(self) -> None:
        oracle = {"WETH": "1500.50", "USDC": 1.0}
        assert _resolve_oracle_price(oracle, "WETH") == Decimal("1500.50")
        assert _resolve_oracle_price(oracle, "USDC") == Decimal("1")

    def test_nested_g12_shape_returns_decimal(self) -> None:
        oracle = {
            "WETH": {"price_usd": "1500.50", "oracle_source": "portfolio_valuer", "fetched_at": "..."},
            "USDC": {"price_usd": "1.0001", "oracle_source": "portfolio_valuer"},
        }
        assert _resolve_oracle_price(oracle, "WETH") == Decimal("1500.50")
        assert _resolve_oracle_price(oracle, "USDC") == Decimal("1.0001")

    def test_mixed_shapes_in_same_oracle(self) -> None:
        # Iteration lane may produce flat; teardown lane produces nested.
        # If both ever appear in the same dict (defensive), each entry
        # should be resolved by its own shape.
        oracle = {
            "WETH": "1500.50",  # flat
            "USDC": {"price_usd": "1.0", "oracle_source": "ev"},  # nested
        }
        assert _resolve_oracle_price(oracle, "WETH") == Decimal("1500.50")
        assert _resolve_oracle_price(oracle, "USDC") == Decimal("1.0")

    def test_case_insensitive_lookup(self) -> None:
        oracle = {"WETH": "1500"}
        assert _resolve_oracle_price(oracle, "WETH") == Decimal("1500")
        assert _resolve_oracle_price(oracle, "weth") == Decimal("1500")
        assert _resolve_oracle_price(oracle, "Weth") == Decimal("1500")

    def test_mixed_case_oracle_key(self) -> None:
        # CodeRabbit 2026-05-04 review: oracle keys can be mixed-case
        # (e.g. ``wstETH``); the lookup must find them regardless of how
        # the asset symbol is cased on the lookup side.
        oracle = {"wstETH": "2500.00", "USDC": "1.0001"}
        assert _resolve_oracle_price(oracle, "WSTETH") == Decimal("2500.00")
        assert _resolve_oracle_price(oracle, "wsteth") == Decimal("2500.00")
        assert _resolve_oracle_price(oracle, "wstETH") == Decimal("2500.00")
        # Nested-shape mixed-case still resolves.
        nested = {"wstETH": {"price_usd": "2500.00"}}
        assert _resolve_oracle_price(nested, "WSTETH") == Decimal("2500.00")

    def test_missing_symbol_returns_none(self) -> None:
        oracle = {"WETH": "1500"}
        assert _resolve_oracle_price(oracle, "ARB") is None

    def test_none_oracle_returns_none(self) -> None:
        assert _resolve_oracle_price(None, "WETH") is None

    def test_empty_oracle_returns_none(self) -> None:
        assert _resolve_oracle_price({}, "WETH") is None

    def test_nested_missing_price_usd_returns_none(self) -> None:
        # Nested entry exists but missing the price_usd key — honest absence,
        # not a fabricated zero.
        oracle = {"WETH": {"oracle_source": "ev", "fetched_at": "..."}}
        assert _resolve_oracle_price(oracle, "WETH") is None

    def test_unparseable_value_returns_none(self) -> None:
        oracle = {"WETH": "not-a-number"}
        assert _resolve_oracle_price(oracle, "WETH") is None

    def test_nested_unparseable_price_returns_none(self) -> None:
        oracle = {"WETH": {"price_usd": "not-a-number", "oracle_source": "ev"}}
        assert _resolve_oracle_price(oracle, "WETH") is None


class TestAmountToUsdShapeTolerance:
    """``_amount_to_usd`` is the wrapper that lending_handler uses for
    SUPPLY/WITHDRAW principal_delta computation. Verify it now reads the
    nested teardown shape correctly — pre-fix it returned None on the
    teardown lane for any non-Aave protocol, breaking L4 split + G6 recon.
    """

    def test_flat_oracle(self) -> None:
        oracle = {"USDT": "1.0001"}
        result = _amount_to_usd(Decimal("2.000001"), oracle, "USDT")
        assert result == Decimal("2.000001") * Decimal("1.0001")

    def test_nested_oracle_codex_x1_regression(self) -> None:
        # Pre-fix this returned None because Decimal(str({"price_usd":...}))
        # failed silently inside _amount_to_usd.
        oracle = {"USDT": {"price_usd": "1.0001", "oracle_source": "portfolio_valuer"}}
        result = _amount_to_usd(Decimal("2.000001"), oracle, "USDT")
        assert result == Decimal("2.000001") * Decimal("1.0001")

    def test_none_amount_returns_none(self) -> None:
        assert _amount_to_usd(None, {"WETH": "1500"}, "WETH") is None

    def test_missing_asset_returns_none(self) -> None:
        assert _amount_to_usd(Decimal("1.0"), {"WETH": "1500"}, "ARB") is None

    def test_zero_amount_returns_zero(self) -> None:
        # Per CLAUDE.md "Empty ≠ zero": measured zero amount must produce
        # measured zero USD (Decimal("0")) — not None (unmeasured).
        assert _amount_to_usd(Decimal("0"), {"USDT": "1.0"}, "USDT") == Decimal("0")
