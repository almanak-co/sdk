"""Tests for the G12 teardown-lane price-oracle stash
(Accounting-AttemptNo17 §A4).

The teardown lane has no per-iteration ``state.price_oracle`` because
``commit_teardown_intent`` runs outside the iteration body. Until this
landed, every teardown row had empty ``price_inputs_json`` and
``gas_usd`` — surfaced as G12 RED on the Accountant Test mainnet runs.

These tests exercise the converter that re-shapes a PortfolioSnapshot's
``token_prices`` dict into the ``build_ledger_entry`` expected form.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.runner._run_loop_helpers import (
    _portfolio_snapshot_to_price_oracle,
)


def _snapshot(
    *,
    token_prices: dict | None = None,
    confidence: str = "HIGH",
    timestamp: datetime | None = None,
) -> SimpleNamespace:
    """Build a stand-in PortfolioSnapshot. The real type lives in
    ``framework.portfolio.models`` but the converter only reads four
    attributes — keeping the test isolated from changes to that schema.
    """
    return SimpleNamespace(
        token_prices=token_prices or {},
        value_confidence=SimpleNamespace(value=confidence, name=confidence),
        timestamp=timestamp or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
    )


def test_returns_none_when_snapshot_missing():
    assert _portfolio_snapshot_to_price_oracle(None) is None


def test_returns_none_when_token_prices_empty():
    assert _portfolio_snapshot_to_price_oracle(_snapshot(token_prices={})) is None


def test_reshapes_into_build_ledger_entry_form():
    snap = _snapshot(
        token_prices={
            "arbitrum:0xaf88d065e77c8cC2239327C5EDb3A432268e5831": {
                "price_usd": "1.0",
                "symbol": "USDC",
                "decimals": 6,
            },
            "arbitrum:0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {
                "price_usd": "3245.50",
                "symbol": "WETH",
                "decimals": 18,
            },
        },
    )
    out = _portfolio_snapshot_to_price_oracle(snap)
    assert out is not None
    assert set(out.keys()) == {"USDC", "WETH"}
    assert out["USDC"]["price_usd"] == "1.0"
    assert out["WETH"]["price_usd"] == "3245.50"
    assert out["USDC"]["oracle_source"] == "portfolio_valuer"
    assert out["WETH"]["oracle_source"] == "portfolio_valuer"
    assert out["USDC"]["confidence"] == "HIGH"
    assert out["USDC"]["fetched_at"] == "2026-05-01T12:00:00+00:00"


def test_drops_entries_missing_symbol_or_price():
    snap = _snapshot(
        token_prices={
            "arbitrum:0xaaa": {"price_usd": "1.0", "symbol": "USDC"},
            "arbitrum:0xbbb": {"price_usd": None, "symbol": "GMX"},  # no price
            "arbitrum:0xccc": {"price_usd": "1.0"},  # no symbol
            "arbitrum:0xddd": "not-a-dict",  # non-dict value
        },
    )
    out = _portfolio_snapshot_to_price_oracle(snap)
    assert out == {
        "USDC": {
            "price_usd": "1.0",
            "oracle_source": "portfolio_valuer",
            "fetched_at": "2026-05-01T12:00:00+00:00",
            "confidence": "HIGH",
        }
    }


def test_unknown_confidence_collapsed_to_estimated():
    snap = _snapshot(
        token_prices={"a:b": {"price_usd": "1.0", "symbol": "X"}},
        confidence="DEGRADED_BIN_STEP_AUTODETECT",
    )
    out = _portfolio_snapshot_to_price_oracle(snap)
    assert out is not None
    assert out["X"]["confidence"] == "ESTIMATED"


def test_decimal_price_preserved_as_string():
    snap = _snapshot(
        token_prices={
            "arbitrum:0xaaa": {"price_usd": Decimal("3245.501234567890"), "symbol": "WETH"},
        },
    )
    out = _portfolio_snapshot_to_price_oracle(snap)
    assert out is not None
    assert out["WETH"]["price_usd"] == "3245.501234567890"
