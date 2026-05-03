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

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.runner._run_loop_helpers import (
    _ensure_native_gas_in_teardown_oracle,
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


# ─── _ensure_native_gas_in_teardown_oracle (VIB-3918) ─────────────────────────


class _FakeOracle:
    """Stand-in for runner.price_oracle. Captures the call args and returns
    a PriceResult-shaped object — enough for the helper's contract."""

    def __init__(self, *, price: str | None = "3500.00", source: str = "gateway"):
        self.calls: list[tuple[str, str, str | None]] = []
        self._price = price
        self._source = source

    async def get_aggregated_price(self, token, quote="USD", *, chain=None):
        self.calls.append((token, quote, chain))
        if self._price is None:
            raise RuntimeError("simulated oracle failure")
        return SimpleNamespace(
            price=Decimal(self._price),
            source=self._source,
            timestamp=datetime(2026, 5, 3, 14, 30, tzinfo=UTC),
            confidence="HIGH",
        )


def _fake_runner(oracle: _FakeOracle | None) -> SimpleNamespace:
    return SimpleNamespace(price_oracle=oracle, config=SimpleNamespace(chain=""))


def _fake_strategy(chain: str = "arbitrum") -> SimpleNamespace:
    return SimpleNamespace(chain=chain)


def test_native_topoff_adds_eth_when_missing_on_arbitrum():
    fake_oracle = _FakeOracle(price="3500.00")
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    teardown_oracle = {
        "USDC": {"price_usd": "1.0", "oracle_source": "portfolio_valuer"},
        "WETH": {"price_usd": "3500.00", "oracle_source": "portfolio_valuer"},
    }
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, teardown_oracle))
    assert out is teardown_oracle  # mutated in place
    assert "ETH" in out
    assert out["ETH"]["price_usd"] == "3500.00"
    assert out["ETH"]["oracle_source"] == "gateway"
    assert out["ETH"]["confidence"] == "HIGH"
    assert fake_oracle.calls == [("ETH", "USD", "arbitrum")]


def test_native_topoff_skips_when_native_already_present():
    fake_oracle = _FakeOracle()
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    teardown_oracle = {"ETH": {"price_usd": "3499.99", "oracle_source": "portfolio_valuer"}}
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, teardown_oracle))
    assert out["ETH"]["price_usd"] == "3499.99"
    assert fake_oracle.calls == []  # no top-off call


def test_native_topoff_returns_input_on_oracle_failure():
    fake_oracle = _FakeOracle(price=None)  # raises on call
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    teardown_oracle = {"USDC": {"price_usd": "1.0"}}
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, teardown_oracle))
    assert out == {"USDC": {"price_usd": "1.0"}}
    assert "ETH" not in out


def test_native_topoff_polygon_uses_matic():
    fake_oracle = _FakeOracle(price="0.85")
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("polygon")
    teardown_oracle = {"USDC": {"price_usd": "1.0"}}
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, teardown_oracle))
    assert "MATIC" in out
    assert out["MATIC"]["price_usd"] == "0.85"
    assert fake_oracle.calls == [("MATIC", "USD", "polygon")]


def test_native_topoff_noop_when_oracle_dict_empty():
    fake_oracle = _FakeOracle()
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, None))
    assert out is None
    out_empty = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, {}))
    assert out_empty == {}
    assert fake_oracle.calls == []


def test_native_topoff_noop_when_runner_has_no_price_oracle():
    runner = _fake_runner(None)
    strategy = _fake_strategy("arbitrum")
    teardown_oracle = {"USDC": {"price_usd": "1.0"}}
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, teardown_oracle))
    assert "ETH" not in out


def test_native_topoff_noop_when_chain_unknown():
    fake_oracle = _FakeOracle()
    runner = _fake_runner(fake_oracle)
    strategy = SimpleNamespace(chain="")
    runner.config = SimpleNamespace(chain="")
    teardown_oracle = {"USDC": {"price_usd": "1.0"}}
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, teardown_oracle))
    assert "ETH" not in out
    assert fake_oracle.calls == []
