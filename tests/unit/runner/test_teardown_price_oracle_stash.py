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

from unittest.mock import patch

from almanak.framework.runner._run_loop_helpers import (
    _ensure_native_gas_in_teardown_oracle,
    _ensure_receipt_legs_in_teardown_oracle,
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
    a PriceResult-shaped object — enough for the helper's contract.

    ``price=None`` models two distinct miss modes, selected by ``raises``:
    ``raises=True`` (default) → ``get_aggregated_price`` throws (the oracle
    errored); ``raises=False`` → it returns successfully with a PriceResult-shaped
    object whose ``price`` is ``None`` (a clean fetch that simply carries no
    price). The helper handles these on separate branches, so the tests need
    both."""

    def __init__(
        self,
        *,
        price: str | None = "3500.00",
        source: str = "gateway",
        raises: bool = True,
    ):
        self.calls: list[tuple[str, str, str | None]] = []
        self._price = price
        self._source = source
        self._raises = raises

    async def get_aggregated_price(self, token, quote="USD", *, chain=None):
        self.calls.append((token, quote, chain))
        if self._price is None:
            if self._raises:
                raise RuntimeError("simulated oracle failure")
            # Successful fetch that carries no price (PriceResult.price is None).
            return SimpleNamespace(
                price=None,
                source=self._source,
                timestamp=datetime(2026, 5, 3, 14, 30, tzinfo=UTC),
                confidence="UNAVAILABLE",
            )
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


def test_native_topoff_initialises_oracle_when_none_or_empty():
    """VIB-5365: gas is paid in ETH on every teardown TX regardless of whether
    the strategy holds any priced asset, so a ``None`` / empty pre-bracket oracle
    (the Pendle LP / PT case — USD-less holdings → empty snapshot) MUST still be
    initialised with the native gas price. The previous ``if not oracle: return``
    early-out left ``gas_usd`` + ``price_inputs_json`` empty on those rows
    (Accountant G2/G12 FAIL)."""
    fake_oracle = _FakeOracle(price="3500.00")
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")

    out_none = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, None))
    assert out_none is not None
    assert out_none["ETH"]["price_usd"] == "3500.00"
    assert out_none["ETH"]["oracle_source"] == "gateway"

    out_empty = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, {}))
    assert out_empty["ETH"]["price_usd"] == "3500.00"

    # One native price fetch per call (None + {}).
    assert fake_oracle.calls == [("ETH", "USD", "arbitrum"), ("ETH", "USD", "arbitrum")]


def test_native_topoff_leaves_none_when_native_price_unavailable():
    """Empty != Zero: when the oracle returns successfully but the result carries
    no native price (``PriceResult.price is None`` — a clean miss, not an
    exception), a ``None`` oracle stays ``None`` (no fabricated entry) and the
    row's ``gas_usd`` stays empty. Exercises the ``if price is None: return oracle``
    branch in ``_ensure_native_gas_in_teardown_oracle`` — distinct from the
    raise/except path covered by ``test_native_topoff_returns_input_on_oracle_failure``."""
    fake_oracle = _FakeOracle(price=None, raises=False)  # returns price-None object, no raise
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    out = asyncio.run(_ensure_native_gas_in_teardown_oracle(runner, strategy, None))
    assert out is None
    # The fetch DID happen and returned (vs the except path) — the None comes from
    # the price-is-None branch, not from an oracle exception.
    assert fake_oracle.calls == [("ETH", "USD", "arbitrum")]


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


# ─── _ensure_receipt_legs_in_teardown_oracle (VIB-5124) ───────────────────────

SUSDAI_ADDR = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
_RESOLVER_PATH = "almanak.framework.data.tokens.get_token_resolver"


def _resolved(symbol: str, coingecko_id: str | None):
    return SimpleNamespace(symbol=symbol, coingecko_id=coingecko_id)


def _close_result(currency0: str | None, currency1: str | None) -> SimpleNamespace:
    """ExecutionResult-like with an LP_CLOSE receipt carrying currency addrs."""
    return SimpleNamespace(
        lp_open_data=None,
        lp_close_data=SimpleNamespace(currency0=currency0, currency1=currency1),
    )


def test_receipt_backfill_prices_coingecko_null_leg_by_address():
    """The non-zero sUSDai leg returned by an LP_CLOSE is priced by ADDRESS via
    the gateway oracle and stored under the canonical SYMBOL key."""
    fake_oracle = _FakeOracle(price="1.04")
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    result = _close_result(SUSDAI_ADDR, USDC_ADDR)

    def _resolve(token, chain, **kwargs):
        return _resolved("SUSDAI", None) if token == SUSDAI_ADDR.lower() else _resolved("USDC", "usd-coin")

    resolver = SimpleNamespace(resolve=_resolve)
    teardown_oracle = {"USDC": {"price_usd": "1.00", "oracle_source": "portfolio_valuer"}}

    with patch(_RESOLVER_PATH, return_value=resolver):
        out = asyncio.run(_ensure_receipt_legs_in_teardown_oracle(runner, strategy, result, teardown_oracle))

    assert out["SUSDAI"]["price_usd"] == "1.04"  # priced by address, keyed by symbol
    assert out["USDC"]["price_usd"] == "1.00"  # untouched
    # Only the coingecko-null leg is priced by address (USDC has a cg id).
    # Address is lowercased at the shared ``_receipt_token_legs`` seam (lane symmetry).
    assert fake_oracle.calls == [(SUSDAI_ADDR.lower(), "USD", "arbitrum")]


def test_receipt_backfill_skips_token_with_coingecko_id():
    fake_oracle = _FakeOracle(price="1.00")
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    result = _close_result(USDC_ADDR, None)
    resolver = SimpleNamespace(resolve=lambda *a, **k: _resolved("USDC", "usd-coin"))

    with patch(_RESOLVER_PATH, return_value=resolver):
        out = asyncio.run(_ensure_receipt_legs_in_teardown_oracle(runner, strategy, result, None))

    assert out is None  # nothing backfilled
    assert fake_oracle.calls == []


def test_receipt_backfill_does_not_overwrite_existing_symbol():
    fake_oracle = _FakeOracle(price="9.99")
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    result = _close_result(SUSDAI_ADDR, None)
    resolver = SimpleNamespace(resolve=lambda *a, **k: _resolved("SUSDAI", None))
    teardown_oracle = {"SUSDAI": {"price_usd": "1.05"}}  # already present

    with patch(_RESOLVER_PATH, return_value=resolver):
        out = asyncio.run(_ensure_receipt_legs_in_teardown_oracle(runner, strategy, result, teardown_oracle))

    assert out["SUSDAI"]["price_usd"] == "1.05"  # preserved
    assert fake_oracle.calls == []


def test_receipt_backfill_miss_leaves_key_absent_empty_not_zero():
    fake_oracle = _FakeOracle(price=None)  # get_aggregated_price raises
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    result = _close_result(SUSDAI_ADDR, None)
    resolver = SimpleNamespace(resolve=lambda *a, **k: _resolved("SUSDAI", None))

    with patch(_RESOLVER_PATH, return_value=resolver):
        out = asyncio.run(_ensure_receipt_legs_in_teardown_oracle(runner, strategy, result, None))

    assert out is None  # nothing added; never a fabricated $0


def test_receipt_backfill_noop_without_result():
    fake_oracle = _FakeOracle()
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    out = asyncio.run(_ensure_receipt_legs_in_teardown_oracle(runner, strategy, None, {"USDC": {"price_usd": "1"}}))
    assert out == {"USDC": {"price_usd": "1"}}
    assert fake_oracle.calls == []


def test_receipt_backfill_noop_without_receipt_legs():
    """A result with no LP receipt (e.g. a teardown SWAP) contributes no legs."""
    fake_oracle = _FakeOracle()
    runner = _fake_runner(fake_oracle)
    strategy = _fake_strategy("arbitrum")
    result = SimpleNamespace(lp_open_data=None, lp_close_data=None)
    out = asyncio.run(_ensure_receipt_legs_in_teardown_oracle(runner, strategy, result, None))
    assert out is None
    assert fake_oracle.calls == []
