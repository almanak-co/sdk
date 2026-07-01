"""VIB-5553 — Curve pool-coin SYMBOL price backfill (crypto-numeraire per-event USD).

A proportional Curve add/remove touches ALL N pool coins (stamped on
``lp_open_data`` / ``lp_close_data`` ``coin_symbols``), but a fungible Curve LP
event lands with empty ``token_in`` / ``token_out`` and no ``currency0`` /
``currency1``. So neither the swap-style token extraction nor the VIB-5124
by-address receipt-leg backfill (which is gated on ``coingecko_id is None``)
surfaces the pool coins. Their prices never reach ``price_inputs_json`` and the
LP handler's non-USD-stable valuation (``_value_curve_legs_usd``) fails closed
for a crypto-numeraire pool (tricrypto USDT/WBTC/WETH), leaving
``cost_basis_usd`` / ``realized_pnl_usd`` / ``fees_total_usd`` NULL (Accountant
G3/G6).

``StrategyRunner._receipt_coin_symbols`` discovers the pool coins from the
receipt; ``StrategyRunner._backfill_coin_symbol_legs`` (iteration lane) and
``_ensure_coin_symbols_in_teardown_oracle`` (teardown lane) price them BY SYMBOL
into the ledger oracle. UNLIKE the VIB-5124 by-address lane there is no
coingecko-null gate: pool coins with CoinGecko ids (WBTC/USDT/WETH) MUST be
priced.

These tests prove:
- ``_receipt_coin_symbols`` extracts / uppercases / dedupes coin_symbols and
  no-ops for missing / non-Curve results.
- The iteration backfill prices every pool coin (even coingecko-id-bearing ones),
  keyed by symbol, in both the nested (with_sources) and flat shapes.
- An already-present symbol is never overwritten (precedence).
- A price miss / non-positive / non-finite price leaves the key absent (Empty≠Zero).
- The teardown twin mirrors all of the above through the async price oracle.
- No-op for non-LP / non-Curve results (no coin_symbols).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner._run_loop_helpers import _ensure_coin_symbols_in_teardown_oracle
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


def _make_runner(price_oracle: object | None = None) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
        decide_timeout_seconds=30.0,
    )
    return StrategyRunner(
        price_oracle=price_oracle or MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _lp_result(*, coin_symbols, side: str = "lp_close_data"):
    """An ExecutionResult stand-in carrying ``coin_symbols`` on the given LP side.

    Mirrors production: the authoritative typed LP data lives in
    ``result.extracted_data[side]`` (the object the ledger serialises); the bare
    ``result.<side>`` attribute is left ``None`` (a stale-attribute stand-in) so
    the test proves the reader prefers ``extracted_data``.
    """
    lp = SimpleNamespace(coin_symbols=coin_symbols)
    return SimpleNamespace(lp_open_data=None, lp_close_data=None, extracted_data={side: lp})


def _market(chain: str = "ethereum", prices: dict[str, Decimal] | None = None):
    prices = prices or {}
    market = MagicMock()
    market.chain = chain
    market.price.side_effect = lambda sym, chain=None: prices.get(sym)
    market.price_data.side_effect = lambda sym, chain=None: SimpleNamespace(source="coingecko")
    return market


# ── _receipt_coin_symbols ────────────────────────────────────────────────────


def test_receipt_coin_symbols_extracts_and_uppercases():
    res = _lp_result(coin_symbols=["usdt", "WBTC", "weth"])
    assert StrategyRunner._receipt_coin_symbols(res) == ["USDT", "WBTC", "WETH"]


def test_receipt_coin_symbols_dedupes_across_open_and_close():
    res = SimpleNamespace(
        lp_open_data=None,
        lp_close_data=None,
        extracted_data={
            "lp_open_data": SimpleNamespace(coin_symbols=["USDT", "WBTC"]),
            "lp_close_data": SimpleNamespace(coin_symbols=["WBTC", "WETH"]),
        },
    )
    assert StrategyRunner._receipt_coin_symbols(res) == ["USDT", "WBTC", "WETH"]


def test_receipt_coin_symbols_prefers_extracted_data_over_stale_attribute():
    # Production shape: the receipt parser stamps coin_symbols on the
    # extracted_data dict entry; the bare .lp_open_data attribute is a STALE
    # pre-enrichment object with no coin_symbols. The reader must prefer the dict.
    res = SimpleNamespace(
        lp_open_data=SimpleNamespace(coin_symbols=None),  # stale attribute
        lp_close_data=None,
        extracted_data={"lp_open_data": SimpleNamespace(coin_symbols=["USDT", "WBTC", "WETH"])},
    )
    assert StrategyRunner._receipt_coin_symbols(res) == ["USDT", "WBTC", "WETH"]


def test_receipt_coin_symbols_reads_dict_shaped_extracted_data():
    # deserialize_extracted_data can leave a plain dict instead of a dataclass.
    res = SimpleNamespace(
        lp_open_data=None,
        lp_close_data=None,
        extracted_data={"lp_close_data": {"coin_symbols": ["USDT", "WBTC", "WETH"]}},
    )
    assert StrategyRunner._receipt_coin_symbols(res) == ["USDT", "WBTC", "WETH"]


def test_receipt_coin_symbols_falls_back_to_attribute_when_no_extracted_data():
    # Older/other results with no extracted_data dict still work via the attribute.
    res = SimpleNamespace(
        lp_open_data=SimpleNamespace(coin_symbols=["USDT", "WBTC"]),
        lp_close_data=None,
    )
    assert StrategyRunner._receipt_coin_symbols(res) == ["USDT", "WBTC"]


@pytest.mark.parametrize(
    "result",
    [
        None,
        SimpleNamespace(lp_open_data=None, lp_close_data=None, extracted_data={}),
        _lp_result(coin_symbols=None),
        _lp_result(coin_symbols=[]),
        _lp_result(coin_symbols=["", "  ", None]),
    ],
)
def test_receipt_coin_symbols_noops_for_missing(result):
    assert StrategyRunner._receipt_coin_symbols(result) == []


# ── iteration lane: _backfill_coin_symbol_legs ───────────────────────────────


def test_iteration_prices_all_pool_coins_by_symbol_nested():
    runner = _make_runner()
    market = _market(prices={"USDT": Decimal("1"), "WBTC": Decimal("58000"), "WETH": Decimal("1574")})
    res = _lp_result(coin_symbols=["USDT", "WBTC", "WETH"])
    out = StrategyRunner._backfill_coin_symbol_legs(
        runner, market, {}, SimpleNamespace(chain="ethereum"), res, with_sources=True
    )
    assert out["WBTC"] == {
        "price_usd": "58000",
        "oracle_source": "coingecko",
        "fetched_at": "",
        "confidence": "HIGH",
    }
    assert out["USDT"]["price_usd"] == "1"
    assert out["WETH"]["price_usd"] == "1574"


def test_iteration_prices_flat_shape():
    runner = _make_runner()
    market = _market(prices={"WBTC": Decimal("58000")})
    res = _lp_result(coin_symbols=["WBTC"])
    out = StrategyRunner._backfill_coin_symbol_legs(
        runner, market, {}, SimpleNamespace(chain="ethereum"), res, with_sources=False
    )
    assert out["WBTC"] == Decimal("58000")


def test_iteration_never_overwrites_existing_symbol():
    runner = _make_runner()
    market = _market(prices={"WETH": Decimal("9999")})
    res = _lp_result(coin_symbols=["WETH"])
    existing = {"WETH": {"price_usd": "1574.08", "oracle_source": "portfolio_valuer", "confidence": "HIGH"}}
    out = StrategyRunner._backfill_coin_symbol_legs(
        runner, market, dict(existing), SimpleNamespace(chain="ethereum"), res, with_sources=True
    )
    assert out["WETH"] == existing["WETH"]  # untouched


@pytest.mark.parametrize("bad_price", [None, Decimal("0"), Decimal("-5"), Decimal("NaN")])
def test_iteration_fail_closed_on_bad_price(bad_price):
    runner = _make_runner()
    market = _market(prices={"WBTC": bad_price})
    res = _lp_result(coin_symbols=["WBTC"])
    out = StrategyRunner._backfill_coin_symbol_legs(
        runner, market, {}, SimpleNamespace(chain="ethereum"), res, with_sources=False
    )
    assert "WBTC" not in out  # Empty ≠ Zero — never fabricate a $0


def test_iteration_noop_without_coin_symbols():
    runner = _make_runner()
    market = _market(prices={"WBTC": Decimal("58000")})
    res = SimpleNamespace(lp_open_data=None, lp_close_data=None)
    out = StrategyRunner._backfill_coin_symbol_legs(
        runner, market, {"USDC": Decimal("1")}, SimpleNamespace(chain="ethereum"), res, with_sources=False
    )
    assert out == {"USDC": Decimal("1")}
    market.price.assert_not_called()


# ── teardown lane: _ensure_coin_symbols_in_teardown_oracle ───────────────────


def _agg(price, *, source="coingecko", confidence="HIGH"):
    return SimpleNamespace(price=price, timestamp=None, confidence=confidence, source=source)


class _AsyncPriceOracle:
    def __init__(self, prices: dict[str, Decimal]):
        self._prices = prices

    async def get_aggregated_price(self, symbol, quote, chain=None):
        return _agg(self._prices.get(symbol))


@pytest.mark.asyncio
async def test_teardown_prices_pool_coins_by_symbol():
    runner = _make_runner(
        price_oracle=_AsyncPriceOracle({"USDT": Decimal("1"), "WBTC": Decimal("58000"), "WETH": Decimal("1574")})
    )
    res = _lp_result(coin_symbols=["USDT", "WBTC", "WETH"])
    out = await _ensure_coin_symbols_in_teardown_oracle(runner, SimpleNamespace(chain="ethereum"), res, {})
    assert out["WBTC"]["price_usd"] == "58000"
    assert out["WBTC"]["confidence"] == "HIGH"
    assert out["USDT"]["price_usd"] == "1"
    assert out["WETH"]["price_usd"] == "1574"


@pytest.mark.asyncio
async def test_teardown_never_overwrites_existing_symbol():
    runner = _make_runner(price_oracle=_AsyncPriceOracle({"WETH": Decimal("9999")}))
    res = _lp_result(coin_symbols=["WETH"])
    existing = {"WETH": {"price_usd": "1574.08", "oracle_source": "portfolio_valuer", "confidence": "HIGH"}}
    out = await _ensure_coin_symbols_in_teardown_oracle(runner, SimpleNamespace(chain="ethereum"), res, dict(existing))
    assert out["WETH"] == existing["WETH"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_price",
    [None, Decimal("0"), Decimal("-5"), Decimal("NaN"), float("nan"), float("inf")],
)
async def test_teardown_fail_closed_on_bad_price(bad_price):
    # A non-finite quote must neither RAISE (Decimal NaN would blow up `<= 0` and
    # escape commit_teardown_intent) nor persist a "nan"/"inf" string into
    # price_inputs_json (a float nan slips past `<= 0`). Both are rejected before
    # the comparison; the key is simply left absent (Empty != Zero).
    runner = _make_runner(price_oracle=_AsyncPriceOracle({"WBTC": bad_price}))
    res = _lp_result(coin_symbols=["WBTC"])
    out = await _ensure_coin_symbols_in_teardown_oracle(runner, SimpleNamespace(chain="ethereum"), res, {})
    assert not out or "WBTC" not in out


def test_iteration_lane_also_rejects_nan():
    # Iteration-lane parity: the by-symbol backfill likewise rejects a NaN quote
    # rather than persisting "nan" (mirrors _backfill_address_priced_legs).
    market = _market(prices={"WBTC": Decimal("NaN")})
    runner = _make_runner()
    res = _lp_result(coin_symbols=["WBTC"])
    out = StrategyRunner._backfill_coin_symbol_legs(
        runner, market, {}, SimpleNamespace(chain="ethereum"), res, with_sources=False
    )
    assert "WBTC" not in out


@pytest.mark.asyncio
async def test_teardown_noop_for_none_result():
    runner = _make_runner(price_oracle=_AsyncPriceOracle({"WBTC": Decimal("58000")}))
    out = await _ensure_coin_symbols_in_teardown_oracle(runner, SimpleNamespace(chain="ethereum"), None, None)
    assert out is None
