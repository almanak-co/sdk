"""Readings at each decision, retained beside the dashboard input for charts."""

import json
from decimal import Decimal
from types import SimpleNamespace

from almanak.connectors.dex_dashboard import readings
from almanak.dashboard import build as b
from almanak.dashboard import export, history
from almanak.dashboard.export import export_input
from almanak.dashboard.models import HistoryRow

# The observation right after a real mint on an Arbitrum fork (WETH/USDC 0.05%, block 520609014):
# the mint deposited 0.217624849871133095 WETH and 502.749960 USDC.
MINTED = SimpleNamespace(
    pool=SimpleNamespace(
        sqrt_price_x96=3785698707529396572500942,
        currency0=SimpleNamespace(decimals=18),
        currency1=SimpleNamespace(decimals=6),
    ),
    wallet0=SimpleNamespace(amount=69),
    wallet1=SimpleNamespace(amount=10134460),
    position=SimpleNamespace(
        liquidity=432217730263332, range=SimpleNamespace(lower=-199480, upper=-198500)
    ),
)


def test_a_minted_position_reads_its_price_band_principal_and_value():
    fields = {key: Decimal(value) for key, value in readings(MINTED).items()}
    assert abs(fields["price"] - Decimal("2283.1")) < 1
    assert fields["range_lower"] < fields["price"] < fields["range_upper"]
    assert abs(fields["position0"] - Decimal("0.217624849871133095")) < Decimal("1e-9")
    assert abs(fields["position1"] - Decimal("502.74996")) < Decimal("0.0001")
    held = (fields["wallet0"] + fields["position0"]) * fields["price"]
    assert abs(fields["value"] - (held + fields["wallet1"] + fields["position1"])) < Decimal(
        "1e-12"
    )


def test_hold_marks_the_starting_wallet_at_the_decisions_price():
    start = SimpleNamespace(
        wallet0=SimpleNamespace(amount=0), wallet1=SimpleNamespace(amount=1_010_000_000)
    )
    fields = {key: Decimal(value) for key, value in readings(MINTED, start).items()}
    assert fields["hold"] == Decimal("1010")
    weth_start = SimpleNamespace(
        wallet0=SimpleNamespace(amount=10**18), wallet1=SimpleNamespace(amount=0)
    )
    marked = Decimal(readings(MINTED, weth_start)["hold"])
    assert abs(marked - fields["price"]) < Decimal("1e-12")
    assert "hold" not in readings(MINTED)


def test_without_a_position_only_price_wallets_and_value_are_read():
    flat = SimpleNamespace(**{**MINTED.__dict__, "position": None})
    assert set(readings(flat)) == {"price", "wallet0", "wallet1", "value"}


def test_history_is_bounded_and_skips_a_repeated_reading(tmp_path, monkeypatch):
    monkeypatch.setattr(history, "MAX_ROWS", 3)
    rows = [HistoryRow(time=t, block=t, fields={"price": str(t)}) for t in range(1, 6)]
    for row in rows:
        assert history.append(tmp_path, row)
    assert not history.append(tmp_path, rows[-1])
    assert [r.block for r in history.load(tmp_path)] == [3, 4, 5]


def test_a_hold_still_adds_its_reading_and_the_input_carries_the_history(
    tmp_path, recorded, reference_strategy, monkeypatch
):
    state = recorded.model_copy(update={"strategy": reference_strategy})
    readings_seen = iter(
        HistoryRow(time=t, block=t, fields={"price": p}) for t, p in ((1, "2283.1"), (2, "2290"))
    )
    monkeypatch.setattr(export, "history_row", lambda _state, observed: next(readings_seen))
    export_input(tmp_path, state, observed=("first",))
    # The same checkpoint (a hold) with a new reading is exported again, history included.
    document = export_input(tmp_path, state, observed=("second",))
    assert document is not None
    prices = [row["fields"]["price"] for row in document["context"]["history"]]
    assert prices == ["2283.1", "2290"]
    stored = json.loads((tmp_path / "input.json").read_text())
    assert len(stored["context"]["history"]) == 2


def test_history_points_feed_a_series():
    context = SimpleNamespace(
        history=(
            HistoryRow(time=1, block=1, fields={"price": "2283.1"}),
            HistoryRow(time=2, block=2, fields={"wallet0": "1"}),
        )
    )
    assert b.history(context, "price") == [(1, Decimal("2283.1"))]


def test_a_native_coin_without_a_configured_symbol_reads_its_chains_coin():
    from almanak.caip import AssetType
    from almanak.connectors.dex_dashboard import symbol_of

    state = SimpleNamespace(config=SimpleNamespace(asset_metadata={}))
    assert symbol_of(state, AssetType("eip155:42161/slip44:60")) == "ETH"
    assert symbol_of(state, AssetType("eip155:56/slip44:714")) == "BNB"
    usdc = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")
    assert symbol_of(state, usdc) is None


def test_the_decisions_price_series_and_the_trades_reach_the_dashboard(tmp_path):
    from datetime import UTC, datetime

    from almanak.caip import AssetType
    from almanak.dashboard import series
    from almanak.dashboard.models import Context
    from almanak.models import AssetMetadata

    weth = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
    usdc = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
    metadata = {
        weth: AssetMetadata(decimals=18, symbol="WETH"),
        usdc: AssetMetadata(decimals=6, symbol="USDC"),
    }
    state = SimpleNamespace(config=SimpleNamespace(asset_metadata=metadata), outcomes=())
    closes = SimpleNamespace(
        binding=SimpleNamespace(asset=weth, source=SimpleNamespace(timeframe="1h")),
        quote=usdc,
        points=tuple(
            SimpleNamespace(source_time=3600 * i, value=Decimal("2000") + i) for i in range(600)
        ),
    )
    series.keep_prices(tmp_path, series.prices_of([closes], state))
    kept = series.load_prices(tmp_path)
    assert kept[0].label == "USDC per WETH" and len(kept[0].points) == series.MAX_POINTS
    assert kept[0].points[-1].value == "2599"

    def outcome(kind, intent, deltas, success=True, at=7200):
        return SimpleNamespace(
            success=success,
            operation=SimpleNamespace(kind=kind, target=SimpleNamespace(allocation="position")),
            decision=SimpleNamespace(intent=SimpleNamespace(kind=intent)),
            after=SimpleNamespace(effective_at=datetime.fromtimestamp(at, UTC)),
            deltas=deltas,
        )

    def effects(o):
        return [SimpleNamespace(key=SimpleNamespace(asset=a), amount=n) for a, n in o.deltas]

    state.outcomes = (
        outcome("approval", "swap_exact_input", []),
        outcome("swap", "swap_exact_input", [(usdc, -100_000_000), (weth, 49_000_000_000_000_000)]),
        outcome("swap", "exit_exchange", [(weth, -1)], success=False),
    )
    trades = series.trades_of(state, effects)
    context = Context.model_construct(prices=kept, trades=trades)
    assert b.trades(context) == [
        (7200, "swap_exact_input", {"USDC": Decimal("-100"), "WETH": Decimal("0.049")})
    ]
    assert b.prices(context)[0] == (3600 * 100, Decimal("2100"))
    assert b.prices(context, usdc) == []


def test_a_fixed_range_position_reads_its_range_from_the_target():
    fixed = SimpleNamespace(
        **{
            **MINTED.__dict__,
            "position": SimpleNamespace(liquidity=MINTED.position.liquidity, range=None),
            "target": SimpleNamespace(range=MINTED.position.range),
        }
    )
    assert readings(fixed) == readings(MINTED)


def amount(value):
    return SimpleNamespace(amount=value)


def test_a_v4_position_counts_its_uncollected_fees_in_value():
    from almanak.connectors.concentrated_math import accrued
    from almanak.connectors.uniswap_v4.dashboard import readings as v4_readings

    liquidity = MINTED.position.liquidity
    growth = (2**128 * 10**15 // liquidity + 1, 2**128 * 2_000_000 // liquidity + 1)
    observed = SimpleNamespace(
        pool=MINTED.pool,
        wallet0=MINTED.wallet0,
        wallet1=MINTED.wallet1,
        owned=SimpleNamespace(position=SimpleNamespace(liquidity=liquidity, range=None)),
        target=SimpleNamespace(range=MINTED.position.range),
        position_liquidity=liquidity,
        fee_growth=growth,
        position_growth=(0, 0),
    )
    fields = {key: Decimal(value) for key, value in v4_readings(observed).items()}
    assert accrued(liquidity, 0, growth[0]) == 10**15
    assert fields["fees0"] == Decimal("0.001") and fields["fees1"] == Decimal("2")
    principal = {key: Decimal(value) for key, value in readings(MINTED).items()}
    assert fields["position0"] == principal["position0"]
    expected = principal["value"] + Decimal("0.001") * fields["price"] + 2
    assert abs(fields["value"] - expected) < Decimal("1e-12")
    unread = SimpleNamespace(**{**observed.__dict__, "fee_growth": None})
    assert "fees0" not in v4_readings(unread)


def aerodrome_observation(stable, reserve0, reserve1, position=None):
    return SimpleNamespace(
        pool=SimpleNamespace(
            source=SimpleNamespace(stable=stable),
            currency0=SimpleNamespace(decimals=18),
            currency1=SimpleNamespace(decimals=6),
            shares=SimpleNamespace(decimals=18),
            reserve0=reserve0,
            reserve1=reserve1,
            balance0=reserve0,
            balance1=reserve1,
            total_supply=10**20,
            index0=3 * 10**18,
            index1=5 * 10**18,
        ),
        wallet0=amount(0),
        wallet1=amount(50_000_000),
        position=position,
    )


def test_an_aerodrome_volatile_position_reads_principal_and_fees_at_the_reserve_price():
    from almanak.caip import AssetType
    from almanak.connectors.aerodrome.dashboard import readings as aerodrome_readings
    from almanak.connectors.aerodrome.positions import Position
    from almanak.models import AssetAmount

    weth = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
    usdc = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
    lp = AssetType("eip155:8453/erc20:0xcdac0d6c6c59727a65f871236188350531885c43")
    position = Position(
        shares=AssetAmount(asset=lp, amount=10**19),
        fees0=AssetAmount(asset=weth, amount=0),
        fees1=AssetAmount(asset=usdc, amount=1_000_000),
        index0=3 * 10**18 - 10**14,
        index1=5 * 10**18,
    )
    observed = aerodrome_observation(False, 100 * 10**18, 250_000 * 10**6, position)
    start = SimpleNamespace(wallet0=amount(10**18), wallet1=amount(0))
    fields = {key: Decimal(value) for key, value in aerodrome_readings(observed, start).items()}
    assert fields["price"] == 2500 and fields["shares"] == 10
    assert (fields["position0"], fields["position1"]) == (10, 25_000)
    # 10 shares accrued 1e14 per share of currency0 since the position's index.
    assert (fields["fees0"], fields["fees1"]) == (Decimal("0.001"), 1)
    assert fields["value"] == Decimal("10.001") * 2500 + 25_000 + 1 + 50
    assert fields["hold"] == 2500
    flat = aerodrome_readings(aerodrome_observation(False, 10**18, 2500 * 10**6))
    assert set(flat) == {"price", "wallet0", "wallet1", "value"}


def test_an_aerodrome_stable_pool_reads_the_curves_marginal_price():
    from almanak.connectors.aerodrome.dashboard import readings as aerodrome_readings

    balanced = aerodrome_readings(aerodrome_observation(True, 10**24, 10**12))
    assert Decimal(balanced["price"]) == 1
    x, y = Decimal(2_000_000), Decimal(1_000_000)
    skewed = Decimal(aerodrome_readings(aerodrome_observation(True, 2 * 10**24, 10**12))["price"])

    def k(a, b):
        return a**3 * b + b**3 * a

    step = Decimal("0.000001")
    # Selling a little currency0 into the pool: currency1 out per currency0 in.
    lo, hi = Decimal(0), y
    for _ in range(200):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if k(x + step, y - mid) > k(x, y) else (lo, mid)
    assert abs(skewed - lo / step) < Decimal("1e-4")
    assert skewed < 1


def test_a_curve_stable_position_is_valued_at_the_pools_rates():
    from almanak.connectors.curve.dashboard import readings as curve_readings

    pool = SimpleNamespace(
        currencies=(SimpleNamespace(decimals=18), SimpleNamespace(decimals=6)),
        shares=SimpleNamespace(decimals=18),
        rates=(10**18, 10**30),
        balances=(1000 * 10**18, 1000 * 10**6),
        total_supply=2000 * 10**18,
    )
    observed = SimpleNamespace(
        pool=pool,
        wallets=SimpleNamespace(root=(amount(0), amount(5_000_000))),
        position=SimpleNamespace(shares=amount(200 * 10**18)),
    )
    start = SimpleNamespace(wallets=SimpleNamespace(root=(amount(0), amount(205_000_000))))
    fields = {key: Decimal(value) for key, value in curve_readings(observed, start).items()}
    assert (fields["shares"], fields["position0"], fields["position1"]) == (200, 100, 100)
    assert (fields["wallet0"], fields["wallet1"]) == (0, 5)
    assert fields["value"] == 205 and fields["hold"] == 205


def test_a_curve_crypto_position_is_valued_at_its_price_oracle():
    from almanak.connectors.curve.dashboard import readings as curve_readings

    pool = SimpleNamespace(
        currencies=(SimpleNamespace(decimals=6), SimpleNamespace(decimals=18)),
        shares=SimpleNamespace(decimals=18),
        price_oracle=2500 * 10**18,
        balances=(2_500_000 * 10**6, 1000 * 10**18),
        total_supply=10**22,
    )
    observed = SimpleNamespace(
        pool=pool,
        wallets=SimpleNamespace(root=(amount(0), amount(10**18))),
        position=SimpleNamespace(shares=amount(10**20)),
    )
    fields = {key: Decimal(value) for key, value in curve_readings(observed).items()}
    assert fields["price1"] == 2500
    assert (fields["position0"], fields["position1"]) == (25_000, 10)
    assert fields["value"] == 25_000 + 11 * 2500


def test_a_full_range_edge_price_reads_without_overflowing():
    from almanak.dashboard.models import HistoryRow

    huge = Decimal(2) ** 160 / Decimal(10) ** 12
    text = history.decimal_text(huge)
    assert text.split(".")[0] == str(int(huge))
    HistoryRow(time=1, block=1, fields={"range_upper": text})
    assert history.decimal_text(Decimal("1e-40")) == "0"


def test_allocation_names_become_distinct_valid_key_prefixes():
    from almanak.dashboard.models import HistoryRow

    names = history.prefixes(["stable.usdc", "stable_usdc", "loan", "stable.usdc"])
    assert names == {"stable.usdc": "stable_usdc", "stable_usdc": "stable_usdc_2", "loan": "loan"}
    HistoryRow(time=1, block=1, fields={f"{p}:debt": "1" for p in names.values()})
