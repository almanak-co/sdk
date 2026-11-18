"""Quote valuation of wallets, positions and replays over synthetic books."""

from decimal import Decimal
from types import SimpleNamespace

from almanak.backtest import valuation
from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.economic_data import RAY, reserve_key
from almanak.backtest.economic_state import EconomicPosition
from almanak.backtest.types import ConnectorId, Name
from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.economic_valuation import AaveAccrual
from almanak.effects import (
    AssetDelta,
    Balance,
    Effects,
    Portfolio,
    Position,
    PositionKey,
    WalletAsset,
)
from almanak.models import Chain
from tests.test_economic_math import market, position
from tests.test_economic_math import reserve as reserve

WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
ETH = AssetType("eip155:1/slip44:60")
MAINNET = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")


class FakePrices:
    quote = "USD"

    def __init__(self, values, units):
        self.values = values
        self.units = units

    def price(self, asset, at, market=None):
        entry = self.values.get(asset)
        if isinstance(entry, dict):
            return entry.get(at)
        return entry

    def decimals(self, asset):
        return self.units.get(asset)


def wallet(asset, account="fund"):
    return WalletAsset(account=account, chain=MAINNET, asset=asset)


def position_key():
    return PositionKey(
        connector=ConnectorId("aave_v4"), chain=MAINNET, account="fund", reference=Name("main")
    )


def test_wallet_valuation_is_exact_and_skips_zero_balances():
    prices = FakePrices({WETH: Decimal("3000.5"), USDC: Decimal(1)}, {WETH: 18, USDC: 6})
    balances = (
        Balance(key=wallet(WETH), amount=2 * 10**18),
        Balance(key=wallet(USDC), amount=5 * 10**6),
        Balance(key=wallet(ETH), amount=0),
    )
    valued, unvalued = valuation.value_wallets(balances, 10, prices)
    assert unvalued == []
    assert [(v.asset, v.units) for v in valued] == [(WETH, 2 * 10**18), (USDC, 5 * 10**6)]
    assert [v.value for v in valued] == [Decimal("6001.0"), Decimal(5)]


def test_missing_price_or_decimals_lists_the_asset_without_throwing():
    prices = FakePrices({WETH: Decimal(3000)}, {WETH: 18})
    balances = (
        Balance(key=wallet(WETH), amount=10**18),
        Balance(key=wallet(USDC), amount=10**6),
        Balance(key=wallet(ETH), amount=10**18),
    )
    valued, unvalued = valuation.value_wallets(balances, 10, prices)
    assert [v.value for v in valued] == [Decimal(3000)]
    assert sorted(unvalued) == sorted([str(ETH), str(USDC)])


def test_position_claims_and_debt_use_share_index_math(reserve):
    view = market(reserve, supply_index=12 * RAY // 10, borrow_index=11 * RAY // 10)
    book = SimpleNamespace(at=lambda reserve, at: view)
    p = position(reserve, shares=2 * 10**18, drawn_shares=100, deficit=7)
    accrued = AaveAccrual(book).accrue(p, 10)
    # A 2.4-asset claim; debt is ceil(100 * 1.1) plus the liquidation deficit.
    price_market = reserve_key(reserve.source, "price")
    assert accrued == (
        valuation.AccruedUnits(WETH, 18, 24 * 10**17, price_market),
        valuation.AccruedUnits(WETH, 18, -117, price_market),
    )


def test_position_valuation_uses_the_positions_own_market_binding(reserve):
    from almanak.backtest.pricing import CoinGeckoPrice, PriceBinding
    from almanak.connectors.aave_v4.economic_valuation import BookPrices

    def source(coin_id):
        return CoinGeckoPrice(
            kind="coingecko",
            provider="synthetic",
            coin_id=coin_id,
            quote="USD",
            interval="5m",
            max_age_seconds=360,
            availability="retrospective_timestamp",
            availability_lag_seconds=0,
        )

    own = reserve.source
    other = reserve.source.model_copy(update={"reserve_id": 7})
    bindings = (
        PriceBinding(
            purpose="valuation",
            chain=own.chain,
            asset=WETH,
            market=reserve_key(own, "price"),
            source=source("weth"),
        ),
        PriceBinding(
            purpose="valuation",
            chain=own.chain,
            asset=WETH,
            market=reserve_key(other, "price"),
            source=source("weth"),
        ),
    )
    rows = {
        reserve_key(own, "price"): SimpleNamespace(payload=SimpleNamespace(value=Decimal(3000))),
        reserve_key(other, "price"): SimpleNamespace(payload=SimpleNamespace(value=Decimal(1500))),
    }
    book = SimpleNamespace(
        at=lambda source, at: market(reserve),
        binding_timeline=lambda binding, start, end: SimpleNamespace(
            at=lambda at: rows[binding.market]
        ),
    )
    prices = BookPrices(book, bindings, {})
    # Two market-specific bindings for one asset leave no generic price.
    assert prices.price(WETH, 10) is None
    p = position(reserve, shares=10**18)
    wrapped = SimpleNamespace(payload=p)
    valued, unvalued = valuation.value_positions((wrapped,), 10, AaveAccrual(book), prices)
    assert unvalued == []
    # One WETH claimed, priced through the position's own market binding.
    assert [v.value for v in valued] == [Decimal(3000)]


def test_position_valuation_marks_missing_prices_and_book_gaps(reserve):
    view = market(reserve)
    gap = InputUnavailable(reserve_key(reserve.source, "interest"), 10, "synthetic gap")

    def missing(reserve, at):
        raise gap

    p = position(reserve, shares=10**18)
    wrapped = SimpleNamespace(payload=p)
    prices = FakePrices({}, {})
    valued, unvalued = valuation.value_positions(
        (wrapped,), 10, AaveAccrual(SimpleNamespace(at=missing)), prices
    )
    assert valued == [] and unvalued == [str(WETH)]
    book = SimpleNamespace(at=lambda reserve, at: view)
    valued, unvalued = valuation.value_positions((wrapped,), 10, AaveAccrual(book), prices)
    assert valued == [] and unvalued == [str(WETH)]


def run_state(start, end, portfolio, completed=(), events=(), time=None):
    return SimpleNamespace(
        schedule=SimpleNamespace(start=start, end=end),
        time=end if time is None else time,
        portfolio=portfolio,
        completed=completed,
        events=events,
    )


def completed_step(at, effects):
    return SimpleNamespace(
        pending=SimpleNamespace(eligible_at=at), transition=SimpleNamespace(effects=effects)
    )


def recorded_event(at, effects):
    return SimpleNamespace(time=at, transition=SimpleNamespace(effects=effects))


def test_replay_reconstructs_holdings_at_each_grid_time():
    initial = Portfolio[EconomicPosition](
        balances=(Balance(key=wallet(WETH), amount=10),), positions=()
    )
    spend = Effects[EconomicPosition](assets=(AssetDelta(key=wallet(WETH), amount=-4),))
    earn = Effects[EconomicPosition](assets=(AssetDelta(key=wallet(WETH), amount=1),))
    state = run_state(
        0,
        10,
        initial.apply(spend).apply(earn),
        completed=(completed_step(5, spend),),
        events=(recorded_event(5, earn),),
    )
    snapshots = valuation.portfolios_at(state, (0, 4, 5, 10))
    amounts = {t: p.balance(wallet(WETH)) for t, p in snapshots.items()}
    # The event applies before the completed step at the same timestamp.
    assert amounts == {0: 10, 4: 10, 5: 7, 10: 7}
    assert snapshots[10] == state.portfolio


def test_unapply_restores_the_initial_portfolio_exactly():
    initial = Portfolio[EconomicPosition](
        balances=(Balance(key=wallet(WETH), amount=10),), positions=()
    )
    delta = Effects[EconomicPosition](assets=(AssetDelta(key=wallet(WETH), amount=-3),))
    assert valuation.unapply(initial.apply(delta), delta) == initial


def test_grid_times_cover_steps_events_window_and_daily_sampling():
    portfolio = Portfolio[EconomicPosition](balances=(), positions=())
    state = run_state(0, 100, portfolio, completed=(completed_step(40, Effects()),))
    times = valuation.grid_times(state)
    assert times[0] == 0 and times[-1] == 100 and 40 in times
    assert len(times) < 25  # short windows never fabricate daily marks
    start = 1787184000  # a UTC day boundary
    state = run_state(start, start + 40 * 86400, portfolio)
    times = valuation.grid_times(state)
    assert len(times) == 41
    assert all(time % 86400 == 0 for time in times)
    state = run_state(start, start + 900 * 86400, portfolio)
    times = valuation.grid_times(state)
    assert len(times) <= 370


def test_equity_curve_sums_wallets_and_positions_with_partial_marks(reserve):
    p = position(reserve, shares=10**18)
    portfolio = Portfolio[EconomicPosition](
        balances=(
            Balance(key=wallet(WETH), amount=10**18),
            Balance(key=wallet(USDC), amount=3 * 10**6),
        ),
        positions=(Position[EconomicPosition](key=position_key(), payload=p),),
    )
    book = SimpleNamespace(at=lambda source, at: market(reserve))
    prices = FakePrices({WETH: Decimal(2000)}, {WETH: 18, USDC: 6})
    state = run_state(0, 10, portfolio)
    points = valuation.equity_curve(state, prices, AaveAccrual(book))
    assert [point.time for point in points] == [0, 10]
    point = points[0]
    assert point.complete is False
    assert point.unvalued == (str(USDC),)
    # 1 WETH wallet plus a 1 WETH supplied claim; USDC is unvalued.
    assert point.value == Decimal(4000)
