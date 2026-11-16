"""The HyperCore backtest model (phase 5): account arithmetic, the venue's refusals and the
reference lifecycle through the economic CLI with the sandboxed strategy.

The market history is the recorded BTC answers of 2026-11-04 (`hypercore_backtest_support`);
the reference figures are those of the real run made with the same dataset.
"""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.backtest import economic_cli
from almanak.backtest.engine import Pending
from almanak.connectors.hyperliquid import perp_math as math
from almanak.connectors.hyperliquid.economic_model import NO_MATCH, position_key
from almanak.connectors.hyperliquid.economic_state import Holding, HyperCoreAccount
from almanak.connectors.hyperliquid.models import (
    ClosePosition,
    Deposit,
    IncreasePosition,
    SetLeverage,
    Withdraw,
)
from almanak.triggers import IntervalEvent
from tests import hypercore_backtest_support as support

T0 = support.START + 120


def account(config, **values) -> HyperCoreAccount:
    return HyperCoreAccount(target=config.target("Main"), **values)


def holding(szi, entry="77000") -> Holding:
    return Holding(szi=szi, entry_px=entry, changed_at=support.START)


@pytest.fixture
def world(tmp_path):
    config, settings = support.parameters(), support.settings()
    book = support.prepared(tmp_path, config, settings)
    yield config, settings, book, support.model(config, settings, book)
    book.close()


def execute(world, intent, payload=None, wallet=None):
    config, settings, _, driver = world
    state = support.seed(config, settings)
    if payload is not None or wallet is not None:
        state = with_account(state, payload, wallet)
    pending = Pending[type(intent)](
        identity="run:0",
        decision_identity="run:decision:0",
        trigger=IntervalEvent(started_at=state.time.datetime(), due_at=state.time.datetime()),
        intent=intent,
        decision_time=state.time,
        eligible_at=T0,
    )
    return driver.execute(state, pending)


def with_account(state, payload, wallet):
    portfolio = state.portfolio
    if payload is not None:
        (held,) = portfolio.positions
        portfolio = portfolio.model_copy(
            update={"positions": (held.model_copy(update={"payload": payload}),)}
        )
    if wallet is not None:
        (balance,) = portfolio.balances
        portfolio = portfolio.model_copy(
            update={"balances": (balance.model_copy(update={"amount": wallet}),)}
        )
    return state.model_copy(update={"portfolio": portfolio})


def target(world):
    return world[0].target("Main")


def test_the_first_deposit_pays_the_activation_fee(world):
    result = execute(world, Deposit(target=target(world), amount=30_000_000)).result
    assert result.success and result.after.activated
    assert (result.after.perp_usdc, result.after.activation_fee) == (29_000_000, 1_000_000)
    activated = account(world[0], activated=True, perp_usdc=5)
    again = execute(world, Deposit(target=target(world), amount=1_000_000), activated).result
    assert again.after.perp_usdc == 1_000_005 and again.after.activation_fee == 0


@pytest.mark.parametrize(
    ("amount", "wallet", "error"),
    [
        (30_000_001, None, "holds less USDC"),
        (1_000_000, None, "must exceed the 1 USDC activation fee"),
    ],
)
def test_deposits_the_fund_cannot_make_are_refused(world, amount, wallet, error):
    transition = execute(world, Deposit(target=target(world), amount=amount), wallet=wallet)
    assert not transition.result.success and error in transition.result.error
    assert transition.effects.assets == () and transition.effects.positions == ()


def test_leverage_needs_an_account_and_a_flat_position(world):
    leverage = SetLeverage(target=target(world), leverage=10)
    assert "no account" in execute(world, leverage).result.error
    flat = account(world[0], activated=True, perp_usdc=10)
    assert execute(world, leverage, flat).result.after.leverage == 10
    held = account(world[0], activated=True, perp_usdc=10, leverage=10, holding=holding("0.001"))
    assert "only while the account holds no position" in execute(world, leverage, held).result.error


def test_another_leverage_is_outside_the_authority(world):
    with pytest.raises(ValueError, match="configured cross leverage"):
        execute(world, SetLeverage(target=target(world), leverage=20))


def entry(world, side="long", size="0.002"):
    return IncreasePosition(target=target(world), side=side, size=size)


def ready(world, **values):
    return account(world[0], activated=True, perp_usdc=29_000_000, leverage=10, **values)


def test_an_entry_fills_at_the_modeled_price_and_pays_the_taker_fee(world):
    result = execute(world, entry(world, size="0.0021234"), ready(world)).result
    # The latest close at 00:02:00 is 77397; the fill model moves it 2 bps against the
    # buy, rounded on the price grid; the size rounds down to five decimals.
    assert result.fill.model_dump() == {
        "side": "buy",
        "size": "0.00212",
        "price": "77413",
        "reference": "77397",
        "limit": "77783",
        "fee": math.fee(Decimal("0.00212"), Decimal(77413), Decimal("4.5")),
        "closed_pnl": 0,
    }
    assert result.after.holding.szi == "0.00212" and result.after.holding.changed_at == T0


@pytest.mark.parametrize(
    ("payload", "intent", "error"),
    [
        ({"leverage": None}, {}, "configured cross leverage set first"),
        ({"holding": holding("-0.001")}, {}, "adds on its own side"),
        ({"perp_usdc": 10_000_000}, {"size": "0.002"}, "Insufficient margin"),
        ({"perp_usdc": 10**12}, {"size": "1.3"}, "exceed what the model liquidates"),
        ({}, {"size": "0.000001"}, "rounds to nothing at the market's size decimals"),
        # 0.00001 BTC at its 77,783 limit is worth less than the venue's 10 USDC minimum.
        ({}, {"size": "0.00001"}, "Order must have minimum value of $10"),
    ],
)
def test_entries_the_venue_or_gateway_would_refuse(world, payload, intent, error):
    values = {"activated": True, "perp_usdc": 29_000_000, "leverage": 10, **payload}
    result = execute(world, entry(world, **intent), account(world[0], **values)).result
    assert not result.success and error in result.error and result.after == result.before


@pytest.mark.parametrize(("size", "admitted"), [("0.0146", True), ("0.01461", False)])
def test_initial_margin_counts_the_fills_fee_and_slippage(tmp_path, size, admitted):
    # 29 USDC at 40x: the fill at 77,413 against the 77,397 reference loses 16 USDC per BTC
    # and pays the fee, so 0.01461 BTC leaves equity below its initial margin.
    config, settings = support.parameters(leverage=40), support.settings()
    book = support.prepared(tmp_path, config, settings)
    try:
        world = (config, settings, book, support.model(config, settings, book))
        payload = account(config, activated=True, perp_usdc=29_000_000, leverage=40)
        result = execute(world, entry(world, size=size), payload).result
    finally:
        book.close()
    assert result.success == admitted
    if admitted:
        reference = Decimal(result.fill.reference)
        assert math.account_value(result.after, reference) >= math.initial_margin(
            result.after, reference
        )
    else:
        assert "Insufficient margin" in result.error


def test_an_ioc_past_its_limit_does_not_fill(tmp_path):
    config, settings = support.parameters(slippage=1), support.settings(slippage_bps="5")
    book = support.prepared(tmp_path, config, settings)
    try:
        world = (config, settings, book, support.model(config, settings, book))
        result = execute(world, entry(world), ready(world)).result
        assert result.error == NO_MATCH and result.fill is None
    finally:
        book.close()


def test_close_and_withdraw_need_something_to_move(world):
    assert (
        "no position"
        in execute(world, ClosePosition(target=target(world)), ready(world)).result.error
    )
    held = ready(world, holding=holding("0.002", "77397"))
    result = execute(world, Withdraw(target=target(world), amount=29_000_000), held).result
    assert "exceeds the" in result.error
    allowed = result.error.split("the ")[1].split(" ")[0]
    assert int(allowed) == math.micro(math.withdrawable(held, Decimal(77397)))


def test_liquidation_price_meets_maintenance_margin(world):
    config = world[0]
    long = account(config, perp_usdc=28_512_298, leverage=40, holding=holding("0.014", "77413"))
    price = math.liquidation_price(long)
    assert abs(math.account_value(long, price) - math.maintenance_margin(long, price)) < Decimal(
        "1e-20"
    )
    assert math.under_maintenance(long, price - 1) and not math.under_maintenance(long, price + 1)
    short = account(config, perp_usdc=28_000_000, leverage=40, holding=holding("-0.014", "77413"))
    price = math.liquidation_price(short)
    assert math.under_maintenance(short, price + 1) and not math.under_maintenance(short, price - 1)
    backed = account(config, perp_usdc=10**10, leverage=1, holding=holding("0.001"))
    assert math.liquidation_price(backed) is None


@pytest.mark.parametrize(
    ("start", "signed", "price", "after", "entry", "pnl"),
    [
        ("0.002", "0.002", "78000", "0.004", "77500", 0),
        ("0.002", "-0.001", "78000", "0.001", "77000", 1_000_000),
        ("0.002", "-0.003", "76000", "-0.001", "76000", -2_000_000),
        ("-0.002", "0.002", "76000", None, None, 2_000_000),
    ],
)
def test_trades_settle_closed_pnl_and_entry_price(world, start, signed, price, after, entry, pnl):
    before = account(world[0], perp_usdc=10_000_000, leverage=10, holding=holding(start))
    changed, closed = math.trade(before, Decimal(signed), Decimal(price), 7, T0)
    assert closed == pnl and changed.perp_usdc == 10_000_000 + pnl - 7 and changed.fees == 7
    if after is None:
        assert changed.holding is None
    else:
        assert (changed.holding.szi, Decimal(changed.holding.entry_px)) == (after, Decimal(entry))


def test_prices_round_against_the_taker_on_the_venue_grid():
    assert math.moved(Decimal(77397), True, Decimal(2), 5) == Decimal(77413)
    assert math.moved(Decimal(77397), False, Decimal(2), 5) == Decimal(77381)
    assert math.on_grid(Decimal("1.234567"), 0, "ROUND_UP") == Decimal("1.2346")
    assert math.moved(Decimal(100), True, Decimal(0), 5) == Decimal(100)


def run_reference(tmp_path, config):
    settings = support.settings()
    dataset = tmp_path / "dataset"
    support.prepared(dataset, config, settings).close()
    args = SimpleNamespace(
        strategy=support.REFERENCE / "strategy.py",
        config=None,
        backtest_config=None,
        dataset=dataset,
        output=tmp_path / "run",
        _backtest_inputs=None,
    )
    return economic_cli.create(args, config, settings), args.output


def test_the_reference_lifecycle_runs_and_reproduces_offline(tmp_path):
    report, run = run_reference(tmp_path, support.parameters())
    assert report["closed"] and report["provider_requests_during_execution"] == 0
    assert [(a["kind"], a["effective_at"][11:19]) for a in report["actions"]] == [
        ("deposit", "00:00:01"),
        ("set_leverage", "00:01:01"),
        ("increase_position", "00:02:01"),
        ("close_position", "02:03:01"),
        ("withdraw", "02:04:01"),
    ]
    assert [e["payment"] for e in report["events"]] == [-1931, -1926]
    account = report["allocations"][0]["account"]
    assert (account["activation_fee"], account["fees"], account["realized_pnl"]) == (
        1_000_000,
        139_017,
        -726_000,
    )
    assert report["net_result"] == -1_868_874 == report["end_equity"] - 30_000_000
    assert economic_cli.reproduce(SimpleNamespace(run=run))["reproduced"]


def test_a_position_the_venue_liquidates_ends_the_hold(tmp_path):
    report, _ = run_reference(tmp_path, support.parameters(leverage=40, size="0.014", hold="4h"))
    (liquidation,) = [e for e in report["events"] if e["kind"] == "liquidation"]
    assert liquidation["effective_at"] == "2026-11-04T03:00:00Z"
    assert Decimal(liquidation["trigger"]).quantize(Decimal("0.01")) == Decimal("76332.49")
    assert (
        liquidation["fill"]["price"] == "76332" and liquidation["fill"]["closed_pnl"] == -15_134_000
    )
    assert [a["kind"] for a in report["actions"]][-1] == "withdraw"
    assert report["allocations"][0]["account"]["withdrawn"] == 12_870_413
    assert report["closed"]


def test_position_keys_name_the_allocation_and_market(world):
    key = position_key(target(world))
    assert key.reference == "allocation:Main:BTC" and str(key.connector) == "hyperliquid"
    assert json.loads(key.model_dump_json())["account"] == "fund"


def reference_namespace():
    """The reference strategy loaded as a module, so its own file is the one measured."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("reference", support.REFERENCE / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return vars(module)


def in_process(request):
    """The worker's own evaluation of the strategy's hooks, in this process."""
    from almanak.connectors.composition import connector_for
    from almanak.strategy import EconomicDecisionRequest
    from almanak.strategy_worker import evaluate

    connector = connector_for(request.context.config)
    binding = connector.backtest("economic")
    kind = "decision" if isinstance(request, EconomicDecisionRequest) else "outcome"
    typed = binding.request_type(kind).model_validate_json(request.model_dump_json())
    return evaluate(reference_namespace(), typed, connector.codec, binding).model_dump_json()


@pytest.mark.parametrize(
    ("values", "kinds", "liquidations"),
    [
        ({}, ["deposit", "set_leverage", "increase_position", "close_position", "withdraw"], 0),
        (
            {"leverage": 40, "size": "0.014", "hold": "4h"},
            ["deposit", "set_leverage", "increase_position", "withdraw"],
            1,
        ),
        (
            {"side": "short", "hold": None},
            ["deposit", "set_leverage", "increase_position", "close_position", "withdraw"],
            0,
        ),
    ],
)
def test_the_reference_hooks_drive_the_model_in_process(
    tmp_path, monkeypatch, values, kinds, liquidations
):
    from almanak.connectors.hyperliquid import economic_model
    from almanak.connectors.hyperliquid.economic_reporting import report

    monkeypatch.setattr(economic_model, "strategy_worker", in_process)
    config, settings = support.parameters(**values), support.settings()
    book = support.prepared(tmp_path / "dataset", config, settings)
    try:
        driver = support.model(config, settings, book)
        state = support.run(tmp_path / "run.sqlite", driver, support.seed(config, settings))
        result = report(state, driver)
    finally:
        book.close()
    assert [a["kind"] for a in result["actions"]] == kinds and result["closed"]
    assert sum(e["kind"] == "liquidation" for e in result["events"]) == liquidations
    assert result["end_equity"] == result["allocations"][0]["wallet_usdc"]
