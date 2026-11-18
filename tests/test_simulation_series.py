"""Series projection: per-step portfolio replay from retained checkpoint and manifest facts."""

import json

import pytest

from almanak.reports import simulation_series

CHAIN = {"chain_id": "eip155:1", "environment": "backtest"}
WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def wallet_key(asset=WETH, account="main"):
    return {"account": account, "chain": CHAIN, "asset": asset}


def position_key(reference="0xspoke/1", account="main"):
    return {"connector": "aave_v4", "chain": CHAIN, "account": account, "reference": reference}


def payload(shares, supplied=0, withdrawn=0):
    return {
        "target": {"allocation": "main"},
        "shares": shares,
        "supplied": supplied,
        "withdrawn": withdrawn,
    }


def position(reference="0xspoke/1", shares=0, supplied=0, withdrawn=0):
    return {"key": position_key(reference), "payload": payload(shares, supplied, withdrawn)}


def delta(amount, asset=WETH, account="main"):
    return {"key": wallet_key(asset, account), "amount": amount}


def open_change(after):
    return {"before": None, "after": after}


def update_change(before, after):
    return {"before": before, "after": after}


def close_change(before):
    return {"before": before, "after": None}


def completed(kind="supply", eligible_at=110, decision_time=100, assets=(), positions=()):
    return {
        "pending": {
            "intent": {"kind": kind},
            "decision_time": decision_time,
            "eligible_at": eligible_at,
        },
        "transition": {
            "complete": True,
            "effects": {"assets": list(assets), "positions": list(positions)},
        },
    }


def event(time, assets=(), positions=()):
    return {
        "identity": f"run:event:{time}",
        "time": time,
        "transition": {
            "effects": {"assets": list(assets), "positions": list(positions)},
            "result": {},
        },
    }


def wire(completed_steps=(), events=(), portfolio=None, time=900, start=100):
    return {
        "schedule": {"start": start, "end": 900},
        "time": time,
        "completed": list(completed_steps),
        "events": list(events),
        "portfolio": portfolio if portfolio is not None else {"balances": [], "positions": []},
    }


def manifest(portfolio=None, funds=None):
    document = {}
    if portfolio is not None:
        document["initial"] = {"portfolio": portfolio, "schedule": {"start": 100}}
    if funds is not None:
        document["settings"] = {"funds": funds}
    return document


def initial_portfolio(balances=(), positions=()):
    return {"balances": list(balances), "positions": list(positions)}


def balance(amount, asset=WETH, account="main"):
    return {"key": wallet_key(asset, account), "amount": amount}


def by_key(entry):
    """The projection sorts wallet entries by their full sorted key JSON."""
    return json.dumps(entry["key"], sort_keys=True, separators=(",", ":"))


def emitted(entries):
    """Expected wallet entries: artifact amounts serialize as decimal strings."""
    return [{**entry, "amount": str(entry["amount"])} for entry in entries]


def test_initial_step_final_replay_matches_the_checkpoint_portfolio():
    opened = position(shares=400, supplied=400)
    grown = position(shares=250, supplied=250, withdrawn=150)
    steps = [
        completed(assets=[delta(-400)], positions=[open_change(opened)]),
        completed(
            kind="withdraw",
            eligible_at=210,
            decision_time=200,
            assets=[delta(150)],
            positions=[update_change(opened, grown)],
        ),
    ]
    final = initial_portfolio(balances=[balance(750)], positions=[grown])
    series = simulation_series(
        wire(steps, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )

    assert series["complete"] is True
    assert [point["source"] for point in series["points"]] == ["initial", "step", "step", "final"]
    initial, supply, withdraw, last = series["points"]
    assert initial == {"time": 100, "source": "initial", "wallets": emitted([balance(1000)])}
    assert supply["action"] == "supply" and supply["decision_time"] == 100
    assert supply["wallets"] == emitted([balance(600)])
    assert supply["positions"] == [{"target": opened["key"], "payload": opened["payload"]}]
    assert withdraw["action"] == "withdraw" and withdraw["decision_time"] == 200
    assert withdraw["wallets"] == emitted([balance(750)])
    assert withdraw["positions"] == [{"target": grown["key"], "payload": grown["payload"]}]
    assert last == {
        "time": 900,
        "source": "final",
        "wallets": emitted([balance(750)]),
        "positions": [{"target": grown["key"], "payload": grown["payload"]}],
    }


def test_initial_portfolio_falls_back_to_the_funds_inventory():
    funds = [
        {
            "name": "main",
            "fund": {"chain": CHAIN, "address": "0x" + "aa" * 20},
            "balances": [
                {"asset": WETH, "amount": 1000},
                {"asset": USDC, "amount": 250},
            ],
        }
    ]
    final = initial_portfolio(balances=[balance(1000), balance(250, USDC)])
    series = simulation_series(wire(portfolio=final), manifest(funds=funds))

    assert series["complete"] is True
    initial = series["points"][0]
    assert initial["source"] == "initial" and initial["time"] == 100
    assert initial["wallets"] == emitted(sorted([balance(1000), balance(250, USDC)], key=by_key))
    # Funds carry no positions; the empty list is omitted.
    assert "positions" not in initial


def test_manifest_initial_state_wins_over_the_funds_inventory():
    funds = [
        {
            "name": "main",
            "fund": {"chain": CHAIN, "address": "0x" + "aa" * 20},
            "balances": [{"asset": WETH, "amount": 5}],
        }
    ]
    final = initial_portfolio(balances=[balance(1000)])
    series = simulation_series(
        wire(portfolio=final), manifest(initial_portfolio([balance(1000)]), funds=funds)
    )
    assert series["points"][0]["wallets"] == emitted([balance(1000)])


def test_missing_manifest_emits_only_the_final_point_marked_incomplete():
    final = initial_portfolio(balances=[balance(750)])
    series = simulation_series(wire([completed()], portfolio=final), None)

    assert series["complete"] is False
    assert series["points"] == [
        {"time": 900, "source": "final", "wallets": emitted([balance(750)])}
    ]


def test_deltas_accumulate_across_assets_and_accounts():
    steps = [
        completed(assets=[delta(-100), delta(-40, USDC), delta(-7, WETH, "reserve")]),
        completed(
            kind="withdraw", eligible_at=210, assets=[delta(30, USDC), delta(7, WETH, "reserve")]
        ),
    ]
    final = initial_portfolio(
        balances=[balance(900), balance(240, USDC), balance(50, WETH, "reserve")]
    )
    initial = initial_portfolio(
        balances=[balance(1000), balance(250, USDC), balance(50, WETH, "reserve")]
    )
    series = simulation_series(wire(steps, portfolio=final), manifest(initial))

    assert series["complete"] is True
    _, first, second, _ = series["points"]
    # Wallet entries sort by their serialized key for deterministic output.
    assert first["wallets"] == emitted(
        sorted([balance(900), balance(210, USDC), balance(43, WETH, "reserve")], key=by_key)
    )
    assert second["wallets"] == emitted(
        sorted([balance(900), balance(240, USDC), balance(50, WETH, "reserve")], key=by_key)
    )


def test_position_close_removes_the_entry_and_zero_balances_mark_exits():
    opened = position(shares=400, supplied=400)
    steps = [
        completed(assets=[delta(-400)], positions=[open_change(opened)]),
        completed(
            kind="withdraw", eligible_at=210, assets=[delta(400)], positions=[close_change(opened)]
        ),
        completed(kind="supply", eligible_at=310, assets=[delta(-1000)]),
    ]
    final = initial_portfolio(balances=[balance(0)])
    series = simulation_series(
        wire(steps, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )

    assert series["complete"] is True
    closed = series["points"][2]
    assert "positions" not in closed
    # Zero-amount wallet entries are kept: they mark exited assets.
    assert series["points"][3]["wallets"] == emitted([balance(0)])
    assert "positions" not in series["points"][3]
    assert series["points"][-1] == {
        "time": 900,
        "source": "final",
        "wallets": emitted([balance(0)]),
    }


def test_event_effects_fold_into_the_running_state_without_their_own_point():
    opened = position(shares=400, supplied=400)
    seized = position(shares=100, supplied=100, withdrawn=300)
    steps = [
        completed(assets=[delta(-400)], positions=[open_change(opened)]),
        completed(kind="withdraw", eligible_at=210, decision_time=200, assets=[delta(300)]),
    ]
    events = [event(150, positions=[update_change(opened, seized)])]
    final = initial_portfolio(balances=[balance(900)], positions=[seized])
    series = simulation_series(
        wire(steps, events, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )

    assert series["complete"] is True
    assert [point["source"] for point in series["points"]] == ["initial", "step", "step", "final"]
    # The liquidation between the intents is visible in the next step point.
    assert series["points"][2]["positions"] == [
        {"target": seized["key"], "payload": seized["payload"]}
    ]


def test_events_apply_before_intents_recorded_at_equal_times():
    opened = position(shares=400, supplied=400)
    reopened = position(shares=50, supplied=50)
    steps = [
        completed(assets=[delta(-400)], positions=[open_change(opened)]),
        # The intent reopens the position the same-time event just closed.
        completed(
            kind="supply",
            eligible_at=150,
            decision_time=140,
            assets=[delta(-50)],
            positions=[open_change(reopened)],
        ),
    ]
    events = [event(150, positions=[close_change(opened)])]
    final = initial_portfolio(balances=[balance(550)], positions=[reopened])
    series = simulation_series(
        wire(steps, events, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )

    assert series["complete"] is True
    assert series["points"][2]["positions"] == [
        {"target": reopened["key"], "payload": reopened["payload"]}
    ]


def test_step_points_are_chronological_even_when_retained_rows_are_not():
    steps = [
        completed(kind="withdraw", eligible_at=210, decision_time=200),
        completed(eligible_at=110, decision_time=100),
    ]
    final = initial_portfolio(balances=[balance(1000)])
    series = simulation_series(
        wire(steps, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )
    assert [point["time"] for point in series["points"]] == [100, 110, 210, 900]


def test_replay_drift_marks_the_series_incomplete_but_keeps_the_points():
    steps = [completed(assets=[delta(-400)])]
    # The checkpoint portfolio disagrees with the replayed 600.
    final = initial_portfolio(balances=[balance(999)])
    series = simulation_series(
        wire(steps, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )

    assert series["complete"] is False
    assert [point["source"] for point in series["points"]] == ["initial", "step", "final"]
    assert series["points"][1]["wallets"] == emitted([balance(600)])
    # The final point still carries the checkpoint portfolio verbatim.
    assert series["points"][2]["wallets"] == emitted([balance(999)])


def test_unknown_wallet_keys_and_negative_balances_mark_incomplete():
    steps = [completed(assets=[delta(-400, USDC), delta(-1200)])]
    final = initial_portfolio(balances=[balance(-200), balance(-400, USDC)])
    series = simulation_series(
        wire(steps, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )
    assert series["complete"] is False
    assert len(series["points"]) == 3


def test_missing_or_unknown_effect_shapes_mark_incomplete_without_throwing():
    sparse = {"pending": {"intent": {"kind": "hold"}, "eligible_at": 110}, "transition": {}}
    weird = completed(positions=[{"after": {"payload": payload(1)}}])
    final = initial_portfolio(balances=[balance(1000)])
    series = simulation_series(
        wire([sparse, weird], portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )
    assert series["complete"] is False
    assert [point["source"] for point in series["points"]] == ["initial", "step", "step", "final"]


def test_conflicting_position_before_state_marks_incomplete():
    opened = position(shares=400, supplied=400)
    other = position(shares=1, supplied=1)
    steps = [
        completed(assets=[delta(-400)], positions=[open_change(opened)]),
        # The recorded before state does not match the owned position.
        completed(kind="withdraw", eligible_at=210, positions=[update_change(other, None)]),
    ]
    final = initial_portfolio(balances=[balance(600)])
    series = simulation_series(
        wire(steps, portfolio=final), manifest(initial_portfolio([balance(1000)]))
    )
    assert series["complete"] is False
    assert len(series["points"]) == 4


@pytest.fixture
def reserve():
    from pathlib import Path

    from almanak.connectors.aave_v4.history import Market

    raw = json.loads(
        (Path(__file__).parent / "fixtures/aave-v4-historical-market.json").read_text()
    )
    return Market.model_validate_json(json.dumps(raw["markets"][0])).reserve


def test_journal_serialized_effects_replay_verbatim_payloads(reserve):
    """The projection consumes the journal's JSON wire, not the in-memory models."""
    from almanak.connectors.aave_v4.backtest import AavePosition, position_key
    from almanak.effects import (
        AssetDelta,
        Balance,
        Effects,
        Portfolio,
        Position,
        PositionChange,
        WalletAsset,
    )
    from almanak.lending import SupplyTarget
    from almanak.models import Account

    target = SupplyTarget(
        allocation="main",
        account="main",
        fund=Account(chain=reserve.source.chain, address="0x" + "aa" * 20),
        reserve=reserve.source,
    )
    key = position_key(target)
    wallet = WalletAsset(account="main", chain=target.fund.chain, asset=target.asset)
    opened = Position[AavePosition](
        key=key, payload=AavePosition(target=target, shares=400, supplied=400)
    )
    effects = Effects[AavePosition](
        assets=(AssetDelta(key=wallet, amount=-400),),
        positions=(PositionChange(before=None, after=opened),),
    )
    initial = Portfolio[AavePosition](balances=(Balance(key=wallet, amount=1000),))
    final = Portfolio[AavePosition](
        balances=(Balance(key=wallet, amount=600),), positions=(opened,)
    )

    step = completed(
        assets=[delta(-400)], positions=[open_change(position(shares=400, supplied=400))]
    )
    step["transition"]["effects"] = json.loads(effects.model_dump_json())
    document = wire([step], portfolio=json.loads(final.model_dump_json()))
    series = simulation_series(document, manifest(json.loads(initial.model_dump_json())))

    assert series["complete"] is True
    supply = series["points"][1]
    expected_key = json.loads(key.model_dump_json())
    expected_payload = json.loads(opened.payload.model_dump_json())
    assert supply["positions"] == [{"target": expected_key, "payload": expected_payload}]
    assert supply["positions"][0]["payload"]["shares"] == 400
    assert series["points"][-1]["positions"] == [
        {"target": expected_key, "payload": expected_payload}
    ]
