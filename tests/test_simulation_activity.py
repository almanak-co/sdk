"""Activity projection: decision timestamps and liquidation events, chronologically merged."""

import json

import pytest

from almanak.reports import simulation_activity

COLLATERAL_ASSET = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
DEBT_ASSET = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def completed(kind="supply", eligible_at=110, decision_time=100, complete=True, target=None):
    intent = {"kind": kind}
    if target is not None:
        intent["target"] = target
    return {
        "pending": {
            "intent": intent,
            "decision_time": decision_time,
            "eligible_at": eligible_at,
        },
        "transition": {
            "complete": complete,
            "effects": {
                "assets": [
                    {
                        "key": {
                            "account": "main",
                            "chain": {"chain_id": "eip155:1", "environment": "backtest"},
                            "asset": COLLATERAL_ASSET,
                        },
                        "amount": -1000,
                    }
                ]
            },
        },
    }


def liquidation_effect():
    return {
        "collateral": {"allocation": "main", "asset": COLLATERAL_ASSET},
        "debt": {"allocation": "main", "asset": DEBT_ASSET},
        "collateral_removed": {"asset": COLLATERAL_ASSET, "amount": 105},
        "debt_restored": {"asset": DEBT_ASSET, "amount": 100},
        "fee_shares": 7,
    }


def event(time, result):
    return {"identity": f"event:{time}", "time": time, "transition": {"result": result}}


def liquidation_event(time, effects=None):
    return event(
        time,
        {
            "kind": "liquidation",
            "effective_at": "2026-01-01T00:00:00+00:00",
            "liquidations": [liquidation_effect()] if effects is None else effects,
            "deficits": [],
            "simulation": {"execution_id": "run:event:0", "model": "economic", "inputs": []},
        },
    )


def test_decision_time_and_eligible_at_project_from_the_pending_entry():
    rows = simulation_activity({"completed": [completed(decision_time=100, eligible_at=110)]})
    assert len(rows) == 1
    row = rows[0]
    assert row["decision_time"] == 100
    assert row["eligible_at"] == 110
    assert row["action"] == "supply" and row["complete"] is True
    assert len(row["asset_changes"]) == 1 and row["asset_changes"][0]["amount"] == "-1000"


def test_runs_without_events_keep_the_intent_only_projection():
    wire = {"completed": [completed(eligible_at=110), completed(kind="withdraw", eligible_at=210)]}
    rows = simulation_activity(wire)
    assert [row["action"] for row in rows] == ["supply", "withdraw"]
    assert all("liquidation" not in row for row in rows)
    assert rows == simulation_activity({**wire, "events": []})


def test_liquidation_rows_carry_effect_facts_and_interleave_chronologically():
    wire = {
        "completed": [
            completed(kind="borrow", eligible_at=120, decision_time=110),
            completed(kind="repay", eligible_at=140, decision_time=130),
        ],
        "events": [liquidation_event(140), liquidation_event(150)],
    }
    rows = simulation_activity(wire)
    assert [row["action"] for row in rows] == ["borrow", "repay", "liquidation", "liquidation"]
    first, second = rows[2], rows[3]
    assert first["eligible_at"] == 140 and second["eligible_at"] == 150
    assert first["complete"] is True
    # The debt target names the liquidated position.
    assert first["scope"] == {"allocation": "main", "asset": DEBT_ASSET}
    assert first["liquidation"] == {
        "collateral_asset": COLLATERAL_ASSET,
        "collateral_removed": "105",
        "debt_asset": DEBT_ASSET,
        "debt_restored": "100",
        "fee_shares": "7",
    }
    # Liquidations change position state, not wallets: no fabricated asset changes.
    assert "asset_changes" not in first


def test_completed_intents_precede_liquidations_at_equal_times():
    wire = {
        "completed": [completed(eligible_at=140)],
        "events": [liquidation_event(140)],
    }
    rows = simulation_activity(wire)
    assert [row["action"] for row in rows] == ["supply", "liquidation"]


def test_one_event_projects_one_row_per_liquidation_effect():
    wire = {
        "events": [liquidation_event(140, effects=[liquidation_effect(), liquidation_effect()])]
    }
    rows = simulation_activity(wire)
    assert len(rows) == 2
    assert all(row["action"] == "liquidation" and row["eligible_at"] == 140 for row in rows)


def test_missing_optional_fields_are_tolerated():
    sparse = {"pending": {"intent": {"kind": "hold"}, "eligible_at": 100}, "transition": {}}
    rows = simulation_activity({"completed": [sparse]})
    assert rows[0]["decision_time"] is None and rows[0]["scope"] is None
    assert rows[0]["complete"] is True and rows[0]["asset_changes"] == []

    effect = liquidation_effect()
    del effect["debt"]
    rows = simulation_activity({"events": [liquidation_event(140, effects=[effect])]})
    assert "scope" not in rows[0]


def test_non_liquidation_events_project_no_rows():
    unavailable = event(
        140,
        {
            "kind": "input_unavailable",
            "effective_at": "2026-01-01T00:00:00+00:00",
            "liquidations": [],
            "deficits": [],
            "simulation": {"execution_id": "run:event:0", "model": "economic", "inputs": []},
            "unavailable": [],
        },
    )
    deficit_only = liquidation_event(150, effects=[])
    assert simulation_activity({"events": [unavailable, deficit_only]}) == []


@pytest.fixture
def reserve():
    from pathlib import Path

    from almanak.connectors.aave_v4.history import Market

    raw = json.loads(
        (Path(__file__).parent / "fixtures/aave-v4-historical-market.json").read_text()
    )
    return Market.model_validate_json(json.dumps(raw["markets"][0])).reserve


def target(reserve, allocation):
    from almanak.lending import SupplyTarget
    from almanak.models import Account

    return SupplyTarget(
        allocation=allocation,
        account="fund",
        fund=Account(chain=reserve.source.chain, address="0x" + "aa" * 20),
        reserve=reserve.source,
    )


def test_journal_serialized_liquidation_projects_exact_facts(reserve):
    """The projection consumes the journal's JSON wire, not the in-memory models."""
    from almanak.backtest.economic_state import (
        EconomicEvent,
        EconomicPosition,
        LiquidationEffect,
    )
    from almanak.backtest.engine import RecordedEvent, Transition
    from almanak.backtest.types import UnixTime
    from almanak.caip import AssetType
    from almanak.effects import Effects
    from almanak.models import AssetAmount

    debt_source = reserve.source.model_copy(
        update={"reserve_id": 7, "asset": AssetType(DEBT_ASSET)}
    )
    effect = LiquidationEffect(
        collateral=target(reserve, "weth"),
        debt=target(reserve.model_copy(update={"source": debt_source}), "usdc"),
        collateral_removed=AssetAmount(asset=AssetType(COLLATERAL_ASSET), amount=10**18),
        debt_restored=AssetAmount(asset=AssetType(DEBT_ASSET), amount=900_000_000),
        fee_shares=5,
    )
    from datetime import UTC, datetime

    result = EconomicEvent(
        kind="liquidation",
        effective_at=datetime(2026, 1, 1, 0, 2, 20, tzinfo=UTC),
        liquidations=(effect,),
        deficits=(),
        simulation={"execution_id": "run:event:0", "model": "economic", "inputs": ()},
    )
    recorded = RecordedEvent(
        identity="run:event:0",
        time=UnixTime(1_767_225_740),
        transition=Transition(effects=Effects[EconomicPosition](), result=result),
    )
    wire = json.loads(recorded.model_dump_json())
    rows = simulation_activity({"events": [wire]})
    assert rows == [
        {
            "action": "liquidation",
            "eligible_at": 1_767_225_740,
            "complete": True,
            "scope": wire["transition"]["result"]["liquidations"][0]["debt"],
            "liquidation": {
                "collateral_asset": COLLATERAL_ASSET,
                "collateral_removed": str(10**18),
                "debt_asset": DEBT_ASSET,
                "debt_restored": "900000000",
                "fee_shares": "5",
            },
        }
    ]
