"""Multi-series charts, markers, callouts and lifecycle steps."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.dashboard import build as b
from almanak.dashboard.models import LineChart, Value
from almanak.models import Block, Chain

ROOT = Path(__file__).parents[1]
CHAIN = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
BLOCK = Block(chain=CHAIN, number=7, hash="0x" + "cd" * 32, timestamp=1792700000)
USDC = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")


def price(id_, amount):
    return b.usd(id_, amount, "pool mid price", price_source="Uniswap V3 WETH/USDC 0.05% mid")


def rsi(id_, amount):
    return b.value(id_, amount, "count", reason="RSI(14) of hourly closes", places=0)


def sample_chart():
    times = [1792600000 + 3600 * i for i in range(6)]
    prices = b.series(
        "price", "WETH", [(t, price(f"p{i}", 3000 + 10 * i)) for i, t in enumerate(times)]
    )
    indicator = b.series(
        "rsi", "RSI 14", [(t, rsi(f"r{i}", 40 + i)) for i, t in enumerate(times)], panel="lower"
    )
    return b.line_chart(
        "replay",
        "When it would have traded",
        [prices, indicator],
        caption="Signal replay over recent closes; not a forecast.",
        references=[b.reference("buy below 30", rsi("buy", 30), "success")],
        lower_marks=["buy below 30"],
        markers=[
            b.marker(times[2], "buy", "success", "price"),
            b.marker(times[4], "sell", "danger"),
        ],
    )


def test_points_and_series_are_exclusive_and_markers_name_known_series():
    chart = sample_chart()
    with pytest.raises(ValidationError, match="either points"):
        LineChart(**{**chart.model_dump(), "points": chart.series[0].points})
    with pytest.raises(ValidationError, match="unknown series"):
        b.line_chart(
            "x", "t", chart.series, caption="", markers=[b.marker(1, "buy", series="nope")]
        )


def test_steps_name_unknown_states_in_the_error():
    flow = b.steps(
        "flow",
        "How it behaves",
        [("supply", "Supply WBTC"), ("loop", "Borrow and re-supply"), ("hold", "Hold")],
        [
            ("supply", "loop", "start"),
            ("loop", "loop", "round 2 if safe"),
            ("loop", "hold", "done"),
        ],
    )
    assert [s.id for s in flow.states] == ["supply", "loop", "hold"]
    with pytest.raises(ValidationError, match="unknown state"):
        b.steps("flow", "t", [("a", "A")], [("a", "exit", "unwind")])


def test_durations_read_as_config_yaml_reads_them():
    assert (b.seconds("7d"), b.seconds("5m"), b.seconds(60)) == (604_800, 300, 60)
    for unread in ("2w", "60", " 5m"):
        with pytest.raises(ValueError, match="duration"):
            b.seconds(unread)


@pytest.mark.parametrize(
    ("change", "refusal"),
    [
        ({"price_source": ""}, "usd values require price_source"),
        ({"unit": "ratio"}, "price_source is only for usd and price"),
        ({"quote": str(USDC)}, "only a price has one"),
        ({"symbol": "USDC"}, "symbol labels token amounts"),
        ({"amount": None, "reason": ""}, "missing values require an explicit reason"),
        ({"reason": ""}, "require a calculation explanation"),
        ({"unit": "ratio", "price_source": "", "asset": str(USDC)}, "non-asset units"),
    ],
)
def test_a_value_refuses_fields_its_unit_does_not_carry(change, refusal):
    valid = b.usd("x", 1, "r", price_source="feed").model_dump(mode="json", exclude_none=True)
    Value.model_validate_json(json.dumps(valid))
    with pytest.raises(ValidationError, match=refusal):
        Value.model_validate_json(json.dumps({**valid, **change}))
