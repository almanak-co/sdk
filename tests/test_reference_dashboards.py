"""Every reference dashboard draws over a recorded fork-test context and its edited variants."""

import copy
import importlib.util
import json
from collections.abc import Mapping
from functools import cache
from pathlib import Path

import pytest
from pydantic import create_model

from almanak import dashboard as ui
from almanak.dashboard.evaluate import Request, execute
from almanak.dashboard.models import Context, Dashboard
from almanak.strategy import StrategyDefinition
from almanak.typed_parameters import TypedParameters, typed

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/reference-dashboards"

# Each reference and the recorded context it draws over: the Ethereum lending references
# draw over their Base twin's recording, whose shape is the same.
REFERENCES = {
    "aave_v3_arbitrum_lending": "aave_v3_arbitrum_lending",
    "aave_v4_weth_supply": "aave_v4_weth_supply",
    "aerodrome_base_volatile_lp": "aerodrome_base_volatile_lp",
    "aerodrome_slipstream_base_lp": "aerodrome_slipstream_base_lp",
    "arbitrum_aave_borrow_lp": "arbitrum_aave_borrow_lp",
    "arbitrum_aave_leverage_loop": "arbitrum_aave_leverage_loop",
    "compound_v3_base_lending": "compound_v3_base_lending",
    "compound_v3_ethereum_lending": "compound_v3_base_lending",
    "curve_arbitrum_stable_lp": "curve_arbitrum_stable_lp",
    "gmx_v2_arbitrum_liquidity": "gmx_v2_arbitrum_liquidity",
    "gmx_v2_arbitrum_position": "gmx_v2_arbitrum_position",
    "morpho_blue_base_lending": "morpho_blue_base_lending",
    "morpho_blue_ethereum_lending": "morpho_blue_base_lending",
    "uniswap_v3_arbitrum_lp": "uniswap_v3_arbitrum_lp",
    "uniswap_v3_base_rsi": "uniswap_v3_base_rsi",
    "uniswap_v4_arbitrum_lp": "uniswap_v4_arbitrum_lp",
}

# Edits that reach the other status branches. `memory` and `parameters` merge into the
# recording's; `latest` merges into the newest history row (None removes a field); `rows`
# keeps that many of the oldest history rows; `prices` replaces the declared series;
# `inventory` sets position inventory amounts by value id; `datasets` drops those tables.
GMX_ETH_CLOSES = {
    "asset": "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "timeframe": "1h",
    "points": [
        {"time": 1793640000 + 3600 * i, "value": value}
        for i, value in enumerate(("2301.5", "2364.2", "2402.9"))
    ],
}
BRANCHES: dict[str, dict[str, dict]] = {
    "aave_v4_weth_supply": {
        "healthy": {"latest": {"main_usdc_1:health": "3.5", "main_usdc:health": "2.6"}},
        "below_floor": {"latest": {"main_usdc_1:health": "1.4"}},
        "no_debt": {
            "latest": {"main_usdc_1:health": None, "main_usdc_1:debt": "0", "main_usdc:debt": "0"},
            "datasets": ["debt_positions"],
        },
        "debt_without_health": {"latest": {"main_usdc_1:health": None, "main_usdc_1:debt": "5"}},
        "one_debt_unread": {
            "latest": {"main_usdc_1:health": "3.5", "main_usdc:health": None, "main_usdc:debt": "5"}
        },
    },
    "aerodrome_base_volatile_lp": {
        "providing": {
            "latest": {
                "shares": "1000",
                "position0": "0.05",
                "position1": "100",
                "fees0": "0.001",
                "fees1": "1.5",
            }
        },
        "missing_readings": {"latest": {"price": None, "value": None, "hold": None}},
        "no_tables": {"datasets": ["exchange_inventory", "pool_shares_and_fees", "payer_costs"]},
    },
    "aerodrome_slipstream_base_lp": {
        "in_range": {"latest": {"range_lower": "2000", "range_upper": "2500"}},
        "out_of_range": {"latest": {"range_lower": "1800", "range_upper": "2100"}},
        "no_readings": {"latest": {"price": None, "value": None, "hold": None}},
        "no_tables": {"datasets": ["exchange_inventory", "payer_costs"]},
    },
    "arbitrum_aave_borrow_lp": {
        "shortfall": {"memory": {"shortfall": "Repay needs 3 USDC more."}},
        "in_range": {
            "memory": {"unwinding": False},
            "latest": {
                "liquidity:price": "2250",
                "liquidity:range_lower": "2000",
                "liquidity:range_upper": "2500",
                "liquidity:position0": "0.01",
                "liquidity:position1": "20",
            },
        },
        "out_of_range": {
            "memory": {"unwinding": False},
            "latest": {
                "liquidity:price": "2600",
                "liquidity:range_lower": "2000",
                "liquidity:range_upper": "2500",
            },
            "datasets": ["fund_inventory", "payer_costs", "curve_pool_shares"],
        },
        "no_liquidity_no_level": {
            "memory": {"unwinding": False},
            "parameters": {"unwind_health_factor": None},
        },
    },
    "arbitrum_aave_leverage_loop": {
        "shortfall": {"memory": {"shortfall": "Repay needs 3 USDC more."}},
        "below_unwind": {
            "memory": {"unwinding": False, "capped_reason": "Borrow capped by the floor."},
            "latest": {
                "loan:health": "1.5",
                "loan:ltv": "0.6",
                "loan:max_ltv": "0.8",
                "loan:debt": "40",
                "collateral:supplied": "0.05",
            },
        },
        "healthy": {
            "memory": {"unwinding": False},
            "latest": {"loan:health": "2.4", "loan:ltv": "0.3", "loan:max_ltv": "0.8"},
            "datasets": ["fund_inventory", "payer_costs"],
        },
        "no_debt": {
            "memory": {"unwinding": False},
            "latest": {"loan:price_usd": None, "collateral:price_usd": None},
        },
        "debt_without_health": {
            "memory": {"unwinding": False},
            "latest": {"loan:health": None, "loan:debt": "40"},
        },
    },
    "curve_arbitrum_stable_lp": {
        "providing": {"latest": {"shares": "9.87"}},
        "no_value": {"latest": {"value": None}},
        "no_tables": {"datasets": ["exchange_inventory", "pool_shares", "payer_costs"]},
    },
    "gmx_v2_arbitrum_liquidity": {
        "providing": {"latest": {"gm_shares": "151.25"}},
        "not_read": {"latest": {"gm_shares": None, "wallet1": None}},
        "no_tables": {"datasets": ["gm_shares", "requests", "submitters"]},
    },
    "gmx_v2_arbitrum_position": {
        "priced_long": {
            "latest": {"size_usd": "2500", "size_tokens": "1.05", "entry_price": "2381.4"},
            "prices": [GMX_ETH_CLOSES],
        },
        "priced_without_entry": {
            "latest": {"size_usd": "2500", "size_tokens": "1.05"},
            "prices": [GMX_ETH_CLOSES],
        },
        "open_long": {
            "latest": {
                "size_usd": "2500.5",
                "size_tokens": "1.05",
                "entry_price": "2381.4",
                "collateral": "0.42",
            }
        },
        "not_read": {
            "latest": {
                "size_usd": None,
                "collateral": None,
                "funding_long": None,
                "funding_short": None,
            }
        },
        "no_tables": {"datasets": ["positions", "requests", "submitters"]},
    },
    "uniswap_v3_arbitrum_lp": {
        "in_range": {"latest": {"range_lower": "2000", "range_upper": "2500"}},
        "out_of_range": {"latest": {"range_lower": "2300", "range_upper": "2600"}},
        "no_readings": {"latest": {"price": None, "value": None, "hold": None}},
    },
    "uniswap_v3_base_rsi": {
        "holding": {"inventory": {"position-inventory0": "500000000000000000"}},
        "faster_rsi": {"parameters": {"period": 7, "oversold": 45, "overbought": 55}},
    },
    "uniswap_v4_arbitrum_lp": {
        "in_range": {
            "latest": {
                "range_lower": "2000",
                "range_upper": "2500",
                "fees0": "0.0001",
                "fees1": "0.2",
            }
        },
        "out_of_range": {"latest": {"range_lower": "2300", "range_upper": "2600"}},
        "full_range": {
            "latest": {
                "range_lower": "0.000001",
                "range_upper": "1000000000",
                "fees0": "0.0001",
                "fees1": "0.2",
            }
        },
        "zero_lower_edge": {"latest": {"range_lower": "0", "range_upper": "1000000"}},
        "no_readings": {"latest": {"price": None, "value": None, "hold": None}},
        "no_tables": {"datasets": ["exchange_inventory", "payer_costs"]},
    },
}


def merge(fields: dict, change: Mapping) -> None:
    for key, value in change.items():
        if value is None:
            fields.pop(key, None)
        else:
            fields[key] = value


def edit(document: dict, change: Mapping) -> dict:
    document = copy.deepcopy(document)
    document["memory"]["values"].update(change.get("memory", {}))
    if "parameters" in change:
        document["config"]["parameters"].update(change["parameters"])
    if "rows" in change:
        document["history"] = document["history"][: change["rows"]]
    if document["history"]:
        merge(document["history"][-1]["fields"], change.get("latest", {}))
    if "prices" in change:
        document["prices"] = change["prices"]
    for value in (v for p in document["positions"] for v in p.get("inventory", ())):
        value["amount"] = change.get("inventory", {}).get(value["id"], value["amount"])
    for key in change.get("datasets", ()):
        document["datasets"].pop(key, None)
    return document


def variants(name: str) -> dict[str, Context]:
    """Every context the reference is drawn over, by label."""
    recorded = json.loads((FIXTURES / f"{REFERENCES[name]}.json").read_text())
    found = {
        "recorded": recorded,
        "first_decision": {**recorded, "history": [], "prices": [], "trades": []},
        "one_row": {**recorded, "history": recorded["history"][:1]},
    }
    if recorded.get("prices"):
        # Part of the declared window read so far: indicators wait for the rest.
        short = [{**row, "points": row["points"][:20]} for row in recorded["prices"]]
        found["partial_window"] = {**recorded, "prices": short}
    for label, change in BRANCHES.get(name, {}).items():
        found[label] = edit(recorded, change)
    return {label: Context.model_validate_json(json.dumps(doc)) for label, doc in found.items()}


@cache
def hook(name: str):
    path = ROOT / "strategies" / name / "dashboard.py"
    spec = importlib.util.spec_from_file_location(f"reference_dashboard_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dashboard


CASES = [(name, label) for name in REFERENCES for label in variants(name)]


def test_every_reference_dashboard_has_a_recorded_context():
    drawn = {path.parent.name for path in (ROOT / "strategies").glob("*/dashboard.py")}
    assert drawn == set(REFERENCES)
    assert {path.stem for path in FIXTURES.glob("*.json")} == set(REFERENCES.values())


@pytest.mark.parametrize(("name", "label"), CASES, ids=[f"{n}-{v}" for n, v in CASES])
def test_reference_dashboard_draws_a_valid_document(name, label):
    result = hook(name)(variants(name)[label])
    assert isinstance(result, ui.Dashboard)
    assert Dashboard.model_validate_json(result.model_dump_json()) == result


@pytest.mark.parametrize("name", REFERENCES)
def test_reference_dashboard_draws_through_the_evaluation_path(name):
    strategy = StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py")
    context = variants(name)["recorded"]
    result = execute(Request(strategy=strategy, context=context))
    assert result is not None
    assert result.model_dump_json() == hook(name)(context).model_dump_json()


def typed_model(values: Mapping):
    fields = {
        key: (tuple if isinstance(value, list) else type(value), ...)
        for key, value in values.items()
    }
    return create_model("Typed", __base__=TypedParameters, **fields)  # type: ignore[call-overload]


@pytest.mark.parametrize("name", REFERENCES)
def test_reference_dashboard_reads_typed_parameters_as_the_mapping(name):
    context = variants(name)["recorded"]
    model = typed_model(context.config.parameters)
    config = typed({"Typed": model}, context.config)
    assert isinstance(config.parameters, model)
    drawn = hook(name)(context.model_copy(update={"config": config}))
    assert drawn.model_dump_json() == hook(name)(context).model_dump_json()
