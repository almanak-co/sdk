"""Range controls combine captured ABI bytes under a synthetic shared block."""

from copy import deepcopy
from decimal import Decimal

import pytest

from qa_lab.e2e_card import REPO, TEMPLATE, Scenario, canonical, digest, load_json
from qa_lab.e2e_opened_target import opened_target, validate_opened_quote, validate_owned_quote
from qa_lab.e2e_price_lineage import _raw_price
from tests.unit.scripts import test_e2e_phase_capture as phase_tests

captured = phase_tests.captured
rebalanced = phase_tests.rebalanced
lane = phase_tests.lane


@pytest.fixture
def geometry(lane):
    directory = REPO / "tests/fixtures/accounting/harness/lp-dual-price-input/price-observations"
    pool = next(value for path in directory.glob("*.json") if "raw_reads" in (value := load_json(path)))
    pool.update(
        fork_identity=lane.opened["fork_identity"],
        block_number=lane.opened["end_block"],
        block_hash=lane.opened["end_block_hash"],
    )
    pool["weth_usdc"] = str(_raw_price(pool, "WETH"))
    scenario = Scenario.model_validate(load_json(REPO / TEMPLATE))
    config = {"rebalance_exit_buffer_pct": "0.01"}
    target = opened_target(scenario, config, lane.opened, pool)
    selected = {"weth_usdc_after": target["target_price_min"]}
    quote = {
        "status": "QUOTED",
        "before": pool,
        "target_min": target["target_price_min"],
        "target_max": target["target_price_max"],
        "selected": selected,
        "quotes": [selected],
    }
    return scenario, config, quote


def test_opened_ticks_define_the_quote_band(lane, geometry):
    scenario, config, quote = geometry
    result = validate_opened_quote(scenario, config, lane.opened, quote)
    assert Decimal(result["target_price_min"]) > Decimal(result["narrow_immediate_trigger_above"])
    assert {result["narrow_token_id"], result["wide_token_id"]} == {
        row["token_id"] for row in lane.opened["generations"]
    }


@pytest.mark.parametrize(
    "fault", ["before_mints", "different_fork", "shared_hash", "reported_price", "target", "undershoot"]
)
def test_quote_cannot_borrow_unrelated_or_infeasible_opening_geometry(lane, geometry, fault):
    scenario, config, original = geometry
    quote = deepcopy(original)
    if fault == "before_mints":
        quote["before"]["block_number"] -= 1
    elif fault == "different_fork":
        quote["before"]["fork_identity"]["instance_id"] = "other"
    elif fault == "shared_hash":
        quote["before"]["block_hash"] = "0x" + "ee" * 32
    elif fault == "reported_price":
        quote["before"]["weth_usdc"] = "1"
    elif fault == "target":
        quote["target_min"] = str(Decimal(quote["target_min"]) - 1)
    else:
        quote["selected"]["weth_usdc_after"] = str(Decimal(quote["target_min"]) - 1)
    with pytest.raises(ValueError):
        validate_opened_quote(scenario, config, lane.opened, quote)


def test_geometry_binding_requires_owned_opening_before_stimulus_reservation(lane, geometry):
    scenario, config, quote = geometry
    preparation = lane.context.root / "preparation"
    (preparation / "config.json").write_bytes(canonical(config))
    with pytest.raises(ValueError, match="no record"):
        validate_owned_quote(lane.context, preparation, scenario, quote, lane.wallet, store=lane.store)

    phase_tests.capture(lane, "open")
    result = validate_owned_quote(lane.context, preparation, scenario, quote, lane.wallet, store=lane.store)
    assert result["opening_sha256"] == digest((lane.context.root / "positions-open.json").read_bytes())
    lane.store.reserve_launch(lane.lease, "stimulus")
    with pytest.raises(ValueError, match="before its launch reservation"):
        validate_owned_quote(lane.context, preparation, scenario, quote, lane.wallet, store=lane.store)


def test_worker_rechecks_quote_only_under_its_unclaimed_reservation(lane, geometry):
    scenario, config, quote = geometry
    preparation = lane.context.root / "preparation"
    (preparation / "config.json").write_bytes(canonical(config))
    phase_tests.capture(lane, "open")
    token = lane.store.reserve_launch(lane.lease, "stimulus")
    args = (lane.context, preparation, scenario, quote, lane.wallet)
    result = validate_owned_quote(*args, store=lane.store, reservation=(lane.lease, token))
    assert result["opening_sha256"] == digest((lane.context.root / "positions-open.json").read_bytes())
    with pytest.raises(ValueError, match="reservation"):
        validate_owned_quote(*args, store=lane.store, reservation=(lane.lease, "wrong-token"))
    lane.store.claim_launch(lane.lease, role="stimulus", token=token)
    with pytest.raises(ValueError, match="reservation"):
        validate_owned_quote(*args, store=lane.store, reservation=(lane.lease, token))
