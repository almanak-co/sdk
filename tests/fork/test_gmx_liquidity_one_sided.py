"""Single-currency GM deposits use the same reusable strategy, escrow and generated authority."""

from pathlib import Path
from uuid import uuid4

import pytest

from almanak.asset_amounts import AssetAmounts
from tests.fork.test_gmx_collateral_paths import run_path
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import resolve_config, retain

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
@pytest.mark.parametrize("input_side", ["long", "short"])
def test_single_currency_gm_lifecycle(request, tmp_path, chain, profile, input_side):
    selected = case(f"gmx_v2_{chain}_liquidity")
    env = request.config.getoption("--fork-env").resolve()
    config = single_currency_config(selected, profile, input_side, env, tmp_path / "resolution")
    config = resolve_config(config, env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-" + input_side + "-only-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "input_side": input_side,
        "status": "failed",
        "phases": [],
    }
    try:
        run_path(selected, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX single-currency liquidity evidence:", artifact)


def single_currency_config(selected, profile, input_side, env, directory):
    config = configuration(selected, profile, env, directory)
    allocation = config.allocations[0]
    excluded = (
        allocation.market.short_asset if input_side == "long" else allocation.market.long_asset
    )
    changes = {
        field: AssetAmounts(
            tuple(zero_currency(value, excluded) for value in getattr(allocation, field).root)
        )
        for field in ("entry_amounts", "additional_amounts")
    }
    changes["initial_inventory"] = limit_unused_assets(
        allocation.initial_inventory, allocation, excluded
    )
    updated = allocation.model_copy(update=changes)
    return type(config).model_validate_json(
        config.model_copy(update={"allocations": (updated,)}).model_dump_json()
    )


def zero_currency(value, excluded):
    return value.model_copy(update={"amount": 0}) if value.asset == excluded else value


def unused_currency_bound(allocation, bound, excluded):
    if bound.asset != excluded:
        return bound.amount
    if bound.asset == allocation.execution_fee.asset:
        return 4 * allocation.execution_fee.amount
    return 0


def limit_unused_assets(values, allocation, excluded):
    return AssetAmounts(
        tuple(
            value.model_copy(update={"amount": unused_currency_bound(allocation, value, excluded)})
            for value in values.root
        )
    )
