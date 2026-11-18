"""QA assertions audit real retained lifecycle evidence and reject missing effects."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.backtest.engine import Journal
from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import EconomicAction
from tests.qa.catalog import ROOT, catalog
from tests.qa.inputs import write_json
from tests.qa.worker import assert_backtest, assert_completed, assert_fork_actions

ECONOMIC = [
    c
    for c in catalog()
    if c.lane == "backtest"
    and c.input_policy == "pinned"
    and "economic" in c.model
    and c.connector in (ConnectorId("aave_v4"), ConnectorId("uniswap_v4"))
]


@pytest.mark.parametrize("case", ECONOMIC, ids=lambda c: c.reference)
def test_backtest_assertions_require_actual_actions_and_closed_state(tmp_path, case):
    case = captured_actions(case)
    manifest, expected, model = prepared(tmp_path, case.reference)
    output = tmp_path / "run"
    output.mkdir()
    try:
        write_json(output / "manifest.json", manifest.model_dump(mode="json"))
        report = connector_for(manifest.config).backtest("economic").report(expected, model)
        write_json(output / "report.json", report)
        Journal(output / "state.sqlite", type(manifest.initial)).create(expected)
        assert_backtest(case, output)
        missing = replace(case, actions=(EconomicAction.REFRESH,))
        with pytest.raises(AssertionError, match="missing economic actions"):
            assert_backtest(missing, output)
        with pytest.raises(AssertionError):
            assert_completed(expected.model_copy(update={"completed": (), "delivered": 0}))
    finally:
        model.book.close()


def prepared(directory, reference):
    if reference in ("uniswap_v4_polygon_lp", "uniswap_v4_polygon_trading"):
        from tests.uniswap_v4_polygon_support import prepared

        return prepared(directory, reference.removeprefix("uniswap_v4_polygon_"))
    if reference in ("uniswap_v4_optimism_lp", "uniswap_v4_optimism_trading"):
        from tests.uniswap_v4_optimism_support import prepared

        return prepared(directory, reference.removeprefix("uniswap_v4_optimism_"))
    if reference in ("uniswap_v4_base_lp", "uniswap_v4_base_trading"):
        from tests.uniswap_v4_base_support import prepared

        return prepared(directory, reference.removeprefix("uniswap_v4_base_"))
    if reference in ("uniswap_v4_arbitrum_lp", "uniswap_v4_arbitrum_trading"):
        from tests.uniswap_v4_arbitrum_support import prepared

        return prepared(directory, reference.removeprefix("uniswap_v4_arbitrum_"))
    names = {
        "aave_v4_weth_supply": "ethereum-reference",
        "aave_v4_avalanche_borrowing": "avalanche",
        "aave_v4_multichain_borrowing": "multichain",
    }
    if reference in names:
        from tests.aave_economic_support import prepared

        return prepared(directory, names[reference])
    if reference == "uniswap_v4_ethereum_native_trading":
        return native_trading_prepared(directory)
    uniswap = {
        "uniswap_v4_trading": ("ethereum", "trading"),
        "uniswap_v4_lp": ("ethereum", "lp"),
        "uniswap_v4_avalanche_trading": ("avalanche", "trading"),
        "uniswap_v4_avalanche_lp": ("avalanche", "lp"),
        "uniswap_v4_bnb_trading": ("bnb", "trading"),
        "uniswap_v4_bnb_lp": ("bnb", "lp"),
        "uniswap_v4_robinhood_trading": ("robinhood", "trading"),
        "uniswap_v4_robinhood_lp": ("robinhood", "lp"),
    }
    if reference not in uniswap:
        raise ValueError("no captured V4 fixture for this reference")
    from tests.uniswap_economic_support import prepared

    return prepared(directory, *uniswap[reference])


# The pinned economic capture #152 declared for the native-ETH reference,
# prepared through the read-only provider on 2026-10-20.
NATIVE_TRADING = (
    ROOT
    / "docs/internal/reports/evidence/2026-09-07-rollout"
    / "uniswap-v4-ethereum-native-economic-2026-10-20"
)


def native_trading_prepared(directory):
    from tests.uniswap_economic_support import prepared_capture

    files = {
        row["path"]: row
        for row in json.loads((NATIVE_TRADING / "manifest.json").read_text())["files"]
    }

    def sealed(name):
        raw = (NATIVE_TRADING / name).read_bytes()
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (
            files[name]["bytes"],
            files[name]["sha256"],
        )
        return gzip.decompress(raw)

    capture = json.loads(sealed("prepared-capture.json.gz"))
    return prepared_capture(
        directory,
        capture["manifest"],
        capture["state"],
        capture["dataset_manifest_bytes"],
        sealed("run/dataset/data.sqlite.gz"),
        "uniswap_v4_ethereum_native_trading",
    )


@pytest.mark.parametrize(
    "reference",
    ("sparklend_ethereum_lending", "pancakeswap_v3_bnb_lp", "pancakeswap_v3_bnb_trading"),
)
def test_new_connectors_cannot_substitute_a_v4_capture(tmp_path, reference):
    with pytest.raises(ValueError, match="no captured V4 fixture"):
        prepared(tmp_path, reference)


def test_fork_assertions_credit_only_successful_scoped_operations(tmp_path):
    path = (
        ROOT
        / "docs/internal/reports/evidence/2026-09-06-backtest-integration"
        / "final-fork-lifecycles.json.gz"
    )
    evidence = json.loads(gzip.decompress(path.read_bytes()))
    row = next(r for r in evidence["runs"] if r["result"]["strategy"] == "uniswap_v4_trading")
    case = next(c for c in catalog() if c.reference == "uniswap_v4_trading" and c.lane == "fork")
    config = tmp_path / "config.yaml"
    config.write_bytes((ROOT / "strategies/uniswap_v4_trading/config.yaml").read_bytes())
    with pytest.raises(AssertionError, match="exit_exchange"):
        assert_fork_actions(case, row["checkpoint"], config)
    case = captured_actions(case)
    assert_fork_actions(case, row["checkpoint"], config)
    state = row["checkpoint"]
    for outcome in state["outcomes"]:
        if outcome["operation"]["kind"] == "swap":
            outcome["success"] = False
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(case, state, config)


@pytest.mark.parametrize("name", ["trading", "lp", "avalanche_trading", "avalanche_lp"])
def test_detailed_history_credits_observed_intents_including_expiry(tmp_path, name):
    from almanak.backtest.cli import close_model
    from tests.uniswap_support import historical_model

    manifest, state, model = historical_model(name)
    case = next(
        c
        for c in catalog()
        if c.reference == "uniswap_v4_" + name
        and c.lane == "backtest"
        and "exact-quoter" in c.model
    )
    case = captured_actions(case)
    if name.endswith("trading"):
        case = replace(case, actions=(EconomicAction.SWAP,))
    try:
        write_json(tmp_path / "manifest.json", manifest.model_dump(mode="json"))
        report = connector_for(manifest.config).backtest("historical_path").report(state, model)
        write_json(tmp_path / "report.json", report)
        Journal(tmp_path / "state.sqlite", type(state)).create(state)
        assert_backtest(case, tmp_path)
        if name.endswith("trading"):
            assert EconomicAction.REVOKE not in case.actions
        else:
            assert EconomicAction.REVOKE in case.actions
    finally:
        close_model(model)


def test_missing_measurements_do_not_become_zero_or_success():
    from tests.qa.worker import assert_measurements

    case = next(
        c
        for c in catalog()
        if c.reference == "aave_v4_weth_supply"
        and c.lane == "backtest"
        and "historical-shares" in c.model
    )
    assert_measurements(case, {"positions": [{"marked_assets": "0"}]})
    with pytest.raises(AssertionError):
        assert_measurements(case, {"positions": [{"marked_assets": None}]})
    with pytest.raises(AssertionError):
        assert_measurements(case, {"positions": []})


def captured_actions(case):
    if case.reference in {
        "uniswap_v4_optimism_lp",
        "uniswap_v4_optimism_trading",
        "uniswap_v4_base_lp",
        "uniswap_v4_base_trading",
        "uniswap_v4_arbitrum_lp",
        "uniswap_v4_arbitrum_trading",
        "uniswap_v4_polygon_lp",
        "uniswap_v4_polygon_trading",
    }:
        return case
    # These assertions audit earlier captures, which did not admit composite exits.
    # The current catalog and new bench runs must still prove EXIT_EXCHANGE.
    return replace(
        case, actions=tuple(a for a in case.actions if a != EconomicAction.EXIT_EXCHANGE)
    )
