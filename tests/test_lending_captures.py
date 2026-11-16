"""Captures that predate strategy.parameters gain exactly their reference's declared knobs."""

import pytest

from almanak.typed_parameters import model_fields, parameters_type
from tests.lending_captures import (
    CONNECTOR_REFERENCES,
    STRATEGIES,
    captured_parameters,
    current_capture,
)

SPOKE = {"spoke": "0x" + "1" * 40, "asset": "eip155:1/erc20:0x" + "2" * 40}
RESERVE = {"asset": "eip155:8453/erc20:0x" + "3" * 40}
COMPOSED_V4 = {"allocations": [{"name": "loan", "reserve": SPOKE}, {"name": "trade"}]}
COMPOSED = {"allocations": [{"name": "loan", "reserve": RESERVE}, {"name": "trade"}]}
LENDING = ("aave", "compound", "morpho", "spark")


def declared(reference, parameters):
    namespace = {"__name__": "__strategy__"}
    path = STRATEGIES / reference / "strategy.py"
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    parameters_type(namespace).model_validate(parameters)
    return set(parameters) == model_fields(path)


@pytest.mark.parametrize(
    ("config", "reference", "expected"),
    [
        (COMPOSED_V4, "ethereum_aave_v4_borrow_and_trade", {"borrow_quantity": "5"}),
        (COMPOSED_V4, "avalanche_aave_v4_borrow_and_trade", {"borrow_quantity": "5"}),
        (COMPOSED, "base_compound_borrow_and_trade", {"borrow_quantity": "200"}),
        (COMPOSED, "base_morpho_borrow_and_trade", {"borrow_quantity": "5"}),
        (
            COMPOSED_V4,
            None,
            {"borrow_quantity": "5", "unwind_margin": "0.25"},
        ),
        ({**COMPOSED, "connector": "compound_v3"}, None, {"borrow_quantity": "200"}),
        ({**COMPOSED, "connector": "morpho_blue"}, None, {"borrow_quantity": "200"}),
        ({**COMPOSED, "connector": "aave_v3"}, None, {"borrow_quantity": "5"}),
        ({**COMPOSED, "connector": "sparklend"}, None, {"borrow_quantity": "5"}),
    ],
)
def test_capture_gains_exactly_its_references_knobs(config, reference, expected):
    parameters = current_capture({"config": config}, reference)["config"]["parameters"]
    assert parameters == expected
    target = reference or CONNECTOR_REFERENCES[config.get("connector", "aave_v4")]
    assert declared(target, parameters)


def test_capture_without_a_reference_or_connector_gains_no_knobs():
    assert "parameters" not in current_capture({"config": COMPOSED})["config"]


def test_recorded_knobs_are_kept():
    config = {**COMPOSED, "parameters": {"borrow_quantity": "7"}}
    assert current_capture({"config": config}, "base_compound_borrow_and_trade")["config"][
        "parameters"
    ] == {"borrow_quantity": "7"}


@pytest.mark.parametrize(
    "reference",
    sorted(p.name for p in STRATEGIES.iterdir() if any(k in p.name for k in LENDING)),
)
def test_injected_knobs_are_the_references_model_fields(reference):
    assert set(captured_parameters(reference)) == model_fields(
        STRATEGIES / reference / "strategy.py"
    )
