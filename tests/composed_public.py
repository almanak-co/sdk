"""Offline public composed parameters rebuilt from frozen verified market facts."""

import json
import re

from almanak.configuration.parameters import assemble, grouped_positions
from almanak.configuration.unresolved import parse_inputs
from almanak.configuration.yaml import parse
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composition import connector_for
from almanak.triggers import IntervalSelection
from tests.test_reference_conversion import (
    LEGACY_MARKETS,
    STRATEGIES,
    authored_positions,
    effective,
    legacy_parameters,
    legacy_positions,
    legacy_snapshot,
)


def frozen_cadence_inputs(name: str, seconds: int):
    """The shipped reference at the frozen snapshot's cadence.

    Shipped references decide at a live interval; the frozen facts, windows and
    economic paths were captured at the legacy cadence.
    """
    path = STRATEGIES / name / "config.yaml"
    text, count = re.subn(
        r"^(  decision_interval: ).*$", rf"\g<1>{seconds}s", path.read_text(), flags=re.M
    )
    assert count == 1, path
    return parse_inputs(parse(text, file=str(path)))


def resolved_parameters(name: str):
    entry = legacy_snapshot(name)
    legacy = effective(entry, "config.yaml")
    expected = legacy_parameters(entry)
    document = frozen_cadence_inputs(name, legacy["decision_trigger"]["seconds"])
    frozen = legacy_positions(legacy)
    markets = {}
    for position_name, position in authored_positions(document.inputs).items():
        selection = document.inputs.markets[position.market]
        markets[position.market] = LEGACY_MARKETS[str(selection.connector)](
            frozen[position_name], legacy["asset_metadata"]
        )
    groups = grouped_positions(document, markets)
    return document, assemble(document, groups, expected.accounts, expected.asset_metadata, ())


def economic_inputs(name: str):
    document, parameters = resolved_parameters(name)
    raw = effective(legacy_snapshot(name), "backtest-economic.yaml")
    binding = connector_for(parameters).backtest(raw["market_model"])
    settings = binding.settings_type.model_validate_json(json.dumps(raw))
    interval = IntervalSelection(configured=parameters.decision_trigger)
    config = ComposedEconomic.parameters(parameters, settings, interval)
    return parameters, config, settings, interval
