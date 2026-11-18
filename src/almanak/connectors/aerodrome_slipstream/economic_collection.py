"""Bound connector preflight and sample decoding to bounded dataset collection."""

from almanak.connectors import v3_economic_collection as family
from almanak.connectors.aerodrome_slipstream.economic_acquisition import prepared_sample
from almanak.connectors.aerodrome_slipstream.economic_data import key
from almanak.connectors.aerodrome_slipstream.history import preflight
from almanak.connectors.v3_economic_collection import contract_bindings as contract_bindings
from almanak.connectors.v3_economic_collection import resolve_boundaries as resolve_boundaries


def collect(writer, history, target, settings):
    return family.collect(
        writer,
        history,
        target,
        settings,
        key=key,
        preflight=preflight,
        prepared_sample=prepared_sample,
    )
