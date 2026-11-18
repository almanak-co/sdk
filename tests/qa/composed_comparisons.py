"""Composed quote checks retain exact connector datasets and historical gateway scopes."""

from almanak.connectors.composed.economic_binding import ComposedEconomic, Manifest
from almanak.connectors.identifiers import ConnectorId
from tests.qa.aerodrome_comparisons import compare_book as compare_aerodrome
from tests.qa.comparisons import compare_book as compare_v4
from tests.qa.curve_comparisons import compare_book as compare_curve
from tests.qa.gmx_comparisons import compare_book as compare_gmx
from tests.qa.inputs import write_json
from tests.qa.pancake_comparisons import compare_book as compare_pancake
from tests.qa.slipstream_comparisons import compare_book as compare_slipstream
from tests.qa.v3_comparisons import compare_book as compare_v3


def compare_components(directory, env_file):
    manifest = Manifest.model_validate_json((directory / "run/manifest.json").read_text())
    book = ComposedEconomic.open_book(manifest)
    try:
        for connector, compare in (
            (ConnectorId("uniswap_v3"), compare_v3),
            (ConnectorId("uniswap_v4"), compare_v4),
            (ConnectorId("curve"), compare_curve),
            (ConnectorId("gmx_v2"), compare_gmx),
            (ConnectorId("pancakeswap_v3"), compare_pancake),
            (ConnectorId("aerodrome"), compare_aerodrome),
            (ConnectorId("aerodrome_slipstream"), compare_slipstream),
        ):
            if connector in book.components:
                compare_component(directory, env_file, manifest, book, connector, compare)
    finally:
        book.close()


def compare_component(directory, env_file, manifest, book, connector, compare):
    records: list[dict[str, object]] = []
    try:
        compare(book.components[connector], manifest.dataset / str(connector), env_file, records)
        assert records and all(row["equal"] for row in records)
    finally:
        write_json(directory / f"contract-comparisons-{connector}.json", records)
