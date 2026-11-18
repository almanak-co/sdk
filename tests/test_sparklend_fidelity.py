"""Sampled-path errors are measured separately from deployed integer comparisons."""

import json
from pathlib import Path

from almanak.connectors.sparklend.economic_book import Book
from almanak.connectors.sparklend.math import normalized_indices
from tests.sparklend_helpers import candidate_spark_support as candidate_spark_support
from tests.test_sparklend_prepared import prepared as prepared

CAPTURE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-08-lending-rollout"
    / "sparklend-ethereum/sampled-fidelity.json"
)


def comparisons(book):
    rows = []
    for source, timeline in book.risks.items():
        for record in timeline.rows:
            time = int(record.available_at)
            historical = normalized_indices(record.payload.reserve, time)
            modeled = book.rates[source].at(time)
            rows.append(
                {
                    "source": source.model_dump(mode="json"),
                    "block": record.payload.reserve.block.model_dump(mode="json"),
                    "time": time,
                    "observed_projection": list(historical),
                    "modeled": list(modeled),
                    "difference": [a - b for a, b in zip(modeled, historical, strict=True)],
                }
            )
    return rows


def test_captured_sampled_path_differences_reproduce_without_provider_access(prepared):
    capture = json.loads(CAPTURE.read_text())
    book = Book(prepared.dataset, prepared.config, prepared.settings)
    try:
        assert comparisons(book) == capture["rows"]
        assert all(len(t.rows) == capture["boundaries_per_reserve"] for t in book.risks.values())
    finally:
        book.close()
