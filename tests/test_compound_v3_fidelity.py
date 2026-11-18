"""Measured sampled-path differences remain separate from contract arithmetic proofs."""

import json

from almanak.connectors.compound_v3.accounting import indices
from almanak.connectors.compound_v3.economic_book import Book
from tests.test_compound_v3_prepared import EVIDENCE
from tests.test_compound_v3_prepared import prepared as prepared


def test_sampled_interest_comparison_reproduces_from_captured_inputs(prepared):
    captured = json.loads((EVIDENCE.parent / "compound-v3-prepared-fidelity.json").read_text())
    book = Book(prepared.dataset, prepared.config, prepared.settings)
    try:
        source, timeline = next(iter(book.risks.items()))
        rows = [r for r in captured["rows"] if r["chain"] == str(source.chain.chain_id)]
        assert len(rows) == len(timeline.rows)
        for row, record in zip(rows, timeline.rows, strict=True):
            time = int(record.available_at)
            historical = indices(record.payload.market, time)
            modeled = book.rates[source].at(time)
            assert row["block"] == record.payload.market.block.model_dump(mode="json")
            assert row["time"] == time
            assert row["comet"] == str(source.comet)
            assert row["observed_projection"] == list(historical)
            assert row["modeled"] == list(modeled)
            assert row["difference"] == [a - b for a, b in zip(modeled, historical, strict=True)]
        assert any(any(row["difference"]) for row in rows)
    finally:
        book.close()
