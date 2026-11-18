"""Historical gateway replacement reconstructs complete Comet datasets from captured reads."""

import gzip
from types import SimpleNamespace

import pytest

from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import Capture, HistoricalGateway, HistoricalReader
from almanak.backtest.prepared import DatasetWriter, Preparation
from almanak.connectors.compound_v3.economic_acquisition import (
    bounds,
    collect_accounting,
    collect_risk,
    contracts,
    discover,
    group_at,
)
from almanak.connectors.compound_v3.economic_book import Book
from almanak.connectors.compound_v3.economic_data import MODEL, REVISION
from almanak.connectors.compound_v3.economic_parameters import identity, markets, requirements
from almanak.connectors.compound_v3.economic_prepare import provision
from tests.test_compound_v3_prepared import EVIDENCE
from tests.test_compound_v3_prepared import prepared as prepared


class Replay(HistoricalReader):
    _rpc = HistoricalReader._rpc

    def __init__(self, chain, gateway):
        super().__init__(chain, "http://unused.invalid", "unused")
        self.gateway = gateway

    def send(self, method, request):
        return self.gateway.dispatch(method, request).model_dump(mode="json")


def test_replaced_historical_gateway_reconstructs_every_input_without_provider_access(
    prepared, tmp_path
):
    inputs = prepared
    network = (
        "base" if str(inputs.config.positions[0].chain.chain_id) == "eip155:8453" else "ethereum"
    )
    path = tmp_path / "capture.sqlite"
    path.write_bytes(gzip.decompress((EVIDENCE / network / "observations.sqlite.gz").read_bytes()))
    capture = Capture(path)
    gateway = HistoricalGateway(
        capture, {}, {b.chain: b.contracts for b in contracts(inputs.config)}
    )
    source = markets(inputs.config)[0]
    original_count = capture.count()
    with Replay(source.chain, gateway) as reader:
        history = EvmHistory(reader)
        discovered = discover(history, source, inputs.settings.schedule.start)
        assert len(discovered.collateral) > 1
        gateway.contracts = {b.chain: b.contracts for b in contracts(inputs.config, (discovered,))}
        bounds({source.chain: history}, inputs.config, inputs.settings)
        directory = tmp_path / "reconstructed"
        writer = DatasetWriter(
            directory,
            Preparation(
                model=MODEL,
                acquisition_revision=REVISION,
                policy_digest=identity(inputs.config, inputs.settings),
                requirements=requirements(inputs.config, inputs.settings),
            ),
        )
        try:
            collect_risk(writer, history, inputs.config, source, inputs.settings)
            collect_accounting(writer, history, inputs.config, source, inputs.settings)
            writer.publish()
        finally:
            writer.close()
    assert capture.count() == original_count
    gateway.close()
    reconstructed = Book(directory, inputs.config, inputs.settings)
    original = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        assert reconstructed.risks[source].rows == original.risks[source].rows
        assert reconstructed.rates[source].records.rows == original.rates[source].records.rows
        assert reconstructed.manifest.database_sha256 == original.manifest.database_sha256
    finally:
        reconstructed.close()
        original.close()


class Headers:
    def __init__(self, times):
        self.blocks = tuple(SimpleNamespace(number=n, timestamp=t) for n, t in enumerate(times))
        self.upper = self.blocks[-1]

    def floor(self, time):
        return max((b for b in self.blocks if b.timestamp <= time), key=lambda b: b.number)

    def header(self, number):
        return self.blocks[number]


def test_timestamp_groups_refuse_missing_boundary_and_excess_blocks():
    history = Headers([10, 20, 20, 30])
    group, following = group_at(history, 20, 10, 2)
    assert [b.number for b in group] == [1, 2] and following.number == 3
    with pytest.raises(ValueError, match="explicit block limit"):
        group_at(history, 20, 10, 1)
    history.floor = lambda time: history.blocks[1]
    with pytest.raises(ValueError, match="latest eligible"):
        group_at(history, 20, 20, 2)


@pytest.mark.parametrize("fault", ["boundary", "blocks", "samples"])
def test_preparation_refuses_unclosed_history_and_explicit_collection_overruns(prepared, fault):
    settings = prepared.settings
    start, end = settings.schedule.start, settings.schedule.deadline
    history = Headers([start, end] if fault == "boundary" else [start, end, end + 12])
    data = settings.data.model_copy(
        update={
            "max_risk_blocks": 1 if fault == "blocks" else 2,
            "max_risk_samples": 1 if fault == "samples" else 2,
        }
    )
    with pytest.raises(ValueError, match="boundary|explicit block or sample limit"):
        bounds(
            {prepared.config.positions[0].chain: history},
            prepared.config,
            settings.model_copy(update={"data": data}),
        )


def test_acquisition_token_and_identity_are_fixed_across_resume(prepared, tmp_path):
    directory = tmp_path / "acquisition"
    directory.mkdir()
    (directory / "acquisition.lock").touch()
    provision(directory, prepared.config, prepared.settings)
    token = directory / "gateway-auth.token"
    assert token.stat().st_mode & 0o777 == 0o600
    original = token.read_bytes()
    provision(directory, prepared.config, prepared.settings)
    assert token.read_bytes() == original
    changed = prepared.settings.model_copy(
        update={"data": prepared.settings.data.model_copy(update={"sample_seconds": 15})}
    )
    with pytest.raises(ValueError, match="cannot change"):
        provision(directory, prepared.config, changed)
    (directory / "acquisition.json").unlink()
    with pytest.raises(ValueError, match="new or matching"):
        provision(directory, prepared.config, prepared.settings)
