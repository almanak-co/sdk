"""Actual bounded chain datasets re-decode completely without any network transport."""

import gzip
import json
import shutil

import httpx
import pytest

from almanak.backtest.evm import EvmHistory
from almanak.backtest.prepared import DatasetWriter
from almanak.connectors.gmx_v2.economic_authority import expanded, provider_scopes
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_collection import (
    collect,
    contract_bindings,
    resolve_boundaries,
)
from almanak.connectors.gmx_v2.economic_config import EconomicConfig
from almanak.connectors.gmx_v2.economic_replay import ReplayReader, validate_frame
from almanak.connectors.gmx_v2.economic_solvency import validate_swap_solvency
from almanak.connectors.gmx_v2.economic_spot_math import quote_swap
from almanak.connectors.gmx_v2.economic_swap_risk import validate_swap_risk
from almanak.connectors.gmx_v2.models import Parameters
from almanak.connectors.gmx_v2.oracle_reader import validate_provider
from tests.gmx_retired import current_json
from tests.gmx_retired import retired_closure as retired_closure
from tests.test_gmx_economic_data import EVIDENCE


def no_transport(*args, **kwargs):
    raise AssertionError("prepared GMX re-decoding attempted network transport")


# The bounded range datasets recaptured on the current closure, after the retired
# deployment's 16 and 17 September revocation (range-capture.py.txt in that directory).
CURRENT = EVIDENCE.with_name(EVIDENCE.name + "-2026-10-20")


def load_prepared(evidence, name, tmp_path, monkeypatch):
    directory = tmp_path / name
    directory.mkdir()
    (directory / "data.sqlite").write_bytes(
        gzip.decompress((evidence / f"{name}-range-data.sqlite.gz").read_bytes())
    )
    shutil.copy2(evidence / f"{name}-range-dataset.json", directory / "dataset.json")
    settings = EconomicConfig.model_validate_json(
        current_json((evidence / f"{name}-range-settings.json").read_text())
    )
    config = Parameters.model_validate_json(
        current_json((evidence / f"{name}-range-parameters.json").read_text())
    )
    expected = json.loads((evidence / f"{name}-range-result.json").read_text())
    monkeypatch.setattr(httpx.Client, "send", no_transport)
    return directory, config, settings, expected


@pytest.fixture(params=["arbitrum", "avalanche"])
def prepared(request, tmp_path, monkeypatch):
    return load_prepared(EVIDENCE, request.param, tmp_path, monkeypatch)


@pytest.fixture(params=["arbitrum", "avalanche"])
def current_prepared(request, tmp_path, monkeypatch):
    return load_prepared(CURRENT, request.param, tmp_path, monkeypatch)


def test_actual_datasets_redecode_every_risk_block_and_publication_without_network(
    current_prepared,
):
    directory, config, settings, expected = current_prepared
    book = MarketBook(directory, config, settings)
    try:
        target = config.target(config.allocations[0].name)
        rows = book.timelines[target].rows
        assert sum(len(row.payload.risk) for row in rows) == expected["risk_blocks"]
        assert sum(len(row.payload.opportunities) for row in rows) == expected["opportunities"]
        assert book.at(target, settings.schedule.start) == rows[0]
        assert book.at(target, settings.schedule.deadline) == rows[-1]
        assert (
            expected["provider_attempts_before_replay"]
            == expected["provider_attempts_after_replay"]
        )
    finally:
        book.close()


def test_entire_collector_reproduces_the_identical_dataset_from_retained_raw_inputs(
    current_prepared,
):
    directory, config, settings, _ = current_prepared
    book = MarketBook(directory, config, settings)
    output = directory.parent / "reproduction"
    target = config.target(config.allocations[0].name)
    rows = book.timelines[target].rows
    digests = tuple(dict.fromkeys(digest for row in rows for digest in row.evidence))
    writer = DatasetWriter(output, book.manifest.preparation)
    try:
        with ReplayReader(target.fund.chain, book.reader, digests) as replay:
            history = EvmHistory(replay)
            collect(writer, history, target, settings)
        manifest = writer.publish()
        assert manifest == book.manifest
    finally:
        writer.close()
        book.close()


def test_staged_provider_and_verifier_authority_redecodes_only_published_sources(current_prepared):
    directory, config, settings, _ = current_prepared
    book = MarketBook(directory, config, settings)
    target = config.target(config.allocations[0].name)
    rows = book.timelines[target].rows
    digests = tuple(dict.fromkeys(digest for row in rows for digest in row.evidence))
    try:
        with ReplayReader(target.fund.chain, book.reader, digests) as replay:
            histories = {target.fund.chain: EvmHistory(replay)}
            boundaries = resolve_boundaries(histories, config, settings)
            contracts = contract_bindings(settings, config, boundaries)
            providers = provider_scopes(histories, config, settings, boundaries)
            assert providers
            addresses = {target.fund.chain: {value[3] for value in providers}}
            provider_grants = expanded(contracts, addresses)
            assert not provider_grants[0].callers
            for block, store, oracle, provider in providers:
                addresses[block.chain].add(
                    validate_provider(replay, block, provider, store, oracle)
                )
            grants = expanded(provider_grants, addresses, config)
            assert len(grants[0].callers) == 2
            assert all(
                call.sender == target.market.contracts.order_handler for call in grants[0].callers
            )
            assert grants[0].transaction_sources == contracts[0].transaction_sources
    finally:
        book.close()


def test_dataset_refuses_changed_collection_policy(prepared):
    directory, config, settings, _ = prepared
    settings = settings.model_copy(
        update={
            "data": settings.data.model_copy(
                update={"maximum_risk_blocks": settings.data.maximum_risk_blocks + 1}
            )
        }
    )
    with pytest.raises(ValueError, match="frozen model or policy"):
        MarketBook(directory, config, settings)


# Economic and replay-integrity claims, not deployment identity: the retained datasets
# re-decode against the retired closure they were collected from (tests.gmx_retired).
@pytest.mark.usefixtures("retired_closure")
@pytest.mark.parametrize("forward", [True, False])
def test_actual_small_swap_retains_risk_and_physical_market_obligations(prepared, forward):
    directory, config, settings, _ = prepared
    book = MarketBook(directory, config, settings)
    try:
        target = config.target(config.allocations[0].name)
        opportunity = next(
            value for row in book.timelines[target].rows for value in row.payload.opportunities
        )
        incoming = opportunity.spot.long if forward else opportunity.spot.short
        calculation = quote_swap(
            opportunity.spot, incoming.price.asset, incoming.pool_amount // 100000
        )
        index = next(
            price
            for price in opportunity.publication.prices
            if price.asset == target.market.index_asset
        )
        validate_swap_risk(opportunity.spot, opportunity.risk, index, calculation)
        validate_swap_solvency(opportunity.spot, opportunity.solvency, calculation)
        outgoing_side = "short" if forward else "long"
        outgoing = getattr(opportunity.solvency, outgoing_side)
        changed = opportunity.solvency.model_copy(
            update={
                outgoing_side: outgoing.model_copy(update={"balance": calculation.amount_out - 1})
            }
        )
        with pytest.raises(ValueError, match="physical market token balance"):
            validate_swap_solvency(opportunity.spot, changed, calculation)
    finally:
        book.close()


# A replay-integrity claim, not deployment identity (see above).
@pytest.mark.usefixtures("retired_closure")
def test_replay_refuses_unretained_reads_and_changed_decoded_risk(prepared):
    directory, config, settings, _ = prepared
    book = MarketBook(directory, config, settings)
    try:
        target = config.target(config.allocations[0].name)
        row = book.timelines[target].rows[0]
        with ReplayReader(target.fund.chain, book.reader, row.evidence) as replay:
            with pytest.raises(ValueError, match="missing retained RPC"):
                replay._rpc("eth_getBalance", [target.fund.address, "latest"])
            original = row.payload.risk[0]
            changed = original.model_copy(update={"market_disabled": not original.market_disabled})
            frame = row.payload.model_copy(update={"risk": (changed, *row.payload.risk[1:])})
            with pytest.raises(ValueError, match="retained DataStore"):
                validate_frame(replay, frame, settings.data.publication(target.allocation), {})
    finally:
        book.close()


# A replay-integrity claim, not deployment identity (see above).
@pytest.mark.usefixtures("retired_closure")
def test_omitting_a_published_opportunity_cannot_pass_redecoding(prepared):
    directory, config, settings, _ = prepared
    book = MarketBook(directory, config, settings)
    try:
        target = config.target(config.allocations[0].name)
        timeline = book.timelines[target]
        candidates = book.validate_boundaries(target, timeline)
        row = next(row for row in timeline.rows if row.payload.opportunities)
        frame = row.payload.model_copy(update={"opportunities": ()})
        with ReplayReader(target.fund.chain, book.reader, row.evidence) as replay:
            with pytest.raises(ValueError, match="complete publication scan"):
                validate_frame(
                    replay, frame, settings.data.publication(target.allocation), candidates
                )
    finally:
        book.close()
