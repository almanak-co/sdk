"""Published reports retain actual historical source identity and target Oracle acceptance."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from eth_abi import decode, encode
from pydantic import ValidationError

from almanak.connectors.gmx_v2.economic_spot_types import SpotState
from almanak.connectors.gmx_v2.published_prices import (
    PublishedPrices,
    read_prices,
    target_acceptance,
)
from almanak.connectors.gmx_v2.published_source import (
    PARAMETERS,
    CalldataSuffix,
    oracle_events,
    report_vectors,
    selected_reports,
    verify_event,
)
from almanak.gateway.ethereum import DataError, EthereumReader
from tests.gmx_retired import retired_closure as retired_closure

ROOT = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-09-gmx-prepared"


@pytest.fixture(params=["arbitrum", "avalanche"])
def published(request):
    name = request.param
    prices = PublishedPrices.model_validate_json(
        (ROOT / f"{name}-published-prices.json").read_text()
    )
    comparison = json.loads((ROOT / f"{name}-spot-reader-comparison.json").read_text())
    market = SpotState.model_validate_json(json.dumps(comparison["state"])).market
    rows = json.loads(gzip.decompress((ROOT / f"{name}-published-capture.json.gz").read_bytes()))
    captured = {
        json.dumps([row["request"]["method"], row["request"]["params"]], sort_keys=True): row[
            "result"
        ]
        for row in rows
    }
    reader = EthereumReader(prices.block.chain, "http://unused.invalid")
    reader._rpc = Mock(
        side_effect=lambda method, params: captured[json.dumps([method, params], sort_keys=True)]
    )
    reader.pin_block(prices.block.number)
    yield SimpleNamespace(
        prices=prices,
        market=market,
        reader=reader,
        transaction=comparison["pricing_input_source"]["source_transaction"],
        receipt=comparison["pricing_input_source"]["source_receipt"],
    )
    reader._client.close()


# The retained capture is of the retired Oracle, whose ValidatedPrice had no raw bounds;
# the replay claims retained-source integrity, not the current deployment.
@pytest.mark.usefixtures("retired_closure")
def test_actual_published_report_replays_historical_bindings_and_target_calls(published):
    p = published
    assert (
        read_prices(p.reader, p.market, p.prices.block, p.prices.source, p.transaction, p.receipt)
        == p.prices
    )
    assert p.prices.minimum_timestamp <= p.prices.maximum_timestamp <= p.prices.block.timestamp
    assert all(binding.block == p.prices.block for binding in p.prices.source_bindings)
    assert all(price.minimum > 0 for price in p.prices.prices)


def test_unselected_handler_or_selector_cannot_supply_prices(published):
    p = published
    for change in ({"to": "0x" + "12" * 20}, {"input": "0x12345678"}):
        assert report_vectors(p.transaction | change, p.prices.source) is None


def test_noncanonical_and_duplicate_vectors_refused(published):
    p = published
    with pytest.raises(DataError, match="canonical shape"):
        report_vectors(p.transaction | {"input": p.transaction["input"] + "00"}, p.prices.source)
    key, (tokens, providers, payloads) = decode(
        PARAMETERS, bytes.fromhex(p.transaction["input"][10:])
    )
    length = p.prices.source.calldata_suffix.length
    suffix = p.transaction["input"][-2 * length :] if length else ""
    calldata = (
        p.prices.source.selector
        + encode(
            PARAMETERS,
            (key, (tokens + tokens[:1], providers + providers[:1], payloads + payloads[:1])),
        ).hex()
        + suffix
    )
    with pytest.raises(DataError, match="duplicate or incomplete"):
        report_vectors(p.transaction | {"input": calldata}, p.prices.source)


def test_missing_asset_and_changed_provider_cannot_substitute(published):
    p = published
    vectors = report_vectors(p.transaction, p.prices.source)
    asset = p.prices.reports[0].asset.reference
    wrong = vectors | {asset: ("0x" + "12" * 20, vectors[asset][1])}
    with pytest.raises(DataError, match="selected source provider"):
        selected_reports(wrong, p.prices.target_bindings, p.prices.source_bindings)
    del vectors[asset]
    assert selected_reports(vectors, p.prices.target_bindings, p.prices.source_bindings) is None
    with pytest.raises(ValueError, match="same distinct assets"):
        selected_reports({}, p.prices.target_bindings, p.prices.source_bindings[:1])


def test_failed_or_misattributed_receipt_refused(published):
    p = published
    with pytest.raises(DataError, match="did not succeed"):
        oracle_events(p.receipt | {"status": "0x0"}, p.market, p.prices.source)
    with pytest.raises(DataError, match="historical chain/block"):
        read_prices(
            p.reader,
            p.market,
            p.prices.block,
            p.prices.source,
            p.transaction,
            p.receipt | {"blockHash": "0x" + "12" * 32},
        )
    _, originals = selected_reports(
        report_vectors(p.transaction, p.prices.source),
        p.prices.target_bindings,
        p.prices.source_bindings,
    )
    with pytest.raises(DataError, match="one selected-asset"):
        verify_event((), p.prices.source_bindings[0], originals[0])


@pytest.mark.parametrize("field", ["source_bindings", "target_bindings"])
def test_bindings_cannot_be_relabelled_to_another_historical_block(published, field):
    data = published.prices.model_dump(mode="json")
    data[field][0]["block"]["number"] += 1
    with pytest.raises(ValidationError, match="historical block"):
        PublishedPrices.model_validate_json(json.dumps(data))


def test_target_oracle_must_return_exact_canonical_prices(published):
    p = published
    p.reader._rpc = Mock(
        return_value="0x" + encode(("(address,uint256,uint256,uint256,address)[]",), ([],)).hex()
    )
    with pytest.raises(DataError, match="bounds differ"):
        target_acceptance(p.reader, p.market, p.prices)
    p.reader._rpc = Mock(
        return_value="0x"
        + encode(("(address,uint256,uint256,uint256,address)[]",), ([],)).hex()
        + "00"
    )
    with pytest.raises(DataError, match="noncanonical result"):
        target_acceptance(p.reader, p.market, p.prices)


def test_suffix_declaration_cannot_exceed_bound():
    with pytest.raises(ValidationError, match="prefix exceeds"):
        CalldataSuffix(prefix="0x1234", length=1)


@pytest.mark.parametrize(
    "change, message",
    [
        ("source_oracle", "source Oracle"),
        ("source_store", "different DataStores"),
        ("source_feed", "explicitly configured"),
        ("future", "future or invalid"),
        ("duplicate", "distinct asset"),
    ],
)
def test_persisted_source_and_temporal_identity_cannot_be_changed(published, change, message):
    data = published.prices.model_dump(mode="json")
    if change == "source_oracle":
        data["source_bindings"][0]["oracle"] = "0x" + "12" * 20
    elif change == "source_store":
        data["source_bindings"][0]["data_store"] = "0x" + "12" * 20
    elif change == "source_feed":
        data["source_bindings"][0]["feed_id"] = "0x" + "12" * 32
    elif change == "future":
        timestamp = published.prices.minimum_timestamp - 1
        data["block"]["timestamp"] = timestamp
        for name in ("source_bindings", "target_bindings"):
            for binding in data[name]:
                binding["block"]["timestamp"] = timestamp
    else:
        data["reports"][1] = data["reports"][0]
    with pytest.raises(ValidationError, match=message):
        PublishedPrices.model_validate_json(json.dumps(data))


def publication_limits(**changes):
    from almanak.connectors.gmx_v2.published_collection import PublicationLimits

    return PublicationLimits(
        **dict(
            page_blocks=1,
            maximum_blocks=1,
            maximum_pages=1,
            maximum_logs=100,
            maximum_transactions=100,
        )
        | changes
    )


# The retained capture is of the retired Oracle, whose ValidatedPrice had no raw bounds;
# the replay claims retained-source integrity, not the current deployment.
@pytest.mark.usefixtures("retired_closure")
def test_actual_bounded_collection_replays_all_selected_publications(published):
    from almanak.connectors.gmx_v2.published_collection import collect_publications

    p = published
    result = collect_publications(
        p.reader, p.market, p.prices.source, p.prices.block, p.prices.block, publication_limits()
    )
    assert result == (p.prices,)


@pytest.mark.parametrize(
    "maximum, message", [("maximum_blocks", "block or page"), ("maximum_pages", "block or page")]
)
def test_publication_span_bounds_apply_before_provider_reads(published, maximum, message):
    from almanak.connectors.gmx_v2.published_collection import scan_publications

    p = published
    last = p.prices.block.model_copy(update={"number": p.prices.block.number + 2})
    limits = publication_limits(maximum_blocks=3, maximum_pages=3).model_copy(update={maximum: 1})
    before = p.reader._rpc.call_count
    with pytest.raises(ValueError, match=message):
        scan_publications(p.reader, p.market, p.prices.source, p.prices.block, last, limits)
    assert p.reader._rpc.call_count == before


def test_empty_publication_range_is_distinct_from_missing_or_failed_reads(published):
    from almanak.connectors.gmx_v2.published_collection import scan_publications

    p = published
    captured = p.reader._rpc.side_effect
    p.reader._rpc.side_effect = lambda method, params: (
        [] if method == "eth_getLogs" else captured(method, params)
    )
    assert (
        scan_publications(
            p.reader,
            p.market,
            p.prices.source,
            p.prices.block,
            p.prices.block,
            publication_limits(),
        )
        == {}
    )
    p.reader._rpc.side_effect = DataError("unavailable", "missing capture")
    with pytest.raises(DataError, match="missing capture"):
        scan_publications(
            p.reader,
            p.market,
            p.prices.source,
            p.prices.block,
            p.prices.block,
            publication_limits(),
        )


def test_publication_log_limit_cannot_be_satisfied_by_truncation(published):
    from almanak.connectors.gmx_v2.published_collection import scan_publications

    p = published
    captured = p.reader._rpc.side_effect
    p.reader._rpc.side_effect = lambda method, params: (
        captured(method, params) * 101 if method == "eth_getLogs" else captured(method, params)
    )
    with pytest.raises(DataError, match="explicit log limit"):
        scan_publications(
            p.reader,
            p.market,
            p.prices.source,
            p.prices.block,
            p.prices.block,
            publication_limits(),
        )


def captured_publication_logs(published):
    from almanak.connectors.gmx_v2.published_collection import publication_scope

    p = published
    scope = publication_scope(p.market, p.prices.block, p.prices.block)
    return p.reader._rpc(
        "eth_getLogs",
        [
            {
                "address": scope.address,
                "topics": list(scope.topics),
                "fromBlock": hex(scope.first_block),
                "toBlock": hex(scope.last_block),
            }
        ],
    )


def test_removed_log_and_conflicting_publication_identity_refused(published):
    from almanak.connectors.gmx_v2.published_collection import selected_transactions

    p = published
    logs = captured_publication_logs(p)
    with pytest.raises(DataError, match="removed or not canonical"):
        selected_transactions([row | {"removed": True} for row in logs], p.market, p.prices.source)
    changed = [row | {"blockHash": "0x" + "12" * 32} for row in logs]
    with pytest.raises(DataError, match="conflicting block identities"):
        selected_transactions(logs + changed, p.market, p.prices.source)


def test_publication_must_keep_canonical_block_and_transaction_order(published):
    from almanak.connectors.gmx_v2.published_collection import publication_prices

    p = published
    index = int(p.transaction["transactionIndex"], 16)
    identity = p.prices.block.number, p.prices.block.hash, index
    with pytest.raises(DataError, match="canonical block"):
        publication_prices(
            p.reader,
            p.market,
            p.prices.source,
            p.prices.transaction,
            (identity[0], "0x" + "12" * 32, index),
        )
    with pytest.raises(DataError, match="transaction ordering"):
        publication_prices(
            p.reader,
            p.market,
            p.prices.source,
            p.prices.transaction,
            (identity[0], identity[1], index + 1),
        )


def test_log_with_another_query_identity_cannot_grant_source_access(published):
    from almanak.connectors.gmx_v2.published_collection import selected_transactions

    p = published
    logs = captured_publication_logs(p)
    with pytest.raises(DataError, match="exact event query"):
        selected_transactions(
            [row | {"address": "0x" + "12" * 20} for row in logs], p.market, p.prices.source
        )
    wrong_source = p.prices.source.model_copy(update={"oracle": "0x" + "12" * 20})
    assert selected_transactions(logs, p.market, wrong_source) == {}


def test_provider_page_cannot_expand_its_block_scope(published):
    from almanak.connectors.gmx_v2.published_collection import scan_publications

    p = published
    captured = p.reader._rpc.side_effect
    p.reader._rpc.side_effect = lambda method, params: (
        [row | {"blockNumber": hex(p.prices.block.number + 1)} for row in captured(method, params)]
        if method == "eth_getLogs"
        else captured(method, params)
    )
    with pytest.raises(DataError, match="authorized page"):
        scan_publications(
            p.reader,
            p.market,
            p.prices.source,
            p.prices.block,
            p.prices.block,
            publication_limits(),
        )
