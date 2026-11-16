"""Published GMX signed reports: calldata, Oracle events, scans and target acceptance.

Every publication here is synthetic (tests/gmx_synthetic_frames.py signs nothing); the
cases pin the selection and refusal rules that the retained-evidence modules replay
against captured Arbitrum and Avalanche publications.
"""

import pytest
from eth_abi import encode

from almanak.connectors.gmx_v2 import published_collection, published_prices
from almanak.connectors.gmx_v2.economic_data import accounting_market
from almanak.connectors.gmx_v2.oracle_reader import normalized_bounds
from almanak.connectors.gmx_v2.published_collection import (
    PublicationLimits,
    publication_prices,
    scan_publications,
    selected_transactions,
)
from almanak.connectors.gmx_v2.published_prices import (
    PRICE_RESULT,
    read_prices,
    target_acceptance,
)
from almanak.connectors.gmx_v2.published_source import (
    PARAMETERS,
    CalldataSuffix,
    oracle_events,
    report_vectors,
    selected_reports,
)
from almanak.gateway.ethereum import DataError
from tests import gmx_synthetic_frames as frames
from tests.gmx_event_logs import event_log, word

MARKET = accounting_market(frames.exchange_target().market)
AT = frames.block(MARKET)
SOURCE = frames.source()
EMITTER = MARKET.contracts.event_emitter
TRANSACTION = "0x" + "cd" * 32
INDEX_TOPIC = word(MARKET.index_asset.reference)
PUBLICATION = frames.publication(MARKET, AT)
ASSETS = tuple(report.asset for report in PUBLICATION.reports)


def calldata(source=SOURCE, tokens=None, providers=None, payloads=None, suffix=b""):
    reports = PUBLICATION.reports
    vectors = (
        tokens or [r.asset.reference for r in reports],
        providers or [r.provider for r in reports],
        payloads or [bytes.fromhex(r.signed_payload[2:]) for r in reports],
    )
    body = encode(PARAMETERS, (b"\x01" * 32, vectors)) + suffix
    return source.selector + body.hex()


def transaction(**changes):
    return {
        "hash": TRANSACTION,
        "to": SOURCE.handler,
        "input": calldata(),
        "blockHash": AT.hash,
        "blockNumber": hex(AT.number),
        "transactionIndex": "0x2",
        "from": "0x" + "c1" * 20,
    } | changes


def price_event(report, binding=None, *, sender=SOURCE.oracle, **changes):
    binding = binding or next(b for b in PUBLICATION.source_bindings if b.asset == report.asset)
    minimum, maximum = normalized_bounds(binding, report)
    values = {
        "address": {"token": report.asset.reference, "provider": binding.provider},
        "uint256": {
            "minPrice": minimum,
            "maxPrice": maximum,
            "timestamp": report.observed_at - binding.timestamp_adjustment,
        },
    }
    fields = {
        "topics": (word(report.asset.reference),),
        "transaction": TRANSACTION,
        "block_hash": AT.hash,
        "number": AT.number,
    } | changes
    return event_log("OraclePriceUpdate", emitter=EMITTER, sender=sender, values=values, **fields)


def receipt(logs=None, **changes):
    events = logs if logs is not None else [price_event(r) for r in PUBLICATION.reports]
    return {
        "transactionHash": TRANSACTION,
        "blockHash": AT.hash,
        "blockNumber": hex(AT.number),
        "status": "0x1",
        "transactionIndex": "0x2",
        "logs": events,
    } | changes


# Calldata vectors


def test_report_vectors_ignore_other_handlers_and_selectors():
    assert report_vectors(transaction(to="0x" + "01" * 20), SOURCE) is None
    assert report_vectors(transaction(input="0x12345678"), SOURCE) is None
    assert report_vectors(transaction(input=None), SOURCE) is None


def test_report_vectors_key_each_payload_by_token():
    vectors = report_vectors(transaction(), SOURCE)
    assert set(vectors) == {asset.reference for asset in ASSETS}
    provider, payload = vectors[MARKET.index_asset.reference]
    assert provider == frames.PROVIDER and payload[:32] == bytes(32)


def test_report_vectors_refuse_noncanonical_suffix_and_duplicate_tokens():
    with pytest.raises(DataError, match="canonical shape"):
        report_vectors(transaction(input=calldata(suffix=b"\x01")), SOURCE)
    suffixed = SOURCE.model_copy(
        update={"calldata_suffix": CalldataSuffix(prefix="0x01", length=2)}
    )
    assert report_vectors(transaction(input=calldata(suffixed, suffix=b"\x01\x02")), suffixed)
    reference = MARKET.long_asset.reference
    duplicated = calldata(tokens=[reference] * len(ASSETS))
    with pytest.raises(DataError, match="duplicate or incomplete"):
        report_vectors(transaction(input=duplicated), SOURCE)


# Report selection


def vectors():
    return report_vectors(transaction(), SOURCE)


def test_selected_reports_decode_each_payload_for_target_and_source():
    reports, originals = selected_reports(
        vectors(), PUBLICATION.target_bindings, PUBLICATION.source_bindings
    )
    assert reports == PUBLICATION.reports
    assert tuple(r.asset for r in originals) == ASSETS


def test_selected_reports_skip_a_publication_missing_an_asset():
    partial = {k: v for k, v in vectors().items() if k != MARKET.short_asset.reference}
    assert (
        selected_reports(partial, PUBLICATION.target_bindings, PUBLICATION.source_bindings) is None
    )


def test_selected_reports_refuse_mismatched_bindings_and_providers():
    with pytest.raises(ValueError, match="same distinct assets"):
        selected_reports(vectors(), PUBLICATION.target_bindings, PUBLICATION.source_bindings[:1])
    with pytest.raises(ValueError, match="same distinct assets"):
        selected_reports(
            vectors(), PUBLICATION.target_bindings[:1] * 2, PUBLICATION.source_bindings[:1]
        )
    other = {k: ("0x" + "01" * 20, payload) for k, (_, payload) in vectors().items()}
    with pytest.raises(DataError, match="selected source provider"):
        selected_reports(other, PUBLICATION.target_bindings, PUBLICATION.source_bindings)


# Oracle events


def test_oracle_events_keep_only_the_selected_oracle_price_updates():
    unrelated = price_event(PUBLICATION.reports[0], sender="0x" + "01" * 20)
    events = oracle_events(receipt([unrelated, *receipt()["logs"]]), MARKET, SOURCE)
    assert [e.value("address", "token") for e in events] == [a.reference for a in ASSETS]


def test_oracle_events_refuse_failed_transactions_and_other_receipts():
    with pytest.raises(DataError, match="did not succeed"):
        oracle_events(receipt(status="0x0"), MARKET, SOURCE)
    moved = price_event(PUBLICATION.reports[0], number=AT.number + 1)
    with pytest.raises(DataError, match="canonical receipt"):
        oracle_events(receipt([moved]), MARKET, SOURCE)


# Price reads and target acceptance


class Reader:
    def __init__(self, responses, chain=MARKET.chain, replaced=()):
        self.chain, self.responses, self.replaced = chain, responses, set(replaced)
        self.calls = []

    def pin_block(self, number):
        tag = "ee" if number in self.replaced else "ab"
        return frames.block(MARKET, number=number).model_copy(update={"hash": "0x" + tag * 32})

    def _rpc(self, method, args):
        self.calls.append((method, args))
        value = self.responses[method]
        return value(args) if callable(value) else value


def accepted_bounds(prices=PUBLICATION):
    return [
        published_prices.accepted_price(report, binding)
        for report, binding in zip(prices.reports, prices.target_bindings, strict=True)
    ]


def oracle_calls(result):
    def call(args):
        selector = args[0]["data"][:10]
        validate = (
            "0x"
            + published_prices.keccak(text="validatePrices((address[],address[],bytes[]),bool)")[
                :4
            ].hex()
        )
        if selector == validate:
            return "0x" + encode([PRICE_RESULT], [result]).hex()
        return "0x"

    return call


def test_target_acceptance_validates_and_sets_the_published_prices():
    reader = Reader({"eth_call": oracle_calls(accepted_bounds())})
    target_acceptance(reader, MARKET, PUBLICATION)
    assert len(reader.calls) == 2
    request, at = reader.calls[0][1]
    assert request["from"] == MARKET.contracts.order_handler
    assert request["to"] == MARKET.contracts.oracle and at["blockHash"] == AT.hash


def test_target_acceptance_refuses_another_target_and_changed_bounds():
    avalanche = frames.perpetual_target("avalanche").market.chain
    other = PUBLICATION.model_copy(update={"block": AT.model_copy(update={"chain": avalanche})})
    with pytest.raises(ValueError, match="configured market Oracle"):
        target_acceptance(Reader({}), MARKET, other)
    changed = accepted_bounds()
    changed[0] = (*changed[0][:1], changed[0][1] + 1, *changed[0][2:])
    with pytest.raises(DataError, match="differ from the published signed reports"):
        target_acceptance(Reader({"eth_call": oracle_calls(changed)}), MARKET, PUBLICATION)
    raw = accepted_bounds()
    raw[0] = (*raw[0][:3], raw[0][3] + 1, *raw[0][4:])
    with pytest.raises(DataError, match="differ from the published signed reports"):
        target_acceptance(Reader({"eth_call": oracle_calls(raw)}), MARKET, PUBLICATION)
    padded = Reader({"eth_call": lambda args: oracle_calls(accepted_bounds())(args) + "00"})
    with pytest.raises(DataError, match="noncanonical result encoding"):
        target_acceptance(padded, MARKET, PUBLICATION)


def test_read_prices_builds_verified_prices_from_a_selected_publication(monkeypatch):
    accepted = []
    monkeypatch.setattr(
        published_prices,
        "read_bindings",
        lambda reader, market, block, source: (
            PUBLICATION.target_bindings,
            PUBLICATION.source_bindings,
        ),
    )
    monkeypatch.setattr(
        published_prices, "target_acceptance", lambda r, m, prices: accepted.append(prices)
    )
    reader = Reader({})
    prices = read_prices(reader, MARKET, AT, SOURCE, transaction(), receipt())
    assert prices == PUBLICATION and accepted == [prices]
    assert (
        read_prices(reader, MARKET, AT, SOURCE, transaction(to="0x" + "01" * 20), receipt()) is None
    )
    partial = calldata(
        tokens=[a.reference for a in ASSETS[:-1]],
        providers=[frames.PROVIDER] * (len(ASSETS) - 1),
        payloads=[bytes.fromhex(r.signed_payload[2:]) for r in PUBLICATION.reports[:-1]],
    )
    assert read_prices(reader, MARKET, AT, SOURCE, transaction(input=partial), receipt()) is None
    with pytest.raises(DataError, match="one selected-asset Oracle event"):
        read_prices(reader, MARKET, AT, SOURCE, transaction(), receipt(logs=[]))


@pytest.mark.parametrize(
    "tx, rx, chain",
    [
        (transaction(hash="0x" + "01" * 32), receipt(), MARKET.chain),
        (transaction(), receipt(blockHash="0x" + "01" * 32), MARKET.chain),
        (transaction(blockHash="0x" + "01" * 32), receipt(), MARKET.chain),
        (transaction(blockNumber="0x1"), receipt(), MARKET.chain),
        (transaction(), receipt(), frames.perpetual_target("avalanche").market.chain),
    ],
    ids=["hash", "receipt-block", "transaction-block", "number", "chain"],
)
def test_read_prices_refuses_another_chain_or_block(tx, rx, chain):
    with pytest.raises(DataError, match="historical chain/block"):
        read_prices(Reader({}, chain=chain), MARKET, AT, SOURCE, tx, rx)


# Publication scans


def update_log(number=AT.number, transaction=TRANSACTION, index="0x2", **changes):
    log = price_event(PUBLICATION.reports[0], number=number, transaction=transaction, **changes)
    log["topics"][2] = INDEX_TOPIC
    log["transactionIndex"] = index
    return log


def test_selected_transactions_identify_each_publication_once():
    other = update_log(sender="0x" + "01" * 20, transaction="0x" + "02" * 32)
    found = selected_transactions([update_log(), update_log(), other], MARKET, SOURCE)
    assert found == {TRANSACTION: (AT.number, AT.hash, 2)}


@pytest.mark.parametrize(
    "log, message",
    [
        ({**update_log(), "removed": True}, "removed or not canonical"),
        (
            event_log(
                "OrderExecuted", emitter=EMITTER, sender=SOURCE.oracle, topics=(INDEX_TOPIC,)
            ),
            "exact event query",
        ),
        (price_event(PUBLICATION.reports[-1]), "indexed market asset"),
    ],
    ids=["removed", "name", "token"],
)
def test_selected_transactions_refuse_noncanonical_or_other_logs(log, message):
    with pytest.raises(DataError, match=message):
        selected_transactions([log], MARKET, SOURCE)


def test_selected_transactions_refuse_conflicting_identities():
    with pytest.raises(DataError, match="conflicting block identities"):
        selected_transactions([update_log(), update_log(index="0x3")], MARKET, SOURCE)


def test_publication_prices_reads_its_exact_transaction(monkeypatch):
    monkeypatch.setattr(published_collection, "read_prices", lambda *args: args[2:])
    reader = Reader(
        {"eth_getTransactionByHash": transaction(), "eth_getTransactionReceipt": receipt()}
    )
    identity = (AT.number, AT.hash, 2)
    block, source, tx, rx = publication_prices(reader, MARKET, SOURCE, TRANSACTION, identity)
    assert block == AT and tx == transaction() and rx == receipt()
    with pytest.raises(DataError, match="canonical block"):
        publication_prices(Reader({}, replaced={AT.number}), MARKET, SOURCE, TRANSACTION, identity)
    missing = Reader({"eth_getTransactionByHash": None, "eth_getTransactionReceipt": receipt()})
    with pytest.raises(DataError, match="exact transaction and receipt"):
        publication_prices(missing, MARKET, SOURCE, TRANSACTION, identity)
    with pytest.raises(DataError, match="transaction ordering"):
        publication_prices(reader, MARKET, SOURCE, TRANSACTION, (AT.number, AT.hash, 3))


def limits(**changes):
    values = {
        "page_blocks": 2,
        "maximum_blocks": 10,
        "maximum_pages": 5,
        "maximum_logs": 10,
        "maximum_transactions": 10,
    }
    return PublicationLimits(**(values | changes))


def pages(logs_by_start):
    return lambda args: logs_by_start.get(int(args[0]["fromBlock"], 16), [])


def test_scan_publications_pages_the_range_and_merges_transactions():
    first, last = frames.block(MARKET, number=100), frames.block(MARKET, number=104)
    later = update_log(number=103, transaction="0x" + "03" * 32)
    reader = Reader({"eth_getLogs": pages({100: [update_log(number=101)], 102: [later]})})
    reader.pin_block = lambda number: frames.block(MARKET, number=number)
    found = scan_publications(reader, MARKET, SOURCE, first, last, limits())
    assert found == {TRANSACTION: (101, AT.hash, 2), "0x" + "03" * 32: (103, AT.hash, 2)}
    assert [int(args[0]["fromBlock"], 16) for _, args in reader.calls] == [100, 102, 104]


def scan(logs_by_start, scan_limits=None, last=104):
    reader = Reader({"eth_getLogs": pages(logs_by_start)})
    reader.pin_block = lambda number: frames.block(MARKET, number=number)
    first, end = frames.block(MARKET, number=100), frames.block(MARKET, number=last)
    return scan_publications(reader, MARKET, SOURCE, first, end, scan_limits or limits())


def test_scan_publications_refuses_each_explicit_limit():
    with pytest.raises(ValueError, match="block or page limit"):
        scan({}, limits(maximum_blocks=4))
    with pytest.raises(ValueError, match="block or page limit"):
        scan({}, limits(maximum_pages=2))
    with pytest.raises(DataError, match="explicit log limit"):
        scan({100: [update_log(number=101)] * 2}, limits(maximum_logs=1))
    two = {
        100: [update_log(number=101)],
        102: [update_log(number=103, transaction="0x" + "03" * 32)],
    }
    with pytest.raises(DataError, match="transaction limit"):
        scan(two, limits(maximum_transactions=1))


def test_scan_publications_refuses_escaped_or_repeated_publications():
    with pytest.raises(DataError, match="escaped its authorized page"):
        scan({100: [update_log(number=103)]})
    with pytest.raises(DataError, match="distinct pages"):
        scan({100: [update_log(number=101)], 102: [update_log(number=102)]})


def test_scan_publications_refuses_moved_boundaries():
    reader = Reader({"eth_getLogs": []}, replaced={104})
    first, last = frames.block(MARKET, number=100), frames.block(MARKET, number=104)
    with pytest.raises(DataError, match="changed canonical boundaries"):
        scan_publications(reader, MARKET, SOURCE, first, last, limits())


def test_published_source_selector_distinguishes_request_kinds():
    kinds = ("order", "deposit", "withdrawal", "relay_batch")
    selectors = {SOURCE.model_copy(update={"request_kind": kind}).selector for kind in kinds}
    assert len(selectors) == len(kinds)
