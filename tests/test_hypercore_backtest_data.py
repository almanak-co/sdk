"""HyperCore backtest data (phase 5, D4): resolution, `/info` history and the dataset.

Everything replays the answers of a real preparation (`tests/hypercore_backtest_support.py`).
"""

import json
import sqlite3

import pytest
from pydantic import ValidationError

from almanak.backtest.history import Capture
from almanak.backtest.hypercore_history import HistoryClient, HistoryScope, HyperCoreCollector
from almanak.connectors.hyperliquid import economic_collection as collection
from almanak.connectors.hyperliquid import economic_data as data
from almanak.connectors.hyperliquid.economic_book import HyperCoreBook
from almanak.connectors.hyperliquid.economic_prepare import gateway_config, require_closed
from almanak.connectors.hyperliquid.resolution import (
    MarketAssertions,
    MarketSelection,
    resolve_market,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import (
    MAX_CANDLES,
    CandleRange,
    CandleSnapshot,
    FundingHistory,
    InfoRequest,
    answered_weight,
    body,
    matched,
    reserved_weight,
)
from tests import hypercore_backtest_support as support

CANDLES = "candleSnapshot.BTC"
FUNDING = "fundingHistory.BTC"


def selection(**assertions):
    return MarketSelection(
        connector="hyperliquid",
        chain=support.resolved().selection.chain,
        market="BTC",
        assertions=MarketAssertions(**assertions) if assertions else None,
    )


def test_retained_reads_resolve_the_btc_market():
    expected = support.resolved()
    with support.retained_reader() as reader:
        market = resolve_market(reader, selection(), expected.block)
    assert market == expected
    assert (market.market.coin, market.market.asset, market.market.size_decimals) == ("BTC", 0, 5)
    assert market.market.max_leverage == 40 and market.market.tier_bound == "150000000.0"
    assert market.summary()["margin_asset"] == str(market.usdc.identifier)


def test_assertions_refuse_a_market_with_other_facts():
    block = support.resolved().block
    with support.retained_reader() as reader:
        resolve_market(reader, selection(asset_index=0, max_leverage=40), block)
        with pytest.raises(ValidationError, match="max_leverage differs"):
            resolve_market(reader, selection(max_leverage=50), block)


def test_resolution_refuses_a_missing_venue_read():
    rows = tuple(row for row in support.resolution_rows() if row.method != "hypercore_info")
    market = support.resolved()
    from almanak.gateway.resolution_evm import RetainedResolutionReader

    with RetainedResolutionReader(market.block.chain, rows) as reader:
        with pytest.raises(DataError, match="lacks a required read"):
            resolve_market(reader, selection(), market.block)


def test_history_queries_send_closed_bodies_and_match_recorded_shapes():
    for label in (CANDLES, FUNDING):
        result = support.recorded(label)
        assert matched(result.request.info, result.result) == result.result
    candles = CandleSnapshot(
        req=CandleRange(coin="BTC", interval="1m", start_time=0, end_time=60000)
    )
    assert body(candles) == {
        "type": "candleSnapshot",
        "req": {"coin": "BTC", "interval": "1m", "startTime": 0, "endTime": 60000},
    }
    assert body(FundingHistory(coin="BTC", start_time=1, end_time=2)) == {
        "type": "fundingHistory",
        "coin": "BTC",
        "startTime": 1,
        "endTime": 2,
    }
    assert reserved_weight(candles) == 20 + 9
    assert reserved_weight(FundingHistory(coin="BTC", start_time=1, end_time=2)) == 20 + 25


def test_a_candle_request_covers_at_most_the_weight_it_reserves():
    widest = CandleRange(coin="BTC", interval="1m", start_time=0, end_time=499 * 60_000)
    query = CandleSnapshot(req=widest)
    assert reserved_weight(query) == answered_weight(query, MAX_CANDLES)
    with pytest.raises(ValidationError, match=f"at most {MAX_CANDLES} candles"):
        CandleRange(coin="BTC", interval="1m", start_time=0, end_time=500 * 60_000)


@pytest.mark.parametrize(
    ("label", "change", "kind"),
    [
        (CANDLES, lambda rows: [{**rows[0], "s": "ETH"}], "market or interval"),
        (CANDLES, lambda rows: [{**rows[0], "t": 0}], "time range"),
        (FUNDING, lambda rows: [{**rows[0], "coin": "ETH"}], "market"),
        (FUNDING, lambda rows: [{**rows[0], "time": 1}], "time range"),
    ],
)
def test_history_answers_for_another_market_or_time_are_refused(label, change, kind):
    result = support.recorded(label)
    with pytest.raises(DataError, match=f"another {kind}"):
        matched(result.request.info, change(result.result))


def test_a_nan_premium_is_refused():
    result = support.recorded(FUNDING)
    with pytest.raises(DataError, match="differs from its shape at 0.premium"):
        matched(result.request.info, [{**result.result[0], "premium": "NaN"}])


def requirement_pair():
    config, settings = support.parameters(), support.settings()
    return data.requirements(config.target("Main"), settings)


def test_requirements_start_at_the_close_and_funding_before_the_window():
    candles, funding = requirement_pair()
    assert (candles.start, candles.end) == (support.START, support.END + 600)
    assert (funding.start, funding.sample_seconds) == (support.START + 1, 3600)
    pages = collection.candle_pages("BTC", "1m", candles)
    assert [page.info.req.start_time for page in pages] == [(support.START - 60) * 1000]
    assert pages[0] == support.recorded(CANDLES).request
    with pytest.raises(ValueError, match="hourly only from 2023-06-08"):
        data.funding_requirement(candles.key.chain, "BTC", data.HOURLY_FUNDING_FROM - 1, 0)


def test_candles_the_venue_no_longer_serves_stop_preparation(tmp_path):
    request = support.recorded(CANDLES).request
    client = support.ReplayClient({request: []})
    with pytest.raises(DataError, match="latest 5000 1m candles"):
        support.prepared(tmp_path, support.parameters(), support.settings(), client)


def test_a_missing_funding_hour_stops_preparation(tmp_path):
    request = support.recorded(FUNDING).request
    rates = support.recorded(FUNDING).result
    client = support.ReplayClient({request: rates[:2] + rates[3:]})
    with pytest.raises(ValueError, match="missing a required rate sample"):
        support.prepared(tmp_path, support.parameters(), support.settings(), client)


def test_a_funding_rate_off_its_hour_is_refused(tmp_path):
    request = support.recorded(FUNDING).request
    rates = support.recorded(FUNDING).result
    moved = [{**rates[0], "time": rates[0]["time"] + 1000}, *rates[1:]]
    client = support.ReplayClient({request: moved})
    with pytest.raises(DataError, match="not written on its hour"):
        support.prepared(tmp_path, support.parameters(), support.settings(), client)


def test_full_funding_pages_continue_from_their_last_rate(tmp_path):
    config, settings = support.parameters(), support.settings()
    _, funding = data.requirements(config.target("Main"), settings)
    first = support.recorded(FUNDING).request
    rates = support.recorded(FUNDING).result
    page = [rates[0]] * collection.FUNDING_PAGE
    follow = InfoRequest(
        info=FundingHistory(
            coin="BTC", start_time=rates[0]["time"] + 1, end_time=first.info.end_time
        )
    )
    client = support.ReplayClient({first: page, follow: rates[1:]})
    records, evidence = collection.read_funding(client, config.target("Main"), funding)
    assert client.sent == [first, follow] and len(evidence) == 2
    assert len(records) == collection.FUNDING_PAGE + len(rates) - 1


def test_the_dataset_rebuilds_every_record_from_its_answer(tmp_path):
    config, settings = support.parameters(), support.settings()
    book = support.prepared(tmp_path, config, settings)
    try:
        target = config.target("Main")
        assert len(book.candles[target].rows) == 371 and len(book.funding[target].rows) == 7
        # The candle that closed at the window start, as the venue wrote it.
        assert book.candle(target, support.START).payload.close == "77435.0"
        assert [row.payload.rate for row in book.funding_between(target, 0, support.END)][:2] == [
            "0.0000125",
            "0.0000125",
        ]
    finally:
        book.close()


def test_a_changed_record_refuses_the_dataset(tmp_path):
    config, settings = support.parameters(), support.settings()
    support.prepared(tmp_path, config, settings).close()
    database = tmp_path / "data.sqlite"
    with sqlite3.connect(database) as db:
        time, raw = db.execute("SELECT time, body FROM inputs ORDER BY time LIMIT 1").fetchone()
        record = json.loads(raw)
        record["payload"]["close"] = "1"
        db.execute("UPDATE inputs SET body=? WHERE time=?", (json.dumps(record), time))
    manifest = json.loads((tmp_path / "dataset.json").read_text())
    from almanak.backtest.dataset import file_digest

    manifest["database_sha256"] = file_digest(database)
    manifest["database_bytes"] = database.stat().st_size
    (tmp_path / "dataset.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="differs from its retained answer"):
        HyperCoreBook(tmp_path, config, settings)


def test_the_gateway_admits_only_declared_reads_and_keeps_answers(tmp_path):
    config, settings = support.parameters(), support.settings()
    scopes = collection.scopes(config, settings)
    assert gateway_config(config, settings).hypercore == scopes
    capture = Capture(tmp_path / "observations.sqlite")
    replay = support.ReplayClient()
    gateway = HyperCoreCollector(capture, scopes, client=replay)
    request = support.recorded(CANDLES).request
    assert gateway.read(request) == support.recorded(CANDLES)
    assert gateway.read(request) == support.recorded(CANDLES)
    assert replay.sent == [request]
    wider = request.info.req.model_copy(update={"end_time": request.info.req.end_time + 60_000})
    with pytest.raises(ValueError, match="outside the declared preparation"):
        gateway.read(InfoRequest(info=CandleSnapshot(req=wider)))
    capture.close()


def test_the_gateway_retries_transient_venue_failures(tmp_path):
    class Flaky(support.ReplayClient):
        def fetch(self, request):
            if not self.sent:
                self.sent.append(request)
                raise DataError("rate_limit", "hyperliquid_info: API rate limit")
            return super().fetch(request)

    capture = Capture(tmp_path / "observations.sqlite")
    scopes = collection.scopes(support.parameters(), support.settings())
    gateway = HyperCoreCollector(capture, scopes, client=Flaky(), pause=lambda seconds: None)
    assert gateway.read(support.recorded(FUNDING).request) == support.recorded(FUNDING)
    capture.close()


def test_the_client_checks_the_gateway_answer_again():
    result = support.recorded(FUNDING)

    class Reader:
        def send(self, operation, request):
            assert operation == "hypercore_read"
            return json.loads(result.model_dump_json())

    assert HistoryClient(Reader()).fetch(result.request) == result
    other = InfoRequest(info=result.request.info.model_copy(update={"start_time": 1}))
    with pytest.raises(DataError, match="another HyperCore request"):
        HistoryClient(Reader()).fetch(other)


def test_scopes_refuse_an_interval_on_funding():
    with pytest.raises(ValidationError, match="only a candle scope"):
        HistoryScope(product="funding", coin="BTC", interval="1m", start_ms=0, end_ms=1)


def test_a_window_must_end_before_the_latest_closed_candle():
    settings = support.settings()
    require_closed(settings, now=support.END + 600 + 60)
    with pytest.raises(ValueError, match="before the latest closed candle"):
        require_closed(settings, now=support.END + 600 + 59)


def test_the_acquisition_gateway_routes_only_hypercore_history_operations():
    from contextlib import nullcontext
    from types import SimpleNamespace

    from almanak.backtest.gateway import AcquisitionGateway, HistoricalRequest

    request = support.recorded(FUNDING).request
    calls = []
    target = SimpleNamespace(
        request_locks=[nullcontext()],
        hypercore=SimpleNamespace(read=lambda value: calls.append(value) or "answer"),
    )
    wrapped = HistoricalRequest.model_validate_json(request.model_dump_json())
    assert AcquisitionGateway.dispatch(target, "hypercore_read", wrapped) == "answer"
    assert calls == [request]
    with pytest.raises(ValueError, match="unsupported HyperCore history operation"):
        AcquisitionGateway.dispatch(target, "vendor_read", request)


def test_acquisition_reads_through_the_gateway_and_publishes(tmp_path, monkeypatch):
    from almanak.connectors.hyperliquid import economic_prepare

    replay = support.ReplayClient()

    class Gateway:
        endpoint = "http://127.0.0.1:1"

        def __init__(self, directory, env_file):
            assert directory == tmp_path

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    class Reader:
        def __init__(self, chain, endpoint, token):
            assert (endpoint, token) == (Gateway.endpoint, "token")

        def send(self, operation, request):
            return json.loads(replay.fetch(request).model_dump_json())

        def __exit__(self, *_):
            pass

    monkeypatch.setattr(economic_prepare, "GatewayProcess", Gateway)
    monkeypatch.setattr(economic_prepare, "HistoricalReader", Reader)
    monkeypatch.setattr(economic_prepare, "read_token", lambda path: "token")
    config, settings = support.parameters(), support.settings()
    economic_prepare.binding.acquire(tmp_path, config, settings, None, None)
    assert len(replay.sent) == 2 and (tmp_path / "dataset.json").is_file()
    HyperCoreBook(tmp_path, config, settings).close()
