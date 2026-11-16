"""Historical GMX source access cannot escape captured events or exact callers."""

import pytest

from almanak.backtest.history import Capture, HistoricalGateway, ReadRequest, validate_read
from almanak.backtest.history_authority import CallerScope, TransactionSourceScope
from almanak.backtest.history_logs import LogScope
from almanak.caip import ChainId
from almanak.models import Chain

CHAIN = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
ORACLE = "0x" + "11" * 20
HANDLER = "0x" + "22" * 20
OTHER = "0x" + "33" * 20
HASH = "0x" + "44" * 32
TX = "0x" + "55" * 32
TOPIC = "0x" + "66" * 32
ANCHOR = {"blockHash": HASH, "requireCanonical": True}
SCOPE = LogScope(address=ORACLE, topics=(TOPIC,), first_block=1, last_block=2)
LOG = {
    "address": ORACLE,
    "topics": [TOPIC],
    "blockNumber": "0x1",
    "blockHash": HASH,
    "transactionHash": TX,
    "transactionIndex": "0x0",
    "logIndex": "0x1",
    "data": "0x",
    "removed": False,
}


def gateway(tmp_path, log=None, header=True):
    capture = Capture(tmp_path / "capture.sqlite")
    capture.put(
        ReadRequest(
            chain=CHAIN,
            method="eth_getLogs",
            params=(
                {"address": ORACLE, "topics": [[TOPIC]], "fromBlock": "0x1", "toBlock": "0x2"},
            ),
        ),
        [LOG if log is None else log],
    )
    if header:
        capture.put(
            ReadRequest(chain=CHAIN, method="eth_getBlockByNumber", params=("0x1", False)),
            {"hash": HASH, "number": "0x1"},
        )
    return HistoricalGateway(
        capture, {}, {CHAIN: (ORACLE,)}, {CHAIN: (SCOPE,)}, transaction_scopes={CHAIN: (SCOPE,)}
    )


@pytest.mark.parametrize("method", ["eth_getTransactionByHash", "eth_getTransactionReceipt"])
def test_transaction_and_receipt_replay_only_from_captured_source(tmp_path, method):
    service = gateway(tmp_path)
    request = ReadRequest(chain=CHAIN, method=method, params=(TX,))
    result = {name: LOG[name] for name in ("blockHash", "blockNumber", "transactionIndex")}
    result.update(
        {"hash" if method == "eth_getTransactionByHash" else "transactionHash": TX, "logs": [LOG]}
    )
    service.capture.put(request, result)
    assert service.dispatch("read", request).result == result
    service.close()
    capture = Capture(tmp_path / "capture.sqlite")
    replay = HistoricalGateway(
        capture, {}, {CHAIN: (ORACLE,)}, {CHAIN: (SCOPE,)}, transaction_scopes={CHAIN: (SCOPE,)}
    )
    assert replay.dispatch("read", request).result == result
    replay.close()


@pytest.mark.parametrize(
    "change",
    [
        {"removed": True},
        {"address": OTHER},
        {"topics": []},
        {"topics": [HASH]},
        {"blockNumber": "0x3"},
        {"blockHash": TX},
    ],
)
def test_changed_source_event_cannot_grant_transaction_access(tmp_path, change):
    service = gateway(tmp_path, dict(LOG, **change))
    try:
        with pytest.raises(ValueError):
            service.dispatch(
                "read", ReadRequest(chain=CHAIN, method="eth_getTransactionByHash", params=(TX,))
            )
    finally:
        service.close()


@pytest.mark.parametrize("params", [(HASH,), (TX, {}), ()])
def test_unevidenced_or_extra_transaction_parameters_refused(tmp_path, params):
    service = gateway(tmp_path)
    try:
        with pytest.raises(ValueError):
            service.dispatch(
                "read", ReadRequest(chain=CHAIN, method="eth_getTransactionByHash", params=params)
            )
    finally:
        service.close()


def test_source_header_required(tmp_path):
    service = gateway(tmp_path, header=False)
    try:
        with pytest.raises(ValueError, match="header"):
            service.dispatch(
                "read", ReadRequest(chain=CHAIN, method="eth_getTransactionByHash", params=(TX,))
            )
    finally:
        service.close()


@pytest.mark.parametrize(
    "change",
    [{"hash": HASH}, {"blockHash": TX}, {"blockNumber": "0x2"}, {"transactionIndex": "0x1"}],
)
def test_changed_transaction_identity_refused_even_in_offline_capture(tmp_path, change):
    service = gateway(tmp_path)
    request = ReadRequest(chain=CHAIN, method="eth_getTransactionByHash", params=(TX,))
    service.capture.put(
        request,
        dict(
            {"hash": TX, "blockHash": HASH, "blockNumber": "0x1", "transactionIndex": "0x0"},
            **change,
        ),
    )
    try:
        with pytest.raises(ValueError):
            service.dispatch("read", request)
    finally:
        service.close()


CALL = {"from": HANDLER, "to": ORACLE, "data": "0x12345678"}
CALLER = CallerScope(sender=HANDLER, target=ORACLE, selectors=("0x12345678",))


def test_exact_caller_grant():
    validate_read(
        ReadRequest(chain=CHAIN, method="eth_call", params=(CALL, ANCHOR)),
        (ORACLE,),
        caller_scopes=(CALLER,),
    )


@pytest.mark.parametrize(
    "change",
    [
        {"from": OTHER},
        {"to": OTHER},
        {"data": "0x87654321"},
        {"value": "0x1"},
        {"stateOverride": {}},
        {"data": "0x"},
    ],
)
def test_caller_target_selector_and_extra_fields_refused(change):
    with pytest.raises(ValueError):
        validate_read(
            ReadRequest(chain=CHAIN, method="eth_call", params=(dict(CALL, **change), ANCHOR)),
            (ORACLE, OTHER),
            caller_scopes=(CALLER,),
        )


def test_caller_requires_explicit_binding_and_no_state_override():
    with pytest.raises(ValueError):
        validate_read(ReadRequest(chain=CHAIN, method="eth_call", params=(CALL, ANCHOR)), (ORACLE,))
    with pytest.raises(ValueError):
        validate_read(
            ReadRequest(chain=CHAIN, method="eth_call", params=(CALL, ANCHOR, {})),
            (ORACLE,),
            caller_scopes=(CALLER,),
        )
    with pytest.raises(ValueError):
        ReadRequest(chain=CHAIN, method="eth_sendTransaction", params=(CALL,))


def test_ordinary_log_scope_does_not_grant_transaction_reads(tmp_path):
    service = gateway(tmp_path)
    service.transaction_scopes.clear()
    try:
        with pytest.raises(ValueError):
            service.dispatch(
                "read", ReadRequest(chain=CHAIN, method="eth_getTransactionByHash", params=(TX,))
            )
    finally:
        service.close()


@pytest.mark.parametrize("valid", [True, False])
def test_batched_transport_validates_before_retention_and_replays(tmp_path, valid):
    import json

    import httpx

    from almanak.backtest.history_batch import BatchedHistoricalGateway, ReadBatch
    from almanak.gateway.ethereum import EthereumReader

    original = gateway(tmp_path)
    rows = [
        {
            "hash": TX,
            "blockHash": HASH if valid else TX,
            "blockNumber": "0x1",
            "transactionIndex": "0x0",
        },
        {
            "transactionHash": TX,
            "blockHash": HASH,
            "blockNumber": "0x1",
            "transactionIndex": "0x0",
            "logs": [LOG],
        },
    ]
    calls = []

    def handle(request):
        body = json.loads(request.content)
        calls.append(body)
        return httpx.Response(
            200,
            json=[{"jsonrpc": "2.0", "id": r["id"], "result": rows[i]} for i, r in enumerate(body)],
        )

    reader = EthereumReader(
        CHAIN, "https://rpc.invalid", client=httpx.Client(transport=httpx.MockTransport(handle))
    )
    service = BatchedHistoricalGateway(
        original.capture,
        {CHAIN: reader},
        {CHAIN: (ORACLE,)},
        {CHAIN: (SCOPE,)},
        transaction_scopes={CHAIN: (SCOPE,)},
    )
    requests = tuple(
        ReadRequest(chain=CHAIN, method=method, params=(TX,))
        for method in ("eth_getTransactionByHash", "eth_getTransactionReceipt")
    )
    try:
        if valid:
            assert [
                r.result for r in service.dispatch("read_many", ReadBatch(requests=requests))
            ] == rows
            assert [
                r.result for r in service.dispatch("read_many", ReadBatch(requests=requests))
            ] == rows
        else:
            with pytest.raises(ValueError):
                service.dispatch("read_many", ReadBatch(requests=requests))
            assert all(service.capture.get(r) is None for r in requests)
        assert len(calls) == 1
    finally:
        service.close()


def test_receipt_must_retain_exact_authorizing_log(tmp_path):
    service = gateway(tmp_path)
    request = ReadRequest(chain=CHAIN, method="eth_getTransactionReceipt", params=(TX,))
    result = {
        "transactionHash": TX,
        "blockHash": HASH,
        "blockNumber": "0x1",
        "transactionIndex": "0x0",
        "logs": [],
    }
    service.capture.put(request, result)
    try:
        with pytest.raises(ValueError, match="source event"):
            service.dispatch("read", request)
    finally:
        service.close()


def test_dedicated_gateway_config_preserves_explicit_grants(tmp_path):
    from almanak.backtest.gateway import (
        ContractBinding,
        GatewayConfig,
        HistoricalRequest,
        backend_for,
    )
    from almanak.backtest.history_batch import ReadBatch

    original = gateway(tmp_path)
    request = ReadRequest(chain=CHAIN, method="eth_getTransactionByHash", params=(TX,))
    result = {"hash": TX, "blockHash": HASH, "blockNumber": "0x1", "transactionIndex": "0x0"}
    original.capture.put(request, result)
    call = ReadRequest(chain=CHAIN, method="eth_call", params=(CALL, ANCHOR))
    original.capture.put(call, "0x")
    original.close()
    (tmp_path / "capture.sqlite").rename(tmp_path / "observations.sqlite")
    config = GatewayConfig(
        providers=(),
        contracts=(
            ContractBinding(
                chain=CHAIN,
                contracts=(ORACLE,),
                logs=(SCOPE,),
                callers=(CALLER,),
                transaction_sources=(TransactionSourceScope.model_validate(SCOPE.model_dump()),),
            ),
        ),
    )
    service = backend_for(
        tmp_path, GatewayConfig.model_validate_json(config.model_dump_json()), None
    )
    try:
        response = service.dispatch(
            "read_many", HistoricalRequest(root=ReadBatch(requests=(request, call)))
        )
        assert [r.result for r in response] == [result, "0x"]
        assert service.readers == {}
    finally:
        service.close()


@pytest.mark.parametrize("topics", [[TOPIC], [TOPIC, HASH], [[TOPIC, HASH], TX], [TOPIC, None]])
def test_transaction_source_requires_exact_indexed_event_identity(topics):
    from almanak.backtest.history_authority import validate_source_query

    scope = TransactionSourceScope(address=ORACLE, topics=(TOPIC, TX), first_block=1, last_block=2)
    query = {"address": ORACLE, "topics": topics, "fromBlock": "0x1", "toBlock": "0x2"}
    with pytest.raises(ValueError):
        validate_source_query((query,), (scope,))
    validate_source_query((dict(query, topics=[[TOPIC], TX]),), (scope,))


def test_transaction_source_range_must_be_ordered():
    with pytest.raises(ValueError):
        TransactionSourceScope(address=ORACLE, topics=(TOPIC,), first_block=2, last_block=1)
