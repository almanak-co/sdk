"""A fork test settles GMX requests only with authentic keeper reports, once each."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak import fork_operator, testflow
from almanak.caip import AssetType, ChainId
from almanak.connectors.composed import requests as composed_requests
from almanak.connectors.gmx_v2 import fork_keeper, report_source
from almanak.connectors.gmx_v2.deployments import compiled
from almanak.connectors.gmx_v2.events import EVENT_DATA
from almanak.connectors.gmx_v2.fresh_reports import FreshReports
from almanak.connectors.gmx_v2.oracle import ENVELOPE_TYPES, REPORT_TYPES, decode_report
from almanak.connectors.gmx_v2.request_binding import Requests
from almanak.gateway.block_time import first_block_at
from almanak.gateway.ethereum import DataError
from almanak.models import Block, Chain
from almanak.testflow_keepers import Forks, Keepers
from tests.composed_public import resolved_parameters

ARBITRUM = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
TOKEN = "0x" + "11" * 20
KEEPER = "0x" + "22" * 20
PROVIDER = "0x" + "33" * 20
EMITTER = "0x" + "55" * 20
SIGNATURE = "address,string,string"


def block(number, timestamp, chain=ARBITRUM):
    return Block(chain=chain, number=number, hash="0x" + f"{number:064x}", timestamp=timestamp)


class Blocks:
    """A reader over blocks two seconds apart, counting its reads."""

    def __init__(self, head):
        self.head, self.reads = head, 0

    def pin_block(self, number):
        self.reads += 1
        number = self.head if number == "latest" else number
        return block(number, 1000 + 2 * number)


@pytest.mark.parametrize("timestamp", [1000, 1001, 1002, 1777, 2998, 3000])
def test_first_block_at_finds_the_earliest_block_reaching_a_time(timestamp):
    reader = Blocks(1000)
    found = first_block_at(reader, timestamp)
    assert found.timestamp >= timestamp
    assert found.number == 0 or reader.pin_block(found.number - 1).timestamp < timestamp
    assert reader.reads < 40


def test_first_block_at_refuses_a_time_the_chain_has_not_reached():
    with pytest.raises(DataError, match="no block has reached"):
        first_block_at(Blocks(10), 5000)


def test_a_fork_ahead_of_the_chain_head_waits_from_the_head():
    reader = Blocks(10)
    assert fork_keeper.start_block(reader, 5000).number == 10
    assert fork_keeper.start_block(reader, 1004).number == 2


def deployment_market(chain):
    records = compiled()["chains"][chain]
    oracle = next(a for a, value in records.items() if value["contract"] == "Oracle")
    contracts = Mock(oracle=oracle, model_dump=Mock(return_value={}))
    return SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId(chain)), contracts=contracts)


@pytest.mark.parametrize("chain", ["eip155:42161", "eip155:43114"])
def test_keeper_handlers_are_the_reviewed_deployment_records(chain):
    records = compiled()["chains"][chain]
    handlers = report_source.selections_for(deployment_market(chain))
    assert [records[s.handler]["contract"] for s in handlers] == [
        "OrderHandler",
        "DepositHandler",
        "WithdrawalHandler",
    ]
    assert [s.kind for s in handlers] == ["order", "deposit", "withdrawal"]
    assert {s.oracle for s in handlers} == {deployment_market(chain).contracts.oracle}


def test_a_chain_without_reviewed_keeper_calldata_has_no_report_source():
    with pytest.raises(DataError, match="no reviewed GMX keeper calldata"):
        report_source.selections_for(
            SimpleNamespace(chain=SimpleNamespace(chain_id=ChainId("eip155:1")), contracts=None)
        )


def test_keeper_calldata_is_the_handler_execute_selector_and_unchanged_payloads():
    parameters = ((TOKEN,), (PROVIDER,), (b"\x01\x02",))
    data = fork_keeper.calldata("order", "0x" + "ab" * 32, parameters)
    selection = report_source.selections_for(deployment_market("eip155:42161"))[0]
    assert data.startswith(selection.selector)
    key, decoded = decode(report_source.PARAMETERS, bytes.fromhex(data[10:]))
    assert key == bytes.fromhex("ab" * 32) and decoded == ((TOKEN,), (PROVIDER,), (b"\x01\x02",))


def report(observed_at=2000):
    fields = (
        bytes.fromhex("55" * 32),
        observed_at - 1,
        observed_at,
        0,
        0,
        observed_at + 60,
        1,
        1,
        1,
    )
    signed = encode(
        ENVELOPE_TYPES,
        ((b"\0" * 32,) * 3, encode(REPORT_TYPES, fields), [b"\1" * 32], [b"\2" * 32], b"\0" * 32),
    )
    payload = "0x" + signed.hex()
    return decode_report(
        AssetType(f"eip155:42161/erc20:{TOKEN}"), "0x" + "44" * 20, signed
    ).model_dump(mode="json") | {"signed_payload": payload}


PAYLOAD = bytes.fromhex(report()["signed_payload"][2:])


class Fork:
    """A fork RPC stand-in: configured provider, fork time and mined receipts."""

    def __init__(self, latest=1500, provider=PROVIDER, status="0x1", logs=()):
        self.latest, self.provider, self.status, self.calls = latest, provider, status, []
        self.logs = list(logs)

    def __call__(self, method, params=()):
        self.calls.append((method, params))
        if method == "eth_call":
            return "0x" + self.provider[2:].rjust(64, "0")
        if method == "eth_getBlockByNumber":
            return {"timestamp": hex(self.latest), "baseFeePerGas": "0x5"}
        if method == "evm_setNextBlockTimestamp":
            self.next = params[0]
        if method == "eth_sendTransaction":
            self.latest = self.next
            return "0x" + "66" * 32
        if method == "eth_getTransactionReceipt":
            mined = {"transactionHash": params[0], "status": self.status, "blockNumber": "0x9"}
            return mined | {"logs": self.logs}
        if method == "debug_traceTransaction":
            return {"error": "execution reverted"}
        return None

    def methods(self):
        return [method for method, _ in self.calls]


def target(handler="0x" + "77" * 20):
    contracts = SimpleNamespace(
        oracle="0x" + "88" * 20,
        data_store="0x" + "99" * 20,
        order_handler=handler,
        event_emitter=EMITTER,
    )
    return SimpleNamespace(market=SimpleNamespace(contracts=contracts, chain=ARBITRUM))


def request(status="accepted", key="0x" + "ab" * 32):
    created = SimpleNamespace(identity=SimpleNamespace(block=SimpleNamespace(timestamp=1400)))
    return SimpleNamespace(status=status, key=key, kind="order", allocation="a", events=[created])


def selected():
    source = {"from": KEEPER, "hash": "0x" + "cc" * 32}
    return {"oracle_source": source, "reports": [report()], "publication_block": {"number": 7}}


def test_a_keeper_execution_routes_the_configured_provider_and_releases_the_keeper():
    rpc = Fork()
    mined = fork_keeper.execute(rpc, target(), request(), selected())
    assert mined["status"] == "0x1"
    assert rpc.methods()[-4:] == [
        "evm_setNextBlockTimestamp",
        "eth_sendTransaction",
        "eth_getTransactionReceipt",
        "anvil_stopImpersonatingAccount",
    ]
    sent = next(params[0] for method, params in rpc.calls if method == "eth_sendTransaction")
    assert sent["from"] == KEEPER and sent["to"] == target().market.contracts.order_handler
    assert (sent["maxFeePerGas"], sent["maxPriorityFeePerGas"]) == ("0xb", "0x0")
    assert rpc.next == 2001
    _, (tokens, providers, payloads) = decode(
        report_source.PARAMETERS, bytes.fromhex(sent["data"][10:])
    )
    assert (tokens, providers, payloads) == ((TOKEN,), (PROVIDER,), (PAYLOAD,))


def test_a_keeper_execution_refuses_passed_reports_a_missing_handler_and_no_provider():
    with pytest.raises(DataError, match="passed the selected reports"):
        fork_keeper.execute(Fork(latest=2001), target(), request(), selected())
    with pytest.raises(DataError, match="no order handler"):
        fork_keeper.execute(Fork(), target(handler=None), request(), selected())
    with pytest.raises(DataError, match="no configured provider"):
        fork_keeper.execute(Fork(provider="0x" + "00" * 20), target(), request(), selected())


class Sources:
    def __init__(self, rpc):
        self.rpc, self.output = rpc, None
        self.reader = SimpleNamespace(pin_block=lambda n: block(5, 5000))

    def origin(self, chain):
        return block(1, 1000)


def keeper(rpc, source):
    forks = SimpleNamespace(rpc=lambda chain: rpc, reader=lambda chain: Blocks(2000))
    settled = fork_keeper.Keeper(forks, Path("."))
    settled.sources = {"m": source}
    settled.source = lambda market: source
    return settled


def state(*requests):
    return SimpleNamespace(requests=requests, config=SimpleNamespace(target=lambda name: target()))


def test_each_accepted_request_settles_once_with_its_evidence():
    rpc = Fork()
    source = Mock(acquire=Mock(return_value=selected()))
    settled = keeper(rpc, source)
    pending = state(request(), request("frozen", "0x01"), request("executed", "0x02"))
    [row] = settled.settle(pending)
    assert row["status"] == "mined" and row["keeper"] == KEEPER and row["source_block"] == 7
    assert row["advanced_seconds"] == 2001 - 1500 and row["report_times"] == [2000]
    assert source.acquire.call_args.kwargs == {"not_before": 1500}
    assert settled.settle(pending) == ()


def test_a_reverted_keeper_execution_is_reported_with_its_trace():
    settled = keeper(Fork(status="0x0"), Mock(acquire=Mock(return_value=selected())))
    [row] = settled.settle(state(request()))
    assert row["status"] == "reverted" and row["revert"] == "execution reverted"


def lifecycle_log(name, key="0x" + "ab" * 32, reason="", data=b""):
    """A GMX EventEmitter log naming `key`, with a cancellation's reason fields."""
    values = [((), ())] * 7
    values[5] = ((("reasonBytes", data),), ())
    values[6] = ((("reason", reason),), ())
    return {
        "address": EMITTER,
        "topics": [
            "0x"
            + keccak(text=f"EventLog2(address,string,string,bytes32,bytes32,{EVENT_DATA})").hex(),
            "0x" + keccak(text=name).hex(),
            key,
            "0x" + "00" * 32,
        ],
        "data": "0x"
        + encode(["address", "string", EVENT_DATA], ["0x" + "77" * 20, name, values]).hex(),
        "transactionHash": "0x" + "66" * 32,
        "blockHash": "0x" + "09" * 32,
        "blockNumber": "0x9",
        "logIndex": "0x1",
    }


def bare(name):
    values = [((), ())] * 7
    return "0x" + encode(["address", "string", EVENT_DATA], ["0x" + "77" * 20, name, values]).hex()


def test_a_keeper_cancelled_request_is_reported_with_its_reason():
    selector = keccak(text="OrderNotFulfillableAtAcceptablePrice(uint256,uint256)")[:4]
    data = selector + encode(["uint256", "uint256"], [2440, 10**18])
    logs = [
        lifecycle_log("OrderCancelled", "0x" + "cd" * 32),
        lifecycle_log("OrderCancelled", data=data),
    ]
    settled = keeper(Fork(logs=logs), Mock(acquire=Mock(return_value=selected())))
    [row] = settled.settle(state(request()))
    # The keeper transaction mined, but GMX cancelled the order it was asked to execute.
    assert row["status"] == "mined" and row["request_status"] == "cancelled"
    assert row["reason"] == "OrderNotFulfillableAtAcceptablePrice"
    assert row["reason_bytes"] == "0x" + data.hex()


@pytest.mark.parametrize(
    ("logs", "fields"),
    [
        ([lifecycle_log("OrderExecuted")], {"request_status": "executed"}),
        (
            [lifecycle_log("OrderCancelled", reason="paused", data=b"\x01")],
            {"request_status": "cancelled", "reason": "paused", "reason_bytes": "0x01"},
        ),
        ([lifecycle_log("OrderCancelled", data=b"\x01\x02\x03\x04")], {"reason": "0x01020304"}),
        ([], {}),
        # A malformed emitter log is skipped; a cancellation without reason fields has none.
        ([lifecycle_log("OrderExecuted") | {"removed": True}], {}),
        (
            [lifecycle_log("OrderCancelled") | {"data": bare("OrderCancelled")}],
            {"request_status": "cancelled"},
        ),
    ],
)
def test_a_keeper_row_names_how_the_request_settled(logs, fields):
    settled = keeper(Fork(logs=logs), Mock(acquire=Mock(return_value=selected())))
    [row] = settled.settle(state(request()))
    assert fields.items() <= row.items()
    if not fields:
        assert "request_status" not in row
    if fields == {"request_status": "cancelled"}:
        assert "reason" not in row


def composed(reference):
    _, parameters = resolved_parameters(reference)
    return Mock(config=parameters, outcomes=(), requests=(), transfers=())


def test_a_composed_deployment_settles_its_gmx_component(monkeypatch):
    monkeypatch.setattr(composed_requests, "component_state", lambda state, gmx: "gmx state")
    settled = composed_requests.ComposedRequests.fork_keeper(None, Path("."))
    parts = list(settled.parts(composed("arbitrum_aave_borrow_and_trade_gmx")))
    assert parts == [(settled.gmx, "gmx state")]
    assert list(settled.parts(composed("arbitrum_aave_borrow_and_trade"))) == []
    assert Requests.fork_keeper(None, Path(".")).project("state") == "state"


def test_a_composed_deployment_without_gmx_keeps_the_requested_fork_time(tmp_path, monkeypatch):
    shifted = []
    monkeypatch.setattr(
        testflow, "decoded", lambda wire: composed("arbitrum_aave_borrow_and_trade")
    )
    monkeypatch.setattr(testflow, "shift_clock", lambda root, seconds: shifted.append(seconds))
    monkeypatch.setattr("almanak.workspace.read", lambda root: {"processes": {}})
    keepers = Keepers(tmp_path, tmp_path / "env")
    wire = {"config": {"connector": None}}
    assert keepers.step(wire, 30) == 30
    keepers.settle(wire)
    assert keepers.keeper.settle(composed("arbitrum_aave_borrow_and_trade")) == ()
    assert keepers.failures == [] and keepers.rows == [] and shifted == []


def test_seek_moves_the_report_cursor_forward_only():
    reports = object.__new__(FreshReports)
    reports.market = SimpleNamespace(chain=ARBITRUM)
    reports.cursor, reports.start, reports.candidates = 101, 100, ["0x01"]
    reports.seek(block(500, 0))
    assert (reports.cursor, reports.start, reports.candidates) == (500, 499, [])
    reports.seek(block(200, 0))
    assert reports.cursor == 500
    with pytest.raises(ValueError, match="market chain"):
        reports.seek(block(900, 0, Chain(chain_id=ChainId("eip155:1"), environment="mainnet")))


def test_keepers_settle_only_unresolved_requests_and_keep_the_clock(tmp_path, monkeypatch):
    shifted = []
    monkeypatch.setattr(testflow, "decoded", lambda wire: SimpleNamespace())
    monkeypatch.setattr(testflow, "shift_clock", lambda root, seconds: shifted.append(seconds))
    binding = Mock()
    binding.fork_keeper.return_value.settle.return_value = ({"advanced_seconds": 9},)
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_from_wire",
        lambda config: SimpleNamespace(requests=binding),
    )
    monkeypatch.setattr("almanak.workspace.read", lambda root: {"processes": {}})
    keepers = Keepers(tmp_path, tmp_path / "env")
    keepers.settle(None)
    assert keepers.keeper is None
    binding.unresolved.return_value = False
    keepers.settle({"config": {}})
    binding.fork_keeper.return_value.settle.assert_not_called()
    binding.unresolved.return_value = True
    keepers.settle({"config": {}})
    keepers.settle({"config": {}})
    assert binding.fork_keeper.call_count == 1 and shifted == [9, 9]
    assert keepers.rows == [{"advanced_seconds": 9}] * 2


def test_polling_decodes_a_checkpoint_only_for_a_keeper_and_once_while_unchanged(
    tmp_path, monkeypatch
):
    decoded = []

    def decode(wire):
        decoded.append(wire["n"])
        return SimpleNamespace()

    monkeypatch.setattr(testflow, "decoded", decode)
    monkeypatch.setattr("almanak.workspace.read", lambda root: {"processes": {}})
    keepers = Keepers(tmp_path, tmp_path / "env")
    for _ in range(3):
        keepers.settle({"config": {"connector": "morpho_blue"}, "n": 0})
        assert keepers.step({"config": {"connector": "morpho_blue"}, "n": 0}, 30) == 30
    assert decoded == [] and keepers.keeper is None
    binding = Mock()
    binding.unresolved.return_value = False
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_from_wire",
        lambda config: SimpleNamespace(requests=binding),
    )
    for n in (1, 1, 1, 2, 2):
        keepers.settle({"config": {}, "n": n})
    assert decoded == [1, 2]


def test_a_keeper_failure_stops_the_lifecycle_with_its_reason():
    failing = Mock(settle=Mock(side_effect=DataError("unavailable", "no eligible report")))
    with pytest.raises(testflow.Stopped, match="no eligible report"):
        testflow.settle_requests(failing, {})
    failing.failures = []
    testflow.settle_requests(failing, {}, stop=False)
    assert failing.failures == ["fork keeper could not settle a request: no eligible report"]
    testflow.settle_requests(None, {})


def test_forks_read_each_nodes_endpoint_and_origin(tmp_path, monkeypatch):
    origin = block(3, 1000).model_dump(mode="json")
    entry = {"endpoint": "http://127.0.0.1:1", "origin": origin}
    monkeypatch.setattr(
        "almanak.workspace.read", lambda root: {"processes": {"fork-eip155:42161": entry}}
    )
    forks = Forks(tmp_path, tmp_path / "env")
    assert forks.origin("eip155:42161") == block(3, 1000)
    assert forks.rpc("eip155:42161").endpoint == "http://127.0.0.1:1"
    with pytest.raises(ValueError, match="no node for eip155:1"):
        forks.rpc("eip155:1")


def test_gmx_forks_start_behind_the_head_and_others_at_it(monkeypatch):
    def market(connector):
        return SimpleNamespace(connector=connector, chain=ARBITRUM.chain_id)

    inputs = SimpleNamespace(markets={"a": market("gmx_v2"), "b": market("uniswap_v3")})
    assert fork_operator.anchor_lag(inputs, ARBITRUM.chain_id) == 10800
    inputs.markets = {"b": market("uniswap_v3")}
    assert fork_operator.anchor_lag(inputs, ARBITRUM.chain_id) == 0


def test_a_lagged_anchor_is_the_first_block_at_the_lagged_time(monkeypatch):
    reader = Blocks(5000)
    reader.__enter__ = lambda: reader
    context = Mock(__enter__=Mock(return_value=reader), __exit__=Mock(return_value=False))
    monkeypatch.setattr("almanak.gateway.read_endpoints.read_endpoint", lambda chain, env: "")
    monkeypatch.setattr(
        "almanak.gateway.read_endpoints.mainnet_reader", lambda chain, endpoint: context
    )
    anchor = fork_operator.pinned_anchor(ARBITRUM.chain_id, None, None, lag=3600)
    assert anchor["height"] == 5000 - 1800
    assert fork_operator.pinned_anchor(ARBITRUM.chain_id, None, None)["height"] == 5000


def test_the_report_source_reads_the_origin_chain_market(monkeypatch):
    built = []
    monkeypatch.setattr(fork_keeper, "FreshReports", lambda *args: built.append(args) or "source")
    monkeypatch.setattr(report_source, "selections_for", lambda market: ("handlers", market.chain))
    local = Chain(chain_id=ARBITRUM.chain_id, environment="anvil:0x01")
    market = Mock(chain=local, share_asset="share")
    market.model_copy.side_effect = lambda update: SimpleNamespace(**update)
    forks = SimpleNamespace(reader=lambda chain: "reader", origin=lambda chain: block(3, 1000))
    settled = fork_keeper.Keeper(forks, Path("."))
    assert settled.source(market) == settled.source(market) == "source"
    [(reader, origin_market, origin, selection, *_)] = built
    assert origin_market.chain == ARBITRUM and origin == block(3, 1000) and reader == "reader"
    assert selection == ("handlers", ARBITRUM)


def test_a_report_source_reads_any_reviewed_handler_execution():
    order, deposit, withdrawal = report_source.selections_for(deployment_market("eip155:43114"))
    parameters = ((TOKEN,), (PROVIDER,), (PAYLOAD,))
    data = fork_keeper.calldata("withdrawal", "0x" + "ab" * 32, parameters)
    transaction = {"to": withdrawal.handler, "input": data}
    assert report_source.source_transaction(transaction, order) is None
    vectors = report_source.transaction_vectors(transaction, (order, deposit, withdrawal))
    assert vectors == {TOKEN: (PROVIDER, PAYLOAD)}
    relay = {"to": "0x" + "51" * 20, "input": data}
    assert report_source.transaction_vectors(relay, (order, deposit, withdrawal)) is None
