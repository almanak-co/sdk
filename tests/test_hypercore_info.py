"""HyperCore `/info` reads: closed request bodies, recorded answer shapes, the gateway hop."""

import json
import secrets
import threading

import httpx
import pytest
from pydantic import ValidationError

from almanak.gateway import hypercore_info
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import (
    API,
    ActiveAssetData,
    ClearinghouseState,
    ExtraAgents,
    GatewayInfo,
    HyperCoreInfo,
    InfoRequest,
    L2Book,
    MetaAndAssetCtxs,
    OpenOrders,
    OrderStatus,
    UserFillsByTime,
    UserFunding,
    UserNonFundingLedgerUpdates,
    UserRole,
    answered_weight,
    body,
    matched,
    reserved_weight,
    weight,
)
from almanak.gateway.observation_gateway import GatewayRequest, ObservationGateway
from almanak.gateway.rate_limits import VARIABLES
from almanak.gateway.service import http_server
from tests.hypercore_backtest_support import RECORDINGS as HISTORY
from tests.hypercore_recordings import RECORDINGS, SUBJECTS, answer, recorded_request

USER = SUBJECTS["leader"]


@pytest.fixture(autouse=True)
def clean_limits(monkeypatch):
    for variable, _ in VARIABLES.values():
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr("almanak.gateway.rate_limits._windows", {})


def test_every_recorded_answer_matches_its_closed_shape():
    types = set()
    for label in RECORDINGS:
        query = recorded_request(label).info
        types.add(query.type)
        assert matched(query, answer(label)) == answer(label)
    # Every admitted query has at least one recorded answer; the history queries' are
    # the answers a real backtest preparation retained (phase 5).
    history = {row["request"]["info"]["type"] for row in HISTORY.values()}
    assert types | history == set(hypercore_info.RESPONSES)


def test_request_bodies_carry_only_the_api_fields():
    fills = UserFillsByTime(user=USER.upper().replace("0X", "0x"), start_time=1, end_time=2)
    assert body(fills) == {"type": "userFillsByTime", "user": USER, "startTime": 1, "endTime": 2}
    assert body(L2Book(coin="kPEPE")) == {"type": "l2Book", "coin": "kPEPE"}
    assert body(MetaAndAssetCtxs()) == {"type": "metaAndAssetCtxs"}
    cloid = "0x" + "ab" * 16
    assert body(OrderStatus(user=USER, oid=cloid))["oid"] == cloid
    assert body(OrderStatus(user=USER, oid=7))["oid"] == 7


@pytest.mark.parametrize(
    "info",
    [
        {"type": "frontendOpenOrders", "user": USER},
        {"type": "openOrders", "user": USER, "dex": "xyz"},
        {"type": "activeAssetData", "user": USER, "coin": "@107"},
        {"type": "extraAgents", "user": USER, "dex": ""},
        {"type": "l2Book", "coin": "@107"},
        {"type": "l2Book", "coin": "xyz:XYZ100"},
        {"type": "l2Book", "coin": "BTC", "nSigFigs": 2},
        {"type": "meta", "dex": "xyz"},
        {"type": "userRole", "user": "0x12"},
        {"type": "orderStatus", "user": USER, "oid": "0x" + "ab" * 15},
        {"type": "orderStatus", "user": USER, "oid": -1},
        {"type": "userFillsByTime", "user": USER, "start_time": 5, "end_time": 4},
        {"type": "userFillsByTime", "user": USER, "start_time": 1},
        {
            "type": "userFillsByTime",
            "user": USER,
            "start_time": 1,
            "end_time": 2,
            "aggregate": True,
        },
    ],
)
def test_requests_outside_the_whitelist_are_refused(info):
    with pytest.raises(ValidationError):
        InfoRequest.model_validate_json(json.dumps({"info": info}))


def drifted(label: str, change) -> object:
    value = answer(label)
    change(value)
    return value


@pytest.mark.parametrize(
    ("label", "change", "where"),
    [
        (
            "clearinghouseState.hlp_child",
            lambda a: a["assetPositions"][0]["position"].update(newField="1"),
            "assetPositions.0.position.newField",
        ),
        (
            "clearinghouseState.hlp_child",
            lambda a: a["marginSummary"].pop("accountValue"),
            "marginSummary.accountValue",
        ),
        (
            "clearinghouseState.leader",
            lambda a: a.update(withdrawable=463.94),
            "withdrawable",
        ),
        ("metaAndAssetCtxs", lambda a: a[1].pop(), ""),
        ("userRole.leader", lambda a: a.update(role="owner"), ""),
        (
            "userNonFundingLedgerUpdates.leader",
            lambda a: a[0]["delta"].pop("type"),
            "0.delta",
        ),
        ("userFillsByTime.hlp_child", lambda a: a[0].update(px="1e-05"), "0.px"),
    ],
)
def test_an_answer_that_differs_from_its_recorded_shape_is_refused(label, change, where):
    query = recorded_request(label).info
    with pytest.raises(DataError) as refused:
        matched(query, drifted(label, change))
    assert refused.value.kind == "malformed"
    assert f"{query.type} answer differs from its shape" in str(refused.value)
    assert where in str(refused.value)


@pytest.mark.parametrize(
    ("label", "change", "what"),
    [
        ("l2Book.BTC", lambda a: a.update(coin="ETH"), "market"),
        ("activeAssetData.leader", lambda a: a.update(coin="ETH"), "account or market"),
        ("activeAssetData.leader", lambda a: a.update(user="0x" + "12" * 20), "account"),
        (
            "orderStatus.filled",
            lambda a: a["order"]["order"].update(oid=a["order"]["order"]["oid"] + 1),
            "order",
        ),
        ("userFillsByTime.hlp_child", lambda a: a[0].update(time=0), "time range"),
        ("userFunding.hlp_child.hour", lambda a: a[-1].update(time=2**53 - 1), "time range"),
    ],
)
def test_an_answer_for_another_identifier_is_refused(label, change, what):
    query = recorded_request(label).info
    with pytest.raises(DataError) as refused:
        matched(query, drifted(label, change))
    assert refused.value.kind == "source_mismatch"
    assert f"answer is for another {what}" in str(refused.value)


def test_an_order_asked_by_cloid_must_echo_that_cloid():
    child = SUBJECTS["hlp_child"]
    by_cloid = OrderStatus(user=child, oid="0x" + "ab" * 16)
    # The recorded order carries no cloid, so it is no answer for one.
    with pytest.raises(DataError) as refused:
        matched(by_cloid, answer("orderStatus.filled"))
    assert refused.value.kind == "source_mismatch"
    known = answer("orderStatus.filled")
    known["order"]["order"]["cloid"] = by_cloid.oid
    assert matched(by_cloid, known) == known
    assert matched(by_cloid, answer("orderStatus.unknown_cloid")) == {"status": "unknownOid"}


def test_open_orders_admit_a_cloid_only_in_its_own_form():
    query = recorded_request("openOrders.cloid_trader").info
    with_cloids = answer("openOrders.cloid_trader")
    assert all("cloid" in order for order in with_cloids)
    assert all("cloid" not in order for order in answer("openOrders.hlp_child"))
    assert matched(query, with_cloids) == with_cloids
    with_cloids[0]["cloid"] = "0x1234"
    with pytest.raises(DataError) as refused:
        matched(query, with_cloids)
    assert refused.value.kind == "malformed" and "0.cloid" in str(refused.value)


def test_fills_admit_the_cloid_of_their_order():
    query = recorded_request("userFillsByTime.canary_safe").info
    fills = answer("userFillsByTime.canary_safe")
    assert [fill["dir"] for fill in fills] == ["Open Long", "Close Long"]
    assert all(fill["cloid"] for fill in fills)
    assert all("cloid" not in fill for fill in answer("userFillsByTime.hlp_child"))
    assert matched(query, fills) == fills
    fills[0]["cloid"] = "0x12"
    with pytest.raises(DataError) as refused:
        matched(query, fills)
    assert refused.value.kind == "malformed" and "0.cloid" in str(refused.value)


def test_the_custody_path_writes_its_own_ledger_types():
    # The canary Safe: activation, class transfers both ways, sends, the withdrawal.
    kinds = [entry["delta"]["type"] for entry in answer("userNonFundingLedgerUpdates.canary_safe")]
    assert {"accountActivationGas", "spotTransfer", "accountClassTransfer", "send"} == set(kinds)


def test_ledger_deltas_keep_their_fields_as_received():
    # Two liquidation entries share one hash, time and delta type: only their fields differ.
    entries = answer("userNonFundingLedgerUpdates.liquidated")
    liquidations = [e for e in entries if e["delta"]["type"] == "liquidation"]
    assert len(liquidations) == 2
    assert len({(e["hash"], e["time"]) for e in liquidations}) == 1
    assert liquidations[0]["delta"] != liquidations[1]["delta"]


def test_recorded_pages_stop_at_the_api_limits():
    # Funding pages hold at most 500 entries; ledger pages 2000, not the documented 500.
    assert len(answer("userFunding.hlp_child.page")) == 500
    assert len(answer("userNonFundingLedgerUpdates.hlp_parent.page")) == 2000


def test_weights_follow_the_documented_table():
    assert weight(UserRole(user=USER)) == 60
    assert weight(ClearinghouseState(user=USER)) == 2
    assert weight(L2Book(coin="BTC")) == 2
    assert weight(MetaAndAssetCtxs()) == 20
    assert weight(ActiveAssetData(user=USER, coin="BTC")) == 20
    assert weight(OpenOrders(user=USER)) == 20
    assert weight(ExtraAgents(user=USER)) == 20
    fills = UserFillsByTime(user=USER, start_time=0, end_time=1)
    funding = UserFunding(user=USER, start_time=0, end_time=1)
    assert weight(fills) == 20
    # A page reserves its most: 2000 fills or 500 payments, one more per 20 items.
    ledger = UserNonFundingLedgerUpdates(user=USER, start_time=0, end_time=1)
    assert (reserved_weight(fills), reserved_weight(funding)) == (120, 45)
    # Undocumented for the ledger, charged conservatively for its 2000-entry pages.
    assert (reserved_weight(ledger), answered_weight(ledger, 104)) == (120, 26)
    assert [answered_weight(fills, items) for items in (0, 1, 20, 21)] == [20, 21, 21, 22]
    assert reserved_weight(UserRole(user=USER)) == answered_weight(UserRole(user=USER), 0)


def test_a_page_reserves_its_most_and_settles_to_its_answer(monkeypatch):
    query = recorded_request("userFillsByTime.hlp_child")
    fills = answer("userFillsByTime.hlp_child")
    cost = answered_weight(query.info, len(fills))
    assert cost < reserved_weight(query.info)
    monkeypatch.setenv(VARIABLES["hyperliquid"][0], str(reserved_weight(query.info) + cost))
    info = direct(lambda _: httpx.Response(200, json=fills))
    # Each page holds its full reservation only until its answer is counted.
    info.fetch(query)
    info.fetch(query)
    with pytest.raises(DataError) as refused:
        info.fetch(query)
    assert refused.value.kind == "rate_limit"
    info.close()


def direct(handler) -> HyperCoreInfo:
    info = HyperCoreInfo()
    info.client = httpx.Client(transport=httpx.MockTransport(handler))
    return info


def test_a_query_is_charged_its_weight_before_it_is_sent(monkeypatch):
    sent = []
    answers = {"userRole": "userRole.leader", "clearinghouseState": "clearinghouseState.leader"}

    def api(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json=answer(answers[sent[-1]["type"]]))

    monkeypatch.setenv(VARIABLES["hyperliquid"][0], "62")
    info = direct(api)
    role = InfoRequest(info=UserRole(user=USER))
    perp = InfoRequest(info=ClearinghouseState(user=USER))
    result = info.fetch(role)
    assert (result.request, result.result) == (role, {"role": "user"})
    assert result.received_at > 1_700_000_000_000
    # 60 of the 62 are spent: another role query is refused, a two-weight one still fits.
    for refused_request, fits in ((role, False), (perp, True), (perp, False)):
        if fits:
            info.fetch(refused_request)
            continue
        with pytest.raises(DataError) as refused:
            info.fetch(refused_request)
        assert refused.value.kind == "rate_limit"
    assert sent == [body(role.info), body(perp.info)]
    info.close()


@pytest.mark.parametrize(
    ("response", "kind"),
    [
        (httpx.Response(429, json={}), "rate_limit"),
        (httpx.Response(500, text="boom"), "unavailable"),
        (httpx.Response(422, text="Failed to deserialize the JSON body"), "unavailable"),
        (httpx.Response(200, text="not json"), "malformed"),
        (httpx.Response(200, json={"role": "owner"}), "malformed"),
    ],
)
def test_api_answers_keep_their_category(response, kind):
    info = direct(lambda _: response)
    with pytest.raises(DataError) as refused:
        info.fetch(InfoRequest(info=UserRole(user=USER)))
    assert refused.value.kind == kind


def test_an_unreachable_api_is_unavailable():
    def fail(_):
        raise httpx.ConnectError("refused")

    with pytest.raises(DataError) as refused:
        direct(fail).fetch(InfoRequest(info=UserRole(user=USER)))
    assert refused.value.kind == "unavailable"
    assert API.startswith("https://api.hyperliquid.xyz/")


@pytest.fixture
def token_file(tmp_path):
    path = tmp_path / "read-gateway.token"
    path.write_text(secrets.token_hex(32))
    path.chmod(0o600)
    return path


@pytest.fixture
def gateway(tmp_path, token_file):
    gateway = ObservationGateway(tmp_path / "operator.env")
    replies = {"answer": answer("userRole.leader")}
    gateway.hypercore = direct(lambda _: httpx.Response(200, json=replies["answer"]))
    server = http_server(
        gateway, token_file, 0, request_type=GatewayRequest, fields={"read", "lookup", "info"}
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", gateway, replies
    finally:
        server.shutdown()
        server.server_close()
        gateway.close()


def test_queries_flow_through_the_read_gateway(gateway, token_file):
    url, _, _ = gateway
    request = InfoRequest(info=UserRole(user=USER))
    with GatewayInfo(url, token_file.read_text()) as info:
        result = info.fetch(request)
    assert (result.request, result.result) == (request, {"role": "user"})


def test_refusals_keep_their_category_through_the_gateway(gateway, token_file, monkeypatch):
    url, served, replies = gateway
    request = InfoRequest(info=UserRole(user=USER))
    with GatewayInfo(url, "wrong") as unauthenticated:
        with pytest.raises(DataError) as refused:
            unauthenticated.fetch(request)
        assert refused.value.kind == "unauthorized"
    with GatewayInfo(url, token_file.read_text()) as info:
        # The gateway refuses a drifted answer itself.
        replies["answer"] = {"role": "owner"}
        with pytest.raises(DataError) as drifted_answer:
            info.fetch(request)
        assert drifted_answer.value.kind == "malformed"
        # A cap of zero refuses every query before the API sees it.
        monkeypatch.setenv(VARIABLES["hyperliquid"][0], "0")
        served.hypercore = direct(lambda _: pytest.fail("the API must not be reached"))
        with pytest.raises(DataError) as capped:
            info.fetch(request)
        assert capped.value.kind == "rate_limit"


def test_the_client_checks_the_gateway_answer_again():
    request = InfoRequest(info=UserRole(user=USER))
    relayed = hypercore_info.InfoResult(
        request=request, received_at=1, result={"role": "user"}
    ).model_dump(mode="json")
    assert hypercore_info.answered(request, relayed).result == {"role": "user"}
    other = InfoRequest(info=UserRole(user=SUBJECTS["fresh"]))
    with pytest.raises(DataError) as mismatched:
        hypercore_info.answered(other, relayed)
    assert mismatched.value.kind == "source_mismatch"
    with pytest.raises(DataError) as reshaped:
        hypercore_info.answered(request, {**relayed, "result": {"role": "owner"}})
    assert reshaped.value.kind == "malformed"
    with pytest.raises(DataError) as invalid:
        hypercore_info.answered(request, {"request": "nonsense"})
    assert invalid.value.kind == "malformed"


def test_an_unreachable_or_garbled_gateway_is_named(tmp_path, token_file):
    request = InfoRequest(info=UserRole(user=USER))
    with GatewayInfo("http://127.0.0.1:9", token_file.read_text()) as info:
        with pytest.raises(DataError) as unreachable:
            info.fetch(request)
        assert unreachable.value.kind == "gateway_unavailable"
        info.client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=["not", "an", "object"])
            )
        )
        with pytest.raises(DataError) as garbled:
            info.fetch(request)
        assert garbled.value.kind == "malformed"


def test_the_gateway_opens_one_api_client_on_first_use(tmp_path):
    gateway = ObservationGateway(tmp_path / "operator.env")
    assert gateway.hypercore is None
    first = gateway.info()
    assert isinstance(first, HyperCoreInfo) and gateway.info() is first
    with first as entered:
        assert entered is first
    gateway.close()
