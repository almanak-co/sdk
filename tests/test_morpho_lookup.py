"""Morpho market lookups: one closed query shape, sent as variables to a fixed document."""

import json

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway import morpho_lookup
from almanak.gateway.ethereum import DataError
from almanak.gateway.morpho_lookup import QUERY, MorphoApi, MorphoMarkets, document, variables

BASE = ChainId("eip155:8453")
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH = "0x4200000000000000000000000000000000000006"
PAGE = {"pageInfo": {"countTotal": 1}, "items": [{"marketId": "0x" + "11" * 32}]}


def test_a_lookup_travels_as_the_variables_of_the_fixed_query():
    lookup = MorphoMarkets(chain=BASE, loan=USDC, collateral=WETH, first=5, skip=10)
    assert variables(lookup) == {
        "chain": 8453,
        "loan": [USDC],
        "collateral": [WETH],
        "listed": True,
        "first": 5,
        "skip": 10,
    }
    # Null variables leave their filters out: any collateral, listed or not.
    everything = variables(MorphoMarkets(chain=BASE, loan=USDC, listed_only=False))
    assert everything["collateral"] is None and everything["listed"] is None


def test_only_indexed_chains_and_bounded_pages_are_accepted():
    for chain in ("eip155:56", "eip155:43114"):
        with pytest.raises(ValueError, match="does not index"):
            MorphoMarkets(chain=ChainId(chain), loan=USDC)
    for bad in ({"first": 0}, {"first": 101}, {"skip": -1}, {"skip": 1001}, {"loan": "0x12"}):
        with pytest.raises(ValueError):
            MorphoMarkets(**{"chain": BASE, "loan": USDC, **bad})


def respond(status, body=None, text=None):
    request = httpx.Request("POST", morpho_lookup.API)
    if text is not None:
        return httpx.Response(status, text=text, request=request)
    return httpx.Response(status, json=body, request=request)


def test_api_answers_keep_their_category():
    assert document(respond(200, {"data": {"markets": PAGE}})) == PAGE
    cases = [
        (respond(429, {}), "rate_limit"),
        (respond(502, {}), "unavailable"),
        (respond(200, text="not json"), "malformed"),
        (
            respond(200, {"errors": [{"message": "unsupported chainId"}], "data": None}),
            "unavailable",
        ),
        (respond(200, {"data": {"markets": {"items": {}}}}), "malformed"),
        (respond(200, {"data": None}), "malformed"),
        (respond(200, {"data": [1]}), "malformed"),
        (respond(200, {"data": "x"}), "malformed"),
        (respond(200, []), "unavailable"),
    ]
    for response, kind in cases:
        with pytest.raises(DataError) as raised:
            document(response)
        assert raised.value.kind == kind
    # The API's own reason for refusing the fixed query is kept for diagnosis.
    with pytest.raises(DataError, match="refused the market query: unsupported chainId"):
        document(respond(200, {"errors": [{"message": "unsupported chainId"}]}))
    with pytest.raises(DataError, match="refused the market query: no reason given"):
        document(respond(200, {"errors": ["not an object"]}))
    # A document that fails the API's validation is answered with HTTP 400 and its errors.
    field = 'Cannot query field "supplyAssetz" on type "MarketState".'
    with pytest.raises(DataError) as raised:
        document(respond(400, {"errors": [{"message": field}]}))
    assert raised.value.kind == "unavailable"
    assert str(raised.value) == f"morpho_api: HTTP 400, the API refused the market query: {field}"
    for body in ({}, {"errors": []}, None):
        with pytest.raises(DataError, match="^morpho_api: HTTP 400$"):
            document(respond(400, body))
    with pytest.raises(DataError, match="^morpho_api: HTTP 400$"):
        document(respond(400, text="<html>bad request</html>"))


def test_the_client_posts_only_the_fixed_query(monkeypatch):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"data": {"markets": PAGE}})

    api = MorphoApi()
    api.client = httpx.Client(transport=httpx.MockTransport(handler))
    assert api.fetch(MorphoMarkets(chain=BASE, loan=USDC)) == PAGE
    (sent,) = seen
    assert str(sent.url) == morpho_lookup.API
    body = json.loads(sent.content)
    assert body == {"query": QUERY, "variables": variables(MorphoMarkets(chain=BASE, loan=USDC))}

    def refused(request):
        raise httpx.ConnectError("refused", request=request)

    api.client = httpx.Client(transport=httpx.MockTransport(refused))
    with pytest.raises(DataError, match="morpho_api: request failed"):
        api.fetch(MorphoMarkets(chain=BASE, loan=USDC))
    api.close()
