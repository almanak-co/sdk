"""The pin facts tool over a fake node: what it reads and what it concludes."""

import json

import pytest

from almanak.caip import ChainId
from almanak.connectors.across.domain import SPOKE_POOLS
from scripts import across_pins as tool

ARBITRUM = 42161
PIN = SPOKE_POOLS[ChainId("eip155:42161")]
WETH = 0x82AF49447D8A07E3BD95BD0D56F35241523FBAB1
INSTALLED_AT = 700
HEAD = 1000


class FakeNode(tool.Node):
    """A chain of `HEAD` one-second blocks whose proxy was upgraded at `INSTALLED_AT`."""

    def __init__(self, code: str, chain: int = ARBITRUM):
        self.code, self.chain, self.asked = code, chain, []

    def rpc(self, method, params):
        self.asked.append(method)
        return getattr(self, method)(*params)

    def eth_blockNumber(self):
        return hex(HEAD)

    def eth_getStorageAt(self, proxy, slot, block):
        assert (proxy, slot) == (PIN.address, tool.IMPLEMENTATION_SLOT)
        number = HEAD if block == "latest" else int(block, 16)
        address = PIN.implementation if number >= INSTALLED_AT else "0x" + "01" * 20
        return "0x" + address[2:].rjust(64, "0")

    def eth_getCode(self, address, block):
        assert address == PIN.implementation
        return self.code

    def eth_getBlockByNumber(self, block, full):
        number = HEAD if block == "latest" else int(block, 16)
        return {"number": hex(number), "timestamp": hex(1_700_000_000 + number)}

    def eth_call(self, call, block):
        views = {
            "depositQuoteTimeBuffer()": 3600,
            "fillDeadlineBuffer()": 21600,
            "chainId()": self.chain,
            "pausedDeposits()": 0,
            "wrappedNativeToken()": WETH,
            "fillStatuses(bytes32)": 0,
            "getRelayerRefund(address,address)": 0,
        }
        by_selector = {tool.selector(name): value for name, value in views.items()}
        return hex(by_selector[call["data"][2:10]])


def whole_code() -> str:
    return "0x60" + "".join(tool.required_code().values())


def test_the_required_code_names_every_selector_event_and_view_the_connector_uses():
    required = tool.required_code()
    assert {"FundsDeposited", "FilledRelay", "ExecutedRelayerRefundRoot"} <= set(required)
    assert {"fillStatuses(bytes32)", "getRelayerRefund(address,address)", "chainId()"} <= set(
        required
    )
    assert required[tool.DEPOSIT_SIGNATURE] == "ad5425c6"
    assert tool.missing_code(whole_code().upper()) == []
    without = whole_code().replace(required["FilledRelay"], "")
    assert tool.missing_code(without) == ["FilledRelay"]


def test_the_facts_of_a_pinned_pool_are_its_pin_its_checks_and_its_installation():
    node = FakeNode(whole_code())
    found = tool.facts(node, ARBITRUM, PIN.address)
    assert (found["chain"], found["implementation"]) == ("eip155:42161", PIN.implementation)
    assert found["missing"] == [] and found["chain_id_matches"] is True
    assert found["wrapped_native_matches"] is True
    assert found["installed"]["block"] == INSTALLED_AT
    assert found["installed"]["at"].startswith("2023-11-14T22:25:00")
    assert found["block_seconds"] == 1.0
    assert json.loads(json.dumps(found)) == found
    # Read-only: nothing but reads was asked.
    assert set(node.asked) <= {
        "eth_blockNumber",
        "eth_getStorageAt",
        "eth_getCode",
        "eth_getBlockByNumber",
        "eth_call",
    }


def test_a_pool_of_another_chain_number_or_other_code_is_reported_not_hidden():
    other = tool.facts(FakeNode("0x60", chain=1), ARBITRUM, PIN.address)
    assert other["chain_id_matches"] is False
    assert set(other["missing"]) == set(tool.required_code())


def test_a_failed_read_reports_the_kind_of_error_and_never_the_endpoint(monkeypatch, tmp_path):
    secret = "https://node.invalid/v2/secret-key"

    def refuse(self, method, params):
        raise RuntimeError(f"refused by {secret}")

    monkeypatch.setattr(tool, "provider_endpoint", lambda provider, env: secret)
    monkeypatch.setattr(tool.Node, "rpc", refuse)
    row = tool.chain_facts(ARBITRUM, PIN.address, tmp_path / ".env")
    assert row == {"chain": "eip155:42161", "address": PIN.address, "error": "RuntimeError"}
    assert "secret" not in json.dumps(row)


class Answer:
    def __init__(self, status, body):
        self.status_code, self.body = status, body

    def json(self):
        return self.body


def test_candidates_are_the_pools_the_venue_names_for_sdk_networks_only(monkeypatch):
    rows = [
        {
            "originChainId": 324,
            "destinationChainId": 1,
            "originToken": "a",
            "destinationToken": "b",
        },
        {"originChainId": 56, "destinationChainId": 1, "originToken": "c", "destinationToken": "d"},
        {
            "originChainId": 56,
            "destinationChainId": 10,
            "originToken": "c",
            "destinationToken": "e",
        },
    ]
    asked = []

    def get(url, params=None, timeout=None):
        if url.endswith("/available-routes"):
            return Answer(200, rows)
        asked.append(params)
        # The venue refuses the 6-decimal amount for an 18-decimal token.
        if params["amount"] == tool.AMOUNTS[0]:
            return Answer(400, {})
        return Answer(200, {"spokePoolAddress": "0xABCD"})

    monkeypatch.setattr(tool.httpx, "get", get)
    assert tool.candidates() == {56: "0xabcd"}
    assert [query["originChainId"] for query in asked] == [56, 56]
    assert all(query["allowUnmatchedDecimals"] == "true" for query in asked)


def test_a_route_the_venue_refuses_at_every_amount_names_no_candidate(monkeypatch):
    monkeypatch.setattr(tool.httpx, "get", lambda url, params=None, timeout=None: Answer(400, {}))
    row = {
        "originChainId": 1,
        "destinationChainId": 10,
        "originToken": "a",
        "destinationToken": "b",
    }
    assert tool.quoted_pool(row) is None


def test_the_command_prints_one_row_per_candidate(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(tool, "candidates", lambda: {10: "0xb", 1: "0xa"})
    monkeypatch.setattr(
        tool, "chain_facts", lambda chain, pool, env: {"chain": chain, "pool": pool}
    )
    assert tool.main(["--env-file", str(tmp_path / ".env")]) == 0
    assert json.loads(capsys.readouterr().out) == [
        {"chain": 1, "pool": "0xa"},
        {"chain": 10, "pool": "0xb"},
    ]
    with pytest.raises(SystemExit):
        tool.main([])
