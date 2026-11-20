"""The public endpoint qualification tool, with every endpoint replaced by a fake.

Offline: nothing here opens a connection. The tool itself is run by hand against the
live endpoints.
"""

import json
import re
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.gateway import providers
from almanak.gateway.ethereum import DataError, ProviderBusy
from almanak.models import Block, Chain
from scripts import qualify_public_rpc as tool

BASE = ChainId("eip155:8453")
ETHEREUM = ChainId("eip155:1")
HEAD = 1_000
NOW = 50_000
KNOWN = "0x" + "ab" * 32


def block(number: int, chain: ChainId = ETHEREUM) -> Block:
    return Block(
        chain=Chain(chain_id=chain, environment="mainnet"),
        number=number,
        hash="0x" + number.to_bytes(32).hex(),
        timestamp=NOW - (HEAD - number),
    )


def receipt(number: int = HEAD) -> dict:
    fields = dict.fromkeys(tool.RECEIPT_FIELDS, "0x1")
    return {**fields, "blockNumber": hex(number), "blockHash": block(number).hash}


class Endpoint:
    """A provider that serves every read at the block the read names."""

    def __init__(self, chain: ChainId = ETHEREUM, **answers):
        self.chain = Chain(chain_id=chain, environment="mainnet")
        self.answers = answers
        self.calls: list[tuple[str, list]] = []
        self.pinned: list = []

    def verify_chain(self):
        self.calls.append(("verify_chain", []))

    @contextmanager
    def retained_pin(self):
        yield

    def pin_block(self, selector):
        self.pinned.append(selector)
        number = {"latest": HEAD, "finalized": HEAD - 100}.get(selector, selector)
        return block(number, self.chain.chain_id)

    def asset(self, token, pinned):
        return SimpleNamespace(decimals=self.answers.get("decimals", 18))

    def _rpc(self, method, params, *, allow_none=False):
        self.calls.append((method, params))
        answer = self.answers.get(method, getattr(self, method, None))
        if isinstance(answer, Exception):
            raise answer
        return answer(params) if callable(answer) else answer

    def eth_call(self, params):
        # The context probe: the timestamp of the block the pin names, then two more words.
        number = int(params[1]["blockHash"], 16)
        return "0x" + (NOW - (HEAD - number)).to_bytes(32).hex() + "00" * 64

    def eth_getBlockByNumber(self, params):
        return {"transactions": ["0x01", KNOWN, "0x03"]}

    def eth_getTransactionReceipt(self, params):
        return receipt() if params == [KNOWN] else None

    def eth_getTransactionByHash(self, params):
        return {"hash": KNOWN} if params == [KNOWN] else None

    eth_getCode = "0x6080"
    eth_getStorageAt = "0x" + "00" * 32
    eth_getBalance = "0x5"
    eth_blockNumber = hex(HEAD)
    eth_getTransactionCount = "0x0"


def probe(reader: Endpoint, endpoint: str = "https://public.example") -> tool.Probe:
    return tool.Probe(evm_network(reader.chain.chain_id), endpoint, reader, reader)  # type: ignore[arg-type]


def test_identity_is_the_proof_a_public_read_gives(monkeypatch):
    seen = []
    monkeypatch.setattr(tool, "verify_public", seen.append)
    reader = Endpoint()
    assert tool.identity(probe(reader)) == {}
    assert reader.calls == [("verify_chain", [])] and seen == [reader]


def test_the_production_guard_refuses_what_the_adapter_refuses():
    served = Endpoint(eth_chainId="0x1", web3_clientVersion="Geth/v1.16.0")
    served.pin_block = lambda selector: SimpleNamespace(hash=evm_network(ETHEREUM).genesis)
    assert tool.production_guard(probe(served)) == {}
    with pytest.raises(ValueError, match="explicit HTTPS RPC endpoint"):
        tool.production_guard(probe(served, "http://public.example"))
    test_node = Endpoint(eth_chainId="0x1", web3_clientVersion="anvil/v1.0.0")
    with pytest.raises(DataError, match="identifies a test node"):
        tool.production_guard(probe(test_node))
    with pytest.raises(DataError, match="different genesis"):
        tool.production_guard(probe(Endpoint(eth_chainId="0x1", web3_clientVersion="Geth")))


@pytest.mark.parametrize(
    ("check", "selector", "number"),
    [
        (tool.reads_at_finalized, "finalized", HEAD - 100),
        (tool.reads_at_latest, "latest", HEAD),
        (tool.reads_at_number, HEAD - tool.NUMBER_DEPTH, HEAD - tool.NUMBER_DEPTH),
    ],
)
def test_pinned_reads_name_the_block_hash_of_each_selector(check, selector, number):
    reader = Endpoint()
    assert check(probe(reader)) == {"block": number, "burn_address_balance": 5}
    assert reader.pinned[-1] == selector
    pin = {"blockHash": block(number).hash, "requireCanonical": True}
    pinned = [params[-1] for method, params in reader.calls if method != "verify_chain"]
    assert pinned == [pin] * 4


def test_a_read_the_provider_runs_at_another_block_is_refused():
    elsewhere = Endpoint(eth_call="0x" + NOW.to_bytes(32).hex() + "00" * 64)
    with pytest.raises(DataError, match=r"pinned to block 900 \(timestamp 49900\) ran at") as ran:
        tool.reads_at_finalized(probe(elsewhere))
    assert ran.value.kind == "inconsistent"
    with pytest.raises(DataError, match="invalid block context probe result") as short:
        tool.reads_at_latest(probe(Endpoint(eth_call="0x")))
    assert short.value.kind == "malformed"


@pytest.mark.parametrize(
    ("answers", "message"),
    [
        ({"decimals": 6}, "does not report 18 decimals"),
        ({"eth_getCode": "0x"}, "code or storage is not served"),
        ({"eth_getStorageAt": "0x0"}, "code or storage is not served"),
    ],
)
def test_pinned_reads_refuse_an_endpoint_without_the_state(answers, message):
    with pytest.raises(DataError, match=message):
        tool.reads_at_latest(probe(Endpoint(**answers)))


def test_submitter_state_needs_a_fresh_head_and_one_nonce():
    reader = Endpoint()
    assert tool.submitter_state(probe(reader), clock=lambda: NOW + 5) == {
        "block": HEAD,
        "nonce": 0,
    }
    assert reader.pinned == [HEAD]
    tags = [params[1] for method, params in reader.calls if method == "eth_getTransactionCount"]
    assert tags == ["latest", "pending"]
    with pytest.raises(DataError, match="head by number is 500 seconds old"):
        tool.submitter_state(probe(Endpoint()), clock=lambda: NOW + 500)
    counts = iter(["0x0", "0x1"])
    pending = Endpoint(eth_getTransactionCount=lambda params: next(counts))
    with pytest.raises(DataError, match="nonces of an idle account differ"):
        tool.submitter_state(probe(pending), clock=lambda: NOW)


def test_the_quote_uses_the_policy_of_a_direct_action(monkeypatch):
    seen = {}

    def quoted(reader, policy, pinned, call):
        seen.update(policy=policy, block=pinned, call=call)
        quote = SimpleNamespace(node_gas_price=None)
        return {"gas_estimate": 21_000, "gas": 26_250, "gas_price": 7, "fee_quote": quote}

    monkeypatch.setattr(tool, "quoted_fees", quoted)
    assert tool.fee_quote(probe(Endpoint())) == {
        "block": HEAD,
        "gas_estimate": 21_000,
        "gas_price_wei": 7,
        "node_gas_price_wei": None,
    }
    policy = seen["policy"]
    assert policy.asset == evm_network(ETHEREUM).native_asset
    limits = ax_actions.FeeLimits(max_gas_price_wei=1, max_transaction_fee=1)
    # The quote runs under the policy a direct action on the chain would state.
    stated = ax_actions.gas_policy("probe", ETHEREUM, limits)
    assert policy == type(policy).model_validate_json(json.dumps(stated))
    assert policy.max_gas_limit == stated["max_gas_limit"]
    wrapped = evm_network(ETHEREUM).wrapped_native.reference
    assert seen["call"] == {
        "from": tool.ACCOUNT,
        "to": wrapped,
        "value": "0x0",
        "data": "0x313ce567",
    }


def test_op_fee_parameters_are_read_only_on_an_op_stack_chain(monkeypatch):
    read = []

    def parameters(reader, pinned):
        read.append(pinned)
        return SimpleNamespace(revision="jovian")

    monkeypatch.setattr(tool, "read_parameters", parameters)
    assert tool.op_fees(probe(Endpoint())) == {"applies": False}
    assert read == []
    assert tool.op_fees(probe(Endpoint(BASE))) == {
        "applies": True,
        "block": HEAD,
        "revision": "jovian",
    }
    assert read == [block(HEAD, BASE)]


def test_the_receipt_read_takes_the_middle_transaction_of_the_newest_block_with_one():
    blocks = {HEAD: [], HEAD - 1: [], HEAD - 2: ["0x01", KNOWN, "0x03"]}
    reader = Endpoint(
        eth_getBlockByNumber=lambda params: {"transactions": blocks[int(params[0], 16)]}
    )
    assert tool.recent_transaction(reader, HEAD) == KNOWN  # type: ignore[arg-type]
    empty = Endpoint(eth_getBlockByNumber={"transactions": []})
    with pytest.raises(DataError, match="no transaction in the last 16 blocks") as none:
        tool.recent_transaction(empty, HEAD)  # type: ignore[arg-type]
    assert none.value.kind == "unavailable"
    assert len(empty.calls) == tool.RECEIPT_BLOCKS
    # Near genesis the walk stops at block 0.
    young = Endpoint(eth_getBlockByNumber={"transactions": []})
    with pytest.raises(DataError):
        tool.recent_transaction(young, 2)  # type: ignore[arg-type]
    assert [params[0] for _, params in young.calls] == ["0x2", "0x1", "0x0"]


@pytest.mark.parametrize(
    ("check", "answers", "message"),
    [
        (tool.reads_at_latest, {"eth_getBalance": 5}, "eth_getBalance: invalid quantity"),
        (tool.reads_at_latest, {"eth_getBalance": "0xzz"}, "eth_getBalance: invalid quantity"),
        (tool.reads_at_latest, {"eth_getBalance": "5"}, "eth_getBalance: invalid quantity"),
        (tool.reads_at_finalized, {"eth_getCode": 7}, "eth_getCode: invalid data"),
        (tool.reads_at_number, {"eth_getStorageAt": ["0x0"]}, "eth_getStorageAt: invalid data"),
        (tool.submitter_state, {"eth_blockNumber": 1000}, "eth_blockNumber: invalid quantity"),
        (
            partial(tool.submitter_state, clock=lambda: NOW),
            {"eth_getTransactionCount": {"nonce": "0x0"}},
            "eth_getTransactionCount: invalid quantity",
        ),
        (
            tool.receipts,
            {"eth_getBlockByNumber": {"transactions": "0xabcd"}},
            "eth_getBlockByNumber: invalid transaction list",
        ),
        (
            tool.receipts,
            {"eth_getBlockByNumber": {"transactions": [{"hash": KNOWN}]}},
            "eth_getBlockByNumber: invalid transaction list",
        ),
        (
            tool.receipts,
            {"eth_getBlockByNumber": []},
            "eth_getBlockByNumber: invalid transaction list",
        ),
        (
            tool.receipts,
            {"eth_getTransactionReceipt": {**receipt(), "blockNumber": HEAD}},
            "eth_getTransactionReceipt: invalid quantity",
        ),
    ],
)
def test_an_answer_of_the_wrong_shape_fails_the_check_as_malformed(check, answers, message):
    """A check ends with its report whatever the endpoint answers."""
    result = tool.outcome("check", check, probe(Endpoint(**answers)))
    assert result == {
        "check": "check",
        "passed": False,
        "error": "malformed",
        "message": message,
        "rate_limited": False,
        "transient": False,
    }


@pytest.mark.parametrize(
    ("check", "patched", "raised", "message"),
    [
        (tool.fee_quote, "quoted_fees", TypeError("int() can't convert"), "quoted_fees"),
        (tool.fee_quote, "quoted_fees", ValueError("invalid literal 0xzz"), "quoted_fees"),
        (tool.op_fees, "read_parameters", KeyError("result"), "read_parameters"),
        (tool.op_fees, "read_parameters", IndexError("out of range"), "read_parameters"),
    ],
)
def test_an_answer_the_sdk_cannot_decode_fails_the_check_without_its_text(
    monkeypatch, check, patched, raised, message
):
    def read(*arguments):
        raise raised

    read.__name__ = patched
    monkeypatch.setattr(tool, patched, read)
    result = tool.outcome("check", check, probe(Endpoint(BASE)))
    kind = type(raised).__name__
    assert (result["passed"], result["error"]) == (False, "malformed")
    assert result["message"] == f"{message}: undecodable answer ({kind})"


def test_the_production_guard_fails_as_malformed_on_an_undecodable_chain_number():
    result = tool.outcome("guard", tool.production_guard, probe(Endpoint(eth_chainId=1)))
    assert (result["error"], result["message"]) == (
        "malformed",
        "verify_provider: undecodable answer (TypeError)",
    )


def test_a_refusal_the_sdk_raises_while_decoding_stays_its_own():
    def refused(*arguments):
        raise DataError("gas_price", "market gas quote exceeds the configured price ceiling")

    with pytest.raises(DataError, match="price ceiling") as kept:
        tool.decoded(refused)
    assert kept.value.kind == "gas_price"
    # Anything that is not a decoding error is a fault of the tool and is not hidden.
    with pytest.raises(RuntimeError):
        tool.decoded(lambda: (_ for _ in ()).throw(RuntimeError("bug")))


def test_receipts_pass_on_a_canonical_receipt_and_null_for_an_unknown_hash():
    reader = Endpoint()
    assert tool.receipts(probe(reader)) == {"block": HEAD}
    unknown = [params for _, params in reader.calls if params == [tool.UNKNOWN_TRANSACTION]]
    assert len(unknown) == 2


@pytest.mark.parametrize(
    ("answers", "message"),
    [
        ({"eth_getTransactionReceipt": None}, "a mined transaction has no receipt"),
        (
            {"eth_getTransactionReceipt": {"blockNumber": hex(HEAD)}},
            "receipt lacks blockHash, effectiveGasPrice",
        ),
        (
            {"eth_getTransactionReceipt": {**receipt(), "blockHash": "0x" + "00" * 32}},
            "receipt block is not canonical by number",
        ),
        ({"eth_getTransactionByHash": None}, "not found by its hash"),
        (
            {"eth_getTransactionByHash": {"hash": KNOWN}},
            "eth_getTransactionByHash: an unknown transaction hash was answered",
        ),
        (
            {"eth_getTransactionReceipt": receipt()},
            "eth_getTransactionReceipt: an unknown transaction hash was answered",
        ),
    ],
)
def test_receipts_refuse_an_endpoint_that_answers_wrongly(answers, message):
    with pytest.raises(DataError, match=message):
        tool.receipts(probe(Endpoint(**answers)))


def test_an_outcome_keeps_the_sdk_refusal_and_marks_a_rate_limit():
    reader = probe(Endpoint())
    assert tool.outcome("served", lambda probe: {"block": 1}, reader) == {
        "check": "served",
        "passed": True,
        "block": 1,
    }

    def refused(probe):
        raise DataError("http", "eth_call: HTTP 403", code=-32602)

    def busy(probe):
        raise ProviderBusy("eth_call", None)

    def invalid(probe):
        raise ValueError("production execution requires an explicit HTTPS RPC endpoint")

    assert tool.outcome("refused", refused, reader) == {
        "check": "refused",
        "passed": False,
        "error": "http",
        "message": "eth_call: HTTP 403",
        "rate_limited": False,
        "transient": False,
    }
    limited = tool.outcome("busy", busy, reader)
    assert (limited["rate_limited"], limited["transient"]) == (True, False)

    def exhausted(probe):
        # What the reader raises once its re-sends of a transient failure ran out.
        raise DataError("transport", "eth_call: transport failed", transient=True)

    marked = tool.outcome("exhausted", exhausted, reader)
    assert (marked["passed"], marked["rate_limited"], marked["transient"]) == (False, False, True)
    # A transient failure is reported, not retried: only rate limits earn another attempt.
    assert not tool.rate_limited({"checks": [marked]})
    assert tool.outcome("invalid", invalid, reader) == {
        "check": "invalid",
        "passed": False,
        "error": "invalid",
        "message": "production execution requires an explicit HTTPS RPC endpoint",
    }


def test_no_check_runs_on_an_endpoint_of_unproven_identity():
    ran = []

    def failing(probe):
        raise DataError("chain_mismatch", "public endpoint has a different genesis")

    def later(probe):
        ran.append(True)
        return {}

    reader = probe(Endpoint())
    stopped = tool.run_checks(reader, (("identity", failing), ("later", later)))
    assert [check["check"] for check in stopped] == ["identity"] and ran == []
    # Any other failure leaves the remaining checks running.
    results = tool.run_checks(reader, (("first", failing), ("later", later)))
    assert [check["passed"] for check in results] == [False, True] and ran == [True]


def test_the_qa_guide_describes_exactly_the_checks_the_tool_runs():
    guide = (Path(__file__).parents[1] / "docs/qa.md").read_text()
    section = guide.split("## Public endpoint qualification")[1].split("\n## ")[0]
    rows = re.findall(r"^\| (`[a-z_]+`(?:, `[a-z_]+`)*) \| .+ \|$", section, flags=re.MULTILINE)
    described = [label for row in rows for label in re.findall(r"`([a-z_]+)`", row)]
    assert described == [label for label, _ in tool.CHECKS]
    for field in ("`rate_limited`", "`transient`", "`malformed`"):
        assert field in section


def test_the_checks_start_with_identity():
    labels = [label for label, _ in tool.CHECKS]
    assert labels[0] == "identity" and len(set(labels)) == len(labels) == 9


def report(*checks: dict) -> dict:
    return {"passed": all(check["passed"] for check in checks), "checks": list(checks)}


LIMITED = {"passed": False, "rate_limited": True}
REFUSED = {"passed": False, "rate_limited": False}
SERVED = {"passed": True}


def test_only_rate_limits_alone_earn_another_attempt():
    assert tool.rate_limited(report(SERVED, LIMITED))
    assert not tool.rate_limited(report(LIMITED, REFUSED))
    assert not tool.rate_limited(report(SERVED))
    assert not tool.rate_limited(report({"passed": False}))


def settle(reports: list[dict], attempts: int = 3):
    waits: list[float] = []
    remaining = iter(reports)
    result = tool.settled(
        ("drpc", ETHEREUM, "https://public.example"),
        attempts,
        30.0,
        qualify=lambda target: next(remaining),
        wait=waits.append,
    )
    return result, waits


def test_a_rate_limited_endpoint_is_checked_again_after_a_pause():
    result, waits = settle([report(LIMITED), report(SERVED)])
    assert result["passed"] and result["attempts"] == 2 and waits == [30.0]
    result, waits = settle([report(LIMITED)] * 3)
    assert not result["passed"] and result["attempts"] == 3 and waits == [30.0, 30.0]
    result, waits = settle([report(REFUSED), report(SERVED)])
    assert not result["passed"] and result["attempts"] == 1 and waits == []


def test_qualifying_opens_both_readers_on_the_endpoint_and_names_only_its_host(monkeypatch):
    opened = []

    class Opened(Endpoint):
        def __init__(self, chain, endpoint, **options):
            super().__init__(chain if isinstance(chain, ChainId) else chain.chain_id)
            opened.append((endpoint, options))

        def __enter__(self):
            return self

        def __exit__(self, *arguments):
            opened.append("closed")

    monkeypatch.setattr(tool, "mainnet_reader", Opened)
    monkeypatch.setattr(tool, "EthereumReader", Opened)
    monkeypatch.setattr(tool, "run_checks", lambda probe: [{"check": "identity", "passed": True}])
    listed = providers.PUBLIC_RPC["publicnode"][ETHEREUM]
    result = tool.qualify(("publicnode", ETHEREUM, listed))
    assert result == {
        "name": "publicnode",
        "chain": "eip155:1",
        "host": "ethereum-rpc.publicnode.com",
        "listed": True,
        "passed": True,
        "checks": [{"check": "identity", "passed": True}],
    }
    assert opened == [
        (listed, {"observation": True}),
        (listed, {"retry_budget": tool.EXECUTION_RETRY_BUDGET}),
        "closed",
        "closed",
    ]
    unlisted = tool.qualify(("other", ETHEREUM, "https://user:secret@other.example/path?key=1"))
    assert unlisted["host"] == "other.example" and not unlisted["listed"]
    assert "secret" not in json.dumps(unlisted) and "key=1" not in json.dumps(unlisted)


def test_the_default_targets_are_every_listed_pair():
    listed = [
        (name, chain, url)
        for name, chains in providers.PUBLIC_RPC.items()
        for chain, url in chains.items()
    ]
    assert tool.arguments([]).targets == listed
    named = tool.arguments(["--name", "drpc", "--chain", "eip155:1"]).targets
    assert named == [("drpc", ETHEREUM, providers.PUBLIC_RPC["drpc"][ETHEREUM])]


def test_a_candidate_is_an_https_endpoint_on_a_known_chain_under_a_valid_name():
    args = tool.arguments(["--candidate", "example", "eip155:8453", "https://rpc.example"])
    assert args.targets == [("example", BASE, "https://rpc.example")]
    for values, message in (
        (["Example", "eip155:8453", "https://rpc.example"], "not a lowercase endpoint name"),
        (["example", "eip155:8453", "http://rpc.example"], "must be an HTTPS URL"),
        (["example", "eip155:8453", "https://user:key@rpc.example"], "must be an HTTPS URL"),
        (["example", "eip155:424242", "https://rpc.example"], "not a CAIP-2 chain with a pinned"),
        (["example", "base", "https://rpc.example"], "not a CAIP-2 chain with a pinned"),
    ):
        with pytest.raises(ValueError, match=message) as refused:
            tool.candidate(values)
        # The refusal never repeats the URL, which may carry a credential.
        assert "rpc.example" not in str(refused.value)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--candidate", "Example", "eip155:8453", "https://rpc.example"], "endpoint name"),
        (["--candidate", "example", "base", "https://rpc.example"], "not a CAIP-2 chain"),
        (["--candidate", "example", "eip155:8453", "http://rpc.example"], "must be an HTTPS URL"),
        (
            ["--name", "drpc", "--candidate", "example", "eip155:8453", "https://rpc.example"],
            "--name and --chain select listed endpoints, not a --candidate",
        ),
        (
            ["--chain", "eip155:1", "--candidate", "example", "eip155:8453", "https://rpc.example"],
            "--name and --chain select listed endpoints, not a --candidate",
        ),
        (["--name", "unlisted"], "no listed public endpoint matches --name and --chain"),
        (["--name", "publicnode", "--chain", "eip155:999"], "no listed public endpoint matches"),
        (["--attempts", "0"], "expected a whole number of at least 1"),
        (["--attempts", "-1"], "expected a whole number of at least 1"),
        (["--attempts", "two"], "expected a whole number of at least 1"),
        (["--pause", "-1"], "expected seconds from 0 to 3600"),
        (["--retry-pause", "soon"], "expected seconds from 0 to 3600"),
        (["--retry-pause", "nan"], "expected seconds from 0 to 3600"),
    ],
)
def test_a_wrong_command_line_is_a_usage_error_before_any_endpoint_is_opened(
    monkeypatch, capsys, arguments, message
):
    monkeypatch.setattr(tool, "qualify", lambda target: pytest.fail("opened an endpoint"))
    with pytest.raises(SystemExit) as stopped:
        tool.main(arguments)
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    assert message in captured.err and captured.out == ""
    assert "Traceback" not in captured.err


def test_attempts_and_pauses_parse_as_whole_attempts_and_seconds():
    args = tool.arguments(["--attempts", "1", "--pause", "0", "--retry-pause", "0.5"])
    assert (args.attempts, args.pause, args.retry_pause) == (1, 0.0, 0.5)


def test_the_report_is_one_json_document_and_a_failure_exits_1(monkeypatch, capsys):
    outcomes = iter([True, False])
    waits: list[float] = []

    def settled(target, attempts, pause, *, wait):
        assert (attempts, pause) == (2, 7.0)
        return {"name": target[0], "chain": str(target[1]), "passed": next(outcomes)}

    monkeypatch.setattr(tool, "settled", settled)
    listed = {"one": {ETHEREUM: "https://one.example", BASE: "https://base.one.example"}}
    monkeypatch.setattr(tool, "PUBLIC_RPC", listed)
    arguments = ["--name", "one", "--attempts", "2", "--retry-pause", "7", "--pause", "1"]
    assert tool.main(arguments, wait=waits.append) == 1
    document = json.loads(capsys.readouterr().out)
    assert document["schema"] == tool.SCHEMA and document["passed"] is False
    assert [entry["passed"] for entry in document["endpoints"]] == [True, False]
    # One pause between endpoints, none before the first.
    assert waits == [1.0]


def test_a_passing_run_exits_0(monkeypatch, capsys):
    monkeypatch.setattr(tool, "settled", lambda target, attempts, pause, *, wait: {"passed": True})
    assert tool.main(["--name", "drpc", "--chain", "eip155:1"], wait=lambda seconds: None) == 0
    assert json.loads(capsys.readouterr().out)["passed"] is True
    assert capsys.readouterr().err == ""
