"""A node's refusal of a transaction submission is named by a fixed phrase, never echoed.

Submissions share the read transport, so a refused broadcast used to read as an
unexplained "RPC rejected the requested read (JSON-RPC -32000)".
"""

import json

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError, EthereumReader, member_result
from almanak.models import Chain

SEND = "eth_sendRawTransaction"
SECRET = " via https://rpc.invalid/v2/secret-key"


def refused(message, code=-32000):
    return {"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": message}}


@pytest.mark.parametrize(
    ("message", "phrase"),
    [
        # go-ethereum core and txpool wording, shared by erigon, reth and anvil.
        ("nonce too low: address 0xabc, tx: 4 state: 7", "nonce too low"),
        ("Nonce too high", "nonce too high"),
        ("already known", "already known"),
        ("known transaction: 0xabc", "already known"),
        ("transaction already imported", "already known"),
        ("replacement transaction underpriced", "replacement transaction underpriced"),
        ("transaction underpriced: tip needed 1, tip permitted 0", "transaction underpriced"),
        ("intrinsic gas too low: gas 0, minimum needed 21000", "intrinsic gas too low"),
        (
            "insufficient funds for gas * price + value: address 0xabc have 0 want 1",
            "insufficient funds",
        ),
        (
            "max fee per gas less than block base fee: address 0xabc, maxFeePerGas: 1, baseFee: 2",
            "max fee per gas less than block base fee",
        ),
        ("fee cap less than block base fee", "max fee per gas less than block base fee"),
        (
            "max priority fee per gas higher than max fee per gas",
            "max priority fee per gas higher than max fee per gas",
        ),
        ("exceeds block gas limit", "exceeds block gas limit"),
        ("gas limit reached", "gas limit reached"),
        (
            "tx fee (1.50 ether) exceeds the configured cap (1.00 ether)",
            "tx fee exceeds the configured cap",
        ),
        ("txpool is full", "txpool is full"),
        ("transaction pool is full", "txpool is full"),
        ("nonce has max value", "nonce has max value"),
        ("max fee per gas higher than 2^256-1", "fee per gas higher than 2^256-1"),
        (
            "transaction priority fee below minimum required priority fee",
            "transaction underpriced",
        ),
        ("chain id mismatch", "invalid chain id"),
        (
            "Incompatible EIP-155 transaction, signed for another chain",
            "invalid chain id",
        ),
        ("invalid sender", "invalid sender"),
        ("invalid chain id for signer", "invalid chain id"),
        (
            "only replay-protected (EIP-155) transactions allowed over RPC",
            "only replay-protected transactions allowed",
        ),
        # Erigon reports its own discard reasons.
        ("IntrinsicGas", "intrinsic gas too low"),
        ("sub-pool is full", "txpool is full"),
        ("nonce gap", "nonce too high"),
        ("existing txn with same hash", "already known"),
        # Nethermind reports its transaction-pool result names.
        ("OldNonce, Current nonce: 7, nonce of rejected tx: 4", "nonce too low"),
        ("NonceGap, Future nonce", "nonce too high"),
        ("NonceTooFarInFuture", "nonce too high"),
        ("nonce too far in future", "nonce too high"),
        ("AlreadyKnown", "already known"),
        ("FeeTooLow, MaxFeePerGas too low", "transaction underpriced"),
        ("InsufficientFunds, Account balance: 0", "insufficient funds"),
        ("GasLimitExceeded", "exceeds block gas limit"),
    ],
)
def test_a_refused_submission_names_its_known_reason_only(message, phrase):
    with pytest.raises(DataError) as error:
        member_result(SEND, refused(message + SECRET))
    assert error.value.kind == "rpc" and error.value.code == -32000
    assert str(error.value) == (
        f"{SEND}: RPC refused the transaction submission (JSON-RPC -32000: {phrase})"
    )


@pytest.mark.parametrize(
    ("error", "message"),
    [
        ({"code": -32000, "message": "secret-key"}, "(JSON-RPC -32000)"),
        ({"code": -32000}, "(JSON-RPC -32000)"),
        ({"message": "nonce too low"}, "(nonce too low)"),
        ({"code": "x", "message": None}, ""),
    ],
)
def test_a_submission_refusal_echoes_no_provider_text(error, message):
    with pytest.raises(DataError) as refusal:
        member_result(SEND, {"jsonrpc": "2.0", "id": 1, "error": error})
    expected = f"{SEND}: RPC refused the transaction submission {message}".rstrip()
    assert str(refusal.value) == expected and "secret" not in str(refusal.value)


def test_reads_keep_their_wording_and_never_take_a_submission_reason():
    with pytest.raises(DataError) as error:
        member_result("eth_call", refused("nonce too low"))
    assert str(error.value) == "eth_call: RPC rejected the requested read (JSON-RPC -32000)"


def test_a_refused_broadcast_is_named_once_and_never_re_sent():
    sent = []

    def answer(request):
        body = json.loads(request.content)
        sent.append(body["method"])
        error = {"code": -32000, "message": "replacement transaction underpriced" + SECRET}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "error": error})

    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    client = httpx.Client(transport=httpx.MockTransport(answer))
    reader = EthereumReader(chain, "https://rpc.invalid", client=client, retry_budget=8.0)
    with pytest.raises(DataError) as error:
        reader._rpc(SEND, ["0x02"])
    assert sent == [SEND] and error.value.kind == "rpc" and not error.value.transient
    assert str(error.value) == (
        f"{SEND}: RPC refused the transaction submission "
        "(JSON-RPC -32000: replacement transaction underpriced)"
    )


def reader_answering(answer):
    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    client = httpx.Client(transport=httpx.MockTransport(answer))
    return EthereumReader(chain, "https://rpc.invalid", client=client)


def test_an_http_refused_submission_names_its_known_reason_only():
    def answer(request):
        error = {"code": -32000, "message": "nonce too low" + SECRET}
        return httpx.Response(400, json={"jsonrpc": "2.0", "id": 1, "error": error})

    with pytest.raises(DataError) as error:
        reader_answering(answer)._rpc(SEND, ["0x02"])
    assert error.value.kind == "http" and error.value.code == -32000
    assert str(error.value) == (
        f"{SEND}: RPC refused the transaction submission (HTTP 400, JSON-RPC -32000: nonce too low)"
    )


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        ({"jsonrpc": "2.0", "id": 1, "error": {"message": "secret-key"}}, "(HTTP 400)"),
        ("secret-key", "(HTTP 400)"),
    ],
)
def test_an_http_refused_submission_echoes_no_body(body, detail):
    def answer(request):
        if isinstance(body, str):
            return httpx.Response(400, text=body)
        return httpx.Response(400, json=body)

    with pytest.raises(DataError) as error:
        reader_answering(answer)._rpc(SEND, ["0x02"])
    assert str(error.value) == f"{SEND}: RPC refused the transaction submission {detail}"


def test_an_http_refused_read_keeps_its_wording():
    def answer(request):
        error = {"code": -32000, "message": "nonce too low" + SECRET}
        return httpx.Response(400, json={"jsonrpc": "2.0", "id": 1, "error": error})

    with pytest.raises(DataError) as error:
        reader_answering(answer)._rpc("eth_getCode", ["0x" + "00" * 20, "latest"])
    assert str(error.value) == "eth_getCode: HTTP 400"


@pytest.mark.parametrize("code", [429, -32005])
def test_a_throttled_submission_is_not_called_refused(code):
    with pytest.raises(DataError) as error:
        member_result(SEND, refused("rate limited" + SECRET, code))
    assert str(error.value) == (
        f"{SEND}: RPC did not accept the transaction submission (JSON-RPC {code})"
    )
