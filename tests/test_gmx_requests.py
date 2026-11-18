"""Accepted requests survive freezing, duplicate evidence and terminal delivery."""

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.gmx_v2.events import EVENT_DATA
from almanak.connectors.gmx_v2.requests import ReceiptIdentity, Request, RequestEvidence
from almanak.models import Account, AssetAmount, Block, Chain

CHAIN = Chain(chain_id=ChainId("eip155:42161"), environment="anvil:gmx-test")
KEY = "0x" + "11" * 32


def evidence(status="accepted", number=10):
    block = Block(chain=CHAIN, number=number, hash="0x" + f"{number:064x}", timestamp=100 + number)
    tx = "0x" + f"{number + 1000:064x}"
    name = (
        "Order"
        + {
            "accepted": "Created",
            "frozen": "Frozen",
            "executed": "Executed",
            "cancelled": "Cancelled",
        }[status]
    )
    values = (((("account", "0x" + "55" * 20),), ()),) + (((), ()),) * 6
    log = {
        "address": "0x" + "22" * 20,
        "topics": [
            "0x" + keccak(text=f"EventLog1(address,string,string,bytes32,{EVENT_DATA})").hex(),
            "0x" + keccak(text=name).hex(),
            KEY,
        ],
        "data": "0x"
        + encode(["address", "string", EVENT_DATA], ["0x" + "33" * 20, name, values]).hex(),
        "transactionHash": tx,
        "blockHash": block.hash,
        "blockNumber": hex(number),
        "logIndex": "0x1",
    }
    return RequestEvidence(
        status=status,
        key=KEY,
        identity=ReceiptIdentity(
            block=block,
            transaction_hash=tx,
            log_index=1,
            emitter="0x" + "22" * 20,
            handler="0x" + "33" * 20,
        ),
        receipt={
            "transactionHash": tx,
            "blockHash": block.hash,
            "blockNumber": hex(number),
            "status": "0x1",
            "logs": [log],
        },
    )


def request():
    asset = AssetType("eip155:42161/erc20:" + "0x" + "44" * 20)
    return Request(
        fund=Account(chain=CHAIN, address=AccountReference("0x" + "55" * 20)),
        allocation="Main",
        decision_identity="f" * 32,
        kind="order",
        key=KEY,
        market=asset,
        escrow=(AssetAmount(asset=asset, amount=100),),
        execution_fee=AssetAmount(asset=asset, amount=10),
        events=(evidence(),),
    )


def test_restart_and_frozen_requests_preserve_acceptance():
    original = request()
    frozen = original.append(evidence("frozen", 11))
    restored = Request.model_validate_json(frozen.model_dump_json())
    assert restored == frozen and restored.unresolved
    assert restored.events[0] == original.events[0]
    assert restored.append(restored.events[-1]) == restored


@pytest.mark.parametrize("status", ["executed", "cancelled"])
def test_terminal_result_is_immutable_and_duplicate_safe(status):
    terminal = request().append(evidence(status, 11))
    assert not terminal.unresolved
    assert terminal.append(terminal.events[-1]) == terminal
    with pytest.raises(ValueError, match="terminal history"):
        terminal.append(evidence("cancelled", 12))


@pytest.mark.parametrize("change", ["key", "chain", "handler", "order"])
def test_history_rejects_unrelated_or_old_evidence(change):
    event = evidence("executed", 11)
    if change == "key":
        event = event.model_copy(update={"key": "0x" + "66" * 32})
    elif change == "chain":
        block = event.identity.block.model_copy(
            update={"chain": CHAIN.model_copy(update={"environment": "mainnet"})}
        )
        event = event.model_copy(
            update={"identity": event.identity.model_copy(update={"block": block})}
        )
    elif change == "handler":
        event = event.model_copy(
            update={"identity": event.identity.model_copy(update={"handler": "0x" + "77" * 20})}
        )
    else:
        event = evidence("executed", 9)
    with pytest.raises(ValueError):
        request().append(event)


def test_successful_transaction_cannot_replace_matching_receipt_identity():
    event = evidence()
    with pytest.raises(ValueError, match="receipt identity"):
        RequestEvidence.model_validate({**event.model_dump(), "receipt": {"status": "0x1"}})


def test_cancellation_admission_cannot_use_another_safe_owned_request():
    from almanak.connectors.gmx_v2.requests import validate_cancellation

    retained = request()
    validate_cancellation(retained, retained.fund, "order", retained.key)
    with pytest.raises(ValueError, match="exact retained"):
        validate_cancellation(retained, retained.fund, "order", "0x" + "99" * 32)
    with pytest.raises(ValueError, match="exact retained"):
        validate_cancellation(retained, retained.fund, "withdrawal", retained.key)
    settled = retained.append(evidence("executed", 11))
    with pytest.raises(ValueError, match="settled"):
        validate_cancellation(settled, settled.fund, "order", settled.key)
