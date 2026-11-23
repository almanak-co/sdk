"""The deposit a receipt proves, matched field by field against the signed call."""

from datetime import UTC, datetime

from eth_abi import decode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak

from almanak.connectors.across.operations import (
    ApproveOperation,
    DepositOperation,
    RevokeOperation,
    deposit_arguments,
)
from almanak.connectors.across.quote import exclusivity_end
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import WalletAsset
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from almanak.transfers import Transfer

CONNECTOR = ConnectorId("across")
FUNDS_DEPOSITED = (
    "FundsDeposited(bytes32,bytes32,uint256,uint256,uint256,uint256,uint32,uint32,uint32,"
    "bytes32,bytes32,bytes32,bytes)"
)
FUNDS_DEPOSITED_TOPIC = "0x" + keccak(text=FUNDS_DEPOSITED).hex()
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
APPROVAL_TOPIC = "0x" + keccak(text="Approval(address,address,uint256)").hex()
# Non-indexed fields, in event order.
DATA_TYPES = (
    "bytes32",
    "bytes32",
    "uint256",
    "uint256",
    "uint32",
    "uint32",
    "uint32",
    "bytes32",
    "bytes32",
    "bytes",
)


def mismatch(message: str) -> DataError:
    return DataError("receipt_mismatch", message)


def one_log(receipt, address: str, topic: str, topics: int) -> dict:
    logs = [
        log
        for log in receipt["logs"]
        if log["address"].lower() == address and log["topics"] and log["topics"][0] == topic
    ]
    if len(logs) != 1 or len(logs[0]["topics"]) != topics:
        raise mismatch("expected exactly one attributed Across event")
    return logs[0]


def topic_word(log, index: int) -> bytes:
    return bytes.fromhex(log["topics"][index][2:])


def topic_address(log, index: int) -> str:
    return "0x" + topic_word(log, index)[12:].hex()


def uint_data(log) -> int:
    raw = bytes.fromhex(log["data"][2:])
    if len(raw) != 32:
        raise mismatch("token event amount is not one ABI word")
    return int.from_bytes(raw)


def deposited(receipt, operation: DepositOperation, block_timestamp: int) -> dict:
    """The decoded `FundsDeposited` event, required to equal the signed deposit.

    `block_timestamp` is the mined block's: the SpokePool adds it to an exclusivity
    offset, so the event's exclusivity deadline is known only once the deposit is mined.
    """
    log = one_log(receipt, operation.target.route.spoke_pool, FUNDS_DEPOSITED_TOPIC, 4)
    try:
        data = decode(DATA_TYPES, bytes.fromhex(log["data"][2:]))
    except (DecodingError, ValueError):
        raise mismatch("Across deposit event data is malformed") from None
    (input_token, output_token, sent, offered, quoted_at, fill_deadline) = data[:6]
    exclusivity_deadline, recipient, relayer, message = data[6:]
    observed = (
        topic_word(log, 3),
        recipient,
        input_token,
        output_token,
        sent,
        offered,
        int.from_bytes(topic_word(log, 1)),
        relayer,
        quoted_at,
        fill_deadline,
        message,
    )
    signed = deposit_arguments(operation)
    if observed != signed[:10] + signed[11:]:
        raise mismatch("Across deposit event differs from the signed deposit")
    if exclusivity_deadline != exclusivity_end(operation.quote, block_timestamp):
        raise mismatch("Across deposit event has another exclusivity deadline")
    return {
        "deposit_id": int.from_bytes(topic_word(log, 2)),
        "exclusivity_deadline": exclusivity_deadline,
        "log_index": int(log["logIndex"], 16),
    }


def token_moved(receipt, operation: DepositOperation) -> int:
    """Base units the sent asset moved from the fund to the SpokePool."""
    target = operation.target
    log = one_log(receipt, target.asset.reference, TRANSFER_TOPIC, 3)
    if (topic_address(log, 1), topic_address(log, 2)) != (
        target.fund.address,
        target.route.spoke_pool,
    ):
        raise mismatch("the sent asset moved between other accounts than fund and SpokePool")
    return uint_data(log)


def approved(receipt, operation) -> None:
    target = operation.target
    log = one_log(receipt, target.asset.reference, APPROVAL_TOPIC, 3)
    expected = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
    if (topic_address(log, 1), topic_address(log, 2), uint_data(log)) != (
        target.fund.address,
        target.route.spoke_pool,
        expected,
    ):
        raise mismatch("Across approval has another owner, spender or bound")


def movement(receipt, operation, block_timestamp: int) -> tuple[int, dict | None]:
    """The amount that left the fund, and the deposit's own identity when one was made."""
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        approved(receipt, operation)
        return 0, None
    deposit = deposited(receipt, operation, block_timestamp)
    moved = token_moved(receipt, operation)
    if moved != operation.amount.amount:
        raise mismatch("the amount that left the fund differs from the signed deposit")
    return moved, deposit


def reference(deposit: dict) -> str:
    """The transfer's reference: the SpokePool's own deposit identifier on the source chain."""
    return str(deposit["deposit_id"])


def opened_transfer(operation: DepositOperation, deposit: dict) -> Transfer:
    """The claim the fund holds once the deposit is mined."""
    target, quote = operation.target, operation.quote
    return Transfer(
        connector=CONNECTOR,
        allocation=target.allocation,
        source=WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset),
        sent=operation.amount,
        destination=WalletAsset(
            account=target.destination_account,
            chain=target.destination_fund.chain,
            asset=target.destination_asset,
        ),
        offered=AssetAmount(asset=target.destination_asset, amount=quote.output_amount),
        reference=reference(deposit),
        fill_deadline=datetime.fromtimestamp(quote.fill_deadline, UTC),
    )
