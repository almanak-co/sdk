"""The reads that attribute a custody request (blueprint 30, section 4.4).

HyperEVM reads go through a JSON-RPC callable; HyperCore reads through the `/info`
whitelist, whose answers are retained with their request and time. HyperCore's credits
to HyperEVM are system transactions, which `credit_transactions` finds through their USDC
`Transfer` log out of CoreDepositWallet, on mainnet and on forks alike (blueprint 30,
section 14.2): a mainnet provider serves a payout's log, transaction and receipt at once
but adds it to the block body a minute or more later, and `eth_getSystemTxsByBlockNumber`
names it by a hash nothing else knows. On a fork the HyperCore model sends it as an
ordinary transaction from the same system address, which reads the same way.
"""

from eth_abi import decode
from eth_utils import keccak

from almanak.connectors.hyperliquid.agents import Submission
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.corewriter import CORE_WRITER, USDC_CONTRACT, USDC_SYSTEM
from almanak.connectors.hyperliquid.custody_requests import (
    CreditLog,
    Evidence,
    SystemTransfer,
    second,
)
from almanak.gateway.hypercore_info import InfoRequest, UserAbstraction, UserNonFundingLedgerUpdates
from almanak.models import address

TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
RAW_ACTION = "0x" + keccak(text="RawAction(address,bytes)").hex()
CDW_TRANSFER = "0x" + keccak(text="transfer(address,uint256)")[:4].hex()


def topic(value: str) -> str:
    return "0x" + value[2:].rjust(64, "0")


def credit_transactions(rpc, number: int) -> list[dict]:
    """Every transaction moving USDC out of CoreDepositWallet in block `number`, which
    `system_transfer` then narrows to a payout's own shape."""
    query = {
        "address": USDC_CONTRACT,
        "topics": [TRANSFER, topic(CORE_DEPOSIT_WALLET)],
        "fromBlock": hex(number),
        "toBlock": hex(number),
    }
    hashes = dict.fromkeys(log["transactionHash"] for log in rpc("eth_getLogs", [query]))
    found = (rpc("eth_getTransactionByHash", [h]) for h in hashes)
    return [transaction for transaction in found if transaction is not None]


def system_transfer(transaction: dict, number: int) -> SystemTransfer | None:
    """A system transaction paying USDC out of CoreDepositWallet, else None."""
    data = transaction.get("input") or "0x"
    if (
        (transaction.get("from") or "").lower() != USDC_SYSTEM
        or (transaction.get("to") or "").lower() != CORE_DEPOSIT_WALLET
        or not data.startswith(CDW_TRANSFER)
    ):
        return None
    recipient, amount = decode(("address", "uint256"), bytes.fromhex(data[10:]))
    return SystemTransfer(
        transaction=transaction["hash"], block=number, recipient=address(recipient), amount=amount
    )


class Reads:
    def __init__(self, rpc, info):
        self.rpc = rpc
        self.info = info

    def timestamp(self, number: int) -> int | None:
        block = self.rpc("eth_getBlockByNumber", [hex(number), False])
        return None if block is None else int(block["timestamp"], 16)

    def second_blocks(self, submission: Submission) -> tuple[int, int]:
        """The blocks whose timestamp is the submission block's second."""
        seconds = submission.time // 1000
        low = submission.block
        while low > 0 and self.timestamp(low - 1) == seconds:
            low -= 1
        high = submission.block
        while self.timestamp(high + 1) == seconds:
            high += 1
        return low, high

    def logs(self, contract: str, topics: list, blocks: tuple[int, int]) -> list[dict]:
        query = {
            "address": contract,
            "topics": topics,
            "fromBlock": hex(blocks[0]),
            "toBlock": hex(blocks[1]),
        }
        return self.rpc("eth_getLogs", [query])

    def credits(self, safe: str, blocks: tuple[int, int]) -> tuple[CreditLog, ...]:
        topics = [TRANSFER, topic(safe), topic(USDC_SYSTEM)]
        return tuple(
            CreditLog(
                transaction=log["transactionHash"],
                log_index=int(log["logIndex"], 16),
                block=int(log["blockNumber"], 16),
                recipient=safe,
                amount=int(log["data"], 16),
            )
            for log in self.logs(CORE_DEPOSIT_WALLET, topics, blocks)
        )

    def actions(self, safe: str, blocks: tuple[int, int]) -> tuple[str, ...]:
        found = []
        for log in self.logs(CORE_WRITER, [RAW_ACTION, topic(safe)], blocks):
            (raw,) = decode(("bytes",), bytes.fromhex(log["data"][2:]))
            found.append("0x" + raw.hex())
        return tuple(found)

    def ledger(self, safe: str, submission: Submission):
        start, end = second(submission)
        query = UserNonFundingLedgerUpdates(user=safe, start_time=start, end_time=end)
        return self.info.fetch(InfoRequest(info=query))

    def system(self, safe, amount, submission: Submission):
        """Every executed system transfer of `amount` to the Safe in the block after the
        submission's, and when the HyperEVM side was observed: the head's time.

        HyperCore delivers a HyperEVM credit in the next block (Hyperliquid's interaction
        timings, and the canary), so a payout in any later block is another withdrawal's
        and never this one's. The head is read separately, so a request whose credit
        never appears still reaches its deadline.
        """
        head = int(self.rpc("eth_blockNumber"), 16)
        head_time = (self.timestamp(head) or 0) * 1000
        if head <= submission.block:
            return (), head_time
        return self.paid(safe, amount, submission.block + 1), head_time

    def paid(self, safe, amount, number: int) -> tuple[SystemTransfer, ...]:
        found = []
        for transaction in credit_transactions(self.rpc, number):
            transfer = system_transfer(transaction, number)
            if transfer is not None and (transfer.recipient, transfer.amount) == (safe, amount):
                if self.credited(transfer):
                    found.append(transfer)
        return tuple(found)

    def credited(self, transfer: SystemTransfer) -> bool:
        """The transfer's receipt succeeded with the exact USDC credit to the Safe.

        A reverted `CoreDepositWallet.transfer` pays nothing whatever its calldata says.
        Mainnet providers serving system transaction receipts is unverified; one that
        serves none leaves the withdrawal unresolved, which halts it."""
        receipt = self.rpc("eth_getTransactionReceipt", [transfer.transaction])
        if receipt is None or receipt.get("status") != "0x1":
            return False
        credit = [TRANSFER, topic(CORE_DEPOSIT_WALLET), topic(transfer.recipient)]
        return any(
            log["address"].lower() == USDC_CONTRACT
            and log["topics"] == credit
            and int(log["data"], 16) == transfer.amount
            for log in receipt["logs"]
        )

    def evidence(self, request, window: int) -> Evidence:
        if request.kind == "standard_mode":
            answer = self.info.fetch(InfoRequest(info=UserAbstraction(user=request.safe)))
            return Evidence(abstraction=answer)
        blocks = self.second_blocks(request.submission)
        found = {
            "ledger": self.ledger(request.safe, request.submission),
            "actions": self.actions(request.safe, blocks),
        }
        if request.kind == "deposit":
            found["credits"] = self.credits(request.safe, blocks)
        if request.kind == "withdrawal":
            system, head_time = self.system(request.safe, request.amount, request.submission)
            found.update(system=system, head_time=head_time)
        return Evidence(**found)
