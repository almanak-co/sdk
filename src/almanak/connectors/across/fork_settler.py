"""Settle an open Across transfer on identified forks, where no relayer or dataworker runs.

A fill is the destination SpokePool's own `fillRelay`, called by a relayer this module
funds on the fork. A refund is a fork-constructed bundle: the SpokePool's admin is
impersonated to relay a root of one leaf this module wrote, and that leaf is executed
through the real contract code. No dataworker proposed it and no HubPool validated it,
so a refund settled here is never promotion or Fork Qualification evidence.
"""

import json
import time
from pathlib import Path
from types import SimpleNamespace

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

from almanak.caip import ChainId
from almanak.connectors.across.domain import spoke_pool
from almanak.connectors.across.operations import DepositOperation, word
from almanak.connectors.across.receipts import movement
from almanak.connectors.across.relay import RELAY_TYPES, deposited_relay, node_number
from almanak.gateway.environments import is_fork
from almanak.gateway.ethereum import DataError

RELAYER = "0x" + keccak(text="almanak across fork relayer")[-20:].hex()
RELAY = RELAY_TYPES[0]
LEAF = "(uint256,uint256,uint256[],uint32,address,address[])"
RELAYED_ROOT_TOPIC = "0x" + keccak(text="RelayedRootBundle(uint32,bytes32,bytes32)").hex()
# An Arbitrum SpokePool takes admin calls from the L1 admin's aliased address.
ARBITRUM = ChainId("eip155:42161")
L1_TO_L2_ALIAS = 0x1111000000000000000000000000000000001111
RECEIPT_SECONDS = 30
FORK_CONSTRUCTED = (
    "fork-constructed refund bundle: an impersonated admin relayed a one-leaf root "
    "written by the fixture; no dataworker proposed it and no HubPool validated it"
)


def selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


def receipt(rpc, transaction_hash) -> dict:
    deadline = time.monotonic() + RECEIPT_SECONDS
    while time.monotonic() < deadline:
        mined = rpc("eth_getTransactionReceipt", [transaction_hash])
        if mined is not None:
            if mined["status"] != "0x1":
                raise DataError("unavailable", f"fork settlement {transaction_hash} reverted")
            return mined
        time.sleep(0.05)
    raise DataError("unavailable", "fork settlement receipt remains unknown")


def sent(rpc, sender: str, to: str, data: bytes) -> dict:
    """One transaction from an impersonated fork account."""
    rpc("anvil_impersonateAccount", [sender])
    rpc("anvil_setBalance", [sender, hex(10**20)])
    try:
        # Explicit fees: a node's defaults can pair a tip above the fee cap.
        head = rpc("eth_getBlockByNumber", ["latest", False])
        fee = hex(2 * int(head.get("baseFeePerGas", "0x0"), 16) + 1)
        call = {"from": sender, "to": to, "data": "0x" + data.hex(), "gas": hex(3_000_000)}
        call |= {"maxFeePerGas": fee, "maxPriorityFeePerGas": "0x0"}
        return receipt(rpc, rpc("eth_sendTransaction", [call]))
    finally:
        rpc("anvil_stopImpersonatingAccount", [sender])


def timestamp(rpc, block="latest") -> int:
    return int(rpc("eth_getBlockByNumber", [block, False])["timestamp"], 16)


def deposit_of(state, transfer):
    """The relay the transfer's own deposit asked for, from its retained receipt."""
    for outcome in state.outcomes:
        if getattr(outcome, "transfer", None) is None:
            continue
        if outcome.transfer.identity != transfer.identity:
            continue
        operation: DepositOperation = outcome.operation
        _, deposit = movement(outcome.receipt, operation, outcome.after.block.timestamp)
        if deposit is None:
            break
        return operation, deposited_relay(operation, deposit)
    raise DataError("integrity", "an open Across transfer has no retained deposit")


def fill_call(relay: tuple, repayment_chain: int, relayer: str) -> bytes:
    arguments = encode((RELAY, "uint256", "bytes32"), (relay, repayment_chain, word(relayer)))
    return selector(f"fillRelay({RELAY},uint256,bytes32)") + arguments


def refund_calls(chain: int, token: str, payee: str, amount: int):
    """`relayRootBundle` of a root of one leaf, and that leaf's execution with no proof."""
    leaf = (0, chain, [amount], 0, to_checksum_address(token), [to_checksum_address(payee)])
    root = keccak(encode((LEAF,), (leaf,)))
    relay = selector("relayRootBundle(bytes32,bytes32)") + root + bytes(32)
    signature = f"executeRelayerRefundLeaf(uint32,{LEAF},bytes32[])"

    def execute(bundle: int) -> bytes:
        return selector(signature) + encode(("uint32", LEAF, "bytes32[]"), (bundle, leaf, []))

    return relay, execute


def admin(rpc, origin: ChainId, pool: str) -> str:
    """The sender the SpokePool accepts admin calls from on this chain."""
    if origin != ARBITRUM:
        raise DataError("unsupported", f"no fork refund bundle is defined for {origin}")
    answer = rpc("eth_call", [{"to": pool, "data": "0x" + selector("crossDomainAdmin()").hex()}])
    aliased = (int(answer, 16) + L1_TO_L2_ALIAS) % 2**160
    return "0x" + aliased.to_bytes(20).hex()


class Settler:
    """Fills each open transfer once. A refund is settled only when asked by name."""

    def __init__(self, forks, directory: Path):
        self.forks, self.directory = forks, directory

    def align(self, state, seconds: int) -> int:
        return seconds

    def settle(self, state) -> tuple[dict, ...]:
        return tuple(self.fill(state, transfer) for transfer in state.transfers if transfer.open)

    def fork(self, chain):
        if not is_fork(chain):
            raise DataError("unsupported", "Across transfers are settled only on identified forks")
        return self.forks.rpc(chain), self.forks.origin(chain)

    def fill(self, state, transfer) -> dict:
        from almanak.gateway.fork_funding import fund_erc20

        operation, relay = deposit_of(state, transfer)
        target = operation.target
        rpc, origin = self.fork(target.destination_fund.chain)
        pool = spoke_pool(target.route.destination_chain).address
        node, amount = SimpleNamespace(rpc=rpc, origin=origin), transfer.offered.amount
        funding = fund_erc20(node, RELAYER, target.destination_asset, amount)
        approve = selector("approve(address,uint256)") + encode(
            ("address", "uint256"), (to_checksum_address(pool), amount)
        )
        now = timestamp(rpc)
        sent(rpc, RELAYER, target.destination_asset.reference, approve)
        # Another relayer may fill only once the named one's exclusivity has ended.
        if int.from_bytes(relay[2]) and now <= relay[10]:
            rpc("evm_setNextBlockTimestamp", [relay[10] + 1])
        chain = node_number(target.destination_fund.chain)
        mined = sent(rpc, RELAYER, pool, fill_call(relay, chain, RELAYER))
        return self.retained(
            {
                "kind": "fill",
                "transfer": transfer.reference,
                "relayer": RELAYER,
                "funding": funding,
                "tx_hash": mined["transactionHash"],
                "block": int(mined["blockNumber"], 16),
                "advanced_seconds": timestamp(rpc, mined["blockNumber"]) - now,
            }
        )

    def refund(self, state, transfer) -> dict:
        """Pay the transfer's sent amount back to its fund through a fork-constructed bundle."""
        operation, _ = deposit_of(state, transfer)
        target = operation.target
        rpc, origin = self.fork(target.fund.chain)
        pool, token = target.route.spoke_pool, target.asset.reference
        now, deadline = timestamp(rpc), int(transfer.fill_deadline.timestamp())
        if now <= deadline:
            rpc("evm_setNextBlockTimestamp", [deadline + 1])
        relay, execute = refund_calls(
            node_number(target.fund.chain), token, target.fund.address, transfer.sent.amount
        )
        relayed = sent(rpc, admin(rpc, origin.chain.chain_id, pool), pool, relay)
        bundle = next(
            int(row["topics"][1], 16)
            for row in relayed["logs"]
            if row["topics"][0] == RELAYED_ROOT_TOPIC
        )
        mined = sent(rpc, RELAYER, pool, execute(bundle))
        return self.retained(
            {
                "kind": "refund",
                "evidence": FORK_CONSTRUCTED,
                "transfer": transfer.reference,
                "root_bundle": bundle,
                "tx_hash": mined["transactionHash"],
                "block": int(mined["blockNumber"], 16),
                "advanced_seconds": timestamp(rpc, mined["blockNumber"]) - now,
            }
        )

    def retained(self, row: dict) -> dict:
        self.directory.mkdir(parents=True, exist_ok=True)
        name = f"{row['kind']}-{row['tx_hash'][2:14]}.json"
        (self.directory / name).write_text(json.dumps(row, indent=2, sort_keys=True) + "\n")
        return row
