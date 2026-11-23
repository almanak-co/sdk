"""The relay a deposit asks for: its hash on the destination chain and its retained reference.

A fill is matched by the hash of the whole relay, never by the deposit identifier
alone: anyone can fill a relay they invent under an existing identifier, and such a
fill pays whatever recipient it names.
"""

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.across.operations import DepositOperation, deposit_arguments
from almanak.gateway.environments import is_fork
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.models import Chain, Model, UInt

# V3RelayData in the SpokePool's field order, then the destination chain's own number.
RELAY_TYPES = (
    "(bytes32,bytes32,bytes32,bytes32,bytes32,uint256,uint256,uint256,uint256,uint32,uint32,bytes)",
    "uint256",
)


class DepositReference(Model):
    """What a later fill match needs and the destination chain cannot read for itself."""

    deposit_id: UInt
    relay_hash: str
    # The quote's own timestamp: no fill of these terms precedes the quote that set them.
    quoted_at: UInt

    @property
    def text(self) -> str:
        return f"{self.deposit_id}:{self.relay_hash}:{self.quoted_at}"


def parse_reference(text: str) -> DepositReference:
    parts = text.split(":")
    try:
        deposit_id, relay_hash, quoted_at = parts
        parsed = DepositReference(
            deposit_id=int(deposit_id), relay_hash=relay_hash, quoted_at=int(quoted_at)
        )
    except ValueError:
        raise DataError("integrity", "Across transfer reference is malformed") from None
    if parsed.text != text or len(relay_hash) != 66 or not relay_hash.startswith("0x"):
        raise DataError("integrity", "Across transfer reference is malformed")
    return parsed


def node_number(chain: Chain) -> int:
    """The chain number a SpokePool reads from its own node (`block.chainid`).

    An identified fork node answers the isolated signing chain, not its origin's number,
    so a relay of a fork is hashed and filled under that number on both sides.
    """
    return 31337 if is_fork(chain) else chain_number(chain.chain_id)


def relay_hash(relay: tuple, destination: int) -> str:
    """`getV3RelayHash` as the destination SpokePool computes it."""
    return "0x" + keccak(encode(RELAY_TYPES, (relay, destination))).hex()


def deposited_relay(operation: DepositOperation, deposit: dict) -> tuple:
    """The relay data of a mined deposit, from its signed call and its own event."""
    signed = deposit_arguments(operation)
    depositor, recipient, input_token, output_token, sent, offered = signed[:6]
    return (
        depositor,
        recipient,
        signed[7],
        input_token,
        output_token,
        sent,
        offered,
        node_number(operation.target.fund.chain),
        deposit["deposit_id"],
        signed[9],
        deposit["exclusivity_deadline"],
        b"",
    )


def deposit_reference(operation: DepositOperation, deposit: dict) -> DepositReference:
    relay = deposited_relay(operation, deposit)
    return DepositReference(
        deposit_id=deposit["deposit_id"],
        relay_hash=relay_hash(relay, node_number(operation.target.destination_fund.chain)),
        quoted_at=operation.quote.timestamp,
    )
