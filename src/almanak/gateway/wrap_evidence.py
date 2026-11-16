"""What one holder wrapped and unwrapped in a receipt, read from the chain's wrapped-native events.

WETH9 and its clones (Ethereum, Base, Optimism; WPOL, WBNB and WAVAX on Polygon, BNB and
Avalanche) emit Deposit and Withdrawal; Robinhood's WETH and
Arbitrum's aeWETH mint and burn with Transfer from and to the zero address instead.
`EvmNetwork.wrapping_emits_transfer` selects the evidence a chain must show; the other kind
must be absent, so one wrap is never counted twice.
"""

from eth_abi import decode
from eth_utils import keccak

from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import events, topic_address
from almanak.models import ZERO_ADDRESS

TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
DEPOSIT = "0x" + keccak(text="Deposit(address,uint256)").hex()
WITHDRAWAL = "0x" + keccak(text="Withdrawal(address,uint256)").hex()


def wrap_flows(receipt, network, holder) -> tuple[int, int]:
    """(wrapped, unwrapped) amounts of the chain's wrapped native for `holder`."""
    token = network.wrapped_native.reference
    minted = [
        (topic_address(e["topics"][1]), topic_address(e["topics"][2]), amount(e))
        for e in events(receipt, token, TRANSFER)
        if len(e["topics"]) == 3 and ZERO_ADDRESS in map(topic_address, e["topics"][1:3])
    ]
    deposits = [(topic_address(e["topics"][1]), amount(e)) for e in events(receipt, token, DEPOSIT)]
    withdrawals = [
        (topic_address(e["topics"][1]), amount(e)) for e in events(receipt, token, WITHDRAWAL)
    ]
    if network.wrapping_emits_transfer:
        if deposits or withdrawals:
            raise DataError("receipt", "wrapped native emitted events this chain does not use")
        wrapped = [
            (receiver, value) for sender, receiver, value in minted if sender == ZERO_ADDRESS
        ]
        unwrapped = [
            (sender, value) for sender, receiver, value in minted if receiver == ZERO_ADDRESS
        ]
    else:
        if minted:
            raise DataError("receipt", "wrapped native minted or burned through Transfer")
        wrapped, unwrapped = deposits, withdrawals
    if any(who != holder for who, _ in (*wrapped, *unwrapped)):
        raise DataError("receipt", "wrapped native moved for an unexpected holder")
    return sum(v for _, v in wrapped), sum(v for _, v in unwrapped)


def amount(event) -> int:
    return decode(("uint256",), bytes.fromhex(event["data"][2:]))[0]
