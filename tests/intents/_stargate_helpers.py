"""On-chain fee oracle for Stargate source-deposit intent tests."""

from eth_abi import decode, encode
from web3 import Web3


def assert_stargate_native_fee(web3: Web3, deposit_tx: dict) -> None:
    """Check msg.value against quoteSend for the actual send, independent of adapter hints."""
    send_type = "(uint32,bytes32,uint256,uint256,bytes,bytes,bytes)"
    send_param, fee, _ = decode([send_type, "(uint256,uint256)", "address"], bytes.fromhex(deposit_tx["data"][10:]))
    calldata = Web3.keccak(text=f"quoteSend({send_type},bool)")[:4] + encode([send_type, "bool"], [send_param, False])
    raw = web3.eth.call({"to": Web3.to_checksum_address(deposit_tx["to"]), "data": calldata})
    native_fee, token_fee = decode(["uint256", "uint256"], raw)
    value = int(deposit_tx["value"])
    assert native_fee > 0
    assert token_fee == fee[1] == 0
    assert value == fee[0], "ERC20 send value must contain only the encoded native messaging fee"
    assert native_fee < value <= (native_fee * 120 + 99) // 100, "Fee must track live quote within 20%"
