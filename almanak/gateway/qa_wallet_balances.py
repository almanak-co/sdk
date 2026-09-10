"""Raw balance observations for the explicitly enabled local QA experiment."""

from web3 import Web3


def capture_wallet_balances(client, wallet, addresses, *, block_identifier="latest"):
    wallet = Web3.to_checksum_address(wallet)
    addresses = sorted({Web3.to_checksum_address(address) for address in addresses})
    block = client.eth.get_block(block_identifier)
    number, block_hash = int(block["number"]), Web3.to_hex(block["hash"])
    tokens = {}
    for address in addresses:
        call = {"to": address, "data": "0x70a08231" + wallet[2:].lower().rjust(64, "0")}
        raw = client.eth.call(call, block_identifier=number)
        if len(raw) != 32:
            raise ValueError("QA wallet balance returned no uint256 measurement")
        tokens[address.lower()] = {"call": call, "response": Web3.to_hex(raw)}
    native = client.eth.get_balance(wallet, block_identifier=number)
    nonce = client.eth.get_transaction_count(wallet, block_identifier=number)
    code = Web3.to_hex(client.eth.get_code(wallet, block_identifier=number))
    if Web3.to_hex(client.eth.get_block(number)["hash"]) != block_hash:
        raise ValueError("QA wallet balance block changed during capture")
    return {
        "schema_version": 1,
        "scope": "raw_wallet_balances",
        "wallet": wallet,
        "block": {"number": number, "hash": block_hash},
        "tokens": tokens,
        "native_wei": str(native),
        "nonce": nonce,
        "wallet_code": code,
    }
