"""Exact compiled stAVAX proxy layout for identified Avalanche fork capital."""

from eth_utils import keccak

from almanak.caip import ChainId

TOKEN = "0xa25eaf2906fa1a3a13edac9b9657108af7b703e3"
IMPLEMENTATION = "0xa14505f315143e16b999b3aca083d306b85b803e"
IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
PROXY_HASH = "7cf821cb6be80c279a33af192aac28b56f85a2306aba7eb715073691c6e02abb"
IMPLEMENTATION_HASH = "38242e7c5a80be89915febe02efae3a853ff4347a56aa8df7b7d863b11eaf9bd"


def verify_layout(node):
    pointer = node.rpc("eth_getStorageAt", [TOKEN, IMPLEMENTATION_SLOT, "latest"])
    if int(pointer, 16) != int(IMPLEMENTATION, 16):
        raise ValueError("stAVAX fixture implementation differs from its compiled balance layout")
    for contract, expected in ((TOKEN, PROXY_HASH), (IMPLEMENTATION, IMPLEMENTATION_HASH)):
        code = node.rpc("eth_getCode", [contract, "latest"])
        if keccak(bytes.fromhex(code[2:])).hex() != expected:
            raise ValueError("stAVAX fixture runtime differs from its compiled balance layout")


def fund_stavax_storage(node, fund, token, amount):
    from almanak.gateway.safe_funding import set_balance_slot

    node._validate_origin()
    if (
        node.origin.chain.chain_id != ChainId("eip155:43114")
        or token != TOKEN
        or type(amount) is not int
        or not 0 <= amount < 2**255
    ):
        raise ValueError("stAVAX fixture funding requires its verified Avalanche deployment")
    verify_layout(node)
    # TokenggAVAX's compiled upgradeable ERC20 layout stores balances at slot 5.
    # Fixture capital does not modify the external vault conversion history.
    set_balance_slot(node, fund, token, amount, 5)
