"""Exact-input swaps with a native PoolKey currency, wrapped and unwrapped inside the router.

The fund only holds and pays the wrapped token, and the call carries no value:

- Native in: PERMIT2_TRANSFER_FROM pulls the input wrapped native to the router, UNWRAP_WETH
  turns all of it into native, and V4_SWAP settles the router's whole native balance before
  swapping the full credit (OPEN_DELTA); TAKE_ALL pays the output to the fund with the minimum.
- Native out: V4_SWAP swaps the fund's input (SETTLE_ALL through Permit2) and TAKEs the full
  native credit to the router; WRAP_ETH wraps the router's whole balance to the fund.

The router's WETH9 immutable is the chain's wrapped native (WPOL, WBNB and WAVAX included),
so the UNWRAP_WETH and WRAP_ETH commands serve every native coin.

Every later step uses the router's whole balance or the open delta, so the pulled amount is
the only input and nothing remains in the router; an unconsumed delta reverts the unlock.
The first input of both templates has one word length, so Roles 2.1 can combine them.
"""

import json
from functools import cache
from pathlib import Path

from eth_abi import encode

from almanak.chains import evm_network
from almanak.connectors.uniswap_v4.abi import ROUTER_EXECUTE, contract_call, swap_parameters
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity

ARTIFACT = Path(__file__).with_name("artifacts") / "native-routes.json"

PERMIT2_TRANSFER_FROM, WRAP_ETH, UNWRAP_WETH, V4_SWAP = 0x02, 0x0B, 0x0C, 0x10
SETTLE, SWAP_EXACT_IN_SINGLE, SETTLE_ALL, TAKE, TAKE_ALL = 0x0B, 0x06, 0x0C, 0x0E, 0x0F
NATIVE = "0x" + "00" * 20
ADDRESS_THIS = "0x" + "00" * 19 + "02"
CONTRACT_BALANCE = 1 << 255
OPEN_DELTA = 0
ETH_IN_COMMANDS = bytes((PERMIT2_TRANSFER_FROM, UNWRAP_WETH, V4_SWAP))
ETH_OUT_COMMANDS = bytes((V4_SWAP, WRAP_ETH))


@cache
def routes() -> dict:
    return json.loads(ARTIFACT.read_text())["chains"]


def require_native_route(target):
    """A native leg needs a reviewed router whose WETH9 immutable is the chain's wrapped native."""
    route = routes().get(str(target.pool.chain.chain_id))
    wrapped = evm_network(target.pool.chain.chain_id).wrapped_native.reference
    if (
        route is None
        or route["router"] != target.contracts.router
        or route["router_weth"] != wrapped
    ):
        raise ValueError(
            "Uniswap V4 native swaps need a reviewed router wrapping the chain's wrapped native"
        )
    if target.liquidity_enabled and (
        route["position_manager"] != target.contracts.position_manager
        or route["position_manager_weth"] != wrapped
    ):
        raise ValueError(
            "Uniswap V4 native liquidity needs a reviewed position manager wrapping the chain's "
            "wrapped native"
        )


def wrapped_identity(target) -> ContractIdentity:
    route = routes().get(str(target.pool.chain.chain_id))
    if route is None:
        raise DataError("authority", "Uniswap V4 native pools have no reviewed route on this chain")
    return ContractIdentity.model_validate(route["wrapped_native"])


def native_swap(target, zero_for_one, amount, minimum, deadline):
    held_in, held_out = (target.assets[0], target.assets[1])[:: 1 if zero_for_one else -1]
    if target.native_index == (0 if zero_for_one else 1):
        commands, inputs = eth_in(target, zero_for_one, held_in, held_out, amount, minimum)
    else:
        commands, inputs = eth_out(target, zero_for_one, held_in, amount, minimum)
    return contract_call(
        target,
        target.contracts.router,
        ROUTER_EXECUTE,
        ("bytes", "bytes[]", "uint256"),
        (commands, inputs, deadline),
    )


def eth_in(target, zero_for_one, wrapped, output, amount, minimum):
    swap = v4_swap(
        bytes((SETTLE, SWAP_EXACT_IN_SINGLE, TAKE_ALL)),
        (
            encode(("address", "uint256", "bool"), (NATIVE, CONTRACT_BALANCE, False)),
            swap_parameters(target, zero_for_one, OPEN_DELTA, minimum),
            encode(("address", "uint256"), (output.reference, minimum)),
        ),
    )
    pull = encode(("address", "address", "uint160"), (wrapped.reference, ADDRESS_THIS, amount))
    unwrap = encode(("address", "uint256"), (ADDRESS_THIS, 0))
    # Trailing zero words are ignored by the router and give both templates one first-input type.
    return ETH_IN_COMMANDS, (pull.ljust(len(swap), b"\0"), unwrap, swap)


def eth_out(target, zero_for_one, held_in, amount, minimum):
    swap = v4_swap(
        bytes((SWAP_EXACT_IN_SINGLE, SETTLE_ALL, TAKE)),
        (
            swap_parameters(target, zero_for_one, amount, minimum),
            encode(("address", "uint256"), (held_in.reference, amount)),
            encode(("address", "address", "uint256"), (NATIVE, ADDRESS_THIS, OPEN_DELTA)),
        ),
    )
    wrap = encode(("address", "uint256"), (target.fund.address, CONTRACT_BALANCE))
    return ETH_OUT_COMMANDS, (swap, wrap)


def v4_swap(actions, params):
    return encode(("bytes", "bytes[]"), (actions, params))
