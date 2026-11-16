"""Native-currency V4 pool keys, targets and router receipts built without provider access."""

from eth_abi import encode

from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.connectors.uniswap_v4.deployments import NETWORK_ROLES
from almanak.connectors.uniswap_v4.domain import UniswapPool
from almanak.connectors.uniswap_v4.hooks import DYNAMIC_FEE
from almanak.connectors.uniswap_v4.math import Q96
from almanak.connectors.uniswap_v4.models import Deployment, Range, Target
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.receipts import SWAP, TRANSFER
from almanak.connectors.uniswap_v4.state import Swap
from almanak.gateway.wrap_evidence import DEPOSIT, WITHDRAWAL
from almanak.models import ZERO_ADDRESS, Account, AssetAmount, Chain

ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
ARBITRUM = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
ROBINHOOD = Chain(chain_id=ChainId("eip155:4663"), environment="mainnet")
BNB = Chain(chain_id=ChainId("eip155:56"), environment="mainnet")
POLYGON = Chain(chain_id=ChainId("eip155:137"), environment="mainnet")
AVALANCHE = Chain(chain_id=ChainId("eip155:43114"), environment="mainnet")
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
NOVAAI = "0x51250b135174ca09450ec01c4aff73cf69dbb590"
BNB_USDT = "0x55d398326f99059ff775485246999027b3197955"
POLYGON_USDC = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
AVALANCHE_USDC = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
FABLES_RAMP_ETH = "0x06a889870c8f83640d6816319f72e2aa579b6080"
PONS_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
FUND = "0x" + "ab" * 20


def native(chain):
    return evm_network(chain.chain_id).native_asset


def wrapped(chain):
    return evm_network(chain.chain_id).wrapped_native


def erc20(chain, reference):
    return AssetType(f"{chain.chain_id}/erc20:{reference}")


def key(name):
    """(chain, currency0, currency1, fee, tick spacing, hook) of surveyed native pools."""
    return {
        "eth_usdc": (ETHEREUM, None, USDC, 3000, 60, ZERO_ADDRESS),
        "arb_eth_usdc": (ARBITRUM, None, ARB_USDC, 500, 10, ZERO_ADDRESS),
        "fables_weth_usdg": (ROBINHOOD, None, USDG, DYNAMIC_FEE, 10, FABLES_RAMP_ETH),
        "pons_novaai": (ROBINHOOD, None, NOVAAI, 0, 200, PONS_HOOK),
        "eth_weth": (ETHEREUM, None, wrapped(ETHEREUM).reference, 500, 10, ZERO_ADDRESS),
        # Hookless pools on the coin each chain calls native, surveyed 2026-10-15.
        "pol_usdc": (POLYGON, None, POLYGON_USDC, 3000, 60, ZERO_ADDRESS),
        "bnb_usdt": (BNB, None, BNB_USDT, 500, 10, ZERO_ADDRESS),
        "avax_usdc": (AVALANCHE, None, AVALANCHE_USDC, 500, 10, ZERO_ADDRESS),
    }[name]


def pool(name):
    chain, _, currency1, fee, spacing, hooks = key(name)
    roles = NETWORK_ROLES[str(chain.chain_id)]
    return UniswapPool.model_validate(
        {
            "kind": "uniswap_v4",
            "chain": chain,
            "pool_manager": roles["pool_manager"],
            "state_view": roles["state_view"],
            "currency0": native(chain),
            "currency1": erc20(chain, currency1),
            "fee": fee,
            "tick_spacing": spacing,
            "hooks": hooks,
        }
    )


def target(name, funding=None, range=None):
    source = pool(name)
    roles = NETWORK_ROLES[str(source.chain.chain_id)]
    return Target(
        allocation="trade",
        account="fund",
        fund=Account(chain=source.chain, address=FUND),
        pool=source,
        contracts=Deployment(
            router=roles["router"],
            position_manager=roles["position_manager"],
            quoter=roles["quoter"],
            permit2=roles["permit2"],
        ),
        range=range,
        funding_asset=funding,
    )


def swap(name, eth_in, amount, minimum):
    selected = target(name)
    held = selected.assets
    source, output = (held[0], held[1]) if eth_in else (held[1], held[0])
    return Swap(
        target=selected,
        amount=AssetAmount(asset=source, amount=amount),
        minimum=AssetAmount(asset=output, amount=minimum),
        deadline=1,
    )


def topic(address):
    return "0x" + "00" * 12 + address[2:]


def log(address, topics, amount):
    return {
        "address": address,
        "topics": topics,
        "data": "0x" + encode(("uint256",), (amount,)).hex(),
    }


def transfer(token, sender, receiver, amount):
    return log(token, [TRANSFER, topic(sender), topic(receiver)], amount)


def wrap_log(chain, holder, amount, wrapping):
    network = evm_network(chain.chain_id)
    token = network.wrapped_native.reference
    if network.wrapping_emits_transfer:
        sender, receiver = (ZERO_ADDRESS, holder) if wrapping else (holder, ZERO_ADDRESS)
        return transfer(token, sender, receiver, amount)
    return log(token, [DEPOSIT if wrapping else WITHDRAWAL, topic(holder)], amount)


def swap_event(selected, delta0, delta1):
    data = encode(
        ("int128", "int128", "uint160", "uint128", "int24", "uint24"),
        (delta0, delta1, Q96, 1, 0, 0),
    )
    return {
        "address": selected.pool.pool_manager,
        "topics": [SWAP, pool_id(selected.pool), topic(selected.contracts.router)],
        "data": "0x" + data.hex(),
    }


def eth_in_receipt(operation, output, take=0, router_dust=0):
    """Fund WETH to the router, router unwrap (with any dust), pool swap, output to the fund."""
    selected = operation.target
    chain, router, manager = (
        selected.pool.chain,
        selected.contracts.router,
        selected.pool.pool_manager,
    )
    paid = operation.amount.amount
    unwrapped = paid + router_dust
    token = selected.assets[1]
    logs = [
        transfer(selected.assets[0].reference, FUND, router, paid),
        wrap_log(chain, router, unwrapped, wrapping=False),
        swap_event(selected, -unwrapped, output),
    ]
    if take:
        logs.append(transfer(token.reference, manager, selected.pool.hooks, take))
    logs.append(transfer(token.reference, manager, FUND, output - take))
    return {"status": "0x1", "logs": logs}


def eth_out_receipt(operation, gross, take=0, router_dust=0):
    """Fund token to the PoolManager, pool swap, router wrap of the native output (and any
    native already in the router) to the fund."""
    selected = operation.target
    chain, router, manager = (
        selected.pool.chain,
        selected.contracts.router,
        selected.pool.pool_manager,
    )
    paid = operation.amount.amount
    received = gross - take + router_dust
    return {
        "status": "0x1",
        "logs": [
            swap_event(selected, gross, -paid),
            transfer(selected.assets[1].reference, FUND, manager, paid),
            wrap_log(chain, router, received, wrapping=True),
            transfer(selected.assets[0].reference, router, FUND, received),
        ],
    }


def liquidity_range(name):
    spacing = key(name)[4]
    return Range(lower=-10 * spacing, upper=10 * spacing)
