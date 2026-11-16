"""Reviewed-hook pool keys, targets and swap receipts built without provider access."""

from eth_abi import encode

from almanak.caip import AssetType, ChainId
from almanak.connectors.uniswap_v4.deployments import NETWORK_ROLES
from almanak.connectors.uniswap_v4.domain import PoolObservation, UniswapPool
from almanak.connectors.uniswap_v4.economic_data import PoolSample, Word
from almanak.connectors.uniswap_v4.hooks import DYNAMIC_FEE, registry
from almanak.connectors.uniswap_v4.math import Q96
from almanak.connectors.uniswap_v4.models import Deployment, Range, Target
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.receipts import MODIFY, SWAP, TRANSFER
from almanak.connectors.uniswap_v4.state import Mint, Swap
from almanak.models import ZERO_ADDRESS, Account, Asset, AssetAmount, Block, Chain

ROBINHOOD = Chain(chain_id=ChainId("eip155:4663"), environment="mainnet")
ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
PONS_TOKEN = "0x39dbed3a2bd333467115de45665cc57f813c4571"
PRIORS = "0xedbf91223639800bcd5756815caf908df3b890be"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
ETH_USDG = "0xe343167631d89b6ffc58b88d6b7fb0228795491d"
FABLES_RAMP = "0x08e52564bad99e05a694b4809f397edca417a080"
FABLES_RWA = "0xa0e8fbff13e24af2b5e61a72800e08a161bde080"
PONS_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
STABLE_PAIR = "0x0000113dcf4add69999fad8f20f2b63f979bfcc0"
FUND = "0x" + "ab" * 20


def asset(chain, reference):
    return AssetType(f"{chain.chain_id}/erc20:{reference}")


def key(name):
    """(chain, currency0, currency1, fee, tick spacing, hook) of the fork-proof pools."""
    return {
        "fables": (ROBINHOOD, PONS_TOKEN, USDG, DYNAMIC_FEE, 60, FABLES_RAMP),
        "stable_pair": (ETHEREUM, USDC, ETH_USDG, DYNAMIC_FEE, 1, STABLE_PAIR),
        "pons": (ROBINHOOD, USDG, PRIORS, 0, 200, PONS_HOOK),
        "hookless": (ROBINHOOD, PONS_TOKEN, USDG, 500, 10, ZERO_ADDRESS),
    }[name]


def pool(name, **overrides):
    chain, currency0, currency1, fee, spacing, hooks = key(name)
    roles = NETWORK_ROLES[str(chain.chain_id)]
    values = {
        "kind": "uniswap_v4",
        "chain": chain,
        "pool_manager": roles["pool_manager"],
        "state_view": roles["state_view"],
        "currency0": asset(chain, currency0),
        "currency1": asset(chain, currency1),
        "fee": fee,
        "tick_spacing": spacing,
        "hooks": hooks,
        **overrides,
    }
    return UniswapPool.model_validate(values)


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
        funding_asset=None if funding is None else asset(source.chain, funding),
    )


def swap(name, currency, amount, minimum, funding=None):
    selected = target(name, funding)
    chain = selected.pool.chain
    out = selected.pool.currency1 if asset(chain, currency) == selected.pool.currency0 else None
    output = out or selected.pool.currency0
    return Swap(
        target=selected,
        amount=AssetAmount(asset=asset(chain, currency), amount=amount),
        minimum=AssetAmount(asset=output, amount=minimum),
        deadline=1,
    )


def pinned(hooks, chain="eip155:4663"):
    return registry()[(chain, hooks)].identity


class Authority:
    """AuthorityReader stand-in answering identity reads from a fixed table."""

    def __init__(self, identities):
        self.identities = identities

    def identity(self, address):
        return self.identities[address]


def topic(address):
    return "0x" + "00" * 12 + address[2:]


def transfer(token, sender, receiver, amount):
    return {
        "address": token,
        "topics": [TRANSFER, topic(sender), topic(receiver)],
        "data": "0x" + encode(("uint256",), (amount,)).hex(),
    }


def swap_receipt(operation, gross, take=0, fee=0):
    """A mined exact-input swap: the Swap event carries the pool deltas before afterSwap."""
    selected = operation.target
    source, manager = selected.pool, selected.pool.pool_manager
    zero_for_one = operation.amount.asset == source.currency0
    inputs, outputs = (
        (source.currency0, source.currency1)
        if zero_for_one
        else (source.currency1, source.currency0)
    )
    deltas = (
        (-operation.amount.amount, gross) if zero_for_one else (gross, -operation.amount.amount)
    )
    data = encode(
        ("int128", "int128", "uint160", "uint128", "int24", "uint24"), (*deltas, Q96, 1, 0, fee)
    )
    logs = [
        transfer(inputs.reference, selected.fund.address, manager, operation.amount.amount),
        {
            "address": manager,
            "topics": [SWAP, pool_id(source), topic(selected.contracts.router)],
            "data": "0x" + data.hex(),
        },
    ]
    if take:
        logs.append(transfer(outputs.reference, manager, source.hooks, take))
    logs.append(transfer(outputs.reference, manager, selected.fund.address, gross - take))
    return {"status": "0x1", "logs": logs}


def sample(name, hook):
    """A one-range sample at tick zero with no initialized ticks inside its coverage."""
    source = pool(name)
    block = Block(chain=source.chain, number=1, hash="0x" + "11" * 32, timestamp=1)
    observation = PoolObservation(
        source=source,
        block=block,
        currency0=Asset(chain=source.chain, identifier=source.currency0, decimals=18),
        currency1=Asset(chain=source.chain, identifier=source.currency1, decimals=18),
        sqrt_price_x96=Q96,
        tick=0,
        liquidity=10**24,
        protocol_fee=0,
        lp_fee=3000,
    )
    spacing = source.tick_spacing
    lower, upper = -50 * spacing, 50 * spacing
    words = tuple(
        Word(index=index, bitmap=0)
        for index in range((lower // spacing) >> 8, ((upper // spacing) >> 8) + 1)
    )
    return PoolSample(
        baseline_digest="0" * 64,
        pool=observation,
        lower=lower,
        upper=upper,
        words=words,
        ticks=(),
        fee_growth=None,
        boundary_gross=None,
        hook=hook,
    )


def mint(name, spacing_multiples=10, liquidity=10**12):
    width = key(name)[4] * spacing_multiples
    selected = target(name, range=Range(lower=-width, upper=width))
    source = selected.pool
    return Mint(
        target=selected,
        liquidity=liquidity,
        maximum0=AssetAmount(asset=source.currency0, amount=10**24),
        maximum1=AssetAmount(asset=source.currency1, amount=10**24),
        deadline=1,
    )


def mint_receipt(operation, token_id, spent):
    """A mined mint: the PositionManager's NFT, the ModifyLiquidity event and the deposits."""
    selected = operation.target
    source, manager, fund = selected.pool, selected.pool.pool_manager, selected.fund.address
    bounds = selected.range
    data = encode(
        ("int24", "int24", "int256", "bytes32"),
        (bounds.lower, bounds.upper, operation.liquidity, token_id.to_bytes(32, "big")),
    )
    position_manager = selected.contracts.position_manager
    return {
        "status": "0x1",
        "logs": [
            {
                "address": position_manager,
                "topics": [TRANSFER, topic(ZERO_ADDRESS), topic(fund), f"0x{token_id:064x}"],
                "data": "0x",
            },
            {
                "address": manager,
                "topics": [MODIFY, pool_id(source), topic(position_manager)],
                "data": "0x" + data.hex(),
            },
            *(
                transfer(currency.reference, fund, manager, amount)
                for currency, amount in zip(selected.assets, spent, strict=True)
            ),
        ],
    }
