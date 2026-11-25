"""Shared values for the Across connector tests: one USDC route from Arbitrum to Base."""

from types import SimpleNamespace

from almanak.caip import AssetType, ChainId
from almanak.connectors.across.authority import declared
from almanak.connectors.across.domain import SPOKE_POOLS, Route
from almanak.connectors.across.models import Allocation, Parameters
from almanak.connectors.across.observations import PoolState, Snapshot
from almanak.connectors.across.quote import parse
from almanak.gateway.evm import native_asset
from almanak.models import (
    Account,
    AccountBinding,
    AssetAmount,
    Block,
    Chain,
    Eoa,
    StrategyAccounts,
)
from almanak.triggers import IntervalTrigger

ARBITRUM = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
BASE = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
USDC = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")
BASE_USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
BASE_WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
PIN = SPOKE_POOLS[ARBITRUM.chain_id]
SENDER = "0x" + "11" * 20
RECIPIENT = "0x" + "22" * 20
RELAYER = "0xfd03abcadaf3f930fa4e37eb2f6ea3a44a41b7f0"
NOW = 1_795_423_000
AMOUNT = 1_000_000_000
BLOCK = Block(chain=ARBITRUM, number=400_000_000, hash="0x" + "ab" * 32, timestamp=NOW)
POOL = PoolState(
    address=PIN.address,
    implementation=PIN.implementation,
    implementation_code_hash=PIN.implementation_code_hash,
    quote_time_buffer=3600,
    fill_deadline_buffer=21600,
    deposits_paused=False,
)


def eoa(name, chain, address):
    fund = Account(chain=chain, address=address)
    return AccountBinding(name=name, profile=Eoa(kind="eoa", fund=fund))


ACCOUNTS = StrategyAccounts(bindings=(eoa("arb", ARBITRUM, SENDER), eoa("base", BASE, RECIPIENT)))


def route(**changes):
    values = {
        "chain": ARBITRUM,
        "asset": USDC,
        "destination_chain": BASE.chain_id,
        "destination_asset": BASE_USDC,
        "decimals": 6,
        "spoke_pool": PIN.address,
        "destination_pool": SPOKE_POOLS[BASE.chain_id],
        "max_fee_bps": 10,
        "quote": "across_api",
        **changes,
    }
    return Route(**values)


def allocation(**changes):
    values = {
        "name": "move",
        "account": "arb",
        "destination_account": "base",
        "route": route(),
        "capital": AMOUNT,
        "native_asset": native_asset(ARBITRUM.chain_id),
        "minimum_native_balance": 10**15,
        **changes,
    }
    return Allocation(**values)


def parameters(*allocations, accounts=ACCOUNTS):
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=accounts,
        allocations=allocations or (allocation(),),
    )


CONFIG = parameters()
TARGET = CONFIG.target("move")
INTENT = CONFIG.bridge("move", AMOUNT)


def body(**changes):
    """A venue answer in the shape `/suggested-fees` returns, mixed strings and numbers."""
    return {
        "outputAmount": "999897038",
        "timestamp": str(NOW - 69),
        "fillDeadline": str(NOW - 69 + 7200),
        "exclusiveRelayer": "0xFD03AbCAdaF3F930fA4E37Eb2f6ea3A44a41b7F0",
        "exclusivityDeadline": 3,
        "spokePoolAddress": "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",
        "destinationSpokePoolAddress": "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64",
        "inputToken": {
            "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "symbol": "USDC",
            "decimals": 6,
            "chainId": 42161,
        },
        "outputToken": {
            "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "symbol": "USDC",
            "decimals": 6,
            "chainId": 8453,
        },
        "isAmountTooLow": False,
        "estimatedFillTimeSec": 1,
        **changes,
    }


def quote(**changes):
    return parse(body(**changes), AMOUNT)


def snapshot(**changes):
    values = {
        "target": TARGET,
        "block": BLOCK,
        "wallet_balance": AMOUNT,
        "allowance": 0,
        "pool": POOL,
        "native_balance": AssetAmount(asset=native_asset(ARBITRUM.chain_id), amount=10**18),
        "payer": TARGET.fund,
        **changes,
    }
    return Snapshot(**values)


def state(**changes):
    """The checkpoint fields planning and validation read."""
    decision = SimpleNamespace(intent=INTENT, identity="0" * 32)
    values = {
        "config": CONFIG,
        "capabilities": declared(CONFIG),
        "decision": decision,
        "outcomes": (),
        "baseline": (),
        "mode": "running",
        "transfers": (),
        "owned": (),
        **changes,
    }
    return SimpleNamespace(**values)


def mined(kind, allowance, identity="0" * 32):
    """A retained successful outcome of this decision, as planning reads it."""
    return SimpleNamespace(
        success=True,
        decision=SimpleNamespace(identity=identity),
        operation=SimpleNamespace(kind=kind),
        after=SimpleNamespace(target=TARGET, allowance=allowance),
        accounting_issues=(),
    )
