"""Identified fork setup and independent observations for Uniswap proofs."""

import json
from pathlib import Path
from types import SimpleNamespace

from eth_utils import keccak, to_checksum_address

from almanak.caip import AssetType, ChainId
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.domain import PoolPriceRequest
from almanak.connectors.uniswap_v4.models import Allocation, Config, Deployment, Range
from almanak.connectors.uniswap_v4.permissions import UniswapPermissions
from almanak.gateway.accounts import prepare_call
from almanak.gateway.permissions import generate, verify
from almanak.gateway.safe_fixture import deploy_profile, owner_transaction
from almanak.gateway.safe_funding import fund_usdc_storage, reviewed_funder
from almanak.models import Account, AccountBinding, AssetAmount, Chain, StrategyAccounts
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalTrigger

ROOT = Path(__file__).parents[2]
DEPLOYMENT = Deployment(
    router="0x66a9893cc07d91d95644aedd05d03f95e1dba8af",
    position_manager="0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
    quoter="0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203",
    permit2="0x000000000022d473030f116ddee9f6b43ac78ba3",
)


def remapped_pool(chain):
    pool = PoolPriceRequest.model_validate_json(
        (ROOT / "examples/ethereum/uniswap-v4-weth-usdc.json").read_text()
    ).source
    return pool.model_copy(
        update={
            "chain": chain,
            "currency0": AssetType(f"{chain.chain_id}/erc20:{pool.currency0.reference}"),
            "currency1": AssetType(f"{chain.chain_id}/erc20:{pool.currency1.reference}"),
        }
    )


def setup(node, reader):
    chain = reader.chain
    owner = node.funded_signer("", 0, 10**18, 61)
    operator = node.funded_signer("", 0, 10**18, 62)
    profile, _ = deploy_profile(node.rpc, chain, owner, operator, "uniswap")
    pool = remapped_pool(chain)
    config = Config(
        accounts=StrategyAccounts(bindings=(AccountBinding(name="uniswap", profile=profile),)),
        decision_trigger=IntervalTrigger(kind="interval", seconds=5),
        allocations=(
            Allocation(
                name="position",
                account="uniswap",
                pool=pool,
                contracts=DEPLOYMENT,
                range=Range(lower=197860, upper=199270),
                capital=AssetAmount(asset=pool.currency0, amount=100_000_000),
                hold_seconds=43200,
                collect_seconds=21600,
                slippage_bps=100,
                deadline_seconds=300,
                native_asset=AssetType(f"{chain.chain_id}/slip44:60"),
                minimum_native_balance=10**15,
            ),
        ),
        fork_origin=node.origin,
        fork_instance=node.metadata["instanceId"],
    )
    fund_usdc_storage(node, profile.fund.address, pool.currency0.reference, 100_000_000)
    backend = SimpleNamespace(reader=reader, guard=node._validate_origin)
    source = StrategyDefinition.read(ROOT / "strategies/uniswap_v4_lp/strategy.py")
    manifest = generate(config, source, backend, connector=UniswapPermissions())
    for call in manifest.partitions[0].transactions:
        owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)
    verify(config, manifest, backend, connector=UniswapPermissions())
    return owner, operator, config, manifest, backend


def fork_chain(node):
    return Chain(
        chain_id=ChainId("eip155:31337"), environment="anvil:" + node.metadata["instanceId"]
    )


def send(node, config, operator, call):
    prepared = prepare_call(config.accounts, "uniswap", call)
    return node.rpc.send(operator, to_checksum_address(prepared.to), prepared.data)


def is_fund_mint(log, target):
    topic = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
    return (
        log["address"].lower() == target.contracts.position_manager
        and log["topics"][0] == topic
        and len(log["topics"]) == 4
        and int(log["topics"][1], 16) == 0
        and log["topics"][2][-40:] == target.fund.address[2:]
    )


def minted_id(receipt, target):
    logs = [log for log in receipt["logs"] if is_fund_mint(log, target)]
    assert len(logs) == 1
    return int(logs[0]["topics"][3], 16)


def market_trade(node, target, reader, *, amount=500_000_000, trader_index=63):
    trader = node.funded_signer("", 0, 10**18, trader_index)
    funding = target.funding_currency
    # Storage funding is verified against the origin chain layout; the fork
    # remaps the chain id but keeps the canonical token address.
    origin_asset = AssetType(f"{node.origin.chain.chain_id}/erc20:{funding.reference}")
    funder = reviewed_funder(origin_asset)
    if funder is None:
        raise ValueError(f"fixture has no verified storage funding mechanism for {origin_asset}")
    funder(node, trader.address, funding.reference, amount)
    other = target.model_copy(update={"fund": Account(chain=reader.chain, address=trader.address)})
    block = reader.pin_block("latest")
    amount = AssetAmount(asset=target.funding_currency, amount=amount)
    calls = (
        abi.approve(other, amount.asset, amount.amount),
        abi.permit(
            other, amount.asset, other.contracts.router, amount.amount, block.timestamp + 300
        ),
        abi.swap(other, amount, 1, block.timestamp + 300),
    )
    return [node.rpc.send(trader, to_checksum_address(c.to), c.data) for c in calls]


def retain(name, payload):
    directory = ROOT / "artifacts/permission-fork"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(json.dumps(payload, indent=2) + "\n")
