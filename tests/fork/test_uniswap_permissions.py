"""Real periphery calls through strategy-generated nested Roles conditions."""

import json

import pytest

from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.deployments import ARTIFACTS
from almanak.connectors.uniswap_v4.math import entry_split, liquidity_for
from almanak.connectors.uniswap_v4.permissions import UniswapPermissions
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.connectors.uniswap_v4.reader import quote, sqrt_bounds, wallet_balance
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.permissions import verify
from almanak.models import AssetAmount, Block
from tests.fork.uniswap_support import fork_chain, market_trade, minted_id, retain, send, setup


@pytest.fixture
def uniswap_fork(request):
    pin = Block.model_validate_json(
        json.dumps(json.loads((ARTIFACTS / "ethereum-25908472.json").read_text())["block"])
    )
    with Fork(request.config.getoption("--fork-env"), pin) as node:
        with EthereumReader(fork_chain(node), node.rpc.endpoint) as reader:
            yield node, reader, setup(node, reader)


@pytest.mark.fork
def test_generated_uniswap_lifecycle(uniswap_fork):
    node, reader, (_, operator, config, manifest, backend) = uniswap_fork
    target = config.target("position")
    receipts = []

    def execute(call):
        receipt = send(node, config, operator, call)
        receipts.append({"call": call.model_dump(mode="json"), "receipt": receipt})
        return receipt

    balanced_entry(reader, target, execute)
    mint_receipt = mint_inventory(reader, target, execute)
    token_id = minted_id(mint_receipt, target)
    market_trade(node, target, reader)
    block = reader.pin_block("latest")
    before = tuple(wallet_balance(reader, target, a, block) for a in target.assets)
    execute(abi.collect(target, token_id, block.timestamp + 300))
    block = reader.pin_block("latest")
    after = tuple(wallet_balance(reader, target, a, block) for a in target.assets)
    assert after[0] > before[0], "external fork swap must generate measured LP fees"
    execute(abi.close(target, token_id, 0, 0, block.timestamp + 300))
    liquidate(reader, target, execute)
    clear_allowances(target, execute)
    verify(config, manifest, backend, connector=UniswapPermissions())
    block = reader.pin_block("latest")
    assert_terminal_inventory(reader, target, block)
    retain(
        "uniswap-permissions.json",
        {"manifest": manifest.model_dump(mode="json"), "receipts": receipts, "token_id": token_id},
    )


def mint_inventory(reader, target, execute):
    block = reader.pin_block("latest")
    balances = tuple(wallet_balance(reader, target, a, block) for a in target.assets)
    price = read_pool(reader, target.pool, block).sqrt_price_x96
    liquidity = liquidity_for(*(b * 99 // 100 for b in balances), price, *sqrt_bounds(target))
    for asset, amount in zip(target.assets, balances, strict=True):
        if asset == target.pool.currency1:
            execute(abi.approve(target, asset, amount))
        execute(
            abi.permit(
                target, asset, target.contracts.position_manager, amount, block.timestamp + 300
            )
        )
    return execute(abi.mint(target, liquidity, *balances, block.timestamp + 300))


def assert_terminal_inventory(reader, target, block):
    assert wallet_balance(reader, target, target.pool.currency1, block) == 0
    assert wallet_balance(reader, target, target.pool.currency0, block) > 0


def liquidate(reader, target, execute):
    block = reader.pin_block("latest")
    amount = AssetAmount(
        asset=target.pool.currency1,
        amount=wallet_balance(reader, target, target.pool.currency1, block),
    )
    minimum = quote(reader, target, amount, block).output.amount * 99 // 100
    execute(abi.approve(target, amount.asset, amount.amount))
    execute(
        abi.permit(
            target, amount.asset, target.contracts.router, amount.amount, block.timestamp + 300
        )
    )
    execute(abi.swap(target, amount, minimum, block.timestamp + 300))


def balanced_entry(reader, target, execute):
    block = reader.pin_block("latest")
    pool = read_pool(reader, target.pool, block)
    bounds = sqrt_bounds(target)
    split = entry_split(
        100_000_000,
        pool.sqrt_price_x96,
        *bounds,
        lambda x: (
            quote(
                reader, target, AssetAmount(asset=target.pool.currency0, amount=x), block
            ).output.amount
        ),
    )
    buy = AssetAmount(asset=target.pool.currency0, amount=split)
    output = quote(reader, target, buy, block).output.amount
    deadline = block.timestamp + 300
    execute(abi.approve(target, buy.asset, 100_000_000))
    execute(abi.permit(target, buy.asset, target.contracts.router, split, deadline))
    execute(abi.swap(target, buy, output * 99 // 100, deadline))


def clear_allowances(target, execute):
    for asset in target.assets:
        for spender in (target.contracts.router, target.contracts.position_manager):
            execute(abi.permit(target, asset, spender, 0, 0))
        execute(abi.approve(target, asset, 0))
