"""Adversarial Roles calls and the accepted wider NFT exit authority."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode
from eth_utils import to_checksum_address

from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.authority import declared
from almanak.connectors.uniswap_v4.lifecycle import UniswapLifecycle
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.observations import snapshot
from almanak.connectors.uniswap_v4.permissions import UniswapPermissions
from almanak.connectors.uniswap_v4.state import Collect, DecisionRecord
from almanak.connectors.uniswap_v4.validation import validate_operation
from almanak.dex_intents import CollectLiquidityFees
from almanak.gateway.permissions import verify
from almanak.gateway.safe_fixture import calldata
from almanak.gateway.safe_funding import fund_usdc_storage
from almanak.models import Account, AssetAmount
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalEvent
from tests.fork.test_safe_permissions import rejected_submission
from tests.fork.test_uniswap_permissions import mint_inventory
from tests.fork.test_uniswap_permissions import uniswap_fork as uniswap_fork
from tests.fork.uniswap_support import ROOT, market_trade, minted_id, retain, send

pytestmark = pytest.mark.fork


def router_mutation(target, call, change):
    commands, inputs, deadline = decode(
        ("bytes", "bytes[]", "uint256"), bytes.fromhex(call.data[10:])
    )
    actions, parameters = decode(("bytes", "bytes[]"), inputs[0])
    if change == "extra_action":
        actions += b"\x0f"
        parameters = (*parameters, parameters[-1])
    if change == "recipient":
        actions = actions[:-1] + b"\x0e"
        parameters = (
            *parameters[:-1],
            encode(
                ("address", "address", "uint256"), (target.assets[1].reference, "0x" + "12" * 20, 1)
            ),
        )
    if change == "commands":
        commands += b"\x10"
        inputs = (*inputs, inputs[0])
    else:
        inputs = (encode(("bytes", "bytes[]"), (actions, parameters)),)
    return call.model_copy(
        update={
            "data": calldata(
                abi.ROUTER_EXECUTE, ("bytes", "bytes[]", "uint256"), (commands, inputs, deadline)
            )
        }
    )


def forbidden(target, deadline):
    amount = AssetAmount(asset=target.assets[0], amount=1_000_000)
    swap = abi.swap(target, amount, 1, deadline)
    changed_pool = target.model_copy(update={"pool": target.pool.model_copy(update={"fee": 3000})})
    other_fund = target.model_copy(
        update={"fund": Account(chain=target.fund.chain, address="0x" + "12" * 20)}
    )
    # Scope-only authority (blueprint 27): amounts and ticks are not on-chain conditions,
    # so quota, unbounded-approval and other-range probes are gone; the gateway checks them.
    return {
        "wrong_pool": abi.swap(changed_pool, amount, 1, deadline),
        "wrong_erc20_spender": abi.approve(target, amount.asset, 1).model_copy(
            update={
                "data": calldata(
                    "approve(address,uint256)", ("address", "uint256"), ("0x" + "12" * 20, 1)
                )
            }
        ),
        "wrong_spender": abi.permit(target, amount.asset, "0x" + "12" * 20, 1, deadline),
        "mint_recipient": abi.mint(other_fund, 1, 1, 1, deadline),
        "close_recipient": abi.close(other_fund, 1, 0, 0, deadline),
        "zero_nft": abi.collect(target, 0, deadline),
        "zero_swap_minimum": abi.swap(target, amount, 0, deadline),
        **{
            name: router_mutation(target, swap, name)
            for name in ("extra_action", "recipient", "commands")
        },
    }


def test_generated_roles_reject_uniswap_parameter_attacks(uniswap_fork):
    node, reader, (_, operator, config, manifest, backend) = uniswap_fork
    target = config.target("position")
    profile = config.accounts.binding(target.account).profile
    before = verify(config, manifest, backend, connector=UniswapPermissions())["partitions"]
    cases = forbidden(target, reader.pin_block("latest").timestamp + 300)
    evidence = [
        rejected_submission(node, operator, profile, (name, call.to, call.data, 0, 0))
        for name, call in cases.items()
    ]
    assert verify(config, manifest, backend, connector=UniswapPermissions())["partitions"] == before
    retain("uniswap-rejections.json", evidence)


def independent_nft(node, reader, target):
    signer = node.funded_signer(to_checksum_address(target.assets[1].reference), 10**16, 10**18, 64)
    fund_usdc_storage(node, signer.address, target.assets[0].reference, 100_000_000)
    other = target.model_copy(
        update={"fund": Account(chain=target.fund.chain, address=signer.address)}
    )

    def execute(call):
        return node.rpc.send(signer, to_checksum_address(call.to), call.data)

    execute(abi.approve(other, target.assets[0], 100_000_000))
    receipt = mint_inventory(reader, other, execute)
    return signer, minted_id(receipt, other)


@pytest.mark.parametrize("authority", ["owns", "approved"])
def test_broad_exit_grant_reaches_unrelated_nft_but_gateway_refuses(uniswap_fork, authority):
    node, reader, (_, operator, config, manifest, backend) = uniswap_fork
    target = config.target("position")
    signer, token_id = independent_nft(node, reader, target)
    if authority == "owns":
        data = calldata(
            "transferFrom(address,address,uint256)",
            ("address", "address", "uint256"),
            (signer.address, target.fund.address, token_id),
        )
    else:
        data = calldata(
            "approve(address,uint256)", ("address", "uint256"), (target.fund.address, token_id)
        )
    node.rpc.send(signer, to_checksum_address(target.contracts.position_manager), data)
    source = StrategyDefinition.read(ROOT / "strategies/uniswap_v4_lp/strategy.py")
    state = UniswapLifecycle.initial(config, source, declared(config), manifest)
    block = reader.pin_block("latest")
    observed = snapshot(
        SimpleNamespace(config=config, reader=reader), "position", block, state.owned[0]
    )
    intent = CollectLiquidityFees[Target](target=target, deadline=block.timestamp + 300)
    at = datetime.fromtimestamp(block.timestamp, UTC)
    decision = DecisionRecord(
        trigger=IntervalEvent(started_at=at, due_at=at),
        identity="ab" * 16,
        intent=intent,
        observations=(observed,),
        memory=state.memory,
    )
    state = state.model_copy(update={"decision": decision, "initialized": True})
    from almanak.caip import AssetId

    unrelated = AssetId(
        f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}/{token_id}"
    )
    with pytest.raises(ValueError, match="retained strategy NFT"):
        validate_operation(
            backend,
            state,
            Collect(target=target, nft=unrelated, deadline=intent.deadline),
            observed,
        )
    market_trade(node, target, reader)
    receipt = send(node, config, operator, abi.collect(target, token_id, intent.deadline))
    closed = send(node, config, operator, abi.close(target, token_id, 0, 0, intent.deadline))
    assert receipt["status"] == closed["status"] == "0x1"
    verify(config, manifest, backend, connector=UniswapPermissions())
    retain(
        f"uniswap-broad-exit-{authority}.json",
        {"token_id": str(unrelated), "collect": receipt, "close": closed},
    )
