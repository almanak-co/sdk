"""Broad Roles exits reach real foreign NFTs while gateway admission refuses them."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import decode
from eth_utils import to_checksum_address

from almanak.connectors.uniswap_v3 import abi, receipts
from almanak.connectors.uniswap_v3.fork import fork_allocation
from almanak.connectors.uniswap_v3.models import Target
from almanak.connectors.uniswap_v3.observations import initial_owned, snapshot
from almanak.connectors.uniswap_v3.operations import Close, Collect, Mint
from almanak.connectors.uniswap_v3.permissions import derive
from almanak.connectors.uniswap_v3.validation import validate_operation
from almanak.dex_intents import CollectLiquidityFees
from almanak.gateway.accounts import prepare_call
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.permissions import installation_bundle
from almanak.gateway.safe_fixture import deploy_profile, owner_transaction
from almanak.gateway.safe_funding import fund_usdc_storage
from almanak.models import Account, AccountBinding, AssetAmount, Chain, StrategyAccounts
from almanak.runtime import strategy_capabilities
from almanak.strategy import StrategyDefinition
from tests.fork.test_uniswap_v3_permissions import permission_node as permission_node
from tests.uniswap_v3_support import parameters

ROOT = Path(__file__).parents[2]
pytestmark = pytest.mark.fork


def prepare(node):
    original = parameters(lp=True)
    chain = Chain(
        chain_id=original.allocations[0].chain.chain_id,
        environment="anvil:" + node.metadata["instanceId"],
    )
    owner = node.funded_signer("", 0, 10**18, 83)
    operator = node.funded_signer("", 0, 10**18, 84)
    profile, _ = deploy_profile(node.rpc, chain, owner, operator, "fund")
    accounts = StrategyAccounts(bindings=(AccountBinding(name="fund", profile=profile),))
    config = original.model_copy(
        update={
            "accounts": accounts,
            "allocations": tuple(fork_allocation(a, chain) for a in original.allocations),
        }
    )
    source = StrategyDefinition.read(ROOT / "strategies/uniswap_v3_ethereum_lp/strategy.py")
    capability = strategy_capabilities(source, config)
    grants = derive(profile, capability)
    for call in installation_bundle(SimpleNamespace(profile=profile, grants=grants)):
        owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)
    target = config.target("position")
    fund_usdc_storage(
        node, profile.fund.address, target.assets[0].reference, config.allocations[0].capital.amount
    )
    return SimpleNamespace(
        node=node,
        profile=profile,
        operator=operator,
        config=config,
        capabilities=capability,
        target=target,
    )


def execute(node, signer, operation):
    call = abi.call(operation)
    return node.rpc.send(signer, to_checksum_address(call.to), call.data)


def foreign_position(fixture, reader):
    target = fixture.target
    signer = fixture.node.funded_signer(
        to_checksum_address(target.assets[1].reference), 10**16, 10**18, 85
    )
    fund_usdc_storage(fixture.node, signer.address, target.assets[0].reference, 100_000_000)
    other = target.model_copy(
        update={"fund": Account(chain=target.fund.chain, address=signer.address)}
    )
    maxima = tuple(
        AssetAmount(asset=a, amount=n)
        for a, n in zip(target.assets, (100_000_000, 10**16), strict=True)
    )
    for maximum in maxima:
        data = abi.calldata(
            "approve(address,uint256)",
            ("address", "uint256"),
            (target.contracts.position_manager, maximum.amount),
        )
        fixture.node.rpc.send(
            signer, to_checksum_address(maximum.asset.reference), "0x" + data.hex()
        )
    operation = Mint(
        target=other,
        maximum0=maxima[0],
        maximum1=maxima[1],
        minimum0=maxima[0].model_copy(update={"amount": 0}),
        minimum1=maxima[1].model_copy(update={"amount": 0}),
        minimum_liquidity=1,
        deadline=reader.pin_block("latest").timestamp + 300,
    )
    receipt = execute(fixture.node, signer, operation)
    effects = receipts.movement(receipt, operation)
    assert effects.nft is not None and effects.liquidity_delta > 0
    return signer, effects.nft, effects.liquidity_delta, receipt


def give_authority(fixture, signer, nft, authority):
    if authority == "owns":
        data = abi.calldata(
            "transferFrom(address,address,uint256)",
            ("address", "address", "uint256"),
            (signer.address, fixture.profile.fund.address, int(nft.token_id)),
        )
    else:
        data = abi.calldata(
            "approve(address,uint256)",
            ("address", "uint256"),
            (fixture.profile.fund.address, int(nft.token_id)),
        )
    return fixture.node.rpc.send(
        signer, to_checksum_address(fixture.target.contracts.position_manager), "0x" + data.hex()
    )


def refuse_gateway(fixture, reader, nft):
    owned = initial_owned(fixture.config, "position")
    block = reader.pin_block("latest")
    backend = SimpleNamespace(config=fixture.config, reader=reader)
    before = snapshot(backend, "position", block, owned, quotes=False)
    intent = CollectLiquidityFees[Target](target=fixture.target, deadline=block.timestamp + 300)
    state = SimpleNamespace(
        config=fixture.config,
        owned=(owned,),
        capabilities=fixture.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(before,)),
        outcomes=(),
    )
    operation = Collect(target=fixture.target, nft=nft, deadline=intent.deadline)
    with pytest.raises(ValueError, match="retained fund-owned NFT"):
        validate_operation(backend, state, operation, before)
    return operation


def through_roles(fixture, operation):
    call = prepare_call(fixture.config.accounts, "fund", abi.call(operation))
    receipt = fixture.node.rpc.send(fixture.operator, to_checksum_address(call.to), call.data)
    assert receipt["status"] == "0x1"
    return receipt


def reject_truncated_exit(fixture, operation):
    call = abi.call(operation)
    (calls,) = decode(("bytes[]",), bytes.fromhex(call.data[10:]))
    malformed = abi.calldata("multicall(bytes[])", ("bytes[]",), ((*calls[:-1], calls[-1][:-1]),))
    wrapped = prepare_call(
        fixture.config.accounts, "fund", call.model_copy(update={"data": "0x" + malformed.hex()})
    )
    with pytest.raises(RuntimeError, match="execution reverted"):
        fixture.node.rpc(
            "eth_call",
            [{"from": fixture.operator.address, "to": wrapped.to, "data": wrapped.data}, "latest"],
        )


@pytest.mark.parametrize("authority", ["owns", "approved"])
def test_foreign_nft_roles_exit_and_gateway_refusal(permission_node, authority):
    fixture = prepare(permission_node)
    with EthereumReader(fixture.target.fund.chain, fixture.node.rpc.endpoint) as reader:
        signer, nft, liquidity, mint = foreign_position(fixture, reader)
        authority_receipt = give_authority(fixture, signer, nft, authority)
        operation = refuse_gateway(fixture, reader, nft)
        collect = through_roles(fixture, operation)
        close_operation = Close(
            target=fixture.target,
            nft=nft,
            liquidity=liquidity,
            minimum0=AssetAmount(asset=fixture.target.assets[0], amount=0),
            minimum1=AssetAmount(asset=fixture.target.assets[1], amount=0),
            deadline=operation.deadline,
        )
        reject_truncated_exit(fixture, close_operation)
        close = through_roles(fixture, close_operation)
        cash = tuple(
            AssetAmount(asset=asset, amount=receipts.token_movement(close, fixture.target, asset))
            for asset in fixture.target.assets
        )
        assert any(value.amount > 0 for value in cash)
        if authority == "approved":
            with pytest.raises(DataError, match="burn from its retained fund"):
                receipts.movement(close, close_operation)
        else:
            receipts.movement(close, close_operation)
        path = ROOT / "artifacts/uniswap-v3-authority" / f"foreign-{authority}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "authority": authority,
                    "nft": str(nft),
                    "mint": mint,
                    "permission": authority_receipt,
                    "collect": collect,
                    "close": close,
                    "gateway_refused": True,
                    "truncated_exit_refused": True,
                    "safe_cash_received": [value.model_dump(mode="json") for value in cash],
                },
                indent=2,
            )
            + "\n"
        )
