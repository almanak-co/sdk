"""Generated V4 exit grants reach foreign NFTs; retained gateway ownership refuses them."""

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from eth_utils import to_checksum_address

from almanak.caip import AssetId
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.math import liquidity_for
from almanak.connectors.uniswap_v4.observations import snapshot
from almanak.connectors.uniswap_v4.planning import operation
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.connectors.uniswap_v4.reader import sqrt_bounds, wallet_balance
from almanak.connectors.uniswap_v4.validation import validate_operation
from almanak.dex_intents import CollectLiquidityFees
from almanak.gateway.accounts import prepare_call
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.safe_fixture import calldata
from almanak.gateway.safe_funding import fund_usdc_storage
from almanak.models import Account, AssetAmount
from tests.fork.uniswap_support import minted_id
from tests.strategy_fork import harness

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def direct(node, signer, call):
    receipt = node.rpc.send(signer, to_checksum_address(call.to), call.data, call.value)
    return {"call": call.model_dump(mode="json"), "receipt": receipt}


def foreign_mint(node, reader, target):
    signer = node.funded_signer("", 0, 10**18, 42161463)
    fund_usdc_storage(node, signer.address, target.funding_currency.reference, 20_000_000)
    other = target.model_copy(update={"fund": Account(chain=reader.chain, address=signer.address)})
    deadline = reader.pin_block("latest").timestamp + 300
    amount = AssetAmount(asset=target.funding_currency, amount=10_000_000)
    records = [
        direct(node, signer, call)
        for call in (
            abi.approve(other, amount.asset, 20_000_000),
            abi.permit(other, amount.asset, other.contracts.router, amount.amount, deadline),
            abi.swap(other, amount, 1, deadline),
        )
    ]
    block = reader.pin_block("latest")
    balances = tuple(wallet_balance(reader, other, asset, block) for asset in target.assets)
    pool = read_pool(reader, target.pool, block)
    liquidity = liquidity_for(
        *(balance * 99 // 100 for balance in balances), pool.sqrt_price_x96, *sqrt_bounds(target)
    )
    for asset, balance in zip(target.assets, balances, strict=True):
        records.append(direct(node, signer, abi.approve(other, asset, balance)))
        records.append(
            direct(
                node,
                signer,
                abi.permit(other, asset, other.contracts.position_manager, balance, deadline),
            )
        )
    minted = direct(node, signer, abi.mint(other, liquidity, *balances, deadline))
    records.append(minted)
    return signer, minted_id(minted["receipt"], other), records


def grant_nft(node, signer, target, token_id, authority):
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
    return node.rpc.send(signer, to_checksum_address(target.contracts.position_manager), data)


def gateway_refuses(reader, state, token_id):
    target = state.owned[0].target
    backend = SimpleNamespace(config=state.config, reader=reader)
    before = snapshot(backend, target.allocation, reader.pin_block("latest"), state.owned[0])
    intent = CollectLiquidityFees[type(target)](
        target=target, deadline=before.block.timestamp + 300
    )
    decision = state.outcomes[-1].decision.model_copy(
        update={"intent": intent, "observations": (before,)}
    )
    retained = state.model_copy(update={"decision": decision})
    planned = operation(state.config, retained, intent, before)
    validate_operation(backend, retained, planned, before)
    nft = AssetId(
        f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}/{token_id}"
    )
    assert planned.nft != nft
    forged = planned.model_copy(update={"nft": nft})
    with pytest.raises(ValueError, match="retained economic intent or NFT"):
        validate_operation(backend, retained, forged, before)
    return {"retained": planned.model_dump(mode="json"), "refused": forged.model_dump(mode="json")}


def exit_probes(node, state, token_id):
    target = state.owned[0].target
    deadline = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16) + 300
    records = []
    for call in (
        abi.collect(target, token_id, deadline),
        abi.close(target, token_id, 0, 0, deadline),
    ):
        wrapped = prepare_call(state.config.accounts, target.account, call)
        profile = state.config.accounts.binding(target.account).profile
        request = {"from": profile.submitter.address, "to": wrapped.to, "data": wrapped.data}
        result = node.rpc("eth_call", [request, "latest"])
        records.append({"call": request, "result": result})
    return records


def prove_foreign(node, state, directory):
    target = state.owned[0].target
    rows = []
    with EthereumReader(target.fund.chain, node.rpc.endpoint) as reader:
        for authority in ("owns", "approved"):
            signer, token_id, mint = foreign_mint(node, reader, target)
            grant = grant_nft(node, signer, target, token_id, authority)
            refused = gateway_refuses(reader, state, token_id)
            calls = exit_probes(node, state, token_id)
            rows.append(
                {
                    "authority": authority,
                    "token_id": token_id,
                    "mint": mint,
                    "grant": grant,
                    "gateway": refused,
                    "read_only_exits": calls,
                }
            )
    (directory / "foreign-nft-authority.json").write_text(json.dumps(rows, indent=2) + "\n")


def test_generated_arbitrum_exits_and_gateway_foreign_nft_refusal(request, tmp_path, monkeypatch):
    case = next(
        c
        for c in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == "uniswap_v4_arbitrum_lp"
    )
    load = harness.load_assertions
    checked = []

    def assertions(selected):
        value = load(selected)
        holding = value.assert_holding

        def check(node, state):
            holding(node, state)
            if not checked:
                prove_foreign(node, state, tmp_path)
                checked.append(True)

        value.assert_holding = check
        return value

    monkeypatch.setattr(harness, "load_assertions", assertions)
    artifact = ROOT / "artifacts/uniswap-v4-arbitrum-foreign-nft" / uuid4().hex
    artifact.mkdir(parents=True, exist_ok=False)
    try:
        harness.run_case(
            case,
            request.config.getoption("--fork-env").resolve(),
            {},
            tmp_path / "workspace",
            artifact / "evidence",
        )
        assert checked == [True]
    finally:
        proof = tmp_path / "foreign-nft-authority.json"
        if proof.exists():
            (artifact / proof.name).write_bytes(proof.read_bytes())
