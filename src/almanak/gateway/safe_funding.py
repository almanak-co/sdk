"""Explicit simulated capital for fork Safes and their independent gas payers."""

import json
from dataclasses import dataclass
from pathlib import Path

from eth_utils import keccak, to_checksum_address

from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.gateway.evm import erc20_address
from almanak.gateway.fork_funding import (
    balance,
    fund_erc20,
    require_code,
    set_storage,
    total_supply,
    transfer_data,
)
from almanak.gateway.morpho_avalanche_fixture import fund_stavax_storage
from almanak.gateway.roles_storage import mapping_slot
from almanak.gateway.safe_fixture import deploy_profile
from almanak.models import AccountBinding


def prepare_safe_accounts(node, config, chain):
    operators = {
        item.name: node.funded_signer("", 0, item.native_balance.amount, item.foundry_account_index)
        for item in config.operators
    }
    node.fixture_owners = {}
    bindings, keys, evidence = [], {}, []
    for index, fund in enumerate(config.funds):
        wrapping = native_wrapping_balance(fund)
        owner = node.funded_signer("", 0, 10**18 + wrapping, 1_000_000_000 + index)
        operator = operators[fund.operator]
        profile, receipts = deploy_profile(node.rpc, chain, owner, operator, fund.account)
        node.fixture_owners[fund.account] = owner
        bindings.append(AccountBinding(name=fund.account, profile=profile))
        keys[fund.account] = operator.key.hex()
        evidence.extend(receipts)
        for asset in fund.assets:
            evidence.append(fund_asset(node, owner, profile.fund.address, asset))
    return bindings, keys, evidence


def native_wrapping_balance(fund):
    return sum(a.balance.amount for a in fund.assets if a.method == "wrap_native")


def fund_asset(node, owner, safe, funding):
    asset = funding.balance.asset
    token = erc20_address(asset, node.origin.chain.chain_id)
    amount = funding.balance.amount
    require_code(node, token)
    if balance(node.rpc, token, safe) != 0:
        raise ValueError("fresh fixture Safe has unexpected initial capital")
    if funding.method == "wrap_native":
        fund_wrapped(node, owner, safe, funding)
        route = {"method": "wrap_native"}
    else:
        route = fund_erc20(node, safe, asset, amount, reviewed_funder(asset))
    if balance(node.rpc, token, safe) != amount:
        raise ValueError("simulated initial capital does not match the declared token balance")
    return {
        "kind": "simulated_initial_capital",
        "fund": safe,
        "origin_asset": str(asset),
        "amount": amount,
        **route,
        "block": int(node.rpc("eth_blockNumber"), 16),
    }


USDC_STORAGE_ASSETS = {
    ChainId(chain): AssetType(f"{chain}/erc20:{token}")
    for chain, token in {
        "eip155:1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "eip155:43114": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
        "eip155:8453": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "eip155:42161": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "eip155:137": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
        "eip155:10": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
    }.items()
}


def reviewed_funder(asset):
    return {
        **dict.fromkeys(USDC_STORAGE_ASSETS.values(), fund_usdc_storage),
        AssetType(
            "eip155:8453/erc20:0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4"
        ): fund_eusd_storage,
        AssetType(
            "eip155:56/erc20:0x55d398326f99059ff775485246999027b3197955"
        ): fund_bnb_usdt_storage,
        AssetType(
            "eip155:56/erc20:0x85375d3e9c4a39350f1140280a8b0de6890a40e7"
        ): fund_bnb_sigma_storage,
        AssetType(
            "eip155:137/erc20:0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"
        ): fund_polygon_weth_storage,
        AssetType(
            "eip155:43114/erc20:0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab"
        ): fund_avalanche_weth_storage,
        AssetType(
            "eip155:43114/erc20:0xa25eaf2906fa1a3a13edac9b9657108af7b703e3"
        ): fund_stavax_storage,
        AssetType(
            "eip155:4663/erc20:0x5fc5360d0400a0fd4f2af552add042d716f1d168"
        ): fund_robinhood_usdg_storage,
        AssetType(
            "eip155:4663/erc20:0x18717dd0fea68fbe42c842c73c6c012c2e9ec226"
        ): fund_pusd_storage,
        AssetType(
            "eip155:4663/erc20:0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec"
        ): fund_stock_storage,
    }.get(asset)


def fund_wrapped(node, owner, fund, funding):
    asset, amount = funding.balance.asset, funding.balance.amount
    if asset != evm_network(node.origin.chain.chain_id).wrapped_native:
        raise ValueError("wrapped funding has no verified implementation for this asset")
    token = to_checksum_address(erc20_address(asset, node.origin.chain.chain_id))
    node.wrap_native(owner, token, amount)
    if owner.address.lower() != fund:
        node.rpc.send(owner, token, transfer_data(fund, amount))


def fund_usdc_storage(node, safe, token, amount):
    node._validate_origin()
    expected = USDC_STORAGE_ASSETS.get(node.origin.chain.chain_id)
    if (
        expected is None
        or token != erc20_address(expected, node.origin.chain.chain_id)
        or amount >= 2**255
    ):
        raise ValueError("local storage funding supports the pinned USDC balance layout only")
    verify_funding_layout(node, token)
    set_balance_slot(node, safe, token, amount, 9)


def fund_eusd_storage(node, fund, token, amount):
    # The captured OptimismMintableERC20 compiler layout puts _balances at slot 0.
    # This non-proxy runtime includes the token's bridge and remote-token immutables.
    fund_verified_slot(node, fund, token, amount, EUSD_LAYOUT)


def set_balance_slot(node, fund, token, amount, layout_slot):
    set_storage(node, token, mapping_slot("address", fund, layout_slot), amount)


@dataclass(frozen=True)
class VerifiedSlot:
    """A non-proxy ERC-20 runtime, pinned by hash, whose balances mapping is one slot."""

    chain: ChainId
    token: str
    runtime_hash: str
    slot: int
    deployment_refusal: str
    runtime_refusal: str


def fund_verified_slot(node, fund, token, amount, layout):
    node._validate_origin()
    if (
        node.origin.chain.chain_id != layout.chain
        or token != layout.token
        or type(amount) is not int
        or not 0 <= amount < 2**255
    ):
        raise ValueError(layout.deployment_refusal)
    code = node.rpc("eth_getCode", [token, "latest"])
    if keccak(bytes.fromhex(code[2:])).hex() != layout.runtime_hash:
        raise ValueError(layout.runtime_refusal)
    set_balance_slot(node, fund, token, amount, layout.slot)


EUSD_LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:8453"),
    token="0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4",
    runtime_hash="68c96f42193c4dcd133a8f38dd6c6b71c318d7b6f72de71f9fc3d72fa89bde8b",
    slot=0,
    deployment_refusal="eUSD fixture funding requires its verified Base deployment",
    runtime_refusal="eUSD fixture runtime differs from its verified slot-0 balance layout",
)

POLYGON_WETH_LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:137"),
    token="0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
    runtime_hash="06025eef5a4b1b3cb0031bce61d6a571f478e213d24984621eafcb59b8d71473",
    slot=0,
    deployment_refusal="Polygon WETH fixture requires its verified bridged ERC-20 deployment",
    runtime_refusal="Polygon WETH runtime differs from its verified balance layout",
)

BNB_USDT_LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:56"),
    token="0x55d398326f99059ff775485246999027b3197955",
    runtime_hash="97a48aa4c129657440dafdacd4c836389734d28cc4a0ca7403e68da660a74a59",
    slot=1,
    deployment_refusal="USDT fixture funding requires its verified BNB deployment",
    runtime_refusal="USDT fixture runtime differs from its verified slot-1 balance layout",
)

AVALANCHE_WETH_LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:43114"),
    token="0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab",
    runtime_hash="90222f4e07309adbb5d5a4b033fc7308b3ac8ff20db7f5c43d56bc2bf5b513d2",
    slot=0,
    deployment_refusal="WETH.e fixture funding requires its verified Avalanche deployment",
    runtime_refusal="WETH.e fixture runtime differs from its verified balance layout",
)

BNB_SIGMA_LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:56"),
    token="0x85375d3e9c4a39350f1140280a8b0de6890a40e7",
    runtime_hash="b22cddb2e977055088ba534372f67097686a0ed4ca49e031be12660967aeb660",
    slot=0,
    deployment_refusal="Sigma fixture funding requires its verified BNB deployment",
    runtime_refusal="Sigma fixture runtime differs from its verified slot-0 balance layout",
)

PUSD_LAYOUT = VerifiedSlot(
    chain=ChainId("eip155:4663"),
    token="0x18717dd0fea68fbe42c842c73c6c012c2e9ec226",
    runtime_hash="0f8dd576ed053f804c9dc4d8518e6415a632f68d211862bf5de550378e82101b",
    slot=0,
    deployment_refusal="pUSD fixture funding requires its verified Robinhood deployment",
    runtime_refusal="pUSD fixture runtime differs from its verified slot-0 balance layout",
)


def verify_funding_layout(node, token):
    from almanak.gateway.ethereum import EthereumReader
    from almanak.gateway.roles_storage import AuthorityReader

    filename = {
        "eip155:1": "ethereum-25908472-authority.json",
        "eip155:43114": "avalanche-94581397-authority.json",
        "eip155:8453": "base-50972164-authority.json",
        "eip155:42161": "arbitrum-502893237-authority.json",
        "eip155:137": "polygon-93426232-authority.json",
        "eip155:10": "optimism-curve-authority.json",
    }[str(node.origin.chain.chain_id)]
    artifact = json.loads((Path(__file__).with_name("artifacts") / filename).read_text())
    expected = next(c for c in artifact["contracts"] if c["address"] == token)
    chain = node.origin.chain.model_copy(
        update={"environment": "anvil:" + node.metadata["instanceId"]}
    )
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        reader.transport_chain = node.rpc.signing_chain
        identity = AuthorityReader(reader, reader.pin_block("latest")).identity(token)
    if identity.model_dump(mode="json") != expected:
        raise ValueError("USDC fixture implementation differs from verified slot-9 balance layout")


def fund_polygon_weth_storage(node, fund, token, amount):
    # MaticWETH is a bridged ERC-20 with _balances in compiler storage slot 0.
    fund_verified_slot(node, fund, token, amount, POLYGON_WETH_LAYOUT)


def fund_bnb_usdt_storage(node, fund, token, amount):
    # BEP20USDT's Solidity 0.5.16 compiler layout places _balances after _owner.
    fund_verified_slot(node, fund, token, amount, BNB_USDT_LAYOUT)


def fund_avalanche_weth_storage(node, fund, token, amount):
    # The independently compiled BridgeToken layout places ERC20 balances at slot 0.
    fund_verified_slot(node, fund, token, amount, AVALANCHE_WETH_LAYOUT)


def fund_bnb_sigma_storage(node, fund, token, amount):
    # Shadow's Solidity 0.8.26 compiler layout retains ERC20 balances at slot 0.
    fund_verified_slot(node, fund, token, amount, BNB_SIGMA_LAYOUT)


def fund_pusd_storage(node, fund, token, amount):
    # The exact non-proxy pUSD/OpenZeppelin compilation places _balances at slot 0.
    fund_verified_slot(node, fund, token, amount, PUSD_LAYOUT)


def fund_stock_storage(node, fund, token, amount):
    from almanak.gateway.robinhood_stock import BALANCES_NAMESPACE

    validate_stock_funding(node, token, amount)
    previous, supply = stock_funding_state(node, fund, token)
    supply += amount - previous
    if not 0 <= supply < 2**256:
        raise ValueError("Stock fixture supply would exceed the compiled uint256 layout")
    set_balance_slot(node, fund, token, amount, BALANCES_NAMESPACE)
    set_storage(node, token, BALANCES_NAMESPACE + 2, supply)
    if balance(node.rpc, token, fund) != amount or total_supply(node.rpc, token) != supply:
        raise ValueError("Stock fixture capital or total supply differs from its declared amount")


def validate_stock_funding(node, token, amount):
    node._validate_origin()
    if (
        node.origin.chain.chain_id != ChainId("eip155:4663")
        or token != "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec"
        or type(amount) is not int
        or not 0 <= amount < 2**255
    ):
        raise ValueError("Stock fixture funding requires its verified Robinhood deployment")


def stock_funding_state(node, fund, token):
    from almanak.gateway.ethereum import EthereumReader
    from almanak.gateway.robinhood_stock import PROXY_HASH, STOCK_HASH
    from almanak.gateway.roles_storage import AuthorityReader

    chain = node.origin.chain.model_copy(
        update={"environment": "anvil:" + node.metadata["instanceId"]}
    )
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        reader.transport_chain = node.rpc.signing_chain
        block = reader.pin_block("latest")
        observed = AuthorityReader(reader, block).identity(token)
        if observed.code_hash != PROXY_HASH or observed.implementation_hash != STOCK_HASH:
            raise ValueError("Stock fixture runtime differs from its verified balance layout")
        previous = reader.call(
            token, "balanceOf(address)", block, inputs=("address",), args=(fund,)
        )[0]
        supply = reader.call(token, "totalSupply()", block)[0]
    return previous, supply


def fund_robinhood_usdg_storage(node, fund, token, amount):
    node._validate_origin()
    if (
        node.origin.chain.chain_id != ChainId("eip155:4663")
        or token != "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
        or type(amount) is not int
        or not 0 <= amount < 2**64
    ):
        raise ValueError(
            "USDG fixture requires its verified Robinhood deployment and uint64 balance"
        )
    verify_robinhood_usdg_layout(node, token)
    verify_robinhood_usdg_account(node, token, fund)
    # Fresh payout-group-zero accounts have only the uint64 base balance in this packed slot.
    set_balance_slot(node, fund, token, amount, 1)


def verify_robinhood_usdg_layout(node, token):
    implementation = "0x68184c449e1a8f34fa18d289737129fd27b66f8f"
    pointer = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
    stored = node.rpc("eth_getStorageAt", [token, pointer, "latest"])
    if int(stored, 16) != int(implementation, 16):
        raise ValueError("USDG fixture implementation pointer differs from verified source")
    for contract, expected in (
        (token, "864cc9ad53b338b82da1f7cab85ab0b3d5c8861acb422b6fec63cf36234f36a6"),
        (implementation, "3a551ac5c744af57e68a1d1431ac403c0f516ffd7d224a75746aee11fc4f3baf"),
    ):
        code = node.rpc("eth_getCode", [contract, "latest"])
        if keccak(bytes.fromhex(code[2:])).hex() != expected:
            raise ValueError("USDG fixture runtime differs from the independently compiled layout")


def verify_robinhood_usdg_account(node, token, fund):
    balance_slot = mapping_slot("address", fund, 1)
    packed = node.rpc("eth_getStorageAt", [token, hex(balance_slot), "latest"])
    if int(packed, 16):
        raise ValueError("USDG fixture requires a fresh account with no balance or payout state")
    frozen_slot = mapping_slot("address", fund, 6)
    frozen = node.rpc("eth_getStorageAt", [token, hex(frozen_slot), "latest"])
    if int(frozen, 16):
        raise ValueError("USDG fixture cannot fund a frozen account")
    settings = node.rpc("eth_getStorageAt", [token, "0x4", "latest"])
    if (int(settings, 16) >> 160) & 255:
        raise ValueError("USDG fixture cannot fund while token transfers are paused")
