"""Native-currency V4 liquidity operations and PositionManager receipts, without provider access."""

from eth_abi import encode

from almanak.caip import AssetId
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.receipts import MODIFY, TRANSFER
from almanak.connectors.uniswap_v4.state import Burn, Collect, Mint
from almanak.models import ZERO_ADDRESS, AssetAmount
from tests.uniswap_native_support import FUND, liquidity_range, target, topic, transfer, wrap_log


def lp_target(name):
    return target(name, range=liquidity_range(name))


def nft(selected, token_id):
    manager = selected.contracts.position_manager
    return AssetId(f"{selected.fund.chain.chain_id}/erc721:{manager}/{token_id}")


def mint(name, liquidity=10**12, maximum0=5 * 10**16, maximum1=200 * 10**6):
    selected = lp_target(name)
    return Mint(
        target=selected,
        liquidity=liquidity,
        maximum0=AssetAmount(asset=selected.assets[0], amount=maximum0),
        maximum1=AssetAmount(asset=selected.assets[1], amount=maximum1),
        deadline=1,
    )


def collect(name, token_id=7):
    selected = lp_target(name)
    return Collect(target=selected, nft=nft(selected, token_id), deadline=1)


def burn(name, token_id=7, liquidity=10**12, minimum0=0, minimum1=0):
    selected = lp_target(name)
    return Burn(
        target=selected,
        nft=nft(selected, token_id),
        liquidity=liquidity,
        minimum0=AssetAmount(asset=selected.assets[0], amount=minimum0),
        minimum1=AssetAmount(asset=selected.assets[1], amount=minimum1),
        deadline=1,
    )


def nft_log(selected, sender, receiver, token_id):
    return {
        "address": selected.contracts.position_manager,
        "topics": [TRANSFER, topic(sender), topic(receiver), f"0x{token_id:064x}"],
        "data": "0x",
    }


def modify_log(selected, liquidity, token_id):
    bounds = selected.range
    data = encode(
        ("int24", "int24", "int256", "bytes32"),
        (bounds.lower, bounds.upper, liquidity, token_id.to_bytes(32, "big")),
    )
    return {
        "address": selected.pool.pool_manager,
        "topics": [MODIFY, pool_id(selected.pool), topic(selected.contracts.position_manager)],
        "data": "0x" + data.hex(),
    }


def mint_receipt(operation, token_id, native_used, token_used, dust=0):
    """Pull the native maximum as wrapped, unwrap, mint, pay, wrap and sweep the rest back."""
    selected = operation.target
    chain, manager = selected.pool.chain, selected.pool.pool_manager
    position_manager = selected.contracts.position_manager
    wrapped, token = selected.assets[0].reference, selected.assets[1].reference
    pulled = operation.maximum0.amount
    returned = pulled - native_used + dust
    return {
        "status": "0x1",
        "logs": [
            transfer(wrapped, FUND, manager, pulled),
            transfer(wrapped, manager, position_manager, pulled),
            wrap_log(chain, position_manager, pulled, wrapping=False),
            nft_log(selected, ZERO_ADDRESS, FUND, token_id),
            modify_log(selected, operation.liquidity, token_id),
            transfer(token, FUND, manager, token_used),
            wrap_log(chain, position_manager, returned, wrapping=True),
            transfer(wrapped, position_manager, FUND, returned),
        ],
    }


def exit_receipt(operation, native_out, token_out):
    """Collect or burn: the token to the fund, the native wrapped and swept to the fund."""
    selected = operation.target
    chain, manager = selected.pool.chain, selected.pool.pool_manager
    position_manager = selected.contracts.position_manager
    wrapped, token = selected.assets[0].reference, selected.assets[1].reference
    token_id = int(operation.nft.token_id)
    logs = []
    if isinstance(operation, Burn):
        logs.append(nft_log(selected, FUND, ZERO_ADDRESS, token_id))
    liquidity = -operation.liquidity if isinstance(operation, Burn) else 0
    logs += [
        modify_log(selected, liquidity, token_id),
        transfer(token, manager, FUND, token_out),
        wrap_log(chain, position_manager, native_out, wrapping=True),
        transfer(wrapped, position_manager, FUND, native_out),
    ]
    return {"status": "0x1", "logs": logs}
