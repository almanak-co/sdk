"""Read-only burn probes on NFTs minted by the actual strategy."""

from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.chains import evm_network
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.math import accrued, amounts
from almanak.connectors.uniswap_v4.observations import snapshot
from almanak.connectors.uniswap_v4.reader import sqrt_bounds


def check_burn_minima(node, reader, state):
    owned = state.owned[0]
    target = owned.target
    observed = snapshot(
        SimpleNamespace(config=state.config, reader=reader),
        target.allocation,
        reader.pin_block("latest"),
        owned,
    )
    principal = amounts(
        owned.position.liquidity, observed.pool.sqrt_price_x96, *sqrt_bounds(target)
    )
    fees = tuple(
        accrued(owned.position.liquidity, old, current)
        for old, current in zip(owned.position.fee_growth, observed.fee_growth, strict=True)
    )
    currency = fee_currency(fees)
    accepted = call_burn(node, observed, principal)
    minimum = list(principal)
    minimum[currency] += 1
    error_data = (
        "0x"
        + (
            keccak(text="MinimumAmountInsufficient(uint128,uint128)")[:4]
            + encode(("uint128", "uint128"), (minimum[currency], principal[currency]))
        ).hex()
    )
    with pytest.raises(RuntimeError, match=error_data):
        call_burn(node, observed, minimum)
    return {
        "block": observed.block.model_dump(mode="json"),
        "nft": str(owned.position.nft),
        "principal": principal,
        "uncollected_fees": fees,
        "accepted_minima": principal,
        "accepted_result": accepted,
        "rejected_minima": minimum,
        "revert_data": error_data,
        "read_only_sender_balance_override": burn_balance_override(target),
    }


def fee_currency(fees):
    for currency, amount in enumerate(fees):
        if amount > 0:
            return currency
    raise AssertionError("bounded fee exercise produced no positive position fee growth")


def call_burn(node, observed, minima):
    target = observed.target
    call = abi.close(
        target,
        int(observed.owned.position.nft.token_id),
        *minima,
        observed.block.timestamp + 300,
    )
    parameters = [
        {
            "from": target.fund.address,
            "to": call.to,
            "data": call.data,
            "gasPrice": "0x0",
        },
        {"blockHash": observed.block.hash, "requireCanonical": True},
    ]
    override = burn_balance_override(target)
    if override:
        parameters.append(override)
    return node.rpc("eth_call", parameters)


def burn_balance_override(target):
    # OP eth_call retains L1 fee affordability checks. This protocol-minimum probe
    # changes no balances and does not model the transaction submitter gas budget.
    if evm_network(target.fund.chain.chain_id).family == "optimism":
        return {target.fund.address: {"balance": hex(10**18)}}
    return {}
