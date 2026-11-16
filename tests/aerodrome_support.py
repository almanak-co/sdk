"""Decode independently captured direct calls into typed SDK operation inputs."""

import gzip
import json

from eth_abi import decode

from almanak.caip import AssetType
from almanak.connectors.aerodrome.domain import AerodromePool, Contracts
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.operations import (
    Approval,
    Close,
    Collect,
    Decrease,
    Increase,
    Mint,
    Swap,
)
from almanak.models import Account, AssetAmount
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/aerodrome-base-discovery")
CAPTURE = json.loads(gzip.decompress((EVIDENCE / "operations.json.gz").read_bytes()))


def target(row, step):
    fund = Account.model_validate_json(json.dumps(step["call"]["fund"]))
    market = row["market"]
    source = AerodromePool(
        kind="aerodrome",
        chain=fund.chain,
        factory=row["roles"]["factory"],
        pool=market["pool"],
        currency0=AssetType(market["tokens"][0]["identifier"]),
        currency1=AssetType(market["tokens"][1]["identifier"]),
        stable=market["stable"],
    )
    return Target(
        allocation="pool",
        account="fund",
        fund=fund,
        pool=source,
        contracts=Contracts(router=row["roles"]["router"]),
        funding_asset=AssetType(row["funding"]),
        liquidity_enabled=True,
    )


def amount(asset, value):
    return AssetAmount(asset=asset, amount=value)


def operation(row, step):
    selected = target(row, step)
    payload = bytes.fromhex(step["call"]["data"][10:])
    kind = step["kind"]
    if kind == "approve":
        spender, value = decode(("address", "uint256"), payload)
        asset = AssetType(f"{selected.fund.chain.chain_id}/erc20:{step['call']['to']}")
        return Approval(target=selected, asset=asset, spender=spender, amount=value)
    if kind == "collect":
        assert not payload
        return Collect(target=selected)
    if kind == "swap":
        value, minimum, routes, recipient, deadline = decode(
            ("uint256", "uint256", "(address,address,bool,address)[]", "address", "uint256"),
            payload,
        )
        assert recipient == selected.fund.address and len(routes) == 1
        incoming = AssetType(f"{selected.fund.chain.chain_id}/erc20:{routes[0][0]}")
        outgoing = AssetType(f"{selected.fund.chain.chain_id}/erc20:{routes[0][1]}")
        return Swap(
            target=selected,
            amount=amount(incoming, value),
            minimum=amount(outgoing, minimum),
            deadline=deadline,
        )
    return liquidity(selected, step, payload)


def liquidity(selected, step, payload):
    kind = step["kind"]
    asset0, asset1 = selected.assets
    if kind in ("mint", "increase"):
        values = decode(
            (
                "address",
                "address",
                "bool",
                "uint256",
                "uint256",
                "uint256",
                "uint256",
                "address",
                "uint256",
            ),
            payload,
        )
        constructor = Mint if kind == "mint" else Increase
        return constructor(
            target=selected,
            maximum0=amount(asset0, values[3]),
            maximum1=amount(asset1, values[4]),
            minimum0=amount(asset0, values[5]),
            minimum1=amount(asset1, values[6]),
            minimum_shares=amount(selected.pool.shares_asset, 1),
            deadline=values[8],
        )
    values = decode(
        ("address", "address", "bool", "uint256", "uint256", "uint256", "address", "uint256"),
        payload,
    )
    constructor = Close if kind == "close" else Decrease
    return constructor(
        target=selected,
        shares=amount(selected.pool.shares_asset, values[3]),
        minimum0=amount(asset0, values[4]),
        minimum1=amount(asset1, values[5]),
        deadline=values[7],
    )


def parameters(invariant, *, lp=False, fund=None):
    from almanak.configuration.yaml import load
    from almanak.connectors.aerodrome.models import Parameters
    from almanak.models import AccountBinding, Eoa, StrategyAccounts
    from scripts.legacy_configuration.author import normalize
    from tests.legacy_references import harness_directory

    style = "lp" if lp else "trading"
    raw = load(harness_directory(f"aerodrome_base_{invariant}_{style}") / "config.yaml").value
    raw.pop("execution")
    author = normalize(raw)
    if fund is None:
        address = "12" if invariant == "stable" else "34"
        fund = Account(chain=author.allocations[0].chain, address="0x" + address * 20)
    return Parameters(
        decision_trigger=author.decision_trigger,
        asset_metadata=author.asset_metadata,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=(AccountBinding(name="fund", profile=Eoa(kind="eoa", fund=fund)),)
        ),
    )
