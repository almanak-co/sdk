"""Direct operator actions on an Aave V3-style Pool, shared by its compatible connectors.

The deployment covers the account's whole risk position in the Pool. Every reserve the
Pool's user bitmap marks as collateral or debt becomes a position beside the requested
one: the connector's risk check refuses a bitmap it cannot fully account for, and a
supply is always risk-checked.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet

# Two bits per reserve ID in the Pool's user bitmap: debt, then collateral.
DEBT, COLLATERAL = 1, 2


def reserve_flags(bitmap: int, reserve_id: int) -> int:
    return (bitmap >> (2 * reserve_id)) & 3


def held_reserves(reader, block, pool: str, account: str) -> dict[str, int]:
    """Each reserve asset the bitmap marks as collateral or debt, with its two flags."""
    bitmap = reader.call(
        pool,
        "getUserConfiguration(address)",
        block,
        inputs=("address",),
        args=(account,),
        outputs=("(uint256)",),
    )[0][0]
    (listed,) = reader.call(pool, "getReservesList()", block, outputs=("address[]",))
    return {
        asset.lower(): reserve_flags(bitmap, reserve_id)
        for reserve_id, asset in enumerate(listed)
        if reserve_flags(bitmap, reserve_id)
    }


def refuse_unbounded_risk(request: ActionRequest, flags: dict[str, int], requested: str) -> None:
    """Refuse the actions a Pool risk-checks while the account carries debt.

    A direct action states no health floor, and without one the risk check tolerates no
    debt at all. Saying so here names the cause; the planning refusal would only report
    a health factor below its minimum.
    """
    if not any(value & DEBT for value in flags.values()):
        return
    releases = request.action == "withdraw" and flags.get(requested, 0) & COLLATERAL
    if request.action == "supply" or releases:
        raise ValueError(
            f"the wallet has debt in this Pool: a {request.action} there is risk-checked "
            "against a health floor, and a direct action states none"
        )


def allocation(connector: str, reader, block, resolved, wallet: Wallet, name: str, flags: int):
    """One position of the wallet in the Pool, with the price source the Pool uses now."""
    from almanak.chains import evm_network
    from almanak.connectors.lending_lookup import family, pool_sources

    sources, _ = pool_sources(reader, family(connector), resolved, block)
    return {
        "name": name,
        "account": wallet.account,
        "reserve": resolved.reserve.model_dump(mode="json"),
        # Authority only: the Pool itself enables a first supply as collateral when the
        # reserve has an LTV, and a held reserve keeps the flag it has.
        "collateral": name == ALLOCATION or bool(flags & COLLATERAL),
        "oracle": sources["oracle"],
        "price_source": sources["price_source"],
        "native_asset": str(evm_network(resolved.selection.chain).native_asset),
        "minimum_native_balance": 0,
    }


def held_market(connector: str, reader, block, requested, asset: str):
    """Resolve another reserve of the requested reserve's Pool, as the connector does."""
    from almanak.connectors.lending_lookup import family, resolve, selection

    chosen = requested.selection
    values = {"chain": str(chosen.chain), "asset": f"{chosen.chain}/erc20:{asset}"}
    if chosen.pool is not None:
        values["pool"] = chosen.pool
    return resolve(reader, family(connector), selection(connector, values), block)


def positions(connector: str, reader, block, resolved, wallet: Wallet, request: ActionRequest):
    from almanak.gateway.evm import erc20_address

    reserve = resolved.reserve
    requested = erc20_address(reserve.asset, resolved.selection.chain).lower()
    flags = held_reserves(reader, block, reserve.pool, wallet.address)
    refuse_unbounded_risk(request, flags, requested)
    entries = [(ALLOCATION, resolved, flags.get(requested, 0))]
    entries += [
        (f"held_{index}", held_market(connector, reader, block, resolved, asset), flags[asset])
        for index, asset in enumerate(sorted(asset for asset in flags if asset != requested))
    ]
    return {
        "connector": connector,
        "allocations": [
            allocation(connector, reader, block, market, wallet, name, value)
            for name, market, value in entries
        ],
    }


def intent(config, observed, request: ActionRequest):
    if request.action == "supply":
        if request.amount is None:
            raise ValueError("supply requires --amount")
        return config.supply(ALLOCATION, request.amount)
    if request.amount is not None:
        raise ValueError("withdraw takes no --amount: it withdraws the whole supplied balance")
    return config.withdraw(ALLOCATION)


def position(state) -> dict:
    owned = next(item for item in state.owned if item.target.allocation == ALLOCATION)
    return {
        "supplied_scaled": str(owned.supply_scaled),
        "debt_scaled": str(owned.debt_scaled),
        "collateral_enabled": owned.collateral_enabled,
    }
