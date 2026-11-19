"""Direct operator actions on an Aave V3-style Pool, shared by its compatible connectors.

The deployment covers the account's whole risk position in the Pool. Every reserve the
Pool's user bitmap marks as collateral or debt becomes a position beside the requested
one: the connector's risk check refuses a bitmap it cannot fully account for, and a
supply is always risk-checked.

One health floor serves the whole Pool, as the connector requires of a fund's
allocations there. It is the operator's `--min-health-factor`; without one the risk
check tolerates no debt at all.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet

# Two bits per reserve ID in the Pool's user bitmap: debt, then collateral.
DEBT, COLLATERAL = 1, 2


def reserve_flags(bitmap: int, reserve_id: int) -> int:
    return (bitmap >> (2 * reserve_id)) & 3


def held_reserves(reader, block, pool: str, account: str) -> dict[str, int]:
    """Each reserve asset the bitmap marks as collateral or debt, with its two flags.

    A reserve is named by its ID. The Pool's reserve list is no index of IDs: it closes
    the gap a dropped reserve leaves.
    """
    bitmap = reader.call(
        pool,
        "getUserConfiguration(address)",
        block,
        inputs=("address",),
        args=(account,),
        outputs=("(uint256)",),
    )[0][0]
    held = {}
    for reserve_id in range((bitmap.bit_length() + 1) // 2):
        flags = reserve_flags(bitmap, reserve_id)
        if flags:
            held[reserve_asset(reader, block, pool, reserve_id)] = flags
    return held


def reserve_asset(reader, block, pool: str, reserve_id: int) -> str:
    (asset,) = reader.call(
        pool,
        "getReserveAddressById(uint16)",
        block,
        inputs=("uint16",),
        args=(reserve_id,),
        outputs=("address",),
    )
    return asset.lower()


# The debt ceiling of a reserve's configuration word: 40 bits from bit 212. A reserve
# with one is isolated collateral.
DEBT_CEILING = (212, 40)


def isolated(reader, block, pool: str, asset: str) -> bool:
    raw = reader.call(
        pool,
        "getConfiguration(address)",
        block,
        inputs=("address",),
        args=(asset,),
        outputs=("(uint256)",),
    )[0][0]
    start, width = DEBT_CEILING
    return bool((raw >> start) & ((1 << width) - 1))


def refuse_isolation(reader, block, pool: str, requested: str, flags: dict[str, int]) -> None:
    """Refuse a supply the Pool may not enable as collateral the way a first supply is.

    The connector accounts a first supply into a reserve with an LTV as collateral. The
    Pool does not enable it when the reserve is isolated collateral, or when the account
    already holds isolated collateral. The supply would then be mined and its outcome
    could not be reconciled, which leaves the wallet claimed by a record nothing resolves.
    """
    if isolated(reader, block, pool, requested):
        raise ValueError(
            "this reserve is isolated collateral in its Pool (it has a debt ceiling): "
            "a direct supply serves ordinary reserves only"
        )
    for asset, value in flags.items():
        if value & COLLATERAL and asset != requested and isolated(reader, block, pool, asset):
            raise ValueError(
                f"the wallet holds isolated collateral ({asset}) in this Pool: a direct "
                "supply beside it is not served"
            )


def risk_checked(request: ActionRequest, flags: dict[str, int], requested: str) -> bool:
    """Whether the Pool risk-checks the request against the account's health floor.

    The connector's own rule: a supply, a borrow and enabling collateral always; with
    debt open, also disabling collateral and withdrawing a reserve used as collateral.
    """
    if request.action in ("supply", "borrow"):
        return True
    indebted = any(value & DEBT for value in flags.values())
    if request.action == "collateral":
        return bool(request.enable) or indebted
    releases = bool(flags.get(requested, 0) & COLLATERAL)
    return request.action == "withdraw" and indebted and releases


def require_floor(request: ActionRequest, flags: dict[str, int], requested: str) -> None:
    """Refuse a risk-checked action on an account with debt that states no health floor.

    Without a floor the risk check tolerates no debt at all. Saying so here names the
    cause; the planning refusal would only report a health factor below its minimum.
    """
    owed = sorted(asset for asset, value in flags.items() if value & DEBT)
    if owed and request.min_health_factor is None and risk_checked(request, flags, requested):
        raise ValueError(
            f"the wallet has debt in this Pool ({', '.join(owed)}): a {request.action} there "
            "is risk-checked against a health floor; state --min-health-factor"
        )


# Repay authority belongs to an allocation that may borrow, which must state a floor. A
# repayment is never risk-checked, so the lowest valid floor is stated for it: no floor
# can then refuse a repayment.
REPAY_FLOOR = 10**18 + 1


def health_floor(request: ActionRequest) -> int | None:
    if request.min_health_factor is None and request.action == "repay":
        return REPAY_FLOOR
    return request.min_health_factor


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


def with_policy(entry: dict, request: ActionRequest) -> dict:
    """Add the request's borrow authority and the Pool's one health floor to a position."""
    floor = health_floor(request)
    if entry["name"] == ALLOCATION and request.action in ("borrow", "repay"):
        entry = {**entry, "borrow": True}
    return entry if floor is None else {**entry, "minimum_health_factor": floor}


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
    require_floor(request, flags, requested)
    entries = [(ALLOCATION, resolved, flags.get(requested, 0))]
    if request.action == "supply":
        refuse_isolation(reader, block, reserve.pool, requested, flags)
    if risk_checked(request, flags, requested):
        # Only an action the Pool risk-checks needs the other reserves: binding them for
        # a repayment or a plain withdrawal would let one the connector cannot verify
        # block an unrelated exit.
        others = sorted(asset for asset in flags if asset != requested)
        entries += [
            (f"held_{index}", held_market(connector, reader, block, resolved, asset), flags[asset])
            for index, asset in enumerate(others)
        ]
    return {
        "connector": connector,
        "allocations": [
            with_policy(allocation(connector, reader, block, market, wallet, name, value), request)
            for name, market, value in entries
        ],
    }


def intent(config, observed, request: ActionRequest):
    """The request as the Pool connector's intent; argparse has settled which flags it has."""
    amount = request.amount
    if request.action == "collateral":
        if request.enable is None:
            raise ValueError("collateral requires --enable or --disable")
        return config.set_collateral(ALLOCATION, request.enable)
    if request.action in ("supply", "borrow"):
        if amount is None:
            raise ValueError(f"{request.action} requires --amount")
        return getattr(config, request.action)(ALLOCATION, amount)
    if amount is None:
        return getattr(config, request.action)(ALLOCATION)
    return getattr(config, f"{request.action}_amount")(ALLOCATION, amount)


def position(state) -> dict:
    owned = next(item for item in state.owned if item.target.allocation == ALLOCATION)
    return {
        "supplied_scaled": str(owned.supply_scaled),
        "debt_scaled": str(owned.debt_scaled),
        "collateral_enabled": owned.collateral_enabled,
    }
