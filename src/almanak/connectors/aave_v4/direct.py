"""Aave V4 direct operator actions: supply, full withdrawal, borrow and full repayment.

A borrow or a repayment covers the wallet's whole risk position on the Spoke. The
requested reserve is the one debt, and the other reserves the wallet holds supply shares
in are bound as its collateral. A supply is not collateral on a Spoke until it is
enabled: the borrow enables each bound reserve first, and a full repayment disables
each one after the debt is gone, so a later withdrawal finds no collateral flag.

A supply or a withdrawal binds only its own reserve. The connector projects health for
a borrow and for nothing else, so a withdrawal of collateral is refused while the wallet
owes anything on the Spoke. A reserve holds one position: the connector refuses a supply
or a withdrawal in a reserve the wallet owes when it plans the first step.
"""

from typing import NamedTuple

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


class Held(NamedTuple):
    """What the wallet holds in one reserve of a Spoke."""

    # Supply shares.
    supplied: int
    # Whether any drawn or premium debt is open.
    owes: bool
    collateral: bool


def held_reserves(reader, block, spoke: str, account: str) -> dict[int, Held]:
    """Each reserve of the Spoke the wallet supplies or owes, by reserve ID.

    A reserve that is enabled as collateral with nothing supplied backs nothing and is
    left out.
    """
    from almanak.connectors.aave_v4.debt import reserve_components

    (count,) = reader.call(spoke, "getReserveCount()", block)
    found = {}
    for reserve_id in range(count):
        debt, supplied = reserve_components(reader, spoke, reserve_id, account, block)
        owes = bool(debt["drawn_shares"] or debt["premium_shares"] or debt["premium_offset_ray"])
        if supplied or owes:
            found[reserve_id] = Held(supplied, owes, debt["collateral_enabled"])
    return found


def allocation(name: str, wallet: Wallet, reserve) -> dict:
    from almanak.chains import evm_network

    return {
        "name": name,
        "account": wallet.account,
        "reserve": reserve.model_dump(mode="json"),
        "native_asset": str(evm_network(reserve.chain.chain_id).native_asset),
    }


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The requested reserve, and for a borrow or a repayment the collateral beside it."""
    reserve = resolved.reserve
    if request.action in ("borrow", "repay"):
        held = held_reserves(reader, block, reserve.spoke, wallet.address)
        return debt_positions(reader, block, resolved, wallet, request, held)
    if request.action == "withdraw":
        require_released(held_reserves(reader, block, reserve.spoke, wallet.address), reserve)
    return {"allocations": [allocation(ALLOCATION, wallet, reserve)]}


def require_released(held: dict[int, Held], reserve) -> None:
    """Refuse to withdraw a reserve that backs an open debt.

    The Spoke admits any withdrawal that leaves the health factor at 1 or above, and no
    health floor applies to a withdrawal here, so nothing else would hold it back.
    """
    requested = held.get(reserve.reserve_id)
    if requested is None or not requested.collateral:
        return
    owed = sorted(reserve_id for reserve_id, item in held.items() if item.owes)
    if owed:
        raise ValueError(
            f"reserve {reserve.reserve_id} is collateral for the wallet's debt in reserve "
            f"{owed[0]} of this Spoke: repay that debt in full first, which releases the "
            "collateral"
        )


def backs_debt(reader, block, spoke: str, reserve_id: int) -> bool:
    """Whether the reserve's current collateral configuration gives a supply any weight."""
    from almanak.connectors.aave_v4.reader import read_dynamic_reserve_config, read_reserve_data

    key = read_reserve_data(reader, spoke, reserve_id, block).dynamic_config_key
    config = read_dynamic_reserve_config(reader, spoke, reserve_id, key, block)
    return config.collateral_factor_bps > 0


def require_one_debt(held: dict[int, Held], requested: int) -> None:
    """Refuse a borrow or a repayment while the wallet owes another reserve of the Spoke.

    The connector admits one debt reserve per account and Spoke, and the collateral a
    repayment releases with a second debt open would be held to no health floor.
    """
    other = sorted(i for i, item in held.items() if item.owes and i != requested)
    if other:
        raise ValueError(
            f"the wallet has debt in reserve {other[0]} of this Spoke, and Aave V4 serves "
            f"one debt reserve per wallet and Spoke: repay reserve {other[0]} in full first"
        )


def collateral_reserves(reader, block, reserve, held: dict[int, Held], request) -> list[int]:
    """The reserve IDs to bind as collateral of the requested debt, in ascending order.

    A repayment enables nothing: it binds every other supply and releases those that are
    collateral. A borrow enables what it binds, so it leaves a supply that can back
    nothing as it is.
    """
    requested = reserve.reserve_id
    require_one_debt(held, requested)
    supplied = sorted(i for i, item in held.items() if item.supplied and i != requested)
    if request.action == "repay":
        if not supplied:
            raise ValueError(
                f"the wallet supplies no reserve of this Spoke other than reserve {requested}: "
                "a repayment is bound with the collateral it releases, and there is none"
            )
        return supplied
    backing = [
        reserve_id
        for reserve_id in supplied
        if held[reserve_id].collateral or backs_debt(reader, block, reserve.spoke, reserve_id)
    ]
    if not backing:
        raise ValueError(
            "the wallet has supplied no collateral on this Spoke: supply a reserve with a "
            f"collateral factor, other than reserve {requested}, before borrowing"
        )
    return backing


def health_floor(request: ActionRequest) -> int:
    """The debt's health floor, at wad precision.

    Only a planned borrow is projected against the floor. A repayment, its approval and
    revocation, and the release of collateral after it are never projected, so a
    repayment that states no floor is given the lowest valid one, as on a Pool: no floor
    can refuse a repayment.
    """
    from almanak.connectors.pool_lending_direct import health_floor as stated

    floor = stated(request)
    if floor is None:
        raise ValueError("borrow requires --min-health-factor")
    return floor


def spoke_reserve(reader, block, resolved, reserve_id: int):
    """Another reserve of the requested reserve's Spoke, resolved as the connector does."""
    from almanak.connectors.lending_lookup import family, resolve, selection

    chosen = resolved.selection
    values = {"chain": str(chosen.chain), "spoke": chosen.spoke, "reserve_id": reserve_id}
    return resolve(reader, family("aave_v4"), selection("aave_v4", values), block).reserve


def debt_positions(reader, block, resolved, wallet: Wallet, request: ActionRequest, held) -> dict:
    """The requested debt with its collateral, the Spoke oracle and the sources it uses now."""
    from almanak.connectors.aave_v4.debt import reserve_source

    reserve = resolved.reserve
    backing = collateral_reserves(reader, block, reserve, held, request)
    floor = health_floor(request)
    names = [f"collateral_{reserve_id}" for reserve_id in backing]
    (oracle,) = reader.call(reserve.spoke, "ORACLE()", block, outputs=("address",))
    debt = {
        **allocation(ALLOCATION, wallet, reserve),
        "collateral": names,
        "minimum_health_factor": floor,
        "oracle": oracle,
        "collateral_price_sources": [
            reserve_source(reader, oracle, reserve_id, block) for reserve_id in backing
        ],
        "debt_price_source": reserve_source(reader, oracle, reserve.reserve_id, block),
    }
    return {
        "allocations": [
            allocation(name, wallet, spoke_reserve(reader, block, resolved, reserve_id))
            for name, reserve_id in zip(names, backing, strict=True)
        ],
        "debts": [debt],
    }


WHOLE = {
    "withdraw": "it withdraws the whole supplied balance",
    "repay": "it repays the whole debt",
}


def intent(config, observed, request: ActionRequest):
    """The request as the connector's intent; a withdrawal and a repayment are whole."""
    if request.action in ("supply", "borrow"):
        if request.amount is None:
            raise ValueError(f"{request.action} requires --amount")
        return getattr(config, request.action)(ALLOCATION, request.amount)
    if request.amount is not None:
        raise ValueError(f"{request.action} takes no --amount: {WHOLE[request.action]}")
    return getattr(config, request.action)(ALLOCATION)


def position(state) -> dict:
    owned = next(item for item in state.owned if item.target.allocation == ALLOCATION)
    return {"supplied_shares": str(owned.shares), "debt_shares": str(owned.drawn_shares)}


DIRECT = DirectActions(
    actions=("supply", "withdraw", "borrow", "repay"),
    positions=positions,
    intent=intent,
    position=position,
)
