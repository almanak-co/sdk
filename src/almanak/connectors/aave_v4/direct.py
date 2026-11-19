"""Aave V4 direct operator actions: supply, full withdrawal, borrow and full repayment.

A borrow covers the wallet's whole risk position on the Spoke. The requested reserve
is the one debt, and the other reserves the wallet supplies that can back it are bound
as its collateral. A supply is not collateral on a Spoke until it is enabled: the
borrow enables each bound reserve first, one `collateral` step each, and only then
plans the borrow step, which is the one step projected against the health floor. A
preview of a first borrow therefore shows a `collateral` step and has not tested the
floor, and a borrow the floor refuses can leave its collateral enabled.

A repayment is always served for a debt the wallet owes and can fund. It binds the
debt and the collateral it releases afterwards: every other supply enabled as
collateral, and none while the wallet owes another reserve of the Spoke. Collateral is
never released while any debt on the Spoke is open.

A supply or a withdrawal binds only its own reserve. A withdrawal of collateral is
refused while the wallet owes anything on the Spoke. A reserve holds one position: the
connector refuses a supply or a withdrawal in a reserve the wallet owes when it plans
the first step.

What is refused here is read once, before the deployment exists, and says why in the
operator's terms. The connector's execution checks hold every release of collateral
to the account's debt again when a step is planned and when it is signed
(`validation.validate_account_release`).
"""

from functools import partial
from typing import NamedTuple

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


class Held(NamedTuple):
    """What the wallet holds in one reserve of a Spoke."""

    # Supply shares.
    supplied: int
    # Whether any drawn or premium debt is open.
    owes: bool
    # The collateral flag, which a reserve can carry with nothing supplied.
    collateral: bool


class Bound(NamedTuple):
    """A reserve bound beside the requested debt, with the Spoke's price source for it."""

    reserve_id: int
    reserve: object
    source: str


def spoke_account(reader, block, spoke: str, account: str) -> dict[int, Held]:
    """The wallet's position in every reserve of the Spoke, by ascending reserve ID."""
    from almanak.connectors.aave_v4.debt import reserve_components

    (count,) = reader.call(spoke, "getReserveCount()", block)
    found = {}
    for reserve_id in range(count):
        debt, supplied = reserve_components(reader, spoke, reserve_id, account, block)
        owes = bool(debt["drawn_shares"] or debt["premium_shares"] or debt["premium_offset_ray"])
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
    """The requested reserve, and for a borrow or a repayment what is bound beside it."""
    reserve = resolved.reserve
    if request.action != "borrow" and request.min_health_factor is not None:
        raise ValueError(
            f"Aave V4 projects health only for a borrow: a floor on a {request.action} "
            "would not be checked; leave out --min-health-factor"
        )
    if request.action == "supply":
        return {"allocations": [allocation(ALLOCATION, wallet, reserve)]}
    held = spoke_account(reader, block, reserve.spoke, wallet.address)
    if request.action == "withdraw":
        require_released(held, reserve)
        return {"allocations": [allocation(ALLOCATION, wallet, reserve)]}
    return debt_positions(reader, block, resolved, wallet, request, held)


def require_released(held: dict[int, Held], reserve) -> None:
    """Refuse to withdraw a reserve that backs an open debt.

    The Spoke admits any withdrawal that leaves the health factor at 1 or above, and no
    health floor applies to a withdrawal here.
    """
    if not held[reserve.reserve_id].collateral:
        return
    owed = [reserve_id for reserve_id, item in held.items() if item.owes]
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


def borrow_collateral(reader, block, reserve, held: dict[int, Held]) -> list[int]:
    """The reserve IDs a borrow of the requested reserve binds as its collateral.

    A borrow enables what it binds, so it leaves a supply that can back nothing as it
    is. The connector admits one debt reserve per account and Spoke, so a borrow is
    refused while the wallet owes another one.
    """
    requested = reserve.reserve_id
    other = [i for i, item in held.items() if item.owes and i != requested]
    if other:
        raise ValueError(
            f"the wallet has debt in reserve {other[0]} of this Spoke, and Aave V4 serves "
            f"one debt reserve per wallet and Spoke: repay reserve {other[0]} in full first"
        )
    backing = [
        reserve_id
        for reserve_id, item in held.items()
        if reserve_id != requested
        and item.supplied
        and (item.collateral or backs_debt(reader, block, reserve.spoke, reserve_id))
    ]
    if not backing:
        raise ValueError(
            "the wallet has supplied no collateral on this Spoke: supply a reserve with a "
            f"collateral factor, other than reserve {requested}, before borrowing"
        )
    return backing


def bound(reader, block, resolved, oracle: str, reserve_id: int) -> Bound:
    """Another reserve of the requested reserve's Spoke, resolved as the connector does."""
    from almanak.connectors.aave_v4.debt import reserve_source
    from almanak.connectors.lending_lookup import family, resolve, selection

    chosen = resolved.selection
    values = {"chain": str(chosen.chain), "spoke": chosen.spoke, "reserve_id": reserve_id}
    reserve = resolve(reader, family("aave_v4"), selection("aave_v4", values), block).reserve
    return Bound(reserve_id, reserve, reserve_source(reader, oracle, reserve_id, block))


def verified(bind, reserve_id: int) -> Bound | None:
    """A reserve to bind beside a repayment, or None where the connector refuses it.

    A reserve the connector cannot verify must not hold back a repayment. A read
    failure that a later attempt may pass is not such a refusal and is raised.
    """
    from almanak.gateway.ethereum import DataError

    try:
        return bind(reserve_id)
    except DataError as error:
        if error.transient:
            raise
        return None
    except ValueError:
        return None


def repay_collateral(bind, held: dict[int, Held], requested: int) -> list[Bound]:
    """What a full repayment of the requested reserve binds beside the debt.

    With no other debt open it is every other supply enabled as collateral: the
    repayment disables each one once the debt is gone. While the wallet owes another
    reserve of the Spoke it releases nothing. One the connector cannot verify stays
    enabled and withdraws like any supply once nothing is owed.

    The connector's debt names at least one supply position of its Spoke. A repayment
    with nothing to release names one reserve whose collateral flag is off, which it
    leaves as it is.
    """
    others = {i: item for i, item in held.items() if i != requested}
    indebted = any(item.owes for item in others.values())
    releasable = [i for i, item in others.items() if item.supplied and item.collateral]
    released = [] if indebted else [found for i in releasable if (found := verified(bind, i))]
    if released:
        return released
    for reserve_id, item in others.items():
        if not item.collateral and (found := verified(bind, reserve_id)):
            return [found]
    raise ValueError(
        f"no other reserve of this Spoke can be bound beside reserve {requested}: the "
        "connector's debt names a supply position of its Spoke, and every other reserve "
        "is enabled as the wallet's collateral or cannot be verified"
    )


def health_floor(request: ActionRequest) -> int:
    """The debt's health floor, at wad precision.

    Only a planned borrow is projected against the floor. A repayment, its approval and
    revocation, and the release of collateral after it are never projected, so a
    repayment is given the lowest valid floor, as on a Pool: no floor can refuse one.
    """
    from almanak.connectors.pool_lending_direct import health_floor as stated

    floor = stated(request)
    if floor is None:
        raise ValueError("borrow requires --min-health-factor")
    return floor


def debt_positions(reader, block, resolved, wallet: Wallet, request: ActionRequest, held) -> dict:
    """The requested debt with what it binds, the Spoke oracle and the sources it uses now."""
    from almanak.connectors.aave_v4.debt import reserve_source

    reserve = resolved.reserve
    (oracle,) = reader.call(reserve.spoke, "ORACLE()", block, outputs=("address",))
    bind = partial(bound, reader, block, resolved, oracle)
    if request.action == "borrow":
        backing = [bind(i) for i in borrow_collateral(reader, block, reserve, held)]
    else:
        backing = repay_collateral(bind, held, reserve.reserve_id)
    names = [f"collateral_{item.reserve_id}" for item in backing]
    debt = {
        **allocation(ALLOCATION, wallet, reserve),
        "collateral": names,
        "minimum_health_factor": health_floor(request),
        "oracle": oracle,
        "collateral_price_sources": [item.source for item in backing],
        "debt_price_source": reserve_source(reader, oracle, reserve.reserve_id, block),
    }
    return {
        "allocations": [
            allocation(name, wallet, item.reserve)
            for name, item in zip(names, backing, strict=True)
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
