"""Morpho Blue direct operator actions: supply to one market leg and withdraw it in full.

Supplying the loan leg lends the loan asset; supplying the collateral leg posts
collateral. Morpho ownership is checked per market, not per leg: the wallet's supply
shares, borrow shares and collateral in the market must equal the configured legs, with
zero for a leg that is not configured. The deployment therefore binds the requested leg
and, when the wallet holds anything in it, the market's other leg.

No allocation carries borrow authority or a health floor, so the deployment can neither
borrow nor repay, and the connector's risk check tolerates no debt. On a wallet with
debt in the market, supplying either leg and withdrawing lent assets still work: none
of them can lower the position's health. Withdrawing the collateral is refused, because
a full withdrawal would leave the debt unbacked and the core rejects it.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet

# The market's other leg, bound only when the wallet holds a position in it.
OTHER_LEG = "other_leg"


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The requested leg, and the market's other leg when the wallet holds it at `block`."""
    from almanak.connectors.morpho_blue.reader import read_position
    from almanak.gateway.evm import native_asset

    reserve = resolved.reserve
    market = reserve.market
    held = read_position(reader, market, wallet.address, block)
    legs = [(ALLOCATION, reserve.asset)]
    if reserve.is_loan and held.collateral:
        legs.append((OTHER_LEG, market.collateral_asset))
    elif not reserve.is_loan and (held.supply_shares or held.borrow_shares):
        legs.append((OTHER_LEG, market.loan_asset))
    document = market.model_dump(mode="json")
    native = str(native_asset(market.chain.chain_id))
    return {
        "connector": "morpho_blue",
        "allocations": [
            {
                "name": name,
                "account": wallet.account,
                "reserve": {"market": document, "asset": str(asset)},
                "native_asset": native,
                # The operator's fee ceilings and the wallet balance bound each step.
                "minimum_native_balance": 0,
            }
            for name, asset in legs
        ],
    }


def intent(config, observed, request: ActionRequest):
    if request.action == "supply":
        if request.amount is None:
            raise ValueError("supply requires --amount")
        require_usable_allowance(requested(observed), request.amount)
        return config.supply(ALLOCATION, request.amount)
    if request.amount is not None:
        raise ValueError("withdraw takes no --amount: it withdraws the whole supplied balance")
    before = requested(observed)
    require_supplied(before)
    require_no_debt_on_collateral(before)
    return config.withdraw(ALLOCATION)


def requested(observed):
    for observation in observed:
        if observation.allocation == ALLOCATION:
            return observation
    raise ValueError("the requested position was not observed")


def require_usable_allowance(before, amount: int) -> None:
    """A supply plans against an allowance of zero or exactly its own amount."""
    from almanak.gateway.ethereum import DataError

    if before.allowance in (0, amount):
        return
    raise DataError(
        "allowance_mismatch",
        f"supply allowance differs from the requested amount: the wallet approves "
        f"{before.allowance} base units to Morpho. Supply exactly that amount, or "
        "withdraw, which also revokes the allowance",
    )


def require_supplied(before) -> None:
    """A withdrawal needs something supplied on its leg, or an allowance to revoke."""
    from almanak.gateway.ethereum import DataError

    position = before.account.position
    supplied = position.supply_shares if before.target.reserve.is_loan else position.collateral
    if supplied or before.allowance:
        return
    message = "nothing is supplied on this leg and no allowance is left to revoke"
    if position.borrow_shares:
        message += (
            f"; the wallet owes {position.borrow_shares} borrow shares, "
            "which direct actions do not repay"
        )
    raise DataError("no_position", message)


def require_no_debt_on_collateral(before) -> None:
    """Refuse the collateral withdrawal the core would reject, and say why."""
    from almanak.gateway.ethereum import DataError

    shares = before.account.position.borrow_shares
    if before.target.reserve.is_loan or not shares:
        return
    raise DataError(
        "open_debt",
        f"the wallet owes {shares} borrow shares in this market, and its collateral "
        "cannot be withdrawn in full while that debt is open. Repay the debt first; "
        "direct actions do not repay",
    )


def position(state) -> dict:
    """The requested leg in its own units, and the other leg when the deployment binds it."""
    owned = {item.target.allocation: item for item in state.owned}
    result = leg(owned[ALLOCATION])
    if OTHER_LEG in owned:
        result["other_leg"] = leg(owned[OTHER_LEG])
    return result


def leg(owned) -> dict:
    """Loan shares are Morpho market shares; collateral is base units of its asset."""
    if owned.target.reserve.is_loan:
        return {
            "leg": "loan",
            "supply_shares": str(owned.supply_shares),
            "borrow_shares": str(owned.borrow_shares),
        }
    return {"leg": "collateral", "collateral": str(owned.collateral)}


DIRECT = DirectActions(
    actions=("supply", "withdraw"), positions=positions, intent=intent, position=position
)
