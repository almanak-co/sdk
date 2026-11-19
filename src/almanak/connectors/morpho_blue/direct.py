"""Morpho Blue direct operator actions on one market leg.

The loan leg lends, borrows and repays the loan asset; the collateral leg posts and
withdraws collateral. Morpho ownership is checked per market, not per leg: the wallet's
supply shares, borrow shares and collateral in the market must equal the configured
legs, with zero for a leg that is not configured. The deployment therefore binds the
requested leg and the market's other leg when the wallet holds anything in it. A borrow
or a repayment binds both legs, as borrow authority needs the collateral leg beside it.

Borrow authority goes to the loan leg of a borrow or a repayment only. One health floor
serves both legs, as the connector requires of a fund's allocations in one market. It
is the operator's `--min-health-factor`; without one the risk check tolerates no debt
at all. The connector risk-checks a borrow and a collateral withdrawal. Supplying either
leg, withdrawing lent assets and repaying cannot lower the position's health, so no
floor is asked of them and none can refuse them.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet

# The market's other leg, bound when the wallet holds a position in it or the request
# needs it.
OTHER_LEG = "other_leg"
# The commands that act on debt: only the loan leg serves them, with borrow authority.
DEBT_ACTIONS = ("borrow", "repay")


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The requested leg, the market's other leg where held or needed, and their policy."""
    from almanak.connectors.morpho_blue.reader import read_position
    from almanak.connectors.pool_lending_direct import health_floor
    from almanak.gateway.evm import native_asset

    reserve = resolved.reserve
    market = reserve.market
    held = read_position(reader, market, wallet.address, block)
    require_served(request, reserve.is_loan, held)
    legs = [(ALLOCATION, reserve.asset)]
    on_debt = request.action in DEBT_ACTIONS
    if reserve.is_loan and (held.collateral or on_debt):
        legs.append((OTHER_LEG, market.collateral_asset))
    elif not reserve.is_loan and (held.supply_shares or held.borrow_shares):
        legs.append((OTHER_LEG, market.loan_asset))
    document = market.model_dump(mode="json")
    native = str(native_asset(market.chain.chain_id))
    # A repayment states the lowest valid floor: its borrow authority must state one,
    # and no floor may refuse it.
    floor = health_floor(request)
    policy = {} if floor is None else {"minimum_health_factor": floor}
    return {
        "connector": "morpho_blue",
        "allocations": [
            {
                "name": name,
                "account": wallet.account,
                "reserve": {"market": document, "asset": str(asset)},
                "borrow": on_debt and name == ALLOCATION,
                "native_asset": native,
                # The operator's fee ceilings and the wallet balance bound each step.
                "minimum_native_balance": 0,
                **policy,
            }
            for name, asset in legs
        ],
    }


def require_served(request: ActionRequest, loan: bool, held) -> None:
    """Refuse, before anything is planned, a request this market leg cannot serve.

    Each refusal names its cause. The connector's own would be a configuration error, or
    a health factor below a floor nobody stated.
    """
    action = request.action
    if action in DEBT_ACTIONS and not loan:
        raise ValueError(f"a {action} is served on the loan leg of the market: state --leg loan")
    if action == "borrow":
        require_borrowable(request, held)
    if action == "withdraw" and not loan and held.borrow_shares:
        require_floored_withdrawal(request)


def require_borrowable(request: ActionRequest, held) -> None:
    if request.min_health_factor is None:
        raise ValueError(
            "a borrow is risk-checked against a health floor; state --min-health-factor"
        )
    if not held.collateral:
        raise ValueError(
            "the wallet has no collateral in this market: supply the collateral leg before a borrow"
        )


def require_floored_withdrawal(request: ActionRequest) -> None:
    """Collateral that backs debt leaves only in part, and only above a stated floor."""
    if request.amount is None:
        raise ValueError(
            "the wallet has debt in this market: its collateral cannot be withdrawn in "
            "full while the debt is open; repay it with `ax repay`, or withdraw an "
            "--amount under --min-health-factor"
        )
    if request.min_health_factor is None:
        raise ValueError(
            "the wallet has debt in this market: a collateral withdraw there is "
            "risk-checked against a health floor; state --min-health-factor, or repay "
            "the debt with `ax repay`"
        )


def intent(config, observed, request: ActionRequest):
    """The request as the connector's intent; argparse has settled which flags it has."""
    action, amount = request.action, request.amount
    if action in ("supply", "borrow"):
        if amount is None:
            raise ValueError(f"{action} requires --amount")
        if action == "supply":
            require_usable_allowance(requested(observed), "supply", amount)
        return getattr(config, action)(ALLOCATION, amount)
    before = requested(observed)
    if action == "repay":
        return repayment(config, before, amount)
    require_supplied(before)
    if amount is None:
        return config.withdraw(ALLOCATION)
    require_within_supplied(before, amount)
    return config.withdraw_amount(ALLOCATION, amount)


def repayment(config, before, amount: int | None):
    require_debt(before)
    if amount is None:
        return config.repay(ALLOCATION)
    require_usable_allowance(before, "repay", amount)
    return config.repay_amount(ALLOCATION, amount)


def requested(observed):
    for observation in observed:
        if observation.allocation == ALLOCATION:
            return observation
    raise ValueError("the requested position was not observed")


# The command that clears an allowance which differs from a request's amount.
ALLOWANCE_EXITS = {
    "supply": "withdraw, which also revokes the allowance",
    "repay": "repay without --amount, which also revokes the allowance",
}


def require_usable_allowance(before, action: str, amount: int) -> None:
    """A supply or a partial repayment plans against an allowance of zero or its amount."""
    from almanak.gateway.ethereum import DataError

    if before.allowance in (0, amount):
        return
    raise DataError(
        "allowance_mismatch",
        f"{action} allowance differs from the requested amount: the wallet approves "
        f"{before.allowance} base units to Morpho. State exactly that amount, or "
        f"{ALLOWANCE_EXITS[action]}",
    )


def require_debt(before) -> None:
    """A repayment needs debt to repay, or an allowance an earlier one left to revoke."""
    from almanak.gateway.ethereum import DataError

    if before.account.position.borrow_shares or before.allowance:
        return
    raise DataError(
        "no_debt", "the wallet owes no debt in this market and no allowance is left to revoke"
    )


def require_supplied(before) -> None:
    """A withdrawal needs something supplied on its leg, or an allowance to revoke."""
    from almanak.gateway.ethereum import DataError

    if supplied(before) or before.allowance:
        return
    message = "nothing is supplied on this leg and no allowance is left to revoke"
    debt = before.account.position.borrow_shares
    if debt:
        message += f"; the wallet owes {debt} borrow shares, which `ax repay` repays"
    raise DataError("no_position", message)


def require_within_supplied(before, amount: int) -> None:
    from almanak.gateway.ethereum import DataError

    held = supplied(before)
    if amount > held:
        raise DataError(
            "exceeds_supplied",
            f"withdraw amount exceeds the supplied balance of {held} base units",
        )


def supplied(before) -> int:
    """Base units supplied on the observed leg: lent assets with interest, or collateral."""
    from almanak.connectors.morpho_blue.risk import supplied_assets

    if before.target.reserve.is_loan:
        return supplied_assets(before.account)
    return before.account.position.collateral


def position(state) -> dict:
    """The requested leg in its own units, and the other leg when the deployment binds it.

    Debt is the loan leg's `borrow_shares`. A wallet with debt always has that leg bound:
    as the requested leg, or as the other leg, which a held position binds.
    """
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
    actions=("supply", "withdraw", "borrow", "repay"),
    positions=positions,
    intent=intent,
    position=position,
)
