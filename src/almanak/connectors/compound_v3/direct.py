"""Compound V3 direct operator actions on one Comet: supply and withdraw either kind of
asset, and borrow and repay the base asset.

The deployment covers the wallet's whole position in the Comet, because Comet ownership
is verified per market: the base principal and every collateral balance must each equal
a configured position or be zero. Only the requested position is acted on.

Borrow and repay authority sits on the base position, which those commands request. One
health floor serves the wallet's whole position in the Comet, as the connector requires
of a fund's allocations there. It is the operator's `--min-health-factor`. The connector
risk-checks a borrow, and a collateral withdrawal while the wallet owes the base asset;
without a floor that check tolerates no debt at all.

A Comet keeps one signed base balance per account, so a base supply to an account in
debt repays, and a base withdrawal above the supplied balance borrows. No command
crosses that line for another: a supply never repays, a withdrawal never borrows, and
a borrow never withdraws.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet

# Repay authority belongs to an allocation that may borrow, which must state a floor. A
# repayment is never risk-checked, so the lowest valid floor is stated for it: no floor
# can then refuse a repayment.
REPAY_FLOOR = 10**18 + 1


def held_name(asset) -> str:
    """The name of a position the wallet holds beside the requested one."""
    return f"held-{asset.reference}"


def held_reserves(market, account):
    """The reserve of every leg the account holds: nonzero base principal, in supply or
    in debt, and every nonzero collateral balance."""
    from almanak.connectors.compound_v3.domain import Reserve

    source = market.source
    held = []
    if account.principal:
        held.append(
            Reserve(market=source, asset=source.base_asset, price_feed=source.base_price_feed)
        )
    for listed, balance in zip(market.collaterals, account.collaterals, strict=True):
        if balance.balance:
            held.append(
                Reserve(market=source, asset=listed.asset_type, price_feed=listed.price_feed)
            )
    return held


def require_statable(request: ActionRequest, reserve, others) -> None:
    """Refuse a borrow or repayment whose deployment the connector could not configure.

    Only the base asset is borrowed, a borrowing position states a health floor, and the
    connector grants borrow authority only beside a collateral position.
    """
    if request.action not in ("borrow", "repay"):
        return
    if not reserve.is_base:
        raise ValueError(
            f"{request.action} names the Comet's base asset, {reserve.market.base_asset};"
            f" {reserve.asset} is collateral there"
        )
    if request.action == "repay":
        return
    if request.min_health_factor is None:
        raise ValueError("borrow requires --min-health-factor")
    if not others:
        raise ValueError(
            "the wallet holds no collateral in this Comet: supply collateral before borrowing"
        )


def health_floor(request: ActionRequest) -> int | None:
    if request.min_health_factor is None and request.action == "repay":
        return REPAY_FLOOR
    return request.min_health_factor


def with_policy(entry: dict, request: ActionRequest, indebted: bool) -> dict:
    """Add the request's borrow authority and the Comet's one health floor to a position.

    A repayment takes its authority only while the wallet owes: without debt there is
    nothing to repay, and the request is refused when its intent is built.
    """
    floor = health_floor(request)
    authorized = request.action == "borrow" or (request.action == "repay" and indebted)
    if entry["name"] == ALLOCATION and authorized:
        entry = {**entry, "borrow": True}
    return entry if floor is None else {**entry, "minimum_health_factor": floor}


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The requested position, then one for every other leg the wallet holds in the Comet.

    Read at the pinned `block`. A leg that appears afterwards is not configured, and the
    deployment then refuses with `ownership_mismatch` instead of acting on a partial view.
    """
    from almanak.caip import AccountReference
    from almanak.chains import evm_network
    from almanak.connectors.compound_v3.reader import read_account, read_market
    from almanak.models import Account

    reserve = resolved.reserve
    market = read_market(reader, reserve.market, block)
    fund = Account(chain=reserve.chain, address=AccountReference(wallet.address))
    account = read_account(reader, market, fund)
    others = [held for held in held_reserves(market, account) if held.asset != reserve.asset]
    require_statable(request, reserve, others)
    named = [(ALLOCATION, reserve), *((held_name(held.asset), held) for held in others)]
    native = str(evm_network(reserve.chain.chain_id).native_asset)
    entries = [
        {
            "name": name,
            "account": wallet.account,
            "reserve": leg.model_dump(mode="json"),
            "native_asset": native,
            # The operator's fee ceilings and the wallet balance bound each step.
            "minimum_native_balance": 0,
        }
        for name, leg in named
    ]
    return {
        "connector": "compound_v3",
        "allocations": [with_policy(entry, request, account.principal < 0) for entry in entries],
    }


def requested(observed):
    return next(item for item in observed if item.allocation == ALLOCATION)


def debt(seen) -> str:
    """The wallet's open base debt, as a clause for a refusal; empty without debt."""
    if seen.account.principal >= 0:
        return ""
    owed, asset = seen.account.borrowed_base, seen.account.market.source.base_asset
    return f"the wallet owes {owed} base units of {asset} in this Comet"


def supplied(seen) -> int:
    """Base units of the requested asset the wallet has supplied, with accrued interest."""
    from almanak.connectors.compound_v3.observations import owned

    if seen.target.reserve.is_base:
        return seen.account.supplied_base
    return owned(seen).collateral


def refuse_allowance(seen, amount: int | None, action: str, clearing: str) -> None:
    """Refuse a stated amount that an allowance left for the Comet does not equal."""
    from almanak.gateway.ethereum import DataError

    if seen.allowance not in (0, amount):
        raise DataError(
            "allowance_mismatch",
            f"the wallet already lets this Comet spend {seen.allowance} base units of"
            f" {seen.target.asset}; a {action} uses an allowance only when it equals"
            f" --amount. State exactly that amount, or {clearing} first, which ends by"
            " setting the allowance to zero",
        )


def refuse_supply(seen, request: ActionRequest) -> None:
    """Refuse a supply that planning would refuse, in the operator's terms.

    The kinds are the connector's own where it has one for the same refusal.
    """
    from almanak.gateway.ethereum import DataError

    owed = debt(seen)
    if owed and seen.target.reserve.is_base:
        raise DataError(
            "unsupported",
            f"{owed}, so supplying it would repay that debt; use `ax repay`",
        )
    refuse_allowance(seen, request.amount, "supply", "withdraw this asset")


def refuse_withdraw(seen, request: ActionRequest) -> None:
    """Refuse a withdrawal that planning would refuse, in the operator's terms."""
    from almanak.gateway.ethereum import DataError

    asset, held, owed = seen.target.asset, supplied(seen), debt(seen)
    # Base supply and base debt exclude each other, so a supplied balance beside debt
    # is collateral, and its withdrawal is risk-checked.
    if held and owed and request.min_health_factor is None:
        raise ValueError(
            "the wallet has debt in this Comet: a collateral withdrawal there is"
            " risk-checked against a health floor; state --min-health-factor"
        )
    # With nothing supplied, a withdrawal only clears an allowance left for the Comet.
    if not held and not seen.allowance:
        suffix = f"; {owed}" if owed else ""
        raise DataError(
            "no_position",
            f"nothing to withdraw: the wallet has no {asset} supplied in this Comet{suffix}",
        )
    if held and request.amount is not None and request.amount > held:
        beyond = "; a base withdrawal above it would borrow, which `ax borrow` does"
        raise DataError(
            "withdrawal_exceeds_supply",
            f"the wallet has {held} base units of {asset} supplied in this Comet"
            + (beyond if seen.target.reserve.is_base else ""),
        )


def refuse_borrow(seen, request: ActionRequest) -> None:
    """Refuse a borrow that would first withdraw the wallet's own base supply."""
    from almanak.gateway.ethereum import DataError

    if seen.account.principal > 0:
        raise DataError(
            "unsupported",
            f"the wallet has {seen.account.supplied_base} base units of {seen.target.asset}"
            " supplied in this Comet, so borrowing it would withdraw that supply first;"
            " use `ax withdraw`",
        )


def refuse_repay(seen, request: ActionRequest) -> None:
    """Refuse a repayment with no debt to repay, or with an allowance it cannot use.

    A full repayment replaces an allowance below the debt and clears what remains.
    """
    from almanak.gateway.ethereum import DataError

    if seen.account.principal >= 0:
        raise DataError(
            "no_position",
            f"nothing to repay: the wallet owes no {seen.target.asset} in this Comet",
        )
    if request.amount is not None:
        refuse_allowance(seen, request.amount, "repayment", "repay the whole debt")


REFUSALS = {
    "supply": refuse_supply,
    "withdraw": refuse_withdraw,
    "borrow": refuse_borrow,
    "repay": refuse_repay,
}


def intent(config, observed, request: ActionRequest):
    """The request as the connector's intent, refused first where planning would refuse.

    Each refusal is built from the observations the request is admitted with.
    """
    amount = request.amount
    if request.action in ("supply", "borrow") and amount is None:
        raise ValueError(f"{request.action} requires --amount")
    REFUSALS[request.action](requested(observed), request)
    if request.action in ("supply", "borrow"):
        return getattr(config, request.action)(ALLOCATION, amount)
    if amount is None:
        return getattr(config, request.action)(ALLOCATION)
    return getattr(config, f"{request.action}_amount")(ALLOCATION, amount)


def position(state) -> dict:
    """The requested asset's collateral and the wallet's base balance in the Comet.

    `collateral` is base units of the requested asset, and is zero for the base asset.
    `base_principal` is the Comet's signed principal of the wallet, whichever asset was
    requested: positive is supply and negative is debt. It is not a present value.
    """
    target = state.config.target(ALLOCATION)
    owned = next(item for item in state.owned if item.target == target)
    # A base position is configured whenever the wallet's principal is not zero.
    principal = next((item.principal for item in state.owned if item.target.reserve.is_base), 0)
    return {
        "asset": str(target.asset),
        "base_principal": str(principal),
        "collateral": str(owned.collateral),
    }


DIRECT = DirectActions(
    actions=("supply", "withdraw", "borrow", "repay"),
    positions=positions,
    intent=intent,
    position=position,
)
