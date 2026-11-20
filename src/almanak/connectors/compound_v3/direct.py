"""Compound V3 direct operator actions: supply one asset to a Comet and withdraw it in full.

The deployment covers the wallet's whole position in the Comet, because Comet ownership
is verified per market: the base principal and every collateral balance must each equal
a configured position or be zero. Only the requested position is acted on.

No position carries borrow authority or a health floor. A wallet that owes the base
asset can still add collateral and clear an allowance; an action that would repay the
debt or release collateral against it is refused.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


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
    named = [(ALLOCATION, reserve), *((held_name(held.asset), held) for held in others)]
    native = str(evm_network(reserve.chain.chain_id).native_asset)
    return {
        "connector": "compound_v3",
        "allocations": [
            {
                "name": name,
                "account": wallet.account,
                "reserve": leg.model_dump(mode="json"),
                "native_asset": native,
                # The operator's fee ceilings and the wallet balance bound each step.
                "minimum_native_balance": 0,
            }
            for name, leg in named
        ],
    }


def requested(observed):
    return next(item for item in observed if item.allocation == ALLOCATION)


def debt(seen) -> str:
    """The wallet's open base debt, as a clause for a refusal; empty without debt."""
    if seen.account.principal >= 0:
        return ""
    owed, asset = seen.account.borrowed_base, seen.account.market.source.base_asset
    return f"the wallet owes {owed} base units of {asset} in this Comet"


def refuse_supply(seen, amount: int) -> None:
    """Refuse a supply that planning would refuse, in the operator's terms.

    The kinds are the connector's own where it has one for the same refusal.
    """
    from almanak.gateway.ethereum import DataError

    owed = debt(seen)
    if owed and seen.target.reserve.is_base:
        raise DataError(
            "unsupported",
            f"{owed}, so supplying it would repay that debt; repayment is not a direct action",
        )
    if seen.allowance not in (0, amount):
        raise DataError(
            "allowance_mismatch",
            f"the wallet already lets this Comet spend {seen.allowance} base units of"
            f" {seen.target.asset}; a supply uses an allowance only when it equals --amount."
            " Supply exactly that amount, or withdraw this asset first: a withdrawal ends"
            " by setting the allowance to zero",
        )


def refuse_withdraw(seen) -> None:
    """Refuse a withdrawal that planning would refuse, in the operator's terms."""
    from almanak.connectors.compound_v3.observations import owned
    from almanak.gateway.ethereum import DataError

    base = seen.target.reserve.is_base
    supplied = seen.account.supplied_base if base else owned(seen).collateral
    owed = debt(seen)
    if supplied and owed:
        # Base supply and base debt exclude each other, so this is collateral.
        raise DataError(
            "health_factor",
            f"{owed}; withdrawing collateral against open debt needs a health floor,"
            " which a direct action does not state. Repay the debt first",
        )
    # With nothing supplied, a withdrawal only clears an allowance left for the Comet.
    if not supplied and not seen.allowance:
        suffix = f"; {owed}" if owed else ""
        raise DataError(
            "no_position",
            f"nothing to withdraw: the wallet has no {seen.target.asset} supplied in this"
            f" Comet{suffix}",
        )


def intent(config, observed, request: ActionRequest):
    seen = requested(observed)
    if request.action == "supply":
        if request.amount is None:
            raise ValueError("supply requires --amount")
        refuse_supply(seen, request.amount)
        return config.supply(ALLOCATION, request.amount)
    if request.amount is not None:
        raise ValueError("withdraw takes no --amount: it withdraws the whole supplied balance")
    refuse_withdraw(seen)
    return config.withdraw(ALLOCATION)


def position(state) -> dict:
    """What the deployment owns in the requested position.

    `base_principal` is the Comet's signed principal, not a present value: positive is
    supply and negative is debt, and it is zero for a collateral asset. `collateral` is
    base units of the asset, and is zero for the base asset.
    """
    target = state.config.target(ALLOCATION)
    owned = next(item for item in state.owned if item.target == target)
    return {
        "asset": str(target.asset),
        "base_principal": str(owned.principal),
        "collateral": str(owned.collateral),
    }


DIRECT = DirectActions(
    actions=("supply", "withdraw"), positions=positions, intent=intent, position=position
)
