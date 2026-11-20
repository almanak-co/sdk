"""Aave V4 direct operator actions: supply to one reserve and withdraw it in full."""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One supply position in the resolved reserve; the Spoke's other reserves stay unbound."""
    from almanak.chains import evm_network

    reserve = resolved.reserve
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "reserve": reserve.model_dump(mode="json"),
                "native_asset": str(evm_network(reserve.chain.chain_id).native_asset),
            }
        ]
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
    owned = state.owned[0]
    return {"supplied_shares": str(owned.shares), "debt_shares": str(owned.drawn_shares)}


DIRECT = DirectActions(
    actions=("supply", "withdraw"), positions=positions, intent=intent, position=position
)
