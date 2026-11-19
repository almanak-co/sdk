"""What `almanak ax` runs as a direct operator action, as each connector declares it.

A connector package that serves direct actions holds a `direct.py` with a `DIRECT`
declaration, and `ax` builds its command choices from these. Every other installed
connector is named in `NOT_DIRECT` with the reason, and a test holds the two together,
so adding a connector without deciding its direct actions fails. Which chains serve an
action is the connector's support declaration (`ConnectorChainSupport.direct_actions`),
which the gateway enforces as well.
"""

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache

# The name every direct deployment gives its one requested position.
ALLOCATION = "position"

# The intent kinds each `ax` command can admit. A chain serves a command when its
# support declares one of them as a direct action; the gateway admits only a declared
# kind. A command with two kinds admits the first without an amount, the whole balance
# or debt, and the second with one.
COMMAND_INTENTS: dict[str, tuple[str, ...]] = {
    "supply": ("supply",),
    "withdraw": ("withdraw", "withdraw_amount"),
    "borrow": ("borrow",),
    "repay": ("repay", "repay_amount"),
    "collateral": ("collateral",),
    "swap": ("swap_exact_input",),
    "open-liquidity": ("open_liquidity",),
    "increase-liquidity": ("increase_liquidity",),
    "decrease-liquidity": ("decrease_liquidity",),
    "collect-liquidity-fees": ("collect_liquidity_fees",),
    "close-liquidity": ("close_liquidity",),
    "revoke-exchange": ("revoke_exchange",),
}


@dataclass(frozen=True)
class ActionRequest:
    """One operator request, as the command line states it."""

    action: str
    # Base units of the asset the action moves; None where the action takes none.
    amount: int | None = None
    # The lowest projected health factor a risk-checked action may leave, at wad
    # precision. None states no floor, which tolerates no debt at all.
    min_health_factor: int | None = None
    # `collateral` only: whether the supplied balance is to back debt.
    enable: bool | None = None
    # A swap only. `sell` is the CAIP-19 asset type sold, one of the pool's
    # currencies. `slippage_bps` is how far below its quote the output may fall, in
    # basis points. `deadline_seconds` is how long after the observed block the swap
    # may still be signed; whether a swap mined later also reverts is the connector's
    # own call, and a Curve call carries no deadline.
    sell: str | None = None
    slippage_bps: int | None = None
    deadline_seconds: int | None = None
    # The CAIP-19 asset type bought, stated only where `DirectActions.names_bought`.
    buy: str | None = None
    # A liquidity action only. `amount0` and `amount1` are the most base units of the
    # pool's currency0 and currency1 a mint or an increase may use. `tick_lower` and
    # `tick_upper` are the range a new position is minted into. `token_id` names an
    # existing position by its position-manager NFT. `liquidity` is how much of that
    # position a decrease removes, in the position's own liquidity units.
    amount0: int | None = None
    amount1: int | None = None
    tick_lower: int | None = None
    tick_upper: int | None = None
    token_id: int | None = None
    liquidity: int | None = None
    # A liquidity action in a pool whose position is its LP token balance. `amounts`
    # pairs a CAIP-19 coin of the pool with the base units of it a deposit uses.
    # `receive` is how a burn pays out: `balanced`, or the CAIP-19 coin it pays alone.
    # There `liquidity` is the LP shares a decrease burns, in base units of the LP token.
    amounts: tuple[tuple[str, int], ...] | None = None
    receive: str | None = None

    @property
    def kind(self) -> str:
        """The intent kind this request admits."""
        kinds = COMMAND_INTENTS[self.action]
        return kinds[1] if len(kinds) > 1 and self.amount is not None else kinds[0]


@dataclass(frozen=True)
class Wallet:
    """The plain account an action runs for, under the name its keystore entry carries."""

    account: str
    address: str


@dataclass(frozen=True)
class DirectActions:
    # The `ax` commands the connector serves; `COMMAND_INTENTS` names what each admits.
    actions: tuple[str, ...]
    # `positions(reader, block, resolved, wallet, request)`: the connector's own part of
    # the deployment configuration for the resolved market, such as its allocations.
    positions: Callable
    # `intent(config, observed, request)`: the request as the connector's intent, built
    # against the observations the coordinator admits it with.
    intent: Callable
    # `position(state)`: what the deployment owns, for the command's result.
    position: Callable
    # Whether a swap states the asset bought as well as the asset sold: the connector's
    # pools can hold more than two coins. Elsewhere the pool decides what is bought.
    names_bought: bool = False
    # `liquidity_flags(command)`: the position flags a liquidity command takes for this
    # connector, as `ActionRequest` field names: those it requires, then those it also
    # takes. None takes the flags of a concentrated-liquidity position: a tick range or
    # an NFT, and an amount of each of the two currencies.
    liquidity_flags: Callable | None = None


# Installed connectors `ax` runs no direct action for, and why.
NOT_DIRECT: dict[str, str] = {
    "gmx_v2": "a GMX request settles through keepers after its transaction is mined, "
    "and cancelling one needs a new signature; a direct command is one process that "
    "ends with its last receipt",
    "hyperliquid": "HyperCore custody is Safe/Roles only, and a direct action runs "
    "for a plain account",
}


@cache
def direct_actions() -> dict[str, DirectActions]:
    from almanak.connectors.discovery import declared

    return declared("DIRECT", module="direct")


def connectors_for(action: str) -> tuple[str, ...]:
    """The connectors that serve one `ax` command, in installation order."""
    return tuple(name for name, found in direct_actions().items() if action in found.actions)


def served_actions() -> tuple[str, ...]:
    """Every `ax` action command some connector serves, in first-declared order."""
    return tuple(dict.fromkeys(a for found in direct_actions().values() for a in found.actions))


def binding(connector: str) -> DirectActions:
    try:
        return direct_actions()[connector]
    except KeyError:
        reason = NOT_DIRECT.get(connector, "unknown connector")
        raise ValueError(f"no direct actions for {connector}: {reason}") from None


def binding_for(config) -> DirectActions:
    """The declaration of the connector a deployment configuration belongs to."""
    # Only an Aave V4 configuration carries no discriminator, as on the wire. A composed
    # configuration carries an empty one and belongs to no single connector.
    name = getattr(config, "connector", "aave_v4")
    if name is None:
        raise ValueError("a composed configuration serves no direct action")
    return binding(str(name))


def require_direct(connector: str, chain, request: ActionRequest) -> None:
    """Refuse a request the connector does not declare for the chain, before any read."""
    from almanak.connectors.composition import connector_from_wire

    if request.action not in binding(connector).actions:
        raise ValueError(f"{connector} serves no direct {request.action}")
    cells = connector_from_wire({"connector": connector}).chain_support
    declared = {str(kind) for cell in cells if cell.chain == chain for kind in cell.direct_actions}
    if request.kind not in declared:
        # The form the operator typed: a second kind is the command with an amount.
        partial = request.kind != COMMAND_INTENTS[request.action][0]
        form = f"{request.action} --amount" if partial else request.action
        raise ValueError(f"{connector} serves no direct {form} on {chain}")


def require_bought(connector: str, request: ActionRequest) -> None:
    """Refuse a swap that leaves the bought asset out where the pool cannot decide it,
    or states it where the pool does, before any read."""
    if request.action != "swap":
        return
    if binding(connector).names_bought:
        if request.buy is None:
            raise ValueError(
                f"{connector} pools can hold more than two coins: --buy must name the asset bought"
            )
    elif request.buy is not None:
        raise ValueError(
            f"{connector} pools hold two currencies, so the asset bought is the one not "
            "sold: --buy is not accepted"
        )
