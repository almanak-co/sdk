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
# kind.
COMMAND_INTENTS: dict[str, tuple[str, ...]] = {
    "supply": ("supply",),
    "withdraw": ("withdraw",),
}


@dataclass(frozen=True)
class ActionRequest:
    """One operator request, as the command line states it."""

    action: str
    # Base units of the asset the action moves; None where the action takes none.
    amount: int | None = None


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


# Installed connectors `ax` runs no direct action for, and why.
NOT_DIRECT: dict[str, str] = {
    "uniswap_v4": "direct actions are not implemented for this connector",
    "uniswap_v3": "direct actions are not implemented for this connector",
    "pancakeswap_v3": "direct actions are not implemented for this connector",
    "aerodrome_slipstream": "direct actions are not implemented for this connector",
    "aerodrome": "direct actions are not implemented for this connector",
    "compound_v3": "direct actions are not implemented for this connector",
    "morpho_blue": "direct actions are not implemented for this connector",
    "curve": "direct actions are not implemented for this connector",
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


def require_direct(connector: str, chain, action: str) -> None:
    """Refuse a command the connector does not declare for the chain, before any read."""
    from almanak.connectors.composition import connector_from_wire

    if action not in binding(connector).actions:
        raise ValueError(f"{connector} serves no direct {action}")
    cells = connector_from_wire({"connector": connector}).chain_support
    declared = {str(kind) for cell in cells if cell.chain == chain for kind in cell.direct_actions}
    if not declared & set(COMMAND_INTENTS[action]):
        raise ValueError(f"{connector} serves no direct {action} on {chain}")
