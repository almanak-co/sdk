"""The reviewed GMX V2 deployment roles of each supported chain.

`deployments.compiled()` stays the single reviewed table; this module gives its
sixteen named roles a typed, validated view for public resolution. A resolver
never searches for a deployment, substitutes another chain's addresses or
accepts a responding contract: an unlisted chain refuses, and a role name that
the compiled closure does not carry exactly once refuses.
"""

from almanak.caip import ChainId
from almanak.connectors.gmx_v2.deployments import ROLES, compiled
from almanak.connectors.gmx_v2.exchange_domain import ExchangeContracts
from almanak.gateway.ethereum import DataError


def roles_for(chain: ChainId) -> ExchangeContracts:
    entries = compiled()["chains"].get(str(chain))
    if entries is None:
        raise DataError("unsupported_chain", f"GMX V2 has no compiled deployment on {chain}")
    return ExchangeContracts.model_validate(
        {field: sole(entries, name, chain) for field, name in ROLES.items()}
    )


def sole(entries: dict, name: str, chain: ChainId) -> str:
    found = [address for address, value in entries.items() if value.get("contract") == name]
    if len(found) != 1:
        raise DataError(
            "identity", f"GMX V2 {name} is not deployed exactly once in the {chain} closure"
        )
    return found[0]


def addresses(contracts: ExchangeContracts) -> tuple[str, ...]:
    return tuple(sorted(contracts.model_dump().values()))
