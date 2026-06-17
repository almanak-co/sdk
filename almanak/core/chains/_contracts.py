"""Shared chain-infrastructure contract address fragments.

Descriptor modules use these helpers to declare membership without repeating
CREATE2 deployment addresses in every chain file. The descriptors remain the
source of truth for which chains expose each contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

SAFE_STACK_CONTRACTS: Mapping[str, str] = MappingProxyType(
    {
        # Safe v1.4.1 deployments. Source: safe-global/safe-deployments.
        "safe_multisend": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
        "safe_proxy_factory_v1_4_1": "0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67",
        "safe_l2_singleton_v1_4_1": "0x29fcB43b46531BcA003ddC8FCB67FFE91900C762",
        # Zodiac Roles infrastructure. Source: gnosisguild/zodiac sdk KnownContracts.
        "zodiac_module_proxy_factory": "0x000000000000aDdB49795b0f9bA5BC298cDda236",
        "zodiac_roles_modifier_singleton": "0x9646fDAD06d3e24444381f44362a3B0eB343D337",
    }
)

ENSO_DELEGATE_CONTRACTS: Mapping[str, str] = MappingProxyType(
    {
        "enso_delegate_primary": "0x7663fd40081dcCd47805c00e613B6beAc3B87F08",
        "enso_delegate_secondary": "0xA2F4f9C6ec598CA8c633024f8851c79CA5F43e48",
    }
)


def safe_stack_contracts(
    *,
    enso_delegate_primary: bool = False,
    enso_delegate_secondary: bool = False,
) -> dict[str, str]:
    """Return a descriptor contracts map for chains with Safe/Zodiac support."""
    contracts = dict(SAFE_STACK_CONTRACTS)
    if enso_delegate_primary:
        contracts["enso_delegate_primary"] = ENSO_DELEGATE_CONTRACTS["enso_delegate_primary"]
    if enso_delegate_secondary:
        contracts["enso_delegate_secondary"] = ENSO_DELEGATE_CONTRACTS["enso_delegate_secondary"]
    return contracts


def safe_multisend_contracts() -> dict[str, str]:
    """Return the legacy Safe MultiSend-only descriptor contracts map."""
    return {"safe_multisend": SAFE_STACK_CONTRACTS["safe_multisend"]}


__all__ = ["ENSO_DELEGATE_CONTRACTS", "SAFE_STACK_CONTRACTS", "safe_multisend_contracts", "safe_stack_contracts"]
