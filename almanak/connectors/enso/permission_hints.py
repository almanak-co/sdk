"""Permission discovery hints and infrastructure permissions."""

import logging

from almanak.framework.execution.signer.safe.constants import SafeOperation
from almanak.framework.permissions.hints import PermissionHints
from almanak.framework.permissions.models import ContractPermission, FunctionPermission

from .adapter import ENSO_FUNCTION_SELECTORS
from .client import CHAIN_MAPPING, ROUTER_ADDRESSES

logger = logging.getLogger(__name__)


def build_enso_infrastructure_permissions(chain: str) -> list[ContractPermission]:
    """Build Enso Router infrastructure permissions for ``chain``.

    Swaps go through the Router via CALL with specific function selectors.
    ``send_allowed=True`` because native-token swaps (ETH, MNT, etc.) send
    value with the router call. Delegates (DELEGATECALL) are only for lending
    operations, which the SDK does not implement for Enso.
    """
    chain_id = CHAIN_MAPPING.get(chain.lower())
    router_addr = ROUTER_ADDRESSES.get(chain_id) if chain_id is not None else None
    if router_addr is None:
        logger.warning("No Enso Router address for chain %r: skipping Enso permissions", chain)
        return []

    return [
        ContractPermission(
            target=router_addr.lower(),
            label="Enso Router",
            operation=SafeOperation.CALL,
            send_allowed=True,
            function_selectors=[
                FunctionPermission(selector=selector, label=name)
                for name, selector in sorted(ENSO_FUNCTION_SELECTORS.items())
            ],
        )
    ]


PERMISSION_HINTS = PermissionHints()

__all__ = ["PERMISSION_HINTS", "build_enso_infrastructure_permissions"]
