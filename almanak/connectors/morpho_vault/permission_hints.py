"""MetaMorpho vault permission hints for permission discovery.

MetaMorpho vault compilation requires a GatewayClient for on-chain reads
(querying the vault's underlying asset, share price, etc.).  Since permission
discovery runs offline without a gateway, we provide static permissions
for the known vault contracts and their underlying ERC-20 tokens.

ERC-4626 function selectors:
- deposit(uint256,address)           = 0x6e553f65
- redeem(uint256,address,address)    = 0xba087652
- approve(address,uint256)           = 0x095ea7b3
"""

from almanak.framework.permissions.constants import METAMORPHO_VAULTS
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry


def _build_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    result: dict[str, list[StaticPermissionEntry]] = {}
    for chain, addrs in METAMORPHO_VAULTS.items():
        result[chain] = [
            # Approve the vault to spend the underlying asset
            StaticPermissionEntry(
                target=addrs["underlying"].lower(),
                label=f"ERC-20 ({addrs['underlying'][:6]}...{addrs['underlying'][-4:]})",
                selectors={"0x095ea7b3": "approve(address,uint256)"},
            ),
            # Deposit into the vault
            StaticPermissionEntry(
                target=addrs["vault"].lower(),
                label="MetaMorpho Vault",
                selectors={
                    "0x6e553f65": "deposit(uint256,address)",
                    "0xba087652": "redeem(uint256,address,address)",
                },
            ),
        ]
    return result


PERMISSION_HINTS = PermissionHints(
    static_permissions=_build_static_permissions(),
)
