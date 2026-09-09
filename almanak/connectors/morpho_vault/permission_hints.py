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

from almanak.connectors._base.erc20_abi import ERC20_APPROVE_SELECTOR
from almanak.connectors.morpho_vault.addresses import METAMORPHO_PERMISSION_VAULTS, METAMORPHO_VAULTS
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry


def _permission_vault_rows() -> list[tuple[str, dict[str, str]]]:
    rows = [(chain, addrs) for chain, addrs in METAMORPHO_VAULTS.items()]
    rows.extend((extra["chain"], extra) for extra in METAMORPHO_PERMISSION_VAULTS)
    return rows


def _build_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    result: dict[str, list[StaticPermissionEntry]] = {}
    seen_targets: dict[str, set[str]] = {}
    for chain, addrs in _permission_vault_rows():
        chain_entries = result.setdefault(chain, [])
        chain_seen = seen_targets.setdefault(chain, set())
        underlying = addrs["underlying"].lower()
        vault = addrs["vault"].lower()
        if underlying not in chain_seen:
            chain_seen.add(underlying)
            chain_entries.append(
                StaticPermissionEntry(
                    target=underlying,
                    label=f"ERC-20 ({addrs['underlying'][:6]}...{addrs['underlying'][-4:]})",
                    selectors={ERC20_APPROVE_SELECTOR: "approve(address,uint256)"},
                )
            )
        if vault not in chain_seen:
            chain_seen.add(vault)
            chain_entries.append(
                StaticPermissionEntry(
                    target=vault,
                    label="MetaMorpho Vault",
                    selectors={
                        "0x6e553f65": "deposit(uint256,address)",
                        "0xba087652": "redeem(uint256,address,address)",
                    },
                )
            )
    return result


PERMISSION_HINTS = PermissionHints(
    static_permissions=_build_static_permissions(),
)
