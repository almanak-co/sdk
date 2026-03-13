"""Dataclasses for Zodiac Roles permission manifests.

A PermissionManifest describes the minimum set of contract interactions
a strategy agent is allowed to perform through a Safe wallet with
Zodiac Roles module restrictions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FunctionPermission:
    """A single function selector permitted on a contract.

    Attributes:
        selector: 4-byte hex selector, e.g. "0x095ea7b3"
        label: Human-readable signature, e.g. "approve(address,uint256)"
    """

    selector: str
    label: str

    def to_dict(self) -> dict[str, str]:
        return {"selector": self.selector, "label": self.label}


@dataclass
class ContractPermission:
    """Permission entry for a single target contract.

    Attributes:
        target: Contract address (checksummed or lowercase hex)
        label: Human-readable name, e.g. "Uniswap V3 SwapRouter02"
        operation: 0 = CALL, 1 = DELEGATECALL (matches SafeOperation enum)
        send_allowed: Whether ETH value transfers are permitted
        function_selectors: Allowed function selectors on this contract
    """

    target: str
    label: str
    operation: int = 0
    send_allowed: bool = False
    function_selectors: list[FunctionPermission] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "label": self.label,
            "operation": self.operation,
            "send_allowed": self.send_allowed,
            "function_selectors": [s.to_dict() for s in self.function_selectors],
        }


@dataclass
class PermissionManifest:
    """Complete permission manifest for a strategy on a single chain.

    Attributes:
        version: Manifest schema version
        chain: Target chain name
        strategy: Strategy identifier
        generated_at: ISO-8601 timestamp of generation
        warnings: Non-fatal issues encountered during generation
        permissions: List of contract permissions
    """

    version: str
    chain: str
    strategy: str
    generated_at: str
    warnings: list[str] = field(default_factory=list)
    permissions: list[ContractPermission] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "chain": self.chain,
            "strategy": self.strategy,
            "generated_at": self.generated_at,
            "warnings": self.warnings,
            "permissions": [p.to_dict() for p in self.permissions],
        }

    @property
    def is_evm_chain(self) -> bool:
        """Check if the manifest's chain is EVM-based."""
        from almanak.core.enums import CHAIN_FAMILY_MAP, Chain, ChainFamily

        try:
            chain_enum = Chain(self.chain.upper())
            return CHAIN_FAMILY_MAP.get(chain_enum) == ChainFamily.EVM
        except ValueError:
            # Unknown chain -- fail closed; don't produce zodiac targets for unrecognized chains
            return False

    def to_zodiac_targets(self) -> list[dict[str, Any]]:
        """Convert permissions to Zodiac Roles Target[] format.

        Returns a list of Target objects compatible with the Zodiac Roles
        modifier's ``applyTargets`` method. The format is consumed directly
        by the platform frontend to grant permissions via the Safe UI.

        Zodiac/Safe is EVM-only. Returns an empty list for non-EVM chains.

        Mapping:
            - target -> address (EIP-55 checksummed)
            - operation + send_allowed -> executionOptions bitmask
            - function_selectors -> clearance (2=Function, 1=Target if empty)
            - selectors -> functions[].selector with wildcarded=true
        """
        if not self.is_evm_chain:
            return []

        from almanak.framework.execution.signer.safe.constants import SafeOperation

        targets: list[dict[str, Any]] = []
        for perm in self.permissions:
            address = _eip55_checksum(perm.target)

            # executionOptions bitmask: bit 0 = Send, bit 1 = DelegateCall
            exec_options = 0
            if perm.send_allowed:
                exec_options |= 1  # Send
            if perm.operation == SafeOperation.DELEGATE_CALL:
                exec_options |= 2  # DelegateCall

            # clearance: 1=Target (all functions), 2=Function (specific selectors)
            has_selectors = len(perm.function_selectors) > 0
            clearance = 2 if has_selectors else 1

            target: dict[str, Any] = {
                "address": address,
                "clearance": clearance,
                "executionOptions": exec_options,
            }

            if has_selectors:
                target["functions"] = [
                    {"selector": sel.selector, "wildcarded": True} for sel in perm.function_selectors
                ]

            targets.append(target)

        return targets


def _eip55_checksum(address: str) -> str:
    """Apply EIP-55 mixed-case checksum to a hex address.

    Non-EVM addresses (e.g. Solana base58) are returned as-is since
    Zodiac Roles is EVM-only and checksumming doesn't apply.
    """
    if not address.startswith("0x"):
        return address
    from web3 import Web3

    return Web3.to_checksum_address(address)
