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
