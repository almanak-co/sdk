"""Compact authoring for exact connector lifecycle declarations.

The effective capability matrix deliberately consumes flat, exact
``LifecycleObligationDecl`` leaves.  Production evidence commonly applies to
several explicitly reviewed cells, however, so repeating the same declaration
metadata hundreds of times would make manifest review error-prone.  This
module provides an authoring-only bundle which expands deterministically to
those existing leaves; it never discovers chains or intents from a connector.
"""

from __future__ import annotations

from dataclasses import dataclass

from almanak.connectors._connector_descriptor import LifecycleObligationDecl
from almanak.core.capability_obligations import (
    ExactTargetFeature,
    ObligationDeclaration,
    ObligationId,
    SupportClaim,
)
from almanak.core.chains import ChainDescriptor
from almanak.core.intent_types import IntentType

__all__ = ["LifecycleClaimCell", "LifecycleDeclarationBundle"]


@dataclass(frozen=True, init=False)
class LifecycleClaimCell:
    """One exact connector-owned claim cell used as bundle input."""

    protocol: str
    chain: ChainDescriptor
    intent: IntentType
    claim: SupportClaim
    exact_target_feature: ExactTargetFeature | None

    def __init__(
        self,
        *,
        protocol: str,
        chain: ChainDescriptor,
        intent: IntentType,
        claim: SupportClaim = SupportClaim.CORE_EXECUTION,
        exact_target_feature: ExactTargetFeature | None = None,
    ) -> None:
        if not isinstance(protocol, str) or not protocol or protocol != protocol.lower() or "-" in protocol:
            raise ValueError("LifecycleClaimCell.protocol must be lowercase and hyphen-free")
        if type(chain) is not ChainDescriptor:
            raise TypeError("LifecycleClaimCell.chain must be a ChainDescriptor")
        if type(intent) is not IntentType:
            raise TypeError("LifecycleClaimCell.intent must be an IntentType")
        if type(claim) is not SupportClaim:
            raise TypeError("LifecycleClaimCell.claim must be a SupportClaim")
        if exact_target_feature is not None and type(exact_target_feature) is not ExactTargetFeature:
            raise TypeError("exact_target_feature must be an ExactTargetFeature or None")
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "intent", intent)
        object.__setattr__(self, "claim", claim)
        object.__setattr__(self, "exact_target_feature", exact_target_feature)

    def sort_key(self) -> tuple[str, ...]:
        return (
            self.protocol,
            self.chain.name,
            self.intent.value,
            self.claim.value,
            self.exact_target_feature.value if self.exact_target_feature else "",
        )


@dataclass(frozen=True)
class LifecycleDeclarationBundle:
    """Expand uniform evidence dispositions over explicit exact claim cells."""

    bundle_id: str
    cells: tuple[LifecycleClaimCell, ...]
    declarations: tuple[ObligationDeclaration, ...]
    source_ref: str
    source_detail: str

    def __post_init__(self) -> None:
        for field_name in ("bundle_id", "source_ref", "source_detail"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"LifecycleDeclarationBundle.{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        if not isinstance(self.cells, tuple) or not self.cells:
            raise ValueError("LifecycleDeclarationBundle.cells must be a non-empty tuple")
        if not all(type(cell) is LifecycleClaimCell for cell in self.cells):
            raise TypeError("LifecycleDeclarationBundle.cells must contain LifecycleClaimCell values")
        cell_keys = [cell.sort_key() for cell in self.cells]
        if len(set(cell_keys)) != len(cell_keys):
            raise ValueError("LifecycleDeclarationBundle.cells contains duplicates")
        if tuple(sorted(self.cells, key=LifecycleClaimCell.sort_key)) != self.cells:
            raise ValueError("LifecycleDeclarationBundle.cells must use canonical order")
        if not isinstance(self.declarations, tuple) or not self.declarations:
            raise ValueError("LifecycleDeclarationBundle.declarations must be a non-empty tuple")
        if not all(type(item) is ObligationDeclaration for item in self.declarations):
            raise TypeError("LifecycleDeclarationBundle.declarations must contain ObligationDeclaration values")
        obligations = [item.obligation for item in self.declarations]
        if len(set(obligations)) != len(obligations):
            raise ValueError("LifecycleDeclarationBundle.declarations contains duplicate obligations")
        obligation_order = {obligation: index for index, obligation in enumerate(ObligationId)}
        if obligations != sorted(obligations, key=obligation_order.__getitem__):
            raise ValueError("LifecycleDeclarationBundle.declarations must use canonical obligation order")

    def expand(self) -> tuple[LifecycleObligationDecl, ...]:
        """Return deterministic flat leaves consumed by ``Connector``."""

        return tuple(
            LifecycleObligationDecl(
                protocol=cell.protocol,
                chain=cell.chain,
                intent=cell.intent,
                claim=cell.claim,
                exact_target_feature=cell.exact_target_feature,
                declaration=declaration,
                rule_id=f"{self.bundle_id}.{declaration.obligation.value}",
                source_ref=self.source_ref,
                source_detail=self.source_detail,
            )
            for cell in self.cells
            for declaration in self.declarations
        )
