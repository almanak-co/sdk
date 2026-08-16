"""Lazy registry for connector-owned exact-venue verifiers."""

from __future__ import annotations

from collections.abc import Iterable

from almanak.connectors._connector import CONNECTOR_REGISTRY, Connector, VenueVerifierDecl
from almanak.framework.venues import (
    BaseVenueVerifier,
    VenueBindingFailure,
    VenueVerificationEvidence,
    VenueVerificationRequest,
    VenueVerificationResult,
    VerifiedVenueBinding,
)


class VenueVerifierRegistryError(ValueError):
    """A venue-verifier declaration or result violates the registry contract."""


class VenueVerifierRegistry:
    """Exact protocol-key registry with lazy symbolic verifier loading."""

    def __init__(self, connectors: Iterable[Connector] | None = None) -> None:
        selected = CONNECTOR_REGISTRY.with_venue_verifiers() if connectors is None else tuple(connectors)
        if any(type(connector) is not Connector for connector in selected):
            raise TypeError("VenueVerifierRegistry connectors must contain exact Connector values")
        self._declarations = self._index(tuple(selected))

    @staticmethod
    def _index(connectors: tuple[Connector, ...]) -> dict[str, VenueVerifierDecl]:
        declarations: dict[str, VenueVerifierDecl] = {}
        owners: dict[str, str] = {}
        for connector in connectors:
            for declaration in connector.venue_verifiers:
                if declaration.protocol not in connector.protocol_keys:
                    raise VenueVerifierRegistryError(
                        f"connector {connector.name!r} does not own venue-verifier protocol {declaration.protocol!r}"
                    )
                owner = owners.get(declaration.protocol)
                if owner is not None:
                    raise VenueVerifierRegistryError(
                        f"venue-verifier protocol {declaration.protocol!r} is declared by both "
                        f"{owner!r} and {connector.name!r}"
                    )
                owners[declaration.protocol] = connector.name
                declarations[declaration.protocol] = declaration
        return declarations

    @staticmethod
    def _normalize_protocol(protocol: str) -> str:
        if type(protocol) is not str or not protocol.strip():
            raise VenueVerifierRegistryError("protocol must be a non-empty string")
        return protocol.strip().lower().replace("-", "_")

    def supported_protocols(self) -> tuple[str, ...]:
        """Return exact declared protocol keys in canonical order."""
        return tuple(sorted(self._declarations))

    def has(self, protocol: str) -> bool:
        """Return whether ``protocol`` resolves to an exact verifier declaration."""
        return self._normalize_protocol(protocol) in self._declarations

    def declaration(self, protocol: str) -> VenueVerifierDecl:
        """Return the exact declaration or fail closed for an unknown protocol."""
        key = self._normalize_protocol(protocol)
        declaration = self._declarations.get(key)
        if declaration is None:
            raise VenueVerifierRegistryError(f"no exact venue verifier is declared for protocol {key!r}")
        return declaration

    def load_class(self, protocol: str) -> type[BaseVenueVerifier]:
        """Load, but do not instantiate, the connector-owned verifier class."""
        declaration = self.declaration(protocol)
        verifier_class = declaration.verifier.load()
        if not isinstance(verifier_class, type) or not issubclass(verifier_class, BaseVenueVerifier):
            raise VenueVerifierRegistryError(f"{declaration.verifier_ref} is not a BaseVenueVerifier subclass")
        return verifier_class

    def validate_result(
        self,
        request: VenueVerificationRequest,
        result: VenueVerificationResult,
    ) -> VenueVerificationResult:
        """Bind a verifier result to its exact manifest declaration and request."""
        if type(request) is not VenueVerificationRequest:
            raise TypeError("request must be an exact VenueVerificationRequest")
        if type(result) not in (VerifiedVenueBinding, VenueBindingFailure):
            raise TypeError("result must be an exact venue verification result")
        declaration = self.declaration(request.protocol)
        self._validate_request(declaration, request)
        if type(result) is VerifiedVenueBinding:
            self._validate_success(declaration, request, result)
        elif result.evidence is not None:
            self._validate_evidence(declaration, request, result.evidence)
        return result

    @staticmethod
    def _validate_request(declaration: VenueVerifierDecl, request: VenueVerificationRequest) -> None:
        if request.chain not in declaration.chains:
            raise VenueVerifierRegistryError(
                f"venue verifier {declaration.protocol!r} does not declare chain {request.chain!r}"
            )
        if request.primitive not in declaration.primitives:
            raise VenueVerifierRegistryError(
                f"venue verifier {declaration.protocol!r} does not declare primitive {request.primitive.value!r}"
            )
        if request.binding_policy_version != declaration.binding_policy_version:
            raise VenueVerifierRegistryError("venue verification request uses the wrong binding policy version")
        names = tuple(component.name for component in request.binding_components)
        if names != declaration.component_names:
            raise VenueVerifierRegistryError(
                f"venue verification request component schema {names!r} does not match {declaration.component_names!r}"
            )

    @classmethod
    def _validate_success(
        cls,
        declaration: VenueVerifierDecl,
        request: VenueVerificationRequest,
        result: VerifiedVenueBinding,
    ) -> None:
        binding = result.binding
        if (
            binding.chain != request.chain
            or binding.protocol != request.protocol
            or binding.primitive is not request.primitive
            or binding.identity_refs != request.requested_refs
            or binding.ordered_assets != request.ordered_assets
            or binding.binding_components != request.binding_components
            or binding.binding_policy_version != request.binding_policy_version
        ):
            raise VenueVerifierRegistryError("verified venue binding does not exactly match its request")
        cls._validate_evidence(declaration, request, result.evidence)

    @staticmethod
    def _validate_evidence(declaration: VenueVerifierDecl, request: VenueVerificationRequest, evidence: object) -> None:
        if type(evidence) is not VenueVerificationEvidence:
            raise TypeError("venue result evidence must be exact VenueVerificationEvidence")
        if evidence.chain != request.chain:
            raise VenueVerifierRegistryError("venue verification evidence chain does not match the request")
        if evidence.verifier_ref != declaration.verifier_ref:
            raise VenueVerifierRegistryError("venue verification evidence names the wrong verifier")
        if evidence.verifier_contract_version != declaration.contract_version:
            raise VenueVerifierRegistryError("venue verification evidence uses the wrong verifier contract version")


__all__ = ["VenueVerifierRegistry", "VenueVerifierRegistryError"]
