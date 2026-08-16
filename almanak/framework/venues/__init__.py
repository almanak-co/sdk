"""Writer-safe exact-venue contracts."""

from .gateway import GatewayClientVenueVerificationGateway
from .receipt import VenueReceiptCorrelationError, correlate_verified_venue_receipts
from .types import (
    VENUE_BINDING_SCHEMA_VERSION,
    ExactVenueBinding,
    VenueBindingComponent,
    VenueBindingFailure,
    VenueBindingFailureReason,
    VenueBindingFailureState,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    VenueVerificationResult,
    VerifiedVenueBinding,
    build_verified_venue_binding,
    canonical_venue_binding_preimage_bytes,
)
from .verifier import BaseVenueVerifier, VenueVerificationGateway, VenueVerificationRequest

__all__ = [
    "VENUE_BINDING_SCHEMA_VERSION",
    "BaseVenueVerifier",
    "ExactVenueBinding",
    "VenueBindingComponent",
    "VenueBindingFailure",
    "VenueBindingFailureReason",
    "VenueBindingFailureState",
    "VenueObservedFact",
    "VenueReferenceNamespace",
    "VenueReceiptCorrelationError",
    "VenueTargetRef",
    "VenueTargetRole",
    "VenueVerificationEvidence",
    "VenueVerificationGateway",
    "GatewayClientVenueVerificationGateway",
    "VenueVerificationRequest",
    "VenueVerificationResult",
    "VerifiedVenueBinding",
    "build_verified_venue_binding",
    "canonical_venue_binding_preimage_bytes",
    "correlate_verified_venue_receipts",
]
