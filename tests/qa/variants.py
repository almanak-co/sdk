"""A variant proof must retain the actual connector target it exercised."""

import json

from almanak.connectors.identifiers import ConnectorId, ProtocolVariant


def matches(connector, target, variant):
    if variant is None:
        return True
    if not isinstance(variant, ProtocolVariant):
        raise ValueError("QA variant must use its validated identity")
    if connector == ConnectorId("curve"):
        from almanak.connectors.curve.domain import protocol_variant

        pool = target["pool"]
        return protocol_variant(pool["family"], pool.get("implementation_revision")) == variant
    if connector == ConnectorId("aerodrome"):
        from almanak.connectors.aerodrome.domain import AerodromePool

        return AerodromePool.model_validate_json(json.dumps(target["pool"])).variant == variant
    raise ValueError("QA protocol variant mapping is missing for this connector")
