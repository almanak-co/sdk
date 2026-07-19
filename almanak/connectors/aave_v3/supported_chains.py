"""Aave V3 strategy-side chain coverage.

Declares the chains on which the Aave V3 connector is alive. Owned by the
connector (one folder = one declaration) so adding a chain is a single edit
here, and the strategy-side ``protocol -> {chains}`` matrix
(:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`) is derived
by iterating the per-connector registry rather than hand-maintained.

See ``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator and ``blueprints/05-connectors.md`` for the self-containment
rationale (add a connector = declare its chains in its own folder).
"""

from __future__ import annotations

# protocol identifier → chains the connector runs on. One module MAY own
# several identifiers when a single connector backs more than one protocol
# key (mirrors ``capabilities.py``'s ``PROTOCOL_CAPABILITIES`` shape).
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    # Runtime chain gate for Aave V3. Kept EXACTLY consistent with the
    # canonical connector manifest's ``strategy_chains`` (VIB-5916): both feed
    # the same shipped lending surface, so the runtime gate must not admit a
    # chain the manifest does not advertise as tested support. ``plasma`` was
    # removed here alongside dropping it from the manifest — its token
    # catalogue is incomplete and it was never proven. ``addresses.py`` still
    # ships plasma pool data (data availability is independent of the shipped
    # support claim); re-add plasma here AND to the manifest together once a
    # proof run lands.
    "aave_v3": frozenset(
        {
            "ethereum",
            "arbitrum",
            "optimism",
            "polygon",
            "base",
            "avalanche",
            "bsc",
            "linea",
            "xlayer",
            "mantle",
        }
    ),
}
