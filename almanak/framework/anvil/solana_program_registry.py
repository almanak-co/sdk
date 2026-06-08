"""Solana protocol program registry for solana-test-validator startup.

A `solana-test-validator` boots empty (slot 0) by default. Strategies that
touch real on-chain protocols (Drift, Orca, Raydium, Meteora, Jupiter,
Kamino, ...) hit ``ProgramAccountNotFound`` because the protocol programs
aren't deployed on the local ledger.

The program IDs are connector-owned data published from each connector's
``CONNECTOR.solana_programs`` manifest field. This module composes that
manifest data into the stable runtime view that ``SolanaForkManager`` clones
from mainnet via ``--clone-upgradeable-program <PROGRAM_ID>`` so every Solana
strategy can execute against a faithful local fork.

Why explicit ``--clone-program`` over ``--warp-slot`` (VIB-3753 design call):
- Deterministic / reproducible: the registry is the only thing that
  changes the validator's program set.
- Doesn't drag random unrelated programs into the validator state.
- Aligns with the connector-by-connector model: adding a new Solana
  connector means adding one manifest spec in that connector folder,
  mirroring how other connector-owned capabilities are declared.

Adding a new Solana connector
-----------------------------
1. Add its program ID constant in ``connectors/<name>/constants.py`` when the
   connector runtime also needs it, or define the verified literal in
   ``connectors/<name>/connector.py`` for REST-only protocols.
2. Publish a ``SolanaProgramSpec`` from ``CONNECTOR.solana_programs``.
3. Run the unit tests (``tests/unit/anvil/test_solana_program_registry.py``).
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec

# =============================================================================
# Registry data model
# =============================================================================


class SolanaProgramEntry(SolanaProgramSpec):
    """A protocol program that the local validator must clone from mainnet.

    Compatibility subclass for the historical framework export. Connector
    manifests publish ``SolanaProgramSpec``; framework callers keep receiving
    ``SolanaProgramEntry`` instances from ``SOLANA_PROTOCOL_PROGRAMS``.
    """


def _build_solana_protocol_programs() -> tuple[SolanaProgramEntry, ...]:
    """Compose connector-owned Solana program specs into a stable registry."""
    entries: list[SolanaProgramEntry] = []
    protocol_owners: dict[str, str] = {}
    program_id_owners: dict[str, str] = {}

    for connector_manifest in CONNECTOR_REGISTRY.with_solana_programs():
        if connector_manifest.solana_programs is None:
            continue
        for spec in connector_manifest.solana_programs:
            protocol_owner = protocol_owners.get(spec.protocol)
            if protocol_owner is not None:
                raise ValueError(
                    f"Solana program protocol {spec.protocol!r} is declared by both "
                    f"{protocol_owner!r} and {connector_manifest.name!r}"
                )
            program_id_owner = program_id_owners.get(spec.program_id)
            if program_id_owner is not None:
                raise ValueError(
                    f"Solana program ID {spec.program_id!r} is declared by both "
                    f"{program_id_owner!r} and {connector_manifest.name!r}"
                )
            protocol_owners[spec.protocol] = connector_manifest.name
            program_id_owners[spec.program_id] = connector_manifest.name
            entries.append(
                SolanaProgramEntry(
                    protocol=spec.protocol,
                    program_id=spec.program_id,
                    upgradeable=spec.upgradeable,
                    notes=spec.notes,
                )
            )

    return tuple(sorted(entries, key=lambda entry: entry.protocol))


SOLANA_PROTOCOL_PROGRAMS: tuple[SolanaProgramEntry, ...] = _build_solana_protocol_programs()


def _required_program_id(protocol: str) -> str:
    """Return a required program id for backwards-compatible module constants."""
    program_id = next((entry.program_id for entry in SOLANA_PROTOCOL_PROGRAMS if entry.protocol == protocol), None)
    if program_id is None:
        raise ValueError(f"Required Solana program {protocol!r} is not published by any connector manifest")
    return program_id


DRIFT_PROGRAM_ID = _required_program_id("drift")
DLMM_PROGRAM_ID = _required_program_id("meteora")
JUPITER_V6_PROGRAM_ID = _required_program_id("jupiter")
KAMINO_LENDING_PROGRAM_ID = _required_program_id("kamino")
METAPLEX_TOKEN_METADATA_PROGRAM_ID = _required_program_id("metaplex_token_metadata")
WHIRLPOOL_PROGRAM_ID = _required_program_id("orca")
CLMM_PROGRAM_ID = _required_program_id("raydium")


def get_protocol_program_ids(*, upgradeable: bool | None = None) -> list[str]:
    """Return program IDs registered for cloning.

    Args:
        upgradeable: If None, return all program IDs. If True/False, filter
            to only upgradeable / non-upgradeable programs respectively.

    Returns:
        Deduplicated list of program ID strings, preserving registry order.
    """
    seen: set[str] = set()
    out: list[str] = []
    for entry in SOLANA_PROTOCOL_PROGRAMS:
        if upgradeable is not None and entry.upgradeable is not upgradeable:
            continue
        if entry.program_id in seen:
            continue
        seen.add(entry.program_id)
        out.append(entry.program_id)
    return out


def get_protocol_for_program_id(program_id: str) -> str | None:
    """Reverse-lookup the protocol name for a given program ID.

    Returns the protocol of the first entry matching ``program_id``, or
    ``None`` if not registered. Useful for log messages and debugging.
    """
    for entry in SOLANA_PROTOCOL_PROGRAMS:
        if entry.program_id == program_id:
            return entry.protocol
    return None


__all__ = [
    "DRIFT_PROGRAM_ID",
    "DLMM_PROGRAM_ID",
    "JUPITER_V6_PROGRAM_ID",
    "KAMINO_LENDING_PROGRAM_ID",
    "METAPLEX_TOKEN_METADATA_PROGRAM_ID",
    "SOLANA_PROTOCOL_PROGRAMS",
    "SolanaProgramEntry",
    "WHIRLPOOL_PROGRAM_ID",
    "CLMM_PROGRAM_ID",
    "get_protocol_for_program_id",
    "get_protocol_program_ids",
]
