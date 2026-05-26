"""Solana protocol program registry for solana-test-validator startup.

A `solana-test-validator` boots empty (slot 0) by default. Strategies that
touch real on-chain protocols (Drift, Orca, Raydium, Meteora, Jupiter,
Kamino, ...) hit ``ProgramAccountNotFound`` because the protocol programs
aren't deployed on the local ledger.

This module is the single source of truth for the protocol program IDs
that ``SolanaForkManager`` clones from mainnet via
``--clone-upgradeable-program <PROGRAM_ID>`` so every Solana strategy can
execute against a faithful local fork.

Each entry pulls the program ID from the connector's own ``constants``
module where possible — connectors already own their program ID for
runtime instruction building, so re-defining the value here would create
a maintenance trap. Where a connector relies on a REST API and does not
itself encode the program ID (Jupiter, Kamino), the program ID is
recorded here with a provenance comment pointing at the upstream source.

Why explicit ``--clone-program`` over ``--warp-slot`` (VIB-3753 design call):
- Deterministic / reproducible: the registry is the only thing that
  changes the validator's program set.
- Doesn't drag random unrelated programs into the validator state.
- Aligns with the connector-by-connector model — adding a new Solana
  connector means appending one row here, mirroring how EVM chain support
  is added one entry at a time elsewhere in the framework.

Adding a new Solana connector
-----------------------------
1. Add its program ID constant in ``connectors/<name>/constants.py``.
2. Append a ``SolanaProgramEntry`` to ``SOLANA_PROTOCOL_PROGRAMS`` below,
   importing the constant from the connector module.
3. Run the unit tests (``tests/unit/anvil/test_solana_program_registry.py``).
"""

from __future__ import annotations

from dataclasses import dataclass

# Connector-owned program IDs. Importing the constants module directly
# avoids triggering each connector's full ``__init__`` (which pulls in the
# adapter, SDK, and receipt parser). The constants modules themselves do
# perform light import-time work — env-var lookups for things like
# ``DRIFT_DATA_API_BASE_URL`` — but no network calls and no expensive
# initialization.
from almanak.connectors.drift.constants import DRIFT_PROGRAM_ID
from almanak.connectors.meteora.constants import DLMM_PROGRAM_ID
from almanak.connectors.orca.constants import (
    METADATA_PROGRAM_ID as ORCA_METADATA_PROGRAM_ID,
)
from almanak.connectors.orca.constants import WHIRLPOOL_PROGRAM_ID
from almanak.connectors.raydium.constants import CLMM_PROGRAM_ID

# =============================================================================
# Program IDs not currently exposed by their connector modules
# =============================================================================
#
# Jupiter and Kamino connectors hit a REST API that returns pre-built
# ``VersionedTransaction``s, so they have no need for the program ID at
# instruction-build time. The validator still needs to clone these programs
# so the resulting transactions can execute on the local fork.
#
# Each ID has been verified against ``getAccountInfo`` on
# ``api.mainnet-beta.solana.com`` — the account exists, is executable, and
# is owned by ``BPFLoaderUpgradeab1e11111111111111111111111`` (which is
# what allows ``--clone-upgradeable-program`` to succeed against it).
#
# Provenance:
# - Jupiter v6 Aggregator: https://station.jup.ag/docs/apis/swap-api
#   (Program ID published on Jupiter docs and Solana Explorer.)
# - Kamino Lending V2 (KLend): https://docs.kamino.finance/
#   (Anchor-deployed program; ID matches the program owner of every
#   reserve account returned by the Kamino REST API.)
#
# Note on Jupiter Lend (Earn): the connector uses the REST API at
# ``https://api.jup.ag/lend`` and the underlying on-chain program ID is
# not currently published in the connector or this codebase. Tracked as
# VIB-3784 — once the program ID is captured and verified via
# ``getAccountInfo`` against ``api.mainnet-beta.solana.com``, append a
# new ``SolanaProgramEntry`` below in the same pattern as Jupiter v6 / Kamino.

JUPITER_V6_PROGRAM_ID = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
KAMINO_LENDING_PROGRAM_ID = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"

# Metaplex Token Metadata is required by Orca's openPositionWithMetadata
# instruction (Position NFTs). Re-export the connector's value under the
# registry's neutral name so the ``__init__`` order in the registry is
# stable even if Raydium also references it.
METAPLEX_TOKEN_METADATA_PROGRAM_ID = ORCA_METADATA_PROGRAM_ID


# =============================================================================
# Registry data model
# =============================================================================


@dataclass(frozen=True)
class SolanaProgramEntry:
    """A protocol program that the local validator must clone from mainnet.

    Attributes:
        protocol: Short protocol name used in logs (``drift``, ``orca``, ...).
        program_id: Solana program address (base58).
        upgradeable: When True, clone via ``--clone-upgradeable-program``;
            when False, clone via ``--clone``. All Anchor / BPFLoaderUpgradeable
            programs (the vast majority of modern Solana protocols) are
            upgradeable; some older non-upgradeable BPF programs (e.g. some
            Metaplex deployments) are not.
        notes: Optional free-form provenance note shown in debug logs only.
    """

    protocol: str
    program_id: str
    upgradeable: bool = True
    notes: str = ""


# =============================================================================
# The registry
# =============================================================================
#
# Order matters only for log readability — the validator does not care.
# Keep alphabetical by protocol so diffs are easy to review.

SOLANA_PROTOCOL_PROGRAMS: tuple[SolanaProgramEntry, ...] = (
    SolanaProgramEntry(
        protocol="drift",
        program_id=DRIFT_PROGRAM_ID,
        notes="Drift V2 perpetual futures (Anchor program).",
    ),
    SolanaProgramEntry(
        protocol="jupiter",
        program_id=JUPITER_V6_PROGRAM_ID,
        notes="Jupiter v6 aggregator — required for any Jupiter-routed swap.",
    ),
    SolanaProgramEntry(
        protocol="kamino",
        program_id=KAMINO_LENDING_PROGRAM_ID,
        notes="Kamino Lending V2 (KLend).",
    ),
    SolanaProgramEntry(
        protocol="metaplex_token_metadata",
        program_id=METAPLEX_TOKEN_METADATA_PROGRAM_ID,
        notes="Required by Orca openPositionWithMetadata for LP NFTs.",
    ),
    SolanaProgramEntry(
        protocol="meteora",
        program_id=DLMM_PROGRAM_ID,
        notes="Meteora DLMM (discrete-bin liquidity book).",
    ),
    SolanaProgramEntry(
        protocol="orca",
        program_id=WHIRLPOOL_PROGRAM_ID,
        notes="Orca Whirlpools concentrated liquidity (CLMM).",
    ),
    SolanaProgramEntry(
        protocol="raydium",
        program_id=CLMM_PROGRAM_ID,
        notes="Raydium CLMM concentrated liquidity.",
    ),
)


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
