"""Unit tests for the Solana protocol program registry (VIB-3753).

The registry is the single source of truth for protocol program IDs that
``solana-test-validator`` must clone from mainnet so strategies running
against the local fork don't hit ``ProgramAccountNotFound`` for Drift,
Orca, Raydium, Meteora, Jupiter, Kamino, etc.
"""

from __future__ import annotations

import re

import pytest

from almanak.framework.anvil.solana_program_registry import (
    DRIFT_PROGRAM_ID,
    JUPITER_V6_PROGRAM_ID,
    KAMINO_LENDING_PROGRAM_ID,
    METAPLEX_TOKEN_METADATA_PROGRAM_ID,
    SOLANA_PROTOCOL_PROGRAMS,
    SolanaProgramEntry,
    get_protocol_for_program_id,
    get_protocol_program_ids,
)

# Solana base58 program IDs are 32 to 44 characters and exclude 0/O/I/l.
_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


# =============================================================================
# Registry shape / consistency
# =============================================================================


class TestRegistryShape:
    def test_registry_is_non_empty(self) -> None:
        assert len(SOLANA_PROTOCOL_PROGRAMS) > 0

    def test_registry_is_a_tuple(self) -> None:
        # Tuple = immutable, prevents accidental in-place mutation by callers.
        assert isinstance(SOLANA_PROTOCOL_PROGRAMS, tuple)

    def test_each_entry_is_solana_program_entry(self) -> None:
        for entry in SOLANA_PROTOCOL_PROGRAMS:
            assert isinstance(entry, SolanaProgramEntry)

    def test_protocol_names_are_unique(self) -> None:
        names = [e.protocol for e in SOLANA_PROTOCOL_PROGRAMS]
        assert len(names) == len(set(names)), f"duplicate protocol entries: {names}"

    def test_program_ids_are_unique(self) -> None:
        ids = [e.program_id for e in SOLANA_PROTOCOL_PROGRAMS]
        assert len(ids) == len(set(ids)), f"duplicate program IDs: {ids}"

    def test_protocols_sorted_alphabetically_for_stable_diffs(self) -> None:
        # Convention documented in the registry module — keeps PR diffs
        # focused when adding a new connector.
        names = [e.protocol for e in SOLANA_PROTOCOL_PROGRAMS]
        assert names == sorted(names), (
            f"SOLANA_PROTOCOL_PROGRAMS must be alphabetical by protocol; got {names}"
        )

    def test_program_ids_are_valid_base58(self) -> None:
        for entry in SOLANA_PROTOCOL_PROGRAMS:
            assert _BASE58_RE.match(entry.program_id), (
                f"{entry.protocol} program ID is not valid Solana base58: {entry.program_id}"
            )


# =============================================================================
# Required protocols for VIB-3753
# =============================================================================
#
# These are the protocols whose strategies were broken by the empty
# validator: drift_perp_lifecycle_solana, edge_sol_orca_sol_usdc_lp,
# edge_sol_raydium_usds_lp, lst_depeg_arb (Jupiter swap leg),
# solana_meme_momentum (Jupiter swap leg). Each MUST appear in the
# registry — regressions here re-break the entire Solana strategy family.


REQUIRED_PROTOCOLS = (
    "drift",
    "jupiter",
    "kamino",
    "meteora",
    "metaplex_token_metadata",
    "orca",
    "raydium",
)


class TestRequiredProtocols:
    @pytest.mark.parametrize("protocol", REQUIRED_PROTOCOLS)
    def test_required_protocol_in_registry(self, protocol: str) -> None:
        names = {e.protocol for e in SOLANA_PROTOCOL_PROGRAMS}
        assert protocol in names, (
            f"{protocol} is required by VIB-3753 but not registered"
        )

    def test_drift_uses_connector_constant(self) -> None:
        # Sourced from connectors/drift/constants.py — keeps the registry
        # in sync with the connector's runtime program_id.
        from almanak.connectors.drift.constants import (
            DRIFT_PROGRAM_ID as CONNECTOR_DRIFT,
        )

        assert DRIFT_PROGRAM_ID == CONNECTOR_DRIFT
        assert get_protocol_for_program_id(CONNECTOR_DRIFT) == "drift"

    def test_orca_uses_connector_constant(self) -> None:
        from almanak.connectors.orca.constants import (
            WHIRLPOOL_PROGRAM_ID,
        )

        assert get_protocol_for_program_id(WHIRLPOOL_PROGRAM_ID) == "orca"

    def test_raydium_uses_connector_constant(self) -> None:
        from almanak.connectors.raydium.constants import (
            CLMM_PROGRAM_ID,
        )

        assert get_protocol_for_program_id(CLMM_PROGRAM_ID) == "raydium"

    def test_meteora_uses_connector_constant(self) -> None:
        from almanak.connectors.meteora.constants import (
            DLMM_PROGRAM_ID,
        )

        assert get_protocol_for_program_id(DLMM_PROGRAM_ID) == "meteora"

    def test_jupiter_v6_program_id_is_canonical(self) -> None:
        # Jupiter v6 program — published on Jupiter docs / Solana Explorer.
        assert JUPITER_V6_PROGRAM_ID == "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

    def test_kamino_lending_program_id_is_canonical(self) -> None:
        assert KAMINO_LENDING_PROGRAM_ID.startswith("KLend")

    def test_metaplex_token_metadata_constant_is_canonical(self) -> None:
        # Metaplex Token Metadata — required by Orca openPositionWithMetadata.
        assert (
            METAPLEX_TOKEN_METADATA_PROGRAM_ID
            == "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
        )


# =============================================================================
# get_protocol_program_ids() — assembled into validator CLI flags
# =============================================================================


class TestGetProtocolProgramIds:
    def test_no_filter_returns_all_unique_program_ids(self) -> None:
        ids = get_protocol_program_ids()
        registry_ids = [e.program_id for e in SOLANA_PROTOCOL_PROGRAMS]
        assert ids == registry_ids
        assert len(ids) == len(set(ids))

    def test_upgradeable_true_returns_only_upgradeable(self) -> None:
        ids = get_protocol_program_ids(upgradeable=True)
        for pid in ids:
            entry = next(e for e in SOLANA_PROTOCOL_PROGRAMS if e.program_id == pid)
            assert entry.upgradeable is True

    def test_upgradeable_false_returns_only_non_upgradeable(self) -> None:
        ids = get_protocol_program_ids(upgradeable=False)
        for pid in ids:
            entry = next(e for e in SOLANA_PROTOCOL_PROGRAMS if e.program_id == pid)
            assert entry.upgradeable is False

    def test_returns_list_not_tuple(self) -> None:
        # Callers (fork manager) extend with strategy-supplied clone_programs;
        # returning a list keeps the existing concatenation pattern intact.
        assert isinstance(get_protocol_program_ids(), list)

    def test_every_call_returns_a_fresh_list(self) -> None:
        # Mutating the result must not corrupt subsequent calls.
        first = get_protocol_program_ids()
        first.append("MUTATION_INJECTED")
        second = get_protocol_program_ids()
        assert "MUTATION_INJECTED" not in second


# =============================================================================
# get_protocol_for_program_id() — used in log messages
# =============================================================================


class TestGetProtocolForProgramId:
    def test_known_program_id_returns_protocol(self) -> None:
        assert get_protocol_for_program_id(JUPITER_V6_PROGRAM_ID) == "jupiter"
        assert get_protocol_for_program_id(DRIFT_PROGRAM_ID) == "drift"

    def test_unknown_program_id_returns_none(self) -> None:
        assert get_protocol_for_program_id("11111111111111111111111111111111") is None

    def test_empty_string_returns_none(self) -> None:
        assert get_protocol_for_program_id("") is None


# =============================================================================
# Integration with SolanaForkManager — VIB-3753 wiring
# =============================================================================


class TestForkManagerWiring:
    """The fork manager must consume the registry, not hardcode program IDs."""

    def test_default_clone_programs_matches_registry(self) -> None:
        from almanak.framework.anvil.solana_fork_manager import DEFAULT_CLONE_PROGRAMS

        assert DEFAULT_CLONE_PROGRAMS == get_protocol_program_ids(upgradeable=True)

    def test_default_clone_program_accounts_matches_registry(self) -> None:
        from almanak.framework.anvil.solana_fork_manager import (
            DEFAULT_CLONE_PROGRAM_ACCOUNTS,
        )

        assert DEFAULT_CLONE_PROGRAM_ACCOUNTS == get_protocol_program_ids(
            upgradeable=False
        )

    def test_jupiter_program_alias_still_exposed(self) -> None:
        # External callers may import JUPITER_PROGRAM from the fork manager.
        from almanak.framework.anvil.solana_fork_manager import JUPITER_PROGRAM

        assert JUPITER_PROGRAM == JUPITER_V6_PROGRAM_ID

    def test_validator_command_clones_every_registered_program(self, tmp_path) -> None:
        """End-to-end: every protocol in the registry produces a CLI flag."""
        from unittest.mock import patch

        from almanak.framework.anvil.solana_fork_manager import SolanaForkManager

        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._temp_dir = str(tmp_path)
        mgr._modified_mint_dir = str(tmp_path / "accounts")

        with patch("os.listdir", return_value=[]):
            cmd = mgr._build_validator_command()

        cmd_str = " ".join(cmd)
        for entry in SOLANA_PROTOCOL_PROGRAMS:
            assert entry.program_id in cmd_str, (
                f"{entry.protocol} program {entry.program_id} not in validator command"
            )

    def test_validator_command_uses_upgradeable_flag_for_upgradeable_programs(
        self, tmp_path
    ) -> None:
        from unittest.mock import patch

        from almanak.framework.anvil.solana_fork_manager import SolanaForkManager

        mgr = SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com")
        mgr._temp_dir = str(tmp_path)
        mgr._modified_mint_dir = str(tmp_path / "accounts")

        with patch("os.listdir", return_value=[]):
            cmd = mgr._build_validator_command()

        # Walk pairs of [flag, value]: every upgradeable program should appear
        # next to ``--clone-upgradeable-program``, never next to ``--clone``.
        upgradeable_ids = set(get_protocol_program_ids(upgradeable=True))
        seen_via_upgradeable: set[str] = set()
        for i, tok in enumerate(cmd[:-1]):
            if tok == "--clone-upgradeable-program":
                seen_via_upgradeable.add(cmd[i + 1])

        # Every upgradeable program in the registry should be in the
        # upgradeable bucket on the command line.
        assert upgradeable_ids.issubset(seen_via_upgradeable)
