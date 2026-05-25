"""Tests for the ChainRegistry / ChainDescriptor system (VIB-4801).

Three layers of guarantee:

1. **Structural** — every ``Chain`` enum member has a registered
   ``ChainDescriptor``; names / aliases / chain_ids are well-formed; the
   registry round-trips correctly.
2. **Legacy byte-identity** — every derived view (CHAIN_IDS,
   ALLOWED_CHAINS, the 6 gas / timeout dicts, NATIVE_TOKEN_INFO,
   NATIVE_TOKEN_SYMBOLS, CHAIN_NATIVE_SYMBOL, fork_manager.CHAIN_IDS) is
   byte-identical to the literal it replaced.
3. **Import-graph isolation** — the registry module ``almanak.core.chains``
   does not import any framework / connector / strategy code. The
   gateway's ``ALLOWED_CHAINS`` allowlist must be deterministic at import
   time, not influenced by which connectors load.
"""

from __future__ import annotations

import sys

import pytest

from almanak.core.chains import (
    ChainDescriptor,
    ChainRegistry,
    GasProfile,
    NativeToken,
    Timeouts,
)
from almanak.core.enums import Chain, ChainFamily

# ---------------------------------------------------------------------------
# Snapshots of the historical literal dicts (frozen at PR-merge time).
#
# These are the trust-boundary regression guards. If any of these
# assertions fail it means the registry-derived view has drifted from the
# pre-VIB-4801 behavior — that needs a deliberate decision, not a silent
# diff.
# ---------------------------------------------------------------------------

HISTORICAL_CHAIN_IDS = {
    Chain.ETHEREUM: 1,
    Chain.ARBITRUM: 42161,
    Chain.OPTIMISM: 10,
    Chain.BASE: 8453,
    Chain.AVALANCHE: 43114,
    Chain.POLYGON: 137,
    Chain.BSC: 56,
    Chain.SONIC: 146,
    Chain.PLASMA: 9745,
    Chain.BLAST: 81457,
    Chain.LINEA: 59144,
    Chain.MANTLE: 5000,
    Chain.BERACHAIN: 80094,
    Chain.MONAD: 143,
    Chain.XLAYER: 196,
    Chain.ZEROG: 16661,
    Chain.SOLANA: 0,
}

HISTORICAL_ALLOWED_CHAINS = frozenset(
    {
        "ethereum",
        "arbitrum",
        "base",
        "optimism",
        "polygon",
        "avalanche",
        "bsc",
        "sonic",
        "plasma",
        "linea",
        "blast",
        "mantle",
        "berachain",
        "solana",
        "monad",
        "xlayer",
        "zerog",
    }
)

HISTORICAL_CHAIN_GAS_BUFFERS = {
    "ethereum": 1.1,
    "arbitrum": 1.5,
    "optimism": 1.5,
    "polygon": 1.2,
    "base": 1.5,
    "avalanche": 1.1,
    "bsc": 1.2,
    "linea": 1.5,
    "plasma": 1.1,
    "blast": 1.5,
    "mantle": 1.5,
    "berachain": 1.2,
    "monad": 1.1,
    "xlayer": 1.3,
    "zerog": 1.1,
}

HISTORICAL_CHAIN_GAS_PRICE_CAPS_GWEI = {
    "ethereum": 300,
    "arbitrum": 10,
    "optimism": 10,
    "polygon": 500,
    "base": 10,
    "avalanche": 100,
    "bsc": 20,
    "linea": 10,
    "plasma": 50,
    "blast": 10,
    "mantle": 10,
    "berachain": 50,
    "sonic": 100,
    "monad": 50,
    "xlayer": 10,
    "zerog": 50,
}

HISTORICAL_CHAIN_GAS_COST_CAPS_NATIVE = {
    "ethereum": 0.1,
    "arbitrum": 0.01,
    "optimism": 0.01,
    "polygon": 50.0,
    "base": 0.01,
    "avalanche": 1.0,
    "bsc": 0.05,
    "mantle": 50.0,
    "berachain": 10.0,
    "monad": 10.0,
    "xlayer": 1.0,
    "zerog": 10.0,
}

HISTORICAL_CHAIN_TX_TIMEOUTS = {
    "ethereum": 300,
    "arbitrum": 120,
    "optimism": 120,
    "polygon": 180,
    "base": 120,
    "avalanche": 120,
    "plasma": 120,
    "mantle": 120,
    "berachain": 120,
    "monad": 60,
    "xlayer": 120,
    "zerog": 120,
}

HISTORICAL_CHAIN_GRPC_EXECUTE_TIMEOUTS = {
    "ethereum": 600,
    "arbitrum": 300,
    "optimism": 300,
    "polygon": 360,
    "base": 300,
    "avalanche": 300,
    "plasma": 300,
    "bsc": 300,
    "sonic": 300,
    "mantle": 300,
    "berachain": 300,
    "monad": 240,
    "xlayer": 300,
    "zerog": 300,
}

HISTORICAL_CHAIN_SIMULATION_BUFFERS = {
    "ethereum": 0.1,
    "arbitrum": 0.5,
    "optimism": 0.5,
    "polygon": 0.2,
    "base": 0.5,
    "avalanche": 0.1,
    "bsc": 0.1,
    "linea": 0.3,
    "plasma": 0.1,
    "blast": 0.5,
    "mantle": 0.5,
    "berachain": 0.2,
    "sonic": 0.1,
    "monad": 0.1,
    "xlayer": 0.3,
    "zerog": 0.1,
}

# Pre-VIB-4801 ``NATIVE_TOKEN_SYMBOLS`` literal from
# ``almanak/gateway/data/balance/web3_provider.py``. Covered all 17 chains.
HISTORICAL_NATIVE_TOKEN_SYMBOLS = {
    "ethereum": "ETH",
    "arbitrum": "ETH",
    "optimism": "ETH",
    "polygon": "MATIC",
    "base": "ETH",
    "avalanche": "AVAX",
    "bsc": "BNB",
    "sonic": "S",
    "blast": "ETH",
    "linea": "ETH",
    "plasma": "XPL",
    "mantle": "MNT",
    "berachain": "BERA",
    "monad": "MON",
    "xlayer": "OKB",
    "zerog": "A0GI",
    "solana": "SOL",
}

# Pre-VIB-4801 ``NATIVE_TOKEN_INFO`` literal from
# ``almanak/gateway/services/onchain_lookup.py``. Covered only 9 chains —
# the registry-derived view now exposes all 16 EVM chains; the byte-identity
# test below asserts the historical entries remain unchanged (superset
# semantics, deliberate expansion documented in the PR body).
HISTORICAL_NATIVE_TOKEN_INFO = {
    "ethereum": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "arbitrum": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "optimism": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "base": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "polygon": {"symbol": "MATIC", "name": "Polygon", "decimals": 18},
    "avalanche": {"symbol": "AVAX", "name": "Avalanche", "decimals": 18},
    "bsc": {"symbol": "BNB", "name": "BNB", "decimals": 18},
    "sonic": {"symbol": "S", "name": "Sonic", "decimals": 18},
    "plasma": {"symbol": "XPL", "name": "Plasma", "decimals": 18},
}

# Pre-VIB-4801 ``ManagedGateway.CHAIN_NATIVE_SYMBOL`` literal. Covered all
# 16 EVM chains (Solana excluded — it's not Anvil-fundable).
HISTORICAL_CHAIN_NATIVE_SYMBOL = {
    "ethereum": "ETH",
    "arbitrum": "ETH",
    "optimism": "ETH",
    "base": "ETH",
    "polygon": "MATIC",
    "avalanche": "AVAX",
    "bsc": "BNB",
    "sonic": "S",
    "blast": "ETH",
    "linea": "ETH",
    "plasma": "XPL",
    "mantle": "MNT",
    "berachain": "BERA",
    "monad": "MON",
    "xlayer": "OKB",
    "zerog": "A0GI",
}


# ---------------------------------------------------------------------------
# 1. Structural — every Chain has a descriptor; roundtrips work
# ---------------------------------------------------------------------------


class TestRegistryStructure:
    """Structural invariants of the registry."""

    def test_every_chain_enum_has_descriptor(self) -> None:
        registered = {d.enum for d in ChainRegistry.all()}
        missing = set(Chain) - registered
        assert not missing, (
            f"Chain enum members without a descriptor: {[c.name for c in missing]}. "
            f"Add a file under almanak/core/chains/."
        )

    def test_no_descriptors_outside_chain_enum(self) -> None:
        registered = {d.enum for d in ChainRegistry.all()}
        stray = registered - set(Chain)
        assert not stray, (
            f"Descriptors registered for enum values not in Chain: {stray}. "
            f"Either add to Chain or remove the stray descriptor."
        )

    @pytest.mark.parametrize("chain", list(Chain), ids=lambda c: c.name)
    def test_descriptor_name_matches_enum_lowercase(self, chain: Chain) -> None:
        d = ChainRegistry.get(chain)
        assert d.name == chain.name.lower()

    @pytest.mark.parametrize("chain", list(Chain), ids=lambda c: c.name)
    def test_roundtrip_name_to_enum_to_name(self, chain: Chain) -> None:
        d = ChainRegistry.get(chain)
        d2 = ChainRegistry.resolve(d.name)
        assert d is d2
        assert d2.enum is chain

    def test_alias_resolution_is_case_insensitive(self) -> None:
        # Canonical name
        assert ChainRegistry.resolve("ethereum").enum is Chain.ETHEREUM
        # Aliases
        assert ChainRegistry.resolve("eth").enum is Chain.ETHEREUM
        assert ChainRegistry.resolve("ETH").enum is Chain.ETHEREUM
        assert ChainRegistry.resolve("  bnb  ").enum is Chain.BSC
        assert ChainRegistry.resolve("0g").enum is Chain.ZEROG

    def test_unknown_name_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown chain"):
            ChainRegistry.resolve("not-a-chain")

    def test_by_id_returns_correct_descriptor(self) -> None:
        assert ChainRegistry.by_id(1).enum is Chain.ETHEREUM
        assert ChainRegistry.by_id(42161).enum is Chain.ARBITRUM
        assert ChainRegistry.by_id(8453).enum is Chain.BASE
        with pytest.raises(ValueError, match="Unknown chain_id"):
            ChainRegistry.by_id(999_999_999)

    def test_register_rejects_duplicate_enum(self) -> None:
        """Two descriptors for the same Chain must raise."""
        with pytest.raises(ValueError, match="Duplicate ChainDescriptor"):
            ChainRegistry.register(
                ChainDescriptor(
                    enum=Chain.ETHEREUM,
                    name="ethereum",
                    chain_id=1,
                    family=ChainFamily.EVM,
                    native=NativeToken(symbol="ETH", name="Ethereum", decimals=18),
                    gas=GasProfile(),
                    timeouts=Timeouts(),
                )
            )

    def test_descriptor_post_init_rejects_name_drift(self) -> None:
        """Descriptor name must equal enum.name.lower()."""
        with pytest.raises(ValueError, match="must equal enum name"):
            ChainDescriptor(
                enum=Chain.ETHEREUM,
                name="ETH",  # wrong case
                chain_id=1,
                family=ChainFamily.EVM,
                native=NativeToken(symbol="ETH", name="Ethereum", decimals=18),
                gas=GasProfile(),
                timeouts=Timeouts(),
            )

    def test_register_rejects_canonical_name_collision_with_existing_alias(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If a prior chain's alias has claimed the same string as the new
        descriptor's canonical name, register() must raise rather than
        silently overwrite the alias mapping in _by_name.
        """
        bsc_descriptor = ChainRegistry.get(Chain.BSC)
        # Forge the corrupt state: an alias entry under "ethereum" owned by BSC.
        monkeypatch.setitem(ChainRegistry._by_name, "ethereum", bsc_descriptor)
        # Evict Chain.ETHEREUM from _by_enum so the enum preflight passes.
        monkeypatch.delitem(ChainRegistry._by_enum, Chain.ETHEREUM)

        with pytest.raises(ValueError, match="Canonical name.*collides"):
            ChainRegistry.register(
                ChainDescriptor(
                    enum=Chain.ETHEREUM,
                    name="ethereum",
                    chain_id=1,
                    family=ChainFamily.EVM,
                    native=NativeToken(symbol="ETH", name="Ethereum", decimals=18),
                    gas=GasProfile(),
                    timeouts=Timeouts(),
                )
            )


# ---------------------------------------------------------------------------
# 2. Wire-format regression — chain_ids MUST match metrics-database values
# ---------------------------------------------------------------------------


class TestWireFormatStability:
    """The integer chain_id values are on-the-wire identifiers owned by
    ``metrics-database``. **Restructuring how we source them is fine;
    renumbering them is not.** Every chain_id must match the historical
    literal byte-for-byte.
    """

    @pytest.mark.parametrize(
        "chain,expected_id",
        list(HISTORICAL_CHAIN_IDS.items()),
        ids=lambda v: v.name if isinstance(v, Chain) else str(v),
    )
    def test_chain_id_matches_historical_value(self, chain: Chain, expected_id: int) -> None:
        d = ChainRegistry.get(chain)
        assert d.chain_id == expected_id, (
            f"Chain.{chain.name} chain_id is now {d.chain_id} but was "
            f"{expected_id} pre-VIB-4801. Renumbering chain_ids is a "
            f"wire-format change owned by metrics-database — coordinate "
            f"with Infra before changing this."
        )


# ---------------------------------------------------------------------------
# 3. Legacy byte-identity — every derived view matches its historical literal
# ---------------------------------------------------------------------------


class TestLegacyDictByteIdentity:
    """Every legacy dict that's now a derived view must remain
    byte-identical at PR-merge time.
    """

    def test_allowed_chains_byte_identical(self) -> None:
        from almanak.gateway.validation import ALLOWED_CHAINS

        assert ALLOWED_CHAINS == HISTORICAL_ALLOWED_CHAINS

    def test_chain_ids_enum_keyed_byte_identical(self) -> None:
        from almanak.core.constants import CHAIN_IDS

        assert dict(CHAIN_IDS) == HISTORICAL_CHAIN_IDS

    def test_chain_ids_string_keyed_runtime_byte_identical(self) -> None:
        from almanak.config.runtime import CHAIN_IDS

        expected = {c.name.lower(): cid for c, cid in HISTORICAL_CHAIN_IDS.items()}
        assert dict(CHAIN_IDS) == expected

    def test_chain_ids_string_keyed_execution_config_byte_identical(self) -> None:
        from almanak.framework.execution.config import CHAIN_IDS

        expected = {c.name.lower(): cid for c, cid in HISTORICAL_CHAIN_IDS.items()}
        assert dict(CHAIN_IDS) == expected

    def test_fork_manager_chain_ids_byte_identical(self) -> None:
        from almanak.framework.anvil.fork_manager import CHAIN_IDS

        expected = {
            c.name.lower(): cid
            for c, cid in HISTORICAL_CHAIN_IDS.items()
            if c is not Chain.SOLANA  # Anvil cannot fork Solana
        }
        assert dict(CHAIN_IDS) == expected

    def test_chain_gas_buffers_byte_identical(self) -> None:
        from almanak.framework.execution.gas.constants import CHAIN_GAS_BUFFERS

        assert dict(CHAIN_GAS_BUFFERS) == HISTORICAL_CHAIN_GAS_BUFFERS

    def test_chain_gas_price_caps_gwei_byte_identical(self) -> None:
        from almanak.framework.execution.gas.constants import CHAIN_GAS_PRICE_CAPS_GWEI

        assert dict(CHAIN_GAS_PRICE_CAPS_GWEI) == HISTORICAL_CHAIN_GAS_PRICE_CAPS_GWEI

    def test_chain_gas_cost_caps_native_byte_identical(self) -> None:
        from almanak.framework.execution.gas.constants import CHAIN_GAS_COST_CAPS_NATIVE

        assert dict(CHAIN_GAS_COST_CAPS_NATIVE) == HISTORICAL_CHAIN_GAS_COST_CAPS_NATIVE

    def test_chain_tx_timeouts_byte_identical(self) -> None:
        from almanak.framework.execution.gas.constants import CHAIN_TX_TIMEOUTS

        assert dict(CHAIN_TX_TIMEOUTS) == HISTORICAL_CHAIN_TX_TIMEOUTS

    def test_chain_grpc_execute_timeouts_byte_identical(self) -> None:
        from almanak.framework.execution.gas.constants import CHAIN_GRPC_EXECUTE_TIMEOUTS

        assert dict(CHAIN_GRPC_EXECUTE_TIMEOUTS) == HISTORICAL_CHAIN_GRPC_EXECUTE_TIMEOUTS

    def test_chain_simulation_buffers_byte_identical(self) -> None:
        from almanak.framework.execution.gas.constants import CHAIN_SIMULATION_BUFFERS

        assert dict(CHAIN_SIMULATION_BUFFERS) == HISTORICAL_CHAIN_SIMULATION_BUFFERS

    def test_native_token_symbols_byte_identical(self) -> None:
        from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS

        assert dict(NATIVE_TOKEN_SYMBOLS) == HISTORICAL_NATIVE_TOKEN_SYMBOLS

    def test_native_token_info_historical_subset_unchanged(self) -> None:
        """The pre-VIB-4801 ``NATIVE_TOKEN_INFO`` literal only covered 9
        chains; the registry-derived view now covers all 16 EVM chains
        (deliberate expansion documented in the PR body). The 9 historical
        entries must remain byte-identical — the test guards the trust
        boundary on what was previously declared, while allowing the new
        entries the registry adds.
        """
        from almanak.gateway.services.onchain_lookup import NATIVE_TOKEN_INFO

        for chain_name, expected_info in HISTORICAL_NATIVE_TOKEN_INFO.items():
            assert chain_name in NATIVE_TOKEN_INFO, (
                f"Historical NATIVE_TOKEN_INFO entry {chain_name!r} disappeared from "
                f"the registry-derived view."
            )
            assert NATIVE_TOKEN_INFO[chain_name] == expected_info, (
                f"NATIVE_TOKEN_INFO[{chain_name!r}] drifted from the historical literal."
            )

    def test_chain_native_symbol_byte_identical(self) -> None:
        from almanak.gateway.managed import ManagedGateway

        assert dict(ManagedGateway.CHAIN_NATIVE_SYMBOL) == HISTORICAL_CHAIN_NATIVE_SYMBOL


# ---------------------------------------------------------------------------
# 4. Import-graph isolation — the registry has no framework/connector deps
# ---------------------------------------------------------------------------


class TestImportGraphIsolation:
    """The registry module must be deterministic at import time —
    independent of which connectors / strategies / framework modules are
    loaded. This is a *trust-boundary* invariant for
    ``gateway.validation.ALLOWED_CHAINS``.
    """

    def test_registry_module_only_imports_core_enums(self) -> None:
        """Walk every module under ``almanak.core.chains`` and assert
        their top-level imports are limited to ``almanak.core.enums`` and
        siblings within the package.
        """
        # Force a clean re-import so we capture imports from a cold start.
        # We do NOT actually do a clean re-import here (would unload other
        # tests' modules); instead we read source.
        import pathlib

        import almanak.core.chains as pkg

        pkg_dir = pathlib.Path(pkg.__file__).parent
        offenders: list[tuple[str, str]] = []
        allowed_prefixes = (
            "almanak.core.enums",
            "almanak.core.chains",
        )
        for py_file in pkg_dir.glob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), 1):
                stripped = line.strip()
                if not stripped.startswith(("from almanak", "import almanak")):
                    continue
                # Tolerate ``from .x import ...`` (relative).
                if stripped.startswith("from ."):
                    continue
                ok = False
                for prefix in allowed_prefixes:
                    if stripped.startswith(f"from {prefix}") or stripped.startswith(
                        f"import {prefix}"
                    ):
                        ok = True
                        break
                if not ok:
                    offenders.append((str(py_file.name), f"L{lineno}: {stripped}"))
        assert not offenders, (
            "almanak.core.chains may only import from almanak.core.enums "
            "(or relative siblings). Offending lines:\n"
            + "\n".join(f"  {fn}: {line}" for fn, line in offenders)
        )

    def test_registry_loads_without_framework_or_gateway(self) -> None:
        """Importing the registry from a clean state must not pull in
        ``almanak.framework.*``, ``almanak.gateway.*``, or
        ``almanak.strategies.*``.

        Implementation: spawn a subprocess so module-cache state is fresh.
        """
        import subprocess
        import textwrap

        script = textwrap.dedent(
            """
            import sys
            import almanak.core.chains  # noqa: F401

            # Match both the exact root module ("almanak.framework") and any
            # submodule ("almanak.framework.foo"). The prior prefix-only check
            # missed exact-root imports, weakening the isolation guard.
            forbidden_roots = ("almanak.framework", "almanak.gateway", "almanak.strategies")
            leaked = sorted(
                m for m in sys.modules
                if m in forbidden_roots or any(m.startswith(f"{root}.") for root in forbidden_roots)
            )
            if leaked:
                print("LEAKED:", *leaked, sep="\\n")
                sys.exit(1)
            print("OK")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"Importing almanak.core.chains leaked into framework / gateway / strategies modules:\n"
            f"{result.stdout}\n{result.stderr}"
        )


# ---------------------------------------------------------------------------
# 5. Cross-check: CHAIN_FAMILY_MAP in core/enums.py must match registry
# ---------------------------------------------------------------------------


class TestChainFamilyMapAgreement:
    """``CHAIN_FAMILY_MAP`` in ``core/enums.py`` is a literal (cannot
    import the registry — circular). It must agree with the registry on
    every Chain. The chains/__init__ module asserts this at import time
    too; this test catches the case where someone bypasses the import-time
    assertion.
    """

    @pytest.mark.parametrize("chain", list(Chain), ids=lambda c: c.name)
    def test_family_matches_registry(self, chain: Chain) -> None:
        from almanak.core.enums import CHAIN_FAMILY_MAP

        assert CHAIN_FAMILY_MAP[chain] is ChainRegistry.get(chain).family
