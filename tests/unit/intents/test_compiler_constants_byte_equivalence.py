"""W6-followup (VIB-4872): pin the byte-equivalent derived views.

VIB-4872 collapsed the per-(chain, protocol) address dicts in
``framework/intents/compiler_constants.py`` into derived views over each
connector's ``addresses.py`` / ``swap_classification.py`` / ``protocol_family.py``
data (VIB-4928 PR-3b moved the swap/lending classification half off the retired
``swap_constants.py`` / ``lp_constants.py`` / ``lending_constants.py`` onto the
connector-self-registering registries). Behaviour is byte-equivalent by
construction — but the only thing keeping it that way is the exact aggregation
logic in the ``_build_*`` helpers.

These tests pin the historical snapshot of every migrated dict /
frozenset so a future "tidy up the helper" refactor cannot silently
change the externally observable lookup at the consumer boundary
(``compiler.py`` / ``swap_adapter.py`` / ``synthetic_intents.py`` /
``discovery.py``).

If a test here fails, the right answer is almost always:

1. Confirm the new behaviour is intentional (an address fixed in the
   connector data is good; a missing entry the central dict used to
   surface is bad).
2. Update the historical snapshot below with a CHANGELOG / PR note.

Do NOT loosen the test (``assert <subset>``) — silent address drift on
the hot path is exactly the failure mode this file exists to catch.
"""

from __future__ import annotations

import pytest


def _lower(d: dict[str, str]) -> dict[str, str]:
    return {k: v.lower() for k, v in d.items()}


def _lower_nested(d: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    return {chain: _lower(inner) for chain, inner in d.items()}


class TestLendingAddressDerivedViews:
    """LENDING_POOL_ADDRESSES, LENDING_POOL_DATA_PROVIDERS, BALANCER_VAULT_ADDRESSES."""

    EXPECTED_LENDING_POOL = _lower_nested({
        "ethereum": {
            "aave_v3": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
            "spark": "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
        },
        "arbitrum": {"aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD"},
        "optimism": {"aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD"},
        "polygon": {"aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD"},
        "base": {"aave_v3": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"},
        "avalanche": {"aave_v3": "0x794a61358D6845594F94dc1DB02A252b5b4814aD"},
        "bsc": {"aave_v3": "0x6807dc923806fE8Fd134338EABCA509979a7e0cB"},
        "sonic": {"aave_v3": "0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3"},
        "linea": {"aave_v3": "0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac"},
        "plasma": {"aave_v3": "0x925a2A7214Ed92428B5b1B090F80b25700095e12"},
        "mantle": {"aave_v3": "0x458F293454fE0d67EC0655f3672301301DD51422"},
        "xlayer": {"aave_v3": "0xE3F3Caefdd7180F884c01E57f65Df979Af84f116"},
    })

    EXPECTED_DATA_PROVIDERS = _lower_nested({
        "ethereum": {
            "aave_v3": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        },
        "arbitrum": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
        "optimism": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
        "polygon": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
        "base": {"aave_v3": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac"},
        "avalanche": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
        "bsc": {"aave_v3": "0xc90Df74A7c16245c5F5C5870327Ceb38Fe5d5328"},
        "sonic": {"aave_v3": "0xc0a344397cfa89dF1e1d3e4fb330834D789cF2CD"},
        "linea": {"aave_v3": "0x47cd4b507B81cB831669c71c7077f4daF6762FF4"},
        "plasma": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
        "mantle": {"aave_v3": "0x487c5c669D9eee6057C44973207101276cf73b68"},
        "xlayer": {"aave_v3": "0x6C505C31714f14e8af2A03633EB2Cdfb4959138F"},
    })

    EXPECTED_BALANCER_VAULT = _lower({
        "ethereum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        "arbitrum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        "optimism": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        "polygon": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        "base": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        "avalanche": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    })

    def test_lending_pool_addresses(self) -> None:
        from almanak.framework.intents.compiler_constants import LENDING_POOL_ADDRESSES

        assert _lower_nested(LENDING_POOL_ADDRESSES) == self.EXPECTED_LENDING_POOL

    def test_lending_pool_data_providers(self) -> None:
        from almanak.framework.intents.compiler_constants import LENDING_POOL_DATA_PROVIDERS

        assert _lower_nested(LENDING_POOL_DATA_PROVIDERS) == self.EXPECTED_DATA_PROVIDERS

    def test_balancer_vault_addresses(self) -> None:
        from almanak.framework.intents.compiler_constants import BALANCER_VAULT_ADDRESSES

        assert _lower(BALANCER_VAULT_ADDRESSES) == self.EXPECTED_BALANCER_VAULT


class TestClassificationFrozensetDerivedViews:
    """SWAP_FEE_TIERS, DEFAULT_SWAP_FEE_TIER, SWAP_ROUTER_V1_*, AAVE_*."""

    def test_swap_fee_tiers(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_FEE_TIERS

        assert SWAP_FEE_TIERS == {
            "uniswap_v3": (100, 500, 3000, 10000),
            "sushiswap_v3": (100, 500, 3000, 10000),
            "pancakeswap_v3": (100, 500, 2500, 10000),
            "agni_finance": (100, 500, 2500, 3000, 10000),
        }

    def test_default_swap_fee_tier(self) -> None:
        from almanak.framework.intents.compiler_constants import DEFAULT_SWAP_FEE_TIER

        assert DEFAULT_SWAP_FEE_TIER == {
            "uniswap_v3": 3000,
            "sushiswap_v3": 3000,
            "pancakeswap_v3": 2500,
            "agni_finance": 3000,
        }

    def test_swap_router_v1_protocols(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_ROUTER_V1_PROTOCOLS

        assert SWAP_ROUTER_V1_PROTOCOLS == frozenset({"sushiswap_v3"})

    def test_swap_router_v1_chain_overrides(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_ROUTER_V1_CHAIN_OVERRIDES

        assert SWAP_ROUTER_V1_CHAIN_OVERRIDES == {
            "mantle": frozenset({"agni_finance"}),
            "zerog": frozenset({"uniswap_v3"}),
        }

    def test_swap_router_algebra_protocols(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_ROUTER_ALGEBRA_PROTOCOLS

        assert SWAP_ROUTER_ALGEBRA_PROTOCOLS == frozenset({"camelot"})

    def test_aave_compatible_protocols(self) -> None:
        from almanak.framework.intents.compiler_constants import AAVE_COMPATIBLE_PROTOCOLS

        assert AAVE_COMPATIBLE_PROTOCOLS == {"aave_v3"}


class TestProtocolRoutersDerivedView:
    """PROTOCOL_ROUTERS spot-checks for hot-path entries.

    A full snapshot would duplicate the connector ``addresses.py``
    contents; instead, lock the entries the central dict has long
    surfaced for every supported (chain, protocol) pair our integration
    tests exercise on Anvil today.
    """

    EXPECTED_SPOT_CHECKS: list[tuple[str, str, str]] = [
        # (chain, protocol, expected lower-cased address)
        # NOTE (VIB-4928 PR-2): ``uniswap_v2`` / ``1inch`` were dropped from
        # this list when the ``_LEGACY_PROTOCOL_ROUTERS`` overlay was retired
        # — they had no connector folder and no functional consumer. The
        # anti-regression guard for their *absence* now lives in
        # ``TestLegacyRoutersRetired`` below.
        ("ethereum", "uniswap_v3", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()),
        ("ethereum", "sushiswap_v3", "0x2E6cd2d30aa43f40aa81619ff4b6E0a41479B13F".lower()),
        ("ethereum", "pancakeswap_v3", "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4".lower()),
        ("arbitrum", "uniswap_v3", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()),
        ("arbitrum", "camelot", "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18".lower()),
        ("optimism", "aerodrome", "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858".lower()),
        ("optimism", "velodrome", "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858".lower()),
        ("base", "uniswap_v3", "0x2626664c2603336E57B271c5C0b26F421741e481".lower()),
        ("base", "aerodrome", "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43".lower()),
        ("mantle", "agni_finance", "0x319B69888b0d11cEC22caA5034e25FfFBDc88421".lower()),
    ]

    @pytest.mark.parametrize(("chain", "protocol", "expected"), EXPECTED_SPOT_CHECKS)
    def test_protocol_router_spot_check(self, chain: str, protocol: str, expected: str) -> None:
        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

        chain_map = PROTOCOL_ROUTERS.get(chain, {})
        actual = chain_map.get(protocol)
        assert actual is not None, (
            f"PROTOCOL_ROUTERS missing entry for {protocol!r} on {chain!r}; "
            f"chain map keys: {sorted(chain_map)}"
        )
        assert actual.lower() == expected, (
            f"PROTOCOL_ROUTERS[{chain!r}][{protocol!r}] drifted from "
            f"historical value {expected!r}; got {actual!r}"
        )

    def test_avalanche_sushiswap_v3_excluded(self) -> None:
        """SushiSwap V3 on Avalanche is deliberately excluded (VIB-2069)."""
        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

        avalanche = PROTOCOL_ROUTERS.get("avalanche", {})
        assert "sushiswap_v3" not in avalanche, (
            "sushiswap_v3 on avalanche must stay excluded (VIB-2069 — "
            "zero usable liquidity); did the connector data leak?"
        )


class TestLegacyRoutersRetired:
    """Anti-regression guard for the retired ``_LEGACY_PROTOCOL_ROUTERS`` overlay.

    VIB-4928 (PR-2) deleted the overlay that advertised five connector-less
    routers. The investigation that authorised the deletion proved none were
    reachable by a functional consumer:

    * absent from ``synthetic_intents._swap_protocols()`` — permission
      discovery never read them;
    * ``get_connector_compiler(...)`` returns ``None`` — no compiler;
    * not Uniswap-V3 forks — the Pendle pre-swap router scan
      (``_select_v3_pre_swap_router``) skipped them;
    * the only path that *could* reach an overlay address (the
      ``DefaultSwapAdapter`` fall-through) encodes a Uniswap-V3
      ``exactInputSingle``, which a V2/aggregator router does not implement.

    These tests pin that the entries stay *gone* (so a future connector data
    edit cannot silently re-surface a dead route) AND that retiring them did
    not disturb the connector-owned routers that legitimately live on the
    same chains.
    """

    # (chain, retired protocol) — every entry the overlay used to inject.
    RETIRED_ENTRIES: list[tuple[str, str]] = [
        ("ethereum", "uniswap_v2"),
        ("ethereum", "1inch"),
        ("arbitrum", "sushiswap"),
        ("arbitrum", "1inch"),
        ("optimism", "1inch"),
        ("polygon", "quickswap"),
        ("polygon", "1inch"),
        ("bsc", "pancakeswap_v2"),
        ("bsc", "sushiswap"),
    ]

    @pytest.mark.parametrize(("chain", "protocol"), RETIRED_ENTRIES)
    def test_retired_router_absent(self, chain: str, protocol: str) -> None:
        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

        chain_map = PROTOCOL_ROUTERS.get(chain, {})
        assert protocol not in chain_map, (
            f"retired legacy router {protocol!r} re-surfaced on {chain!r} "
            f"(VIB-4928 PR-2 deleted the _LEGACY_PROTOCOL_ROUTERS overlay; "
            f"it has no connector folder and no functional consumer). "
            f"chain map keys: {sorted(chain_map)}"
        )

    def test_aggregator_never_in_routers(self) -> None:
        """``1inch`` was an aggregator-only entry — it must appear on no chain."""
        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

        offenders = {chain for chain, protos in PROTOCOL_ROUTERS.items() if "1inch" in protos}
        assert not offenders, (
            f"1inch (aggregator-only, retired in VIB-4928 PR-2) re-surfaced "
            f"in PROTOCOL_ROUTERS on chains: {sorted(offenders)}"
        )

    # Connector-owned routers that share a chain with a retired overlay entry.
    # Retiring the overlay must leave these byte-identical (they derive from
    # each connector's addresses.py, never from the overlay).
    SURVIVING_NEIGHBOURS: list[tuple[str, str, str]] = [
        ("ethereum", "uniswap_v3", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()),
        ("arbitrum", "uniswap_v3", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()),
        ("arbitrum", "camelot", "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18".lower()),
        ("optimism", "aerodrome", "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858".lower()),
        ("polygon", "uniswap_v3", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()),
    ]

    @pytest.mark.parametrize(("chain", "protocol", "expected"), SURVIVING_NEIGHBOURS)
    def test_surviving_neighbour_unchanged(self, chain: str, protocol: str, expected: str) -> None:
        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

        actual = PROTOCOL_ROUTERS.get(chain, {}).get(protocol)
        assert actual is not None, (
            f"connector-owned router {protocol!r} on {chain!r} vanished when "
            f"the overlay was retired — the deletion was not surgical"
        )
        assert actual.lower() == expected, (
            f"PROTOCOL_ROUTERS[{chain!r}][{protocol!r}] drifted from "
            f"{expected!r} to {actual!r} during overlay retirement"
        )


class TestSwapQuoterDerivedView:
    """SWAP_QUOTER_ADDRESSES spot-checks + bnb alias."""

    def test_bnb_alias_mirrors_bsc(self) -> None:
        """The VIB-708 unification copies bsc -> bnb under the same key set."""
        from almanak.framework.intents.compiler_constants import SWAP_QUOTER_ADDRESSES

        bsc = SWAP_QUOTER_ADDRESSES.get("bsc")
        bnb = SWAP_QUOTER_ADDRESSES.get("bnb")
        assert bsc is not None and bnb is not None, (
            "Both bsc + bnb entries must exist for the VIB-708 quoter alias"
        )
        assert bnb == bsc, f"bnb quoter map drifted from bsc: bsc={bsc} bnb={bnb}"

    def test_camelot_quoter_pinned(self) -> None:
        """Camelot V3 quoter address must round-trip from camelot/addresses.py."""
        from almanak.framework.intents.compiler_constants import SWAP_QUOTER_ADDRESSES

        camelot_arbitrum = SWAP_QUOTER_ADDRESSES["arbitrum"]["camelot"].lower()
        assert camelot_arbitrum == "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E".lower()


class TestLpPositionManagersDerivedView:
    """LP_POSITION_MANAGERS hot-path spot-checks."""

    EXPECTED_SPOT_CHECKS: list[tuple[str, str, str]] = [
        ("ethereum", "uniswap_v3", "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()),
        # V4 PositionManager — now derives from uniswap_v4/addresses.py
        # (VIB-4874). The previous legacy-overlay value (0xBd2165...e83b24)
        # was a garbled non-contract and has been removed; this is the
        # on-chain-verified Ethereum PositionManager.
        ("ethereum", "uniswap_v4", "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e".lower()),
        ("ethereum", "traderjoe_v2", "0x9A93a421b74F1c5755b83dD2C211614dC419C44b".lower()),
        ("arbitrum", "camelot", "0x00c7f3082833e796A5b3e4Bd59f6642FF44DCD15".lower()),
        # fluid was removed from LP_POSITION_MANAGERS in Phase 1 (VIB-5029):
        # SWAP-only, routerless -- no framework role table applies.
        ("base", "aerodrome", "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43".lower()),
        ("base", "aerodrome_slipstream", "0x827922686190790b37229fd06084350E74485b72".lower()),
        ("avalanche", "traderjoe_v2", "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30".lower()),
    ]

    @pytest.mark.parametrize(("chain", "protocol", "expected"), EXPECTED_SPOT_CHECKS)
    def test_lp_position_manager_spot_check(
        self, chain: str, protocol: str, expected: str
    ) -> None:
        from almanak.framework.intents.compiler_constants import LP_POSITION_MANAGERS

        chain_map = LP_POSITION_MANAGERS.get(chain, {})
        actual = chain_map.get(protocol)
        assert actual is not None, (
            f"LP_POSITION_MANAGERS missing entry for {protocol!r} on {chain!r}"
        )
        assert actual.lower() == expected


class TestCrossConnectorCollisionDetection:
    """The swap-classification registry raises if two connectors register the
    same protocol slug with conflicting fee-tier / default-fee-tier values.

    VIB-4928 (PR-3b) moved this collision gate from the ``_build_swap_fee_tiers``
    / ``_build_default_swap_fee_tier`` consumers onto
    ``SwapClassificationRegistry.register``; the SEMANTIC — two conflicting
    contributions for one slug raise ``ValueError`` — is preserved (the
    ``match=`` strings are byte-identical). A synthetic ``__test_dex__`` slug is
    registered so the assertion never collides with a boot-populated real slug,
    and it is popped afterwards so it cannot leak into the shared registry other
    tests read.
    """

    def test_swap_fee_tiers_collision_detected(self) -> None:
        from almanak.connectors._strategy_base.swap_classification_registry import (
            SwapClassificationRegistry,
            SwapClassificationSpec,
        )

        try:
            SwapClassificationRegistry.register(
                SwapClassificationSpec(protocol="__test_dex__", fee_tiers=(100, 500), default_fee_tier=100)
            )
            # A second registration of the same slug with a different fee-tier
            # set must fail loudly.
            with pytest.raises(ValueError, match="conflicting SWAP_FEE_TIERS"):
                SwapClassificationRegistry.register(
                    SwapClassificationSpec(protocol="__test_dex__", fee_tiers=(1, 2, 3), default_fee_tier=100)
                )
        finally:
            SwapClassificationRegistry._specs.pop("__test_dex__", None)

    def test_default_swap_fee_tier_collision_detected(self) -> None:
        from almanak.connectors._strategy_base.swap_classification_registry import (
            SwapClassificationRegistry,
            SwapClassificationSpec,
        )

        try:
            SwapClassificationRegistry.register(
                SwapClassificationSpec(protocol="__test_dex__", fee_tiers=(100, 500), default_fee_tier=100)
            )
            # Same fee tiers, conflicting default tier → DEFAULT collision.
            with pytest.raises(ValueError, match="conflicting DEFAULT_SWAP_FEE_TIER"):
                SwapClassificationRegistry.register(
                    SwapClassificationSpec(protocol="__test_dex__", fee_tiers=(100, 500), default_fee_tier=500)
                )
        finally:
            SwapClassificationRegistry._specs.pop("__test_dex__", None)

    def test_router_roles_union_not_overwritten(self) -> None:
        """Same slug, matching fee tiers, differing router roles → union (merge).

        The fee-tier roles are collision-checked; the router roles
        (``router_v1`` / ``router_v1_chains`` / ``router_algebra``) are
        union-semantics, mirroring the pre-PR-3b ``_build_swap_router_*``
        builders' ``|=`` / ``.update``. A second contribution for one slug must
        merge — not clobber — those roles, else a fork adding a chain-specific
        V1 override for an existing slug would silently lose it depending on
        registration order.
        """
        from almanak.connectors._strategy_base.swap_classification_registry import (
            SwapClassificationRegistry,
            SwapClassificationSpec,
        )

        try:
            SwapClassificationRegistry.register(
                SwapClassificationSpec(
                    protocol="__test_dex__",
                    fee_tiers=(100, 500),
                    default_fee_tier=100,
                    router_v1=False,
                    router_v1_chains=("mantle",),
                    router_algebra=False,
                )
            )
            # Same fee-tier roles; each router role flips / adds a chain.
            SwapClassificationRegistry.register(
                SwapClassificationSpec(
                    protocol="__test_dex__",
                    fee_tiers=(100, 500),
                    default_fee_tier=100,
                    router_v1=True,
                    router_v1_chains=("zerog",),
                    router_algebra=True,
                )
            )
            merged = SwapClassificationRegistry._specs["__test_dex__"]
            assert merged.router_v1 is True
            assert merged.router_algebra is True
            # Ordered set-union: first contribution's chain precedes the second's.
            assert merged.router_v1_chains == ("mantle", "zerog")
            # Fee-tier roles are untouched by the merge.
            assert merged.fee_tiers == (100, 500)
            assert merged.default_fee_tier == 100
        finally:
            SwapClassificationRegistry._specs.pop("__test_dex__", None)


class TestNftPositionManagerDerivedViews:
    """UNIV3 / PancakeSwap V3 / Slipstream NPM ``{chain: address}`` views.

    VIB-4928 (PR-3c): these now resolve via ``AddressRegistry`` (was a direct
    ``connector.addresses`` import). The NPM address is the emitter component of
    an LP position's ``physical_identity_hash`` — value-bearing, so pin the full
    maps case-exact (UniV3 = EIP-55, PancakeSwap / Slipstream = lowercased,
    ``bnb`` alias of ``bsc`` preserved) AND insertion-order-exact, matching the
    order convention the rest of this file enforces. A drift here is a silent
    hash corruption on the migration backfill's hot path.

    The expected dicts below are written in the builder's insertion order —
    connector ``addresses.py`` chain order (uniswap_v3 chains, then the
    agni_finance Mantle overlay for UniV3), with the ``bnb`` alias appended last.
    """

    EXPECTED_UNIV3 = {
        "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
        "avalanche": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
        "bsc": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
        "monad": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53",
        "mantle": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",
        "xlayer": "0x315e413A11AB0df498eF83873012430ca36638Ae",
        "zerog": "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72",
        "robinhood": "0x73991a25C818Bf1f1128dEAaB1492D45638DE0D3",
        "bnb": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
    }

    EXPECTED_PANCAKE = {
        "bsc": "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
        "ethereum": "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
        "arbitrum": "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
        "base": "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
        "linea": "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
        "bnb": "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
    }

    EXPECTED_SLIPSTREAM = {"base": "0x827922686190790b37229fd06084350e74485b72"}

    def test_univ3_npm(self) -> None:
        from almanak.framework.intents.compiler_constants import UNIV3_NFT_POSITION_MANAGERS

        assert UNIV3_NFT_POSITION_MANAGERS == self.EXPECTED_UNIV3
        # Insertion order is part of the byte-equivalence contract (CodeRabbit).
        assert list(UNIV3_NFT_POSITION_MANAGERS.items()) == list(self.EXPECTED_UNIV3.items())

    def test_pancakeswap_v3_npm(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            PANCAKESWAP_V3_NFT_POSITION_MANAGERS,
        )

        assert PANCAKESWAP_V3_NFT_POSITION_MANAGERS == self.EXPECTED_PANCAKE
        assert (
            list(PANCAKESWAP_V3_NFT_POSITION_MANAGERS.items())
            == list(self.EXPECTED_PANCAKE.items())
        )

    def test_slipstream_npm(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            SLIPSTREAM_NFT_POSITION_MANAGERS,
        )

        assert SLIPSTREAM_NFT_POSITION_MANAGERS == self.EXPECTED_SLIPSTREAM
        assert (
            list(SLIPSTREAM_NFT_POSITION_MANAGERS.items())
            == list(self.EXPECTED_SLIPSTREAM.items())
        )

    def test_univ3_excludes_curated_chains(self) -> None:
        """blast / linea stay excluded (curated subset, VIB-4864)."""
        from almanak.framework.intents.compiler_constants import UNIV3_NFT_POSITION_MANAGERS

        assert "blast" not in UNIV3_NFT_POSITION_MANAGERS
        assert "linea" not in UNIV3_NFT_POSITION_MANAGERS

    def test_univ3_does_not_source_sushiswap_v3(self) -> None:
        """VIB-4971 invariant: the canonical UniV3 NPM map must NOT pick up
        sushiswap_v3's distinct ``position_manager`` — that would change
        ``physical_identity_hash`` for sushi LP positions. sushi's ethereum NPM
        (``0x2214…``) must be absent from the map's values.
        """
        from almanak.framework.intents.compiler_constants import UNIV3_NFT_POSITION_MANAGERS

        sushi_eth_npm = "0x2214A42d8e2A1d20635c2cb0664422c528B6A432".lower()
        assert sushi_eth_npm not in {v.lower() for v in UNIV3_NFT_POSITION_MANAGERS.values()}

    # VIB-4583: the V4 derived views feed physical_identity_hash_univ4 (PositionManager)
    # and the registry grouping gate — pin them so a registry/address refactor can't
    # silently change V4 position identity. Lowercased (address normalization).
    EXPECTED_UNIV4 = {
        "ethereum": "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e",
        "base": "0x7c5f5a4bbd8fd63184577525326123b519429bdc",
        "arbitrum": "0xd88f38f930b7952f2db2432cb002e7abbf3dd869",
        "optimism": "0x3c3ea4b57a46241e54610e5f022e5c45859a1017",
        "polygon": "0x1ec2ebf4f37e7363fdfe3551602425af0b3ceef9",
        "avalanche": "0xb74b1f14d2754acfcbbe1a221023a5cf50ab8acd",
        "bsc": "0x7a4a5c919ae2541aed11041a1aeee68f1287f95b",
    }

    def test_univ4_npm(self) -> None:
        from almanak.framework.intents.compiler_constants import UNIV4_NFT_POSITION_MANAGERS

        assert UNIV4_NFT_POSITION_MANAGERS == self.EXPECTED_UNIV4
        # All values lowercased — physical_identity_hash_univ4 lowercases the PM,
        # so the source map must already be normalized for byte-fidelity.
        assert all(v == v.lower() for v in UNIV4_NFT_POSITION_MANAGERS.values())

    def test_univ4_npm_disjoint_from_univ3(self) -> None:
        """V4 PositionManagers must not collide with V3 NPMs (distinct identity space)."""
        from almanak.framework.intents.compiler_constants import (
            UNIV3_NFT_POSITION_MANAGERS,
            UNIV4_NFT_POSITION_MANAGERS,
        )

        v3 = {v.lower() for v in UNIV3_NFT_POSITION_MANAGERS.values()}
        v4 = {v.lower() for v in UNIV4_NFT_POSITION_MANAGERS.values()}
        assert v3.isdisjoint(v4)

    def test_univ4_lp_grouping_protocols(self) -> None:
        from almanak.framework.intents.compiler_constants import UNIV4_LP_GROUPING_PROTOCOLS

        assert UNIV4_LP_GROUPING_PROTOCOLS == frozenset({"uniswap_v4"})
