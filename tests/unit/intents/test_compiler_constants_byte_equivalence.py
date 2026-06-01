"""W6-followup (VIB-4872): pin the byte-equivalent derived views.

VIB-4872 collapsed the per-(chain, protocol) address dicts in
``framework/intents/compiler_constants.py`` into derived views over each
connector's ``addresses.py`` / ``swap_constants.py`` / ``lending_constants.py``
data. Behaviour is byte-equivalent by construction — but the only thing
keeping it that way is the exact aggregation logic in the
``_build_*`` helpers.

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
            "radiant_v2": "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07",
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
            "radiant_v2": "0x362f3BB63Cff83bd169aE1793979E9e537993813",
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

    def test_aave_v2_forks(self) -> None:
        from almanak.framework.intents.compiler_constants import AAVE_V2_FORKS

        assert AAVE_V2_FORKS == {"radiant_v2"}

    def test_aave_compatible_protocols(self) -> None:
        from almanak.framework.intents.compiler_constants import AAVE_COMPATIBLE_PROTOCOLS

        assert AAVE_COMPATIBLE_PROTOCOLS == {"aave_v3", "radiant_v2"}


class TestProtocolRoutersDerivedView:
    """PROTOCOL_ROUTERS spot-checks for hot-path entries.

    A full snapshot would duplicate the connector ``addresses.py``
    contents; instead, lock the entries the central dict has long
    surfaced for every supported (chain, protocol) pair our integration
    tests exercise on Anvil today.
    """

    EXPECTED_SPOT_CHECKS: list[tuple[str, str, str]] = [
        # (chain, protocol, expected lower-cased address)
        ("ethereum", "uniswap_v3", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()),
        ("ethereum", "sushiswap_v3", "0x2E6cd2d30aa43f40aa81619ff4b6E0a41479B13F".lower()),
        ("ethereum", "pancakeswap_v3", "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4".lower()),
        ("ethereum", "uniswap_v2", "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower()),
        ("ethereum", "1inch", "0x1111111254EEB25477B68fb85Ed929f73A960582".lower()),
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
        ("arbitrum", "fluid", "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085".lower()),
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
    """The collision guard in `_build_swap_fee_tiers` / `_build_default_swap_fee_tier`
    raises if two connectors publish conflicting values for the same protocol.

    Smoke-test the guard by monkey-patching one connector's contribution
    to disagree with another's — the rebuild should fail loudly.
    """

    def test_swap_fee_tiers_collision_detected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors.sushiswap_v3 import swap_constants as _sushi_sc
        from almanak.framework.intents import compiler_constants as cc

        # Fake a conflicting contribution from sushiswap (publishes
        # "uniswap_v3" with a different fee-tier set than the canonical
        # uniswap_v3 connector). The aggregator should raise.
        monkeypatch.setattr(
            _sushi_sc,
            "SWAP_FEE_TIERS",
            {"uniswap_v3": (1, 2, 3)},
        )
        with pytest.raises(ValueError, match="conflicting SWAP_FEE_TIERS"):
            cc._build_swap_fee_tiers()

    def test_default_swap_fee_tier_collision_detected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from almanak.connectors.sushiswap_v3 import swap_constants as _sushi_sc
        from almanak.framework.intents import compiler_constants as cc

        monkeypatch.setattr(
            _sushi_sc,
            "DEFAULT_SWAP_FEE_TIER",
            {"uniswap_v3": 100},
        )
        with pytest.raises(ValueError, match="conflicting DEFAULT_SWAP_FEE_TIER"):
            cc._build_default_swap_fee_tier()
