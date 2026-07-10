"""VIB-4928 PR-3a: full-dict equivalence pins for the role-registry inversion.

PR-3a inverts the six per-protocol *address* tables in
``almanak/framework/intents/compiler_constants.py`` so they fan out over a
connector-self-registering :class:`ContractRoleRegistry` instead of
hand-importing each connector's ``addresses.py``. The six tables move
real-money contract addresses to the compiler / swap adapter / synthetic
intents / lending pre-flight — any drift = wrong contract = catastrophic
loss.

The existing ``test_compiler_constants_byte_equivalence.py`` only spot-checks
hot-path entries and ``.lower()``-normalises. This module is the
**full-dict, exact-case, insertion-order-pinned** contract that guards the
inversion:

* ``TestAddressTableFullDictSnapshot`` — every one of the six tables compared
  against a frozen literal snapshot of the pre-inversion output, addresses
  EXACTLY (no ``.lower()`` — these tables preserve EIP-55 case), and with
  insertion order pinned via ``list(...items())`` (Python ``dict.__eq__``
  ignores order, but the chain / per-protocol key order is load-bearing for
  any consumer that iterates).

The snapshots below were captured from the pre-PR-3a builders at commit
9de6c5583 (post-VIB-4928 PR-2). If a test here fails, the right answer is
almost always: a connector address edit drifted the derived view — verify
the new value on-chain before touching the snapshot, and add a CHANGELOG / PR
note for any intentional change.

Do NOT loosen these (``.lower()`` / ``assert <subset>`` / drop the order
check) — silent address or ordering drift on the hot path is exactly the
failure mode this file exists to catch.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Frozen pre-inversion snapshots (exact case, exact insertion order).
# ---------------------------------------------------------------------------

EXPECTED_PROTOCOL_ROUTERS: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "sushiswap_v3": "0x2E6cd2d30aa43f40aa81619ff4b6E0a41479B13F",
        "pancakeswap_v3": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",
    },
    "arbitrum": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "sushiswap_v3": "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c",
        "pancakeswap_v3": "0x32226588378236Fd0c7c4053999F88aC0e5cAc77",
        "camelot": "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18",
    },
    "optimism": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "sushiswap_v3": "0x8516944E89f296eb6473d79aED1Ba12088016c9e",
        "aerodrome": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",
        "velodrome": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",
    },
    "polygon": {
        "uniswap_v3": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        "sushiswap_v3": "0x0aF89E1620b96170e2a9D0b68fEebb767eD044c3",
    },
    "base": {
        "uniswap_v3": "0x2626664c2603336E57B271c5C0b26F421741e481",
        "sushiswap_v3": "0xfB7ef66A7e61fF9e400671e4b5BFbaBE2ea025B4",
        "pancakeswap_v3": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",
        "aerodrome": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
    },
    "avalanche": {"uniswap_v3": "0xbb00FF08d01D300023C629E8fFfFcb65A5a578cE"},
    "bsc": {
        "uniswap_v3": "0xB971eF87ede563556b2ED4b1C0b0019111Dd85d2",
        "sushiswap_v3": "0xB45e53277a7e0F1D35f2a77160e91e25507f1763",
        "pancakeswap_v3": "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4",
    },
    "linea": {
        "uniswap_v3": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
        "pancakeswap_v3": "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86",
    },
    "monad": {"uniswap_v3": "0xfE31F71C1b106EAc32F1A19239c9a9A72ddfb900"},
    "mantle": {
        "uniswap_v3": "0x738fD6d10bCc05c230388B4027CAd37f82fe2AF2",
        "agni_finance": "0x319B69888b0d11cEC22caA5034e25FfFBDc88421",
    },
    "xlayer": {"uniswap_v3": "0x4f0C28f5926AFDA16bf2506D5D9e57Ea190f9bcA"},
    "zerog": {"uniswap_v3": "0x8B598A7C136215A95ba0282b4d832B9f9801f2e2"},
    "robinhood": {"uniswap_v3": "0xCaf681a66D020601342297493863E78C959E5cb2"},
}

EXPECTED_LP_POSITION_MANAGERS: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e",
        "sushiswap_v3": "0x2214A42d8e2A1d20635c2cb0664422c528B6A432",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "traderjoe_v2": "0x9A93a421b74F1c5755b83dD2C211614dC419C44b",
    },
    "arbitrum": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0xd88F38F930b7952f2DB2432Cb002E7abbF3dD869",
        "sushiswap_v3": "0xF0cBce1942A68BEB3d1b73F0dd86C8DCc363eF49",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "traderjoe_v2": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",
        "camelot": "0x00c7f3082833e796A5b3e4Bd59f6642FF44DCD15",
    },
    "optimism": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0x3C3Ea4B57a46241e54610e5f022E5c45859A1017",
        "sushiswap_v3": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
        "aerodrome": "0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858",
    },
    "polygon": {
        "uniswap_v3": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "uniswap_v4": "0x1Ec2eBf4F37E7363FDfe3551602425af0B3ceef9",
        "sushiswap_v3": "0xb7402ee99F0A008e461098AC3A27F4957Df89a40",
    },
    "base": {
        "uniswap_v3": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
        "uniswap_v4": "0x7C5f5A4bBd8fD63184577525326123B519429bDc",
        "sushiswap_v3": "0x80C7DD17B01855a6D2347444a0FCC36136a314de",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "aerodrome": "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
        "aerodrome_slipstream": "0x827922686190790b37229fd06084350E74485b72",
    },
    "avalanche": {
        "uniswap_v3": "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
        "uniswap_v4": "0xB74b1F14d2754AcfcbBe1a221023a5cf50Ab8ACD",
        "traderjoe_v2": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",
    },
    "bsc": {
        "uniswap_v3": "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
        "uniswap_v4": "0x7A4a5c919aE2541AeD11041A1AEeE68f1287f95b",
        "sushiswap_v3": "0xF70c086618dcf2b1A461311275e00D6B722ef914",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "traderjoe_v2": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",
    },
    "linea": {
        "uniswap_v3": "0x4615C383F85D0a2BbED973d83ccecf5CB7121463",
        "pancakeswap_v3": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    },
    "monad": {"uniswap_v3": "0x7197E214c0b767cFB76Fb734ab638E2c192F4E53"},
    "mantle": {
        "uniswap_v3": "0x5911cB3633e764939edc2d92b7e1ad375Bb57649",
        "agni_finance": "0x218bf598D1453383e2F4AA7b14fFB9BfB102D637",
    },
    "xlayer": {"uniswap_v3": "0x315e413A11AB0df498eF83873012430ca36638Ae"},
    "zerog": {"uniswap_v3": "0x8F67A30Ed186e3E1f6504c6dE3239Ef43A2e0d72"},
    "robinhood": {"uniswap_v3": "0x73991a25C818Bf1f1128dEAaB1492D45638DE0D3"},
}

EXPECTED_SWAP_QUOTER_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        "sushiswap_v3": "0x64e8802FE490fa7cc61d3463958199161Bb608A7",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "arbitrum": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        "sushiswap_v3": "0x0524E833cCD057e4d7A296e3aaAb9f7675964Ce1",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        "camelot": "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E",
    },
    "optimism": {"uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"},
    "polygon": {
        "uniswap_v3": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
    },
    "base": {
        "uniswap_v3": "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "avalanche": {"uniswap_v3": "0xbe0F5544EC67e9B3b2D979aaA43f18Fd87E6257F"},
    "bsc": {
        "uniswap_v3": "0x78D78E420Da98ad378D7799bE8f4AF69033EB077",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "linea": {
        "uniswap_v3": "0x42bE4D6527829FeFA1493e1fb9F3676d2425C3C1",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
    "monad": {"uniswap_v3": "0x661E93cca42AfacB172121EF892830cA3b70F08d"},
    "mantle": {
        "uniswap_v3": "0xdD489C75be1039ec7d843A6aC2Fd658350B067Cf",
        "agni_finance": "0xc4aaDc921E1cdb66c5300Bc158a313292923C0cb",
    },
    "xlayer": {"uniswap_v3": "0x976183AC3d09840D243A88c0268BADb3B3e3259f"},
    "zerog": {"uniswap_v3": "0xd00883722cECAD3A1c60bCA611f09e1851a0bE02"},
    "robinhood": {"uniswap_v3": "0x33e885eD0Ec9bF04EcfB19341582aADCb4c8A9E7"},
    "bnb": {
        "uniswap_v3": "0x78D78E420Da98ad378D7799bE8f4AF69033EB077",
        "sushiswap_v3": "0xb1E835Dc2785b52265711e17fCCb0fd018226a6e",
        "pancakeswap_v3": "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
    },
}

EXPECTED_LENDING_POOL_ADDRESSES: dict[str, dict[str, str]] = {
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
    "linea": {"aave_v3": "0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac"},
    "plasma": {"aave_v3": "0x925a2A7214Ed92428B5b1B090F80b25700095e12"},
    "sonic": {"aave_v3": "0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3"},
    "mantle": {"aave_v3": "0x458F293454fE0d67EC0655f3672301301DD51422"},
    "xlayer": {"aave_v3": "0xE3F3Caefdd7180F884c01E57f65Df979Af84f116"},
}

EXPECTED_LENDING_POOL_DATA_PROVIDERS: dict[str, dict[str, str]] = {
    "ethereum": {"aave_v3": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"},
    "arbitrum": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
    "optimism": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
    "polygon": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
    "base": {"aave_v3": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac"},
    "avalanche": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
    "bsc": {"aave_v3": "0xc90Df74A7c16245c5F5C5870327Ceb38Fe5d5328"},
    "linea": {"aave_v3": "0x47cd4b507B81cB831669c71c7077f4daF6762FF4"},
    "plasma": {"aave_v3": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"},
    "sonic": {"aave_v3": "0xc0a344397cfa89dF1e1d3e4fb330834D789cF2CD"},
    "mantle": {"aave_v3": "0x487c5c669D9eee6057C44973207101276cf73b68"},
    "xlayer": {"aave_v3": "0x6C505C31714f14e8af2A03633EB2Cdfb4959138F"},
}

EXPECTED_BALANCER_VAULT_ADDRESSES: dict[str, str] = {
    "ethereum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "arbitrum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "optimism": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "polygon": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "base": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "avalanche": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
}


def _ordered_items_nested(d: dict[str, dict[str, str]]) -> list[tuple[str, list[tuple[str, str]]]]:
    """Recursively snapshot a 2-level dict as ordered ``items()`` lists.

    Python ``dict.__eq__`` ignores insertion order, so a plain ``==`` would
    pass even if the chain or per-protocol key order drifted. The chain order
    (and the per-protocol key order within each chain) is load-bearing for any
    consumer that iterates the table, and it is what the inversion's
    registration order + ordered-chain accessor exist to preserve. Comparing
    ``items()`` lists pins both.
    """
    return [(chain, list(inner.items())) for chain, inner in d.items()]


class TestAddressTableFullDictSnapshot:
    """Full-dict, exact-case, insertion-order-pinned pins for the six tables.

    Each table is asserted twice: once with ``==`` (value equality, exact
    case — no ``.lower()``) and once on ``items()`` lists (insertion order).
    """

    def test_protocol_routers(self) -> None:
        from almanak.framework.intents.compiler_constants import PROTOCOL_ROUTERS

        assert PROTOCOL_ROUTERS == EXPECTED_PROTOCOL_ROUTERS
        assert _ordered_items_nested(PROTOCOL_ROUTERS) == _ordered_items_nested(EXPECTED_PROTOCOL_ROUTERS)

    def test_lp_position_managers(self) -> None:
        from almanak.framework.intents.compiler_constants import LP_POSITION_MANAGERS

        assert LP_POSITION_MANAGERS == EXPECTED_LP_POSITION_MANAGERS
        assert _ordered_items_nested(LP_POSITION_MANAGERS) == _ordered_items_nested(EXPECTED_LP_POSITION_MANAGERS)

    def test_swap_quoter_addresses(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_QUOTER_ADDRESSES

        assert SWAP_QUOTER_ADDRESSES == EXPECTED_SWAP_QUOTER_ADDRESSES
        assert _ordered_items_nested(SWAP_QUOTER_ADDRESSES) == _ordered_items_nested(EXPECTED_SWAP_QUOTER_ADDRESSES)

    def test_lending_pool_addresses(self) -> None:
        from almanak.framework.intents.compiler_constants import LENDING_POOL_ADDRESSES

        assert LENDING_POOL_ADDRESSES == EXPECTED_LENDING_POOL_ADDRESSES
        assert _ordered_items_nested(LENDING_POOL_ADDRESSES) == _ordered_items_nested(EXPECTED_LENDING_POOL_ADDRESSES)

    def test_lending_pool_data_providers(self) -> None:
        from almanak.framework.intents.compiler_constants import LENDING_POOL_DATA_PROVIDERS

        assert LENDING_POOL_DATA_PROVIDERS == EXPECTED_LENDING_POOL_DATA_PROVIDERS
        assert _ordered_items_nested(LENDING_POOL_DATA_PROVIDERS) == _ordered_items_nested(
            EXPECTED_LENDING_POOL_DATA_PROVIDERS
        )

    def test_balancer_vault_addresses(self) -> None:
        from almanak.framework.intents.compiler_constants import BALANCER_VAULT_ADDRESSES

        assert BALANCER_VAULT_ADDRESSES == EXPECTED_BALANCER_VAULT_ADDRESSES
        assert list(BALANCER_VAULT_ADDRESSES.items()) == list(EXPECTED_BALANCER_VAULT_ADDRESSES.items())

    def test_spark_lending_data_provider_omitted(self) -> None:
        """Spark publishes a ``pool_data_provider`` but is intentionally absent
        from ``LENDING_POOL_DATA_PROVIDERS`` (the legacy central dict only
        carried aave_v3 for the lending pre-flight). PR-3a must preserve that
        omission — the Spark ``contract_roles`` registration declares
        ``LENDING_POOL`` only, never ``LENDING_DATA_PROVIDER``.
        """
        from almanak.framework.intents.compiler_constants import LENDING_POOL_DATA_PROVIDERS

        for chain, providers in LENDING_POOL_DATA_PROVIDERS.items():
            assert "spark" not in providers, (
                f"spark leaked into LENDING_POOL_DATA_PROVIDERS[{chain!r}] — the "
                f"intentional Spark omission (VIB-4928 PR-3a) was lost"
            )


def _legacy_build_protocol_routers() -> dict[str, dict[str, str]]:
    """Pre-PR-3a ``_build_protocol_routers`` (hand-imported connector tables).

    Kept verbatim in the test so ``TestRegistryMatchesLegacyBuilders`` can
    assert the registry-driven builder is byte-equivalent BEFORE the legacy
    body is deleted from ``compiler_constants.py``. Imports are local (mirrors
    the source) so collecting this module does not eagerly pull connectors.
    """
    from almanak.connectors.aerodrome.addresses import AERODROME
    from almanak.connectors.camelot.addresses import CAMELOT
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3

    # Pre-PR-3c central exclusions (now connector-declared via
    # ``ContractRoleSpec.surface_exclusions``) — inlined here so this reference
    # implementation stays independent of ``compiler_constants`` internals.
    _PROTOCOL_ROUTER_EXCLUSIONS = frozenset({("sushiswap_v3", "avalanche"), ("uniswap_v3", "blast")})

    routers: dict[str, dict[str, str]] = {}
    sources: tuple[tuple[str, dict[str, dict[str, str]], str], ...] = (
        ("uniswap_v3", UNISWAP_V3, "swap_router"),
        ("sushiswap_v3", SUSHISWAP_V3, "swap_router"),
        ("pancakeswap_v3", PANCAKESWAP_V3, "swap_router"),
        ("agni_finance", AGNI_FINANCE, "swap_router"),
        ("aerodrome", AERODROME, "router"),
        ("camelot", CAMELOT, "swap_router"),
    )
    for protocol, table, kind in sources:
        for chain, kinds in table.items():
            if (protocol, chain) in _PROTOCOL_ROUTER_EXCLUSIONS:
                continue
            address = kinds.get(kind)
            if address is None:
                continue
            routers.setdefault(chain, {})[protocol] = address
    optimism = routers.get("optimism")
    if optimism is not None and "aerodrome" in optimism:
        optimism.setdefault("velodrome", optimism["aerodrome"])
    return routers


def _legacy_build_lp_position_managers() -> dict[str, dict[str, str]]:
    """Pre-PR-3a ``_build_lp_position_managers`` (verbatim)."""
    from almanak.connectors.aerodrome.addresses import AERODROME
    from almanak.connectors.camelot.addresses import CAMELOT
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    # Pre-PR-3c central exclusions (now connector-declared) — inlined.
    _PROTOCOL_ROUTER_EXCLUSIONS = frozenset({("sushiswap_v3", "avalanche"), ("uniswap_v3", "blast")})

    managers: dict[str, dict[str, str]] = {}
    sources: tuple[tuple[str, dict[str, dict[str, str]], str], ...] = (
        ("uniswap_v3", UNISWAP_V3, "position_manager"),
        ("uniswap_v4", UNISWAP_V4, "position_manager"),
        ("sushiswap_v3", SUSHISWAP_V3, "position_manager"),
        ("pancakeswap_v3", PANCAKESWAP_V3, "nft"),
        ("agni_finance", AGNI_FINANCE, "position_manager"),
        ("aerodrome", AERODROME, "router"),
        ("aerodrome_slipstream", AERODROME, "cl_nft"),
        ("traderjoe_v2", TRADERJOE_V2, "router"),
        ("camelot", CAMELOT, "position_manager"),
    )
    for protocol, table, kind in sources:
        for chain, kinds in table.items():
            if (protocol, chain) in _PROTOCOL_ROUTER_EXCLUSIONS:
                continue
            address = kinds.get(kind)
            if address is None:
                continue
            managers.setdefault(chain, {})[protocol] = address
    return managers


def _legacy_build_swap_quoter_addresses() -> dict[str, dict[str, str]]:
    """Pre-PR-3a ``_build_swap_quoter_addresses`` (verbatim)."""
    from almanak.connectors.camelot.addresses import CAMELOT
    from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
    from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3
    from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE, UNISWAP_V3

    # Pre-PR-3c central exclusions (now connector-declared) — inlined.
    _SWAP_QUOTER_EXCLUSIONS = frozenset(
        {("sushiswap_v3", "avalanche"), ("sushiswap_v3", "optimism"), ("uniswap_v3", "blast")}
    )

    quoters: dict[str, dict[str, str]] = {}
    sources: tuple[tuple[str, dict[str, dict[str, str]], str], ...] = (
        ("uniswap_v3", UNISWAP_V3, "quoter_v2"),
        ("sushiswap_v3", SUSHISWAP_V3, "quoter_v2"),
        ("pancakeswap_v3", PANCAKESWAP_V3, "quoter"),
        ("agni_finance", AGNI_FINANCE, "quoter_v2"),
        ("camelot", CAMELOT, "quoter"),
    )
    for protocol, table, kind in sources:
        for chain, kinds in table.items():
            if (protocol, chain) in _SWAP_QUOTER_EXCLUSIONS:
                continue
            address = kinds.get(kind)
            if address is None:
                continue
            quoters.setdefault(chain, {})[protocol] = address
    bsc = quoters.get("bsc")
    if bsc is not None:
        quoters["bnb"] = dict(bsc)
    return quoters


def _legacy_build_lending_pool_addresses() -> dict[str, dict[str, str]]:
    """Pre-PR-3a ``_build_lending_pool_addresses`` (verbatim)."""
    from almanak.connectors.aave_v3.addresses import AAVE_V3
    from almanak.connectors.spark.addresses import SPARK

    sources: tuple[tuple[str, dict[str, dict[str, str]]], ...] = (
        ("aave_v3", AAVE_V3),
        ("spark", SPARK),
    )
    pools: dict[str, dict[str, str]] = {}
    for protocol, table in sources:
        for chain, kinds in table.items():
            pool = kinds.get("pool")
            if pool is None:
                continue
            pools.setdefault(chain, {})[protocol] = pool
    return pools


def _legacy_build_lending_pool_data_providers() -> dict[str, dict[str, str]]:
    """Pre-PR-3a ``_build_lending_pool_data_providers`` (verbatim — aave_v3 only)."""
    from almanak.connectors.aave_v3.addresses import AAVE_V3

    sources: tuple[tuple[str, dict[str, dict[str, str]]], ...] = (("aave_v3", AAVE_V3),)
    providers: dict[str, dict[str, str]] = {}
    for protocol, table in sources:
        for chain, kinds in table.items():
            provider = kinds.get("pool_data_provider")
            if provider is None:
                continue
            providers.setdefault(chain, {})[protocol] = provider
    return providers


def _legacy_build_balancer_vault_addresses() -> dict[str, str]:
    """Pre-PR-3a ``_build_balancer_vault_addresses`` (verbatim)."""
    from almanak.connectors.balancer_v2.addresses import BALANCER_V2

    return {chain: kinds["vault"] for chain, kinds in BALANCER_V2.items() if "vault" in kinds}


class TestRegistryMatchesLegacyBuilders:
    """Transitional pin: the registry-driven ``_build_*`` == the legacy body.

    This is the equivalence-before-delete contract (VIB-4928 PR-3a). The
    legacy builder bodies are copied verbatim into this module (the
    ``_legacy_build_*`` helpers above). Each test asserts the new
    registry-driven ``compiler_constants._build_*`` produces an output
    byte-identical (value + insertion order) to its legacy twin. The legacy
    source bodies are only deleted once these are green in CI.
    """

    def _assert_nested_identical(self, new: dict[str, dict[str, str]], legacy: dict[str, dict[str, str]]) -> None:
        assert new == legacy
        assert _ordered_items_nested(new) == _ordered_items_nested(legacy)

    def test_protocol_routers_match(self) -> None:
        from almanak.framework.intents import compiler_constants as cc

        self._assert_nested_identical(cc._build_protocol_routers(), _legacy_build_protocol_routers())

    def test_lp_position_managers_match(self) -> None:
        from almanak.framework.intents import compiler_constants as cc

        self._assert_nested_identical(cc._build_lp_position_managers(), _legacy_build_lp_position_managers())

    def test_swap_quoter_addresses_match(self) -> None:
        from almanak.framework.intents import compiler_constants as cc

        self._assert_nested_identical(cc._build_swap_quoter_addresses(), _legacy_build_swap_quoter_addresses())

    def test_lending_pool_addresses_match(self) -> None:
        from almanak.framework.intents import compiler_constants as cc

        self._assert_nested_identical(cc._build_lending_pool_addresses(), _legacy_build_lending_pool_addresses())

    def test_lending_pool_data_providers_match(self) -> None:
        from almanak.framework.intents import compiler_constants as cc

        self._assert_nested_identical(
            cc._build_lending_pool_data_providers(),
            _legacy_build_lending_pool_data_providers(),
        )

    def test_balancer_vault_addresses_match(self) -> None:
        from almanak.framework.intents import compiler_constants as cc

        new = cc._build_balancer_vault_addresses()
        legacy = _legacy_build_balancer_vault_addresses()
        assert new == legacy
        assert list(new.items()) == list(legacy.items())


class TestConnectorDeclaredSurfaceMetadata:
    """VIB-4928 PR-3c: the surface metadata that ``compiler_constants`` fans out
    over (``npm_view`` / ``surface_exclusions`` / ``router_aliases``) is
    connector-declared on ``ContractRoleSpec``.

    Pins the registry-level declarations directly (independent of the derived
    tables) so a connector spec edit that would silently change
    ``PROTOCOL_ROUTERS`` / ``LP_POSITION_MANAGERS`` / ``SWAP_QUOTER_ADDRESSES`` /
    the NPM views — or break the VIB-4971 sushi invariant — fails loudly here.
    """

    def test_npm_view_contributors(self) -> None:
        import almanak.connectors._strategy_contract_role_registry  # noqa: F401
        from almanak.connectors._strategy_base.contract_role_registry import (
            CONTRACT_ROLE_REGISTRY,
            NpmView,
        )

        assert CONTRACT_ROLE_REGISTRY.protocols_with_npm_view(NpmView.UNIV3) == (
            "uniswap_v3",
            "agni_finance",
        )
        assert CONTRACT_ROLE_REGISTRY.protocols_with_npm_view(NpmView.PANCAKESWAP) == ("pancakeswap_v3",)
        assert CONTRACT_ROLE_REGISTRY.protocols_with_npm_view(NpmView.SLIPSTREAM) == ("aerodrome_slipstream",)

    def test_sushiswap_v3_declares_no_npm_view(self) -> None:
        """VIB-4971 invariant at the registry level: sushiswap_v3 must NOT feed
        the canonical UniV3 NPM map (it ships a distinct ``position_manager``;
        the backfill binds its LP positions to the Uniswap NPM)."""
        import almanak.connectors._strategy_contract_role_registry  # noqa: F401
        from almanak.connectors._strategy_base.contract_role_registry import (
            CONTRACT_ROLE_REGISTRY,
        )

        assert CONTRACT_ROLE_REGISTRY.npm_view("sushiswap_v3") is None

    def test_surface_exclusions(self) -> None:
        import almanak.connectors._strategy_contract_role_registry  # noqa: F401
        from almanak.connectors._strategy_base.contract_role_registry import (
            CONTRACT_ROLE_REGISTRY,
            ContractRole,
        )

        # uniswap_v3 blast — published in addresses.py, never surfaced (all 3 tables).
        for role in (
            ContractRole.ROUTER,
            ContractRole.LP_POSITION_MANAGER,
            ContractRole.QUOTER,
        ):
            assert CONTRACT_ROLE_REGISTRY.surface_exclusions("uniswap_v3", role) == frozenset({"blast"})
        # sushiswap_v3: avalanche (router/lp/quoter) + optimism (quoter only).
        assert CONTRACT_ROLE_REGISTRY.surface_exclusions("sushiswap_v3", ContractRole.ROUTER) == frozenset(
            {"avalanche"}
        )
        assert CONTRACT_ROLE_REGISTRY.surface_exclusions("sushiswap_v3", ContractRole.LP_POSITION_MANAGER) == frozenset(
            {"avalanche"}
        )
        assert CONTRACT_ROLE_REGISTRY.surface_exclusions("sushiswap_v3", ContractRole.QUOTER) == frozenset(
            {"avalanche", "optimism"}
        )
        # A protocol with no declared exclusions returns empty.
        assert CONTRACT_ROLE_REGISTRY.surface_exclusions("pancakeswap_v3", ContractRole.ROUTER) == frozenset()

    def test_router_aliases(self) -> None:
        import almanak.connectors._strategy_contract_role_registry  # noqa: F401
        from almanak.connectors._strategy_base.contract_role_registry import (
            CONTRACT_ROLE_REGISTRY,
        )

        assert dict(CONTRACT_ROLE_REGISTRY.router_aliases("aerodrome")) == {"velodrome": frozenset({"optimism"})}
        assert dict(CONTRACT_ROLE_REGISTRY.router_aliases("uniswap_v3")) == {}
