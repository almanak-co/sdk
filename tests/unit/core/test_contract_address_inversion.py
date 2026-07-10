"""Equivalence tests for the CS-5 money-path inversions (VIB-4851 Phase E).

Chainlink feeds, the Safe multisend map, LP position managers, and the
Enso bridge token table move to descriptor / connector ownership. Every
legacy literal is frozen verbatim below; the four DELIBERATE divergences
are pinned individually, each backed by on-chain evidence gathered on
2026-06-11 (eth_getCode: the legacy addresses are EMPTY, the registry
addresses hold the real contracts).
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import (
    chainlink_chain_ids_map,
    chainlink_eth_denominated_map,
    chainlink_usd_feeds_map,
    contract_address_map,
)

# ── Frozen legacy literals (verbatim from the pre-CS-5 modules) ─────────────

FROZEN_CHAINLINK_PRICE_FEEDS: dict[str, dict[str, str]] = {
    "ethereum": {
        "ETH/USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
        "BTC/USD": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
        "LINK/USD": "0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
        "USDC/USD": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",
        "USDT/USD": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",
        "DAI/USD": "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9",
        "AAVE/USD": "0x547a514d5e3769680Ce22B2361c10Ea13619e8a9",
        "UNI/USD": "0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
        "CRV/USD": "0xcD627aa160A6fA45Eb793D19286F3879d5cdCe0a",
        "COMP/USD": "0xdBD020CAef83eFd542f4de03864E8c5D2d9bc6CA",
        "MKR/USD": "0xEC1D1b3b0443256Cc3860E24a46f108E699cF2b4",
        "SNX/USD": "0xDC3EA94CD0AC27d9A86C180091e7f78C683d3699",
        "MATIC/USD": "0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676",
        "ARB/USD": "0x31697852a68433DBcC2FF612A4c1C919a0254678",
        "LDO/USD": "0x4e844125952d32acdF339be976C98FE6D1F5F8bE",
        "WSTETH/USD": "0x164b276057258D81941072Eb5f9D7F71C3Dd94b8",
        "CBETH/USD": "0xF017fcB346A1885194689bA23Eff2fE6fA5C483b",
        "RETH/USD": "0x536218f9E9Eb48863970252233c8F271f554C2d0",
        "SOL/USD": "0x4ffC43a60e009B551865A93d232E33Fce9f01507",
    },
    "arbitrum": {
        "ETH/USD": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
        "BTC/USD": "0x6CE185860A4963106506C203335A2910F5E5E8CC",
        "LINK/USD": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
        "USDC/USD": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
        "USDT/USD": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
        "DAI/USD": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",
        "ARB/USD": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
        "GMX/USD": "0xDB98056FecFff59D032aB628337A4887110df3dB",
        "UNI/USD": "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",
        "AAVE/USD": "0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034",
        "PENDLE/USD": "0x66853E19D73C0F9301fE099c324A1e9726953C89",
        "RDNT/USD": "0x20d0Fcab0ECFD078B036b6CAf1FaC69A6453b352",
        "MAGIC/USD": "0x47E55cCec6582838E173f252D08Afd8116c2202d",
        "WOO/USD": "0x5d5Ab15fb857De6FA209B6B41C7375F1C4BD9B90",
        "SOL/USD": "0x24ceA4b8ce57cdA5058b924B9B9987992450590c",
    },
    "base": {
        "ETH/USD": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
        "BTC/USD": "0x64c911996d3C6Ac71e9B8934F4e4f21B9C3bD7d1",
        "LINK/USD": "0x17CAb8FE31E32f08326e5E27412894e49B0f9D65",
        "USDC/USD": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B",
        "DAI/USD": "0x591e79239a7d679378eC8c847e5038150364C78F",
        "CBETH/USD": "0xd7818272B9e248357d13057AAb0B417aF31E817d",
    },
    "optimism": {
        "ETH/USD": "0x13e3Ee699D1909E989722E753853AE30b17e08c5",
        "BTC/USD": "0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593",
        "LINK/USD": "0xCC232DcFAaE6354cE191bd574108c1Ad03F86CeA",
        "USDC/USD": "0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3",
        "USDT/USD": "0xECef79e109E997BCa29c1c0897EC9D7678e00BB1",
        "DAI/USD": "0x8dBa75e83DA73cc766A7e5a0ee71F656BAb470d6",
        "OP/USD": "0x0D276FC14719f9292D5C1eA2198673d1f4269246",
        "SNX/USD": "0x2FCF37343e916eAEd1f1DdaaF84458a359b53877",
        "AAVE/USD": "0x338ed6787f463394D24813b297401B9F05a8C9d1",
        "WSTETH/USD": "0x698B585CbC4407e2D54aa898B2600B53C68958f7",
    },
    "polygon": {
        "ETH/USD": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "BTC/USD": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "MATIC/USD": "0xAB594600376Ec9fD91F8e885dADF0CE036862dE0",
        "LINK/USD": "0xd9FFdb71EbE7496cC440152d43986Aae0AB76665",
        "USDC/USD": "0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7",
        "USDT/USD": "0x0A6513e40db6EB1b165753AD52E80663aeA50545",
        "DAI/USD": "0x4746DeC9e833A82EC7C2C1356372CcF2cfcD2F3D",
        "AAVE/USD": "0x72484B12719E23115761D5DA1646945632979bB6",
        "UNI/USD": "0xdf0Fb4e4F928d2dCB76f438575fDD8682386e13C",
        "CRV/USD": "0x336584C8E6Dc19637A5b36206B1c79923111b405",
        "WSTETH/USD": "0x10f964234cae09cB6a9854B56FF7D4F38Cda5E6a",
    },
    "bsc": {
        "BNB/USD": "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE",
        "BTC/USD": "0x264990fbd0A4796A3E3d8E37C4d5F87a3aCa5Ebf",
        "ETH/USD": "0x9ef1B8c0E4F7dc8bF5719Ea496883DC6401d5b2e",
        "USDC/USD": "0x51597f405303c4377E36123CbF172bc359765377",
        "USDT/USD": "0xB97Ad0E74fa7d920791E90258A6E2085088b4320",
        "DAI/USD": "0x132d3C0B1D2cEa0BC552588063bdBb210FDeecfA",
        "LINK/USD": "0xca236E327F629f9Fc2c30A4E95775EbF0B89fac8",
        "CAKE/USD": "0xb6064eD41d4F67e353768AA239CA98F9c422E159",
        "AAVE/USD": "0xA8357BF572460fC40f4B0aCacbB2a6A61c89f475",
    },
    "avalanche": {
        "AVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
        "ETH/USD": "0x976B3D034E162d8bD72D6b9C989d545b839003b0",
        "BTC/USD": "0x2779D32d5166BAaa2B2b658333bA7e6Ec0C65743",
        "LINK/USD": "0x49cCd9Ca821efeAb2B98C60Dc60f518e765EdADc",
        "USDC/USD": "0xF096872672F44d6EBA71458D74fe67F9a77a23B9",
        "USDT/USD": "0xEBE676ee90Fe1112671f19b6B7459bC678B67e8a",
        "DAI/USD": "0x51D7180edA2260cc4F6e4EebB82FEF5c3c2B8300",
        "AAVE/USD": "0x3CA13391E9fb38a75330fb28f8cc2eB3D9ceceED",
        "JOE/USD": "0x02D35d3a8aC3e1626d3eE09A78Dd87286F5E8e3a",
        "WAVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
    },
    "sonic": {
        "ETH/USD": "0x824364077993847f71293B24ccA8567c00c2de11",
        "USDC/USD": "0x7A8443a2a5D772db7f1E40DeFe32db485108F128",
        "S/USD": "0xc76dFb89fF298145b417d221B2c747d84952e01d",
    },
    "linea": {
        "ETH/USD": "0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA",
        "BTC/USD": "0x7A99092816C8BD5ec8ba229e3a6E6Da1E628E1F9",
        "USDC/USD": "0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5",
        "USDT/USD": "0xefCA2bbe0EdD0E22b2e0d2F8248E99F4bEf4A7dB",
        "DAI/USD": "0x5133D67c38AFbdd02997c14Abd8d83676B4e309A",
    },
}

FROZEN_CHAINLINK_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "base": 8453,
    "optimism": 10,
    "polygon": 137,
    "bsc": 56,
    "avalanche": 43114,
    "sonic": 146,
    "linea": 59144,
}

FROZEN_ETH_DENOMINATED_FEEDS: dict[str, dict[str, str]] = {
    "ethereum": {
        "WSTETH/ETH": "0x86392dC19c0b719886221c78AB11eb8Cf5c52812",
    },
    "arbitrum": {
        "WSTETH/ETH": "0xb523AE262D20A936BC152e6023996e46FDC2A95D",
    },
    "base": {
        "WSTETH/ETH": "0x43a5C292A453A3bF3606fa856197f09D7B74251a",
    },
}

MULTISEND_CREATE2 = "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"
# Legacy keys included "gnosis" — not a registered chain, unreachable from
# every consumer (all resolve through registered-chain names).
# "hyperevm" declares the full Safe/Zodiac stack via safe_stack_contracts()
# (VIB-5606): canonical CREATE2 addresses, on-chain-verified live on chain 999,
# so it is in BOTH the multisend set and the full safe-stack set.
# "robinhood" (4663) likewise declares the full stack via safe_stack_contracts()
# (VIB-5708): the Zodiac ModuleProxyFactory + Roles v2 mastercopy were deployed
# at canonical CREATE2 addresses on 2026-07-09, so it is in BOTH sets too.
FROZEN_MULTISEND_CHAINS = frozenset(
    {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "gnosis",
        "bsc",
        "mantle",
        "xlayer",
        "hyperevm",
        "robinhood",
    }
)
FROZEN_SAFE_STACK_CHAINS = FROZEN_MULTISEND_CHAINS - {"gnosis", "mantle", "xlayer"}
FROZEN_SAFE_SIGNER_CONTRACTS: dict[str, str] = {
    "safe_proxy_factory_v1_4_1": "0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67",
    "safe_l2_singleton_v1_4_1": "0x29fcB43b46531BcA003ddC8FCB67FFE91900C762",
    "zodiac_module_proxy_factory": "0x000000000000aDdB49795b0f9bA5BC298cDda236",
    "zodiac_roles_modifier_singleton": "0x9646fDAD06d3e24444381f44362a3B0eB343D337",
}
FROZEN_ENSO_DELEGATES: dict[str, str] = {
    "enso_delegate_primary": "0x7663fd40081dccd47805c00e613b6beac3b87f08",
    "enso_delegate_secondary": "0xa2f4f9c6ec598ca8c633024f8851c79ca5f43e48",
}
FROZEN_ENSO_PRIMARY_CHAINS = frozenset({"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bsc"})

FROZEN_LP_UNISWAP: dict[str, str] = {
    "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
    "avalanche": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "bnb": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
}

FROZEN_LP_PROTO: dict[str, dict[str, str]] = {
    "sushiswap_v3": {
        "ethereum": "0x2214A42d8e2A1d20635c2cb0664422c528B6A432",
        "arbitrum": "0xf0cbCe1942A68BEb3d1B73f0Dd86C8Dcc643EF99",
        "optimism": "0x1af415a1EbA07a4986a52B6f2e7dE7003D82231e",
        "polygon": "0xb7402ee99F0A008e461098AC3A27F4957Df89a40",
        "base": "0x80C7DD17B01855a6D2347444a0FCC36136a314de",
        "avalanche": "0x18350b048AB366ed601fFDbC669110Ecb36016f3",
        "bnb": "0xF70c086618dcf2b1A461311275e00D6B722ef914",
    },
    "pancakeswap_v3": {
        "ethereum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "arbitrum": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "base": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
        "bnb": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364",
    },
}

# The three legacy entries proven WRONG on-chain (eth_getCode == "0x" at the
# legacy address; full bytecode at the registry address). Keyed by
# (protocol, canonical chain) -> (legacy_wrong, registry_correct).
PINNED_ONCHAIN_FIXES: dict[tuple[str, str], tuple[str, str]] = {
    ("sushiswap_v3", "arbitrum"): (
        "0xf0cbCe1942A68BEb3d1B73f0Dd86C8Dcc643EF99",
        "0xF0cBce1942A68BEB3d1b73F0dd86C8DCc363eF49",
    ),
    ("uniswap_v3", "avalanche"): (
        "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "0x655C406EBFa14EE2006250925e54ec43AD184f8B",
    ),
    ("uniswap_v3", "bsc"): (
        "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        "0x7b8A01B39D58278b5DE7e48c8449c9f4F5170613",
    ),
}

FROZEN_ENSO_TOKENS: dict[str, dict[str, str]] = {
    "base": {
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "WETH": "0x4200000000000000000000000000000000000006",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
    "ethereum": {
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
    "optimism": {
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "WETH": "0x4200000000000000000000000000000000000006",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
}


class TestChainlinkInversion:
    def test_usd_feeds_byte_equivalent(self) -> None:
        derived = {c: dict(v) for c, v in chainlink_usd_feeds_map().items()}
        assert derived == FROZEN_CHAINLINK_PRICE_FEEDS

    def test_chain_ids_byte_equivalent(self) -> None:
        assert dict(chainlink_chain_ids_map()) == FROZEN_CHAINLINK_CHAIN_IDS

    def test_chain_ids_cannot_drift_from_descriptor(self) -> None:
        for chain, chain_id in chainlink_chain_ids_map().items():
            assert ChainRegistry.resolve(chain).chain_id == chain_id

    def test_eth_denominated_byte_equivalent(self) -> None:
        derived = {c: dict(v) for c, v in chainlink_eth_denominated_map().items()}
        assert derived == FROZEN_ETH_DENOMINATED_FEEDS

    def test_public_module_views(self) -> None:
        from almanak.core.chainlink import (
            CHAINLINK_CHAIN_IDS,
            CHAINLINK_PRICE_FEEDS,
            ETH_DENOMINATED_FEEDS,
        )

        assert {c: dict(v) for c, v in CHAINLINK_PRICE_FEEDS.items()} == FROZEN_CHAINLINK_PRICE_FEEDS
        assert dict(CHAINLINK_CHAIN_IDS) == FROZEN_CHAINLINK_CHAIN_IDS
        assert {c: dict(v) for c, v in ETH_DENOMINATED_FEEDS.items()} == FROZEN_ETH_DENOMINATED_FEEDS


class TestSafeMultisendInversion:
    def test_derived_map_equals_legacy_minus_unreachable_gnosis(self) -> None:
        derived = dict(contract_address_map("safe_multisend"))
        assert set(derived) == FROZEN_MULTISEND_CHAINS - {"gnosis"}
        assert set(derived.values()) == {MULTISEND_CREATE2}

    def test_get_multisend_address_contract(self) -> None:
        from almanak.framework.execution.signer.safe.constants import get_multisend_address

        for chain in sorted(FROZEN_MULTISEND_CHAINS - {"gnosis"}):
            assert get_multisend_address(chain) == MULTISEND_CREATE2


class TestSafeSignerAddressInversion:
    def test_safe_and_zodiac_contract_maps_match_verified_safe_chains(self) -> None:
        for key, expected in FROZEN_SAFE_SIGNER_CONTRACTS.items():
            derived = dict(contract_address_map(key))
            assert set(derived) == FROZEN_SAFE_STACK_CHAINS
            assert set(derived.values()) == {expected}

    def test_legacy_safe_constants_derive_from_descriptor_contracts(self) -> None:
        from almanak.framework.execution.signer.safe.constants import (
            MODULE_PROXY_FACTORY,
            ROLES_MODIFIER_SINGLETON,
            SAFE_L2_SINGLETON_V1_4_1,
            SAFE_PROXY_FACTORY_V1_4_1,
        )

        assert SAFE_PROXY_FACTORY_V1_4_1 == FROZEN_SAFE_SIGNER_CONTRACTS["safe_proxy_factory_v1_4_1"]
        assert SAFE_L2_SINGLETON_V1_4_1 == FROZEN_SAFE_SIGNER_CONTRACTS["safe_l2_singleton_v1_4_1"]
        assert MODULE_PROXY_FACTORY == FROZEN_SAFE_SIGNER_CONTRACTS["zodiac_module_proxy_factory"]
        assert ROLES_MODIFIER_SINGLETON == FROZEN_SAFE_SIGNER_CONTRACTS["zodiac_roles_modifier_singleton"]

    def test_enso_delegate_contract_maps_match_legacy_membership(self) -> None:
        primary = dict(contract_address_map("enso_delegate_primary"))
        secondary = dict(contract_address_map("enso_delegate_secondary"))

        assert set(primary) == FROZEN_ENSO_PRIMARY_CHAINS
        assert {address.lower() for address in primary.values()} == {FROZEN_ENSO_DELEGATES["enso_delegate_primary"]}
        assert {chain: address.lower() for chain, address in secondary.items()} == {
            "ethereum": FROZEN_ENSO_DELEGATES["enso_delegate_secondary"]
        }

    def test_hyperevm_resolves_full_safe_stack_without_enso(self) -> None:
        """VIB-5606: HyperEVM (999) registers the canonical Safe/Zodiac stack so
        the Safe-wallet execution path resolves on chain 999, but declares NO
        Enso delegate (Enso isn't deployed there; CoreWriter is a CALL target)."""
        from almanak.framework.execution.signer.safe.constants import get_multisend_address

        # MultiSend + every Safe/Zodiac signer contract resolves for hyperevm.
        assert get_multisend_address("hyperevm") == MULTISEND_CREATE2
        for key, expected in FROZEN_SAFE_SIGNER_CONTRACTS.items():
            assert contract_address_map(key).get("hyperevm") == expected
        # No Enso delegate registered on hyperevm (would force DELEGATECALL).
        assert "hyperevm" not in contract_address_map("enso_delegate_primary")
        assert "hyperevm" not in contract_address_map("enso_delegate_secondary")

    def test_robinhood_resolves_full_safe_stack_without_enso(self) -> None:
        """VIB-5708: Robinhood Chain (4663) registers the canonical Safe/Zodiac
        stack so the Safe-wallet execution path resolves on chain 4663, but
        declares NO Enso delegate (Enso isn't deployed there)."""
        from almanak.framework.execution.signer.safe.constants import get_multisend_address

        # MultiSend + every Safe/Zodiac signer contract resolves for robinhood.
        assert get_multisend_address("robinhood") == MULTISEND_CREATE2
        for key, expected in FROZEN_SAFE_SIGNER_CONTRACTS.items():
            assert contract_address_map(key).get("robinhood") == expected
        # No Enso delegate registered on robinhood (would force DELEGATECALL).
        assert "robinhood" not in contract_address_map("enso_delegate_primary")
        assert "robinhood" not in contract_address_map("enso_delegate_secondary")

    def test_enso_delegate_operation_decision_derives_from_descriptor_contracts(self) -> None:
        from almanak.framework.execution.signer.safe.constants import (
            ENSO_DELEGATE_ADDRESSES,
            SafeOperation,
            get_operation_type,
            is_enso_delegate,
        )

        assert ENSO_DELEGATE_ADDRESSES == set(FROZEN_ENSO_DELEGATES.values())
        for address in FROZEN_ENSO_DELEGATES.values():
            assert is_enso_delegate(address.upper())
            assert get_operation_type(address) == SafeOperation.DELEGATE_CALL
        assert get_operation_type("0x0000000000000000000000000000000000000001") == SafeOperation.CALL


class TestLPPositionManagerInversion:
    def _resolve(self, chain: str, protocol: str) -> str | None:
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        reader = LPPositionReader.__new__(LPPositionReader)
        return reader._resolve_position_manager(chain, protocol)

    def test_legacy_entries_resolve_identically_except_pinned_fixes(self) -> None:
        for protocol, table in [("uniswap_v3", FROZEN_LP_UNISWAP), *FROZEN_LP_PROTO.items()]:
            for chain, legacy in table.items():
                canonical = ChainRegistry.resolve(chain).name  # bnb -> bsc
                got = self._resolve(chain, protocol)
                fix = PINNED_ONCHAIN_FIXES.get((protocol, canonical))
                if fix is not None:
                    wrong, correct = fix
                    assert legacy == wrong
                    assert got == correct, (protocol, chain)
                else:
                    assert got == legacy, (protocol, chain)

    def test_bnb_alias_and_canonical_resolve_the_same(self) -> None:
        assert self._resolve("bnb", "pancakeswap_v3") == self._resolve("bsc", "pancakeswap_v3")

    def test_unknown_protocol_falls_back_to_uniswap(self) -> None:
        assert self._resolve("ethereum", "no_such_protocol") == FROZEN_LP_UNISWAP["ethereum"]

    def test_miss_stays_none(self) -> None:
        assert self._resolve("berachain", "sushiswap_v3") is None


class TestEnsoTokenInversion:
    def test_legacy_chains_byte_identical(self) -> None:
        from almanak.framework.execution.enso_state_provider import TOKEN_ADDRESSES

        for chain, tokens in FROZEN_ENSO_TOKENS.items():
            assert TOKEN_ADDRESSES[chain] == tokens, chain

    def test_membership_is_descriptor_derived(self) -> None:
        # Documented widening: membership == chains whose tokens catalogue
        # declares both usdc and weth.
        from almanak.framework.execution.enso_state_provider import TOKEN_ADDRESSES

        expected = {
            d.name
            for d in ChainRegistry.all()
            if d.tokens is not None and "usdc" in d.tokens and "weth" in d.tokens
        }
        assert set(TOKEN_ADDRESSES) == expected
        assert set(FROZEN_ENSO_TOKENS) <= expected
