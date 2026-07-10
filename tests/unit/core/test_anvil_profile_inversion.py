"""Equivalence tests for the CS-6 Anvil-profile inversion (VIB-4851 Phase E).

Fork-test funding facts (token addresses, balance slots, whale fallbacks,
wrapped-native deposit gate, Mantle gas quirk) move onto
``ChainDescriptor.anvil``; teardown bridged-stable variants move onto
``ChainDescriptor.bridged_stablecoin_variants``. Legacy tables are frozen
verbatim (display-case keys are load-bearing and compared
case-SENSITIVELY); the two documented divergences are pinned: sonic's
wrapped symbol is stored as the true contract symbol ``"wS"`` (legacy
``"WS"``; the only consumer uppercases both sides), and the
protocol-harness default token catalogue widens to the full funding map
(zero external constructors; legacy entries address-identical).
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import (
    anvil_balance_slots_map,
    anvil_block_gas_limit_map,
    anvil_funding_tokens_map,
    anvil_whale_tokens_map,
    bridged_stablecoin_map,
    wrapped_native_deposit_symbol_map,
)

FROZEN_TOKEN_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "GMX": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "stETH": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
        "rETH": "0xae78736Cd615f374D3085123A210448E74Fc6393",
        "cbETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "swETH": "0xf951E335afb289353dc249e82926178EaC7DEd78",
        "ankrETH": "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb",
        "pufETH": "0xD9A442856C234a39a81a089C06451EBAa4306a72",
    },
    "optimism": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "OP": "0x4200000000000000000000000000000000000042",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    },
    "polygon": {
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
    },
    "avalanche": {
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDC.e": "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "BTC.b": "0x152b9d0FdC40C096757F570A51E494bd4b943E50",
        "sAVAX": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
    },
    "linea": {
        "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
    },
    "plasma": {
        "WXPL": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
        "USDT0": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
        "FUSDT0": "0x1DD4b13fcAE900C60a350589BE8052959D2Ed27B",
        "PENDLE": "0x17Bac5F906c9A0282aC06a59958D85796c831f24",
    },
    "berachain": {
        "WBERA": "0x6969696969696969696969696969696969696969",
        "HONEY": "0xFCBD14DC51f0A4d49d5E53C2E0950e0bC26d0Dce",
        "USDC.e": "0x549943e04f40284185054145c6E4e9568C1D3241",
        "WETH": "0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
        "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
    },
    "sonic": {
        "wS": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        "WETH": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "USDC": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
        "USDT": "0x6047828dc181963ba44974801FF68e538dA5eaF9",
    },
    "monad": {
        "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
        "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        "USDT0": "0xe7cd86e13AC4309349F30B3435a9d337750fC82D",
        "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
    },
    "xlayer": {
        "WOKB": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        "WETH": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
        "xETH": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
        "USDC": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
        "USDT": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
        "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
        "WBTC": "0xEA034fb02eB1808C2cc3adbC15f447B93CbE08e1",
    },
    "robinhood": {
        "WETH": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
        "USDG": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
        "USDe": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
    },
}

FROZEN_KNOWN_BALANCE_SLOTS: dict[str, dict[str, int]] = {
    "arbitrum": {
        "USDC": 9,
        "WETH": 51,
        "USDC.e": 51,
        "USDT": 51,
        "DAI": 2,
        "WBTC": 51,
        "ARB": 51,
        "GMX": 0,
        "wstETH": 1,
    },
    "ethereum": {
        "USDC": 9,
        "WETH": 3,
        "USDT": 2,
        "DAI": 2,
        "WBTC": 0,
        "wstETH": 0,
    },
    "base": {
        "USDC": 9,
        "WETH": 3,
        "USDbC": 9,
        "DAI": 0,
        "wstETH": 1,
    },
    "avalanche": {
        "USDC": 9,
        "WAVAX": 3,
        "USDT": 2,
        "USDC.e": 0,
        "WETH.e": 0,
        "BTC.b": 0,
        "sAVAX": 0,
    },
    "optimism": {
        "USDC": 9,
        "WETH": 3,
        "USDT": 0,
        "USDC.e": 0,
        "OP": 0,
    },
    "polygon": {
        "USDC": 9,
        "WETH": 3,
        "USDT": 2,
        "WMATIC": 3,
        "USDC.e": 0,
    },
    "bsc": {
        "USDC": 1,
        "WBNB": 3,
        "USDT": 1,
        "BUSD": 0,
    },
    "linea": {
        "USDC": 9,
        "WETH": 3,
        "USDT": 51,
    },
    "sonic": {
        "USDC": 9,
        "WETH": 0,
    },
    "xlayer": {
        "USDT0": 51,
    },
    "robinhood": {
        "WETH": 51,
        "USDG": 1,
        "USDe": 5,
    },
}

FROZEN_WHALE_FUNDED_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "USDC": "0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341",
    },
    "base": {
        "CBBTC": "0xBdb9300b7CDE636d9cD4AFF00f6F009fFBBc8EE6",
    },
    "robinhood": {
        "WETH": "0x07aE8551Be970cB1cCa11Dd7a11F47Ae82e70E67",
        "USDG": "0x2d4d2A025b10C09BDbd794B4FCe4F7ea8C7d7bB4",
        "USDE": "0x70aC345AB736ce145E0D4B5deCEd7A8bcB0E4033",
    },
}

FROZEN_WRAPPED_NATIVE_TOKENS: dict[str, str] = {
    "ethereum": "WETH",
    "arbitrum": "WETH",
    "base": "WETH",
    "optimism": "WETH",
    "polygon": "WMATIC",
    "linea": "WETH",
    "avalanche": "WAVAX",
    "bsc": "WBNB",
    "sonic": "WS",
    "mantle": "WMNT",
    "robinhood": "WETH",
}

FROZEN_CHAIN_BLOCK_GAS_LIMITS: dict[str, int] = {
    "mantle": 3000000000,
}

FROZEN_CHAIN_BRIDGED_STABLECOINS: dict[str, tuple[str, ...]] = {
    "arbitrum": ("USDC.e",),
    "optimism": ("USDC.e",),
    "polygon": ("USDC.e",),
    "avalanche": ("USDC.e", "DAI.e", "USDT.e"),
    "base": ("USDbC",),
    "berachain": ("USDC.e",),
}

FROZEN_HARNESS_TEST_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    },
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    },
    "optimism": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
    },
    "polygon": {
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
    "avalanche": {
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
    },
}


class TestAnvilProfileInversion:
    def test_funding_tokens_byte_equivalent_case_sensitive(self) -> None:
        derived = {c: dict(v) for c, v in anvil_funding_tokens_map().items()}
        assert derived == FROZEN_TOKEN_ADDRESSES

    def test_balance_slots_byte_equivalent(self) -> None:
        derived = {c: dict(v) for c, v in anvil_balance_slots_map().items()}
        assert derived == FROZEN_KNOWN_BALANCE_SLOTS

    def test_slot_keys_present_in_funding_tokens_case_sensitive(self) -> None:
        # Cross-table invariant: every slot-patched symbol must have an
        # address row, with EXACT display case ("wstETH", "USDC.e").
        for chain, slots in anvil_balance_slots_map().items():
            tokens = anvil_funding_tokens_map()[chain]
            for symbol in slots:
                assert symbol in tokens, (chain, symbol)

    def test_whales_byte_equivalent(self) -> None:
        derived = {c: dict(v) for c, v in anvil_whale_tokens_map().items()}
        assert derived == FROZEN_WHALE_FUNDED_TOKENS

    def test_wrapped_deposit_gate_membership_and_symbols(self) -> None:
        derived = dict(wrapped_native_deposit_symbol_map())
        assert set(derived) == set(FROZEN_WRAPPED_NATIVE_TOKENS)
        for chain, legacy_symbol in FROZEN_WRAPPED_NATIVE_TOKENS.items():
            # Pinned divergence: sonic stores the true contract symbol
            # "wS" where the legacy map said "WS"; the only consumer
            # (fork_manager deposit-funding) uppercases both sides.
            assert derived[chain].upper() == legacy_symbol.upper(), chain

    def test_deposit_gate_does_not_widen_to_unverified_wrappers(self) -> None:
        # Chains with a wrapped_symbol but WITHOUT the legacy deposit
        # verification must stay outside the gate.
        gated = set(wrapped_native_deposit_symbol_map())
        for d in ChainRegistry.all():
            if d.name not in FROZEN_WRAPPED_NATIVE_TOKENS:
                assert d.name not in gated, d.name

    def test_block_gas_limits_byte_equivalent(self) -> None:
        assert dict(anvil_block_gas_limit_map()) == FROZEN_CHAIN_BLOCK_GAS_LIMITS

    def test_bridged_stablecoins_byte_equivalent_incl_order(self) -> None:
        derived = dict(bridged_stablecoin_map())
        assert derived == FROZEN_CHAIN_BRIDGED_STABLECOINS
        # Anti-widening: every other chain returns no variants (absence is
        # load-bearing — VIB-3814: a phantom USDC.e on BSC burned the
        # teardown harness window).
        for d in ChainRegistry.all():
            if d.name not in FROZEN_CHAIN_BRIDGED_STABLECOINS:
                assert d.bridged_stablecoin_variants == (), d.name

    def test_fork_manager_module_views(self) -> None:
        from almanak.framework.anvil.fork_manager import (
            KNOWN_BALANCE_SLOTS,
            TOKEN_ADDRESSES,
            WHALE_FUNDED_TOKENS,
        )

        assert {c: dict(v) for c, v in TOKEN_ADDRESSES.items()} == FROZEN_TOKEN_ADDRESSES
        assert {c: dict(v) for c, v in KNOWN_BALANCE_SLOTS.items()} == FROZEN_KNOWN_BALANCE_SLOTS
        assert {c: dict(v) for c, v in WHALE_FUNDED_TOKENS.items()} == FROZEN_WHALE_FUNDED_TOKENS

    def test_solana_fork_manager_descriptor_cross_check(self) -> None:
        from almanak.framework.anvil.solana_fork_manager import (
            SOLANA_TOKEN_MINTS,
            WSOL_MINT,
        )

        assert WSOL_MINT == "So11111111111111111111111111111111111111112"
        solana = ChainRegistry.resolve("solana")
        assert solana.native.wrapped_address == WSOL_MINT
        assert SOLANA_TOKEN_MINTS["SOL"] == WSOL_MINT

    def test_harness_widening_pinned(self) -> None:
        from almanak.framework.testing.protocol_harness import TestContext

        context = TestContext(adapter=None, config=None)
        # Every legacy (chain, symbol) entry resolves to the SAME address …
        for chain, tokens in FROZEN_HARNESS_TEST_TOKENS.items():
            for symbol, address in tokens.items():
                assert context.test_tokens[chain][symbol] == address, (chain, symbol)
        # … and the documented widening: membership == the funding map.
        assert set(context.test_tokens) == set(anvil_funding_tokens_map())


class TestPriceSourceIntersection:
    """CS-7: the paper-engine 6-chain price-source set derives as
    feeds ∩ TWAP pools instead of three hand-kept copies."""

    FROZEN_PRICE_SOURCE_CHAINS = frozenset(
        {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"}
    )

    def test_engine_set_byte_equivalent(self) -> None:
        from almanak.framework.backtesting.paper.engine import _PRICE_SOURCE_CHAINS

        assert _PRICE_SOURCE_CHAINS == self.FROZEN_PRICE_SOURCE_CHAINS

    def test_divergence_gate_byte_equivalent(self) -> None:
        from almanak.framework.backtesting.paper._engine_helpers import (
            _chainlink_divergence_chains,
        )

        assert _chainlink_divergence_chains() == self.FROZEN_PRICE_SOURCE_CHAINS

    def test_subset_semantics_preserved(self) -> None:
        # bsc / linea / sonic have Chainlink feeds but no TWAP pools — they
        # must stay OUT (the legacy comment's deliberate-subset contract).
        from almanak.core.chains._helpers import chainlink_usd_feeds_map
        from almanak.framework.backtesting.paper.engine import _PRICE_SOURCE_CHAINS

        assert {"bsc", "linea", "sonic"} <= set(chainlink_usd_feeds_map())
        assert not ({"bsc", "linea", "sonic"} & _PRICE_SOURCE_CHAINS)
