"""Aave V3 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the entries previously held in ``almanak.core.contracts`` (W1 / VIB-4853
/ epic VIB-4851). Surfaced to non-connector callers through
:class:`GatewayAddressCapability` on ``AaveV3GatewayConnector``;
strategy-side connector code reads the dicts directly.

Two address surfaces live here:

* ``AAVE_V3`` — per-chain Pool + PoolDataProvider + AaveOracle addresses
  for every deployed Aave V3 market.
* ``AAVE_V3_TOKENS`` — the canonical underlying-token address catalogue
  (one entry per market reserve) consumed by the strategy-side adapter.

The contract-kind vocabulary (``pool`` / ``pool_data_provider`` /
``oracle``) is connector-private — callers outside this folder should
consume the gateway registry, not guess key names.
"""

from __future__ import annotations

AAVE_V3: dict[str, dict[str, str]] = {
    "ethereum": {
        "pool": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "pool_data_provider": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        "oracle": "0x54586bE62E3c3580375aE3723C145253060Ca0C2",
    },
    "arbitrum": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7",
    },
    "optimism": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xD81eb3728a631871a7eBBaD631b5f424909f0c77",
    },
    "polygon": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xb023e699F5a33916Ea823A16485e259257cA8Bd1",
    },
    "base": {
        "pool": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
        "pool_data_provider": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac",
        "oracle": "0x2Cc0Fc26eD4563A5ce5e8bdcfe1A2878676Ae156",
    },
    "avalanche": {
        "pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xEBd36016B3eD09D4693Ed4251c67Bd858c3c7C9C",
    },
    "bsc": {
        "pool": "0x6807dc923806fE8Fd134338EABCA509979a7e0cB",
        "pool_data_provider": "0xc90Df74A7c16245c5F5C5870327Ceb38Fe5d5328",
        "oracle": "0x39bc1bfDa2130d6Bb6DBEfd366939b4c7aa7C697",
    },
    "linea": {
        "pool": "0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac",
        "pool_data_provider": "0x47cd4b507B81cB831669c71c7077f4daF6762FF4",
        "oracle": "0xCFDAdA7DCd2e785cF706BaDBC2B8Af5084d595e9",
    },
    "plasma": {
        "pool": "0x925a2A7214Ed92428B5b1B090F80b25700095e12",
        "pool_data_provider": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
        "oracle": "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7",
    },
    "sonic": {
        "pool": "0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3",
        "pool_data_provider": "0xc0a344397cfa89dF1e1d3e4fb330834D789cF2CD",
        "oracle": "0xD63f7658C66B2934Bd234D79D06aEF5290734B30",
    },
    "mantle": {
        "pool": "0x458F293454fE0d67EC0655f3672301301DD51422",
        "pool_data_provider": "0x487c5c669D9eee6057C44973207101276cf73b68",
        "oracle": "0x47a063CfDa980532267970d478EC340C0F80E8df",
    },
    "xlayer": {
        # Aave V3.6 — Governance Proposal #460
        # Source: https://github.com/aave-dao/aave-address-book/blob/main/src/AaveV3XLayer.sol
        "pool": "0xE3F3Caefdd7180F884c01E57f65Df979Af84f116",
        "pool_data_provider": "0x6C505C31714f14e8af2A03633EB2Cdfb4959138F",
        "oracle": "0x91FC11136d5615575a0fC5981Ab5C0C54418E2C6",
    },
}

AAVE_V3_TOKENS: dict[str, dict[str, str]] = {
    "ethereum": {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "LINK": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
        "AAVE": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
        "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "cbETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "rETH": "0xae78736Cd615f374D3085123A210448E74Fc6393",
    },
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        "LINK": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        "rETH": "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8",
    },
    "optimism": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "wstETH": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb",
        "OP": "0x4200000000000000000000000000000000000042",
        "rETH": "0x9Bcef72be871e61ED4fBbc7630889beE758eb81D",
    },
    "polygon": {
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "WBTC": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
        "LINK": "0x53E0bca35eC356BD5ddDFebbD1Fc0fD03FaBad39",
        "wstETH": "0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
        "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    },
    "avalanche": {
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "WETH.e": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
        "DAI.e": "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70",
        "WBTC.e": "0x50b7545627a5162F82A992c33b87aDc75187B218",
        "LINK.e": "0x5947BB275c521040051D82396192181b413227A3",
        "sAVAX": "0x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE",
    },
    "bsc": {
        "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "WETH": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
        "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "USDT": "0x55d398326f99059fF775485246999027B3197955",
        "BTCB": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",
    },
    "linea": {
        "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
        "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        "DAI": "0x4AF15ec2A0BD43Db75dd04E62FAA3B8EF36b00d5",
        "WBTC": "0x3aAB2285ddcDdaD8edf438C1bAB47e1a9D05a9b4",
    },
    "plasma": {
        "WETH": "0x9895D81bB462A195b4922ED7De0e3ACD007c32CB",
    },
    "sonic": {
        "wS": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        "WETH": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
        "USDC": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
        "USDT": "0x6047828dc181963ba44974801FF68e538dA5eaF9",
    },
    "mantle": {
        "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",  # Canonical Mantle Bridged WETH (deterministic bridge address, not a placeholder)
        "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
        "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
        "USDe": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
        "GHO": "0xfc421aD3C883Bf9E7C4f42dE845C4e4405799e73",
    },
    "xlayer": {
        # Aave V3.6 reserves on X-Layer (verified on-chain via getReservesList)
        "WOKB": "0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",  # USD₮0 (LayerZero bridged USDT)
        "xETH": "0xE7B000003A45145decf8a28FC755aD5eC5EA025A",
        "xBTC": "0xb7C00000bcDEeF966b20B3D884B98E64d2b06b4f",
        "GHO": "0xDe6539018B095353A40753Dc54C91C68c9487D4E",
        "USDG": "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8",  # Gravity USD (Aave borrow reserve)
    },
}

__all__ = ["AAVE_V3", "AAVE_V3_TOKENS"]
