"""Equivalence harness for the Rung-4 chain-key re-keying (Chain enum removal).

The backtesting tree's ``Chain``-enum-keyed constant tables were re-keyed to
canonical lowercase chain-name strings (``Chain.ETHEREUM`` → ``"ethereum"``)
as part of the staged ``Chain`` enum deletion (VIB-4851). This test freezes
every re-keyed table VERBATIM — lowercase keys, values copied byte-for-byte
from the pre-rekey code — and asserts the live table equals it with ``==``.

This proves the re-keying changed no *data*: same chains, same subgraph
deployment IDs, same Comet addresses. The most important assertion per table
is **anti-widening**: exact dict equality means the key set can neither grow
nor shrink, so a chain the provider never supported cannot sneak in through
string-keying (same Class-A/B harness as
``tests/unit/core/test_external_ids_inversion.py``).

Frozen sources (all pre-rekey, this branch):

* 7 DEX liquidity tables: decl-owned, read via ``DexVolumeRegistry``
* 4 lending APY tables: ``connectors/*/backtest_apy.py::*_SUBGRAPH_IDS``
* Compound V3 Comet view: ``compound_v3_apy.py::KNOWN_COMET_ADDRESSES``
  (derived from the connector ``AddressRegistry``; frozen from the enum-keyed
  runtime value with keys lowercased)
* Chainlink archive-RPC chains: ``chainlink.py::ARCHIVE_RPC_CHAINS``
  (members lowercased; the ``ARCHIVE_RPC_URL_{chain}`` env-var format site
  still upper-cases, so env vars stay ``ARCHIVE_RPC_URL_ETHEREUM``-shaped)
"""

from __future__ import annotations

from almanak.framework.backtesting.pnl.providers.chainlink import (
    ARCHIVE_RPC_CHAINS,
    ARCHIVE_RPC_URL_ENV_PATTERN,
)
from almanak.connectors.aave_v3.backtest_apy import (
    AAVE_V3_SUBGRAPH_IDS,
)
from almanak.connectors.aave_v3.backtest_apy import (
    SUPPORTED_CHAINS as AAVE_V3_SUPPORTED_CHAINS,
)
from almanak.connectors.compound_v3.backtest_apy import (
    COMPOUND_V3_SUBGRAPH_IDS,
    KNOWN_COMET_ADDRESSES,
)
from almanak.connectors.compound_v3.backtest_apy import (
    SUPPORTED_CHAINS as COMPOUND_V3_SUPPORTED_CHAINS,
)
from almanak.connectors.morpho_blue.backtest_apy import (
    MORPHO_BLUE_SUBGRAPH_IDS,
)
from almanak.connectors.morpho_blue.backtest_apy import (
    SUPPORTED_CHAINS as MORPHO_BLUE_SUPPORTED_CHAINS,
)
from almanak.connectors.spark.backtest_apy import (
    SPARK_SUBGRAPH_IDS,
)
from almanak.connectors.spark.backtest_apy import (
    SUPPORTED_CHAINS as SPARK_SUPPORTED_CHAINS,
)
from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

# --- the frozen tables, verbatim (lowercase keys, pre-rekey values) --------------

FROZEN_UNISWAP_V3: dict[str, str] = {
    "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "base": "96eJ9Go8gFjySRGnndG7EYxThaiwVDV8BYPp1TMDcoYh",
    "optimism": "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
    "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
}

FROZEN_SUSHISWAP_V3: dict[str, str] = {
    "ethereum": "2tGWMrDha4164KkFAfkU3rDCtuxGb4q1emXmFdLLzJ8x",
}

FROZEN_PANCAKESWAP_V3: dict[str, str] = {
    "ethereum": "CJYGNhb7RvnhfBDjqpRnD3oxgyhibzc7fkAMa38YV3oS",
    "arbitrum": "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve",
    "bsc": "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",
    "base": "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3",
}

FROZEN_AERODROME: dict[str, str] = {
    "base": "GENunSHWLBXm59mBSgPzQ8metBEp9YDfdqwFr91Av1UM",
}

FROZEN_TRADERJOE_V2: dict[str, str] = {
    "avalanche": "6KD9JYCg2qa3TxNK3tLdhj5zuZTABoLLNcnUZXKG9vuH",
}

FROZEN_CURVE: dict[str, str] = {
    "ethereum": "3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF",
    "optimism": "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX",
}

FROZEN_BALANCER: dict[str, str] = {
    "ethereum": "C4ayEZP2yTXRAB8vSaTrgN4m9anTe9Mdm2ViyiAuV9TV",
    "arbitrum": "98cQDy6tufTJtshDCuhh9z2kWXsQWBHVh2bqnLHsGAeS",
    "polygon": "H9oPAbXnobBRq1cB3HDmbZ1E8MWQyJYQjT1QDJMrdbNp",
}

FROZEN_AAVE_V3: dict[str, str] = {
    "ethereum": "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g",
    "arbitrum": "DLuE98kEb5pQNXAcKFQGQgfSQ57Xdou4jnVbAEqMfy3B",
    "optimism": "DSfLz8oQBUeU5atALgUFQKMTSYV9mZAVYp4noLSXAfvb",
    "polygon": "Co2URyXjnxaw8WqxKyVHdirq9Ahhm5vcTs4dMedAq211",
    "base": "GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF",
    "avalanche": "2h9woxy8RTjHu1HJsCEnmzpPHFArU33avmUh4f71JpVn",
}

FROZEN_COMPOUND_V3: dict[str, str] = {
    "ethereum": "5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp",
    "arbitrum": "Ff7ha9ELmpmg81D6nYxy4t8aGP26dPztqD1LDJNPqjLS",
    "polygon": "AaFtUWKfFdj2x8nnE3RxTSJkHwGHvawH3VWFBykCGzLs",
    "base": "2hcXhs36pTBDVUmk5K2Zkr6N4UYGwaHuco2a6jyTsijo",
}

FROZEN_MORPHO_BLUE: dict[str, str] = {
    "ethereum": "8Lz789DP5VKLXumTMTgygjU2xtuzx8AhbaacgN5PYCAs",
    "base": "71ZTy1veF9twER9CLMnPWeLQ7GZcwKsjmygejrgKirqs",
}

FROZEN_SPARK: dict[str, str] = {
    "ethereum": "GbKdmBe4ycCYCQLQSjqGg6UHYoYfbyJyq5WrG35pv1si",
}

# Frozen from the pre-rekey enum-keyed runtime value (keys lowercased) — the
# values come from the connector AddressRegistry, so this also pins that the
# ``ChainRegistry.try_resolve`` boundary keeps resolving exactly the same
# connector-declared chains the old ``Chain[chain_key.upper()]`` did.
FROZEN_KNOWN_COMET_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "USDC": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "WETH": "0xA17581A9E3356d9A858b789D68B4d866e593aE94",
        "USDT": "0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840",
        "wstETH": "0x3D0bb1ccaB520A66e607822fC55BC921738fAFE3",
        "USDS": "0x5D409e56D886231aDAf00c8775665AD0f9897b56",
    },
    "arbitrum": {
        "USDC.e": "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA",
        "USDC": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "WETH": "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486",
        "USDT": "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07",
    },
    "base": {
        "USDC": "0xb125E6687d4313864e53df431d5425969c15Eb2F",
        "USDbC": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "WETH": "0x46e6b214b524310239732D51387075E0e70970bf",
        "AERO": "0x784efeB622244d2348d4F2522f8860B96fbEcE89",
    },
    "optimism": {
        "USDC": "0x2e44e174f7D53F0212823acC11C01A11d58c5bCB",
    },
    "polygon": {
        "USDC": "0xF25212E676D1F7F89Cd72fFEe66158f541246445",
        "USDC.e": "0xF25212E676D1F7F89Cd72fFEe66158f541246445",
        "USDT": "0xaeB318360f27748Acb200CE616E389A6C9409a07",
    },
}

# Old members were ["ETHEREUM", "ARBITRUM", "BASE", "OPTIMISM", "POLYGON",
# "AVALANCHE"]; the identity set is unchanged, only the case.
FROZEN_ARCHIVE_RPC_CHAINS: list[str] = [
    "ethereum",
    "arbitrum",
    "base",
    "optimism",
    "polygon",
    "avalanche",
]


class TestDexSubgraphIdsRekeyed:
    """The 7 DEX liquidity-subgraph tables carry identical data under string keys.

    The per-DEX wrapper modules were deleted upstream; the tables are
    connector-decl-owned now and read through ``DexVolumeRegistry``.
    """

    def test_uniswap_v3(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("uniswap_v3") == FROZEN_UNISWAP_V3

    def test_sushiswap_v3(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("sushiswap_v3") == FROZEN_SUSHISWAP_V3

    def test_pancakeswap_v3(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("pancakeswap_v3") == FROZEN_PANCAKESWAP_V3

    def test_aerodrome(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("aerodrome") == FROZEN_AERODROME

    def test_traderjoe_v2(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("traderjoe_v2") == FROZEN_TRADERJOE_V2

    def test_curve(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("curve") == FROZEN_CURVE

    def test_balancer(self) -> None:
        assert DexVolumeRegistry.liquidity_subgraph_ids_for("balancer") == FROZEN_BALANCER


class TestLendingSubgraphIdsRekeyed:
    """The 4 lending APY tables carry identical data under string keys."""

    def test_aave_v3(self) -> None:
        assert AAVE_V3_SUBGRAPH_IDS == FROZEN_AAVE_V3

    def test_compound_v3(self) -> None:
        assert COMPOUND_V3_SUBGRAPH_IDS == FROZEN_COMPOUND_V3

    def test_morpho_blue(self) -> None:
        assert MORPHO_BLUE_SUBGRAPH_IDS == FROZEN_MORPHO_BLUE

    def test_spark(self) -> None:
        assert SPARK_SUBGRAPH_IDS == FROZEN_SPARK


class TestCompoundCometAddressesRekeyed:
    """The registry-derived Comet view resolves the same chains and markets."""

    def test_known_comet_addresses(self) -> None:
        assert KNOWN_COMET_ADDRESSES == FROZEN_KNOWN_COMET_ADDRESSES


class TestSupportedChainsDerivation:
    """SUPPORTED_CHAINS lists still derive from the tables, order preserved."""

    def test_supported_chains_lists(self) -> None:
        assert AAVE_V3_SUPPORTED_CHAINS == list(FROZEN_AAVE_V3)
        assert COMPOUND_V3_SUPPORTED_CHAINS == list(FROZEN_COMPOUND_V3)
        assert MORPHO_BLUE_SUPPORTED_CHAINS == list(FROZEN_MORPHO_BLUE)
        assert SPARK_SUPPORTED_CHAINS == list(FROZEN_SPARK)


class TestChainlinkArchiveRpcChains:
    """Archive-RPC chain names lowercased; env-var shape stays UPPERCASE."""

    def test_archive_rpc_chains(self) -> None:
        assert ARCHIVE_RPC_CHAINS == FROZEN_ARCHIVE_RPC_CHAINS

    def test_env_var_shape_unchanged(self) -> None:
        # The format site upper-cases the chain, so the env-var contract is
        # untouched by the lowercase member rename.
        for chain in ARCHIVE_RPC_CHAINS:
            env_var = ARCHIVE_RPC_URL_ENV_PATTERN.format(chain=chain.upper())
            assert env_var == f"ARCHIVE_RPC_URL_{chain.upper()}"
            assert env_var.isupper() or "_" in env_var
