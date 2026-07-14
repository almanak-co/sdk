"""Robinhood Chain (chain_id 4663) — Arbitrum Orbit L2 settling to Ethereum.

Robinhood Chain went live on mainnet 2026-07-01. It is a Nitro/Orbit rollup
(ArbSys / ArbGasInfo precompiles verified live on-chain 2026-07-09), native gas
token ETH, ~0.1s block time. The canonical wrapped native is WETH
(``0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73``) — resolved from the token0 leg
of the earliest Uniswap V3 pools (factory
``0x1f7d7550B1b028f7571E69A784071F0205FD2EfA``), NOT from ``WETH9()`` on the
periphery (which reverts here) and NOT from a Blockscout symbol search (many
same-symbol scam tokens). It is WETH9-style: ``deposit()`` / ``withdraw()`` are
present in the runtime bytecode, so it is managed-Anvil deposit-fundable.

Registered plumbing-first (VIB-5706); Uniswap V3 (swap + LP) and Morpho Blue
(lending) now target Robinhood (VIB-5709, VIB-5710). The fields nothing consumes
today (receipt polling, gas buffers/caps, chainlink, simulation) remain at their
documented-miss defaults — the framework defaults apply and no measured
chain-specific values have been taken yet. Note ``rpc.block_time_seconds`` and
``explorer.api_url`` are both still unset, which keeps Robinhood out of
``DEFAULT_ARCHIVE_RPC_CHAINS`` (``almanak/config/backtest.py``): backtests of a
Robinhood strategy silently get no historical gas/Chainlink/TWAP data. Two
non-defaults:

* ``gas`` declares the Arbitrum L1-fee oracle (``arbitrum_nodeinterface`` at the
  ArbGasInfo precompile ``0x…006C``, verified responding on 4663) because
  Robinhood is a genuine L2 that posts calldata to Ethereum — unlike HyperEVM,
  a standalone L1 with no L1 data cost.
* ``contracts=safe_stack_contracts(enso_delegate_primary=False)`` — the
  canonical Safe v1.4.1 + Zodiac Roles v2 stack (VIB-5708). The Safe v1.4.1
  stack was already live; the Zodiac ModuleProxyFactory + Roles v2 mastercopy
  (plus the Integrity/Packer linked libs) were deployed at their canonical
  CREATE2 addresses on 2026-07-09 via ERC-2470 replay and verified
  byte-identical to Base (see ``docs/internal/scripts/deploy_zodiac_robinhood.sh``).
  No Enso delegate: Enso is not deployed on Robinhood, so advertising the
  DELEGATECALL delegates would target non-existent code.

Every value below is a public, explorer-verifiable fact.
"""

from almanak.core.enums import ChainFamily

from ._contracts import safe_stack_contracts
from ._descriptor import (
    AnvilProfile,
    ChainDescriptor,
    Explorer,
    GasProfile,
    NativeToken,
    RpcProfile,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        name="robinhood",
        chain_id=4663,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            # Canonical WETH — token0 of the earliest V3 pools; WETH9-style
            # (deposit()/withdraw() present). Verified on-chain 2026-07-09.
            wrapped_address="0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
            slip44=60,  # SLIP-44 coin type for Ether (CAIP-19 native)
        ),
        # Orbit L2: declare the Arbitrum ArbGasInfo precompile for L1 data-cost
        # estimation (Plan 026). Gas buffers/caps left unset — the framework
        # defaults apply (buffer 1.2; the price/cost caps are advisory maps with
        # no production consumer, operators cap via MAX_GAS_*). Arbitrum, the
        # chain this rollup derives from, measures buffer=1.5 and
        # simulation_buffer=0.5; no equivalent measurement taken here yet.
        gas=GasProfile(
            l1_fee_oracle_kind="arbitrum_nodeinterface",
            l1_fee_oracle_address="0x000000000000000000000000000000000000006C",
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.mainnet.chain.robinhood.com",
            # Verified live: robinhood-mainnet.g.alchemy.com (https + wss).
            alchemy_prefix="robinhood",
            anvil_port=8560,  # next free port after hyperevm (8559)
        ),
        # Blockscout explorer. Only the human-facing browse URL is declared; the
        # Etherscan-compatible API surface (api_url / api_key_env) is still unset.
        # Declaring it (with rpc.block_time_seconds) is what admits Robinhood to
        # DEFAULT_ARCHIVE_RPC_CHAINS and the backtesting historical providers.
        explorer=Explorer(browse_url="https://robinhoodchain.blockscout.com"),
        # Canonical Safe v1.4.1 + Zodiac Roles v2 stack (CREATE2 — same address
        # on every EVM chain; deployed + on-chain-verified on Robinhood, VIB-5708).
        # No Enso delegate (Enso not deployed here).
        contracts=safe_stack_contracts(enso_delegate_primary=False),
        # Lowercase symbol → chain-canonical ERC-20 address, all verified on-chain
        # at block 5_610_000. USDG is "Global Dollar" (Paxos, 6 dec) — the chain's
        # canonical stable and the loan asset of every Morpho market; it shares the
        # USDG ticker with an unrelated "Gravity USD" on X-Layer, so it is keyed by
        # its own address, never aliased to USDC. USDe is Ethena's canonical
        # cross-chain address (Morpho collateral, PR 2). No canonical Circle-USDC /
        # Tether-USDT with real liquidity exists on 4663 — deliberately omitted, not
        # invented (the 6-dec USDC/USDT that exist are dead, ~11 holders).
        tokens={
            "weth": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
            "usdg": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
            "usde": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
        },
        # Per-vendor chain slugs, all verified 2026-07-09: CoinGecko asset-platform
        # id "robinhood" (chain_identifier 4663), DexScreener "robinhood",
        # GeckoTerminal "robinhood" (went live 2026-07-09), and DeFiLlama chain slug
        # "robinhood-chain" (/v2/historicalChainTvl/robinhood-chain returns data).
        external_ids={
            "coingecko": "robinhood",
            "dexscreener": "robinhood",
            "geckoterminal": "robinhood",
            "defillama": "robinhood-chain",
        },
        # Managed-Anvil fork-test funding (facts verified at block 5_610_000). WETH
        # additionally funds via WETH9 deposit() (wrapped_native_deposit). Balance
        # slots are standard keccak(abi.encode(holder, slot)); for WETH/USDG the
        # slot targets the EIP-1967 proxy address. Whales are the impersonation
        # fallback when slot-patching fails.
        anvil=AnvilProfile(
            funding_tokens={
                "WETH": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
                "USDG": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
                "USDe": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
            },
            balance_slots={
                "WETH": 51,
                "USDG": 1,
                "USDe": 5,
            },
            whale_funded_tokens={
                # WETH whale is an EOA holder, NOT the V3 pool 0x69Bf… — never
                # impersonate a protocol contract (pool-owned balance / hooks).
                "WETH": "0x07aE8551Be970cB1cCa11Dd7a11F47Ae82e70E67",
                "USDG": "0x2d4d2A025b10C09BDbd794B4FCe4F7ea8C7d7bB4",
                "USDE": "0x70aC345AB736ce145E0D4B5deCEd7A8bcB0E4033",
            },
            wrapped_native_deposit=True,
        ),
        aliases=(),
    )
)
