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
chain-specific values have been taken yet.

``rpc.block_time_seconds`` and ``explorer.api_url`` stay ``None`` **on purpose**
(VIB-5811) — each is a verified finding, not an unfinished deferral, and the
per-field comments below carry the live evidence. Setting them does not enable
backtesting; it silently corrupts it. Robinhood therefore stays out of
``DEFAULT_ARCHIVE_RPC_CHAINS`` (``almanak/config/backtest.py``), which costs
nothing real: the Chainlink and TWAP providers do not consult that tuple at all
(their ``ARCHIVE_RPC_CHAINS`` lists are hardcoded, and ``CHAINLINK_PRICE_FEEDS``
has no ``robinhood`` key), and the gas provider's archive path would return
wrong-era data (see ``rpc``). Chainlink *is* live on 4663 (55 feeds in the
official reference-data-directory, ``latestRoundData`` verified answering
2026-07-14) — wiring it is its own ticket and needs an archive RPC: the
descriptor's ``public_rpc`` is state-pruned (``eth_getBalance`` at an old block
→ "missing trie node"), while Alchemy serves archive depth. Three non-defaults:

* ``gas`` declares the Arbitrum L1-fee oracle (``arbitrum_nodeinterface`` at the
  ArbGasInfo precompile ``0x…006C``, verified responding on 4663) because
  Robinhood is a genuine L2 that posts calldata to Ethereum — unlike HyperEVM,
  a standalone L1 with no L1 data cost.
* ``gas`` also carries measured backtest fallback fees (VIB-5811). Without them
  every Robinhood backtest priced gas at Ethereum's 22 gwei — ~413x the real
  ~0.053 gwei.
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
        #
        # Backtest fallback fees (VIB-5811), measured live 2026-07-14 from
        # baseFeePerGas sampled every 1000 blocks over the last 20_000 blocks:
        # min 0.05293 / median 0.05328 / max 0.05427 gwei (eth_gasPrice agreed
        # at 0.05340). 0.055 rounds the median UP — these feed backtest cost
        # estimation, where over-stating gas is the conservative direction.
        # priority_fee is a measured 0.0, not an unmeasured blank: the Orbit
        # sequencer runs first-come-first-served with no priority auction, so
        # there is no tip to pay (same shape as arbitrum, which this rollup
        # derives from, and bsc). Empty≠Zero — every consumer gates on
        # ``is None`` (pnl/config.py:103, providers/gas.py:399), so the 0.0
        # survives as a measured value rather than collapsing to "unset".
        #
        # Without these, ``default_gas_price_gwei_for_chain`` falls through to
        # ``ChainRegistry.conservative_gas_fallback()`` → ethereum's 20+2=22
        # gwei, over-stating Robinhood gas by ~413x (a $3 demo swap simulated
        # $7.06 of gas and turned a +4.16% benchmark into a -36.46% return).
        gas=GasProfile(
            l1_fee_oracle_kind="arbitrum_nodeinterface",
            l1_fee_oracle_address="0x000000000000000000000000000000000000006C",
            fallback_base_fee_gwei=0.055,
            fallback_priority_fee_gwei=0.0,
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.mainnet.chain.robinhood.com",
            # Verified live: robinhood-mainnet.g.alchemy.com (https + wss).
            alchemy_prefix="robinhood",
            anvil_port=8560,  # next free port after hyperevm (8559)
            # block_time_seconds is deliberately NOT set (VIB-5811). Do not add
            # it without reading this: the value is known, and wiring it is the
            # bug.
            #
            # Measured live 2026-07-14: 0.1002 s/block, stable to 4 decimals
            # across 10/100/1k/10k/100k/1M-block windows. But block time is NOT
            # constant over this chain's history — realised s/block by range:
            #
            #     1 →     1_000   1310.65     (block 1 is 2026-04-30)
            # 1_000 →   100_000     30.14
            # 100k  →   500_000      2.28
            # 500k  → 1_000_000      0.37
            #   1M  → 2_000_000      0.14
            #   4M  →    latest      0.1002   (stable only since 2026-07-08)
            #
            # The sole consumer that would use it, gateway/data/gas/etherscan.py,
            # extrapolates linearly across the WHOLE span:
            #     target_block = max(1, latest - int(seconds_ago / block_time))
            # which is only valid if block time held constant back to the target.
            # With 0.1 that clamps to block 1 for any window ≥14d and silently
            # returns block 1's baseFeePerGas — stamped DataConfidence.HIGH —
            # from a timestamp 45-290 days off the request. Mainnet launched
            # 2026-07-01, so a normal 30/90d backtest predates the chain outright.
            #
            # Setting this also silently widens BLOCKS_PER_DAY / replay --chain
            # choices past an explicit guard that exists to prevent exactly that
            # (tests/unit/core/test_chain_config_maps_inversion.py, "membership
            # did not widen"). Revisit only once the clamp is a bisect
            # (Blockscout's block/getblocknobytime works here) or bounded by a
            # timestamp-drift check.
        ),
        # Blockscout explorer. Only the human-facing browse URL is declared.
        #
        # api_url / api_key_env are deliberately NOT set (VIB-5811) — this is a
        # verified finding, not a deferral (it discharges VIB-5706's "left for
        # the first-connector PR" note). Explorer.api_url means "this chain has
        # an Etherscan-compatible gas oracle"; Robinhood does not. The only
        # Etherscan-style query the SDK ever issues is
        # {"module": "gastracker", "action": "gasoracle"}
        # (gateway/data/gas/etherscan.py) and Blockscout on 4663 answers it
        # {"message":"Unknown module","result":null,"status":"0"} (verified live
        # 2026-07-14; module=proxy is absent too). Declaring api_url would only
        # trade "no explorer API URL configured" for "gas oracle returned
        # unsuccessful status" and add a rate-limited HTTP round-trip per call.
        # Gas prices exist on Blockscout's v2 REST surface (/api/v2/stats), which
        # is not Etherscan-compatible and has no SDK consumer. No api_key_env:
        # the API needs no key (every probe above was unauthenticated).
        explorer=Explorer(browse_url="https://robinhoodchain.blockscout.com"),
        # Canonical Safe v1.4.1 + Zodiac Roles v2 stack (CREATE2 — same address
        # on every EVM chain; deployed + on-chain-verified on Robinhood, VIB-5708).
        # No Enso delegate (Enso not deployed here).
        contracts=safe_stack_contracts(enso_delegate_primary=False),
        # Lowercase symbol → chain-canonical ERC-20 address, all verified on-chain
        # at block 5_610_000. USDG is "Global Dollar" (Paxos, 6 dec), the loan asset
        # of every Morpho market; it shares the USDG ticker with an unrelated
        # "Gravity USD" on X-Layer, so it is keyed by its own address, never aliased
        # to USDC. USDe is Ethena's canonical cross-chain address (Morpho collateral,
        # PR 2). No canonical Circle-USDC / Tether-USDT with real liquidity exists on
        # 4663 — deliberately omitted, not invented (the 6-dec USDC/USDT that exist
        # are dead, ~11 holders). Which of these is the chain's dollar is NOT stated
        # here in prose: it is declared as data on ``canonical_stable`` below, with
        # one reader (VIB-5727).
        tokens={
            "weth": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
            "usdg": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
            "usde": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
        },
        # The chain's canonical dollar, declared as DATA because no ordering
        # heuristic over the token registry gets 4663 right (VIB-5727).
        #
        # Both USDG and USDe are registered stablecoins here, so any
        # registry-order / is_stablecoin ranking is free to pick either — and
        # the framework's generic picker
        # (``permissions/synthetic_intents.py:_candidate_stable_symbols``)
        # picks USDE first. That answer is not merely arbitrary, it is WRONG:
        # USDe has zero-liquidity pools on 4663 (VIB-5729 — a USDG→USDe swap
        # leg is refused by the price-impact guard), while WETH/USDG is the
        # only pair with a real V3 pool (~$3.5M TVL, fee tier 500). A
        # consolidation or synthetic-approval target of USDe therefore cannot
        # route at all.
        #
        # The deciding fact — which dollar has liquidity — is not derivable
        # from any registry field, so it is declared here rather than inferred.
        # A swap connector's permission-hints module currently hand-pins the
        # same fact; folding that override into this field is tracked separately
        # (it flips the Zodiac manifest and needs its own regression review).
        canonical_stable="USDG",
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
