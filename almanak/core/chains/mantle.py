"""Mantle (chain_id 5000) — L2."""

from almanak.core.enums import ChainFamily

from ._contracts import safe_multisend_contracts
from ._descriptor import (
    AnvilProfile,
    ChainDescriptor,
    Explorer,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        name="mantle",
        chain_id=5000,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="MNT",
            name="Mantle",
            decimals=18,
            wrapped_address="0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
            coingecko_id="mantle",
            wrapped_symbol="WMNT",
            wrapped_coingecko_id="mantle",
            # No verified Mantle SLIP-44 entry; native CAIP-19 stays fail-loud.
            slip44=None,
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            # VIB-4879: bumped 10 → 100. Mainnet snapshot 2026-05-27
            # observed Mantle live gas at ~50 gwei, so the previous cap
            # blocked every intent regardless of any env override.
            # cost_cap_native (50 MNT) remains the defense-in-depth
            # backstop for accidental absurd-fee scenarios.
            price_cap_gwei=100,
            cost_cap_native=50.0,
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES.
            #
            # STALE PREMISE, CORRECTED 2026-07-22: this table was originally
            # sized under "Mantle gas units are ~2000x higher than L1
            # equivalents" (a real historical Mantle gas-metering quirk,
            # measured via cast estimate at the time: USDC approve ~203M,
            # wrap ~118M, unwrap ~146M). That premise no longer holds.
            # Live evidence gathered 2026-07-22: current Mantle mainnet block
            # gas limit is 60,000,000 (every value below individually
            # exceeded it, several by 10x+); a full recent block used only
            # 163,345 gas total; and a real live `eth_estimateGas` for an
            # actual MNT->USDT swap via Agni Finance returned 347,088 — normal
            # L1-equivalent scale, not the ~2000x-inflated figures below. This
            # means the "never go below the compiler-provided floor" safety
            # clamp (orchestrator.py `_update_gas_estimate`) was turning a
            # perfectly good live estimate into an unsubmittable
            # "exceeds block gas limit" transaction on every swap, 100% of
            # the time (the floor is `override x gas.buffer`, e.g. this
            # table's OLD swap_simple=500_000_000 x 1.5 buffer = 750,000,000
            # -- 12x the real block limit).
            #
            # Values below are the OLD table divided by 2000 (the file's own
            # documented original inflation factor) — directly corroborated
            # for approve/swap_simple/unwrap_eth by 6 independent real Mantle
            # mainnet transactions (see docs/internal/uat-runs/VIB-5958/evidence.md;
            # the live estimate now legitimately wins over this floor, exactly
            # as intended). The remaining entries share the same chain-level
            # gas-metering premise so the same 2000x correction should hold,
            # but split into two different kinds of "not yet verified":
            #   - lp_mint / lp_increase_liquidity / lp_decrease_liquidity /
            #     lp_collect / lp_burn / vault_deposit: currently UNREACHABLE
            #     dead config — no LP or vault connector declares support for
            #     this chain today (see each connector's own
            #     supported_chains.py — which connectors run on which chain is
            #     CONNECTOR knowledge, never named here), so no code path can
            #     exercise them.
            #   - lending_supply / lending_borrow: REACHABLE (a lending
            #     connector IS registered for this chain) but NOT independently
            #     re-measured — a real mainnet SUPPLY attempt on this chain hit
            #     an unrelated revert before gas estimation was ever reached
            #     (VIB-5959). These are live money-path floors the moment
            #     VIB-5959 clears; re-measure via `cast estimate` against real
            #     mainnet contracts before trusting them at a large
            #     TRADING_CAP_USD, and update this comment with the fresh
            #     measurements when done.
            operation_overrides={
                "approve": 125_000,
                "swap_simple": 250_000,
                "swap_multi_hop": 400_000,
                "wrap_eth": 100_000,
                "unwrap_eth": 100_000,
                "lp_mint": 500_000,
                "lp_increase_liquidity": 200_000,
                "lp_decrease_liquidity": 250_000,
                "lp_collect": 200_000,
                "lp_burn": 100_000,
                "lending_supply": 300_000,
                "lending_borrow": 450_000,
                "vault_deposit": 200_000,
            },
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.mantle.xyz",
            alchemy_prefix="mantle",
            anvil_port=8556,
        ),
        explorer=Explorer(browse_url="https://mantlescan.xyz"),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
            "usdt": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
            "weth": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
            "wmnt": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "mantle",
            "dexscreener": "mantle",
            "geckoterminal": "mantle",
        },
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_multisend_contracts(),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            wrapped_native_deposit=True,
            block_gas_limit=3_000_000_000,  # Mantle non-standard gas accounting (VIB-3666/VIB-3746)
        ),
        aliases=(),
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("MNT", "WMNT", "USDC", "USDT", "WETH", "mETH"),
    )
)
