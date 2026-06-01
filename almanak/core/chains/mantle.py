"""Mantle (chain_id 5000) — L2."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, RpcProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.MANTLE,
        name="mantle",
        chain_id=5000,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="MNT",
            name="Mantle",
            decimals=18,
            wrapped_address="0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
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
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES. Mantle gas
            # units are ~2000x higher than L1 equivalents (a Uniswap V3
            # swap uses ~150k on L1 but ~340M on Mantle). Gas prices are
            # proportionally lower (~0.02 Gwei), so actual cost in MNT is
            # comparable to other L2s (~$0.006/swap). Fallback values used
            # when simulation (Tenderly/Alchemy) is unavailable. Measured
            # via cast estimate: USDC approve ~203M, wrap ~118M,
            # unwrap ~146M.
            operation_overrides={
                "approve": 250_000_000,
                "swap_simple": 500_000_000,
                "swap_multi_hop": 800_000_000,
                "wrap_eth": 200_000_000,
                "unwrap_eth": 200_000_000,
                "lp_mint": 1_000_000_000,
                "lp_increase_liquidity": 400_000_000,
                "lp_decrease_liquidity": 500_000_000,
                "lp_collect": 400_000_000,
                "lp_burn": 200_000_000,
                "lending_supply": 600_000_000,
                "lending_borrow": 900_000_000,
                "vault_deposit": 400_000_000,
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
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
            "usdt": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
            "weth": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
            "wmnt": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        },
        aliases=(),
    )
)
