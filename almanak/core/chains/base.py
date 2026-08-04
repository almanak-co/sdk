"""Base (chain_id 8453) — Coinbase L2 (OP Stack)."""

from almanak.core.enums import ChainFamily

from ._contracts import safe_stack_contracts
from ._descriptor import (
    AnvilProfile,
    ChainDescriptor,
    Explorer,
    ExternalChainIds,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        name="base",
        chain_id=8453,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ether",
            decimals=18,
            wrapped_address="0x4200000000000000000000000000000000000006",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
            slip44=60,  # SLIP-44 coin type for Ether (CAIP-19 native)
        ),
        external_ids=ExternalChainIds(
            tenderly="base",
            coingecko="base",
            dexscreener="base",
            coingecko_onchain="base",
            defillama="base",
            defillama_display="Base",
            zerion="base",
            moralis="base",
            okx="8453",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=0.01,
            fallback_base_fee_gwei=0.001,
            fallback_priority_fee_gwei=0.001,
            # OP-stack GasPriceOracle predeploy for L1 data-cost estimation (Plan 026).
            # Same predeploy address as Optimism — OP Stack standard.
            l1_fee_oracle_kind="op_gaspriceoracle",
            l1_fee_oracle_address="0x420000000000000000000000000000000000000F",
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://base-rpc.publicnode.com",
            alchemy_prefix="base",
            tenderly_subdomain="base",
            anvil_port=8548,
            block_time_seconds=2.0,
            rate_limit_rpm=300,
            fork_requires_archive=True,
        ),
        explorer=Explorer(
            api_url="https://api.basescan.org/api",
            api_key_env="BASESCAN_API_KEY",
            browse_url="https://basescan.org",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "weth": "0x4200000000000000000000000000000000000006",
        },
        simulation=SimulationProfile(tenderly_supported=True, alchemy_network="base-mainnet"),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_stack_contracts(enso_delegate_primary=True),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WETH": "0x4200000000000000000000000000000000000006",
                "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
                "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
                "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
            },
            balance_slots={
                "USDC": 9,
                "WETH": 3,
                "USDbC": 9,
                "DAI": 0,
                "wstETH": 1,
            },
            whale_funded_tokens={
                "CBBTC": "0xBdb9300b7CDE636d9cD4AFF00f6F009fFBBc8EE6",
            },
            wrapped_native_deposit=True,
        ),
        bridged_stablecoin_variants=("USDbC",),
        aliases=(),
        color="#0052ff",  # Plan 027: Base blue (from legacy CHAIN_COLORS)
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("ETH", "WETH", "USDC", "USDbC", "DAI", "cbETH"),
    )
)
