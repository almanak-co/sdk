"""Ethereum mainnet (chain_id 1) — L1.

Source values mirror the legacy scattered dicts as of VIB-4801. Do not
change numeric values here without an explicit owner sign-off; the
chain_id is the on-the-wire identifier owned by ``metrics-database``.
"""

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
        name="ethereum",
        chain_id=1,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ether",
            decimals=18,
            wrapped_address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
            slip44=60,  # SLIP-44 coin type for Ether (CAIP-19 native)
        ),
        external_ids=ExternalChainIds(
            tenderly="mainnet",
            coingecko="ethereum",
            dexscreener="ethereum",
            coingecko_onchain="eth",
            defillama="ethereum",
            defillama_display="Ethereum",
            zerion="ethereum",
            moralis="eth",
            okx="1",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=300,
            cost_cap_native=0.1,
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES. Proxy tokens
            # like USDC need ~150k+ delegatecall gas, hence the buffer.
            operation_overrides={
                "swap_simple": 180000,
                "swap_multi_hop": 300000,
            },
            # Backtest-only fallback estimate (feeds
            # ``default_gas_price_gwei_for_chain`` and ``DEFAULT_GAS_PRICES``;
            # the live lane uses ``min_priority_fee_gwei`` below). Retuned
            # 2026-07-24 from the legacy 20+2=22 gwei, which was calibrated
            # for pre-blob L1 and overstated post-blob mainnet gas ~140x
            # (observed ~0.156 gwei total, 2026-07): every backtest using the
            # chain default simulated $15-20 of gas per mainnet tx. Base
            # matches the OBSERVED_TYPICAL_GAS_GWEI snapshot
            # (``framework/execution/gas/constants.py``, 2026-05-27/28
            # multi-RPC sweep); priority matches the ~0.05 gwei landable tip
            # measured in the VIB-5673 investigation. Total 0.21 gwei still
            # rounds up from observed — the conservative direction for
            # backtest cost estimation (same convention as robinhood.py).
            fallback_base_fee_gwei=0.16,
            fallback_priority_fee_gwei=0.05,
            # VIB-5419: live-submit tip floor. L1 nodes legitimately return
            # eth_maxPriorityFeePerGas=0; without a floor the tx ships with
            # tip≈0 and stalls/drops when the base fee rises.
            # VIB-5673: retuned 2.0 → 0.02 and made congestion-relative. This
            # is a SOFT anti-stall heuristic, not a protocol minimum. 2.0 gwei
            # was calibrated for pre-2024 L1 (base 20-50 gwei, so ~5% tip);
            # post-blob L1 sits at ~0.16 gwei, leaving the floor at 12.5x the
            # base fee and 86% of max_fee — it overrode the node's own landable
            # ~0.05 gwei estimate and, since the tip is always paid, cost ~10x
            # on every L1 tx. The effective floor is now
            # max(0.02, 0.05 * base_fee): ~5% of base once base > 0.4 gwei, and
            # pinned at the 0.02 gwei absolute component below that. 0.02 gwei
            # is a "greater than zero" anti-stall token for the case where the
            # node itself suggests 0 (i.e. blocks are not full).
            min_priority_fee_gwei=0.02,
        ),
        timeouts=Timeouts(
            tx_confirmation=300,
            grpc_execute=600,
        ),
        rpc=RpcProfile(
            public_rpc="https://ethereum-rpc.publicnode.com",
            alchemy_prefix="eth",
            tenderly_subdomain="mainnet",
            anvil_port=8549,
            block_time_seconds=12.0,
            rate_limit_rpm=300,
            fork_requires_archive=True,
            fork_cold_start_slow=True,
        ),
        explorer=Explorer(
            api_url="https://api.etherscan.io/api",
            api_key_env="ETHERSCAN_API_KEY",
            browse_url="https://etherscan.io",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS in
        # ``framework/intents/compiler_constants.py``. Lowercase symbol
        # keys, chain-canonical addresses.
        tokens={
            "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        },
        simulation=SimulationProfile(tenderly_supported=True, alchemy_network="eth-mainnet"),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_stack_contracts(enso_delegate_primary=True, enso_delegate_secondary=True),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
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
            balance_slots={
                "USDC": 9,
                "WETH": 3,
                "USDT": 2,
                "DAI": 2,
                "WBTC": 0,
                "wstETH": 0,
            },
            whale_funded_tokens={
                "USDC": "0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341",
            },
            wrapped_native_deposit=True,
        ),
        reorg_safe_depth=12,  # VIB-3350: deep reorg window on L1
        aliases=("eth", "mainnet"),
        color="#627eea",  # Plan 027: Ethereum blue (from legacy CHAIN_COLORS)
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("ETH", "WETH", "USDC", "USDT", "WBTC", "DAI", "stETH", "wstETH"),
    )
)
