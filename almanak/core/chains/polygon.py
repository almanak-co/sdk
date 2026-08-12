"""Polygon PoS (chain_id 137) — Ethereum sidechain."""

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
        name="polygon",
        chain_id=137,
        family=ChainFamily.EVM,
        # Native symbol stays "MATIC" deliberately. Polygon renamed MATIC -> POL
        # (Sept 2024, 1:1), and the token resolver canonicalizes the native
        # sentinel to POL for token identity — but gas/price display remains
        # pinned to MATIC (the oracle MATIC/USD feed key and the gateway native
        # label derived from this descriptor). Managed Anvil funding uses the
        # address-shaped native sentinel and is independent of either ticker.
        # The two symbol views are bridged: ``symbol`` stays MATIC (gas/price
        # canonical) while ``accepted_symbols=("POL",)`` makes
        # both symbols route to the native-balance path (VIB-4851 A1, the
        # registry-derived replacement for NATIVE_SYMBOLS_BY_CHAIN["polygon"]).
        # Do NOT flip ``symbol`` to POL in isolation — see
        # tests/unit/core/test_polygon_native_symbol_parity.py for the contract.
        native=NativeToken(
            symbol="MATIC",
            name="Polygon",
            decimals=18,
            wrapped_address="0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
            accepted_symbols=("POL",),
            # POL id preferred over deprecated matic-network (VIB-3137)
            coingecko_id="polygon-ecosystem-token",
            wrapped_symbol="WMATIC",
            wrapped_coingecko_id="wmatic",
            slip44=966,  # SLIP-44 "Matic" — Polygon native (CAIP-19)
        ),
        external_ids=ExternalChainIds(
            tenderly="polygon",
            coingecko="polygon-pos",
            dexscreener="polygon",
            coingecko_onchain="polygon_pos",
            defillama="polygon",
            defillama_display="Polygon",
            zerion="polygon",
            moralis="polygon",
            okx="137",
        ),
        gas=GasProfile(
            buffer=1.2,
            simulation_buffer=0.2,
            # VIB-4879: bumped 500 → 1000. Mainnet snapshot 2026-05-27
            # observed Polygon live gas at ~284 gwei, leaving the previous
            # 500 cap with only 1.76× spike headroom. Polygon's PoS
            # economics make 5-10× short spikes routine during NFT mints
            # and busy DeFi periods. 1000 gwei = ~$0.013 per 150k-gas tx
            # at POL ~$0.087, well below cost_cap_native (50 MATIC) and
            # SANE_GWEI_CEILING (10_000).
            price_cap_gwei=1000,
            cost_cap_native=50.0,
            # Backtest-only fallback estimate (feeds
            # ``default_gas_price_gwei_for_chain`` and ``DEFAULT_GAS_PRICES``;
            # the live lane uses ``min_priority_fee_gwei`` below). Retuned
            # 2026-07-24 from the legacy 30+30=60 gwei, which UNDERSTATED
            # PoS-era gas ~5x — the one stale fallback in this defect class
            # (VIB-5811) that erred cheap, flattering every Polygon backtest.
            # base 285 rounds the OBSERVED_TYPICAL_GAS_GWEI snapshot (283.95,
            # 2026-05-27/28 multi-RPC sweep) up; a fresh 2026-07-24 sweep
            # (baseFeePerGas every 1000 blocks over the last 20_000, blocks
            # ~90.735M-90.755M) measured min 247.4 / median 251.1 / max 257.2
            # gwei — same magnitude, so the higher 2026-05 evidence stays as
            # the conservative pin. priority 30 is the protocol-enforced
            # validator tip floor (``min_priority_fee_gwei`` below; the
            # node's live tip suggestion measured 28.97 gwei on 2026-07-24).
            fallback_base_fee_gwei=285.0,
            fallback_priority_fee_gwei=30.0,
            # VIB-5419: live-submit tip floor. Polygon PoS validators enforce
            # a ~30 gwei minimum priority fee (mirrors the polymarket gateway's
            # POLYGON_MIN_PRIORITY_FEE_WEI); a node returning a lower estimate
            # would otherwise have its tx dropped.
            # VIB-5673: DELIBERATELY LEFT ABSOLUTE at 30.0 while ethereum and
            # avalanche were retuned down. This is a HARD, protocol-enforced
            # minimum, not a soft anti-stall heuristic — and it is already
            # well-calibrated (~10% of polygon's ~284 gwei measured base fee),
            # so it is not part of the VIB-5673 overpay. The relative term
            # (0.05 * base_fee = ~14 gwei at that base) is applied as a MAX
            # against this value precisely so it can never undercut the
            # validator minimum and get txs dropped.
            min_priority_fee_gwei=30.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=180,
            grpc_execute=360,
        ),
        rpc=RpcProfile(
            public_rpc="https://polygon-bor-rpc.publicnode.com",
            alchemy_prefix="polygon",
            anvil_port=8551,
            poa=True,
            block_time_seconds=2.0,
            rate_limit_rpm=300,
            fork_requires_archive=True,
            fork_cold_start_slow=True,
        ),
        explorer=Explorer(
            api_url="https://api.polygonscan.com/api",
            api_key_env="POLYGONSCAN_API_KEY",
            browse_url="https://polygonscan.com",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "usdt": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
            "weth": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_stack_contracts(enso_delegate_primary=True),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
                "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
                "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
                "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
                "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
            },
            balance_slots={
                "USDC": 9,
                "WETH": 3,
                "USDT": 2,
                "WMATIC": 3,
                "USDC.e": 0,
            },
            wrapped_native_deposit=True,
        ),
        bridged_stablecoin_variants=("USDC.e",),
        reorg_safe_depth=10,  # VIB-3350: PoS reorg window
        aliases=("matic",),
        color="#8247e5",  # Plan 027: Polygon purple (from legacy CHAIN_COLORS)
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("MATIC", "WMATIC", "USDC", "USDC.e", "USDT", "WETH", "WBTC", "DAI"),
    )
)
