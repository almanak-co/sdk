"""HyperEVM (chain_id 999) — Hyperliquid's EVM execution layer (standalone L1).

Native gas token is HYPE; the canonical wrapper is WHYPE
(``0x5555555555555555555555555555555555555555``).

Distinct from the HyperCore perps connector — a perps orderbook venue (its own
venue key, not an EVM chain). This descriptor is the general-purpose EVM chain:
enum ``HYPEREVM``, canonical name ``"hyperevm"``. The two do not collide, and
the perps venue key is deliberately NOT registered as an alias here.

HYPE's CoinGecko coin id and the official RPC host both contain the perps
connector's protocol key (shared project identity). The self-containment guard
(``tests/unit/connectors/test_supported_chains_registry.py``, VIB-5575) treats
those vendor-data contexts as legitimate, but the connector key must still never
appear here as prose or a support declaration.

Mostly plumbing-only registration: no protocol *connector* targets HyperEVM
yet, so the test-infra / pricing-vendor fields that nothing consumes today
(``anvil``, ``chainlink``, ``simulation``, gas caps) are left at their
documented-miss defaults and populated when the first connector lands. The one
non-default is ``contracts=safe_stack_contracts()`` — the canonical Safe v1.4.1
+ Zodiac Roles stack, whose CREATE2 addresses are identical on every EVM chain
and are on-chain-verified live on HyperEVM (VIB-5606). Registering it makes the
Safe-wallet execution path (``get_multisend_address``, the Safe/Roles signer
address maps) resolve on chain 999 so a Safe/Zodiac-scoped agent can execute
here; without it those lookups raise. No Enso delegate is declared (Enso is not
deployed on HyperEVM, and CoreWriter is a plain CALL, not a DELEGATECALL
target). Every value below is a public, explorer-verifiable fact.
"""

from almanak.core.enums import ChainFamily

from ._contracts import safe_stack_contracts
from ._descriptor import (
    ChainDescriptor,
    Explorer,
    GasProfile,
    NativeToken,
    RpcProfile,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        name="hyperevm",
        chain_id=999,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="HYPE",
            name="Hyperliquid",
            decimals=18,
            wrapped_address="0x5555555555555555555555555555555555555555",
            wrapped_symbol="WHYPE",
            # HYPE's CoinGecko coin id equals the perps connector's protocol key
            # (shared project identity). The self-containment guard treats
            # coingecko_id values as vendor data, so this is allowed (VIB-5575).
            coingecko_id="hyperliquid",
            # WHYPE has its own CoinGecko listing, 1:1 with HYPE.
            wrapped_coingecko_id="wrapped-hype",
            # SLIP-0044 registered coin type for HYPE / Hyperliquid (0x999).
            slip44=2457,
        ),
        # Empty profile: gas knobs left unset (fall back to framework defaults at
        # the lookup boundary) until a strategy actually executes on HyperEVM.
        gas=GasProfile(),
        rpc=RpcProfile(
            # Official public RPC. The host contains the perps connector's key
            # (shared project identity); the self-containment guard treats URL
            # literals as vendor data, so this is allowed (VIB-5575).
            public_rpc="https://rpc.hyperliquid.xyz/evm",
            anvil_port=8559,  # first free port (8545–8558, 8899 already taken)
        ),
        explorer=Explorer(browse_url="https://hyperevmscan.io"),
        # Canonical Safe v1.4.1 + Zodiac Roles stack (CREATE2 — same address on
        # every EVM chain; on-chain-verified live on HyperEVM, VIB-5606). No Enso
        # delegate: Enso is not deployed here and CoreWriter is a CALL target.
        contracts=safe_stack_contracts(),
        # Lowercase symbol → chain-canonical ERC-20 address (explorer-verified).
        tokens={
            "whype": "0x5555555555555555555555555555555555555555",
            "usdt0": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
        },
        # Per-vendor chain slugs (values verbatim). HyperEVM's price-vendor slug
        # is "hyperevm" on CoinGecko and DexScreener. Other vendors (GeckoTerminal,
        # DeFiLlama, …) are left for the first-connector PR — matching the vendor
        # coverage the other no-connector chains (blast/linea/plasma) declare today.
        external_ids={
            "coingecko": "hyperevm",
            "dexscreener": "hyperevm",
        },
        aliases=(),  # deliberately NOT the perps venue key (a different product)
    )
)
