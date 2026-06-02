"""Compound V3 (Comet) contract addresses, markets, and account-state table.

Single source of truth for this connector's on-chain address / market literals
(VIB-4929 PR-3b / epic VIB-4851). Relocated verbatim from ``adapter.py``, which
now re-exports these names for backward compatibility. Owning them here keeps
every Compound address / market literal in one connector-private module — the
same ownership pattern as ``morpho_blue/addresses.py``.

Surfaces:

* ``COMPOUND_V3_COMET_ADDRESSES`` — per-chain ``{market_id: comet_address}``. The
  Comet is per-*market* (one deployment per base asset), unlike the Aave family's
  single per-chain ``pool``.
* ``COMPOUND_V3_MARKETS`` — per-chain ``{market_id: {base_token, base_token_address,
  collaterals: {SYMBOL: {liquidation_collateral_factor, ...}}}}`` catalogue.
* ``_DEFAULT_MARKET_BY_CHAIN`` / ``default_compound_v3_market_for_chain`` — the
  canonical Comet ``market_id`` per chain when the intent omits ``market_id``.
* ``COMPOUND_V3_ACCOUNT_STATE_MARKETS`` — the derived per-market table the
  strategy-side account-state read consumes (VIB-4929 PR-3b): each
  ``COMPOUND_V3_MARKETS`` entry with its ``comet_address`` folded in, so the
  framework registry's ``market_params`` accessor stays fully generic (it just
  returns a connector-owned dict — no Compound-specific merge in the registry).
  The two source maps are intentionally NOT one-to-one, and either gap fails the
  account-state read closed (byte-equivalent to the legacy reader, which returned
  ``None`` when either lookup missed):

  * A market in ``COMPOUND_V3_MARKETS`` but absent from ``COMPOUND_V3_COMET_ADDRESSES``
    gets ``comet_address=None`` → the pure ``build_calls`` planner returns no calls.
  * A **Comet-only** market — in ``COMPOUND_V3_COMET_ADDRESSES`` but with no
    ``COMPOUND_V3_MARKETS`` config (today: ethereum ``wsteth`` / ``usds``) — is simply
    absent from this derived table (it iterates ``COMPOUND_V3_MARKETS`` keys), so
    ``market_params`` returns ``None`` for it.

  Onboarding a Comet-only market's account-state read is a data task (add its
  ``COMPOUND_V3_MARKETS`` entry: base token + collateral liquidation factors), not a
  framework change — out of scope for the PR-3b migration.

Gateway-boundary note: strategy-side, pure dict literals, no network egress.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

COMPOUND_V3_COMET_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "usdc": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "weth": "0xA17581A9E3356d9A858b789D68B4d866e593aE94",
        "usdt": "0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840",
        "wsteth": "0x3D0bb1ccaB520A66e607822fC55BC921738fAFE3",
        "usds": "0x5D409e56D886231aDAf00c8775665AD0f9897b56",
    },
    "arbitrum": {
        "usdc_bridged": "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA",
        "usdc": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "weth": "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486",
        "usdt": "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07",
    },
    "base": {
        "usdc": "0xb125E6687d4313864e53df431d5425969c15Eb2F",
        "weth": "0x46e6b214b524310239732D51387075E0e70970bf",
        "aero": "0x784efeB622244d2348d4F2522f8860B96fbEcE89",
    },
    "optimism": {
        # Verified on-chain: baseToken() returns 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85 (USDC on Optimism)
        "usdc": "0x2e44e174f7D53F0212823acC11C01A11d58c5bCB",
    },
    "polygon": {
        # Verified on-chain: baseToken() returns 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 (USDC.e bridged on Polygon)
        # Collateral: WETH (80% CF), WBTC (75%), WMATIC (65%), MaticX (55%)
        "usdc_e": "0xF25212E676D1F7F89Cd72fFEe66158f541246445",
        # Alias used by rate monitor (USDC.e -> usdc_bridged mapping in _COMPOUND_V3_TOKEN_TO_MARKET)
        "usdc_bridged": "0xF25212E676D1F7F89Cd72fFEe66158f541246445",
    },
}

_DEFAULT_MARKET_BY_CHAIN: dict[str, str] = {
    "ethereum": "usdc",
    "arbitrum": "usdc",
    "base": "usdc",
    "optimism": "usdc",
    "polygon": "usdc_e",
}


def default_compound_v3_market_for_chain(chain: str) -> str:
    """Return the canonical Comet ``market_id`` to use on ``chain`` when the
    caller omits ``intent.market_id``.

    Always returns a key that exists in ``COMPOUND_V3_COMET_ADDRESSES[chain]``
    when ``chain`` is supported; falls back to ``"usdc"`` for unknown chains
    so out-of-tree callers see a stable default.
    """
    return _DEFAULT_MARKET_BY_CHAIN.get(chain, "usdc")


COMPOUND_V3_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        "usdc": {
            "name": "USDC Market",
            "base_token": "USDC",
            "base_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collaterals": {
                "WETH": {
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "COMP": {
                    "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
                    "borrow_collateral_factor": Decimal("0.65"),
                    "liquidation_collateral_factor": Decimal("0.70"),
                    "liquidation_factor": Decimal("0.93"),
                },
                "UNI": {
                    "address": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
                    "borrow_collateral_factor": Decimal("0.75"),
                    "liquidation_collateral_factor": Decimal("0.81"),
                    "liquidation_factor": Decimal("0.93"),
                },
                "LINK": {
                    "address": "0x514910771AF9Ca656af840dff83E8264EcF986CA",
                    "borrow_collateral_factor": Decimal("0.79"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.93"),
                },
            },
        },
        "weth": {
            "name": "WETH Market",
            "base_token": "WETH",
            "base_token_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "collaterals": {
                "wstETH": {
                    "address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "cbETH": {
                    "address": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "rETH": {
                    "address": "0xae78736Cd615f374D3085123A210448E74Fc6393",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
            },
        },
        "usdt": {
            "name": "USDT Market",
            "base_token": "USDT",
            "base_token_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "collaterals": {
                "WETH": {
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
    },
    "arbitrum": {
        "usdc": {
            "name": "USDC Market (Native)",
            "base_token": "USDC",
            "base_token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "collaterals": {
                "WETH": {
                    "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "ARB": {
                    "address": "0x912CE59144191C1204E64559FE8253a0e49E6548",
                    "borrow_collateral_factor": Decimal("0.55"),
                    "liquidation_collateral_factor": Decimal("0.60"),
                    "liquidation_factor": Decimal("0.90"),
                },
                "GMX": {
                    "address": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
                    "borrow_collateral_factor": Decimal("0.50"),
                    "liquidation_collateral_factor": Decimal("0.55"),
                    "liquidation_factor": Decimal("0.90"),
                },
            },
        },
        "usdc_bridged": {
            "name": "USDC.e Market (Bridged)",
            "base_token": "USDC.e",
            "base_token_address": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            "collaterals": {
                "WETH": {
                    "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
        "weth": {
            "name": "WETH Market",
            "base_token": "WETH",
            "base_token_address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "collaterals": {
                "wstETH": {
                    "address": "0x5979D7b546E38E414F7E9822514be443A4800529",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "rETH": {
                    "address": "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
            },
        },
        "usdt": {
            "name": "USDT Market",
            "base_token": "USDT",
            "base_token_address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "collaterals": {
                "WETH": {
                    "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "WBTC": {
                    "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                    "borrow_collateral_factor": Decimal("0.70"),
                    "liquidation_collateral_factor": Decimal("0.77"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
    },
    "base": {
        "usdc": {
            "name": "USDC Market",
            "base_token": "USDC",
            "base_token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "collaterals": {
                "WETH": {
                    "address": "0x4200000000000000000000000000000000000006",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "cbETH": {
                    "address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
        "weth": {
            "name": "WETH Market",
            "base_token": "WETH",
            "base_token_address": "0x4200000000000000000000000000000000000006",
            "collaterals": {
                "cbETH": {
                    "address": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
                "wstETH": {
                    "address": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
                    "borrow_collateral_factor": Decimal("0.90"),
                    "liquidation_collateral_factor": Decimal("0.93"),
                    "liquidation_factor": Decimal("0.975"),
                },
            },
        },
        "aero": {
            "name": "AERO Market",
            "base_token": "AERO",
            "base_token_address": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
            "collaterals": {
                "WETH": {
                    "address": "0x4200000000000000000000000000000000000006",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        },
    },
    "optimism": {
        # USDC Comet on Optimism -- verified on-chain: baseToken() = 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85 (USDC)
        "usdc": {
            "name": "USDC Market",
            "base_token": "USDC",
            "base_token_address": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            "collaterals": {
                "WETH": {
                    "address": "0x4200000000000000000000000000000000000006",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "wstETH": {
                    "address": "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.95"),
                },
                "OP": {
                    "address": "0x4200000000000000000000000000000000000042",
                    "borrow_collateral_factor": Decimal("0.65"),
                    "liquidation_collateral_factor": Decimal("0.70"),
                    "liquidation_factor": Decimal("0.93"),
                },
            },
        },
    },
    "polygon": {
        # USDC.e Comet on Polygon -- verified on-chain: baseToken() = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 (USDC.e)
        "usdc_e": {
            "name": "USDC.e Market",
            "base_token": "USDC.e",
            "base_token_address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            "collaterals": {
                "WETH": {
                    "address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.93"),
                },
                "WBTC": {
                    "address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
                    "borrow_collateral_factor": Decimal("0.75"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.90"),
                },
                "WMATIC": {
                    "address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
                    "borrow_collateral_factor": Decimal("0.65"),
                    "liquidation_collateral_factor": Decimal("0.80"),
                    "liquidation_factor": Decimal("0.90"),
                },
                "MaticX": {
                    "address": "0xfa68FB4628DFF1028CFEc22b4162FCcd0d45efb6",
                    "borrow_collateral_factor": Decimal("0.55"),
                    "liquidation_collateral_factor": Decimal("0.65"),
                    "liquidation_factor": Decimal("0.90"),
                },
            },
        },
        # Alias used by rate monitor (USDC.e -> usdc_bridged mapping in _COMPOUND_V3_TOKEN_TO_MARKET)
        # Points to same USDC.e Comet on Polygon as usdc_e
        "usdc_bridged": {
            "name": "USDC.e Market",
            "base_token": "USDC.e",
            "base_token_address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            "collaterals": {
                "WETH": {
                    "address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
                    "borrow_collateral_factor": Decimal("0.80"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.93"),
                },
                "WBTC": {
                    "address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
                    "borrow_collateral_factor": Decimal("0.75"),
                    "liquidation_collateral_factor": Decimal("0.85"),
                    "liquidation_factor": Decimal("0.90"),
                },
                "WMATIC": {
                    "address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
                    "borrow_collateral_factor": Decimal("0.65"),
                    "liquidation_collateral_factor": Decimal("0.80"),
                    "liquidation_factor": Decimal("0.90"),
                },
                "MaticX": {
                    "address": "0xfa68FB4628DFF1028CFEc22b4162FCcd0d45efb6",
                    "borrow_collateral_factor": Decimal("0.55"),
                    "liquidation_collateral_factor": Decimal("0.65"),
                    "liquidation_factor": Decimal("0.90"),
                },
            },
        },
    },
}


# Derived per-market account-state table (VIB-4929 PR-3b). Each market's params
# with its Comet address folded in, so the strategy-side ``LendingReadRegistry``
# can resolve everything the pure account-state spec needs (base token, collateral
# liquidation factors, AND the per-market Comet target) from one connector-owned
# table — keeping the framework registry generic. Iterates ``COMPOUND_V3_MARKETS``
# keys: a Comet-only market id (no market params) has no account-state entry and
# fails closed in the planner, matching the legacy reader.
COMPOUND_V3_ACCOUNT_STATE_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    chain: {
        market_id: {**params, "comet_address": COMPOUND_V3_COMET_ADDRESSES.get(chain, {}).get(market_id)}
        for market_id, params in markets.items()
    }
    for chain, markets in COMPOUND_V3_MARKETS.items()
}


__all__ = [
    "COMPOUND_V3_ACCOUNT_STATE_MARKETS",
    "COMPOUND_V3_COMET_ADDRESSES",
    "COMPOUND_V3_MARKETS",
    "default_compound_v3_market_for_chain",
]
