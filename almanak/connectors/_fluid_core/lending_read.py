"""Fluid fToken aggregate account-state read (VIB-5030).

fTokens are ERC-4626 vaults with a single supply leg and NO debt leg, so the
aggregate state is simply the wallet's share balance marked to underlying:

    assets = balanceOf(wallet) * convertToAssets(1e18) / 1e18
    collateral_usd = assets / 10**decimals(underlying) * price(underlying)
    debt_usd = 0 (measured zero — supplying to an fToken cannot create debt)
    health_factor = None (no liquidation surface on a pure supply)

The read is **market-scoped** (Compound V3 / Silo V2 precedent): the target
fToken is bound by the registry from the market table's ``comet_address``
(the registry's generic market-scoped target key), and the market id derives
from the intent's underlying token symbol via ``query_inputs_fn`` — fluid
lending intents carry no ``market_id`` (one fToken per underlying per chain,
VIB-5030 position-key design).

The market table pins are verified live on-chain against
``LendingResolver.getAllFTokens()`` + ``asset()`` (2026-06-11); the
``test_fluid_lending`` intent tests re-verify them against the fork on every
run, so a (theoretical) fToken migration fails tests rather than silently
reading a stale vault.

Gateway-boundary note: strategy-side pure planners/reducers + dict literals,
no network egress; the framework reader executes the ``EthCall``s through
the gateway.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.lending_read_base import (
    AccountStateQuery,
    AccountStateReadSpec,
    EthCall,
    LendingAccountState,
    pad_address,
)

logger = logging.getLogger(__name__)

# ERC-4626 / ERC-20 read selectors
_BALANCE_OF_SELECTOR = "0x70a08231"
_CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"

# Share-price probe scale: assets-per-1e18-shares. Using a fixed probe keeps
# the planner pure (no dependence on the wallet's balance read) and division
# by the same constant in the reducer is exact for any share decimals.
_SHARE_PROBE = 10**18

#: Per-chain fToken market catalogue, keyed by lowercased underlying symbol.
#: ``comet_address`` is the registry's generic market-scoped target key (the
#: fToken vault); ``loan_token`` names the symbol whose price/decimals the
#: framework injects (``valuation_role_keys``). Verified on-chain 2026-06-11.
FLUID_FTOKEN_MARKETS: dict[str, dict[str, dict[str, Any]]] = {
    "arbitrum": {
        "usdc": {"comet_address": "0x1A996cb54bb95462040408C06122D45D6Cdb6096", "loan_token": "USDC"},
        "usdt": {"comet_address": "0x4A03F37e7d3fC243e3f99341d36f4b829BEe5E03", "loan_token": "USDT"},
    },
    "base": {
        "usdc": {"comet_address": "0xf42f5795D9ac7e9D757dB633D693cD548Cfd9169", "loan_token": "USDC"},
        "weth": {"comet_address": "0x9272D6153133175175Bc276512B2336BE3931CE9", "loan_token": "WETH"},
    },
}


def _pad_uint(value: int) -> str:
    return f"{value:064x}"


def _query_inputs_from_intent(intent: Any) -> dict[str, Any]:
    """Market id = the intent's underlying token symbol.

    Fluid lending intents carry no ``market_id`` (the compiler resolves the
    single fToken per underlying); the market table is keyed by underlying
    symbol, so the read derives its key from ``intent.token`` directly.
    An explicit ``intent.market_id`` is ignored here on purpose — the
    compiler already validates it matches the resolved fToken.
    """
    return {"market_id": getattr(intent, "token", None)}


def _build_fluid_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Plan the two fToken reads: share balance, then share price probe.

    Fails closed (returns ``[]``) when the market-scoped fToken target was
    not bound — an unknown market never reads against a placeholder.
    """
    ftoken = query.position_manager_address
    if not ftoken:
        return []
    user_hex = pad_address(query.wallet_address)
    return [
        EthCall(to=ftoken, data=_BALANCE_OF_SELECTOR + user_hex),
        EthCall(to=ftoken, data=_CONVERT_TO_ASSETS_SELECTOR + _pad_uint(_SHARE_PROBE)),
    ]


def _decode_word(hex_data: str | None) -> int | None:
    if not hex_data:
        return None
    data = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data
    if len(data) < 64:
        return None
    try:
        return int(data[:64], 16)
    except ValueError:
        return None


def _reduce_fluid_account_state(query: AccountStateQuery, results: list[str | None]) -> LendingAccountState | None:
    """Reduce (share balance, share price) into the canonical aggregate state.

    Empty ≠ Zero: any missing read, price, or decimals fails CLOSED
    (returns ``None`` → the accounting row degrades to ESTIMATED) — a
    fabricated zero would masquerade as a measured-empty position.
    """
    if len(results) < 2:
        return None
    shares = _decode_word(results[0])
    assets_per_probe = _decode_word(results[1])
    if shares is None or assets_per_probe is None:
        return None

    token = query.loan_token
    prices = query.prices
    decimals = query.decimals
    if not token or prices is None or decimals is None:
        return None
    if token not in prices or token not in decimals:
        return None
    price = prices[token]
    if price is None:
        return None

    # Share-probe approximation: ``shares × convertToAssets(1e18) // 1e18``
    # floors and can differ from the exact ``convertToAssets(shares)`` by
    # sub-wei dust (the vault's own rounding applied at a different scale).
    # Acceptable for USD marking — this value only prices the position; the
    # compiler's withdraw path uses the exact per-wallet form on-chain.
    assets_raw = shares * assets_per_probe // _SHARE_PROBE
    collateral_usd = (Decimal(assets_raw) / Decimal(10 ** decimals[token])) * price

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=Decimal("0"),  # measured zero — fToken supply has no debt leg
        health_factor=None,  # no liquidation surface on a pure supply
        liquidation_threshold_bps=None,
        e_mode_category=None,
        lltv=None,
    )


#: Aggregate account-state read for Fluid fTokens. Market-scoped (empty
#: ``contract_kinds``): the registry binds the fToken from the market table's
#: ``comet_address``. Not USD-native — the underlying's price/decimals are
#: injected via ``valuation_role_keys``.
ACCOUNT_STATE_READ_SPEC = AccountStateReadSpec(
    contract_kinds=(),
    build_calls=_build_fluid_account_state_calls,
    reduce_calls=_reduce_fluid_account_state,
    valuation_role_keys=(("loan_token", "loan_token"),),
    normalize_market_id=str.lower,
    query_inputs_fn=_query_inputs_from_intent,
)

__all__ = ["ACCOUNT_STATE_READ_SPEC", "FLUID_FTOKEN_MARKETS"]
