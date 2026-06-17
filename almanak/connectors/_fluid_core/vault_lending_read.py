"""Fluid vault (NFT-CDP) aggregate account-state read (VIB-5031, ADR §3.2).

Registered on the **``fluid_vault`` manifest** — the lending-read registry
holds exactly one ``account_state`` per manifest name, and the ``fluid``
slot already carries the Phase-2 token-derived fToken spec
(``fluid/lending_read.py``). The two specs never mix.

Bespoke spec in the silo_v2 / euler_v2 / benqi tradition (no Aave-style
``getUserAccountData``): a SINGLE wallet-scoped call —
``VaultResolver.positionsByUser(wallet)`` — returns every position with
its vault address, supply/borrow amounts, and the paired
``VaultEntireData`` (liquidation threshold, vault-oracle price, token
pair). The reducer filters to the entry whose vault == ``market_id``.

Two truth sources, deliberately distinct (ADR §3.2):

- ``health_factor`` is PROTOCOL TRUTH — computed from the vault's OWN
  oracle data carried inside ``VaultEntireData``
  (``HF = collateral × liquidationThreshold / debt`` with both legs in
  the vault oracle's terms). This is the ratio liquidation actually
  keys on; divergence from our-oracle USD values is expected and fine.
- ``collateral_usd`` / ``debt_usd`` use the injected valuation seam
  (prices/decimals from the framework reader via ``valuation_role_keys``)
  — non-USD-native, like Morpho/Silo/Euler.

Empty ≠ Zero, three distinct answers:

- Position ABSENT (wallet holds no NFT on the vault): measured
  ``Decimal("0")`` amounts + ``health_factor=None`` (the HF of an empty
  position is undefined, not zero).
- Read FAILURE (missing/undecodable blob, missing injected valuation):
  ``None`` — the framework reader fails closed; the accounting row
  degrades to ESTIMATED, never fabricated zeros.
- Position PRESENT, collateral > 0, zero debt: HF = the 999999 sentinel
  (Morpho convention: no liquidation surface, but position is open).
- Position PRESENT but fully closed (supply == 0 AND borrow == 0): HF =
  ``None`` (Empty != Zero != None: a closed NFT shell has no financial
  surface; sentinel 999999 would be misleading).

Gateway-boundary note: pure planner/reducer + ``eth_abi`` decoding only;
the gateway-routed ``eth_call`` lives in the framework reader.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from eth_abi import decode as abi_decode

from almanak.connectors._fluid_core.vault_sdk import (
    USER_POSITION_TYPE,
    VAULT_ENTIRE_DATA_TYPE,
)
from almanak.connectors._strategy_base.lending_read_base import (
    AccountStateQuery,
    AccountStateReadSpec,
    EthCall,
    LendingAccountState,
    pad_address,
)

logger = logging.getLogger(__name__)

#: ``positionsByUser(address)`` — pinned, byte-verified (verification report).
_POSITIONS_BY_USER_SELECTOR = "0x347ca8bb"

#: Fluid oracle exchange-rate scale: ``debt_units = col_units × price / 1e27``.
_ORACLE_PRICE_SCALE = 10**27
_BPS = Decimal(10**4)

#: No-debt / undefined-HF sentinel + serialisation cap (Morpho convention).
_HF_SENTINEL = Decimal("999999")


def _query_inputs_from_intent(intent: Any) -> dict[str, Any]:
    """Market id = the intent's vault address (required on fluid_vault intents)."""
    return {"market_id": getattr(intent, "market_id", None)}


def _build_fluid_vault_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Plan the single wallet-scoped read: ``positionsByUser(wallet)``.

    Fails closed (returns ``[]``) when the resolver target was not bound or
    the query carries no ``market_id`` — vault account state is per-market,
    so a missing market id has no well-defined read (Morpho precedent).
    """
    resolver = query.position_manager_address
    if not resolver or not query.market_id:
        return []
    return [EthCall(to=resolver, data=_POSITIONS_BY_USER_SELECTOR + pad_address(query.wallet_address))]


def _decode_positions_blob(blob_hex: str) -> list[tuple[Any, Any]] | None:
    """Typed-ABI decode of the ``(UserPosition[], VaultEntireData[])`` return.

    Returns index-aligned ``(position_tuple, vault_tuple)`` pairs, or ``None``
    on a truncated / undecodable / misaligned blob (fail closed — never
    word-offset arithmetic, never a partial decode).
    """
    raw = blob_hex[2:] if blob_hex[:2].lower() == "0x" else blob_hex
    try:
        positions, vaults = abi_decode(
            [f"{USER_POSITION_TYPE}[]", f"{VAULT_ENTIRE_DATA_TYPE}[]"],
            bytes.fromhex(raw),
        )
    except Exception:
        logger.debug("Undecodable Fluid positionsByUser blob", exc_info=True)
        return None
    if len(positions) != len(vaults):
        logger.debug("Misaligned Fluid positionsByUser arrays: %d vs %d", len(positions), len(vaults))
        return None
    return list(zip(positions, vaults, strict=True))


def _protocol_truth_health_factor(supply: int, borrow: int, oracle_price: int, liq_threshold_bps: int) -> Decimal:
    """``HF = (collateral × liquidationThreshold) / debt`` in vault-oracle terms."""
    if borrow <= 0:
        return _HF_SENTINEL
    col_in_debt_units = supply * oracle_price // _ORACLE_PRICE_SCALE
    health_factor = (Decimal(col_in_debt_units) * Decimal(liq_threshold_bps) / _BPS) / Decimal(borrow)
    return min(health_factor, _HF_SENTINEL)


def _reduce_fluid_vault_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Reduce the ``positionsByUser`` blob to the canonical aggregate state.

    Serialization shape note: ``lltv`` carries the vault's
    ``liquidationThreshold`` as a fraction and ``liquidation_threshold_bps``
    stays ``None`` — the Morpho-family branch of ``lending_state_to_dict``
    derives the bps from ``lltv``, so setting both would be a parallel,
    drift-prone encoding.
    """
    blob_hex = results[0] if results else None
    if not blob_hex:
        return None
    pairs = _decode_positions_blob(blob_hex)
    if pairs is None:
        return None

    market_id = (query.market_id or "").lower()
    if not market_id:
        return None

    matching = [(pos, vault) for pos, vault in pairs if str(vault[0]).lower() == market_id]
    if not matching:
        # Measured empty — the wallet provably holds no NFT on this vault.
        return LendingAccountState(
            collateral_usd=Decimal("0"),
            debt_usd=Decimal("0"),
            health_factor=None,  # the HF of an absent position is undefined, not zero
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=None,
        )
    # One-NFT-per-(wallet,vault) invariant: deterministic lowest-nftId
    # selection mirrors ``FluidVaultSDK.resolve_user_nft_for_vault``.
    position, vault = min(matching, key=lambda pair: int(pair[0][0]))
    supply = int(position[9])  # exchange-price-scaled token units (report note 4)
    borrow = int(position[10])  # dustBorrow already netted out

    # ── Closed shell BEFORE the valuation gate ───────────────────────────────
    # A fully-closed position (supply=0 AND borrow=0) is an empty NFT shell —
    # both legs are measured zeros and need NO prices to value; HF is undefined
    # (Empty != Zero != None). Checking this before the injected-valuation gate
    # keeps a missing price seam from turning a measured-closed position into
    # a read failure.
    if supply == 0 and borrow == 0:
        return LendingAccountState(
            collateral_usd=Decimal("0"),
            debt_usd=Decimal("0"),
            health_factor=None,
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=None,
        )

    # ── Injected valuation seam (Empty ≠ Zero: missing input ⇒ fail closed) ──
    collateral_token = query.collateral_token
    loan_token = query.loan_token
    prices = query.prices
    decimals = query.decimals
    if collateral_token is None or loan_token is None or prices is None or decimals is None:
        return None
    if collateral_token not in decimals or loan_token not in decimals:
        return None
    collateral_price = prices.get(collateral_token)
    loan_price = prices.get(loan_token)
    if collateral_price is None or loan_price is None:
        return None
    collateral_usd = (Decimal(supply) / Decimal(10 ** decimals[collateral_token])) * collateral_price
    debt_usd = (Decimal(borrow) / Decimal(10 ** decimals[loan_token])) * loan_price

    # ── Protocol-truth HF from the vault's OWN oracle data ───────────────────
    configs = vault[4]
    liq_threshold_bps = int(configs[3])
    # HF is the LIQUIDATION-truth ratio: the on-chain liquidation boundary
    # keys on oraclePriceLiquidate (configs[10]), not oraclePriceOperate
    # (configs[9]) — the two differ on some vaults, and the operate price
    # would over/understate liquidation risk.
    oracle_price_liquidate = int(configs[10])
    if liq_threshold_bps <= 0 or oracle_price_liquidate <= 0:
        return None  # misconfigured / unreadable vault risk params — unmeasured
    health_factor = _protocol_truth_health_factor(supply, borrow, oracle_price_liquidate, liq_threshold_bps)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,  # derived from lltv by the serializer (Morpho family)
        e_mode_category=None,  # no e-mode concept
        lltv=Decimal(liq_threshold_bps) / _BPS,
    )


#: Aggregate account-state read for Fluid vaults (NFT-CDP, ``fluid_vault``).
#: Target = the per-chain VaultResolver (``contract_kinds``, resolved through
#: the ``fluid_vault`` address table). Not USD-native: the market table
#: (``FLUID_VAULT_MARKETS``) names the collateral/loan symbols the framework
#: reader prices + injects. ``normalize_market_id=str.lower`` because market
#: ids are 20-byte vault ADDRESSES, not 32-byte hashes (the default Morpho
#: zfill(64) normalisation would mangle them).
ACCOUNT_STATE_READ_SPEC = AccountStateReadSpec(
    contract_kinds=("vault_resolver",),
    build_calls=_build_fluid_vault_account_state_calls,
    reduce_calls=_reduce_fluid_vault_account_state,
    valuation_role_keys=(
        ("collateral_token", "collateral_token"),
        ("loan_token", "loan_token"),
    ),
    normalize_market_id=str.lower,
    query_inputs_fn=_query_inputs_from_intent,
)

__all__ = ["ACCOUNT_STATE_READ_SPEC"]
