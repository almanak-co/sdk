"""Lending accounting event builder (VIB-3418).

Wired into strategy_runner after every successful SUPPLY / BORROW / REPAY / WITHDRAW.

Before-state: None — we capture after-state only via a post-execution gateway read.
              Pre-execution state capture is a follow-up item; omitting it is honest
              (None) rather than STALE (fabricated from a snapshot that may have
              drifted before the TX landed).

After-state (Aave V3): Pool.getUserAccountData — one call gives collateral_usd,
                        debt_usd, health_factor, liquidation_threshold.

FIFO interest attribution:
  BORROW → record_borrow() adds a principal lot to FIFOBasisStore.
  REPAY  → match_repay() consumes lots FIFO; interest = repay_amount − principal_consumed.
            If no lots exist for the position, unmatched_amount is non-zero and
            interest_delta_usd is None (UNAVAILABLE — never fabricated).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)

# ─── Aave V3 Pool.getUserAccountData(address user) ────────────────────────────
# Selector: keccak256("getUserAccountData(address)")[:4] = 0xbf92857c
_AAVE_GET_ACCOUNT_DATA_SELECTOR = "0xbf92857c"
_AAVE_USD_SCALE = Decimal("1e8")  # 8-decimal USD base unit
_AAVE_HF_SCALE = Decimal("1e18")  # 1.0 HF = 1e18

# ─── Lending intent types ──────────────────────────────────────────────────────
_LENDING_INTENT_TYPES = frozenset({"SUPPLY", "BORROW", "REPAY", "WITHDRAW"})

# ─── Chain native gas token (for gas_usd conversion) ──────────────────────────
_CHAIN_NATIVE_TOKEN: dict[str, str] = {
    "ethereum": "ETH",
    "arbitrum": "ETH",
    "optimism": "ETH",
    "base": "ETH",
    "linea": "ETH",
    "plasma": "ETH",
    "polygon": "MATIC",
    "avalanche": "AVAX",
    "bsc": "BNB",
    "sonic": "S",
    "mantle": "MNT",
    "xlayer": "OKB",
}


@dataclass
class AaveAccountState:
    """Post-execution account summary from Pool.getUserAccountData."""

    collateral_usd: Decimal
    debt_usd: Decimal
    health_factor: Decimal  # normalised (1.0 = healthy)
    liquidation_threshold_bps: int  # e.g. 8500 → 85 %


def _pad_address(address: str) -> str:
    """Left-pad an EVM address to 32 bytes (64 hex chars, no 0x prefix)."""
    return address.lower().replace("0x", "").zfill(64)


def _decode_word(hex_data: str, word_index: int) -> int:
    start = word_index * 64
    return int(hex_data[start : start + 64], 16)


def _gateway_eth_call(gateway_client: Any, chain: str, to: str, data: str) -> str | None:
    """Make an eth_call via the gateway's public eth_call API."""
    try:
        return gateway_client.eth_call(chain, to, data)
    except Exception:
        logger.debug("gateway eth_call failed", exc_info=True)
        return None


def read_aave_account_state(
    gateway_client: Any,
    chain: str,
    wallet_address: str,
) -> AaveAccountState | None:
    """Read Aave V3 Pool.getUserAccountData for *wallet_address* via gateway.

    Returns normalised USD values and a 1e18-normalised health factor, or None
    if the gateway call fails.

    getUserAccountData returns:
      [0] totalCollateralBase  (uint256, 1e8 USD)
      [1] totalDebtBase        (uint256, 1e8 USD)
      [2] availableBorrowsBase (uint256, 1e8 USD) -- not used
      [3] currentLiquidationThreshold (uint256, bps, e.g. 8500)
      [4] ltv                  (uint256, bps) -- not used
      [5] healthFactor         (uint256, 1e18)
    """
    try:
        from almanak.framework.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES

        pool_address = AAVE_V3_POOL_ADDRESSES.get(chain.lower())
        if not pool_address:
            return None

        calldata = _AAVE_GET_ACCOUNT_DATA_SELECTOR + _pad_address(wallet_address)
        hex_data = _gateway_eth_call(gateway_client, chain, pool_address, calldata)
        if not hex_data:
            return None

        raw = hex_data.replace("0x", "")
        if len(raw) < 6 * 64:  # expect ≥ 6 words
            return None

        collateral_usd = Decimal(_decode_word(raw, 0)) / _AAVE_USD_SCALE
        debt_usd = Decimal(_decode_word(raw, 1)) / _AAVE_USD_SCALE
        liquidation_threshold_bps = _decode_word(raw, 3)
        hf_raw = _decode_word(raw, 5)
        # Cap unrealistically large HF (empty position → max sentinel)
        health_factor = min(Decimal(hf_raw) / _AAVE_HF_SCALE, Decimal("999999"))

        return AaveAccountState(
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            health_factor=health_factor,
            liquidation_threshold_bps=liquidation_threshold_bps,
        )
    except Exception:
        logger.debug("read_aave_account_state failed", exc_info=True)
        return None


def _derive_position_key(protocol: str, chain: str, wallet: str, market_id: str | None, asset: str) -> str:
    """Canonical position key for a lending position."""
    parts = ["lending", chain.lower(), protocol.lower(), wallet.lower()]
    if market_id:
        parts.append(market_id.lower())
    parts.append(asset.lower())
    return ":".join(parts)


def _intent_asset(intent: Any) -> str:
    """Extract the primary asset symbol from a lending intent."""
    # SUPPLY / WITHDRAW: intent.token
    # BORROW: intent.borrow_token (collateral_token is the collateral side)
    # REPAY: intent.token
    for attr in ("borrow_token", "token"):
        v = getattr(intent, attr, None)
        if v:
            return str(v)
    return "UNKNOWN"


def _intent_market_id(intent: Any) -> str | None:
    return getattr(intent, "market_id", None)


def _intent_type_value(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _to_lending_event_type(intent_type_str: str):
    """Map IntentType string to LendingEventType.  Returns None for non-lending intents."""
    from almanak.framework.accounting.models import LendingEventType

    _MAP = {
        "SUPPLY": LendingEventType.SUPPLY,
        "BORROW": LendingEventType.BORROW,
        "REPAY": LendingEventType.REPAY,
        "WITHDRAW": LendingEventType.WITHDRAW,
    }
    return _MAP.get(intent_type_str.upper())


def _ray_to_bps(ray_value: int | float | Decimal | str | None) -> int | None:
    """Convert an APR value to integer basis-points (1 bps = 0.01 %).

    Accepts two input forms:
    - Already-fractional decimal (e.g. Decimal("0.05") → 500 bps): produced
      by Aave V3 / Spark / Radiant receipt parsers which pre-normalize from ray.
    - Raw ray integer (≥ 1, scale 1e27): produced by synthetic test fixtures.
    """
    if ray_value is None:
        return None
    try:
        v = Decimal(str(ray_value))
        if v < Decimal("1"):
            # Already normalized fraction (e.g. 0.05 = 5% APY)
            bps = v * Decimal("10000")
        else:
            # Raw ray — divide by 1e27 first
            bps = v / Decimal("1e27") * Decimal("10000")
        return int(bps.to_integral_value(rounding="ROUND_HALF_UP"))
    except Exception:
        return None


def _amount_to_usd(amount_human: Decimal | None, price_oracle: dict | None, asset: str) -> Decimal | None:
    """Convert a human-readable token amount to USD using the price_oracle dict."""
    if amount_human is None or price_oracle is None:
        return None
    price = price_oracle.get(asset.upper()) or price_oracle.get(asset.lower())
    if price is None:
        return None
    try:
        return Decimal(str(price)) * amount_human
    except Exception:
        return None


def build_lending_accounting_event(
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    strategy_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
    ledger_entry_id: str | None = None,
) -> Any | None:
    """Build a LendingAccountingEvent for a completed lending intent.

    Returns None for non-lending intents or if the intent type cannot be mapped.

    FIFO lot tracking:
      - BORROW  → records a lot; interest_delta_usd = None at borrow time.
      - REPAY   → matches lots; interest_delta_usd = excess over principal.
      - SUPPLY / WITHDRAW → principal_delta_usd only.
    """
    from almanak.framework.accounting.models import (
        AccountingConfidence,
        AccountingIdentity,
        LendingAccountingEvent,
    )

    intent_type_str = _intent_type_value(intent)
    if intent_type_str not in _LENDING_INTENT_TYPES:
        return None

    lending_event_type = _to_lending_event_type(intent_type_str)
    if lending_event_type is None:
        return None

    now = datetime.now(UTC)
    protocol = getattr(intent, "protocol", "") or ""
    asset = _intent_asset(intent)
    market_id = _intent_market_id(intent)
    position_key = _derive_position_key(protocol, chain, wallet_address, market_id, asset)

    extracted = getattr(result, "extracted_data", None) or {}
    tx_hash = getattr(result, "tx_hash", None) or ""

    # ── Amounts & APRs from extracted_data ────────────────────────────────────
    raw_amount: int | None = (
        extracted.get("supply_amount")
        or extracted.get("borrow_amount")
        or extracted.get("repay_amount")
        or extracted.get("withdraw_amount")
    )
    amount_human: Decimal | None = None
    if raw_amount is not None:
        try:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            resolver = get_token_resolver()
            token_info = resolver.resolve(asset, chain=chain)
            if token_info is None:
                logger.debug("token resolution returned None for %s on %s, skipping amount", asset, chain)
            else:
                amount_human = Decimal(str(raw_amount)) / Decimal(10**token_info.decimals)
        except Exception:
            logger.debug("token decimal resolution failed for %s, skipping amount conversion", asset)

    supply_apr_bps = _ray_to_bps(extracted.get("supply_rate"))
    borrow_apr_bps = _ray_to_bps(extracted.get("borrow_rate"))

    # ── Gas ───────────────────────────────────────────────────────────────────
    # ExecutionResult exposes total_gas_cost_wei (sum of all tx costs in the bundle).
    # Convert to native-token units (wei → 1e18), then look up the chain-specific
    # gas token (ETH on EVM L1/L2, AVAX on Avalanche, etc.).
    gas_cost_wei = getattr(result, "total_gas_cost_wei", None)
    gas_cost_native: Decimal | None = None
    if gas_cost_wei is not None and gas_cost_wei > 0:
        try:
            gas_cost_native = Decimal(str(gas_cost_wei)) / Decimal(10**18)
        except Exception:
            pass
    native_token = _CHAIN_NATIVE_TOKEN.get(chain.lower(), "ETH")
    gas_usd = _amount_to_usd(gas_cost_native, price_oracle, native_token)

    # ── FIFO lot matching ─────────────────────────────────────────────────────
    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None

    if amount_human is not None:
        if intent_type_str == "BORROW":
            basis_store.record_borrow(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                principal_amount=amount_human,
                timestamp=now,
            )
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)
            interest_delta_usd = None  # interest accrues, not known at borrow time

        elif intent_type_str == "REPAY":
            match_result = basis_store.match_repay(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                repay_amount=amount_human,
            )
            if match_result.unmatched_amount > 0:
                # No basis lots → interest is UNAVAILABLE, not zero
                logger.debug(
                    "REPAY unmatched for %s: unmatched=%.6f (no BORROW lots recorded)",
                    position_key,
                    match_result.unmatched_amount,
                )
                principal_delta_usd = _amount_to_usd(match_result.repaid_principal, price_oracle, asset)
                interest_delta_usd = None  # UNAVAILABLE — cannot fabricate
            else:
                principal_delta_usd = _amount_to_usd(match_result.repaid_principal, price_oracle, asset)
                interest_delta_usd = _amount_to_usd(match_result.interest_or_yield, price_oracle, asset)

        elif intent_type_str in ("SUPPLY", "WITHDRAW"):
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)

    # ── After-state: Aave V3 getUserAccountData ───────────────────────────────
    after_state: AaveAccountState | None = None
    # Only query getUserAccountData for protocols whose pool address resolves via
    # AAVE_V3_POOL_ADDRESSES. Spark and Radiant V2 use different pool contracts;
    # querying the Aave V3 pool for those protocols returns wrong data with HIGH
    # confidence. Add their addresses to a separate registry when ready.
    is_aave = protocol.lower() in ("aave_v3", "aave")
    if is_aave and gateway_client is not None:
        after_state = read_aave_account_state(gateway_client, chain, wallet_address)

    collateral_after = after_state.collateral_usd if after_state else None
    debt_after = after_state.debt_usd if after_state else None
    net_equity_after = (
        (collateral_after - debt_after) if (collateral_after is not None and debt_after is not None) else None
    )
    hf_after = after_state.health_factor if after_state else None
    lt_bps = after_state.liquidation_threshold_bps if after_state else None
    liquidation_threshold = Decimal(lt_bps) / Decimal("10000") if lt_bps is not None else None

    # Confidence: HIGH if we got a live after-state read, ESTIMATED otherwise
    confidence = AccountingConfidence.HIGH if after_state is not None else AccountingConfidence.ESTIMATED
    unavailable_reason = "" if after_state is not None else "post-execution on-chain read unavailable"

    identity = AccountingIdentity(
        id=f"lending_{deployment_id}_{cycle_id}_{intent_type_str}_{tx_hash[-8:] if tx_hash else 'unknown'}",
        deployment_id=deployment_id,
        strategy_id=strategy_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=now,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id or "",
    )

    return LendingAccountingEvent(
        identity=identity,
        event_type=lending_event_type,
        position_key=position_key,
        market_id=market_id or "",
        asset=asset,
        collateral_value_before_usd=None,  # pre-execution read not yet implemented
        collateral_value_after_usd=collateral_after,
        debt_value_before_usd=None,
        debt_value_after_usd=debt_after,
        net_equity_before_usd=None,
        net_equity_after_usd=net_equity_after,
        health_factor_before=None,
        health_factor_after=hf_after,
        liquidation_threshold=liquidation_threshold,
        lltv=None,
        supply_apr_bps=supply_apr_bps,
        borrow_apr_bps=borrow_apr_bps,
        principal_delta_usd=principal_delta_usd,
        interest_delta_usd=interest_delta_usd,
        gas_usd=gas_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
