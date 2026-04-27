"""Lending accounting event builder (VIB-3418).

Wired into strategy_runner after every successful SUPPLY / BORROW / REPAY / WITHDRAW.

Before-state (VIB-3489): captured via capture_lending_pre_state() called by the runner
                          BEFORE the transaction is submitted.  The runner passes the
                          result as pre_execution_state to build_lending_accounting_event().
                          If the read fails, None is passed and before fields are None
                          with an unavailable_reason note — never fabricated or stale.

After-state (Aave V3): Pool.getUserAccountData — one call gives collateral_usd,
                        debt_usd, health_factor, liquidation_threshold.

After-state (Morpho Blue): position(id, user) + market(id) — two calls give collateral
                            (raw units), borrow shares, and market totals needed to
                            convert shares → assets. lltv comes from the market params
                            stored in the adapter registry.

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

from almanak.framework.accounting.ids import make_accounting_event_id

logger = logging.getLogger(__name__)

# ─── Aave V3 Pool.getUserAccountData(address user) ────────────────────────────
# Selector: keccak256("getUserAccountData(address)")[:4] = 0xbf92857c
_AAVE_GET_ACCOUNT_DATA_SELECTOR = "0xbf92857c"
_AAVE_USD_SCALE = Decimal("1e8")  # 8-decimal USD base unit
_AAVE_HF_SCALE = Decimal("1e18")  # 1.0 HF = 1e18

# ─── Morpho Blue position/market selectors (VIB-3483) ─────────────────────────
# position(bytes32 id, address user) → (uint256 supplyShares, uint128 borrowShares, uint128 collateral)
_MORPHO_POSITION_SELECTOR = "0x93c52062"
# market(bytes32 id) → (uint128 totalSupplyAssets, uint128 totalSupplyShares,
#                        uint128 totalBorrowAssets, uint128 totalBorrowShares,
#                        uint128 lastUpdate, uint128 fee)
_MORPHO_MARKET_SELECTOR = "0x5c60e39a"
_MORPHO_LLTV_SCALE = Decimal("1e18")  # lltv is 1e18-scaled

# ─── Lending intent types ──────────────────────────────────────────────────────
_LENDING_INTENT_TYPES = frozenset({"SUPPLY", "BORROW", "REPAY", "WITHDRAW", "DELEVERAGE"})

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


@dataclass
class MorphoBlueAccountState:
    """Post-execution position summary from Morpho Blue position() + market() calls."""

    collateral_usd: Decimal
    debt_usd: Decimal
    health_factor: Decimal  # normalised (1.0 = healthy); None-sentinel if no debt
    lltv: Decimal  # liquidation LTV as a fraction (e.g. 0.86 for 86%)


def _normalize_market_id_hex(market_id: str) -> str:
    """Return the 32-byte market ID as 64 lowercase hex chars (no 0x prefix)."""
    raw = market_id.lower().replace("0x", "")
    return raw.zfill(64)


def read_morpho_blue_account_state(
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    market_id: str,
    collateral_token: str,
    loan_token: str,
    collateral_decimals: int,
    loan_decimals: int,
    lltv_raw: int,
    price_oracle: dict | None,
) -> MorphoBlueAccountState | None:
    """Read Morpho Blue position and market state for *wallet_address* via gateway.

    Makes two eth_call reads against the Morpho Blue contract:
      1. position(bytes32 id, address user) — borrowShares, collateral (raw uint128)
      2. market(bytes32 id)                 — totalBorrowAssets, totalBorrowShares (uint128)

    Then computes:
      borrow_assets = borrowShares * totalBorrowAssets / totalBorrowShares
      collateral_value_usd = collateral_amount_human * collateral_price_usd
      debt_value_usd = borrow_amount_human * loan_price_usd
      health_factor  = (collateral_value_usd * lltv) / debt_value_usd

    Returns None (with debug log) if any gateway call fails.
    Returns None for health_factor (no-debt sentinel) when borrow_shares == 0.

    Args:
        gateway_client: Gateway client exposing eth_call(chain, to, data).
        chain: Chain name (e.g. "ethereum", "arbitrum").
        wallet_address: Position owner address.
        market_id: Morpho Blue market ID (bytes32 hex, with or without 0x).
        collateral_token: Collateral token symbol (for price lookup).
        loan_token: Loan token symbol (for price lookup).
        collateral_decimals: Decimals for collateral token.
        loan_decimals: Decimals for loan token.
        lltv_raw: Raw LLTV from market params (1e18-scaled int, e.g. 860000000000000000 = 86%).
        price_oracle: Dict mapping token symbol → USD price (Decimal or float).

    Returns:
        MorphoBlueAccountState or None on failure.
    """
    try:
        from almanak.framework.connectors.morpho_blue.adapter import MORPHO_BLUE_ADDRESSES

        morpho_address = MORPHO_BLUE_ADDRESSES.get(chain.lower())
        if not morpho_address:
            logger.debug("read_morpho_blue_account_state: no Morpho Blue address for chain=%s", chain)
            return None

        market_hex = _normalize_market_id_hex(market_id)
        user_hex = _pad_address(wallet_address)

        # ── Call 1: position(bytes32 id, address user) ──────────────────────
        position_calldata = _MORPHO_POSITION_SELECTOR + market_hex + user_hex
        position_raw = _gateway_eth_call(gateway_client, chain, morpho_address, position_calldata)
        if not position_raw:
            logger.debug("read_morpho_blue_account_state: position() call failed for market=%s", market_id[:18])
            return None
        pos_hex = position_raw.replace("0x", "")
        if len(pos_hex) < 3 * 64:
            logger.debug("read_morpho_blue_account_state: position() response too short (%d chars)", len(pos_hex))
            return None

        # Word layout:
        #   [0]  supplyShares (uint256) — not used here
        #   [1]  borrowShares (uint128 padded to 256)
        #   [2]  collateral   (uint128 padded to 256)
        borrow_shares = _decode_word(pos_hex, 1)
        collateral_raw = _decode_word(pos_hex, 2)

        # ── Call 2: market(bytes32 id) ───────────────────────────────────────
        # market() returns the Market struct as 6 ABI-encoded uint128 values.
        # Standard Solidity ABI encoding pads each uint128 to a full 32-byte word:
        #   Word 0 (hex [0:64]):    totalSupplyAssets
        #   Word 1 (hex [64:128]):  totalSupplyShares
        #   Word 2 (hex [128:192]): totalBorrowAssets
        #   Word 3 (hex [192:256]): totalBorrowShares
        #   Word 4 (hex [256:320]): lastUpdate
        #   Word 5 (hex [320:384]): fee
        market_calldata = _MORPHO_MARKET_SELECTOR + market_hex
        market_raw = _gateway_eth_call(gateway_client, chain, morpho_address, market_calldata)
        if not market_raw:
            logger.debug("read_morpho_blue_account_state: market() call failed for market=%s", market_id[:18])
            return None
        mkt_hex = market_raw.replace("0x", "")
        if len(mkt_hex) < 6 * 64:  # 6 words × 64 hex chars each
            logger.debug("read_morpho_blue_account_state: market() response too short (%d chars)", len(mkt_hex))
            return None

        # Each uint128 occupies the lower 16 bytes of a 32-byte slot, but ABI-encoded
        # as a 32-byte word with leading zeros. The market() return is 6 separate
        # uint128 values packed as 6 full 32-byte (64 hex-char) words.
        total_borrow_assets = int(mkt_hex[128:192], 16)  # word index 2
        total_borrow_shares = int(mkt_hex[192:256], 16)  # word index 3

        # ── shares → assets ──────────────────────────────────────────────────
        if borrow_shares == 0:
            borrow_assets = 0
        elif total_borrow_shares == 0:
            borrow_assets = 0
        else:
            # Round up to be conservative — never under-count debt
            borrow_assets = (borrow_shares * total_borrow_assets + total_borrow_shares - 1) // total_borrow_shares

        # ── Convert raw amounts to human-decimal ─────────────────────────────
        collateral_amount = Decimal(collateral_raw) / Decimal(10**collateral_decimals)
        borrow_amount = Decimal(borrow_assets) / Decimal(10**loan_decimals)

        # ── USD values via price oracle ───────────────────────────────────────
        collateral_price = None
        loan_price = None
        if price_oracle is not None:
            collateral_price = price_oracle.get(collateral_token.upper()) or price_oracle.get(collateral_token.lower())
            loan_price = price_oracle.get(loan_token.upper()) or price_oracle.get(loan_token.lower())

        if collateral_price is None or loan_price is None:
            logger.debug(
                "read_morpho_blue_account_state: price not available for collateral=%s loan=%s",
                collateral_token,
                loan_token,
            )
            return None

        collateral_value_usd = collateral_amount * Decimal(str(collateral_price))
        debt_value_usd = borrow_amount * Decimal(str(loan_price))

        # ── LLTV and health factor ─────────────────────────────────────────────
        lltv = Decimal(lltv_raw) / _MORPHO_LLTV_SCALE

        if borrow_shares == 0 or debt_value_usd == 0:
            # No debt — health factor is undefined (infinite). Return a sentinel.
            health_factor = Decimal("999999")
        else:
            health_factor = (collateral_value_usd * lltv) / debt_value_usd

        # Cap unrealistically large HF (avoid overflow in serialisation)
        health_factor = min(health_factor, Decimal("999999"))

        return MorphoBlueAccountState(
            collateral_usd=collateral_value_usd,
            debt_usd=debt_value_usd,
            health_factor=health_factor,
            lltv=lltv,
        )

    except Exception:
        logger.debug("read_morpho_blue_account_state failed", exc_info=True)
        return None


def capture_lending_pre_state(
    *,
    intent: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    price_oracle: dict | None,
) -> AaveAccountState | MorphoBlueAccountState | None:
    """Read on-chain lending state BEFORE the transaction is submitted (VIB-3489).

    Called by the strategy runner before executing the intent bundle.  The
    returned state is later forwarded as ``pre_execution_state`` to
    ``build_lending_accounting_event()`` so that before/after deltas can be
    computed.

    Returns None (silently, with a debug log) when:
    - The gateway client is not available.
    - The intent is not a supported lending protocol (Aave V3 / Morpho Blue).
    - Any gateway eth_call fails.

    Never raises; never substitutes stale data on failure.
    """
    if gateway_client is None:
        return None

    protocol = str(getattr(intent, "protocol", "") or "").lower()
    intent_type_str = _intent_type_value(intent)

    if intent_type_str not in _LENDING_INTENT_TYPES:
        return None

    # ── Aave V3 ──────────────────────────────────────────────────────────────
    if protocol in ("aave_v3", "aave"):
        aave_state: AaveAccountState | None = read_aave_account_state(gateway_client, chain, wallet_address)
        if aave_state is None:
            logger.debug("capture_lending_pre_state: Aave read returned None for chain=%s", chain)
        return aave_state

    # ── Morpho Blue (all lending intent types for parity with Aave V3) ──────
    if protocol == "morpho_blue":
        market_id = _intent_market_id(intent)
        if not market_id:
            logger.debug("capture_lending_pre_state: Morpho market_id missing — skipping pre-state read")
            return None

        collateral_token_sym: str | None = getattr(intent, "collateral_token", None)
        loan_token_sym: str | None = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)

        _collateral_decimals: int | None = None
        _loan_decimals: int | None = None
        _lltv_raw: int | None = None

        try:
            from almanak.framework.connectors.morpho_blue.adapter import MORPHO_MARKETS

            _markets_for_chain = MORPHO_MARKETS.get(chain.lower(), {})
            # O(1) lookup using the same normalisation as _normalize_market_id_hex
            _normalized_key = "0x" + _normalize_market_id_hex(market_id)
            _market_info: dict | None = _markets_for_chain.get(_normalized_key)

            if _market_info is not None:
                if collateral_token_sym is None:
                    collateral_token_sym = _market_info.get("collateral_token")
                if loan_token_sym is None:
                    loan_token_sym = _market_info.get("loan_token")
                _lltv_raw = _market_info.get("lltv")

                try:
                    from almanak.framework.data.tokens.resolver import get_token_resolver

                    _resolver = get_token_resolver()
                    if collateral_token_sym:
                        _ct = _resolver.resolve(collateral_token_sym, chain=chain)
                        if _ct:
                            _collateral_decimals = _ct.decimals
                    if loan_token_sym:
                        _lt = _resolver.resolve(loan_token_sym, chain=chain)
                        if _lt:
                            _loan_decimals = _lt.decimals
                except Exception:
                    logger.debug("capture_lending_pre_state: token resolver failed for Morpho Blue", exc_info=True)
        except Exception:
            logger.debug("capture_lending_pre_state: MORPHO_MARKETS lookup failed for chain=%s", chain, exc_info=True)

        if not (
            collateral_token_sym
            and loan_token_sym
            and _collateral_decimals is not None
            and _loan_decimals is not None
            and _lltv_raw is not None
        ):
            logger.debug(
                "capture_lending_pre_state: Morpho Blue pre-state skipped (missing params) for market=%s",
                market_id[:18] if market_id else "?",
            )
            return None

        morpho_state: MorphoBlueAccountState | None = read_morpho_blue_account_state(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            market_id=market_id,
            collateral_token=collateral_token_sym,
            loan_token=loan_token_sym,
            collateral_decimals=_collateral_decimals,
            loan_decimals=_loan_decimals,
            lltv_raw=_lltv_raw,
            price_oracle=price_oracle,
        )
        if morpho_state is None:
            logger.debug(
                "capture_lending_pre_state: Morpho Blue read returned None for market=%s",
                market_id[:18] if market_id else "?",
            )
        return morpho_state

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
        "DELEVERAGE": LendingEventType.DELEVERAGE,
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
    pre_execution_state: AaveAccountState | MorphoBlueAccountState | None = None,
) -> Any | None:
    """Build a LendingAccountingEvent for a completed lending intent.

    Returns None for non-lending intents or if the intent type cannot be mapped.

    pre_execution_state (VIB-3489): on-chain account state captured BEFORE the
    transaction was submitted, obtained by calling capture_lending_pre_state()
    in the runner.  When None, before fields are left as None rather than
    fabricated — honest absence is always preferred over stale data.

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
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)
            _borrow_id_seed = tx_hash or ledger_entry_id or position_key
            basis_store.record_borrow(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                principal_amount=amount_human,
                principal_usd=principal_delta_usd,
                timestamp=now,
                lot_id=make_accounting_event_id(deployment_id, cycle_id, "BORROW_LOT", _borrow_id_seed, position_key),
            )
            interest_delta_usd = None  # interest accrues, not known at borrow time

        elif intent_type_str in ("REPAY", "DELEVERAGE"):
            # DELEVERAGE is structurally a repay: it reduces an open borrow lot.
            match_result = basis_store.match_repay(
                deployment_id=deployment_id,
                position_key=position_key,
                token=asset,
                repay_amount=amount_human,
            )
            if match_result.unmatched_amount > 0:
                # No basis lots → interest is UNAVAILABLE, not zero
                logger.debug(
                    "%s unmatched for %s: unmatched=%.6f (no BORROW lots recorded)",
                    intent_type_str,
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

    # ── After-state: protocol-specific on-chain read ─────────────────────────
    aave_state: AaveAccountState | None = None
    morpho_state: MorphoBlueAccountState | None = None
    morpho_unavailable_reason: str = ""

    # Only query getUserAccountData for protocols whose pool address resolves via
    # AAVE_V3_POOL_ADDRESSES. Spark and Radiant V2 use different pool contracts;
    # querying the Aave V3 pool for those protocols returns wrong data with HIGH
    # confidence. Add their addresses to a separate registry when ready.
    is_aave = protocol.lower() in ("aave_v3", "aave")
    is_morpho = protocol.lower() == "morpho_blue"

    if is_aave and gateway_client is not None:
        aave_state = read_aave_account_state(gateway_client, chain, wallet_address)

    if is_morpho and gateway_client is not None and intent_type_str in ("BORROW", "REPAY", "DELEVERAGE"):
        # Morpho Blue HF persistence (VIB-3483): requires market_id, collateral/loan
        # token symbols and decimals, and lltv from the market registry.
        if not market_id:
            morpho_unavailable_reason = "market_id missing from intent — cannot read Morpho Blue position"
            logger.debug("read_morpho_blue_account_state skipped: %s", morpho_unavailable_reason)
        else:
            # Resolve collateral/loan token info from the intent and market registry.
            collateral_token_sym: str | None = getattr(intent, "collateral_token", None)
            loan_token_sym: str | None = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)

            # Try to get market params from adapter registry for decimals + lltv.
            _collateral_decimals: int | None = None
            _loan_decimals: int | None = None
            _lltv_raw: int | None = None

            try:
                from almanak.framework.connectors.morpho_blue.adapter import MORPHO_MARKETS

                _markets_for_chain = MORPHO_MARKETS.get(chain.lower(), {})
                _market_info: dict | None = None
                for _mid, _info in _markets_for_chain.items():
                    if _mid.lower().lstrip("0x") == market_id.lower().lstrip("0x"):
                        _market_info = _info
                        break

                if _market_info is not None:
                    if collateral_token_sym is None:
                        collateral_token_sym = _market_info.get("collateral_token")
                    if loan_token_sym is None:
                        loan_token_sym = _market_info.get("loan_token")
                    _lltv_raw = _market_info.get("lltv")

                    # Resolve decimals via token resolver
                    try:
                        from almanak.framework.data.tokens.resolver import get_token_resolver

                        _resolver = get_token_resolver()
                        if collateral_token_sym:
                            _ct = _resolver.resolve(collateral_token_sym, chain=chain)
                            if _ct:
                                _collateral_decimals = _ct.decimals
                        if loan_token_sym:
                            _lt = _resolver.resolve(loan_token_sym, chain=chain)
                            if _lt:
                                _loan_decimals = _lt.decimals
                    except Exception:
                        logger.debug("token resolver failed for Morpho Blue HF read", exc_info=True)

            except Exception:
                logger.debug("MORPHO_MARKETS lookup failed for chain=%s", chain, exc_info=True)

            # Only proceed if we have all required inputs
            if (
                collateral_token_sym
                and loan_token_sym
                and _collateral_decimals is not None
                and _loan_decimals is not None
                and _lltv_raw is not None
            ):
                morpho_state = read_morpho_blue_account_state(
                    gateway_client=gateway_client,
                    chain=chain,
                    wallet_address=wallet_address,
                    market_id=market_id,
                    collateral_token=collateral_token_sym,
                    loan_token=loan_token_sym,
                    collateral_decimals=_collateral_decimals,
                    loan_decimals=_loan_decimals,
                    lltv_raw=_lltv_raw,
                    price_oracle=price_oracle,
                )
                if morpho_state is None:
                    morpho_unavailable_reason = "Morpho Blue position/market gateway read failed"
            else:
                morpho_unavailable_reason = "Morpho Blue HF read skipped: missing " + (
                    ", ".join(
                        x
                        for x, v in [
                            ("collateral_token", collateral_token_sym),
                            ("loan_token", loan_token_sym),
                            ("collateral_decimals", _collateral_decimals),
                            ("loan_decimals", _loan_decimals),
                            ("lltv", _lltv_raw),
                        ]
                        if not v
                    )
                )
                logger.debug("read_morpho_blue_account_state skipped: %s", morpho_unavailable_reason)

    # ── Unify after-state fields from whichever protocol provided data ────────
    # Priority: Aave state > Morpho state > None
    got_after_state = aave_state is not None or morpho_state is not None

    if aave_state is not None:
        collateral_after: Decimal | None = aave_state.collateral_usd
        debt_after: Decimal | None = aave_state.debt_usd
        hf_after: Decimal | None = aave_state.health_factor
        lt_bps: int | None = aave_state.liquidation_threshold_bps
        liquidation_threshold: Decimal | None = Decimal(lt_bps) / Decimal("10000") if lt_bps is not None else None
        lltv_after: Decimal | None = None
    elif morpho_state is not None:
        collateral_after = morpho_state.collateral_usd
        debt_after = morpho_state.debt_usd
        # health_factor = 999999 is the no-debt sentinel — store None for "undefined HF" only
        # when borrow is truly zero (callers must not use HF == 999999 as a trigger).
        hf_after = morpho_state.health_factor
        lt_bps = None  # Morpho Blue uses lltv directly, not lt_bps
        liquidation_threshold = morpho_state.lltv  # LLTV serves as liquidation_threshold
        lltv_after = morpho_state.lltv
    else:
        collateral_after = None
        debt_after = None
        hf_after = None
        lt_bps = None
        liquidation_threshold = None
        lltv_after = None

    net_equity_after = (
        (collateral_after - debt_after) if (collateral_after is not None and debt_after is not None) else None
    )

    # ── Before-state: from pre_execution_state (VIB-3489) ────────────────────
    # pre_execution_state is captured by the runner BEFORE the tx is submitted.
    # If None (read failed or not available), before fields stay None — honest
    # absence is preferred over stale data. Absence is signaled by before fields
    # being None; it does NOT affect unavailable_reason (which tracks after-state
    # quality) or confidence.
    collateral_before: Decimal | None = None
    debt_before: Decimal | None = None
    hf_before: Decimal | None = None
    net_equity_before: Decimal | None = None

    if pre_execution_state is not None:
        # Both AaveAccountState and MorphoBlueAccountState share the same field
        # names for the data being extracted — no protocol-specific branching needed.
        collateral_before = pre_execution_state.collateral_usd
        debt_before = pre_execution_state.debt_usd
        hf_before = pre_execution_state.health_factor
        if collateral_before is not None and debt_before is not None:
            net_equity_before = collateral_before - debt_before

    # Confidence: HIGH if we got a live after-state read, ESTIMATED otherwise.
    # unavailable_reason tracks the primary (after-state) signal only — callers
    # interpret confidence + unavailable_reason as a pair. Pre-state absence is
    # already observable via the before fields being None; polluting
    # unavailable_reason with it would degrade HIGH-confidence events when
    # pre-state was simply not yet available on this cycle.
    confidence = AccountingConfidence.HIGH if got_after_state else AccountingConfidence.ESTIMATED
    if not got_after_state:
        if is_morpho and morpho_unavailable_reason:
            unavailable_reason = morpho_unavailable_reason
        else:
            unavailable_reason = "post-execution on-chain read unavailable"
    else:
        unavailable_reason = ""

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
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
        collateral_value_before_usd=collateral_before,
        collateral_value_after_usd=collateral_after,
        debt_value_before_usd=debt_before,
        debt_value_after_usd=debt_after,
        net_equity_before_usd=net_equity_before,
        net_equity_after_usd=net_equity_after,
        health_factor_before=hf_before,
        health_factor_after=hf_after,
        liquidation_threshold=liquidation_threshold,
        lltv=lltv_after,
        supply_apr_bps=supply_apr_bps,
        borrow_apr_bps=borrow_apr_bps,
        principal_delta_usd=principal_delta_usd,
        interest_delta_usd=interest_delta_usd,
        gas_usd=gas_usd,
        amount_token=amount_human,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )
