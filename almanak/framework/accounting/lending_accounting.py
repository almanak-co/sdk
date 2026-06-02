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

import dataclasses
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

from almanak.connectors._strategy_base.lending_read_base import (
    _AAVE_GET_USER_EMODE_SELECTOR,
    LendingAccountState,
    parse_user_emode_hex,
)
from almanak.framework.accounting.gas_pricing import native_token_for_chain
from almanak.framework.accounting.ids import make_accounting_event_id

logger = logging.getLogger(__name__)

# VIB-4929 PR-3a: Aave + Morpho aggregate account-state reads go through the
# single generic ``read_lending_account_state``, which drives the connector-owned
# specs (``AAVE_FORK_ACCOUNT_STATE_READ`` / ``MORPHO_BLUE_ACCOUNT_STATE_READ`` in
# ``lending_read_base``) via ``LendingReadRegistry``. Adding a lending connector
# to the read path requires ZERO framework edits here — no per-protocol
# ``read_<protocol>_account_state`` function, no selector, scale, cap, lltv, HF
# sentinel, or decode is duplicated in this module. ``read_aave_user_emode`` is
# the one remaining single-call helper (used by the Tier-2 Aave registry), still
# decoding via the imported ``_AAVE_GET_USER_EMODE_SELECTOR`` + ``parse_user_emode_hex``.
# Compound V3 stays on its legacy ``read_compound_v3_account_state`` transitionally
# (it has no addresses.py / AddressRegistry entry yet; folds in at PR-3b).

# ─── Compound V3 selectors ────────────────────────────────────────────────────
# userCollateral(address account, address asset) → (uint128 balance, uint128 reserved)
_COMPOUND_V3_USER_COLLATERAL_SELECTOR = "0x2b92a07d"
# borrowBalanceOf(address account) → uint256
_COMPOUND_V3_BORROW_BALANCE_SELECTOR = "0x374c49b4"
# balanceOf(address account) → uint256  (base-asset supplied balance on the Comet)
_COMPOUND_V3_BALANCE_OF_SELECTOR = "0x70a08231"

# ─── Lending intent types ──────────────────────────────────────────────────────
_LENDING_INTENT_TYPES = frozenset({"SUPPLY", "BORROW", "REPAY", "WITHDRAW", "DELEVERAGE"})

# Chain native gas token resolution lives in ``gas_pricing.native_token_for_chain`` —
# a single framework source of truth shared with the EVM gas_usd writer
# (VIB-3805). The previous local map diverged on plasma (ETH vs XPL) and
# missed several chains in the gateway-side ``NATIVE_TOKEN_SYMBOLS``.


def _pad_address(address: str) -> str:
    """Left-pad an EVM address to 32 bytes (64 hex chars, no 0x prefix)."""
    return address.lower().replace("0x", "").zfill(64)


def _decode_word(hex_data: str, word_index: int) -> int:
    start = word_index * 64
    return int(hex_data[start : start + 64], 16)


def _gateway_eth_call(
    gateway_client: Any, chain: str, to: str, data: str, block: int | str | None = None
) -> str | None:
    """Make an eth_call via the gateway's public eth_call API.

    ``block`` (VIB-4589 / F7) — passed through to ``GatewayClient.eth_call``.
    Callers reading **post-execution** state MUST pin to the receipt's block
    (``receipt.block_number``) to avoid racing the upstream RPC's receipt
    indexer; reads with ``block=None`` fall back to ``"latest"`` and are
    only safe for **pre-execution** captures where the read precedes the
    submitted tx by definition.
    """
    try:
        return gateway_client.eth_call(chain, to, data, block=block)
    except TypeError:
        # Backwards-compat with mocks / older gateway clients that don't accept
        # the ``block`` kwarg yet. We only fall back to the legacy 3-arg form
        # when the caller wasn't pinning the read in the first place — i.e.
        # ``block is None`` or ``block == "latest"``. Pinned reads (an int
        # block_number, or any other tag) MUST fail closed here: silently
        # downgrading to ``"latest"`` would reintroduce the exact stale
        # post-state race VIB-4589 / F7 is closing.
        if block is None or (isinstance(block, str) and block == "latest"):
            try:
                return gateway_client.eth_call(chain, to, data)
            except Exception:
                logger.debug("gateway eth_call failed (legacy path)", exc_info=True)
                return None
        logger.warning(
            "gateway eth_call: block=%r requested but client signature rejects "
            "the kwarg; refusing to fall back to 'latest' to preserve pinning "
            "(VIB-4589). Caller will get None and skip the post-state read.",
            block,
        )
        return None
    except Exception:
        logger.debug("gateway eth_call failed", exc_info=True)
        return None


def read_aave_user_emode(
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    pool_address: str,
    block: int | str | None = None,
) -> int | None:
    """Read Aave V3 ``Pool.getUserEMode(user)`` and return the category (uint8 range).

    VIB-4213 — required for the Tier-2 Aave V3 registry identity tuple. A USDC
    supply inside e-mode category 1 (stables) has different LTV/LT than a USDC
    supply outside e-mode, so the registry MUST disambiguate.

    Returns:
        - ``int`` (0..255) when the call succeeds. ``0`` is a real, valid value
          meaning "user is not in any e-mode category" — distinct from ``None``.
        - ``None`` when the gateway call fails, returns empty/malformed hex, the
          chain has no configured Aave V3 pool, or the decoded value lies
          outside the documented uint8 range (Aave V3 e-mode category ids are
          stored as uint8). Empty ≠ Zero (AGENTS.md §Accounting).
    """
    calldata = _AAVE_GET_USER_EMODE_SELECTOR + _pad_address(wallet_address)
    hex_data = _gateway_eth_call(gateway_client, chain, pool_address, calldata, block=block)
    # Decode via the connector-owned spec parser (single source) — identical
    # uint8 / Empty≠Zero semantics to the former inline decode, pinned by PR-1's
    # account-state spec test.
    return parse_user_emode_hex(hex_data)


def read_lending_account_state(
    *,
    protocol: str,
    chain: str,
    wallet_address: str,
    market_id: str | None,
    gateway_client: Any,
    price_oracle: dict | None,
    block: int | str | None = None,
) -> LendingAccountState | None:
    """Read a wallet's aggregate lending account state for any spec-backed protocol.

    The single generic reader VIB-4929 PR-3a uses in place of the per-protocol
    ``read_<protocol>_account_state`` executors (Aave + Morpho). Adding a lending
    connector to the read path now requires **zero**
    framework edits here: the connector publishes an ``ACCOUNT_STATE_READ_SPEC``
    (+ a ``market_params`` table and ``valuation_role_keys`` if it is not
    USD-native), and this reader drives it through the registry.

    The framework keeps exactly the two responsibilities the connector spec must
    stay pure of (Gateway-boundary rule + purity contract):

    1. **Price + decimals resolution** for non-USD-native protocols. The spec
       declares *which* tokens to value via ``valuation_role_keys``; the registry
       resolves those to ``(query_field, token_symbol)`` pairs against the
       connector's market table; this reader resolves each token's USD price (via
       the shape-tolerant :func:`_resolve_oracle_price`) and decimals (via the
       token resolver) and **injects** them onto the query. USD-native protocols
       (the Aave family) declare no roles, so this loop is empty and no oracle is
       touched.
    2. **The gateway round-trip + block pinning.** The spec only *describes +
       decodes* the reads; this reader executes each planned :class:`EthCall` via
       :func:`_gateway_eth_call` (block pinning + legacy-signature fallback
       preserved) and hands the blobs to the spec's pure reducer.

    Fails closed (returns ``None``, never a fabricated zero — Empty ≠ Zero) when:
    the chain has no read-target address; a declared valuation token has no
    resolvable price or decimals; the protocol/chain has no plan; or the spec
    reducer rejects the blobs. ``interest_rate_mode`` is intent metadata the
    caller overlays after this read — it is never decoded here.

    Args:
        protocol: Protocol identifier (e.g. ``"aave_v3"``, ``"morpho_blue"``, or
            the ``"aave"`` alias) — resolved through the registry.
        chain: Chain identifier (e.g. ``"ethereum"``).
        wallet_address: Position owner address.
        market_id: Per-market id for per-market protocols (Morpho); ``None`` for
            whole-account protocols (the Aave family).
        gateway_client: Gateway client exposing ``eth_call(chain, to, data, block=...)``.
        price_oracle: ``{symbol: price}`` (or nested ``{symbol: {price_usd: ...}}``)
            map used to value non-USD-native positions. Unused for the Aave family.
        block: Optional block to pin every read to (VIB-4589 / F7). ``None`` →
            ``"latest"`` (safe for pre-execution captures); post-execution
            captures MUST pass ``receipt.block_number``.
    """
    from almanak.connectors._strategy_base.lending_read_base import AccountStateQuery
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
    from almanak.framework.data.tokens.exceptions import TokenNotFoundError
    from almanak.framework.data.tokens.resolver import get_token_resolver

    try:
        # Resolve the per-chain read target (contract kind from the connector's
        # own ``addresses.py``). Same address the pre-VIB-4929 path read from the
        # connector's address map. Fail closed when the chain has no deployment.
        pm = LendingReadRegistry.position_manager_address(protocol, chain)
        if not pm:
            logger.debug("read_lending_account_state: no read-target for protocol=%s chain=%s", protocol, chain)
            return None

        # Per-market params the reducer needs but cannot read on-chain cheaply
        # (e.g. Morpho's lltv). None for whole-account protocols.
        market_params = LendingReadRegistry.market_params(protocol, chain, market_id) if market_id else None

        # Resolve + inject the valuation inputs for non-USD-native protocols. The
        # spec declares the roles; the registry names the tokens from the market
        # table; the framework prices them here. Aave declares no roles ⇒ empty.
        prices: dict[str, Decimal] = {}
        decimals: dict[str, int] = {}
        injected_tokens: dict[str, str] = {}  # query_field -> symbol (e.g. collateral_token, loan_token)
        resolver = get_token_resolver()
        for query_field, symbol in LendingReadRegistry.valuation_roles(protocol, chain, market_id):
            price = _resolve_oracle_price(price_oracle, symbol)
            if price is None:
                logger.debug(
                    "read_lending_account_state: price unavailable for %s (%s) on %s",
                    symbol,
                    query_field,
                    chain,
                )
                return None  # Empty ≠ Zero — fail closed, never fabricate.
            try:
                token_info = resolver.resolve(symbol, chain=chain)
            except TokenNotFoundError:
                logger.debug("read_lending_account_state: cannot resolve decimals for %s on %s", symbol, chain)
                return None
            if token_info is None:
                return None
            prices[symbol] = price
            decimals[symbol] = token_info.decimals
            injected_tokens[query_field] = symbol

        query = AccountStateQuery(
            chain=chain,
            wallet_address=wallet_address,
            market_id=market_id,
            block=block,
            prices=prices or None,
            decimals=decimals or None,
            market_params=market_params,
            collateral_token=injected_tokens.get("collateral_token"),
            loan_token=injected_tokens.get("loan_token"),
        )

        plan = LendingReadRegistry.resolve_account_state_plan(protocol, query)
        if plan is None:
            return None

        # The framework owns the gateway round-trip; the spec only describes the
        # reads. Block pinning + legacy-signature fallback are preserved by
        # ``_gateway_eth_call``; ``None`` for any failed read (the reducer fails
        # closed on a missing required blob).
        results = [_gateway_eth_call(gateway_client, chain, call.to, call.data, block=block) for call in plan.calls]
        return plan.reduce(plan.query, results)
    except Exception:
        logger.debug("read_lending_account_state failed for protocol=%s chain=%s", protocol, chain, exc_info=True)
        return None


@dataclass
class CompoundV3AccountState:
    """Post-execution account summary from Compound V3 Comet userCollateral + borrowBalanceOf."""

    collateral_usd: Decimal
    debt_usd: Decimal
    health_factor: Decimal | None


# crap-allowlist: VIB-4638 — read_compound_v3_account_state is cc=27 / CRAP=35
# pre-existing (already noqa: C901). VIB-4589 / F7 only added passthrough
# plumbing (block= kwarg + docstring + 4 forwarded kwargs) — zero cc delta.
# A proper 3-helper refactor is filed as VIB-4638 and follows the CRAP
# refactor protocol separately from this accounting correctness fix.
def read_compound_v3_account_state(  # noqa: C901
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    collateral_token: str,
    borrow_token: str,
    price_oracle: dict | None,
    market_id: str | None = None,
    block: int | str | None = None,
) -> CompoundV3AccountState | None:
    """Read Compound V3 account state via gateway eth_call.

    Resolves the Comet address and makes two reads.  The first call differs
    depending on whether collateral_token is the market's base asset:

    - Base-asset SUPPLY/WITHDRAW (collateral_token == market base_token, e.g.
      supplying USDC to the USDC Comet):
        balanceOf(wallet) → uint256  (supplied base balance)
        borrowBalanceOf(wallet) → uint256  (net borrow; usually 0 for pure supply)
      health_factor is set to the no-risk sentinel (999999) because base-asset
      supply positions have no liquidation threshold.

    - Collateral SUPPLY/WITHDRAW and BORROW/REPAY (collateral_token ≠ base_token):
        userCollateral(wallet, collateralTokenAddress) → (uint128 balance, uint128 reserved)
        borrowBalanceOf(wallet) → uint256
      health_factor is computed as (collateral_usd × LCF) / debt_usd.

    market_id (e.g. "usdc", "weth") is the preferred way to select the Comet.
    When provided it is used directly for the COMPOUND_V3_COMET_ADDRESSES lookup
    and the actual base asset is derived from COMPOUND_V3_MARKETS so that
    SUPPLY/WITHDRAW callers (which pass the collateral as borrow_token) still read
    the correct market's debt balance.  When omitted, borrow_token is used as the
    market key (original BORROW/REPAY behaviour).

    ``block`` (VIB-4589 / F7) — optional block reference threaded into every
    underlying eth_call so the read pins to a single block. ``None`` (default)
    falls back to ``"latest"`` (safe for pre-execution captures, where the
    read precedes the submitted tx by definition). Post-execution captures
    MUST pass ``receipt.block_number`` to avoid racing the upstream RPC's
    receipt indexer — see the rationale in :func:`capture_lending_post_state`.

    Returns None on any failure (missing prices, gateway unavailable, etc.).
    """
    try:
        from almanak.connectors.compound_v3.adapter import (
            COMPOUND_V3_COMET_ADDRESSES,
            COMPOUND_V3_MARKETS,
        )
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError
        from almanak.framework.data.tokens.resolver import get_token_resolver

        # ── Comet address ─────────────────────────────────────────────────────
        # Prefer market_id (exact market key like "usdc", "weth") over borrow_token.
        # For SUPPLY/WITHDRAW intents borrow_token is the collateral asset being
        # supplied — not the market base asset — so it would select the wrong Comet.
        chain_lower = chain.lower()
        chain_markets = COMPOUND_V3_COMET_ADDRESSES.get(chain_lower, {})
        resolved_market_key = (market_id or borrow_token).lower()
        comet_address = chain_markets.get(resolved_market_key)
        if not comet_address:
            logger.debug(
                "read_compound_v3_account_state: no Comet for chain=%s market=%s",
                chain,
                resolved_market_key,
            )
            return None

        # ── Derive effective borrow token from registry ───────────────────────
        # When market_id is known, look up the market's base_token so the debt
        # balance and price are decoded against the correct asset regardless of
        # what the caller passed as borrow_token.
        effective_borrow_token = borrow_token
        market_registry = COMPOUND_V3_MARKETS.get(chain_lower, {}).get(resolved_market_key)
        if market_id:
            registry_base = market_registry.get("base_token") if market_registry else None
            if not registry_base:
                logger.debug(
                    "read_compound_v3_account_state: missing market registry/base_token for chain=%s market=%s",
                    chain,
                    resolved_market_key,
                )
                return None
            effective_borrow_token = registry_base

        resolver = get_token_resolver()

        # ── Collateral token address ────────────────────────────────────────
        try:
            collateral_info = resolver.resolve(collateral_token, chain=chain)
        except TokenNotFoundError:
            logger.debug("read_compound_v3_account_state: can't resolve collateral=%s", collateral_token)
            return None
        collateral_address = collateral_info.address
        collateral_decimals = collateral_info.decimals

        # ── Borrow token decimals ───────────────────────────────────────────
        try:
            borrow_info = resolver.resolve(effective_borrow_token, chain=chain)
        except TokenNotFoundError:
            logger.debug("read_compound_v3_account_state: can't resolve borrow=%s", effective_borrow_token)
            return None
        borrow_decimals = borrow_info.decimals

        # ── Prices ─────────────────────────────────────────────────────────
        # Use the shape-tolerant resolver so the teardown lane's nested
        # ``{symbol: {price_usd, …}}`` oracle works alongside the iteration
        # lane's flat ``{symbol: price}`` shape (Codex 2026-05-04 review).
        collateral_price = _resolve_oracle_price(price_oracle, collateral_token)
        borrow_price = _resolve_oracle_price(price_oracle, effective_borrow_token)
        if collateral_price is None or borrow_price is None:
            logger.debug(
                "read_compound_v3_account_state: price missing for collateral=%s borrow=%s",
                collateral_token,
                effective_borrow_token,
            )
            return None

        # ── Detect base-asset SUPPLY/WITHDRAW ──────────────────────────────
        # In Compound V3, supplying the market's base asset (e.g. USDC in the
        # USDC Comet) is tracked via balanceOf(wallet), NOT userCollateral().
        # userCollateral() always returns zero for the base asset because Comet
        # stores supplied base amounts in its internal accounting, not the
        # collateral mapping.  We detect this by comparing collateral_token
        # against the registry base_token.
        registry_base_token = market_registry.get("base_token", "") if market_registry else ""
        is_base_asset_supply = collateral_token.upper() == registry_base_token.upper()

        account_hex = _pad_address(wallet_address)

        if is_base_asset_supply:
            # ── Call 1 (base): balanceOf(wallet) → uint256 ──────────────────
            # Returns the supplied base-asset balance.  Expressed in base-token
            # decimals (same as borrow_decimals since base==collateral here).
            balance_of_calldata = _COMPOUND_V3_BALANCE_OF_SELECTOR + account_hex
            balance_of_raw = _gateway_eth_call(gateway_client, chain, comet_address, balance_of_calldata, block=block)
            if not balance_of_raw:
                logger.debug("read_compound_v3_account_state: balanceOf() call failed for base asset")
                return None
            balance_of_hex = balance_of_raw.replace("0x", "")
            if len(balance_of_hex) < 64:
                return None
            collateral_balance_raw = int(balance_of_hex[:64], 16)

            # ── Call 2 (base): borrowBalanceOf(wallet) → always zero ─────────
            # Base-asset suppliers cannot have borrow debt at the same time in
            # Compound V3 — a non-zero borrow would net against the supply.
            # We still call borrowBalanceOf() for correctness (net position).
            borrow_calldata = _COMPOUND_V3_BORROW_BALANCE_SELECTOR + account_hex
            borrow_raw = _gateway_eth_call(gateway_client, chain, comet_address, borrow_calldata, block=block)
            if not borrow_raw:
                logger.debug("read_compound_v3_account_state: borrowBalanceOf() call failed")
                return None
            borrow_hex_data = borrow_raw.replace("0x", "")
            if len(borrow_hex_data) < 64:
                return None
            borrow_balance_raw = int(borrow_hex_data[:64], 16)

            # ── USD values (base asset) ─────────────────────────────────────
            # collateral == base asset, so both use borrow_decimals / borrow_price
            supplied_amount = Decimal(collateral_balance_raw) / Decimal(10**borrow_decimals)
            borrow_amount = Decimal(borrow_balance_raw) / Decimal(10**borrow_decimals)
            collateral_usd = supplied_amount * borrow_price
            debt_usd = borrow_amount * borrow_price

            # Pure base-asset supply has no liquidation risk — sentinel HF.
            health_factor: Decimal | None = Decimal("999999")
        else:
            # ── Call 1: userCollateral(wallet, collateralTokenAddress) ──────
            collateral_hex = _pad_address(collateral_address)
            collateral_calldata = _COMPOUND_V3_USER_COLLATERAL_SELECTOR + account_hex + collateral_hex
            collateral_raw = _gateway_eth_call(gateway_client, chain, comet_address, collateral_calldata, block=block)
            if not collateral_raw:
                logger.debug("read_compound_v3_account_state: userCollateral() call failed")
                return None
            collateral_hex_data = collateral_raw.replace("0x", "")
            if len(collateral_hex_data) < 128:  # (uint128 balance, uint128 reserved) = 2 words
                logger.debug(
                    "read_compound_v3_account_state: userCollateral() response too short (%d chars)",
                    len(collateral_hex_data),
                )
                return None
            collateral_balance_raw = int(collateral_hex_data[:64], 16)

            # ── Call 2: borrowBalanceOf(wallet) ─────────────────────────────
            borrow_calldata = _COMPOUND_V3_BORROW_BALANCE_SELECTOR + account_hex
            borrow_raw = _gateway_eth_call(gateway_client, chain, comet_address, borrow_calldata, block=block)
            if not borrow_raw:
                logger.debug("read_compound_v3_account_state: borrowBalanceOf() call failed")
                return None
            borrow_hex_data = borrow_raw.replace("0x", "")
            if len(borrow_hex_data) < 64:
                return None
            borrow_balance_raw = int(borrow_hex_data[:64], 16)

            # ── USD values ──────────────────────────────────────────────────
            collateral_amount = Decimal(collateral_balance_raw) / Decimal(10**collateral_decimals)
            borrow_amount = Decimal(borrow_balance_raw) / Decimal(10**borrow_decimals)
            collateral_usd = collateral_amount * collateral_price
            debt_usd = borrow_amount * borrow_price

            # ── Health factor via per-asset liquidation_collateral_factor ────
            # HF = (collateral_usd * LCF) / debt_usd where LCF < 1.
            # Raw collateral_usd / debt_usd overstates safety; we use the static
            # registry value rather than a live on-chain read to avoid extra calls.
            if debt_usd == 0:
                health_factor = Decimal("999999")
            else:
                market_data = COMPOUND_V3_MARKETS.get(chain_lower, {}).get(resolved_market_key, {})
                collateral_upper = collateral_token.upper()
                col_entry = market_data.get("collaterals", {}).get(collateral_upper)
                if col_entry is None:
                    # Case-insensitive fallback for mixed-case symbols like wstETH
                    for k, v in market_data.get("collaterals", {}).items():
                        if k.upper() == collateral_upper:
                            col_entry = v
                            break
                lcf: Decimal | None = col_entry.get("liquidation_collateral_factor") if col_entry else None
                if lcf is None:
                    logger.debug(
                        "read_compound_v3_account_state: LCF not found for collateral=%s market=%s",
                        collateral_token,
                        resolved_market_key,
                    )
                    health_factor = None
                else:
                    health_factor = min((collateral_usd * lcf) / debt_usd, Decimal("999999"))

        return CompoundV3AccountState(
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            health_factor=health_factor,
        )

    except Exception:
        logger.debug("read_compound_v3_account_state failed", exc_info=True)
        return None


# crap-allowlist: pre-state arm for Compound V3, exercised by integration tests
# in tests/framework/accounting/test_lending_pre_execution_state_vib3489.py and
# tests/framework/accounting/test_compound_v3_account_state.py. Unit-scope
# coverage is artificially low (6%) — see
# docs/internal/coverage-w1-misplacement-audit.md §2 for the measurement-window
# explanation. Combined-scope coverage is 73%.
def _capture_compound_v3_pre_state(
    *,
    intent: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any,
    price_oracle: dict | None,
    block: int | str | None = None,
) -> CompoundV3AccountState | None:
    """Compound V3 pre-state arm — SUPPLY/WITHDRAW require ``intent.market_id``."""
    intent_type_str = _intent_type_value(intent)
    intent_market_id: str | None = getattr(intent, "market_id", None)

    if intent_type_str in ("SUPPLY", "WITHDRAW"):
        # market_id is required for SUPPLY/WITHDRAW: without it we cannot
        # determine which Comet to query — falling back to the collateral token
        # symbol would select the wrong market on chains with multiple Comets.
        if not intent_market_id:
            logger.debug(
                "capture_lending_pre_state: Compound V3 pre-state skipped"
                " (market_id required for SUPPLY/WITHDRAW but not set)"
            )
            return None
        # intent.token is the collateral asset; market_id identifies the Comet and
        # its base asset (used for borrowBalanceOf and debt pricing).
        collateral_token_sym: str | None = getattr(intent, "token", None)
        borrow_token_sym: str | None = getattr(intent, "token", None)  # overridden by market_id
    else:
        collateral_token_sym = getattr(intent, "collateral_token", None)
        borrow_token_sym = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)

    if not collateral_token_sym:
        logger.debug("capture_lending_pre_state: Compound V3 pre-state skipped (missing collateral token)")
        return None

    compound_pre_state = read_compound_v3_account_state(
        gateway_client=gateway_client,
        chain=chain,
        wallet_address=wallet_address,
        collateral_token=collateral_token_sym,
        borrow_token=borrow_token_sym or "",
        price_oracle=price_oracle,
        market_id=intent_market_id,
        block=block,
    )
    if compound_pre_state is None:
        logger.debug("capture_lending_pre_state: Compound V3 read returned None for chain=%s", chain)
    return compound_pre_state


# Registry: protocol identifier → per-protocol pre-state reader.
# VIB-4929 PR-3a: Aave + Morpho route through the generic ``read_lending_account_state``
# (no per-protocol entry here — see ``capture_lending_pre_state``). Compound V3 stays on
# its legacy executor transitionally — it has no addresses.py / AddressRegistry entry yet,
# so it cannot resolve through the registry's account-state path until PR-3b.
_LendingState = LendingAccountState | CompoundV3AccountState | None
_PreStateReader = Callable[..., _LendingState]
_PROTOCOL_PRE_STATE_READERS: dict[str, _PreStateReader] = {
    # transitional: folds into the generic reader in PR-3b
    "compound_v3": _capture_compound_v3_pre_state,
}

# Protocols the generic ``read_lending_account_state`` path is *enabled* for on the
# live-money accounting read path. Registering an account-state spec
# (``LendingReadRegistry._ACCOUNT_STATE_LOADERS``) makes a connector spec-*capable*,
# but ENABLING it here is a deliberate, per-protocol opt-in: each entry was migrated
# AND fork/byte-equivalence-verified in its PR. This gate is what stops a connector
# that merely registered a spec — e.g. Spark, an Aave-fork that opted into
# ``_ACCOUNT_STATE_LOADERS`` but whose generic read is not yet framework-verified
# (VIB-4963) — from silently producing HIGH-confidence reads. Add a protocol here
# only once its generic read is verified on a real fork.
_GENERIC_PRE_STATE_PROTOCOLS: frozenset[str] = frozenset({"aave_v3", "aave", "morpho_blue"})


def _overlay_aave_interest_rate_mode(state: LendingAccountState, intent: Any) -> LendingAccountState:
    """Overlay the Aave intent-layer ``interest_rate_mode`` onto a decoded state.

    Aave's ``interest_rate_mode`` is intent metadata, not an on-chain field — the
    generic reader never decodes it. For BORROW/REPAY intents we thread it onto
    the (frozen) :class:`LendingAccountState` via ``dataclasses.replace`` so it
    lands in ``pre_state_json`` / ``post_state_json``, mirroring the pre-VIB-4929
    Aave pre-state capture behaviour byte-for-byte:

    * ``intent.interest_rate_mode`` set → ``str(...)`` of it.
    * unset on a BORROW/REPAY → ``"variable"`` (the rate mode the on-chain tx
      actually carries; stable mode is deprecated on Aave V3 —
      ``connectors/base/lending/aave_helpers.py``).

    SUPPLY/WITHDRAW (and non-BORROW/REPAY) leave it ``None``.
    """
    intent_type_str = _intent_type_value(intent).upper()
    if intent_type_str not in {"BORROW", "REPAY"}:
        return state
    rate_mode = getattr(intent, "interest_rate_mode", None)
    # InterestRateMode is a ``Literal["variable"]`` at the intent layer; str()
    # handles both the Literal value and any future enum. Falls back to the
    # rate mode the BORROW/REPAY dispatch will actually carry.
    resolved = str(rate_mode) if rate_mode is not None else "variable"
    return dataclasses.replace(state, interest_rate_mode=resolved)


def capture_lending_pre_state(
    *,
    intent: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    price_oracle: dict | None,
    block: int | str | None = None,
) -> LendingAccountState | CompoundV3AccountState | None:
    """Read on-chain lending state BEFORE the transaction is submitted (VIB-3489).

    Called by the strategy runner before executing the intent bundle.  The
    returned state is later forwarded as ``pre_execution_state`` to
    ``build_lending_accounting_event()`` so that before/after deltas can be
    computed.

    Returns None (silently, with a debug log) when:
    - The gateway client is not available.
    - The intent is not a supported lending protocol (Aave V3 / Morpho Blue / Compound V3).
    - Any gateway eth_call fails.

    Never raises; never substitutes stale data on failure.

    VIB-4929 PR-3a dispatch: Aave V3 + Morpho Blue route through the generic
    :func:`read_lending_account_state`, but ONLY for protocols explicitly enabled
    in ``_GENERIC_PRE_STATE_PROTOCOLS`` (migrated AND fork-verified in their PR). A
    connector that merely *registers* an account-state spec is spec-capable but is
    NOT auto-enabled on this live-money read path — e.g. Spark (an Aave-fork that
    opted into ``_ACCOUNT_STATE_LOADERS``) stays unread → ESTIMATED until it is
    verified and added (VIB-4963). Compound V3 stays on its legacy
    :func:`_capture_compound_v3_pre_state` transitionally (folds into the generic
    reader in PR-3b).

    VIB-4589 / F7: ``block`` pins every underlying eth_call to a single
    block reference. Pre-state captures pass ``None`` (→ ``"latest"`` — safe
    because the read precedes submission). Post-state captures pass
    ``receipt.block_number`` (via :func:`capture_lending_post_state`) so the
    snapshot reflects exactly the state produced by the confirmed receipt
    and cannot race the upstream RPC's receipt indexer.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    if gateway_client is None:
        return None

    if _intent_type_value(intent) not in _LENDING_INTENT_TYPES:
        return None

    protocol = str(getattr(intent, "protocol", "") or "").lower()

    # transitional: folds into the generic reader in PR-3b
    legacy_reader = _PROTOCOL_PRE_STATE_READERS.get(protocol)
    if legacy_reader is not None:
        return legacy_reader(
            intent=intent,
            chain=chain,
            wallet_address=wallet_address,
            gateway_client=gateway_client,
            price_oracle=price_oracle,
            block=block,
        )

    # Generic path — gated to the explicitly-enabled, fork-verified protocols
    # (``_GENERIC_PRE_STATE_PROTOCOLS``). A spec-capable-but-unverified connector
    # (e.g. Spark — VIB-4963) is NOT read here: it stays unread (→ ESTIMATED),
    # preserving pre-VIB-4929 behavior rather than silently upgrading to HIGH.
    if protocol not in _GENERIC_PRE_STATE_PROTOCOLS:
        return None
    inputs = LendingReadRegistry.query_inputs(protocol, intent)
    if inputs is None:
        return None

    state = read_lending_account_state(
        protocol=protocol,
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        price_oracle=price_oracle,
        block=block,
        **inputs,
    )
    if state is None:
        return None
    # Aave-family intent-metadata overlay (interest_rate_mode). Gated on the
    # structural family discriminator the reducer stamps, not a protocol-name
    # string — keeps the framework consumer protocol-agnostic.
    if state.family == "aave":
        state = _overlay_aave_interest_rate_mode(state, intent)
    return state


def capture_lending_post_state(
    *,
    intent: Any,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    price_oracle: dict | None,
    block: int | str | None = None,
) -> LendingAccountState | CompoundV3AccountState | None:
    """Read on-chain lending state AFTER the transaction confirms (VIB-3474).

    The post-state capture is the missing piece that ships
    ``transaction_ledger.post_state_json`` for lending intents. The legacy
    ``build_lending_accounting_event()`` performed the same read inline; we now
    expose it as a standalone capture so the runner can populate the column
    *before* it is serialised to the ledger row, which the new
    ``category_handlers/lending_handler.py`` then reads back.

    The implementation delegates to ``capture_lending_pre_state`` — the
    only difference is temporal (called by the runner after TX confirmation).
    VIB-4589 / F7: callers SHOULD pass ``block=receipt.block_number`` so the
    read pins to the exact block of the confirmed receipt. The pre-fix
    behaviour (``block=None`` → ``"latest"``) caused stale post-state on
    mainnet when the upstream RPC's receipt indexer trailed the call site
    — a confirmed WITHDRAW receipt was not yet visible to the next
    ``"latest"`` view, so the read returned a near-full collateral balance.

    Returns ``None`` (silently, with a debug log) when the intent isn't a
    supported lending protocol or any gateway call fails. Never raises; never
    fabricates stale data.
    """
    return capture_lending_pre_state(
        intent=intent,
        chain=chain,
        wallet_address=wallet_address,
        gateway_client=gateway_client,
        price_oracle=price_oracle,
        block=block,
    )


def lending_state_to_dict(
    state: LendingAccountState | CompoundV3AccountState | None,
    *,
    protocol: str,
) -> dict[str, Any] | None:
    """Serialize a captured lending state to the ``pre_state_json`` /
    ``post_state_json`` shape that ``category_handlers/lending_handler.py`` reads.

    Returns ``None`` when ``state`` is ``None`` so callers can fall through
    to the wallet-balances-only path without fabricating zeros.

    Schema (Accounting-AttemptNo17 §3 D3, extended by VIB-4213 §Aave V3):
    ```json
    {
        "protocol": "aave_v3",
        "collateral_usd": "15420.50",
        "debt_usd": "8200.00",
        "health_factor": "1.882",
        "liquidation_threshold_bps": 8500,
        "e_mode_category": 0,
        "interest_rate_mode": "variable",
        "lltv": "0.86"
    }
    ```

    All numeric fields are stringified Decimals — the handler parses with
    ``Decimal(str(post_state["..."]))`` so JSON round-trip is loss-free.

    VIB-4929 PR-3a: Aave + Morpho now share the unified
    :class:`LendingAccountState`. The persisted dict stays **byte-identical** to
    the pre-PR per-protocol shapes:

    * **Aave family** (``state.family == "aave"``): emits ``liquidation_threshold_bps``
      (decoded int), ``e_mode_category``, AND ``interest_rate_mode``. CRITICAL: the
      last two keys are emitted **even when their value is ``None``** (JSON null) —
      the pre-PR ``isinstance(AaveAccountState)`` branch did this unconditionally,
      so they are gated on the **structural** ``family`` discriminator, NOT on
      value-presence. Dropping them when ``None`` would silently shrink the
      persisted dict. Empty ≠ Zero: a measured ``e_mode_category == 0`` (user not
      in any e-mode) stays distinguishable from ``null`` (read failed).
    * **Morpho family** (``LendingAccountState`` with ``lltv`` set, no Aave
      discriminator): emits ``lltv`` (str) + a derived ``liquidation_threshold_bps``
      (``round(lltv * 10000)``, ROUND_HALF_UP) and never the Aave-only keys.
    * **Compound V3** (transitional ``CompoundV3AccountState``): only the common
      three keys.
    """
    if state is None:
        return None
    out: dict[str, Any] = {"protocol": protocol.lower()}
    # collateral_usd / debt_usd / health_factor are present on every state type.
    out["collateral_usd"] = str(state.collateral_usd) if state.collateral_usd is not None else None
    out["debt_usd"] = str(state.debt_usd) if state.debt_usd is not None else None
    out["health_factor"] = str(state.health_factor) if state.health_factor is not None else None

    if isinstance(state, LendingAccountState):
        if state.family == "aave":
            # Gated on the structural family discriminator, NOT value-presence —
            # the pre-PR AaveAccountState branch emitted all three keys
            # unconditionally. liquidation_threshold_bps is always populated for a
            # non-None Aave read (the spec requires the primary getUserAccountData
            # blob); int() matches the pre-PR cast.
            if state.liquidation_threshold_bps is not None:
                out["liquidation_threshold_bps"] = int(state.liquidation_threshold_bps)
            # e_mode_category (int | None) — emit None (JSON null) when the
            # secondary getUserEMode read failed; the raw int otherwise (incl. the
            # measured ``0`` = "not in any e-mode").
            out["e_mode_category"] = state.e_mode_category
            # interest_rate_mode (str | None) — set on BORROW/REPAY only;
            # SUPPLY/WITHDRAW and the post-state path leave it None ⇒ JSON null.
            out["interest_rate_mode"] = state.interest_rate_mode
        elif state.lltv is not None:
            # Morpho family: lltv IS the liquidation threshold; surface it in bps
            # too so the handler's lltv-aware path doesn't need to branch on protocol.
            out["lltv"] = str(state.lltv)
            try:
                out["liquidation_threshold_bps"] = int(
                    (state.lltv * Decimal("10000")).to_integral_value(rounding="ROUND_HALF_UP")
                )
            except (InvalidOperation, TypeError, ValueError):
                pass
    return out


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


def _select_lending_raw_amount(extracted: dict) -> int | None:
    """Return the canonical raw-int amount for a lending intent from enriched data.

    MorphoMay15 §6.2 (F2): Morpho Blue isolated-market SUPPLY intents emit
    ``SupplyCollateral`` on-chain — distinct from the loan-side ``Supply``.
    The enricher's per-protocol overlay (``EXTRACTION_SPECS_BY_PROTOCOL[
    "morpho_blue"]``) surfaces the collateral assets as
    ``supply_collateral_amount``. Without including it in this lookup,
    ``raw_amount`` stays ``None`` for Morpho collateral supplies and the
    SUPPLY accounting branch silently emits ``amount_token=None`` /
    ``principal_delta_usd=None``. ``supply_amount`` retains precedence so the
    loan-side path is unchanged. The symmetric ``withdraw_collateral_amount``
    slot is reserved for the WITHDRAW leg once the Morpho parser exposes
    that extractor.
    """
    return (
        extracted.get("supply_amount")
        or extracted.get("supply_collateral_amount")
        or extracted.get("borrow_amount")
        or extracted.get("repay_amount")
        or extracted.get("withdraw_amount")
    )


def _ray_to_bps(ray_value: int | float | Decimal | str | None) -> int | None:
    """Convert an APR value to integer basis-points (1 bps = 0.01 %).

    Accepts two input forms:
    - Already-fractional decimal (e.g. Decimal("0.05") → 500 bps): produced
      by Aave V3 / Spark receipt parsers which pre-normalize from ray.
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


def _resolve_oracle_price(price_oracle: dict | None, asset: str) -> Decimal | None:
    """Look up a token's USD price from the price_oracle dict, tolerant of both shapes.

    Accepts both the legacy flat ``{symbol: price}`` shape (returned by
    ``MarketSnapshot.get_price_oracle_dict()``) AND the AttemptNo17 G12
    nested shape ``{symbol: {"price_usd": ..., "oracle_source": ..., ...}}``
    that ``_portfolio_snapshot_to_price_oracle`` produces for the teardown
    lane (VIB-3934 + Codex 2026-05-04 review). Without the nested branch,
    teardown ledger rows on Morpho Blue / Compound V3 silently lost
    collateral/debt/HF because the readers passed the dict to
    ``Decimal(str(...))`` and got None back.

    Symbol lookup is case-insensitive — tries exact match first (cheap fast
    path), then falls back to a normalized-key map so mixed-case oracle keys
    like ``"wstETH"`` resolve regardless of the asset's casing
    (CodeRabbit 2026-05-04 review).
    """
    if price_oracle is None:
        return None
    candidate = price_oracle.get(asset)
    if candidate is None:
        # Mixed-case fallback: build a one-shot lower-keyed lookup so any
        # oracle entry whose key normalizes to the same lower form matches.
        # ``next(iter(...), None)`` returns the first colliding value when
        # multiple oracle keys share a normalized form (rare); the asset's
        # canonical symbol from ``Intent`` is always single-cased so
        # collisions in real callers don't happen.
        asset_lower = asset.lower()
        candidate = next(
            (v for k, v in price_oracle.items() if isinstance(k, str) and k.lower() == asset_lower),
            None,
        )
    if candidate is None:
        return None
    if isinstance(candidate, dict):
        candidate = candidate.get("price_usd")
        if candidate is None:
            return None
    try:
        return Decimal(str(candidate))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _amount_to_usd(amount_human: Decimal | None, price_oracle: dict | None, asset: str) -> Decimal | None:
    """Convert a human-readable token amount to USD using the price_oracle dict.

    Tolerant of both flat and nested oracle shapes via :func:`_resolve_oracle_price`.
    """
    if amount_human is None:
        return None
    price = _resolve_oracle_price(price_oracle, asset)
    if price is None:
        return None
    try:
        return price * amount_human
    except (InvalidOperation, ValueError, ArithmeticError):
        return None


# crap-allowlist: VIB-4437 / VIB-4440 — replay-path counterpart of handle_lending
# (also allowlisted at lending_handler.py:78 under VIB-4257). Dispatches over 5
# lending intent types × 3 protocols (Aave, Morpho, Compound); CRAP=165 (cc=99,
# cov=81%) reflects the integration matrix, not a tidiness gap. Refactor tracked
# under VIB-4440 and must follow .claude/rules/crap-refactor.md.
def build_lending_accounting_event(  # noqa: C901
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    gateway_client: Any | None,
    basis_store: FIFOBasisStore,
    price_oracle: dict | None,
    ledger_entry_id: str | None = None,
    pre_execution_state: LendingAccountState | CompoundV3AccountState | None = None,
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
    raw_amount: int | None = _select_lending_raw_amount(extracted)
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
    native_token = native_token_for_chain(chain)
    gas_usd = _amount_to_usd(gas_cost_native, price_oracle, native_token)

    # ── FIFO lot matching ─────────────────────────────────────────────────────
    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None

    # VIB-3964: a single chain+wallet wallet-basis pool is shared across the SWAP
    # handler and the lending writers — BORROW / WITHDRAW credit it, SUPPLY /
    # REPAY drain it. Mirroring on-chain wallet flow into the FIFO store is what
    # lets a SWAP that disposes a borrowed (or withdrawn) token report a non-null
    # ``realized_pnl_usd`` and unblocks the looping G6 reconciliation cell.
    _chain_norm = chain.lower().strip() if chain else ""
    _wallet_norm = wallet_address.lower().strip() if wallet_address else ""
    swap_wallet_key = f"swap:{_chain_norm}:{_wallet_norm}" if _chain_norm and _wallet_norm else ""

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
                source_ledger_entry_id=ledger_entry_id,
            )
            # VIB-3964: borrowed tokens land in the wallet — credit the wallet
            # basis pool so a follow-up SWAP that disposes them gets a basis.
            if swap_wallet_key:
                basis_store.record_swap_acquisition(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                    cost_usd=principal_delta_usd,
                    timestamp=now,
                    lot_id=make_accounting_event_id(
                        deployment_id, cycle_id, "BORROW_WALLET_LOT", _borrow_id_seed, asset
                    ),
                    source="BORROW",
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
            # VIB-3964: REPAY drains wallet inventory — dispose the swap-key
            # lots so the wallet pool stays consistent with on-chain balance.
            # Returned (cost_consumed, unmatched) is intentionally discarded
            # here; lending realized-PnL still routes through match_repay
            # above. The disposal exists purely to mirror wallet flow.
            if swap_wallet_key:
                basis_store.match_swap_disposal(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                )

        elif intent_type_str == "SUPPLY":
            principal_delta_usd = _amount_to_usd(amount_human, price_oracle, asset)
            # VIB-3964: SUPPLY drains wallet inventory — dispose to keep the
            # wallet basis pool truthful for a later WITHDRAW-then-SWAP.
            if swap_wallet_key:
                basis_store.match_swap_disposal(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                )
            # VIB-3964 (G6 closer): also record the supplied principal as a
            # BORROW-style lot keyed under ``supply:<lending_pk>`` so a later
            # WITHDRAW can FIFO-match and surface ``interest_accrued_usd``.
            # Symmetric with the live writer path in
            # ``category_handlers/lending_handler.py`` — keeping the two writers
            # in lock-step is the contract that prevents drift between live
            # and replay (CodeRabbit 2026-05-04).
            _supply_id_seed = tx_hash or ledger_entry_id or position_key
            basis_store.record_borrow(
                deployment_id=deployment_id,
                position_key=f"supply:{position_key}",
                token=asset,
                principal_amount=amount_human,
                principal_usd=principal_delta_usd,
                timestamp=now,
                lot_id=make_accounting_event_id(
                    deployment_id, cycle_id, "SUPPLY_LOT", _supply_id_seed, f"supply:{position_key}"
                ),
                source_ledger_entry_id=ledger_entry_id,
            )

        elif intent_type_str == "WITHDRAW":
            # Total withdraw value in USD — used as wallet-basis lot cost
            # but NOT as the event's ``principal_delta_usd``. The split
            # mirrors REPAY (pr-auditor 2026-05-04 item 2): principal is
            # the matched supply principal only; the residual is interest.
            _withdraw_total_usd = _amount_to_usd(amount_human, price_oracle, asset)
            # VIB-3964: WITHDRAW credits the wallet (principal + accrued
            # supply interest). Mint a swap-key lot for the FULL withdraw
            # amount so the next SWAP that disposes the withdrawn token
            # can compute realized PnL.
            if swap_wallet_key:
                _withdraw_id_seed = tx_hash or ledger_entry_id or position_key
                basis_store.record_swap_acquisition(
                    deployment_id=deployment_id,
                    position_key=swap_wallet_key,
                    token=asset,
                    amount=amount_human,
                    cost_usd=_withdraw_total_usd,
                    timestamp=now,
                    lot_id=make_accounting_event_id(
                        deployment_id, cycle_id, "WITHDRAW_WALLET_LOT", _withdraw_id_seed, asset
                    ),
                    source="WITHDRAW",
                )
            # VIB-3964 (G6 closer): FIFO-match the SUPPLY lots and split
            # principal vs interest the same way REPAY does. Trust the
            # matched ``interest_or_yield`` only when the FIFO match was
            # either fully principal-covered OR the implied interest is
            # bounded by consumed principal — see the lending_handler
            # counterpart for the full reasoning (Codex 2026-05-04 P2).
            _supply_match = basis_store.match_repay(
                deployment_id=deployment_id,
                position_key=f"supply:{position_key}",
                token=asset,
                repay_amount=amount_human,
            )
            if _supply_match.unmatched_amount > 0:
                principal_delta_usd = _withdraw_total_usd
                interest_delta_usd = None
            elif (
                _supply_match.repaid_principal >= amount_human
                or _supply_match.interest_or_yield <= _supply_match.repaid_principal
            ):
                principal_delta_usd = _amount_to_usd(_supply_match.repaid_principal, price_oracle, asset)
                interest_delta_usd = _amount_to_usd(_supply_match.interest_or_yield, price_oracle, asset)
            else:
                principal_delta_usd = _withdraw_total_usd
                interest_delta_usd = None

    # ── After-state: on-chain read ───────────────────────────────────────────
    # VIB-4929 PR-3a: Aave V3 + Morpho Blue share the unified ``LendingAccountState``,
    # read through the single generic ``read_lending_account_state`` — but only for
    # protocols explicitly enabled + fork-verified in ``_GENERIC_PRE_STATE_PROTOCOLS``.
    # A spec-capable-but-unverified connector (e.g. Spark, which opted into
    # ``_ACCOUNT_STATE_LOADERS``) is NOT auto-read here (→ ESTIMATED; VIB-4963).
    # Compound V3 stays on its legacy reader transitionally (PR-3b).
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    generic_state: LendingAccountState | None = None
    morpho_unavailable_reason: str = ""

    is_morpho = protocol.lower() == "morpho_blue"
    is_compound_v3 = protocol.lower() == "compound_v3"  # transitional: folds into the generic reader in PR-3b
    compound_v3_state: CompoundV3AccountState | None = None

    # Generic read path for every protocol with a connector-owned account-state
    # spec (Aave, Morpho, …) — Compound is excluded (handled transitionally below).
    # ``block=None`` here preserves the pre-VIB-4929 event-builder semantics: the
    # event-builder post-state read was never block-pinned (the pinned post-state
    # capture is the runner's ``capture_lending_post_state`` path).
    if not is_compound_v3 and gateway_client is not None and protocol.lower() in _GENERIC_PRE_STATE_PROTOCOLS:
        query_inputs = LendingReadRegistry.query_inputs(protocol, intent)
        if query_inputs is not None and (
            not is_morpho or intent_type_str in ("BORROW", "REPAY", "DELEVERAGE", "SUPPLY", "WITHDRAW")
        ):
            # Morpho post-state HF persistence covers all lending intent types so
            # post-state is in parity with pre-state (VIB-4432). market_id is
            # required for Morpho — surface the same diagnostic as the pre-PR path.
            if is_morpho and not market_id:
                morpho_unavailable_reason = "market_id missing from intent — cannot read Morpho Blue position"
                logger.debug("read_lending_account_state skipped: %s", morpho_unavailable_reason)
            else:
                generic_state = read_lending_account_state(
                    protocol=protocol,
                    chain=chain,
                    wallet_address=wallet_address,
                    gateway_client=gateway_client,
                    price_oracle=price_oracle,
                    **query_inputs,
                )
                # Aave-family intent-metadata overlay (interest_rate_mode), gated on
                # the structural family discriminator — parity with the pre-state arm.
                if generic_state is not None and generic_state.family == "aave":
                    generic_state = _overlay_aave_interest_rate_mode(generic_state, intent)
                if generic_state is None and is_morpho:
                    morpho_unavailable_reason = "Morpho Blue position/market gateway read failed"

    if (
        is_compound_v3
        and gateway_client is not None
        and intent_type_str in ("BORROW", "REPAY", "DELEVERAGE", "SUPPLY", "WITHDRAW")
    ):
        intent_market_id_c3 = getattr(intent, "market_id", None)
        if intent_type_str in ("SUPPLY", "WITHDRAW"):
            # market_id is required for SUPPLY/WITHDRAW: without it we cannot
            # determine which Comet to query — falling back to the collateral token
            # symbol would select the wrong market on chains with multiple Comets.
            if not intent_market_id_c3:
                logger.debug(
                    "capture_lending_post_state: Compound V3 post-state skipped"
                    " (market_id required for SUPPLY/WITHDRAW but not set)"
                )
            else:
                collateral_token_sym_c3 = getattr(intent, "token", None)
                borrow_token_sym_c3 = getattr(intent, "token", None)  # overridden by market_id
                if collateral_token_sym_c3:
                    compound_v3_state = read_compound_v3_account_state(
                        gateway_client=gateway_client,
                        chain=chain,
                        wallet_address=wallet_address,
                        collateral_token=collateral_token_sym_c3,
                        borrow_token=borrow_token_sym_c3 or "",
                        price_oracle=price_oracle,
                        market_id=intent_market_id_c3,
                    )
        else:
            collateral_token_sym_c3 = getattr(intent, "collateral_token", None)
            borrow_token_sym_c3 = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)
            if collateral_token_sym_c3:
                compound_v3_state = read_compound_v3_account_state(
                    gateway_client=gateway_client,
                    chain=chain,
                    wallet_address=wallet_address,
                    collateral_token=collateral_token_sym_c3,
                    borrow_token=borrow_token_sym_c3 or "",
                    price_oracle=price_oracle,
                    market_id=intent_market_id_c3,
                )

    # ── Unify after-state fields from whichever protocol provided data ────────
    # Priority: generic state (Aave / Morpho) > Compound V3 state > None.
    got_after_state = generic_state is not None or compound_v3_state is not None

    if generic_state is not None:
        # Single field extraction off the unified ``LendingAccountState`` — no
        # per-protocol ``isinstance`` priority chain (VIB-4929 PR-3a). The
        # protocol-shape differences are carried structurally on the state:
        #   * Aave family: ``liquidation_threshold_bps`` set, ``lltv`` None →
        #     ``liquidation_threshold = bps / 10000``.
        #   * Morpho: ``lltv`` set, ``liquidation_threshold_bps`` None → lltv IS
        #     the liquidation threshold (no-debt HF stays the 999999 sentinel;
        #     callers must not treat HF == 999999 as a trigger).
        collateral_after: Decimal | None = generic_state.collateral_usd
        debt_after: Decimal | None = generic_state.debt_usd
        hf_after: Decimal | None = generic_state.health_factor
        lt_bps: int | None = generic_state.liquidation_threshold_bps
        lltv_after: Decimal | None = generic_state.lltv
        if lt_bps is not None:
            liquidation_threshold: Decimal | None = Decimal(lt_bps) / Decimal("10000")
        elif generic_state.lltv is not None:
            liquidation_threshold = generic_state.lltv  # LLTV serves as liquidation_threshold
        else:
            liquidation_threshold = None
    elif compound_v3_state is not None:
        collateral_after = compound_v3_state.collateral_usd
        debt_after = compound_v3_state.debt_usd
        hf_after = compound_v3_state.health_factor
        lt_bps = None  # Compound V3 uses per-asset collateral factors, not a single threshold
        liquidation_threshold = None
        lltv_after = None
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
        # The unified LendingAccountState and the transitional CompoundV3AccountState
        # share the same field names for the data being extracted — no protocol-
        # specific branching needed.
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

    # ── DELEVERAGE enrichment (VIB-3490) ─────────────────────────────────────
    # For DELEVERAGE events, persist the observed HF as health_factor_before
    # (pre-trigger snapshot) so analytics can reconstruct the risk state at the
    # moment the deleverage was triggered without needing a separate pre-read.
    #
    # Trigger metadata (trigger_reason, observed_hf, target_hf) is appended to
    # unavailable_reason ONLY when the event is already estimated/degraded (i.e.
    # got_after_state is False). When confidence is HIGH the deleverage context
    # is emitted as a debug log only — it must not overwrite an empty
    # unavailable_reason, as that would incorrectly signal data degradation to
    # downstream consumers.
    hf_before_from_intent: Decimal | None = None  # populated below for DELEVERAGE only
    if intent_type_str == "DELEVERAGE":
        trigger_reason = getattr(intent, "trigger_reason", "") or ""
        observed_hf_intent = getattr(intent, "observed_hf", None)
        target_hf_intent = getattr(intent, "target_hf", None)

        # Persist the observed HF as health_factor_before (pre-trigger snapshot).
        if observed_hf_intent is not None:
            try:
                hf_before_from_intent = Decimal(str(observed_hf_intent))
            except (ValueError, TypeError, InvalidOperation):
                pass

        # Build trigger context string for logging / degraded-event annotation.
        parts: list[str] = []
        if trigger_reason:
            parts.append(f"DELEVERAGE: {trigger_reason}")
        else:
            parts.append("DELEVERAGE: emergency-triggered")
        if observed_hf_intent is not None:
            parts.append(f"observed_hf={observed_hf_intent}")
        if target_hf_intent is not None:
            parts.append(f"target_hf={target_hf_intent}")
        deleverage_context = "; ".join(parts)

        if unavailable_reason:
            # Event is already degraded/estimated — safe to append trigger context.
            unavailable_reason = f"{deleverage_context} | {unavailable_reason}"

        logger.debug(
            "DELEVERAGE accounting event enriched: %s (position=%s, confidence=%s)",
            deleverage_context,
            position_key,
            confidence.value,
        )

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
        deployment_id=deployment_id,
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
        # For DELEVERAGE intents: prefer the observed_hf from the intent (the exact HF
        # at the moment the strategy triggered the deleverage) over the pre-execution
        # gateway read. For all other intent types use the pre-execution state read.
        health_factor_before=hf_before_from_intent if hf_before_from_intent is not None else hf_before,
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
