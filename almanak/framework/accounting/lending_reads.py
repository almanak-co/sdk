"""Light lending account-state readers (VIB-4851 PR-2).

Extracted from :mod:`almanak.framework.accounting.lending_accounting` so the
read path stays free of the heavy ``execution.*`` closure that the accounting
*event-builder* pulls in (via ``gas_pricing`` / ``ids``). The framework data
surface (``MarketSnapshot`` → ``position_health``) reads lending account state
through this module; the gateway-boundary import-closure guard
(``tests/framework/data/test_pool_history_source_inspection.py``) requires that
read path to reach **only** light dependencies.

This module owns the two framework responsibilities the connector account-state
specs must stay pure of (Gateway-boundary rule + purity contract): the
gateway-routed ``eth_call`` round-trip (+ block pinning) and the price/decimals
resolution for non-USD-native protocols. The pure spec *describes + decodes* the
reads; this module *executes* them.

Imports are deliberately light: only the connector account-state seam
(``lending_read_base`` / ``lending_read_registry``), the token resolver
(``almanak.framework.data.tokens.*``), and stdlib. It must NOT import
``gas_pricing``, ``ids``, or anything that pulls ``execution.*`` — the
event-builder in ``lending_accounting`` keeps those (and re-exports these
readers for back-compat, so every existing
``from ...lending_accounting import read_lending_account_state`` importer keeps
working unchanged).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.connectors._strategy_base.lending_read_base import (
    _AAVE_GET_USER_EMODE_SELECTOR,
    LendingAccountState,
    parse_user_emode_hex,
)

logger = logging.getLogger(__name__)


def _pad_address(address: str) -> str:
    """Left-pad an EVM address to 32 bytes (64 hex chars, no 0x prefix)."""
    return address.lower().replace("0x", "").zfill(64)


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
    collateral_token: str | None = None,
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
        collateral_token: Intent-derived collateral symbol for protocols whose
            collateral leg the market catalogue does not name (Compound V3 — its
            spec ``query_inputs_fn`` supplies it). Priced + address-resolved here and
            injected onto the query. ``None`` for the Aave family and Morpho.
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

        # Intent-derived collateral leg (VIB-4929 PR-3b, e.g. Compound V3): the spec's
        # ``query_inputs_fn`` names a collateral token the market catalogue does not
        # (any approved collateral can back a Comet). The framework owns price /
        # decimals / address resolution so the spec stays pure. Address resolution is
        # decoupled from pricing (Gemini review): resolve the collateral *address*
        # whenever a collateral token is named — the non-base ``userCollateral`` path
        # needs it even if a valuation role already priced the token — and price it
        # only when it was not already injected (e.g. base-asset supply, where
        # collateral == the role-priced base token).
        collateral_address: str | None = None
        if collateral_token:
            try:
                col_info = resolver.resolve(collateral_token, chain=chain)
            except TokenNotFoundError:
                logger.debug("read_lending_account_state: cannot resolve collateral %s on %s", collateral_token, chain)
                return None
            if col_info is None:
                return None
            collateral_address = col_info.address
            if collateral_token not in prices:
                price = _resolve_oracle_price(price_oracle, collateral_token)
                if price is None:
                    logger.debug(
                        "read_lending_account_state: price unavailable for collateral %s on %s",
                        collateral_token,
                        chain,
                    )
                    return None  # Empty ≠ Zero — fail closed, never fabricate.
                prices[collateral_token] = price
                decimals[collateral_token] = col_info.decimals

        query = AccountStateQuery(
            chain=chain,
            wallet_address=wallet_address,
            market_id=market_id,
            block=block,
            prices=prices or None,
            decimals=decimals or None,
            market_params=market_params,
            collateral_token=collateral_token or injected_tokens.get("collateral_token"),
            loan_token=injected_tokens.get("loan_token"),
            collateral_address=collateral_address,
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
