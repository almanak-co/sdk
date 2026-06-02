"""Resolve amount='all' for intents before compilation.

This module provides the single point of resolution for `amount="all"`
across all intent types. It is called unconditionally as the first step
of `IntentCompiler.compile()`, ensuring that no unresolved `amount="all"`
reaches protocol-specific compilers.

Resolution semantics by intent type:
- WITHDRAW: Query protocol supply balance (aToken, cToken, shares)
- REPAY: Query protocol debt balance (variable debt, borrow balance)
- SWAP / SUPPLY / BRIDGE / STAKE: Query wallet ERC-20 balance
- CHAINED (amount="all" in IntentSequence): Left for sequence resolution

Design doc: docs/internal/discussions/amount-all-resolution-20260407.md
Ticket: VIB-2537
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger("almanak.framework.intents.amount_resolver")


class AmountResolutionCategory(Enum):
    """How to resolve amount='all' for a given intent type."""

    WALLET_BALANCE = "wallet_balance"  # ERC-20 wallet balance (swap, supply, bridge)
    PROTOCOL_SUPPLY = "protocol_supply"  # Protocol supply position (withdraw)
    PROTOCOL_DEBT = "protocol_debt"  # Protocol debt position (repay)
    NOT_APPLICABLE = "not_applicable"  # Intent type doesn't use amount="all"


# Map intent types to resolution categories
_INTENT_TYPE_TO_CATEGORY: dict[str, AmountResolutionCategory] = {
    "SWAP": AmountResolutionCategory.WALLET_BALANCE,
    "SUPPLY": AmountResolutionCategory.WALLET_BALANCE,
    "BRIDGE": AmountResolutionCategory.WALLET_BALANCE,
    "STAKE": AmountResolutionCategory.WALLET_BALANCE,
    "VAULT_DEPOSIT": AmountResolutionCategory.WALLET_BALANCE,
    "WRAP_NATIVE": AmountResolutionCategory.WALLET_BALANCE,
    "UNWRAP_NATIVE": AmountResolutionCategory.WALLET_BALANCE,
    "WITHDRAW": AmountResolutionCategory.PROTOCOL_SUPPLY,
    "REPAY": AmountResolutionCategory.PROTOCOL_DEBT,
}


class ProtocolBalanceReader(ABC):
    """Per-protocol balance reader for amount='all' resolution.

    Each lending protocol implements this to query on-chain positions
    via gateway RPC eth_call.
    """

    @abstractmethod
    def get_supply_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        """Return current supply balance in wei, including accrued interest.

        Args:
            protocol: The concrete protocol identifier (e.g. ``"aave_v3"``,
                ``"spark"``) the caller resolved. A single
                reader may serve several Aave-fork protocols whose on-chain data
                providers differ per chain, so the protocol must be threaded
                through to the position query — never inferred from a default.

        Returns None if the query fails (no RPC, protocol unsupported on chain, etc.).
        """
        ...

    @abstractmethod
    def get_debt_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        """Return current debt balance in wei, including accrued interest.

        Args:
            protocol: The concrete protocol identifier the caller resolved; see
                :meth:`get_supply_balance` for why it must be threaded through.

        Returns None if the query fails.
        """
        ...

    @property
    @abstractmethod
    def supported_protocols(self) -> list[str]:
        """Protocol identifiers this reader handles."""
        ...


# ---------------------------------------------------------------------------
# Per-protocol reader implementations
# ---------------------------------------------------------------------------


class AaveV3BalanceReader(ProtocolBalanceReader):
    """Balance reader for Aave V3 and Spark (Aave-fork) protocols.

    Uses the existing LendingPositionReader to query getUserReserveData
    via gateway RPC.
    """

    @property
    def supported_protocols(self) -> list[str]:
        return ["aave_v3", "spark"]

    def get_supply_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        from ..valuation.lending_position_reader import LendingPositionReader

        reader = LendingPositionReader(gateway_client=gateway_client)
        position = reader.read_position(chain, token_address, wallet, protocol=protocol)
        if position is None:
            return None
        return position.current_atoken_balance

    def get_debt_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        from ..valuation.lending_position_reader import LendingPositionReader

        reader = LendingPositionReader(gateway_client=gateway_client)
        position = reader.read_position(chain, token_address, wallet, protocol=protocol)
        if position is None:
            return None
        return position.total_debt


class CompoundV3BalanceReader(ProtocolBalanceReader):
    """Balance reader for Compound V3.

    Queries Comet.balanceOf(wallet) for supply and
    Comet.borrowBalanceOf(wallet) for debt via gateway RPC eth_call.
    """

    # Function selectors
    _BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)
    _BORROW_BALANCE_OF_SELECTOR = "0x374c49b4"  # borrowBalanceOf(address)

    @property
    def supported_protocols(self) -> list[str]:
        return ["compound_v3"]

    def _get_comet_address(self, chain: str, market_id: str | None) -> str | None:
        from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES

        markets = COMPOUND_V3_COMET_ADDRESSES.get(chain, {})
        if not market_id:
            # Don't silently default to USDC — wrong market means wrong balance.
            # Return None to trigger withdraw_all fallback.
            if len(markets) == 1:
                # Only one market on this chain — safe to use it
                return next(iter(markets.values()))
            logger.warning(
                "Compound V3 market_id not specified and %d markets available on %s — "
                "cannot determine correct Comet contract",
                len(markets),
                chain,
            )
            return None
        return markets.get(market_id)

    def _eth_call(self, gateway_client: Any, chain: str, to: str, data: str) -> str | None:
        """Make an eth_call via gateway RPC."""
        try:
            from almanak.gateway.proto import gateway_pb2

            rpc_stub = getattr(gateway_client, "_rpc_stub", None)
            if rpc_stub is None:
                return None
            timeout = getattr(getattr(gateway_client, "config", None), "timeout", 10)
            params_json = json.dumps([{"to": to, "data": data}, "latest"])
            response = rpc_stub.Call(
                gateway_pb2.RpcRequest(chain=chain, method="eth_call", params=params_json),
                timeout=timeout,
            )
            if not response.success:
                return None
            if response.result:
                return json.loads(response.result)
            return None
        except Exception:
            logger.debug("Compound V3 eth_call failed", exc_info=True)
            return None

    def _query_balance(
        self,
        selector: str,
        chain: str,
        wallet: str,
        market_id: str | None,
        gateway_client: Any,
    ) -> int | None:
        """Query a uint256 balance from Comet using the given function selector."""
        if gateway_client is None:
            return None
        comet = self._get_comet_address(chain, market_id)
        if not comet:
            return None
        wallet_padded = wallet.lower().replace("0x", "").zfill(64)
        calldata = selector + wallet_padded
        result_hex = self._eth_call(gateway_client, chain, comet, calldata)
        if not result_hex:
            return None
        try:
            return int(result_hex.replace("0x", ""), 16)
        except (ValueError, TypeError):
            return None

    def get_supply_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,  # noqa: ARG002 — single-protocol reader; accepted for interface symmetry
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        return self._query_balance(self._BALANCE_OF_SELECTOR, chain, wallet, market_id, gateway_client)

    def get_debt_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,  # noqa: ARG002 — single-protocol reader; accepted for interface symmetry
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        return self._query_balance(self._BORROW_BALANCE_OF_SELECTOR, chain, wallet, market_id, gateway_client)


class MorphoBlueBalanceReader(ProtocolBalanceReader):
    """Balance reader for Morpho Blue.

    Queries the Morpho contract's position(marketId, user) to get
    supply shares and borrow shares, then converts to underlying amounts
    using market state.

    For withdraw_all, Morpho uses shares-based withdrawal (not MAX_UINT256)
    because MAX_UINT256 overflows Morpho's internal mulDiv/uint128 cast.
    This reader returns the share values that the adapter needs.
    """

    @property
    def supported_protocols(self) -> list[str]:
        return ["morpho", "morpho_blue"]

    def get_supply_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,  # noqa: ARG002 — single-protocol reader; accepted for interface symmetry
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        # Morpho Blue uses shares-based withdrawal, so we delegate to the adapter's
        # existing get_position_on_chain() which handles all the complexity.
        # The adapter.withdraw() with withdraw_all=True already queries on-chain position.
        # We return a sentinel value to signal "use withdraw_all=True path".
        # This is because Morpho's supply balance requires share-to-asset conversion
        # that only the adapter knows how to do correctly.
        return None  # Signal: use withdraw_all flag path instead

    def get_debt_balance(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,  # noqa: ARG002 — single-protocol reader; accepted for interface symmetry
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> int | None:
        # Same as supply — Morpho uses shares-based repayment
        return None


# ---------------------------------------------------------------------------
# Reader registry
# ---------------------------------------------------------------------------

_READERS: list[ProtocolBalanceReader] = [
    AaveV3BalanceReader(),
    CompoundV3BalanceReader(),
    MorphoBlueBalanceReader(),
]

_PROTOCOL_TO_READER: dict[str, ProtocolBalanceReader] = {}
for _reader in _READERS:
    for _proto in _reader.supported_protocols:
        _PROTOCOL_TO_READER[_proto] = _reader


def get_reader_for_protocol(protocol: str) -> ProtocolBalanceReader | None:
    """Look up the balance reader for a protocol."""
    return _PROTOCOL_TO_READER.get(protocol.lower())


# ---------------------------------------------------------------------------
# Main resolution function
# ---------------------------------------------------------------------------


def resolve_amount_all(  # noqa: C901
    intent: Any,
    *,
    chain: str,
    wallet_address: str,
    gateway_client: Any = None,
) -> Any:
    """Resolve amount='all' on an intent, returning a new intent with concrete amount.

    This function is the single point of resolution for amount='all'. It is called
    unconditionally at the top of IntentCompiler.compile().

    If the intent does not have amount='all', it is returned unchanged.

    Args:
        intent: Any intent object with an 'amount' field
        chain: Target blockchain
        wallet_address: Wallet address for balance queries
        gateway_client: Gateway client for RPC queries

    Returns:
        The intent with resolved amount, or the original intent if no resolution needed.
    """
    # Check if this intent has amount="all"
    amount = getattr(intent, "amount", None)
    if amount != "all":
        return intent

    # Check for withdraw_all flag — if set, adapter handles it; no resolution needed
    if getattr(intent, "withdraw_all", False):
        return intent
    if getattr(intent, "repay_full", False):
        return intent

    intent_type = getattr(intent, "intent_type", None)
    if intent_type is None:
        return intent

    intent_type_str = str(intent_type.value if hasattr(intent_type, "value") else intent_type).upper()
    category = _INTENT_TYPE_TO_CATEGORY.get(intent_type_str, AmountResolutionCategory.NOT_APPLICABLE)

    if category == AmountResolutionCategory.NOT_APPLICABLE:
        return intent  # Let the compiler handle it (or reject it)

    protocol = getattr(intent, "protocol", None)
    protocol_lower = protocol.lower() if protocol else ""
    token = getattr(intent, "token", None) or getattr(intent, "from_token", None) or ""
    market_id = getattr(intent, "market_id", None)

    # -----------------------------------------------------------------------
    # WALLET BALANCE resolution (swap, supply, bridge, stake, etc.)
    # -----------------------------------------------------------------------
    if category == AmountResolutionCategory.WALLET_BALANCE:
        # This path is handled by the existing compiler methods (bridge, wrap, unwrap)
        # and by the caller sites (strategy_runner, teardown). We don't resolve here
        # to avoid duplicating the gas-reservation logic for native tokens.
        # The compiler's per-intent-type methods already handle this correctly.
        return intent

    # -----------------------------------------------------------------------
    # PROTOCOL SUPPLY BALANCE resolution (withdraw)
    # -----------------------------------------------------------------------
    if category == AmountResolutionCategory.PROTOCOL_SUPPLY:
        reader = get_reader_for_protocol(protocol_lower)

        if reader is None:
            logger.warning(
                "No balance reader for protocol '%s' — amount='all' will be converted to withdraw_all=True",
                protocol_lower,
            )
            return _set_withdraw_all(intent)

        # Resolve token address
        token_address = _resolve_token_address(token, chain)
        if not token_address:
            logger.warning(
                "Cannot resolve token '%s' on %s for protocol balance query — falling back to withdraw_all=True",
                token,
                chain,
            )
            return _set_withdraw_all(intent)

        balance_wei = reader.get_supply_balance(
            chain=chain,
            token_address=token_address,
            wallet=wallet_address,
            protocol=protocol_lower,
            market_id=market_id,
            gateway_client=gateway_client,
        )

        if balance_wei is None:
            # Reader couldn't query — fall back to withdraw_all=True
            logger.info(
                "Protocol balance query returned None for %s/%s — using withdraw_all=True",
                protocol_lower,
                token,
            )
            return _set_withdraw_all(intent)

        if balance_wei <= 0:
            logger.info("Protocol supply balance is 0 for %s/%s — nothing to withdraw", protocol_lower, token)
            # Use withdraw_all=True with zero balance; the adapter will handle gracefully.
            # Cannot use amount=0 because WithdrawIntent validates amount > 0 when not withdraw_all.
            return _set_withdraw_all(intent)

        # Convert wei to token units
        try:
            decimals = _get_token_decimals(token, chain)
        except Exception:
            logger.warning(
                "Cannot determine decimals for %s on %s — falling back to withdraw_all=True",
                token,
                chain,
            )
            return _set_withdraw_all(intent)
        amount_decimal = Decimal(balance_wei) / Decimal(10**decimals)
        logger.info(
            "Resolved withdraw amount='all' for %s/%s: %s (from %d wei)",
            protocol_lower,
            token,
            amount_decimal,
            balance_wei,
        )
        return _set_resolved_amount(intent, amount_decimal)

    # -----------------------------------------------------------------------
    # PROTOCOL DEBT BALANCE resolution (repay)
    # -----------------------------------------------------------------------
    if category == AmountResolutionCategory.PROTOCOL_DEBT:
        reader = get_reader_for_protocol(protocol_lower)

        if reader is None:
            logger.warning(
                "No balance reader for protocol '%s' — amount='all' for repay will be converted to repay_full=True",
                protocol_lower,
            )
            return _set_repay_full(intent)

        token_address = _resolve_token_address(token, chain)
        if not token_address:
            logger.warning(
                "Cannot resolve token '%s' on %s for debt balance query — falling back to repay_full=True",
                token,
                chain,
            )
            return _set_repay_full(intent)

        balance_wei = reader.get_debt_balance(
            chain=chain,
            token_address=token_address,
            wallet=wallet_address,
            protocol=protocol_lower,
            market_id=market_id,
            gateway_client=gateway_client,
        )

        if balance_wei is None:
            logger.info(
                "Debt balance query returned None for %s/%s — using repay_full=True",
                protocol_lower,
                token,
            )
            return _set_repay_full(intent)

        if balance_wei <= 0:
            logger.info("Protocol debt balance is 0 for %s/%s — nothing to repay", protocol_lower, token)
            return _set_repay_full(intent)

        try:
            decimals = _get_token_decimals(token, chain)
        except Exception:
            logger.warning(
                "Cannot determine decimals for %s on %s — falling back to repay_full=True",
                token,
                chain,
            )
            return _set_repay_full(intent)
        amount_decimal = Decimal(balance_wei) / Decimal(10**decimals)
        logger.info(
            "Resolved repay amount='all' for %s/%s: %s (from %d wei)",
            protocol_lower,
            token,
            amount_decimal,
            balance_wei,
        )
        return _set_resolved_amount(intent, amount_decimal)

    return intent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_token_address(token: str, chain: str) -> str | None:
    """Resolve a token symbol or address to an address."""
    try:
        from ..data.tokens import get_token_resolver
        from ..data.tokens.resolver import TokenNotFoundError

        resolver = get_token_resolver()
        resolved = resolver.resolve(token, chain)
        return resolved.address
    except (ImportError, TokenNotFoundError):
        # If the token is already an address, use it directly
        if token.startswith("0x") and len(token) == 42:
            return token
        return None
    except Exception:
        logger.debug("Unexpected error resolving token '%s' on %s", token, chain, exc_info=True)
        if token.startswith("0x") and len(token) == 42:
            return token
        return None


def _get_token_decimals(token: str, chain: str) -> int:
    """Get token decimals. Raises if decimals are unknown.

    Per codebase guidelines: never default to 18 decimals.
    The caller should handle the exception (fall back to withdraw_all).
    """
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()
    return resolver.get_decimals(chain, token)


def _set_resolved_amount(intent: Any, amount: Decimal) -> Any:
    """Set a resolved concrete amount on an intent."""
    try:
        from . import Intent

        return Intent.set_resolved_amount(intent, amount)
    except (ImportError, AttributeError, TypeError) as e:
        logger.debug("Intent.set_resolved_amount failed: %s, trying model_copy", e)
        if hasattr(intent, "model_copy"):
            return intent.model_copy(update={"amount": amount})
        return intent


def _set_withdraw_all(intent: Any) -> Any:
    """Convert amount='all' to withdraw_all=True."""
    try:
        if hasattr(intent, "model_copy"):
            return intent.model_copy(update={"withdraw_all": True, "amount": Decimal("0")})
    except Exception as e:
        logger.warning("Failed to set withdraw_all on intent: %s", e)
    return intent


def _set_repay_full(intent: Any) -> Any:
    """Convert amount='all' to repay_full=True."""
    try:
        if hasattr(intent, "model_copy"):
            return intent.model_copy(update={"repay_full": True, "amount": Decimal("0")})
    except Exception as e:
        logger.warning("Failed to set repay_full on intent: %s", e)
    return intent
