"""Gateway-clean per-protocol on-chain balance readers.

This module owns the ``ProtocolBalanceReader`` abstraction, the concrete
per-protocol readers, and the ``get_reader_for_protocol`` registry lookup.
It is deliberately **gateway-boundary clean**: its only outbound data path is
the gateway gRPC client threaded in by the caller (``eth_call`` /
``LendingPositionReader``), and it imports *nothing* from the heavyweight
intent-compiler / execution tree.

Why it lives apart from ``amount_resolver``: ``amount_resolver`` resolves
``amount="all"`` and therefore imports the ``Intent`` vocabulary
(``from . import Intent``), which transitively pulls the compiler and execution
egress modules into its import closure. The balance readers need none of that —
they only read on-chain positions. Keeping them in their own module lets
gateway-clean entry points (``MarketSnapshot``, the pool-history data layer)
consume the reader registry without dragging the egress tree into their
import closure (the closure guard in
``tests/framework/data/test_pool_history_source_inspection.py``).

These readers were extracted verbatim from ``amount_resolver`` (VIB-2537);
``amount_resolver`` now re-exports them for backward compatibility.
Relocation rationale: VIB-5468 / VIB-5484.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger("almanak.framework.intents.balance_readers")


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
    _BASE_TOKEN_SELECTOR = "0xc55dae63"  # baseToken()

    @property
    def supported_protocols(self) -> list[str]:
        return ["compound_v3"]

    def _get_comet_address(self, chain: str, market_id: str | None) -> str | None:
        from almanak.connectors._strategy_base.address_registry import AddressRegistry

        markets = AddressRegistry.addresses_for("compound_v3", chain)
        if not markets:
            # Unsupported/unconfigured chain — the connector publishes no Comet
            # table for it. Return None so the caller falls back to withdraw_all
            # rather than guessing an address.
            return None
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

    def _base_token_unconfirmed(
        self, chain: str, token_address: str, market_id: str | None, gateway_client: Any
    ) -> bool:
        """True when ``token_address`` CANNOT be positively confirmed as the comet's base asset.

        Comet's ``balanceOf()`` / ``borrowBalanceOf()`` report the BASE-asset
        position only (collateral lives behind ``collateralBalanceOf(account, asset)``).
        Sizing a read off them for a collateral token — or when we cannot prove the
        token IS the base — would drive the wrong amount. **Fail closed**: only a
        positively-confirmed base token (``baseToken()`` read succeeds and matches)
        returns False (safe to read). Every uncertainty — no token, no comet,
        ``baseToken()`` unreadable (RPC fault), or a mismatch — returns True →
        callers return ``None`` (unmeasured, Empty≠Zero) rather than a base-asset
        figure for a non-base leg.
        """
        if not token_address:
            return True
        comet = self._get_comet_address(chain, market_id)
        if not comet:
            return True
        raw = self._eth_call(gateway_client, chain, comet, self._BASE_TOKEN_SELECTOR)
        if not raw:
            return True
        base = "0x" + raw.replace("0x", "").zfill(64)[-40:]
        return base.lower() != token_address.lower()

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
        # Comet.balanceOf reports the BASE-asset supply only; read it ONLY for a
        # positively-confirmed base token — otherwise unmeasured (None), never the
        # wrong base balance for a collateral leg or an unverifiable base.
        if self._base_token_unconfirmed(chain, token_address, market_id, gateway_client):
            return None
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
        # Compound V3 debt is ALWAYS the base asset; read borrowBalanceOf ONLY for a
        # positively-confirmed base token — otherwise unmeasured (None), never the
        # base debt figure for a non-base/unverifiable token.
        if self._base_token_unconfirmed(chain, token_address, market_id, gateway_client):
            return None
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
