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
import math
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger("almanak.framework.intents.balance_readers")


def _gateway_eth_call(gateway_client: Any, chain: str, to: str, data: str) -> str | None:
    """Execute an ``eth_call`` via the gateway RPC channel; ``None`` on any failure.

    The gateway gRPC channel is the ONLY outbound path this module uses. The
    connector supplies the ABI-encoded ``data`` (selector + args); this helper
    never owns a protocol encoding, so the Gateway-boundary discipline holds
    (framework routes the read, connector owns the ABI). Empty ≠ Zero: a failed
    or empty read returns ``None``, never a fabricated result.
    """
    try:
        from almanak.gateway.proto import gateway_pb2

        rpc_stub = getattr(gateway_client, "_rpc_stub", None)
        if rpc_stub is None:
            return None
        # Bound the RPC: a config with ``timeout=None`` (or a non-numeric / ≤0 /
        # non-finite ``inf``/``nan``) value would pass an UNBOUNDED (or invalid)
        # timeout to the gRPC Call and could hang a teardown read. Coerce anything
        # that is not a positive FINITE number to a finite default (CodeRabbit). A
        # disconnected client still fail-closes: the Call raises and the outer
        # ``except`` returns None (unmeasured → the guard degrades conservatively),
        # so no explicit is_connected pre-check is needed.
        _cfg_timeout = getattr(getattr(gateway_client, "config", None), "timeout", None)
        timeout = (
            _cfg_timeout
            if isinstance(_cfg_timeout, int | float)
            and not isinstance(_cfg_timeout, bool)
            and math.isfinite(_cfg_timeout)
            and _cfg_timeout > 0
            else 10
        )
        params_json = json.dumps([{"to": to, "data": data}, "latest"])
        response = rpc_stub.Call(
            gateway_pb2.RpcRequest(chain=chain, method="eth_call", params=params_json),
            timeout=timeout,
        )
        if not response.success:
            return None
        if response.result:
            # Empty ≠ Zero, and a well-formed eth_call result is a hex string;
            # a malformed node reply could decode to bool/int/dict/list, so
            # only return an actual string (else treat as unmeasured → None).
            res = json.loads(response.result)
            return res if isinstance(res, str) else None
        return None
    except Exception:
        logger.debug("gateway eth_call failed (to=%s chain=%s)", to, chain, exc_info=True)
        return None


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

    def get_reserve_position(
        self,
        chain: str,
        token_address: str,
        wallet: str,
        *,
        protocol: str | None = None,
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> tuple[int | None, int | None]:
        """Raw per-reserve ``(supply_wei, debt_wei)`` for the teardown keep-decision.

        Distinct from :meth:`get_supply_balance` / :meth:`get_debt_balance`, which
        the ``amount="all"`` resolver consumes (and which Morpho deliberately leaves
        ``None`` to force its shares-based ``withdraw_all`` path — never overload
        those). This read is the price-independent on-chain position read the
        lending teardown guard consults via
        :meth:`~almanak.framework.market.snapshot.MarketSnapshot.lending_position_balances`.

        The default composes the two balance readers, so Aave / Spark / Compound V3
        keep their existing (VIB-5468 / VIB-5484) behaviour unchanged. A protocol
        whose flat balance needs a bespoke raw read (Morpho Blue's
        ``position(id, user)``) overrides this. Empty ≠ Zero: either leg is ``None``
        when unmeasured.
        """
        return (
            self.get_supply_balance(
                chain, token_address, wallet, protocol=protocol, market_id=market_id, gateway_client=gateway_client
            ),
            self.get_debt_balance(
                chain, token_address, wallet, protocol=protocol, market_id=market_id, gateway_client=gateway_client
            ),
        )


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
        """Make an eth_call via gateway RPC (shared gateway-routed helper)."""
        return _gateway_eth_call(gateway_client, chain, to, data)

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

    def get_reserve_position(
        self,
        chain: str,
        token_address: str,  # noqa: ARG002 — Morpho position is keyed by (market_id, user), not the token
        wallet: str,
        *,
        protocol: str | None = None,  # noqa: ARG002 — single-protocol reader; accepted for interface symmetry
        market_id: str | None = None,
        gateway_client: Any = None,
    ) -> tuple[int | None, int | None]:
        """Raw ``(collateral_wei, borrow_shares)`` from ``position(marketId, user)`` (VIB-5418).

        Overrides the default (which composes the shares-blind supply/debt stubs
        above and would return ``(None, None)``). Morpho collateral is a raw
        ``uint128`` (NOT shares) and IS directly readable; ``borrow_shares == 0``
        iff the whole-position debt is zero because Morpho markets are ISOLATED
        (exactly one collateral + one loan token). The teardown keep-decision only
        needs "collateral present + no debt", so no shares→assets conversion /
        ``market(id)`` totals are needed (avoids rounding). ABI / selector are
        connector-owned (``lending_read_base``); the gateway routes the read.

        This does NOT change ``get_supply_balance`` / ``get_debt_balance`` (still
        ``None``), so the ``amount="all"`` resolver keeps Morpho's shares-based
        ``withdraw_all`` path (no regression). Empty ≠ Zero: ``(None, None)`` when
        unmeasured (no client, no market id, no Morpho address on chain, or a
        short / failed read).
        """
        if gateway_client is None or not market_id:
            return (None, None)
        from almanak.connectors._strategy_base.address_registry import AddressRegistry
        from almanak.connectors._strategy_base.lending_read_base import (
            build_morpho_position_calldata,
            decode_morpho_position,
        )

        morpho = AddressRegistry.resolve_contract_address("morpho_blue", chain, ("morpho",))
        if not morpho:
            return (None, None)
        result_hex = _gateway_eth_call(gateway_client, chain, morpho, build_morpho_position_calldata(market_id, wallet))
        if not result_hex:
            return (None, None)
        decoded = decode_morpho_position(result_hex)
        if decoded is None:
            return (None, None)
        collateral_raw, borrow_shares = decoded
        return (collateral_raw, borrow_shares)


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
