"""Receipt parser for Fluid DEX LP (SmartLending, Phase 4 / VIB-5032).

SmartLending LP is fungible (ERC-20 shares, no NFT, no tick range). The money
path is decoded from plain ERC-20 ``Transfer`` events (validation report P0.7):

* OPEN: wrapper share mint ``Transfer(0x0 → wallet)`` identifies the wrapper +
  wallet; the two deposited legs are ``Transfer(wallet → …)`` of token0/token1.
* CLOSE: wrapper share burn ``Transfer(wallet → 0x0)``; the returned legs are
  ``Transfer(… → wallet)`` of token0/token1.

Fungible discipline (curve/aerodrome-classic precedent): ``LPOpenData.position_id``
is ``0`` and tick/liquidity fields are ``None``; ``LPCloseData.fees0/fees1`` are
``None`` (fees auto-compound into the share price — Empty ≠ Zero, never ``0``).
``extract_position_id`` returns the wrapper address (Solidly LP-id semantics).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from almanak.connectors._fluid_core.addresses import FLUID_DEX_LP_NATIVE_SENTINEL, FLUID_SMARTLENDING_MARKETS
from almanak.connectors._strategy_base.base.receipt_parser import BaseReceiptParser

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData

logger = logging.getLogger(__name__)

# ERC-20 Transfer(address indexed from, address indexed to, uint256 value).
_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ZERO_ADDR = "0x0000000000000000000000000000000000000000"
#: Native-ETH sentinel as it appears in a wrapper's token0/token1 slot. A native
#: leg rides as ``msg.value`` and emits NO ERC-20 Transfer, so the log scan
#: CANNOT measure it — that leg is left ``None`` (honest "unmeasured", Empty ≠
#: Zero) for the runner's native-balance-bracket capture to fill at ledger-build
#: time (VIB-5121). A fabricated ``0`` here is the exact money bug VIB-5032
#: refused to ship.
_NATIVE_SENTINEL = FLUID_DEX_LP_NATIVE_SENTINEL.lower()

# Flatten the wrapper universe once: lowercased wrapper -> market entry.
# Lowercase the key explicitly: ``_transfers()`` lowercases every emitter before
# the ``_WRAPPERS.get(emitter)`` lookup, so a checksum/mixed-case wrapper key
# would be unreachable. FLUID_SMARTLENDING_MARKETS already lowercases its keys,
# but this guards against a future source row that is not pre-lowercased.
_WRAPPERS: dict[str, dict[str, Any]] = {
    str(wrapper).lower(): entry for rows in FLUID_SMARTLENDING_MARKETS.values() for wrapper, entry in rows.items()
}


def _norm(addr: Any) -> str:
    if isinstance(addr, bytes):
        return "0x" + addr.hex().lower()
    return str(addr).lower() if addr else ""


def _topic_addr(topic: Any) -> str:
    """Decode an indexed-address topic (last 20 bytes) to a lowercased hex address."""
    h = _norm(topic)
    if h.startswith("0x"):
        h = h[2:]
    h = h.rjust(64, "0")
    return "0x" + h[-40:]


def _data_uint(data: Any) -> int:
    h = _norm(data)
    if h.startswith("0x"):
        h = h[2:]
    return int(h[:64], 16) if h else 0


class FluidDexLpReceiptParser(BaseReceiptParser[dict, dict]):
    """Fungible-LP receipt extractor for Fluid SmartLending wrappers."""

    SUPPORTED_EXTRACTIONS: frozenset[str] = frozenset({"lp_open_data", "lp_close_data", "position_id"})

    def __init__(self, chain: str = "arbitrum") -> None:
        # The receipt registry constructs every parser as ``parser_class(chain=...)``
        # (receipt_registry.py — ``parser_class(**kwargs)``); accept it to match the
        # sibling ``FluidReceiptParser`` contract. The Transfer-log scan is
        # chain-agnostic (wrapper addresses are globally unique), but we store the
        # chain for parity and future per-chain scoping.
        super().__init__(registry=None, known_topics={_TRANSFER_TOPIC0})
        self.chain = chain.lower()

    # -- BaseReceiptParser abstract hooks (unused — extract_* scan directly) --

    def _decode_log_data(self, event_name, topics, data, contract_address):  # noqa: ANN001, D102
        return {}

    def _create_event(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102
        return None

    def _build_result(self, events, receipt, tx_hash, block_number, tx_success, **kwargs):  # noqa: ANN001, D102
        return {"success": tx_success, "transaction_hash": tx_hash, "block_number": block_number}

    # -- log scan -----------------------------------------------------------

    def _transfers(self, receipt: dict[str, Any]) -> list[tuple[str, str, str, int]]:
        """Return [(emitter, from, to, amount)] for every ERC-20 Transfer log."""
        out: list[tuple[str, str, str, int]] = []
        for log in receipt.get("logs", []) or []:
            topics = log.get("topics", []) or []
            if len(topics) < 3 or _norm(topics[0]) != _TRANSFER_TOPIC0:
                continue
            out.append(
                (
                    _norm(log.get("address", "")),
                    _topic_addr(topics[1]),
                    _topic_addr(topics[2]),
                    _data_uint(log.get("data", "")),
                )
            )
        return out

    def _identify(self, receipt: dict[str, Any], *, opening: bool) -> tuple[str, str, dict[str, Any]] | None:
        """Find (wrapper, wallet, market_entry) from the share mint/burn Transfer."""
        for emitter, frm, to, _amt in self._transfers(receipt):
            entry = _WRAPPERS.get(emitter)
            if entry is None:
                continue
            if opening and frm == _ZERO_ADDR:
                return emitter, to, entry  # mint -> wallet is `to`
            if not opening and to == _ZERO_ADDR:
                return emitter, frm, entry  # burn -> wallet is `from`
        return None

    # -- extraction contract ------------------------------------------------

    def extract_position_id(self, receipt: dict[str, Any]) -> str | None:
        ident = self._identify(receipt, opening=True) or self._identify(receipt, opening=False)
        return ident[0] if ident else None

    def extract_lp_open_data(self, receipt: dict[str, Any]) -> LPOpenData | None:
        from almanak.framework.execution.extracted_data import LPOpenData

        ident = self._identify(receipt, opening=True)
        if ident is None:
            return None
        wrapper, wallet, entry = ident
        token0 = str(entry["token0"]).lower()
        token1 = str(entry["token1"]).lower()
        native0 = token0 == _NATIVE_SENTINEL
        native1 = token1 == _NATIVE_SENTINEL
        # SINGLE-INTENT receipt assumption: the wallet's net token0/token1 OUTflow
        # IS the deposit. We do NOT restrict the counterparty — Fluid routes the
        # legs through the Liquidity Layer (NOT the wrapper/DEX), so a
        # ``to in {wrapper, dex}`` allowlist would wrongly drop the real leg
        # (verified: the on-chain intent test fails under that restriction). This
        # is safe because ``fluid_dex_lp`` is ``no_zodiac`` — one intent per tx, so
        # the receipt's logs are already intent-scoped and carry no unrelated
        # token0/token1 transfers. If bundled (multi-intent) execution ever lands,
        # the correct fix is intent-scoped log routing at the runner, not a
        # connector-side counterparty allowlist (Fluid's internal routing is not
        # enumerable here).
        #
        # Empty != Zero (CLAUDE.md §Accounting): the Transfer scan MEASURES every
        # ERC-20 leg, so an unfunded ERC-20 leg is a measured ``0``. A NATIVE leg
        # (msg.value, no Transfer) is UNMEASURABLE from logs — seed it ``None``
        # (honest unmeasured) so the runner's native-balance-bracket capture fills
        # it at ledger-build time (VIB-5121). A fabricated ``0`` would be the money
        # bug VIB-5032 refused to ship.
        amount0: int | None = None if native0 else 0
        amount1: int | None = None if native1 else 0
        shares = 0
        for emitter, frm, to, amt in self._transfers(receipt):
            if not native0 and frm == wallet and emitter == token0:
                amount0 = (amount0 or 0) + amt
            elif not native1 and frm == wallet and emitter == token1:
                amount1 = (amount1 or 0) + amt
            elif emitter == wrapper and frm == _ZERO_ADDR and to == wallet:
                shares += amt
        return LPOpenData(
            position_id=0,  # fungible: no NFT id (curve/aerodrome-classic convention)
            tick_lower=None,
            tick_upper=None,
            liquidity=shares,  # share balance minted
            amount0=amount0,
            amount1=amount1,
            pool_address=wrapper,
            # VIB-4426 mechanism: stamp the token ADDRESSES so the LP accounting
            # handler (`_v4_realign_token_pair`) resolves symbols/decimals by
            # address. The fungible position key tails the wrapper address (not
            # the V3 ``<t0>/<t1>/<fee>`` descriptor), so without this the handler
            # cannot scale amount0/amount1 and the typed payload fails validation.
            currency0=token0,
            currency1=token1,
        )

    def extract_lp_close_data(self, receipt: dict[str, Any]) -> LPCloseData | None:
        from almanak.framework.execution.extracted_data import LPCloseData

        ident = self._identify(receipt, opening=False)
        if ident is None:
            return None
        wrapper, wallet, entry = ident
        token0 = str(entry["token0"]).lower()
        token1 = str(entry["token1"]).lower()
        native0 = token0 == _NATIVE_SENTINEL
        native1 = token1 == _NATIVE_SENTINEL
        # SINGLE-INTENT receipt assumption — see extract_lp_open_data: no
        # counterparty restriction (Fluid returns the legs via the Liquidity
        # Layer, not the wrapper/DEX). Safe because fluid_dex_lp is no_zodiac
        # (one intent per tx → intent-scoped logs).
        #
        # Empty != Zero: a NATIVE returned leg (ETH credited via msg.value-style
        # internal call, no ERC-20 Transfer) is UNMEASURABLE from logs — seed it
        # ``None`` so the runner's native-balance-bracket capture fills it
        # (VIB-5121). ERC-20 legs are measured from ``…→wallet`` Transfers.
        amount0: int | None = None if native0 else 0
        amount1: int | None = None if native1 else 0
        shares_burned = 0
        for emitter, frm, to, amt in self._transfers(receipt):
            if not native0 and to == wallet and emitter == token0:
                amount0 = (amount0 or 0) + amt
            elif not native1 and to == wallet and emitter == token1:
                amount1 = (amount1 or 0) + amt
            elif emitter == wrapper and to == _ZERO_ADDR and frm == wallet:
                shares_burned += amt
        return LPCloseData(
            amount0_collected=amount0,
            amount1_collected=amount1,
            fees0=None,  # Empty != Zero — fees auto-compound into share price.
            fees1=None,
            liquidity_removed=shares_burned or None,
            pool_address=wrapper,
            source="collect",
            # VIB-4426 mechanism: token ADDRESSES so the LP handler resolves
            # symbols/decimals by address (the wrapper-tailed position key has no
            # ``<t0>/<t1>/<fee>`` descriptor). See extract_lp_open_data.
            currency0=token0,
            currency1=token1,
        )


__all__ = ["FluidDexLpReceiptParser"]
