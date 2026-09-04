"""Gateway-side verified market resolution for Morpho Blue (VIB-5985).

Implements :class:`GatewayLendingMarketDiscoveryCapability` for Morpho Blue:

* ``list_morpho_markets`` — OFFLINE candidate listing from the curated
  ``MORPHO_MARKETS`` catalogue. Token filters are resolved to addresses before
  matching (symbols are spoofable). Returns EVERY match; never ranks / picks.
* ``verify_morpho_market`` — ON-CHAIN verification of one exact market id. Reads
  ``idToMarketParams(id)`` through a gateway-supplied ``eth_call`` transport,
  recomputes the Morpho market id from the returned params
  (``keccak256(abi.encode(loanToken, collateralToken, oracle, irm, lltv))``) and
  compares it to the requested id. A mismatch is a loud
  :class:`LendingMarketVerificationError`; a non-existent market returns ``None``.
  On a verified match it also reads ``market(id)`` for the market's raw
  loan-token liquidity (``total_supply_assets`` / ``total_borrow_assets``,
  ALM-3515) — a best-effort supplementary read that never fails the
  verification itself; see :func:`_read_market_liquidity`.

Morpho is permissionless — anyone can deploy a same-pair market with a hostile
oracle/IRM — so a market id is only trustworthy after this recompute-and-compare.
The curated catalogue is a candidate source, never proof of anything.

This module lives under ``almanak/connectors/morpho_blue/gateway/`` and performs
NO network egress of its own: the ``eth_call`` transport is injected by the
gateway servicer (the gateway layer owns the socket, per AGENTS.md §Gateway
boundary). ``web3.py`` is used only for ABI-encoding / checksums / keccak, which
the gateway boundary explicitly permits.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from eth_abi import encode as abi_encode
from web3 import Web3

from almanak.connectors._base.gateway_capabilities import (
    LENDING_MARKET_KIND_ISOLATED_PAIR,
    LENDING_MARKET_SOURCE_CURATED_CATALOG,
    LENDING_MARKET_SOURCE_ONCHAIN_VERIFY,
    LendingMarketRecord,
    LendingMarketVerificationError,
)

from ..addresses import MORPHO_BLUE, MORPHO_BLUE_TOKENS, MORPHO_MARKETS

logger = logging.getLogger(__name__)

# idToMarketParams(bytes32) selector — keccak256("idToMarketParams(bytes32)")[:4].
# Mirrors ``MorphoBlueSDK.ID_TO_MARKET_PARAMS_SELECTOR`` (sdk.py) verbatim.
_ID_TO_MARKET_PARAMS_SELECTOR = "0x2c3c9157"
# market(bytes32) selector — keccak256("market(bytes32)")[:4]. Mirrors
# ``MorphoBlueGatewayConnector``'s rate-history reader
# (``_MORPHO_BLUE_MARKET_SELECTOR`` in gateway/provider.py); duplicated here
# rather than imported because provider.py imports this module (`from . import
# market_discovery`), so importing back from provider.py would be circular.
_MARKET_SELECTOR = "0x5c60e39a"
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_LLTV_SCALE = 10**18  # LLTV is 1e18-scaled (0.86e18 = 86%).
# Market struct is six uint128 words: totalSupplyAssets, totalSupplyShares,
# totalBorrowAssets, totalBorrowShares, lastUpdate, fee.
_MARKET_STRUCT_WORDS = 6
_MARKET_STRUCT_HEX_LEN = _MARKET_STRUCT_WORDS * 64
# The identity read (idToMarketParams) and this supplementary liquidity read
# share one eth_call transport whose per-call server-side budget
# (GATEWAY_PT_RPC_TIMEOUT_SECONDS, pt_rpc_adapter.py) equals the CLIENT's
# GetLendingMarket gRPC deadline (_GATEWAY_RPC_TIMEOUT_SECONDS,
# snapshot.py). Two uncapped sequential calls can together exceed that single
# client deadline, turning a slow-but-successful identity verification into a
# DEADLINE_EXCEEDED -- which would fail a strategy's boot gate, contradicting
# this read's "never fails the verification itself" contract. Bounding this
# call alone shrinks that window (a sub-8s liquidity read can no longer be
# the cause) but does not eliminate it -- an identity read alone taking
# >22s under the shared 30s-per-call budget can still exceed the combined
# deadline; closing that fully needs a shared elapsed budget across both
# calls, which is a larger change than this bound.
_LIQUIDITY_READ_TIMEOUT_SECONDS = 8.0

# Morpho market kind is always an isolated collateral↔loan pair.
_MORPHO_KIND = LENDING_MARKET_KIND_ISOLATED_PAIR


def _lltv_to_bps(lltv: int) -> int:
    """Convert a 1e18-scaled LLTV to basis points (0.915e18 → 9150 bps).

    Integer arithmetic only — every real Morpho LLTV is a whole number of bps,
    so the floor is exact; a pathological non-bps value floors deterministically
    rather than importing float error.
    """
    return (int(lltv) * 10_000) // _LLTV_SCALE


def _normalize_market_id(market_id: str) -> str:
    """Lowercase, 0x-prefix, zero-pad a market id to 66 chars (mirrors SDK)."""
    mid = market_id.strip()
    if not mid.startswith("0x") and not mid.startswith("0X"):
        mid = "0x" + mid
    mid = "0x" + mid[2:].lower()
    if len(mid) != 66:
        mid = "0x" + mid[2:].zfill(64)
    return mid


def recompute_morpho_market_id(
    *,
    loan_token: str,
    collateral_token: str,
    oracle: str,
    irm: str,
    lltv: int,
) -> str:
    """Recompute the Morpho market id from its params.

    ``id = keccak256(abi.encode(loanToken, collateralToken, oracle, irm, lltv))``
    with the params tuple in Morpho's canonical field order. All five members are
    static ABI types, so the single-tuple encoding equals the flat encoding — we
    use the tuple form to match the on-chain ``MarketParams`` struct layout and
    the existing per-chain recompute tests exactly.
    """
    encoded = abi_encode(
        ["(address,address,address,address,uint256)"],
        [
            (
                Web3.to_checksum_address(loan_token),
                Web3.to_checksum_address(collateral_token),
                Web3.to_checksum_address(oracle),
                Web3.to_checksum_address(irm),
                int(lltv),
            )
        ],
    )
    return "0x" + Web3.keccak(encoded).hex()


def _resolve_token_filter(chain: str, value: str | None) -> str | None:
    """Resolve a symbol-or-address token filter to a lowercased address.

    Symbols are spoofable, so a filter is only meaningful once resolved to an
    address. Accepts a 0x-address verbatim (lowercased); otherwise looks the
    value up as a symbol in ``MORPHO_BLUE_TOKENS[chain]`` (case-insensitive).
    Returns ``None`` for an empty filter. Raises ``ValueError`` for a symbol that
    resolves to nothing on this chain — a filter the connector cannot honour is
    an explicit caller error, not a silently-empty match.
    """
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    if v.startswith("0x") and len(v) == 42:
        return v.lower()
    tokens = {sym.lower(): addr for sym, addr in MORPHO_BLUE_TOKENS.get(chain, {}).items()}
    resolved = tokens.get(v.lower())
    if resolved is None:
        raise ValueError(
            f"cannot resolve token filter {value!r} on chain {chain!r} "
            f"(known symbols: {sorted(MORPHO_BLUE_TOKENS.get(chain, {}))})"
        )
    return resolved.lower()


def _reverse_symbol(chain: str, address: str) -> str:
    """Best-effort address→symbol using the connector's token catalogue ("" if unknown)."""
    addr = address.lower()
    for sym, tok_addr in MORPHO_BLUE_TOKENS.get(chain, {}).items():
        if tok_addr.lower() == addr:
            return sym
    return ""


def _record_from_catalog_entry(
    *,
    chain: str,
    market_id: str,
    info: dict[str, Any],
    verified: bool,
    source: str,
) -> LendingMarketRecord:
    """Build a ``LendingMarketRecord`` from a curated ``MORPHO_MARKETS`` entry."""
    return LendingMarketRecord(
        kind=_MORPHO_KIND,
        protocol="morpho_blue",
        chain=chain,
        market_id=_normalize_market_id(market_id),
        collateral_token=str(info.get("collateral_token_address", "")).lower(),
        collateral_symbol=str(info.get("collateral_token", "")),
        loan_token=str(info.get("loan_token_address", "")).lower(),
        loan_symbol=str(info.get("loan_token", "")),
        lltv_bps=_lltv_to_bps(int(info["lltv"])),
        oracle=str(info.get("oracle", "")).lower(),
        irm=str(info.get("irm", "")).lower(),
        verified=verified,
        source=source,
    )


def morpho_discovery_chains() -> frozenset[str]:
    """Chains for which Morpho markets are resolvable (= curated-catalogue keys)."""
    return frozenset(MORPHO_MARKETS.keys())


def list_morpho_markets(
    *,
    chain: str,
    collateral_token: str | None = None,
    loan_token: str | None = None,
    lltv_bps: int | None = None,
) -> list[LendingMarketRecord]:
    """Offline candidate listing from ``MORPHO_MARKETS`` (no egress).

    Filters are ANDed. Token filters are address-resolved before matching.
    Returns EVERY match in deterministic market-id order; never ranks or picks.
    Candidates are ``verified=False`` / ``source="curated_catalog"``.
    """
    markets = MORPHO_MARKETS.get(chain, {})
    want_collateral = _resolve_token_filter(chain, collateral_token)
    want_loan = _resolve_token_filter(chain, loan_token)

    out: list[LendingMarketRecord] = []
    for market_id in sorted(markets):
        info = markets[market_id]
        if want_collateral is not None and str(info.get("collateral_token_address", "")).lower() != want_collateral:
            continue
        if want_loan is not None and str(info.get("loan_token_address", "")).lower() != want_loan:
            continue
        if lltv_bps is not None and _lltv_to_bps(int(info["lltv"])) != int(lltv_bps):
            continue
        out.append(
            _record_from_catalog_entry(
                chain=chain,
                market_id=market_id,
                info=info,
                verified=False,
                source=LENDING_MARKET_SOURCE_CURATED_CATALOG,
            )
        )
    return out


def _decode_address_word(word_hex: str) -> str:
    """Decode a 32-byte ABI word into a lowercased 0x address (last 20 bytes)."""
    return "0x" + word_hex[-40:].lower()


async def _read_market_liquidity(
    *,
    chain: str,
    morpho_addr: str,
    market_id: str,
    eth_call: Callable[[str, str], Awaitable[str]],
) -> tuple[str, str]:
    """Read ``market(bytes32)`` and return ``(total_supply_assets, total_borrow_assets)``.

    Raw loan-token base units, decimal-as-string (ALM-3515). This is a
    best-effort, SUPPLEMENTARY read layered on top of identity verification —
    a transport failure or a short/malformed response returns ``("", "")``
    (Empty ≠ Zero: unmeasured, never a fabricated ``"0"``) rather than raising,
    so a liquidity-read hiccup never turns a successfully verified market id
    into a hard failure. A well-formed struct that is genuinely all zero (e.g.
    a dormant market) is a measured zero and returns ``("0", "0")``.
    """
    try:
        calldata = _MARKET_SELECTOR + market_id[2:]
        raw = await asyncio.wait_for(eth_call(morpho_addr, calldata), timeout=_LIQUIDITY_READ_TIMEOUT_SECONDS)
    except Exception:
        logger.warning("Morpho Blue market(%s) liquidity read failed on %s", market_id, chain, exc_info=True)
        return "", ""
    if not isinstance(raw, str):
        # The injected eth_call is typed to return str; a non-conforming
        # implementation returning None/non-str must still read as a
        # malformed response, never crash this "never fails identity
        # verification" best-effort read.
        logger.warning("Morpho Blue market(%s) on %s returned a non-string result (%r)", market_id, chain, type(raw))
        return "", ""

    result = raw[2:] if raw.startswith(("0x", "0X")) else raw
    if len(result) < _MARKET_STRUCT_HEX_LEN:
        logger.warning(
            "Morpho Blue market(%s) on %s returned a short payload (%d hex chars, need %d)",
            market_id,
            chain,
            len(result),
            _MARKET_STRUCT_HEX_LEN,
        )
        return "", ""

    try:
        # Each field is a uint128 ABI-encoded into a 32-byte word -- a
        # correctly-encoded response always zero-pads the upper 16 bytes. A
        # nonzero upper half means either a malformed/truncated response or a
        # struct layout this reader doesn't understand -- never silently
        # truncate it into a wrong (and possibly enormous) liquidity number.
        # This catches non-zero-padding corruption specifically; unlike
        # idToMarketParams, there is no recomputable ground truth for a
        # liquidity value, so a compromised RPC returning a correctly-padded
        # but simply wrong number is not detectable here.
        if int(result[0:32], 16) != 0 or int(result[128:160], 16) != 0:
            logger.warning("Morpho Blue market(%s) on %s returned a non-zero-padded uint128 word", market_id, chain)
            return "", ""
        total_supply_assets = int(result[32:64], 16)
        total_borrow_assets = int(result[160:192], 16)
    except ValueError:
        logger.warning("Morpho Blue market(%s) on %s returned an undecodable payload", market_id, chain)
        return "", ""

    return str(total_supply_assets), str(total_borrow_assets)


async def verify_morpho_market(
    *,
    chain: str,
    market_id: str,
    eth_call: Callable[[str, str], Awaitable[str]],
) -> LendingMarketRecord | None:
    """Verify one Morpho market id on-chain via the injected ``eth_call``.

    ``eth_call(to_address, calldata_hex) -> result_hex``. Reads
    ``idToMarketParams(id)``, recomputes the id from the returned params and
    compares. Returns a ``verified=True`` record on a match, ``None`` for a
    non-existent market, and raises :class:`LendingMarketVerificationError` on a
    recompute mismatch.
    """
    morpho_addr = MORPHO_BLUE.get(chain, {}).get("morpho")
    if not morpho_addr:
        raise ValueError(f"Morpho Blue is not deployed / registered on chain {chain!r}")

    mid = _normalize_market_id(market_id)
    calldata = _ID_TO_MARKET_PARAMS_SELECTOR + mid[2:]
    raw = await eth_call(morpho_addr, calldata)

    result = raw[2:] if raw.startswith(("0x", "0X")) else raw
    if len(result) < 320:
        raise LendingMarketVerificationError(
            f"idToMarketParams({mid}) on {chain} returned a short payload ({len(result)} hex chars, need 320)"
        )

    loan_token = _decode_address_word(result[0:64])
    collateral_token = _decode_address_word(result[64:128])
    oracle = _decode_address_word(result[128:192])
    irm = _decode_address_word(result[192:256])
    lltv = int(result[256:320], 16)

    # Zero loan token = market id was never created on-chain (not-found).
    if loan_token == _ZERO_ADDRESS:
        return None

    recomputed = recompute_morpho_market_id(
        loan_token=loan_token,
        collateral_token=collateral_token,
        oracle=oracle,
        irm=irm,
        lltv=lltv,
    )
    if recomputed.lower() != mid.lower():
        raise LendingMarketVerificationError(
            f"Morpho market id verification failed on {chain}: requested {mid} "
            f"but on-chain params recompute to {recomputed} — refusing to return "
            f"an unverified market (possible compromised RPC or forged id)"
        )

    # Enrich symbols: prefer the curated catalogue entry, else reverse-lookup the
    # token catalogue; leave "" when unknown (Empty ≠ Zero — never fabricated).
    catalog = MORPHO_MARKETS.get(chain, {}).get(mid)
    loan_symbol = str(catalog.get("loan_token", "")) if catalog else _reverse_symbol(chain, loan_token)
    collateral_symbol = (
        str(catalog.get("collateral_token", "")) if catalog else _reverse_symbol(chain, collateral_token)
    )

    total_supply_assets, total_borrow_assets = await _read_market_liquidity(
        chain=chain,
        morpho_addr=morpho_addr,
        market_id=mid,
        eth_call=eth_call,
    )

    return LendingMarketRecord(
        kind=_MORPHO_KIND,
        protocol="morpho_blue",
        chain=chain,
        market_id=mid,
        collateral_token=collateral_token,
        collateral_symbol=collateral_symbol,
        loan_token=loan_token,
        loan_symbol=loan_symbol,
        lltv_bps=_lltv_to_bps(lltv),
        oracle=oracle,
        irm=irm,
        verified=True,
        source=LENDING_MARKET_SOURCE_ONCHAIN_VERIFY,
        total_supply_assets=total_supply_assets,
        total_borrow_assets=total_borrow_assets,
    )
