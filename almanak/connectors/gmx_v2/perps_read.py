"""GMX V2 perp-position read + valuation, published as a ``PerpsReadSpec``.

This module is the GMX V2 implementation of the strategy-side perps-read seam
(:mod:`almanak.connectors._strategy_base.perps_read_base`). It folds three pieces
of GMX-specific knowledge that previously lived in the framework into the
connector, as **pure** data + functions (no gateway, no egress):

* ``_build_gmx_calls`` — plans the single on-chain ``Reader.getAccountPositions``
  range read (replacing the SDK's web3 ``Contract`` round-trip + the
  ``getAccountPositionCount`` pre-query; the framework reader executes the
  resulting :class:`EthCall` via the gateway).
* ``_reduce_gmx_positions`` — decodes the ``Position.Props[]`` ABI return into
  :class:`PerpsPositionOnChain`, byte-identical to the SDK's
  ``_parse_raw_positions`` field mapping, and returns a
  :class:`PerpsReadResult` whose ``ok`` flag distinguishes a failed read from an
  empty book (Empty≠Zero).
* ``_gmx_market_metadata`` — the relocated index-token symbol + decimals lookup
  (from the framework portfolio valuer), reading the connector's own
  ``GMX_V2_MARKETS`` / ``_GMX_V2_INDEX_TOKEN_DECIMALS`` tables.
* ``value_perps_position`` — the relocated GMX mark-to-market formula
  (byte-identical to the framework perp valuer).

Gateway-boundary note: ``eth_abi`` / ``eth_utils`` are used here only for ABI
encode/decode and selector derivation (pure utilities) — never to open a
provider. The gateway-routed ``eth_call`` lives in the framework reader.

VIB-4930 (epic VIB-4851).
"""

from __future__ import annotations

import logging
from decimal import Decimal

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import function_signature_to_4byte_selector, to_checksum_address

from almanak.connectors._strategy_base.perps_read_base import (
    EthCall,
    PerpsMarketMeta,
    PerpsPositionOnChain,
    PerpsPositionQuery,
    PerpsPositionValue,
    PerpsReadResult,
    PerpsReadSpec,
)
from almanak.connectors.gmx_v2.adapter import (
    _GMX_V2_INDEX_TOKEN_DECIMALS,
    GMX_V2_MARKETS,
)

logger = logging.getLogger(__name__)

# GMX V2 stores USD values with 30 decimal places (mirrors perps_valuer).
_GMX_USD_DECIMALS = 30
_USD_DIVISOR = Decimal(10**_GMX_USD_DECIMALS)

# Maximum positions to fetch in a single range read. The GMX V2 Reader handles
# ranges gracefully — returns only existing positions within [start, end) — so a
# single ``[0, 100)`` read replaces the SDK's count pre-query (sdk.py:259-261).
_MAX_POSITION_RANGE = 100

# Reader.getAccountPositions(address dataStore, address account, uint256 start,
# uint256 end) -> Position.Props[]. The selector is derived from the signature
# (pure; no provider). Output ABI built from the known struct so a typo in the
# 11-field Numbers tuple can't silently corrupt the decode.
_GET_ACCOUNT_POSITIONS_SIG = "getAccountPositions(address,address,uint256,uint256)"
_GET_ACCOUNT_POSITIONS_SELECTOR = function_signature_to_4byte_selector(_GET_ACCOUNT_POSITIONS_SIG)
_POSITION_NUMBERS = "(" + ",".join(["uint256"] * 11) + ")"  # Position.Numbers (11 uints)
# Position.Props = (Addresses(3 addrs), Numbers(11 uints), Flags(1 bool)).
_POSITION_PROPS = f"((address,address,address),{_POSITION_NUMBERS},(bool))"
_GET_ACCOUNT_POSITIONS_OUTPUT = f"{_POSITION_PROPS}[]"


def _build_gmx_calls(query: PerpsPositionQuery) -> list[EthCall]:
    """Plan the single ``Reader.getAccountPositions(dataStore, account, 0, 100)`` read.

    Targets the resolved ``reader`` contract; the resolved ``data_store`` is the
    first calldata argument. Returns ``[]`` (fail-closed) when either role is
    unresolved — the reducer then yields an ``ok=False`` empty result.
    """
    targets = query.targets or {}
    reader = targets.get("reader")
    data_store = targets.get("data_store")
    if not reader or not data_store:
        return []
    args = abi_encode(
        ["address", "address", "uint256", "uint256"],
        [data_store, query.wallet_address, 0, _MAX_POSITION_RANGE],
    )
    data = "0x" + (_GET_ACCOUNT_POSITIONS_SELECTOR + args).hex()
    return [EthCall(to=reader, data=data)]


def _reduce_gmx_positions(query: PerpsPositionQuery, results: list[str | None]) -> PerpsReadResult:
    """Decode the ``getAccountPositions`` return into active positions.

    Field mapping is byte-identical to ``GMXV2SDK._parse_raw_positions``
    (sdk.py:500-531): ``addresses=(account, market, collateralToken)``,
    ``numbers[0..10]`` the eleven Position.Numbers uints, ``flags[0]=isLong``.
    Only ``numbers`` indices the valuer consumes are surfaced on
    :class:`PerpsPositionOnChain`; the claimable-funding / block fields are
    decoded-but-dropped exactly as the framework reader dropped them.

    Empty≠Zero: a ``None`` blob (the gateway ``eth_call`` failed) or a malformed
    decode yields ``ok=False`` (unmeasured); a successful decode of an empty
    array yields ``ok=True`` with no positions (a measured empty book).
    """
    blob = results[0] if results else None
    if not blob:
        return PerpsReadResult(positions=(), ok=False)
    try:
        raw = bytes.fromhex(blob[2:] if blob[:2].lower() == "0x" else blob)
        decoded = abi_decode([_GET_ACCOUNT_POSITIONS_OUTPUT], raw)[0]
    except Exception:
        logger.debug("Failed to decode GMX getAccountPositions return", exc_info=True)
        return PerpsReadResult(positions=(), ok=False)

    positions: list[PerpsPositionOnChain] = []
    for props in decoded:
        addresses, numbers, flags = props[0], props[1], props[2]
        # ``eth_abi`` returns addresses lower-cased; the legacy web3 contract call
        # returned them EIP-55 checksummed. Checksum here so the relocated decode is
        # byte-identical to ``_parse_raw_positions`` (downstream lower-cases anyway,
        # but a faithful relocation preserves the raw form).
        pos = PerpsPositionOnChain(
            account=to_checksum_address(addresses[0]),
            market=to_checksum_address(addresses[1]),
            collateral_token=to_checksum_address(addresses[2]),
            size_in_usd=numbers[0],  # 30 decimals
            size_in_tokens=numbers[1],  # index token decimals
            collateral_amount=numbers[2],  # collateral token decimals
            is_long=flags[0],
            borrowing_factor=numbers[3],
            funding_fee_amount_per_size=numbers[4],
            increased_at_time=numbers[9],
            decreased_at_time=numbers[10],
            key_prefix="gmx",
        )
        if pos.is_active:  # size_in_usd > 0 — matches the legacy reader's filter
            positions.append(pos)
    return PerpsReadResult(positions=tuple(positions), ok=True)


def _gmx_market_metadata(market_address: str, chain: str) -> PerpsMarketMeta | None:
    """Resolve a GMX market's index-token symbol + decimals for valuation.

    Combines the relocated framework helpers ``_resolve_perps_index_token`` (name
    table ``GMX_V2_MARKETS``) and ``_get_perps_index_decimals`` (decimals table
    ``_GMX_V2_INDEX_TOKEN_DECIMALS``). Both lookups are case-insensitive on the
    market address. Returns ``None`` when the market is unknown **or** its index
    decimals are not catalogued — valuation needs both, and the framework already
    fell back to the strategy value whenever either was missing (preserves the
    ``_get_perps_index_decimals`` ``None`` behaviour, NOT the adapter's default-18).
    """
    addr_lower = market_address.lower()

    symbol: str | None = None
    for name, addr in GMX_V2_MARKETS.get(chain, {}).items():
        if addr.lower() == addr_lower:
            symbol = name.split("/")[0]  # "ETH/USD" -> "ETH"
            break
    if not symbol:
        return None

    for addr, decimals in _GMX_V2_INDEX_TOKEN_DECIMALS.get(chain, {}).items():
        if addr.lower() == addr_lower:
            return PerpsMarketMeta(index_token_symbol=symbol, index_token_decimals=decimals)
    return None


def value_perps_position(
    *,
    size_in_usd: int,
    size_in_tokens: int,
    collateral_amount: int,
    is_long: bool,
    mark_price_usd: Decimal,
    collateral_token_price_usd: Decimal,
    collateral_token_decimals: int,
    index_token_decimals: int,
    pending_funding_fees_usd: Decimal = Decimal("0"),
    pending_borrowing_fees_usd: Decimal = Decimal("0"),
    market: str = "",
) -> PerpsPositionValue:
    """Value a single GMX V2 perpetual position at current market price.

    The GMX V2 mark-to-market formula used by the frontend and keepers. Pure (no
    I/O). Relocated byte-identical from the framework perp valuer; the framework
    now reaches it through ``PerpsReadRegistry.value_position`` rather than
    importing it directly.
    """
    # Convert raw values to human-readable
    size_usd = Decimal(size_in_usd) / _USD_DIVISOR
    tokens = Decimal(size_in_tokens) / Decimal(10**index_token_decimals)
    collateral = Decimal(collateral_amount) / Decimal(10**collateral_token_decimals)

    # Collateral value at current price
    collateral_value = collateral * collateral_token_price_usd

    # Entry price: size_in_usd / size_in_tokens (both raw, result in USD)
    if size_in_tokens > 0:
        entry_price = size_usd / tokens
    else:
        entry_price = Decimal("0")

    # Unrealized PnL: price movement * token quantity
    if tokens > 0 and mark_price_usd > 0:
        if is_long:
            unrealized_pnl = tokens * (mark_price_usd - entry_price)
        else:
            unrealized_pnl = tokens * (entry_price - mark_price_usd)
    else:
        unrealized_pnl = Decimal("0")

    # Pending fees (funding + borrowing)
    pending_fees = pending_funding_fees_usd + pending_borrowing_fees_usd

    # Net value: what the trader would receive if closing now
    net_value = collateral_value + unrealized_pnl - pending_fees

    # Leverage: notional / collateral
    if collateral_value > 0:
        leverage = size_usd / collateral_value
    else:
        leverage = Decimal("0")

    return PerpsPositionValue(
        market=market,
        is_long=is_long,
        size_usd=size_usd,
        collateral_value_usd=collateral_value,
        entry_price_usd=entry_price,
        mark_price_usd=mark_price_usd,
        unrealized_pnl_usd=unrealized_pnl,
        pending_fees_usd=pending_fees,
        net_value_usd=net_value,
        leverage=leverage,
    )


#: GMX V2's published perp-read capability. Read targets are the per-chain
#: ``reader`` (the call target) and ``data_store`` (a calldata arg), both owned by
#: ``gmx_v2/addresses.py``. GMX returns the whole position book in one range read,
#: so ``markets_for_chain`` is ``None`` (not a per-market venue).
PERPS_READ_SPEC = PerpsReadSpec(
    contract_kinds={"reader": ("reader",), "data_store": ("data_store",)},
    build_calls=_build_gmx_calls,
    reduce_calls=_reduce_gmx_positions,
    market_metadata=_gmx_market_metadata,
    value_position=value_perps_position,
    position_key_prefix="gmx",
    markets_for_chain=None,
)
