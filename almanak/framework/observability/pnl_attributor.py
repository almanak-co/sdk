"""PnL attribution for immutable-ID positions (LP + perps).

Raw observables are canonical; derived attribution is versioned and recomputable.
A bug in the formula becomes a version bump, not permanent bad data.

Version history
---------------

**v1** — placeholder attribution. ``fee_pnl_usd`` was hardcoded to ``0`` for LP
and to ``-gas`` for perps. Impermanent loss was ``0``. Adequate for schema
plumbing but not for PnL reporting.

**v2 (VIB-3205)** — real IL and fee PnL:

LP v2 formula::

    principal_deposited = value_usd at OPEN
    principal_recovered = value_usd at CLOSE
    entry_state         = attribution_json["entry_state"] at OPEN
                          {amount0, amount1, price0, price1}
    current_prices      = market snapshot at CLOSE
    hodl_value          = amount0_open * price0_now + amount1_open * price1_now
    il_usd              = principal_recovered - hodl_value
                          (negative = LP lost vs HODL)
    price_pnl_usd       = hodl_value - principal_deposited
    protocol_fees_usd   = open.protocol_fees_usd + close.protocol_fees_usd
                          (VIB-3204 ProtocolFees.total_usd on each triggering tx)
    fee_pnl_usd         = -protocol_fees_usd  when total_usd is known
                          None                 when both events omit the field
    net_pnl_usd         = principal_recovered - principal_deposited
                          + (fee_pnl_usd or 0)
                          - total_gas
                          # NOTE: IL is NOT added here. principal_recovered
                          # already reflects the on-chain outcome of the LP
                          # position (post-IL), so adding ``il_usd`` would
                          # double-count it. ``il_usd`` is a DIAGNOSTIC
                          # attribution field (V_lp - V_hold) that explains
                          # part of the gap between what a HODLer would have
                          # made and what the LP realized; it is not a
                          # separate cashflow.

Perp v2 formula::

    price_pnl_usd    = unrealized_pnl (from protocol, signed)
    protocol_fees_usd= open.protocol_fees_usd + close.protocol_fees_usd
                       (perp_fee_usd component; gmx_v2 currently None)
    fee_pnl_usd      = -protocol_fees_usd when known, else None
    funding_pnl_usd  = -funding_fee_usd from close_event.attribution_json
                       (stamped by _apply_perp from extract_funding_fee_usd;
                        None until GMX V2 EventUtils decoder lands — VIB-3497)
    funding_fee_usd  = raw funding cost preserved in attribution dict
                       so recompute_attribution() cycles do not lose the value
                       (VIB-3519)
    net_pnl_usd      = price_pnl_usd + (fee_pnl_usd or 0)
                       + (funding_pnl_usd or 0) - total_gas

**v3 (VIB-3519)** — ``attribute_perp()`` persists ``funding_fee_usd`` raw
value in attribution dict to survive ``recompute_attribution()`` cycles.

LP rebalance gas attribution (VIB-3493)
---------------------------------------

Per-lifecycle ``attribute_lp()`` answers "what did *this LP position*
cost?" Multi-rebalance strategies — open → close → open → close cycles
under the same logical strategy — make per-lifecycle gas misleading on
its own: each individual lifecycle has only one OPEN tx and one CLOSE
tx, so the gas spend looks artificially cheap, while the strategy-
total gas is much higher (every rebalance is a CLOSE+OPEN pair, both
of which carry their own gas).

The framework's chosen model is **continuous strategy-level LP**:
every LP-typed PositionEvent's ``gas_usd`` accumulates against the
strategy, regardless of which lifecycle currently owns it. The
``almanak strat pnl`` LP section reports strategy-total LP gas via
``attribute_lp_strategy()`` so multi-rebalance behaviour is visible
in one place (rebalance count, open/close counts, total gas).
Per-lifecycle ``attribute_lp()`` remains correct for "what did this
single position cost" reporting.

The alternative model — explicit ``LP_REBALANCE`` lifecycle events —
is reserved (``LPEventType.LP_REBALANCE`` exists in
``almanak.framework.accounting.models``) but not currently emitted by
any connector. Choosing the continuous-strategy-level model lets us
report accurately without a connector-side schema migration first.

Missing-data semantics (critical)
---------------------------------

"Unknown" and "measured zero" are different and must not be conflated:

- ``protocol_fees_usd == ""`` on a raw event  => parser did not emit ProtocolFees
  (legacy row or connector without VIB-3204 support). Attribution emits
  ``fee_pnl_usd = None`` instead of 0.
- ``protocol_fees_usd == "0"``                => parser measured a zero fee
  (e.g. Aave V3 supply/borrow). Attribution emits ``fee_pnl_usd = 0``.
- ``entry_state`` missing from ``open.attribution_json`` => legacy OPEN row
  written before VIB-3205 schema extension. Attribution emits ``il_usd = None``.
"""

import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

# v3 bumps the formula: attribute_perp persists funding_fee_usd raw value in
# attribution dict to survive recompute_attribution() cycles (VIB-3519).
CURRENT_VERSION = 3


def _dec(value: Any) -> Decimal:
    """Safely convert to Decimal, returning 0 on failure.

    Logs a warning for non-empty values that fail conversion so corrupt
    financial data is visible rather than silently becoming $0.
    """
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning("PnL attribution: could not convert %r to Decimal, defaulting to 0", value)
        return Decimal("0")


def _protocol_fee_or_none(event: dict) -> Decimal | None:
    """Return the protocol fee USD from an event, or None if not captured.

    Distinguishes three cases:
    - ``protocol_fees_usd`` field present and parseable -> ``Decimal(value)``
      (includes measured zero)
    - ``protocol_fees_usd`` missing / empty / None       -> ``None``
    - present but malformed                              -> ``None`` (and warn)

    The "None" return lets callers emit ``fee_pnl_usd = None`` on the
    attribution dict rather than silently substituting zero, preserving the
    "unknown" semantic for legacy rows written before VIB-3205.
    """
    raw = event.get("protocol_fees_usd")
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning("PnL attribution: malformed protocol_fees_usd=%r, treating as unknown", raw)
        return None


def _sum_protocol_fees(open_event: dict, close_event: dict) -> Decimal | None:
    """Sum protocol fees from open and close events.

    VIB-3205 audit fix (pr-auditor Important #4): both sides must be
    measured (or both explicitly measured-zero) for the sum to be
    trustworthy. Substituting zero for an unknown side would quietly
    under-attribute — and that contradicts the module-level design
    ("Unknown and measured zero are different and must not be
    conflated"). During a phased VIB-3204 rollout (connectors ship fees
    on one side at a time), this guard returns ``None`` for the rough
    window where only one side is known; callers distinguish that from
    the measured-zero case and surface it accordingly.

    Returns:
        - ``Decimal(sum)`` when BOTH ``open_fee`` and ``close_fee`` are
          measured (including both being ``Decimal(0)`` — measured zero).
        - ``None`` when either side is unknown.
    """
    open_fee = _protocol_fee_or_none(open_event)
    close_fee = _protocol_fee_or_none(close_event)
    if open_fee is None or close_fee is None:
        return None
    return open_fee + close_fee


def _entry_state_from_open(open_event: dict) -> dict | None:
    """Extract the entry_state sidecar from an OPEN event's attribution_json.

    VIB-3205 schema: the OPEN ``PositionEvent`` carries ``attribution_json``
    that may contain an ``entry_state`` dict capturing initial token amounts
    and per-token prices. Returns ``None`` for legacy rows written before
    this extension so IL math can skip rather than silently emit a wrong
    number.
    """
    raw = open_event.get("attribution_json")
    if not raw or raw == "{}":
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        logger.warning("PnL attribution: malformed attribution_json on OPEN, skipping entry_state")
        return None
    if not isinstance(data, dict):
        return None
    entry = data.get("entry_state")
    return entry if isinstance(entry, dict) else None


def _current_prices_from_close(close_event: dict) -> dict | None:
    """Extract current-at-close per-token prices injected by the runner.

    VIB-3205: the runner attaches ``current_prices`` (a dict of
    ``{symbol_or_address: price_usd_str}``) to the CLOSE event's
    ``attribution_json`` right before ``run_attribution_on_close`` — sourced
    from the most recent ``PortfolioSnapshot.token_prices``. IL math needs
    these to evaluate HODL value at close time.
    """
    raw = close_event.get("attribution_json")
    if not raw or raw == "{}":
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    prices = data.get("current_prices")
    return prices if isinstance(prices, dict) else None


def _pair_close_with_open(history: list, close_event: Any) -> dict | None:
    """Pair a CLOSE event with its matching OPEN, reopen-safe.

    ``history`` comes from ``get_position_history`` ORDER BY timestamp ASC,
    so the earliest event is first. For a position that gets reopened under
    the same ``position_id`` (GMX V2 perps, lending markets, any
    OPEN -> CLOSE -> OPEN -> CLOSE cycle), we walk the history forward and
    maintain a stack of unpaired OPENs. When we find the CLOSE we're
    attributing, the matching OPEN is the top of that stack.

    Returns the OPEN event dict, or ``None`` if no matching OPEN exists
    (e.g. legacy row, or the event sequence is malformed).
    """
    close_id = getattr(close_event, "id", None) if not isinstance(close_event, dict) else close_event.get("id")
    unpaired_opens: list[dict] = []
    for evt in history:
        evt_type = evt.get("event_type")
        if evt_type == "OPEN":
            unpaired_opens.append(evt)
        elif evt_type == "CLOSE":
            if evt.get("id") == close_id:
                return unpaired_opens[-1] if unpaired_opens else None
            # Pop: this CLOSE pairs with the most recent unpaired OPEN.
            if unpaired_opens:
                unpaired_opens.pop()
    # Fell through without finding the target CLOSE in history — fall back
    # to the last unpaired OPEN so a CLOSE that isn't yet persisted to
    # history still gets attributed (covers the hot-path where the caller
    # passes the just-saved CLOSE event before the store re-reads it).
    return unpaired_opens[-1] if unpaired_opens else None


def _price_for_token(prices: dict, token: str) -> Decimal | None:
    """Look up a token price in a flexible ``{key: price_or_dict}`` dict.

    Supports two shapes to match ``PortfolioSnapshot.token_prices`` and the
    simpler ``{symbol: price}`` form we persist on attribution_json:

    - ``{"USDC": "1.00"}``                          -> flat price map
    - ``{"arbitrum:0xaf88...": {"price_usd": "1.00", "symbol": "USDC"}}``
      -> PortfolioSnapshot shape; we match by suffix after ``chain:`` and
      by ``symbol`` field.

    Comparison is case-insensitive. Returns ``None`` when the token is
    missing or the price cannot be parsed to Decimal.
    """
    if not prices or not token:
        return None
    needle = str(token).lower()

    def _parse(v: Any) -> Decimal | None:
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return None

    for key, val in prices.items():
        key_str = str(key).lower()
        # Direct match: flat {symbol/address: price_str}
        if key_str == needle or key_str.endswith(":" + needle):
            if isinstance(val, dict):
                price = val.get("price_usd")
                parsed = _parse(price) if price is not None else None
                if parsed is not None:
                    return parsed
                continue
            parsed = _parse(val)
            if parsed is not None:
                return parsed
        # PortfolioSnapshot shape: check nested symbol field
        if isinstance(val, dict):
            symbol = val.get("symbol")
            if symbol and str(symbol).lower() == needle:
                price = val.get("price_usd")
                parsed = _parse(price) if price is not None else None
                if parsed is not None:
                    return parsed
    return None


def compute_impermanent_loss(open_event: dict, close_event: dict) -> Decimal | None:
    """Compute impermanent loss for an LP position.

    ``IL = V_lp - V_hold`` where

    * ``V_lp`` = ``close_event["value_usd"]`` (current LP position value
      supplied by PositionValuer; includes both in-range and out-of-range
      single-sided positions)
    * ``V_hold`` = ``amount0_open * price0_now + amount1_open * price1_now``
      (what holding the entry tokens would be worth at close-time prices)

    Negative IL means the LP lost value compared to HODL.

    Returns ``None`` when either input is missing — legacy OPEN rows without
    ``entry_state`` (pre-VIB-3205 schema), OPEN rows with unparseable
    ``entry_state``, or CLOSE rows missing ``current_prices``. Attribution
    must not silently emit an incorrect zero.
    """
    entry = _entry_state_from_open(open_event)
    if entry is None:
        return None
    prices = _current_prices_from_close(close_event)
    if prices is None:
        return None

    amount0_open = _dec(entry.get("amount0"))
    amount1_open = _dec(entry.get("amount1"))
    if amount0_open == 0 and amount1_open == 0:
        return None

    token0 = entry.get("token0") or open_event.get("token0") or ""
    token1 = entry.get("token1") or open_event.get("token1") or ""

    price0_now = _price_for_token(prices, token0) if token0 else None
    price1_now = _price_for_token(prices, token1) if token1 else None

    # CodeRabbit audit fix: return None when ANY non-zero leg is missing
    # its price. The previous code defaulted missing prices to 0, which
    # silently under-priced the hodl for a two-sided LP and produced a
    # concrete (but wrong) impermanent_loss_usd. Single-sided legs
    # (amount == 0) are OK to skip because their contribution is
    # mathematically zero regardless of the missing price.
    if (price0_now is None and amount0_open != 0) or (price1_now is None and amount1_open != 0):
        return None

    hodl = (amount0_open * (price0_now or Decimal("0"))) + (amount1_open * (price1_now or Decimal("0")))
    v_lp = _dec(close_event.get("value_usd"))
    return v_lp - hodl


def compute_fee_apy(
    open_event: dict,
    collect_events: list[dict],
    close_event: dict | None = None,
) -> Decimal | None:
    """Compute time-weighted fee APY for an LP position.

    VIB-3494: uses COLLECT_FEES events (and optionally a CLOSE event that
    includes fees) to compute annualised fee yield over the hold period.

    Formula::

        total_fees_usd  = sum of fees_token0_usd + fees_token1_usd across
                          all collect events.  USD values are derived from
                          the event's ``value_usd`` field when fee-specific
                          USD fields are absent (conservative approximation).
        hold_days       = (last_collect_timestamp - open_timestamp).total_seconds
                          / 86_400  (clamped to ≥ 1 day to avoid div-by-zero)
        principal_usd   = open_event["value_usd"]
        apy             = (total_fees_usd / principal_usd) / hold_days * 365

    Returns:
        Annualised fee APY as a Decimal (e.g. ``Decimal("0.12")`` = 12 %),
        or ``None`` when inputs are insufficient (no collect events, zero
        principal, missing timestamps).

    Note: this is a *realised* APY over the hold period, not a projected APY.
    Fee amounts are token-denominated on the raw events; USD conversion is
    approximated from the event's ``value_usd`` when available.

    When ``close_event`` is provided, its ``value_usd`` is added to the fee
    sum (for protocols that bundle fee collection into the close TX), and its
    timestamp is used as the end of the hold period when it is later than the
    last collect event's timestamp.
    """
    if not collect_events:
        return None

    principal_usd = _dec(open_event.get("value_usd"))
    if principal_usd <= 0:
        return None

    # Sum fees across all collect events.  value_usd on a COLLECT_FEES event
    # represents the total USD value of fees collected in that transaction.
    total_fees_usd = Decimal("0")
    for evt in collect_events:
        fee_usd = _dec(evt.get("value_usd"))
        total_fees_usd += fee_usd
    # Include close_event fees when the close TX also collects (e.g. Uniswap V3
    # RemoveLiquidity bundles the accrued fee into the close).
    if close_event is not None:
        total_fees_usd += _dec(close_event.get("value_usd"))

    if total_fees_usd <= 0:
        return None

    # Compute hold duration from open to the latest of the collect events
    # and the close event (if provided).
    open_ts_raw = open_event.get("timestamp")
    if not open_ts_raw:
        return None

    # Build candidate end timestamps.  Use the latest available.
    end_ts_candidates: list[object] = [evt.get("timestamp") for evt in collect_events if evt.get("timestamp")]
    if close_event is not None and close_event.get("timestamp"):
        end_ts_candidates.append(close_event.get("timestamp"))
    if not end_ts_candidates:
        return None

    try:
        from datetime import datetime

        def _parse_ts(raw: object) -> datetime | None:
            if isinstance(raw, datetime):
                return raw
            if isinstance(raw, str):
                return datetime.fromisoformat(raw)
            return None

        open_dt = _parse_ts(open_ts_raw)
        if open_dt is None:
            return None

        end_dts = [_parse_ts(ts) for ts in end_ts_candidates]
        end_dts_valid = [dt for dt in end_dts if dt is not None]
        if not end_dts_valid:
            return None

        last_dt = max(end_dts_valid)
        hold_seconds = (last_dt - open_dt).total_seconds()
        hold_days = Decimal(str(max(hold_seconds / 86_400, 1.0)))  # floor at 1 day
    except (TypeError, ValueError, OverflowError):
        return None

    try:
        apy = (total_fees_usd / principal_usd) / hold_days * Decimal("365")
        return apy
    except ZeroDivisionError:
        return None


def attribute_lp(open_event: dict, close_event: dict) -> dict:
    """Compute LP PnL attribution from OPEN and CLOSE events.

    Args:
        open_event: The OPEN position event dict.
        close_event: The CLOSE position event dict.

    Returns:
        Attribution dict with versioned breakdown. Numeric fields are encoded
        as strings for JSON fidelity; "unknown" fields are encoded as
        ``None`` (not ``"0"``) to preserve the distinction for dashboards.
    """
    principal_deposited = _dec(open_event.get("value_usd"))

    # Amounts recovered at close
    amount0_recovered = _dec(close_event.get("amount0"))
    amount1_recovered = _dec(close_event.get("amount1"))
    close_value_usd = _dec(close_event.get("value_usd"))

    # Fees collected at close (raw token-denominated; kept for audit trail)
    fees_token0 = _dec(close_event.get("fees_token0"))
    fees_token1 = _dec(close_event.get("fees_token1"))

    # Gas costs across the lifecycle
    open_gas = _dec(open_event.get("gas_usd"))
    close_gas = _dec(close_event.get("gas_usd"))
    total_gas = open_gas + close_gas

    # If we have close_value_usd, use it directly as principal_recovered.
    # Otherwise fall back to 0 (raw data incomplete).
    principal_recovered = close_value_usd if close_value_usd else Decimal("0")

    # Fee PnL (VIB-3205): sum of ProtocolFees.total_usd on open+close trigger
    # txs, expressed as a *cost* (negative contribution to net PnL).
    # Returns None when both sides omit the field so dashboards can flag
    # "unknown" rather than mis-render a placeholder zero.
    fees_paid = _sum_protocol_fees(open_event, close_event)
    fee_pnl: Decimal | None = None if fees_paid is None else -fees_paid

    # Impermanent loss (VIB-3205): real IL when entry_state + current_prices
    # are available, else None. None must not cascade into net_pnl as 0;
    # instead, net_pnl when IL is unknown omits the IL term (HODL-relative
    # attribution simply isn't available for this row).
    il = compute_impermanent_loss(open_event, close_event)

    # Price PnL = what hodling would have given - what was deposited
    # When IL is known, price_pnl = principal_recovered - il - principal_deposited
    # (equivalently: hodl_value - principal_deposited). When IL is unknown,
    # fall back to the v1 notion (principal_recovered - principal_deposited)
    # so dashboards still have a usable number.
    if il is not None and principal_deposited:
        hodl_value = principal_recovered - il
        price_pnl = hodl_value - principal_deposited
    elif principal_deposited:
        price_pnl = principal_recovered - principal_deposited
    else:
        price_pnl = Decimal("0")

    # Net PnL = principal_recovered - principal_deposited + fee_pnl - gas
    # IL is *already captured* in principal_recovered (which reflects actual
    # value at close), so it must NOT be added again here — it's broken out
    # as a reporting signal only.
    net_pnl = principal_recovered + (fee_pnl or Decimal("0")) - principal_deposited - total_gas

    return {
        "version": CURRENT_VERSION,
        "position_type": "LP",
        "principal_deposited_usd": str(principal_deposited),
        "principal_recovered_usd": str(principal_recovered),
        "fees_token0": str(fees_token0),
        "fees_token1": str(fees_token1),
        "fee_pnl_usd": None if fee_pnl is None else str(fee_pnl),
        "impermanent_loss_usd": None if il is None else str(il),
        "price_pnl_usd": str(price_pnl),
        "gas_usd": str(total_gas),
        "net_pnl_usd": str(net_pnl),
        "amount0_recovered": str(amount0_recovered),
        "amount1_recovered": str(amount1_recovered),
    }


def attribute_lp_strategy(position_events: list[dict]) -> dict:
    """Strategy-level LP attribution across all rebalances (VIB-3493).

    Per-lifecycle ``attribute_lp`` is the right primitive for "what did
    *this position* cost". For a multi-rebalance strategy that opens
    and closes the same logical LP position dozens of times, the per-
    lifecycle view paints each individual position as artificially
    cheap — half of the strategy's gas is "between lifecycles" (the
    rebalance pair: CLOSE_old + OPEN_new). Mid-lifecycle CLOSE gas
    pairs with the closing LP, but the OPEN gas of the immediately-
    following position lands on the new lifecycle, so neither
    lifecycle individually owns "the rebalance".

    The Almanak chosen model is **continuous strategy-level LP**:
    every LP-typed PositionEvent's ``gas_usd`` accumulates against
    the strategy. ``almanak strat pnl``'s ``Gas costs`` line already
    aggregates strategy-level gas at the ledger layer; this helper is
    the LP-only decomposition so the LP section can show "rebalance
    cycles seen" alongside "total LP gas spent across them".

    The alternative model (explicit ``LP_REBALANCE`` lifecycle events)
    is left available — ``LPEventType.LP_REBALANCE`` is reserved in
    ``almanak.framework.accounting.models`` — but no production caller
    emits it today.  Choosing this model now lets ``strat pnl`` report
    accurately without a connector-side schema migration.

    Args:
        position_events: Mixed list of LP/perp/lending PositionEvent
            dicts (e.g. from ``store.get_position_events``). Non-LP
            events are ignored. ``event_type`` is normalised case-
            insensitively so legacy callers passing enum-derived strings
            still work.

    Returns:
        Strategy-level LP totals — all amounts as Decimal-encoded strings:

        - ``total_gas_usd``: sum of ``gas_usd`` across all LP events
          (OPEN, CLOSE, COLLECT_FEES, SNAPSHOT, …). Continuous-model
          gas — captures rebalance gas regardless of which lifecycle
          owns the tx event.
        - ``open_gas_usd`` / ``close_gas_usd``: sub-totals for the
          two lifecycle-defining event types.
        - ``open_count`` / ``close_count``: number of LP OPEN/CLOSE
          events across the strategy.
        - ``close_open_pairs``: number of adjacent CLOSE→OPEN
          transitions in the supplied event stream. **This is a
          heuristic, not a true rebalance count** — multi-pool /
          multi-protocol strategies that close one position and open
          an unrelated position next iteration will inflate this
          number. Treated as "rebalance" only when paired with stable
          ``unique_position_ids`` over the same window. An explicit
          ``LP_REBALANCE`` event lane would supersede it.
        - ``unique_position_ids``: count of distinct ``position_id``
          values touched.
    """
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    LP_TYPE = "LP"

    total = Decimal("0")
    open_gas = Decimal("0")
    close_gas = Decimal("0")
    open_count = 0
    close_count = 0
    position_ids: set[str] = set()

    # Track CLOSE→OPEN adjacency. Walk events oldest-first; the order
    # the caller supplied is not guaranteed (SQLite reads return
    # newest-first by default). Sort key uses ``timestamp`` then
    # ``str(id)`` so that mixed runtime types (UUID strings in production,
    # integer ids in some tests) sort deterministically without raising
    # ``TypeError`` from a heterogeneous comparison.
    sortable: list[dict] = sorted(
        (e for e in position_events if (e.get("position_type") or "").upper() == LP_TYPE),
        key=lambda e: (e.get("timestamp") or "", str(e.get("id") or "")),
    )

    last_lifecycle_event: str | None = None
    close_open_pairs = 0

    for evt in sortable:
        evt_type = (evt.get("event_type") or "").upper()
        gas_val = _dec(evt.get("gas_usd"))
        total += gas_val

        if evt_type == OPEN:
            open_count += 1
            open_gas += gas_val
            pid = evt.get("position_id") or ""
            if pid:
                position_ids.add(str(pid))
            if last_lifecycle_event == CLOSE:
                close_open_pairs += 1
            last_lifecycle_event = OPEN
        elif evt_type == CLOSE:
            close_count += 1
            close_gas += gas_val
            pid = evt.get("position_id") or ""
            if pid:
                position_ids.add(str(pid))
            last_lifecycle_event = CLOSE
        # Non-lifecycle LP events (COLLECT_FEES, SNAPSHOT, …) still
        # contribute to total_gas but don't shift the rebalance state
        # machine.

    return {
        "version": CURRENT_VERSION,
        "model": "continuous_strategy_level",
        "total_gas_usd": str(total),
        "open_gas_usd": str(open_gas),
        "close_gas_usd": str(close_gas),
        "open_count": open_count,
        "close_count": close_count,
        "close_open_pairs": close_open_pairs,
        "unique_position_ids": len(position_ids),
    }


def _funding_fee_from_close(close_event: dict) -> Decimal | None:
    """Read ``funding_fee_usd`` stamped in the CLOSE event's ``attribution_json``.

    ``_apply_perp`` (position_events.py) writes this value from the
    ``funding_fee_usd`` key in ``result.extracted_data`` (VIB-3497). The
    value is the accumulated funding cost in USD for the position lifecycle,
    sourced from the receipt parser's ``extract_funding_fee_usd`` method.

    Returns ``None`` when the key is absent (parser did not extract funding)
    or when the value cannot be parsed — preserving the "unknown" semantic
    rather than silently emitting zero.
    """
    raw = close_event.get("attribution_json")
    if not raw or raw == "{}":
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    val = data.get("funding_fee_usd")
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning("PnL attribution: malformed funding_fee_usd=%r in attribution_json, treating as unknown", val)
        return None


def attribute_perp(open_event: dict, close_event: dict) -> dict:
    """Compute perp PnL attribution from OPEN and CLOSE events.

    Args:
        open_event: The OPEN position event dict.
        close_event: The CLOSE position event dict.

    Returns:
        Attribution dict with versioned breakdown.

    Funding PnL (VIB-3497):
        ``funding_pnl_usd`` is populated from the ``funding_fee_usd`` field
        stamped in ``close_event["attribution_json"]`` by ``_apply_perp``
        (position_events.py) when the receipt parser extracts it.
        ``None`` means the parser did not yet emit a funding value (the
        GMX V2 EventUtils decoder is prerequisite work). ``Decimal("0")``
        means a measured zero (e.g. position held for <1 funding period).
        ``net_pnl_usd`` includes ``funding_pnl_usd`` when known.
    """
    entry_price = _dec(open_event.get("entry_price") or close_event.get("entry_price"))
    mark_price = _dec(close_event.get("mark_price"))
    unrealized_pnl = _dec(close_event.get("unrealized_pnl"))
    leverage = _dec(close_event.get("leverage") or open_event.get("leverage"))
    is_long = close_event.get("is_long")
    if is_long is None:
        is_long = open_event.get("is_long")

    open_gas = _dec(open_event.get("gas_usd"))
    close_gas = _dec(close_event.get("gas_usd"))
    total_gas = open_gas + close_gas

    # Price PnL from protocol's unrealized_pnl (already signed for direction)
    price_pnl = unrealized_pnl

    # Fee PnL (VIB-3205): real protocol fees paid on open+close tx. Replaces
    # the v1 ``-gas`` proxy. None when the connector does not yet emit
    # ProtocolFees (e.g. gmx_v2 is currently None — see VIB-3211 follow-up).
    fees_paid = _sum_protocol_fees(open_event, close_event)
    fee_pnl: Decimal | None = None if fees_paid is None else -fees_paid

    # Funding PnL (VIB-3497): accumulated funding cost for the position
    # lifecycle. Sourced from the close event's attribution_json sidecar
    # where _apply_perp stamps the receipt parser's funding_fee_usd.
    # Negative convention: funding is a cost paid by the position holder,
    # so funding_pnl_usd = -funding_fee_usd (deducted from net).
    raw_funding = _funding_fee_from_close(close_event)
    funding_pnl: Decimal | None = None if raw_funding is None else -raw_funding

    # net_pnl includes funding when known. Using (x or 0) keeps the formula
    # consistent: None (unknown) contributes 0 to net rather than crashing.
    net_pnl = price_pnl + (fee_pnl or Decimal("0")) + (funding_pnl or Decimal("0")) - total_gas

    return {
        "version": CURRENT_VERSION,
        "position_type": "PERP",
        "entry_price": str(entry_price),
        "exit_price": str(mark_price),
        "leverage": str(leverage),
        "is_long": is_long,
        "price_pnl_usd": str(price_pnl),
        "fee_pnl_usd": None if fee_pnl is None else str(fee_pnl),
        "funding_pnl_usd": None if funding_pnl is None else str(funding_pnl),
        # VIB-3519: persist the raw funding_fee_usd alongside funding_pnl_usd so
        # that _funding_fee_from_close() can read it back on a subsequent
        # recompute_attribution() call. Without this, the first write stores only
        # the computed attribution dict (which lacks funding_fee_usd), and the
        # second recompute silently drops funding_pnl_usd.
        "funding_fee_usd": None if raw_funding is None else str(raw_funding),
        "gas_usd": str(total_gas),
        "net_pnl_usd": str(net_pnl),
    }


def compute_attribution(open_event: dict, close_event: dict) -> str:
    """Compute PnL attribution JSON for a position lifecycle.

    Args:
        open_event: The OPEN event dict (from get_position_history).
        close_event: The CLOSE event dict.

    Returns:
        JSON string with versioned attribution, or '{}' on failure.
    """
    try:
        position_type = (close_event.get("position_type") or open_event.get("position_type") or "").upper()

        if position_type == "LP":
            result = attribute_lp(open_event, close_event)
        elif position_type == "PERP":
            result = attribute_perp(open_event, close_event)
        else:
            logger.debug("Unknown position type for attribution: %s", position_type)
            return "{}"

        return json.dumps(result)
    except Exception:
        logger.debug("Attribution computation failed", exc_info=True)
        return "{}"


def build_entry_state(
    *,
    token0: str,
    token1: str,
    amount0: Any,
    amount1: Any,
    price0: Any = None,
    price1: Any = None,
) -> dict[str, Any]:
    """Package initial token amounts and prices for later IL attribution.

    Stored under ``attribution_json["entry_state"]`` on ``OPEN`` events so
    that the subsequent CLOSE-time ``compute_impermanent_loss`` can evaluate
    HODL value without re-reading the OPEN's raw tx. Normalises to strings
    for JSON fidelity; callers may pass Decimal / int / str.
    """

    def _s(v: Any) -> str | None:
        if v is None:
            return None
        return str(v)

    return {
        "token0": token0 or "",
        "token1": token1 or "",
        "amount0": _s(amount0) or "0",
        "amount1": _s(amount1) or "0",
        "price0": _s(price0),
        "price1": _s(price1),
    }


async def _fetch_latest_token_prices(
    store: Any,
    deployment_id: str,
    token0: str = "",
    token1: str = "",
    chain: str = "",
    price_oracle: Any = None,
) -> dict[str, Any] | None:
    """Read per-token prices for IL attribution.

    Primary source: latest PortfolioSnapshot.token_prices (already archived).
    Fallback (VIB-3420): when no snapshot exists yet (first-iteration OPEN
    before the first snapshot is written), query the price oracle directly so
    that entry_state prices are never silently null.

    Returns None only when both sources are unavailable. Callers must treat
    None as UNAVAILABLE and emit il_usd=None rather than a misleading zero.
    """
    if hasattr(store, "get_latest_snapshot"):
        try:
            snapshot = await store.get_latest_snapshot(deployment_id)
            if snapshot is not None:
                snapshot_prices = getattr(snapshot, "token_prices", None)
                if isinstance(snapshot_prices, dict) and snapshot_prices:
                    return snapshot_prices
        except Exception:  # noqa: BLE001
            logger.debug("Failed to fetch latest snapshot for %s", deployment_id, exc_info=True)

    # Fallback: use the price oracle when no snapshot exists yet.
    # This covers the common case where a strategy opens its first position on
    # the very first iteration before the portfolio valuer has run.
    #
    # Two oracle shapes are supported:
    #   1. Plain dict (StrategyRunner.price_oracle): {symbol: Decimal | str}
    #      _price_for_token handles flat dicts directly; returned as-is so the
    #      caller can look up token0/token1 in the same format.
    #   2. Async protocol (PriceOracle with get_aggregated_price): per-token
    #      async calls are made and results are collected into a dict.
    if price_oracle is None or not (token0 or token1):
        return None

    if isinstance(price_oracle, dict):
        # Plain dict oracle — return as-is if non-empty so _price_for_token
        # can do its symbol/address matching. If the dict uses symbol keys but
        # the event uses address tokens, _price_for_token will return None for
        # those tokens, which is honest (UNAVAILABLE) rather than wrong.
        return price_oracle if price_oracle else None

    prices: dict[str, Any] = {}
    for token in filter(None, [token0, token1]):
        try:
            result = await price_oracle.get_aggregated_price(token, chain=chain or None)
            if result is not None and getattr(result, "price", None) is not None:
                prices[token.lower()] = str(result.price)
        except Exception:  # noqa: BLE001
            logger.debug("Price oracle fallback failed for token %s", token, exc_info=True)
    return prices if prices else None


async def stamp_entry_state_on_open(
    store: Any,
    open_event: Any,
    price_oracle: Any = None,
) -> None:
    """Persist entry_state on the OPEN event's attribution_json (VIB-3205).

    Called by StrategyRunner after ``save_position_event`` for OPEN events so
    that the later CLOSE-time ``compute_impermanent_loss`` can evaluate HODL
    value. entry_state captures the initial token amounts + per-token prices
    read from the latest ``PortfolioSnapshot.token_prices``.

    price_oracle is used as a fallback when no snapshot exists yet (VIB-3420).
    This covers the common case where a strategy opens its first LP position on
    the first iteration before the portfolio valuer has produced a snapshot.
    Without this fallback, impermanent_loss_usd is permanently null for these
    positions.
    """
    try:
        token0 = getattr(open_event, "token0", "") or ""
        token1 = getattr(open_event, "token1", "") or ""
        amount0 = getattr(open_event, "amount0", "") or "0"
        amount1 = getattr(open_event, "amount1", "") or "0"

        prices = await _fetch_latest_token_prices(
            store,
            open_event.deployment_id,
            token0=token0,
            token1=token1,
            chain=getattr(open_event, "chain", "") or "",
            price_oracle=price_oracle,
        )
        price0 = _price_for_token(prices, token0) if prices else None
        price1 = _price_for_token(prices, token1) if prices else None

        entry_state = build_entry_state(
            token0=token0,
            token1=token1,
            amount0=amount0,
            amount1=amount1,
            price0=price0,
            price1=price1,
        )

        # Merge with any existing attribution_json content to preserve
        # fields written by other subsystems.
        existing: dict[str, Any] = {}
        raw = getattr(open_event, "attribution_json", "") or ""
        if raw and raw != "{}":
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    existing = parsed
            except (json.JSONDecodeError, TypeError):
                existing = {}
        existing["entry_state"] = entry_state

        new_json = json.dumps(existing)
        open_event.attribution_json = new_json
        if hasattr(store, "update_position_attribution"):
            await store.update_position_attribution(
                open_event.id, new_json, getattr(open_event, "attribution_version", 0) or 0
            )
        else:
            await store.save_position_event(open_event)
    except Exception:  # noqa: BLE001
        # VIB-3205 audit fix (pr-auditor Important #5): escalate from debug
        # to warning. This is a financially-sensitive module — losing
        # entry_state on OPEN permanently breaks IL for that position,
        # and debug-level logs are invisible in production.
        logger.warning("Failed to stamp entry_state on OPEN", exc_info=True)


async def run_attribution_on_close(
    store: Any,
    close_event: Any,
) -> str:
    """Look up the OPEN event and compute attribution for a CLOSE event.

    Called by StrategyRunner after saving a CLOSE position event.
    Updates the event's attribution_json in the store.

    VIB-3205: this function injects ``current_prices`` from the latest
    ``PortfolioSnapshot.token_prices`` into the close_event's
    ``attribution_json`` right before dispatch so the LP attribution can
    compute real impermanent loss against HODL value.

    Args:
        store: StateManager or SQLiteStore with get_position_history/save_position_event.
        close_event: The PositionEvent being closed.

    Returns:
        The computed attribution JSON string.
    """
    attribution = "{}"
    try:
        history = await store.get_position_history(close_event.deployment_id, close_event.position_id)
        # VIB-3205 audit fix (pr-auditor Blocker #2): pair each CLOSE with the
        # OPEN that IMMEDIATELY PRECEDES it among unpaired OPENs, not the
        # first-ever OPEN. Positions that get reopened under the same
        # ``position_id`` (GMX V2 perps, lending markets, any OPEN -> CLOSE
        # -> OPEN -> CLOSE cycle) were otherwise mis-paired with the stale
        # first OPEN — wrong ``principal_deposited`` and wrong ``entry_state``
        # prices. Same class of bug as VIB-3206's reopen issue.
        open_event = _pair_close_with_open(history, close_event)

        if open_event is None:
            logger.debug(
                "No matching OPEN event found for position %s, skipping attribution",
                close_event.position_id,
            )
            return attribution

        close_dict = close_event.to_dict() if hasattr(close_event, "to_dict") else {}

        # VIB-3205: inject current-at-close prices into the close_event's
        # attribution_json sidecar so compute_impermanent_loss can evaluate
        # HODL value. Best-effort — if no snapshot exists yet, IL silently
        # falls back to None rather than mis-reporting zero.
        prices = await _fetch_latest_token_prices(
            store,
            close_event.deployment_id,
            chain=getattr(close_event, "chain", "") or "",
        )
        if prices:
            close_attr = close_dict.get("attribution_json") or "{}"
            try:
                close_attr_parsed = json.loads(close_attr) if isinstance(close_attr, str) else close_attr
                if not isinstance(close_attr_parsed, dict):
                    close_attr_parsed = {}
            except (json.JSONDecodeError, TypeError):
                close_attr_parsed = {}
            close_attr_parsed["current_prices"] = prices
            close_dict["attribution_json"] = json.dumps(close_attr_parsed)

        attribution = compute_attribution(open_event, close_dict)

        if attribution != "{}":
            # CodeRabbit audit fix: persist ``current_prices`` INSIDE the
            # attribution_json we store, so ``recompute_attribution`` can
            # re-derive IL / price_pnl from the SAME close-time snapshot
            # when the formula changes. Without this, recomputes would
            # degrade every v2 LP close back to ``impermanent_loss_usd=None``
            # because the transient sidecar set at lines 639-648 above
            # was never committed to disk.
            if prices:
                try:
                    attr_parsed = json.loads(attribution)
                    if isinstance(attr_parsed, dict):
                        attr_parsed["current_prices"] = prices
                        attribution = json.dumps(attr_parsed)
                except (json.JSONDecodeError, TypeError):
                    # Attribution isn't a dict — leave as-is; recompute will
                    # log a warning if it ever hits this row.
                    logger.debug("Could not embed current_prices into attribution JSON")

            close_event.attribution_json = attribution
            close_event.attribution_version = CURRENT_VERSION
            # Use partial update to avoid overwriting stored fields. Pass
            # deployment_id (CR audit on PR #2018) so the GSM client can
            # forward it to the gateway proto request as defense-in-depth
            # wire-level scope; SQLite ignores it because event UUIDs are
            # globally unique by construction.
            if hasattr(store, "update_position_attribution"):
                await store.update_position_attribution(
                    close_event.id,
                    attribution,
                    CURRENT_VERSION,
                    deployment_id=getattr(close_event, "deployment_id", "") or "",
                )
            else:
                await store.save_position_event(close_event)
            logger.debug(
                "Attribution v%d computed for position %s",
                CURRENT_VERSION,
                close_event.position_id,
            )
    except Exception:
        # VIB-3205 audit fix (pr-auditor Important #5): escalate from debug
        # to warning so production log pipelines surface attribution
        # failures. A silent attribution failure leaves the dashboard
        # showing corrupted / missing PnL for that close.
        logger.warning("Failed to run attribution on close", exc_info=True)

    return attribution


async def recompute_attribution(
    store: Any,
    deployment_id: str,
    version: int = CURRENT_VERSION,
) -> int:
    """Batch-recompute attribution for all closed positions.

    Useful when the attribution formula is updated (version bump).

    Args:
        store: StateManager or SQLiteStore with position event methods.
        deployment_id: Strategy deployment to recompute.
        version: Target attribution version.

    Returns:
        Number of positions recomputed.
    """
    count = 0
    try:
        close_events = await store.get_position_events(deployment_id, event_type="CLOSE", limit=10000)

        for close_dict in close_events:
            position_id = close_dict.get("position_id", "")
            if not position_id:
                continue

            # Skip if already at target version
            existing_version = close_dict.get("attribution_version", 0)
            if existing_version >= version:
                continue

            history = await store.get_position_history(deployment_id, position_id)
            # Same reopen-safe pairing rule as run_attribution_on_close.
            open_event = _pair_close_with_open(history, close_dict)
            if open_event is None:
                continue

            attribution = compute_attribution(open_event, close_dict)
            if attribution != "{}":
                # CodeRabbit audit fix (round 3): preserve the persisted
                # ``current_prices`` sidecar across a recompute. Without this,
                # ``compute_attribution`` rebuilds the attribution payload from
                # scratch and the new JSON no longer contains current_prices —
                # so the next call to ``recompute_attribution`` (or any future
                # read via ``_current_prices_from_close``) would silently lose
                # the close-time price snapshot, and IL would collapse back to
                # None. Re-merge current_prices from the stored attribution
                # into the newly computed one before persisting.
                try:
                    existing_attr = json.loads(close_dict.get("attribution_json") or "{}")
                    new_attr = json.loads(attribution)
                    if (
                        isinstance(existing_attr, dict)
                        and isinstance(new_attr, dict)
                        and "current_prices" in existing_attr
                    ):
                        new_attr["current_prices"] = existing_attr["current_prices"]
                        attribution = json.dumps(new_attr)
                except (json.JSONDecodeError, TypeError):
                    # Stored attribution isn't valid JSON — nothing to preserve.
                    pass
                # Use partial update to avoid wiping stored fields. Pass
                # deployment_id (CR audit on PR #2018) so the GSM client
                # can forward it to the gateway proto request as wire-level
                # scope; SQLite ignores it because event UUIDs are globally
                # unique by construction.
                if hasattr(store, "update_position_attribution"):
                    await store.update_position_attribution(
                        close_dict["id"],
                        attribution,
                        version,
                        deployment_id=close_dict.get("deployment_id") or deployment_id,
                    )
                else:
                    from .position_events import PositionEvent

                    # Fallback: reconstruct the event
                    evt_obj = PositionEvent(
                        id=close_dict["id"],
                        deployment_id=close_dict.get("deployment_id", ""),
                        position_id=position_id,
                        position_type=close_dict.get("position_type", ""),
                        event_type="CLOSE",
                        protocol=close_dict.get("protocol", ""),
                        chain=close_dict.get("chain", ""),
                        attribution_json=attribution,
                        attribution_version=version,
                        amount0=close_dict.get("amount0", ""),
                        amount1=close_dict.get("amount1", ""),
                        value_usd=close_dict.get("value_usd", ""),
                        fees_token0=close_dict.get("fees_token0", ""),
                        fees_token1=close_dict.get("fees_token1", ""),
                        tx_hash=close_dict.get("tx_hash", ""),
                        gas_usd=close_dict.get("gas_usd", ""),
                        ledger_entry_id=close_dict.get("ledger_entry_id", ""),
                        unrealized_pnl=close_dict.get("unrealized_pnl", ""),
                        entry_price=close_dict.get("entry_price", ""),
                        mark_price=close_dict.get("mark_price", ""),
                        leverage=close_dict.get("leverage", ""),
                        protocol_fees_usd=close_dict.get("protocol_fees_usd", ""),
                    )
                    await store.save_position_event(evt_obj)
                count += 1

    except Exception:
        logger.debug("Batch recompute failed", exc_info=True)

    logger.info("Recomputed attribution for %d positions (v%d)", count, version)
    return count
