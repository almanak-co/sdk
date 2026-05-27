"""Extracted Data Models for Result Enrichment.

This module defines the typed data classes used to represent extracted
data from transaction receipts. These models are populated by the
ResultEnricher component and attached to ExecutionResult.

The design follows "UX First, Safety Always" - providing strongly typed
data that strategy authors can access directly without manual parsing.

Example:
    result = await orchestrator.execute(intent)
    if result.swap_amounts:
        print(f"Swapped: {result.swap_amounts.amount_in_decimal}")
    if result.position_id:
        print(f"Position: {result.position_id}")
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any


class SlippageSource(StrEnum):
    """VIB-4087 — provenance of a ``slippage_bps`` value.

    A bare integer is ambiguous: a 0-bps reading from on-chain log
    decoding (RECEIPT_DECODED) is not the same as a 0-bps reading from
    a pre/post wallet-balance reconciliation (BALANCE_DELTA), and
    neither is the same as "no source" (NONE — slippage is undefined,
    not zero). Persisting the source alongside the value lets
    downstream consumers (Accountant Test, dashboard, audit) treat each
    case correctly.

    Receipt parsers MUST stamp this whenever they emit ``slippage_bps``.
    Parsers that genuinely cannot measure slippage emit ``slippage_bps``
    of None and ``slippage_source = NONE``.
    """

    RECEIPT_DECODED = "RECEIPT_DECODED"
    BALANCE_DELTA = "BALANCE_DELTA"
    NONE = "NONE"


@dataclass(frozen=True)
class SwapAmounts:
    """Extracted swap execution data.

    Represents the token amounts exchanged in a swap transaction.
    All fields are immutable (frozen=True) for safety.

    Attributes:
        amount_in: Raw input amount (in token's smallest unit)
        amount_out: Raw output amount (in token's smallest unit)
        amount_in_decimal: Human-readable input amount, or ``None`` when
            the parser could not resolve ``token_in`` decimals. Per the
            "Empty != zero" invariant in ``docs/internal/blueprints/27-accounting.md``:
            ``Decimal(0)`` is a measured zero, ``None`` is unmeasured.
            Never substitute one for the other.
        amount_out_decimal: Human-readable output amount, or ``None`` when
            unmeasured (parsers fail-close on output decimals, so this is
            populated for every successful parse today; typed as optional
            for consistency with ``amount_in_decimal``).
        effective_price: Actual execution price (out/in), or ``None`` when
            unmeasurable (e.g. unresolved input decimals). Per the
            "Empty != zero" invariant a literal ``Decimal(0)`` here would
            silently corrupt slippage / lot-pricing reconciliation.
        slippage_bps: Actual slippage in basis points (None if unknown)
        expected_out_decimal: Pre-slippage-discount expected output in human
            units, sourced from the compiler's ActionBundle metadata
            (VIB-3203). Persisting this alongside ``slippage_bps`` gives
            downstream consumers the source-of-truth used to compute the
            realized slippage. ``None`` when the compile path did not supply
            a quote.
        token_in: Input token address or symbol
        token_out: Output token address or symbol
        amount_in_decimal_resolved: ``True`` when ``amount_in_decimal`` was
            computed from a resolved ``decimals`` value on the token
            resolver. ``False`` means the parser could not resolve
            decimals for ``token_in``; ``amount_in_decimal`` is then
            ``None`` (the parser MUST NOT substitute a measured zero —
            issue #1778, "Empty != zero" invariant).
            Defaults to ``True`` so existing parsers that do not populate
            the flag continue to behave as before.
        amount_out_decimal_resolved: Analogous flag for
            ``amount_out_decimal``. See ``amount_in_decimal_resolved``.

    Example:
        if result.swap_amounts:
            price = result.swap_amounts.effective_price
            slippage = result.swap_amounts.slippage_bps
    """

    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal | None
    amount_out_decimal: Decimal | None
    effective_price: Decimal | None = None
    slippage_bps: int | None = None
    expected_out_decimal: Decimal | None = None
    token_in: str | None = None
    token_out: str | None = None
    amount_in_decimal_resolved: bool = True
    amount_out_decimal_resolved: bool = True
    # VIB-4087 — provenance of ``slippage_bps``. Defaults to NONE so a
    # parser that emits SwapAmounts without explicitly stamping the
    # source produces a self-describing ``slippage_source=NONE`` row
    # rather than a silently-misleading 0-bps reading. Connectors must
    # set this to RECEIPT_DECODED or BALANCE_DELTA when they have a
    # measured value.
    slippage_source: SlippageSource = SlippageSource.NONE

    # Aliases: amount_in_human / amount_out_human (VIB-295)
    # Strategy authors naturally reach for _human instead of _decimal.
    def __getattr__(self, name: str) -> Any:
        if name == "amount_in_human":
            return self.amount_in_decimal
        if name == "amount_out_human":
            return self.amount_out_decimal
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_in_decimal": str(self.amount_in_decimal) if self.amount_in_decimal is not None else None,
            "amount_out_decimal": str(self.amount_out_decimal) if self.amount_out_decimal is not None else None,
            "effective_price": str(self.effective_price) if self.effective_price is not None else None,
            "slippage_bps": self.slippage_bps,
            # VIB-4087 — slippage_source provenance. ``slippage_bps`` value alone
            # is ambiguous: receipt-decoded vs balance-delta-fallback vs none-
            # available all read identically as integers. Persisting the source
            # alongside the value lets downstream consumers (Accountant Test,
            # dashboard, audit) treat each case correctly.
            "slippage_source": str(self.slippage_source),
            "expected_out_decimal": str(self.expected_out_decimal) if self.expected_out_decimal is not None else None,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in_decimal_resolved": self.amount_in_decimal_resolved,
            "amount_out_decimal_resolved": self.amount_out_decimal_resolved,
        }


@dataclass(frozen=True)
class LPCloseData:
    """Extracted LP close execution data.

    Represents the amounts collected when closing an LP position,
    including principal and fees.

    Attributes:
        amount0_collected: Total amount of token0 collected (principal + fees)
        amount1_collected: Total amount of token1 collected (principal + fees)
        fees0: Fees earned in token0 (if separately tracked)
        fees1: Fees earned in token1 (if separately tracked)
        liquidity_removed: Amount of liquidity removed (if available)
        additional_amounts: Amounts for coins beyond token0/token1 (e.g., Curve 3/4-coin pools).
            Maps coin index to raw amount: {2: 50000000, 3: 91000000000000000000}.
        additional_fees: Fees for coins beyond token0/token1.
            Maps coin index to fee amount: {2: 100000, 3: 0}.
        current_tick: Pool's current tick at the moment of close (VIB-3940).
            Mirrors ``LPOpenData.current_tick`` so the framework can derive
            ``in_range`` at close-time and stamp it on the LP_CLOSE event,
            closing the lane-symmetry gap with LP_OPEN. Sourced from a Swap
            event in the close receipt when present, with a slot0() RPC
            fallback in the runner. None when no Swap event is in the
            receipt and the slot0 fallback could not run.
        pool_address: V3 pool address that emitted the Burn event (VIB-3940).
            Required input for the framework's slot0 fallback. Empty when the
            parser couldn't identify the pool. Mirrors ``LPOpenData.pool_address``.
        source: Provenance tag for the on-chain event the close amounts were
            decoded from. ``"collect"`` = sourced from a ``Collect`` event
            (principal + already-accrued fees, the truth on transfer);
            ``"decrease_liquidity"`` = sourced from a ``DecreaseLiquidity``
            event (principal unlocked into ``tokensOwed`` but not yet
            transferred). For protocols whose close is a two-tx sequence
            (Aerodrome Slipstream: ``decreaseLiquidity`` then ``collect``),
            ResultEnricher uses this tag to prefer the ``Collect``-sourced
            extraction across receipts so accrued fees on chain are not
            silently dropped from the registry payload (VIB-4310). Optional
            for backward compatibility: single-tx parsers may leave it
            ``None``; the enricher then falls back to first-match semantics.

    Example:
        if result.lp_close_data:
            total_0 = result.lp_close_data.amount0_collected
            fees_0 = result.lp_close_data.fees0
            # For 4-coin pools (e.g., Curve NG):
            all_amounts = result.lp_close_data.all_amounts  # [amt0, amt1, amt2, amt3]
    """

    amount0_collected: int
    amount1_collected: int
    # VIB-4470 — Empty ≠ Zero. ``None`` means the parser did not measure fees
    # separately (the canonical case for protocols that bundle fees into the
    # withdrawal amount). A numeric value — including ``0`` — is a measured
    # observation. Flipping the default from ``0`` to ``None`` removes the
    # silent measured-zero lie that propagated through to LP accounting events.
    fees0: int | None = None
    fees1: int | None = None
    liquidity_removed: int | None = None
    additional_amounts: dict[int, int] | None = None
    additional_fees: dict[int, int] | None = None
    current_tick: int | None = None  # VIB-3940
    pool_address: str = ""  # VIB-3940 — for framework slot0 fallback
    source: str | None = None  # VIB-4310 — "collect" | "decrease_liquidity" | None
    # VIB-4426 P1 #4 — V4 canonical currency addresses in PoolKey-sorted order
    # (``int(currency0, 16) < int(currency1, 16)``). The V4 receipt parser
    # populates these from the PoolKey lookup so the LP accounting handler can
    # resolve symbols / decimals by ADDRESS instead of by user-intent index.
    # Pre-fix: a user-supplied pool string like ``"USDC/WETH/3000"`` would
    # silently mis-attribute ``amount0`` (the WETH leg) to ``token0=USDC``
    # — wrong decimals, wrong USD price. V3 parsers leave these ``None``;
    # the handler falls back to user-intent order in that case.
    currency0: str | None = None
    currency1: str | None = None

    @property
    def all_amounts(self) -> list[int]:
        """Return all coin amounts as a list, including additional coins."""
        result = [self.amount0_collected, self.amount1_collected]
        if self.additional_amounts:
            for i in sorted(self.additional_amounts):
                result.append(self.additional_amounts[i])
        return result

    @property
    def all_fees(self) -> list[int | None]:
        """Return all fee amounts as a list, including additional coins.

        Per Empty ≠ Zero: ``None`` slots stand for "unmeasured by this parser".
        """
        result: list[int | None] = [self.fees0, self.fees1]
        if self.additional_fees:
            for i in sorted(self.additional_fees):
                result.append(self.additional_fees[i])
        return result

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        d: dict[str, Any] = {
            "amount0_collected": str(self.amount0_collected),
            "amount1_collected": str(self.amount1_collected),
            # VIB-4470 — preserve None as JSON null (unmeasured), distinct
            # from the string "0" (measured zero) per Empty ≠ Zero.
            "fees0": (str(self.fees0) if self.fees0 is not None else None),
            "fees1": (str(self.fees1) if self.fees1 is not None else None),
            # Preserve measured zero per the "Empty != Zero" invariant
            # (CLAUDE.md §Accounting). A truthy check would collapse the
            # measured 0 case to None — CodeRabbit pushback on PR #2256.
            "liquidity_removed": (str(self.liquidity_removed) if self.liquidity_removed is not None else None),
            "current_tick": self.current_tick,  # VIB-3940
            "pool_address": self.pool_address,  # VIB-3940
            "source": self.source,  # VIB-4310
            # VIB-4426 P1 #4
            "currency0": self.currency0,
            "currency1": self.currency1,
        }
        if self.additional_amounts:
            d["additional_amounts"] = {str(k): str(v) for k, v in self.additional_amounts.items()}
        if self.additional_fees:
            d["additional_fees"] = {str(k): str(v) for k, v in self.additional_fees.items()}
        return d


@dataclass(frozen=True)
class LPOpenData:
    """Extracted LP open execution data.

    Represents the data extracted when opening a new LP position,
    including the position ID and range parameters.

    Attributes:
        position_id: NFT position ID (tokenId)
        tick_lower: Lower tick boundary of the position
        tick_upper: Upper tick boundary of the position
        liquidity: Amount of liquidity minted
        amount0: Actual amount of token0 deposited
        amount1: Actual amount of token1 deposited
        current_tick: Pool's current tick at the moment of mint (VIB-3887).
            Used to derive ``in_range`` on ``position_events``. Sourced
            from the gateway-side receipt parser (which has authority to
            call ``slot0().tick`` after the mint receipt). Framework code
            consumes this field — it never populates it via direct RPC.
            None when the gateway didn't (yet) carry the field.
        pool_address: V3 pool address for the position (VIB-3893). Populated
            by the receipt parser from the Pool Mint event. Used by the
            framework to fall back to a ``slot0()`` lookup when the receipt
            had no Swap event (pure NPM.mint LP_OPEN — the canonical
            Almanak swap-then-mint-across-cycles path produces this).
            Empty string when the parser couldn't identify the pool.
        position_hash: V3: always None. V4: keccak-hashed position key —
            see VIB-4473.

    Example:
        if result.position_id:  # Core field
            # Access additional data via extracted_data
            lp_data = result.get_extracted("lp_open_data", LPOpenData)
            if lp_data:
                print(f"Range: {lp_data.tick_lower} - {lp_data.tick_upper}")
    """

    position_id: int
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: int | None = None
    amount0: int | None = None
    amount1: int | None = None
    current_tick: int | None = None  # VIB-3887
    pool_address: str = ""  # VIB-3893 — for framework slot0 fallback
    position_hash: str | None = None  # VIB-4473 — V4 lot-matching anchor
    # VIB-4426 P1 #4 — V4 canonical currency addresses (see LPCloseData).
    currency0: str | None = None
    currency1: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position_id": self.position_id,
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "liquidity": str(self.liquidity) if self.liquidity else None,
            "amount0": str(self.amount0) if self.amount0 else None,
            "amount1": str(self.amount1) if self.amount1 else None,
            "current_tick": self.current_tick,
            "pool_address": self.pool_address,
            "position_hash": self.position_hash,
            # VIB-4426 P1 #4
            "currency0": self.currency0,
            "currency1": self.currency1,
        }


@dataclass(frozen=True)
class BorrowData:
    """Extracted borrow execution data.

    Represents the data from a borrow transaction on lending protocols.

    Attributes:
        borrow_amount: Amount borrowed (raw units)
        borrow_rate: Interest rate at time of borrow (if available)
        debt_token: Address or symbol of the debt token
        health_factor: Health factor after borrow (if available)
    """

    borrow_amount: int
    borrow_rate: Decimal | None = None
    debt_token: str | None = None
    health_factor: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "borrow_amount": str(self.borrow_amount),
            "borrow_rate": str(self.borrow_rate) if self.borrow_rate else None,
            "debt_token": self.debt_token,
            "health_factor": str(self.health_factor) if self.health_factor else None,
        }


@dataclass(frozen=True)
class SupplyData:
    """Extracted supply execution data.

    Represents the data from a supply transaction on lending protocols.

    Attributes:
        supply_amount: Amount supplied (raw units)
        a_token_received: Amount of aToken/receipt token received
        supply_rate: Supply APY at time of supply (if available)
    """

    supply_amount: int
    a_token_received: int | None = None
    supply_rate: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "supply_amount": str(self.supply_amount),
            "a_token_received": str(self.a_token_received) if self.a_token_received else None,
            "supply_rate": str(self.supply_rate) if self.supply_rate else None,
        }


@dataclass(frozen=True)
class PerpData:
    """Extracted perpetual position data.

    Represents the data from perpetual position operations.

    Attributes:
        position_id: Position identifier
        size_delta: Change in position size
        collateral: Collateral amount
        entry_price: Entry price (for opens)
        exit_price: Exit price (for closes)
        leverage: Position leverage
        realized_pnl: Realized PnL (for closes)
        fees_paid: Total fees paid
        funding_fee_usd: Accumulated funding fees in USD at close (VIB-3497).
            None = unavailable (parser has not yet implemented extraction).
            Decimal("0") = measured zero funding (position held for <1 funding period).
    """

    position_id: str | int | None = None
    size_delta: int | None = None
    collateral: int | None = None
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    leverage: Decimal | None = None
    realized_pnl: Decimal | None = None
    fees_paid: int | None = None
    funding_fee_usd: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position_id": str(self.position_id) if self.position_id else None,
            "size_delta": str(self.size_delta) if self.size_delta else None,
            "collateral": str(self.collateral) if self.collateral else None,
            "entry_price": str(self.entry_price) if self.entry_price else None,
            "exit_price": str(self.exit_price) if self.exit_price else None,
            "leverage": str(self.leverage) if self.leverage else None,
            "realized_pnl": str(self.realized_pnl) if self.realized_pnl else None,
            "fees_paid": str(self.fees_paid) if self.fees_paid else None,
            "funding_fee_usd": str(self.funding_fee_usd) if self.funding_fee_usd is not None else None,
        }


@dataclass(frozen=True)
class StakeData:
    """Extracted staking execution data.

    Represents the data from staking/unstaking transactions.

    Attributes:
        stake_amount: Amount staked/unstaked
        shares_received: Shares/receipt tokens received (for stake)
        underlying_received: Underlying tokens received (for unstake)
        stake_token: Address or symbol of the stake token
    """

    stake_amount: int
    shares_received: int | None = None
    underlying_received: int | None = None
    stake_token: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stake_amount": str(self.stake_amount),
            "shares_received": str(self.shares_received) if self.shares_received else None,
            "underlying_received": str(self.underlying_received) if self.underlying_received else None,
            "stake_token": self.stake_token,
        }


@dataclass(frozen=True)
class PredictionSetupTx:
    """Per-tx record of a Polymarket V2 on-chain setup transaction (VIB-3710).

    Mirrors :class:`almanak.connectors.polymarket.models.SetupTxInfo`
    but lives in the framework execution layer so consumers downstream of the
    enricher (strategy callbacks, accounting handler) do not need to import
    from the connector. The connector-side struct is the wire model; this
    struct is the framework-side projection that flows on
    :class:`PredictionFill`.

    Attributes:
        tx_hash: 0x-prefixed Polygon tx hash for the approval / wrap.
        description: Human-readable label (e.g. ``"Approve pUSD → CTF V2 exchange"``).
        gas_used: Receipt ``gasUsed`` (gas units consumed).
        gas_price_wei: Effective gas price in wei (string-encoded for Decimal precision).
        total_cost_wei: ``gas_used * gas_price_wei`` (string-encoded for precision).
    """

    tx_hash: str
    description: str
    gas_used: int
    gas_price_wei: str
    total_cost_wei: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "description": self.description,
            "gas_used": self.gas_used,
            "gas_price_wei": self.gas_price_wei,
            "total_cost_wei": self.total_cost_wei,
        }


@dataclass(frozen=True)
class PredictionFill:
    """Extracted Polymarket CLOB fill data (VIB-3218).

    Polymarket orders submit off-chain; the CLOB API returns "order accepted"
    before the order is matched. A ``PREDICTION_BUY`` strategy that flips its
    ``position_open`` flag on submission-success was persisting the REQUESTED
    size -- not the actual fill -- so partial IOC fills and unfilled GTC
    limits both got booked as full positions. This struct carries the actual
    fill amount from the CLOB response back to the strategy.

    VIB-3710 extension: also carries the gateway-side setup transactions
    (approvals + source-asset → pUSD wrap) and the operator fee charged at
    match time. The downstream prediction handler folds both into a
    fully-loaded cost basis so realized PnL on SELL/REDEEM is no longer
    systematically optimistic.

    Attributes:
        filled_shares: Shares actually filled at response time. 0 means the
            order is either resting on the book (GTC/live) or was rejected
            (IOC/unmatched). Never assume `== requested_shares`.
        requested_shares: Shares the intent asked for. Kept alongside
            ``filled_shares`` so strategies can detect partial fills without
            re-reading the intent.
        avg_fill_price: Volume-weighted average price of immediate fills.
            None when no portion of the order filled yet.
        order_id: CLOB-assigned order identifier for follow-up queries.
        status: Lowercase CLOB order lifecycle state as a free-form string
            ("matched", "live", "unmatched", "delayed", …). The typed status
            is on :class:`ClobExecutionResult`; this field is a hint for
            logging / diagnostics without reaching into extracted_data.
        setup_txs: On-chain setup transactions submitted by the gateway
            before this order. Empty list when allowances were already in
            place AND no wrap was needed. (VIB-3710)
        fee_pusd: pUSD operator fee charged at match time, in human units.
            None when the order did not match (no fee yet) or when the CLOB
            response did not carry a fee field. (VIB-3710)

    Example::

        def on_intent_executed(self, intent, success, result):
            if not success or result.prediction_fill is None:
                return
            fill = result.prediction_fill
            if fill.filled_shares == 0:
                return  # GTC resting or IOC unmatched -- position NOT open
            self._filled_shares = fill.filled_shares
            self._position_open = True
    """

    filled_shares: Decimal
    requested_shares: Decimal
    avg_fill_price: Decimal | None = None
    order_id: str | None = None
    status: str | None = None
    setup_txs: tuple[PredictionSetupTx, ...] = ()
    fee_pusd: Decimal | None = None

    @property
    def is_filled(self) -> bool:
        """True when at least some portion of the order filled."""
        return self.filled_shares > 0

    @property
    def is_fully_filled(self) -> bool:
        """True when the full requested size filled."""
        return self.filled_shares >= self.requested_shares and self.filled_shares > 0

    @property
    def is_partial(self) -> bool:
        """True when some but not all of the requested size filled."""
        return 0 < self.filled_shares < self.requested_shares

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "filled_shares": str(self.filled_shares),
            "requested_shares": str(self.requested_shares),
            "avg_fill_price": str(self.avg_fill_price) if self.avg_fill_price is not None else None,
            "order_id": self.order_id,
            "status": self.status,
            "setup_txs": [tx.to_dict() for tx in self.setup_txs],
            "fee_pusd": str(self.fee_pusd) if self.fee_pusd is not None else None,
        }


@dataclass(frozen=True)
class BridgeData:
    """Extracted bridge execution data (VIB-3226).

    Typed view of what happened on the *source* chain when a BRIDGE intent
    executed. Populated by :class:`ResultEnricher` after a bridge adapter's
    receipt parser (Across / Stargate / LiFi) extracts the deposit event.

    Semantics:
        - All fields describe the **source-chain** transaction (the deposit).
          The destination-chain settlement is observed asynchronously by
          :class:`EnsoStateProvider` and is NOT guaranteed to be present at
          enrichment time. ``destination_tx_hash`` is a forward-looking hook
          and will be ``None`` on first enrichment for nearly every bridge.
        - ``amount_sent_raw`` is the raw on-chain integer in the token's
          smallest unit (as observed from the deposit log / ERC-20 Transfer).
          ``amount_sent`` is the same value as a human-readable Decimal using
          the input token's decimals. Raising rather than defaulting to 18
          is handled in the parser — if decimals cannot be resolved, the
          parser returns ``None`` and the enricher treats it as a missing
          extraction (VIB-3226 does not introduce silent 18-decimal lies).
        - ``bridge_name`` is the lowercased adapter identifier the framework
          uses in its registries (``"across"``, ``"stargate"``, ``"lifi"``),
          NOT the human-readable display name ("Across", "Stargate").

    Attributes:
        source_tx_hash: Source-chain transaction hash for the deposit.
        source_chain: Canonical source chain identifier (e.g. ``"base"``).
        destination_chain: Canonical destination chain identifier. For
            protocols that encode a chain id on-chain (Across ``destinationChainId``,
            Stargate LZ eid), the parser translates that to the framework's
            chain name when possible; unknown chain ids fall back to the raw
            value as a string.
        token_symbol: Uppercase symbol of the token being bridged (e.g.
            ``"USDC"``). Sourced from the intent when the parser cannot
            recover it from the receipt alone.
        source_token_address: ERC-20 contract address of the bridged token
            on the source chain (lowercased 0x...). Optional — not every
            bridge event carries it.
        destination_token_address: ERC-20 contract address on the destination
            chain. Optional — populated when the deposit event includes a
            destination-token field (Across depositV3 / LiFi quote); None
            otherwise.
        amount_sent: Human-readable amount the wallet deposited into the
            bridge on the source chain.
        amount_sent_raw: Raw integer amount in the token's smallest unit.
        bridge_name: Lowercased bridge adapter identifier used in the
            receipt-parser registry.
        destination_tx_hash: Destination-chain settlement tx hash, if the
            parser was able to discover it synchronously. This is almost
            always ``None`` at first enrichment (settlement is async);
            strategies that need the destination tx should continue to use
            ``EnsoStateProvider.wait_for_bridge_completion``.
        expected_amount_out: Expected amount delivered on the destination
            chain per the compiler-time quote (pre-slippage). ``None`` when
            the parser could not resolve it.

    Example::

        def on_intent_executed(self, intent, success, result):
            if success and result.bridge_data:
                bd = result.bridge_data
                self.state["last_bridge"] = {
                    "amount": str(bd.amount_sent),
                    "from": bd.source_chain,
                    "to": bd.destination_chain,
                    "tx": bd.source_tx_hash,
                    "bridge": bd.bridge_name,
                }
    """

    source_tx_hash: str
    source_chain: str
    destination_chain: str
    token_symbol: str
    amount_sent: Decimal
    amount_sent_raw: int
    bridge_name: str
    source_token_address: str | None = None
    destination_token_address: str | None = None
    destination_tx_hash: str | None = None
    expected_amount_out: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source_tx_hash": self.source_tx_hash,
            "source_chain": self.source_chain,
            "destination_chain": self.destination_chain,
            "token_symbol": self.token_symbol,
            "amount_sent": str(self.amount_sent),
            "amount_sent_raw": str(self.amount_sent_raw),
            "bridge_name": self.bridge_name,
            "source_token_address": self.source_token_address,
            "destination_token_address": self.destination_token_address,
            "destination_tx_hash": self.destination_tx_hash,
            "expected_amount_out": str(self.expected_amount_out) if self.expected_amount_out is not None else None,
        }


@dataclass(frozen=True)
class ProtocolFees:
    """Protocol fees paid by the user on a single transaction.

    VIB-3204: Structured accounting of fees captured by the protocol (as
    opposed to gas paid to the chain, which is tracked separately on the
    ExecutionResult). Strategy authors and PnL attribution consumers read
    this to attribute net PnL correctly.

    Semantics:
        - ``swap_fee_usd``: fee captured by the DEX (Uniswap V3 fee tier,
          Aerodrome fee, etc.). For aggregators (Enso, LiFi), this is the
          integrator fee.
        - ``lp_fee_usd``: fee captured by LPs on swaps that traverse their
          pool. For ``LP_COLLECT_FEES`` intents, this is the realized fee
          distribution.
        - ``lending_origination_fee_usd``: origination / withdrawal fee
          charged by lending protocols. Most (Aave V3, Morpho Blue) are
          zero — populate ``Decimal(0)``, not ``None``, so downstream code
          can distinguish "measured to be zero" from "unknown".
        - ``vault_fee_usd``: ERC-4626 deposit/redeem fee.
        - ``perp_fee_usd``: perps open/close fee (not funding — that is
          tracked separately).
        - ``total_usd``: sum of all populated components. Callers should
          use this when attributing net PnL. When ``unavailable_reason``
          is set, ``total_usd`` is ``None`` (the fee exists but cannot be
          measured from the on-chain receipt).

    VIB-3495 — unavailability semantics:
        - ``total_usd = None`` + ``unavailable_reason`` set: the parser
          detected that fees exist but the on-chain data does not carry
          the USD amount (e.g. Aerodrome pool-fee rate is off-chain).
          Attribution must treat this as "unknown", not "zero". This is
          DISTINCT from returning ``None`` from the parser entirely (which
          means "this parser does not implement protocol-fee extraction at
          all"). An explicit ProtocolFees with unavailable_reason signals
          "we checked; the data isn't available in the receipt".
        - ``total_usd = Decimal(0)`` with all components None: the
          protocol verified zero fees were charged (e.g. Aave V3 supply).
        - ``total_usd = Decimal(X > 0)``: measured fee amount.

    All amounts are *USD*. Raw token-denominated fees belong in
    ``extracted_data`` under protocol-specific keys; this struct is for
    attributing PnL impact.
    """

    total_usd: Decimal | None
    swap_fee_usd: Decimal | None = None
    lp_fee_usd: Decimal | None = None
    lending_origination_fee_usd: Decimal | None = None
    vault_fee_usd: Decimal | None = None
    perp_fee_usd: Decimal | None = None
    # VIB-3495: set when the fee is known to exist but cannot be measured
    # from the on-chain receipt (e.g. "protocol_fee_not_emitted_in_receipt").
    # When set, total_usd MUST be None.  Attribution preserves the
    # "unknown" semantic instead of defaulting to zero.
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate the ``total_usd`` == sum-of-populated-components invariant.

        VIB-3204 audit fix (pr-auditor Blocker #3): without this check,
        callers could construct ``ProtocolFees(total_usd=Decimal(0), ...)``
        with ``swap_fee_usd=None`` (a semantic lie: "fees measured to be
        zero" + "swap fee not measured") — two consumers looking at the
        same struct would disagree on whether a fee was paid. PnL
        attribution would then systematically under-attribute swap costs.

        Rule: ``total_usd`` must equal the sum of all populated component
        fields. If no components are populated, ``total_usd`` must be 0
        (vacuously true — the struct represents "nothing measured yet,
        but nothing detected either" = no fee).

        VIB-3495: when ``unavailable_reason`` is set, ``total_usd`` must
        be ``None`` and no component fields may be populated. This is the
        "known-unknown" case.
        """
        # VIB-3495: unavailable path — total_usd must be None, no components.
        if self.unavailable_reason is not None:
            if self.total_usd is not None:
                raise ValueError(
                    "ProtocolFees: when unavailable_reason is set, total_usd must be None. "
                    f"Got total_usd={self.total_usd}"
                )
            components_with_values = [
                name
                for name, val in (
                    ("swap_fee_usd", self.swap_fee_usd),
                    ("lp_fee_usd", self.lp_fee_usd),
                    ("lending_origination_fee_usd", self.lending_origination_fee_usd),
                    ("vault_fee_usd", self.vault_fee_usd),
                    ("perp_fee_usd", self.perp_fee_usd),
                )
                if val is not None
            ]
            if components_with_values:
                raise ValueError(
                    "ProtocolFees: when unavailable_reason is set, no component fields "
                    f"may be populated. Got: {components_with_values}"
                )
            return

        # Normal path — total_usd must be a non-negative Decimal.
        if self.total_usd is None:
            raise ValueError(
                "ProtocolFees.total_usd must be a non-negative Decimal when "
                "unavailable_reason is not set. Use unavailable_reason= for "
                "the known-unknown case."
            )

        components = (
            ("swap_fee_usd", self.swap_fee_usd),
            ("lp_fee_usd", self.lp_fee_usd),
            ("lending_origination_fee_usd", self.lending_origination_fee_usd),
            ("vault_fee_usd", self.vault_fee_usd),
            ("perp_fee_usd", self.perp_fee_usd),
        )
        # CodeRabbit audit fix (round 3): reject negative fee values. A
        # protocol fee is a cost the wallet paid — it cannot be negative.
        # A buggy parser emitting a negative value would silently flip the
        # sign of net PnL in ``PnLAttributor`` because ``protocol_fees_usd``
        # is subtracted from gross. Catch the lie at the struct boundary.
        if self.total_usd < 0:
            raise ValueError(f"ProtocolFees.total_usd must be non-negative, got {self.total_usd}")
        for name, val in components:
            if val is not None and val < 0:
                raise ValueError(f"ProtocolFees.{name} must be non-negative, got {val}")

        populated = [v for _, v in components if v is not None]
        expected_total = sum(populated, Decimal(0))
        if self.total_usd != expected_total:
            raise ValueError(
                "ProtocolFees.total_usd must equal the sum of populated "
                f"components. Got total_usd={self.total_usd}, sum of "
                f"populated={expected_total}. Populated components: "
                f"swap_fee_usd={self.swap_fee_usd}, lp_fee_usd={self.lp_fee_usd}, "
                f"lending_origination_fee_usd={self.lending_origination_fee_usd}, "
                f"vault_fee_usd={self.vault_fee_usd}, perp_fee_usd={self.perp_fee_usd}."
            )

    @property
    def is_unavailable(self) -> bool:
        """True when the fee is known to exist but cannot be measured from receipt data."""
        return self.unavailable_reason is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_usd": str(self.total_usd) if self.total_usd is not None else None,
            "swap_fee_usd": str(self.swap_fee_usd) if self.swap_fee_usd is not None else None,
            "lp_fee_usd": str(self.lp_fee_usd) if self.lp_fee_usd is not None else None,
            "lending_origination_fee_usd": (
                str(self.lending_origination_fee_usd) if self.lending_origination_fee_usd is not None else None
            ),
            "vault_fee_usd": str(self.vault_fee_usd) if self.vault_fee_usd is not None else None,
            "perp_fee_usd": str(self.perp_fee_usd) if self.perp_fee_usd is not None else None,
            "unavailable_reason": self.unavailable_reason,
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "SlippageSource",
    "SwapAmounts",
    "LPCloseData",
    "LPOpenData",
    "BorrowData",
    "SupplyData",
    "PerpData",
    "StakeData",
    "PredictionFill",
    "PredictionSetupTx",
    "ProtocolFees",
    "BridgeData",
]
