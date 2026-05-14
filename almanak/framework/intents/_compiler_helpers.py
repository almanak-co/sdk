"""Pure compiler skeleton helpers shared by ``IntentCompiler._compile_swap``
and ``IntentCompiler._compile_lp_open`` (Phase 6B.2), plus LiFi/TraderJoe
V2 compile paths (Phase 6B.5).

These helpers are pure functions (no I/O, no side effects) that implement
the shared skeleton of the highest-complexity compile paths. They are
designed to be wired into the existing compile methods. Phase 6B.2 added
the shared swap/LP skeleton; Phase 6B.5 adds LiFi-specific value/gas
parsing and a protocol-agnostic bin-step probe used by TraderJoe V2 swap
compilation.

Scope / contract:
    - Every helper here MUST be pure. Anything that needs ``self._gateway_client``,
      ``self._allowance_cache``, RPC access, logging, or time-of-day MUST NOT
      live here. Use callables the caller passes in (e.g. ``now_ts``) if a
      deterministic clock is needed.
    - Helpers do NOT build error messages that tests grep. The caller builds
      the final ``CompilationResult.error`` string so existing string assertions
      in ``tests/unit/intents/test_compiler_swap_lp_characterization.py``
      continue to hold at the wiring stage.
    - No circular imports: this module only imports from the standard library,
      ``..models.reproduction_bundle.ActionBundle``, and the compiler's own
      data classes in ``.compiler_models``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.reproduction_bundle import ActionBundle
    from .compiler_models import TransactionData


# ---------------------------------------------------------------------------
# Slippage / min-amount math
# ---------------------------------------------------------------------------


def compute_min_amount_out(expected_amount: int, max_slippage: Decimal) -> int:
    """Return the lower bound for a transaction output given slippage tolerance.

    Mirrors the single-line math used both in swap compilation (one min_amount_out)
    and in LP_OPEN compilation (one per token). Using a helper here means callers
    can't accidentally apply slippage twice or drift on truncation behaviour.

    Args:
        expected_amount: The expected output amount (integer wei / smallest units).
            Must be >= 0.
        max_slippage: Slippage tolerance as a fraction (``Decimal("0.01")`` = 1%).
            Must be in ``[0, 1]`` inclusive. ``0`` is allowed and yields
            ``expected_amount`` unchanged.

    Returns:
        ``int(expected_amount * (1 - max_slippage))`` using Decimal arithmetic
        so rounding matches the existing compiler code exactly. Always ``int``.

    Raises:
        ValueError: If ``expected_amount`` is negative, or ``max_slippage`` is
            outside ``[0, 1]``.
    """
    if expected_amount < 0:
        raise ValueError(f"expected_amount must be >= 0 (got {expected_amount})")
    if max_slippage < Decimal("0") or max_slippage > Decimal("1"):
        raise ValueError(f"max_slippage must be in [0, 1] (got {max_slippage})")
    return int(Decimal(str(expected_amount)) * (Decimal("1") - max_slippage))


def choose_safer_quote(oracle_estimate: int, quoter_amount: int | None) -> tuple[int, bool]:
    """Pick the safer of the oracle estimate and the on-chain quoter amount.

    Callers use this to produce the slippage basis for ``min_amount_out``:
    when the quoter is available AND lower than the oracle estimate, it
    reflects real pool depth and is the safer floor. Otherwise the oracle
    estimate is used as-is.

    Args:
        oracle_estimate: Oracle-derived expected output (must be > 0 for the
            caller's slippage math to make sense; this helper does not enforce).
        quoter_amount: On-chain quoter output, or ``None`` if the quoter call
            was skipped / failed.

    Returns:
        ``(safer_amount, used_quoter)`` — ``used_quoter`` is True iff the
        quoter value was chosen (strictly less than the oracle estimate).
    """
    if quoter_amount is not None and quoter_amount < oracle_estimate:
        return quoter_amount, True
    return oracle_estimate, False


# ---------------------------------------------------------------------------
# Price impact guard
# ---------------------------------------------------------------------------


class PriceImpactDecision(Enum):
    """Outcome categories for ``check_price_impact``.

    Kept as an ``Enum`` (not strings) so callers can dispatch without
    grep-brittle string matching. Error messages are built by the caller
    because the real compiler's messages include protocol-specific context
    (token symbols, pool address, etc.) that doesn't belong in a pure helper.
    """

    OK = "OK"
    IMPACT_TOO_HIGH = "IMPACT_TOO_HIGH"
    QUOTER_MISSING_FAIL_CLOSED = "QUOTER_MISSING_FAIL_CLOSED"
    SKIPPED_OFFLINE = "SKIPPED_OFFLINE"
    SKIPPED_NO_ORACLE = "SKIPPED_NO_ORACLE"


@dataclass(frozen=True)
class PriceImpactCheckResult:
    """Result of a price-impact guard check (pure data).

    Attributes:
        decision: Categorical outcome.
        price_impact: Computed impact as a ``Decimal`` fraction when the
            computation actually ran. ``None`` for non-IMPACT decisions.
        effective_max_impact: The maximum impact threshold that was applied
            (``intent.max_price_impact`` override or config default).
            ``None`` when the check was skipped.
    """

    decision: PriceImpactDecision
    price_impact: Decimal | None = None
    effective_max_impact: Decimal | None = None


def check_price_impact(
    *,
    oracle_estimate: int,
    quoter_amount: int | None,
    intent_max_impact: Decimal | None,
    config_max_impact: Decimal,
    offline_mode: bool,
    using_placeholders: bool,
) -> PriceImpactCheckResult:
    """Compute and evaluate swap price impact.

    Mirrors the guard in ``IntentCompiler._compile_swap`` (~lines 1510-1554 in
    the current file) so a future wiring PR (Phase 6B.3) can replace that
    branch with a single call plus a result-switch.

    Decision table:
        - ``quoter_amount is not None`` AND ``oracle_estimate > 0`` AND NOT
          ``using_placeholders`` → compute impact, compare to
          ``intent_max_impact or config_max_impact``. Returns OK or
          IMPACT_TOO_HIGH with the computed impact attached.
        - ``using_placeholders`` (unit-test mode): SKIPPED_OFFLINE. Callers
          must not use oracle-only slippage in this mode; the real compiler
          permits it because placeholder prices already decouple the math
          from reality.
        - ``quoter_amount is None`` AND ``oracle_estimate > 0`` AND
          ``offline_mode`` True → SKIPPED_OFFLINE.
        - ``quoter_amount is None`` AND ``oracle_estimate > 0`` AND
          ``offline_mode`` False → QUOTER_MISSING_FAIL_CLOSED (caller fails
          compilation).
        - ``oracle_estimate == 0`` → SKIPPED_NO_ORACLE (nothing to compare
          against).

    Args:
        oracle_estimate: Oracle-derived expected output (wei).
        quoter_amount: On-chain quoter output or None.
        intent_max_impact: Per-intent override or None.
        config_max_impact: Compiler config default (always set by
            ``IntentCompilerConfig.__post_init__``).
        offline_mode: ``using_placeholders OR permission_discovery``
            — matches the compiler's offline-mode semantics and relaxes the
            quoter-missing rule.
        using_placeholders: Whether ``IntentCompiler._using_placeholders``
            is True. Distinct from ``offline_mode`` because the IMPACT branch
            is skipped when using placeholders, but the QUOTER_MISSING branch
            is only relaxed when ``offline_mode`` is True.

    Returns:
        PriceImpactCheckResult with the decision and (for IMPACT decisions)
        the computed impact and effective max.
    """
    if oracle_estimate <= 0:
        return PriceImpactCheckResult(decision=PriceImpactDecision.SKIPPED_NO_ORACLE)

    max_impact = intent_max_impact if intent_max_impact is not None else config_max_impact

    if quoter_amount is not None:
        if using_placeholders:
            return PriceImpactCheckResult(decision=PriceImpactDecision.SKIPPED_OFFLINE)
        price_impact = Decimal(1) - (Decimal(quoter_amount) / Decimal(oracle_estimate))
        if price_impact > max_impact:
            return PriceImpactCheckResult(
                decision=PriceImpactDecision.IMPACT_TOO_HIGH,
                price_impact=price_impact,
                effective_max_impact=max_impact,
            )
        return PriceImpactCheckResult(
            decision=PriceImpactDecision.OK,
            price_impact=price_impact,
            effective_max_impact=max_impact,
        )

    # quoter_amount is None
    if offline_mode:
        return PriceImpactCheckResult(decision=PriceImpactDecision.SKIPPED_OFFLINE)
    return PriceImpactCheckResult(decision=PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED)


# ---------------------------------------------------------------------------
# Deadline
# ---------------------------------------------------------------------------


def compute_deadline(default_deadline_seconds: int, *, now_ts: int | None = None) -> int:
    """Compute a Unix deadline timestamp for a compiled transaction.

    Mirrors the ``int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds``
    line that appears in both ``_compile_swap`` and ``_compile_lp_open``. Accepts
    an injectable ``now_ts`` so tests don't need to patch module globals.

    Args:
        default_deadline_seconds: Seconds to add to the current time. Must be
            a positive integer (a zero-or-negative deadline is a misconfiguration
            that would produce immediately-expired transactions).
        now_ts: Optional explicit current Unix timestamp (seconds). When None,
            uses ``datetime.now(UTC)``.

    Returns:
        Unix timestamp in seconds.

    Raises:
        ValueError: If ``default_deadline_seconds`` is not positive.
    """
    if default_deadline_seconds <= 0:
        raise ValueError(f"default_deadline_seconds must be > 0 (got {default_deadline_seconds})")
    base = now_ts if now_ts is not None else int(datetime.now(UTC).timestamp())
    return base + default_deadline_seconds


# ---------------------------------------------------------------------------
# ActionBundle assembly
# ---------------------------------------------------------------------------


def sum_transaction_gas(transactions: list[TransactionData]) -> int:
    """Sum ``gas_estimate`` across a list of ``TransactionData`` entries.

    Trivial on its own, but extracted so the assembly helper below can stay
    small and the ``total_gas = sum(tx.gas_estimate for tx in transactions)``
    pattern has exactly one definition.
    """
    return sum(tx.gas_estimate for tx in transactions)


def assemble_action_bundle(
    *,
    intent_type: str,
    transactions: list[TransactionData],
    metadata: dict,
) -> ActionBundle:
    """Build an ``ActionBundle`` from a list of typed transactions and metadata.

    Mirrors the identical block that appears at the end of ``_compile_swap``
    (line ~1611) and ``_compile_lp_open`` (line ~2724):

        action_bundle = ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={ ... caller-built ... },
        )

    The caller still owns metadata construction (keys are intent-specific);
    this helper only standardises the ``transactions=[tx.to_dict() ...]``
    mapping so no one accidentally passes raw dataclass instances into a
    field that ``ActionBundle.to_dict()`` already assumes is JSON-serialisable.

    Args:
        intent_type: Typically ``IntentType.SWAP.value`` / ``IntentType.LP_OPEN.value``.
        transactions: Ordered list of ``TransactionData`` to serialize.
        metadata: Intent-specific metadata dict. Passed through untouched so
            existing tests that grep specific keys (``amount_in``,
            ``min_amount_out``, ``tick_lower``, ...) continue to match.

    Returns:
        A new ``ActionBundle`` ready for assignment to
        ``CompilationResult.action_bundle``.
    """
    # Imported here (not at module scope) to keep the helper module's import
    # graph minimal at test-collection time. ``ActionBundle`` is also
    # imported under TYPE_CHECKING at the top of this file for type hints.
    from ..models.reproduction_bundle import ActionBundle as _ActionBundle

    return _ActionBundle(
        intent_type=intent_type,
        transactions=[tx.to_dict() for tx in transactions],
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# LiFi aggregator helpers (Phase 6B.5)
#
# LiFi's quote.transaction_request carries `value` and `gas_limit` as strings
# that may be decimal or hex ("0x..."). The compile path parses these twice
# (once for TX value, once for gas) and picks the best gas estimate from
# multiple candidate sources. Extracting them here keeps the compile method's
# control flow linear and matches the existing math exactly.
# ---------------------------------------------------------------------------


def parse_lifi_tx_value(raw_value: object | None) -> int:
    """Parse LiFi ``transaction_request.value`` into an integer wei amount.

    LiFi returns the TX value as either a decimal string, a ``0x``-prefixed
    hex string, or ``None``/empty (native-free swaps). Every shape must map
    to a non-negative integer ready for ``TransactionData.value``.

    Args:
        raw_value: The raw ``value`` field from ``quote.transaction_request``.
            Accepted shapes: ``None`` / empty string / ``"0"`` → ``0``;
            decimal string → parsed as base-10; ``0x``-prefixed string → hex.

    Returns:
        Integer wei amount (``0`` for missing/empty input). Always
        non-negative — negative parses raise ``ValueError``.

    Raises:
        ValueError: If the string is non-empty but cannot be parsed as int,
            or if the parsed value is negative (TX values are unsigned wei).
    """
    if not raw_value:
        return 0
    raw_str = str(raw_value)
    parsed = int(raw_str, 16) if raw_str.startswith("0x") else int(raw_str)
    if parsed < 0:
        raise ValueError(f"LiFi transaction_request.value must be non-negative, got {parsed} from {raw_str!r}")
    return parsed


def choose_lifi_gas_estimate(
    *,
    total_gas_estimate: int,
    gas_limit: object | None,
    default: int = 200_000,
) -> int:
    """Pick the best available gas estimate for a LiFi-built transaction.

    Preference order (matches existing compile code):
        1. ``quote.estimate.total_gas_estimate`` when positive.
        2. ``quote.transaction_request.gas_limit`` (decimal or hex string).
        3. ``default`` (falls back to 200_000 to match pre-refactor behaviour).

    Args:
        total_gas_estimate: LiFi ``quote.estimate.total_gas_estimate``. Zero
            means "no estimate"; negative values are treated identically.
        gas_limit: LiFi ``quote.transaction_request.gas_limit`` — string,
            integer, or ``None``. Hex-prefixed strings are parsed as hex.
        default: Fallback gas when neither signal is usable.

    Returns:
        A positive integer gas estimate.
    """
    if total_gas_estimate and total_gas_estimate > 0:
        return total_gas_estimate
    if gas_limit:
        try:
            gl = str(gas_limit)
            parsed = int(gl, 16) if gl.startswith("0x") else int(gl)
        except (ValueError, TypeError):
            return default
        # Gas of 0 or negative would produce a TX that can't include the
        # intrinsic gas cost; treat those as unusable and fall through.
        if parsed > 0:
            return parsed
    return default


# ---------------------------------------------------------------------------
# TraderJoe V2 helpers (Phase 6B.5)
#
# TraderJoe V2's Liquidity Book AMM exposes multiple pools per pair, one per
# ``bin_step``. The compiler probes a fixed order of common bin steps until
# a pool is found. Extracting this loop keeps the auto-detect logic isolated
# and reusable for LP-open, LP-close, and swap paths.
# ---------------------------------------------------------------------------


def probe_traderjoe_bin_step(
    *,
    probe: object,
    token_a: str,
    token_b: str,
    not_found_exception: type[BaseException],
    candidates: tuple[int, ...] = (20, 25, 15, 10, 50, 5, 100, 1),
    is_liquid: object | None = None,
) -> tuple[int | None, int | None, BaseException | None]:
    """Find the first TraderJoe V2 bin step with an existing (liquid) pool for a pair.

    Calls ``probe(token_a, token_b, bin_step)`` for each candidate in order
    and returns the first bin step whose probe succeeds. The caller is
    responsible for the final fail-closed messaging — this helper only
    reports "found" vs "not found" and bubbles unexpected exceptions back
    (along with the bin step that broke) so the caller can attach
    protocol-specific context.

    Liquidity-aware mode (VIB-4374): when ``is_liquid`` is supplied, the
    returned pool address is additionally screened by ``is_liquid(addr)``.
    A False return means "pool exists but has no usable liquidity" — the
    probe iterates past it like the not-found case. This mirrors the
    blueprint's Pool Selection Policy for V3-style swaps (do not assume a
    single fee tier has viable liquidity in both directions) and matches
    the failure mode observed on arbitrum TJv2 WETH/USDC, where the first
    autodetected bin_step is an empty pool while bin_step=15 is the live
    one. ``is_liquid`` is optional to preserve existing callers that have
    no cheap way to probe reserves.

    Args:
        probe: Callable invoked as ``probe(token_a, token_b, bin_step)``.
            Typically ``tj_adapter.sdk.get_pool_address``. Must raise
            ``not_found_exception`` on "no pool" and any other exception
            on an unexpected failure (RPC flake, token resolution, etc.).
        token_a: First token address (order irrelevant for probe semantics).
        token_b: Second token address.
        not_found_exception: Concrete exception class that means "probe
            found no pool at this bin step" — the probe iterates past this
            cleanly. Passed explicitly so the helper doesn't have to import
            the connector-specific exception.
        candidates: Tuple of bin steps to probe, in preference order. The
            default mirrors the order used by the compiler (popular first).
        is_liquid: Optional callable invoked as ``is_liquid(pool_address)``
            after a successful pool lookup. Return True to accept the
            candidate, False to iterate past as if it were not found.
            Exceptions raised here are treated like an unexpected probe
            failure and reported via the ``(None, bs, exc)`` return shape.

    Returns:
        ``(bin_step, None, None)`` on success. ``(None, None, None)`` when
        every candidate raised ``not_found_exception`` (or was screened out
        by ``is_liquid``). ``(None, bs, exc)`` when candidate ``bs`` raised
        an unexpected exception — the caller converts this into a ``FAILED``
        ``CompilationResult`` naming the broken bin step.
    """
    if not callable(probe):
        raise TypeError("probe must be callable")
    if is_liquid is not None and not callable(is_liquid):
        raise TypeError("is_liquid must be callable when provided")
    for bin_step in candidates:
        try:
            pool_address = probe(token_a, token_b, bin_step)
            if is_liquid is not None and not is_liquid(pool_address):
                continue
            return bin_step, None, None
        except not_found_exception:
            continue
        except Exception as exc:  # noqa: BLE001 — caller reshapes
            return None, bin_step, exc
    return None, None, None


# ---------------------------------------------------------------------------
# Gateway/RPC resolution for SDK adapters that accept either one
# (Phase 6B.5)
#
# Several compile paths (TraderJoe V2 swap, TraderJoe V2 LP open, TraderJoe
# V2 LP close, ...) share the same normalisation: a disconnected gateway
# client is treated as absent and the caller falls back to rpc_url. This
# helper encodes the normalisation without the compiler's RPC lookup —
# callers still provide the two sources.
# ---------------------------------------------------------------------------


def normalise_gateway_or_rpc(
    *,
    gateway_client: object | None,
    rpc_url_supplier: object,
) -> tuple[object | None, str | None]:
    """Normalise ``(gateway_client, rpc_url)`` for adapters that accept either.

    Adapters like ``TraderJoeV2Adapter`` can be driven either by a connected
    gateway client (production) or by a direct RPC URL (local/backtest).
    The compiler treats a gateway client that answers ``is_connected=False``
    the same as no client at all, then falls back to the RPC URL supplier.

    Args:
        gateway_client: Candidate gateway client. Must expose
            ``is_connected`` (bool attribute or property) when non-None.
            A value of ``None`` is accepted and forwarded as ``None``.
        rpc_url_supplier: Zero-arg callable returning the chain's RPC URL
            (or ``None`` / empty string). The supplier is only invoked when
            ``gateway_client`` is unusable — saves a lookup when the
            gateway path is taken.

    Returns:
        ``(client_or_none, rpc_url_or_none)``. Exactly one of these will be
        set to a truthy value in the happy path; callers must raise on the
        (None, None) / (None, "") case with their own error message so
        protocol-specific context (adapter name, config hint) is preserved.
    """
    client: object | None = gateway_client
    if client is not None and not getattr(client, "is_connected", False):
        client = None

    if client is not None:
        return client, None

    rpc_url = rpc_url_supplier() if callable(rpc_url_supplier) else None
    return None, rpc_url or None
