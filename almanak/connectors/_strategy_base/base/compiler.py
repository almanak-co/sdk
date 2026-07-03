"""Base contract for connector-owned intent compilers."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter
    from almanak.framework.intents.bridge import BridgeIntent
    from almanak.framework.intents.compiler_models import CompilationResult, TokenInfo, TransactionData
    from almanak.framework.intents.vocabulary import (
        CollectFeesIntent,
        LPCloseIntent,
        LPOpenIntent,
        PerpCloseIntent,
        PerpOpenIntent,
        StakeIntent,
        SwapIntent,
        UnstakeIntent,
    )


class CompilerServices(Protocol):
    """Typed boundary for framework services a connector compiler may use."""

    def resolve_token(self, token: str, chain: str | None = None) -> TokenInfo | None: ...

    def resolve_dest_wallet(self, dest_chain: str) -> str: ...

    def require_token_price(self, symbol: str) -> Decimal: ...

    def usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int: ...

    def calculate_expected_output(self, amount_in: int, from_token: TokenInfo, to_token: TokenInfo) -> int: ...

    def build_approve_tx(self, token_address: str, spender: str, amount: int) -> list[TransactionData]: ...

    def get_chain_rpc_url(self) -> str | None: ...

    def validate_pool(self, result: Any, intent_id: str) -> CompilationResult | None: ...

    def format_amount(self, amount: int, decimals: int) -> str: ...

    def parse_pool_info(self, pool: str) -> tuple[TokenInfo, TokenInfo, int, bool] | None: ...

    def price_to_tick(self, price: Decimal, *, token0_decimals: int, token1_decimals: int) -> int: ...

    def get_tick_spacing(self, fee_tier: int) -> int: ...

    def get_wrapped_native_address(self) -> str | None: ...

    def query_position_liquidity(self, position_manager: str, token_id: int) -> int | None: ...

    def query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]: ...

    def query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None: ...

    def query_erc20_balance_for_chain(self, token_address: str, wallet_address: str, chain: str) -> int | None: ...

    def query_native_balance_for_chain(self, wallet_address: str, chain: str) -> int | None: ...

    def eth_call(self, to: str, data: str, *, chain: str | None = None) -> str | None: ...

    def default_swap_adapter(self, protocol: str) -> DefaultSwapAdapter: ...


@dataclass(frozen=True, kw_only=True)
class BaseCompilerContext:
    """Primitive-agnostic context passed from the framework compiler to connectors.

    Holds only fields that apply to *any* on-chain compiler — chain/RPC
    plumbing, framework services, and ``default_deadline_seconds`` (every
    on-chain tx has a deadline, lending/perp included). Swap-specific
    knobs live on :class:`SwapCompilerContext`; LP-specific machinery on
    :class:`CLCompilerContext`.
    """

    chain: str
    wallet_address: str
    rpc_url: str | None
    rpc_timeout: float
    permission_discovery: bool
    allow_placeholder_prices: bool
    token_resolver: Any
    gateway_client: Any
    price_oracle: Any
    cache: Any
    services: CompilerServices
    default_protocol: str = ""
    # Universal tx concept — any on-chain tx (swap, LP, lending supply,
    # perp open, bridge call) wants a block-timestamp-relative deadline.
    # Default mirrors IntentCompilerConfig so direct construction in
    # unit-test fixtures keeps working without per-fixture updates.
    default_deadline_seconds: int = 600


@dataclass(frozen=True, kw_only=True)
class SwapCompilerContext(BaseCompilerContext):
    """Swap-pipeline context — for any connector that compiles swaps.

    Holds the slippage / price-impact knobs the swap pipeline reads.
    Future non-CL swap compilers (Algebra forks, custom AMMs) inherit
    from here, not from :class:`CLCompilerContext`. Lending/perp/bridge
    connectors do NOT need these fields and should subclass
    :class:`BaseCompilerContext` directly (or define their own
    intermediate context if a pattern emerges).
    """

    max_price_impact_pct: Decimal = Decimal("0.05")
    using_placeholders: bool = False


@dataclass(frozen=True, kw_only=True)
class CLAdapterFactoryContext:
    """Inputs needed to build concentrated-liquidity adapter factories.

    The framework owns runtime plumbing such as chain, RPC, and config values.
    Connector compilers own the concrete adapter classes they need for those
    factories. Keeping this as a small value object prevents framework compiler
    context assembly from importing connector-specific adapters.
    """

    chain: str
    rpc_url: str | None
    rpc_timeout: float
    gateway_client: Any
    swap_pool_selection_mode: Literal["auto", "fixed"]
    fixed_swap_fee_tier: int | None
    default_swap_adapter_cls: Callable[..., Any] | None = None


@dataclass(frozen=True, kw_only=True)
class CLCompilerContext(SwapCompilerContext):
    """Concentrated-liquidity compiler context.

    Adds the CL-specific machinery (swap-adapter / LP-adapter factories,
    fee-tier selection state, LP slippage) on top of the swap-pipeline
    knobs from :class:`SwapCompilerContext` and the generic infra from
    :class:`BaseCompilerContext`.
    """

    protocol: str
    default_swap_adapter_factory: Callable[[str], Any]
    lp_adapter_factory: Callable[[str], Any]
    swap_pool_selection_mode: Literal["auto", "fixed"]
    fixed_swap_fee_tier: int | None
    default_lp_slippage: Decimal


@dataclass(frozen=True, kw_only=True)
class PerpCompilerContext(BaseCompilerContext):
    """Perpetuals compiler context.

    Perp connector compilers need the normalized protocol key because several
    venue surfaces share one implementation while preserving distinct strategy
    protocol names, e.g. ``aster_perps`` and ``pancakeswap_perps``.
    """

    protocol: str


class PreflightOutcome(Enum):
    """Result class of a pre-submit feasibility check (VIB-5374 / RC-2).

    A connector's :meth:`BaseProtocolCompiler.preflight` returns one of these to
    say whether an intent can *structurally* land on-chain BEFORE any calldata is
    built — surfacing doomed intents as clean compile FAILs instead of paying gas
    on an inevitable on-chain revert. Generalises the hardcoded VIB-3823
    LP_OPEN zero-liquidity gate into a declared, connector-owned hook.
    """

    #: The intent can proceed; the compiler builds calldata as usual.
    FEASIBLE = "feasible"
    #: Structurally doomed (expired market, native fee > balance, borrow > LTV
    #: capacity). Retrying with the same inputs reproduces the same on-chain
    #: revert, so the seam emits a **permanent** compile FAIL the state machine
    #: routes to HOLD — never the data-class retry budget, never the breaker.
    INFEASIBLE = "infeasible"
    #: A transient data gap (a required on-chain read was unavailable) left
    #: feasibility *undetermined*. The seam emits a **retryable** compile FAIL so
    #: the breaker treats it as a tolerant data-class failure, not an action
    #: failure — fresh data on the next iteration may resolve it.
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class PreflightVerdict:
    """Verdict returned by :meth:`BaseProtocolCompiler.preflight`.

    ``error_prefix`` is a stable, venue-specific token strategies (and the
    retry-classification keyword table in
    ``almanak.framework.intents.error_keywords``) match on — e.g.
    ``"PENDLE_MARKET_EXPIRED"``. It MUST NOT contain the substring ``"revert"``:
    the state machine classifies any error containing ``revert`` as a transient
    REVERT *before* the permanent-keyword table is consulted (cf. the VIB-3828 /
    VIB-3818 hazard pinned in ``error_keywords.py``).
    """

    outcome: PreflightOutcome
    reason: str = ""
    error_prefix: str = ""

    @classmethod
    def feasible(cls) -> PreflightVerdict:
        """Canonical FEASIBLE verdict (the default — intent may proceed)."""
        return cls(outcome=PreflightOutcome.FEASIBLE)


class BaseProtocolCompiler[CompilerContextT: BaseCompilerContext](ABC):
    """Root ABC for connector-owned intent compilers.

    The base intentionally knows nothing about specific intent types. Subclasses
    implement :meth:`compile` and dispatch internally on ``intent.intent_type``
    (or ``isinstance(intent, …)``) to per-primitive handlers they define. Two
    opt-in convenience helpers are provided: :meth:`_check_context` for the
    context-type guard, and :meth:`_unsupported` for the canonical "does not
    support" fail-close. Neither is required — connectors can write their own
    custom fail-close messages, which is the typical pattern.

    Feasibility preflight (VIB-5374 / RC-2)
    ---------------------------------------
    Every category base :meth:`compile` funnels through :meth:`_check_context`
    then :meth:`_run_preflight` BEFORE per-primitive dispatch. The default
    :meth:`preflight` returns FEASIBLE so existing compiles are byte-identical;
    a connector opts a venue in by overriding :meth:`preflight` on its compiler
    class — zero edits to ``intents/compiler.py``, ``settings.py``, or any
    framework file. The override IS the registration.
    """

    protocols: ClassVar[frozenset[str]]
    intents: ClassVar[frozenset[Any]]
    chains: ClassVar[frozenset[str] | None] = None
    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    @abstractmethod
    def compile(self, ctx: CompilerContextT, intent: Any) -> CompilationResult:
        """Compile one intent. Subclass dispatches on intent type internally."""

    def preflight(self, ctx: CompilerContextT, intent: Any) -> PreflightVerdict:
        """Pre-submit feasibility check, run before per-primitive dispatch.

        The base returns FEASIBLE unconditionally — connectors override this to
        reject structurally-doomed intents (expired Pendle markets, GMX/Stargate
        native-fee shortfalls, Euler over-LTV borrows) at compile time. An
        override MUST be side-effect-free and use only ``ctx``-mediated reads
        (``ctx.services.*`` / ``ctx.gateway_client`` / ``ctx.price_oracle``) — no
        raw RPC or HTTP (gateway-boundary rule). Return ``UNAVAILABLE`` (not
        ``INFEASIBLE``) when a required read could not be performed, so a
        transient data gap never gets stamped as a permanent failure.
        """
        return PreflightVerdict.feasible()

    def _run_preflight(self, ctx: Any, intent: Any) -> CompilationResult | None:
        """Run :meth:`preflight` and convert a non-FEASIBLE verdict to a FAILED result.

        Returns ``None`` on FEASIBLE (the common case → caller proceeds with
        per-primitive dispatch). On INFEASIBLE the FAILED result carries the
        stable ``error_prefix`` so the state machine's keyword table classifies
        it as ``COMPILATION_PERMANENT`` (fail-fast → HOLD). On UNAVAILABLE the
        result is marked retryable so the breaker treats it as data-class.

        Defensive by contract: a connector ``preflight`` that itself raises must
        NOT take down the compile — an exception is swallowed and treated as
        FEASIBLE (fail-open), because a buggy feasibility check should degrade to
        the pre-VIB-5374 behaviour (let the intent compile), never harden into a
        false reject.
        """
        try:
            verdict = self.preflight(ctx, intent)
        except Exception:  # pragma: no cover - defensive; preflight must be pure
            logger.exception("%s.preflight raised; treating intent as FEASIBLE (fail-open)", type(self).__name__)
            return None
        if verdict.outcome is PreflightOutcome.FEASIBLE:
            return None
        retryable = verdict.outcome is PreflightOutcome.UNAVAILABLE
        error = f"{verdict.error_prefix}: {verdict.reason}" if verdict.error_prefix else verdict.reason
        return _failed_result(getattr(intent, "intent_id", ""), error, retryable=retryable)

    def _check_context(self, ctx: Any, intent: Any) -> CompilationResult | None:
        """Return a FAILED result if ``ctx`` isn't the declared ``context_type``, else ``None``."""
        if not isinstance(ctx, self.context_type):
            return _failed_result(
                getattr(intent, "intent_id", ""),
                f"{type(self).__name__} requires {self.context_type.__name__}, got {type(ctx).__name__}",
            )
        return None

    def _unsupported(self, intent: Any) -> CompilationResult:
        """Canonical "does not support intent type X" result for unhandled intents."""
        return _failed_result(
            getattr(intent, "intent_id", ""),
            f"{type(self).__name__} does not support intent type {getattr(intent, 'intent_type', None)}",
        )


class BaseConcentratedLiquidityCompiler(BaseProtocolCompiler[CLCompilerContext]):
    """Concentrated-liquidity connector compilers — swap + LP + collect-fees.

    The CL category has a known shape: every CL connector compiles swaps,
    LP opens/closes, and standalone fee collection. The four primitives are
    declared abstract here so missing implementations fail at class
    definition rather than at runtime dispatch. A concrete :meth:`compile`
    is provided since every CL connector wants the same intent-type
    dispatch — subclasses only implement the four primitives.
    """

    context_type: ClassVar[type[BaseCompilerContext]] = CLCompilerContext

    @staticmethod
    def _fetch_lp_pool_slot0(ctx: CLCompilerContext, pool_check: Any) -> tuple[int, int] | None:
        """Read a V3-style pool's ``slot0()`` (sqrtPriceX96, tick) for LP sizing.

        Shared by every V3-family CL compiler (Uniswap V3 and its forks) so the
        forks do not import each other's compilers. Returns ``None`` (caller
        falls back to oracle-derived amounts) on any failure.
        """
        if not pool_check.pool_address:
            return None
        gateway_connected = ctx.gateway_client is not None and ctx.gateway_client.is_connected
        if not (ctx.rpc_url or gateway_connected):
            return None
        from almanak.connectors._strategy_base.v3_pool_validation import fetch_v3_pool_sqrt_price_x96

        try:
            slot0_result = fetch_v3_pool_sqrt_price_x96(
                pool_check.pool_address,
                ctx.rpc_url,
                chain=ctx.chain,
                gateway_client=ctx.gateway_client,
            )
        except Exception as exc:
            logger.warning(
                "LP slot0 lookup failed for pool %s; proceeding with oracle-derived amounts "
                "which may cause 'Price slippage check' revert if oracle/pool prices diverge: %s",
                pool_check.pool_address,
                exc,
            )
            return None
        if slot0_result is None:
            return None
        sqrt_price_x96, current_tick = slot0_result
        if sqrt_price_x96 is None or sqrt_price_x96 <= 0 or current_tick is None:
            return None
        return sqrt_price_x96, current_tick

    def build_default_swap_adapter_factory(
        self,
        factory_context: CLAdapterFactoryContext,
    ) -> Callable[[str], Any]:
        """Build the swap adapter factory for CL-family swap legs.

        Most CL-family compilers use the shared V3-style swap adapter. Concrete
        connectors may override this if their swap router requires a different
        adapter while still sharing the CL context shape.
        """
        adapter_cls = factory_context.default_swap_adapter_cls
        if adapter_cls is None:
            from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter

            adapter_cls = DefaultSwapAdapter

        def factory(protocol: str) -> Any:
            return adapter_cls(
                factory_context.chain,
                protocol,
                pool_selection_mode=factory_context.swap_pool_selection_mode,
                fixed_fee_tier=factory_context.fixed_swap_fee_tier,
                rpc_url=factory_context.rpc_url,
                rpc_timeout=factory_context.rpc_timeout,
                gateway_client=factory_context.gateway_client,
            )

        return factory

    def build_lp_adapter_factory(
        self,
        factory_context: CLAdapterFactoryContext,
    ) -> Callable[[str], Any]:
        """Build the LP adapter factory for CL-family LP legs.

        Connectors that compile NFT-position LP operations override this to
        return their connector-owned adapter. The default is intentionally lazy:
        swap-only CL compilers can still share ``CLCompilerContext`` without
        importing an LP adapter they never use.
        """
        _ = factory_context

        def unsupported(protocol: str) -> Any:
            raise NotImplementedError(
                f"{type(self).__name__} does not provide an LP adapter factory for protocol {protocol!r}"
            )

        return unsupported

    def compile(self, ctx: CLCompilerContext, intent: Any) -> CompilationResult:
        from almanak.framework.intents.vocabulary import IntentType

        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if (pf := self._run_preflight(ctx, intent)) is not None:
            return pf
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        if intent_type == IntentType.LP_OPEN:
            return self.compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self.compile_lp_close(ctx, intent)
        if intent_type == IntentType.LP_COLLECT_FEES:
            return self.compile_collect_fees(ctx, intent)
        return self._unsupported(intent)

    @abstractmethod
    def compile_swap(self, ctx: CLCompilerContext, intent: SwapIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_lp_open(self, ctx: CLCompilerContext, intent: LPOpenIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_lp_close(self, ctx: CLCompilerContext, intent: LPCloseIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_collect_fees(self, ctx: CLCompilerContext, intent: CollectFeesIntent) -> CompilationResult: ...


class BasePerpCompiler(BaseProtocolCompiler[PerpCompilerContext]):
    """Perpetuals connector compilers — open and close positions."""

    context_type: ClassVar[type[BaseCompilerContext]] = PerpCompilerContext

    def compile(self, ctx: PerpCompilerContext, intent: Any) -> CompilationResult:
        from almanak.framework.intents.vocabulary import IntentType

        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if (pf := self._run_preflight(ctx, intent)) is not None:
            return pf
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.PERP_OPEN:
            return self.compile_perp_open(ctx, intent)
        if intent_type == IntentType.PERP_CLOSE:
            return self.compile_perp_close(ctx, intent)
        if intent_type == IntentType.PERP_CANCEL_ORDER:
            return self.compile_perp_cancel(ctx, intent)
        if intent_type == IntentType.PERP_WITHDRAW:
            return self.compile_perp_withdraw(ctx, intent)
        return self._unsupported(intent)

    @abstractmethod
    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult: ...

    def compile_perp_cancel(self, ctx: PerpCompilerContext, intent: Any) -> CompilationResult:
        """Compile a PERP_CANCEL_ORDER intent (cancel a pending order, recover collateral).

        Not abstract: pending-order cancellation is venue-specific (GMX V2 only, for
        now — VIB-5568). Perp connectors that do not support it inherit this default,
        which reports unsupported. A connector that supports it overrides this AND
        declares ``IntentType.PERP_CANCEL_ORDER`` in its ``intents`` so the registry
        routes cancels only to it.
        """
        return self._unsupported(intent)

    def compile_perp_withdraw(self, ctx: PerpCompilerContext, intent: Any) -> CompilationResult:
        """Compile a PERP_WITHDRAW intent (withdraw free margin off-chain → L1).

        Not abstract: off-chain-account withdrawal is venue-specific (Hyperliquid
        only, for now — VIB-5617; the CoreWriter spotSend HyperCore→HyperEVM bridge).
        Perp connectors that do not support it inherit this default, which reports
        unsupported. A connector that supports it overrides this AND declares
        ``IntentType.PERP_WITHDRAW`` in its ``intents`` so the registry routes
        withdraws only to it.
        """
        return self._unsupported(intent)


class BaseBridgeCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Bridge connector compilers — cross-chain transfer intents."""

    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        from almanak.framework.intents.vocabulary import IntentType

        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if (pf := self._run_preflight(ctx, intent)) is not None:
            return pf
        if getattr(intent, "intent_type", None) == IntentType.BRIDGE:
            return self.compile_bridge(ctx, intent)
        return self._unsupported(intent)

    @abstractmethod
    def compile_bridge(self, ctx: BaseCompilerContext, intent: BridgeIntent) -> CompilationResult: ...


class BaseStakingCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Staking connector compilers — stake and unstake intents."""

    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        from almanak.framework.intents.vocabulary import IntentType

        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if (pf := self._run_preflight(ctx, intent)) is not None:
            return pf
        if self.chains is not None and ctx.chain not in self.chains:
            supported = ", ".join(sorted(self.chains))
            return _failed_result(
                getattr(intent, "intent_id", ""),
                f"{type(self).__name__} only supported on {supported}, got: {ctx.chain}",
            )
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.STAKE:
            return self.compile_stake(ctx, intent)
        if intent_type == IntentType.UNSTAKE:
            return self.compile_unstake(ctx, intent)
        return self._unsupported(intent)

    @abstractmethod
    def compile_stake(self, ctx: BaseCompilerContext, intent: StakeIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_unstake(self, ctx: BaseCompilerContext, intent: UnstakeIntent) -> CompilationResult: ...

    def _bundle_to_result(self, intent: Any, action_bundle: Any, *, tx_type: str) -> CompilationResult:
        """Convert adapter ActionBundle dict transactions into compiler result transactions."""
        from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData

        if action_bundle.metadata.get("error"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=action_bundle.metadata["error"],
                intent_id=getattr(intent, "intent_id", ""),
            )

        transactions = [
            TransactionData(
                to=tx_dict.get("to", ""),
                value=int(tx_dict.get("value") or 0),
                data=tx_dict.get("data", "0x"),
                gas_estimate=int(tx_dict.get("gas_estimate") or 0),
                description=tx_dict.get("description", ""),
                tx_type=tx_dict.get("tx_type", tx_type),
            )
            for tx_dict in action_bundle.transactions
        ]
        return CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=getattr(intent, "intent_id", ""),
            action_bundle=action_bundle,
            transactions=transactions,
            total_gas_estimate=sum(tx.gas_estimate for tx in transactions),
            warnings=[],
        )


__all__ = [
    "BaseCompilerContext",
    "BaseBridgeCompiler",
    "CLAdapterFactoryContext",
    "BaseConcentratedLiquidityCompiler",
    "BasePerpCompiler",
    "BaseProtocolCompiler",
    "BaseStakingCompiler",
    "CLCompilerContext",
    "CompilerServices",
    "PerpCompilerContext",
    "PreflightOutcome",
    "PreflightVerdict",
    "SwapCompilerContext",
]


def _failed_result(intent_id: str, error: str, *, retryable: bool = False) -> CompilationResult:
    from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

    return CompilationResult(status=CompilationStatus.FAILED, intent_id=intent_id, error=error, is_transient=retryable)
