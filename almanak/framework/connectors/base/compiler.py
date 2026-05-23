"""Base contract for connector-owned intent compilers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

if TYPE_CHECKING:
    from almanak.framework.connectors.base.swap_adapter import DefaultSwapAdapter
    from almanak.framework.intents.compiler_models import CompilationResult, TokenInfo, TransactionData
    from almanak.framework.intents.vocabulary import (
        CollectFeesIntent,
        LPCloseIntent,
        LPOpenIntent,
        PerpCloseIntent,
        PerpOpenIntent,
        SwapIntent,
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
    swap_pool_selection_mode: str
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


class BaseProtocolCompiler[CompilerContextT: BaseCompilerContext](ABC):
    """Root ABC for connector-owned intent compilers.

    The base intentionally knows nothing about specific intent types. Subclasses
    implement :meth:`compile` and dispatch internally on ``intent.intent_type``
    (or ``isinstance(intent, …)``) to per-primitive handlers they define. Two
    opt-in convenience helpers are provided: :meth:`_check_context` for the
    context-type guard, and :meth:`_unsupported` for the canonical "does not
    support" fail-close. Neither is required — connectors can write their own
    custom fail-close messages, which is the typical pattern.
    """

    protocols: ClassVar[frozenset[str]]
    intents: ClassVar[frozenset[Any]]
    chains: ClassVar[frozenset[str] | None] = None
    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    @abstractmethod
    def compile(self, ctx: CompilerContextT, intent: Any) -> CompilationResult:
        """Compile one intent. Subclass dispatches on intent type internally."""

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

    def compile(self, ctx: CLCompilerContext, intent: Any) -> CompilationResult:
        from almanak.framework.intents.vocabulary import IntentType

        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
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
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.PERP_OPEN:
            return self.compile_perp_open(ctx, intent)
        if intent_type == IntentType.PERP_CLOSE:
            return self.compile_perp_close(ctx, intent)
        return self._unsupported(intent)

    @abstractmethod
    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult: ...


__all__ = [
    "BaseCompilerContext",
    "BaseConcentratedLiquidityCompiler",
    "BasePerpCompiler",
    "BaseProtocolCompiler",
    "CLCompilerContext",
    "CompilerServices",
    "PerpCompilerContext",
    "SwapCompilerContext",
]


def _failed_result(intent_id: str, error: str) -> CompilationResult:
    from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

    return CompilationResult(status=CompilationStatus.FAILED, intent_id=intent_id, error=error)
